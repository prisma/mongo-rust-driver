use std::{pin::Pin, task::{Context, Poll}, marker::PhantomData};

use bson::RawDocument;
use futures_util::FutureExt;
use futures_core::future::BoxFuture;
use futures_core::Stream;
use serde::{Deserialize, de::DeserializeOwned};
use tokio::sync::oneshot;

use crate::{error::{Result, Error}, Client, operation::GetMore, change_stream::event::ResumeToken, ClientSession, cmap::conn::PinnedConnectionHandle, client::options::ServerAddress};

use super::{common::{CursorState, Advance, kill_cursor}, CursorInformation, CursorSpecification, PinnedConnection};

/// Skunkworks cursor re-impl
pub struct InnerCursor {
    client: Client,
    info: CursorInformation,
    state: CursorState,
    session: Option<ClientSession>,
    drop_address: Option<ServerAddress>,
    #[cfg(test)]
    kill_watcher: Option<oneshot::Sender<()>>,
}

impl InnerCursor {
    async fn advance(&mut self) -> Result<bool> {
        loop {
            match self.state.advance() {
                Advance::HasValue => return Ok(true),
                Advance::Exhausted => return Ok(false),
                Advance::NeedGetMore => (),
            }

            let get_more = GetMore::new(self.info.clone(), self.state.pinned_connection.handle());
            let result = self.client.execute_operation(get_more, None).await;
            self.state.handle_result(result)?;
        }
    }

    fn current(&self) -> &RawDocument {
        self.state.buffer().current().unwrap()
    }

    /// Extract the stored implicit session, if any.  The provider cannot be started again after
    /// this call.
    fn take_implicit_session(&mut self) -> Option<ClientSession> {
        self.session.take()
    }

    fn deserialize_current<'a, T>(&'a self) -> Result<T>
    where
        T: Deserialize<'a>,
    {
        bson::from_slice(self.current().as_bytes()).map_err(Error::from)
    }    
}

impl Drop for InnerCursor {
    fn drop(&mut self) {
        if self.state.exhausted {
            return;
        }
        kill_cursor(
            self.client.clone(),
            &self.info.ns,
            self.info.id,
            self.state.pinned_connection.replicate(),
            self.drop_address.take(),
            #[cfg(test)]
            self.kill_watcher.take(),
        );
    }
}

enum CursorMode {
    Manual(InnerCursor),
    Streaming(BoxFuture<'static, (InnerCursor, Result<bool>)>),
}

/// skunkworks cursor re-impl
pub struct Cursor<T> {
    mode: CursorMode,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Cursor<T> {
    /// Move the cursor forward, potentially triggering requests to the database for more results
    /// if the local buffer has been exhausted.
    ///
    /// This will keep requesting data from the server until either the cursor is exhausted
    /// or batch with results in it has been received.
    ///
    /// The return value indicates whether new results were successfully returned (true) or if
    /// the cursor has been closed (false).
    ///
    /// Note: [`Cursor::current`] and [`Cursor::deserialize_current`] must only be called after
    /// [`Cursor::advance`] returned `Ok(true)`. It is an error to call either of them without
    /// calling [`Cursor::advance`] first or after [`Cursor::advance`] returns an error / false.
    pub async fn advance(&mut self) -> Result<bool> {
        self.inner_mut()?.advance().await
    }

    /// Returns a reference to the current result in the cursor.
    ///
    /// # Panics
    /// [`Cursor::advance`] must return `Ok(true)` before [`Cursor::current`] can be
    /// invoked. Calling [`Cursor::current`] after [`Cursor::advance`] does not return true
    /// or without calling [`Cursor::advance`] at all may result in a panic.
    ///
    /// ```
    /// # use mongodb::{Client, bson::Document, error::Result};
    /// # async fn foo() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    /// # let coll = client.database("stuff").collection::<Document>("stuff");
    /// let mut cursor = coll.find(None, None).await?;
    /// while cursor.advance().await? {
    ///     println!("{:?}", cursor.current());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn current(&self) -> &RawDocument {
        self.inner_mut().unwrap().current()
    }

    /// Deserialize the current result to the generic type associated with this cursor.
    ///
    /// # Panics
    /// [`Cursor::advance`] must return `Ok(true)` before [`Cursor::deserialize_current`] can be
    /// invoked. Calling [`Cursor::deserialize_current`] after [`Cursor::advance`] does not return
    /// true or without calling [`Cursor::advance`] at all may result in a panic.
    ///
    /// ```
    /// # use mongodb::{Client, error::Result};
    /// # async fn foo() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    /// # let db = client.database("foo");
    /// use serde::Deserialize;
    ///
    /// #[derive(Debug, Deserialize)]
    /// struct Cat<'a> {
    ///     #[serde(borrow)]
    ///     name: &'a str
    /// }
    ///
    /// let coll = db.collection::<Cat>("cat");
    /// let mut cursor = coll.find(None, None).await?;
    /// while cursor.advance().await? {
    ///     println!("{:?}", cursor.deserialize_current()?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn deserialize_current<'a>(&'a self) -> Result<T>
    where
        T: Deserialize<'a>,
    {
        self.inner()?.deserialize_current()
    }

    /// Update the type streamed values will be parsed as.
    pub fn with_type<'a, D>(self) -> Cursor<D>
    where
        D: Deserialize<'a>,
    {
        Cursor {
            mode: self.mode,
            _phantom: Default::default(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new(
        client: Client,
        spec: CursorSpecification,
        session: Option<ClientSession>,
        pin: Option<PinnedConnectionHandle>,
    ) -> Self {
        let (state, info) = CursorState::new(spec, PinnedConnection::new(pin));
        Self {
            mode: CursorMode::Manual(InnerCursor {
                client,
                info,
                state,
                session,
                drop_address: None,
                #[cfg(test)]
                kill_watcher: None,    
            }),
            _phantom: Default::default(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn post_batch_resume_token(&self) -> Result<Option<&ResumeToken>> {
        Ok(self.inner()?.state.post_batch_resume_token.as_ref())
    }

    #[allow(dead_code)]
    pub(crate) fn take_implicit_session(&mut self) -> Result<Option<ClientSession>> {
        Ok(self.inner_mut()?.take_implicit_session())
    }

    fn inner(&self) -> Result<&InnerCursor> {
        match &self.mode {
            CursorMode::Manual(inner) => Ok(inner),
            _ => Err(Error::internal(
                "streaming the cursor was cancelled while a request was in progress and must \
                 be continued before iterating manually",
            ))
        }
    }

    fn inner_mut(&mut self) -> Result<&mut InnerCursor> {
        match &mut self.mode {
            CursorMode::Manual(inner) => Ok(inner),
            _ => Err(Error::internal(
                "streaming the cursor was cancelled while a request was in progress and must \
                 be continued before iterating manually",
            ))
        }
    }
}

impl<T> Stream for Cursor<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = Pin::into_inner(self);
        loop {
            let mut out = None;
            take_mut::take(&mut this.mode, |mode| {
                match mode {
                    CursorMode::Manual(mut inner) => {
                        CursorMode::Streaming(Box::pin(async move {
                            let result = inner.advance().await;
                            (inner, result)
                        }))
                    }
                    CursorMode::Streaming(mut fut) => {
                        match fut.poll_unpin(cx) {
                            Poll::Pending => {
                                out = Some(Poll::Pending);
                                CursorMode::Streaming(fut)
                            }
                            Poll::Ready((inner, result)) => {
                                let r = match result {
                                    Ok(true) => Some(inner.deserialize_current()),
                                    Ok(false) => None,
                                    Err(e) => Some(Err(e)),
                                };
                                out = Some(Poll::Ready(r));
                                CursorMode::Manual(inner)
                            }
                        }
                    }
                }
            });
            if let Some(r) = out {
                return r;
            }
        }
    }
}

