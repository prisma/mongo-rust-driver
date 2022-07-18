use std::{pin::Pin, task::{Context, Poll}, marker::PhantomData};

use bson::RawDocument;
use futures_util::FutureExt;
use futures_core::future::BoxFuture;
use futures_core::Stream;
use serde::{Deserialize, de::DeserializeOwned};

use crate::{error::{Result, Error}, Client, operation::GetMore, results::GetMoreResult};

use super::{common::{CursorState, Advance}, CursorInformation};

/// Skunkworks cursor re-impl
pub struct Cursor<T> {
    client: Client,
    info: CursorInformation,
    state: CursorState,
    pending_get_more: Option<BoxFuture<'static, Result<GetMoreResult>>>,
    _phantom: PhantomData<T>,
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
        if self.pending_get_more.is_some() {
            panic!("no u");
        }
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

    /// Returns a reference to the current result in the cursor.
    pub fn current(&self) -> &RawDocument {
        self.state.buffer.current().unwrap()
    }

    /// Deserialize the current result to the generic type associated with this cursor.
    pub fn deserialize_current<'a>(&'a self) -> Result<T>
    where
        T: Deserialize<'a>,
    {
        bson::from_slice(self.current().as_bytes()).map_err(Error::from)
    }

    /// Update the type streamed values will be parsed as.
    pub fn with_type<'a, D>(self) -> Cursor<D>
    where
        D: Deserialize<'a>,
    {
        Cursor {
            client: self.client,
            info: self.info,
            state: self.state,
            pending_get_more: self.pending_get_more,
            _phantom: Default::default(),
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
            match this.pending_get_more.take() {
                None => {
                    match this.state.advance() {
                        Advance::HasValue => return Poll::Ready(Some(this.deserialize_current())),
                        Advance::Exhausted => return Poll::Ready(None),
                        Advance::NeedGetMore => {
                            let client = this.client.clone();
                            let info = this.info.clone();
                            let pinned_conn = this.state.pinned_connection.replicate();
                            this.pending_get_more = Some(Box::pin(async move {
                                let get_more = GetMore::new(info, pinned_conn.handle());
                                client.execute_operation(get_more, None).await
                            }));
                            continue;
                        }
                    }
                }
                Some(mut f) => {
                    match f.poll_unpin(cx) {
                        Poll::Pending => {
                            this.pending_get_more = Some(f);
                            return Poll::Pending;
                        }
                        Poll::Ready(result) => {
                            if let Err(e) = this.state.handle_result(result) {
                                return Poll::Ready(Some(Err(e)));
                            }
                            continue;
                        },
                    }
                }
            }
        }
    }
}