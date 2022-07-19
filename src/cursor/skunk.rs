use std::{pin::Pin, task::{Context, Poll}, marker::PhantomData};

use bson::RawDocument;
use futures_util::FutureExt;
use futures_core::future::BoxFuture;
use futures_core::Stream;
use serde::{Deserialize, de::DeserializeOwned};

use crate::{error::{Result, Error}, Client, operation::GetMore};

use super::{common::{CursorState, Advance}, CursorInformation};

/// Skunkworks cursor re-impl
pub struct InnerCursor {
    client: Client,
    info: CursorInformation,
    state: CursorState,
}

impl InnerCursor {
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

    /// Returns a reference to the current result in the cursor.
    fn current(&self) -> &RawDocument {
        self.state.buffer.current().unwrap()
    }

    fn deserialize_current<'a, T>(&'a self) -> Result<T>
    where
        T: Deserialize<'a>,
    {
        bson::from_slice(self.current().as_bytes()).map_err(Error::from)
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
    fn inner(&self) -> Result<&InnerCursor> {
        match &self.mode {
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

