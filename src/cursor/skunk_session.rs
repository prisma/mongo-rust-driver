#![allow(missing_docs)]

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use bson::{Document, RawDocument};
use futures_util::{FutureExt, StreamExt};
use futures_core::Stream;
use futures_core::future::BoxFuture;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::operation::GetMore;
use crate::{ClientSession, Client};
use crate::error::{Error, Result};

use super::CursorInformation;
use super::common::{CursorState, Advance};

pub struct SessionCursor<T> {
    inner: InnerSessionCursor,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> SessionCursor<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    pub fn stream<'session>(
        &mut self,
        session: &'session mut ClientSession,
    ) -> SessionCursorStream<'_, 'session, T> {
        todo!()
    }

    pub async fn next(&mut self, session: &mut ClientSession) -> Option<Result<T>> {
        self.stream(session).next().await
    }
}

impl<T> SessionCursor<T> {
    pub async fn advance(&mut self, session: &mut ClientSession) -> Result<bool> {
        todo!()
    }

    pub fn current(&self) -> &RawDocument {
        todo!()
    }

    pub fn deserialize_current<'a>(&'a self) -> Result<T>
    where
        T: Deserialize<'a>,
    {
        bson::from_slice(self.current().as_bytes()).map_err(Error::from)
    }

    pub fn with_type<'a, D>(mut self) -> SessionCursor<D>
    where
        D: Deserialize<'a>,
    {
        todo!()
    }
}

pub struct InnerSessionCursor {
    client: Client,
    info: CursorInformation,
    state: CursorState,
}

impl InnerSessionCursor {
    async fn advance(&mut self, mut session: Option<&mut ClientSession>) -> Result<bool> {
        loop {
            match self.state.advance() {
                Advance::HasValue => return Ok(true),
                Advance::Exhausted => return Ok(false),
                Advance::NeedGetMore => (),
            }

            let get_more = GetMore::new(self.info.clone(), self.state.pinned_connection.handle());
            let result = self.client.execute_operation(get_more, session.as_deref_mut()).await;
            self.state.handle_result(result)?;
        }
    }

    fn current(&self) -> &RawDocument {
        self.state.buffer().current().unwrap()
    }

    fn deserialize_current<'a, T>(&'a self) -> Result<T>
    where
        T: Deserialize<'a>,
    {
        bson::from_slice(self.current().as_bytes()).map_err(Error::from)
    }
}

pub struct SessionCursorStream<'cursor, 'session, T = Document> {
    cursor: &'cursor mut SessionCursor<T>,
    mode: SessionCursorStreamMode<'session>,
}

impl<'cursor, 'session, T> Stream for SessionCursorStream<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = Pin::into_inner(self);
        loop {
            let mut out = None;
            take_mut::take(&mut this.mode, |mode| {
                match mode {
                    SessionCursorStreamMode::Idle { mut inner, session } => {
                        SessionCursorStreamMode::InProgress(Box::pin(async move {
                            let result = inner.advance(Some(session)).await;
                            AdvanceResult { result, inner, session }
                        }))
                    }
                    SessionCursorStreamMode::InProgress(mut fut) => {
                        match fut.poll_unpin(cx) {
                            Poll::Pending => {
                                out = Some(Poll::Pending);
                                SessionCursorStreamMode::InProgress(fut)
                            }
                            Poll::Ready(AdvanceResult { result, inner, session }) => {
                                let r = match result {
                                    Ok(true) => Some(inner.deserialize_current()),
                                    Ok(false) => None,
                                    Err(e) => Some(Err(e)),
                                };
                                out = Some(Poll::Ready(r));
                                SessionCursorStreamMode::Idle { inner, session }
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

enum SessionCursorStreamMode<'session> {
    Idle {
        inner: InnerSessionCursor,
        session: &'session mut ClientSession,
    },
    InProgress(BoxFuture<'session, AdvanceResult<'session>>),
}

struct AdvanceResult<'session> {
    result: Result<bool>,
    inner: InnerSessionCursor,
    session: &'session mut ClientSession,
}