#![allow(missing_docs)]

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use bson::{Document, RawDocument};
use futures_util::StreamExt;
use futures_core::Stream;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::ClientSession;
use crate::error::{Error, Result};

pub struct SessionCursor<T> {
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

pub struct SessionCursorStream<'cursor, 'session, T = Document> {
    cursor: &'cursor mut SessionCursor<T>,
    session: &'session mut ClientSession,
}

impl<'cursor, 'session, T> Stream for SessionCursorStream<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
