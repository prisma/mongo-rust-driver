use std::{pin::Pin, task::{Context, Poll}};

use bson::RawDocument;
use futures_core::Stream;
use serde::{Deserialize, de::DeserializeOwned};

use crate::error::Result;

#[derive(Debug)]
pub struct Cursor<T> {
    _ph: T,
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
        todo!()
    }

    /// Returns a reference to the current result in the cursor.
    pub fn current(&self) -> &RawDocument {
        todo!()
    }

    /// Update the type streamed values will be parsed as.
    pub fn with_type<'a, D>(mut self) -> Cursor<D>
    where
        D: Deserialize<'a>,
    {
        todo!()
    }    
}

impl<T> Stream for Cursor<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}