use std::{pin::Pin, task::{Context, Poll}, marker::PhantomData};

use bson::RawDocument;
use futures_core::Stream;
use serde::{Deserialize, de::DeserializeOwned};

use crate::{error::{Result, ErrorKind}, Client, operation::GetMore};

use super::{common::{CursorState, CursorBuffer}, CursorInformation, PinnedConnection};

/// Skunkworks cursor re-impl
#[derive(Debug)]
pub struct Cursor<T> {
    client: Client,
    info: CursorInformation,
    state: CursorState,
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
        loop {
            self.state.buffer.advance();
            if !self.state.buffer.is_empty() {
                break;
            }
            // if moving the offset puts us at the end of the buffer, perform another
            // getMore if the cursor is still alive.
            if self.state.exhausted {
                return Ok(false);
            }

            let client = self.client.clone();
            let spec = self.info.clone();
            let get_more = GetMore::new(spec, self.state.pinned_connection.handle());
            let result = client.execute_operation(get_more, None).await;
            match result {
                Ok(get_more) => {
                    if get_more.exhausted {
                        self.mark_exhausted();
                    }
                    self.state.buffer = CursorBuffer::new(get_more.batch);
                    self.state.post_batch_resume_token = get_more.post_batch_resume_token;
    
                    Ok(())
                }
                Err(e) => {
                    if matches!(*e.kind, ErrorKind::Command(ref e) if e.code == 43 || e.code == 237) {
                        self.mark_exhausted();
                    }
    
                    if e.is_network_error() {
                        // Flag the connection as invalid, preventing a killCursors command,
                        // but leave the connection pinned.
                        self.state.pinned_connection.invalidate();
                    }
    
                    Err(e)    
                }
            }?;
        }
        todo!()
    }

    fn mark_exhausted(&mut self) {
        self.state.exhausted = true;
        self.state.pinned_connection = PinnedConnection::Unpinned;
    }

    /// Returns a reference to the current result in the cursor.
    pub fn current(&self) -> &RawDocument {
        self.state.buffer.current().unwrap()
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