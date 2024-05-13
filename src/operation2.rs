use bson::RawDocumentBuf;

use crate::{
    bson,
    cmap::{Command, StreamDescription},
    error::Result,
};

/// A trait modeling the behavior of a server side operation.
///
/// No methods in this trait should have default behaviors to ensure that wrapper operations
/// replicate all behavior.  Default behavior is provided by the `OperationDefault` trait.
pub(crate) trait Operation: crate::operation::Operation {
    /// Returns the command that should be sent to the server as part of this operation.
    /// The operation may store some additional state that is required for handling the response.
    fn build(&mut self, description: &StreamDescription) -> Result<Command<RawDocumentBuf>>;
}
