use bson::{RawDocument, RawDocumentBuf, Timestamp};

use crate::{
    bson,
    cmap::{conn::PinnedConnectionHandle, Command, RawCommandResponse, StreamDescription},
    error::{Error, Result},
    operation::Retryability,
    options::WriteConcern,
    selection_criteria::SelectionCriteria,
};

/// A trait modeling the behavior of a server side operation.
///
/// No methods in this trait should have default behaviors to ensure that wrapper operations
/// replicate all behavior.  Default behavior is provided by the `OperationDefault` trait.
pub(crate) trait Operation {
    /// The output type of this operation.
    type O;

    /// The name of the server side command associated with this operation.
    const NAME: &'static str;

    /// Returns the command that should be sent to the server as part of this operation.
    /// The operation may store some additional state that is required for handling the response.
    fn build(&mut self, description: &StreamDescription) -> Result<Command<RawDocumentBuf>>;

    /// Parse the response for the atClusterTime field.
    /// Depending on the operation, this may be found in different locations.
    fn extract_at_cluster_time(&self, _response: &RawDocument) -> Result<Option<Timestamp>>;

    /// Interprets the server response to the command.
    fn handle_response(
        &self,
        response: RawCommandResponse,
        description: &StreamDescription,
    ) -> Result<Self::O>;

    /// Interpret an error encountered while sending the built command to the server, potentially
    /// recovering.
    fn handle_error(&self, error: Error) -> Result<Self::O>;

    /// Criteria to use for selecting the server that this operation will be executed on.
    fn selection_criteria(&self) -> Option<&SelectionCriteria>;

    /// Whether or not this operation will request acknowledgment from the server.
    fn is_acknowledged(&self) -> bool;

    /// The write concern to use for this operation, if any.
    fn write_concern(&self) -> Option<&WriteConcern>;

    /// Returns whether or not this command supports the `readConcern` field.
    fn supports_read_concern(&self, _description: &StreamDescription) -> bool;

    /// Whether this operation supports sessions or not.
    fn supports_sessions(&self) -> bool;

    /// The level of retryability the operation supports.
    fn retryability(&self) -> crate::operation::Retryability;

    /// Updates this operation as needed for a retry.
    fn update_for_retry(&mut self);

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle>;

    fn name(&self) -> &str;
}

// A mirror of the `Operation` trait, with default behavior where appropriate.  Should only be
// implemented by operation types that do not delegate to other operations.
pub(crate) trait OperationWithDefaults {
    /// The output type of this operation.
    type O;

    /// The name of the server side command associated with this operation.
    const NAME: &'static str;

    /// Returns the command that should be sent to the server as part of this operation.
    /// The operation may store some additional state that is required for handling the response.
    fn build(&mut self, description: &StreamDescription) -> Result<Command<RawDocumentBuf>>;

    /// Parse the response for the atClusterTime field.
    /// Depending on the operation, this may be found in different locations.
    fn extract_at_cluster_time(&self, _response: &RawDocument) -> Result<Option<Timestamp>> {
        Ok(None)
    }

    /// Interprets the server response to the command.
    fn handle_response(
        &self,
        response: RawCommandResponse,
        description: &StreamDescription,
    ) -> Result<Self::O>;

    /// Interpret an error encountered while sending the built command to the server, potentially
    /// recovering.
    fn handle_error(&self, error: Error) -> Result<Self::O> {
        Err(error)
    }

    /// Criteria to use for selecting the server that this operation will be executed on.
    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        None
    }

    /// Whether or not this operation will request acknowledgment from the server.
    fn is_acknowledged(&self) -> bool {
        self.write_concern()
            .map(WriteConcern::is_acknowledged)
            .unwrap_or(true)
    }

    /// The write concern to use for this operation, if any.
    fn write_concern(&self) -> Option<&WriteConcern> {
        None
    }

    /// Returns whether or not this command supports the `readConcern` field.
    fn supports_read_concern(&self, _description: &StreamDescription) -> bool {
        false
    }

    /// Whether this operation supports sessions or not.
    fn supports_sessions(&self) -> bool {
        true
    }

    /// The level of retryability the operation supports.
    fn retryability(&self) -> Retryability {
        Retryability::None
    }

    /// Updates this operation as needed for a retry.
    fn update_for_retry(&mut self) {}

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        None
    }

    fn name(&self) -> &str {
        Self::NAME
    }
}

impl<T: OperationWithDefaults> Operation for T {
    type O = T::O;
    const NAME: &'static str = T::NAME;
    fn build(&mut self, description: &StreamDescription) -> Result<Command<RawDocumentBuf>> {
        self.build(description)
    }
    fn extract_at_cluster_time(&self, response: &RawDocument) -> Result<Option<Timestamp>> {
        self.extract_at_cluster_time(response)
    }
    fn handle_response(
        &self,
        response: RawCommandResponse,
        description: &StreamDescription,
    ) -> Result<Self::O> {
        self.handle_response(response, description)
    }
    fn handle_error(&self, error: Error) -> Result<Self::O> {
        self.handle_error(error)
    }
    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.selection_criteria()
    }
    fn is_acknowledged(&self) -> bool {
        self.is_acknowledged()
    }
    fn write_concern(&self) -> Option<&WriteConcern> {
        self.write_concern()
    }
    fn supports_read_concern(&self, description: &StreamDescription) -> bool {
        self.supports_read_concern(description)
    }
    fn supports_sessions(&self) -> bool {
        self.supports_sessions()
    }
    fn retryability(&self) -> Retryability {
        self.retryability()
    }
    fn update_for_retry(&mut self) {
        self.update_for_retry()
    }
    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        self.pinned_connection()
    }
    fn name(&self) -> &str {
        self.name()
    }
}
