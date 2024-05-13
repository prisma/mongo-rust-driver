use bson::{doc, RawBsonRef, RawDocument};

use std::{sync::atomic::Ordering, time::Instant};

use super::{options::ServerAddress, session::TransactionState, Client, ClientSession};
use crate::{
    bson::Document,
    cmap::{
        conn::wire::{next_request_id, Message},
        Connection,
        ConnectionPool,
        RawCommandResponse,
    },
    cursor::{session::SessionCursor, Cursor, CursorSpecification},
    error::{Error, ErrorKind, Result, RETRYABLE_WRITE_ERROR},
    event::command::{
        CommandEvent,
        CommandFailedEvent,
        CommandStartedEvent,
        CommandSucceededEvent,
    },
    operation::{
        AbortTransaction,
        CommandErrorBody,
        CommitTransaction,
        Operation as OriginalFlavor,
        Retryability,
    },
    operation2::Operation,
    options::SelectionCriteria,
    sdam::{HandshakePhase, ServerType},
    selection_criteria::ReadPreference,
    tracking_arc::TrackingArc,
    ClusterTime,
};

impl Client {
    /// Execute the given operation.
    ///
    /// Server selection will performed using the criteria specified on the operation, if any, and
    /// an implicit session will be created if the operation and write concern are compatible with
    /// sessions and an explicit session is not provided.
    pub(crate) async fn execute_operation_2<T: Operation>(
        &self,
        op: T,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<T::O> {
        self.execute_operation_with_details_2(op, session)
            .await
            .map(|details| details.output)
    }

    async fn execute_operation_with_details_2<T: Operation>(
        &self,
        op: T,
        session: impl Into<Option<&mut ClientSession>>,
    ) -> Result<ExecutionDetails<T>> {
        if self.inner.shutdown.executed.load(Ordering::SeqCst) {
            return Err(ErrorKind::Shutdown.into());
        }
        Box::pin(async {
            // TODO RUST-9: allow unacknowledged write concerns
            if !op.is_acknowledged() {
                return Err(ErrorKind::InvalidArgument {
                    message: "Unacknowledged write concerns are not supported".to_string(),
                }
                .into());
            }
            let session = session.into();
            if let Some(session) = &session {
                if !TrackingArc::ptr_eq(&self.inner, &session.client().inner) {
                    return Err(ErrorKind::InvalidArgument {
                        message: "the session provided to an operation must be created from the \
                                  same client as the collection/database"
                            .into(),
                    }
                    .into());
                }

                if let Some(SelectionCriteria::ReadPreference(read_preference)) =
                    op.selection_criteria()
                {
                    if session.in_transaction() && read_preference != &ReadPreference::Primary {
                        return Err(ErrorKind::Transaction {
                            message: "read preference in a transaction must be primary".into(),
                        }
                        .into());
                    }
                }
            }
            self.execute_operation_with_retry_2(op, session).await
        })
        .await
    }

    /// Execute the given operation, returning the cursor created by the operation.
    ///
    /// Server selection be will performed using the criteria specified on the operation, if any.
    pub(crate) async fn execute_cursor_operation_2<Op, T>(&self, op: Op) -> Result<Cursor<T>>
    where
        Op: Operation<O = CursorSpecification>,
    {
        Box::pin(async {
            let mut details = self.execute_operation_with_details_2(op, None).await?;
            let pinned =
                self.pin_connection_for_cursor(&details.output, &mut details.connection)?;
            Ok(Cursor::new(
                self.clone(),
                details.output,
                details.implicit_session,
                pinned,
            ))
        })
        .await
    }

    pub(crate) async fn execute_session_cursor_operation_2<Op, T>(
        &self,
        op: Op,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<T>>
    where
        Op: Operation<O = CursorSpecification>,
    {
        let mut details = self
            .execute_operation_with_details_2(op, &mut *session)
            .await?;

        let pinned =
            self.pin_connection_for_session(&details.output, &mut details.connection, session)?;
        Ok(SessionCursor::new(self.clone(), details.output, pinned))
    }

    /// Selects a server and executes the given operation on it, optionally using a provided
    /// session. Retries the operation upon failure if retryability is supported or after
    /// reauthenticating if reauthentication is required.
    async fn execute_operation_with_retry_2<T: Operation>(
        &self,
        mut op: T,
        mut session: Option<&mut ClientSession>,
    ) -> Result<ExecutionDetails<T>> {
        // If the current transaction has been committed/aborted and it is not being
        // re-committed/re-aborted, reset the transaction's state to TransactionState::None.
        if let Some(ref mut session) = session {
            if matches!(
                session.transaction.state,
                TransactionState::Committed { .. }
            ) && op.name() != CommitTransaction::NAME
                || session.transaction.state == TransactionState::Aborted
                    && op.name() != AbortTransaction::NAME
            {
                session.transaction.reset();
            }
        }

        let mut retry: Option<ExecutionRetry> = None;
        let mut implicit_session: Option<ClientSession> = None;
        loop {
            if retry.is_some() {
                op.update_for_retry();
            }

            let selection_criteria = session
                .as_ref()
                .and_then(|s| s.transaction.pinned_mongos())
                .or_else(|| op.selection_criteria());

            let server = match self
                .select_server(
                    selection_criteria,
                    op.name(),
                    retry.as_ref().map(|r| &r.first_server),
                )
                .await
            {
                Ok(server) => server,
                Err(mut err) => {
                    retry.first_error()?;

                    err.add_labels_and_update_pin(None, &mut session, None)?;
                    return Err(err);
                }
            };
            let server_addr = server.address.clone();

            let mut conn = match get_connection(&session, &op, &server.pool).await {
                Ok(c) => c,
                Err(mut err) => {
                    retry.first_error()?;

                    err.add_labels_and_update_pin(None, &mut session, None)?;
                    if err.is_read_retryable() && self.inner.options.retry_writes != Some(false) {
                        err.add_label(RETRYABLE_WRITE_ERROR);
                    }

                    let op_retry = match self.get_op_retryability_2(&op, &session) {
                        Retryability::Read => err.is_read_retryable(),
                        Retryability::Write => err.is_write_retryable(),
                        _ => false,
                    };
                    if err.is_pool_cleared() || op_retry {
                        retry = Some(ExecutionRetry {
                            prior_txn_number: None,
                            first_error: err,
                            first_server: server_addr.clone(),
                        });
                        continue;
                    } else {
                        return Err(err);
                    }
                }
            };

            if !conn.supports_sessions() && session.is_some() {
                return Err(ErrorKind::SessionsNotSupported.into());
            }

            if conn.supports_sessions()
                && session.is_none()
                && op.supports_sessions()
                && op.is_acknowledged()
            {
                implicit_session = Some(ClientSession::new(self.clone(), None, true).await);
                session = implicit_session.as_mut();
            }

            let retryability = self.get_retryability_2(&conn, &op, &session)?;
            if retryability == Retryability::None {
                retry.first_error()?;
            }

            let txn_number = retry
                .as_ref()
                .and_then(|r| r.prior_txn_number)
                .or_else(|| get_txn_number(&mut session, retryability));

            let details = match self
                .execute_operation_on_connection_2(
                    &mut op,
                    &mut conn,
                    &mut session,
                    txn_number,
                    retryability,
                )
                .await
            {
                Ok(output) => ExecutionDetails {
                    output,
                    connection: conn,
                    implicit_session,
                },
                Err(mut err) => {
                    // If the error is a reauthentication required error, we reauthenticate and
                    // retry the operation.
                    if err.is_reauthentication_required() {
                        let credential = self.inner.options.credential.as_ref().ok_or(
                            ErrorKind::Authentication {
                                message: "No Credential when reauthentication required error \
                                          occured"
                                    .to_string(),
                            },
                        )?;
                        let server_api = self.inner.options.server_api.as_ref();

                        credential
                            .mechanism
                            .as_ref()
                            .ok_or(ErrorKind::Authentication {
                                message: "No AuthMechanism when reauthentication required error \
                                          occured"
                                    .to_string(),
                            })?
                            .reauthenticate_stream(&mut conn, credential, server_api)
                            .await?;
                        continue;
                    }
                    err.wire_version = conn.stream_description()?.max_wire_version;

                    // Retryable writes are only supported by storage engines with document-level
                    // locking, so users need to disable retryable writes if using mmapv1.
                    if let ErrorKind::Command(ref mut command_error) = *err.kind {
                        if command_error.code == 20
                            && command_error.message.starts_with("Transaction numbers")
                        {
                            command_error.message = "This MongoDB deployment does not support \
                                                     retryable writes. Please add \
                                                     retryWrites=false to your connection string."
                                .to_string();
                        }
                    }

                    self.inner
                        .topology
                        .handle_application_error(
                            server_addr.clone(),
                            err.clone(),
                            HandshakePhase::after_completion(&conn),
                        )
                        .await;
                    // release the connection to be processed by the connection pool
                    drop(conn);
                    // release the selected server to decrement its operation count
                    drop(server);

                    if let Some(r) = retry {
                        if (err.is_server_error()
                            || err.is_read_retryable()
                            || err.is_write_retryable())
                            && !err.contains_label("NoWritesPerformed")
                        {
                            return Err(err);
                        } else {
                            return Err(r.first_error);
                        }
                    } else if retryability == Retryability::Read && err.is_read_retryable()
                        || retryability == Retryability::Write && err.is_write_retryable()
                    {
                        retry = Some(ExecutionRetry {
                            prior_txn_number: txn_number,
                            first_error: err,
                            first_server: server_addr.clone(),
                        });
                        continue;
                    } else {
                        return Err(err);
                    }
                }
            };
            return Ok(details);
        }
    }

    /// Executes an operation on a given connection, optionally using a provided session.
    async fn execute_operation_on_connection_2<T: Operation>(
        &self,
        op: &mut T,
        connection: &mut Connection,
        session: &mut Option<&mut ClientSession>,
        txn_number: Option<i64>,
        retryability: Retryability,
    ) -> Result<T::O> {
        if let Some(wc) = op.write_concern() {
            wc.validate()?;
        }

        let stream_description = connection.stream_description()?;
        let is_sharded = stream_description.initial_server_type == ServerType::Mongos;
        let mut cmd = Operation::build(op, stream_description)?;
        self.inner.topology.update_command_with_read_pref(
            connection.address(),
            &mut cmd,
            op.selection_criteria(),
        );

        match session {
            Some(ref mut session) if op.supports_sessions() && op.is_acknowledged() => {
                cmd.set_session(session);
                if let Some(txn_number) = txn_number {
                    cmd.set_txn_number(txn_number);
                }
                if session
                    .options()
                    .and_then(|opts| opts.snapshot)
                    .unwrap_or(false)
                {
                    if connection
                        .stream_description()?
                        .max_wire_version
                        .unwrap_or(0)
                        < 13
                    {
                        let labels: Option<Vec<_>> = None;
                        return Err(Error::new(
                            ErrorKind::IncompatibleServer {
                                message: "Snapshot reads require MongoDB 5.0 or later".into(),
                            },
                            labels,
                        ));
                    }
                    cmd.set_snapshot_read_concern(session);
                }
                // If this is a causally consistent session, set `readConcern.afterClusterTime`.
                // Causal consistency defaults to true, unless snapshot is true.
                else if session.causal_consistency()
                    && matches!(
                        session.transaction.state,
                        TransactionState::None | TransactionState::Starting
                    )
                    && op.supports_read_concern(stream_description)
                {
                    cmd.set_after_cluster_time(session);
                }

                match session.transaction.state {
                    TransactionState::Starting => {
                        cmd.set_start_transaction();
                        cmd.set_autocommit();
                        if session.causal_consistency() {
                            cmd.set_after_cluster_time(session);
                        }

                        if let Some(ref options) = session.transaction.options {
                            if let Some(ref read_concern) = options.read_concern {
                                cmd.set_read_concern_level(read_concern.level.clone());
                            }
                        }
                        if self.is_load_balanced() {
                            session.pin_connection(connection.pin()?);
                        } else if is_sharded {
                            session.pin_mongos(connection.address().clone());
                        }
                        session.transaction.state = TransactionState::InProgress;
                    }
                    TransactionState::InProgress => cmd.set_autocommit(),
                    TransactionState::Committed { .. } | TransactionState::Aborted => {
                        cmd.set_autocommit();

                        // Append the recovery token to the command if we are committing or aborting
                        // on a sharded transaction.
                        if is_sharded {
                            if let Some(ref recovery_token) = session.transaction.recovery_token {
                                cmd.set_recovery_token(recovery_token);
                            }
                        }
                    }
                    _ => {}
                }
                session.update_last_use();
            }
            Some(ref session) if !op.supports_sessions() && !session.is_implicit() => {
                return Err(ErrorKind::InvalidArgument {
                    message: format!("{} does not support sessions", cmd.name),
                }
                .into());
            }
            Some(ref session) if !op.is_acknowledged() && !session.is_implicit() => {
                return Err(ErrorKind::InvalidArgument {
                    message: "Cannot use ClientSessions with unacknowledged write concern"
                        .to_string(),
                }
                .into());
            }
            _ => {}
        }

        let session_cluster_time = session.as_ref().and_then(|session| session.cluster_time());
        let client_cluster_time = self.inner.topology.cluster_time();
        let max_cluster_time = std::cmp::max(session_cluster_time, client_cluster_time.as_ref());
        if let Some(cluster_time) = max_cluster_time {
            cmd.set_cluster_time(cluster_time);
        }

        let connection_info = connection.info();
        let service_id = connection.service_id();
        let request_id = next_request_id();

        if let Some(ref server_api) = self.inner.options.server_api {
            cmd.set_server_api(server_api);
        }

        let should_redact = cmd.should_redact();
        let should_compress = cmd.should_compress();

        let cmd_name = cmd.name.clone();
        let target_db = cmd.target_db.clone();

        #[allow(unused_mut)]
        let mut message = Message::from_command(cmd, Some(request_id))?;
        #[cfg(feature = "in-use-encryption-unstable")]
        {
            let guard = self.inner.csfle.read().await;
            if let Some(ref csfle) = *guard {
                if csfle.opts().bypass_auto_encryption != Some(true) {
                    let encrypted_payload = self
                        .auto_encrypt(csfle, &message.document_payload, &target_db)
                        .await?;
                    message.document_payload = encrypted_payload;
                }
            }
        }

        self.emit_command_event(|| {
            let command_body = if should_redact {
                Document::new()
            } else {
                message.get_command_document()
            };
            CommandEvent::Started(CommandStartedEvent {
                command: command_body,
                db: target_db.clone(),
                command_name: cmd_name.clone(),
                request_id,
                connection: connection_info.clone(),
                service_id,
            })
        })
        .await;

        let start_time = Instant::now();
        let command_result = match connection.send_message(message, should_compress).await {
            Ok(response) => {
                async fn handle_response<T: Operation>(
                    client: &Client,
                    op: &T,
                    session: &mut Option<&mut ClientSession>,
                    is_sharded: bool,
                    response: RawCommandResponse,
                ) -> Result<RawCommandResponse> {
                    let raw_doc = RawDocument::from_bytes(response.as_bytes())?;

                    let ok = match raw_doc.get("ok")? {
                        Some(b) => crate::bson_util::get_int_raw(b).ok_or_else(|| {
                            ErrorKind::InvalidResponse {
                                message: format!(
                                    "expected ok value to be a number, instead got {:?}",
                                    b
                                ),
                            }
                        })?,
                        None => {
                            return Err(ErrorKind::InvalidResponse {
                                message: "missing 'ok' value in response".to_string(),
                            }
                            .into())
                        }
                    };

                    let cluster_time: Option<ClusterTime> = raw_doc
                        .get("$clusterTime")?
                        .and_then(RawBsonRef::as_document)
                        .map(|d| bson::from_slice(d.as_bytes()))
                        .transpose()?;

                    let at_cluster_time = op.extract_at_cluster_time(raw_doc)?;

                    client
                        .update_cluster_time(cluster_time, at_cluster_time, session)
                        .await;

                    if let (Some(session), Some(ts)) = (
                        session.as_mut(),
                        raw_doc
                            .get("operationTime")?
                            .and_then(RawBsonRef::as_timestamp),
                    ) {
                        session.advance_operation_time(ts);
                    }

                    if ok == 1 {
                        if let Some(ref mut session) = session {
                            if is_sharded && session.in_transaction() {
                                let recovery_token = raw_doc
                                    .get("recoveryToken")?
                                    .and_then(RawBsonRef::as_document)
                                    .map(|d| bson::from_slice(d.as_bytes()))
                                    .transpose()?;
                                session.transaction.recovery_token = recovery_token;
                            }
                        }

                        Ok(response)
                    } else {
                        Err(response
                            .body::<CommandErrorBody>()
                            .map(|error_response| error_response.into())
                            .unwrap_or_else(|e| {
                                Error::from(ErrorKind::InvalidResponse {
                                    message: format!("error deserializing command error: {}", e),
                                })
                            }))
                    }
                }
                handle_response(self, op, session, is_sharded, response).await
            }
            Err(err) => Err(err),
        };

        let duration = start_time.elapsed();

        match command_result {
            Err(mut err) => {
                self.emit_command_event(|| {
                    let mut err = err.clone();
                    if should_redact {
                        err.redact();
                    }

                    CommandEvent::Failed(CommandFailedEvent {
                        duration,
                        command_name: cmd_name.clone(),
                        failure: err,
                        request_id,
                        connection: connection_info.clone(),
                        service_id,
                    })
                })
                .await;

                if let Some(ref mut session) = session {
                    if err.is_network_error() {
                        session.mark_dirty();
                    }
                }

                err.add_labels_and_update_pin(Some(connection), session, Some(retryability))?;
                op.handle_error(err)
            }
            Ok(response) => {
                self.emit_command_event(|| {
                    let reply = if should_redact {
                        Document::new()
                    } else {
                        response
                            .body()
                            .unwrap_or_else(|e| doc! { "deserialization error": e.to_string() })
                    };

                    CommandEvent::Succeeded(CommandSucceededEvent {
                        duration,
                        reply,
                        command_name: cmd_name.clone(),
                        request_id,
                        connection: connection_info.clone(),
                        service_id,
                    })
                })
                .await;

                #[cfg(feature = "in-use-encryption-unstable")]
                let response = {
                    let guard = self.inner.csfle.read().await;
                    if let Some(ref csfle) = *guard {
                        let new_body = self.auto_decrypt(csfle, response.raw_body()).await?;
                        RawCommandResponse::new_raw(response.source, new_body)
                    } else {
                        response
                    }
                };

                match op.handle_response(response, connection.stream_description()?) {
                    Ok(response) => Ok(response),
                    Err(mut err) => {
                        err.add_labels_and_update_pin(
                            Some(connection),
                            session,
                            Some(retryability),
                        )?;
                        Err(err)
                    }
                }
            }
        }
    }

    /// Returns the retryability level for the execution of this operation.
    fn get_op_retryability_2<T: Operation>(
        &self,
        op: &T,
        session: &Option<&mut ClientSession>,
    ) -> Retryability {
        if session
            .as_ref()
            .map(|session| session.in_transaction())
            .unwrap_or(false)
        {
            return Retryability::None;
        }
        match op.retryability() {
            Retryability::Read if self.inner.options.retry_reads != Some(false) => {
                Retryability::Read
            }
            // commitTransaction and abortTransaction should be retried regardless of the
            // value for retry_writes set on the Client
            Retryability::Write
                if op.name() == CommitTransaction::NAME
                    || op.name() == AbortTransaction::NAME
                    || self.inner.options.retry_writes != Some(false) =>
            {
                Retryability::Write
            }
            _ => Retryability::None,
        }
    }

    /// Returns the retryability level for the execution of this operation on this connection.
    fn get_retryability_2<T: Operation>(
        &self,
        conn: &Connection,
        op: &T,
        session: &Option<&mut ClientSession>,
    ) -> Result<Retryability> {
        match self.get_op_retryability_2(op, session) {
            Retryability::Read => Ok(Retryability::Read),
            Retryability::Write if conn.stream_description()?.supports_retryable_writes() => {
                Ok(Retryability::Write)
            }
            _ => Ok(Retryability::None),
        }
    }
}

async fn get_connection<T: Operation>(
    session: &Option<&mut ClientSession>,
    op: &T,
    pool: &ConnectionPool,
) -> Result<Connection> {
    let session_pinned = session
        .as_ref()
        .and_then(|s| s.transaction.pinned_connection());
    match (session_pinned, op.pinned_connection()) {
        (Some(c), None) | (None, Some(c)) => c.take_connection().await,
        (Some(session_handle), Some(op_handle)) => {
            // An operation executing in a transaction should be sharing the same pinned connection.
            debug_assert_eq!(session_handle.id(), op_handle.id());
            session_handle.take_connection().await
        }
        (None, None) => pool.check_out().await,
    }
}

fn get_txn_number(
    session: &mut Option<&mut ClientSession>,
    retryability: Retryability,
) -> Option<i64> {
    match session {
        Some(ref mut session) => {
            if session.transaction.state != TransactionState::None {
                Some(session.txn_number())
            } else {
                match retryability {
                    Retryability::Write => Some(session.get_and_increment_txn_number()),
                    _ => None,
                }
            }
        }
        None => None,
    }
}

struct ExecutionDetails<T: Operation> {
    output: T::O,
    connection: Connection,
    implicit_session: Option<ClientSession>,
}

struct ExecutionRetry {
    prior_txn_number: Option<i64>,
    first_error: Error,
    first_server: ServerAddress,
}

trait RetryHelper {
    fn first_error(&mut self) -> Result<()>;
}

impl RetryHelper for Option<ExecutionRetry> {
    fn first_error(&mut self) -> Result<()> {
        match self.take() {
            Some(r) => Err(r.first_error),
            None => Ok(()),
        }
    }
}
