use bson::{Document, doc, Bson};
use futures_util::StreamExt;

use crate::{
    test::{CommandEvent, FailPoint, FailPointMode, FailCommandOptions},
    event::command::{CommandSucceededEvent, CommandStartedEvent},
    change_stream::event::{ChangeStreamEvent, OperationType},
};

use super::{LOCK, EventClient};

/// Prose test 1: ChangeStream must continuously track the last seen resumeToken
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn tracks_resume_token() -> Result<(), Box<dyn std::error::Error>> {
    // TODO aegnor: restrict to replica sets and sharded clusters
    let _guard = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    let db = client.database("change_stream_tests");
    let coll = db.collection::<Document>("track_resume_token");
    let mut stream = coll.watch(None, None).await?;
    let mut tokens = vec![];
    tokens.push(stream.resume_token().unwrap().parsed()?);
    for _ in 0..3 {
        coll.insert_one(doc! {}, None).await?;
        stream.next().await.transpose()?;
        tokens.push(stream.resume_token().unwrap().parsed()?);
    }

    let expected_tokens: Vec<_> = client.get_command_events(&["aggregate", "getMore"])
        .into_iter()
        .filter_map(|ev| {
            match ev {
                CommandEvent::Succeeded(s) => Some(s),
                _ => None,
            }
        })
        .map(expected_token)
        .collect();
    assert_eq!(tokens, expected_tokens);

    Ok(())
}

fn expected_token(ev: CommandSucceededEvent) -> Bson {
    let cursor = ev.reply.get_document("cursor").unwrap();
    if let Some(token) = cursor.get("postBatchResumeToken") {
        token.clone()
    } else {
        cursor
            .get_array("nextBatch")
            .unwrap()[0]
            .as_document()
            .unwrap()
            .get("_id")
            .unwrap()
            .clone()
    }
}

/// Prose test 2: ChangeStream will throw an exception if the server response is missing the resume token
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn errors_on_missing_token() -> Result<(), Box<dyn std::error::Error>> {
    // TODO aegnor: restrict to replica sets and sharded clusters
    let _guard = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    let db = client.database("change_stream_tests");
    let coll = db.collection::<Document>("errors_on_missing_token");
    let mut stream = coll.watch(vec![
        doc! { "$project": { "_id": 0 } },
    ], None).await?;
    coll.insert_one(doc! {}, None).await?;
    assert!(stream.next().await.transpose().is_err());

    Ok(())
}

/// Prose test 3: After receiving a resumeToken, ChangeStream will automatically resume one time on a resumable error
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]  // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn resumes_on_error() -> Result<(), Box<dyn std::error::Error>> {
    // TODO aegnor: restrict to replica sets and sharded clusters
    let _guard = LOCK.run_exclusively().await;

    let client = EventClient::new().await;
    let db = client.database("change_stream_tests");
    let coll = db.collection::<Document>("resumes_on_error");
    coll.drop(None).await?;
    let mut stream = coll.watch(None, None).await?;

    coll.insert_one(doc! { "_id": 1 }, None).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 1 }
    ));

    let _guard = FailPoint::fail_command(
        &["getMore"],
        FailPointMode::Times(1),
        FailCommandOptions::builder()
            .error_code(43)
            .build(),
    ).enable(&client, None).await?;

    coll.insert_one(doc! { "_id": 2 }, None).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 2 }
    ));

    Ok(())
}

/// Prose test 4: ChangeStream will not attempt to resume on any error encountered while executing an aggregate command
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]  // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn does_not_resume_aggregate() -> Result<(), Box<dyn std::error::Error>> {
    // TODO aegnor: restrict to replica sets and sharded clusters
    let _guard = LOCK.run_exclusively().await;

    let client = EventClient::new().await;
    let db = client.database("change_stream_tests");
    let coll = db.collection::<Document>("does_not_resume_aggregate");
    coll.drop(None).await?;

    let _guard = FailPoint::fail_command(
        &["aggregate"],
        FailPointMode::Times(1),
        FailCommandOptions::builder()
            .error_code(43)
            .build(),
    ).enable(&client, None).await?;

    assert!(coll.watch(None, None).await.is_err());

    Ok(())
}

// Prose test 5: removed from spec.

// TODO aegnor:
// Prose test 6: ChangeStream will perform server selection before attempting to resume, using initial readPreference

/// Prose test 7: A cursor returned from an aggregate command with a cursor id and an initial empty batch is not closed on the driver side
#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn empty_batch_not_closed() -> Result<(), Box<dyn std::error::Error>> {
    // TODO aegnor: restrict to replica sets and sharded clusters
    let _guard = LOCK.run_concurrently().await;

    let client = EventClient::new().await;
    let db = client.database("change_stream_tests");
    let coll = db.collection::<Document>("empty_batch_not_closed");
    let mut stream = coll.watch(None, None).await?;

    assert!(stream.next_if_any().await?.is_none());

    coll.insert_one(doc! { }, None).await?;
    stream.next().await.transpose()?;

    let events = client.get_command_events(&["aggregate", "getMore"]);
    assert_eq!(events.len(), 4);
    let cursor_id = match &events[1] {
        CommandEvent::Succeeded(CommandSucceededEvent {
            reply, ..
        }) => reply.get_document("cursor")?.get_i64("id")?,
        _ => panic!("unexpected event {:#?}", events[1]),
    };
    let get_more_id = match &events[2] {
        CommandEvent::Started(CommandStartedEvent {
            command, ..
        }) => command.get_i64("getMore")?,
        _ => panic!("unexpected event {:#?}", events[2]),
    };
    assert_eq!(cursor_id, get_more_id);

    Ok(())
}

/// Prose test 8: The killCursors command sent during the "Resume Process" must not be allowed to throw an exception
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]  // multi_thread required for FailPoint
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn resume_kill_cursor_error_suppressed() -> Result<(), Box<dyn std::error::Error>> {
    // TODO aegnor: restrict to replica sets and sharded clusters
    let _guard = LOCK.run_exclusively().await;

    let client = EventClient::new().await;
    let db = client.database("change_stream_tests");
    let coll = db.collection::<Document>("resume_kill_cursor_error_suppressed");
    coll.drop(None).await?;
    let mut stream = coll.watch(None, None).await?;

    coll.insert_one(doc! { "_id": 1 }, None).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 1 }
    ));

    let _getmore_guard = FailPoint::fail_command(
        &["getMore"],
        FailPointMode::Times(1),
        FailCommandOptions::builder()
            .error_code(43)
            .build(),
    ).enable(&client, None).await?;

    let _killcursors_guard = FailPoint::fail_command(
        &["killCursors"],
        FailPointMode::Times(1),
        FailCommandOptions::builder()
            .error_code(43)
            .build(),
    ).enable(&client, None).await?;

    coll.insert_one(doc! { "_id": 2 }, None).await?;
    assert!(matches!(stream.next().await.transpose()?,
        Some(ChangeStreamEvent {
            operation_type: OperationType::Insert,
            document_key: Some(key),
            ..
        }) if key == doc! { "_id": 2 }
    ));

    Ok(())
}