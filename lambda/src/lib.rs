pub mod backoff;
pub mod config;
pub mod environment;
pub mod replay;

use std::collections::HashMap;

use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{Error, LambdaEvent};
use tracing::{info, warn};

use crate::config::{ConfigError, ConfigStore, ResolvedQueueConfig};
use crate::environment::Environment;
use crate::replay::{build_replay_request, send_replay_batch, ReplayRequest};

/// Response shape consumed by the SQS event source mapping's
/// `ReportBatchItemFailures` functionality.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchResponse {
    pub batch_item_failures: Vec<BatchItemFailure>,
}

/// A single message that should be returned to the queue for retry.
#[derive(Debug, serde::Serialize)]
pub struct BatchItemFailure {
    pub item_identifier: String,
}

/// Replays each SQS record back to its configured queue, reporting batch item failures.
///
/// A message is reported as a failure when it has exhausted its replay
/// attempts, has no configuration yet, is malformed, or could not be sent.
///
/// # Errors
///
/// Returns an error only when a replay queue's configuration cannot be resolved
/// (e.g. SSM is unavailable); a single malformed record is reported as a batch
/// item failure instead of failing the whole batch.
pub async fn handle(
    env: &Environment,
    sqs: &aws_sdk_sqs::Client,
    config_store: &tokio::sync::Mutex<ConfigStore>,
    event: LambdaEvent<SqsEvent>,
) -> Result<BatchResponse, Error> {
    // Resolve each unique replay ARN's config exactly once, then release the
    // lock so the batch sends below do not serialize on config access.
    let mut config_store = config_store.lock().await;
    let mut resolved: HashMap<String, Option<ResolvedQueueConfig>> = HashMap::new();
    for record in &event.payload.records {
        if let Some(arn) = record.event_source_arn.as_deref() {
            if !resolved.contains_key(arn) {
                let config = match resolve_config(env, &mut config_store, Some(arn)).await {
                    Ok(config) => config,
                    Err(error) => return Err(Error::from(error)),
                };
                resolved.insert(arn.to_string(), config);
            }
        }
    }
    drop(config_store);

    // Build the replay requests up front; any record without a request is a
    // batch item failure (no config, retry maximum reached, or malformed), so
    // only that message is redelivered rather than the whole batch.
    let mut failures = Vec::new();
    let mut sends: Vec<(String, ReplayRequest)> = Vec::new();
    for record in &event.payload.records {
        let config = match record.event_source_arn.as_deref() {
            None => {
                warn!(
                    message_id = record.message_id.as_deref().unwrap_or("unknown"),
                    "Record has no eventSourceARN."
                );
                push_failure(&mut failures, record.message_id.as_ref());
                continue;
            }
            Some(arn) => match resolved.get(arn) {
                Some(Some(config)) => config,
                Some(None) => {
                    warn!(
                        message_id = record.message_id.as_deref().unwrap_or("unknown"),
                        "No replay configuration found for message."
                    );
                    push_failure(&mut failures, record.message_id.as_ref());
                    continue;
                }
                None => unreachable!("every replay ARN was resolved in the first pass"),
            },
        };

        match build_replay_request(config, record) {
            Ok(Some(request)) => {
                let Some(message_id) = record.message_id.clone() else {
                    warn!("Skipping record without a message id; it cannot be reported as a batch item failure.");
                    continue;
                };
                sends.push((message_id, request));
            }
            Ok(None) => {
                info!(
                    message_id = record.message_id.as_deref().unwrap_or("unknown"),
                    "Retry maximum reached."
                );
                push_failure(&mut failures, record.message_id.as_ref());
            }
            Err(error) => {
                warn!(
                    message_id = record.message_id.as_deref().unwrap_or("unknown"),
                    %error,
                    "Rejecting malformed message."
                );
                push_failure(&mut failures, record.message_id.as_ref());
            }
        }
    }

    // Send the replay requests; any message SQS rejected is reported as a
    // batch item failure so only it is redelivered, not the whole batch.
    for message_id in send_replay_batch(sqs, sends).await {
        failures.push(BatchItemFailure {
            item_identifier: message_id,
        });
    }

    Ok(BatchResponse {
        batch_item_failures: failures,
    })
}

/// Resolves the replay destination for a record from the SSM config store,
/// keyed by the replay queue ARN.
///
/// # Errors
///
/// Returns [`ConfigError::Missing`] when no `eventSourceARN` is given and
/// [`ConfigError::Load`] when the SSM parameters cannot be read.
pub async fn resolve_config(
    env: &Environment,
    config_store: &mut ConfigStore,
    event_source_arn: Option<&str>,
) -> Result<Option<ResolvedQueueConfig>, ConfigError> {
    let arn = event_source_arn.ok_or(ConfigError::Missing("eventSourceARN"))?;
    config_store
        .config_for(arn)
        .await?
        .map_or_else(|| Ok(None), |config| Ok(Some(config.resolve(env))))
}

pub fn push_failure(failures: &mut Vec<BatchItemFailure>, message_id: Option<&String>) {
    if let Some(message_id) = message_id {
        failures.push(BatchItemFailure {
            item_identifier: message_id.clone(),
        });
    }
}
