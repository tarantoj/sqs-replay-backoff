pub mod backoff;
pub mod config;
pub mod environment;
pub mod replay;

use std::collections::{HashMap, HashSet};

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

/// Replays each SQS record back to its configured queue, reporting a batch
/// item failure for any message that has exhausted its replay attempts, has no
/// configuration yet, or could not be sent.
///
/// # Errors
///
/// Returns an error when a record is missing its `eventSourceARN` or the
/// configuration for a replay queue cannot be resolved.
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
    // failure (no config or retry maximum reached).
    let mut failures = Vec::new();
    let mut sends: Vec<(String, ReplayRequest)> = Vec::new();
    let mut reportable = HashSet::new();
    for (index, record) in event.payload.records.iter().enumerate() {
        let config = match record.event_source_arn.as_deref() {
            None => return Err(Error::from(ConfigError::Missing("eventSourceARN"))),
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
                let message_id = record
                    .message_id
                    .clone()
                    .unwrap_or_else(|| format!("entry-{index}"));
                if record.message_id.is_some() {
                    reportable.insert(message_id.clone());
                }
                sends.push((message_id, request));
            }
            Ok(None) => {
                info!(
                    message_id = record.message_id.as_deref().unwrap_or("unknown"),
                    "Retry maximum reached."
                );
                push_failure(&mut failures, record.message_id.as_ref());
            }
            Err(error) => return Err(Error::from(error)),
        }
    }

    // Send the replay requests; any message SQS rejected is reported as a
    // batch item failure so only it is redelivered, not the whole batch.
    for message_id in send_replay_batch(sqs, sends).await {
        if reportable.contains(&message_id) {
            failures.push(BatchItemFailure {
                item_identifier: message_id,
            });
        }
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
