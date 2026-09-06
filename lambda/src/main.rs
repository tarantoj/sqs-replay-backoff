mod backoff;
mod config;
mod environment;
mod replay;

use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use tracing::{info, warn};

use crate::config::{ConfigError, ConfigStore, ResolvedQueueConfig};
use crate::environment::Environment;
use crate::replay::{build_replay_request, send_replay};

/// Response shape consumed by the SQS event source mapping's
/// `ReportBatchItemFailures` functionality.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct BatchResponse {
    batch_item_failures: Vec<BatchItemFailure>,
}

/// A single message that should be returned to the queue for retry.
#[derive(serde::Serialize)]
struct BatchItemFailure {
    item_identifier: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda_runtime::tracing::init_default_subscriber();

    let env = Environment::from_env()?;
    info!(
        queue_url = env.queue_url.as_deref().unwrap_or("(config store)"),
        config_path = %env.config_path,
        max_attempts = env.max_attempts,
        backoff_rate_seconds = env.backoff_rate_seconds,
        maximum_delay_seconds = env.maximum_delay_seconds,
        use_jitter = env.use_jitter,
        "Configuration"
    );

    let shared_config = aws_config::load_from_env().await;
    let sqs = aws_sdk_sqs::Client::new(&shared_config);
    let ssm = aws_sdk_ssm::Client::new(&shared_config);
    let config_store = tokio::sync::Mutex::new(ConfigStore::new(ssm, env.config_path.clone()));

    run(service_fn(|event: LambdaEvent<SqsEvent>| {
        handle(&env, &sqs, &config_store, event)
    }))
    .await
}

/// Replays each SQS record back to its configured queue, reporting a batch
/// item failure for any message that has exhausted its replay attempts or has
/// no configuration yet.
async fn handle(
    env: &Environment,
    sqs: &aws_sdk_sqs::Client,
    config_store: &tokio::sync::Mutex<ConfigStore>,
    event: LambdaEvent<SqsEvent>,
) -> Result<BatchResponse, Error> {
    let mut config_store = config_store.lock().await;
    let mut failures = Vec::new();
    for record in &event.payload.records {
        let config = match resolve_config(
            env,
            &mut config_store,
            record.event_source_arn.as_deref(),
        )
        .await
        {
            Ok(Some(config)) => config,
            Ok(None) => {
                warn!(
                    message_id = record.message_id.as_deref().unwrap_or("unknown"),
                    "No replay configuration found for message."
                );
                push_failure(&mut failures, &record.message_id);
                continue;
            }
            Err(error) => return Err(Error::from(error)),
        };

        match build_replay_request(&config, record) {
            Ok(Some(request)) => {
                if let Err(error) = send_replay(sqs, &request).await {
                    return Err(Error::from(error));
                }
            }
            Ok(None) => {
                info!(
                    message_id = record.message_id.as_deref().unwrap_or("unknown"),
                    "Retry maximum reached."
                );
                push_failure(&mut failures, &record.message_id);
            }
            Err(error) => return Err(Error::from(error)),
        }
    }
    Ok(BatchResponse {
        batch_item_failures: failures,
    })
}

/// Resolves the replay destination for a record: the single `QUEUE_URL` in
/// legacy mode, or the per-queue config from SSM keyed by the replay queue ARN.
async fn resolve_config(
    env: &Environment,
    config_store: &mut ConfigStore,
    event_source_arn: Option<&str>,
) -> Result<Option<ResolvedQueueConfig>, ConfigError> {
    if let Some(queue_url) = &env.queue_url {
        return Ok(Some(ResolvedQueueConfig {
            queue_url: queue_url.clone(),
            max_attempts: env.max_attempts,
            backoff_rate_seconds: env.backoff_rate_seconds,
            maximum_delay_seconds: env.maximum_delay_seconds,
            use_jitter: env.use_jitter,
        }));
    }

    let arn = event_source_arn.ok_or(ConfigError::Missing("eventSourceARN"))?;
    match config_store.config_for(arn).await? {
        Some(config) => Ok(Some(config.resolve(env))),
        None => Ok(None),
    }
}

fn push_failure(failures: &mut Vec<BatchItemFailure>, message_id: &Option<String>) {
    if let Some(message_id) = message_id {
        failures.push(BatchItemFailure {
            item_identifier: message_id.clone(),
        });
    }
}
