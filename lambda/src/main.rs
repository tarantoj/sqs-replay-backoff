mod backoff;
mod environment;
mod replay;

use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use tracing::info;

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
        queue_url = %env.queue_url,
        max_attempts = env.max_attempts,
        backoff_rate_seconds = env.backoff_rate_seconds,
        maximum_delay_seconds = env.maximum_delay_seconds,
        "Configuration"
    );

    let sqs = aws_sdk_sqs::Client::new(&aws_config::load_from_env().await);

    run(service_fn(|event: LambdaEvent<SqsEvent>| {
        handle(&env, &sqs, event)
    }))
    .await
}

/// Replays each SQS record back to the configured queue, reporting a batch
/// item failure for any message that has exhausted its replay attempts.
async fn handle(
    env: &Environment,
    sqs: &aws_sdk_sqs::Client,
    event: LambdaEvent<SqsEvent>,
) -> Result<BatchResponse, Error> {
    let mut failures = Vec::new();
    for record in &event.payload.records {
        match build_replay_request(env, record) {
            Ok(Some(request)) => {
                send_replay(sqs, &request).await?;
            }
            Ok(None) => {
                info!(
                    message_id = record.message_id.as_deref().unwrap_or("unknown"),
                    "Retry maximum reached."
                );
                if let Some(message_id) = &record.message_id {
                    failures.push(BatchItemFailure {
                        item_identifier: message_id.clone(),
                    });
                }
            }
            Err(error) => return Err(Error::from(error)),
        }
    }
    Ok(BatchResponse {
        batch_item_failures: failures,
    })
}
