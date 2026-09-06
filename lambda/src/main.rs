use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use tracing::info;

use sqs_replayer_lambda::{config::ConfigStore, environment::Environment, handle};

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda_runtime::tracing::init_default_subscriber();

    let env = Environment::from_env()?;
    info!(
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
