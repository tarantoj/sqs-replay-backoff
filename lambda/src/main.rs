use lambda_runtime::Error;

fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    tracing::info!("sqs-replayer-lambda: TODO implement generic SQS replayer handler");
    Ok(())
}
