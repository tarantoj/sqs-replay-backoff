//! Shared helpers for the replayer integration tests.
//!
//! The tests exercise the full Lambda replay flow ([`sqs_replayer_lambda::handle`])
//! against a mock SQS/SSM backend, so no AWS credentials or account are needed.

use std::collections::HashMap;

use aws_lambda_events::event::sqs::{SqsEvent, SqsMessage};
use lambda_runtime::{Context, LambdaEvent};
use serde_json::{json, Value};
use sqs_replayer_lambda::config::ConfigStore;
use sqs_replayer_lambda::environment::Environment;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};
/// Message attribute tracking how many times a message has been replayed.
pub const REPLAY_NUM_PROPERTY_NAME: &str = "sqs-dlq-replay-num";
/// SSM parameter path under which per-queue replay configs are stored.
pub const SSM_PATH: &str = "/sqs-replay/queues/";
/// Replay queue ARN used by the test records.
pub const REPLAY_ARN: &str = "arn:aws:sqs:us-east-1:123456789012:ReplayQueue";
/// Source queue URL used by the test configs.
pub const SOURCE_URL: &str = "https://sqs.us-east-1.amazonaws.com/123456789012/SourceQueue";

/// Config JSON with every tuning value set, mirroring what the construct
/// writes to SSM when all options are provided.
pub fn config_all_tuning() -> Value {
    json!({
        "replayQueueArn": REPLAY_ARN,
        "destinationQueueUrl": SOURCE_URL,
        "maxAttempts": 5,
        "backoffRate": 30,
        "maximumDelay": 900,
        "useJitter": false
    })
}

/// Config JSON with only the routing fields, mirroring what the construct
/// writes when tuning options are omitted (the lambda applies defaults).
pub fn config_minimal() -> Value {
    json!({
        "replayQueueArn": REPLAY_ARN,
        "destinationQueueUrl": SOURCE_URL
    })
}

/// A wiremock server emulating SSM and SQS, with AWS SDK clients pointed at it.
pub struct Harness {
    pub server: MockServer,
    pub sqs: aws_sdk_sqs::Client,
    pub ssm: aws_sdk_ssm::Client,
}

impl Harness {
    pub async fn start() -> Self {
        let server = MockServer::start().await;
        let uri = server.uri();
        Self {
            server,
            sqs: sqs_client(&uri).await,
            ssm: ssm_client(&uri).await,
        }
    }

    pub fn environment() -> Environment {
        Environment {
            config_path: SSM_PATH.to_string(),
            max_attempts: 5,
            backoff_rate_seconds: 30,
            maximum_delay_seconds: 900,
            use_jitter: false,
        }
    }

    pub fn config_store(&self) -> tokio::sync::Mutex<ConfigStore> {
        tokio::sync::Mutex::new(ConfigStore::new(self.ssm.clone(), SSM_PATH.to_string()))
    }

    /// Runs the replay flow over the given records and returns its result.
    pub async fn run(
        &self,
        records: Vec<SqsMessage>,
    ) -> Result<sqs_replayer_lambda::BatchResponse, lambda_runtime::Error> {
        let mut payload = SqsEvent::default();
        payload.records = records;
        let event = LambdaEvent {
            payload,
            context: Context::default(),
        };
        sqs_replayer_lambda::handle(&Self::environment(), &self.sqs, &self.config_store(), event)
            .await
    }
}

/// Builds an SQS client that talks to the mock backend at `uri`.
pub async fn sqs_client(uri: &str) -> aws_sdk_sqs::Client {
    aws_sdk_sqs::Client::new(&sdk_config(uri).await)
}

/// Builds an SSM client that talks to the mock backend at `uri`.
pub async fn ssm_client(uri: &str) -> aws_sdk_ssm::Client {
    aws_sdk_ssm::Client::new(&sdk_config(uri).await)
}

/// Cross-service config pointing every request at the mock backend, with
/// retries disabled so failing tests fail fast.
async fn sdk_config(uri: &str) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region("us-east-1")
        .credentials_provider(aws_credential_types::Credentials::new(
            "access", "secret", None, None, "test",
        ))
        .endpoint_url(uri)
        .retry_config(aws_config::retry::RetryConfig::disabled())
        .load()
        .await
}

/// Builds an SQS message for the shared [`REPLAY_ARN`] replay queue.
pub fn record(message_id: &str, body: &str, replay_num: Option<u32>) -> SqsMessage {
    record_for(REPLAY_ARN, message_id, body, replay_num, &HashMap::new())
}

/// Builds an SQS message for a specific replay queue ARN, with optional
/// `sqs-dlq-replay-num` and system attributes (e.g. `AWSTraceHeader`).
pub fn record_for(
    event_source_arn: &str,
    message_id: &str,
    body: &str,
    replay_num: Option<u32>,
    attributes: &HashMap<String, String>,
) -> SqsMessage {
    let message_attributes = replay_num.map_or_else(
        || json!({}),
        |num| {
            json!({
                REPLAY_NUM_PROPERTY_NAME: {
                    "stringValue": num.to_string(),
                    "stringListValues": [],
                    "binaryListValues": [],
                    "dataType": "Number"
                }
            })
        },
    );
    serde_json::from_value(json!({
        "messageId": message_id,
        "receiptHandle": format!("receipt-{message_id}"),
        "body": body,
        "attributes": attributes,
        "messageAttributes": message_attributes,
        "eventSourceARN": event_source_arn,
        "eventSource": "aws:sqs",
        "awsRegion": "us-east-1"
    }))
    .unwrap()
}

/// Builds an SQS message with no `eventSourceARN`.
pub fn record_without_arn(message_id: &str, body: &str) -> SqsMessage {
    serde_json::from_value(json!({
        "messageId": message_id,
        "receiptHandle": format!("receipt-{message_id}"),
        "body": body,
        "attributes": {},
        "messageAttributes": {},
        "eventSource": "aws:sqs",
        "awsRegion": "us-east-1"
    }))
    .unwrap()
}

/// Mounts an SSM `GetParametersByPath` mock returning `parameters` (name/value
/// pairs), optionally followed by `next_token` to force another page.
pub async fn mount_ssm_config(
    server: &MockServer,
    parameters: &[(&str, Value)],
    next_token: Option<&str>,
) {
    Mock::given(method("POST"))
        .and(path("/"))
        .and(header("x-amz-target", "AmazonSSM.GetParametersByPath"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/x-amz-json-1.1")
                .set_body_json(ssm_response(parameters, next_token)),
        )
        .mount(server)
        .await;
}

pub fn ssm_response(parameters: &[(&str, Value)], next_token: Option<&str>) -> Value {
    let mut body = json!({
        "Parameters": parameters
            .iter()
            .map(|(name, value)| {
                json!({
                    "Name": name,
                    "Type": "String",
                    "Value": value.to_string(),
                    "Version": 1
                })
            })
            .collect::<Vec<_>>()
    });
    if let Some(token) = next_token {
        body["NextToken"] = json!(token);
    }
    body
}

/// Mounts an SSM `GetParametersByPath` mock that serves `first_page` and then,
/// when asked with a `NextToken`, serves `second_page`.
pub async fn mount_ssm_paged(server: &MockServer, first_page: Value, second_page: Value) {
    Mock::given(method("POST"))
        .and(path("/"))
        .and(header("x-amz-target", "AmazonSSM.GetParametersByPath"))
        .respond_with(move |request: &Request| {
            let has_token = request
                .body_json::<Value>()
                .is_ok_and(|body| body.get("NextToken").is_some());
            let page = if has_token { &second_page } else { &first_page };
            ResponseTemplate::new(200).set_body_json(page.clone())
        })
        .mount(server)
        .await;
}

/// Mounts an SQS `SendMessageBatch` mock that accepts every entry.
pub async fn mount_sqs_success(server: &MockServer) {
    mount_sqs_batch(server, &[], &[]).await;
}

/// Mounts an SQS `SendMessageBatch` mock accepting `ok` ids and rejecting the
/// `failed` ids.
pub async fn mount_sqs_batch(server: &MockServer, ok: &[&str], failed: &[&str]) {
    Mock::given(method("POST"))
        .and(path("/"))
        .and(header("x-amz-target", "AmazonSQS.SendMessageBatch"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/x-amz-json-1.0")
                .set_body_json(sqs_batch_response(ok, failed)),
        )
        .mount(server)
        .await;
}

/// Mounts an SQS `SendMessageBatch` mock that fails the whole request.
pub async fn mount_sqs_error(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/"))
        .and(header("x-amz-target", "AmazonSQS.SendMessageBatch"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal failure"))
        .mount(server)
        .await;
}

fn sqs_batch_response(ok: &[&str], failed: &[&str]) -> Value {
    json!({
        "Successful": ok
            .iter()
            .map(|id| {
                json!({
                    "Id": id,
                    "MessageId": format!("msg-{id}"),
                    "MD5OfMessageBody": "abc"
                })
            })
            .collect::<Vec<_>>(),
        "Failed": failed
            .iter()
            .map(|id| {
                json!({
                    "Id": id,
                    "SenderFault": true,
                    "Code": "SomeError",
                    "Message": "Rejected"
                })
            })
            .collect::<Vec<_>>()
    })
}

/// Returns the JSON bodies of every `SendMessageBatch` request the mock backend
/// received.
pub async fn batch_requests(server: &MockServer) -> Vec<Value> {
    let requests = server.received_requests().await.unwrap();
    requests
        .iter()
        .filter(|request| is_target(request, "AmazonSQS.SendMessageBatch"))
        .map(|request| request.body_json::<Value>().unwrap())
        .collect()
}

/// Returns the JSON bodies of every `GetParametersByPath` request the mock
/// backend received.
pub async fn ssm_requests(server: &MockServer) -> Vec<Value> {
    let requests = server.received_requests().await.unwrap();
    requests
        .iter()
        .filter(|request| is_target(request, "AmazonSSM.GetParametersByPath"))
        .map(|request| request.body_json::<Value>().unwrap())
        .collect()
}

fn is_target(request: &Request, target: &str) -> bool {
    request
        .headers
        .get("x-amz-target")
        .is_some_and(|value| value.to_str().is_ok_and(|header| header == target))
}

/// Returns the `index`-th entry (1-based) of a `SendMessageBatch` request.
pub fn entry(request: &Value, index: usize) -> &Value {
    &request["Entries"][index - 1]
}

/// Counts the message entries in a `SendMessageBatch` request.
pub fn entry_count(request: &Value) -> usize {
    request["Entries"].as_array().map_or(0, Vec::len)
}

/// Reads a per-entry message attribute value by name.
pub fn attribute_value<'a>(request: &'a Value, index: usize, name: &str) -> Option<&'a str> {
    entry(request, index)["MessageAttributes"][name]["StringValue"].as_str()
}
