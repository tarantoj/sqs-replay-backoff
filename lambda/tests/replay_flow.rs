//! Integration tests for the replayer Lambda's end-to-end flow.
//!
//! Each test runs [`sqs_replayer_lambda::handle`] against a mock SQS/SSM
//! backend (see `support`), covering config resolution from SSM, backoff delay
//! computation, `SendMessageBatch` chunking, and batch item failure reporting.

mod support;

use std::collections::HashMap;

use serde_json::json;
use support::*;

const OTHER_SOURCE_URL: &str = "https://sqs.us-east-1.amazonaws.com/123456789012/OtherQueue";
const ARN_A: &str = "arn:aws:sqs:us-east-1:123456789012:QueueA";
const ARN_B: &str = "arn:aws:sqs:us-east-1:123456789012:QueueB";

fn failure_ids(response: &sqs_replayer_lambda::BatchResponse) -> Vec<&str> {
    response
        .batch_item_failures
        .iter()
        .map(|failure| failure.item_identifier.as_str())
        .collect()
}

#[tokio::test]
async fn replays_a_message_to_its_source_with_backoff_delay() {
    let harness = Harness::start().await;
    mount_ssm_config(
        &harness.server,
        &[("queue/config", config_all_tuning())],
        None,
    )
    .await;
    mount_sqs_success(&harness.server).await;

    let response = harness
        .run(vec![record("id-1", "Hello from SQS!", None)])
        .await
        .unwrap();
    assert!(response.batch_item_failures.is_empty());

    let sends = batch_requests(&harness.server).await;
    assert_eq!(sends.len(), 1);
    let request = &sends[0];
    assert_eq!(request["QueueUrl"].as_str(), Some(SOURCE_URL));
    assert_eq!(
        entry(request, 1)["MessageBody"].as_str(),
        Some("Hello from SQS!")
    );
    assert_eq!(entry(request, 1)["DelaySeconds"].as_u64(), Some(60));
    assert_eq!(
        attribute_value(request, 1, REPLAY_NUM_PROPERTY_NAME),
        Some("1")
    );
}

#[tokio::test]
async fn resolves_the_destination_and_tuning_from_the_construct_config() {
    let harness = Harness::start().await;
    let config = json!({
        "replayQueueArn": REPLAY_ARN,
        "destinationQueueUrl": OTHER_SOURCE_URL,
        "maxAttempts": 3,
        "backoffRate": 60,
        "maximumDelay": 300,
        "useJitter": false
    });
    mount_ssm_config(&harness.server, &[("queue/config", config)], None).await;
    mount_sqs_success(&harness.server).await;

    let response = harness
        .run(vec![record("id-1", "body", None)])
        .await
        .unwrap();
    assert!(response.batch_item_failures.is_empty());

    let request = &batch_requests(&harness.server).await[0];
    assert_eq!(request["QueueUrl"].as_str(), Some(OTHER_SOURCE_URL));
    assert_eq!(entry(request, 1)["DelaySeconds"].as_u64(), Some(120));
}

#[tokio::test]
async fn applies_global_defaults_when_the_config_omits_tuning() {
    let harness = Harness::start().await;
    mount_ssm_config(&harness.server, &[("queue/config", config_minimal())], None).await;
    mount_sqs_success(&harness.server).await;

    let response = harness
        .run(vec![record("id-1", "body", None)])
        .await
        .unwrap();
    assert!(response.batch_item_failures.is_empty());

    let request = &batch_requests(&harness.server).await[0];
    assert_eq!(request["QueueUrl"].as_str(), Some(SOURCE_URL));
    assert_eq!(entry(request, 1)["DelaySeconds"].as_u64(), Some(60));
}

#[tokio::test]
async fn reports_a_batch_item_failure_when_max_attempts_is_reached() {
    let harness = Harness::start().await;
    mount_ssm_config(
        &harness.server,
        &[("queue/config", config_all_tuning())],
        None,
    )
    .await;

    let response = harness
        .run(vec![record("id-1", "body", Some(5))])
        .await
        .unwrap();
    assert_eq!(failure_ids(&response), vec!["id-1"]);
    assert!(batch_requests(&harness.server).await.is_empty());
}

#[tokio::test]
async fn reports_a_batch_item_failure_when_no_config_exists() {
    let harness = Harness::start().await;
    mount_ssm_config(&harness.server, &[], None).await;

    let response = harness
        .run(vec![record("id-1", "body", None)])
        .await
        .unwrap();
    assert_eq!(failure_ids(&response), vec!["id-1"]);
    assert!(batch_requests(&harness.server).await.is_empty());
}

#[tokio::test]
async fn loads_configs_across_multiple_ssm_pages() {
    let harness = Harness::start().await;
    mount_ssm_paged(
        &harness.server,
        ssm_response(
            &[(
                ARN_B,
                json!({
                    "replayQueueArn": ARN_B,
                    "destinationQueueUrl": OTHER_SOURCE_URL
                }),
            )],
            Some("page-2"),
        ),
        ssm_response(&[(ARN_A, config_for(ARN_A))], None),
    )
    .await;
    mount_sqs_success(&harness.server).await;

    let response = harness
        .run(vec![
            record_for(ARN_A, "id-a", "from a", None, &HashMap::new()),
            record_for(ARN_B, "id-b", "from b", None, &HashMap::new()),
        ])
        .await
        .unwrap();
    assert!(response.batch_item_failures.is_empty());

    let ssm_calls = ssm_requests(&harness.server).await;
    assert_eq!(ssm_calls.len(), 2);
    assert_eq!(ssm_calls[1].get("NextToken").unwrap(), "page-2");
    assert_eq!(batch_requests(&harness.server).await.len(), 2);
}

#[tokio::test]
async fn skips_malformed_configs_and_reports_the_missing_destination() {
    let harness = Harness::start().await;
    mount_ssm_config(
        &harness.server,
        &[
            ("queue/bad", json!("this is not valid config json")),
            ("queue/good", config_for(ARN_A)),
        ],
        None,
    )
    .await;
    mount_sqs_success(&harness.server).await;

    let response = harness
        .run(vec![
            record_for(ARN_A, "id-a", "from a", None, &HashMap::new()),
            record_for(ARN_B, "id-b", "from b", None, &HashMap::new()),
        ])
        .await
        .unwrap();
    assert_eq!(failure_ids(&response), vec!["id-b"]);

    let sends = batch_requests(&harness.server).await;
    assert_eq!(sends.len(), 1);
    assert_eq!(entry(&sends[0], 1)["Id"].as_str(), Some("id-a"));
}

#[tokio::test]
async fn splits_replays_across_sqs_calls_at_the_entry_limit() {
    let harness = Harness::start().await;
    mount_ssm_config(
        &harness.server,
        &[("queue/config", config_all_tuning())],
        None,
    )
    .await;
    mount_sqs_success(&harness.server).await;

    let records: Vec<_> = (0..25)
        .map(|i| record(&format!("id-{i}"), "body", None))
        .collect();
    let response = harness.run(records).await.unwrap();
    assert!(response.batch_item_failures.is_empty());

    let sends = batch_requests(&harness.server).await;
    assert_eq!(sends.len(), 3);
    assert!(sends.iter().all(|request| entry_count(request) <= 10));
    let total: usize = sends.iter().map(entry_count).sum();
    assert_eq!(total, 25);
}

#[tokio::test]
async fn splits_replays_when_a_payload_exceeds_the_sqs_limit() {
    let harness = Harness::start().await;
    mount_ssm_config(
        &harness.server,
        &[("queue/config", config_all_tuning())],
        None,
    )
    .await;
    mount_sqs_success(&harness.server).await;

    let big = "a".repeat(120 * 1024);
    let records: Vec<_> = (0..3)
        .map(|i| record(&format!("id-{i}"), &big, None))
        .collect();
    let response = harness.run(records).await.unwrap();
    assert!(response.batch_item_failures.is_empty());

    let sends = batch_requests(&harness.server).await;
    assert_eq!(sends.len(), 2);
    assert!(sends.iter().all(|request| entry_count(request) <= 2));
}

#[tokio::test]
async fn reports_rejected_entries_as_batch_item_failures() {
    let harness = Harness::start().await;
    mount_ssm_config(
        &harness.server,
        &[("queue/config", config_all_tuning())],
        None,
    )
    .await;
    mount_sqs_batch(&harness.server, &["id-ok"], &["id-bad"]).await;

    let response = harness
        .run(vec![
            record("id-ok", "ok", None),
            record("id-bad", "bad", None),
        ])
        .await
        .unwrap();
    assert_eq!(failure_ids(&response), vec!["id-bad"]);
}

#[tokio::test]
async fn reports_every_entry_when_the_batch_call_errors() {
    let harness = Harness::start().await;
    mount_ssm_config(
        &harness.server,
        &[("queue/config", config_all_tuning())],
        None,
    )
    .await;
    mount_sqs_error(&harness.server).await;

    let response = harness
        .run(vec![
            record("id-1", "one", None),
            record("id-2", "two", None),
        ])
        .await
        .unwrap();
    let mut ids = failure_ids(&response);
    ids.sort_unstable();
    assert_eq!(ids, vec!["id-1", "id-2"]);
}

#[tokio::test]
async fn reports_a_batch_item_failure_when_a_record_has_no_event_source_arn() {
    let harness = Harness::start().await;
    mount_ssm_config(
        &harness.server,
        &[("queue/config", config_all_tuning())],
        None,
    )
    .await;

    let response = harness
        .run(vec![record_without_arn("id-1", "body")])
        .await
        .unwrap();
    assert_eq!(failure_ids(&response), vec!["id-1"]);
    assert!(batch_requests(&harness.server).await.is_empty());
}

#[tokio::test]
async fn reports_a_batch_item_failure_when_a_replay_num_is_not_a_number() {
    let harness = Harness::start().await;
    mount_ssm_config(
        &harness.server,
        &[("queue/config", config_all_tuning())],
        None,
    )
    .await;

    let record: aws_lambda_events::event::sqs::SqsMessage = serde_json::from_value(json!({
        "messageId": "id-1",
        "receiptHandle": "receipt-id-1",
        "body": "body",
        "attributes": {},
        "messageAttributes": {
            REPLAY_NUM_PROPERTY_NAME: {
                "stringValue": "not-a-number",
                "stringListValues": [],
                "binaryListValues": [],
                "dataType": "String"
            }
        },
        "eventSourceARN": REPLAY_ARN,
        "eventSource": "aws:sqs",
        "awsRegion": "us-east-1"
    }))
    .unwrap();

    let response = harness.run(vec![record]).await.unwrap();
    assert_eq!(failure_ids(&response), vec!["id-1"]);
    assert!(batch_requests(&harness.server).await.is_empty());
}

#[tokio::test]
async fn reports_a_batch_item_failure_when_a_message_has_no_body() {
    let harness = Harness::start().await;
    mount_ssm_config(
        &harness.server,
        &[("queue/config", config_all_tuning())],
        None,
    )
    .await;

    let record: aws_lambda_events::event::sqs::SqsMessage = serde_json::from_value(json!({
        "messageId": "id-1",
        "receiptHandle": "receipt-id-1",
        "attributes": {},
        "messageAttributes": {},
        "eventSourceARN": REPLAY_ARN,
        "eventSource": "aws:sqs",
        "awsRegion": "us-east-1"
    }))
    .unwrap();

    let response = harness.run(vec![record]).await.unwrap();
    assert_eq!(failure_ids(&response), vec!["id-1"]);
    assert!(batch_requests(&harness.server).await.is_empty());
}

#[tokio::test]
async fn forwards_the_trace_header_to_the_replayed_message() {
    let harness = Harness::start().await;
    mount_ssm_config(
        &harness.server,
        &[("queue/config", config_all_tuning())],
        None,
    )
    .await;
    mount_sqs_success(&harness.server).await;

    let trace = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1";
    let attributes = HashMap::from([("AWSTraceHeader".to_string(), trace.to_string())]);
    let response = harness
        .run(vec![record_for(
            REPLAY_ARN,
            "id-1",
            "body",
            None,
            &attributes,
        )])
        .await
        .unwrap();
    assert!(response.batch_item_failures.is_empty());

    let request = &batch_requests(&harness.server).await[0];
    assert_eq!(
        entry(request, 1)["MessageSystemAttributes"]["AWSTraceHeader"]["StringValue"].as_str(),
        Some(trace)
    );
}

/// Config JSON for a specific replay queue ARN.
fn config_for(replay_arn: &str) -> serde_json::Value {
    json!({
        "replayQueueArn": replay_arn,
        "destinationQueueUrl": SOURCE_URL
    })
}
