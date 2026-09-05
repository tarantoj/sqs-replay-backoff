use std::collections::HashMap;

use aws_lambda_events::event::sqs::{SqsMessage, SqsMessageAttribute};
use aws_sdk_sqs::operation::send_message::SendMessageOutput;
use aws_sdk_sqs::types::MessageAttributeValue;
use aws_sdk_sqs::Client;
use aws_smithy_types::Blob;

use crate::backoff::backoff;
use crate::environment::Environment;

/// Message attribute tracking how many times a message has been replayed.
pub const REPLAY_NUM_PROPERTY_NAME: &str = "sqs-dlq-replay-num";

/// A fully-formed SQS `SendMessage` request, ready to be sent.
#[derive(Debug, Clone)]
pub struct ReplayRequest {
    pub queue_url: String,
    pub message_body: String,
    pub delay_seconds: u32,
    pub message_attributes: HashMap<String, MessageAttributeValue>,
    pub message_deduplication_id: Option<String>,
    pub message_group_id: Option<String>,
}

/// Builds a [`ReplayRequest`] for an SQS record, or `None` when the replay
/// attempt limit has been exceeded (the message should be reported as failed).
pub fn build_replay_request(
    env: &Environment,
    record: &SqsMessage,
) -> Result<Option<ReplayRequest>, ReplayError> {
    let mut replay_num = replay_num(record)?;
    replay_num += 1;

    if replay_num > env.max_attempts {
        return Ok(None);
    }

    Ok(Some(ReplayRequest {
        queue_url: env.queue_url.clone(),
        message_body: record.body.clone().ok_or(ReplayError::MissingBody)?,
        delay_seconds: backoff(
            env.backoff_rate_seconds,
            env.maximum_delay_seconds,
            replay_num,
        ),
        message_attributes: map_message_attributes(&record.message_attributes, replay_num),
        message_deduplication_id: record.attributes.get("MessageDeduplicationId").cloned(),
        message_group_id: record.attributes.get("MessageGroupId").cloned(),
    }))
}

/// Sends a replay request back to the queue.
pub async fn send_replay(
    sqs: &Client,
    request: &ReplayRequest,
) -> Result<SendMessageOutput, ReplayError> {
    let mut builder = sqs
        .send_message()
        .queue_url(&request.queue_url)
        .message_body(&request.message_body)
        .set_delay_seconds(Some(request.delay_seconds as i32))
        .set_message_attributes(Some(request.message_attributes.clone()));

    if let Some(deduplication_id) = &request.message_deduplication_id {
        builder = builder.set_message_deduplication_id(Some(deduplication_id.clone()));
    }
    if let Some(group_id) = &request.message_group_id {
        builder = builder.set_message_group_id(Some(group_id.clone()));
    }

    builder
        .send()
        .await
        .map_err(|error| ReplayError::Send(Box::new(error)))
}

/// Reads the existing replay count for a record, treating a missing attribute
/// or missing string value as the first attempt.
fn replay_num(record: &SqsMessage) -> Result<u32, ReplayError> {
    let value = record
        .message_attributes
        .get(REPLAY_NUM_PROPERTY_NAME)
        .and_then(|attribute| attribute.string_value.as_deref())
        .unwrap_or("0");
    value
        .parse::<u32>()
        .map_err(|_| ReplayError::InvalidReplayNum)
}

/// Maps incoming message attributes to `SendMessage` attributes, adding the
/// replay count.
fn map_message_attributes(
    input: &HashMap<String, SqsMessageAttribute>,
    replay_num: u32,
) -> HashMap<String, MessageAttributeValue> {
    let mut attributes = HashMap::with_capacity(input.len() + 1);
    for (name, attribute) in input {
        attributes.insert(
            name.clone(),
            MessageAttributeValue::builder()
                .set_string_value(attribute.string_value.clone())
                .set_binary_value(
                    attribute
                        .binary_value
                        .as_ref()
                        .map(|value| Blob::new(value.to_vec())),
                )
                .set_string_list_values(if attribute.string_list_values.is_empty() {
                    None
                } else {
                    Some(attribute.string_list_values.clone())
                })
                .set_binary_list_values(if attribute.binary_list_values.is_empty() {
                    None
                } else {
                    Some(
                        attribute
                            .binary_list_values
                            .iter()
                            .map(|value| Blob::new(value.to_vec()))
                            .collect(),
                    )
                })
                .set_data_type(Some(attribute.data_type.clone().unwrap_or_default()))
                .build()
                .expect("message attribute value has a data type"),
        );
    }

    attributes.insert(
        REPLAY_NUM_PROPERTY_NAME.to_string(),
        MessageAttributeValue::builder()
            .set_string_value(Some(replay_num.to_string()))
            .data_type("Number")
            .build()
            .expect("replay count attribute is fully populated"),
    );

    attributes
}

/// Replay processing error.
#[derive(Debug)]
pub enum ReplayError {
    MissingBody,
    InvalidReplayNum,
    Send(Box<aws_sdk_sqs::error::SdkError<aws_sdk_sqs::operation::send_message::SendMessageError>>),
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayError::MissingBody => write!(f, "message has no body"),
            ReplayError::InvalidReplayNum => {
                write!(f, "sqs-dlq-replay-num attribute is not a number")
            }
            ReplayError::Send(error) => write!(f, "failed to send replayed message: {error}"),
        }
    }
}

impl std::error::Error for ReplayError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn env() -> Environment {
        Environment {
            queue_url: "https://sqs.ap-southeast-2.amazonaws.com/12345/MyQueue".to_string(),
            max_attempts: 5,
            backoff_rate_seconds: 30,
            maximum_delay_seconds: 900,
        }
    }

    fn record() -> SqsMessage {
        serde_json::from_value(serde_json::json!({
            "messageId": "19dd0b57-b21e-4ac1-bd88-01bbb068cb78",
            "receiptHandle": "MessageReceiptHandle",
            "body": "Hello from SQS!",
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1523232000000",
                "SenderId": "123456789012",
                "ApproximateFirstReceiveTimestamp": "1523232000001"
            },
            "messageAttributes": {},
            "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:MyQueue",
            "eventSource": "aws:sqs",
            "awsRegion": "us-east-1"
        }))
        .unwrap()
    }

    fn attribute(value: Option<&str>, data_type: &str) -> SqsMessageAttribute {
        serde_json::from_value(serde_json::json!({
            "stringValue": value,
            "stringListValues": [],
            "binaryListValues": [],
            "dataType": data_type
        }))
        .unwrap()
    }

    #[test]
    fn adds_a_delay_and_replay_num_to_a_message() {
        let request = build_replay_request(&env(), &record())
            .unwrap()
            .expect("first attempt is within max attempts");
        assert_eq!(request.delay_seconds, 60);
        let replay = &request.message_attributes[REPLAY_NUM_PROPERTY_NAME];
        assert_eq!(replay.string_value.as_deref(), Some("1"));
        assert_eq!(replay.data_type(), "Number");
    }

    #[test]
    fn adds_a_delay_and_increments_an_existing_replay_num() {
        let mut record = record();
        record.message_attributes.insert(
            REPLAY_NUM_PROPERTY_NAME.to_string(),
            attribute(Some("1"), "String"),
        );
        let request = build_replay_request(&env(), &record)
            .unwrap()
            .expect("second attempt is within max attempts");
        let replay = &request.message_attributes[REPLAY_NUM_PROPERTY_NAME];
        assert_eq!(replay.string_value.as_deref(), Some("2"));
        assert_eq!(replay.data_type(), "Number");
    }

    #[test]
    fn preserves_incoming_message_attributes() {
        let mut record = record();
        record
            .message_attributes
            .insert("custom".to_string(), attribute(Some("value"), "String"));
        let request = build_replay_request(&env(), &record).unwrap().unwrap();
        let custom = &request.message_attributes["custom"];
        assert_eq!(custom.string_value.as_deref(), Some("value"));
        assert_eq!(custom.data_type(), "String");
    }

    #[test]
    fn rejects_a_message_when_max_attempts_is_reached() {
        let mut record = record();
        record.message_attributes.insert(
            REPLAY_NUM_PROPERTY_NAME.to_string(),
            attribute(Some("5"), "Number"),
        );
        assert!(build_replay_request(&env(), &record).unwrap().is_none());
    }

    #[test]
    fn treats_a_missing_string_value_as_first_attempt() {
        let mut record = record();
        record.message_attributes.insert(
            REPLAY_NUM_PROPERTY_NAME.to_string(),
            attribute(None, "Number"),
        );
        let request = build_replay_request(&env(), &record)
            .unwrap()
            .expect("missing string value defaults to first attempt");
        let replay = &request.message_attributes[REPLAY_NUM_PROPERTY_NAME];
        assert_eq!(replay.string_value.as_deref(), Some("1"));
    }

    #[test]
    fn copies_fifo_attributes_when_present() {
        let mut record = record();
        record
            .attributes
            .insert("MessageDeduplicationId".to_string(), "dedupe".to_string());
        record
            .attributes
            .insert("MessageGroupId".to_string(), "group".to_string());
        let request = build_replay_request(&env(), &record).unwrap().unwrap();
        assert_eq!(request.message_deduplication_id.as_deref(), Some("dedupe"));
        assert_eq!(request.message_group_id.as_deref(), Some("group"));
    }

    #[test]
    fn omits_fifo_attributes_when_absent() {
        let request = build_replay_request(&env(), &record()).unwrap().unwrap();
        assert_eq!(request.message_deduplication_id, None);
        assert_eq!(request.message_group_id, None);
    }
}
