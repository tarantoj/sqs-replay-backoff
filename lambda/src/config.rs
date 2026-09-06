use std::collections::HashMap;
use std::time::{Duration, Instant};

use aws_sdk_ssm::Client;
use serde::Deserialize;
use tracing::warn;

use crate::environment::Environment;

/// Per-queue replay configuration, stored in SSM Parameter Store and keyed by
/// replay queue ARN.
#[derive(Debug, Clone, Deserialize)]
pub struct QueueConfig {
    /// ARN of the replay queue this configuration belongs to.
    #[serde(rename = "replayQueueArn")]
    pub replay_queue_arn: String,
    /// Queue to send replayed messages back to.
    #[serde(rename = "destinationQueueUrl")]
    pub destination_queue_url: String,
    /// Maximum replay attempts.
    #[serde(rename = "maxAttempts", default)]
    pub max_attempts: Option<u32>,
    /// Multiplier for the backoff calculation, in seconds.
    #[serde(rename = "backoffRate", default)]
    pub backoff_rate_seconds: Option<u32>,
    /// Upper bound for the backoff delay, in seconds.
    #[serde(rename = "maximumDelay", default)]
    pub maximum_delay_seconds: Option<u32>,
    /// Whether to add full jitter to the backoff delay.
    #[serde(rename = "useJitter", default)]
    pub use_jitter: Option<bool>,
}

/// A [`QueueConfig`] with global defaults applied, ready for the replay logic.
#[derive(Debug, Clone)]
pub struct ResolvedQueueConfig {
    pub queue_url: String,
    pub max_attempts: u32,
    pub backoff_rate_seconds: u32,
    pub maximum_delay_seconds: u32,
    pub use_jitter: bool,
}

impl QueueConfig {
    /// Applies the lambda's global defaults to any tuning value not set in the
    /// stored config.
    #[must_use]
    pub fn resolve(&self, defaults: &Environment) -> ResolvedQueueConfig {
        ResolvedQueueConfig {
            queue_url: self.destination_queue_url.clone(),
            max_attempts: self.max_attempts.unwrap_or(defaults.max_attempts),
            backoff_rate_seconds: self
                .backoff_rate_seconds
                .unwrap_or(defaults.backoff_rate_seconds),
            maximum_delay_seconds: self
                .maximum_delay_seconds
                .unwrap_or(defaults.maximum_delay_seconds),
            use_jitter: self.use_jitter.unwrap_or(defaults.use_jitter),
        }
    }
}

/// Loads per-queue replay configurations from SSM Parameter Store and caches
/// them for a short time so new queues are picked up without a redeploy.
pub struct ConfigStore {
    client: Client,
    path: String,
    cache: Option<(Instant, HashMap<String, QueueConfig>)>,
    ttl: Duration,
}

impl ConfigStore {
    #[must_use]
    pub const fn new(client: Client, path: String) -> Self {
        Self {
            client,
            path,
            cache: None,
            ttl: Duration::from_secs(60),
        }
    }

    /// Returns the configuration for a replay queue ARN, refreshing the cache
    /// when it is stale.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Load`] when the SSM parameters cannot be read.
    pub async fn config_for(
        &mut self,
        replay_arn: &str,
    ) -> Result<Option<QueueConfig>, ConfigError> {
        self.refresh_if_stale().await?;
        Ok(self
            .cache
            .as_ref()
            .and_then(|(_, configs)| configs.get(replay_arn).cloned()))
    }

    async fn refresh_if_stale(&mut self) -> Result<(), ConfigError> {
        match &self.cache {
            Some((loaded_at, _)) if loaded_at.elapsed() < self.ttl => Ok(()),
            _ => self.refresh().await,
        }
    }

    async fn refresh(&mut self) -> Result<(), ConfigError> {
        let mut configs = HashMap::new();
        let mut next_token: Option<String> = None;
        loop {
            let mut request = self
                .client
                .get_parameters_by_path()
                .path(&self.path)
                .recursive(true);
            if let Some(token) = &next_token {
                request = request.set_next_token(Some(token.clone()));
            }
            let response = request
                .send()
                .await
                .map_err(|error| ConfigError::Load(Box::new(error)))?;

            if let Some(parameters) = &response.parameters {
                for parameter in parameters {
                    if let Some(value) = parameter.value() {
                        match serde_json::from_str::<QueueConfig>(value) {
                            Ok(config) => {
                                configs.insert(config.replay_queue_arn.clone(), config);
                            }
                            Err(error) => {
                                warn!(
                                    parameter = parameter.name().unwrap_or("unknown"),
                                    %error,
                                    "Skipping malformed replay config"
                                );
                            }
                        }
                    }
                }
            }

            next_token = response.next_token().map(str::to_string);
            if next_token.is_none() {
                break;
            }
        }
        self.cache = Some((Instant::now(), configs));
        Ok(())
    }
}

/// Config store loading error.
#[derive(Debug)]
pub enum ConfigError {
    Missing(&'static str),
    Load(
        Box<
            aws_sdk_ssm::error::SdkError<
                aws_sdk_ssm::operation::get_parameters_by_path::GetParametersByPathError,
            >,
        >,
    ),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing(name) => write!(f, "missing {name}"),
            Self::Load(error) => {
                write!(f, "failed to load replay configs from SSM: {error}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn environment() -> Environment {
        Environment {
            config_path: "/sqs-replay/queues/".to_string(),
            max_attempts: 5,
            backoff_rate_seconds: 30,
            maximum_delay_seconds: 900,
            use_jitter: false,
        }
    }

    #[test]
    fn parses_a_queue_config_with_tuning() {
        let config: QueueConfig = serde_json::from_str(
            r#"{
                "replayQueueArn": "arn:aws:sqs:us-east-1:123456789012:ReplayQueue",
                "destinationQueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/Queue",
                "maxAttempts": 3,
                "backoffRate": 60,
                "maximumDelay": 300
            }"#,
        )
        .unwrap();
        assert_eq!(
            config.replay_queue_arn,
            "arn:aws:sqs:us-east-1:123456789012:ReplayQueue"
        );
        assert_eq!(
            config.destination_queue_url,
            "https://sqs.us-east-1.amazonaws.com/123456789012/Queue"
        );
        assert_eq!(config.max_attempts, Some(3));
        assert_eq!(config.backoff_rate_seconds, Some(60));
        assert_eq!(config.maximum_delay_seconds, Some(300));
        assert_eq!(config.use_jitter, None);
    }

    #[test]
    fn applies_global_defaults_when_tuning_is_omitted() {
        let config: QueueConfig = serde_json::from_str(
            r#"{
                "replayQueueArn": "arn:aws:sqs:us-east-1:123456789012:ReplayQueue",
                "destinationQueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/Queue"
            }"#,
        )
        .unwrap();
        let resolved = config.resolve(&environment());
        assert_eq!(
            resolved.queue_url,
            "https://sqs.us-east-1.amazonaws.com/123456789012/Queue"
        );
        assert_eq!(resolved.max_attempts, 5);
        assert_eq!(resolved.backoff_rate_seconds, 30);
        assert_eq!(resolved.maximum_delay_seconds, 900);
        assert!(!resolved.use_jitter);
    }

    #[test]
    fn defaults_follow_the_environment() {
        let config: QueueConfig = serde_json::from_str(
            r#"{
                "replayQueueArn": "arn:aws:sqs:us-east-1:123456789012:ReplayQueue",
                "destinationQueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/Queue"
            }"#,
        )
        .unwrap();
        let environment = Environment {
            max_attempts: 2,
            backoff_rate_seconds: 10,
            maximum_delay_seconds: 120,
            ..environment()
        };
        let resolved = config.resolve(&environment);
        assert_eq!(resolved.max_attempts, 2);
        assert_eq!(resolved.backoff_rate_seconds, 10);
        assert_eq!(resolved.maximum_delay_seconds, 120);
    }

    #[test]
    fn resolves_jitter_from_the_config_and_environment() {
        let config: QueueConfig = serde_json::from_str(
            r#"{
                "replayQueueArn": "arn:aws:sqs:us-east-1:123456789012:ReplayQueue",
                "destinationQueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/Queue",
                "useJitter": true
            }"#,
        )
        .unwrap();
        assert!(config.resolve(&environment()).use_jitter);
    }

    #[test]
    fn jitter_falls_back_to_the_environment_default() {
        let config: QueueConfig = serde_json::from_str(
            r#"{
                "replayQueueArn": "arn:aws:sqs:us-east-1:123456789012:ReplayQueue",
                "destinationQueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/Queue"
            }"#,
        )
        .unwrap();
        let environment = Environment {
            use_jitter: true,
            ..environment()
        };
        assert!(config.resolve(&environment).use_jitter);
    }

    #[test]
    fn rejects_a_malformed_config() {
        let result: Result<QueueConfig, _> = serde_json::from_str(
            r#"{"replayQueueArn": "arn:aws:sqs:us-east-1:123456789012:ReplayQueue"}"#,
        );
        assert!(result.is_err());
    }
}
