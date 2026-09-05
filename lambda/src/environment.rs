use std::collections::HashMap;
use std::env;

use url::Url;

/// Default SSM parameter path under which per-queue replay configs are stored.
pub const DEFAULT_CONFIG_PATH: &str = "/sqs-replay/queues/";
/// Maximum allowed delay in seconds, matching the SQS message timer limit of
/// 15 minutes.
pub const MAXIMUM_DELAY_LIMIT_SECONDS: u32 = 15 * 60;

/// Environment configuration for the SQS replayer lambda.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Environment {
    /// Queue to send messages back to. When set, the lambda runs in single-queue
    /// mode and ignores the SSM config store.
    pub queue_url: Option<String>,
    /// SSM parameter path under which per-queue replay configs are stored.
    pub config_path: String,
    /// Maximum replay attempts.
    pub max_attempts: u32,
    /// Multiplier for the backoff calculation, in seconds.
    pub backoff_rate_seconds: u32,
    /// Upper bound for the backoff delay, in seconds.
    pub maximum_delay_seconds: u32,
}

impl Environment {
    /// Reads and validates the configuration from the lambda's environment.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::parse(&env::vars().collect())
    }

    /// Parses and validates configuration from a set of environment values.
    pub fn parse(values: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let queue_url = values
            .get("QUEUE_URL")
            .map(|value| validate_url(value))
            .transpose()?;

        let config_path = values
            .get("SSM_PARAMETER_PATH")
            .cloned()
            .unwrap_or_else(|| DEFAULT_CONFIG_PATH.to_string());
        if !config_path.starts_with('/') {
            return Err(ConfigError::Invalid(
                "SSM_PARAMETER_PATH must start with '/'".to_string(),
            ));
        }

        Ok(Environment {
            queue_url,
            config_path,
            max_attempts: positive_int(values, "MAX_ATTEMPTS", 5)?,
            backoff_rate_seconds: positive_int(values, "BACKOFF_RATE", 30)?,
            maximum_delay_seconds: maximum_delay(values)?,
        })
    }
}

fn validate_url(value: &str) -> Result<String, ConfigError> {
    let parsed = Url::parse(value)
        .map_err(|_| ConfigError::Invalid("QUEUE_URL must be a valid URL".to_string()))?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(ConfigError::Invalid(
            "QUEUE_URL must use the http or https scheme".to_string(),
        ));
    }
    Ok(value.to_string())
}

/// Parses a positive integer environment variable, falling back to `default`
/// when unset.
fn positive_int(
    values: &HashMap<String, String>,
    name: &str,
    default: u32,
) -> Result<u32, ConfigError> {
    match values.get(name) {
        None => Ok(default),
        Some(value) => {
            let parsed = value
                .parse::<u32>()
                .map_err(|_| ConfigError::Invalid(format!("{name} must be a positive integer")))?;
            if parsed == 0 {
                return Err(ConfigError::Invalid(format!(
                    "{name} must be a positive integer"
                )));
            }
            Ok(parsed)
        }
    }
}

/// Parses `MAXIMUM_DELAY`, enforcing the SQS message timer limit.
fn maximum_delay(values: &HashMap<String, String>) -> Result<u32, ConfigError> {
    let delay = positive_int(values, "MAXIMUM_DELAY", MAXIMUM_DELAY_LIMIT_SECONDS)?;
    if delay > MAXIMUM_DELAY_LIMIT_SECONDS {
        return Err(ConfigError::Invalid(format!(
            "MAXIMUM_DELAY must be at most {MAXIMUM_DELAY_LIMIT_SECONDS} seconds"
        )));
    }
    Ok(delay)
}

/// Configuration parsing and validation error.
#[derive(Debug)]
pub enum ConfigError {
    Invalid(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Invalid(message) => write!(f, "invalid configuration: {message}"),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars() -> HashMap<String, String> {
        HashMap::from([(
            "QUEUE_URL".to_string(),
            "https://sqs.ap-southeast-2.amazonaws.com/12345/MyQueue".to_string(),
        )])
    }

    #[test]
    fn sets_defaults_when_optional_values_unset() {
        let result = Environment::parse(&vars()).unwrap();
        assert_eq!(
            result,
            Environment {
                queue_url: Some(
                    "https://sqs.ap-southeast-2.amazonaws.com/12345/MyQueue".to_string()
                ),
                config_path: "/sqs-replay/queues/".to_string(),
                max_attempts: 5,
                backoff_rate_seconds: 30,
                maximum_delay_seconds: 900,
            }
        );
    }

    #[test]
    fn defaults_to_the_config_store_when_queue_url_is_unset() {
        let result = Environment::parse(&HashMap::new()).unwrap();
        assert_eq!(result.queue_url, None);
        assert_eq!(result.config_path, "/sqs-replay/queues/");
    }

    #[test]
    fn accepts_explicit_values() {
        let mut values = vars();
        values.insert("MAX_ATTEMPTS".to_string(), "3".to_string());
        values.insert("BACKOFF_RATE".to_string(), "60".to_string());
        values.insert("MAXIMUM_DELAY".to_string(), "120".to_string());
        values.insert("SSM_PARAMETER_PATH".to_string(), "/custom/".to_string());
        let result = Environment::parse(&values).unwrap();
        assert_eq!(result.max_attempts, 3);
        assert_eq!(result.backoff_rate_seconds, 60);
        assert_eq!(result.maximum_delay_seconds, 120);
        assert_eq!(result.config_path, "/custom/");
    }

    #[test]
    fn errors_when_queue_url_is_not_a_url() {
        let values = HashMap::from([("QUEUE_URL".to_string(), "abc".to_string())]);
        assert!(Environment::parse(&values).is_err());
    }

    #[test]
    fn errors_when_queue_url_has_non_http_scheme() {
        let values = HashMap::from([("QUEUE_URL".to_string(), "ftp://example.com".to_string())]);
        assert!(Environment::parse(&values).is_err());
    }

    #[test]
    fn errors_when_config_path_is_not_absolute() {
        let values = HashMap::from([("SSM_PARAMETER_PATH".to_string(), "rel".to_string())]);
        assert!(Environment::parse(&values).is_err());
    }

    #[test]
    fn errors_when_backoff_rate_is_negative() {
        let mut values = vars();
        values.insert("BACKOFF_RATE".to_string(), "-1".to_string());
        assert!(Environment::parse(&values).is_err());
    }

    #[test]
    fn errors_when_maximum_delay_is_negative() {
        let mut values = vars();
        values.insert("MAXIMUM_DELAY".to_string(), "-1".to_string());
        assert!(Environment::parse(&values).is_err());
    }

    #[test]
    fn errors_when_max_attempts_is_negative() {
        let mut values = vars();
        values.insert("MAX_ATTEMPTS".to_string(), "-1".to_string());
        assert!(Environment::parse(&values).is_err());
    }

    #[test]
    fn errors_when_maximum_delay_is_greater_than_15_minutes() {
        let mut values = vars();
        values.insert("MAXIMUM_DELAY".to_string(), "1000".to_string());
        assert!(Environment::parse(&values).is_err());
    }

    #[test]
    fn errors_when_maximum_delay_is_a_float() {
        let mut values = vars();
        values.insert("MAXIMUM_DELAY".to_string(), "3.14".to_string());
        assert!(Environment::parse(&values).is_err());
    }
}
