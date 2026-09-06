use std::collections::HashMap;
use std::env;

/// Default SSM parameter path under which per-queue replay configs are stored.
pub const DEFAULT_CONFIG_PATH: &str = "/sqs-replay/queues/";
/// Maximum allowed delay in seconds, matching the SQS message timer limit of
/// 15 minutes.
pub const MAXIMUM_DELAY_LIMIT_SECONDS: u32 = 15 * 60;

/// Environment configuration for the SQS replayer lambda.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Environment {
    /// SSM parameter path under which per-queue replay configs are stored.
    pub config_path: String,
    /// Maximum replay attempts.
    pub max_attempts: u32,
    /// Multiplier for the backoff calculation, in seconds.
    pub backoff_rate_seconds: u32,
    /// Upper bound for the backoff delay, in seconds.
    pub maximum_delay_seconds: u32,
    /// Whether to add full jitter to the backoff delay.
    pub use_jitter: bool,
}

impl Environment {
    /// Reads and validates the configuration from the lambda's environment.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::parse(&env::vars().collect())
    }

    /// Parses and validates configuration from a set of environment values.
    pub fn parse(values: &HashMap<String, String>) -> Result<Self, ConfigError> {
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
            config_path,
            max_attempts: positive_int(values, "MAX_ATTEMPTS", 5)?,
            backoff_rate_seconds: positive_int(values, "BACKOFF_RATE", 30)?,
            maximum_delay_seconds: maximum_delay(values)?,
            use_jitter: bool_flag(values, "BACKOFF_JITTER", false)?,
        })
    }
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

/// Parses a boolean flag, falling back to `default` when unset.
fn bool_flag(
    values: &HashMap<String, String>,
    name: &str,
    default: bool,
) -> Result<bool, ConfigError> {
    match values.get(name) {
        None => Ok(default),
        Some(value) => match value.to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(ConfigError::Invalid(format!(
                "{name} must be either 'true' or 'false'"
            ))),
        },
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
        HashMap::new()
    }

    #[test]
    fn sets_defaults_when_optional_values_unset() {
        let result = Environment::parse(&vars()).unwrap();
        assert_eq!(
            result,
            Environment {
                config_path: "/sqs-replay/queues/".to_string(),
                max_attempts: 5,
                backoff_rate_seconds: 30,
                maximum_delay_seconds: 900,
                use_jitter: false,
            }
        );
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
    fn jitter_defaults_to_false() {
        let result = Environment::parse(&HashMap::new()).unwrap();
        assert!(!result.use_jitter);
    }

    #[test]
    fn accepts_jitter_flag() {
        let mut values = vars();
        values.insert("BACKOFF_JITTER".to_string(), "true".to_string());
        let result = Environment::parse(&values).unwrap();
        assert!(result.use_jitter);
    }

    #[test]
    fn rejects_an_invalid_jitter_flag() {
        let mut values = vars();
        values.insert("BACKOFF_JITTER".to_string(), "yes".to_string());
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
