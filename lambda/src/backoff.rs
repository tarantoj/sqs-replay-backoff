use rand::Rng;

/// Returns a random number between 0 and `max` inclusive.
fn random_int(max: u32) -> u32 {
    rand::rng().random_range(0..=max)
}

/// `min(max, base * 2^attempt)`
///
/// Example:
///
/// With a base of `30` and a max of `900`, the delay for each attempt will be
/// 1. `30 * 2^1 = 60`
/// 2. `30 * 2^2 = 120`
/// 3. `30 * 2^3 = 240`
/// 4. `30 * 2^4 = 480`
/// 5. `30 * 2^5 = 960 -> 900`
#[must_use]
pub fn backoff(base: u32, max: u32, attempt: u32) -> u32 {
    max.min(base.saturating_mul(2u32.saturating_pow(attempt)))
}

/// Calculates exponential backoff with jitter: a random number between 0 and
/// the result returned by [`backoff`].
///
/// See "Full Jitter" in
/// [Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/).
#[must_use]
pub fn backoff_with_jitter(base: u32, max: u32, attempt: u32) -> u32 {
    random_int(backoff(base, max, attempt))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_per_attempt_and_caps_at_max() {
        let expected = [60, 120, 240, 480, 900, 900];
        for (attempt, expected_delay) in (1u32..=6).zip(expected) {
            assert_eq!(backoff(30, 900, attempt), expected_delay);
        }
    }

    #[test]
    fn backoff_with_jitter_stays_within_upper_bound() {
        for attempt in 1..=6 {
            for _ in 0..100 {
                let value = backoff_with_jitter(30, 900, attempt);
                assert!(value <= backoff(30, 900, attempt));
            }
        }
    }
}
