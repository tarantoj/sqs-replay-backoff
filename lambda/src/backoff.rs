use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Monotonic counter mixed into the PRNG seed so concurrent invocations (and
/// back-to-back calls within the same nanosecond) do not draw identical
/// streams.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Returns a random number between 0 and `max` inclusive, using a small
/// dependency-free xorshift64* generator seeded from the clock and a counter.
#[allow(
    clippy::cast_possible_truncation,
    reason = "result of the modulo is bounded by max, a u32"
)]
fn random_int(max: u32) -> u32 {
    if max == 0 {
        return 0;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| u64::from(duration.subsec_nanos()));
    let mut state = COUNTER.fetch_add(1, Ordering::Relaxed)
        ^ (nanos << 32)
        ^ nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    (state.wrapping_mul(0x2545_F491_4F6C_DD1D) % (u64::from(max) + 1)) as u32
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
pub fn backoff(base: u32, max: u32, attempt: u32) -> u32 {
    max.min(base.saturating_mul(2u32.saturating_pow(attempt)))
}

/// Calculates exponential backoff with jitter: a random number between 0 and
/// the result returned by [`backoff`].
///
/// See "Full Jitter" in
/// [Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/).
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
