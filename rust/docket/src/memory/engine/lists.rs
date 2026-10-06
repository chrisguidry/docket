//! List command helpers for the Python binding layer.
//!
//! This module provides helper functions for Redis list commands.
//! The actual Python method implementations live in lib.rs following
//! the established pattern (via #[pymethods] on BurnerRedis).
//!
//! List commands implemented:
//! - LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LINSERT,
//! - LREM, LSET, LTRIM, LMOVE, RPOPLPUSH, BRPOP, BLPOP, BLMOVE
//!
//! The core list logic lives in Store (src/store.rs).

use crate::memory::engine::store::StoreError;

/// Which end of a list to operate on (for LMOVE/BLMOVE/RPOPLPUSH).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListEnd {
    Left,
    Right,
}

/// Whether LINSERT operates before or after the pivot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InsertPosition {
    Before,
    After,
}

/// LREM direction interpreted from the signed count argument.
///
/// - `Head(n)`: count > 0 — scan head-to-tail, remove up to N matches.
/// - `Tail(n)`: count < 0 — scan tail-to-head, remove up to N matches.
/// - `All`:    count == 0 — remove all matches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LremDirection {
    Head(usize),
    Tail(usize),
    All,
}

/// Parse "LEFT" / "RIGHT" case-insensitively for LMOVE/BLMOVE.
pub fn parse_list_end(s: &str) -> Result<ListEnd, StoreError> {
    match s.to_ascii_uppercase().as_str() {
        "LEFT" => Ok(ListEnd::Left),
        "RIGHT" => Ok(ListEnd::Right),
        _ => Err(StoreError::Syntax(format!(
            "ERR syntax error: expected LEFT or RIGHT, got {}",
            s
        ))),
    }
}

/// Parse "BEFORE" / "AFTER" case-insensitively for LINSERT.
pub fn parse_linsert_where(s: &str) -> Result<InsertPosition, StoreError> {
    match s.to_ascii_uppercase().as_str() {
        "BEFORE" => Ok(InsertPosition::Before),
        "AFTER" => Ok(InsertPosition::After),
        _ => Err(StoreError::Syntax(format!(
            "ERR syntax error: expected BEFORE or AFTER, got {}",
            s
        ))),
    }
}

/// Map a signed LREM count to its direction.
pub fn parse_lrem_count(count: i64) -> LremDirection {
    match count.cmp(&0) {
        std::cmp::Ordering::Greater => LremDirection::Head(count as usize),
        std::cmp::Ordering::Less => LremDirection::Tail(count.unsigned_abs() as usize),
        std::cmp::Ordering::Equal => LremDirection::All,
    }
}

/// Normalize a Python/Redis-style (negative-allowed) inclusive range to concrete
/// usize bounds for a list of `len` elements.
///
/// Returns `None` when the normalized range is empty:
/// - the list is empty,
/// - start > end after normalization,
/// - the normalized end is still negative (range entirely before the list).
///
/// Matches Redis LRANGE/LTRIM semantics:
/// - negative indices offset from the tail (-1 is last element)
/// - `start` clamps to `[0, len-1]`
/// - `end` clamps up to `len-1` (but can stay negative if originally very negative)
pub fn normalize_range_indices(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let n = len as i64;
    // Normalize start: negatives offset from tail, clamp NEGATIVE results to 0.
    // Do NOT clamp positive starts to n-1 here — a start past the end must
    // produce an empty range (None), not a 1-element result.
    let start = if start < 0 { (start + n).max(0) } else { start };
    // Normalize end: negatives offset from tail, clamp positive values to n-1.
    let end = if stop < 0 { stop + n } else { stop.min(n - 1) };
    if start >= n || end < 0 || start > end {
        return None;
    }
    Some((start as usize, end as usize))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_list_end_uppercase() {
        assert_eq!(parse_list_end("LEFT").unwrap(), ListEnd::Left);
        assert_eq!(parse_list_end("RIGHT").unwrap(), ListEnd::Right);
    }

    #[test]
    fn parse_list_end_lowercase() {
        assert_eq!(parse_list_end("left").unwrap(), ListEnd::Left);
        assert_eq!(parse_list_end("right").unwrap(), ListEnd::Right);
    }

    #[test]
    fn parse_list_end_invalid() {
        assert!(parse_list_end("up").is_err());
        assert!(parse_list_end("").is_err());
    }

    #[test]
    fn parse_linsert_where_variants() {
        assert_eq!(
            parse_linsert_where("BEFORE").unwrap(),
            InsertPosition::Before
        );
        assert_eq!(
            parse_linsert_where("after").unwrap(),
            InsertPosition::After
        );
        assert!(parse_linsert_where("AROUND").is_err());
    }

    #[test]
    fn parse_lrem_count_sign() {
        assert_eq!(parse_lrem_count(3), LremDirection::Head(3));
        assert_eq!(parse_lrem_count(-2), LremDirection::Tail(2));
        assert_eq!(parse_lrem_count(0), LremDirection::All);
    }

    #[test]
    fn parse_lrem_count_i64_min_no_overflow() {
        // Regression: -i64::MIN overflows in i64; use unsigned_abs() instead.
        // This test runs under cargo test (debug profile, overflow-checks ON)
        // and proves we never compute -i64::MIN as i64.
        let result = parse_lrem_count(i64::MIN);
        assert_eq!(
            result,
            LremDirection::Tail(i64::MIN.unsigned_abs() as usize)
        );
        // Also verify the magnitude is the expected 2^63.
        if let LremDirection::Tail(n) = result {
            assert_eq!(n as u64, 9_223_372_036_854_775_808_u64);
        } else {
            panic!("expected LremDirection::Tail, got {:?}", result);
        }
    }

    #[test]
    fn normalize_range_indices_matrix() {
        // 9-case matrix from RESEARCH.md Pattern 2:
        assert_eq!(normalize_range_indices(0, -1, 5), Some((0, 4))); // all
        assert_eq!(normalize_range_indices(0, 100, 5), Some((0, 4))); // end clamps
        assert_eq!(normalize_range_indices(-100, 100, 5), Some((0, 4))); // both clamp
        assert_eq!(normalize_range_indices(-3, -1, 5), Some((2, 4))); // last three
        assert_eq!(normalize_range_indices(-3, 2, 5), Some((2, 2))); // one element
        assert_eq!(normalize_range_indices(5, 10, 5), None); // start past end
        assert_eq!(normalize_range_indices(3, 2, 5), None); // start > end
        assert_eq!(normalize_range_indices(-10, -6, 5), None); // end < 0 after
        assert_eq!(normalize_range_indices(0, 0, 0), None); // empty list
    }
}
