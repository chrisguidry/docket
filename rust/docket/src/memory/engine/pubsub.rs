/// Redis-style glob pattern matching for PSUBSCRIBE patterns.
/// Supports: * (any string), ? (one char), [abc] (char class), [^abc] (negated), \ (escape)
///
/// Uses iterative matching with backtracking (star_pi/star_si variables) instead of
/// recursion to prevent stack overflow on adversarial patterns (T-10-01 mitigation).
/// Complexity is O(m*n) where m = pattern length, n = string length.
pub fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut pi = 0usize; // pattern index
    let mut si = 0usize; // string index
    let mut star_pi: Option<usize> = None; // pattern index after last '*'
    let mut star_si: usize = 0; // string index when we matched last '*'

    while si < string.len() {
        if pi < pattern.len() && pattern[pi] == b'\\' {
            // Escape: next pattern byte must match literally
            pi += 1;
            if pi < pattern.len() && string[si] == pattern[pi] {
                pi += 1;
                si += 1;
                continue;
            }
            // Escape at end of pattern or mismatch -- try backtrack
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            // Star: skip consecutive stars, record backtrack point
            while pi < pattern.len() && pattern[pi] == b'*' {
                pi += 1;
            }
            // If star is at end of pattern, it matches everything remaining
            if pi == pattern.len() {
                return true;
            }
            star_pi = Some(pi);
            star_si = si;
            continue;
        } else if pi < pattern.len() && pattern[pi] == b'?' {
            // Question mark: match exactly one byte
            pi += 1;
            si += 1;
            continue;
        } else if pi < pattern.len() && pattern[pi] == b'[' {
            // Character class: [abc], [^abc], [!abc]
            pi += 1; // skip '['
            let negated = if pi < pattern.len() && (pattern[pi] == b'^' || pattern[pi] == b'!') {
                pi += 1;
                true
            } else {
                false
            };

            let mut found = false;
            let mut class_end = false;
            while pi < pattern.len() && pattern[pi] != b']' {
                // Check for range: a-z (only if hyphen is NOT at start or end of class)
                if pi + 2 < pattern.len() && pattern[pi + 1] == b'-' && pattern[pi + 2] != b']' {
                    let range_start = pattern[pi];
                    let range_end = pattern[pi + 2];
                    // Only match if range is valid (start <= end), otherwise treat as no match
                    if range_start <= range_end && string[si] >= range_start && string[si] <= range_end {
                        found = true;
                    }
                    pi += 3; // skip char, '-', char
                } else {
                    if string[si] == pattern[pi] {
                        found = true;
                    }
                    pi += 1;
                }
            }

            if pi < pattern.len() && pattern[pi] == b']' {
                class_end = true;
                pi += 1; // skip ']'
            }

            if !class_end {
                // Unterminated '[' -- treat as literal mismatch
                // Fall through to backtrack
            } else if (found && !negated) || (!found && negated) {
                si += 1;
                continue;
            }
            // Class didn't match -- fall through to backtrack
        } else if pi < pattern.len() && pattern[pi] == string[si] {
            // Literal match
            pi += 1;
            si += 1;
            continue;
        }

        // No match at current position -- try backtracking to last '*'
        if let Some(sp) = star_pi {
            pi = sp;
            star_si += 1;
            si = star_si;
        } else {
            return false;
        }
    }

    // String exhausted -- skip any trailing '*' in pattern
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Star wildcard --
    #[test]
    fn test_star_matches_everything() {
        assert!(glob_match(b"*", b"hello"));
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"*", b"foo.bar.baz"));
    }

    #[test]
    fn test_star_prefix() {
        assert!(glob_match(b"hello*", b"hello"));
        assert!(glob_match(b"hello*", b"helloworld"));
        assert!(!glob_match(b"hello*", b"hell"));
    }

    #[test]
    fn test_star_suffix() {
        assert!(glob_match(b"*world", b"world"));
        assert!(glob_match(b"*world", b"helloworld"));
        assert!(!glob_match(b"*world", b"worlds"));
    }

    #[test]
    fn test_star_middle() {
        assert!(glob_match(b"h*d", b"hd"));
        assert!(glob_match(b"h*d", b"held"));
        assert!(!glob_match(b"h*d", b"hello"));
    }

    #[test]
    fn test_multiple_stars() {
        assert!(glob_match(b"*.*", b"foo.bar"));
        assert!(glob_match(b"*.*.*", b"a.b.c"));
        assert!(!glob_match(b"*.*", b"foobar"));
    }

    // -- Question mark --
    #[test]
    fn test_question_mark_single_char() {
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h?llo", b"hallo"));
        assert!(!glob_match(b"h?llo", b"hllo"));
        assert!(!glob_match(b"h?llo", b"heello"));
    }

    #[test]
    fn test_question_mark_at_end() {
        assert!(glob_match(b"hell?", b"hello"));
        assert!(!glob_match(b"hell?", b"hell"));
    }

    // -- Character classes --
    #[test]
    fn test_char_class_basic() {
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[ae]llo", b"hillo"));
    }

    #[test]
    fn test_char_class_negated_caret() {
        assert!(glob_match(b"h[^ae]llo", b"hillo"));
        assert!(!glob_match(b"h[^ae]llo", b"hello"));
        assert!(!glob_match(b"h[^ae]llo", b"hallo"));
    }

    #[test]
    fn test_char_class_negated_bang() {
        assert!(glob_match(b"h[!ae]llo", b"hillo"));
        assert!(!glob_match(b"h[!ae]llo", b"hello"));
    }

    // -- Backslash escape --
    #[test]
    fn test_escape_star() {
        assert!(glob_match(b"hello\\*", b"hello*"));
        assert!(!glob_match(b"hello\\*", b"helloworld"));
    }

    #[test]
    fn test_escape_question() {
        assert!(glob_match(b"hello\\?", b"hello?"));
        assert!(!glob_match(b"hello\\?", b"hellox"));
    }

    #[test]
    fn test_escape_backslash() {
        assert!(glob_match(b"hello\\\\", b"hello\\"));
    }

    // -- Empty patterns and strings --
    #[test]
    fn test_empty_pattern_empty_string() {
        assert!(glob_match(b"", b""));
    }

    #[test]
    fn test_empty_pattern_nonempty_string() {
        assert!(!glob_match(b"", b"hello"));
    }

    #[test]
    fn test_nonempty_pattern_empty_string() {
        assert!(!glob_match(b"hello", b""));
        assert!(glob_match(b"*", b""));
    }

    // -- Exact match --
    #[test]
    fn test_exact_match() {
        assert!(glob_match(b"hello", b"hello"));
        assert!(!glob_match(b"hello", b"world"));
        assert!(!glob_match(b"hello", b"hell"));
        assert!(!glob_match(b"hello", b"helloo"));
    }

    // -- Redis pub/sub realistic patterns --
    #[test]
    fn test_redis_channel_patterns() {
        assert!(glob_match(b"__keyevent@*__:*", b"__keyevent@0__:expired"));
        assert!(glob_match(b"__keyspace@*__:*", b"__keyspace@0__:mykey"));
        assert!(glob_match(b"channel.*", b"channel.foo"));
        assert!(glob_match(b"channel.*", b"channel.bar"));
        assert!(!glob_match(b"channel.*", b"other.foo"));
    }

    // -- ReDoS-style input (T-10-01 mitigation) --
    // This test verifies the iterative implementation completes quickly.
    // A recursive implementation would stack overflow or take exponential time.
    #[test]
    fn test_redos_resistance() {
        // Pattern with many stars, long non-matching string
        let pattern = b"*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b";
        let string = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaac";
        // Should return false quickly (not hang or overflow)
        assert!(!glob_match(pattern, string));
    }

    #[test]
    fn test_redos_long_star_sequence() {
        // Many consecutive stars should behave like a single star
        let pattern = b"****************************";
        assert!(glob_match(pattern, b"anything"));
        assert!(glob_match(pattern, b""));
    }

    // -- Character class ranges (D-04) --
    #[test]
    fn test_char_class_range_lowercase() {
        assert!(glob_match(b"h[a-z]llo", b"hello"));
        assert!(glob_match(b"h[a-z]llo", b"hallo"));
        assert!(!glob_match(b"h[a-z]llo", b"hAllo")); // A is outside a-z
    }

    #[test]
    fn test_char_class_range_digits() {
        assert!(glob_match(b"[0-9]abc", b"5abc"));
        assert!(!glob_match(b"[0-9]abc", b"xabc"));
    }

    #[test]
    fn test_char_class_reversed_range() {
        // Reversed range [z-a] should match nothing (empty range)
        assert!(!glob_match(b"[z-a]llo", b"ello"));
        assert!(!glob_match(b"[z-a]llo", b"mllo"));
    }

    #[test]
    fn test_char_class_leading_hyphen() {
        // Leading hyphen is literal
        assert!(glob_match(b"[-abc]def", b"-def"));
        assert!(glob_match(b"[-abc]def", b"adef"));
    }

    #[test]
    fn test_char_class_trailing_hyphen() {
        // Trailing hyphen is literal
        assert!(glob_match(b"[abc-]def", b"-def"));
        assert!(glob_match(b"[abc-]def", b"bdef"));
    }
}
