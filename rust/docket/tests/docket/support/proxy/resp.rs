//! Finding where a RESP command or reply ends in a buffer.

/// The length of the RESP2 command at the start of `buffer` and its
/// arguments, once it has fully arrived.
pub(super) fn parse_command(buffer: &[u8]) -> Option<(usize, Vec<Vec<u8>>)> {
    let (count, mut at) = line(buffer, 0, b'*')?;
    let mut args = Vec::new();
    for _ in 0..count {
        let (length, start) = line(buffer, at, b'$')?;
        let end = start + usize::try_from(length).ok()?;
        if buffer.len() < end + 2 {
            return None;
        }
        args.push(buffer[start..end].to_vec());
        at = end + 2;
    }
    Some((at, args))
}

/// Reads `<prefix><number>\r\n` at `at`, and returns the number and where
/// the next line starts.
fn line(buffer: &[u8], at: usize, prefix: u8) -> Option<(i64, usize)> {
    if *buffer.get(at)? != prefix {
        return None;
    }
    let end = at + buffer[at..].windows(2).position(|pair| pair == b"\r\n")?;
    let number = std::str::from_utf8(&buffer[at + 1..end])
        .ok()?
        .parse()
        .ok()?;
    Some((number, end + 2))
}

/// Where the RESP2 or RESP3 reply that starts at `at` ends, once it has
/// fully arrived.
pub(super) fn reply_end(buffer: &[u8], at: usize) -> Option<usize> {
    let end = at
        + buffer
            .get(at..)?
            .windows(2)
            .position(|pair| pair == b"\r\n")?;
    let header = &buffer[at + 1..end];
    let next = end + 2;
    let elements = |count: i64, mut position: usize| {
        for _ in 0..count.max(0) {
            position = reply_end(buffer, position)?;
        }
        Some(position)
    };
    let count = || -> Option<i64> { std::str::from_utf8(header).ok()?.parse().ok() };
    match buffer[at] {
        b'+' | b'-' | b':' | b'_' | b',' | b'#' | b'(' => Some(next),
        b'$' | b'!' | b'=' => {
            let length: i64 = std::str::from_utf8(header).ok()?.parse().ok()?;
            if length < 0 {
                return Some(next);
            }
            let finish = next + usize::try_from(length).ok()? + 2;
            (buffer.len() >= finish).then_some(finish)
        }
        b'*' | b'~' | b'>' => elements(count()?, next),
        b'%' => elements(count()? * 2, next),
        // Attributes come ahead of the reply they describe.
        b'|' => reply_end(buffer, elements(count()? * 2, next)?),
        _ => None,
    }
}
