use std::fmt::{Display, Formatter};

use anyhow::anyhow;

/// Used for parsing hex strings in stubscript contexts.
/// E.g. `!: HANDSHAKE 00 00 FF 01`
///
/// This parser ignores all whitespace and expects the resulting string to be a valid hex string
/// of even length.
///
/// # Example
/// ```
/// let res = parse_stubscript_hex_string("  0 F 22 1234 Ab").unwrap();
/// assert_eq!(res, vec![0x0F, 0x22, 0x12, 0x34, 0xAB]);
/// ```
pub(crate) fn parse_stubscript_hex_string(byte_str: &str) -> anyhow::Result<Vec<u8>> {
    let clean = byte_str.replace(char::is_whitespace, "");
    if clean.len() % 2 != 0 {
        return Err(anyhow!("invalid hex string"));
    }
    let mut res = Vec::with_capacity(byte_str.len() / 2);
    for i in (0..clean.len()).step_by(2) {
        match u8::from_str_radix(&clean[i..i + 2], 16) {
            Ok(val) => {
                res.push(val);
            }
            Err(e) => return Err(anyhow!(e)),
        }
    }
    Ok(res)
}

/// Used for parsing hex strings inside JOLT contexts.
/// E.g. `!: MSG {"#": "00 00 FF 01"}`
///
/// This parse is whitespace sensitive:
///  * literals may not start with whitespace
///  * whitespaces is optional between bytes
///  * bytes may be represented by one or two hex characters
///
/// # Examples
/// ```
/// let res = parse_jolt_hex_string("0  F 12").unwrap();
/// assert_eq!(res, vec![0x0, 0xF, 0x12]);
/// ```
///
/// ```
/// assert!(parse_jolt_hex_string("1234").is_err());
/// ```
pub(crate) fn parse_jolt_hex_string(s: &str) -> anyhow::Result<Vec<u8>> {
    if s.is_empty() {
        return Ok(vec![]);
    }
    if s.chars()
        .next()
        .expect("checked for empty above")
        .is_whitespace()
    {
        return Err(anyhow!("Hex string may not start with whitespace"));
    }
    let mut result = Vec::with_capacity(s.len() / 2);
    let mut start_offset = None;
    let mut start_idx = None;
    for (i, (offset, ch)) in s.char_indices().enumerate() {
        if ch.is_whitespace() {
            if start_offset.is_none() {
                continue;
            }
            return Err(anyhow!(
                "Hex sting contains unexpected whitespace {ch} (at {i}): \
                whitespace may only occur between pairs of characters"
            ));
        }
        if !ch.is_ascii_hexdigit() {
            return Err(anyhow!(
                "Hex sting contains invalid hex character {ch} (at {i})"
            ));
        }
        let start_offset_val = *start_offset.get_or_insert(offset);
        let start_idx_val = *start_idx.get_or_insert(i);
        if i == start_idx_val + 1 {
            result.push(
                u8::from_str_radix(&s[start_offset_val..offset + ch.len_utf8()], 16)
                    .expect("checked for valid hex u8 above"),
            );
            start_offset = None;
            start_idx = None;
        }
    }
    if let Some(start_offset) = start_offset {
        return Err(anyhow!(
            "Hex string has non-paired trailing character(s) {:?}",
            &s[start_offset..]
        ));
    }
    Ok(result)
}

pub(crate) fn fmt_bytes(bytes: &[u8]) -> impl Display + '_ {
    struct BytesDisplay<'a>(&'a [u8]);

    impl Display for BytesDisplay<'_> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let mut bytes = self.0.iter();
            match bytes.next() {
                None => {}
                Some(first) => {
                    write!(f, "{first:02X}")?;
                    for u in bytes {
                        write!(f, " {u:02X}")?;
                    }
                }
            }
            Ok(())
        }
    }

    BytesDisplay(bytes)
}

pub(crate) fn fmt_bytes_compact(bytes: &[u8]) -> impl Display + '_ {
    struct BytesDisplay<'a>(&'a [u8]);

    impl Display for BytesDisplay<'_> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let mut bytes = self.0.iter();
            match bytes.next() {
                None => {}
                Some(first) => {
                    write!(f, "0x{first:02X}")?;
                    for u in bytes {
                        write!(f, "{u:02X}")?;
                    }
                }
            }
            Ok(())
        }
    }

    BytesDisplay(bytes)
}

pub(crate) fn serialize_fmt_bytes(bytes: &[u8]) -> impl serde::Serialize + '_ {
    struct BytesDisplay<'a>(&'a [u8]);

    impl serde::Serialize for BytesDisplay<'_> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            serializer.collect_str(&fmt_bytes(self.0))
        }
    }

    BytesDisplay(bytes)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[test]
    fn test_parse_stubscript_hex_string() {
        let res = parse_stubscript_hex_string("  0 F 22 1234 Ab 01 \t ").unwrap();
        assert_eq!(res, vec![0x0F, 0x22, 0x12, 0x34, 0xAB, 0x01]);
    }

    #[rstest]
    #[case::empty("", vec![])]
    #[case::data("0F 12 aB 01 aBCd\t ", vec![0x0F, 0x12, 0xAB, 0x01, 0xAB, 0xCD])]
    fn test_parse_jolt_hex_string(#[case] input: &str, #[case] expected: Vec<u8>) {
        let res = parse_jolt_hex_string(input).unwrap();
        assert_eq!(res, expected);
    }

    #[rstest]
    #[case::leading_space1(" ")]
    #[case::leading_space2(" FF")]
    #[case::invalid_char("0x")]
    #[case::non_paired_nibbles("F F")]
    fn test_parse_jolt_hex_string_invalid_inputs(#[case] input: &str) {
        parse_jolt_hex_string(input).unwrap_err();
    }

    #[test]
    fn test_fmt_bytes() {
        let bytes = vec![0x0F, 0x22, 0x12, 0x34, 0xAB, 0x01, 0x00];
        let formatted = fmt_bytes(&bytes).to_string();
        assert_eq!(formatted, "0F 22 12 34 AB 01 00");
    }

    #[test]
    fn test_fmt_bytes_compact() {
        let bytes = vec![0x00, 0x0F, 0x22, 0x12, 0x34, 0xAB, 0x01, 0x00];
        let formatted = fmt_bytes_compact(&bytes).to_string();
        assert_eq!(formatted, "0x000F221234AB0100");
    }
}
