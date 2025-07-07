use std::fmt::{Display, Formatter};

use anyhow::anyhow;

pub(crate) fn str_to_bytes(byte_str: &str) -> anyhow::Result<Vec<u8>> {
    let clean = byte_str.replace(' ', "");
    if clean.len() % 2 != 0 {
        return Err(anyhow!("invalid hex string"));
    }
    let mut res = Vec::with_capacity(byte_str.len() / 2);
    for i in (0..clean.len()).step_by(2) {
        match u8::from_str_radix(&clean[i..i + 2], 16) {
            Ok(val) => {
                res.push(val);
                continue;
            }
            Err(e) => return Err(anyhow!(e)),
        }
    }
    Ok(res)
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

#[cfg(test)]
mod test {
    use crate::str_bytes::str_to_bytes;

    #[test]
    pub fn test() {
        let bytes = "FF 00 22FF ffa1";
        let bytes = str_to_bytes(bytes).unwrap();
        assert_eq!(vec![255, 0, 34, 255, 255, 161], bytes);
    }
}
