use crate::bolt_version::JoltVersion;
pub(super) mod element_id;

pub(super) fn normalize_seconds_nanos(second: i64, nanos: i64) -> Option<(i64, u32)> {
    let mut seconds = second.checked_add(nanos / 1_000_000_000)?;
    let mut nanos = nanos % 1_000_000_000;
    if nanos < 0 {
        seconds = seconds.checked_sub(1)?;
        nanos += 1_000_000_000;
    }
    Some((seconds, nanos.try_into().unwrap()))
}

#[inline]
pub(super) fn fmt_jolt_sigil_and_map<'a>(
    f: &mut std::fmt::Formatter<'a>,
    sigil: &'_ str,
    jolt_version_inner: JoltVersion,
    jolt_version_context: JoltVersion,
    inner_wrap: Option<(&'_ str, &'_ str)>,
    inner: impl FnOnce(&mut std::fmt::Formatter<'a>) -> std::fmt::Result,
) -> std::fmt::Result {
    let with_postfix = jolt_version_inner != jolt_version_context;

    f.write_str("{\"")?;
    f.write_str(sigil)?;
    if with_postfix {
        f.write_str("v")?;
        f.write_str(jolt_version_inner.to_str())?;
    }
    f.write_str("\": ")?;
    if let Some((inner_prefix, _)) = inner_wrap {
        f.write_str(inner_prefix)?;
    }
    inner(f)?;
    if let Some((_, inner_suffix)) = inner_wrap {
        f.write_str(inner_suffix)?;
    }
    f.write_str("}")
}

#[cfg(test)]
mod test {
    use std::borrow::Cow;

    use super::*;

    use rstest::rstest;

    #[test]
    fn test_normalize_seconds_nanos() {
        assert_eq!(normalize_seconds_nanos(0, 0), Some((0, 0)));
        assert_eq!(normalize_seconds_nanos(0, 1_000_000_000), Some((1, 0)));
        assert_eq!(normalize_seconds_nanos(0, -1_000_000_000), Some((-1, 0)));
        assert_eq!(normalize_seconds_nanos(0, 1_000_000_001), Some((1, 1)));
        assert_eq!(
            normalize_seconds_nanos(0, -1_000_000_001),
            Some((-2, 999_999_999))
        );

        assert_eq!(normalize_seconds_nanos(1, 0), Some((1, 0)));
        assert_eq!(normalize_seconds_nanos(1, 1_000_000_000), Some((2, 0)));
        assert_eq!(normalize_seconds_nanos(1, -1_000_000_000), Some((0, 0)));
        assert_eq!(normalize_seconds_nanos(1, 1_000_000_001), Some((2, 1)));
        assert_eq!(
            normalize_seconds_nanos(1, -1_000_000_001),
            Some((-1, 999_999_999))
        );

        assert_eq!(normalize_seconds_nanos(-1, 0), Some((-1, 0)));
        assert_eq!(normalize_seconds_nanos(-1, 1_000_000_000), Some((0, 0)));
        assert_eq!(normalize_seconds_nanos(-1, -1_000_000_000), Some((-2, 0)));
        assert_eq!(normalize_seconds_nanos(-1, 1_000_000_001), Some((0, 1)));
        assert_eq!(
            normalize_seconds_nanos(-1, -1_000_000_001),
            Some((-3, 999_999_999))
        );

        assert_eq!(normalize_seconds_nanos(i64::MAX, 0), Some((i64::MAX, 0)));
        assert_eq!(normalize_seconds_nanos(i64::MAX, 1_000_000_000), None);
        assert_eq!(
            normalize_seconds_nanos(i64::MAX, -1_000_000_000),
            Some((i64::MAX - 1, 0))
        );
        assert_eq!(normalize_seconds_nanos(i64::MAX, 1_000_000_001), None);
        assert_eq!(
            normalize_seconds_nanos(i64::MAX, -1_000_000_001),
            Some((i64::MAX - 2, 999_999_999))
        );

        assert_eq!(normalize_seconds_nanos(i64::MIN, 0), Some((i64::MIN, 0)));
        assert_eq!(
            normalize_seconds_nanos(i64::MIN, 1_000_000_000),
            Some((i64::MIN + 1, 0))
        );
        assert_eq!(normalize_seconds_nanos(i64::MIN, -1_000_000_000), None);
        assert_eq!(
            normalize_seconds_nanos(i64::MIN, 1_000_000_001),
            Some((i64::MIN + 1, 1))
        );
        assert_eq!(normalize_seconds_nanos(i64::MIN, -1_000_000_001), None);
    }

    #[rstest]
    #[case(JoltVersion::V1, JoltVersion::V1, "")]
    fn test_fmt_jolt_sigil_and_map(
        #[values("", "SigIL💯", "?", "T")] sigil: &'static str,
        #[case] jolt_version_inner: JoltVersion,
        #[case] jolt_version_context: JoltVersion,
        #[case] version_suffix: &'static str,
        #[values(None, Some(("", "")), Some(("{]", "\"}")))] inner_wrap: Option<(
            &'static str,
            &'static str,
        )>,
    ) {
        const INNER: &str = "inner💅\"";

        struct Formattable {
            sigil: &'static str,
            jolt_version_inner: JoltVersion,
            jolt_version_context: JoltVersion,
            inner_wrap: Option<(&'static str, &'static str)>,
        }
        impl std::fmt::Display for Formattable {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                fmt_jolt_sigil_and_map(
                    f,
                    self.sigil,
                    self.jolt_version_inner,
                    self.jolt_version_context,
                    self.inner_wrap,
                    |f| f.write_str(INNER),
                )
            }
        }

        let body: Cow<str> = match inner_wrap {
            Some((open, close)) => format!("{open}{INNER}{close}").into(),
            None => INNER.into(),
        };
        let expected = format!("{{\"{sigil}{version_suffix}\": {body}}}");
        let output = format!(
            "{}",
            Formattable {
                sigil,
                jolt_version_inner,
                jolt_version_context,
                inner_wrap
            }
        );

        assert_eq!(output, expected);
    }
}
