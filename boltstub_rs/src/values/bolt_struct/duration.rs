use std::cell::LazyCell;
use std::fmt::{Debug, Display};
use std::str::FromStr;

use regex::{Captures, Regex};

use crate::bolt_version::JoltVersion;
use crate::parse_error::ParseError;
use crate::util::opt_res_ret;
use crate::values::bolt_struct::_common::normalize_seconds_nanos;
use crate::values::bolt_struct::_parsing::{check_last_pack_stream_field, next_pack_stream_field};
use crate::values::bolt_struct::TAG_DURATION;
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

#[derive(Debug, Copy, Clone)]
pub(crate) struct JoltDuration {
    pub(crate) months: i64,
    pub(crate) days: i64,
    pub(crate) seconds: i64,
    pub(crate) nanos: i64,
}

impl JoltDuration {
    pub(crate) fn parse(s: &str) -> Option<Result<Self, ParseError>> {
        #[allow(
            clippy::unnecessary_wraps,
            reason = "Allows for using opt_res_ret => improved readability"
        )]
        fn i64_capture(
            i: usize,
            name: &str,
            captures: &Captures,
        ) -> Option<Result<i64, ParseError>> {
            Some(match captures.get(i) {
                None => Ok(0),
                Some(c) => i64::from_str(c.as_str()).map_err(|e| {
                    ParseError::new(format!(
                        "Failed to parse duration {name} {}: {e}",
                        c.as_str()
                    ))
                }),
            })
        }

        thread_local! {
            static DURATION_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(concat!(
                    r"^P(?:(-?\d+)Y)?(?:(-?\d+)M)?(?:(-?\d+)D)?",
                    r"(?:T(?:(-?\d+)H)?(?:(-?\d+)M)?(?:((-)?\d+|\d*)(?:\.(\d+))?S)?)?$"
                )).unwrap()
            });
        }
        let captures = DURATION_RE.with(|re| re.captures(s))?;

        let years = opt_res_ret!(i64_capture(1, "years", &captures));
        let months = opt_res_ret!(i64_capture(2, "months", &captures));
        let days = opt_res_ret!(i64_capture(3, "days", &captures));
        let hours = opt_res_ret!(i64_capture(4, "hours", &captures));
        let minutes = opt_res_ret!(i64_capture(5, "minutes", &captures));
        let seconds = opt_res_ret!(i64_capture(6, "seconds", &captures));
        let seconds_sign = captures.get(7).map_or(1, |_| -1);
        let mut nanos = opt_res_ret!(captures.get(8).map_or(Some(Ok(0)), |m| {
            let padded = format!("{:0<9}", m.as_str());
            if padded.len() > 9 {
                return Some(Err(ParseError::new(
                    "Duration has too many sub-seconds digits \
                        (only nanoseconds are allowed, up to 9 digits)",
                )));
            }
            Some(Ok(
                i64::from_str(&padded).expect("regex + length check enforce nanos to be i64")
            ))
        }));
        assert!(
            (0..=999_999_999).contains(&nanos),
            "regex enforces nanos to not overflow into seconds"
        );
        nanos *= seconds_sign;

        let Some(months) = years
            .checked_mul(12)
            .and_then(|years_as_months| months.checked_add(years_as_months))
        else {
            return Some(Err(ParseError::new(
                "Duration months (together with years) are overflowing",
            )));
        };
        let Some(seconds) = hours
            .checked_mul(60)
            .and_then(|hours_as_minutes| minutes.checked_add(hours_as_minutes))
            .and_then(|minutes| seconds.checked_add(minutes.checked_mul(60)?))
        else {
            return Some(Err(ParseError::new(
                "Duration seconds (together with hours and minutes) are overflowing",
            )));
        };
        Some(Ok(Self {
            months,
            days,
            seconds,
            nanos,
        }))
    }

    pub(crate) fn as_struct(&self) -> PackStreamStruct {
        PackStreamStruct {
            tag: TAG_DURATION,
            fields: vec![
                PackStreamValue::Integer(self.months),
                PackStreamValue::Integer(self.days),
                PackStreamValue::Integer(self.seconds),
                PackStreamValue::Integer(self.nanos),
            ],
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct BoltDuration(JoltDuration);

impl BoltDuration {
    pub(super) fn from_struct(s: &PackStreamStruct, _jolt_version: JoltVersion) -> Option<Self> {
        let PackStreamStruct { tag, fields } = s;
        if *tag != TAG_DURATION {
            return None;
        }
        let mut fields = fields.iter();
        let months = next_pack_stream_field(&mut fields)?;
        let days = next_pack_stream_field(&mut fields)?;
        let seconds = next_pack_stream_field(&mut fields)?;
        let nanos = next_pack_stream_field(&mut fields)?;
        let (seconds, nanos) = normalize_seconds_nanos(seconds, nanos)?;
        if !check_last_pack_stream_field(fields) {
            return None;
        }
        Some(Self(JoltDuration {
            months,
            days,
            seconds,
            nanos: nanos.into(),
        }))
    }

    pub(super) fn jolt_fmt(&self, _jolt_version: JoltVersion) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            this: &'a BoltDuration,
        }

        fn write_component(
            f: &mut std::fmt::Formatter<'_>,
            n: i64,
            designator: &'_ str,
        ) -> std::fmt::Result {
            if n == 0 {
                Ok(())
            } else {
                Display::fmt(&n, f)?;
                f.write_str(designator)
            }
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let BoltDuration(JoltDuration {
                    months,
                    days,
                    seconds,
                    mut nanos,
                }) = self.this;
                let (years, months) = (months / 12, months % 12);
                let (weeks, days) = (days / 7, days % 7);
                let (hours, seconds) = (seconds / 3600, seconds % 3600);
                let (minutes, mut seconds) = (seconds / 60, seconds % 60);
                if seconds < 0 {
                    seconds += 1;
                    nanos = 1_000_000_000 - nanos;
                }

                let has_time = hours != 0 || minutes != 0 || seconds != 0 || nanos != 0;
                let has_days = years != 0 || months != 0 || weeks != 0 || days != 0;

                f.write_str(r#"{"T": "P"#)?;
                write_component(f, years, "Y")?;
                write_component(f, months, "M")?;
                write_component(f, weeks, "W")?;
                write_component(f, days, "D")?;
                if has_time || !has_days {
                    f.write_str("T")?;
                    write_component(f, hours, "H")?;
                    write_component(f, minutes, "M")?;
                    if seconds != 0 || nanos != 0 || !has_time {
                        Display::fmt(&seconds, f)?;
                        if nanos != 0 {
                            f.write_str(".")?;
                            write!(f, "{nanos:09}")?;
                        }
                        f.write_str("S")?;
                    }
                }
                f.write_str(r#""}"#)
            }
        }

        JoltFormatter { this: self }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unreadable_literal, reason = "Not readable either way")]

    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("P12Y", Some(Ok((12 * 12, 0, 0, 0))))]
    #[case("P13M", Some(Ok((13, 0, 0, 0))))]
    #[case("P12345D", Some(Ok((0, 12345, 0, 0))))]
    #[case("PT1234H", Some(Ok((0, 0, 1234 * 60 * 60, 0))))]
    #[case("PT123456M", Some(Ok((0, 0, 123456 * 60, 0))))]
    #[case("PT12345678S", Some(Ok((0, 0, 12345678, 0))))]
    #[case("PT0.123456789S", Some(Ok((0, 0, 0, 123456789))))]
    #[case("PT0.01S", Some(Ok((0, 0, 0, 10_000_000))))]
    #[case("PT0.1000000S", Some(Ok((0, 0, 0, 100_000_000))))]
    #[case("P-12Y", Some(Ok((-12 * 12, 0, 0, 0))))]
    #[case("P-13M", Some(Ok((-13, 0, 0, 0))))]
    #[case("P-12345D", Some(Ok((0, -12345, 0, 0))))]
    #[case("PT-1234H", Some(Ok((0, 0, -1234 * 60 * 60, 0))))]
    #[case("PT-123456M", Some(Ok((0, 0, -123456 * 60, 0))))]
    #[case("PT-12345678S", Some(Ok((0, 0, -12345678, 0))))]
    #[case("PT-0.123456789S", Some(Ok((0, 0, 0, -123456789))))]
    #[case("PT-0.01S", Some(Ok((0, 0, 0, -10_000_000))))]
    #[case("PT-0.1000000S", Some(Ok((0, 0, 0, -100_000_000))))]
    #[case("PT", Some(Ok((0, 0, 0, 0))))]
    #[case("P", Some(Ok((0, 0, 0, 0))))]
    #[case("P12Y13M40DT10H70M80.1S", Some(Ok((12 * 12 + 13, 40, (10 * 60 + 70) * 60 + 80, 100_000_000))))]
    #[case("P12Y-13M40DT-10H70M-80.01234567S", Some(Ok((12 * 12 - 13, 40, (-10 * 60 + 70) * 60 - 80, -12345670))))]
    #[case("P12DT10H70M", Some(Ok((0, 12, (10 * 60 + 70) * 60, 0))))]
    #[case("P12Y13M40DT10H70M80.0000000000S", Some(Err(())))] // too many nanos digits
    #[case("P12Y13M40DT10H70.1M10S", None)] // invalid minutes format
    #[case("P5W", None)] // unsupported: weeks component
    #[case("P123", None)]
    #[case("PT123", None)]
    // years overflows months
    #[case("P768614336404564650Y", Some(Ok((768614336404564650 * 12, 0, 0, 0))))]
    #[case("P768614336404564651Y", Some(Err(())))]
    #[case("P-768614336404564650Y", Some(Ok((-768614336404564650 * 12, 0, 0, 0))))]
    #[case("P-768614336404564651Y", Some(Err(())))]
    // months overflow
    #[case("P9223372036854775807M", Some(Ok((9223372036854775807, 0, 0, 0))))]
    #[case("P9223372036854775808M", Some(Err(())))]
    #[case("P-9223372036854775808M", Some(Ok((-9223372036854775808, 0, 0, 0))))]
    #[case("P-9223372036854775809M", Some(Err(())))]
    // years * 12 + months overflows months
    #[case("P1Y9223372036854775795M", Some(Ok((9223372036854775807, 0, 0, 0))))]
    #[case("P1Y9223372036854775796M", Some(Err(())))]
    #[case("P2Y9223372036854775795M", Some(Err(())))]
    #[case("P-1Y-9223372036854775796M", Some(Ok((-9223372036854775808, 0, 0, 0))))]
    #[case("P-1Y-9223372036854775797M", Some(Err(())))]
    #[case("P-2Y-9223372036854775796M", Some(Err(())))]
    // days overflow
    #[case("P9223372036854775807D", Some(Ok((0, 9223372036854775807, 0, 0))))]
    #[case("P9223372036854775808D", Some(Err(())))]
    #[case("P-9223372036854775808D", Some(Ok((0, -9223372036854775808, 0, 0))))]
    #[case("P-9223372036854775809D", Some(Err(())))]
    // hours overflows seconds
    #[case("PT2562047788015215H", Some(Ok((0, 0, 2562047788015215 * 60 * 60, 0))))]
    #[case("PT2562047788015216H", Some(Err(())))]
    #[case("PT-2562047788015215H", Some(Ok((0, 0, -2562047788015215 * 60 * 60, 0))))]
    #[case("PT-2562047788015216H", Some(Err(())))]
    // minutes overflows seconds
    #[case("PT153722867280912930M", Some(Ok((0, 0, 153722867280912930 * 60, 0))))]
    #[case("PT153722867280912931M", Some(Err(())))]
    #[case("PT-153722867280912930M", Some(Ok((0, 0, -153722867280912930 * 60, 0))))]
    #[case("PT-153722867280912931M", Some(Err(())))]
    // seconds overflow
    #[case("PT9223372036854775807S", Some(Ok((0, 0, 9223372036854775807, 0))))]
    #[case("PT9223372036854775808S", Some(Err(())))]
    #[case("PT-9223372036854775808S", Some(Ok((0, 0, -9223372036854775808, 0))))]
    #[case("PT-9223372036854775809S", Some(Err(())))]
    // hours + minutes + seconds overflow seconds
    #[case("PT1H2M9223372036854772087S", Some(Ok((0, 0, 9223372036854775807, 0))))]
    #[case("PT2H2M9223372036854772087S", Some(Err(())))]
    #[case("PT1H3M9223372036854772087S", Some(Err(())))]
    #[case("PT1H2M9223372036854772088S", Some(Err(())))]
    #[case("PT-1H-2M-9223372036854772088S", Some(Ok((0, 0, -9223372036854775808, 0))))]
    #[case("PT-2H-2M-9223372036854772088S", Some(Err(())))]
    #[case("PT-1H-3M-9223372036854772088S", Some(Err(())))]
    #[case("PT-1H-2M-9223372036854772089S", Some(Err(())))]
    // nanoseconds overflow
    #[case("PT0.999999999S", Some(Ok((0, 0, 0, 999_999_999))))]
    #[case("PT-0.999999999S", Some(Ok((0, 0, 0, -999_999_999))))]
    #[case("PT0.1000000000S", Some(Err(())))]
    #[case("PT-0.1000000000S", Some(Err(())))]
    fn test_jolt_duration_parse(
        #[case] input: &str,
        #[case] expected: Option<Result<(i64, i64, i64, i64), ()>>,
    ) {
        let result = dbg!(JoltDuration::parse(dbg!(input)));
        match (result, expected) {
            (Some(Ok(parsed)), Some(Ok((months, days, seconds, nanos)))) => {
                assert_eq!(parsed.months, months);
                assert_eq!(parsed.days, days);
                assert_eq!(parsed.seconds, seconds);
                assert_eq!(parsed.nanos, nanos);
            }
            (None, None) | (Some(Err(_)), Some(Err(()))) => {}
            (result, expected) => panic!(
                "Unexpected result for input: {input}\nExpected: {expected:?}, Got: {result:?}"
            ),
        }
    }
}
