use std::cell::LazyCell;
use std::fmt::{Debug, Display};
use std::str::FromStr;

use regex::{Captures, Regex};

use crate::bolt_version::JoltVersion;
use crate::jolt::JoltSigil;
use crate::parse_error::ParseError;
use crate::util::opt_res_ret;
use crate::values::bolt_struct::_common::{fmt_jolt_sigil_and_map, normalize_seconds_nanos};
use crate::values::bolt_struct::_parsing::{check_last_pack_stream_field, next_pack_stream_field};
use crate::values::bolt_struct::TAG_DURATION;
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

const SIGIL: &str = JoltSigil::Temporal.str();

#[derive(Debug, Copy, Clone)]
pub(crate) struct JoltDurationData {
    pub(crate) months: i64,
    pub(crate) days: i64,
    pub(crate) seconds: i64,
    pub(crate) nanos: i64,
}

impl JoltDurationData {
    pub(crate) fn parse(s: &str, _jolt_version: JoltVersion) -> Option<Result<Self, ParseError>> {
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
                    "Duration has too many sub-second digits \
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

    pub(super) fn jolt_fmt(
        &self,
        jolt_version_data: JoltVersion,
        jolt_version_ctx: JoltVersion,
    ) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            data: &'a JoltDurationData,
            jolt_version_data: JoltVersion,
            jolt_version_ctx: JoltVersion,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                fmt_jolt_sigil_and_map(
                    f,
                    SIGIL,
                    self.jolt_version_data,
                    self.jolt_version_ctx,
                    Some(("\"", "\"")),
                    |f| self.data.repr(f),
                )
            }
        }

        JoltFormatter {
            data: self,
            jolt_version_data,
            jolt_version_ctx,
        }
    }

    fn repr(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn write_component(
            f: &mut std::fmt::Formatter<'_>,
            n: impl Display + Default + PartialEq + Copy,
            designator: &'_ str,
        ) -> std::fmt::Result {
            if n == Default::default() {
                Ok(())
            } else {
                Display::fmt(&n, f)?;
                f.write_str(designator)
            }
        }

        let Self {
            months,
            days,
            seconds,
            nanos,
        } = *self;
        let (years, months) = (months / 12, months % 12);
        let total_ns: i128 = i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
        let seconds_sign = if total_ns < 0 { "-" } else { "" };
        let (seconds, nanos): (i128, i64) = (
            total_ns / 1_000_000_000,
            (total_ns % 1_000_000_000)
                .try_into()
                .expect("±999_999_999 fits into i64"),
        );
        let (hours, seconds) = (seconds / 3600, seconds % 3600);
        let (minutes, seconds) = (seconds / 60, seconds % 60);

        let has_time = hours != 0 || minutes != 0 || seconds != 0 || nanos != 0;
        let has_days = years != 0 || months != 0 || days != 0;

        f.write_str("P")?;
        write_component(f, years, "Y")?;
        write_component(f, months, "M")?;
        write_component(f, days, "D")?;
        if has_time || !has_days {
            f.write_str("T")?;
            write_component(f, hours, "H")?;
            write_component(f, minutes, "M")?;
            if seconds != 0 || nanos != 0 || !has_time {
                f.write_str(seconds_sign)?;
                Display::fmt(&seconds.abs(), f)?;
                if nanos != 0 {
                    f.write_str(".")?;
                    let nanos_str = format!("{:09}", nanos.abs());
                    Display::fmt(nanos_str.trim_end_matches('0'), f)?;
                }
                f.write_str("S")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct JoltDuration {
    pub(crate) data: JoltDurationData,
    pub(super) jolt_version: JoltVersion,
}

impl JoltDuration {
    pub(crate) fn parse(s: &str, jolt_version: JoltVersion) -> Option<Result<Self, ParseError>> {
        JoltDurationData::parse(s, jolt_version)
            .map(|res| res.map(|data| Self { data, jolt_version }))
    }

    pub(crate) fn as_struct(&self) -> PackStreamStruct {
        PackStreamStruct {
            tag: TAG_DURATION,
            fields: vec![
                PackStreamValue::Integer(self.data.months),
                PackStreamValue::Integer(self.data.days),
                PackStreamValue::Integer(self.data.seconds),
                PackStreamValue::Integer(self.data.nanos),
            ],
        }
    }

    pub(super) fn from_struct(s: &PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
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
        let data = JoltDurationData {
            months,
            days,
            seconds,
            nanos: nanos.into(),
        };
        Some(Self { data, jolt_version })
    }

    pub(super) fn jolt_fmt(&self, jolt_version: JoltVersion) -> impl Display + '_ {
        self.data.jolt_fmt(self.jolt_version, jolt_version)
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
        let result = dbg!(JoltDuration::parse(dbg!(input), JoltVersion::V1));
        match (result, expected) {
            (Some(Ok(parsed)), Some(Ok((months, days, seconds, nanos)))) => {
                assert_eq!(parsed.data.months, months);
                assert_eq!(parsed.data.days, days);
                assert_eq!(parsed.data.seconds, seconds);
                assert_eq!(parsed.data.nanos, nanos);
            }
            (None, None) | (Some(Err(_)), Some(Err(()))) => {}
            (result, expected) => panic!(
                "Unexpected result for input: {input}\nExpected: {expected:?}, Got: {result:?}"
            ),
        }
    }

    #[rstest]
    #[case("P12Y", (12 * 12, 0, 0, 0))]
    #[case("P1Y1M", (13, 0, 0, 0))]
    #[case("P12345D", (0, 12345, 0, 0))]
    #[case("PT1234H", (0, 0, 1234 * 60 * 60, 0))]
    #[case("PT12H34M56S", (0, 0, (12 * 60 + 34) * 60 + 56, 0))]
    #[case("PT0.123456789S", (0, 0, 0, 123_456_789))]
    #[case("PT0.01S", (0, 0, 0, 10_000_000))]
    #[case("PT0.1S", (0, 0, 0, 100_000_000))]
    #[case("P-12Y", (-12 * 12, 0, 0, 0))]
    #[case("P-1Y-1M", (-13, 0, 0, 0))]
    #[case("P-12345D", (0, -12345, 0, 0))]
    #[case("PT-1234H", (0, 0, -1234 * 60 * 60, 0))]
    #[case("PT-1234H-56M", (0, 0, -(1234 * 60 + 56) * 60, 0))]
    #[case("PT-0.123456789S", (0, 0, 0, -123_456_789))]
    #[case("PT-0.01S", (0, 0, 0, -10_000_000))]
    #[case("PT-0.1S", (0, 0, 0, -100_000_000))]
    #[case("PT0S", (0, 0, 0, 0))]
    #[case("PT1.123S", (0, 0, 1, 123_000_000))]
    #[case("PT0.123S", (0, 0, 0, 123_000_000))]
    #[case("PT-1.123S", (0, 0, -1, -123_000_000))]
    #[case("PT-0.123S", (0, 0, 0, -123_000_000))]
    #[case("PT0.999999999S", (0, 0, 0, 999_999_999))]
    #[case("PT-0.999999999S", (0, 0, 0, -999_999_999))]
    #[case("PT-0.000000001S", (0, 0, -1, 999_999_999))]
    #[case("PT0.000000001S", (0, 0, 1, -999_999_999))]
    #[case("P768614336404564650Y7M", (i64::MAX, 0, 0, 0))]
    #[case("P-768614336404564650Y-8M", (i64::MIN, 0, 0, 0))]
    #[case("P9223372036854775807D", (0, i64::MAX, 0, 0))]
    #[case("P-9223372036854775808D", (0, i64::MIN, 0, 0))]
    #[case("PT2562047788015215H30M7S", (0, 0, i64::MAX, 0))]
    #[case("PT-2562047788015215H-30M-8S", (0, 0, i64::MIN, 0))]
    #[case("PT2562047H47M16.854775807S", (0, 0, 0, i64::MAX))]
    #[case("PT-2562047H-47M-16.854775808S", (0, 0, 0, i64::MIN))]
    #[case("PT2562047790577263H17M23.854775807S", (0, 0, i64::MAX, i64::MAX))]
    #[case("PT-2562047790577263H-17M-24.854775808S", (0, 0, i64::MIN, i64::MIN))]
    #[case("PT2562047785453167H42M50.145224192S", (0, 0, i64::MAX, i64::MIN))]
    #[case("PT-2562047785453167H-42M-51.145224193S", (0, 0, i64::MIN, i64::MAX))]
    fn test_jolt_duration_fmt(#[case] expected: &str, #[case] components: (i64, i64, i64, i64)) {
        const JOLT_VERSION: JoltVersion = JoltVersion::V1;
        let duration = JoltDuration {
            data: JoltDurationData {
                months: components.0,
                days: components.1,
                seconds: components.2,
                nanos: components.3,
            },
            jolt_version: JOLT_VERSION,
        };
        let result = dbg!(dbg!(duration).jolt_fmt(JOLT_VERSION).to_string());
        let expected = format!(r#"{{"T": "{expected}"}}"#);
        assert_eq!(result, expected);
    }
}
