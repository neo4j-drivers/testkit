use std::cell::LazyCell;
use std::fmt::Debug;
use std::str::FromStr;

use chrono::{Duration, NaiveDate};
use regex::Regex;
use serde::Serialize;

use crate::bolt_version::JoltVersion;
use crate::jolt::JoltSigil;
use crate::parse_error::ParseError;
use crate::values::bolt_struct::_parsing::{check_last_pack_stream_field, next_pack_stream_field};
use crate::values::bolt_struct::TAG_DATE;
use crate::values::jolt_ser::JoltSigilMapSer;
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

const UNIX_EPOCH_DATE: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
const SIGIL: &str = JoltSigil::Temporal.str();

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct JoltDateData {
    pub(crate) date: NaiveDate,
}

impl JoltDateData {
    pub(super) fn parse(s: &str, _jolt_version: JoltVersion) -> Option<Result<Self, ParseError>> {
        thread_local! {
            static DATE_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(r"^(\d{4}|[+-]\d{5,9})(?:-(\d{2}))?(?:-(\d{2}))?$").unwrap()
            });
        }
        let captures = DATE_RE.with(|re| re.captures(s))?;
        let year = i32::from_str(
            captures
                .get(1)
                .expect("regex enforces existence of year")
                .as_str(),
        )
        .expect("regex enforces year to be i32");
        let month = captures.get(2).map_or(1, |m| {
            u32::from_str(m.as_str()).expect("regex enforces month to be u32")
        });
        let day = captures.get(3).map_or(1, |d| {
            u32::from_str(d.as_str()).expect("regex enforces day to be u32")
        });
        let Some(date) = NaiveDate::from_ymd_opt(year, month, day) else {
            return Some(Err(ParseError::new(format!("Unparsable date {s:?}"))));
        };
        Some(Ok(Self { date }))
    }

    pub(super) fn jolt_serialize<S>(
        self,
        serializer: S,
        jolt_version_data: JoltVersion,
        jolt_version_ctx: JoltVersion,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        struct DataSer(JoltDateData);

        impl serde::Serialize for DataSer {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.collect_str(&self.0.repr())
            }
        }

        let body = DataSer(self);
        let map = JoltSigilMapSer::new(SIGIL, jolt_version_data, jolt_version_ctx, &body);
        map.serialize(serializer)
    }

    pub(super) fn repr(&self) -> impl std::fmt::Display + '_ {
        self.date.format("%Y-%m-%d")
    }
}

#[derive(Debug, Copy, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct JoltDate {
    pub(crate) data: JoltDateData,
    pub(super) jolt_version: JoltVersion,
}

impl JoltDate {
    pub(crate) fn parse(s: &str, jolt_version: JoltVersion) -> Option<Result<Self, ParseError>> {
        JoltDateData::parse(s, jolt_version).map(|res| res.map(|data| Self { data, jolt_version }))
    }

    pub(crate) fn as_struct(self) -> PackStreamStruct {
        let days_since_epoch = self.data.date - UNIX_EPOCH_DATE;
        PackStreamStruct {
            tag: TAG_DATE,
            fields: vec![PackStreamValue::Integer(days_since_epoch.num_days())],
        }
    }

    #[expect(clippy::similar_names, reason = "can't come up with better naming")]
    pub(super) fn from_struct(s: &PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        if s.tag != TAG_DATE {
            return None;
        }
        let mut fields = s.fields.iter();
        let days_since_epoch: i64 = next_pack_stream_field(&mut fields)?;
        let date = UNIX_EPOCH_DATE + Duration::days(days_since_epoch);
        if !check_last_pack_stream_field(&mut fields) {
            return None;
        }

        let data = JoltDateData { date };

        Some(Self { data, jolt_version })
    }

    pub(super) fn jolt_serialize<S>(
        self,
        serializer: S,
        jolt_version_ctx: JoltVersion,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.data
            .jolt_serialize(serializer, self.jolt_version, jolt_version_ctx)
    }
}

#[cfg(test)]
mod tests {
    use crate::ext::serde_json::support_pub::JoltSerializer;
    use crate::values::tests::{all_jolt_versions, jolt_serializer};

    use super::*;

    use rstest::rstest;
    use rstest_reuse::apply;

    #[apply(all_jolt_versions)]
    #[case("0000-01-01", (0, 1, 1))]
    #[case("0000-01", (0, 1, 1))]
    #[case("0000", (0, 1, 1))]
    #[case("1234", (1234, 1, 1))]
    #[case("1970-01-01", (1970, 1, 1))]
    #[case("2026-05-19", (2026, 5, 19))]
    #[case("2024-01-01", (2024, 1, 1))]
    #[case("2024-01-31", (2024, 1, 31))]
    #[case("2024-02-01", (2024, 2, 1))]
    #[case("2024-02-29", (2024, 2, 29))]
    #[case("2024-03-01", (2024, 3, 1))]
    #[case("2024-03-31", (2024, 3, 31))]
    #[case("2024-04-01", (2024, 4, 1))]
    #[case("2024-04-30", (2024, 4, 30))]
    #[case("2024-05-01", (2024, 5, 1))]
    #[case("2024-05-31", (2024, 5, 31))]
    #[case("2024-06-01", (2024, 6, 1))]
    #[case("2024-06-30", (2024, 6, 30))]
    #[case("2024-07-01", (2024, 7, 1))]
    #[case("2024-07-31", (2024, 7, 31))]
    #[case("2024-08-01", (2024, 8, 1))]
    #[case("2024-08-31", (2024, 8, 31))]
    #[case("2024-09-01", (2024, 9, 1))]
    #[case("2024-09-30", (2024, 9, 30))]
    #[case("2024-10-01", (2024, 10, 1))]
    #[case("2024-10-31", (2024, 10, 31))]
    #[case("2024-11-01", (2024, 11, 1))]
    #[case("2024-11-30", (2024, 11, 30))]
    #[case("2024-12-01", (2024, 12, 1))]
    #[case("2024-12-31", (2024, 12, 31))]
    #[case("2023-02-28", (2023, 2, 28))]
    fn test_parse(#[case] input: &str, #[case] ymd: (i32, u32, u32), jolt_version: JoltVersion) {
        let naive_date = NaiveDate::from_ymd_opt(ymd.0, ymd.1, ymd.2).expect("failed to load ymd");
        let jolt_date = JoltDate::parse(input, jolt_version)
            .expect("case input rejected")
            .expect("case input failed");
        assert_eq!(jolt_date.data.date, naive_date);
    }

    #[apply(all_jolt_versions)]
    #[case("0000-01-01", (0, 1, 1))]
    #[case("1234-01-01", (1234, 1, 1))]
    #[case("1970-01-01", (1970, 1, 1))]
    #[case("2026-05-19", (2026, 5, 19))]
    #[case("2024-01-01", (2024, 1, 1))]
    #[case("2024-01-31", (2024, 1, 31))]
    #[case("2024-02-01", (2024, 2, 1))]
    #[case("2024-02-29", (2024, 2, 29))]
    #[case("2024-03-01", (2024, 3, 1))]
    #[case("2024-03-31", (2024, 3, 31))]
    #[case("2024-04-01", (2024, 4, 1))]
    #[case("2024-04-30", (2024, 4, 30))]
    #[case("2024-05-01", (2024, 5, 1))]
    #[case("2024-05-31", (2024, 5, 31))]
    #[case("2024-06-01", (2024, 6, 1))]
    #[case("2024-06-30", (2024, 6, 30))]
    #[case("2024-07-01", (2024, 7, 1))]
    #[case("2024-07-31", (2024, 7, 31))]
    #[case("2024-08-01", (2024, 8, 1))]
    #[case("2024-08-31", (2024, 8, 31))]
    #[case("2024-09-01", (2024, 9, 1))]
    #[case("2024-09-30", (2024, 9, 30))]
    #[case("2024-10-01", (2024, 10, 1))]
    #[case("2024-10-31", (2024, 10, 31))]
    #[case("2024-11-01", (2024, 11, 1))]
    #[case("2024-11-30", (2024, 11, 30))]
    #[case("2024-12-01", (2024, 12, 1))]
    #[case("2024-12-31", (2024, 12, 31))]
    #[case("2023-02-28", (2023, 2, 28))]
    #[case("-0001-02-03", (-1, 2, 3))]
    #[case("+12345-02-03", (12345, 2, 3))]
    #[case("-12345-02-03", (-12345, 2, 3))]
    #[case("+262142-02-03", (262_142, 2, 3))]
    #[case("-262143-02-03", (-262_143, 2, 3))]
    fn test_fmt(
        #[case] expected: &str,
        #[case] ymd: (i32, u32, u32),
        jolt_version: JoltVersion,
        mut jolt_serializer: JoltSerializer<Vec<u8>>,
    ) {
        let naive_date = NaiveDate::from_ymd_opt(ymd.0, ymd.1, ymd.2).expect("failed to load ymd");
        let jolt_date = JoltDate {
            data: JoltDateData { date: naive_date },
            jolt_version,
        };

        jolt_date
            .jolt_serialize(jolt_serializer.ser(), jolt_version)
            .expect("Failed to serialize Jolt");
        let formatted = jolt_serializer.into_string();

        let expected = format!(r#"{{"T": "{expected}"}}"#);
        assert_eq!(formatted, expected);
    }

    #[apply(all_jolt_versions)]
    #[case("P1S")]
    #[case("2000-01-01T")]
    #[case("+2000-01-01")]
    #[case("-2000-01-01")]
    #[case("2000-01-01T00:00:00")]
    #[case("2000-01-01T00:00")]
    #[case("197000")]
    #[case("19700001")]
    #[case("12345-01-01")]
    #[case("1-01-01")]
    #[case("12-01-01")]
    #[case("123-01-01")]
    #[case("12345-01-01")]
    #[case("+1-01-01")]
    #[case("+12-01-01")]
    #[case("+123-01-01")]
    #[case("-1-01-01")]
    #[case("-12-01-01")]
    #[case("-123-01-01")]
    #[case("1970-1-01")]
    #[case("1970-01-1")]
    #[case("1970-001-01")]
    #[case("1970-01-001")]
    fn test_no_match_parse(#[case] input: &str, jolt_version: JoltVersion) {
        let parsed = JoltDate::parse(input, jolt_version);
        assert!(dbg!(parsed).is_none());
    }

    #[apply(all_jolt_versions)]
    #[case("1970-00-01")]
    #[case("1970-01-00")]
    #[case("1970-01-00")]
    #[case("1970-01-32")]
    #[case("2004-02-00")]
    #[case("2004-02-30")]
    #[case("2000-02-00")]
    #[case("2100-02-29")]
    #[case("1970-03-00")]
    #[case("1970-03-32")]
    #[case("1970-04-00")]
    #[case("1970-04-31")]
    #[case("1970-05-00")]
    #[case("1970-05-32")]
    #[case("1970-06-00")]
    #[case("1970-06-31")]
    #[case("1970-07-00")]
    #[case("1970-07-32")]
    #[case("1970-08-00")]
    #[case("1970-08-32")]
    #[case("1970-09-00")]
    #[case("1970-09-31")]
    #[case("1970-10-00")]
    #[case("1970-10-32")]
    #[case("1970-11-00")]
    #[case("1970-11-31")]
    #[case("1970-12-00")]
    #[case("1970-12-32")]
    #[case("1970-13-01")]
    #[case("+262143-02-03")]
    #[case("-262144-02-03")]
    fn test_invalid_parse(#[case] input: &str, jolt_version: JoltVersion) {
        JoltDate::parse(input, jolt_version)
            .expect("case input must not be rejected")
            .expect_err("case input should fail to parse");
    }

    #[apply(all_jolt_versions)]
    #[case("0000-01-01")]
    #[case("1234-01-01")]
    #[case("1970-01-01")]
    #[case("2026-05-19")]
    #[case("2024-01-01")]
    #[case("2024-01-31")]
    #[case("2024-02-01")]
    #[case("2024-02-29")]
    #[case("2024-03-01")]
    #[case("2024-03-31")]
    #[case("2024-04-01")]
    #[case("2024-04-30")]
    #[case("2024-05-01")]
    #[case("2024-05-31")]
    #[case("2024-06-01")]
    #[case("2024-06-30")]
    #[case("2024-07-01")]
    #[case("2024-07-31")]
    #[case("2024-08-01")]
    #[case("2024-08-31")]
    #[case("2024-09-01")]
    #[case("2024-09-30")]
    #[case("2024-10-01")]
    #[case("2024-10-31")]
    #[case("2024-11-01")]
    #[case("2024-11-30")]
    #[case("2024-12-01")]
    #[case("2024-12-31")]
    #[case("2023-02-28")]
    #[case("-0001-02-03")]
    #[case("+12345-02-03")]
    #[case("-12345-02-03")]
    #[case("+262142-02-03")]
    #[case("-262143-02-03")]
    fn test_to_struct(#[case] input: &str, jolt_version: JoltVersion) {
        let date = NaiveDate::parse_from_str(input, "%Y-%m-%d").expect("invalid date input");
        let days_since_epoch = (date - UNIX_EPOCH_DATE).num_days();
        let data = JoltDateData { date };
        let jolt_date = JoltDate { data, jolt_version };

        let struct_ = jolt_date.as_struct();

        let expected = PackStreamStruct {
            tag: TAG_DATE,
            fields: vec![days_since_epoch.into()],
        };
        assert_eq!(struct_, expected);
    }

    #[apply(all_jolt_versions)]
    #[case("0000-01-01")]
    #[case("1234-01-01")]
    #[case("1970-01-01")]
    #[case("2026-05-19")]
    #[case("2024-01-01")]
    #[case("2024-01-31")]
    #[case("2024-02-01")]
    #[case("2024-02-29")]
    #[case("2024-03-01")]
    #[case("2024-03-31")]
    #[case("2024-04-01")]
    #[case("2024-04-30")]
    #[case("2024-05-01")]
    #[case("2024-05-31")]
    #[case("2024-06-01")]
    #[case("2024-06-30")]
    #[case("2024-07-01")]
    #[case("2024-07-31")]
    #[case("2024-08-01")]
    #[case("2024-08-31")]
    #[case("2024-09-01")]
    #[case("2024-09-30")]
    #[case("2024-10-01")]
    #[case("2024-10-31")]
    #[case("2024-11-01")]
    #[case("2024-11-30")]
    #[case("2024-12-01")]
    #[case("2024-12-31")]
    #[case("2023-02-28")]
    #[case("-0001-02-03")]
    #[case("+12345-02-03")]
    #[case("-12345-02-03")]
    #[case("+262142-02-03")]
    #[case("-262143-02-03")]
    fn test_from_struct(#[case] input: &str, jolt_version: JoltVersion) {
        let date = NaiveDate::parse_from_str(input, "%Y-%m-%d").expect("invalid date input");
        let days_since_epoch = (date - UNIX_EPOCH_DATE).num_days();
        let struct_ = PackStreamStruct {
            tag: TAG_DATE,
            fields: vec![days_since_epoch.into()],
        };

        let jolt_date = JoltDate::from_struct(&struct_, jolt_version).expect("failed to load");

        let data = JoltDateData { date };
        let expected = JoltDate { data, jolt_version };
        assert_eq!(jolt_date, expected);
    }
}
