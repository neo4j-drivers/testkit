use std::cell::LazyCell;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

use chrono::{
    DateTime, Duration, FixedOffset, MappedLocalTime, NaiveDateTime, Offset, TimeDelta, TimeZone,
    Utc,
};
use chrono_tz::Tz;
use log::warn;
use regex::Regex;
use serde::Serialize;

use crate::bolt_version::JoltVersion;
use crate::jolt::JoltSigil;
use crate::parse_error::ParseError;
use crate::util::opt_res_ret;
use crate::values::bolt_struct::_common::normalize_seconds_nanos;
use crate::values::bolt_struct::_parsing::{check_last_pack_stream_field, next_pack_stream_field};
use crate::values::bolt_struct::date::JoltDateData;
use crate::values::bolt_struct::time::JoltTimeData;
use crate::values::bolt_struct::{
    TAG_DATE_TIME_V1, TAG_DATE_TIME_V2, TAG_DATE_TIME_ZONE_ID_V1, TAG_DATE_TIME_ZONE_ID_V2,
    TAG_LOCAL_DATE_TIME,
};
use crate::values::jolt_ser::JoltSigilMapSer;
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

const UNIX_EPOCH_DATE_TIME: DateTime<Utc> = DateTime::from_timestamp(0, 0).unwrap();
const SIGIL: &str = JoltSigil::Temporal.str();

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub(crate) struct JoltDateTime<'a> {
    pub(crate) date: JoltDateData,
    pub(crate) time: JoltTimeData<'a>,
    pub(super) jolt_version: JoltVersion,
}

impl<'a> JoltDateTime<'a> {
    pub(crate) fn parse(s: &'a str, jolt_version: JoltVersion) -> Option<Result<Self, ParseError>> {
        thread_local! {
            static DATE_TIME_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new("^(.*?)T(.*)$").unwrap()
            });
        }
        let captures = DATE_TIME_RE.with(|re| re.captures(s))?;
        let date = opt_res_ret!(JoltDateData::parse(&captures[1], jolt_version));
        let time_group = captures.get(2).unwrap().as_str();
        let time = opt_res_ret!(JoltTimeData::parse(time_group, jolt_version));
        if time.time_zone_id.is_some() && time.utc_offset_seconds.is_none() {
            return Some(Err(ParseError::new(
                "DateTime with named zone requires an explicit time offset to avoid ambiguity.",
            )));
        }
        Some(Ok(Self {
            date,
            time,
            jolt_version,
        }))
    }

    pub(crate) fn into_struct(self) -> Option<Result<PackStreamStruct, ParseError>> {
        let jolt_version = self.jolt_version;
        let JoltDateData { date } = self.date;
        let JoltTimeData {
            time,
            utc_offset_seconds,
            time_zone_id,
        } = self.time;
        let date_time = NaiveDateTime::new(date, time);
        Some(Ok(match (utc_offset_seconds, time_zone_id) {
            (Some(utc_offset_seconds), Some(time_zone_id)) => {
                // date time zone id

                // let tz = match Tz::from_str(time_zone_id) {
                //     Ok(tz) => tz,
                //     Err(e) => {
                //         return Some(Err(ParseError::new(format!(
                //             "Failed to load time zone id {time_zone_id:?}: {e}"
                //         ))))
                //     }
                // };
                // let date_time = date_time_utc.with_timezone(&tz);
                // let found_offset_seconds = date_time.offset().fix().local_minus_utc();
                // if found_offset_seconds != utc_offset_seconds {
                //     // "Timezone database for {s} does not agree with the offset \
                //     // {utc_offset_seconds} seconds, found {found_offset_seconds}. \
                //     // Either there is a typo or the timezone database is outdated."
                //     todo!("Emit warning, possibly a typo in the script");
                // }
                match jolt_version {
                    JoltVersion::V1 => {
                        let date_time = date_time.and_utc();
                        let seconds = date_time.timestamp();
                        let nanos = date_time.timestamp_subsec_nanos();
                        PackStreamStruct {
                            tag: TAG_DATE_TIME_ZONE_ID_V1,
                            fields: vec![
                                PackStreamValue::Integer(seconds),
                                PackStreamValue::Integer(nanos.into()),
                                PackStreamValue::String(String::from(time_zone_id)),
                            ],
                        }
                    }
                    JoltVersion::V2 | JoltVersion::V3 | JoltVersion::V4 => {
                        let date_time_utc =
                            date_time.and_utc() - TimeDelta::new(utc_offset_seconds, 0).unwrap();
                        let seconds = date_time_utc.timestamp();
                        let nanos = date_time_utc.timestamp_subsec_nanos();
                        PackStreamStruct {
                            tag: TAG_DATE_TIME_ZONE_ID_V2,
                            fields: vec![
                                PackStreamValue::Integer(seconds),
                                PackStreamValue::Integer(nanos.into()),
                                PackStreamValue::String(String::from(time_zone_id)),
                            ],
                        }
                    }
                }
            }
            (Some(utc_offset_seconds), None) => {
                // date time
                match jolt_version {
                    JoltVersion::V1 => {
                        let date_time = date_time.and_utc();
                        let seconds = date_time.timestamp();
                        let nanos = date_time.timestamp_subsec_nanos();
                        PackStreamStruct {
                            tag: TAG_DATE_TIME_V1,
                            fields: vec![
                                PackStreamValue::Integer(seconds),
                                PackStreamValue::Integer(nanos.into()),
                                PackStreamValue::Integer(utc_offset_seconds),
                            ],
                        }
                    }
                    JoltVersion::V2 | JoltVersion::V3 | JoltVersion::V4 => {
                        let Some(tz) = utc_offset_seconds
                            .try_into()
                            .ok()
                            .and_then(FixedOffset::east_opt)
                        else {
                            return Some(Err(ParseError::new(format!(
                                "utc offset {utc_offset_seconds} out of range"
                            ))));
                        };

                        let date_time = date_time.and_local_timezone(tz).unwrap();
                        let seconds = date_time.timestamp();
                        let nanos = date_time.timestamp_subsec_nanos();
                        PackStreamStruct {
                            tag: TAG_DATE_TIME_V2,
                            fields: vec![
                                PackStreamValue::Integer(seconds),
                                PackStreamValue::Integer(nanos.into()),
                                PackStreamValue::Integer(utc_offset_seconds),
                            ],
                        }
                    }
                }
            }
            (None, Some(_)) => return Some(Err(ParseError::new(
                "DateTime with named zone requires an explicit time offset to avoid ambiguity. \
                E.g., `2025-03-06T18:05:02+01:00[Europe/Stockholm]` instead of \
                `2025-03-06T18:05:02[Europe/Stockholm]`",
            ))),
            (None, None) => {
                // local date time
                let date_time = date_time.and_utc();
                let seconds = date_time.timestamp();
                let nanos = date_time.timestamp_subsec_nanos();
                PackStreamStruct {
                    tag: TAG_LOCAL_DATE_TIME,
                    fields: vec![
                        PackStreamValue::Integer(seconds),
                        PackStreamValue::Integer(nanos.into()),
                    ],
                }
            }
        }))
    }

    #[expect(clippy::too_many_lines, reason = "A problem for future-us")]
    pub(super) fn from_struct(s: &'a PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        let PackStreamStruct { tag, fields } = s;
        let mut fields = fields.iter();
        let this = match *tag {
            TAG_DATE_TIME_ZONE_ID_V1 if jolt_version == JoltVersion::V1 => {
                let seconds_since_epoch: i64 = next_pack_stream_field(&mut fields)?;
                let nanos: i64 = next_pack_stream_field(&mut fields)?;
                let zone_id: &str = next_pack_stream_field(&mut fields)?;
                if !check_last_pack_stream_field(&mut fields) {
                    return None;
                }
                let (seconds_since_epoch, nanos) =
                    normalize_seconds_nanos(seconds_since_epoch, nanos)?;
                let date_time = DateTime::from_timestamp(seconds_since_epoch, nanos)?.naive_local();
                let Ok(tz) = Tz::from_str(zone_id) else {
                    warn!(
                        "Received unknown time zone {zone_id}. \
                        Representations might look misleading"
                    );
                    return Some(Self::new_unknown_tz(date_time, zone_id, jolt_version));
                };
                let date_time = match dbg!(tz.from_local_datetime(&date_time)) {
                    MappedLocalTime::Single(d) => d,
                    MappedLocalTime::Ambiguous(d1, d2) => {
                        warn!(
                            "Received datetime with ambiguous local time: {date_time} {tz}. \
                            Datetime will be loaded as {d1} (alternative: {d2})"
                        );
                        d1
                    }
                    MappedLocalTime::None => {
                        warn!(
                            "Received datetime with non-existing local time: {date_time} {tz}. \
                            Datetime will be loaded without offset"
                        );
                        return Some(Self::new_unknown_tz(date_time, zone_id, jolt_version));
                    }
                };
                Self::new_tz(&date_time, zone_id, jolt_version)
            }
            TAG_DATE_TIME_ZONE_ID_V2 if jolt_version >= JoltVersion::V2 => {
                let seconds_since_epoch: i64 = next_pack_stream_field(&mut fields)?;
                let nanos: i64 = next_pack_stream_field(&mut fields)?;
                let zone_id: &str = next_pack_stream_field(&mut fields)?;
                if !check_last_pack_stream_field(&mut fields) {
                    return None;
                }
                let (seconds_since_epoch, nanos) =
                    normalize_seconds_nanos(seconds_since_epoch, nanos)?;
                let utc_date_time =
                    dbg!(DateTime::from_timestamp(seconds_since_epoch, nanos)?.naive_local());
                let Ok(tz) = Tz::from_str(zone_id) else {
                    warn!(
                        "Received unknown time zone {zone_id}. \
                        Representations might look misleading"
                    );
                    return Some(Self::new_unknown_tz(utc_date_time, zone_id, jolt_version));
                };
                Self::new_tz(
                    &dbg!(tz.from_utc_datetime(&utc_date_time)),
                    zone_id,
                    jolt_version,
                )
            }
            TAG_DATE_TIME_V1 if jolt_version == JoltVersion::V1 => {
                let seconds_since_epoch: i64 = next_pack_stream_field(&mut fields)?;
                let nanos: i64 = next_pack_stream_field(&mut fields)?;
                let utc_offset_seconds: i64 = next_pack_stream_field(&mut fields)?;
                if !check_last_pack_stream_field(&mut fields) {
                    return None;
                }
                let (seconds_since_epoch, nanos) =
                    normalize_seconds_nanos(seconds_since_epoch, nanos)?;
                let date_time = DateTime::from_timestamp(seconds_since_epoch, nanos)?.naive_local();
                Self::new(date_time, Some(utc_offset_seconds), None, jolt_version)
            }
            TAG_DATE_TIME_V2 if jolt_version >= JoltVersion::V2 => {
                let seconds_since_epoch: i64 = next_pack_stream_field(&mut fields)?;
                let nanos: i64 = next_pack_stream_field(&mut fields)?;
                let utc_offset_seconds: i64 = next_pack_stream_field(&mut fields)?;
                if !check_last_pack_stream_field(&mut fields) {
                    return None;
                }
                let (seconds_since_epoch, nanos) =
                    normalize_seconds_nanos(seconds_since_epoch, nanos)?;
                let utc_date_time =
                    DateTime::from_timestamp(seconds_since_epoch, nanos)?.naive_local();
                let local_date_time =
                    utc_date_time.checked_add_signed(TimeDelta::new(utc_offset_seconds, 0)?)?;
                Self::new(
                    local_date_time,
                    Some(utc_offset_seconds),
                    None,
                    jolt_version,
                )
            }
            TAG_LOCAL_DATE_TIME => {
                let seconds_since_epoch: i64 = next_pack_stream_field(&mut fields)?;
                let nanos: i64 = next_pack_stream_field(&mut fields)?;
                if !check_last_pack_stream_field(&mut fields) {
                    return None;
                }
                let (seconds_since_epoch, nanos) =
                    normalize_seconds_nanos(seconds_since_epoch, nanos)?;
                let date_time = UNIX_EPOCH_DATE_TIME + Duration::new(seconds_since_epoch, nanos)?;
                Self::new_naive(date_time.naive_local(), jolt_version)
            }
            _ => return None,
        };
        if !check_last_pack_stream_field(&mut fields) {
            return None;
        }
        Some(this)
    }

    fn new(
        date_time: NaiveDateTime,
        utc_offset_seconds: Option<i64>,
        time_zone_id: Option<&'a str>,
        jolt_version: JoltVersion,
    ) -> Self {
        let date = date_time.date();
        let date = JoltDateData { date };
        let time = date_time.time();
        let time = JoltTimeData {
            time,
            utc_offset_seconds,
            time_zone_id,
        };
        Self {
            date,
            time,
            jolt_version,
        }
    }

    fn new_naive(date_time: NaiveDateTime, jolt_version: JoltVersion) -> Self {
        Self::new(date_time, None, None, jolt_version)
    }

    fn new_unknown_tz(
        date_time: NaiveDateTime,
        time_zone_id: &'a str,
        jolt_version: JoltVersion,
    ) -> Self {
        Self::new(date_time, Some(0), Some(time_zone_id), jolt_version)
    }

    fn new_tz<Tz: TimeZone>(
        date_time: &DateTime<Tz>,
        time_zone_id: &'a str,
        jolt_version: JoltVersion,
    ) -> Self {
        let local_date_time = date_time.naive_local();
        let utc_offset = date_time.offset().fix().local_minus_utc();
        Self::new(
            local_date_time,
            Some(utc_offset.into()),
            Some(time_zone_id),
            jolt_version,
        )
    }

    pub(super) fn jolt_serialize<S>(
        &self,
        serializer: S,
        jolt_version_ctx: JoltVersion,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        struct DataSer<'a>(&'a JoltDateTime<'a>);

        impl Display for DataSer<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}T{}", self.0.date.repr(), self.0.time.repr())
            }
        }

        impl serde::Serialize for DataSer<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.collect_str(self)
            }
        }

        let body = DataSer(self);
        let map = JoltSigilMapSer::new(SIGIL, self.jolt_version, jolt_version_ctx, &body);
        map.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime};

    use crate::ext::serde_json::support_pub::JoltSerializer;
    use crate::values::tests::{
        all_jolt_versions, jolt_serializer,
        jolt_versions_v1_and_down as jolt_versions_without_utc_fix,
        jolt_versions_v2_and_up as jolt_versions_with_utc_fix,
    };

    use super::*;

    use rstest::rstest;
    use rstest_reuse::apply;

    #[apply(all_jolt_versions)]
    #[case(
        "0000-01-01T00:00",
        (0, 1, 1),
        (0, 0, 0, 0),
        None,
        None,
    )]
    #[case(
        "2024-01-02T03:04:05",
        (2024, 1, 2),
        (3, 4, 5, 0),
        None,
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123456789",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        None,
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123",
        (2024, 1, 2),
        (3, 4, 5, 123_000_000),
        None,
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123000",
        (2024, 1, 2),
        (3, 4, 5, 123_000_000),
        None,
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123456789+1234[Europe/Stockholm]",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some((12 * 60 + 34) * 60),
        Some("Europe/Stockholm"),
    )]
    #[case(
        "2024-01-02T03:04:05.123456789+12:34[Europe/Stockholm]",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some((12 * 60 + 34) * 60),
        Some("Europe/Stockholm"),
    )]
    #[case(
        "2024-01-02T03:04:05.123456789-12:34",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some(-(12 * 60 + 34) * 60),
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123456789Z",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some(0),
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123456789+00:00",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some(0),
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123456789-00:00",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some(0),
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123456789+0000[Europe/Neo4j]",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some(0),
        Some("Europe/Neo4j"),
    )]
    #[case(
        "2024-01-02T03:04:05.123456789+24:00",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some(24 * 60 * 60),
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123456789-24:00",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some(-24 * 60 * 60),
        None,
    )]
    fn test_parse(
        #[case] input: &str,
        #[case] ymd: (i32, u32, u32),
        #[case] hmsn: (u32, u32, u32, u32),
        #[case] utc_offset_seconds: Option<i64>,
        #[case] time_zone_id: Option<&'_ str>,
        jolt_version: JoltVersion,
    ) {
        dbg!(input);
        let date = NaiveDate::from_ymd_opt(ymd.0, ymd.1, ymd.2).expect("failed to load ymd");
        let time = NaiveTime::from_hms_nano_opt(hmsn.0, hmsn.1, hmsn.2, hmsn.3)
            .expect("failed to load hmsn");
        let date_time = NaiveDateTime::new(date, time);
        let expected = JoltDateTime::new(date_time, utc_offset_seconds, time_zone_id, jolt_version);

        let jolt_date_time = JoltDateTime::parse(input, jolt_version)
            .expect("case input rejected")
            .expect("case input failed");

        assert_eq!(jolt_date_time.date, expected.date);
        assert_eq!(jolt_date_time.time, expected.time);
    }

    #[apply(all_jolt_versions)]
    #[case(
        "0000-01-01T00:00:00",
        (0, 1, 1),
        (0, 0, 0, 0),
        None,
        None,
    )]
    #[case(
        "2024-01-02T03:04:05",
        (2024, 1, 2),
        (3, 4, 5, 0),
        None,
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123456789",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        None,
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123",
        (2024, 1, 2),
        (3, 4, 5, 123_000_000),
        None,
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123456789+12:34[Europe/Stockholm]",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some((12 * 60 + 34) * 60),
        Some("Europe/Stockholm"),
    )]
    #[case(
        "2024-01-02T03:04:05.123456789-12:34",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some(-(12 * 60 + 34) * 60),
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123456789Z",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some(0),
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123456789+00:00[Europe/Neo4j]",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some(0),
        Some("Europe/Neo4j"),
    )]
    #[case(
        "2024-01-02T03:04:05.123456789+24:00",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some(24 * 60 * 60),
        None,
    )]
    #[case(
        "2024-01-02T03:04:05.123456789-24:00",
        (2024, 1, 2),
        (3, 4, 5, 123_456_789),
        Some(-24 * 60 * 60),
        None,
    )]
    fn test_fmt(
        #[case] expected: &str,
        #[case] ymd: (i32, u32, u32),
        #[case] hmsn: (u32, u32, u32, u32),
        #[case] utc_offset_seconds: Option<i64>,
        #[case] time_zone_id: Option<&'_ str>,
        jolt_version: JoltVersion,
        mut jolt_serializer: JoltSerializer<Vec<u8>>,
    ) {
        let date = NaiveDate::from_ymd_opt(ymd.0, ymd.1, ymd.2).expect("failed to load ymd");
        let time = NaiveTime::from_hms_nano_opt(hmsn.0, hmsn.1, hmsn.2, hmsn.3)
            .expect("failed to load hmsn");
        let date_time = NaiveDateTime::new(date, time);
        let date_time =
            JoltDateTime::new(date_time, utc_offset_seconds, time_zone_id, jolt_version);
        dbg!(&date_time);

        date_time
            .jolt_serialize(jolt_serializer.ser(), jolt_version)
            .expect("Failed to serialize Jolt");
        let formatted = jolt_serializer.into_string();

        let expected = format!(r#"{{"T": "{expected}"}}"#);
        assert_eq!(formatted, expected);
    }

    #[apply(all_jolt_versions)]
    #[case("2024-01-02")]
    #[case("2024-01-02T")]
    #[case("03:04:05.123456789")]
    #[case("T03:04:05.123456789")]
    #[case("2024-01-02TT03:04:05.123456789")]
    #[case("2024-01-0203:04:05.123456789")]
    #[case("2024-01-02X03:04:05.123456789")]
    fn test_no_match_parse(#[case] input: &str, jolt_version: JoltVersion) {
        let parsed = JoltDateTime::parse(input, jolt_version);
        assert!(dbg!(parsed).is_none());
    }

    #[apply(all_jolt_versions)]
    #[case("2024-01-02T03:04:05.1234567890")]
    #[case("2024-01-02T03:04:05.123456789[Europe/Stockholm]")]
    #[case("2024-01-02T03:04:05.123456789+24:01")]
    #[case("2024-01-02T03:04:05.123456789+25:00")]
    #[case("2024-01-02T03:04:05.123456789-24:01")]
    #[case("2024-01-02T03:04:05.123456789-25:00")]
    fn test_invalid_parse(#[case] input: &str, jolt_version: JoltVersion) {
        JoltDateTime::parse(input, jolt_version)
            .expect("case input must not be rejected")
            .expect_err("case input should fail to parse");
    }

    #[apply(jolt_versions_without_utc_fix)]
    #[case(
        "1970-01-02T01:01:01.000001234",
        TAG_LOCAL_DATE_TIME,
        vec![90061.into(), 1234.into()],
    )]
    #[case(
        "1970-01-02T01:01:01.1234+01",
        TAG_DATE_TIME_V1,
        vec![90061.into(), 123_400_000.into(), 3600.into()],
    )]
    // packstream v1 bug: non-unique times
    #[case(
        "1970-10-25T01:30-04:00[America/New_York]",
        TAG_DATE_TIME_ZONE_ID_V1,
        vec![25_666_200.into(), 0.into(), "America/New_York".into()],
    )]
    // Wow! The same encoding \o/ [/sarcasm]
    #[case(
        "1970-10-25T01:30-05:00[America/New_York]",
        TAG_DATE_TIME_ZONE_ID_V1,
        vec![25_666_200.into(), 0.into(), "America/New_York".into()],
    )]
    #[case(
        "1970-01-02T01:01:01.1234+01[Europe/Stockholm]",
        TAG_DATE_TIME_ZONE_ID_V1,
        vec![90061.into(), 123_400_000.into(), "Europe/Stockholm".into()],
    )]
    fn test_to_struct_v1(
        #[case] input: &str,
        #[case] tag: u8,
        #[case] fields: Vec<PackStreamValue>,
        jolt_version: JoltVersion,
    ) {
        let date_time = JoltDateTime::parse(input, jolt_version)
            .expect("non-datetime input")
            .expect("invalid input");

        let struct_ = date_time
            .into_struct()
            .expect("no struct")
            .expect("invalid struct");

        let expected = PackStreamStruct { tag, fields };
        assert_eq!(struct_, expected);
    }

    #[apply(jolt_versions_with_utc_fix)]
    #[case(
        "1970-01-02T01:01:01.000001234",
        TAG_LOCAL_DATE_TIME,
        vec![90061.into(), 1234.into()],
    )]
    #[case(
        "1970-01-02T01:01:01.1234+01",
        TAG_DATE_TIME_V2,
        vec![(90061 - 3600).into(), 123_400_000.into(), 3600.into()],
    )]
    #[case(
        "1970-10-25T01:30-04:00[America/New_York]",
        TAG_DATE_TIME_ZONE_ID_V2,
        vec![(25_666_200 + 4 * 3600).into(), 0.into(), "America/New_York".into()],
    )]
    #[case(
        "1970-10-25T01:30-05:00[America/New_York]",
        TAG_DATE_TIME_ZONE_ID_V2,
        vec![(25_666_200 + 5 * 3600).into(), 0.into(), "America/New_York".into()],
    )]
    #[case(
        "1970-01-02T01:01:01.1234+01[Europe/Stockholm]",
        TAG_DATE_TIME_ZONE_ID_V2,
        vec![(90061 - 3600).into(), 123_400_000.into(), "Europe/Stockholm".into()],
    )]
    fn test_to_struct_v2_plus(
        #[case] input: &str,
        #[case] tag: u8,
        #[case] fields: Vec<PackStreamValue>,
        jolt_version: JoltVersion,
    ) {
        let date_time = JoltDateTime::parse(input, jolt_version)
            .expect("non-datetime input")
            .expect("invalid input");

        let struct_ = date_time
            .into_struct()
            .expect("no struct")
            .expect("invalid struct");

        let expected = PackStreamStruct { tag, fields };
        assert_eq!(struct_, expected);
    }

    #[apply(jolt_versions_without_utc_fix)]
    #[case(
        "1970-01-02T01:01:01.000001234",
        TAG_LOCAL_DATE_TIME,
        vec![90061.into(), 1234.into()],
    )]
    #[case(
        "1970-01-02T01:01:01.1234+01",
        TAG_DATE_TIME_V1,
        vec![90061.into(), 123_400_000.into(), 3600.into()],
    )]
    // packstream v1 bug: non-unique times
    #[case(
        // could also be "1970-10-25T01:30-05:00[America/New_York]"
        // either is fine, but consistency is desired
        "1970-10-25T01:30-04:00[America/New_York]",
        TAG_DATE_TIME_ZONE_ID_V1,
        vec![25_666_200.into(), 0.into(), "America/New_York".into()],
    )]
    #[case(
        "1970-01-02T01:01:01.1234+01[Europe/Stockholm]",
        TAG_DATE_TIME_ZONE_ID_V1,
        vec![90061.into(), 123_400_000.into(), "Europe/Stockholm".into()],
    )]
    fn test_from_struct_v1(
        #[case] input: &str,
        #[case] tag: u8,
        #[case] fields: Vec<PackStreamValue>,
        jolt_version: JoltVersion,
    ) {
        let struct_ = PackStreamStruct { tag, fields };

        let date_time = JoltDateTime::from_struct(&struct_, jolt_version).expect("failed to load");

        let expected = JoltDateTime::parse(input, jolt_version)
            .expect("non-datetime input")
            .expect("invalid input");
        assert_eq!(date_time, expected);
    }

    #[apply(jolt_versions_with_utc_fix)]
    #[case(
        "1970-01-02T01:01:01.000001234",
        TAG_LOCAL_DATE_TIME,
        vec![90061.into(), 1234.into()],
    )]
    #[case(
        "1970-01-02T01:01:01.1234+01",
        TAG_DATE_TIME_V2,
        vec![(90061 - 3600).into(), 123_400_000.into(), 3600.into()],
    )]
    #[case(
        "1970-10-25T01:30-04:00[America/New_York]",
        TAG_DATE_TIME_ZONE_ID_V2,
        vec![(25_666_200 + 4 * 3600).into(), 0.into(), "America/New_York".into()],
    )]
    #[case(
        "1970-10-25T01:30-05:00[America/New_York]",
        TAG_DATE_TIME_ZONE_ID_V2,
        vec![(25_666_200 + 5 * 3600).into(), 0.into(), "America/New_York".into()],
    )]
    #[case(
        "1970-01-02T01:01:01.1234+01[Europe/Stockholm]",
        TAG_DATE_TIME_ZONE_ID_V2,
        vec![(90061 - 3600).into(), 123_400_000.into(), "Europe/Stockholm".into()],
    )]
    fn test_from_struct_v2_plus(
        #[case] input: &str,
        #[case] tag: u8,
        #[case] fields: Vec<PackStreamValue>,
        jolt_version: JoltVersion,
    ) {
        let struct_ = PackStreamStruct { tag, fields };

        let date_time = JoltDateTime::from_struct(&struct_, jolt_version).expect("failed to load");

        let expected = JoltDateTime::parse(input, jolt_version)
            .expect("non-datetime input")
            .expect("invalid input");
        assert_eq!(date_time, expected);
    }
}
