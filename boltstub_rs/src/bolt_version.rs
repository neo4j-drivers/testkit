use std::borrow::Cow;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::atomic::AtomicI64;

use crate::types::Resolvable;
use crate::values::bolt_message::{BoltMessage, SerializedBoltMessage};
use crate::values::pack_stream_value::{PackStreamValue, PackStreamVersion};
use indexmap::{indexmap, IndexMap};
use itertools::{EitherOrBoth, Itertools};

#[derive(Debug, Eq, PartialEq, Clone, Copy, Ord, PartialOrd)]
pub enum BoltVersion {
    V1,
    V2,
    V3,
    V4_0,
    V4_1,
    V4_2,
    V4_3,
    V4_4,
    V5_0,
    V5_1,
    V5_2,
    V5_3,
    V5_4,
    V5_5,
    V5_6,
    V5_7,
    V5_8,
    V6_0,
    V6_1,
}

impl Display for BoltVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self <= &BoltVersion::V3 {
            return write!(f, "{}", self.major());
        }
        write!(f, "{}.{}", self.major(), self.minor())
    }
}

impl BoltVersion {
    pub fn match_valid_version(major: u8, minor: Option<u8>) -> Option<Self> {
        Some(match (major, minor) {
            (1, None | Some(0)) => BoltVersion::V1,
            (2, None | Some(0)) => BoltVersion::V2,
            (3, None | Some(0)) => BoltVersion::V3,
            (4, Some(x)) => match x {
                0 => BoltVersion::V4_0,
                1 => BoltVersion::V4_1,
                2 => BoltVersion::V4_2,
                3 => BoltVersion::V4_3,
                4 => BoltVersion::V4_4,
                _ => return None,
            },
            (5, Some(x)) => match x {
                0 => BoltVersion::V5_0,
                1 => BoltVersion::V5_1,
                2 => BoltVersion::V5_2,
                3 => BoltVersion::V5_3,
                4 => BoltVersion::V5_4,
                5 => BoltVersion::V5_5,
                6 => BoltVersion::V5_6,
                7 => BoltVersion::V5_7,
                8 => BoltVersion::V5_8,
                _ => return None,
            },
            (6, Some(x)) => match x {
                0 => BoltVersion::V6_0,
                1 => BoltVersion::V6_1,
                _ => return None,
            },
            _ => return None,
        })
    }

    pub fn major(self) -> u8 {
        match self {
            BoltVersion::V1 => 1,
            BoltVersion::V2 => 2,
            BoltVersion::V3 => 3,
            BoltVersion::V4_0 => 4,
            BoltVersion::V4_1 => 4,
            BoltVersion::V4_2 => 4,
            BoltVersion::V4_3 => 4,
            BoltVersion::V4_4 => 4,
            BoltVersion::V5_0 => 5,
            BoltVersion::V5_1 => 5,
            BoltVersion::V5_2 => 5,
            BoltVersion::V5_3 => 5,
            BoltVersion::V5_4 => 5,
            BoltVersion::V5_5 => 5,
            BoltVersion::V5_6 => 5,
            BoltVersion::V5_7 => 5,
            BoltVersion::V5_8 => 5,
            BoltVersion::V6_0 => 6,
            BoltVersion::V6_1 => 6,
        }
    }

    pub fn minor(self) -> u8 {
        match self {
            BoltVersion::V1 => 0,
            BoltVersion::V2 => 0,
            BoltVersion::V3 => 0,
            BoltVersion::V4_0 => 0,
            BoltVersion::V4_1 => 1,
            BoltVersion::V4_2 => 2,
            BoltVersion::V4_3 => 3,
            BoltVersion::V4_4 => 4,
            BoltVersion::V5_0 => 0,
            BoltVersion::V5_1 => 1,
            BoltVersion::V5_2 => 2,
            BoltVersion::V5_3 => 3,
            BoltVersion::V5_4 => 4,
            BoltVersion::V5_5 => 5,
            BoltVersion::V5_6 => 6,
            BoltVersion::V5_7 => 7,
            BoltVersion::V5_8 => 8,
            BoltVersion::V6_0 => 0,
            BoltVersion::V6_1 => 0,
        }
    }

    pub fn jolt_version(self) -> JoltVersion {
        match self {
            BoltVersion::V1 => JoltVersion::V1,
            BoltVersion::V2 => JoltVersion::V1,
            BoltVersion::V3 => JoltVersion::V1,
            BoltVersion::V4_0 => JoltVersion::V1,
            BoltVersion::V4_1 => JoltVersion::V1,
            BoltVersion::V4_2 => JoltVersion::V1,
            BoltVersion::V4_3 => JoltVersion::V1,
            BoltVersion::V4_4 => JoltVersion::V1,
            BoltVersion::V5_0 => JoltVersion::V2,
            BoltVersion::V5_1 => JoltVersion::V2,
            BoltVersion::V5_2 => JoltVersion::V2,
            BoltVersion::V5_3 => JoltVersion::V2,
            BoltVersion::V5_4 => JoltVersion::V2,
            BoltVersion::V5_5 => JoltVersion::V2,
            BoltVersion::V5_6 => JoltVersion::V2,
            BoltVersion::V5_7 => JoltVersion::V2,
            BoltVersion::V5_8 => JoltVersion::V2,
            BoltVersion::V6_0 => JoltVersion::V3,
            BoltVersion::V6_1 => JoltVersion::V4,
        }
    }

    pub fn packstream_version(self) -> PackStreamVersion {
        match self {
            BoltVersion::V1 => PackStreamVersion::V1,
            BoltVersion::V2 => PackStreamVersion::V1,
            BoltVersion::V3 => PackStreamVersion::V1,
            BoltVersion::V4_0 => PackStreamVersion::V1,
            BoltVersion::V4_1 => PackStreamVersion::V1,
            BoltVersion::V4_2 => PackStreamVersion::V1,
            BoltVersion::V4_3 => PackStreamVersion::V1,
            BoltVersion::V4_4 => PackStreamVersion::V1,
            BoltVersion::V5_0 => PackStreamVersion::V1,
            BoltVersion::V5_1 => PackStreamVersion::V1,
            BoltVersion::V5_2 => PackStreamVersion::V1,
            BoltVersion::V5_3 => PackStreamVersion::V1,
            BoltVersion::V5_4 => PackStreamVersion::V1,
            BoltVersion::V5_5 => PackStreamVersion::V1,
            BoltVersion::V5_6 => PackStreamVersion::V1,
            BoltVersion::V5_7 => PackStreamVersion::V1,
            BoltVersion::V5_8 => PackStreamVersion::V1,
            BoltVersion::V6_0 => PackStreamVersion::V1,
            BoltVersion::V6_1 => PackStreamVersion::V2,
        }
    }

    pub fn supports_minor(self) -> bool {
        self >= BoltVersion::V4_0
    }

    pub fn supports_range(self) -> bool {
        self >= BoltVersion::V4_2
    }

    pub fn max_handshake_manifest_version(self) -> u8 {
        match self {
            BoltVersion::V1 => 0,
            BoltVersion::V2 => 0,
            BoltVersion::V3 => 0,
            BoltVersion::V4_0 => 0,
            BoltVersion::V4_1 => 0,
            BoltVersion::V4_2 => 0,
            BoltVersion::V4_3 => 0,
            BoltVersion::V4_4 => 0,
            BoltVersion::V5_0 => 0,
            BoltVersion::V5_1 => 0,
            BoltVersion::V5_2 => 0,
            BoltVersion::V5_3 => 0,
            BoltVersion::V5_4 => 0,
            BoltVersion::V5_5 => 0,
            BoltVersion::V5_6 => 1,
            BoltVersion::V5_7 => 1,
            BoltVersion::V5_8 => 1,
            BoltVersion::V6_0 => 1,
            BoltVersion::V6_1 => 1,
        }
    }

    pub fn backwards_equivalent_versions(self) -> &'static [(u8, u8)] {
        static NONE: [(u8, u8); 0] = [];
        static ALIASES_V4_2: [(u8, u8); 1] = [(4, 1)];

        match self {
            BoltVersion::V1 => &NONE,
            BoltVersion::V2 => &NONE,
            BoltVersion::V3 => &NONE,
            BoltVersion::V4_0 => &NONE,
            BoltVersion::V4_1 => &NONE,
            BoltVersion::V4_2 => &ALIASES_V4_2,
            BoltVersion::V4_3 => &NONE,
            BoltVersion::V4_4 => &NONE,
            BoltVersion::V5_0 => &NONE,
            BoltVersion::V5_1 => &NONE,
            BoltVersion::V5_2 => &NONE,
            BoltVersion::V5_3 => &NONE,
            BoltVersion::V5_4 => &NONE,
            BoltVersion::V5_5 => &NONE,
            BoltVersion::V5_6 => &NONE,
            BoltVersion::V5_7 => &NONE,
            BoltVersion::V5_8 => &NONE,
            BoltVersion::V6_0 => &NONE,
            BoltVersion::V6_1 => &NONE,
        }
    }

    pub fn message_tag_from_request(self, name: &str) -> Option<u8> {
        Some(match name {
            "INIT" if self < BoltVersion::V3 => 0x01,
            "HELLO" if self >= BoltVersion::V3 => 0x01,
            "LOGON" if self >= BoltVersion::V5_1 => 0x6A,
            "LOGOFF" if self >= BoltVersion::V5_1 => 0x6B,
            "TELEMETRY" if self >= BoltVersion::V5_4 => 0x54,
            "GOODBYE" => 0x02,
            "ACK_FAILURE" if self < BoltVersion::V3 => 0x0E,
            "RESET" => 0x0F,
            "RUN" => 0x10,
            "DISCARD_ALL" if self < BoltVersion::V4_0 => 0x2F,
            "DISCARD" if self >= BoltVersion::V4_0 => 0x2F,
            "PULL_ALL" if self < BoltVersion::V4_0 => 0x3F,
            "PULL" if self >= BoltVersion::V4_0 => 0x3F,
            "BEGIN" => 0x11,
            "COMMIT" => 0x12,
            "ROLLBACK" => 0x13,
            "ROUTE" if self >= BoltVersion::V4_3 => 0x66,
            _ => return None,
        })
    }

    #[allow(
        clippy::unused_self,
        reason = "response tags might change in a future protocol version"
    )]
    pub fn message_tag_from_response(self, name: &str) -> Option<u8> {
        Some(match name {
            "SUCCESS" => 0x70,
            "IGNORED" => 0x7E,
            "FAILURE" => 0x7F,
            "RECORD" => 0x71,
            _ => return None,
        })
    }

    pub fn message_name_from_tag(self, tag: u8) -> Option<&'static str> {
        Some(match tag {
            0x01 if self < BoltVersion::V3 => "INIT",
            0x01 if self >= BoltVersion::V3 => "HELLO",
            0x6A if self >= BoltVersion::V5_1 => "LOGON",
            0x6B if self >= BoltVersion::V5_1 => "LOGOFF",
            0x54 if self >= BoltVersion::V5_4 => "TELEMETRY",
            0x02 => "GOODBYE",
            0x0E if self < BoltVersion::V3 => "ACK_FAILURE",
            0x0F => "RESET",
            0x10 => "RUN",
            0x2F if self < BoltVersion::V4_0 => "DISCARD_ALL",
            0x2F if self >= BoltVersion::V4_0 => "DISCARD",
            0x3F if self < BoltVersion::V4_0 => "PULL_ALL",
            0x3F if self >= BoltVersion::V4_0 => "PULL",
            0x11 => "BEGIN",
            0x12 => "COMMIT",
            0x13 => "ROLLBACK",
            0x66 if self >= BoltVersion::V4_3 => "ROUTE",
            0x70 => "SUCCESS",
            0x7E => "IGNORED",
            0x7F => "FAILURE",
            0x71 => "RECORD",
            _ => return None,
        })
    }

    pub fn message_auto_response(self, tag: u8) -> Resolvable<SerializedBoltMessage> {
        const SUCCESS_TAG: u8 = 0x70;
        let success_meta = match tag {
            0x01 => {
                // INIT/HELLO
                let mut success_map = IndexMap::with_capacity(4);
                success_map.insert(
                    String::from("server"),
                    PackStreamValue::String(String::from(self.server_agent())),
                );
                if self < BoltVersion::V3 {
                    let fields = vec![PackStreamValue::Dict(success_map)];
                    let message = BoltMessage::new(SUCCESS_TAG, fields, self);
                    let serialized_message = message
                        .into_serialized()
                        .expect("auto response must use available PackStream types");
                    return Resolvable::Static(serialized_message);
                }
                if self >= BoltVersion::V5_7 {
                    // Simplification: the real server only includes this field
                    // if handshake manifest v1 or newer was used.
                    success_map.insert(
                        String::from("protocol_version"),
                        PackStreamValue::String(self.to_string()),
                    );
                }
                if self >= BoltVersion::V5_8 {
                    success_map.insert(
                        String::from("hints"),
                        PackStreamValue::Dict(
                            indexmap! {String::from("ssr.enabled") => PackStreamValue::Boolean(true)},
                        ),
                    );
                }
                return Resolvable::Dynamic {
                    func: Box::new(move || {
                        static CONNECTION_ID: AtomicI64 = AtomicI64::new(1);
                        let mut success_map = success_map.clone();
                        let connection_id =
                            CONNECTION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        assert_ne!(connection_id, 0, "Connection ID overflowed");
                        success_map.insert(
                            String::from("connection_id"),
                            PackStreamValue::String(format!("bolt-{connection_id}")),
                        );
                        let fields = vec![PackStreamValue::Dict(success_map)];
                        BoltMessage::new(SUCCESS_TAG, fields, self)
                            .into_serialized()
                            .expect("auto response must use available PackStream types")
                    }),
                    repr: format!("Auto-HELLO-SUCCESS @{}:{}", file!(), line!()),
                };
            }
            _ => IndexMap::default(),
        };
        Resolvable::Static(
            BoltMessage::new(SUCCESS_TAG, vec![PackStreamValue::Dict(success_meta)], self)
                .into_serialized()
                .expect("auto response must use available PackStream types"),
        )
    }

    fn server_agent(self) -> &'static str {
        match self {
            BoltVersion::V1 => "Neo4j/3.3.0",
            BoltVersion::V2 => "Neo4j/3.4.0",
            BoltVersion::V3 => "Neo4j/3.5.0",
            BoltVersion::V4_0 => "Neo4j/4.0.0",
            BoltVersion::V4_1 => "Neo4j/4.1.0",
            BoltVersion::V4_2 => "Neo4j/4.2.0",
            BoltVersion::V4_3 => "Neo4j/4.3.0",
            BoltVersion::V4_4 => "Neo4j/4.4.0",
            BoltVersion::V5_0 => "Neo4j/5.0.0",
            BoltVersion::V5_1 => "Neo4j/5.5.0",
            BoltVersion::V5_2 => "Neo4j/5.7.0",
            BoltVersion::V5_3 => "Neo4j/5.9.0",
            BoltVersion::V5_4 => "Neo4j/5.13.0",
            BoltVersion::V5_5 => "Neo4j/5.21.0",
            BoltVersion::V5_6 => "Neo4j/5.23.0",
            BoltVersion::V5_7 => "Neo4j/5.26.0",
            BoltVersion::V5_8 => "Neo4j/5.26.0",
            BoltVersion::V6_0 => "Neo4j/2025.10.0",
            // TODO: update once Bolt 6.1 has been released server-side
            BoltVersion::V6_1 => "Neo4j/2026.06.0",
        }
    }
}

#[derive(Debug)]
pub struct BoltCapabilities(Vec<u8>);

impl PartialEq for BoltCapabilities {
    fn eq(&self, other: &Self) -> bool {
        #[allow(clippy::verbose_bit_mask, reason = "improves readability")]
        self.0.iter().zip_longest(other.0.iter()).all(|e| match e {
            EitherOrBoth::Both(a, b) => a & 0x7F == b & 0x7F,
            EitherOrBoth::Left(byte) | EitherOrBoth::Right(byte) => *byte & 0x7F == 0,
        })
    }
}

impl Eq for BoltCapabilities {}

impl Default for BoltCapabilities {
    fn default() -> Self {
        Self(vec![0x00])
    }
}

const CAPABILITIES_MAX_BITS: usize = 63;

impl BoltCapabilities {
    pub fn from_bytes(mut bytes: Vec<u8>) -> Result<Self, Cow<'static, str>> {
        if bytes.is_empty() {
            bytes.push(0);
        }
        let is_var_int = bytes.iter().rev().skip(1).all(|b| b & 0x80 != 0)
            && bytes.iter().last().unwrap() & 0x80 == 0;
        if !is_var_int {
            return Err("Bolt capabilities are not base128 VarInt encoded".into());
        }
        if Self::msb(&bytes).unwrap_or(usize::MAX) >= CAPABILITIES_MAX_BITS {
            return Err("Bolt capabilities are too long (max 63 bits)".into());
        }
        Ok(Self(bytes))
    }

    pub fn raw(&self) -> &[u8] {
        &self.0
    }

    /// index of the most significant (non-zero) bit
    fn msb(bytes: &[u8]) -> Result<usize, &'static str> {
        let mut msb: usize = 0;
        for (i, mut byte) in bytes.iter().copied().enumerate() {
            byte &= 0x7F;
            if byte == 0 {
                continue;
            }
            let leading_zeros: u8 = byte
                .leading_zeros()
                .try_into()
                .expect("max of 8 fits into u8");
            msb = i
                .checked_mul(7)
                .and_then(|i| i.checked_add((8 - leading_zeros).into()))
                .ok_or("Bolt capabilities are too long to count bits")?;
        }
        Ok(msb)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, derive_more::Display)]
pub enum JoltVersion {
    #[display("Jolt v1")]
    V1,
    /// * Fixes temporal types' representation being ambiguous in V1.
    /// * Adds element ids to nodes and relationships.
    #[display("Jolt v2")]
    V2,
    /// * Adds support for vector and unsupported types.
    #[display("Jolt v3")]
    V3,
    #[expect(clippy::doc_markdown, reason = "PackStream is the name of the game")]
    /// * Adds support for UUID (PackStream V2)
    #[display("Jolt v4")]
    V4,
}

impl JoltVersion {
    pub fn parse(s: &str) -> Result<Self, String> {
        let jolt_version =
            i32::from_str(s).map_err(|e| format!("Jolt version must be i32 (found {s:?}): {e}"))?;
        Ok(match jolt_version {
            1 => Self::V1,
            2 => Self::V2,
            3 => Self::V3,
            4 => Self::V4,
            _ => return Err(format!("Unknown jolt version: {s}")),
        })
    }

    pub fn to_str(self) -> &'static str {
        match self {
            Self::V1 => "1",
            Self::V2 => "2",
            Self::V3 => "3",
            Self::V4 => "4",
        }
    }

    /// Make an iterator going over the neighboring versions of this version.
    /// Higher versions have priority over lower versions.
    ///
    /// Example: `V3.proximity_iter()` would yield [V4, V2, V5, V1, V6, V7, ...]
    pub fn proximity_iter(self) -> impl Iterator<Item = Self> {
        JoltProximityIter::new(self)
    }

    fn next(self) -> Option<Self> {
        match self {
            Self::V1 => Some(Self::V2),
            Self::V2 => Some(Self::V3),
            Self::V3 => Some(Self::V4),
            Self::V4 => None,
        }
    }

    fn prev(self) -> Option<Self> {
        match self {
            Self::V1 => None,
            Self::V2 => Some(Self::V1),
            Self::V3 => Some(Self::V2),
            Self::V4 => Some(Self::V3),
        }
    }

    #[cfg(test)]
    pub(crate) fn min_bolt_version(self) -> BoltVersion {
        match self {
            JoltVersion::V1 => BoltVersion::V1,
            JoltVersion::V2 => BoltVersion::V5_0,
            JoltVersion::V3 => BoltVersion::V6_0,
            JoltVersion::V4 => BoltVersion::V6_1,
        }
    }
}

struct JoltProximityIter {
    upper_next: bool,
    upper: Option<JoltVersion>,
    lower: Option<JoltVersion>,
}

impl JoltProximityIter {
    fn new(version: JoltVersion) -> Self {
        Self {
            upper_next: true,
            upper: version.next(),
            lower: version.prev(),
        }
    }
}

impl Iterator for JoltProximityIter {
    type Item = JoltVersion;

    fn next(&mut self) -> Option<Self::Item> {
        if self.upper_next || self.lower.is_none() {
            if let Some(upper) = self.upper {
                self.upper = upper.next();
                self.upper_next = false;
                return Some(upper);
            }
        }
        if let Some(lower) = self.lower {
            self.lower = lower.prev();
            self.upper_next = true;
            return Some(lower);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;

    #[rstest]
    fn test_auto_response(
        #[values(
            BoltVersion::V1,
            BoltVersion::V2,
            BoltVersion::V3,
            BoltVersion::V4_0,
            BoltVersion::V4_1,
            BoltVersion::V4_2,
            BoltVersion::V4_3,
            BoltVersion::V4_4,
            BoltVersion::V5_0,
            BoltVersion::V5_1,
            BoltVersion::V5_2,
            BoltVersion::V5_3,
            BoltVersion::V5_4,
            BoltVersion::V5_5,
            BoltVersion::V5_6,
            BoltVersion::V5_7,
            BoltVersion::V5_8,
            BoltVersion::V6_0,
            BoltVersion::V6_1
        )]
        version: BoltVersion,
        #[values(
            0x00, 0x6A, 0x6B, 0x54, 0x02, 0x0E, 0x0F, 0x10, 0x2F, 0x2F, 0x3F, 0x3F, 0x11, 0x12,
            0x13, 0x66, 0x70, 0x7E, 0x7F, 0x71
        )]
        tag: u8,
    ) {
        let response = version.message_auto_response(tag);
        let Resolvable::Static(response) = response else {
            panic!("Expected static auto response for {version} and tag {tag:#04X}");
        };
        let SerializedBoltMessage { message, data } = response;
        let (message, packed_message) = serialize_message(message);

        assert_eq!(data, packed_message);
        assert_bolt_version(&message, version);
        let meta = extract_success_meta(message);

        assert!(
            meta.is_empty(),
            "Auto response metadata map must be empty for non-HELLO messages"
        );
    }

    #[derive(Debug, Copy, Clone)]
    struct HelloAutoResponseCase {
        version: BoltVersion,
        number_fields: usize,
        has_connection_id: bool,
        has_protocol_version: bool,
        has_ssr_hint: bool,
    }

    #[rstest]
    #[case::v1(HelloAutoResponseCase{
        version: BoltVersion::V1,
        number_fields: 1,
        has_connection_id: false,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v2(HelloAutoResponseCase{
        version: BoltVersion::V2,
        number_fields: 1,
        has_connection_id: false,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v3(HelloAutoResponseCase{
        version: BoltVersion::V3,
        number_fields: 2,
        has_connection_id: true,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v4_0(HelloAutoResponseCase{
        version: BoltVersion::V4_0,
        number_fields: 2,
        has_connection_id: true,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v4_1(HelloAutoResponseCase{
        version: BoltVersion::V4_1,
        number_fields: 2,
        has_connection_id: true,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v4_2(HelloAutoResponseCase{
        version: BoltVersion::V4_2,
        number_fields: 2,
        has_connection_id: true,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v4_3(HelloAutoResponseCase{
        version: BoltVersion::V4_3,
        number_fields: 2,
        has_connection_id: true,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v4_4(HelloAutoResponseCase{
        version: BoltVersion::V4_4,
        number_fields: 2,
        has_connection_id: true,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v5_0(HelloAutoResponseCase{
        version: BoltVersion::V5_0,
        number_fields: 2,
        has_connection_id: true,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v5_1(HelloAutoResponseCase{
        version: BoltVersion::V5_1,
        number_fields: 2,
        has_connection_id: true,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v5_2(HelloAutoResponseCase{
        version: BoltVersion::V5_2,
        number_fields: 2,
        has_connection_id: true,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v5_3(HelloAutoResponseCase{
        version: BoltVersion::V5_3,
        number_fields: 2,
        has_connection_id: true,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v5_4(HelloAutoResponseCase{
        version: BoltVersion::V5_4,
        number_fields: 2,
        has_connection_id: true,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v5_5(HelloAutoResponseCase{
        version: BoltVersion::V5_5,
        number_fields: 2,
        has_connection_id: true,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v5_6(HelloAutoResponseCase{
        version: BoltVersion::V5_6,
        number_fields: 2,
        has_connection_id: true,
        has_protocol_version: false,
        has_ssr_hint: false,
    })]
    #[case::v5_7(HelloAutoResponseCase{
        version: BoltVersion::V5_7,
        number_fields: 3,
        has_connection_id: true,
        has_protocol_version: true,
        has_ssr_hint: false,
    })]
    #[case::v5_8(HelloAutoResponseCase{
        version: BoltVersion::V5_8,
        number_fields: 4,
        has_connection_id: true,
        has_protocol_version: true,
        has_ssr_hint: true,
    })]
    #[case::v6_0(HelloAutoResponseCase{
        version: BoltVersion::V6_0,
        number_fields: 4,
        has_connection_id: true,
        has_protocol_version: true,
        has_ssr_hint: true,
    })]
    #[case::v6_1(HelloAutoResponseCase{
        version: BoltVersion::V6_1,
        number_fields: 4,
        has_connection_id: true,
        has_protocol_version: true,
        has_ssr_hint: true,
    })]
    fn test_hello_auto_response(#[case] case: HelloAutoResponseCase) {
        let hello_tag = case
            .version
            .message_tag_from_request("HELLO")
            .or_else(|| case.version.message_tag_from_request("INIT"))
            .expect("Missing HELLO/INIT message");
        let response = case.version.message_auto_response(hello_tag);

        if case.has_connection_id {
            assert!(matches!(&response, Resolvable::Dynamic { .. }));
        } else {
            assert!(matches!(&response, Resolvable::Static(_)));
        }
        let response = response.resolve();
        let SerializedBoltMessage { message, data } = response.into_owned();
        let (message, packed_message) = serialize_message(message);

        assert_eq!(data, packed_message);
        assert_bolt_version(&message, case.version);
        let mut meta = extract_success_meta(message);
        assert_eq!(
            meta.len(),
            case.number_fields,
            "Auto response metadata map must have the expected number of fields"
        );
        let server = meta
            .swap_remove("server")
            .expect("Auto response metadata map must have a server field")
            .try_into_string()
            .expect("Auto response server field must be a string");
        assert!(
            regex::Regex::new(r"^Neo4j/\d+(\.\d+){1,2}$")
                .unwrap()
                .is_match(&server),
            "Auto response server field has wrong format"
        );
        if case.has_connection_id {
            let connection_id = meta
                .swap_remove("connection_id")
                .expect("Auto response metadata map must have a connection_id field")
                .try_into_string()
                .expect("Auto response connection_id field must be a string");
            assert!(
                regex::Regex::new(r"^bolt-\d+$")
                    .unwrap()
                    .is_match(&connection_id),
                "Connection ID must have the format 'bolt-<number>'"
            );
        }
        if case.has_protocol_version {
            let protocol_version = meta
                .swap_remove("protocol_version")
                .expect("Auto response metadata map must have a protocol_version field")
                .try_into_string()
                .expect("Auto response protocol_version field must be a string");
            assert_eq!(
                protocol_version,
                case.version.to_string(),
                "Protocol version in auto response must match the Bolt version"
            );
        }
        let mut hints = meta.swap_remove("hints").map(|hints| {
            hints
                .try_into_map()
                .expect("Auto response hints field must be a map")
        });
        if case.has_ssr_hint {
            let ssr_hint = hints
                .as_mut()
                .and_then(|hints| hints.swap_remove("ssr.enabled"))
                .expect("Auto response hints map must have an ssr.enabled field if it is expected")
                .try_into_bool()
                .expect("Auto response ssr.enabled hint must be a bool");
            assert!(ssr_hint, "Auto response ssr.enabled hint must be true");
        } else {
            assert!(
                hints.is_none(),
                "Auto response must not have a hints field if ssr hint is not expected"
            );
        }
    }

    fn serialize_message(message: BoltMessage) -> (BoltMessage, Vec<u8>) {
        let message = message
            .into_serialized()
            .expect("Message must be serializable");
        let SerializedBoltMessage {
            message,
            data: packed_message,
        } = message;
        (message, packed_message)
    }

    fn assert_bolt_version(message: &BoltMessage, version: BoltVersion) {
        assert_eq!(
            message.bolt_version, version,
            "Message must have the same Bolt version as the request"
        );
    }

    fn extract_success_meta(message: BoltMessage) -> IndexMap<String, PackStreamValue> {
        let bolt_version = message.bolt_version;
        let success_tag = bolt_version.message_tag_from_response("SUCCESS").unwrap();
        assert_eq!(message.tag, success_tag, "Auto response must be SUCCESS");
        assert_eq!(
            message.fields.len(),
            1,
            "Auto response must have exactly one field (the metadata map)"
        );
        let meta = message.fields.into_iter().next().unwrap();
        meta.try_into_map()
            .expect("Auto response field must be a map")
    }

    #[rstest]
    #[case::v1(
        JoltVersion::V1,
        vec![JoltVersion::V2, JoltVersion::V3, JoltVersion::V4]
    )]
    #[case::v2(
        JoltVersion::V2,
        vec![JoltVersion::V3, JoltVersion::V1, JoltVersion::V4]
    )]
    #[case::v3(
        JoltVersion::V3,
        vec![JoltVersion::V4, JoltVersion::V2, JoltVersion::V1]
    )]
    #[case::v4(
        JoltVersion::V4,
        vec![JoltVersion::V3, JoltVersion::V2, JoltVersion::V1]
    )]
    fn test_jolt_proximity_iter(#[case] version: JoltVersion, #[case] expected: Vec<JoltVersion>) {
        let proximity_versions: Vec<JoltVersion> = version.proximity_iter().collect();
        assert_eq!(proximity_versions, expected);
    }
}
