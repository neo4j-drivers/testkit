use std::borrow::Cow;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::atomic::AtomicI64;

use crate::types::Resolvable;
use crate::values::pack_stream_value::PackStreamValue;
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
            (1, None) | (1, Some(0)) => BoltVersion::V1,
            (2, None) | (2, Some(0)) => BoltVersion::V2,
            (3, None) | (3, Some(0)) => BoltVersion::V3,
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
            _ => return None,
        })
    }

    pub fn major(&self) -> u8 {
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
        }
    }

    pub fn minor(&self) -> u8 {
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
        }
    }

    pub fn jolt_version(&self) -> JoltVersion {
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
        }
    }

    pub fn supports_minor(&self) -> bool {
        self >= &BoltVersion::V4_0
    }

    pub fn supports_range(&self) -> bool {
        self >= &BoltVersion::V4_2
    }

    pub fn max_handshake_manifest_version(&self) -> u8 {
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
        }
    }

    pub fn backwards_equivalent_versions(&self) -> &'static [(u8, u8)] {
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
        }
    }

    pub fn message_tag_from_request(&self, name: &str) -> Option<u8> {
        Some(match name {
            "INIT" if self < &BoltVersion::V3 => 0x01,
            "HELLO" if self >= &BoltVersion::V3 => 0x01,
            "LOGON" if self >= &BoltVersion::V5_1 => 0x6A,
            "LOGOFF" if self >= &BoltVersion::V5_1 => 0x6B,
            "TELEMETRY" if self >= &BoltVersion::V5_4 => 0x54,
            "GOODBYE" => 0x02,
            "ACK_FAILURE" if self < &BoltVersion::V3 => 0x0E,
            "RESET" => 0x0F,
            "RUN" => 0x10,
            "DISCARD_ALL" if self < &BoltVersion::V4_0 => 0x2F,
            "DISCARD" if self >= &BoltVersion::V4_0 => 0x2F,
            "PULL_ALL" if self < &BoltVersion::V4_0 => 0x3F,
            "PULL" if self >= &BoltVersion::V4_0 => 0x3F,
            "BEGIN" => 0x11,
            "COMMIT" => 0x12,
            "ROLLBACK" => 0x13,
            "ROUTE" if self >= &BoltVersion::V4_3 => 0x66,
            _ => return None,
        })
    }

    pub fn message_tag_from_response(&self, name: &str) -> Option<u8> {
        Some(match name {
            "SUCCESS" => 0x70,
            "IGNORED" => 0x7E,
            "FAILURE" => 0x7F,
            "RECORD" => 0x71,
            _ => return None,
        })
    }

    pub fn message_name_from_tag(&self, tag: u8) -> Option<&'static str> {
        Some(match tag {
            0x01 if self < &BoltVersion::V3 => "INIT",
            0x01 if self >= &BoltVersion::V3 => "HELLO",
            0x6A if self >= &BoltVersion::V5_1 => "LOGON",
            0x6B if self >= &BoltVersion::V5_1 => "LOGOFF",
            0x54 if self >= &BoltVersion::V5_4 => "TELEMETRY",
            0x02 => "GOODBYE",
            0x0E if self < &BoltVersion::V3 => "ACK_FAILURE",
            0x0F => "RESET",
            0x10 => "RUN",
            0x2F if self < &BoltVersion::V4_0 => "DISCARD_ALL",
            0x2F if self >= &BoltVersion::V4_0 => "DISCARD",
            0x3F if self < &BoltVersion::V4_0 => "PULL_ALL",
            0x3F if self >= &BoltVersion::V4_0 => "PULL",
            0x11 => "BEGIN",
            0x12 => "COMMIT",
            0x13 => "ROLLBACK",
            0x66 if self >= &BoltVersion::V4_3 => "ROUTE",
            0x70 => "SUCCESS",
            0x7E => "IGNORED",
            0x7F => "FAILURE",
            0x71 => "RECORD",
            _ => return None,
        })
    }

    pub fn message_auto_response(&self, tag: u8) -> Option<Resolvable<(u8, Vec<PackStreamValue>)>> {
        const SUCCESS_TAG: u8 = 0x70;
        let success_meta = match tag {
            0x01 => {
                // INIT/HELLO
                let mut success_map = IndexMap::with_capacity(4);
                success_map.insert(
                    String::from("server"),
                    PackStreamValue::String(String::from(self.server_agent())),
                );
                if self < &BoltVersion::V3 {
                    return Some(Resolvable::Static((
                        SUCCESS_TAG,
                        vec![PackStreamValue::Dict(success_map)],
                    )));
                }
                if self >= &BoltVersion::V5_7 {
                    success_map.insert(
                        String::from("protocol_version"),
                        PackStreamValue::String(self.to_string()),
                    );
                }
                if self >= &BoltVersion::V5_8 {
                    success_map.insert(
                        String::from("hints"),
                        PackStreamValue::Dict(
                            indexmap! {String::from("ssr.enabled") => PackStreamValue::Boolean(true)},
                        ),
                    );
                }
                return Some(Resolvable::Dynamic {
                    func: Box::new(move || {
                        let mut success_map = success_map.clone();
                        static CONNECTION_ID: AtomicI64 = AtomicI64::new(1);
                        let connection_id =
                            CONNECTION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        assert_ne!(connection_id, 0, "Connection ID overflowed");
                        success_map.insert(
                            String::from("connection_id"),
                            PackStreamValue::String(format!("bolt-{connection_id}")),
                        );
                        (SUCCESS_TAG, vec![PackStreamValue::Dict(success_map)])
                    }),
                    repr: format!("Auto-HELLO-SUCCESS @{}:{}", file!(), line!()),
                });
            }
            _ => Default::default(),
        };
        Some(Resolvable::Static((
            SUCCESS_TAG,
            vec![PackStreamValue::Dict(success_meta)],
        )))
    }

    fn server_agent(&self) -> &'static str {
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
        }
    }
}

#[derive(Debug)]
pub(crate) struct BoltCapabilities(Vec<u8>);

impl PartialEq for BoltCapabilities {
    fn eq(&self, other: &Self) -> bool {
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
    pub(crate) fn from_bytes(mut bytes: Vec<u8>) -> Result<Self, Cow<'static, str>> {
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

    pub(crate) fn raw(&self) -> &[u8] {
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

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum JoltVersion {
    V1,
    V2,
}

impl JoltVersion {
    pub(crate) fn parse(s: &str) -> Result<Self, String> {
        let jolt_version =
            i32::from_str(s).map_err(|e| format!("Jolt version must be i32 (found {s:?}): {e}"))?;
        Ok(match jolt_version {
            1 => Self::V1,
            2 => Self::V2,
            _ => return Err(format!("Unknown jolt version: {s}")),
        })
    }
}
