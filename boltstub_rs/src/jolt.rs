use crate::bolt_version::JoltVersion;

#[derive(Debug, Copy, Clone)]
pub(crate) enum JoltSigil {
    Bool,
    Integer,
    Float,
    String,
    Bytes,
    List,
    Dict,
    Temporal,
    Spatial,
    Node,
    RelationshipForward,
    RelationshipBackward,
    Path,
    Vector,
    Uuid,
    UnsupportedType,
}

impl JoltSigil {
    pub(crate) fn from_str(s: &str, jolt_version: JoltVersion) -> Option<Self> {
        Some(match s {
            "?" => Self::Bool,
            "Z" => Self::Integer,
            "R" => Self::Float,
            "U" => Self::String,
            "#" => Self::Bytes,
            "[]" => Self::List,
            "{}" => Self::Dict,
            "T" => Self::Temporal,
            "@" => Self::Spatial,
            "()" => Self::Node,
            "->" => Self::RelationshipForward,
            "<-" => Self::RelationshipBackward,
            ".." => Self::Path,
            "V" if jolt_version >= JoltVersion::V3 => Self::Vector,
            "UT" if jolt_version >= JoltVersion::V3 => Self::UnsupportedType,
            "UU" if jolt_version >= JoltVersion::V4 => Self::Uuid,
            _ => return None,
        })
    }

    pub(crate) const fn str(self) -> &'static str {
        match self {
            Self::Bool => "?",
            Self::Integer => "Z",
            Self::Float => "R",
            Self::String => "U",
            Self::Bytes => "#",
            Self::List => "[]",
            Self::Dict => "{}",
            Self::Temporal => "T",
            Self::Spatial => "@",
            Self::Node => "()",
            Self::RelationshipForward => "->",
            Self::RelationshipBackward => "<-",
            Self::Path => "..",
            Self::Vector => "V",
            Self::Uuid => "UU",
            Self::UnsupportedType => "UT",
        }
    }
}
