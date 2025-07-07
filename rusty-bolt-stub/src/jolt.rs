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
}

impl JoltSigil {
    pub(crate) fn from_str(s: &str) -> Option<Self> {
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
            _ => return None,
        })
    }
}
