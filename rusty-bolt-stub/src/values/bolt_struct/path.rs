use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::rc::Rc;

use indexmap::IndexMap;
use itertools::Itertools;
use serde_json::Value as JsonValue;

use crate::bolt_version::JoltVersion;
use crate::parse_error::ParseError;
use crate::parser::ActorConfig;
use crate::values::bolt_struct::_parsing::{check_last_pack_stream_field, next_pack_stream_field};
use crate::values::bolt_struct::node::BoltNode;
use crate::values::bolt_struct::relationship::{
    BoltRelationship, BoltRelationshipElementIdExt, BoltUnboundRelationship,
};
use crate::values::bolt_struct::{JoltNode, JoltRelationship, TAG_PATH, TAG_UNBOUND_RELATIONSHIP};
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

#[derive(Debug, Clone)]
pub(crate) struct JoltPath {
    pub(crate) nodes: Vec<JoltNode>,
    pub(crate) relationships: Vec<JoltUnboundRelationship>,
    pub(crate) indices: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoltUnboundRelationship {
    pub(crate) id: i64,
    pub(crate) rel_type: String,
    pub(crate) properties: IndexMap<String, PackStreamValue>,
    pub(crate) element_id: Option<String>,
}

impl From<JoltRelationship> for JoltUnboundRelationship {
    fn from(value: JoltRelationship) -> Self {
        Self {
            id: value.id,
            rel_type: value.rel_type,
            properties: value.properties,
            element_id: value.element_id_ext.map(|ext| ext.element_id),
        }
    }
}

impl JoltUnboundRelationship {
    pub(crate) fn into_struct(self) -> PackStreamStruct {
        let mut fields = Vec::with_capacity(4);
        fields.extend([
            PackStreamValue::Integer(self.id),
            PackStreamValue::String(self.rel_type),
            PackStreamValue::Dict(self.properties),
        ]);
        if let Some(element_id) = self.element_id {
            fields.push(PackStreamValue::String(element_id));
        }
        PackStreamStruct {
            tag: TAG_UNBOUND_RELATIONSHIP,
            fields,
        }
    }
}

impl JoltPath {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(format!(
                "Expected array after sigil \"..\", but found {v:?}"
            )));
        };
        if fields.len() % 2 != 1 {
            return Err(ParseError::new(format!(
                "Expected odd number of fields after sigil \"..\", but found {}",
                fields.len()
            )));
        }
        let mut fields = fields.into_iter().enumerate().peekable();
        let mut nodes = Vec::with_capacity(fields.len() / 2 + 1);
        let mut relationships = Vec::with_capacity(fields.len() / 2);

        let (i, field) = fields.next().expect("checked non-empty above");
        nodes.push(Self::next_node(i, field, jolt_version, config)?);

        while let Some((i, field)) = fields.next() {
            let relationship = Self::next_relationship(i, field, jolt_version, config)?;
            relationships.push(relationship);
            let (i, field) = fields.next().expect("checked uneven size above");
            let node = Self::next_node(i, field, jolt_version, config)?;
            nodes.push(node);
        }
        match jolt_version {
            JoltVersion::V1 => Self::encode_indices::<IdPathIndexer>(nodes, relationships),
            JoltVersion::V2 => Self::encode_indices::<ElementIdPathIndexer>(nodes, relationships),
        }
    }

    fn next_node(
        i: usize,
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<JoltNode, ParseError> {
        fn not_node(i: usize, v: impl Debug) -> ParseError {
            ParseError::new(format!(
                "Expected path entry at index {i} to be a Jolt node, but found {v:?}"
            ))
        }

        let JsonValue::Object(v) = v else {
            return Err(not_node(i, v));
        };
        if v.len() != 1 {
            return Err(not_node(i, v));
        }
        if v.keys().next().expect("checked not-empty above") != "()" {
            return Err(not_node(i, v));
        }
        JoltNode::parse(
            v.into_values().next().expect("checked non-empty above"),
            jolt_version,
            config,
        )
    }

    fn next_relationship(
        i: usize,
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<JoltRelationship, ParseError> {
        fn not_relationship(i: usize, v: impl Debug) -> ParseError {
            ParseError::new(format!(
                "Expected path entry at index {i} to be a Jolt relationship, but found {v:?}"
            ))
        }

        let JsonValue::Object(v) = v else {
            return Err(not_relationship(i, v));
        };
        if v.len() != 1 {
            return Err(not_relationship(i, v));
        }
        let reverse = match &**v.keys().next().expect("checked non-empty above") {
            "->" => false,
            "<-" => true,
            _ => return Err(not_relationship(i, v)),
        };
        let mut relationship = JoltRelationship::parse(
            v.into_values().next().expect("checked non-empty above"),
            jolt_version,
            config,
        )?;
        if reverse {
            relationship.flip_direction();
        }
        Ok(relationship)
    }

    fn encode_indices<P: PathIndexer>(
        nodes: Vec<JoltNode>,
        relationships: Vec<JoltRelationship>,
    ) -> Result<Self, ParseError> {
        assert_eq!(relationships.len() + 1, nodes.len());
        i64::try_from(nodes.len() + relationships.len())
            .expect("How does even fit into your memory?!?");

        let nodes = nodes.into_iter().map(Rc::new).collect::<Vec<_>>();
        let relationships = relationships.into_iter().map(Rc::new).collect::<Vec<_>>();
        let mut node_indices = Vec::with_capacity(nodes.len() - 1);
        let mut relationship_indices = Vec::with_capacity(relationships.len());

        let mut unique_nodes = IndexMap::with_capacity(nodes.len());
        for (i, node) in nodes.iter().enumerate() {
            let idx = P::node_index(node);
            if let Some(other_node) = unique_nodes.get(idx) {
                if other_node != node {
                    return Err(ParseError::new(format!(
                        "Path contains multiple nodes with the same {} {idx:?} \
                        but different values",
                        P::name()
                    )));
                }
            } else {
                unique_nodes.insert(idx.clone(), Rc::clone(node));
            }
            if i != 0 {
                node_indices.push(unique_nodes.get_index_of(idx).expect("inserted above") as i64);
            }
        }
        let mut unique_relationships = IndexMap::with_capacity(relationships.len());
        for (i, relationship) in relationships.iter().enumerate() {
            let idx = P::relationship_index(relationship);
            if let Some(other_relationship) = unique_relationships.get(idx) {
                if other_relationship != relationship {
                    return Err(ParseError::new(format!(
                        "Path contains multiple relationships with the same {} {idx:?} \
                        but different values",
                        P::name()
                    )));
                }
            } else {
                unique_relationships.insert(idx.clone(), Rc::clone(relationship));
            }
            let prev_node = &nodes[i];
            let prev_node_idx = P::node_index(prev_node);
            let start_node_idx = P::relationship_start_index(relationship);
            let next_node = &nodes[i + 1];
            let next_node_idx = P::node_index(next_node);
            let end_node_idx = P::relationship_end_index(relationship);
            let positive_index = unique_relationships
                .get_index_of(idx)
                .expect("inserted above") as i64
                + 1;
            if (start_node_idx, end_node_idx) == (prev_node_idx, next_node_idx) {
                relationship_indices.push(positive_index);
            } else if (start_node_idx, end_node_idx) == (next_node_idx, prev_node_idx) {
                relationship_indices.push(-positive_index);
            } else {
                return Err(ParseError::new(format!(
                    "Path relationship at position {} does not connect the previous and next nodes",
                    i * 2 + 1,
                )));
            }
        }

        let indices = relationship_indices
            .into_iter()
            .interleave(node_indices.into_iter())
            .collect();

        drop(nodes);
        let nodes = unique_nodes
            .into_values()
            .map(|rc| Rc::into_inner(rc).expect("dropped all other owners"))
            .collect();
        drop(relationships);
        let relationships = unique_relationships
            .into_values()
            .map(|rc| Rc::into_inner(rc).expect("dropped all other owners"))
            .map(Into::into)
            .collect();

        Ok(Self {
            nodes,
            relationships,
            indices,
        })
    }

    pub(crate) fn into_struct(self) -> PackStreamStruct {
        let nodes = PackStreamValue::List(
            self.nodes
                .into_iter()
                .map(JoltNode::into_struct)
                .map(PackStreamValue::Struct)
                .collect(),
        );
        let relationships = PackStreamValue::List(
            self.relationships
                .into_iter()
                .map(JoltUnboundRelationship::into_struct)
                .map(PackStreamValue::Struct)
                .collect(),
        );
        let indices = PackStreamValue::List(
            self.indices
                .into_iter()
                .map(PackStreamValue::Integer)
                .collect(),
        );
        PackStreamStruct {
            tag: TAG_PATH,
            fields: vec![nodes, relationships, indices],
        }
    }
}

trait PathIndexer {
    type Index: Hash + Eq + Debug + Clone;
    fn name() -> &'static str;
    fn node_index(node: &JoltNode) -> &Self::Index;
    fn relationship_index(relationship: &JoltRelationship) -> &Self::Index;
    fn relationship_start_index(relationship: &JoltRelationship) -> &Self::Index;
    fn relationship_end_index(relationship: &JoltRelationship) -> &Self::Index;
}

struct IdPathIndexer;
impl PathIndexer for IdPathIndexer {
    type Index = i64;
    fn name() -> &'static str {
        "id"
    }
    fn node_index<'a>(node: &JoltNode) -> &Self::Index {
        &node.id
    }
    fn relationship_index(relationship: &JoltRelationship) -> &Self::Index {
        &relationship.id
    }
    fn relationship_start_index(relationship: &JoltRelationship) -> &Self::Index {
        &relationship.start_node_id
    }
    fn relationship_end_index(relationship: &JoltRelationship) -> &Self::Index {
        &relationship.end_node_id
    }
}

struct ElementIdPathIndexer;
impl PathIndexer for ElementIdPathIndexer {
    type Index = String;
    fn name() -> &'static str {
        "element id"
    }
    fn node_index(node: &JoltNode) -> &Self::Index {
        node.element_id
            .as_ref()
            .expect("cannot user ElementIdPathIndexer without element ids")
    }
    fn relationship_index(relationship: &JoltRelationship) -> &Self::Index {
        &relationship
            .element_id_ext
            .as_ref()
            .expect("cannot user ElementIdPathIndexer without element ids")
            .element_id
    }
    fn relationship_start_index(relationship: &JoltRelationship) -> &Self::Index {
        &relationship
            .element_id_ext
            .as_ref()
            .expect("cannot user ElementIdPathIndexer without element ids")
            .start_node_element_id
    }
    fn relationship_end_index(relationship: &JoltRelationship) -> &Self::Index {
        &relationship
            .element_id_ext
            .as_ref()
            .expect("cannot user ElementIdPathIndexer without element ids")
            .end_node_element_id
    }
}

#[derive(Debug, Clone)]
pub(super) struct BoltPath<'a> {
    pub(super) nodes: Vec<BoltNode<'a>>,
    pub(super) relationships: Vec<BoltUnboundRelationship<'a>>,
    pub(super) indices: Vec<i64>,
}

impl<'a> BoltPath<'a> {
    pub(super) fn from_struct(s: &'a PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        if s.tag != TAG_PATH {
            return None;
        }
        let mut fields = s.fields.iter();
        let nodes = next_pack_stream_field::<&[PackStreamValue]>(&mut fields)?
            .iter()
            .map(|v| BoltNode::from_struct(v.as_struct()?, jolt_version))
            .collect::<Option<_>>()?;
        let relationships = next_pack_stream_field::<&[PackStreamValue]>(&mut fields)?
            .iter()
            .map(|v| BoltUnboundRelationship::from_struct(v.as_struct()?, jolt_version))
            .collect::<Option<_>>()?;
        let indices: Vec<_> = next_pack_stream_field(&mut fields)?;
        if !check_last_pack_stream_field(&mut fields) {
            return None;
        }
        let this = Self {
            nodes,
            relationships,
            indices,
        };
        match this.is_valid() {
            false => None,
            true => Some(this),
        }
    }

    fn is_valid(&self) -> bool {
        let Some(rels_count) = i64::try_from(self.relationships.len()).ok() else {
            return false;
        };
        let Some(nodes_count) = i64::try_from(self.nodes.len()).ok() else {
            return false;
        };
        if nodes_count == 0 {
            return false;
        }
        self.indices.len() % 2 == 0
            && self
                .indices
                .iter()
                .step_by(2)
                .all(|&i| i != 0 && (1..=rels_count).contains(&i.abs()))
            && self
                .indices
                .iter()
                .skip(1)
                .step_by(2)
                .all(|i| (0..nodes_count).contains(i))
    }

    pub(super) fn jolt_fmt(&self, jolt_version: JoltVersion) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            this: &'a BoltPath<'a>,
            jolt_version: JoltVersion,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(r#"{"..": ["#)?;
                let mut prev_node = &self.this.nodes[0];
                prev_node.jolt_fmt(self.jolt_version).fmt(f)?;
                let mut indices = self.this.indices.iter();
                while let Some(rel_idx) = indices.next() {
                    let node_idx = indices.next().expect("checked even size in from_struct");
                    let next_node = &self.this.nodes[*node_idx as usize];
                    let relationship =
                        &self.this.relationships[rel_idx.unsigned_abs() as usize - 1];
                    let mut relationship = BoltRelationship {
                        id: relationship.id,
                        start_node_id: prev_node.id,
                        rel_type: relationship.rel_type,
                        end_node_id: next_node.id,
                        properties: relationship.properties,
                        element_id_ext: relationship.element_id.map(|element_id| {
                            let start_node_element_id = prev_node.element_id.expect(
                                "from_struct asserts that either all or no element_id are present",
                            );
                            let end_node_element_id = next_node.element_id.expect(
                                "from_struct asserts that either all or no element_id are present",
                            );
                            BoltRelationshipElementIdExt {
                                element_id,
                                start_node_element_id,
                                end_node_element_id,
                            }
                        }),
                    };
                    if *rel_idx < 0 {
                        relationship.flip_direction()
                    }
                    f.write_str(", ")?;
                    relationship.jolt_fmt(self.jolt_version).fmt(f)?;
                    f.write_str(", ")?;
                    next_node.jolt_fmt(self.jolt_version).fmt(f)?;
                    prev_node = next_node;
                }

                f.write_str(r#"]}"#)
            }
        }

        JoltFormatter {
            this: self,
            jolt_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bolt_version::BoltVersion;

    #[test]
    fn test_parse_path() {
        let config = ActorConfig {
            bolt_version: BoltVersion::V4_4,
            bolt_version_raw: (4, 4),
            bolt_capabilities: Default::default(),
            handshake_manifest_version: None,
            handshake: None,
            handshake_response: None,
            handshake_delay: None,
            allow_restart: false,
            allow_concurrent: false,
            auto_responses: Default::default(),
            py_lines: vec![],
        };

        let input = r#"[{"()": [1, ["l"], {}]}, {"->": [2, 1, "RELATES_TO", 3, {}]}, {"()": [3, ["l"], {}]}, {"->": [4, 3, "RELATES_TO", 1, {}]}, {"()": [1, ["l"], {}]}]"#;
        let parsed = serde_json::from_str::<JsonValue>(input).unwrap();
        let JoltPath {
            nodes,
            relationships,
            indices,
        } = JoltPath::parse(parsed, JoltVersion::V1, &config).unwrap();
        assert_eq!(
            nodes,
            vec![
                JoltNode {
                    id: 1,
                    labels: vec![String::from("l")],
                    properties: Default::default(),
                    element_id: None,
                },
                JoltNode {
                    id: 3,
                    labels: vec![String::from("l")],
                    properties: Default::default(),
                    element_id: None,
                },
            ]
        );
        assert_eq!(
            relationships,
            vec![
                JoltUnboundRelationship {
                    id: 2,
                    rel_type: String::from("RELATES_TO"),
                    properties: Default::default(),
                    element_id: None,
                },
                JoltUnboundRelationship {
                    id: 4,
                    rel_type: String::from("RELATES_TO"),
                    properties: Default::default(),
                    element_id: None,
                },
            ]
        );
        assert_eq!(indices, vec![1, 1, 2, 0]);
    }
}
