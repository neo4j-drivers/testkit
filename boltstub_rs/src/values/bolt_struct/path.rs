use std::borrow::{Borrow, Cow};
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::rc::Rc;

use indexmap::IndexMap;
use itertools::Itertools;
use serde_json::Value as JsonValue;

use crate::bolt_version::JoltVersion;
use crate::jolt::JoltSigil;
use crate::parse_error::ParseError;
use crate::parser::ActorConfig;
use crate::values::bolt_struct::_common::element_id::ElementIdExt;
use crate::values::bolt_struct::_common::fmt_jolt_sigil_and_map;
use crate::values::bolt_struct::_parsing::{check_last_pack_stream_field, next_pack_stream_field};
use crate::values::bolt_struct::node::JoltNodeData;
use crate::values::bolt_struct::relationship::{
    JoltRelationshipData, JoltRelationshipElementIdExt, JoltUnboundRelationship,
    JoltUnboundRelationshipData,
};
use crate::values::bolt_struct::{JoltNode, JoltRelationship, TAG_PATH};
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

const SIGIL: &str = JoltSigil::Path.str();

#[derive(Debug, Clone)]
pub(crate) struct JoltPathData<'a> {
    pub(crate) nodes: Vec<JoltNodeData<'a>>,
    pub(crate) relationships: Vec<JoltUnboundRelationshipData<'a>>,
    pub(crate) indices: Vec<i64>,
}

impl JoltPathData<'static> {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        let JsonValue::Array(fields) = v else {
            return Err(ParseError::new(format!(
                "Expected array after sigil \"{SIGIL}\", but found {v:?}"
            )));
        };
        if fields.len() % 2 != 1 {
            return Err(ParseError::new(format!(
                "Expected odd number of fields after sigil \"{SIGIL}\", but found {}",
                fields.len()
            )));
        }
        if i64::try_from(fields.len() / 2).is_err() {
            return Err(ParseError::new("Path can contain at most i64::MAX nodes"));
        }
        let mut fields = fields.into_iter().enumerate().peekable();
        let mut nodes = Vec::with_capacity(fields.len() / 2 + 1);
        let mut relationships = Vec::with_capacity(fields.len() / 2);

        let (i, field) = fields.next().expect("checked non-empty above");
        nodes.push(Self::next_node(i, field, jolt_version, config)?.data);

        while let Some((i, field)) = fields.next() {
            let relationship = Self::next_relationship(i, field, jolt_version, config)?;
            relationships.push(relationship.data);
            let (i, field) = fields.next().expect("checked uneven size above");
            let node = Self::next_node(i, field, jolt_version, config)?;
            nodes.push(node.data);
        }
        match ElementIdExt::uses_element_id(jolt_version) {
            false => Self::encode_indices(&IdPathIndexer::new(), nodes, relationships),
            true => Self::encode_indices(
                &ElementIdPathIndexer::new(jolt_version),
                nodes,
                relationships,
            ),
        }
    }

    fn next_node(
        i: usize,
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<JoltNode<'static>, ParseError> {
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
    ) -> Result<JoltRelationship<'static>, ParseError> {
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
        indexer: &P,
        nodes: Vec<JoltNodeData<'static>>,
        relationships: Vec<JoltRelationshipData<'static>>,
    ) -> Result<Self, ParseError> {
        assert_eq!(relationships.len() + 1, nodes.len());
        i64::try_from(nodes.len() + relationships.len())
            .expect("How does this even fit into your memory?!?");

        let nodes = nodes.into_iter().map(Rc::new).collect::<Vec<_>>();
        let relationships = relationships.into_iter().map(Rc::new).collect::<Vec<_>>();
        let mut node_indices = Vec::with_capacity(nodes.len() - 1);
        let mut relationship_indices = Vec::with_capacity(relationships.len());

        let mut unique_nodes = IndexMap::with_capacity(nodes.len());
        for (i, node) in nodes.iter().enumerate() {
            let idx = indexer.node_index(node);
            if let Some(other_node) = unique_nodes.get(idx) {
                if other_node != node {
                    return Err(ParseError::new(format!(
                        "Path contains multiple nodes with the same {} {idx:?} \
                        but different values",
                        P::name()
                    )));
                }
            } else {
                unique_nodes.insert(idx.to_owned(), Rc::clone(node));
            }
            if i != 0 {
                let node_idx = unique_nodes.get_index_of(idx).expect("inserted above");
                let node_idx = i64::try_from(node_idx).expect("parse limits nodes count to fit");
                node_indices.push(node_idx);
            }
        }
        let mut unique_relationships = IndexMap::with_capacity(relationships.len());
        for (i, relationship) in relationships.iter().enumerate() {
            let idx = indexer.relationship_index(relationship);
            if let Some(other_relationship) = unique_relationships.get(idx) {
                if other_relationship != relationship {
                    return Err(ParseError::new(format!(
                        "Path contains multiple relationships with the same {} {idx:?} \
                        but different values",
                        P::name()
                    )));
                }
            } else {
                unique_relationships.insert(idx.to_owned(), Rc::clone(relationship));
            }
            let prev_node = &nodes[i];
            let prev_node_idx = indexer.node_index(prev_node);
            let start_node_idx = indexer.relationship_start_index(relationship);
            let next_node = &nodes[i + 1];
            let next_node_idx = indexer.node_index(next_node);
            let end_node_idx = indexer.relationship_end_index(relationship);
            let rel_idx = unique_relationships
                .get_index_of(idx)
                .expect("inserted above");
            let positive_index =
                i64::try_from(rel_idx).expect("parse limits relationships count to fit") + 1;
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
            .interleave(node_indices)
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

        Ok(JoltPathData {
            nodes,
            relationships,
            indices,
        })
    }
}

impl JoltPathData<'_> {
    pub(super) fn jolt_fmt(
        &self,
        jolt_version_data: JoltVersion,
        jolt_version_ctx: JoltVersion,
    ) -> impl Display + '_ {
        struct JoltFormatter<'a> {
            data: &'a JoltPathData<'a>,
            jolt_version_data: JoltVersion,
            jolt_version_ctx: JoltVersion,
        }

        impl Display for JoltFormatter<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                fmt_jolt_sigil_and_map(
                    f,
                    SIGIL,
                    self.jolt_version_data,
                    self.jolt_version_ctx,
                    Some(("[", "]")),
                    |f| {
                        let mut prev_node = &self.data.nodes[0];
                        prev_node
                            .jolt_fmt(self.jolt_version_data, self.jolt_version_data)
                            .fmt(f)?;
                        let mut indices = self.data.indices.iter();
                        while let Some(rel_idx) = indices.next() {
                            let rel_idx_usize = usize::try_from(rel_idx.unsigned_abs())
                                .expect("is_valid asserts that all indexes fit in usize")
                                - 1;
                            let node_idx =
                                indices.next().expect("checked even size in from_struct");
                            let node_idx = usize::try_from(*node_idx)
                                .expect("is_valid asserts that all indexes fit in usize");
                            let next_node = &self.data.nodes[node_idx];
                            let relationship = &self.data.relationships[rel_idx_usize];
                            let mut relationship = JoltRelationshipData {
                                id: relationship.id,
                                start_node_id: prev_node.id,
                                rel_type: Cow::Borrowed(&*relationship.rel_type),
                                end_node_id: next_node.id,
                                properties: Cow::Borrowed(&*relationship.properties),
                                element_id_ext: ElementIdExt::new_lazy(
                                    self.jolt_version_data,
                                    || JoltRelationshipElementIdExt {
                                        element_id: Cow::Borrowed(
                                            relationship
                                                .element_id
                                                .inner(self.jolt_version_data)
                                                .as_ref()
                                                .expect("either all or no element_id are present"),
                                        ),
                                        start_node_element_id: Cow::Borrowed(
                                            prev_node
                                                .element_id
                                                .inner(self.jolt_version_data)
                                                .as_ref()
                                                .expect("either all or no element_id are present"),
                                        ),
                                        end_node_element_id: Cow::Borrowed(
                                            next_node
                                                .element_id
                                                .inner(self.jolt_version_data)
                                                .as_ref()
                                                .expect("either all or no element_id are present"),
                                        ),
                                    },
                                ),
                            };
                            if *rel_idx < 0 {
                                relationship.flip_direction();
                            }
                            f.write_str(", ")?;
                            relationship
                                .jolt_fmt(self.jolt_version_data, self.jolt_version_data)
                                .fmt(f)?;
                            f.write_str(", ")?;
                            next_node
                                .jolt_fmt(self.jolt_version_data, self.jolt_version_data)
                                .fmt(f)?;
                            prev_node = next_node;
                        }
                        Ok(())
                    },
                )
            }
        }

        JoltFormatter {
            data: self,
            jolt_version_data,
            jolt_version_ctx,
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
        self.indices.len().is_multiple_of(2)
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
}

#[derive(Debug, Clone)]
pub(crate) struct JoltPath<'a> {
    pub(crate) data: JoltPathData<'a>,
    pub(crate) jolt_version: JoltVersion,
}

impl JoltPath<'static> {
    pub(crate) fn parse(
        v: JsonValue,
        jolt_version: JoltVersion,
        config: &ActorConfig,
    ) -> Result<Self, ParseError> {
        JoltPathData::parse(v, jolt_version, config).map(|data| Self { data, jolt_version })
    }
}

impl<'a> JoltPath<'a> {
    pub(crate) fn into_struct(self) -> PackStreamStruct {
        let nodes = PackStreamValue::List(
            self.data
                .nodes
                .into_iter()
                .map(|data| JoltNode::new(data, self.jolt_version).into_struct())
                .map(PackStreamValue::Struct)
                .collect(),
        );
        let relationships = PackStreamValue::List(
            self.data
                .relationships
                .into_iter()
                .map(|data| JoltUnboundRelationship::new(data, self.jolt_version).into_struct())
                .map(PackStreamValue::Struct)
                .collect(),
        );
        let indices = PackStreamValue::List(
            self.data
                .indices
                .into_iter()
                .map(PackStreamValue::Integer)
                .collect(),
        );
        PackStreamStruct {
            tag: TAG_PATH,
            fields: vec![nodes, relationships, indices],
        }
    }

    pub(super) fn from_struct(s: &'a PackStreamStruct, jolt_version: JoltVersion) -> Option<Self> {
        if s.tag != TAG_PATH {
            return None;
        }
        let mut fields = s.fields.iter();
        let nodes = next_pack_stream_field::<&[PackStreamValue]>(&mut fields)?
            .iter()
            .map(|v| Some(JoltNode::from_struct(v.as_struct()?, jolt_version)?.data))
            .collect::<Option<_>>()?;
        let relationships = next_pack_stream_field::<&[PackStreamValue]>(&mut fields)?
            .iter()
            .map(|v| Some(JoltUnboundRelationship::from_struct(v.as_struct()?, jolt_version)?.data))
            .collect::<Option<_>>()?;
        let indices: Vec<_> = next_pack_stream_field(&mut fields)?;
        if !check_last_pack_stream_field(&mut fields) {
            return None;
        }
        let data = JoltPathData {
            nodes,
            relationships,
            indices,
        };
        data.is_valid().then(|| Self { data, jolt_version })
    }

    pub(super) fn jolt_fmt(&self, jolt_version: JoltVersion) -> impl Display + '_ {
        self.data.jolt_fmt(self.jolt_version, jolt_version)
    }
}

trait PathIndexer {
    type Index: Hash + Eq + Debug + Borrow<Self::IndexBorrow>;
    type IndexBorrow: Hash + Eq + Debug + ToOwned<Owned = Self::Index> + ?Sized;
    fn name() -> &'static str;
    fn node_index<'a>(&self, node: &'a JoltNodeData<'a>) -> &'a Self::IndexBorrow;
    fn relationship_index<'a>(
        &self,
        relationship: &'a JoltRelationshipData<'a>,
    ) -> &'a Self::IndexBorrow;
    fn relationship_start_index<'a>(
        &self,
        relationship: &'a JoltRelationshipData<'a>,
    ) -> &'a Self::IndexBorrow;
    fn relationship_end_index<'a>(
        &self,
        relationship: &'a JoltRelationshipData<'a>,
    ) -> &'a Self::IndexBorrow;
}

struct IdPathIndexer;
impl IdPathIndexer {
    fn new() -> Self {
        Self
    }
}
impl PathIndexer for IdPathIndexer {
    type Index = i64;
    type IndexBorrow = i64;
    fn name() -> &'static str {
        "id"
    }
    fn node_index<'a>(&self, node: &'a JoltNodeData<'a>) -> &'a Self::IndexBorrow {
        &node.id
    }
    fn relationship_index<'a>(
        &self,
        relationship: &'a JoltRelationshipData<'a>,
    ) -> &'a Self::IndexBorrow {
        &relationship.id
    }
    fn relationship_start_index<'a>(
        &self,
        relationship: &'a JoltRelationshipData<'a>,
    ) -> &'a Self::IndexBorrow {
        &relationship.start_node_id
    }
    fn relationship_end_index<'a>(
        &self,
        relationship: &'a JoltRelationshipData<'a>,
    ) -> &'a Self::IndexBorrow {
        &relationship.end_node_id
    }
}

struct ElementIdPathIndexer {
    jolt_version: JoltVersion,
}
impl ElementIdPathIndexer {
    fn new(jolt_version: JoltVersion) -> Self {
        assert!(
            ElementIdExt::uses_element_id(jolt_version),
            "cannot use ElementIdPathIndexer with \
             {jolt_version:?}: does not support element ids"
        );
        Self { jolt_version }
    }
}
impl PathIndexer for ElementIdPathIndexer {
    type Index = String;
    type IndexBorrow = str;
    fn name() -> &'static str {
        "element id"
    }
    fn node_index<'a>(&self, node: &'a JoltNodeData<'a>) -> &'a Self::IndexBorrow {
        node.element_id
            .inner(self.jolt_version)
            .map(std::ops::Deref::deref)
            .expect("availability checked in constructor")
    }
    fn relationship_index<'a>(
        &self,
        relationship: &'a JoltRelationshipData<'a>,
    ) -> &'a Self::IndexBorrow {
        &relationship
            .element_id_ext
            .inner(self.jolt_version)
            .as_ref()
            .expect("availability checked in constructor")
            .element_id
    }
    fn relationship_start_index<'a>(
        &self,
        relationship: &'a JoltRelationshipData<'a>,
    ) -> &'a Self::IndexBorrow {
        &relationship
            .element_id_ext
            .inner(self.jolt_version)
            .as_ref()
            .expect("availability checked in constructor")
            .start_node_element_id
    }
    fn relationship_end_index<'a>(
        &self,
        relationship: &'a JoltRelationshipData<'a>,
    ) -> &'a Self::IndexBorrow {
        &relationship
            .element_id_ext
            .inner(self.jolt_version)
            .as_ref()
            .expect("availability checked in constructor")
            .end_node_element_id
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;

    use super::*;
    use crate::bolt_version::{BoltCapabilities, BoltVersion};

    fn no_element_id(jolt_version: JoltVersion) -> ElementIdExt<Cow<'static, str>> {
        assert!(
            !ElementIdExt::uses_element_id(jolt_version),
            "{jolt_version:?} uses element_id"
        );
        ElementIdExt::new(jolt_version, "".into())
    }

    #[test]
    fn test_parse_path() {
        let config = ActorConfig {
            bolt_version: BoltVersion::V4_4,
            bolt_version_raw: (4, 4),
            bolt_capabilities: BoltCapabilities::default(),
            handshake_manifest_version: None,
            handshake: None,
            handshake_response: None,
            handshake_delay: None,
            allow_restart: false,
            allow_concurrent: false,
            auto_responses: IndexMap::default(),
            py_lines: vec![],
        };

        let input = r#"[{"()": [1, ["l"], {}]}, {"->": [2, 1, "RELATES_TO", 3, {}]}, {"()": [3, ["l"], {}]}, {"->": [4, 3, "RELATES_TO", 1, {}]}, {"()": [1, ["l"], {}]}]"#;
        let parsed = serde_json::from_str::<JsonValue>(input).unwrap();
        let parsed = JoltPath::parse(parsed, JoltVersion::V1, &config).unwrap();
        let JoltPath { data, jolt_version } = parsed;
        let JoltPathData {
            nodes,
            relationships,
            indices,
        } = data;
        assert_eq!(jolt_version, JoltVersion::V1);
        assert_eq!(
            nodes,
            vec![
                JoltNodeData {
                    id: 1,
                    labels: vec![Cow::Owned(String::from("l"))],
                    properties: Cow::Owned(IndexMap::default()),
                    element_id: no_element_id(jolt_version),
                },
                JoltNodeData {
                    id: 3,
                    labels: vec![Cow::Owned(String::from("l"))],
                    properties: Cow::Owned(IndexMap::default()),
                    element_id: no_element_id(jolt_version),
                },
            ]
        );
        assert_eq!(
            relationships,
            vec![
                JoltUnboundRelationshipData {
                    id: 2,
                    rel_type: Cow::Owned(String::from("RELATES_TO")),
                    properties: Cow::Owned(IndexMap::default()),
                    element_id: no_element_id(jolt_version),
                },
                JoltUnboundRelationshipData {
                    id: 4,
                    rel_type: Cow::Owned(String::from("RELATES_TO")),
                    properties: Cow::Owned(IndexMap::default()),
                    element_id: no_element_id(jolt_version),
                },
            ]
        );
        assert_eq!(indices, vec![1, 1, 2, 0]);
    }
}
