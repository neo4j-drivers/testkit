use std::borrow::Cow;
use std::cell::LazyCell;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::result::Result as StdResult;
use std::str::FromStr;
use std::time::Duration;

use anyhow::anyhow;
use indexmap::IndexMap;
use itertools::Itertools;
use log::{trace, warn};
use regex::Regex;
use serde_json::{Deserializer, Map as JsonMap, Value as JsonValue};

use crate::bang_line::BangLine;
use crate::bolt_version::{BoltCapabilities, BoltVersion, JoltVersion};
use crate::context::Context;
use crate::error::script_excerpt;
use crate::ext::serde_json as serde_json_ext;
use crate::jolt::JoltSigil;
use crate::parse_error::ParseError;
use crate::str_bytes;
use crate::types::actor_types::{
    ActorBlock, AutoMessageHandler, ClientMessageValidator, ConditionBlock, ScriptLine,
    ServerAction, ServerActionLine, ServerMessageSender,
};
use crate::types::{BoolIsh, BoolIsh::*, Branch, Resolvable, ScanBlock, Script};
use crate::util::opt_res_ret;
use crate::values::bolt_message::BoltMessage;
use crate::values::bolt_struct::{
    JoltDate, JoltDateTime, JoltDuration, JoltNode, JoltPath, JoltPoint, JoltRelationship,
    JoltTime, TAG_DATE, TAG_DURATION, TAG_LOCAL_TIME, TAG_POINT_2D, TAG_POINT_3D, TAG_TIME,
};
use crate::values::pack_stream_value::{PackStreamStruct, PackStreamValue};

#[derive(Debug)]
pub struct ActorScript<'a> {
    pub config: ActorConfig,
    pub tree: ActorBlock,
    pub script: Script<'a>,
}

#[derive(Debug)]
pub struct ActorConfig {
    pub bolt_version: BoltVersion,
    pub bolt_version_raw: (u8, u8),
    pub bolt_capabilities: BoltCapabilities,
    pub handshake_manifest_version: Option<u8>,
    pub handshake: Option<Vec<u8>>,
    pub handshake_response: Option<Vec<u8>>,
    pub handshake_delay: Option<Duration>,
    pub allow_restart: bool,
    pub allow_concurrent: bool,
    pub auto_responses: IndexMap<u8, AutoBangLineHandler>,
    pub py_lines: Vec<(Context, String)>,
}

type Result<T> = StdResult<T, ParseError>;
type ValidateValueFn = Box<dyn Fn(&PackStreamValue) -> anyhow::Result<()> + 'static + Send + Sync>;
type ValidateValuesFn =
    Box<dyn Fn(&[PackStreamValue]) -> anyhow::Result<()> + 'static + Send + Sync>;

pub fn contextualize_res<T>(res: Result<T>, script_name: &str, script: &str) -> anyhow::Result<T> {
    match res {
        Ok(t) => Ok(t),
        Err(e) => Err(match e.ctx {
            None => anyhow::anyhow!("Error parsing script: {}", e.message),
            Some(ctx) => {
                if ctx.start_line_number == ctx.end_line_number {
                    anyhow::anyhow!(
                        "Error parsing script on line ({}): {}\n{}",
                        ctx.start_line_number,
                        e.message,
                        script_excerpt(script_name, script, ctx)
                    )
                } else {
                    anyhow::anyhow!(
                        "Error parsing script on lines ({}-{}): {}\n{}",
                        ctx.start_line_number,
                        ctx.end_line_number,
                        e.message,
                        script_excerpt(script_name, script, ctx)
                    )
                }
            }
        }),
    }
}

pub fn parse(script: Script) -> Result<ActorScript> {
    let config = parse_config(&script.bang_lines)?;
    let (body_ctx, body_blocks) = &script.body;
    let tree = parse_blocks(*body_ctx, body_blocks, &config)?;

    trace!(
        "Parser output\n\
        ================================================================\n\
        config: {config:#?}\n\
        tree: {tree:#?}\n\
        ================================================================",
    );

    Ok(ActorScript {
        config,
        tree,
        script,
    })
}

fn parse_config(bang_lines: &[BangLine]) -> Result<ActorConfig> {
    let mut bolt_version: Option<((u8, u8), BoltVersion, BoltCapabilities)> = None;
    let mut handshake_manifest_version: Option<(Context, u8)> = None;
    let mut handshake: Option<Vec<u8>> = None;
    let mut handshake_response: Option<(Context, Vec<u8>)> = None;
    let mut handshake_delay: Option<Duration> = None;
    let mut allow_restart: Option<()> = None;
    let mut allow_concurrent: Option<()> = None;
    let mut auto_responses = IndexMap::new();
    let mut py_lines: Vec<(Context, String)> = Vec::new();

    for bang_line in bang_lines {
        match bang_line {
            BangLine::Version(ctx, (ctx_bolt, bolt), capabilities) => {
                if bolt_version.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple BOLT version bang lines found",
                    ));
                }
                let (raw_version, version) = parse_bolt_version(*ctx_bolt, bolt)?;
                let capabilities = match capabilities {
                    None => BoltCapabilities::default(),
                    Some((ctx_cap, cap)) => {
                        let cap_bytes = str_bytes::str_to_bytes(cap)
                            .map_err(|e| ParseError::new_ctx(*ctx_cap, e.to_string()))?;
                        BoltCapabilities::from_bytes(cap_bytes)
                            .map_err(|e| ParseError::new_ctx(*ctx_cap, e.to_string()))?
                    }
                };
                bolt_version = Some((raw_version, version, capabilities));
            }
            BangLine::HandshakeManifest(ctx, (ctx_arg, arg)) => {
                if handshake_manifest_version.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple handshake manifest bang lines found",
                    ));
                }
                let version = u8::from_str(arg).map_err(|e| {
                    ParseError::new_ctx(
                        *ctx_arg,
                        format!("Invalid handshake manifest version (expecting u8): {}", e),
                    )
                })?;
                handshake_manifest_version = Some((*ctx, version));
            }
            BangLine::Handshake(ctx, (ctx_byte, byte_str)) => {
                if handshake.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple handshake bang lines found",
                    ));
                }

                let data = str_bytes::str_to_bytes(byte_str)
                    .map_err(|e| ParseError::new_ctx(*ctx_byte, e.to_string()))?;
                handshake = Some(data);
            }
            BangLine::HandshakeResponse(ctx, (ctx_byte, byte_str)) => {
                if handshake_response.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple handshake response bang lines found",
                    ));
                }

                let data = str_bytes::str_to_bytes(byte_str)
                    .map_err(|e| ParseError::new_ctx(*ctx_byte, e.to_string()))?;
                handshake_response = Some((*ctx, data));
            }
            BangLine::HandshakeDelay(ctx, (ctx_delay, delay)) => {
                if handshake_delay.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple handshake delay bang lines found",
                    ));
                }

                let delay = f64::from_str(delay)
                    .map_err(|e| ParseError::new_ctx(*ctx_delay, e.to_string()))?;
                let delay = Duration::try_from_secs_f64(delay).map_err(|e| {
                    ParseError::new_ctx(*ctx_delay, format!("Failed to parse handshake delay: {e}"))
                })?;
                handshake_delay = Some(delay);
            }
            BangLine::Auto(ctx, (ctx_msg, msg)) => {
                let ctx_old = auto_responses.insert(msg, (ctx, ctx_msg));
                if let Some((ctx_old, _)) = ctx_old {
                    warn!(
                        "Specified auto response for message \"{msg}\" more than once \
                        ({ctx} and {ctx_old})."
                    );
                }
            }
            BangLine::AllowRestart(ctx) => {
                if allow_restart.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple allow restart bang lines found",
                    ));
                }
                allow_restart = Some(());
            }
            BangLine::AllowConcurrent(ctx) => {
                if allow_concurrent.is_some() {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        "Multiple allow concurrent bang lines found",
                    ));
                }
                allow_concurrent = Some(());
            }
            BangLine::Python(_, (line_ctx, line)) => {
                py_lines.push((*line_ctx, line.clone()));
            }
            BangLine::Comment(_) => {}
        }
    }

    let (bolt_version_raw, bolt_version, bolt_capabilities) =
        bolt_version.ok_or(ParseError::new("Bolt version bang line missing"))?;
    let allow_concurrent = allow_concurrent.is_some();
    let allow_restart = allow_restart.is_some();
    if allow_concurrent && allow_restart {
        warn!(
            "Both allow restart and allow concurrent bang lines were found. \
            Allow concurrent already implies allow restart."
        );
    }
    let handshake_manifest_version = match handshake_manifest_version {
        None => None,
        Some((ctx, handshake_manifest_version)) => {
            if handshake.is_some() || handshake_response.is_some() {
                return Err(ParseError::new_ctx(
                    ctx,
                    "Handshake manifest version bang line cannot be used with handshake or \
                    handshake response bang lines",
                ));
            }
            Some(handshake_manifest_version)
        }
    };
    let handshake_response = match handshake_response {
        None => None,
        Some((ctx, handshake_response)) => {
            if handshake.is_none() {
                return Err(ParseError::new_ctx(
                    ctx,
                    "Handshake response bang line requires handshake bang line to be present",
                ));
            }
            Some(handshake_response)
        }
    };
    let auto_responses =
        auto_responses
            .into_iter()
            .map(|(msg, (ctx, ctx_msg))| {
                let Some(request_tag) = bolt_version.message_tag_from_request(msg) else {
                    return Err(ParseError::new_ctx(
                        *ctx_msg,
                        format!(
                            "Unknown request message name {msg:?} for BOLT version {bolt_version}"
                        ),
                    ));
                };
                let Some(response_resolver) = bolt_version.message_auto_response(request_tag)
                else {
                    return Err(ParseError::new_ctx(
                        *ctx,
                        format!(
                            "BOLT version {bolt_version} has no auto-response for message {msg:?}",
                        ),
                    ));
                };
                Ok((
                    request_tag,
                    AutoBangLineHandler {
                        ctx: *ctx_msg,
                        sender: Box::new(SenderBytes::new_auto_response(
                            response_resolver,
                            bolt_version,
                        )),
                    },
                ))
            })
            .collect::<Result<_>>()?;

    Ok(ActorConfig {
        bolt_version,
        bolt_version_raw,
        bolt_capabilities,
        handshake_manifest_version,
        handshake,
        handshake_response,
        handshake_delay,
        allow_restart,
        allow_concurrent,
        auto_responses,
        py_lines,
    })
}

fn parse_bolt_version(ctx: Context, s: &str) -> Result<((u8, u8), BoltVersion)> {
    // let mut current_ctx = ctx;
    // current_ctx.end_byte = current_ctx.start_byte;
    let mut current_start = 0;
    let mut segments = Vec::with_capacity(2);
    for (offset, char) in s.char_indices() {
        let component = if char == '.' {
            &s[current_start..offset]
        } else if offset + char.len_utf8() == s.len() {
            // last char
            &s[current_start..]
        } else {
            continue;
        };

        segments.push(u8::from_str(component).map_err(|e| {
            let ctx = Context {
                start_line_number: ctx.start_line_number,
                end_line_number: ctx.end_line_number,
                start_byte: ctx.start_byte + current_start,
                end_byte: ctx.start_byte + offset,
            };
            ParseError::new_ctx(
                ctx,
                format!("Invalid BOLT component {component:?} (must be u8): {e}"),
            )
        })?);
        current_start = offset + char.len_utf8();
    }

    fn convert_bolt_version(
        ctx: Context,
        major: u8,
        minor: Option<u8>,
    ) -> Result<((u8, u8), BoltVersion)> {
        let version = BoltVersion::match_valid_version(major, minor).ok_or_else(|| {
            let version = match minor {
                None => {
                    format!("{major}")
                }
                Some(minor) => {
                    format!("{major}.{minor}")
                }
            };
            ParseError::new_ctx(ctx, format!("Unknown BOLT version {version}"))
        })?;
        Ok(((major, minor.unwrap_or_default()), version))
    }

    match segments.len() {
        0 => Err(ParseError::new_ctx(
            ctx,
            "BOLT version must have at least one version component",
        )),
        1 => convert_bolt_version(ctx, segments[0], None),
        2 => convert_bolt_version(ctx, segments[0], Some(segments[1])),
        n => Err(ParseError::new_ctx(
            ctx,
            format!("BOLT version has too many components (expecting 1 or 2, got {n})"),
        )),
    }
}

fn parse_blocks(ctx: Context, blocks: &[ScanBlock], config: &ActorConfig) -> Result<ActorBlock> {
    let mut actor_blocks = condense_actor_blocks(blocks.iter().map(|b| parse_block(b, config)))?;
    validate_list_children(&actor_blocks)?;

    match actor_blocks.len() {
        0 => Ok(ActorBlock::NoOp(ctx)),
        1 => Ok(actor_blocks.remove(0)),
        _ => Ok(ActorBlock::BlockList(ctx, actor_blocks)),
    }
}

fn parse_block(block: &ScanBlock, config: &ActorConfig) -> Result<IntermediateActorBlock> {
    match block {
        ScanBlock::List(ctx, scan_blocks) => {
            parse_blocks(*ctx, scan_blocks, config).map(Into::into)
        }
        ScanBlock::Alt(ctx, scan_blocks) => {
            let mut actor_blocks = Vec::with_capacity(scan_blocks.len());
            for block in scan_blocks {
                let b = parse_block(block, config)?;
                let b = finalize_intermediate(b)?;
                validate_alt_child(&b)?;
                actor_blocks.push(b);
            }
            Ok(ActorBlock::Alt(*ctx, actor_blocks).into())
        }
        ScanBlock::Parallel(ctx, scan_blocks) => {
            let mut actor_blocks = Vec::with_capacity(scan_blocks.len());
            for block in scan_blocks {
                let b = parse_block(block, config)?;
                let b = finalize_intermediate(b)?;
                validate_parallel_child(&b)?;
                actor_blocks.push(b);
            }
            Ok(ActorBlock::Parallel(*ctx, actor_blocks).into())
        }
        ScanBlock::Optional(ctx, optional_scan_block) => {
            let b = parse_block(optional_scan_block, config)?;
            let b = finalize_intermediate(b)?;
            validate_non_action(&b, Some("inside an optional block"))?;
            validate_non_empty(&b, Some("inside an optional block"))?;
            Ok(ActorBlock::Optional(*ctx, Box::new(b)).into())
        }
        ScanBlock::Repeat0(ctx, b) => {
            let b = parse_block(b, config)?;
            let b = finalize_intermediate(b)?;
            validate_non_action(&b, Some("inside a repeat block"))?;
            validate_non_empty(&b, Some("inside a repeat block"))?;
            Ok(ActorBlock::Repeat(*ctx, Box::new(b), 0).into())
        }
        ScanBlock::Repeat1(ctx, b) => {
            let b = parse_block(b, config)?;
            let b = finalize_intermediate(b)?;
            validate_non_action(&b, Some("inside a repeat block"))?;
            validate_non_empty(&b, Some("inside a repeat block"))?;
            Ok(ActorBlock::Repeat(*ctx, Box::new(b), 1).into())
        }
        ScanBlock::ClientMessage(ctx, (message_name_ctx, message_name), body) => {
            let validator = create_validator(
                *message_name_ctx,
                message_name,
                body.as_ref().map(|(ctx, body)| (*ctx, body.as_str())),
                config,
            )
            .map_err(|mut e| {
                e.ctx.get_or_insert(*ctx);
                e
            })?;
            Ok(ActorBlock::ClientMessageValidate(*ctx, validator).into())
        }
        ScanBlock::ServerMessage(ctx, (message_name_ctx, message_name), body) => {
            let server_message_sender = create_message_sender(
                *message_name_ctx,
                message_name,
                body.as_ref().map(|(ctx, body)| (*ctx, body.as_str())),
                config,
            )
            .map_err(|mut e| {
                e.ctx.get_or_insert(*ctx);
                e
            })?;
            Ok(ActorBlock::ServerMessageSend(*ctx, server_message_sender).into())
        }
        ScanBlock::ServerAction(ctx, (action_name_ctx, action_name), body) => {
            let server_action = create_server_action(
                *action_name_ctx,
                action_name,
                body.as_ref().map(|(ctx, body)| (*ctx, body.as_str())),
            )
            .map_err(|mut e| {
                e.ctx.get_or_insert(*ctx);
                e
            })?;
            Ok(ActorBlock::ServerActionLine(*ctx, server_action).into())
        }
        ScanBlock::AutoMessage(
            ctx,
            (client_message_name_ctx, client_message_name),
            client_body,
        ) => Ok(ActorBlock::AutoMessage(
            *ctx,
            create_auto_message_handler(
                *client_message_name_ctx,
                client_message_name,
                client_body
                    .as_ref()
                    .map(|(ctx, body)| (*ctx, body.as_str())),
                config,
            )?,
        )
        .into()),
        ScanBlock::Comment(ctx) => Ok(ActorBlock::NoOp(*ctx).into()),
        ScanBlock::Python(ctx, (_, py)) => {
            // python can't be validated, so we just assume it will work, if it errors at run time,
            // the actor can explain error from ctx
            Ok(ActorBlock::Python(*ctx, py.clone()).into())
        }
        ScanBlock::ConditionPart(ctx, branch_type, condition, body) => {
            let body = parse_block(body, config)?;
            let body = finalize_intermediate(body)?;
            match (branch_type, condition) {
                (Branch::If, condition) => {
                    let Some((_, condition)) = condition else {
                        return Err(ParseError::new_ctx(*ctx, "IF condition is missing"));
                    };
                    validate_non_empty(&body, Some("as IF body"))?;
                    Ok(IntermediateActorBlock::PartialCondition(
                        PartialCondition::If((*ctx, condition.clone(), Box::new(body))),
                    ))
                }
                (Branch::ElseIf, condition) => {
                    let Some((_, condition)) = condition else {
                        return Err(ParseError::new_ctx(*ctx, "ELIF condition is missing"));
                    };
                    validate_non_empty(&body, Some("as ELIF body"))?;
                    Ok(IntermediateActorBlock::PartialCondition(
                        PartialCondition::ElseIf((*ctx, condition.clone(), Box::new(body))),
                    ))
                }
                (Branch::Else, condition) => {
                    if let Some((ctx, _)) = condition {
                        return Err(ParseError::new_ctx(*ctx, "ELSE may not have a condition"));
                    }
                    validate_non_empty(&body, Some("as ELSE body"))?;
                    Ok(IntermediateActorBlock::PartialCondition(
                        PartialCondition::Else((*ctx, Box::new(body))),
                    ))
                }
            }
        }
    }
}

fn condense_actor_blocks(
    blocks: impl IntoIterator<Item = Result<IntermediateActorBlock>>,
) -> Result<Vec<ActorBlock>> {
    let blocks = blocks.into_iter();
    let mut res = Vec::with_capacity(blocks.size_hint().1.unwrap_or_default());
    for block in blocks {
        let block = block?;
        let last = res.last_mut();
        match (block, last) {
            (IntermediateActorBlock::Finished(ActorBlock::NoOp(_)), _) => continue,
            (
                IntermediateActorBlock::Finished(ActorBlock::BlockList(ctx, list)),
                Some(ActorBlock::BlockList(ctx_last, list_last)),
            ) => {
                *ctx_last = ctx_last.fuse(&ctx);
                list_last.extend(list)
            }
            (IntermediateActorBlock::Finished(block), _) => res.push(block),
            (
                IntermediateActorBlock::PartialCondition(PartialCondition::If((ctx, cond, body))),
                _,
            ) => {
                let block = ActorBlock::Condition(
                    ctx,
                    ConditionBlock {
                        if_: (ctx, cond.clone(), body),
                        else_if: vec![],
                        else_: None,
                    },
                );
                res.push(block);
            }
            (
                IntermediateActorBlock::PartialCondition(PartialCondition::ElseIf((
                    ctx,
                    cond,
                    body,
                ))),
                Some(ActorBlock::Condition(ctx_last, cond_last)),
            ) => {
                if cond_last.else_.is_some() {
                    // previous condition has an ELSE, so we can't add another ELIF
                    return Err(missing_leading_if(ctx, "ELIF"));
                }
                *ctx_last = ctx_last.fuse(&ctx);
                cond_last.else_if.push((ctx, cond, body));
            }
            (IntermediateActorBlock::PartialCondition(PartialCondition::ElseIf((ctx, ..))), _) => {
                return Err(missing_leading_if(ctx, "ELIF"));
            }
            (
                IntermediateActorBlock::PartialCondition(PartialCondition::Else((ctx, body))),
                Some(ActorBlock::Condition(ctx_last, cond_last)),
            ) => {
                if cond_last.else_.is_some() {
                    // previous condition has an ELSE, so we can't add another ELSE
                    return Err(missing_leading_if(ctx, "ELSE"));
                }
                *ctx_last = ctx_last.fuse(&ctx);
                cond_last.else_ = Some((ctx, body));
            }
            (IntermediateActorBlock::PartialCondition(PartialCondition::Else((ctx, ..))), _) => {
                return Err(missing_leading_if(ctx, "ELSE"));
            }
        }
    }
    Ok(res)
}

#[derive(Debug)]
enum IntermediateActorBlock {
    Finished(ActorBlock),
    PartialCondition(PartialCondition),
}

impl From<ActorBlock> for IntermediateActorBlock {
    fn from(value: ActorBlock) -> Self {
        IntermediateActorBlock::Finished(value)
    }
}

#[derive(Debug)]
enum PartialCondition {
    If((Context, String, Box<ActorBlock>)),
    ElseIf((Context, String, Box<ActorBlock>)),
    Else((Context, Box<ActorBlock>)),
}

#[derive(Debug)]
pub(crate) struct AutoBangLineHandler {
    pub(crate) ctx: Context,
    pub(crate) sender: Box<dyn ServerMessageSender>,
}

fn create_auto_message_handler(
    client_message_name_ctx: Context,
    client_message_name: &str,
    client_body: Option<(Context, &str)>,
    config: &ActorConfig,
) -> Result<AutoMessageHandler> {
    let client_validator = create_validator(
        client_message_name_ctx,
        client_message_name,
        client_body,
        config,
    )?;
    let client_message_tag = config
        .bolt_version
        .message_tag_from_request(client_message_name)
        .expect("checked in create_validator");
    let response_resolver = config
        .bolt_version
        .message_auto_response(client_message_tag)
        .ok_or_else(|| {
            ParseError::new(format!(
                "Bolt version {} has not auto-response for message {client_message_name}",
                config.bolt_version,
            ))
        })?;
    let server_sender = Box::new(SenderBytes::new_auto_response(
        response_resolver,
        config.bolt_version,
    ));
    Ok(AutoMessageHandler {
        client_validator,
        server_sender,
    })
}

fn create_message_sender(
    message_name_ctx: Context,
    message_name: &str,
    message_body: Option<(Context, &str)>,
    config: &ActorConfig,
) -> Result<Box<dyn ServerMessageSender>> {
    let tag = config
        .bolt_version
        .message_tag_from_response(message_name)
        .ok_or_else(|| {
            ParseError::new_ctx(
                message_name_ctx,
                format!(
                    "Unknown response message name {message_name:?} for BOLT version {}",
                    config.bolt_version,
                ),
            )
        })?;
    let fields = transcode_body(message_body, config)?;
    let data = BoltMessage::new(tag, fields, config.bolt_version).into_data();

    Ok(Box::new(SenderBytes::new(
        data,
        SenderBytesLine::Ctx {
            message_name: message_name.into(),
            message_body: message_body.map(|(_, s)| s.into()),
            ctx: match &message_body {
                None => message_name_ctx,
                Some((ctx, _)) => message_name_ctx.fuse(ctx),
            },
        },
    )))
}

enum SenderBytes {
    Static {
        data: Vec<u8>,
        line: SenderBytesLine,
    },
    Dynamic {
        data: Box<dyn Fn() -> (u8, Vec<PackStreamValue>) + Send + Sync>,
        repr: String,
        bolt_version: BoltVersion,
    },
}

#[derive(Debug)]
enum SenderBytesLine {
    Ctx {
        ctx: Context,
        #[allow(dead_code, reason = "Very useful in Debug representation")]
        message_name: String,
        #[allow(dead_code, reason = "Very useful in Debug representation")]
        message_body: Option<String>,
    },
    Repr(String),
}

impl SenderBytes {
    pub fn new(data: Vec<u8>, line: SenderBytesLine) -> Self {
        Self::Static { data, line }
    }

    pub fn new_auto_response(
        resolver: Resolvable<(u8, Vec<PackStreamValue>)>,
        bolt_version: BoltVersion,
    ) -> Self {
        match resolver {
            Resolvable::Static((tag, fields)) => {
                let response_message = BoltMessage::new(tag, fields, bolt_version);
                let response_repr = response_message.repr();
                let response_data = response_message.into_data();
                Self::new(response_data, SenderBytesLine::Repr(response_repr))
            }
            Resolvable::Dynamic { func, repr } => Self::Dynamic {
                data: func,
                repr,
                bolt_version,
            },
        }
    }
}

impl ScriptLine for SenderBytes {
    fn line_repr<'a: 'c, 'b: 'c, 'c>(&'b self, script: &'a str) -> Option<&'c str> {
        match self {
            Self::Static { line, .. } => Some(match line {
                SenderBytesLine::Ctx { ctx, .. } => ctx.original_line(script),
                SenderBytesLine::Repr(repr) => repr,
            }),
            Self::Dynamic { .. } => None,
        }
    }

    fn line_number(&self) -> Option<usize> {
        match self {
            Self::Static {
                line: SenderBytesLine::Ctx { ctx, .. },
                ..
            } => Some(ctx.start_line_number),
            Self::Static { .. } | Self::Dynamic { .. } => None,
        }
    }
}

impl Debug for SenderBytes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static { data, line } => f
                .debug_struct("SenderBytes::Static")
                .field("data", data)
                .field("line", line)
                .finish(),
            Self::Dynamic {
                repr,
                bolt_version,
                data: _,
            } => f
                .debug_struct("SenderBytes::Dynamic")
                .field("data", &"...")
                .field("repr", repr)
                .field("bolt_version", bolt_version)
                .finish(),
        }
    }
}

impl ServerMessageSender for SenderBytes {
    fn send(&self) -> anyhow::Result<Cow<[u8]>> {
        Ok(match self {
            SenderBytes::Static { data, line: _ } => data.into(),
            SenderBytes::Dynamic {
                data,
                repr: _,
                bolt_version,
            } => {
                let (tag, fields) = data();
                let message = BoltMessage::new(tag, fields, *bolt_version);
                message.into_data().into()
            }
        })
    }
}

fn transcode_body(
    msg: Option<(Context, &str)>,
    config: &ActorConfig,
) -> Result<Vec<PackStreamValue>> {
    let Some((ctx, msg)) = msg else {
        return Ok(vec![]);
    };
    load_json_values(msg, ctx)?
        .into_iter()
        .map(|field| transcode_field(field, config))
        .collect()
}

fn load_json_values(body: &str, ctx: Context) -> Result<Vec<JsonValue>> {
    Deserializer::from_str(body)
        .into_iter()
        .collect::<StdResult<Vec<JsonValue>, _>>()
        .map_err(|err| {
            let mut err = ParseError::from(err);
            err.add_ctx_offset(ctx.start_line_number, ctx.start_byte);
            err
        })
}

pub(crate) fn transcode_field(field: JsonValue, config: &ActorConfig) -> Result<PackStreamValue> {
    Ok(match field {
        JsonValue::Null => PackStreamValue::Null,
        JsonValue::Bool(value) => PackStreamValue::Boolean(value),
        JsonValue::Number(value) if value.is_i64() => {
            PackStreamValue::Integer(value.as_i64().unwrap())
        }
        JsonValue::Number(value) if value.is_f64() => {
            PackStreamValue::Float(value.as_f64().unwrap())
        }
        JsonValue::Number(value) => {
            return Err(ParseError::new(format!(
                "Can't parse number (must be i64 or f64) {value:?}",
            )));
        }
        JsonValue::String(value) => PackStreamValue::String(value),
        JsonValue::Array(value) => PackStreamValue::List(
            value
                .into_iter()
                .map(|v| transcode_field(v, config))
                .collect::<Result<Vec<_>>>()?,
        ),
        JsonValue::Object(value) => {
            let value = match transcode_jolt_value(value, config)? {
                IsJoltValue::Yes(jolt_value) => return Ok(jolt_value),
                IsJoltValue::No(value) => value,
            };

            PackStreamValue::Dict(
                value
                    .into_iter()
                    .map(|(k, v)| transcode_field(v, config).map(|v| (k, v)))
                    .collect::<Result<_>>()?,
            )
        }
    })
}

enum IsJoltValue {
    Yes(PackStreamValue),
    No(JsonMap<String, JsonValue>),
}

fn transcode_jolt_value(
    value: JsonMap<String, JsonValue>,
    config: &ActorConfig,
) -> Result<IsJoltValue> {
    if value.len() != 1 {
        return Ok(IsJoltValue::No(value));
    }
    let (sigil, value) = value.into_iter().next().expect("non-empty check above");
    let (versionless_sigil, jolt_version) = parse_jolt_sigil(&sigil, config)?;
    let Some(sigil) = JoltSigil::from_str(versionless_sigil) else {
        return Ok(IsJoltValue::No([(sigil, value)].into_iter().collect()));
    };
    Ok(match sigil {
        JoltSigil::Bool => {
            if !value.is_boolean() {
                return Err(ParseError::new(format!(
                    "Expected bool after sigil \"?\", but found {value:?}",
                )));
            }
            IsJoltValue::Yes(transcode_field(value, config)?)
        }
        JoltSigil::Integer => {
            let JsonValue::String(value) = value else {
                return Err(ParseError::new(format!(
                    "Expected string after sigil \"Z\", but found {value:?}",
                )));
            };
            let value = i64::from_str(&value).map_err(|e| {
                ParseError::new(format!(
                    "Failed to parse i64 after sigil \"Z\": {value:?} because {e}",
                ))
            })?;
            IsJoltValue::Yes(transcode_field(JsonValue::Number(value.into()), config)?)
        }
        JoltSigil::Float => {
            let JsonValue::String(value) = value else {
                return Err(ParseError::new(format!(
                    "Expected string after sigil \"R\", but found {value:?}",
                )));
            };
            let value = f64::from_str(&value).map_err(|e| {
                ParseError::new(format!(
                    "Failed to parse f64 after sigil \"R\": {value:?} because {e}",
                ))
            })?;
            IsJoltValue::Yes(PackStreamValue::Float(value))
        }
        JoltSigil::String => {
            if !value.is_string() {
                return Err(ParseError::new(format!(
                    "Expected string after sigil \"U\", but found {value:?}",
                )));
            }
            IsJoltValue::Yes(transcode_field(value, config)?)
        }
        JoltSigil::Bytes => {
            let bytes = parse_jolt_bytes(value)?;
            IsJoltValue::Yes(PackStreamValue::Bytes(bytes))
        }
        JoltSigil::List => {
            if !value.is_array() {
                return Err(ParseError::new(format!(
                    "Expected array after sigil \"[]\", but found {value:?}",
                )));
            }
            IsJoltValue::Yes(transcode_field(value, config)?)
        }
        JoltSigil::Dict => {
            if !value.is_object() {
                return Err(ParseError::new(format!(
                    "Expected object after sigil \"{{}}\", but found {value:?}",
                )));
            };
            IsJoltValue::Yes(transcode_field(value, config)?)
        }
        JoltSigil::Temporal => {
            let JsonValue::String(value) = value else {
                return Err(ParseError::new(format!(
                    "Expected temporal string after sigil \"T\", but found {value:?}",
                )));
            };
            let value = transcode_date_value(&value)
                .or_else(|| transcode_time_value(&value))
                .or_else(|| transcode_date_time_value(&value, jolt_version))
                .or_else(|| transcode_duration_value(&value))
                .transpose()?
                .ok_or_else(|| {
                    ParseError::new(format!(
                        "Expected temporal string after sigil \"T\", but found {value:?}",
                    ))
                })?;
            IsJoltValue::Yes(value)
        }
        JoltSigil::Spatial => {
            let JsonValue::String(value) = value else {
                return Err(ParseError::new(format!(
                    "Expected spatial string after sigil \"@\", but found {value:?}",
                )));
            };
            let bolt_point = JoltPoint::parse(&value)?;
            IsJoltValue::Yes(PackStreamValue::Struct(bolt_point.as_struct()))
        }
        JoltSigil::Node => {
            let bolt_node = JoltNode::parse(value, jolt_version, config)?;
            IsJoltValue::Yes(PackStreamValue::Struct(bolt_node.into_struct()))
        }
        JoltSigil::RelationshipForward => {
            let bolt_relationship = JoltRelationship::parse(value, jolt_version, config)?;
            IsJoltValue::Yes(PackStreamValue::Struct(bolt_relationship.into_struct()))
        }
        JoltSigil::RelationshipBackward => {
            let mut bolt_relationship = JoltRelationship::parse(value, jolt_version, config)?;
            bolt_relationship.flip_direction();
            IsJoltValue::Yes(PackStreamValue::Struct(bolt_relationship.into_struct()))
        }
        JoltSigil::Path => {
            let bolt_path = JoltPath::parse(value, jolt_version, config)?;
            IsJoltValue::Yes(PackStreamValue::Struct(bolt_path.into_struct()))
        }
    })
}

fn transcode_date_value(s: &str) -> Option<Result<PackStreamValue>> {
    let bolt_date = opt_res_ret!(JoltDate::parse(s));
    let value_struct = opt_res_ret!(bolt_date.as_struct());
    Some(Ok(PackStreamValue::Struct(value_struct)))
}

fn transcode_time_value(s: &str) -> Option<Result<PackStreamValue>> {
    let bolt_time = opt_res_ret!(JoltTime::parse(s));
    let value_struct = opt_res_ret!(bolt_time.as_struct());
    Some(Ok(PackStreamValue::Struct(value_struct)))
}

fn transcode_date_time_value(
    s: &str,
    jolt_version: JoltVersion,
) -> Option<Result<PackStreamValue>> {
    let bolt_date_time = opt_res_ret!(JoltDateTime::parse(s));
    let value_struct = opt_res_ret!(bolt_date_time.into_struct(jolt_version));
    Some(Ok(PackStreamValue::Struct(value_struct)))
}

fn transcode_duration_value(s: &str) -> Option<Result<PackStreamValue>> {
    let bolt_duration = opt_res_ret!(JoltDuration::parse(s));
    let value_struct = opt_res_ret!(bolt_duration.as_struct());
    Some(Ok(PackStreamValue::Struct(value_struct)))
}

fn create_server_action(
    action_name_ctx: Context,
    action_name: &str,
    message_body: Option<(Context, &str)>,
) -> Result<Box<dyn ServerActionLine>> {
    let ctx = match &message_body {
        None => action_name_ctx,
        Some((ctx, _)) => action_name_ctx.fuse(ctx),
    };
    let action = match action_name {
        "EXIT" => {
            check_server_action_no_body(action_name, message_body)?;
            ServerAction::Exit
        }
        "SHUTDOWN" => {
            check_server_action_no_body(action_name, message_body)?;
            ServerAction::Shutdown
        }
        "NOOP" => {
            check_server_action_no_body(action_name, message_body)?;
            ServerAction::Noop
        }
        "RAW" => {
            let arg = check_server_action_hex_body(action_name, ctx, message_body)?;
            ServerAction::Raw(arg)
        }
        "SLEEP" => {
            let arg = check_server_action_duration_body(action_name, ctx, message_body)?;
            ServerAction::Sleep(arg)
        }
        "ASSERT ORDER" => {
            let arg = check_server_action_duration_body(action_name, ctx, message_body)?;
            ServerAction::AssertOrder(arg)
        }
        _ => {
            return Err(ParseError::new_ctx(
                ctx,
                format!("Unknown server action {action_name}"),
            ))
        }
    };
    Ok(Box::new(ParsedServerAction { ctx, action }))
}

#[derive(Debug, Clone)]
struct ParsedServerAction {
    ctx: Context,
    action: ServerAction,
}

impl ScriptLine for ParsedServerAction {
    fn line_repr<'a: 'c, 'b: 'c, 'c>(&'b self, script: &'a str) -> Option<&'c str> {
        Some(self.ctx.original_line(script))
    }

    fn line_number(&self) -> Option<usize> {
        Some(self.ctx.start_line_number)
    }
}

impl ServerActionLine for ParsedServerAction {
    fn get_action(&self) -> &ServerAction {
        &self.action
    }
}

fn check_server_action_no_body(
    action_name: &str,
    message_body: Option<(Context, &str)>,
) -> Result<()> {
    if let Some((ctx, body)) = message_body {
        return Err(ParseError::new_ctx(
            ctx,
            format!("Server action {action_name} does not accept any arguments, found {body}"),
        ));
    }
    Ok(())
}

fn check_server_action_hex_body(
    action_name: &str,
    ctx: Context,
    message_body: Option<(Context, &str)>,
) -> Result<Vec<u8>> {
    let Some((ctx, body)) = message_body else {
        return Err(ParseError::new_ctx(
            ctx,
            format!("Server action {action_name} requires a hex argument, found none"),
        ));
    };
    str_bytes::str_to_bytes(body)
        .map_err(|e| ParseError::new_ctx(ctx, format!("Failed to parse hex argument: {e}")))
}

fn check_server_action_duration_body(
    action_name: &str,
    ctx: Context,
    message_body: Option<(Context, &str)>,
) -> Result<Duration> {
    let Some((ctx, body)) = message_body else {
        return Err(ParseError::new_ctx(
            ctx,
            format!("Server action {action_name} requires a duration argument (float), found none"),
        ));
    };
    let arg = f64::from_str(body)
        .map_err(|e| ParseError::new_ctx(ctx, format!("Failed to parse float argument: {e}")))?;
    Duration::try_from_secs_f64(arg).map_err(|e| {
        ParseError::new_ctx(
            ctx,
            format!("Failed to parse float {arg} as duration (seconds): {e}"),
        )
    })
}

struct ValidatorImpl<T: Fn(&BoltMessage) -> anyhow::Result<()> + Send + Sync> {
    pub func: T,
    pub ctx: Context,
}

impl<T: Fn(&BoltMessage) -> anyhow::Result<()> + Send + Sync> Debug for ValidatorImpl<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValidatorImpl")
            .field("ctx", &self.ctx)
            .finish()
    }
}

impl<T: Fn(&BoltMessage) -> anyhow::Result<()> + Send + Sync> ScriptLine for ValidatorImpl<T> {
    fn line_repr<'a: 'c, 'b: 'c, 'c>(&'b self, script: &'a str) -> Option<&'c str> {
        Some(self.ctx.original_line(script))
    }

    fn line_number(&self) -> Option<usize> {
        Some(self.ctx.start_line_number)
    }
}

impl<T: Fn(&BoltMessage) -> anyhow::Result<()> + Send + Sync> ClientMessageValidator
    for ValidatorImpl<T>
{
    fn validate(&self, message: &BoltMessage) -> anyhow::Result<()> {
        (self.func)(message)
    }
}

fn create_validator(
    message_name_ctx: Context,
    message_name: &str,
    body: Option<(Context, &str)>,
    config: &ActorConfig,
) -> Result<Box<dyn ClientMessageValidator>> {
    let expected_tag = config
        .bolt_version
        .message_tag_from_request(message_name)
        .ok_or_else(|| {
            ParseError::new_ctx(
                message_name_ctx,
                format!(
                    "Unknown request message name {message_name:?} for BOLT version {}",
                    config.bolt_version
                ),
            )
        })?;
    let expected_body_behavior = build_fields_validator(body, config)?;
    Ok(Box::new(ValidatorImpl {
        func: move |client_message| {
            if client_message.tag != expected_tag {
                return Err(anyhow!("the tags did not match."));
            }
            expected_body_behavior(&client_message.fields)
        },
        ctx: match &body {
            None => message_name_ctx,
            Some((ctx, _)) => message_name_ctx.fuse(ctx),
        },
    }))
}

fn build_no_fields_validator(message: &[PackStreamValue]) -> anyhow::Result<()> {
    match message.len() {
        0 => Ok(()),
        _ => Err(anyhow!("Expected 0 fields")),
    }
}

fn build_field_validator(field: JsonValue, config: &ActorConfig) -> Result<ValidateValueFn> {
    Ok(match field {
        JsonValue::Null => Box::new(|msg| match msg {
            PackStreamValue::Null => Ok(()),
            _ => Err(anyhow!("Expected null")),
        }),
        JsonValue::Bool(expected) => Box::new(move |msg| match msg {
            PackStreamValue::Boolean(received) if received == &expected => Ok(()),
            _ => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
        }),
        JsonValue::Number(expected) if expected.is_i64() => {
            let expected = expected.as_i64().expect("checked in match arm");
            Box::new(move |msg| match msg {
                PackStreamValue::Integer(received) if received == &expected => Ok(()),
                _ => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
            })
        }
        JsonValue::Number(expected) if expected.is_f64() => {
            let expected = expected.as_f64().expect("checked in match arm");
            assert!(
                expected.is_finite(),
                "json does not allow for NaN or Inf. Therefore, we don't handle those here",
            );
            Box::new(move |msg| match msg {
                PackStreamValue::Float(received) if received == &expected => Ok(()),
                _ => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
            })
        }
        JsonValue::Number(expected) => {
            return Err(ParseError::new(format!(
                "Can't parse number (must be i64 or f64) {expected:?}",
            )));
        }
        JsonValue::String(expected) => Box::new(move |msg| match msg {
            _ if expected == "*" => Ok(()),
            PackStreamValue::String(received) => validate_str_field_eq(&expected, received),
            _ => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
        }),
        JsonValue::Array(expected) => {
            let validators = expected
                .into_iter()
                .map(|value| build_field_validator(value, config))
                .collect::<Result<Vec<_>>>()?;
            Box::new(move |msg| {
                let PackStreamValue::List(received) = msg else {
                    return Err(anyhow!("Expected list, found {:?}", msg));
                };
                if validators.len() != received.len() {
                    return Err(anyhow!(
                        "Expected {} fields, found {}",
                        validators.len(),
                        received.len()
                    ));
                }
                for (validator, received) in validators.iter().zip(received.iter()) {
                    validator(received)?;
                }
                Ok(())
            })
        }
        JsonValue::Object(expected) => {
            let expected = match build_jolt_validator(expected, config)? {
                IsJoltValidator::Yes(jolt_validator) => return Ok(jolt_validator),
                IsJoltValidator::No(expected) => expected,
            };

            build_map_validator(expected, config)?
        }
    })
}

fn build_map_validator(
    expected: JsonMap<String, JsonValue>,
    config: &ActorConfig,
) -> Result<ValidateValueFn> {
    let mut required_keys = HashSet::new();
    let mut unique_keys = HashSet::new();
    let validators = expected
        .into_iter()
        .map(|(key, expect_for_key)| {
            let (key, validator, req) = build_map_entry_validator(&key, expect_for_key, config)?;
            if req {
                required_keys.insert(key.clone());
            }
            if !unique_keys.insert(key.clone()) {
                return Err(ParseError::new(format!(
                    "contains same unescaped key twice {key}"
                )));
            }
            Ok((key, validator))
        })
        .collect::<Result<HashMap<_, _>>>()?;

    Ok(Box::new(move |msg| {
        let PackStreamValue::Dict(received) = msg else {
            return Err(anyhow!("Expected map, found {:?}", msg));
        };

        let mut matched_keys = HashSet::new();

        for (key, value) in received {
            let Some(validator) = validators.get(key) else {
                return Err(anyhow!("Unexpected Key:{key} was received."));
            };
            validator(value)?;
            matched_keys.insert(key.clone());
        }

        match matched_keys.is_superset(&required_keys) {
            true => Ok(()),
            false => {
                let mut missing_keys = required_keys.difference(&matched_keys);
                Err(anyhow!(
                    "Received message with missing keys: {:?}",
                    missing_keys.join(", ")
                ))
            }
        }
    }))
}

/// # Returns
///  * String: the map key this validator should be applied to with markers stripped from it.
///  * ValidateValueFn: the validator function
///  * bool: whether the key is required (`true`), or optional (`false`).
fn build_map_entry_validator(
    key: &str,
    expected: JsonValue,
    config: &ActorConfig,
) -> Result<(String, ValidateValueFn, bool)> {
    //  JSON object keys:
    //    * The key will be **unescaped** before it is matched:
    //      `\\`, `\[`, `\]`, `\{`, and `\}` are turned into `\`, `[`, `]`, `{`, and `}` respectively.
    //    * If the escaped key starts with `[` and ends with `]`, the corresponding key/value pair is **optional**.
    //      E.g., `C: PULL {"[n]": 1000}` matches `C: PULL {}` and `C: PULL {"n": 1000}`, but not `C: PULL {"n": 1000, "m": 1001}`,  `C: PULL {"n": 1}`, or  `C: PULL null`.
    //    * If the escaped key ends on `{}` after potential optional-brackets (s. above) have been stripped, the corresponding value will be compared **sorted** if it's a list.
    //      E.g., `C: MSG {"foo{}": [1, 2]}` will match `C: MSG {"foo": [1, 2]}` and `C: MSG {"foo": [2, 1]}`, but `C: MSG {"foo{}": "ba"}` will not match `C: MSG {"foo": "ab"}`.
    //    * Example for **optional** and **sorted**: `C: MSG {"[foo{}]": [1, 2]}`.

    let key = ParsedMapKey::parse(key);
    let required = !key.is_optional;
    let ordered = key.is_ordered;
    let key = key.unescaped;
    let validator: ValidateValueFn = match expected {
        JsonValue::Array(expected) if ordered => {
            let validators = expected
                .into_iter()
                .map(|value| build_field_validator(value, config))
                .collect::<Result<Vec<_>>>()?;
            Box::new(move |msg| {
                let PackStreamValue::List(received) = msg else {
                    return Err(anyhow!("Expected list, found {:?}", msg));
                };
                if validators.len() != received.len() {
                    return Err(anyhow!(
                        "Expected {} fields, found {}",
                        validators.len(),
                        received.len()
                    ));
                }
                let mut left_validators = HashSet::new();
                left_validators.extend(0..validators.len());
                'values: for value_received in received.iter() {
                    for validator_idx in &left_validators {
                        let validator_idx = *validator_idx;
                        let validator = &validators[validator_idx];
                        if let Ok(()) = validator(value_received) {
                            left_validators.remove(&validator_idx);
                            continue 'values;
                        }
                    }
                    return Err(anyhow!(
                        "Unexpected value in any order array: {:?}",
                        value_received
                    ));
                }
                Ok(())
            })
        }
        _ => build_field_validator(expected, config)?,
    };
    Ok((key, validator, required))
}

enum IsJoltValidator {
    Yes(ValidateValueFn),
    No(JsonMap<String, JsonValue>),
}

fn build_jolt_validator(
    expected: JsonMap<String, JsonValue>,
    config: &ActorConfig,
) -> Result<IsJoltValidator> {
    // try to build jolt matcher (incl. Structs like datetime)
    //   * if has 1 key-value pair
    //   * && key is known sigil
    //   * Special string "*" applies: E.g., `C: RUN {"Z": "*"}` will match any integer: `C: RUN 1` and `C: RUN 2`, but not `C: RUN 1.2` or `C: RUN "*"`.
    fn is_match_all(value: &JsonValue) -> bool {
        value.as_str().map(|s| s == "*").unwrap_or_default()
    }

    macro_rules! match_any {
        ($typ:expr, $pattern:pat) => {
            return Ok(IsJoltValidator::Yes(Box::new(|msg| match msg {
                $pattern => Ok(()),
                _ => Err(anyhow!("Expected any {:} found {:?}", $typ, msg)),
            })))
        };
    }

    // https://docs.google.com/document/d/1QK4OcC0tZ08lKqVr-3-z8HpPY9jFeEh6zZPhMm15D-w/edit?tab=t.0
    if expected.len() != 1 {
        return Ok(IsJoltValidator::No(expected));
    }
    let (sigil, expected) = expected.into_iter().next().expect("non-empty check above");
    let (versionless_sigil, jolt_version) = parse_jolt_sigil(&sigil, config)?;
    let Some(parsed_sigil) = JoltSigil::from_str(versionless_sigil) else {
        return Ok(IsJoltValidator::No(
            [(sigil, expected)].into_iter().collect(),
        ));
    };
    Ok(match parsed_sigil {
        JoltSigil::Bool => {
            if is_match_all(&expected) {
                match_any!("bool", PackStreamValue::Boolean(_));
            }
            if !expected.is_boolean() {
                return Err(ParseError::new(format!(
                    "Expected bool after sigil \"?\", but found {expected:?}",
                )));
            }
            IsJoltValidator::Yes(build_field_validator(expected, config)?)
        }
        JoltSigil::Integer => {
            if is_match_all(&expected) {
                match_any!("integer", PackStreamValue::Integer(_));
            }
            let JsonValue::String(expected) = expected else {
                return Err(ParseError::new(format!(
                    "Expected string after sigil \"Z\", but found {expected:?}",
                )));
            };
            let expected = i64::from_str(&expected).map_err(|e| {
                ParseError::new(format!(
                    "Failed to parse i64 after sigil \"Z\": {expected:?} because {e}",
                ))
            })?;
            IsJoltValidator::Yes(build_field_validator(
                JsonValue::Number(expected.into()),
                config,
            )?)
        }
        JoltSigil::Float => {
            if is_match_all(&expected) {
                match_any!("float", PackStreamValue::Float(_));
            }
            let JsonValue::String(expected) = expected else {
                return Err(ParseError::new(format!(
                    "Expected string after sigil \"R\", but found {expected:?}",
                )));
            };
            let expected = f64::from_str(&expected).map_err(|e| {
                ParseError::new(format!(
                    "Failed to parse f64 after sigil \"R\": {expected:?} because {e}",
                ))
            })?;
            IsJoltValidator::Yes(Box::new(move |msg| {
                match msg == &PackStreamValue::Float(expected) {
                    true => Ok(()),
                    false => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
                }
            }))
        }
        JoltSigil::String => {
            if is_match_all(&expected) {
                match_any!("string", PackStreamValue::String(_));
            }
            if !expected.is_string() {
                return Err(ParseError::new(format!(
                    "Expected string after sigil \"U\", but found {expected:?}",
                )));
            }
            IsJoltValidator::Yes(build_field_validator(expected, config)?)
        }
        JoltSigil::Bytes => {
            if is_match_all(&expected) {
                match_any!("bytes", PackStreamValue::Bytes(_));
            }
            let bytes = parse_jolt_bytes(expected)?;
            IsJoltValidator::Yes(Box::new(move |msg| match msg {
                PackStreamValue::Bytes(received) if &bytes == received => Ok(()),
                _ => Err(anyhow!("Expected bytes {:?} found {:?}", &bytes, msg)),
            }))
        }
        JoltSigil::List => {
            if is_match_all(&expected) {
                match_any!("list", PackStreamValue::List(_));
            }
            if !expected.is_array() {
                return Err(ParseError::new(format!(
                    "Expected array after sigil \"[]\", but found {expected:?}",
                )));
            }
            IsJoltValidator::Yes(build_field_validator(expected, config)?)
        }
        JoltSigil::Dict => {
            if is_match_all(&expected) {
                match_any!("map", PackStreamValue::Dict(_));
            }
            let JsonValue::Object(expected) = expected else {
                return Err(ParseError::new(format!(
                    "Expected object after sigil \"{{}}\", but found {expected:?}",
                )));
            };
            IsJoltValidator::Yes(build_map_validator(expected, config)?)
        }
        JoltSigil::Temporal => {
            let JsonValue::String(expected) = expected else {
                return Err(ParseError::new(format!(
                    "Expected temporal string after sigil \"T\", but found {expected:?}",
                )));
            };
            let validator = build_date_validator(&expected)
                .or_else(|| build_time_validator(&expected))
                .or_else(|| build_date_time_validator(&expected, jolt_version))
                .or_else(|| build_duration_validator(&expected))
                .transpose()?
                .ok_or_else(|| {
                    ParseError::new(format!(
                        "Expected temporal string after sigil \"T\", but found {expected:?}",
                    ))
                })?;
            IsJoltValidator::Yes(validator)
        }
        JoltSigil::Spatial => {
            if is_match_all(&expected) {
                return Ok(IsJoltValidator::Yes(Box::new(|msg| match msg {
                    PackStreamValue::Struct(PackStreamStruct { tag, fields }) => {
                        match (tag, fields.as_slice()) {
                            (
                                &TAG_POINT_2D,
                                [PackStreamValue::Integer(_), PackStreamValue::Float(_), PackStreamValue::Float(_)],
                            ) => Ok(()),
                            (
                                &TAG_POINT_3D,
                                [PackStreamValue::Integer(_), PackStreamValue::Float(_), PackStreamValue::Float(_), PackStreamValue::Float(_)],
                            ) => Ok(()),
                            _ => Err(anyhow!("Expected any date struct found {msg:?}")),
                        }
                    }
                    _ => Err(anyhow!("Expected any spatial struct found {msg:?}")),
                })));
            }
            let JsonValue::String(expected) = expected else {
                return Err(ParseError::new(format!(
                    "Expected spatial string after sigil \"@\", but found {expected:?}",
                )));
            };
            let bolt_point = JoltPoint::parse(&expected)?;
            let expected = PackStreamValue::Struct(bolt_point.as_struct());
            IsJoltValidator::Yes(Box::new(move |msg| match msg == &expected {
                true => Ok(()),
                false => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
            }))
        }
        JoltSigil::Node => {
            let fixed_syntax = format!(
                r#"{{"\{sigil}": {}}}"#,
                serde_json_ext::compact_pretty_print(&expected)
                    .expect("JsonValue cannot fail Json serialization")
            );
            return Err(ParseError::new(format!(
                "Node structs cannot be received by the server, only sent. \
                If you meant to match a map, use `{fixed_syntax}` instead.",
            )));
        }
        JoltSigil::RelationshipForward | JoltSigil::RelationshipBackward => {
            let fixed_syntax = format!(
                r#"{{"\{sigil}": {}}}"#,
                serde_json_ext::compact_pretty_print(&expected)
                    .expect("JsonValue cannot fail Json serialization")
            );
            return Err(ParseError::new(format!(
                "Relationship structs cannot be received by the server, only sent. \
                If you meant to match a map, use `{fixed_syntax}` instead.",
            )));
        }
        JoltSigil::Path => {
            let fixed_syntax = format!(
                r#"{{"\{sigil}": {}}}"#,
                serde_json_ext::compact_pretty_print(&expected)
                    .expect("JsonValue cannot fail Json serialization")
            );
            return Err(ParseError::new(format!(
                "Path structs cannot be received by the server, only sent. \
                If you meant to match a map, use `{fixed_syntax}` instead.",
            )));
        }
    })
}

fn parse_jolt_sigil<'a>(sigil: &'a str, config: &ActorConfig) -> Result<(&'a str, JoltVersion)> {
    thread_local! {
        static SIGIL_VERSION_RE: LazyCell<Regex> = LazyCell::new(|| {
            Regex::new(r"^(.+?)(?:v(\d+))?$").unwrap()
        });
    }
    SIGIL_VERSION_RE.with(|re| {
        let Some(captures) = re.captures(sigil) else {
            return Ok((sigil, config.bolt_version.jolt_version()));
        };
        let Some(version_overwrite) = captures.get(2) else {
            return Ok((sigil, config.bolt_version.jolt_version()));
        };
        let version_overwrite = version_overwrite.as_str();
        let jolt_version = JoltVersion::parse(version_overwrite)?;
        Ok((
            captures
                .get(1)
                .expect("regex enforces one group to exist")
                .as_str(),
            jolt_version,
        ))
    })
}

fn parse_jolt_bytes(expected: JsonValue) -> Result<Vec<u8>> {
    Ok(match expected {
        JsonValue::String(expected) => parse_hex_string(&expected)?,
        JsonValue::Array(expected) => {
            let mut bytes = Vec::with_capacity(expected.len());
            for (i, b) in expected.into_iter().enumerate() {
                let Some(b) = b.as_i64() else {
                    return Err(ParseError::new(format!(
                        "Array after sigil \"#\" must contain only ints, but found {b:?} (at {i})",
                    )));
                };
                let b: u8 = b.try_into().map_err(|_| {
                    ParseError::new(format!(
                        "Array after sigil \"#\" must contain only u8, but found {b} (at {i})",
                    ))
                })?;
                bytes.push(b);
            }
            bytes
        }
        _ => {
            return Err(ParseError::new(format!(
                "Expected string or array after sigil \"#\", but found {expected:?}",
            )));
        }
    })
}

fn parse_hex_string(s: &str) -> Result<Vec<u8>> {
    if s.is_empty() {
        return Ok(vec![]);
    }
    if s.chars()
        .next()
        .expect("checked for empty above")
        .is_whitespace()
    {
        return Err(ParseError::new("Hex string may not start with whitespace"));
    }
    let mut result = Vec::new();
    let mut start_offset = None;
    let mut start_idx = None;
    for (i, (offset, ch)) in s.char_indices().enumerate() {
        if ch.is_whitespace() {
            if start_offset.is_none() {
                continue;
            }
            return Err(ParseError::new(format!(
                "Hex sting contains unexpected whitespace {ch} (at {i}): \
                whitespace may only occur between pairs of characters"
            )));
        }
        if !ch.is_ascii_hexdigit() {
            return Err(ParseError::new(format!(
                "Hex sting contains invalid hex character {ch} (at {i})"
            )));
        }
        let start_offset_val = *start_offset.get_or_insert(offset);
        let start_idx_val = *start_idx.get_or_insert(i);
        if i == start_idx_val + 1 {
            result.push(
                u8::from_str_radix(&s[start_offset_val..offset + ch.len_utf8()], 16)
                    .expect("checked for valid hex u8 above"),
            );
            start_offset = None;
            start_idx = None;
        }
    }
    if let Some(start_offset) = start_offset {
        return Err(ParseError::new(format!(
            "Hex string has non-paired trailing character(s) {:?}",
            &s[start_offset..]
        )));
    }
    Ok(result)
}

/// yes:
/// 2020-01-01
/// 2020-01
/// 2020
///
/// no:
/// 2020-1-1
/// --1
fn build_date_validator(s: &str) -> Option<Result<ValidateValueFn>> {
    if s == "*" {
        return Some(Ok(Box::new(|msg| match msg {
            PackStreamValue::Struct(PackStreamStruct { tag, fields }) => {
                match (tag, fields.as_slice()) {
                    (&TAG_DATE, [PackStreamValue::Integer(_)]) => Ok(()),
                    _ => Err(anyhow!("Expected any date struct found {msg:?}")),
                }
            }
            _ => Err(anyhow!("Expected any date struct found {msg:?}")),
        })));
    }
    let bolt_date = opt_res_ret!(JoltDate::parse(s));
    let expected_struct = opt_res_ret!(bolt_date.as_struct());
    Some(Ok(build_struct_match_validator(expected_struct)))
}

/// yes:
/// 12:00:00.000000000+0000
/// 12:00:00.000+0000
/// 12:00:00+00:00
/// 12:00:00+00
/// 12:00:00Z
/// 12:00Z
/// 12Z
/// 12:00:00-01
///
/// no:
/// 12:00:00.0000000000Z
/// 12:00:00-0000
/// 12:0:0Z
/// 12:00:00+01:02:03
/// 12:00:00+00:00\[Europe/Berlin]
fn build_time_validator(s: &str) -> Option<Result<ValidateValueFn>> {
    if s == "*" {
        return Some(Ok(Box::new(|msg| match msg {
            PackStreamValue::Struct(PackStreamStruct { tag, fields }) => {
                match (tag, fields.as_slice()) {
                    (&TAG_LOCAL_TIME, [PackStreamValue::Integer(_)]) => Ok(()),
                    (&TAG_TIME, [PackStreamValue::Integer(_), PackStreamValue::Integer(_)]) => {
                        Ok(())
                    }
                    _ => Err(anyhow!("Expected any time struct found {msg:?}")),
                }
            }
            _ => Err(anyhow!("Expected any time struct found {msg:?}")),
        })));
    }
    let bolt_time = opt_res_ret!(JoltTime::parse(s));
    let expected_struct = opt_res_ret!(bolt_time.as_struct());
    Some(Ok(build_struct_match_validator(expected_struct)))
}

/// yes:
/// `<date_re>T<time_re><timezone_name>`
/// where `<date_re>` is anything that works for [`build_date_validator`], `<time_re>`
/// is anything that works for [`build_time_validator`] and `<timezone_name>` (optional) is any
/// timezone name in square brackets, e.g., `[Europe/Stockholm]`. `<timezone_name>` may only be
/// present, when `<time_re>` has a time offset.
///
/// no:
/// anything else
fn build_date_time_validator(
    s: &str,
    jolt_version: JoltVersion,
) -> Option<Result<ValidateValueFn>> {
    if s == "*" {
        return Some(Ok(Box::new(move |msg| match msg {
            PackStreamValue::Struct(PackStreamStruct { tag, fields }) => {
                match (jolt_version, tag, fields.as_slice()) {
                    (
                        JoltVersion::V1,
                        0x66,
                        [PackStreamValue::Integer(_), PackStreamValue::Integer(_), PackStreamValue::String(_)],
                    ) => Ok(()),
                    (
                        JoltVersion::V2,
                        0x69,
                        [PackStreamValue::Integer(_), PackStreamValue::Integer(_), PackStreamValue::String(_)],
                    ) => Ok(()),
                    (
                        JoltVersion::V1,
                        0x46,
                        [PackStreamValue::Integer(_), PackStreamValue::Integer(_), PackStreamValue::Integer(_)],
                    ) => Ok(()),
                    (
                        JoltVersion::V2,
                        0x49,
                        [PackStreamValue::Integer(_), PackStreamValue::Integer(_), PackStreamValue::Integer(_)],
                    ) => Ok(()),
                    (_, 0x64, [PackStreamValue::Integer(_), PackStreamValue::Integer(_)]) => Ok(()),
                    _ => Err(anyhow!("Expected any date time struct found {msg:?}")),
                }
            }
            _ => Err(anyhow!("Expected any date time struct found {msg:?}")),
        })));
    }
    let bolt_date_time = opt_res_ret!(JoltDateTime::parse(s));
    let expected_struct = opt_res_ret!(bolt_date_time.into_struct(jolt_version));
    Some(Ok(build_struct_match_validator(expected_struct)))
}

/// yes:
/// P12Y13M40DT10H70M80.000000000S
/// P12Y-13M40DT-10H70M80.000000000S
/// P12Y
/// PT70M
/// P12T10H70M
///
/// no:
/// P12Y13M40DT10H70M80.0000000000S
/// P12Y13M40DT10H70.1M10S
/// P5W
fn build_duration_validator(s: &str) -> Option<Result<ValidateValueFn>> {
    if s == "*" {
        return Some(Ok(Box::new(|msg| match msg {
            PackStreamValue::Struct(PackStreamStruct { tag, fields }) => {
                match (tag, fields.as_slice()) {
                    (
                        &TAG_DURATION,
                        [PackStreamValue::Integer(_), PackStreamValue::Integer(_), PackStreamValue::Integer(_), PackStreamValue::Integer(_)],
                    ) => Ok(()),
                    _ => Err(anyhow!("Expected any duration struct found {msg:?}")),
                }
            }
            _ => Err(anyhow!("Expected any duration struct found {msg:?}")),
        })));
    }
    let JoltDuration {
        months,
        days,
        seconds,
        nanos,
    } = opt_res_ret!(JoltDuration::parse(s));
    let total_nanos = i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
    Some(Ok(Box::new(move |msg| match msg {
        PackStreamValue::Struct(received) => {
            if received.tag != TAG_DURATION {
                return Err(anyhow!(
                    "Expected duration (tag {TAG_DURATION:#X}), found {msg:?}",
                ));
            }
            fn get_int_field(
                i: usize,
                name: &str,
                received: &PackStreamStruct,
            ) -> anyhow::Result<i64> {
                match received.fields.get(i) {
                    None => Err(anyhow!(
                        "Received invalid duration: {:?} (missing {name})",
                        received
                    )),
                    Some(PackStreamValue::Integer(value)) => Ok(*value),
                    Some(_) => Err(anyhow!(
                        "Received invalid duration: {:?} ({name} not integer)",
                        received
                    )),
                }
            }
            let received_months = get_int_field(0, "months", received)?;
            let received_days = get_int_field(1, "days", received)?;
            let received_seconds = get_int_field(2, "seconds", received)?;
            let received_nanos = get_int_field(3, "nanoseconds", received)?;
            let received_total_nanos =
                i128::from(received_seconds) * 1_000_000_000 + i128::from(received_nanos);
            if received_months != months {
                return Err(anyhow!(
                    "Expected duration months: {months}, found {received_months} in {received:?}",
                ));
            }
            if received_days != days {
                return Err(anyhow!(
                    "Expected duration days: {days}, found {received_days} in {received:?}",
                ));
            }
            if received_total_nanos != total_nanos {
                return Err(anyhow!(
                    "Expected duration with total nanoseconds: {total_nanos}, \
                    found {received_total_nanos} in {received:?}",
                ));
            }
            Ok(())
        }
        _ => Err(anyhow!("Expected duration, found {:?}", msg)),
    })))
}

fn build_struct_match_validator(expected: PackStreamStruct) -> ValidateValueFn {
    let expected = PackStreamValue::Struct(expected);
    Box::new(move |msg| match msg == &expected {
        true => Ok(()),
        false => Err(anyhow!("Expected {:?} found {:?}", expected, msg)),
    })
}

struct ParsedMapKey {
    pub is_optional: bool,
    pub is_ordered: bool,
    pub unescaped: String,
}

impl ParsedMapKey {
    fn parse(key: &str) -> Self {
        thread_local! {
            static UNESCAPE_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(r"\\([\{\}\[\]\\])").unwrap()
            });
        }
        let unescaped = UNESCAPE_RE.with(|re| re.replace_all(key, r"$1"));
        let mut unescaped_ref = unescaped.as_ref();

        thread_local! {
            static FLAGS_RE: LazyCell<Regex> = LazyCell::new(|| {
                Regex::new(r"^(\[)?(?:\\[\\\{}\}\[\]]|[^\\])*?(\{\})?(\])?$").unwrap()
            });
        }
        let flags = FLAGS_RE.with(|re| re.captures(key).expect("regex matches anything"));
        let is_optional = flags.get(1).is_some() && flags.get(3).is_some();
        let is_ordered = flags.get(2).is_some();

        if is_optional {
            unescaped_ref = &unescaped_ref[1..unescaped.len() - 1]
        }
        if is_ordered {
            unescaped_ref = &unescaped_ref[..unescaped_ref.len() - 2]
        }

        Self {
            is_optional,
            is_ordered,
            unescaped: unescaped_ref.to_string(),
        }
    }
}

fn validate_str_field_eq(expected: &str, received: &str) -> anyhow::Result<()> {
    #[inline]
    fn match_(expected: &str, received: &str) -> anyhow::Result<()> {
        match expected == received {
            true => Ok(()),
            false => Err(anyhow!("Expected {:?} found {:?}", expected, received)),
        }
    }
    if !expected.contains(r"\") {
        match_(expected, received)
    } else {
        let escaped = expected.replace(r"\\", r"\");
        match_(&escaped, received)
    }
}

fn build_fields_validator(
    msg_body: Option<(Context, &str)>,
    config: &ActorConfig,
) -> Result<ValidateValuesFn> {
    let Some((ctx, line)) = msg_body else {
        return Ok(Box::new(build_no_fields_validator));
    };
    // RUN "RETURN $n AS n" {"n": 1}
    let field_validators: Vec<_> = load_json_values(line, ctx)?
        .into_iter()
        .map(|field| build_field_validator(field, config))
        .collect::<Result<_>>()?;

    Ok(Box::new(move |fields| {
        if fields.len() != field_validators.len() {
            return Err(anyhow!(
                "Expected {} fields, but found {}",
                fields.len(),
                field_validators.len()
            ));
        }
        for (field, field_validator) in fields.iter().zip(&field_validators) {
            field_validator(field)?;
        }
        Ok(())
    }))
}

fn is_skippable(block: &ActorBlock) -> bool {
    match block {
        ActorBlock::BlockList(_, blocks)
        | ActorBlock::Alt(_, blocks)
        | ActorBlock::Parallel(_, blocks) => blocks.iter().all(is_skippable),
        ActorBlock::Repeat(_, block, count) => *count == 0 || is_skippable(block),
        ActorBlock::Optional(..) | ActorBlock::NoOp(..) => true,
        ActorBlock::ClientMessageValidate(..)
        | ActorBlock::ServerMessageSend(..)
        | ActorBlock::ServerActionLine(..)
        | ActorBlock::Python(..)
        | ActorBlock::AutoMessage(..) => false,
        // For static analysis we consider conditional blocks to be skippable
        // if either they are non-exhaustive or any branch is skippable.
        // At runtime we can be more differentiated.
        ActorBlock::Condition(_, ConditionBlock { else_: None, .. }) => true,
        ActorBlock::Condition(
            _,
            ConditionBlock {
                if_,
                else_if,
                else_: Some(else_),
            },
        ) => {
            let (_, _, if_) = if_;
            is_skippable(if_)
                || else_if
                    .iter()
                    .any(|(_, _, else_if_block)| is_skippable(else_if_block))
                || is_skippable(&else_.1)
        }
    }
}

fn has_deterministic_end(block: &ActorBlock) -> bool {
    match block {
        ActorBlock::BlockList(_, blocks) => blocks
            .iter()
            .rev()
            .filter(|b| !matches!(b, ActorBlock::NoOp(_)))
            .map(has_deterministic_end)
            .next()
            .unwrap_or(true),
        ActorBlock::ClientMessageValidate(..)
        | ActorBlock::ServerMessageSend(..)
        | ActorBlock::ServerActionLine(..)
        | ActorBlock::Python(..) => true,
        ActorBlock::Alt(_, blocks) | ActorBlock::Parallel(_, blocks) => {
            blocks.iter().all(has_deterministic_end)
        }
        ActorBlock::Optional(..) | ActorBlock::Repeat(..) => false,
        ActorBlock::AutoMessage(..) => true,
        ActorBlock::NoOp(..) => true,
        ActorBlock::Condition(_, condition_block) => {
            let (_, _, ref if_block) = condition_block.if_;
            has_deterministic_end(if_block)
                && condition_block
                    .else_if
                    .iter()
                    .all(|(_, _, else_if_block)| has_deterministic_end(else_if_block))
                && condition_block
                    .else_
                    .as_ref()
                    .map(|(_, else_block)| has_deterministic_end(else_block))
                    .unwrap_or(true)
        }
    }
}

fn is_action_block(block: &ActorBlock) -> bool {
    match block {
        ActorBlock::Python(..)
        | ActorBlock::ServerMessageSend(..)
        | ActorBlock::ServerActionLine(..) => true,
        ActorBlock::BlockList(_, blocks) => 'arm: {
            for block in blocks {
                if is_action_block(block) {
                    break 'arm true;
                }
                if !is_skippable(block) {
                    break;
                }
            }
            break 'arm false;
        }
        ActorBlock::Alt(_, blocks) | ActorBlock::Parallel(_, blocks) => {
            blocks.iter().any(is_action_block)
        }
        ActorBlock::Condition(_, condition_block) => {
            let (_, _, ref if_block) = condition_block.if_;
            is_action_block(if_block)
                || condition_block
                    .else_if
                    .iter()
                    .any(|(_, _, else_if_block)| is_action_block(else_if_block))
                || condition_block
                    .else_
                    .as_ref()
                    .map(|(_, else_block)| is_action_block(else_block))
                    .unwrap_or(false)
        }
        ActorBlock::Optional(_, block) => is_action_block(block),
        ActorBlock::Repeat(_, block, _) => is_action_block(block),
        ActorBlock::ClientMessageValidate(..)
        | ActorBlock::AutoMessage(..)
        | ActorBlock::NoOp(..) => false,
    }
}

/*

TODO: how should IFs work? Eager? What when nested inside alt blocks, etc.

S: BAZ
IF foo
    S: BAZ2
    C: FOO
ELIF bar
    C: BAR

{{
    IF foo
        C: FOO
----
    IF bar
        C: BAR
}}

 */

fn validate_non_action(block: &ActorBlock, context: Option<&str>) -> Result<()> {
    if is_action_block(block) {
        Err(ParseError::new_ctx(
            *block.ctx(),
            format!(
                "Cannot have a block that requires server initiative (ambiguous) {}",
                context.unwrap_or("here")
            ),
        ))
    } else {
        Ok(())
    }
}

fn validate_non_empty(block: &ActorBlock, context: Option<&str>) -> Result<()> {
    match is_empty(block) {
        False => Ok(()),
        True => Err(ParseError::new_ctx(
            *block.ctx(),
            format!("Cannot have an empty block {}", context.unwrap_or("here")),
        )),
        Maybe => Err(ParseError::new_ctx(
            *block.ctx(),
            format!(
                "Cannot have a potentially empty block {}",
                context.unwrap_or("here")
            ),
        )),
    }
}

fn is_empty(block: &ActorBlock) -> BoolIsh {
    match block {
        ActorBlock::BlockList(_, blocks) => {
            let mut res = True;
            for block in blocks {
                match is_empty(block) {
                    True => continue,
                    False => return False,
                    Maybe => res = Maybe,
                }
            }
            res
        }
        ActorBlock::ClientMessageValidate(_, _) => False,
        ActorBlock::ServerMessageSend(_, _) => False,
        ActorBlock::ServerActionLine(_, _) => False,
        ActorBlock::Python(_, _) => False,
        ActorBlock::Condition(_, cond) => match cond.else_ {
            None => Maybe,
            Some(_) => False,
        },
        ActorBlock::Alt(_, blocks) => {
            debug_assert!(blocks.iter().all(|b| is_empty(b) == False));
            False
        }
        ActorBlock::Parallel(_, blocks) => {
            debug_assert!(blocks.iter().all(|b| is_empty(b) == False));
            False
        }
        ActorBlock::Optional(_, block) => {
            debug_assert!(is_empty(block) == False);
            False
        }
        ActorBlock::Repeat(_, block, _) => {
            debug_assert!(is_empty(block) == False);
            False
        }
        ActorBlock::AutoMessage(_, _) => False,
        ActorBlock::NoOp(_) => True,
    }
}

fn validate_list_children(blocks: &[ActorBlock]) -> Result<()> {
    let mut previous_has_deterministic_end = true;
    for block in blocks {
        if !previous_has_deterministic_end {
            validate_non_action(block, Some("after a block with non-deterministic end"))?;
        }
        previous_has_deterministic_end = has_deterministic_end(block);
    }
    Ok(())
}

fn validate_alt_child(b: &ActorBlock) -> Result<()> {
    validate_non_empty(b, Some("as an Alt block branch"))?;
    validate_non_action(b, Some("as an Alt block branch"))?;
    Ok(())
}

fn validate_parallel_child(b: &ActorBlock) -> Result<()> {
    validate_non_empty(b, Some("as an Parallel block branch"))?;
    validate_non_action(b, Some("as an Parallel block branch"))?;
    Ok(())
}

fn finalize_intermediate(block: IntermediateActorBlock) -> Result<ActorBlock> {
    Ok(match block {
        IntermediateActorBlock::Finished(block) => block,
        IntermediateActorBlock::PartialCondition(block) => match block {
            PartialCondition::If((ctx, condition, children)) => ActorBlock::Condition(
                ctx,
                ConditionBlock {
                    if_: (ctx, condition, children),
                    else_if: Vec::new(),
                    else_: None,
                },
            ),
            PartialCondition::ElseIf((ctx, ..)) => return Err(missing_leading_if(ctx, "ELIF")),
            PartialCondition::Else((ctx, ..)) => return Err(missing_leading_if(ctx, "ELSE")),
        },
    })
}

fn missing_leading_if(ctx: Context, block_name: &str) -> ParseError {
    ParseError::new_ctx(ctx, format!("Missing leading IF for {}", block_name))
}

#[cfg(test)]
mod test {
    use indexmap::indexmap;
    use serde_json::Number;

    use super::*;

    #[test]
    fn should_validate_any_hello() {
        let tag = "HELLO";
        let body = r#"{"{}": "*"}"#;
        let ctx = Context {
            start_line_number: 0,
            end_line_number: 0,
            start_byte: 0,
            end_byte: 0,
        };
        let cfg = ActorConfig {
            bolt_version: BoltVersion::V5_0,
            bolt_version_raw: (5, 0),
            bolt_capabilities: BoltCapabilities::from_bytes(vec![0x00]).unwrap(),
            handshake_manifest_version: None,
            handshake: None,
            handshake_response: None,
            handshake_delay: None,
            allow_restart: false,
            allow_concurrent: false,
            auto_responses: Default::default(),
            py_lines: Vec::new(),
        };
        let validator = create_validator(ctx, tag, Some((ctx, body)), &cfg).unwrap();

        let cm = BoltMessage::new(
            0x01,
            vec![PackStreamValue::Dict(
                indexmap! {String::from("foo") => PackStreamValue::String(String::from("bar"))},
            )],
            BoltVersion::V5_0,
        );

        validator.validate(&cm).unwrap()
    }

    #[test]
    fn json_test() {
        let mut v: Vec<StdResult<JsonValue, _>> = Deserializer::from_str("1").into_iter().collect();
        let JsonValue::Number(n) = v.pop().unwrap().unwrap() else {
            panic!("Expected number");
        };
        dbg!(&n);
        let Number { .. } = n;
        dbg!(n.as_i64());
    }

    #[test]
    fn remove_optional_marker() {
        let key = "[jeff]";
        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff")
    }

    #[test]
    fn remove_ordered_marker() {
        let key = "jeff{}";
        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff")
    }

    #[test]
    fn remove_both_marker() {
        let key = "[jeff{}]";

        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff")
    }

    #[test]
    fn remove_optional_marker_with_escaped() {
        let key = r"[jeff\{\}]";
        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff{}")
    }

    #[test]
    fn handle_doubled_slash() {
        let key = r"jeff\{}";
        let escaped = ParsedMapKey::parse(key);
        assert_eq!(escaped.unescaped, "jeff{}")
    }

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
        let input = r#"{"..": [{"()": [1, ["l"], {}]}, {"->": [2, 1, "RELATES_TO", 3, {}]}, {"()": [3, ["l"], {}]}, {"->": [4, 3, "RELATES_TO", 1, {}]}, {"()": [1, ["l"], {}]}]}"#;
        let JsonValue::Object(parsed) = serde_json::from_str::<JsonValue>(input).unwrap() else {
            panic!("Expected object");
        };
        assert!(matches!(
            transcode_jolt_value(parsed, &config).unwrap(),
            IsJoltValue::Yes(_)
        ));
    }
}
