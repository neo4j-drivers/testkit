use std::borrow::Cow;
use std::cell::LazyCell;
use std::fmt::{Debug, Formatter};
use std::result::Result as StdResult;
use std::str::FromStr;
use std::time::Duration;

use anyhow::anyhow;
use indexmap::IndexMap;
use log::{trace, warn};
use regex::Regex;
use serde_json::{Deserializer, Map as JsonMap, Value as JsonValue};
use uuid::Uuid;

use crate::bang_line::BangLine;
use crate::bolt_version::{BoltCapabilities, BoltVersion, JoltVersion};
use crate::context::Context;
use crate::error::script_excerpt;
use crate::jolt::JoltSigil;
use crate::parse_error::ParseError;
use crate::str_bytes;
use crate::types::actor_types::{
    ActorBlock, AutoMessageHandler, ClientMessageValidator, ConditionBlock, ScriptLine,
    ServerAction, ServerActionLine, ServerMessageSender,
};
#[allow(clippy::enum_glob_use, reason = "Essential variants (like Some/Err)")]
use crate::types::BoolIsh::*;
use crate::types::{BoolIsh, Branch, Resolvable, ScanBlock, Script};
use crate::util::opt_res_ret;
use crate::values::bolt_message::{BoltMessage, SerializedBoltMessage};
use crate::values::bolt_struct::{
    JoltDate, JoltDateTime, JoltDuration, JoltNode, JoltPath, JoltPoint, JoltRelationship,
    JoltStruct, JoltTime, JoltUnsupportedType, JoltVector,
};
use crate::values::pack_stream_value::{PackStreamValue, PackStreamVersion};
use jolt_validators::build_fields_validator;

mod jolt_validators;

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
    let mut parsed_bang_lines = ParsedBangLines::new();

    for bang_line in bang_lines {
        match bang_line {
            BangLine::Version(ctx, (ctx_bolt, bolt), capabilities) => {
                parsed_bang_lines.with_version(ctx, ctx_bolt, bolt, capabilities.as_ref())?;
            }
            BangLine::HandshakeManifest(ctx, (ctx_arg, arg)) => {
                parsed_bang_lines.with_handshake_manifest(ctx, ctx_arg, arg)?;
            }
            BangLine::Handshake(ctx, (ctx_byte, byte_str)) => {
                parsed_bang_lines.with_handshake(ctx, ctx_byte, byte_str)?;
            }
            BangLine::HandshakeResponse(ctx, (ctx_byte, byte_str)) => {
                parsed_bang_lines.with_handshake_response(ctx, ctx_byte, byte_str)?;
            }
            BangLine::HandshakeDelay(ctx, (ctx_delay, delay)) => {
                parsed_bang_lines.with_handshake_delay(ctx, ctx_delay, delay)?;
            }
            BangLine::Auto(ctx, (ctx_msg, msg)) => {
                parsed_bang_lines.with_auto(ctx, ctx_msg, msg);
            }
            BangLine::AllowRestart(ctx) => {
                parsed_bang_lines.with_allow_restart(ctx)?;
            }
            BangLine::AllowConcurrent(ctx) => {
                parsed_bang_lines.with_allow_concurrent(ctx)?;
            }
            BangLine::Python(_, (line_ctx, line)) => {
                parsed_bang_lines.with_python(line_ctx, line.clone());
            }
            BangLine::Comment(_) => {}
        }
    }

    parsed_bang_lines.into()
}

#[derive(Debug)]
struct ParsedBangLines<'a> {
    bolt_version: Option<((u8, u8), BoltVersion, BoltCapabilities)>,
    handshake_manifest_version: Option<(Context, u8)>,
    handshake: Option<Vec<u8>>,
    handshake_response: Option<(Context, Vec<u8>)>,
    handshake_delay: Option<Duration>,
    allow_restart: Option<()>,
    allow_concurrent: Option<()>,
    auto_responses: IndexMap<&'a str, (Context, Context)>,
    py_lines: Vec<(Context, String)>,
}

impl<'a> ParsedBangLines<'a> {
    fn new() -> Self {
        Self {
            bolt_version: None,
            handshake_manifest_version: None,
            handshake: None,
            handshake_response: None,
            handshake_delay: None,
            allow_restart: None,
            allow_concurrent: None,
            auto_responses: IndexMap::new(),
            py_lines: Vec::new(),
        }
    }

    fn with_version(
        &mut self,
        ctx: &Context,
        ctx_bolt: &Context,
        bolt: &str,
        capabilities: Option<&(Context, String)>,
    ) -> Result<()> {
        if self.bolt_version.is_some() {
            return Err(ParseError::new_ctx(
                *ctx,
                "Multiple BOLT version bang lines found",
            ));
        }
        let (raw_version, version) = parse_bolt_version(*ctx_bolt, bolt)?;
        let capabilities = match capabilities {
            None => BoltCapabilities::default(),
            Some((ctx_cap, cap)) => {
                let cap_bytes = str_bytes::parse_stubscript_hex_string(cap)
                    .map_err(|e| ParseError::new_ctx(*ctx_cap, e.to_string()))?;
                BoltCapabilities::from_bytes(cap_bytes)
                    .map_err(|e| ParseError::new_ctx(*ctx_cap, e.to_string()))?
            }
        };
        self.bolt_version = Some((raw_version, version, capabilities));
        Ok(())
    }

    fn with_handshake_manifest(
        &mut self,
        ctx: &Context,
        ctx_arg: &Context,
        arg: &str,
    ) -> Result<()> {
        if self.handshake_manifest_version.is_some() {
            return Err(ParseError::new_ctx(
                *ctx,
                "Multiple handshake manifest bang lines found",
            ));
        }
        let version = u8::from_str(arg).map_err(|e| {
            ParseError::new_ctx(
                *ctx_arg,
                format!("Invalid handshake manifest version (expecting u8): {e}"),
            )
        })?;
        self.handshake_manifest_version = Some((*ctx, version));
        Ok(())
    }

    fn with_handshake(&mut self, ctx: &Context, ctx_byte: &Context, byte_str: &str) -> Result<()> {
        if self.handshake.is_some() {
            return Err(ParseError::new_ctx(
                *ctx,
                "Multiple handshake bang lines found",
            ));
        }

        let data = str_bytes::parse_stubscript_hex_string(byte_str)
            .map_err(|e| ParseError::new_ctx(*ctx_byte, e.to_string()))?;
        self.handshake = Some(data);
        Ok(())
    }

    fn with_handshake_response(
        &mut self,
        ctx: &Context,
        ctx_byte: &Context,
        byte_str: &str,
    ) -> Result<()> {
        if self.handshake_response.is_some() {
            return Err(ParseError::new_ctx(
                *ctx,
                "Multiple handshake response bang lines found",
            ));
        }

        let data = str_bytes::parse_stubscript_hex_string(byte_str)
            .map_err(|e| ParseError::new_ctx(*ctx_byte, e.to_string()))?;
        self.handshake_response = Some((*ctx, data));
        Ok(())
    }

    fn with_handshake_delay(
        &mut self,
        ctx: &Context,
        ctx_delay: &Context,
        delay: &str,
    ) -> Result<()> {
        if self.handshake_delay.is_some() {
            return Err(ParseError::new_ctx(
                *ctx,
                "Multiple handshake delay bang lines found",
            ));
        }

        let delay =
            f64::from_str(delay).map_err(|e| ParseError::new_ctx(*ctx_delay, e.to_string()))?;
        let delay = Duration::try_from_secs_f64(delay).map_err(|e| {
            ParseError::new_ctx(*ctx_delay, format!("Failed to parse handshake delay: {e}"))
        })?;
        self.handshake_delay = Some(delay);
        Ok(())
    }

    fn with_auto(&mut self, ctx: &Context, ctx_msg: &Context, msg: &'a str) {
        let ctx_old = self.auto_responses.insert(msg, (*ctx, *ctx_msg));
        if let Some((ctx_old, _)) = ctx_old {
            warn!(
                "Specified auto response for message \"{msg}\" more than once \
                ({ctx} and {ctx_old})."
            );
        }
    }

    fn with_allow_restart(&mut self, ctx: &Context) -> Result<()> {
        if self.allow_restart.is_some() {
            return Err(ParseError::new_ctx(
                *ctx,
                "Multiple allow restart bang lines found",
            ));
        }
        self.allow_restart = Some(());
        Ok(())
    }

    fn with_allow_concurrent(&mut self, ctx: &Context) -> Result<()> {
        if self.allow_concurrent.is_some() {
            return Err(ParseError::new_ctx(
                *ctx,
                "Multiple allow concurrent bang lines found",
            ));
        }
        self.allow_concurrent = Some(());
        Ok(())
    }

    fn with_python(&mut self, line_ctx: &Context, line: String) {
        self.py_lines.push((*line_ctx, line));
    }
}

impl<'a> From<ParsedBangLines<'a>> for Result<ActorConfig> {
    fn from(value: ParsedBangLines<'a>) -> Self {
        let (bolt_version_raw, bolt_version, bolt_capabilities) = value
            .bolt_version
            .ok_or(ParseError::new("Bolt version bang line missing"))?;
        let allow_concurrent = value.allow_concurrent.is_some();
        let allow_restart = value.allow_restart.is_some();
        if allow_concurrent && allow_restart {
            warn!(
                "Both allow restart and allow concurrent bang lines were found. \
                Allow concurrent already implies allow restart."
            );
        }
        let handshake_manifest_version = match value.handshake_manifest_version {
            None => None,
            Some((ctx, handshake_manifest_version)) => {
                if value.handshake.is_some() || value.handshake_response.is_some() {
                    return Err(ParseError::new_ctx(
                        ctx,
                        "Handshake manifest version bang line cannot be used with handshake or \
                        handshake response bang lines",
                    ));
                }
                Some(handshake_manifest_version)
            }
        };
        let handshake_response = match value.handshake_response {
            None => None,
            Some((ctx, handshake_response)) => {
                if value.handshake.is_none() {
                    return Err(ParseError::new_ctx(
                        ctx,
                        "Handshake response bang line requires handshake bang line to be present",
                    ));
                }
                Some(handshake_response)
            }
        };
        let auto_responses = value
            .auto_responses
            .into_iter()
            .map(|(msg, (_, ctx_msg))| parse_auto_bang_line(msg, ctx_msg, bolt_version))
            .collect::<Result<_>>()?;

        Ok(ActorConfig {
            bolt_version,
            bolt_version_raw,
            bolt_capabilities,
            handshake_manifest_version,
            handshake: value.handshake,
            handshake_response,
            handshake_delay: value.handshake_delay,
            allow_restart,
            allow_concurrent,
            auto_responses,
            py_lines: value.py_lines,
        })
    }
}

fn parse_auto_bang_line(
    msg: &str,
    ctx_msg: Context,
    bolt_version: BoltVersion,
) -> Result<(u8, AutoBangLineHandler)> {
    let Some(request_tag) = bolt_version.message_tag_from_request(msg) else {
        return Err(ParseError::new_ctx(
            ctx_msg,
            format!("Unknown request message name {msg:?} for BOLT version {bolt_version}"),
        ));
    };
    let response_resolver = bolt_version.message_auto_response(request_tag);
    Ok((
        request_tag,
        AutoBangLineHandler {
            ctx: ctx_msg,
            sender: Box::new(SenderBytes::new_auto_response(response_resolver)),
        },
    ))
}

fn parse_bolt_version(ctx: Context, s: &str) -> Result<((u8, u8), BoltVersion)> {
    fn convert_bolt_version(
        ctx: Context,
        major: u8,
        minor: Option<u8>,
    ) -> Result<((u8, u8), BoltVersion)> {
        let version = BoltVersion::match_valid_version(major, minor).ok_or_else(|| {
            let version = match minor {
                None => format!("{major}"),
                Some(minor) => format!("{major}.{minor}"),
            };
            ParseError::new_ctx(ctx, format!("Unknown BOLT version {version}"))
        })?;
        Ok(((major, minor.unwrap_or_default()), version))
    }

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

fn parse_block(block: &ScanBlock, config: &ActorConfig) -> Result<IntermediateActorBlock> {
    match block {
        ScanBlock::List(ctx, scan_blocks) => {
            parse_blocks(*ctx, scan_blocks, config).map(Into::into)
        }
        ScanBlock::Alt(ctx, scan_blocks) => {
            parse_alt_block(config, ctx, scan_blocks).map(Into::into)
        }
        ScanBlock::Parallel(ctx, scan_blocks) => {
            parse_parallel_block(config, ctx, scan_blocks).map(Into::into)
        }
        ScanBlock::Optional(ctx, optional_scan_block) => {
            parse_optional_block(config, ctx, optional_scan_block).map(Into::into)
        }
        ScanBlock::Repeat0(ctx, b) => parse_repeat_block(config, ctx, b, 0).map(Into::into),
        ScanBlock::Repeat1(ctx, b) => parse_repeat_block(config, ctx, b, 1).map(Into::into),
        ScanBlock::ClientMessage(ctx, (message_name_ctx, message_name), body) => {
            parse_client_message_block(config, ctx, message_name_ctx, message_name, body.as_ref())
                .map(Into::into)
        }
        ScanBlock::ServerMessage(ctx, (message_name_ctx, message_name), body) => {
            parse_server_message_block(config, ctx, message_name_ctx, message_name, body.as_ref())
                .map(Into::into)
        }
        ScanBlock::ServerAction(ctx, (action_name_ctx, action_name), body) => {
            parse_server_action_block(ctx, action_name_ctx, action_name, body.as_ref())
                .map(Into::into)
        }
        ScanBlock::AutoMessage(
            ctx,
            (client_message_name_ctx, client_message_name),
            client_body,
        ) => parse_auto_message_block(
            config,
            ctx,
            client_message_name_ctx,
            client_message_name,
            client_body.as_ref(),
        )
        .map(Into::into),
        ScanBlock::Comment(ctx) => Ok(ActorBlock::NoOp(*ctx).into()),
        ScanBlock::Python(ctx, (_, py)) => {
            // python can't be validated, so we just assume it will work, if it errors at run time,
            // the actor can explain error from ctx
            Ok(ActorBlock::Python(*ctx, py.clone()).into())
        }
        ScanBlock::ConditionPart(ctx, branch_type, condition, body) => {
            parse_condition_part_block(config, ctx, *branch_type, condition.as_ref(), body)
        }
    }
}

fn parse_alt_block(
    config: &ActorConfig,
    ctx: &Context,
    scan_blocks: &[ScanBlock],
) -> Result<ActorBlock> {
    let mut actor_blocks = Vec::with_capacity(scan_blocks.len());
    for block in scan_blocks {
        let b = parse_block(block, config)?;
        let b = finalize_intermediate(b)?;
        validate_alt_child(&b)?;
        actor_blocks.push(b);
    }
    Ok(ActorBlock::Alt(*ctx, actor_blocks))
}

fn parse_parallel_block(
    config: &ActorConfig,
    ctx: &Context,
    scan_blocks: &[ScanBlock],
) -> Result<ActorBlock> {
    let mut actor_blocks = Vec::with_capacity(scan_blocks.len());
    for block in scan_blocks {
        let b = parse_block(block, config)?;
        let b = finalize_intermediate(b)?;
        validate_parallel_child(&b)?;
        actor_blocks.push(b);
    }
    Ok(ActorBlock::Parallel(*ctx, actor_blocks))
}

fn parse_optional_block(
    config: &ActorConfig,
    ctx: &Context,
    optional_scan_block: &ScanBlock,
) -> Result<ActorBlock> {
    let b = parse_block(optional_scan_block, config)?;
    let b = finalize_intermediate(b)?;
    validate_non_action(&b, Some("inside an optional block"))?;
    validate_non_empty(&b, Some("inside an optional block"))?;
    Ok(ActorBlock::Optional(*ctx, Box::new(b)))
}

fn parse_repeat_block(
    config: &ActorConfig,
    ctx: &Context,
    body_block: &ScanBlock,
    repetitions: usize,
) -> Result<ActorBlock> {
    let body_block = parse_block(body_block, config)?;
    let body_block = finalize_intermediate(body_block)?;
    validate_non_action(&body_block, Some("inside a repeat block"))?;
    validate_non_empty(&body_block, Some("inside a repeat block"))?;
    Ok(ActorBlock::Repeat(*ctx, Box::new(body_block), repetitions))
}

fn parse_client_message_block(
    config: &ActorConfig,
    ctx: &Context,
    message_name_ctx: &Context,
    message_name: &str,
    body: Option<&(Context, String)>,
) -> Result<ActorBlock> {
    let validator = create_validator(
        *message_name_ctx,
        message_name,
        body.map(|(ctx, body)| (*ctx, body.as_str())),
        config,
    )
    .map_err(|mut e| {
        e.ctx.get_or_insert(*ctx);
        e
    })?;
    Ok(ActorBlock::ClientMessageValidate(*ctx, validator))
}

fn parse_server_message_block(
    config: &ActorConfig,
    ctx: &Context,
    message_name_ctx: &Context,
    message_name: &str,
    body: Option<&(Context, String)>,
) -> Result<ActorBlock> {
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
    Ok(ActorBlock::ServerMessageSend(*ctx, server_message_sender))
}

fn parse_server_action_block(
    ctx: &Context,
    action_name_ctx: &Context,
    action_name: &str,
    body: Option<&(Context, String)>,
) -> Result<ActorBlock> {
    let server_action = create_server_action(
        *action_name_ctx,
        action_name,
        body.as_ref().map(|(ctx, body)| (*ctx, body.as_str())),
    )
    .map_err(|mut e| {
        e.ctx.get_or_insert(*ctx);
        e
    })?;
    Ok(ActorBlock::ServerActionLine(*ctx, server_action))
}

fn parse_auto_message_block(
    config: &ActorConfig,
    ctx: &Context,
    client_message_name_ctx: &Context,
    client_message_name: &str,
    client_body: Option<&(Context, String)>,
) -> Result<ActorBlock> {
    Ok(ActorBlock::AutoMessage(
        *ctx,
        create_auto_message_handler(
            *client_message_name_ctx,
            client_message_name,
            client_body
                .as_ref()
                .map(|(ctx, body)| (*ctx, body.as_str())),
            config,
        )?,
    ))
}

fn parse_condition_part_block(
    config: &ActorConfig,
    ctx: &Context,
    branch_type: Branch,
    condition: Option<&(Context, String)>,
    body: &ScanBlock,
) -> Result<IntermediateActorBlock> {
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

fn parse_blocks(ctx: Context, blocks: &[ScanBlock], config: &ActorConfig) -> Result<ActorBlock> {
    let mut actor_blocks = condense_actor_blocks(blocks.iter().map(|b| parse_block(b, config)))?;
    validate_list_children(&actor_blocks)?;

    match actor_blocks.len() {
        0 => Ok(ActorBlock::NoOp(ctx)),
        1 => Ok(actor_blocks.remove(0)),
        _ => Ok(ActorBlock::BlockList(ctx, actor_blocks)),
    }
}

fn condense_actor_blocks(
    blocks: impl IntoIterator<Item = Result<IntermediateActorBlock>>,
) -> Result<Vec<ActorBlock>> {
    let blocks = blocks.into_iter();
    let mut res = Vec::with_capacity(blocks.size_hint().1.unwrap_or_default());
    for block in blocks {
        let block = block?;
        let prev = res.last_mut();
        match (block, prev) {
            (IntermediateActorBlock::Finished(ActorBlock::NoOp(_)), _) => {}
            (
                IntermediateActorBlock::Finished(ActorBlock::BlockList(ctx, list)),
                Some(ActorBlock::BlockList(ctx_prev, list_prev)),
            ) => {
                *ctx_prev = ctx_prev.fuse(&ctx);
                list_prev.extend(list);
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
                Some(ActorBlock::Condition(ctx_prev, cond_prev)),
            ) => {
                if cond_prev.else_.is_some() {
                    // previous condition has an ELSE, so we can't add another ELIF
                    return Err(missing_leading_if(ctx, "ELIF"));
                }
                *ctx_prev = ctx_prev.fuse(&ctx);
                cond_prev.else_if.push((ctx, cond, body));
            }
            (IntermediateActorBlock::PartialCondition(PartialCondition::ElseIf((ctx, ..))), _) => {
                return Err(missing_leading_if(ctx, "ELIF"));
            }
            (
                IntermediateActorBlock::PartialCondition(PartialCondition::Else((ctx, body))),
                Some(ActorBlock::Condition(ctx_prev, cond_prev)),
            ) => {
                if cond_prev.else_.is_some() {
                    // previous condition has an ELSE, so we can't add another ELSE
                    return Err(missing_leading_if(ctx, "ELSE"));
                }
                *ctx_prev = ctx_prev.fuse(&ctx);
                cond_prev.else_ = Some((ctx, body));
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
pub struct AutoBangLineHandler {
    pub ctx: Context,
    pub sender: Box<dyn ServerMessageSender>,
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
        .message_auto_response(client_message_tag);
    let server_sender = Box::new(SenderBytes::new_auto_response(response_resolver));
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
    let data = BoltMessage::new(tag, fields, config.bolt_version)
        .into_serialized()
        .map_err(|e| {
            ParseError::new_ctx(message_name_ctx, format!("Cannot serialize message: {e}"))
        })?
        .data;

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
        data: Box<dyn Fn() -> SerializedBoltMessage + Send + Sync>,
        repr: String,
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

    pub fn new_auto_response(resolver: Resolvable<SerializedBoltMessage>) -> Self {
        match resolver {
            Resolvable::Static(message) => {
                let SerializedBoltMessage {
                    message: response_message,
                    data: response_data,
                } = message;
                let response_repr = response_message.repr();
                Self::new(response_data, SenderBytesLine::Repr(response_repr))
            }
            Resolvable::Dynamic { func, repr } => Self::Dynamic { data: func, repr },
        }
    }
}

impl ScriptLine for SenderBytes {
    fn line_repr<'a: 'c, 'b: 'c, 'c>(&'b self, script: &'a str) -> Option<&'c str> {
        match self {
            Self::Static { line, .. } => Some(match line {
                SenderBytesLine::Ctx { ctx, .. } => ctx.original_source(script),
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
            Self::Dynamic { data: _, repr } => f
                .debug_struct("SenderBytes::Dynamic")
                .field("data", &"...")
                .field("repr", repr)
                .finish(),
        }
    }
}

impl ServerMessageSender for SenderBytes {
    fn send(&self) -> anyhow::Result<Cow<'_, [u8]>> {
        Ok(match self {
            SenderBytes::Static { data, line: _ } => data.into(),
            SenderBytes::Dynamic { data, repr: _ } => {
                let SerializedBoltMessage { message: _, data } = data();
                data.into()
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

pub fn transcode_field(field: JsonValue, config: &ActorConfig) -> Result<PackStreamValue> {
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
    let Some(parsed_sigil) = JoltSigil::from_str(versionless_sigil, jolt_version) else {
        return Ok(IsJoltValue::No([(sigil, value)].into_iter().collect()));
    };
    match parsed_sigil {
        JoltSigil::Bool => transcode_jolt_value_bool(value, config),
        JoltSigil::Integer => transcode_jolt_value_int(value, config),
        JoltSigil::Float => transcode_jolt_value_float(value),
        JoltSigil::String => transcode_jolt_value_str(value, config),
        JoltSigil::Bytes => transcode_jolt_value_bytes(value),
        JoltSigil::List => transcode_jolt_value_list(value, config),
        JoltSigil::Dict => transcode_jolt_value_dict(value, config),
        JoltSigil::Temporal => transcode_jolt_value_temporal(value, jolt_version),
        JoltSigil::Spatial => transcode_jolt_value_spatial(value, jolt_version),
        JoltSigil::Node => transcode_jolt_value_node(value, jolt_version, config),
        JoltSigil::RelationshipForward => {
            transcode_jolt_value_relationship_forward(value, jolt_version, config)
        }
        JoltSigil::RelationshipBackward => {
            transcode_jolt_value_relationship_backward(value, jolt_version, config)
        }
        JoltSigil::Path => transcode_jolt_value_path(value, jolt_version, config),
        JoltSigil::Vector => transcode_jolt_value_vector(value, jolt_version, config),
        JoltSigil::Uuid => transcode_jolt_value_uuid(value, config),
        JoltSigil::UnsupportedType => {
            transcode_jolt_value_unsupported_type(value, jolt_version, config)
        }
        JoltSigil::Struct => transcode_jolt_value_struct(value, jolt_version, config),
    }
}

fn transcode_jolt_value_bool(value: JsonValue, config: &ActorConfig) -> Result<IsJoltValue> {
    if !value.is_boolean() {
        return Err(ParseError::new(format!(
            "Expected bool after sigil \"?\", but found {value:?}",
        )));
    }
    Ok(IsJoltValue::Yes(transcode_field(value, config)?))
}

fn transcode_jolt_value_int(value: JsonValue, config: &ActorConfig) -> Result<IsJoltValue> {
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
    let packstream_value = transcode_field(JsonValue::Number(value.into()), config)?;
    Ok(IsJoltValue::Yes(packstream_value))
}

fn transcode_jolt_value_float(value: JsonValue) -> Result<IsJoltValue> {
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
    Ok(IsJoltValue::Yes(PackStreamValue::Float(value)))
}

fn transcode_jolt_value_str(value: JsonValue, config: &ActorConfig) -> Result<IsJoltValue> {
    if !value.is_string() {
        return Err(ParseError::new(format!(
            "Expected string after sigil \"U\", but found {value:?}",
        )));
    }
    Ok(IsJoltValue::Yes(transcode_field(value, config)?))
}

fn transcode_jolt_value_bytes(value: JsonValue) -> Result<IsJoltValue> {
    let bytes = parse_jolt_bytes(value)?;
    Ok(IsJoltValue::Yes(PackStreamValue::Bytes(bytes)))
}

fn transcode_jolt_value_list(value: JsonValue, config: &ActorConfig) -> Result<IsJoltValue> {
    if !value.is_array() {
        return Err(ParseError::new(format!(
            "Expected array after sigil \"[]\", but found {value:?}",
        )));
    }
    Ok(IsJoltValue::Yes(transcode_field(value, config)?))
}

fn transcode_jolt_value_dict(value: JsonValue, config: &ActorConfig) -> Result<IsJoltValue> {
    if !value.is_object() {
        return Err(ParseError::new(format!(
            "Expected object after sigil \"{{}}\", but found {value:?}",
        )));
    }
    Ok(IsJoltValue::Yes(transcode_field(value, config)?))
}

fn transcode_jolt_value_temporal(
    value: JsonValue,
    jolt_version: JoltVersion,
) -> Result<IsJoltValue> {
    let JsonValue::String(value) = value else {
        return Err(ParseError::new(format!(
            "Expected temporal string after sigil \"T\", but found {value:?}",
        )));
    };
    let value = transcode_jolt_value_date(&value, jolt_version)
        .or_else(|| transcode_jolt_value_time(&value, jolt_version))
        .or_else(|| transcode_jolt_value_date_time(&value, jolt_version))
        .or_else(|| transcode_jolt_value_duration(&value, jolt_version))
        .transpose()?
        .ok_or_else(|| {
            ParseError::new(format!(
                "Expected temporal string after sigil \"T\", but found {value:?}",
            ))
        })?;
    Ok(IsJoltValue::Yes(value))
}

fn transcode_jolt_value_date(
    s: &str,
    jolt_version: JoltVersion,
) -> Option<Result<PackStreamValue>> {
    let bolt_date = opt_res_ret!(JoltDate::parse(s, jolt_version));
    let value_struct = bolt_date.as_struct();
    Some(Ok(PackStreamValue::Struct(value_struct)))
}

fn transcode_jolt_value_time(
    s: &str,
    jolt_version: JoltVersion,
) -> Option<Result<PackStreamValue>> {
    let bolt_time = opt_res_ret!(JoltTime::parse(s, jolt_version));
    let value_struct = opt_res_ret!(bolt_time.as_struct());
    Some(Ok(PackStreamValue::Struct(value_struct)))
}

fn transcode_jolt_value_date_time(
    s: &str,
    jolt_version: JoltVersion,
) -> Option<Result<PackStreamValue>> {
    let bolt_date_time = opt_res_ret!(JoltDateTime::parse(s, jolt_version));
    let value_struct = opt_res_ret!(bolt_date_time.into_struct());
    Some(Ok(PackStreamValue::Struct(value_struct)))
}

fn transcode_jolt_value_duration(
    s: &str,
    jolt_version: JoltVersion,
) -> Option<Result<PackStreamValue>> {
    let bolt_duration = opt_res_ret!(JoltDuration::parse(s, jolt_version));
    let value_struct = bolt_duration.as_struct();
    Some(Ok(PackStreamValue::Struct(value_struct)))
}

fn transcode_jolt_value_spatial(
    value: JsonValue,
    jolt_version: JoltVersion,
) -> Result<IsJoltValue> {
    let JsonValue::String(value) = value else {
        return Err(ParseError::new(format!(
            "Expected spatial string after sigil \"@\", but found {value:?}",
        )));
    };
    let bolt_point = JoltPoint::parse(&value, jolt_version)?;
    Ok(IsJoltValue::Yes(PackStreamValue::Struct(
        bolt_point.as_struct(),
    )))
}

fn transcode_jolt_value_node(
    value: JsonValue,
    jolt_version: JoltVersion,
    config: &ActorConfig,
) -> Result<IsJoltValue> {
    let bolt_node = JoltNode::parse(value, jolt_version, config)?;
    Ok(IsJoltValue::Yes(PackStreamValue::Struct(
        bolt_node.into_struct(),
    )))
}

fn transcode_jolt_value_relationship_forward(
    value: JsonValue,
    jolt_version: JoltVersion,
    config: &ActorConfig,
) -> Result<IsJoltValue> {
    let bolt_relationship = JoltRelationship::parse(value, jolt_version, config)?;
    Ok(IsJoltValue::Yes(PackStreamValue::Struct(
        bolt_relationship.into_struct(),
    )))
}

fn transcode_jolt_value_relationship_backward(
    value: JsonValue,
    jolt_version: JoltVersion,
    config: &ActorConfig,
) -> Result<IsJoltValue> {
    let mut bolt_relationship = JoltRelationship::parse(value, jolt_version, config)?;
    bolt_relationship.flip_direction();
    Ok(IsJoltValue::Yes(PackStreamValue::Struct(
        bolt_relationship.into_struct(),
    )))
}

fn transcode_jolt_value_path(
    value: JsonValue,
    jolt_version: JoltVersion,
    config: &ActorConfig,
) -> Result<IsJoltValue> {
    let bolt_path = JoltPath::parse(value, jolt_version, config)?;
    Ok(IsJoltValue::Yes(PackStreamValue::Struct(
        bolt_path.into_struct(),
    )))
}

fn transcode_jolt_value_vector(
    value: JsonValue,
    jolt_version: JoltVersion,
    config: &ActorConfig,
) -> Result<IsJoltValue> {
    let bolt_path = JoltVector::parse(value, jolt_version, config)?;
    Ok(IsJoltValue::Yes(PackStreamValue::Struct(
        bolt_path.into_struct(),
    )))
}

fn transcode_jolt_value_uuid(value: JsonValue, config: &ActorConfig) -> Result<IsJoltValue> {
    let packstream_version = config.bolt_version.packstream_version();
    if packstream_version < PackStreamVersion::V2 {
        return Err(ParseError::new(format!(
            "UUID is not supported in {packstream_version}"
        )));
    }

    let JsonValue::String(value) = value else {
        return Err(ParseError::new(format!(
            "Expected string after sigil \"UU\", but found {value:?}",
        )));
    };
    let uuid = Uuid::parse_str(&value).map_err(|e| {
        ParseError::new(format!(
            "Failed to parse UUID string after sigil \"UU\": {value:?}: {e}",
        ))
    })?;
    Ok(IsJoltValue::Yes(PackStreamValue::Uuid(uuid)))
}

fn transcode_jolt_value_unsupported_type(
    value: JsonValue,
    jolt_version: JoltVersion,
    config: &ActorConfig,
) -> Result<IsJoltValue> {
    let bolt_unsupported_type = JoltUnsupportedType::parse(value, jolt_version, config)?;
    Ok(IsJoltValue::Yes(PackStreamValue::Struct(
        bolt_unsupported_type.into_struct(),
    )))
}

fn transcode_jolt_value_struct(
    value: JsonValue,
    jolt_version: JoltVersion,
    config: &ActorConfig,
) -> Result<IsJoltValue> {
    let bolt_unsupported_type = JoltStruct::parse(value, jolt_version, config)?;
    Ok(IsJoltValue::Yes(PackStreamValue::Struct(
        bolt_unsupported_type.into_struct(),
    )))
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
        Some(self.ctx.original_source(script))
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
    str_bytes::parse_stubscript_hex_string(body)
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
            .finish_non_exhaustive()
    }
}

impl<T: Fn(&BoltMessage) -> anyhow::Result<()> + Send + Sync> ScriptLine for ValidatorImpl<T> {
    fn line_repr<'a: 'c, 'b: 'c, 'c>(&'b self, script: &'a str) -> Option<&'c str> {
        Some(self.ctx.original_source(script))
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
    str_bytes::parse_jolt_hex_string(s).map_err(|e| ParseError::new(format!("{e}")))
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
                    .is_none_or(|(_, else_block)| has_deterministic_end(else_block))
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
                    .is_some_and(|(_, else_block)| is_action_block(else_block))
        }
        ActorBlock::Optional(_, block) => is_action_block(block),
        ActorBlock::Repeat(_, block, _) => is_action_block(block),
        ActorBlock::ClientMessageValidate(..)
        | ActorBlock::AutoMessage(..)
        | ActorBlock::NoOp(..) => false,
    }
}

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
                    True => {}
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
        ActorBlock::Optional(_, block) | ActorBlock::Repeat(_, block, _) => {
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
    ParseError::new_ctx(ctx, format!("Missing leading IF for {block_name}"))
}

#[cfg(test)]
mod tests {
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
            auto_responses: IndexMap::default(),
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

        validator.validate(&cm).unwrap();
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
