use std::cmp::max;
use std::ops::RangeTo;

use anyhow::anyhow;
use log::trace;
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while1};
use nom::character::complete::{line_ending, multispace0, not_line_ending, space0, space1};
use nom::combinator::{complete, cond, consumed, eof, map, opt, peek, recognize, value};
use nom::error::{context, ErrorKind, FromExternalError, ParseError};
use nom::multi::{many1, many_till};
use nom::sequence::{delimited, pair, preceded, terminated};
use nom::{AsChar, Compare, InputLength, InputTake, InputTakeAtPosition, Offset, Parser, Slice};
use nom_span::Spanned;

use crate::bang_line::BangLine;
use crate::context::Context;
use crate::error::script_excerpt;
use crate::types::{Branch, ScanBlock, Script};

type PError<I> = nom::error::Error<I>;
type Input<'a> = Spanned<&'a str>;
type IResult<'a, O> = nom::IResult<Input<'a>, O, PError<Input<'a>>>;

impl From<Input<'_>> for Context {
    fn from(value: Input) -> Self {
        let start_line_number = value.line();
        Self {
            start_line_number,
            end_line_number: start_line_number + max(1, value.data().lines().count()) - 1,
            start_byte: value.byte_offset(),
            end_byte: value.byte_offset() + value.len(),
        }
    }
}

pub fn scan_script<'a>(
    input: &'a str,
    name: &'a str,
) -> Result<Script<'a>, nom::Err<PError<Input<'a>>>> {
    let span = Spanned::new(input, true);
    let (span, (bangs, body)) = complete(terminated(
        pair(
            preceded(multispace0, context("Bang line headers", scan_bang_lines)),
            delimited(multispace0, opt(scan_body), multispace0),
        ),
        eof,
    ))(span)?;
    if !span.is_empty() {
        return Err(nom::Err::Failure(PError::from_external_error(
            span,
            ErrorKind::Complete,
            anyhow!("Trailing input"),
        )));
    }
    let body = body.unwrap_or((span.into(), vec![]));

    trace!(
        "Scan output\n\
        ================================================================\n\
        bang_lines: {bangs:#?}\n\
        body: {body:#?}\n\
        ================================================================",
    );

    Ok(Script {
        name,
        bang_lines: bangs,
        body,
        input,
    })
}

pub fn contextualize_res<T>(
    res: Result<T, nom::Err<PError<Input>>>,
    script_name: &str,
    script: &str,
) -> anyhow::Result<T> {
    match res {
        Ok(t) => Ok(t),
        Err(e) => Err({
            match e {
                nom::Err::Incomplete(_) => {
                    panic!("Working only with complete data")
                }
                nom::Err::Error(e) | nom::Err::Failure(e) => {
                    let mut ctx: Context = e.input.into();
                    ctx.end_byte = ctx.start_byte + 1;
                    ctx.end_line_number = ctx.start_line_number;
                    anyhow!(
                        "Syntax error near\n{}",
                        script_excerpt(script_name, script, ctx)
                    )
                }
            }
        }),
    }
}

// #################
// Header/Bang Lines
// #################
fn scan_bang_lines(input: Input) -> IResult<Vec<BangLine>> {
    many1(alt((
        context("bolt version bang", bolt_version_bang_line),
        context(
            "handshake manifest bang",
            string_arg_bang_line("HANDSHAKE_MANIFEST", BangLine::HandshakeManifest),
        ),
        context(
            "handshake bang",
            string_arg_bang_line("HANDSHAKE", BangLine::Handshake),
        ),
        context(
            "handshake response bang",
            string_arg_bang_line("HANDSHAKE_RESPONSE", BangLine::HandshakeResponse),
        ),
        context(
            "handshake delay bang",
            string_arg_bang_line("HANDSHAKE_DELAY", BangLine::HandshakeDelay),
        ),
        context("auto bang", auto_bang_line),
        context(
            "allow restart",
            simple_bang_line("ALLOW RESTART", BangLine::AllowRestart),
        ),
        context(
            "allow concurrent",
            simple_bang_line("ALLOW CONCURRENT", BangLine::AllowConcurrent),
        ),
        context(
            "python response bang",
            string_arg_bang_line("PY", BangLine::Python),
        ),
        context("comment bang line", comment(BangLine::Comment)),
    )))(input)
}

fn bolt_version_bang_line(input: Input) -> IResult<BangLine> {
    map(
        preceded(
            multispace0,
            consumed(preceded(
                bang_line("BOLT"),
                preceded(
                    space1,
                    terminated(
                        alt((
                            map(
                                pair(non_space, preceded(space1, rest_of_line)),
                                |(arg1, arg2)| (arg1, Some(arg2)),
                            ),
                            map(rest_of_line, |arg1| (arg1, None)),
                        )),
                        peek(end_of_line),
                    ),
                ),
            )),
        ),
        |(ctx, (arg1, arg2))| {
            BangLine::Version(
                ctx.into(),
                (arg1.into(), String::from(*arg1)),
                arg2.map(|arg| (arg.into(), String::from(*arg))),
            )
        },
    )(input)
}

fn auto_bang_line(input: Input) -> IResult<BangLine> {
    map(
        preceded(
            multispace0,
            consumed(preceded(
                bang_line("AUTO"),
                preceded(space1, terminated(message_name, peek(end_of_line))),
            )),
        ),
        |(ctx, message)| BangLine::Auto(ctx.into(), (message.into(), String::from(*message))),
    )(input)
}

fn string_arg_bang_line<'a>(
    expect: &'static str,
    mut res: impl FnMut(Context, (Context, String)) -> BangLine + 'static,
) -> impl FnMut(Input<'a>) -> IResult<'a, BangLine> {
    map(
        preceded(
            multispace0,
            consumed(preceded(
                bang_line(expect),
                preceded(space1, terminated(rest_of_line, peek(end_of_line))),
            )),
        ),
        move |(ctx, message)| res(ctx.into(), (message.into(), String::from(*message))),
    )
}

fn simple_bang_line<'a>(
    expect: &'static str,
    mut res: impl FnMut(Context) -> BangLine,
) -> impl FnMut(Input<'a>) -> IResult<'a, BangLine> {
    map(
        preceded(
            multispace0,
            terminated(recognize(bang_line(expect)), peek(end_of_line)),
        ),
        move |ctx| res(ctx.into()),
    )
}

fn bang_line<'a>(expect: &'static str) -> impl FnMut(Input<'a>) -> IResult<'a, Input<'a>> {
    preceded(tag("!:"), preceded(space1, tag(expect)))
}

// ##########
//    Body
// ##########
fn wrap_block_vec(context: Context, blocks: Vec<ScanBlock>) -> ScanBlock {
    match blocks.len() {
        1 => blocks.into_iter().next().unwrap(),
        _ => ScanBlock::List(context, blocks),
    }
}

fn scan_body(input: Input) -> IResult<(Context, Vec<ScanBlock>)> {
    let (input, (i, blocks)) = consumed(many1(scan_block))(input)?;
    Ok((input, (i.into(), blocks)))
}

fn scan_block(input: Input) -> IResult<ScanBlock> {
    preceded(
        multispace0,
        alt((
            context(
                "alternative block",
                map(consumed(multiblock("{{", "}}", "----")), |(i, b)| {
                    ScanBlock::Alt(i.into(), b)
                }),
            ),
            context(
                "parallel block",
                map(consumed(multiblock("{{", "}}", "++++")), |(i, b)| {
                    ScanBlock::Parallel(i.into(), b)
                }),
            ),
            context(
                "simple block",
                map(consumed(block("{{", "}}")), |(i, b)| {
                    wrap_block_vec(i.into(), b)
                }),
            ),
            context(
                "repeat 0 block",
                map(consumed(block("{*", "*}")), |(i, b)| {
                    ScanBlock::Repeat0(i.into(), Box::new(wrap_block_vec(i.into(), b)))
                }),
            ),
            context(
                "repeat 1 block",
                map(consumed(block("{+", "+}")), |(i, b)| {
                    ScanBlock::Repeat1(i.into(), Box::new(wrap_block_vec(i.into(), b)))
                }),
            ),
            context(
                "optional block",
                map(consumed(block("{?", "?}")), |(i, b)| {
                    ScanBlock::Optional(i.into(), Box::new(wrap_block_vec(i.into(), b)))
                }),
            ),
            context(
                "auto line",
                message_with_simple_name(Some("A:"), ScanBlock::AutoMessage),
            ),
            context("auto repeat 0 line", auto_repeat_0),
            context("auto repeat 1 line", auto_repeat_1),
            context("auto optional", auto_optional),
            context("comment line", comment(ScanBlock::Comment)),
            context(
                "python lines",
                message_simple_content(Some("PY:"), ScanBlock::Python),
            ),
            context(
                "IF",
                message_simple_content_with_block(Some("IF:"), |outer, inner, block| {
                    ScanBlock::ConditionPart(outer, Branch::If, Some(inner), Box::new(block))
                }),
            ),
            context(
                "ELIF",
                message_simple_content_with_block(Some("ELIF:"), |outer, inner, block| {
                    ScanBlock::ConditionPart(outer, Branch::ElseIf, Some(inner), Box::new(block))
                }),
            ),
            context(
                "ELSE",
                message_empty_content_with_block(Some("ELSE:"), |outer, block| {
                    ScanBlock::ConditionPart(outer, Branch::Else, None, Box::new(block))
                }),
            ),
            context(
                "client lines",
                multi_message(Some("C:"), ScanBlock::ClientMessage),
            ),
            context(
                "server lines",
                // TODO: use server_message_name
                multi_message_or_action(
                    Some("S:"),
                    ScanBlock::ServerMessage,
                    ScanBlock::ServerAction,
                ),
            ),
        )),
    )(input)
}

fn keyword(input: Input) -> IResult<()> {
    void(alt((
        tag("{{"),
        tag("}}"),
        tag("----"),
        tag("++++"),
        tag("{*"),
        tag("*}"),
        tag("{+"),
        tag("+}"),
        tag("{?"),
        tag("?}"),
        tag("C:"),
        tag("S:"),
        tag("PY:"),
        tag("A:"),
        tag("*:"),
        tag("+:"),
        tag("?:"),
        tag("#"),
    )))(input)
}

// ############
// Simple Lines
// ############
fn multi_message<'a, 'b>(
    message_tag: Option<&'static str>,
    mut block: impl FnMut(Context, (Context, String), Option<(Context, String)>) -> ScanBlock + 'b,
) -> impl FnMut(Input<'a>) -> IResult<'a, ScanBlock> + 'b {
    move |input| {
        let (input, (ctx, blocks)) = consumed(multi_message_vec(
            message_tag,
            |ctx, msg_name, body| block(ctx, (msg_name.into(), String::from(*msg_name)), body),
            message_name,
        ))(input)?;
        Ok((input, wrap_block_vec(ctx.into(), blocks)))
    }
}

fn multi_message_or_action<'a, 'b>(
    message_tag: Option<&'static str>,
    mut block: impl FnMut(Context, (Context, String), Option<(Context, String)>) -> ScanBlock + 'b,
    mut action_block: impl FnMut(Context, (Context, String), Option<(Context, String)>) -> ScanBlock
        + 'b,
) -> impl FnMut(Input<'a>) -> IResult<'a, ScanBlock> + 'b {
    move |input| {
        let (input, (ctx, blocks)) = consumed(multi_message_vec(
            message_tag,
            |ctx, (msg_name, msg_name_stripped, msg_type), body| match msg_type {
                MessageNameType::Name => block(
                    ctx,
                    (msg_name.into(), String::from(*msg_name_stripped)),
                    body,
                ),
                MessageNameType::Action => action_block(
                    ctx,
                    (msg_name.into(), String::from(*msg_name_stripped)),
                    body,
                ),
            },
            server_message_name,
        ))(input)?;
        Ok((input, wrap_block_vec(ctx.into(), blocks)))
    }
}

fn multi_message_vec<'a, 'b, 'c, N: 'a>(
    message_tag: Option<&'static str>,
    mut block: impl FnMut(Context, N, Option<(Context, String)>) -> ScanBlock + 'b,
    mut message_name_matcher: impl FnMut(Input<'a>) -> IResult<'a, N> + 'c,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<ScanBlock>> {
    move |input| {
        let (input, head) = many1(context(
            "explicit line",
            message(message_tag, &mut block, &mut message_name_matcher),
        ))(input)?;
        let (input, (tail, _)) = context(
            "implicit line",
            map(
                opt(many_till(
                    context(
                        "implicit line",
                        message(None, &mut block, &mut message_name_matcher),
                    ),
                    peek(preceded(multispace0, alt((void(eof), keyword)))),
                )),
                Option::unwrap_or_default,
            ),
        )(input)?;
        Ok((input, head.into_iter().chain(tail).collect()))
    }
}

fn message_with_simple_name<'a, 'b>(
    tag: Option<&'static str>,
    mut block: impl FnMut(Context, (Context, String), Option<(Context, String)>) -> ScanBlock + 'b,
) -> impl FnMut(Input<'a>) -> IResult<'a, ScanBlock> {
    message(
        tag,
        move |ctx, name, body| block(ctx, (name.into(), String::from(*name)), body),
        message_name,
    )
}

fn message<'a, 'b, 'n, N: 'a>(
    tag: Option<&'static str>,
    mut block: impl (FnMut(Context, N, Option<(Context, String)>) -> ScanBlock) + 'b,
    message_name_matcher: impl FnMut(Input<'a>) -> IResult<'a, N> + 'n,
) -> impl FnMut(Input<'a>) -> IResult<'a, ScanBlock> {
    map(
        terminated(
            preceded(
                multispace0,
                consumed(prefixed_line(tag, message_name_matcher)),
            ),
            peek(end_of_line),
        ),
        move |(ctx, (message, arg))| {
            block(
                ctx.into(),
                message,
                arg.map(|arg| (arg.into(), String::from(*arg))),
            )
        },
    )
}

fn prefixed_line<'a, 'n, N: 'a>(
    prefix: Option<&'static str>,
    message_name_matcher: impl FnMut(Input<'a>) -> IResult<'a, N> + 'n,
) -> impl FnMut(Input<'a>) -> IResult<'a, (N, Option<Input<'a>>)> {
    preceded(
        multispace0,
        preceded(
            cond(
                prefix.is_some(),
                terminated(tag(prefix.unwrap_or("")), space0),
            ),
            pair(
                context("message name", message_name_matcher),
                context(
                    "message body",
                    alt((
                        map(preceded(space1, rest_of_line), Some),
                        value(None, terminated(space0, peek(end_of_line))),
                    )),
                ),
            ),
        ),
    )
}

fn message_simple_content<'a>(
    tag: Option<&'static str>,
    mut block: impl FnMut(Context, (Context, String)) -> ScanBlock,
) -> impl FnMut(Input<'a>) -> IResult<'a, ScanBlock> {
    map(
        message_simple_content_matcher(tag),
        move |(ctx, content)| block(ctx.into(), (content.into(), String::from(*content))),
    )
}

fn message_simple_content_matcher<'a>(
    tag: Option<&'static str>,
) -> impl FnMut(Input<'a>) -> IResult<'a, (Input<'a>, Input<'a>)> {
    preceded(
        multispace0,
        terminated(
            consumed(prefixed_line_simple_content(tag)),
            peek(end_of_line),
        ),
    )
}

fn message_simple_content_with_block<'a>(
    tag: Option<&'static str>,
    mut block: impl FnMut(Context, (Context, String), ScanBlock) -> ScanBlock,
) -> impl FnMut(Input<'a>) -> IResult<'a, ScanBlock> {
    map(
        pair(message_simple_content_matcher(tag), scan_block),
        move |((ctx, content), subsequent)| {
            block(
                ctx.into(),
                (content.into(), String::from(*content)),
                subsequent,
            )
        },
    )
}

fn prefixed_line_simple_content<'a>(
    prefix: Option<&'static str>,
) -> impl FnMut(Input<'a>) -> IResult<'a, Input<'a>> {
    preceded(
        cond(
            prefix.is_some(),
            terminated(tag(prefix.unwrap_or_default()), space0),
        ),
        preceded(space0, rest_of_line),
    )
}

fn message_empty_content_with_block<'a>(
    tag: Option<&'static str>,
    mut block: impl FnMut(Context, ScanBlock) -> ScanBlock,
) -> impl FnMut(Input<'a>) -> IResult<'a, ScanBlock> {
    map(
        pair(
            preceded(
                multispace0,
                terminated(
                    recognize(prefixed_line_empty_content(tag)),
                    peek(end_of_line),
                ),
            ),
            scan_block,
        ),
        move |(ctx, subsequent_block)| block(ctx.into(), subsequent_block),
    )
}

fn prefixed_line_empty_content<'a>(
    prefix: Option<&'static str>,
) -> impl FnMut(Input<'a>) -> IResult<'a, ()> {
    preceded(
        cond(
            prefix.is_some(),
            terminated(tag(prefix.unwrap_or_default()), space0),
        ),
        void(space0),
    )
}

fn comment<'a, T>(
    mut block: impl FnMut(Context) -> T + 'static,
) -> impl FnMut(Input<'a>) -> IResult<'a, T> {
    map(
        preceded(
            multispace0,
            terminated(
                recognize(preceded(tag("#"), opt(rest_of_line))),
                peek(end_of_line),
            ),
        ),
        move |ctx| block(ctx.into()),
    )
}

// #################
// Logic/Flow Blocks
// #################
fn block<'a>(
    opening: &'static str,
    closing: &'static str,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<ScanBlock>> {
    preceded(
        multispace0,
        preceded(
            terminated(tag(opening), end_of_line),
            terminated(
                many1(scan_block),
                preceded(multispace0, terminated(tag(closing), end_of_line)),
            ),
        ),
    )
}

// TODO: add tests
fn multiblock<'a>(
    opening: &'static str,
    closing: &'static str,
    sep: &'static str,
) -> impl FnMut(Input<'a>) -> IResult<'a, Vec<ScanBlock>> {
    preceded(
        delimited(multispace0, tag(opening), end_of_line),
        terminated(
            separated_list2(
                delimited(multispace0, tag(sep), end_of_line),
                map(
                    preceded(multispace0, consumed(many1(scan_block))),
                    |(ctx, blocks)| wrap_block_vec(ctx.into(), blocks),
                ),
            ),
            delimited(multispace0, tag(closing), end_of_line),
        ),
    )
}

// ###############
// Syntactic Sugar
// ###############
// TODO: add tests
fn auto_repeat_0(input: Input) -> IResult<ScanBlock> {
    auto_syntactic_sugar(input, "*:", ScanBlock::Repeat0)
}

// TODO: add tests
fn auto_repeat_1(input: Input) -> IResult<ScanBlock> {
    auto_syntactic_sugar(input, "+:", ScanBlock::Repeat1)
}

// TODO: add tests
fn auto_optional(input: Input) -> IResult<ScanBlock> {
    auto_syntactic_sugar(input, "?:", ScanBlock::Optional)
}

fn auto_syntactic_sugar<'a>(
    input: Input<'a>,
    prefix: &'static str,
    mut wrapper_block: impl FnMut(Context, Box<ScanBlock>) -> ScanBlock,
) -> IResult<'a, ScanBlock> {
    let (input, (line_ctx, (message_name, args))) =
        consumed(prefixed_line(Some(prefix), message_name))(input)?;
    Ok((
        input,
        wrapper_block(
            line_ctx.into(),
            Box::new(ScanBlock::AutoMessage(
                line_ctx.into(),
                (message_name.into(), String::from(*message_name)),
                args.map(|s| (s.into(), String::from(*s))),
            )),
        ),
    ))
}

// #########
// Utilities
// #########
fn message_name<T, E: ParseError<T>>(input: T) -> nom::IResult<T, T, E>
where
    T: InputTakeAtPosition + Clone,
    <T as InputTakeAtPosition>::Item: AsChar + Clone,
{
    take_while1(|item: T::Item| item.clone().is_alphanum() || item.as_char() == '_')(input)
}

fn server_message_name<T, E: ParseError<T>>(input: T) -> nom::IResult<T, (T, T, MessageNameType), E>
where
    T: InputTakeAtPosition
        + Clone
        + Compare<&'static str>
        + InputTake
        + Offset
        + Slice<RangeTo<usize>>,
    <T as InputTakeAtPosition>::Item: AsChar + Clone,
{
    alt((
        map(
            take_while1(|item: T::Item| item.clone().is_alphanum() || item.as_char() == '_'),
            |o: T| (o.clone(), o, MessageNameType::Name),
        ),
        map(
            consumed(delimited(
                tag("<"),
                take_while1(|item: T::Item| item.as_char() != '>'),
                tag(">"),
            )),
            |(outer, inner)| (outer, inner, MessageNameType::Action),
        ),
    ))(input)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MessageNameType {
    Name,
    Action,
}

fn non_space<T, E: ParseError<T>>(input: T) -> nom::IResult<T, T, E>
where
    T: InputTakeAtPosition,
    <T as InputTakeAtPosition>::Item: AsChar + Clone,
{
    input.split_at_position1_complete(|item| item.as_char().is_whitespace(), ErrorKind::Alpha)
}

fn rest_of_line<'a, E: ParseError<Input<'a>>>(
    input: Input<'a>,
) -> nom::IResult<Input<'a>, Input<'a>, E> {
    let (input, line) = terminated(not_line_ending, peek(end_of_line))(input)?;
    if line.is_empty() {
        return Err(nom::Err::Error(E::from_error_kind(
            input,
            ErrorKind::Complete,
        )));
    }
    match line.rfind(|c: char| !c.is_whitespace()) {
        None => Ok((input, line)),
        Some(i) => {
            let rest = line.slice(i..);
            let (_, c) = rest
                .char_indices()
                .next()
                .expect("not at of string, because i points to non-whitespace char");
            Ok((input, line.slice(..i + c.len_utf8())))
        }
    }
}

fn end_of_line<'a, E: ParseError<Input<'a>>>(input: Input<'a>) -> nom::IResult<Input<'a>, (), E> {
    let eol = alt((line_ending, eof));
    void(preceded(space0, eol))(input)
}

fn void<F, I, O, E: ParseError<I>>(f: F) -> impl FnMut(I) -> nom::IResult<I, (), E>
where
    F: Parser<I, O, E>,
{
    value((), f)
}

pub fn separated_list2<I, O, O2, E, F, G>(
    mut sep: G,
    mut f: F,
) -> impl FnMut(I) -> nom::IResult<I, Vec<O>, E>
where
    I: Clone + InputLength,
    F: Parser<I, O, E>,
    G: Parser<I, O2, E>,
    E: ParseError<I>,
{
    move |mut i: I| {
        let mut res = Vec::new();

        // Parse the first element
        match f.parse(i.clone()) {
            Err(e) => return Err(e),
            Ok((i1, o)) => {
                res.push(o);
                i = i1;
            }
        }

        loop {
            let len = i.input_len();
            match sep.parse(i.clone()) {
                Err(nom::Err::Error(e)) => {
                    if res.len() == 1 {
                        return Err(nom::Err::Error(e));
                    }
                    return Ok((i, res));
                }
                Err(e) => return Err(e),
                Ok((i1, _)) => {
                    // infinite loop check: the parser must always consume
                    if i1.input_len() == len {
                        return Err(nom::Err::Error(E::from_error_kind(
                            i1,
                            ErrorKind::SeparatedList,
                        )));
                    }

                    match f.parse(i1.clone()) {
                        Err(nom::Err::Error(_)) => return Ok((i, res)),
                        Err(e) => return Err(e),
                        Ok((i2, o)) => {
                            res.push(o);
                            i = i2;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
mod tests {
    use indoc::indoc;
    use nom_span::Spanned;
    use rstest::rstest;

    use crate::bang_line::BangLine;
    use crate::context::Context;
    use crate::types::{Branch, ScanBlock};

    #[test]
    fn test_scan_minimal_script() {
        let input = "!: BOLT 5.5\n";
        let result = dbg!(super::scan_script(input, "test.script"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_failing_scan() {
        let input = "!: BOLT 5.4\n\nF: NOPE foo\n";
        let result = dbg!(super::scan_script(input, "test.script"));
        assert!(result.is_err());
        println!("{:}", result.unwrap_err());
    }

    fn wrap_input(input: &str) -> super::Input<'_> {
        Spanned::new(input, true)
    }

    fn new_ctx(
        start_line_number: usize,
        end_line_number: usize,
        start_byte: usize,
        end_byte: usize,
    ) -> Context {
        Context {
            start_line_number,
            end_line_number,
            start_byte,
            end_byte,
        }
    }

    // #################
    // Header/Bang Lines
    // #################
    fn str_bang_line(
        bang: impl FnOnce(Context, (Context, String)) -> BangLine,
        total_len: usize,
        s: &'static str,
    ) -> BangLine {
        str_bang_line_offset(bang, 1, 0, total_len, s)
    }

    fn str_bang_line_offset(
        bang: impl FnOnce(Context, (Context, String)) -> BangLine,
        line: usize,
        offset: usize,
        total_len: usize,
        s: &'static str,
    ) -> BangLine {
        let start = offset;
        let end = offset + total_len;
        bang(
            new_ctx(line, line, start, end),
            (new_ctx(line, line, end - s.len(), end), s.into()),
        )
    }

    fn bolt_bang_line(
        total_len: usize,
        version_offset: usize,
        version: &'static str,
        capabilities: Option<(usize, &'static str)>,
    ) -> BangLine {
        bolt_bang_line_offset(1, 0, total_len, version_offset, version, capabilities)
    }

    fn bolt_bang_line_offset(
        line: usize,
        offset: usize,
        total_len: usize,
        version_offset: usize,
        version: &'static str,
        capabilities: Option<(usize, &'static str)>,
    ) -> BangLine {
        let start = offset;
        let end = offset + total_len;
        let version_start = start + version_offset;
        let version_end = version_start + version.len();
        let capabilities = capabilities.map(|(offset, s)| {
            let cap_start = start + offset;
            let cap_end = cap_start + s.len();
            (new_ctx(line, line, cap_start, cap_end), s.into())
        });
        BangLine::Version(
            new_ctx(line, line, start, end),
            (
                new_ctx(line, line, version_start, version_end),
                version.into(),
            ),
            capabilities,
        )
    }

    #[rstest]
    #[case::bolt_5_4("!: BOLT 5.4\n", bolt_bang_line(11, 8, "5.4", None))]
    #[case::bolt_5_4_spaces("!:  BOLT   5.4  \n", bolt_bang_line(16, 11, "5.4", None))]
    #[case::bolt_foobar("!: BOLT foobar\n", bolt_bang_line(14, 8, "foobar", None))]
    #[case::bolt_capabilities(
        "!:  BOLT \t foobar  \t cap \t- abilities  \n",
        bolt_bang_line(39, 11, "foobar", Some((21, "cap \t- abilities")))
    )]
    #[case::hanshake_manifest(
        "!: HANDSHAKE_MANIFEST 5\n",
        str_bang_line(BangLine::HandshakeManifest, 23, "5")
    )]
    #[case::handshake(
        "!: HANDSHAKE 00 00 FF 05\n",
        str_bang_line(BangLine::Handshake, 24, "00 00 FF 05")
    )]
    #[case::handshake_response(
        "!: HANDSHAKE_RESPONSE 00 00 00 00\n",
        str_bang_line(BangLine::HandshakeResponse, 33, "00 00 00 00")
    )]
    #[case::handshake_delay(
        "!: HANDSHAKE_DELAY 1.2345678901234567890123456789\n",
        str_bang_line(BangLine::HandshakeDelay, 49, "1.2345678901234567890123456789")
    )]
    #[case::auto_reset("!: AUTO RESET\n", str_bang_line(BangLine::Auto, 13, "RESET"))]
    #[case::auto_foo("!: AUTO foo\n", str_bang_line(BangLine::Auto, 11, "foo"))]
    #[case::restart("!: ALLOW RESTART\n", BangLine::AllowRestart(new_ctx(1, 1, 0, 16)))]
    #[case::concurrent(
        "!: ALLOW CONCURRENT\n",
        BangLine::AllowConcurrent(new_ctx(1, 1, 0, 19))
    )]
    #[case::py("!: PY a = 1\n", str_bang_line(BangLine::Python, 11, "a = 1"))]
    fn test_scan_bang_line(
        #[case] input: &'static str,
        #[case] mut expected: BangLine,
        #[values(1, 3)] repetition: usize,
    ) {
        let input = input.repeat(repetition);
        let result = dbg!(super::scan_script(input.as_str(), "test.script"));
        let result = result.unwrap();
        let start_bytes = expected.ctx().start_byte;
        let end_bytes = expected.ctx().end_byte;
        let bytes_count = end_bytes - start_bytes;
        assert_eq!(result.bang_lines.len(), repetition);
        for (bl, line) in result.bang_lines.iter().zip(1..repetition + 1) {
            if line > 1 {
                expected.add_offset(1, bytes_count + 1);
            }
            assert_eq!(bl, &expected);
        }
    }

    #[test]
    fn test_scan_multiple_bangs() {
        let input = "!: BOLT 5.4\n!: AUTO Nonsense\n!: ALLOW RESTART\n";
        let result = dbg!(super::scan_script(input, "test.script"));
        let result = result.unwrap();
        assert_eq!(result.bang_lines.len(), 3);
        assert_eq!(
            result.bang_lines.first(),
            Some(&BangLine::Version(
                new_ctx(1, 1, 0, 11),
                (new_ctx(1, 1, 8, 11), "5.4".into()),
                None
            ))
        );
        assert_eq!(
            result.bang_lines.get(1),
            Some(&str_bang_line_offset(BangLine::Auto, 2, 12, 16, "Nonsense"))
        );
        assert_eq!(
            result.bang_lines.get(2),
            Some(&BangLine::AllowRestart(new_ctx(3, 3, 29, 45)))
        );
    }

    #[rstest]
    fn test_auto_bang_line(#[values("", "\n", "\t", " ", "\t\n\n\t  ")] ending: &'static str) {
        let base = "!: AUTO Nonsense";
        let input = format!("{base}{ending}");
        let bytes = base.len() + ending.find(char::is_whitespace).unwrap_or(ending.len());
        let (input, bl) = super::auto_bang_line(wrap_input(&input)).unwrap();
        assert_eq!(*input, ending);
        assert_eq!(bl, str_bang_line(BangLine::Auto, bytes, "Nonsense"));
    }

    #[rstest]
    fn test_auto_bang_line_script(
        #[values("", "\n", "\t", " ", "\t\n\n\t  ")] ending: &'static str,
    ) {
        let base = "!: AUTO Nonsense";
        let input = format!("{base}{ending}");
        let bytes = base.len() + ending.find(char::is_whitespace).unwrap_or(ending.len());
        let result = dbg!(super::scan_script(&input, "test.script"));
        let result = result.unwrap();
        assert_eq!(
            result.bang_lines.first(),
            Some(&str_bang_line(BangLine::Auto, bytes, "Nonsense"))
        );
    }

    #[rstest]
    fn test_scan_concurrent_ok(#[values("", "\n", "\t", " ", "\t\n\n\t  ")] ending: &'static str) {
        let base = "!: ALLOW CONCURRENT";
        let input = format!("{base}{ending}");
        let bytes = base.len() + ending.find(char::is_whitespace).unwrap_or(ending.len());
        let input = wrap_input(&input);
        let mut f = super::simple_bang_line("ALLOW CONCURRENT", BangLine::AllowConcurrent);
        let result = f(input);
        let (rem, bang) = result.unwrap();
        assert_eq!(*rem, ending);
        assert_eq!(bang, BangLine::AllowConcurrent(new_ctx(1, 1, 0, bytes)));
    }

    // ##########
    //    Body
    // ##########

    // ############
    // Simple Lines
    // ############
    #[rstest]
    #[case::implicit("C: Foo a b\n   Bar lel lol", 3, 7, 10, 14, 14, 18, 25)]
    #[case::explicit("C: Foo a b\nC: Bar lel lol", 3, 7, 10, 11, 14, 18, 25)]
    #[case::implicit_space_post("C: Foo a b \n   Bar lel lol ", 3, 7, 11, 15, 15, 19, 27)]
    #[case::explicit_space_post("C: Foo a b \nC: Bar lel lol ", 3, 7, 11, 12, 15, 19, 27)]
    #[case::implicit_space_pre("C:  Foo a b \n    Bar lel lol", 4, 8, 12, 17, 17, 21, 28)]
    #[case::explicit_space_pre("C:  Foo a b \nC:  Bar lel lol", 4, 8, 12, 13, 17, 21, 28)]
    fn test_multi_message(
        #[case] input: &'static str,
        #[case] b_msg_start_1: usize,
        #[case] b_body_start_1: usize,
        #[case] b_end_1: usize,
        #[case] b_start_2: usize,
        #[case] b_msg_start_2: usize,
        #[case] b_body_start_2: usize,
        #[case] b_end_2: usize,
        #[values("", "\n", "\nS: Baz\n", "\n \n\n  S: Baz\n", "\n?}")] ending: &'static str,
    ) {
        let input = format!("{input}{ending}");
        let (rem, block) = dbg!(super::multi_message(Some("C:"), ScanBlock::ClientMessage)(
            wrap_input(&input)
        ))
        .unwrap();
        let body_1 = "a b";
        let b_body_end_1 = b_body_start_1 + body_1.len();
        let body_2 = "lel lol";
        let b_body_end_2 = b_body_start_2 + body_2.len();
        assert_eq!(*rem, ending);
        assert_eq!(
            block,
            ScanBlock::List(
                new_ctx(1, 2, 0, b_end_2),
                vec![
                    ScanBlock::ClientMessage(
                        new_ctx(1, 1, 0, b_end_1),
                        (
                            new_ctx(1, 1, b_msg_start_1, b_msg_start_1 + 3),
                            "Foo".into()
                        ),
                        Some((new_ctx(1, 1, b_body_start_1, b_body_end_1), body_1.into()))
                    ),
                    ScanBlock::ClientMessage(
                        new_ctx(2, 2, b_start_2, b_end_2),
                        (
                            new_ctx(2, 2, b_msg_start_2, b_msg_start_2 + 3),
                            "Bar".into()
                        ),
                        Some((new_ctx(2, 2, b_body_start_2, b_body_end_2), body_2.into()))
                    )
                ]
            )
        );
    }

    #[test]
    fn test_foo() {
        let input = "C: Foo a b\nC: Bar lel lol";
        let ending = "\nS:Baz\n";
        let input = format!("{input}{ending}");
        let (rem, block) = dbg!(super::multi_message(Some("C:"), ScanBlock::ClientMessage)(
            wrap_input(&input)
        ))
        .unwrap();
        assert_eq!(*rem, ending);
        assert_eq!(
            block,
            ScanBlock::List(
                new_ctx(1, 2, 0, 25),
                vec![
                    ScanBlock::ClientMessage(
                        new_ctx(1, 1, 0, 10),
                        (new_ctx(1, 1, 3, 6), "Foo".into()),
                        Some((new_ctx(1, 1, 7, 10), "a b".into()))
                    ),
                    ScanBlock::ClientMessage(
                        new_ctx(2, 2, 11, 25),
                        (new_ctx(2, 2, 14, 17), "Bar".into()),
                        Some((new_ctx(2, 2, 18, 25), "lel lol".into()))
                    )
                ]
            )
        );
    }

    #[rstest]
    #[case::no_space("C:RUN", 2, 5)]
    #[case::clean("C: RUN", 3, 6)]
    #[case::trailing("C: RUN  ", 3, 8)]
    #[case::messy("C:   RUN  ", 5, 10)]
    fn test_client_message_with_no_args(
        #[case] input: &str,
        #[case] bytes_msg_start: usize,
        #[case] bytes: usize,
    ) {
        let result = super::message_with_simple_name(Some("C:"), ScanBlock::ClientMessage)(
            wrap_input(input),
        );
        let (rem, block) = result.unwrap();
        assert_eq!(*rem, "");
        assert_eq!(
            block,
            ScanBlock::ClientMessage(
                new_ctx(1, 1, 0, bytes),
                (
                    new_ctx(1, 1, bytes_msg_start, bytes_msg_start + 3),
                    "RUN".into()
                ),
                None
            )
        );
    }

    #[rstest]
    #[case::no_space("C:RUN foo bar", 2, 6, 13)]
    #[case::no_space("C: RUN foo bar", 3, 7, 14)]
    #[case::trailing("C: RUN foo bar  ", 3, 7, 16)]
    #[case::messy("C:  RUN   foo bar  ", 4, 10, 19)]
    fn test_client_message_con_args(
        #[case] input: &str,
        #[case] message_start: usize,
        #[case] body_start: usize,
        #[case] bytes: usize,
    ) {
        let result = super::message_with_simple_name(Some("C:"), ScanBlock::ClientMessage)(
            wrap_input(input),
        );
        let (rem, block) = result.unwrap();
        let body = "foo bar";
        let body_end = body_start + body.len();
        assert_eq!(*rem, "");
        assert_eq!(
            block,
            ScanBlock::ClientMessage(
                new_ctx(1, 1, 0, bytes),
                (
                    new_ctx(1, 1, message_start, message_start + 3),
                    "RUN".into()
                ),
                Some((new_ctx(1, 1, body_start, body_end), body.into()))
            )
        );
    }

    #[rstest]
    #[case::no_space("#C:RUN foo bar", 14)]
    #[case::no_space("#C: RUN foo bar", 15)]
    #[case::trailing("#C: RUN foo bar  ", 17)]
    #[case::messy("#C:  RUN   foo bar  ", 20)]
    #[case::messy("#", 1)]
    fn test_comment(#[case] input: &str, #[case] bytes: usize) {
        let result = super::comment(ScanBlock::Comment)(wrap_input(input));
        let (rem, block) = result.unwrap();
        assert_eq!(*rem, "");
        assert_eq!(block, ScanBlock::Comment(new_ctx(1, 1, 0, bytes)));
    }

    #[rstest]
    fn test_python_line() {
        let input = wrap_input("PY: print('Hello, World!')");
        let result = dbg!(super::message_simple_content(
            Some("PY:"),
            ScanBlock::Python
        )(input));
        let (rem, block) = result.unwrap();
        assert_eq!(*rem, "");
        assert_eq!(
            block,
            ScanBlock::Python(
                new_ctx(1, 1, 0, 26),
                (new_ctx(1, 1, 4, 26), "print('Hello, World!')".into())
            )
        );
    }

    #[rstest]
    #[case::cond_if("IF: foo == 10", Branch::If, Some("foo == 10"))]
    #[case::cond_else_if("ELIF: baz == 20", Branch::ElseIf, Some("bay == 20"))]
    #[case::cond_else("ELSE:", Branch::Else, None)]
    fn test_conditions(
        #[case] input: &str,
        #[case] expected_branch: Branch,
        #[case] expected_condition: Option<&str>,
    ) {
        let result = super::scan_block(wrap_input(input));
        let (rem, block) = result.unwrap();
        let ScanBlock::ConditionPart(_, branch, condition, _) = block else {
            panic!("Expected ConditionPart, found {block:?}");
        };
        assert_eq!(branch, expected_branch);
        assert_eq!(
            condition.as_ref().map(|(_, cond)| cond.as_str()),
            expected_condition
        );
        assert_eq!(*rem, "");
    }

    // #################
    // Logic/Flow Blocks
    // #################
    #[test]
    fn test_simple_block() {
        let input = wrap_input("{{\n    C: RUN\n    S: OK\n}}");
        let result = dbg!(super::block("{{", "}}")(input));
        let (rem, blocks) = result.unwrap();
        assert_eq!(*rem, "");
        assert_eq!(
            blocks,
            vec![
                ScanBlock::ClientMessage(
                    new_ctx(2, 2, 7, 13),
                    (new_ctx(2, 2, 10, 13), "RUN".into()),
                    None
                ),
                ScanBlock::ServerMessage(
                    new_ctx(3, 3, 18, 23),
                    (new_ctx(3, 3, 21, 23), "OK".into()),
                    None
                ),
            ]
        );
    }

    #[test]
    fn test_multi_block() {
        let input = wrap_input("{{\n    C: RUN1\n    S: OK\n----\n    C: RUN2\n}}");
        let result = dbg!(super::multiblock("{{", "}}", "----")(input));
        let (rem, blocks) = result.unwrap();
        assert_eq!(*rem, "");
        assert_eq!(
            blocks,
            vec![
                ScanBlock::List(
                    new_ctx(2, 3, 7, 24),
                    vec![
                        ScanBlock::ClientMessage(
                            new_ctx(2, 2, 7, 14),
                            (new_ctx(2, 2, 10, 14), "RUN1".into()),
                            None
                        ),
                        ScanBlock::ServerMessage(
                            new_ctx(3, 3, 19, 24),
                            (new_ctx(3, 3, 22, 24), "OK".into()),
                            None
                        ),
                    ]
                ),
                ScanBlock::ClientMessage(
                    new_ctx(5, 5, 34, 41),
                    (new_ctx(5, 5, 37, 41), "RUN2".into()),
                    None
                ),
            ]
        );
    }

    // ###############
    // Syntactic Sugar
    // ###############

    // #########
    // Utilities
    // #########
    fn call_message_name(input: &str) -> super::IResult<super::Input> {
        super::message_name(wrap_input(input))
    }

    #[rstest]
    #[case::trailing_space("FOO ", "FOO", " ")]
    #[case::trailing_tab("FOO\t", "FOO", "\t")]
    #[case::trailing_lf("FOO\n", "FOO", "\n")]
    #[case::trailing_crlf("FOO\r\n", "FOO", "\r\n")]
    #[case::simple("FOO", "FOO", "")]
    #[case::alphanum("F2OO3", "F2OO3", "")]
    #[case::alphanum_underscore("F2O_O3", "F2O_O3", "")]
    #[case::dash("ABC-DEF", "ABC", "-DEF")]
    #[case::slash("ABC/DEF", "ABC", "/DEF")]
    fn test_message_name(
        #[case] input: &str,
        #[case] expected_taken: &str,
        #[case] expected_rem: &str,
    ) {
        let result = dbg!(call_message_name(input));
        let (rem, taken) = result.unwrap();
        assert_eq!(*rem, expected_rem);
        assert_eq!(*taken, expected_taken);
    }

    #[rstest]
    #[case::empty("")]
    #[case::leading_space(" FOO")]
    #[case::leading_tab("\tFOO")]
    #[case::leading_lf("\nFOO")]
    #[case::leading_crlf("\r\nFOO")]
    fn test_not_message_name(#[case] input: &str) {
        let result = dbg!(call_message_name(input));
        result.unwrap_err();
    }

    fn call_end_of_line(input: &str) -> super::IResult<()> {
        super::end_of_line(wrap_input(input))
    }

    #[rstest]
    #[case::crlf("\r\n")]
    #[case::lf("\n")]
    fn test_eol(#[case] eol: &str) {
        let input = format!("\t {eol}jeff");
        let (rem, ()) = call_end_of_line(&input).unwrap();
        assert_eq!(*rem, "jeff");
    }

    fn call_rest_of_line(input: &str) -> super::IResult<super::Input> {
        super::rest_of_line(wrap_input(input))
    }

    #[rstest]
    #[case::crlf("\r\n")]
    #[case::lf("\n")]
    fn test_rest_of_line(
        #[case] eol: &str,
        #[values("ALLOW CONCURRENT", "A C ", " A C", "\tA\tC\t")] content: &str,
    ) {
        let input = format!("{content}{eol}");
        let result = call_rest_of_line(input.as_str());
        let (rem, taken) = result.unwrap();
        assert_eq!(*taken, content.trim_end());
        assert_eq!(*rem, eol);
    }

    #[test]
    fn test_full_script() {
        let input = indoc! {r#"
            # Comment 1
            !: BOLT 5.6
            # Comment 2

            # COMMENT 3
            C: HELLO {"{}": "*"}
            # COMMENT 4
            S: SUCCESS {"server": "Neo4j/5.23.0, "connection_id": "bolt-123456789"}
            A: LOGON {"{}": "*"}
            *: RESET
            {?
                ?: TELEMETRY {"{}": "*"}
                C: RUN {"U": "*"} {"{}": "*"} {"{}": "*"}
                S: SUCCESS {"fields": ["n.name"]}
                {{
                    C: PULL {"n": {"Z": "*"}}
                ----
                    C: DISCARD {"n": {"Z": "*"}}
                }}
                S: SUCCESS {"type": "w"}
            ?}

            *: RESET
            ?: GOODBYE"#
        };

        dbg!(super::scan_script(input, "test.script")).unwrap();
    }

    #[test]
    fn test_server_message_name_normal_name() {
        let (rem, taken) = super::server_message_name::<_, super::PError<_>>("AB_C").unwrap();
        assert_eq!(taken.0, "AB_C");
        assert_eq!(taken.1, "AB_C");
        assert_eq!(taken.2, super::MessageNameType::Name);
        assert_eq!(rem, "");
    }

    #[test]
    fn test_server_message_name_action() {
        let (rem, taken) = super::server_message_name::<_, super::PError<_>>("<A1 O?>").unwrap();
        assert_eq!(taken.0, "<A1 O?>");
        assert_eq!(taken.1, "A1 O?");
        assert_eq!(taken.2, super::MessageNameType::Action);
        assert_eq!(rem, "");
    }
}
