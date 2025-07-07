use crate::context::Context;

#[derive(Debug, PartialEq, Clone)]
pub enum BangLine {
    Version(Context, (Context, String), Option<(Context, String)>),
    HandshakeManifest(Context, (Context, String)),
    Handshake(Context, (Context, String)),
    HandshakeResponse(Context, (Context, String)),
    HandshakeDelay(Context, (Context, String)),
    Auto(Context, (Context, String)),
    AllowRestart(Context),
    AllowConcurrent(Context),
    Python(Context, (Context, String)),
    Comment(Context),
}

#[cfg(test)]
impl BangLine {
    pub fn ctx(&self) -> &Context {
        match self {
            Self::Version(ctx, ..) => ctx,
            Self::HandshakeManifest(ctx, ..) => ctx,
            Self::Handshake(ctx, ..) => ctx,
            Self::HandshakeResponse(ctx, ..) => ctx,
            Self::HandshakeDelay(ctx, ..) => ctx,
            Self::Auto(ctx, ..) => ctx,
            Self::AllowRestart(ctx, ..) => ctx,
            Self::AllowConcurrent(ctx, ..) => ctx,
            Self::Python(ctx, ..) => ctx,
            Self::Comment(ctx, ..) => ctx,
        }
    }

    pub fn add_offset(&mut self, lines: usize, bytes: usize) {
        fn add_offset_ctx<const N: usize>(contexts: [&mut Context; N], lines: usize, bytes: usize) {
            for context in contexts {
                context.start_line_number += lines;
                context.end_line_number += lines;
                context.start_byte += bytes;
                context.end_byte += bytes;
            }
        }

        match self {
            BangLine::Version(ctx, (ctx_arg1, _), Some((ctx_arg2, _))) => {
                add_offset_ctx([ctx, ctx_arg1, ctx_arg2], lines, bytes)
            }
            BangLine::Version(ctx, (ctx_arg1, _), None) => {
                add_offset_ctx([ctx, ctx_arg1], lines, bytes)
            }
            BangLine::HandshakeManifest(ctx, (ctx_arg, _)) => {
                add_offset_ctx([ctx, ctx_arg], lines, bytes)
            }
            BangLine::Handshake(ctx, (ctx_arg, _)) => add_offset_ctx([ctx, ctx_arg], lines, bytes),
            BangLine::HandshakeResponse(ctx, (ctx_arg, _)) => {
                add_offset_ctx([ctx, ctx_arg], lines, bytes)
            }
            BangLine::HandshakeDelay(ctx, (ctx_arg, _)) => {
                add_offset_ctx([ctx, ctx_arg], lines, bytes)
            }
            BangLine::Auto(ctx, (ctx_arg, _)) => add_offset_ctx([ctx, ctx_arg], lines, bytes),
            BangLine::AllowRestart(ctx) => add_offset_ctx([ctx], lines, bytes),
            BangLine::AllowConcurrent(ctx) => add_offset_ctx([ctx], lines, bytes),
            BangLine::Python(ctx, (ctx_arg, _)) => add_offset_ctx([ctx, ctx_arg], lines, bytes),
            BangLine::Comment(ctx) => add_offset_ctx([ctx], lines, bytes),
        }
    }
}
