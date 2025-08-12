use std::error::Error;

use crate::context::Context;

#[derive(Debug)]
pub(crate) struct ParseError {
    pub(crate) message: String,
    pub(crate) ctx: Option<Context>,
}

impl ParseError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ctx: None,
        }
    }

    pub(crate) fn new_ctx(ctx: Context, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            ctx: Some(ctx),
        }
    }

    pub(crate) fn add_ctx_offset(&mut self, line: usize, offset: usize) {
        if let Some(ctx) = &mut self.ctx {
            ctx.start_line_number += line;
            ctx.end_line_number += line;
            ctx.start_byte += offset;
            ctx.end_byte += offset;
        }
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(ctx) = &self.ctx {
            write!(f, "{}: {}", ctx, self.message)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl Error for ParseError {}

impl From<serde_json::Error> for ParseError {
    fn from(err: serde_json::Error) -> Self {
        let line = err.line();
        match line {
            0 => Self::new(err.to_string()),
            _ => {
                let col = err.column();
                let ctx = Context {
                    start_line_number: line - 1,
                    end_line_number: line - 1,
                    start_byte: col.saturating_sub(1),
                    end_byte: col.saturating_sub(1),
                };
                Self::new_ctx(ctx, err.to_string())
            }
        }
    }
}

impl From<String> for ParseError {
    fn from(err: String) -> Self {
        Self::new(err)
    }
}
