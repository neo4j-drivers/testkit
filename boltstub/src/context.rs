use std::cmp::{max, min};
use std::fmt::Display;

use crate::types::actor_types::ScriptLine;

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct Context {
    pub(crate) start_line_number: usize,
    pub(crate) end_line_number: usize,
    pub(crate) start_byte: usize,
    pub(crate) end_byte: usize,
}

impl Display for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (start, end) = (self.start_line_number, self.end_line_number);
        if start == end {
            return write!(f, "line {start}");
        }
        write!(f, "lines {start}-{end}")
    }
}

impl ScriptLine for Context {
    fn line_repr<'a: 'c, 'b: 'c, 'c>(&'b self, script: &'a str) -> Option<&'c str> {
        Some(self.original_line(script))
    }

    fn line_number(&self) -> Option<usize> {
        Some(self.start_line_number)
    }
}

impl Context {
    pub fn fuse(&self, other: &Context) -> Context {
        Context {
            start_line_number: min(self.start_line_number, other.start_line_number),
            end_line_number: max(self.end_line_number, other.end_line_number),
            start_byte: min(self.start_byte, other.start_byte),
            end_byte: max(self.end_byte, other.end_byte),
        }
    }

    pub fn original_line<'a>(&self, script: &'a str) -> &'a str {
        // let mut to_start = self.start_line_number.saturating_sub(1);
        // let mut to_end = self.end_line_number.saturating_sub(1);
        //
        // let mut current_offset = 0;
        // let mut start = 0;
        // let mut end = script.len();
        // while let Some(i) = script.find("\n") {
        //     if to_start == 0 {
        //         start = current_offset;
        //     }
        //     if to_end == 0 {
        //         end = current_offset + i;
        //         break;
        //     }
        //     current_offset += i + "\n".len();
        //     to_start = to_start.wrapping_sub(1);
        //     to_end = to_end.wrapping_sub(1);
        // }
        &script[self.start_byte..self.end_byte]
    }
}
