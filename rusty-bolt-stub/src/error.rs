use crate::context::Context;

const CONTEXT_LINES: usize = 3;

pub(crate) fn script_excerpt(script_name: &str, script: &str, ctx: Context) -> String {
    let start_line = ctx.start_line_number;
    let end_line = ctx.end_line_number;
    let line_offset = ctx.start_byte - get_line_offset(script, start_line);
    let mut column = None;

    let line_num_max = script.lines().count();
    let line_num_width = line_num_max.to_string().len();
    let lines = script.lines().enumerate();
    let mut excerpt_lines =
        Vec::<String>::with_capacity(end_line - start_line + 4 + 2 + CONTEXT_LINES);
    if start_line.saturating_sub(CONTEXT_LINES) > 1 {
        excerpt_lines.push(format!("  {: >width$} ...", "", width = line_num_width));
    }
    for (mut line_num, line) in lines {
        line_num += 1;
        if line_num < start_line.saturating_sub(CONTEXT_LINES) {
            // pre context
            continue;
        }
        if line_num < start_line {
            // context before the excerpt
            excerpt_lines.push(format!(
                "  {line_num: >width$} {line}",
                width = line_num_width
            ));
            continue;
        }
        if line_num <= end_line {
            // the excerpt
            excerpt_lines.push(format!(
                "> {line_num: >width$} {line}",
                width = line_num_width
            ));
        }
        if line_num == start_line {
            let col = line[..line_offset].chars().count();
            column = Some(col);
            excerpt_lines.push(
                std::iter::repeat_n(" ", col + 3 + line_num_width)
                    .chain(["^"].into_iter())
                    .collect::<String>(),
            );
        }
        if line_num <= end_line {
            continue;
        }
        // context after the excerpt
        excerpt_lines.push(format!(
            "  {line_num: >width$} {line}",
            width = line_num_width
        ));
        if line_num >= end_line.saturating_add(CONTEXT_LINES) {
            // past context
            break;
        }
    }
    if end_line.saturating_add(CONTEXT_LINES) < line_num_max {
        excerpt_lines.push(format!("  {: >width$} ...", "", width = line_num_width));
    }
    let name_line = match column {
        None => {
            format!("{script_name}:{start_line}")
        }
        Some(column) => {
            format!("{script_name}:{start_line}:{column}")
        }
    };
    format!("{name_line}\n{}", excerpt_lines.join("\n"))
}

fn get_line_offset(s: &str, line_nr: usize) -> usize {
    let mut offset = 0;
    for (i, line) in s.lines().enumerate() {
        if i + 1 == line_nr {
            return offset;
        }
        offset += line.len() + 1;
    }
    offset
}
