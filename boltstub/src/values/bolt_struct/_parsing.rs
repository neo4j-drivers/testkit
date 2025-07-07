mod json;
mod pack_stream;

pub(super) use json::{check_last_json_field, next_json_field};
pub(super) use pack_stream::{check_last_pack_stream_field, next_pack_stream_field};
