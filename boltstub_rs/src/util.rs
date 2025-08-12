/// Like `?`, but for Option<Result<T>>.
macro_rules! opt_res_ret {
    ($e:expr) => {
        match $e {
            Some(Ok(v)) => v,
            Some(Err(e)) => return Some(Err(e.into())),
            None => return None,
        }
    };
}

pub(crate) use opt_res_ret;
