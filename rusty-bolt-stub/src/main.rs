mod bang_line;
mod bolt_version;
mod context;
mod error;
mod ext;
mod jolt;
mod logging;
mod net;
mod net_actor;
mod parse_error;
mod parser;
mod python;
mod scanner;
mod str_bytes;
mod types;
mod util;
mod values;

use std::fmt::Display;
use std::io::Write;
use std::process::{ExitCode, Termination};
use std::sync::OnceLock;
use std::{fmt, io};

use anyhow::{anyhow, Context};
use clap::Parser;
use log::{debug, LevelFilter};

use crate::logging::init_logging;
use crate::parser::ActorScript;
use crate::python::run_python;

const TIMEOUT_HELP: &str = "The number of seconds for which the stub server will run \
before automatically terminating. If unspecified, the server will wait for 30 seconds.";
const LISTEN_ADDR_HELP: &str = "The base address on which to listen for incoming \
connections in INTERFACE:PORT format, where INTERFACE may be omitted for 'localhost'. Each script \
(which doesn't specify an explicit port number) will use subsequent ports. If completely omitted, \
this defaults to ':17687'.";
const VERBOSE_HELP: &str = "Show more detail about the client-server exchange. \
Supply the flag up to 3 times to increase verbosity with each.";

#[derive(Parser)]
struct StubArgs {
    #[arg(short, long, default_value_t=30.0, help=TIMEOUT_HELP)]
    timeout: f32,
    #[arg(long="listen-addr", short, default_value=":17687", help=LISTEN_ADDR_HELP)]
    listen_addr: String,
    #[arg(short, long, action=clap::ArgAction::Count, help=VERBOSE_HELP)]
    verbose: u8,
    script: String,
}

static SCRIPT: OnceLock<String> = OnceLock::new();
static SCRIPT_NAME: OnceLock<String> = OnceLock::new();
static PARSED: OnceLock<ActorScript> = OnceLock::new();

fn main() -> MainResult {
    MainResult(main_raw_error())
}

fn main_raw_error() -> Result<(), MainError> {
    let args = StubArgs::parse();
    let log_level = match args.verbose {
        0 => LevelFilter::Off,
        1 => LevelFilter::Info,
        2 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };
    init_logging(log_level);
    if args.verbose > 3 {
        log::warn!("Verbose level capped at 3");
    }

    SCRIPT_NAME.get_or_init(move || args.script);
    let script_name = SCRIPT_NAME.get().unwrap();
    let script_text = with_exit_code(99, || {
        std::fs::read_to_string(script_name)
            .with_context(|| format!("Failed to read script file: {}", script_name))
    })?;
    debug!(
        "Read script file: {}\n\
        ================================================================\n\
        {script_text}\n\
        ================================================================",
        script_name
    );
    SCRIPT.get_or_init(move || script_text);
    let script = SCRIPT.get().unwrap();

    let output = with_exit_code(99, || {
        scanner::contextualize_res(
            scanner::scan_script(script, script_name),
            script_name,
            script,
        )
    })?;
    let engine = with_exit_code(99, || {
        parser::contextualize_res(parser::parse(output), script_name, script)
    })?;

    PARSED.get_or_init(move || engine);

    for (ctx, py_line) in PARSED.get().unwrap().config.py_lines.iter() {
        debug!("Running Python bang ({ctx}): {py_line}");
        python::contextualize_res(run_python(py_line), *ctx, script_name, script)?;
    }

    let mut server = net::Server::new(&args.listen_addr, PARSED.get().unwrap());
    with_exit_code(99, || server.start())?;
    if !server.ever_acted() {
        return Err(MainError {
            err: anyhow!("Script never started."),
            code: 3.into(),
        });
    }
    Ok(())
}

#[derive(Debug)]
struct MainResult(Result<(), MainError>);

#[derive(Debug)]
struct MainError {
    err: anyhow::Error,
    code: ExitCode,
}

impl Display for MainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.err.fmt(f)
    }
}

impl Termination for MainResult {
    fn report(self) -> ExitCode {
        match self.0 {
            Ok(_) => ExitCode::SUCCESS,
            Err(MainError { err, code }) => {
                let _ = io::stderr().write_fmt(format_args!("Error: {err:?}\n"));
                code
            }
        }
    }
}

impl<E: Into<anyhow::Error>> From<E> for MainError {
    fn from(err: E) -> Self {
        MainError {
            err: err.into(),
            code: ExitCode::FAILURE,
        }
    }
}

fn with_exit_code<T, E: Into<anyhow::Error>>(
    code: u8,
    f: impl FnOnce() -> Result<T, E>,
) -> Result<T, MainError> {
    match f() {
        Ok(res) => Ok(res),
        Err(err) => {
            let code = code.into();
            let err = err.into();
            Err(MainError { err, code })
        }
    }
}
