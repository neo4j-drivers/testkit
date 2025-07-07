use std::io;
use std::io::Write;

use fern::Dispatch;
use log::{Level, LevelFilter, Log, Metadata, Record};
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, StandardStreamLock, WriteColor};

pub(super) fn init_logging(min_level: LevelFilter) {
    let force_verbose = min_level > LevelFilter::Info;
    let color_log: Box<dyn Log> = Box::new(ColoredLogger::new(force_verbose));

    Dispatch::new()
        .chain(Dispatch::new().level(min_level).chain(color_log))
        .apply()
        .expect("Failed to initialize the logger");
}

struct ColoredLogger {
    stream: StandardStream,
    force_verbose: bool,
    color_rich_wrap: ColorSpec,
    color_time: ColorSpec,
    color_module: ColorSpec,
    color_error: ColorSpec,
    color_warn: ColorSpec,
    color_info: ColorSpec,
    color_debug: ColorSpec,
    color_trace: ColorSpec,
}

impl ColoredLogger {
    fn new(force_verbose: bool) -> Self {
        fn new_color_spec(color: Color) -> ColorSpec {
            let mut color_spec = ColorSpec::new();
            color_spec.set_fg(Some(color));
            color_spec
        }

        Self {
            stream: StandardStream::stdout(ColorChoice::Auto),
            force_verbose,
            color_rich_wrap: new_color_spec(Color::White),
            color_time: new_color_spec(Color::Black),
            color_module: new_color_spec(Color::White),
            color_error: new_color_spec(Color::Red),
            color_warn: new_color_spec(Color::Yellow),
            color_info: new_color_spec(Color::Green),
            color_debug: new_color_spec(Color::Cyan),
            color_trace: new_color_spec(Color::Magenta),
        }
    }

    fn lock(&self) -> ColoredLoggerGuard<'_> {
        ColoredLoggerGuard {
            stream: self.stream.lock(),
            color_rich_wrap: &self.color_rich_wrap,
            color_time: &self.color_time,
            color_module: &self.color_module,
            color_error: &self.color_error,
            color_warn: &self.color_warn,
            color_info: &self.color_info,
            color_debug: &self.color_debug,
            color_trace: &self.color_trace,
        }
    }
}

struct ColoredLoggerGuard<'a> {
    stream: StandardStreamLock<'a>,
    color_rich_wrap: &'a ColorSpec,
    color_time: &'a ColorSpec,
    color_module: &'a ColorSpec,
    color_error: &'a ColorSpec,
    color_warn: &'a ColorSpec,
    color_info: &'a ColorSpec,
    color_debug: &'a ColorSpec,
    color_trace: &'a ColorSpec,
}

impl ColoredLoggerGuard<'_> {
    fn write_rich_log_start(&mut self) -> io::Result<()> {
        // self.stream.set_color(self.color_rich_wrap)?;
        // self.stream.write_all("[".as_bytes())?;
        self.stream.reset()?;
        Ok(())
    }

    fn write_rich_log_end(&mut self) -> io::Result<()> {
        self.stream.set_color(self.color_rich_wrap)?;
        self.stream.write_all(": ".as_bytes())?;
        self.stream.reset()?;
        Ok(())
    }

    fn write_str(&mut self, s: &str) -> io::Result<()> {
        self.stream.write_all(s.as_bytes())
    }

    fn write_time(&mut self) -> io::Result<()> {
        self.stream.set_color(self.color_time)?;
        write!(
            self.stream,
            "{}",
            chrono::Local::now().format("%H:%M:%S%.3f")
        )?;
        self.stream.reset()
    }

    fn write_level(&mut self, level: Level) -> io::Result<()> {
        match level {
            Level::Error => self.stream.set_color(self.color_error)?,
            Level::Warn => self.stream.set_color(self.color_warn)?,
            Level::Info => self.stream.set_color(self.color_info)?,
            Level::Debug => self.stream.set_color(self.color_debug)?,
            Level::Trace => self.stream.set_color(self.color_trace)?,
        }
        write!(self.stream, "{level:5}")?;
        self.stream.reset()
    }

    fn write_module(&mut self, module: Option<&str>) -> io::Result<()> {
        let Some(module) = module else { return Ok(()) };
        self.stream.set_color(self.color_module)?;
        self.stream.write_all(module.as_bytes())?;
        self.stream.reset()
    }
}

impl Log for ColoredLogger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        fallback_on_error(record, |record| match record.level() {
            Level::Info if !self.force_verbose => {
                let mut lock = self.lock();
                lock.write_time()?;
                writeln!(lock.stream, "  {message}", message = record.args())
            }
            level => {
                let mut lock = self.lock();
                lock.write_rich_log_start()?;
                lock.write_time()?;
                lock.write_str(" ")?;
                lock.write_module(record.module_path())?;
                lock.write_str(" ")?;
                lock.write_level(level)?;
                lock.write_rich_log_end()?;
                lock.write_str(" ")?;
                writeln!(lock.stream, "{}", record.args())
            }
        });
    }

    fn flush(&self) {
        let _ = self.stream.lock().flush();
    }
}

// Copied from fern 0.7.1
// License: MIT
// Copyright (c) 2014-2017 David Ross
#[inline(always)]
fn fallback_on_error<F>(record: &Record, log_func: F)
where
    F: FnOnce(&Record) -> Result<(), io::Error>,
{
    if let Err(error) = log_func(record) {
        backup_logging(record, &error)
    }
}

fn backup_logging(record: &Record, error: &io::Error) {
    let second = write!(
        io::stderr(),
        "Error performing logging.\
         \n\tattempted to log: {}\
         \n\trecord: {:?}\
         \n\tlogging error: {}",
        record.args(),
        record,
        error
    );

    if let Err(second_error) = second {
        panic!(
            "Error performing stderr logging after error occurred during regular logging.\
             \n\tattempted to log: {}\
             \n\trecord: {:?}\
             \n\tfirst logging error: {}\
             \n\tstderr error: {}",
            record.args(),
            record,
            error,
            second_error,
        );
    }
}
// end of copied code
