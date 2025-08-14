use std::fmt::Display;

use crate::net_actor::NetActor;

pub(super) trait HasLoggingCtx {
    fn logging_ctx(&self) -> LoggingCtx;
}

impl<T> HasLoggingCtx for NetActor<'_, T> {
    fn logging_ctx(&self) -> LoggingCtx {
        LoggingCtx {
            ports: (self.peer_addr.port(), self.local_addr.port()),
        }
    }
}

impl HasLoggingCtx for LoggingCtx {
    fn logging_ctx(&self) -> LoggingCtx {
        *self
    }
}

#[derive(Debug, Copy, Clone)]
pub(super) struct LoggingCtx {
    ports: (u16, u16),
}

impl LoggingCtx {
    pub(super) fn fmt_ports(&self) -> impl Display {
        struct PortsDisplay {
            peer_port: u16,
            local_port: u16,
        }

        impl Display for PortsDisplay {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "#{:04X}>#{:04X}", self.peer_port, self.local_port)
            }
        }

        PortsDisplay {
            peer_port: self.ports.0,
            local_port: self.ports.1,
        }
    }
}

macro_rules! log {
    ($lvl:expr, $actor:ident, $($arg:tt)+) => {{
        #[allow(unused_imports)]
        use crate::net_actor::logging::HasLoggingCtx;
        let ctx = $actor.logging_ctx();
        log::log!(
            $lvl,
            "[{log_ports}]  {log_msg}",
            log_ports = ctx.fmt_ports(),
            log_msg = format_args!($($arg)+),
        );
    }};
}
pub(super) use log;

macro_rules! trace {
    ($actor:ident, $($arg:tt)+) => {
        crate::net_actor::logging::log!(log::Level::Trace, $actor, $($arg)+)
    };
}
pub(super) use trace;

macro_rules! debug {
    ($actor:ident, $($arg:tt)+) => {
        crate::net_actor::logging::log!(log::Level::Debug, $actor, $($arg)+)
    };
}
pub(super) use debug;

macro_rules! info {
    ($actor:ident, $($arg:tt)+) => {
        crate::net_actor::logging::log!(log::Level::Info, $actor, $($arg)+)
    };
}
pub(super) use info;

macro_rules! error {
    ($actor:ident, $($arg:tt)+) => {
        crate::net_actor::logging::log!(log::Level::Error, $actor, $($arg)+)
    };
}
pub(super) use error;
