use crate::bang_line::BangLine;
use crate::context::Context;
use std::borrow::Cow;
use std::fmt::Debug;

#[derive(Debug)]
pub struct Script<'a> {
    pub(crate) name: &'a str,
    pub(crate) bang_lines: Vec<BangLine>,
    pub(crate) body: (Context, Vec<ScanBlock>),
    pub(crate) input: &'a str,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ScanBlock {
    List(Context, Vec<ScanBlock>),
    Alt(Context, Vec<ScanBlock>),
    Parallel(Context, Vec<ScanBlock>),
    Optional(Context, Box<ScanBlock>),
    Repeat0(Context, Box<ScanBlock>),
    Repeat1(Context, Box<ScanBlock>),
    ClientMessage(Context, (Context, String), Option<(Context, String)>),
    ServerMessage(Context, (Context, String), Option<(Context, String)>),
    ServerAction(Context, (Context, String), Option<(Context, String)>),
    AutoMessage(Context, (Context, String), Option<(Context, String)>),
    Comment(Context),
    Python(Context, (Context, String)),
    ConditionPart(Context, Branch, Option<(Context, String)>, Box<ScanBlock>),
}

#[derive(Debug, Eq, PartialEq)]
pub enum Branch {
    If,
    ElseIf,
    Else,
}

#[derive(Debug, Eq, PartialEq)]
pub enum BoolIsh {
    True,
    False,
    Maybe,
}

pub enum Resolvable<T> {
    Static(T),
    Dynamic {
        func: Box<dyn Fn() -> T + Send + Sync>,
        repr: String,
    },
}

impl<T: ToOwned<Owned = T>> Resolvable<T> {
    #[allow(dead_code, reason = "Most useful function")]
    pub fn resolve(&self) -> Cow<T> {
        match self {
            Self::Static(t) => Cow::Borrowed(t),
            Self::Dynamic { func, .. } => Cow::Owned(func()),
        }
    }
}

impl<T: Debug> Debug for Resolvable<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static(t) => write!(f, "{t:?}"),
            Self::Dynamic { repr, .. } => write!(f, "{repr}"),
        }
    }
}

pub mod actor_types {
    use std::borrow::Cow;
    use std::fmt::Debug;
    use std::time::Duration;

    use crate::context::Context;
    use crate::values::bolt_message::BoltMessage;

    pub trait ScriptLine: Debug + Send + Sync {
        fn line_repr<'a: 'c, 'b: 'c, 'c>(&'b self, script: &'a str) -> Option<&'c str>;
        fn line_number(&self) -> Option<usize>;
    }

    pub trait ClientMessageValidator: ScriptLine {
        fn validate(&self, message: &BoltMessage) -> anyhow::Result<()>;
    }

    pub trait ServerMessageSender: ScriptLine + Send {
        fn send(&self) -> anyhow::Result<Cow<[u8]>>;
    }

    pub trait ServerActionLine: ScriptLine + Send {
        fn get_action(&self) -> &ServerAction;
    }

    #[derive(Debug, Clone)]
    pub enum ServerAction {
        Exit,
        Shutdown,
        Noop,
        Raw(Vec<u8>),
        Sleep(Duration),
        AssertOrder(Duration),
    }

    #[derive(Debug)]
    pub struct AutoMessageHandler {
        pub(crate) client_validator: Box<dyn ClientMessageValidator>,
        pub(crate) server_sender: Box<dyn ServerMessageSender>,
    }

    #[derive(Debug)]
    pub enum ActorBlock {
        BlockList(Context, Vec<ActorBlock>),
        ClientMessageValidate(Context, Box<dyn ClientMessageValidator>),
        ServerMessageSend(Context, Box<dyn ServerMessageSender>),
        ServerActionLine(Context, Box<dyn ServerActionLine>),
        Python(Context, String),
        Condition(Context, ConditionBlock),
        Alt(Context, Vec<ActorBlock>),
        Parallel(Context, Vec<ActorBlock>),
        Optional(Context, Box<ActorBlock>),
        Repeat(Context, Box<ActorBlock>, usize),
        AutoMessage(Context, AutoMessageHandler),
        NoOp(Context),
    }

    #[derive(Debug)]
    pub struct ConditionBlock {
        pub if_: (Context, String, Box<ActorBlock>),
        pub else_if: Vec<(Context, String, Box<ActorBlock>)>,
        pub else_: Option<(Context, Box<ActorBlock>)>,
    }

    impl ActorBlock {
        pub fn ctx(&self) -> &Context {
            match self {
                ActorBlock::BlockList(ctx, _)
                | ActorBlock::ClientMessageValidate(ctx, _)
                | ActorBlock::ServerMessageSend(ctx, _)
                | ActorBlock::ServerActionLine(ctx, _)
                | ActorBlock::Python(ctx, _)
                | ActorBlock::Condition(ctx, _)
                | ActorBlock::Alt(ctx, _)
                | ActorBlock::Parallel(ctx, _)
                | ActorBlock::Optional(ctx, _)
                | ActorBlock::Repeat(ctx, _, _)
                | ActorBlock::AutoMessage(ctx, _)
                | ActorBlock::NoOp(ctx) => ctx,
            }
        }
    }
}
