//! One client connection: read requests, run them, write replies, and push
//! pub/sub messages between requests.

use std::ops::ControlFlow;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf};
use tokio::time::{Instant, sleep_until};

use crate::memory::commands::{self, Args, Block, Command, Read};
use crate::memory::engine::store::Store;
use crate::memory::pubsub::{Kind, Subscriptions};
use crate::memory::resp::{Reply, parse_request};

/// The commands a connection with subscriptions still accepts.
const SUBSCRIBED_COMMANDS: [&str; 5] = [
    "SUBSCRIBE",
    "PSUBSCRIBE",
    "UNSUBSCRIBE",
    "PUNSUBSCRIBE",
    "PING",
];

/// Commands queued after MULTI, waiting for EXEC.
struct Transaction {
    queued: Vec<Vec<Bytes>>,
    /// A command failed to queue, so EXEC must refuse the whole transaction.
    aborted: bool,
}

/// What running a command produced.
enum Outcome {
    Replies(Vec<Reply>),
    Read(Read),
}

impl From<Reply> for Outcome {
    fn from(reply: Reply) -> Self {
        Self::Replies(vec![reply])
    }
}

pub(crate) struct Session {
    store: Arc<Store>,
    reader: ReadHalf<DuplexStream>,
    writer: WriteHalf<DuplexStream>,
    input: BytesMut,
    output: BytesMut,
    transaction: Option<Transaction>,
    subscriptions: Subscriptions,
}

impl Session {
    pub(crate) fn new(store: Arc<Store>, stream: DuplexStream) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        Self {
            subscriptions: Subscriptions::new(Arc::clone(&store)),
            store,
            reader,
            writer,
            input: BytesMut::new(),
            output: BytesMut::new(),
            transaction: None,
        }
    }

    /// Serves the client until it disconnects or breaks the protocol.
    pub(crate) async fn run(mut self) -> std::io::Result<()> {
        loop {
            let flow = self.run_buffered_requests().await;
            self.writer.write_all(&self.output).await?;
            self.output.clear();
            if flow.is_break() {
                return Ok(());
            }
            tokio::select! {
                read = self.reader.read_buf(&mut self.input) => {
                    if !matches!(read, Ok(count) if count > 0) {
                        return Ok(());
                    }
                }
                message = self.subscriptions.next_message() => message.encode(&mut self.output),
            }
        }
    }

    /// Runs every whole request in the input buffer.  Breaks when the
    /// connection has to close.
    async fn run_buffered_requests(&mut self) -> ControlFlow<()> {
        loop {
            match parse_request(&mut self.input) {
                Ok(Some(request)) => self.handle(request).await?,
                Ok(None) => return ControlFlow::Continue(()),
                Err(error) => {
                    error.reply().encode(&mut self.output);
                    return ControlFlow::Break(());
                }
            }
        }
    }

    async fn handle(&mut self, request: Vec<Bytes>) -> ControlFlow<()> {
        let mut request = request.into_iter();
        // Redis ignores an empty request.
        let Some(name) = request.next() else {
            return ControlFlow::Continue(());
        };
        let name = String::from_utf8_lossy(&name).to_ascii_uppercase();
        let args: Vec<Bytes> = request.collect();
        let replies = match name.as_str() {
            "MULTI" if self.transaction.is_some() => {
                vec![Reply::Error("ERR MULTI calls can not be nested".to_owned())]
            }
            "MULTI" => {
                self.transaction = Some(Transaction {
                    queued: Vec::new(),
                    aborted: false,
                });
                vec![Reply::ok()]
            }
            "EXEC" | "DISCARD" => match self.transaction.take() {
                None => vec![Reply::Error(format!("ERR {name} without MULTI"))],
                Some(transaction) if name == "EXEC" => vec![self.exec(transaction)],
                Some(_) => vec![Reply::ok()],
            },
            _ => match &mut self.transaction {
                Some(transaction) => vec![queue(transaction, name, args)],
                None => match self.execute(&name, args) {
                    Outcome::Replies(replies) => replies,
                    Outcome::Read(read) => vec![self.wait(&read).await?],
                },
            },
        };
        for reply in replies {
            reply.encode(&mut self.output);
        }
        ControlFlow::Continue(())
    }

    /// Runs one command outside of MULTI, or inside EXEC.
    fn execute(&mut self, name: &str, args: Vec<Bytes>) -> Outcome {
        if self.subscriptions.is_active() && !SUBSCRIBED_COMMANDS.contains(&name) {
            return Reply::Error(format!(
                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                name.to_ascii_lowercase()
            ))
            .into();
        }
        let mut args = Args::new(name, args);
        match name {
            "SUBSCRIBE" | "PSUBSCRIBE" => match args.rest() {
                Ok(names) => Outcome::Replies(self.subscriptions.subscribe(kind(name), names)),
                Err(error) => Reply::from(error).into(),
            },
            "UNSUBSCRIBE" | "PUNSUBSCRIBE" => {
                let names = args.remaining();
                Outcome::Replies(self.subscriptions.unsubscribe(kind(name), names))
            }
            // In subscribe mode, PING answers in the shape of a message.
            "PING" if self.subscriptions.is_active() => {
                let message = args.remaining().into_iter().next().unwrap_or_default();
                Reply::bulks([Bytes::from("pong"), message]).into()
            }
            _ => match commands::lookup(name) {
                None => unknown_command(name).into(),
                Some(Command::Reply(command)) => command(&self.store, &mut args)
                    .unwrap_or_else(Reply::from)
                    .into(),
                Some(Command::Read(command)) => match command(&self.store, &mut args) {
                    Ok(read) => Outcome::Read(read),
                    Err(error) => Reply::from(error).into(),
                },
            },
        }
    }

    /// Runs the queued commands one after another.  Each `Store` call takes
    /// its own lock, so other connections can run between them.
    fn exec(&mut self, transaction: Transaction) -> Reply {
        if transaction.aborted {
            return Reply::Error(
                "EXECABORT Transaction discarded because of previous errors.".to_owned(),
            );
        }
        let mut replies = Vec::with_capacity(transaction.queued.len());
        for mut request in transaction.queued {
            let args = request.split_off(1);
            let name = String::from_utf8_lossy(&request[0]).to_ascii_uppercase();
            match self.execute(&name, args) {
                Outcome::Replies(mut more) => replies.append(&mut more),
                // A transaction never waits, as in Redis.
                Outcome::Read(read) => {
                    replies.push(read.attempt(&self.store).unwrap_or(Reply::Nil));
                }
            }
        }
        Reply::Array(replies)
    }

    /// Tries a read until it finds entries or its block runs out.  Breaks
    /// when the client disconnects while it waits.
    async fn wait(&mut self, read: &Read) -> ControlFlow<(), Reply> {
        let deadline = match read.block {
            Block::Never => {
                return ControlFlow::Continue(read.attempt(&self.store).unwrap_or(Reply::Nil));
            }
            Block::For(duration) => Some(Instant::now() + duration),
            Block::Forever => None,
        };
        let notify = self.store.stream_notify();
        loop {
            // Arm the notification before trying, so an XADD between the
            // try and the wait still wakes this read.
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(reply) = read.attempt(&self.store) {
                return ControlFlow::Continue(reply);
            }
            tokio::select! {
                () = &mut notified => {}
                () = expiry(deadline) => return ControlFlow::Continue(Reply::Nil),
                // Requests that arrive meanwhile wait in the buffer, as they
                // would behind a blocked command in Redis.
                read = self.reader.read_buf(&mut self.input) => {
                    if !matches!(read, Ok(count) if count > 0) {
                        return ControlFlow::Break(());
                    }
                }
            }
        }
    }
}

fn kind(name: &str) -> Kind {
    if name.starts_with('P') {
        Kind::Pattern
    } else {
        Kind::Channel
    }
}

fn unknown_command(name: &str) -> Reply {
    Reply::Error(format!(
        "ERR unknown command '{}'",
        name.to_ascii_lowercase()
    ))
}

/// Queues a command for EXEC.  An unknown command fails now and spoils the
/// transaction, as in Redis.
fn queue(transaction: &mut Transaction, name: String, args: Vec<Bytes>) -> Reply {
    let known = commands::lookup(&name).is_some() || SUBSCRIBED_COMMANDS.contains(&name.as_str());
    if !known {
        transaction.aborted = true;
        return unknown_command(&name);
    }
    let mut request = Vec::with_capacity(args.len() + 1);
    request.push(Bytes::from(name));
    request.extend(args);
    transaction.queued.push(request);
    Reply::Status("QUEUED".to_owned())
}

async fn expiry(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}
