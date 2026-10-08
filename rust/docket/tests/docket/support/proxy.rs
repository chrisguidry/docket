//! A TCP proxy in front of Redis that fails chosen commands, for testing
//! what docket does when Redis refuses a command or goes away.

use std::collections::VecDeque;
use std::fmt::Write;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinSet;

type Matcher = Box<dyn Fn(&[Vec<u8>]) -> bool + Send>;

/// Fails the next `times` commands that `matches` picks, with `error` in
/// place of the reply, or with a generic error when `error` is `None`.
struct Rule {
    matches: Matcher,
    times: usize,
    error: Option<String>,
}

#[derive(Default)]
struct State {
    rules: Vec<Rule>,
    cut: bool,
    /// Drops every command without an answer, as if Redis stopped
    /// answering without closing its connections.
    silent: bool,
    connections: JoinSet<()>,
    /// How many of each command reached Redis, by upper-case name.
    counts: std::collections::HashMap<String, usize>,
    /// How many reads from a client carried each command, by upper-case
    /// name.  A client writes a pipeline at once and waits for its replies,
    /// so a small pipeline arrives in one read.
    batches: std::collections::HashMap<String, usize>,
    /// Ends every subscribed connection, and only those.
    drop_subscribers: Arc<tokio::sync::Notify>,
}

impl State {
    /// The error reply for this command when it should fail, spending one
    /// of a rule's failures.
    fn failure(&mut self, args: &[Vec<u8>], name: &[u8]) -> Option<Vec<u8>> {
        let rule = self
            .rules
            .iter_mut()
            .find(|rule| rule.times > 0 && (rule.matches)(args))?;
        rule.times -= 1;
        let error = rule.error.clone().unwrap_or_else(|| {
            format!("ERR injected failure for {}", String::from_utf8_lossy(name))
        });
        Some(format!("-{error}\r\n").into_bytes())
    }
}

pub struct Proxy {
    address: SocketAddr,
    /// The upstream URL's credentials, which docket still sends through
    /// the proxy.
    userinfo: Option<String>,
    state: Arc<Mutex<State>>,
}

impl Proxy {
    /// A proxy in front of the Redis at `upstream`, a `host:port`.
    pub async fn start(upstream: String, userinfo: Option<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        tokio::spawn(accept(listener, upstream, Arc::clone(&state)));
        Self {
            address,
            userinfo,
            state,
        }
    }

    /// The URL of the proxy.
    pub fn url(&self) -> String {
        match &self.userinfo {
            Some(userinfo) => format!("redis://{userinfo}@{}/0", self.address),
            None => format!("redis://{}/0", self.address),
        }
    }

    /// Answers the next `times` commands named `command` with an error,
    /// without sending them to Redis.
    pub fn fail(&self, command: &str, times: usize) {
        let command = command.to_ascii_uppercase();
        self.add(times, None, move |args| {
            args[0].eq_ignore_ascii_case(command.as_bytes())
        });
    }

    /// Answers the next `times` runs of the Lua script with this source with
    /// an error.
    pub fn fail_script(&self, source: &str, times: usize) {
        self.refuse_script(source, times, None);
    }

    /// Answers the next `times` runs of the Lua script with this source with
    /// `error`, such as `READONLY You can't write against a read only
    /// replica.`
    pub fn fail_script_with(&self, source: &str, times: usize, error: &str) {
        self.refuse_script(source, times, Some(error.to_owned()));
    }

    fn refuse_script(&self, source: &str, times: usize, error: Option<String>) {
        let sha = Sha1::digest(source.as_bytes())
            .iter()
            .fold(String::new(), |mut hex, byte| {
                let _ = write!(hex, "{byte:02x}");
                hex
            });
        self.add(times, error, move |args| {
            args[0].eq_ignore_ascii_case(b"EVALSHA")
                && args
                    .get(1)
                    .is_some_and(|arg| arg.eq_ignore_ascii_case(sha.as_bytes()))
        });
    }

    fn add(
        &self,
        times: usize,
        error: Option<String>,
        matches: impl Fn(&[Vec<u8>]) -> bool + Send + 'static,
    ) {
        self.state.lock().unwrap().rules.push(Rule {
            matches: Box::new(matches),
            times,
            error,
        });
    }

    /// Drops every connection and refuses new ones, as if Redis went away.
    pub fn cut(&self) {
        let mut state = self.state.lock().unwrap();
        state.cut = true;
        state.connections.abort_all();
    }

    /// Drops every command from now on without an answer, on the open
    /// connections and on new ones.
    pub fn silence(&self) {
        self.state.lock().unwrap().silent = true;
    }

    /// Accepts connections and answers commands again.
    pub fn heal(&self) {
        let mut state = self.state.lock().unwrap();
        state.cut = false;
        state.silent = false;
    }

    /// Drops every connection that has subscribed to a channel, as if Redis
    /// dropped only the subscriptions.
    pub fn drop_subscribers(&self) {
        self.state.lock().unwrap().drop_subscribers.notify_waiters();
    }

    /// How many reads from a client carried a command named `command`, which
    /// is how many pipelines carried it, when the pipelines are small.
    pub fn batches(&self, command: &str) -> usize {
        let state = self.state.lock().unwrap();
        state
            .batches
            .get(&command.to_ascii_uppercase())
            .copied()
            .unwrap_or(0)
    }

    /// How many commands named `command` reached the proxy, failed or not.
    pub fn count(&self, command: &str) -> usize {
        let state = self.state.lock().unwrap();
        state
            .counts
            .get(&command.to_ascii_uppercase())
            .copied()
            .unwrap_or(0)
    }
}

async fn accept(listener: TcpListener, upstream: String, state: Arc<Mutex<State>>) {
    while let Ok((client, _)) = listener.accept().await {
        let mut locked = state.lock().unwrap();
        if locked.cut {
            continue;
        }
        let upstream = upstream.clone();
        let shared = Arc::clone(&state);
        locked.connections.spawn(async move {
            if let Ok(server) = TcpStream::connect(&upstream).await {
                relay(client, server, shared).await;
            }
        });
    }
}

/// What the client gets for one command, in the order it sent them.
enum Pending {
    /// The next reply from Redis.
    Forwarded,
    /// An error the proxy made up.
    Injected(Vec<u8>),
}

/// One proxied connection's shared state.
struct Connection {
    pending: Mutex<VecDeque<Pending>>,
    /// Set once the client subscribes; from then on Redis pushes messages
    /// that answer no command, so the proxy forwards bytes untouched.
    transparent: AtomicBool,
    to_client: UnboundedSender<Vec<u8>>,
}

async fn relay(client: TcpStream, server: TcpStream, state: Arc<Mutex<State>>) {
    let (client_read, mut client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();
    let (to_client, mut outgoing) = unbounded_channel::<Vec<u8>>();
    let connection = Arc::new(Connection {
        pending: Mutex::new(VecDeque::new()),
        transparent: AtomicBool::new(false),
        to_client,
    });

    let writer = tokio::spawn(async move {
        while let Some(bytes) = outgoing.recv().await {
            if client_write.write_all(&bytes).await.is_err() {
                break;
            }
        }
    });
    let replies = tokio::spawn(replies(server_read, Arc::clone(&connection)));
    let drop_subscribers = Arc::clone(&state.lock().unwrap().drop_subscribers);
    let dropped = async {
        loop {
            drop_subscribers.notified().await;
            if connection.transparent.load(Ordering::SeqCst) {
                break;
            }
        }
    };
    tokio::select! {
        () = commands(client_read, server_write, &connection, &state) => {}
        () = dropped => {}
    }
    replies.abort();
    writer.abort();
}

/// Passes Redis's replies to the client, with injected errors in place.
async fn replies(mut server: OwnedReadHalf, connection: Arc<Connection>) {
    let mut buffer = Vec::new();
    let mut chunk = vec![0u8; 16384];
    while let Ok(read @ 1..) = server.read(&mut chunk).await {
        buffer.extend_from_slice(&chunk[..read]);
        if connection.transparent.load(Ordering::SeqCst) {
            let _ = connection.to_client.send(std::mem::take(&mut buffer));
            continue;
        }
        while let Some(length) = reply_end(&buffer, 0) {
            let mut out: Vec<u8> = buffer.drain(..length).collect();
            let mut pending = connection.pending.lock().unwrap();
            pending.pop_front();
            // Injected errors wait until the replies ahead of them arrive.
            while let Some(Pending::Injected(_)) = pending.front() {
                if let Some(Pending::Injected(error)) = pending.pop_front() {
                    out.extend(error);
                }
            }
            let _ = connection.to_client.send(out);
        }
    }
}

/// Reads the client's commands, and forwards each one or fails it.
async fn commands(
    mut client: OwnedReadHalf,
    mut server: OwnedWriteHalf,
    connection: &Connection,
    state: &Mutex<State>,
) {
    let mut buffer = Vec::new();
    let mut chunk = vec![0u8; 16384];
    while let Ok(read @ 1..) = client.read(&mut chunk).await {
        buffer.extend_from_slice(&chunk[..read]);
        let mut forward = Vec::new();
        let mut names = std::collections::HashSet::new();
        while let Some((length, args)) = parse_command(&buffer) {
            let raw: Vec<u8> = buffer.drain(..length).collect();
            let name = args
                .first()
                .map(|arg| arg.to_ascii_uppercase())
                .unwrap_or_default();
            if state.lock().unwrap().silent {
                continue;
            }
            if name == b"SUBSCRIBE" || name == b"PSUBSCRIBE" {
                connection.transparent.store(true, Ordering::SeqCst);
            }
            names.insert(String::from_utf8_lossy(&name).into_owned());
            *state
                .lock()
                .unwrap()
                .counts
                .entry(String::from_utf8_lossy(&name).into_owned())
                .or_default() += 1;
            let failure = if connection.transparent.load(Ordering::SeqCst) {
                None
            } else {
                state.lock().unwrap().failure(&args, &name)
            };
            let Some(error) = failure else {
                connection
                    .pending
                    .lock()
                    .unwrap()
                    .push_back(Pending::Forwarded);
                forward.extend(raw);
                continue;
            };
            let mut pending = connection.pending.lock().unwrap();
            if pending.is_empty() {
                let _ = connection.to_client.send(error);
            } else {
                pending.push_back(Pending::Injected(error));
            }
        }
        {
            let mut state = state.lock().unwrap();
            for name in names {
                *state.batches.entry(name).or_default() += 1;
            }
        }
        if !forward.is_empty() && server.write_all(&forward).await.is_err() {
            break;
        }
    }
}

/// The length of the RESP2 command at the start of `buffer` and its
/// arguments, once it has fully arrived.
fn parse_command(buffer: &[u8]) -> Option<(usize, Vec<Vec<u8>>)> {
    let (count, mut at) = line(buffer, 0, b'*')?;
    let mut args = Vec::new();
    for _ in 0..count {
        let (length, start) = line(buffer, at, b'$')?;
        let end = start + usize::try_from(length).ok()?;
        if buffer.len() < end + 2 {
            return None;
        }
        args.push(buffer[start..end].to_vec());
        at = end + 2;
    }
    Some((at, args))
}

/// Reads `<prefix><number>\r\n` at `at`, and returns the number and where
/// the next line starts.
fn line(buffer: &[u8], at: usize, prefix: u8) -> Option<(i64, usize)> {
    if *buffer.get(at)? != prefix {
        return None;
    }
    let end = at + buffer[at..].windows(2).position(|pair| pair == b"\r\n")?;
    let number = std::str::from_utf8(&buffer[at + 1..end])
        .ok()?
        .parse()
        .ok()?;
    Some((number, end + 2))
}

/// Where the RESP2 reply that starts at `at` ends, once it has fully
/// arrived.
fn reply_end(buffer: &[u8], at: usize) -> Option<usize> {
    let end = at
        + buffer
            .get(at..)?
            .windows(2)
            .position(|pair| pair == b"\r\n")?;
    let header = &buffer[at + 1..end];
    let next = end + 2;
    match buffer[at] {
        b'+' | b'-' | b':' => Some(next),
        b'$' => {
            let length: i64 = std::str::from_utf8(header).ok()?.parse().ok()?;
            if length < 0 {
                return Some(next);
            }
            let finish = next + usize::try_from(length).ok()? + 2;
            (buffer.len() >= finish).then_some(finish)
        }
        b'*' => {
            let count: i64 = std::str::from_utf8(header).ok()?.parse().ok()?;
            let mut position = next;
            for _ in 0..count.max(0) {
                position = reply_end(buffer, position)?;
            }
            Some(position)
        }
        _ => None,
    }
}
