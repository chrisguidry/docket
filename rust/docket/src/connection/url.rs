//! The URL forms a docket accepts, the same ones pydocket accepts.

use percent_encoding::percent_decode_str;

use crate::error::{Error, Result};

const DEFAULT_SENTINEL_PORT: u16 = 26379;

/// Where a docket URL points.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Target {
    /// `redis://`, `rediss://`, and `unix://`: one server.
    Standalone(String),
    /// `redis+cluster://` and `rediss+cluster://`: a cluster, found from the
    /// node in the URL.
    Cluster(String),
    /// `redis+sentinel://` and `rediss+sentinel://`: the master that the
    /// sentinels report for a service.
    Sentinel(SentinelUrl),
    /// `memory://`: the in-process engine.
    Memory(String),
}

/// A parsed `redis+sentinel://[user:pass@]host[:port][,host:port...]/service[/db]` URL.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SentinelUrl {
    pub sentinels: Vec<(String, u16)>,
    pub service: String,
    pub db: i64,
    pub tls: bool,
    /// Credentials for the master, from the userinfo.
    pub username: Option<String>,
    pub password: Option<String>,
    /// Credentials for the sentinels, from `?sentinel_username=` and
    /// `?sentinel_password=`.
    pub daemon_username: Option<String>,
    pub daemon_password: Option<String>,
}

/// The query parameters whose values are passwords.
const SECRET_PARAMETERS: [&str; 1] = ["sentinel_password"];

/// `url` with `***` in place of each password, for error messages and debug
/// output.  It works on URLs that do not parse, since those are the ones
/// that end up in errors.
pub(crate) fn redact(url: &str) -> String {
    let (rest, query) = match url.split_once('?') {
        Some((rest, query)) => (rest, Some(query)),
        None => (url, None),
    };
    // The userinfo ends at the last `@`, since a password may hold an
    // unescaped `@` or `/`.
    let start = rest.find("://").map_or(0, |scheme| scheme + 3);
    let mut redacted = match rest.rfind('@').filter(|at| *at > start) {
        Some(at) => match rest[start..at].split_once(':') {
            Some((username, _)) => format!("{}{username}:***{}", &rest[..start], &rest[at..]),
            None => rest.to_owned(),
        },
        None => rest.to_owned(),
    };
    if let Some(query) = query {
        let parameters: Vec<String> = query
            .split('&')
            .map(|parameter| match parameter.split_once('=') {
                Some((name, _)) if SECRET_PARAMETERS.contains(&name) => format!("{name}=***"),
                _ => parameter.to_owned(),
            })
            .collect();
        redacted.push('?');
        redacted.push_str(&parameters.join("&"));
    }
    redacted
}

pub(crate) fn parse(url: &str) -> Result<Target> {
    let Some((scheme, rest)) = url.split_once("://") else {
        return Err(Error::url(url, "it has no scheme"));
    };
    match scheme {
        "redis" | "rediss" | "unix" | "redis+unix" => Ok(Target::Standalone(url.to_owned())),
        "redis+cluster" | "rediss+cluster" => {
            let plain = scheme.trim_end_matches("+cluster");
            Ok(Target::Cluster(format!("{plain}://{rest}")))
        }
        "redis+sentinel" | "rediss+sentinel" => {
            parse_sentinel(url, rest, scheme == "rediss+sentinel").map(Target::Sentinel)
        }
        "memory" => Ok(Target::Memory(url.to_owned())),
        _ => Err(Error::url(url, format!("docket does not know {scheme}://"))),
    }
}

fn parse_sentinel(url: &str, rest: &str, tls: bool) -> Result<SentinelUrl> {
    let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let (userinfo, hosts) = match authority.rsplit_once('@') {
        Some((userinfo, hosts)) => (Some(userinfo), hosts),
        None => (None, authority),
    };

    let mut sentinels = Vec::new();
    for member in hosts.split(',').map(str::trim).filter(|m| !m.is_empty()) {
        sentinels.push(parse_member(url, member)?);
    }
    if sentinels.is_empty() {
        return Err(Error::url(url, "it names no sentinel host"));
    }

    let mut segments = path.split('/').filter(|s| !s.is_empty());
    let Some(service) = segments.next() else {
        return Err(Error::url(
            url,
            "it names no service, as in redis+sentinel://localhost:26379/mymaster",
        ));
    };
    let db = match segments.next() {
        Some(db) => db
            .parse()
            .map_err(|_| Error::url(url, format!("{db} is not a database number")))?,
        None => 0,
    };

    let (username, password) = match userinfo {
        Some(userinfo) => {
            let (user, pass) = userinfo.split_once(':').unwrap_or((userinfo, ""));
            (decoded(user), decoded(pass))
        }
        None => (None, None),
    };

    let mut daemon_username = None;
    let mut daemon_password = None;
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        match name {
            "sentinel_username" => daemon_username = decoded(value),
            "sentinel_password" => daemon_password = decoded(value),
            _ => {}
        }
    }

    Ok(SentinelUrl {
        sentinels,
        service: decoded(service).unwrap_or_default(),
        db,
        tls,
        username,
        password,
        daemon_username,
        daemon_password,
    })
}

fn parse_member(url: &str, member: &str) -> Result<(String, u16)> {
    // A bracketed IPv6 address has colons of its own, so the port is what
    // follows the closing bracket.
    let (host, port) = if let Some(bracketed) = member.strip_prefix('[') {
        let (host, after) = bracketed
            .split_once(']')
            .ok_or_else(|| Error::url(url, format!("{member} has no closing bracket")))?;
        (host, after.strip_prefix(':'))
    } else {
        match member.rsplit_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (member, None),
        }
    };
    if host.is_empty() {
        return Err(Error::url(url, format!("{member} has no host")));
    }
    let port = match port {
        Some(port) => port
            .parse()
            .map_err(|_| Error::url(url, format!("{member} has no valid port")))?,
        None => DEFAULT_SENTINEL_PORT,
    };
    Ok((host.to_owned(), port))
}

fn decoded(text: &str) -> Option<String> {
    let text = percent_decode_str(text).decode_utf8_lossy().into_owned();
    (!text.is_empty()).then_some(text)
}

#[cfg(test)]
mod tests;
