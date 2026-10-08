//! The redis-rs clients for each kind of server, built with docket's
//! connection settings.

use std::time::Duration;

use redis::cluster::ClusterClient;
use redis::sentinel::{SentinelClient, SentinelClientBuilder, SentinelServerType};
use redis::{Client, ConnectionAddr, IntoConnectionInfo, RedisResult, TlsMode};

use super::url::SentinelUrl;
use super::{Provider, Settings};

/// A client for the cluster that `node` belongs to.
pub(super) fn cluster_client(
    node: &str,
    settings: &Settings,
    response_timeout: Option<Duration>,
    credentials: Option<&Provider>,
) -> RedisResult<ClusterClient> {
    let (min_wait, max_wait) = settings.retry_millis();
    let builder = ClusterClient::builder([node])
        .connection_timeout(settings.connection_timeout)
        .retries(settings.retries)
        .min_retry_wait(min_wait)
        .max_retry_wait(max_wait)
        .tcp_settings(settings.tcp());
    let builder = match response_timeout {
        Some(timeout) => builder.response_timeout(timeout),
        None => builder,
    };
    let builder = match credentials {
        Some(provider) => builder.set_credentials_provider(provider.clone()),
        None => builder,
    };
    builder.build()
}

/// A client for one server, with docket's TCP settings.
pub(super) fn client(url: &str, settings: &Settings) -> RedisResult<Client> {
    let info = url.into_connection_info()?.set_tcp_settings(settings.tcp());
    Client::open(info)
}

pub(super) fn sentinel_client(
    sentinel: SentinelUrl,
    settings: &Settings,
) -> RedisResult<SentinelClient> {
    let tls = sentinel.tls.then_some(TlsMode::Secure);
    let addresses = sentinel
        .sentinels
        .into_iter()
        .map(|(host, port)| match tls {
            Some(_) => ConnectionAddr::TcpTls {
                host,
                port,
                insecure: false,
                tls_params: None,
            },
            None => ConnectionAddr::Tcp(host, port),
        });
    // Neither step fails for the addresses and TLS settings built here.
    SentinelClientBuilder::new(addresses, sentinel.service, SentinelServerType::Master).and_then(
        |builder| {
            let mut builder = builder
                .set_client_to_redis_db(sentinel.db)
                .set_client_to_redis_tcp_settings(settings.tcp())
                .set_client_to_sentinel_tcp_settings(settings.tcp());
            if let Some(tls) = tls {
                builder = builder
                    .set_client_to_redis_tls_mode(tls)
                    .set_client_to_sentinel_tls_mode(tls);
            }
            if let Some(username) = sentinel.username {
                builder = builder.set_client_to_redis_username(username);
            }
            if let Some(password) = sentinel.password {
                builder = builder.set_client_to_redis_password(password);
            }
            if let Some(username) = sentinel.daemon_username {
                builder = builder.set_client_to_sentinel_username(username);
            }
            if let Some(password) = sentinel.daemon_password {
                builder = builder.set_client_to_sentinel_password(password);
            }
            builder.build()
        },
    )
}
