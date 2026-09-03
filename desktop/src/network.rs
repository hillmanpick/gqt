use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    time::Duration,
};

use anyhow::{Context, Result};
use reqwest::{
    Url,
    blocking::{Client, ClientBuilder},
};

const LOCAL_PROXY_PORTS: [u16; 7] = [7890, 7897, 7899, 10808, 10809, 20170, 20171];
const BINANCE_EGRESS_PROXY: &str = "http://127.0.0.1:18080";

pub fn client(timeout: Duration) -> Result<Client> {
    client_builder(timeout)
        .build()
        .context("无法创建网络请求客户端")
}

pub fn binance_client(timeout: Duration) -> Result<Client> {
    binance_client_builder(timeout)
        .build()
        .context("无法创建 Binance 网络请求客户端")
}

pub fn client_builder(timeout: Duration) -> ClientBuilder {
    let builder = Client::builder().timeout(timeout);
    match configured_proxy() {
        Some(proxy) => {
            builder.proxy(reqwest::Proxy::all(&proxy).expect("validated HTTP proxy URL"))
        }
        None => builder,
    }
}

pub fn configured_proxy() -> Option<String> {
    let configured = [
        "GQT_PROXY",
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
    ]
    .into_iter()
    .find_map(|name| {
        std::env::var(name).ok().and_then(|value| {
            let value = value.trim();
            if value.starts_with("http://") || value.starts_with("https://") {
                Some(value.to_string())
            } else {
                None
            }
        })
    });

    select_proxy(configured, detected_local_proxy())
}

pub fn binance_client_builder(timeout: Duration) -> ClientBuilder {
    let builder = Client::builder().timeout(timeout).no_proxy();
    // Binance Futures must never silently fall back to the host's direct route:
    // a VPN node change can expose a restricted mainland/datacenter IP and return 451.
    // TradingWorkspace starts the local egress bridge before any Binance request.
    builder.proxy(reqwest::Proxy::all(BINANCE_EGRESS_PROXY).expect("valid Binance proxy URL"))
}

fn detected_local_proxy() -> Option<String> {
    LOCAL_PROXY_PORTS.iter().find_map(|port| {
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), *port);
        TcpStream::connect_timeout(&address, Duration::from_millis(250))
            .is_ok()
            .then(|| format!("http://127.0.0.1:{port}"))
    })
}

fn select_proxy(configured: Option<String>, detected_local: Option<String>) -> Option<String> {
    match (configured, detected_local) {
        (Some(configured), Some(local)) if !is_loopback_proxy(&configured) => Some(local),
        (Some(configured), _) => Some(configured),
        (None, local) => local,
    }
}

fn is_loopback_proxy(proxy: &str) -> bool {
    Url::parse(proxy)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_proxy_url_is_valid() {
        for port in LOCAL_PROXY_PORTS {
            assert!(reqwest::Proxy::all(format!("http://127.0.0.1:{port}")).is_ok());
        }
    }

    #[test]
    fn local_proxy_wins_over_remote_environment_proxy() {
        assert_eq!(
            select_proxy(
                Some("http://192.168.5.2:7890".into()),
                Some("http://127.0.0.1:7890".into())
            ),
            Some("http://127.0.0.1:7890".into())
        );
    }

    #[test]
    fn remote_environment_proxy_is_used_without_a_local_proxy() {
        assert_eq!(
            select_proxy(Some("http://192.168.5.2:7890".into()), None),
            Some("http://192.168.5.2:7890".into())
        );
    }

    #[test]
    fn configured_loopback_proxy_remains_preferred() {
        assert_eq!(
            select_proxy(
                Some("http://localhost:7897".into()),
                Some("http://127.0.0.1:7890".into())
            ),
            Some("http://localhost:7897".into())
        );
    }
}
