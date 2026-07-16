use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    time::Duration,
};

use anyhow::{Context, Result};
use reqwest::blocking::{Client, ClientBuilder};

const LOCAL_PROXY: &str = "http://127.0.0.1:10808";
const LOCAL_PROXY_ADDRESS: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10808);

pub fn client(timeout: Duration) -> Result<Client> {
    client_builder(timeout)
        .build()
        .context("无法创建网络请求客户端")
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
    for name in [
        "GQT_PROXY",
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
    ] {
        if let Ok(value) = std::env::var(name) {
            let value = value.trim();
            if value.starts_with("http://") || value.starts_with("https://") {
                return Some(value.to_string());
            }
        }
    }

    TcpStream::connect_timeout(&LOCAL_PROXY_ADDRESS, Duration::from_millis(250))
        .is_ok()
        .then(|| LOCAL_PROXY.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_proxy_url_is_valid() {
        assert!(reqwest::Proxy::all(LOCAL_PROXY).is_ok());
    }
}
