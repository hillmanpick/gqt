use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    time::Duration,
};

use anyhow::{Context, Result};
use reqwest::blocking::{Client, ClientBuilder};

const LOCAL_PROXY_PORTS: [u16; 7] = [7890, 7897, 7899, 10808, 10809, 20170, 20171];

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

    detected_local_proxy()
}

pub fn configured_docker_proxy() -> Option<String> {
    configured_proxy().map(|proxy| proxy_for_docker(&proxy))
}

fn detected_local_proxy() -> Option<String> {
    LOCAL_PROXY_PORTS.iter().find_map(|port| {
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), *port);
        TcpStream::connect_timeout(&address, Duration::from_millis(250))
            .is_ok()
            .then(|| format!("http://127.0.0.1:{port}"))
    })
}

fn proxy_for_docker(proxy: &str) -> String {
    proxy
        .replace("://127.0.0.1:", "://host.docker.internal:")
        .replace("://localhost:", "://host.docker.internal:")
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
    fn rewrites_loopback_proxy_for_docker() {
        assert_eq!(
            proxy_for_docker("http://127.0.0.1:7890"),
            "http://host.docker.internal:7890"
        );
        assert_eq!(
            proxy_for_docker("http://localhost:10808"),
            "http://host.docker.internal:10808"
        );
        assert_eq!(
            proxy_for_docker("http://10.0.0.2:7890"),
            "http://10.0.0.2:7890"
        );
    }
}
