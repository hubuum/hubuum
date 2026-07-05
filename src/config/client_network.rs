use std::net::IpAddr;
use std::str::FromStr;

use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use serde::{Deserialize, Serialize};

use crate::errors::ApiError;

pub fn parse_client_allowlist(s: &str) -> Result<ClientAllowlist, String> {
    ClientAllowlist::from_str(s).map_err(|e| e.to_string())
}

pub fn parse_trusted_proxies(s: &str) -> Result<TrustedProxies, String> {
    TrustedProxies::from_str(s).map_err(|e| e.to_string())
}

/// Client IP allowlist - either allow all (`*`) or specific IPs/CIDRs
#[derive(Debug, Clone)]
pub enum ClientAllowlist {
    Any,
    Nets(Vec<IpNet>),
}

impl Serialize for ClientAllowlist {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ClientAllowlist::Any => serializer.serialize_str("*"),
            ClientAllowlist::Nets(nets) => {
                let s = nets
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                serializer.serialize_str(&s)
            }
        }
    }
}

impl<'de> Deserialize<'de> for ClientAllowlist {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ClientAllowlist::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl ClientAllowlist {
    /// Parse a CLI/env string into a ClientAllowlist
    pub fn parse_cli(input: &str) -> Result<Self, ApiError> {
        let trimmed = input.trim();

        if trimmed == "*" {
            return Ok(Self::Any);
        }

        let nets: Vec<IpNet> = trimmed
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(Self::parse_net)
            .collect::<Result<_, _>>()?;

        if nets.is_empty() {
            return Err(ApiError::BadRequest(
                "client allowlist cannot be empty".into(),
            ));
        }

        Ok(Self::Nets(nets))
    }

    /// Check if an IP address is allowed
    pub fn allows(&self, ip: IpAddr) -> bool {
        match self {
            ClientAllowlist::Any => true,
            ClientAllowlist::Nets(nets) => nets.iter().any(|net| match (net, ip) {
                (IpNet::V4(net), IpAddr::V4(addr)) => net.contains(&addr),
                (IpNet::V6(net), IpAddr::V6(addr)) => net.contains(&addr),
                _ => false,
            }),
        }
    }

    /// Parse a network CIDR or single IP
    fn parse_net(raw: &str) -> Result<IpNet, ApiError> {
        IpNet::from_str(raw)
            .or_else(|_| Self::ip_to_host_net(raw))
            .map_err(|_| ApiError::BadRequest(format!("Invalid IP/CIDR: {}", raw)))
    }

    /// Convert a single IP address to a /32 or /128 network
    fn ip_to_host_net(raw: &str) -> Result<IpNet, ()> {
        let ip: IpAddr = raw.parse().map_err(|_| ())?;
        match ip {
            IpAddr::V4(addr) => Ipv4Net::new(addr, 32).map(IpNet::from).map_err(|_| ()),
            IpAddr::V6(addr) => Ipv6Net::new(addr, 128).map(IpNet::from).map_err(|_| ()),
        }
    }
}

impl FromStr for ClientAllowlist {
    type Err = ApiError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_cli(s)
    }
}

/// Trusted reverse-proxy networks used to resolve the real client IP from a forwarded
/// hop chain. Unlike [`ClientAllowlist`], an empty set is valid and means "no trusted
/// proxies configured".
#[derive(Debug, Clone, Default)]
pub struct TrustedProxies(Vec<IpNet>);

impl TrustedProxies {
    /// The configured trusted-proxy networks.
    pub fn nets(&self) -> &[IpNet] {
        &self.0
    }
}

impl FromStr for TrustedProxies {
    type Err = ApiError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let nets = s
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(ClientAllowlist::parse_net)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(TrustedProxies(nets))
    }
}

impl Serialize for TrustedProxies {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = self
            .0
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for TrustedProxies {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        TrustedProxies::from_str(&s).map_err(serde::de::Error::custom)
    }
}
