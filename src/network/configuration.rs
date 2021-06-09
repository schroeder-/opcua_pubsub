// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use crate::constants::PubSubTransportProfil;
use crate::until::decode_extension;
use log::error;
use opcua_types::status_code::StatusCode;
use opcua_types::DecodingOptions;
use opcua_types::KeyValuePair;
use opcua_types::NetworkAddressUrlDataType;
use opcua_types::ObjectId;
use opcua_types::PubSubConnectionDataType;
use opcua_types::UAString;
use opcua_types::Variant;
use url::Url;
/// Configures a PubSubConnection with needed settings
#[derive(Clone)]
pub enum ConnectionConfig {
    Uadp(UadpConfig),
    Mqtt(MqttConfig),
}

impl ConnectionConfig {
    pub fn from_cfg(cfg: &PubSubConnectionDataType) -> Result<Self, StatusCode> {
        let net = decode_extension::<NetworkAddressUrlDataType>(
            &cfg.address,
            ObjectId::NetworkAddressUrlDataType_Encoding_DefaultBinary,
            &DecodingOptions::default(),
        )?;
        match PubSubTransportProfil::from(&cfg.transport_profile_uri) {
            PubSubTransportProfil::UdpUadp => Ok(Self::Uadp(UadpConfig::new_with_if(
                net.url,
                net.network_interface,
            ))),
            PubSubTransportProfil::MqttUadp => Ok(Self::Mqtt(MqttConfig::new_from_probs(
                net.url,
                &cfg.connection_properties,
            ))),
            PubSubTransportProfil::AmqpUadp
            | PubSubTransportProfil::MqttJson
            | PubSubTransportProfil::AmqpJson
            | PubSubTransportProfil::EthUadp
            | PubSubTransportProfil::Unkown => Err(StatusCode::BadNotImplemented),
        }
    }

    pub const fn get_transport_profile(&self) -> PubSubTransportProfil {
        match self {
            ConnectionConfig::Uadp(_) => PubSubTransportProfil::UdpUadp,
            ConnectionConfig::Mqtt(_) => PubSubTransportProfil::MqttUadp,
        }
    }
    pub fn get_address(&self) -> NetworkAddressUrlDataType {
        match self {
            ConnectionConfig::Uadp(x) => NetworkAddressUrlDataType {
                network_interface: x.network_if(),
                url: x.url.clone(),
            },
            ConnectionConfig::Mqtt(x) => NetworkAddressUrlDataType {
                network_interface: UAString::null(),
                url: x.url.clone(),
            },
        }
    }

    pub fn get_connection_properties(&self) -> Vec<KeyValuePair> {
        match self {
            ConnectionConfig::Uadp(_) => Vec::new(),
            ConnectionConfig::Mqtt(x) => x.get_connection_properties(),
        }
    }
}

impl From<UadpConfig> for ConnectionConfig {
    fn from(ua: UadpConfig) -> Self {
        Self::Uadp(ua)
    }
}

impl From<MqttConfig> for ConnectionConfig {
    fn from(ua: MqttConfig) -> Self {
        Self::Mqtt(ua)
    }
}
/// Configures a Uadp Connection
#[derive(Clone)]
pub struct UadpConfig {
    network_if: Option<UAString>,
    url: UAString,
}

/// Choose which mqtt version
#[derive(Clone, PartialEq, Copy)]
pub enum MqttVersion {
    All = 0,
    OnlyV3_1 = 3,
    OnlyV3_1_1 = 4,
    OnlyV5 = 5,
}

impl MqttVersion {
    const fn try_from_i32(value: i32) -> Option<Self> {
        Some(match value {
            0 => Self::All,
            3 => Self::OnlyV3_1,
            4 => Self::OnlyV3_1_1,
            5 => Self::OnlyV5,
            _ => return None,
        })
    }
}
#[derive(Clone)]
/// Configures a Mqtt Connection
pub struct MqttConfig {
    pub(crate) url: UAString,
    pub(crate) username: UAString,
    pub(crate) password: UAString,
    pub(crate) clean_session: bool,
    pub(crate) version: MqttVersion,
}

impl UadpConfig {
    /// new `UadpConfig`
    /// ```
    /// let cfg = UadpConfig::new("opc.udp://239.0.0.1:4840".into());
    /// ```
    pub const fn new(url: UAString) -> Self {
        Self {
            network_if: None,
            url,
        }
    }
    /// new `UadpConfig` with Network interface
    /// ```
    /// let cfg = UadpConfig::new_with_if("127.0.0.1".into(), "opc.udp://239.0.0.1:4840".into());
    /// ```
    #[must_use]
    pub const fn new_with_if(url: UAString, network_if: UAString) -> Self {
        Self {
            network_if: Some(network_if),
            url,
        }
    }

    pub fn network_if(&self) -> UAString {
        self.network_if
            .as_ref()
            .map_or_else(UAString::null, |s| s.clone())
    }
    #[must_use]
    pub const fn url(&self) -> &UAString {
        &self.url
    }

    #[must_use]
    pub const fn profile(&self) -> PubSubTransportProfil {
        PubSubTransportProfil::UdpUadp
    }

    /// Extracts the config needed for creating a connection
    pub(crate) fn get_config(&self) -> Result<(String, String), StatusCode> {
        let hostname_port = match Url::parse(&self.url.to_string()) {
            Ok(uri) => match uri.scheme() {
                "opc.udp" => {
                    let u = format!(
                        "{}:{}",
                        uri.host_str().unwrap_or("localhost"),
                        uri.port().unwrap_or(4840)
                    );
                    u
                }
                _ => {
                    error!("No Invalid uri for uadp: {}", self.url);
                    return Err(StatusCode::BadInvalidArgument);
                }
            },
            Err(err) => {
                error!("Invalid uri: {} - {}", self.url, err);
                return Err(StatusCode::BadInvalidArgument);
            }
        };
        Ok((hostname_port, self.network_if().to_string()))
    }
}

impl MqttConfig {
    /// new `UadpConfig`
    /// url example: opc.udp://239.0.0.1:4840
    pub fn new(url: UAString) -> Self {
        Self {
            url,
            username: UAString::null(),
            password: UAString::null(),
            clean_session: false,
            version: MqttVersion::All,
        }
    }

    pub fn get_connection_properties(&self) -> Vec<KeyValuePair> {
        let kv = vec![
            KeyValuePair {
                key: "UserName".into(),
                value: self.username.clone().into(),
            },
            KeyValuePair {
                key: "Password".into(),
                value: self.password.clone().into(),
            },
            KeyValuePair {
                key: "CleanSession".into(),
                value: self.clean_session.into(),
            },
            KeyValuePair {
                key: "ProtocolVersion".into(),
                value: (self.version as i32).into(),
            },
        ];
        kv
    }

    pub fn new_from_probs(url: UAString, cfg: &Option<Vec<KeyValuePair>>) -> Self {
        let mut mq = Self::new(url);
        if let Some(probs) = &cfg {
            let get_val = |key: UAString| {
                probs
                    .iter()
                    .find(|x| x.key.name == key)
                    .map_or(&Variant::Empty, |f| &f.value)
            };
            if let Variant::String(ref u) = get_val("UserName".into()) {
                mq.username(u.clone());
            }
            if let Variant::String(ref u) = get_val("Password".into()) {
                mq.password(u.clone());
            }
            if let Variant::Boolean(u) = get_val("CleanSession".into()) {
                if *u {
                    mq.clean_session();
                }
            }
            if let Variant::Int32(u) = get_val("ProtocolVersion".into()) {
                if let Some(v) = MqttVersion::try_from_i32(*u) {
                    mq.mqtt_version(v);
                }
            }
        }
        mq
    }

    pub const fn url(&self) -> &UAString {
        &self.url
    }

    pub const fn profile(&self) -> PubSubTransportProfil {
        PubSubTransportProfil::MqttUadp
    }

    /// Sets the username for the connection
    pub fn username(&mut self, username: UAString) -> &mut Self {
        self.username = username;
        self
    }

    /// Sets the password for the connection
    pub fn password(&mut self, password: UAString) -> &mut Self {
        self.password = password;
        self
    }

    /// Clears session if one exists, default the connection is keeped
    pub fn clean_session(&mut self) -> &mut Self {
        self.clean_session = true;
        self
    }

    /// Setting the Version
    pub fn mqtt_version(&mut self, version: MqttVersion) -> &mut Self {
        self.version = version;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_urn() {
        let url = Url::parse("opc.udp://10.23.1.2:1234").unwrap();
        println!("{:?}", url);
        assert_eq!(url.scheme(), "opc.udp");
        assert_eq!(url.host_str(), Some("10.23.1.2"));
        assert_eq!(url.port(), Some(1234));
        let url = Url::parse("amqps://abc:1234/xxx").unwrap();
        assert_eq!(url.scheme(), "amqps");
        let url = Url::parse("wss://abc:123/xxx").unwrap();
        assert_eq!(url.scheme(), "wss");
        let url = Url::parse("mqtts://abc:1234/hhas").unwrap();
        assert_eq!(url.scheme(), "mqtts");
    }
}
