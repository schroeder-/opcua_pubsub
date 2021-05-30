// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use crate::constants::PubSubTransportProfil;
use crate::until::decode_extension;
use log::error;
use opcua_types::status_code::StatusCode;
use opcua_types::DecodingOptions;
use opcua_types::NetworkAddressUrlDataType;
use opcua_types::ObjectId;
use opcua_types::PubSubConnectionDataType;
use opcua_types::UAString;
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
            PubSubTransportProfil::UdpUadp => Ok(ConnectionConfig::Uadp(UadpConfig::new_with_if(
                net.url,
                net.network_interface,
            ))),
            PubSubTransportProfil::MqttUadp => {
                // @TODO parse cfg
                // cfg.connection_properties
                Ok(ConnectionConfig::Mqtt(MqttConfig::new(net.url)))
            }
            PubSubTransportProfil::AmqpUadp
            | PubSubTransportProfil::MqttJson
            | PubSubTransportProfil::AmqpJson
            | PubSubTransportProfil::EthUadp
            | PubSubTransportProfil::Unkown => Err(StatusCode::BadNotImplemented),
        }
    }
}

impl From<UadpConfig> for ConnectionConfig {
    fn from(ua: UadpConfig) -> Self {
        ConnectionConfig::Uadp(ua)
    }
}

impl From<MqttConfig> for ConnectionConfig {
    fn from(ua: MqttConfig) -> Self {
        ConnectionConfig::Mqtt(ua)
    }
}
/// Configures a Uadp Connection
#[derive(Clone)]
pub struct UadpConfig {
    network_if: Option<UAString>,
    url: UAString,
}
#[derive(Clone)]
/// Configures a Mqtt Connection
/// @TODO support more configuration
pub struct MqttConfig {
    url: UAString,
}

impl UadpConfig {
    /// new UadpConfig
    /// url example: opc.udp://239.0.0.1:4840
    pub fn new(url: UAString) -> Self {
        UadpConfig {
            network_if: None,
            url,
        }
    }
    /// new UadpConfig with Network interface
    /// example:
    /// url: opc.udp://239.0.0.1:4840
    /// network_if: "127.0.0.1"
    pub fn new_with_if(url: UAString, network_if: UAString) -> Self {
        UadpConfig {
            network_if: Some(network_if),
            url,
        }
    }

    pub fn network_if(&self) -> UAString {
        if let Some(s) = &self.network_if {
            s.clone()
        } else {
            UAString::null()
        }
    }

    pub fn url(&self) -> &UAString {
        &self.url
    }

    pub fn profile(self) -> PubSubTransportProfil {
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
    /// new UadpConfig
    /// url example: opc.udp://239.0.0.1:4840
    pub fn new(url: UAString) -> Self {
        MqttConfig { url }
    }
    pub fn url(&self) -> &UAString {
        &self.url
    }

    pub fn profile(self) -> PubSubTransportProfil {
        PubSubTransportProfil::MqttUadp
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
