// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use opcua_types::UAString;

/// The transport profile to use for this PubSubConnections
pub enum PubSubTransportProfil {
    /// http://opcfoundation.org/UA-Profile/Transport/pubsub-udp-uadp
    UdpUadp,
    /// http://opcfoundation.org/UA-Profile/Transport/pubsub-mqtt-json
    MqttJson,
    /// http://opcfoundation.org/UA-Profile/Transport/pubsub-mqtt-uadp
    MqttUadp,
    /// http://opcfoundation.org/UA-Profile/Transport/pubsub-amqp-uadp
    AmqpUadp,
    /// http://opcfoundation.org/UA-Profile/Transport/pubsub-amqp-json
    AmqpJson,
    /// http://opcfoundation.org/UA-Profile/Transport/pubsub-eth-uadp
    EthUadp,
    /// Unkown
    Unkown,
}

impl PubSubTransportProfil {
    pub fn to_string(&self) -> UAString {
        match self {
            PubSubTransportProfil::UdpUadp => {
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-udp-uadp"
            }
            PubSubTransportProfil::AmqpUadp => {
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-amqp-uadp"
            }
            PubSubTransportProfil::AmqpJson => {
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-amqp-json"
            }
            PubSubTransportProfil::MqttJson => {
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-mqtt-json"
            }
            PubSubTransportProfil::MqttUadp => {
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-mqtt-uadp"
            }
            PubSubTransportProfil::EthUadp => {
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-eth-uadp"
            }
            _ => "",
        }
        .into()
    }
}

impl From<&UAString> for PubSubTransportProfil {
    /// Get from a UAString containing an urn
    fn from(transport: &UAString) -> Self {
        if let Some(str) = transport.value() {
            match str.as_str() {
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-udp-uadp" => Self::UdpUadp,
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-amqp-uadp" => Self::AmqpUadp,
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-amqp-json" => Self::AmqpJson,
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-mqtt-json" => Self::MqttJson,
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-mqtt-uadp" => Self::MqttUadp,
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-eth-uadp" => Self::EthUadp,
                _ => Self::Unkown,
            }
        } else {
            Self::Unkown
        }
    }
}
