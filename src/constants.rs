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

impl From<UAString> for PubSubTransportProfil {
    /// Get from a UAString containing an urn
    fn from(transport: UAString) -> Self {
        if let Some(str) = transport.value() {
            match str.as_str() {
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-udp-uadp" => {
                    PubSubTransportProfil::UdpUadp
                }
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-amqp-uadp" => {
                    PubSubTransportProfil::AmqpUadp
                }
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-amqp-json" => {
                    PubSubTransportProfil::AmqpJson
                }
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-mqtt-json" => {
                    PubSubTransportProfil::MqttJson
                }
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-mqtt-uadp" => {
                    PubSubTransportProfil::MqttUadp
                }
                "http://opcfoundation.org/UA-Profile/Transport/pubsub-eth-uadp" => {
                    PubSubTransportProfil::EthUadp
                }
                _ => PubSubTransportProfil::Unkown,
            }
        } else {
            PubSubTransportProfil::Unkown
        }
    }
}
