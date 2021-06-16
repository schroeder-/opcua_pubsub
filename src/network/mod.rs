// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2020 Alexander Schrode
pub mod configuration;
pub mod mqtt;
pub mod uadp;

pub use self::mqtt::MqttConnection;
use self::mqtt::MqttReceiver;
pub use self::uadp::UadpNetworkConnection;
use self::uadp::UadpNetworkReceiver;
use log::error;
use opcua_types::status_code::StatusCode;
use opcua_types::BrokerWriterGroupTransportDataType;
use std::io;
/// Abstraction of Connection layer
pub enum Connections {
    Uadp(UadpNetworkConnection),
    Mqtt(MqttConnection),
}

/// Abstraction of Receiver
pub enum ConnectionReceiver {
    Uadp(UadpNetworkReceiver),
    Mqtt(MqttReceiver),
}

#[derive(Clone)]
pub enum TransportSettings {
    None,
    BrokerWrite(BrokerWriterGroupTransportDataType),
    BrokerDataSetWrite(opcua_types::BrokerDataSetWriterTransportDataType),
}

#[derive(Clone)]
pub enum ReaderTransportSettings {
    None,
    BrokerDataSetReader(opcua_types::BrokerDataSetReaderTransportDataType),
}

impl Connections {
    /// sends a multicast message
    pub async fn send(&self, b: &[u8], settings: &TransportSettings) -> io::Result<usize> {
        match self {
            Connections::Uadp(s) => s.send(b).await,
            Connections::Mqtt(s) => match settings {
                TransportSettings::BrokerWrite(_cfg) => s.publish(b, settings).await,
                TransportSettings::BrokerDataSetWrite(_cfg) => s.publish(b, settings).await,
                TransportSettings::None => {
                    error!("mqtt need broker transport settings!");
                    Err(std::io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "broker not configured",
                    ))
                }
            },
        }
    }
    /// Start the connection on the sender side
    pub async fn start(&mut self) -> Result<(), StatusCode> {
        match self {
            // Uadp atm has stateless so no connection building is needed
            Connections::Uadp(_) => Ok(()),
            Connections::Mqtt(s) => s.start().await,
        }
    }

    /// subscribe to data
    pub async fn subscribe(&self, settings: &ReaderTransportSettings) -> Result<(), StatusCode> {
        match self {
            Connections::Uadp(_s) => Ok(()), // Not supported
            Connections::Mqtt(s) => {
                if let ReaderTransportSettings::BrokerDataSetReader(_cfg) = settings {
                    s.subscribe(settings).await
                } else {
                    error!("mqtt need broker transport settings!");
                    Err(StatusCode::BadInvalidArgument)
                }
            }
        }
    }
    /// unsubscribe to data
    pub async fn unsubscribe(&self, settings: &ReaderTransportSettings) -> Result<(), StatusCode> {
        match self {
            Connections::Uadp(_s) => Ok(()), // Not supported
            Connections::Mqtt(s) => {
                if let ReaderTransportSettings::BrokerDataSetReader(_cfg) = settings {
                    s.unsubscribe(settings).await
                } else {
                    error!("mqtt need broker transport settings!");
                    Err(StatusCode::BadInvalidArgument)
                }
            }
        }
    }

    /// creates a receiver for udp messages
    pub fn create_receiver(&mut self) -> std::io::Result<ConnectionReceiver> {
        Ok(match self {
            Connections::Uadp(s) => ConnectionReceiver::Uadp(s.create_receiver()?),
            Connections::Mqtt(s) => ConnectionReceiver::Mqtt(s.create_receiver()),
        })
    }
}

impl ConnectionReceiver {
    pub async fn receive_msg(&mut self) -> Result<(String, Vec<u8>), StatusCode> {
        match self {
            ConnectionReceiver::Mqtt(s) => s.receive_msg().await,
            ConnectionReceiver::Uadp(s) => Ok(("UADP".to_string(), s.receive_msg().await?)),
        }
    }
}
