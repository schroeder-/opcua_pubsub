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
    pub fn send(&self, b: &[u8], settings: &TransportSettings) -> io::Result<usize> {
        match self {
            Connections::Uadp(s) => s.send(b),
            Connections::Mqtt(s) => match settings {
                TransportSettings::BrokerWrite(_cfg) => s.publish(b, settings),
                TransportSettings::BrokerDataSetWrite(_cfg) => s.publish(b, settings),
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
    /// subscribe to data
    pub fn subscribe(&self, settings: &ReaderTransportSettings) -> Result<(), StatusCode> {
        match self {
            Connections::Uadp(_s) => Ok(()), // Not supported
            Connections::Mqtt(s) => {
                if let ReaderTransportSettings::BrokerDataSetReader(_cfg) = settings {
                    s.subscribe(settings)
                } else {
                    error!("mqtt need broker transport settings!");
                    Err(StatusCode::BadInvalidArgument)
                }
            }
        }
    }
    /// unsubscribe to data
    pub fn unsubscribe(&self, settings: &ReaderTransportSettings) -> Result<(), StatusCode> {
        match self {
            Connections::Uadp(_s) => Ok(()), // Not supported
            Connections::Mqtt(s) => {
                if let ReaderTransportSettings::BrokerDataSetReader(_cfg) = settings {
                    s.unsubscribe(settings)
                } else {
                    error!("mqtt need broker transport settings!");
                    Err(StatusCode::BadInvalidArgument)
                }
            }
        }
    }

    /// creates a receiver for udp messages
    pub fn create_receiver(&self) -> std::io::Result<ConnectionReceiver> {
        Ok(match self {
            Connections::Uadp(s) => ConnectionReceiver::Uadp(s.create_receiver()?),
            Connections::Mqtt(s) => ConnectionReceiver::Mqtt(s.create_receiver()),
        })
    }
}

impl ConnectionReceiver {
    pub fn receive_msg(&self) -> Result<(String, Vec<u8>), StatusCode> {
        match self {
            ConnectionReceiver::Mqtt(s) => s.receive_msg(),
            ConnectionReceiver::Uadp(s) => Ok(("UADP".to_string(), s.receive_msg()?)),
        }
    }
}
