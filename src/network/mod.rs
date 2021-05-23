// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2020 Alexander Schrode
pub(crate) mod mqtt;
pub(crate) mod uadp;

pub(crate) use self::uadp::UadpNetworkConnection;
pub(crate) use self::mqtt::MqttConnection;


use self::uadp::UadpNetworkReceiver;
use self::mqtt::MqttReceiever;
use std::io;
use opcua_types::status_code::StatusCode;

/// Abstraction of Connection layer
pub(crate) enum Connections{
    Uadp(UadpNetworkConnection),
    Mqtt(MqttConnection)
}

/// Abstraction of Receiver
pub enum ConnectionReceiver{
    Uadp(UadpNetworkReceiver),
    Mqtt(MqttReceiever)
}



impl Connections{
    /// sends a multicast message
    pub fn send(&self, b: &[u8]) -> io::Result<usize> {
        match self{
            Connections::Uadp(s) => s.send(b),
            Connections::Mqtt(s) => s.send(b)
        }
    }
    // creates a receiver for udp messages
    pub fn create_receiver(&self) -> std::io::Result<ConnectionReceiver> {
        Ok(match self{
            Connections::Uadp(s) => ConnectionReceiver::Uadp(s.create_receiver()?),
            Connections::Mqtt(s) => ConnectionReceiver::Mqtt(s.create_receiver())
        })
    }
}

impl ConnectionReceiver{
    pub fn receive_msg(&self) -> Result<Vec<u8>, StatusCode> {
        match self{
            ConnectionReceiver::Mqtt(s) => s.receive_msg(),
            ConnectionReceiver::Uadp(s) => s.receive_msg()
        }
    }
}

