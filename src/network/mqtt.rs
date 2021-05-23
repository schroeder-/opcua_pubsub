// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2020 Alexander Schrode
use std::io;
use opcua_types::status_code::StatusCode;

pub struct MqttConnection{
}

pub struct MqttReceiever{
}

impl MqttReceiever{
    pub fn new() -> Self{
        MqttReceiever {}
    }

    pub fn receive_msg(&self) -> Result<Vec<u8>, StatusCode> {
        Err(StatusCode::BadNotImplemented)
    }
}

impl MqttConnection{
    pub fn send(&self, b: &[u8]) -> io::Result<usize> {
        Ok(0)
    }

    pub fn create_receiver(&self) -> MqttReceiever{
        MqttReceiever::new()
    }
}
