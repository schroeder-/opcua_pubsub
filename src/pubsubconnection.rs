// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2020 Alexander Schrode

use opcua_types::string::UAString;
use opcua_types::Variant;
use opcua_types::status_code::StatusCode;
use opcua_types::DecodingLimits;
use crate::network::{UadpNetworkConnection, UadpNetworkReciver};
use crate::pubsubmessage::UadpNetworkMessage;
use opcua_core::comms::url::server_url_from_endpoint_url;
use log::{error};
use std::io::Cursor;

pub struct PubSubConnectionBuilder{
    name: UAString,
    url: UAString,
    enabled: bool,
    publisher_id: Variant,
}


pub enum PubSubTransportProfil {
    UdpUadp, // http://opcfoundation.org/UA-Profile/Transport/pubsub-udp-uadp
}


#[allow(dead_code)]
pub struct PubSubConnection{
    profile: PubSubTransportProfil,
    url: String,
    publisher_id: Variant,
    connection: UadpNetworkConnection
}

pub struct PubSubReciver{
    recv: UadpNetworkReciver,
}

impl PubSubReciver{
    /// Recive a UadpNetworkMessage
    pub fn recive_msg(&self) -> Result<UadpNetworkMessage, StatusCode>{
        let data = self.recv.recive_msg()?;

        let mut stream = Cursor::new(&data);
        let decoding_options = DecodingLimits::default();
    
        let msg = UadpNetworkMessage::decode(&mut stream, &decoding_options)?;
        Ok(msg)
    }
}

impl PubSubConnection {
    /// accepts
    pub fn new(url: String, publisher_id: Variant,) -> Result<Self, StatusCode> {
        //@TODO check for correct scheme!! (opc.udp://xxxx)
        //      currently every scheme is changed to udpuadp
        let url = match server_url_from_endpoint_url(&url){
            Ok(u) => u,
            Err(_) => return Err(StatusCode::BadServerUriInvalid)
        };
        let profile = PubSubTransportProfil::UdpUadp;
        // Check if publisher_id is valid the specs only allow UIntegers and String as id!
        match publisher_id {
            Variant::String(_)
            | Variant::Byte(_)
            | Variant::UInt16(_)
            | Variant::UInt32(_)
            | Variant::UInt64(_) => {}
            _ => return Err(StatusCode::BadTypeMismatch),
        }
        let connection = match UadpNetworkConnection::new(&url) {
            Ok(con) => con,
            Err(e) => {
                error!("Creating UadpNetworkconnection: {:-?}", e);
                return Err(StatusCode::BadCommunicationError)
            }
        };
        return Ok(PubSubConnection {
            profile,
            url,
            publisher_id,
            connection
        });
    }
    /// Create a new UadpReciver 
    pub fn create_reciver(&self) -> Result<PubSubReciver, StatusCode>{
        let recv = match self.connection.create_reciver() {
            Ok(r) => r,
            Err(_) => return Err(StatusCode::BadCommunicationError),
        };
        Ok(PubSubReciver{recv})
    }
    /// Send a UadpMessage 
    pub fn send(self: &Self, msg: &mut UadpNetworkMessage) -> Result<(), StatusCode>{
        let mut c = Vec::new();
        msg.header.publisher_id = Some(self.publisher_id.clone());
        msg.encode(&mut c)?;
        match self.connection.send(&c){
            Ok(_) => Ok(()),
            Err(err) => {
                error!("Uadp error sending message - {:?}", err);
                Err(StatusCode::BadCommunicationError)
            }
        }
    }
}

impl PubSubConnectionBuilder{
    pub fn new() -> Self{

        PubSubConnectionBuilder{ url: "".into(), name: "UADP Connection 1".into(), enabled: true, publisher_id: 12345_u16.into()}
    }

    pub fn set_name<'a>(&'a mut self, name: UAString) -> &'a mut Self{
        self.name = name;
        self
    }

    pub fn set_url<'a>(&'a mut self, url: UAString) -> &'a mut Self{
        self.url = url;
        self
    }

    pub fn set_enabled<'a>(&'a mut self, en: bool) -> &'a mut Self{
        self.enabled = en;
        self
    }

    pub fn set_publisher_id<'a>(&'a mut self, var: Variant) -> &'a mut Self{
        self.publisher_id = var;
        self
    }
    
    pub fn build(&self) -> Result<PubSubConnection, StatusCode>{
        Ok(PubSubConnection::new(self.url.to_string(), self.publisher_id.clone())?)
    }
}


