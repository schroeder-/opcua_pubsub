// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2020 Alexander Schrode

use opcua_types::string::UAString;
use opcua_types::status_code::StatusCode;
use opcua_types::{Variant, DecodingLimits, DataValue, ConfigurationVersionDataType, NodeId};
use crate::network::{UadpNetworkConnection, UadpNetworkReceiver};
use crate::message::UadpNetworkMessage;
use crate::writer::WriterGroupe;
use crate::pubdataset::{PublishedDataSet, DataSetInfo, Promoted};
use log::{error, warn};
use std::io::Cursor;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;

pub trait PubSubDataSource{
    fn get_pubsub_value(&self, nid: & NodeId) -> Result<DataValue, ()>;
}

type PubSubDataSourceT = dyn PubSubDataSource + Sync + Send;

pub struct SimpleAddressSpace{
    node_map: HashMap<NodeId, DataValue>
}

impl SimpleAddressSpace{
    pub fn new() -> Self{
        SimpleAddressSpace{ node_map: HashMap::new() }
    }

    pub fn set_value(& mut self, nid: &NodeId, dv: DataValue){
        self.node_map.insert(nid.clone(), dv);
    }

    pub fn remove_value(& mut self, nid: &NodeId){
        self.node_map.remove(nid);
    }

    pub fn new_arc_lock() -> Arc<RwLock<Self>>{
        Arc::new(RwLock::new(SimpleAddressSpace::new()))
    }
}

impl PubSubDataSource for SimpleAddressSpace{
    fn get_pubsub_value(&self, nid: & NodeId) -> Result<DataValue, ()>{
        match self.node_map.get(nid){
            Some(v) => Ok(v.clone()),
            None => Err(())
        }
    }
}

#[cfg(feature  = "server-integration")]
use opcua_server::address_space::AddressSpace;

#[cfg(feature = "server-integration")]
impl PubSubDataSource for AddressSpace{
    fn get_pubsub_value(&self, nid: &NodeId) -> Result<DataValue, ()>{
        self.get_variable_value(nid)
    }
}



#[allow(dead_code)]
pub struct PubSubConnectionBuilder{
    name: UAString,
    url: UAString,
    enabled: bool,
    publisher_id: Variant
}


pub enum PubSubTransportProfil {
    UdpUadp, // http://opcfoundation.org/UA-Profile/Transport/pubsub-udp-uadp
}


#[allow(dead_code)]
pub struct PubSubConnection{
    profile: PubSubTransportProfil,
    url: String,
    publisher_id: Variant,
    connection: UadpNetworkConnection,
    datasets: Vec<PublishedDataSet>,
    writer: Vec<WriterGroupe>,
    network_message_no: u16,
    data_source: Arc<RwLock<PubSubDataSourceT>>
}

pub struct PubSubReceiver{
    recv: UadpNetworkReceiver,
}

impl PubSubReceiver{
    /// Receive a UadpNetworkMessage
    pub fn receive_msg(&self) -> Result<UadpNetworkMessage, StatusCode>{
        let data = self.recv.receive_msg()?;

        let mut stream = Cursor::new(&data);
        let decoding_options = DecodingLimits::default();
    
        let msg = UadpNetworkMessage::decode(&mut stream, &decoding_options)?;
        Ok(msg)
    }
}



impl PubSubConnection {
    /// accepts
    pub fn new(url: String, publisher_id: Variant, data_source: Arc<RwLock<PubSubDataSourceT>>) -> Result<Self, StatusCode> {
        //@TODO check for correct scheme!! (opc.udp://xxxx)
        //      currently every scheme is changed to udpuadp
        //let url_udp = match (&url){
        //    Ok(u) => u,
        //    Err(_) => return Err(StatusCode::BadServerUriInvalid)
        //};
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
            connection,
            datasets: Vec::new(),
            writer: Vec::new(),
            network_message_no: 0,
            data_source
        });
    }
    /// Create a new UadpReceiver 
    pub fn create_receiver(&self) -> Result<PubSubReceiver, StatusCode>{
        let recv = match self.connection.create_receiver() {
            Ok(r) => r,
            Err(_) => return Err(StatusCode::BadCommunicationError),
        };
        Ok(PubSubReceiver{recv})
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
    pub fn add_dataset(&mut self, dataset: PublishedDataSet) {
        self.datasets.push(dataset);
    }

    pub fn add_writer_groupe(&mut self, group: WriterGroupe){
        self.writer.push(group);
    }

    pub fn poll(&mut self, _timedelta: u64){
        let mut msgs = Vec::with_capacity(self.writer.len());
        let mut net_offset = 0;
        let inf = PubSubDataSetInfo{data_source: &self.data_source, datasets: &self.datasets};
        for w in &mut self.writer{
            if w.tick(){
                if let Some(msg) = w.generate_message(self.network_message_no + net_offset, &self.publisher_id, &inf){
                    msgs.push((msg, w.writer_group_id));
                    net_offset += 1;
                }
            }
        }
        self.network_message_no += net_offset;
        for (msg, id) in msgs.iter(){
            let mut c = Vec::new();
            match msg.encode(&mut c){
                Ok(_) => {
                    if let Err(err) = self.connection.send(&c){
                    error!("Uadp error sending message - {:?}", err);
                    }
                },
                Err(err) => {
                    error!("Uadp error decoding message WriterGroupe {} - {}", id, err);
                }
            }
        }
            
    }
}

struct PubSubDataSetInfo<'a>{
    data_source: &'a Arc<RwLock<PubSubDataSourceT>>,
    datasets: &'a Vec<PublishedDataSet>   
}

impl<'a> DataSetInfo for PubSubDataSetInfo<'a>{
    fn collect_values(&self, name: &UAString) -> Vec::<(Promoted, DataValue)>{
        if let Some(ds) = self.datasets.iter().find(|x| &x.name == name){
            let guard = self.data_source.write().unwrap();
            let d_source = &(*guard);
            ds.get_data(d_source)
        } else {
            warn!("DataSet {} not found", name);
            Vec::new()
        }
    }
    fn get_config_version(&self, name: &UAString) -> ConfigurationVersionDataType{
        if let Some(ds) = self.datasets.iter().find(|x| &x.name == name){
            ds.get_config_version()
        } else {
            warn!("DataSet {} not found", name);
            ConfigurationVersionDataType { major_version: 0, minor_version: 0}
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
    
    pub fn build(&self, data_source: Arc<RwLock<PubSubDataSourceT>>) -> Result<PubSubConnection, StatusCode>{
        Ok(PubSubConnection::new(self.url.to_string(), self.publisher_id.clone(), data_source)?)
    }
}


