// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode

use crate::message::UadpNetworkMessage;
use crate::network::{UadpNetworkConnection, UadpNetworkReceiver};
use crate::pubdataset::{
    DataSetInfo, DataSetTarget, Promoted, PubSubFieldMetaData, PublishedDataSet,
};
use crate::reader::ReaderGroup;
use crate::writer::WriterGroup;
use log::{error, warn};
use opcua_types::status_code::StatusCode;
use opcua_types::string::UAString;
use opcua_types::{ConfigurationVersionDataType, DataValue, DecodingLimits, NodeId, Variant};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, RwLock, Mutex};
use crate::callback::{OnReceiveValueFn, OnPubSubReciveValues};

#[derive(Debug)]
pub struct DataSourceErr;

pub trait PubSubDataSource {
    fn get_pubsub_value(&self, nid: &NodeId) -> Result<DataValue, DataSourceErr>;
    fn set_pubsub_value(
        &mut self,
        target: &DataSetTarget,
        dv: DataValue,
        meta: &PubSubFieldMetaData,
    );
}

pub type PubSubDataSourceT = dyn PubSubDataSource + Sync + Send;

pub struct SimpleAddressSpace {
    node_map: HashMap<NodeId, DataValue>,
}

impl SimpleAddressSpace {
    pub fn new() -> Self {
        SimpleAddressSpace {
            node_map: HashMap::new(),
        }
    }

    pub fn set_value(&mut self, nid: &NodeId, dv: DataValue) {
        self.node_map.insert(nid.clone(), dv);
    }

    pub fn remove_value(&mut self, nid: &NodeId) {
        self.node_map.remove(nid);
    }

    pub fn new_arc_lock() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(SimpleAddressSpace::new()))
    }
}

impl Default for SimpleAddressSpace {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubDataSource for SimpleAddressSpace {
    fn get_pubsub_value(&self, nid: &NodeId) -> Result<DataValue, DataSourceErr> {
        match self.node_map.get(nid) {
            Some(v) => Ok(v.clone()),
            None => Err(DataSourceErr),
        }
    }

    fn set_pubsub_value(
        &mut self,
        target: &DataSetTarget,
        dv: DataValue,
        _meta: &PubSubFieldMetaData,
    ) {
        let nid = &target.0.target_node_id;
        match &target.update_dv(dv) {
            Ok(res) => {
                self.node_map.insert(nid.clone(), res.clone());
            }
            Err(err) => error!("Couldn't update variable {} -> {}", nid, err),
        };
    }
}

#[cfg(feature = "server-integration")]
use opcua_server::address_space::AddressSpace;

#[cfg(feature = "server-integration")]
impl PubSubDataSource for AddressSpace {
    fn get_pubsub_value(&self, nid: &NodeId) -> Result<DataValue, DataSourceErr> {
        self.get_variable_value(nid).or(Err(DataSourceErr))
    }

    fn set_pubsub_value(
        &mut self,
        target: &DataSetTarget,
        dv: DataValue,
        meta: &PubSubFieldMetaData,
    ) {
        let nid = &target.0.target_node_id;
        if let Some(v) = self.find_variable_mut(nid) {
            if let Err(err) = target.update_variable(dv, v) {
                error!("Couldn't update variable {} -> {}", nid, err);
            }
        } else {
            error!("Node {} not found -> {}", nid, meta.name());
        }
    }
}

#[allow(dead_code)]
pub struct PubSubConnectionBuilder {
    name: UAString,
    url: UAString,
    enabled: bool,
    publisher_id: Variant,
    value_recv: Option<Arc<Mutex<dyn OnPubSubReciveValues + Send>>>
}

pub enum PubSubTransportProfil {
    UdpUadp, // http://opcfoundation.org/UA-Profile/Transport/pubsub-udp-uadp
}

#[allow(dead_code)]
pub struct PubSubConnection {
    profile: PubSubTransportProfil,
    url: String,
    publisher_id: Variant,
    connection: UadpNetworkConnection,
    datasets: Vec<PublishedDataSet>,
    writer: Vec<WriterGroup>,
    reader: Vec<ReaderGroup>,
    network_message_no: u16,
    data_source: Arc<RwLock<PubSubDataSourceT>>,
    value_recv: Option<Arc<Mutex<dyn OnPubSubReciveValues + Send>>>
}

pub struct PubSubReceiver {
    recv: UadpNetworkReceiver,
}

impl PubSubReceiver {
    /// Receive a UadpNetworkMessage
    pub fn receive_msg(&self) -> Result<UadpNetworkMessage, StatusCode> {
        let data = self.recv.receive_msg()?;

        let mut stream = Cursor::new(&data);
        let decoding_options = DecodingLimits::default();

        let msg = UadpNetworkMessage::decode(&mut stream, &decoding_options)?;
        Ok(msg)
    }

    pub fn run(&self, pubsub: Arc<RwLock<PubSubConnection>>) {
        loop {
            match self.receive_msg() {
                Ok(msg) => {
                    let ps = pubsub.write().unwrap();
                    ps.handle_message(msg);
                }
                Err(err) => {
                    warn!("UadpReciver: error reading message {}!", err);
                }
            }
        }
    }
}

impl PubSubConnection {
    /// Creats new Pubsub Connection
    pub fn new(
        url: String,
        publisher_id: Variant,
        data_source: Arc<RwLock<PubSubDataSourceT>>,
        value_recv: Option<Arc<Mutex<dyn OnPubSubReciveValues + Send>>>
    ) -> Result<Self, StatusCode> {
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
                return Err(StatusCode::BadCommunicationError);
            }
        };
        Ok(PubSubConnection {
            profile,
            url,
            publisher_id,
            connection,
            datasets: Vec::new(),
            writer: Vec::new(),
            reader: Vec::new(),
            network_message_no: 0,
            data_source,
            value_recv: None
        })
    }
    /// add datavalue recv callback when values change
    pub fn set_datavalue_recv(&mut self, cb: Option<Arc<Mutex<dyn OnPubSubReciveValues + Send>>>){
        self.value_recv = cb;
    }

    /// Create a new UadpReceiver
    pub fn create_receiver(&self) -> Result<PubSubReceiver, StatusCode> {
        let recv = match self.connection.create_receiver() {
            Ok(r) => r,
            Err(_) => return Err(StatusCode::BadCommunicationError),
        };
        Ok(PubSubReceiver { recv })
    }
    /// Send a UadpMessage
    pub fn send(&self, msg: &mut UadpNetworkMessage) -> Result<(), StatusCode> {
        let mut c = Vec::new();
        msg.header.publisher_id = Some(self.publisher_id.clone());
        msg.encode(&mut c)?;
        match self.connection.send(&c) {
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

    pub fn add_writer_group(&mut self, group: WriterGroup) {
        self.writer.push(group);
    }

    pub fn add_reader_group(&mut self, group: ReaderGroup) {
        self.reader.push(group);
    }

    pub fn handle_message(&self, msg: UadpNetworkMessage) {
        for rg in self.reader.iter() {
            rg.handle_message(&msg, &self.data_source);
        }
    }
    /// runs pubs in a new thread
    /// currently 2 threads are used
    pub fn run_thread(
        pubsub: Arc<RwLock<Self>>,
    ) -> (std::thread::JoinHandle<()>, std::thread::JoinHandle<()>) {
        let pubsub_reader = pubsub.clone();
        let th1 = std::thread::spawn(move || {
            let receiver = {
                let ps = pubsub_reader.write().unwrap();
                ps.create_receiver().unwrap()
            };
            receiver.run(pubsub_reader);
        });
        let th2 = std::thread::spawn(move || loop {
            let delay = {
                let mut ps = pubsub.write().unwrap();
                ps.drive_writer()
            };
            std::thread::sleep(delay);
        });
        (th1, th2)
    }

    /// runs the pubsub forever
    pub fn run(self) {
        let s = Arc::new(RwLock::new(self));
        let (th1, th2) = Self::run_thread(s);
        th1.join().unwrap();
        th2.join().unwrap();
    }
    /// Runs all writer, that should run and returns the next call to pull
    pub fn drive_writer(&mut self) -> std::time::Duration {
        let mut msgs = Vec::with_capacity(self.writer.len());
        let mut net_offset = 0;
        let inf = PubSubDataSetInfo {
            data_source: &self.data_source,
            datasets: &self.datasets,
        };
        for w in &mut self.writer {
            if w.tick() {
                if let Some(msg) = w.generate_message(
                    self.network_message_no + net_offset,
                    &self.publisher_id,
                    &inf,
                ) {
                    msgs.push((msg, w.writer_group_id));
                    net_offset += 1;
                }
            }
        }
        self.network_message_no = self.network_message_no.wrapping_add(net_offset);
        for (msg, id) in msgs.iter() {
            let mut c = Vec::new();
            match msg.encode(&mut c) {
                Ok(_) => {
                    if let Err(err) = self.connection.send(&c) {
                        error!("Uadp error sending message - {:?}", err);
                    }
                }
                Err(err) => {
                    error!("Uadp error decoding message WriterGroup {} - {}", id, err);
                }
            }
        }
        let ret = std::time::Duration::from_millis(500);
        self.writer.iter().fold(ret, |a, w| a.min(w.next_tick()))
    }
}

struct PubSubDataSetInfo<'a> {
    data_source: &'a Arc<RwLock<PubSubDataSourceT>>,
    datasets: &'a Vec<PublishedDataSet>,
}

impl<'a> DataSetInfo for PubSubDataSetInfo<'a> {
    fn collect_values(&self, name: &UAString) -> Vec<(Promoted, DataValue)> {
        if let Some(ds) = self.datasets.iter().find(|x| &x.name == name) {
            let guard = self.data_source.write().unwrap();
            let d_source = &(*guard);
            ds.get_data(d_source)
        } else {
            warn!("DataSet {} not found", name);
            Vec::new()
        }
    }
    fn get_config_version(&self, name: &UAString) -> ConfigurationVersionDataType {
        if let Some(ds) = self.datasets.iter().find(|x| &x.name == name) {
            ds.get_config_version()
        } else {
            warn!("DataSet {} not found", name);
            ConfigurationVersionDataType {
                major_version: 0,
                minor_version: 0,
            }
        }
    }
}

impl PubSubConnectionBuilder {
    pub fn new() -> Self {
        PubSubConnectionBuilder {
            url: "".into(),
            name: "UADP Connection 1".into(),
            enabled: true,
            publisher_id: 12345_u16.into(),
            value_recv: None
        }
    }

    pub fn set_name(&mut self, name: UAString) -> &mut Self {
        self.name = name;
        self
    }

    pub fn set_url(&mut self, url: UAString) -> &mut Self {
        self.url = url;
        self
    }

    pub fn set_enabled(&mut self, en: bool) -> &mut Self {
        self.enabled = en;
        self
    }

    pub fn set_publisher_id(&mut self, var: Variant) -> &mut Self {
        self.publisher_id = var;
        self
    }

    pub fn add_value_receiver<T: OnPubSubReciveValues + Send + 'static>(&mut self, value_recv: T){
        self.value_recv = Some(Arc::new(Mutex::new(value_recv)));
    }

    pub fn build(
        &self,
        data_source: Arc<RwLock<PubSubDataSourceT>>,
    ) -> Result<PubSubConnection, StatusCode> {
        Ok(PubSubConnection::new(
            self.url.to_string(),
            self.publisher_id.clone(),
            data_source,
            self.value_recv.clone()
        )?)
    }
}

impl Default for PubSubConnectionBuilder {
    fn default() -> Self {
        Self::new()
    }
}
