// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode

use crate::address_space::PubSubDataSourceT;
use crate::callback::OnPubSubReceiveValues;
use crate::dataset::{DataSetInfo, Promoted, PublishedDataSet};
use crate::message::InformationType;
use crate::message::ResponseType;
use crate::message::UadpDataSetMetaDataResp;
use crate::message::UadpDataSetWriterResp;
use crate::message::UadpDiscoveryResponse;
use crate::message::UadpMessageChunkManager;
use crate::message::UadpNetworkMessage;
use crate::message::UadpPayload;
use crate::message::UadpPublisherEndpointsResp;
use crate::network::configuration::*;
use crate::network::{
    ConnectionReceiver, Connections, MqttConnection, TransportSettings, UadpNetworkConnection,
};
use crate::prelude::PubSubDataSource;
use crate::prelude::SimpleAddressSpace;
use crate::reader::ReaderGroup;
use crate::writer::DataSetWriter;
use crate::writer::WriterGroup;
use log::{error, warn};
use opcua_types::status_code::StatusCode;
use opcua_types::string::UAString;
use opcua_types::DataSetMetaDataType;
use opcua_types::DecodingOptions;
use opcua_types::EndpointDescription;
use opcua_types::ExtensionObject;
use opcua_types::ObjectId;
use opcua_types::PubSubConnectionDataType;
use opcua_types::WriterGroupDataType;
use opcua_types::{ConfigurationVersionDataType, DataValue, Variant};
use std::convert::TryFrom;
use std::io::Cursor;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, RwLock};
/// Id for a PubsubConnection
#[derive(Debug, PartialEq, Clone)]
pub struct PubSubConnectionId(pub u32);

/// Helps Building a Connection
/// @TODO add an builder example
#[allow(dead_code)]
pub struct PubSubConnectionBuilder {
    name: UAString,
    enabled: bool,
    publisher_id: Variant,
    value_recv: Option<Arc<Mutex<dyn OnPubSubReceiveValues + Send>>>,
    network_config: ConnectionConfig,
}

/// Implements the Connection.
#[allow(dead_code)]
pub struct PubSubConnection {
    name: UAString,
    network_config: ConnectionConfig,
    publisher_id: Variant,
    connection: Connections,
    writer: Vec<WriterGroup>,
    reader: Vec<ReaderGroup>,
    network_message_no: u16,
    data_source: Arc<RwLock<PubSubDataSourceT>>,
    value_recv: Option<Arc<Mutex<dyn OnPubSubReceiveValues + Send>>>,
    id: PubSubConnectionId,
    discovery_network_message_no: u16,
    discovery_dechunk: UadpMessageChunkManager,
    endpoints: Vec<EndpointDescription>,
}

pub struct PubSubReceiver {
    recv: ConnectionReceiver,
}

impl PubSubReceiver {
    /// Receive a UadpNetworkMessage
    pub fn receive_msg(&self) -> Result<(String, UadpNetworkMessage), StatusCode> {
        let (topic, data) = self.recv.receive_msg()?;
        let mut stream = Cursor::new(&data);
        let decoding_options = DecodingOptions::default();

        let msg = UadpNetworkMessage::decode(&mut stream, &decoding_options)?;
        Ok((topic, msg))
    }

    /*pub fn run(&self, pubsub: Arc<RwLock<PubSubConnection>>) {
        loop {
            match self.receive_msg() {
                Ok((topic, msg)) => {
                    let mut ps = pubsub.write().unwrap();
                    ps.handle_message(&topic.into(), msg, &Vec::new());
                }
                Err(err) => {
                    warn!("UadpReceiver: error reading message {}!", err);
                }
            }
        }
    }*/

    pub fn recv_to_channel(&self, tx: Sender<ConnectionAction>, id: &PubSubConnectionId) {
        loop {
            match self.receive_msg() {
                Ok((topic, msg)) => {
                    tx.send(ConnectionAction::GotUadp(id.clone(), topic, msg))
                        .unwrap();
                }
                Err(err) => {
                    warn!("UadpReceiver: error reading message {}!", err);
                }
            }
        }
    }
}

impl PubSubConnection {
    /// Creates new Pubsub Connection
    pub fn new(
        network_config: ConnectionConfig,
        publisher_id: Variant,
        data_source: Arc<RwLock<PubSubDataSourceT>>,
        value_recv: Option<Arc<Mutex<dyn OnPubSubReceiveValues + Send>>>,
    ) -> Result<Self, StatusCode> {
        // Check if transport profile is supported
        let connection = match &network_config {
            ConnectionConfig::Uadp(cfg) => match UadpNetworkConnection::new(cfg) {
                Ok(con) => Connections::Uadp(con),
                Err(e) => {
                    error!("Creating UadpNetworkConnection: {:-?}", e);
                    return Err(StatusCode::BadCommunicationError);
                }
            },
            #[cfg(feature = "mqtt")]
            ConnectionConfig::Mqtt(cfg) => Connections::Mqtt(MqttConnection::new(cfg)?),
        };
        // Check if publisher_id is valid the specs only allow UIntegers and String as id!
        match publisher_id {
            Variant::String(_)
            | Variant::Byte(_)
            | Variant::UInt16(_)
            | Variant::UInt32(_)
            | Variant::UInt64(_) => {}
            _ => return Err(StatusCode::BadTypeMismatch),
        }

        Ok(PubSubConnection {
            name: "Con".into(),
            network_config,
            publisher_id,
            connection,
            writer: Vec::new(),
            reader: Vec::new(),
            network_message_no: 0,
            discovery_network_message_no: 0,
            data_source,
            value_recv,
            id: PubSubConnectionId(0),
            discovery_dechunk: UadpMessageChunkManager::new(0),
            endpoints: Vec::new(),
        })
    }
    /// add data value recv callback when values change
    pub fn set_data_value_recv(
        &mut self,
        cb: Option<Arc<Mutex<dyn OnPubSubReceiveValues + Send>>>,
    ) {
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
        match self.connection.send(&c, &TransportSettings::None) {
            Ok(_) => Ok(()),
            Err(err) => {
                error!("Uadp error sending message - {:?}", err);
                Err(StatusCode::BadCommunicationError)
            }
        }
    }

    pub fn add_writer_group(&mut self, group: WriterGroup) {
        self.writer.push(group);
    }

    pub fn add_reader_group(&mut self, group: ReaderGroup) {
        self.reader.push(group);
    }

    pub fn handle_message(&mut self, topic: &UAString, msg: UadpNetworkMessage) {
        let msg = if msg.is_chunk() {
            // Dataset Id = 0 => Discovery Request or Response Chunk
            if *msg.dataset_payload.first().unwrap_or(&10_u16) == 0 {
                self.discovery_dechunk.add_chunk(&msg)
            } else {
                Some(msg)
            }
        } else {
            Some(msg)
        };
        if let Some(msg) = msg {
            match msg.payload {
                UadpPayload::Chunk(_) | UadpPayload::DataSets(_) => {
                    for rg in self.reader.iter_mut() {
                        rg.handle_message(&topic, &msg, &self.data_source, &self.value_recv);
                    }
                }
                UadpPayload::DiscoveryRequest(req) => {
                    //@FIXME the specs say that discovery responses should be delayed 100-500 ms
                    // and grouped into one if multiple arrive in this time frame. This is to limitation traffic
                    match req.information_type() {
                        InformationType::PublisherEndpoints => {
                            self.send_discovery_endpoint(Some(self.endpoints.clone()));
                        }
                        InformationType::DataSetMetaData => {
                            self.send_dataset_writer_cfg_response();
                        }
                        InformationType::DataSetWriter => {
                            if let Some(_writer_ids) = req.dataset_writer_ids() {
                                // @TODO get published dataset to writer
                                //self.send_metadata_for_datasets(writer_ids);
                            }
                        }
                    }
                }
                UadpPayload::DiscoveryResponse(_) => {}
                UadpPayload::None => {}
            }
        }
    }
    /// Enable DatasetReader
    pub fn enable(&self) {
        for r in self.reader.iter() {
            for cfg in r.get_transport_cfg().iter() {
                if let Err(err) = self.connection.subscribe(cfg) {
                    warn!("Error activating broker config: {}", err);
                }
            }
        }
    }
    /// Check if the connection is valid and can be used
    pub fn is_valid(&self) -> Result<(), StatusCode> {
        //@TODO check object
        Ok(())
    }

    /// Get the id of the connection
    pub fn id(&self) -> &PubSubConnectionId {
        &self.id
    }

    // @Hack is this rly need or change the construction?
    pub(crate) fn set_id(&mut self, id: PubSubConnectionId) {
        self.id = id;
    }

    /// Disable DatasetReader
    pub fn disable(&self) {
        for r in self.reader.iter() {
            for cfg in r.get_transport_cfg().iter() {
                if let Err(err) = self.connection.unsubscribe(cfg) {
                    warn!("Error deactivate broker config {}", err);
                }
            }
        }
    }

    /// Runs all writer, that should run and returns the next call to pull
    pub fn drive_writer(&mut self, datasets: &[PublishedDataSet]) -> std::time::Duration {
        let mut msgs = Vec::new();
        let mut net_offset = 0;
        let inf = PubSubDataSetInfo {
            data_source: &self.data_source,
            datasets,
        };
        for w in &mut self.writer {
            if w.tick() {
                for msg in w.generate_messages(
                    self.network_message_no + net_offset,
                    &self.publisher_id,
                    &inf,
                ) {
                    msgs.push((msg, w.writer_group_id, w.transport_settings()));
                    net_offset += 1;
                }
            }
        }
        self.network_message_no = self.network_message_no.wrapping_add(net_offset);
        for (msg, id, transport_settings) in msgs.iter() {
            let mut c = Vec::new();
            match msg.encode(&mut c) {
                Ok(_) => {
                    if let Err(err) = self.connection.send(&c, transport_settings) {
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

    /// Sends all endpoints via pubsub only supported via UADP over ethernet or udp
    pub fn send_discovery_endpoint(&mut self, endps: Option<Vec<EndpointDescription>>) {
        let mut msg = UadpNetworkMessage::new();
        msg.header.publisher_id = Some(self.publisher_id.clone());
        let status = if endps.is_some() {
            StatusCode::Good
        } else {
            StatusCode::BadNotImplemented
        };
        let response = UadpPublisherEndpointsResp::new(endps.clone(), status);
        msg.payload = UadpPayload::DiscoveryResponse(Box::new(UadpDiscoveryResponse::new(
            InformationType::PublisherEndpoints,
            self.discovery_network_message_no,
            ResponseType::PublisherEndpoints(response),
        )));
        self.discovery_network_message_no = self.discovery_network_message_no.wrapping_add(1);
        let mut c = Vec::new();
        match msg.encode(&mut c) {
            Ok(_) => {
                if let Err(err) = self.connection.send(&c, &TransportSettings::None) {
                    error!("Uadp error sending discovery message - {:?}", err);
                }
            }
            Err(err) => {
                error!(
                    "Uadp error decoding message DiscoveryResponse get endpoints - {}",
                    err
                );
            }
        }
    }
    /// Sends all Meta Data for datasets in list if found else send StatusCode bad
    pub fn send_metadata_for_datasets(&mut self, datasets: &[u16], dss: Vec<&PublishedDataSet>) {
        let mut offset = 0;
        for w in self.writer.iter() {
            offset = datasets
                .iter()
                .map(|id| (id, w.get_dataset_writer(*id)))
                .fold(offset, |i, (id, d)| {
                    self.send_metadata(*id, d, &dss, i);
                    i + 1
                });
        }
        self.update_discovery_network_no(u16::try_from(offset).unwrap_or(0));
    }

    /// Sends Meta Data for a dataset if ds_writer is empty send status code bad
    /// for example DatasetWriter id doesn't exist
    pub fn send_metadata(
        &self,
        ds_writer_id: u16,
        ds_writer: Option<&DataSetWriter>,
        dss: &Vec<&PublishedDataSet>,
        offset: u16,
    ) {
        let mut msg = UadpNetworkMessage::new();
        msg.header.publisher_id = Some(self.publisher_id.clone());
        let mut status = if ds_writer.is_some() {
            StatusCode::Good
        } else {
            StatusCode::BadNotImplemented
        };
        let meta_data = DataSetMetaDataType {
            namespaces: None,
            structure_data_types: None,
            enum_data_types: None,
            simple_data_types: None,
            name: UAString::null(),
            description: "".into(),
            fields: None,
            data_set_class_id: opcua_types::Guid::null(),
            configuration_version: ConfigurationVersionDataType {
                major_version: 0,
                minor_version: 0,
            },
        };
        let (meta_data, transport) = if let Some(dsw) = ds_writer {
            let ds_name = &dsw.dataset_name;
            if let Some(ds) = dss.iter().find(|d| &d.name == ds_name) {
                (ds.generate_meta_data(), dsw.transport_settings())
            } else {
                status = StatusCode::BadNoData;
                (meta_data, &TransportSettings::None)
            }
        } else {
            (meta_data, &TransportSettings::None)
        };
        let response = UadpDataSetMetaDataResp::new(ds_writer_id, meta_data, status);
        msg.payload = UadpPayload::DiscoveryResponse(Box::new(UadpDiscoveryResponse::new(
            InformationType::DataSetMetaData,
            self.discovery_network_message_no + offset,
            ResponseType::DataSetMetaData(response),
        )));
        let mut c = Vec::new();
        match msg.encode(&mut c) {
            Ok(_) => {
                if let Err(err) = self.connection.send(&c, transport) {
                    error!("Uadp error sending discovery message - {:?}", err);
                }
            }
            Err(err) => {
                error!(
                    "Uadp error decoding message DiscoveryResponse get endpoints - {}",
                    err
                );
            }
        }
    }

    fn update_discovery_network_no(&mut self, val: u16) {
        self.discovery_network_message_no = self.discovery_network_message_no.wrapping_add(val);
    }

    /// Create the message for writer cfg, the offset is to workaround borrowing as mut
    fn send_ds_writer_cfg_internal(
        &self,
        cfg: WriterGroupDataType,
        writer: Vec<u16>,
        transport_settings: &TransportSettings,
        seq_offset: u16,
    ) {
        let mut msg = UadpNetworkMessage::new();
        msg.header.publisher_id = Some(self.publisher_id.clone());
        let status = vec![StatusCode::Good; writer.len()];
        let response = UadpDataSetWriterResp::new(Some(writer), cfg, Some(status));
        msg.payload = UadpPayload::DiscoveryResponse(Box::new(UadpDiscoveryResponse::new(
            InformationType::DataSetWriter,
            self.discovery_network_message_no.wrapping_add(seq_offset),
            ResponseType::DataSetWriter(response),
        )));
        let mut c = Vec::new();
        match msg.encode(&mut c) {
            Ok(_) => {
                if let Err(err) = self.connection.send(&c, transport_settings) {
                    error!("Uadp error sending discovery message - {:?}", err);
                }
            }
            Err(err) => {
                error!(
                    "Uadp error decoding message DiscoveryRespons get endpoints - {}",
                    err
                );
            }
        }
    }

    fn send_dataset_writer_cfg_response(&mut self) {
        let off = self
            .writer
            .iter()
            .enumerate()
            .fold(0, |_, (offset, writer_g)| {
                let (cfg, writer) = writer_g.generate_info();
                self.send_ds_writer_cfg_internal(
                    cfg,
                    writer,
                    writer_g.transport_settings(),
                    u16::try_from(offset).unwrap_or(0),
                );
                offset
            });
        self.update_discovery_network_no(u16::try_from(off).unwrap_or(0));
    }

    /// Sends WriterGroup Data
    pub fn send_dataset_writer_cfg(&mut self, writer_g: &WriterGroup) {
        let (cfg, writer) = writer_g.generate_info();
        self.send_ds_writer_cfg_internal(cfg, writer, writer_g.transport_settings(), 0);
        self.update_discovery_network_no(1);
    }

    fn internal_update(&mut self, cfg: &PubSubConnectionDataType) -> Result<(), StatusCode> {
        self.name = cfg.name.clone();
        if let Some(rgs) = &cfg.reader_groups {
            for rg in rgs.iter() {
                if let Some(r) = self.reader.iter_mut().find(|r| r.name == rg.name) {
                    r.update(rg)?;
                } else {
                    self.add_reader_group(ReaderGroup::from_cfg(rg)?);
                }
            }
        }
        if let Some(wgs) = &cfg.writer_groups {
            for wg in wgs.iter() {
                if let Some(w) = self
                    .writer
                    .iter_mut()
                    .find(|w| w.writer_group_id == wg.writer_group_id)
                {
                    w.update(wg)?;
                } else {
                    self.add_writer_group(WriterGroup::from_cfg(wg)?);
                }
            }
        }
        Ok(())
    }
    /// Updates the connection from PubSubConnectionDataType
    pub fn update(&mut self, cfg: &PubSubConnectionDataType) -> Result<(), StatusCode> {
        // Update connection
        //@TODO update the connection
        //let con_s = ConnectionConfig::from_cfg(cfg);
        self.internal_update(cfg)?;
        Ok(())
    }

    pub fn from_cfg(
        cfg: &PubSubConnectionDataType,
        ds: Option<Arc<RwLock<dyn PubSubDataSource + Sync + Send>>>,
    ) -> Result<Self, StatusCode> {
        let data_source = match ds {
            Some(ds) => ds,
            None => SimpleAddressSpace::new_arc_lock(),
        };
        let net_cfg = ConnectionConfig::from_cfg(cfg)?;
        let mut s = PubSubConnection::new(net_cfg, cfg.publisher_id.clone(), data_source, None)?;
        s.internal_update(cfg)?;
        Ok(s)
    }

    pub fn generate_cfg(&self) -> Result<PubSubConnectionDataType, StatusCode> {
        Ok(PubSubConnectionDataType {
            name: self.name.clone(),
            enabled: false,
            publisher_id: self.publisher_id.clone(),
            transport_profile_uri: self.network_config.get_transport_profile().to_string(),
            address: ExtensionObject::from_encodable(
                ObjectId::NetworkAddressDataType_Encoding_DefaultBinary,
                &self.network_config.get_address(),
            ),
            connection_properties: Some(self.network_config.get_connection_properties()),
            transport_settings: opcua_types::ExtensionObject::null(),
            writer_groups: Some(self.writer.iter().map(|w| w.generate_info().0).collect()),
            reader_groups: Some(self.reader.iter().map(|r| r.generate_info()).collect()),
        })
    }

    /// Get a reference to the pub sub connection's publisher id.
    pub fn publisher_id(&self) -> &Variant {
        &self.publisher_id
    }

    /// Set the pub sub connection's name.
    pub fn set_name(&mut self, name: UAString) {
        self.name = name;
    }

    /// Get a reference to the pub sub connection's name.
    pub fn name(&self) -> &UAString {
        &self.name
    }
}

struct PubSubDataSetInfo<'a> {
    data_source: &'a Arc<RwLock<PubSubDataSourceT>>,
    datasets: &'a [PublishedDataSet],
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
            ds.config_version()
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
            name: "UADP Connection 1".into(),
            enabled: true,
            publisher_id: 12345_u16.into(),
            value_recv: None,
            network_config: ConnectionConfig::Uadp(UadpConfig::new("224.0.0.22:4800".into())),
        }
    }

    pub fn name(&mut self, name: UAString) -> &mut Self {
        self.name = name;
        self
    }

    pub fn network_config(&mut self, cfg: ConnectionConfig) -> &mut Self {
        self.network_config = cfg;
        self
    }

    pub fn uadp(&mut self, cfg: UadpConfig) -> &mut Self {
        self.network_config = ConnectionConfig::Uadp(cfg);
        self
    }

    pub fn mqtt(&mut self, cfg: MqttConfig) -> &mut Self {
        self.network_config = ConnectionConfig::Mqtt(cfg);
        self
    }

    pub fn enabled(&mut self, en: bool) -> &mut Self {
        self.enabled = en;
        self
    }

    pub fn publisher_id(&mut self, var: Variant) -> &mut Self {
        self.publisher_id = var;
        self
    }

    pub fn add_value_receiver<T: OnPubSubReceiveValues + Send + 'static>(&mut self, value_recv: T) {
        self.value_recv = Some(Arc::new(Mutex::new(value_recv)));
    }

    pub fn build(
        &self,
        data_source: Arc<RwLock<PubSubDataSourceT>>,
    ) -> Result<PubSubConnection, StatusCode> {
        PubSubConnection::new(
            self.network_config.clone(),
            self.publisher_id.clone(),
            data_source,
            self.value_recv.clone(),
        )
    }
}

impl Default for PubSubConnectionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub enum ConnectionAction {
    GotUadp(PubSubConnectionId, String, UadpNetworkMessage),
    DoLoop(PubSubConnectionId),
}
