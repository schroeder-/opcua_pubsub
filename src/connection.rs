use crate::address_space::DataSource;
// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use crate::callback::OnPubSubReceiveValues;
use crate::dataset::{DataSetInfo, Promoted, PublishedDataSet};
use crate::discovery::DiscoveryHandler;
use crate::message::InformationType;
use crate::message::UadpDiscoveryRequest;
use crate::message::UadpMessageChunkManager;
use crate::message::UadpNetworkMessage;
use crate::message::UadpPayload;
use crate::network::{configuration::*, ReaderTransportSettings};
use crate::network::{Connections, MqttConnection, TransportSettings, UadpNetworkConnection};
use crate::prelude::PubSubDataSource;
use crate::prelude::SimpleAddressSpace;
use crate::reader::ReaderGroup;
use crate::writer::WriterGroup;
use log::{error, warn};
use opcua_types::status_code::StatusCode;
use opcua_types::string::UAString;
use opcua_types::DecodingOptions;
use opcua_types::EndpointDescription;
use opcua_types::ExtensionObject;
use opcua_types::ObjectId;
use opcua_types::PubSubConnectionDataType;
use opcua_types::{ConfigurationVersionDataType, DataValue, Variant};
use std::io::Cursor;
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::{mpsc, mpsc::Sender};
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
    rx: mpsc::Receiver<Result<(String, Vec<u8>), StatusCode>>,
    tx: mpsc::Sender<ConnectionMessage>,
    handle: tokio::task::JoinHandle<()>,
    writer: Vec<WriterGroup>,
    reader: Vec<ReaderGroup>,
    network_message_no: u16,
    data_source: Arc<RwLock<PubSubDataSource>>,
    value_recv: Option<Arc<Mutex<dyn OnPubSubReceiveValues + Send>>>,
    id: PubSubConnectionId,
    discovery_dechunk: UadpMessageChunkManager,
    endpoints: Vec<EndpointDescription>,
    discovery: DiscoveryHandler,
}

pub struct PubSubReceiver<'a> {
    con: &'a mut PubSubConnection,
}

impl<'a> PubSubReceiver<'a> {
    /// Receive a UadpNetworkMessage
    pub async fn receive_msg(&mut self) -> Result<(String, UadpNetworkMessage), StatusCode> {
        self.con.recv().await
    }

    pub async fn recv_to_channel(&mut self, tx: Sender<ConnectionAction>, id: &PubSubConnectionId) {
        loop {
            match self.receive_msg().await {
                Ok((topic, msg)) => {
                    if let Err(e) = tx
                        .send(ConnectionAction::GotUadp(id.clone(), topic, msg))
                        .await
                    {
                        warn!("Send Error {}", e);
                    }
                }
                Err(err) => {
                    warn!("UadpReceiver: error reading message {}!", err);
                }
            }
        }
    }
}

enum ConnectionMessage {
    Start,
    Shutdown,
    SendMsg(Vec<u8>, TransportSettings), // @TODO dont send transport settings with every call
    Subscribe(ReaderTransportSettings),
    Unsubscribe(ReaderTransportSettings),
    Stop,
}

type ConRecv = tokio::sync::mpsc::Receiver<Result<(String, Vec<u8>), StatusCode>>;
type ConSender = tokio::sync::mpsc::Sender<ConnectionMessage>;

impl PubSubConnection {
    /// Creates new Pubsub Connection
    pub fn new(
        network_config: ConnectionConfig,
        publisher_id: Variant,
        data_source: Arc<RwLock<PubSubDataSource>>,
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
        let (rx, tx, handle) = Self::start_connection(connection);
        Ok(Self {
            name: "Con".into(),
            network_config,
            publisher_id,
            rx,
            tx,
            handle,
            writer: Vec::new(),
            reader: Vec::new(),
            network_message_no: 0,
            data_source,
            value_recv,
            id: PubSubConnectionId(0),
            discovery_dechunk: UadpMessageChunkManager::new(0),
            endpoints: Vec::new(),
            discovery: DiscoveryHandler::new(),
        })
    }

    fn start_connection(con: Connections) -> (ConRecv, ConSender, tokio::task::JoinHandle<()>) {
        let (isend, recv) = tokio::sync::mpsc::channel::<Result<(String, Vec<u8>), StatusCode>>(10);
        let (send, mut irecv) = mpsc::channel::<ConnectionMessage>(10);
        let handle = tokio::spawn(async move  {
            let con = con;
            let mut recv = con.create_receiver().expect("Error creating receiver");
            loop {
                //@TODO start read after start and stop then
                tokio::select! {
                    val = irecv.recv() => {
                        match val.unwrap(){
                            ConnectionMessage::Start=>{
                                con.start().await.expect("Start error");
                            },
                            ConnectionMessage::Shutdown =>{
                                break;
                            },
                            ConnectionMessage::SendMsg(msg, transport) =>{
                                con.send(&msg, &transport).await.expect("Send error");
                            },
                            ConnectionMessage::Subscribe(settings) => {
                                con.subscribe(&settings).await.expect("Subscribe error");
                            },
                            ConnectionMessage::Unsubscribe(settings) => {
                                con.unsubscribe(&settings).await.expect("Subscribe error");
                            },
                            ConnectionMessage::Stop => {
                                //@TODO stop recv
                            }
                        }
                    }
                    val = recv.receive_msg() => {
                        isend.send(val).await.expect("Recv connection data error");
                    }
                }
            }
        });
        (recv, send, handle)
    }

    /// add data value recv callback when values change
    pub fn set_data_value_recv(
        &mut self,
        cb: Option<Arc<Mutex<dyn OnPubSubReceiveValues + Send>>>,
    ) {
        self.value_recv = cb;
    }

    /// Create a new UadpReceiver
    pub fn create_receiver<'a>(&'a mut self) -> Result<PubSubReceiver, StatusCode> {
        Ok(PubSubReceiver { con: self })
    }
    /// Send a UadpMessage
    pub async fn send(&self, msg: &mut UadpNetworkMessage) -> Result<(), StatusCode> {
        let mut c = Vec::new();
        msg.header.publisher_id = Some(self.publisher_id.clone());
        msg.encode(&mut c)?;
        match self
            .tx
            .send(ConnectionMessage::SendMsg(c, TransportSettings::None))
            .await
        {
            Ok(_) => Ok(()),
            Err(_) => Err(StatusCode::BadCommunicationError),
        }
    }

    pub async fn recv(&mut self) -> Result<(String, UadpNetworkMessage), StatusCode> {
        let (topic, data) = self.rx.recv().await.unwrap()?;
        let mut stream = Cursor::new(&data);
        let decoding_options = DecodingOptions::default();

        let msg = UadpNetworkMessage::decode(&mut stream, &decoding_options)?;
        Ok((topic, msg))
    }


    pub async fn recv_timeout(&mut self, timeout: std::time::Duration) -> Result<(String, UadpNetworkMessage), StatusCode> {
        let tout = tokio::time::sleep(timeout);
        tokio::pin!(tout);
        tokio::select! {
            res = self.rx.recv() => {
                match res.unwrap(){
                    Ok((topic, data)) => {
                        let mut stream = Cursor::new(&data);
                        let decoding_options = DecodingOptions::default();
                
                        let msg = UadpNetworkMessage::decode(&mut stream, &decoding_options)?;
                        Ok((topic, msg))
                    },
                    Err(e) => Err(e),
                }
            }
            _ = &mut tout => {
                Err(StatusCode::BadTimeout)
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
                        rg.handle_message(topic, &msg, &self.data_source, &self.value_recv);
                    }
                }
                UadpPayload::DiscoveryRequest(req) => {
                    self.discovery.handle_request(&req);
                }
                UadpPayload::DiscoveryResponse(_) => {}
                UadpPayload::None => {}
            }
        }
    }
    /// Enable DatasetReader
    pub async fn enable(&mut self) -> Result<(), StatusCode> {
        if let Err(_) = self.tx.send(ConnectionMessage::Start).await {
            return Err(StatusCode::BadCommunicationError);
        }
        for r in self.reader.iter() {
            for cfg in r.get_transport_cfg().iter() {
                if let Err(err) = self
                    .tx
                    .send(ConnectionMessage::Subscribe((*cfg).clone()))
                    .await
                {
                    warn!("Error activating broker config: {}", err);
                    return Err(StatusCode::BadCommunicationError);
                }
            }
        }
        Ok(())
    }
    /// Check if the connection is valid and can be used
    pub const fn is_valid(&self) -> Result<(), StatusCode> {
        //@TODO check object
        Ok(())
    }

    /// Get the id of the connection
    pub const fn id(&self) -> &PubSubConnectionId {
        &self.id
    }

    // @Hack is this rly need or change the construction?
    pub(crate) fn set_id(&mut self, id: PubSubConnectionId) {
        self.id = id;
    }

    /// Disable DatasetReader
    pub async fn disable(&self) {
        if let Err(e) = self.tx.send(ConnectionMessage::Stop).await {
            warn!("Error deactivate connection {}", e);
        }
        for r in self.reader.iter() {
            for cfg in r.get_transport_cfg().iter() {
                if let Err(err) = self
                    .tx
                    .send(ConnectionMessage::Unsubscribe((*cfg).clone()))
                    .await
                {
                    warn!("Error deactivate broker config {}", err);
                }
            }
        }
    }

    /// Runs all writer, that should run and returns the next call to pull
    pub async fn drive_writer(&mut self, datasets: &[PublishedDataSet]) -> std::time::Duration {
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
                    if let Err(err) = self
                        .tx
                        .send(ConnectionMessage::SendMsg(c, (*transport_settings).clone()))
                        .await
                    {
                        error!("Uadp error sending message - {}", err);
                    }
                }
                Err(err) => {
                    error!("Uadp error decoding message WriterGroup {} - {}", id, err);
                }
            }
        }
        if self.discovery.has_endpoint_response() {
            self.send_pending_discovery_endpoint(Some(self.endpoints.clone()))
                .await;
        }
        if self.discovery.has_meta_response() {
            self.send_pending_metadata(datasets).await;
        }
        if self.discovery.has_writer_response() {
            self.send_pending_writer_response().await;
        }
        // Check next call
        let next = std::time::Duration::from_millis(500);
        let next = self.writer.iter().fold(next, |a, w| a.min(w.next_tick()));
        self.discovery.get_next_time(next)
    }

    /// Sends all endpoints via pubsub only supported via UADP over ethernet or udp
    /// This is async
    pub fn send_discovery_endpoint(&mut self) {
        self.discovery.handle_request(&UadpDiscoveryRequest::new(
            InformationType::PublisherEndpoints,
            None,
        ))
    }

    /// Sends all pending endpoint responses
    async fn send_pending_discovery_endpoint(&mut self, endps: Option<Vec<EndpointDescription>>) {
        let msg = self
            .discovery
            .generate_endpoint_response(self.publisher_id.clone(), endps);
        let mut c = Vec::new();
        match msg.encode(&mut c) {
            Ok(_) => {
                if let Err(err) = self
                    .tx
                    .send(ConnectionMessage::SendMsg(c, TransportSettings::None))
                    .await
                {
                    error!("Uadp error sending discovery message - {}", err);
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

    //@TODO remove datasets
    pub async fn run_loop(self, datasets: Arc<tokio::sync::Mutex<Vec<PublishedDataSet>>>) {
        let s = Arc::new(tokio::sync::Mutex::new(self));
        let o = s.clone();
        tokio::task::spawn(async move {
            let mut o = o.lock().await;
            loop {
                match o.recv().await {
                    Ok((topic, msg)) => o.handle_message(&topic.into(), msg),
                    Err(err) => {
                        error!("Message: {:?}", err);
                    }
                }
            }
        });
        loop {
            let dur = {
                let ds = datasets.lock().await;
                let mut s = s.lock().await;
                s.drive_writer(&ds).await
            };
            tokio::time::sleep(dur).await;
        }
    }

    /// Sends pending metadata
    async fn send_pending_metadata(&mut self, datasets: &[PublishedDataSet]) {
        let writer: Vec<_> = self
            .writer
            .iter()
            .map(|w| w.writer())
            .fold(Vec::new(), |v, w| [v, w].concat());
        let msgs =
            self.discovery
                .generate_meta_responses(self.publisher_id.clone(), writer, datasets);
        for (msg, transport) in msgs {
            let mut c = Vec::new();
            match msg.encode(&mut c) {
                Ok(_) => {
                    if let Err(err) = self
                        .tx
                        .send(ConnectionMessage::SendMsg(c, transport.clone()))
                        .await
                    {
                        error!("Uadp error sending discovery message - {}", err);
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
    }
    /// Sends all Meta Data for datasets in list if found else send StatusCode bad
    /// This is async
    pub fn send_metadata_for_datasets(&mut self, datasets: Option<Vec<u16>>) {
        self.discovery.handle_request(&UadpDiscoveryRequest::new(
            InformationType::DataSetMetaData,
            datasets,
        ))
    }

    async fn send_pending_writer_response(&mut self) {
        let msgs = self
            .discovery
            .generate_writer_response(&self.publisher_id, self.writer.iter().collect());
        for (msg, transport) in msgs {
            let mut c = Vec::new();
            match msg.encode(&mut c) {
                Ok(_) => {
                    if let Err(err) = self
                        .tx
                        .send(ConnectionMessage::SendMsg(c, transport.clone()))
                        .await
                    {
                        error!("Uadp error sending discovery message - {}", err);
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
    }

    /// Sends Write Config
    pub fn send_dataset_writer_cfg(&mut self) {
        //@FixMe atm all WriterGroup Configs are send
        self.discovery.handle_request(&UadpDiscoveryRequest::new(
            InformationType::DataSetMetaData,
            Some(vec![0u16]),
        ))
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
        ds: Option<Arc<RwLock<PubSubDataSource>>>,
    ) -> Result<Self, StatusCode> {
        let data_source = match ds {
            Some(ds) => ds,
            None => PubSubDataSource::new_arc(SimpleAddressSpace::new_arc_lock()),
        };
        let net_cfg = ConnectionConfig::from_cfg(cfg)?;
        let mut s = Self::new(net_cfg, cfg.publisher_id.clone(), data_source, None)?;
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
    pub const fn publisher_id(&self) -> &Variant {
        &self.publisher_id
    }

    /// Set the pub sub connection's name.
    pub fn set_name(&mut self, name: UAString) {
        self.name = name;
    }

    /// Get a reference to the pub sub connection's name.
    pub const fn name(&self) -> &UAString {
        &self.name
    }
}

impl Drop for PubSubConnection {
    fn drop(&mut self) {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let stx = self.tx.clone();
        // This doesnt work
        tokio::task::spawn(
          async move {
                warn!("call");
                if let Err(e) = stx
                    .send_timeout(
                        ConnectionMessage::Shutdown,
                        std::time::Duration::from_millis(100),
                    )
                    .await
                {
                    warn!("drop timeout error: #{}", e);
                }
                tx.send(()).unwrap();
            });
    
        if rx.recv_timeout(std::time::Duration::from_millis(300)).is_err(){
            warn!("Recv Timeout")
        }
    }
}

struct PubSubDataSetInfo<'a> {
    data_source: &'a Arc<RwLock<PubSubDataSource>>,
    datasets: &'a [PublishedDataSet],
}

impl<'a> DataSetInfo for PubSubDataSetInfo<'a> {
    fn collect_values(&self, name: &UAString) -> Vec<(Promoted, DataValue)> {
        if let Some(ds) = self.datasets.iter().find(|x| &x.name == name) {
            let mut guard = self.data_source.write().unwrap();
            let d_source = &mut (*guard);
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
        Self {
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
        data_source: Arc<RwLock<dyn DataSource + Sync + Send>>,
    ) -> Result<PubSubConnection, StatusCode> {
        Ok(PubSubConnection::new(
            self.network_config.clone(),
            self.publisher_id.clone(),
            PubSubDataSource::new_arc(data_source.clone()),
            self.value_recv.clone(),
        )?)
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
