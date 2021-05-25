// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode

use crate::address_space::PubSubDataSourceT;
use crate::callback::OnPubSubReciveValues;
use crate::dataset::{DataSetInfo, Promoted, PublishedDataSet};
use crate::message::UadpNetworkMessage;
use crate::network::{
    ConnectionReceiver, Connections, MqttConnection, TransportSettings, UadpNetworkConnection,
};
use crate::reader::ReaderGroup;
use crate::writer::WriterGroup;
use log::{error, warn};
use opcua_types::status_code::StatusCode;
use opcua_types::string::UAString;
use opcua_types::{ConfigurationVersionDataType, DataValue, DecodingLimits, Variant};
use std::io::Cursor;
use std::sync::{Arc, Mutex, RwLock};
use url::Url;

/// Id for a pubsubconnection
#[derive(Debug, PartialEq, Clone)]
pub struct PubSubConnectionId(pub u32);

#[allow(dead_code)]
/// Helps Building a Connection
/// @TODO add an builder example
pub struct PubSubConnectionBuilder {
    name: UAString,
    url: UAString,
    enabled: bool,
    publisher_id: Variant,
    value_recv: Option<Arc<Mutex<dyn OnPubSubReciveValues + Send>>>,
}

pub enum PubSubTransportProfil {
    /// http://opcfoundation.org/UA-Profile/Transport/pubsub-udp-uadp
    UdpUadp,
    ///
    Mqtt,
    ///
    Amqp,
    /// "http://opcfoundation.org/UA-Profile/Transport/pubsub-eth-uadp" unsupported
    EthUadp,
}

pub enum PubSubEncoding {
    Json,
    Uadp,
}

#[allow(dead_code)]
pub struct PubSubConnection {
    profile: PubSubTransportProfil,
    url: String,
    publisher_id: Variant,
    connection: Connections,
    datasets: Vec<PublishedDataSet>,
    writer: Vec<WriterGroup>,
    reader: Vec<ReaderGroup>,
    network_message_no: u16,
    data_source: Arc<RwLock<PubSubDataSourceT>>,
    value_recv: Option<Arc<Mutex<dyn OnPubSubReciveValues + Send>>>,
    id: PubSubConnectionId,
}

pub struct PubSubReceiver {
    recv: ConnectionReceiver,
}

impl PubSubReceiver {
    /// Receive a UadpNetworkMessage
    pub fn receive_msg(&self) -> Result<(String, UadpNetworkMessage), StatusCode> {
        let (topic, data) = self.recv.receive_msg()?;
        let mut stream = Cursor::new(&data);
        let decoding_options = DecodingLimits::default();

        let msg = UadpNetworkMessage::decode(&mut stream, &decoding_options)?;
        Ok((topic, msg))
    }

    pub fn run(&self, pubsub: Arc<RwLock<PubSubConnection>>) {
        loop {
            match self.receive_msg() {
                Ok((topic, msg)) => {
                    let ps = pubsub.write().unwrap();
                    ps.handle_message(&topic.into(), msg);
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
        value_recv: Option<Arc<Mutex<dyn OnPubSubReciveValues + Send>>>,
    ) -> Result<Self, StatusCode> {
        // from url choose which protocol to use
        let (uri, profile) = match Url::parse(&url) {
            Ok(uri) => match uri.scheme() {
                "opc.udp" => {
                    let u = format!(
                        "{}:{}",
                        uri.host_str().unwrap_or("localhost"),
                        uri.port().unwrap_or(4840)
                    );
                    (u, PubSubTransportProfil::UdpUadp)
                }
                "mqtt" | "mqtts" => (url.clone(), PubSubTransportProfil::Mqtt),
                "amqps" | "amqp" => (url.clone(), PubSubTransportProfil::Amqp),
                _ => {
                    error!("Unkown scheme uri: {} - {}", url, uri.scheme());
                    return Err(StatusCode::BadInvalidArgument);
                }
            },
            Err(err) => {
                error!("Invalid uri: {} - {}", url, err);
                return Err(StatusCode::BadInvalidArgument);
            }
        };
        // Check if transport profile is supported
        let connection = match profile {
            PubSubTransportProfil::UdpUadp => match UadpNetworkConnection::new(&uri) {
                Ok(con) => Connections::Uadp(con),
                Err(e) => {
                    error!("Creating UadpNetworkconnection: {:-?}", e);
                    return Err(StatusCode::BadCommunicationError);
                }
            },
            #[cfg(feature = "mqtt")]
            PubSubTransportProfil::Mqtt => Connections::Mqtt(MqttConnection::new(&url)?),
            _ => return Err(StatusCode::BadNotImplemented),
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
            profile,
            url,
            publisher_id,
            connection,
            datasets: Vec::new(),
            writer: Vec::new(),
            reader: Vec::new(),
            network_message_no: 0,
            data_source,
            value_recv,
            id: PubSubConnectionId(0),
        })
    }
    /// add datavalue recv callback when values change
    pub fn set_datavalue_recv(&mut self, cb: Option<Arc<Mutex<dyn OnPubSubReciveValues + Send>>>) {
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
    pub fn add_dataset(&mut self, dataset: PublishedDataSet) {
        self.datasets.push(dataset);
    }

    pub fn add_writer_group(&mut self, group: WriterGroup) {
        self.writer.push(group);
    }

    pub fn add_reader_group(&mut self, group: ReaderGroup) {
        self.reader.push(group);
    }

    pub fn handle_message(&self, topic: &UAString, msg: UadpNetworkMessage) {
        for rg in self.reader.iter() {
            rg.handle_message(&topic, &msg, &self.data_source, &self.value_recv);
        }
    }
    /// Enable datasetreader
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

    /// Disable datasetreader
    pub fn disable(&self) {
        for r in self.reader.iter() {
            for cfg in r.get_transport_cfg().iter() {
                if let Err(err) = self.connection.unsubscribe(cfg) {
                    warn!("Error deactivate broker config {}", err);
                }
            }
        }
    }

    /// runs pubs in a new thread
    /// currently 2 threads are used
    pub fn run_thread(
        pubsub: Arc<RwLock<Self>>,
    ) -> (std::thread::JoinHandle<()>, std::thread::JoinHandle<()>) {
        {
            let ps = pubsub.write().unwrap();
            ps.enable();
        }
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
        let (th1, th2) = Self::run_thread(s.clone());
        th1.join().unwrap();
        th2.join().unwrap();
        let s = s.write().unwrap();
        s.disable();
    }
    /// Runs all writer, that should run and returns the next call to pull
    pub fn drive_writer(&mut self) -> std::time::Duration {
        let mut msgs = Vec::new();
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
            value_recv: None,
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

    pub fn add_value_receiver<T: OnPubSubReciveValues + Send + 'static>(&mut self, value_recv: T) {
        self.value_recv = Some(Arc::new(Mutex::new(value_recv)));
    }

    pub fn build(
        &self,
        data_source: Arc<RwLock<PubSubDataSourceT>>,
    ) -> Result<PubSubConnection, StatusCode> {
        PubSubConnection::new(
            self.url.to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_urn() {
        let url = Url::parse("opc.udp://10.23.1.2:1234").unwrap();
        println!("{:?}", url);
        assert_eq!(url.scheme(), "opc.udp");
        assert_eq!(url.host_str(), Some("10.23.1.2"));
        assert_eq!(url.port(), Some(1234));
        let url = Url::parse("amqps://abc:1234/xxx").unwrap();
        assert_eq!(url.scheme(), "amqps");
        let url = Url::parse("wss://abc:123/xxx").unwrap();
        assert_eq!(url.scheme(), "wss");
        let url = Url::parse("mqtts://abc:1234/hhas").unwrap();
        assert_eq!(url.scheme(), "mqtts");
    }
}
