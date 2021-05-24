use crate::callback::OnPubSubReciveValues;
use crate::connection::PubSubDataSourceT;
use crate::message::uadp::{UadpDataSetMessage, UadpMessageType, UadpNetworkMessage};
use crate::network::ReaderTransportSettings;
use crate::pubdataset::{PubSubFieldMetaData, SubscribedDataSet, UpdateTarget};
use std::sync::{Arc, Mutex, RwLock};

use log::{error, warn};
use opcua_types::{status_code::StatusCode, string::UAString, DataValue, DateTime, Variant};
/// Reader group
pub struct ReaderGroup {
    pub name: UAString,
    reader: Vec<DataSetReader>,
}

/// Builds a dataset
pub struct DataSetReaderBuilder {
    name: UAString,
    publisher_id: Variant,
    writer_group_id: u16,
    dataset_writer_id: u16,
    transport_settings: ReaderTransportSettings,
}

/// DataSetReader targets a publisher and the dataset
/// if publisher_id = Variant::Empty every publisher is read
pub struct DataSetReader {
    name: UAString,
    publisher_id: Variant,
    writer_group_id: u16,
    dataset_writer_id: u16,
    fields: Vec<PubSubFieldMetaData>,
    sub_data_set: SubscribedDataSet,
    transport_settings: ReaderTransportSettings,
}

impl DataSetReaderBuilder {
    pub fn new() -> Self {
        DataSetReaderBuilder {
            name: "DataSet Reader".into(),
            publisher_id: Variant::Empty,
            writer_group_id: 0_u16,
            dataset_writer_id: 0_u16,
            transport_settings: ReaderTransportSettings::None,
        }
    }

    pub fn new_for_broker(
        topic: &UAString,
        meta_topic: &UAString,
        qos: opcua_types::BrokerTransportQualityOfService,
    ) -> Self {
        DataSetReaderBuilder {
            name: "DataSet Reader".into(),
            publisher_id: Variant::Empty,
            writer_group_id: 0_u16,
            dataset_writer_id: 0_u16,
            transport_settings: ReaderTransportSettings::BrokerDataSetReader(
                opcua_types::BrokerDataSetReaderTransportDataType {
                    authentication_profile_uri: "".into(),
                    meta_data_queue_name: meta_topic.clone(),
                    queue_name: topic.clone(),
                    requested_delivery_guarantee: qos,
                    resource_uri: "".into(),
                },
            ),
        }
    }

    pub fn name(&mut self, name: UAString) -> &mut Self {
        self.name = name;
        self
    }
    /// Sets the targeted publisher id
    pub fn publisher_id(&mut self, pub_id: Variant) -> &mut Self {
        self.publisher_id = pub_id;
        self
    }
    /// Sets the targeted writer_group_id
    pub fn writer_group_id(&mut self, writer_group_id: u16) -> &mut Self {
        self.writer_group_id = writer_group_id;
        self
    }
    /// Sets the targeted dataset_writer
    pub fn dataset_writer_id(&mut self, dataset_writer_id: u16) -> &mut Self {
        self.dataset_writer_id = dataset_writer_id;
        self
    }

    pub fn build(&self) -> DataSetReader {
        DataSetReader {
            name: self.name.clone(),
            publisher_id: self.publisher_id.clone(),
            writer_group_id: self.writer_group_id,
            dataset_writer_id: self.dataset_writer_id,
            fields: Vec::new(),
            sub_data_set: SubscribedDataSet::new(),
            transport_settings: self.transport_settings.clone(),
        }
    }
}

impl Default for DataSetReaderBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ReaderGroup {
    pub fn new(name: UAString) -> Self {
        ReaderGroup {
            name,
            reader: Vec::new(),
        }
    }

    /// Loads all transport settings to sub, this is needed for broker support
    pub fn get_transport_cfg(&self) -> Vec<&ReaderTransportSettings> {
        self.reader.iter().map(|r| r.transport_settings()).collect()
    }

    /// Check the message and forward it to the datasets
    pub fn handle_message(
        &self,
        topic: &UAString,
        msg: &UadpNetworkMessage,
        data_source: &Arc<RwLock<PubSubDataSourceT>>,
        cb: &Option<Arc<Mutex<dyn OnPubSubReciveValues + Send>>>,
    ) {
        for r in self.reader.iter() {
            r.handle_message(topic, msg, &data_source, cb);
        }
    }
    /// Adds Dataset to the group
    pub fn add_dataset_reader(&mut self, dataset_reader: DataSetReader) {
        self.reader.push(dataset_reader);
    }
}

impl DataSetReader {
    /// Check if message is for this reader
    fn check_message(&self, topic: &UAString, msg: &UadpNetworkMessage) -> Option<usize> {
        if let ReaderTransportSettings::BrokerDataSetReader(cfg) = &self.transport_settings {
            if topic != &cfg.queue_name {
                return None;
            }
        }
        // Check if message is for this publisher_id
        if self.publisher_id != Variant::Empty {
            match &msg.header.publisher_id {
                Some(pub_id) => {
                    if &self.publisher_id != pub_id {
                        return None;
                    }
                }
                None => return None,
            }
        }
        // Find out if the writer_groupe is contained in the message
        if let Some(gp) = &msg.group_header {
            if let Some(wg_id) = gp.writer_group_id {
                if wg_id == self.writer_group_id {
                    // Find the dataset if contained
                    if let Some(idx) = msg
                        .dataset_payload
                        .iter()
                        .position(|ds| ds == &self.dataset_writer_id)
                    {
                        return Some(idx);
                    }
                }
            }
        }
        None
    }

    fn handle_fields(&self, ds: &UadpDataSetMessage) -> Vec<UpdateTarget> {
        let mut ret = Vec::new();
        let server_t = DateTime::now();
        let status = match ds.header.status {
            Some(s) => StatusCode::from_u32(s as u32).unwrap_or(StatusCode::Good),
            None => StatusCode::Good,
        };
        let source_t = if let Some(dt) = &ds.header.time_stamp {
            dt.clone()
        } else {
            DateTime::now()
        };

        match &ds.data {
            UadpMessageType::KeyDeltaFrameRaw(_) => {
                error!("Raw Frames not implemented");
            }
            UadpMessageType::KeyDeltaFrameValue(vals) => {
                for (id, v) in vals.iter() {
                    let p = *id as usize;
                    if p < self.fields.len() {
                        let f = &self.fields[p];
                        ret.push(UpdateTarget(f.data_set_field_id().clone(), v.clone(), f));
                    } else {
                        warn!(
                            "Unknown field {} in {} : value: {}",
                            id,
                            self.name,
                            v.value.as_ref().unwrap_or(&Variant::String("Error".into()))
                        );
                    }
                }
            }
            UadpMessageType::KeyDeltaFrameVariant(vals) => {
                for (id, v) in vals.iter() {
                    let p = *id as usize;
                    if p < self.fields.len() {
                        let f = &self.fields[p];
                        let mut dv = DataValue::value_only(v.clone());
                        dv.source_timestamp = Some(source_t.clone());
                        dv.server_timestamp = Some(server_t.clone());
                        dv.status = Some(status);
                        ret.push(UpdateTarget(f.data_set_field_id().clone(), dv, f));
                    } else {
                        warn!("Unknown field {} in {}: value: {}", p, self.name, v);
                    }
                }
            }
            UadpMessageType::KeyFrameDataValue(vals) => {
                for (p, v) in vals.iter().enumerate() {
                    if p < self.fields.len() {
                        let f = &self.fields[p];
                        ret.push(UpdateTarget(f.data_set_field_id().clone(), v.clone(), f));
                    } else {
                        warn!(
                            "Unknown field {} in {} : value: {}",
                            p,
                            self.name,
                            v.value.as_ref().unwrap_or(&Variant::String("Error".into()))
                        );
                    }
                }
            }
            UadpMessageType::KeyFrameRaw(_) => {
                //@TODO parse raw missing
                error!("Raw Frames not implemented");
            }
            UadpMessageType::KeyFrameVariant(vals) => {
                for (p, v) in vals.iter().enumerate() {
                    if p < self.fields.len() {
                        let f = &self.fields[p];
                        let mut dv = DataValue::value_only(v.clone());
                        dv.source_timestamp = Some(source_t.clone());
                        dv.server_timestamp = Some(server_t.clone());
                        dv.status = Some(status);
                        ret.push(UpdateTarget(f.data_set_field_id().clone(), dv, f));
                    } else {
                        warn!("Unknown field {} in {}: value: {}", p, self.name, v);
                    }
                }
            }
            // Ignore keep alive this message is for the pusubconnection
            UadpMessageType::KeepAlive => {}
            // Support is missing @TODO implement Events
            UadpMessageType::Event(_) => {
                error!("Event Message not supported in Dataset {}!", self.name);
            }
        }
        ret
    }
    /// Handle a message if it matches the writer
    pub fn handle_message(
        &self,
        topic: &UAString,
        msg: &UadpNetworkMessage,
        data_source: &Arc<RwLock<PubSubDataSourceT>>,
        cb: &Option<Arc<Mutex<dyn OnPubSubReciveValues + Send>>>,
    ) {
        if let Some(idx) = self.check_message(topic, msg) {
            if idx < msg.dataset.len() {
                let ds = &msg.dataset[idx];
                let res = self.handle_fields(ds);
                if !res.is_empty() {
                    if let Some(cb) = cb {
                        let mut cb = cb.lock().unwrap();
                        cb.data_recived(self, &res);
                    }
                    self.sub_data_set.update_targets(res, &data_source);
                }
            }
        }
    }

    pub fn add_field(&mut self, field: PubSubFieldMetaData) {
        self.fields.push(field);
    }

    pub fn sub_data_set(&mut self) -> &mut SubscribedDataSet {
        &mut self.sub_data_set
    }

    pub fn name(&self) -> &UAString {
        &self.name
    }

    pub fn transport_settings(&self) -> &ReaderTransportSettings {
        &self.transport_settings
    }
}
