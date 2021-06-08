// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use crate::dataset::{generate_version_time, DataSetInfo, Promoted, PublishedDataSet};
use crate::message::{
    UadpDataSetMessage, UadpGroupHeader, UadpMessageType, UadpNetworkMessage, UadpPayload,
};
use crate::network::TransportSettings;
use crate::until::decode_extension;
use crate::{
    DataSetFieldContentMask, UadpDataSetMessageContentMask, UadpNetworkMessageContentMask,
};
use chrono::Utc;
use log::error;
use opcua_types::status_code::StatusCode;
use opcua_types::string::UAString;
use opcua_types::{
    BrokerDataSetWriterTransportDataType, DataSetOrderingType,
    DatagramWriterGroupTransportDataType, DecodingOptions, Duration, ObjectTypeId,
    UadpDataSetWriterMessageDataType, UadpWriterGroupMessageDataType,
};
use opcua_types::{BrokerTransportQualityOfService, Guid};
use opcua_types::{
    BrokerWriterGroupTransportDataType, ConfigurationVersionDataType, DataValue, Variant,
};
use opcua_types::{
    DataSetWriterDataType, DateTime, ExtensionObject, LocalizedText, MessageSecurityMode,
    WriterGroupDataType,
};

pub struct DataSetWriterBuilder {
    name: UAString,
    description: LocalizedText,
    dataset_writer_id: u16,
    field_content_mask: DataSetFieldContentMask,
    message_content_mask: UadpDataSetMessageContentMask,
    key_frame_count: u32,
    dataset_name: UAString,
    transport_settings: TransportSettings,
}

impl DataSetWriterBuilder {
    pub fn new(pds: &PublishedDataSet) -> Self {
        DataSetWriterBuilder {
            name: "DataSetWriter".into(),
            description: "".into(),
            dataset_writer_id: 12345,
            field_content_mask: DataSetFieldContentMask::None,
            key_frame_count: 10,
            message_content_mask: UadpDataSetMessageContentMask::MajorVersion
                | UadpDataSetMessageContentMask::MinorVersion
                | UadpDataSetMessageContentMask::Timestamp,
            dataset_name: pds.name.clone(),
            transport_settings: TransportSettings::None,
        }
    }

    /// For broker based protocols like mqtt and AMQP configuration like topic name are needed
    pub fn new_for_broker(
        pds: &PublishedDataSet,
        meta_publish_interval: f64,
        meta_topic: &UAString,
        meta_qos: &BrokerTransportQualityOfService,
    ) -> Self {
        DataSetWriterBuilder {
            description: "".into(),
            name: "DataSetWriter".into(),
            dataset_writer_id: 12345,
            field_content_mask: DataSetFieldContentMask::None,
            key_frame_count: 10,
            message_content_mask: UadpDataSetMessageContentMask::MajorVersion
                | UadpDataSetMessageContentMask::MinorVersion
                | UadpDataSetMessageContentMask::Timestamp,
            dataset_name: pds.name.clone(),
            transport_settings: TransportSettings::BrokerDataSetWrite({
                opcua_types::BrokerDataSetWriterTransportDataType {
                    queue_name: UAString::null(),
                    authentication_profile_uri: UAString::null(),
                    requested_delivery_guarantee: *meta_qos,
                    resource_uri: UAString::null(),
                    meta_data_queue_name: meta_topic.clone(),
                    meta_data_update_time: meta_publish_interval,
                }
            }),
        }
    }

    pub fn description(&mut self, desc: LocalizedText) -> &mut Self {
        self.description = desc;
        self
    }

    pub fn name(&mut self, name: UAString) -> &mut Self {
        self.name = name;
        self
    }

    pub fn dataset_writer_id(&mut self, id: u16) -> &mut Self {
        self.dataset_writer_id = id;
        self
    }

    pub fn content_mask(&mut self, mask: DataSetFieldContentMask) -> &mut Self {
        self.field_content_mask = mask;
        self
    }

    pub fn message_setting(&mut self, mask: UadpDataSetMessageContentMask) -> &mut Self {
        self.message_content_mask = mask;
        self
    }

    pub fn key_frame_count(&mut self, key_frame_count: u32) -> &mut Self {
        self.key_frame_count = key_frame_count;
        self
    }

    pub fn build(&self) -> DataSetWriter {
        DataSetWriter {
            name: self.name.clone(),
            description: self.description.clone(),
            dataset_writer_id: self.dataset_writer_id,
            dataset_name: self.dataset_name.clone(),
            field_content_mask: self.field_content_mask,
            message_content_mask: self.message_content_mask,
            key_frame_count: self.key_frame_count,
            delta_frame_counter: 0,
            config_version: ConfigurationVersionDataType {
                major_version: 0,
                minor_version: 0,
            },
            sequence_no: 0,
            transport_settings: self.transport_settings.clone(),
        }
    }
}

pub struct DataSetWriter {
    pub name: UAString,
    pub description: LocalizedText,
    pub dataset_writer_id: u16,
    pub dataset_name: UAString,
    field_content_mask: DataSetFieldContentMask,
    message_content_mask: UadpDataSetMessageContentMask,
    key_frame_count: u32,
    delta_frame_counter: u32,
    config_version: ConfigurationVersionDataType,
    sequence_no: u16,
    transport_settings: TransportSettings,
}

impl DataSetWriter {
    /// Get Transport Settings
    pub fn transport_settings(&self) -> &TransportSettings {
        &self.transport_settings
    }

    pub fn decode_transport_message_setting(
        cfg: &DataSetWriterDataType,
    ) -> Result<(TransportSettings, UadpDataSetWriterMessageDataType), StatusCode> {
        let set = match decode_extension::<UadpDataSetWriterMessageDataType>(
            &cfg.message_settings,
            opcua_types::ObjectId::UadpDataSetWriterMessageDataType_Encoding_DefaultBinary,
            &DecodingOptions::default(),
        ) {
            Ok(set) => set,
            Err(_) => UadpDataSetWriterMessageDataType {
                data_set_message_content_mask: UadpDataSetMessageContentMask::None,
                configured_size: 0,
                network_message_number: 0,
                data_set_offset: 0,
            },
        };
        let tr = {
            if let Ok(t) = decode_extension::<BrokerDataSetWriterTransportDataType>(
                &cfg.message_settings,
                opcua_types::ObjectId::BrokerDataSetWriterTransportDataType_Encoding_DefaultBinary,
                &DecodingOptions::default(),
            ) {
                TransportSettings::BrokerDataSetWrite(t)
            } else {
                TransportSettings::None
            }
        };
        Ok((tr, set))
    }

    pub fn update(&mut self, cfg: &DataSetWriterDataType) -> Result<(), StatusCode> {
        self.dataset_writer_id = cfg.data_set_writer_id;
        self.delta_frame_counter = 0;
        self.name = cfg.name.clone();
        self.key_frame_count = cfg.key_frame_count;
        self.dataset_name = cfg.data_set_name.clone();
        self.field_content_mask = cfg.data_set_field_content_mask;
        let (transport_setting, msg_settings) = Self::decode_transport_message_setting(&cfg)?;
        self.transport_settings = transport_setting;
        self.message_content_mask = msg_settings.data_set_message_content_mask;
        Ok(())
    }

    pub fn from_cfg(cfg: &DataSetWriterDataType) -> Result<Self, StatusCode> {
        let mut s = DataSetWriter {
            name: "".into(),
            description: "".into(),
            dataset_writer_id: 1234_u16,
            dataset_name: "".into(),
            field_content_mask: DataSetFieldContentMask::None,
            message_content_mask: UadpDataSetMessageContentMask::None,
            key_frame_count: 0,
            delta_frame_counter: 0,
            config_version: ConfigurationVersionDataType {
                major_version: 0,
                minor_version: 0,
            },
            sequence_no: 0,
            transport_settings: TransportSettings::None,
        };
        s.update(cfg)?;
        Ok(s)
    }

    pub fn generate_info(&self) -> DataSetWriterDataType {
        let transport_settings = match &self.transport_settings {
            TransportSettings::BrokerDataSetWrite(x) => ExtensionObject::from_encodable(
                &opcua_types::ObjectTypeId::BrokerDataSetWriterTransportType,
                &opcua_types::BrokerDataSetWriterTransportDataType {
                    queue_name: x.queue_name.clone(),
                    resource_uri: x.resource_uri.clone(),
                    authentication_profile_uri: x.authentication_profile_uri.clone(),
                    requested_delivery_guarantee: x.requested_delivery_guarantee,
                    meta_data_queue_name: x.meta_data_queue_name.clone(),
                    meta_data_update_time: x.meta_data_update_time,
                },
            ),
            TransportSettings::BrokerWrite(_) => {
                panic!("Didn't expect Broker Write Groupe settings here");
            }
            TransportSettings::None => {
                // @TODO check if this ok because for datagram there are no transport settings for DatasetWriter
                ExtensionObject::null()
            }
        };

        let message_settings = opcua_types::UadpDataSetWriterMessageDataType {
            data_set_message_content_mask: self.message_content_mask,
            configured_size: 0,
            network_message_number: 0,
            data_set_offset: 0,
        };
        let message_settings = ExtensionObject::from_encodable(
            opcua_types::ObjectTypeId::UadpDataSetWriterMessageType,
            &message_settings,
        );

        DataSetWriterDataType {
            name: self.name.clone(),
            enabled: true,
            data_set_writer_id: self.dataset_writer_id,
            data_set_field_content_mask: self.field_content_mask,
            key_frame_count: self.key_frame_count,
            data_set_name: self.dataset_name.clone(),
            data_set_writer_properties: None,
            transport_settings,
            message_settings,
        }
    }

    pub fn generate_header(&self, msg: &mut UadpDataSetMessage) {
        msg.header.valid = true;
        if self
            .message_content_mask
            .contains(UadpDataSetMessageContentMask::MajorVersion)
        {
            msg.header.cfg_major_version = Some(self.config_version.major_version);
        }
        if self
            .message_content_mask
            .contains(UadpDataSetMessageContentMask::MinorVersion)
        {
            msg.header.cfg_minor_version = Some(self.config_version.minor_version);
        }
        if self
            .message_content_mask
            .contains(UadpDataSetMessageContentMask::PicoSeconds)
        {
            // Pico seconds are not supported
            msg.header.pico_seconds = Some(0);
        }
        if self
            .message_content_mask
            .contains(UadpDataSetMessageContentMask::SequenceNumber)
        {
            msg.header.sequence_no = Some(self.sequence_no);
        }
        if self
            .message_content_mask
            .contains(UadpDataSetMessageContentMask::Status)
        {
            //@TODO what todo with the status code?
            // 0 represents StatusCode Good
            msg.header.status = Some(0_u16);
        }
        if self
            .message_content_mask
            .contains(UadpDataSetMessageContentMask::Timestamp)
        {
            msg.header.time_stamp = Some(DateTime::now());
        }
    }
    pub fn generate_message(
        &mut self,
        ds: Vec<(Promoted, DataValue)>,
        config_version: &ConfigurationVersionDataType,
        publishing_interval: Duration,
    ) -> Option<UadpDataSetMessage> {
        self.delta_frame_counter += 1;
        if config_version != &self.config_version {
            self.config_version = config_version.clone();
            self.delta_frame_counter = self.key_frame_count;
        }
        // Delta Frame counter is counted up each time the Writer is called
        // if delta frame is lower then frame_counter generate a delta frame.
        // The exception is if the dataset only contains 1 field because generating
        // a delta frame for 1 element is longer then sending a keyframe
        let dataset = if ds.len() > 1 && self.delta_frame_counter < self.key_frame_count {
            let time_diff = chrono::Duration::milliseconds(
                (publishing_interval * self.delta_frame_counter as f64) as i64,
            );
            let offset: DateTime = (Utc::now() - time_diff).into();
            // Generate a DeltaFrame with Variants
            if self.field_content_mask.is_empty() {
                let mut cnt: u16 = 0;
                let mut vec = Vec::new();
                for (_, d) in ds {
                    cnt += 1;
                    if let Some(dt) = d.source_timestamp {
                        if dt.ticks() > offset.ticks() {
                            vec.push((
                                cnt,
                                // Variant Value or StatusCode if no Value
                                match d.value {
                                    Some(x) => x,
                                    None => Variant::StatusCode(
                                        d.status.unwrap_or(StatusCode::BadUnexpectedError),
                                    ),
                                },
                            ));
                        }
                    }
                }
                if vec.is_empty() {
                    None
                } else {
                    Some(UadpMessageType::KeyDeltaFrameVariant(vec))
                }
            }
            // Generate a DeltaFrame with RawData
            else if self
                .field_content_mask
                .contains(DataSetFieldContentMask::RawData)
            {
                None // @TODO Atm raw transport is not supported
            }
            // Generate a DeltaFrame with DataValue
            else {
                let mut cnt: u16 = 0;
                let mut vec = Vec::new();
                for (_, mut d) in ds {
                    cnt += 1;
                    if let Some(dt) = &d.source_timestamp {
                        if dt.ticks() > offset.ticks() {
                            if !self
                                .field_content_mask
                                .contains(DataSetFieldContentMask::SourceTimestamp)
                            {
                                d.server_timestamp = None;
                            }
                            if !self
                                .field_content_mask
                                .contains(DataSetFieldContentMask::ServerTimestamp)
                            {
                                d.source_timestamp = None;
                            }
                            if !self
                                .field_content_mask
                                .contains(DataSetFieldContentMask::StatusCode)
                            {
                                d.status = None;
                            }
                            vec.push((cnt, d));
                        }
                    }
                }
                if vec.is_empty() {
                    None
                } else {
                    Some(UadpMessageType::KeyDeltaFrameValue(vec))
                }
            }
        } else {
            // Generate KeyFrame with variants
            if self.field_content_mask.is_empty() {
                let mut vec = Vec::new();
                for (_, d) in ds {
                    let v = match d.value {
                        Some(x) => x,
                        None => {
                            Variant::StatusCode(d.status.unwrap_or(StatusCode::BadUnexpectedError))
                        }
                    };
                    vec.push(v);
                }
                Some(UadpMessageType::KeyFrameVariant(vec))
            }
            // Generate Keyframe with raw value
            else if self
                .field_content_mask
                .contains(DataSetFieldContentMask::RawData)
            {
                None // @TODO Not supported at the moment
            }
            // Generate KeyFrame with DataValue
            else {
                let mut vec = Vec::new();
                for (_, mut d) in ds {
                    if !self
                        .field_content_mask
                        .contains(DataSetFieldContentMask::SourceTimestamp)
                    {
                        d.server_timestamp = None;
                    }
                    if !self
                        .field_content_mask
                        .contains(DataSetFieldContentMask::ServerTimestamp)
                    {
                        d.source_timestamp = None;
                    }
                    if !self
                        .field_content_mask
                        .contains(DataSetFieldContentMask::StatusCode)
                    {
                        d.status = None;
                    }
                    vec.push(d);
                }
                Some(UadpMessageType::KeyFrameDataValue(vec))
            }
        };

        if self.delta_frame_counter >= self.key_frame_count {
            self.delta_frame_counter = 0;
        }
        if let Some(data) = dataset {
            let mut message = UadpDataSetMessage::new(data);
            self.generate_header(&mut message);
            self.sequence_no += self.sequence_no.wrapping_add(1);
            Some(message)
        } else {
            self.sequence_no += self.sequence_no.wrapping_add(1);
            None
        }
    }
}

#[allow(dead_code)]
pub struct WriterGroup {
    pub name: UAString,
    enabled: bool,
    pub writer_group_id: u16,
    publishing_interval: Duration,
    keep_alive_time: f64,
    message_settings: UadpNetworkMessageContentMask,
    sequence_no: u16,
    writer: Vec<DataSetWriter>,
    group_version: u32,
    last_action: DateTime,
    transport_settings: TransportSettings,
    max_network_message_size: u32,
    priority: u8,
    ordering: DataSetOrderingType,
}

impl WriterGroup {
    /// Check if action is required
    pub fn tick(&mut self) -> bool {
        //@TODO check KeepAlive
        // Convert publishing_interval from Milliseconds + fraction to 100 nano Seconds
        (self.last_action.ticks() + (self.publishing_interval * 1000.0) as i64)
            < DateTime::now().ticks()
    }

    /// Get Transport Settings
    pub fn transport_settings(&self) -> &TransportSettings {
        &self.transport_settings
    }

    pub fn decode_transport_message_setting(
        cfg: &WriterGroupDataType,
    ) -> Result<(TransportSettings, UadpWriterGroupMessageDataType), StatusCode> {
        let set = decode_extension::<UadpWriterGroupMessageDataType>(
            &cfg.message_settings,
            opcua_types::ObjectId::UadpWriterGroupMessageDataType_Encoding_DefaultBinary,
            &DecodingOptions::default(),
        )?;
        let tr = {
            if let Ok(t) = decode_extension::<BrokerWriterGroupTransportDataType>(
                &cfg.message_settings,
                opcua_types::ObjectId::BrokerWriterGroupTransportDataType_Encoding_DefaultBinary,
                &DecodingOptions::default(),
            ) {
                TransportSettings::BrokerWrite(t)
            } else {
                TransportSettings::None
            }
        };
        Ok((tr, set))
    }

    pub fn update(&mut self, cfg: &WriterGroupDataType) -> Result<(), StatusCode> {
        let (transport, set) = Self::decode_transport_message_setting(cfg)?;
        self.name = cfg.name.clone();
        self.enabled = false;
        self.group_version = set.group_version;
        self.keep_alive_time = cfg.keep_alive_time;
        self.last_action = DateTime::null();
        self.max_network_message_size = cfg.max_network_message_size;
        self.priority = cfg.priority;
        self.publishing_interval = cfg.publishing_interval;
        self.sequence_no = 0;
        self.transport_settings = transport;
        self.message_settings = set.network_message_content_mask;
        self.writer_group_id = cfg.writer_group_id;
        self.ordering = set.data_set_ordering;
        if let Some(dswg) = &cfg.data_set_writers {
            for dsw in dswg {
                if let Some(w) = self
                    .writer
                    .iter_mut()
                    .find(|x| x.dataset_writer_id == dsw.data_set_writer_id)
                {
                    w.update(&dsw)?;
                } else {
                    self.add_dataset_writer(DataSetWriter::from_cfg(&dsw)?);
                }
            }
        }
        Ok(())
    }

    pub fn from_cfg(cfg: &WriterGroupDataType) -> Result<Self, StatusCode> {
        let mut s = WriterGroup {
            name: cfg.name.clone(),
            enabled: false,
            writer_group_id: cfg.writer_group_id,
            publishing_interval: 0.0,
            keep_alive_time: 0.0,
            message_settings: UadpNetworkMessageContentMask::None,
            writer: Vec::new(),
            sequence_no: 0,
            group_version: generate_version_time(),
            last_action: DateTime::null(),
            transport_settings: TransportSettings::None,
            // MTU for ethernet
            max_network_message_size: 1472,
            priority: 126,
            ordering: DataSetOrderingType::Undefined,
        };
        s.update(cfg)?;
        Ok(s)
    }

    /// Calculate next time the writer has to write data
    pub fn next_tick(&self) -> std::time::Duration {
        // calc point in 100ns where next action takes place
        let next = self.last_action.ticks() + (self.publishing_interval * 1000.0) as i64;
        std::time::Duration::from_micros(((next - DateTime::now().ticks()) / 10) as u64)
    }

    /// Generates the info of the writer groupe
    pub fn generate_info(&self) -> (WriterGroupDataType, Vec<u16>) {
        let transport_settings = match &self.transport_settings {
            TransportSettings::BrokerDataSetWrite(_) => {
                panic!("Did expect Broker Data Set Write here");
            }
            TransportSettings::BrokerWrite(x) => ExtensionObject::from_encodable(
                &ObjectTypeId::BrokerWriterGroupTransportType,
                &BrokerWriterGroupTransportDataType {
                    queue_name: x.queue_name.clone(),
                    resource_uri: x.resource_uri.clone(),
                    authentication_profile_uri: x.authentication_profile_uri.clone(),
                    requested_delivery_guarantee: x.requested_delivery_guarantee,
                },
            ),
            TransportSettings::None => {
                ExtensionObject::from_encodable(
                    ObjectTypeId::DatagramWriterGroupTransportType,
                    &DatagramWriterGroupTransportDataType {
                        /// Repeats not implemented
                        message_repeat_count: 0,
                        message_repeat_delay: 0.0,
                    },
                )
            }
        };

        let message_settings = opcua_types::UadpWriterGroupMessageDataType {
            group_version: self.group_version,
            data_set_ordering: self.ordering,
            network_message_content_mask: self.message_settings,
            sampling_offset: -1.0,
            publishing_offset: None,
        };
        let message_settings = ExtensionObject::from_encodable(
            opcua_types::ObjectTypeId::UadpWriterGroupMessageType,
            &message_settings,
        );

        (
            WriterGroupDataType {
                name: self.name.clone(),
                enabled: self.enabled,
                security_mode: MessageSecurityMode::None,
                security_group_id: "".into(),
                security_key_services: None,
                max_network_message_size: self.max_network_message_size,
                group_properties: None,
                writer_group_id: self.writer_group_id,
                publishing_interval: self.publishing_interval,
                keep_alive_time: self.keep_alive_time,
                priority: self.priority,
                locale_ids: None,
                header_layout_uri: "".into(),
                transport_settings,
                message_settings,
                data_set_writers: Some(self.writer.iter().map(|w| w.generate_info()).collect()),
            },
            self.writer.iter().map(|w| w.dataset_writer_id).collect(),
        )
    }

    pub fn generate_msg<T: DataSetInfo>(
        &mut self,
        network_no: u16,
        writer: &[usize],
        publisher_id: &Variant,
        ds: &T,
    ) -> Option<UadpNetworkMessage> {
        let mut message = UadpNetworkMessage::new();
        if self
            .message_settings
            .contains(UadpNetworkMessageContentMask::DataSetClassId)
        {
            message.header.dataset_class_id = Some(Guid::null());
        }
        if self
            .message_settings
            .contains(UadpNetworkMessageContentMask::PublisherId)
        {
            message.header.publisher_id = Some(publisher_id.clone());
        }
        if self
            .message_settings
            .contains(UadpNetworkMessageContentMask::GroupHeader)
        {
            let ver = if self
                .message_settings
                .contains(UadpNetworkMessageContentMask::GroupVersion)
            {
                Some(self.group_version)
            } else {
                None
            };
            let net_no = if self
                .message_settings
                .contains(UadpNetworkMessageContentMask::NetworkMessageNumber)
            {
                Some(network_no)
            } else {
                None
            };
            let seq_no = if self
                .message_settings
                .contains(UadpNetworkMessageContentMask::SequenceNumber)
            {
                Some(self.sequence_no)
            } else {
                None
            };
            let wg_id = if self
                .message_settings
                .contains(UadpNetworkMessageContentMask::WriterGroupId)
            {
                Some(self.writer_group_id)
            } else {
                None
            };
            message.group_header = Some(UadpGroupHeader {
                writer_group_id: wg_id,
                group_version: ver,
                network_message_no: net_no,
                sequence_no: seq_no,
            });
        }
        if self
            .message_settings
            .contains(UadpNetworkMessageContentMask::PicoSeconds)
        {
            message.picoseconds = Some(0);
        }

        if self
            .message_settings
            .contains(UadpNetworkMessageContentMask::Timestamp)
        {
            message.timestamp = Some(DateTime::now());
        }
        let sz = writer.len();
        let mut datasets = Vec::new();
        for wr in writer {
            let w = &mut self.writer[*wr];
            let vals = ds.collect_values(&w.dataset_name);
            // Only send Promoted Fields if 1 Dataset is send
            if sz == 1
                && self
                    .message_settings
                    .contains(UadpNetworkMessageContentMask::PromotedFields)
            {
                for (promoted, val) in vals.iter() {
                    if promoted.0 {
                        if let Some(v) = &val.value {
                            message.promoted_fields.push(v.clone());
                        }
                    }
                }
            }
            let cfg = ds.get_config_version(&w.dataset_name);
            let dataset = w.generate_message(vals, &cfg, self.publishing_interval);
            if let Some(ds) = dataset {
                datasets.push(ds);
                if self
                    .message_settings
                    .contains(UadpNetworkMessageContentMask::PayloadHeader)
                {
                    message.dataset_payload.push(w.dataset_writer_id);
                }
            }
        }
        // don't send a message with empty dataset
        if datasets.is_empty() {
            None
        } else {
            message.payload = UadpPayload::DataSets(datasets);
            self.sequence_no = self.sequence_no.wrapping_add(1);
            Some(message)
        }
    }

    pub fn generate_messages<T: DataSetInfo>(
        &mut self,
        network_no: u16,
        publisher_id: &Variant,
        ds: &T,
    ) -> Vec<UadpNetworkMessage> {
        //@TODO do keep alive
        //@TODO implement reordering of messages if size limit is hit
        // Sort datasets
        if self.ordering == DataSetOrderingType::AscendingWriterId
            || self.ordering == DataSetOrderingType::AscendingWriterIdSingle
        {
            self.writer
                .sort_by(|x, y| x.dataset_writer_id.cmp(&y.dataset_writer_id));
        }
        // Only one Datasets
        if self.ordering == DataSetOrderingType::AscendingWriterIdSingle || self.writer.len() == 1 {
            let mut v = Vec::new();
            let sz_dsw = self.writer.len();
            for w in 0..sz_dsw {
                if let Some(x) = self.generate_msg(network_no, &[w], publisher_id, ds) {
                    // Chunk message
                    if self.max_network_message_size > 0
                        && x.byte_len() > self.max_network_message_size as usize
                    {
                        let m = x.chunk(self.max_network_message_size as usize, self.sequence_no);
                        match m {
                            Ok(mut x) => {
                                v.append(&mut x);
                            }
                            Err(_) => {
                                error!("Chunking failed!");
                            }
                        }
                    } else {
                        v.push(x);
                    }
                }
            }
            v
        // Generate all Datasets in one message
        } else {
            let u = self.writer.len();
            let w: Vec<usize> = (0..u).collect();
            if let Some(x) = self.generate_msg(network_no, &w, publisher_id, ds) {
                vec![x]
            } else {
                Vec::new()
            }
        }
    }

    pub fn add_dataset_writer(&mut self, dsw: DataSetWriter) {
        self.group_version = generate_version_time();
        self.writer.push(dsw);
    }

    pub fn get_dataset_writer(&self, id: u16) -> Option<&DataSetWriter> {
        self.writer.iter().find(|ds| ds.dataset_writer_id == id)
    }

    /// Get a reference to the writer group's writer.
    pub fn writer(&self) -> Vec<&DataSetWriter> {
        self.writer.iter().collect()
    }
}

pub struct WriterGroupBuilder {
    name: UAString,
    group_id: u16,
    publish_interval: Duration,
    keep_alive_time: f64,
    message_settings: UadpNetworkMessageContentMask,
    transport_settings: TransportSettings,
    ordering: DataSetOrderingType,
}

impl WriterGroupBuilder {
    pub fn new() -> Self {
        WriterGroupBuilder {
            name: "WriterGroup".into(),
            group_id: 12345,
            publish_interval: 1000.0,
            keep_alive_time: 10000.0,
            message_settings: UadpNetworkMessageContentMask::PayloadHeader
                | UadpNetworkMessageContentMask::PublisherId
                | UadpNetworkMessageContentMask::WriterGroupId
                | UadpNetworkMessageContentMask::GroupHeader,
            transport_settings: TransportSettings::None,
            ordering: DataSetOrderingType::Undefined,
        }
    }

    pub fn new_for_broker(topic: &UAString, qos: &BrokerTransportQualityOfService) -> Self {
        WriterGroupBuilder {
            name: "WriterGroup".into(),
            group_id: 12345,
            publish_interval: 1000.0,
            keep_alive_time: 10000.0,
            message_settings: UadpNetworkMessageContentMask::PayloadHeader
                | UadpNetworkMessageContentMask::PublisherId
                | UadpNetworkMessageContentMask::WriterGroupId
                | UadpNetworkMessageContentMask::GroupHeader,
            transport_settings: TransportSettings::BrokerWrite({
                BrokerWriterGroupTransportDataType {
                    queue_name: topic.clone(),
                    authentication_profile_uri: UAString::null(),
                    requested_delivery_guarantee: *qos,
                    resource_uri: UAString::null(),
                }
            }),
            ordering: DataSetOrderingType::Undefined,
        }
    }

    /// DataSetOrderingType::Undefined =>  The ordering of DataSetMessages is not specified.
    /// DataSetOrderingType::AscendingWriterId => DataSetMessages are ordered ascending by the value of their corresponding DataSetWriterIds.
    /// DataSetOrderingType::AscendingWriterIdSingle => DataSetMessages are ordered ascending by the value of their corresponding DataSetWriterIds and only one DataSetMessage is sent per NetworkMessage.
    pub fn set_ordering(&mut self, ordering: DataSetOrderingType) -> &mut Self {
        self.ordering = ordering;
        self
    }

    pub fn set_name(&mut self, name: UAString) -> &mut Self {
        self.name = name;
        self
    }

    pub fn set_keep_alive_time(&mut self, keep_alive_time: f64) -> &mut Self {
        self.keep_alive_time = keep_alive_time;
        self
    }

    pub fn set_publish_interval(&mut self, publish_interval: Duration) -> &mut Self {
        self.publish_interval = publish_interval;
        self
    }

    pub fn set_group_id(&mut self, id: u16) -> &mut Self {
        self.group_id = id;
        self
    }

    pub fn set_message_setting(&mut self, mask: UadpNetworkMessageContentMask) -> &mut Self {
        self.message_settings = mask;
        self
    }

    pub fn build(&self) -> WriterGroup {
        WriterGroup {
            name: self.name.clone(),
            enabled: false,
            writer_group_id: self.group_id,
            publishing_interval: self.publish_interval,
            keep_alive_time: self.keep_alive_time,
            message_settings: self.message_settings,
            writer: Vec::new(),
            sequence_no: 0,
            group_version: generate_version_time(),
            last_action: DateTime::null(),
            transport_settings: self.transport_settings.clone(),
            // MTU for ethernet
            max_network_message_size: 1472,
            priority: 126,
            ordering: self.ordering,
        }
    }
}

impl Default for WriterGroupBuilder {
    fn default() -> Self {
        Self::new()
    }
}
