// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use crate::message::{UadpDataSetMessage, UadpGroupHeader, UadpMessageType, UadpNetworkMessage};
use crate::pubdataset::{generate_version_time, DataSetInfo, Promoted, PublishedDataSet};
use crate::{
    DataSetFieldContentFlags, UadpDataSetMessageContentFlags, UadpNetworkMessageContentFlags,
};
use chrono::Utc;
use opcua_types::status_code::StatusCode;
use opcua_types::string::UAString;
use opcua_types::DateTime;
use opcua_types::Duration;
use opcua_types::Guid;
use opcua_types::{ConfigurationVersionDataType, DataValue, Variant};

pub struct DataSetWriterBuilder {
    name: UAString,
    dataset_writer_id: u16,
    field_content_mask: DataSetFieldContentFlags,
    message_content_mask: UadpDataSetMessageContentFlags,
    key_frame_count: u32,
    dataset_name: UAString,
}

impl DataSetWriterBuilder {
    pub fn new(pds: &PublishedDataSet) -> Self {
        DataSetWriterBuilder {
            name: "DataSetWriter".into(),
            dataset_writer_id: 12345,
            field_content_mask: DataSetFieldContentFlags::NONE,
            key_frame_count: 10,
            message_content_mask: UadpDataSetMessageContentFlags::MAJORVERSION
                | UadpDataSetMessageContentFlags::MINORVERSION
                | UadpDataSetMessageContentFlags::TIMESTAMP,
            dataset_name: pds.name.clone(),
        }
    }

    pub fn set_name<'a>(&'a mut self, name: UAString) -> &'a mut Self {
        self.name = name;
        self
    }

    pub fn set_dataset_writer_id<'a>(&'a mut self, id: u16) -> &'a mut Self {
        self.dataset_writer_id = id;
        self
    }

    pub fn set_content_mask<'a>(&'a mut self, mask: DataSetFieldContentFlags) -> &'a mut Self {
        self.field_content_mask = mask;
        self
    }

    pub fn set_message_setting<'a>(
        &'a mut self,
        mask: UadpDataSetMessageContentFlags,
    ) -> &'a mut Self {
        self.message_content_mask = mask;
        self
    }

    pub fn set_key_frame_count<'a>(&'a mut self, key_frame_count: u32) -> &'a mut Self {
        self.key_frame_count = key_frame_count;
        self
    }

    pub fn build(&self) -> DataSetWriter {
        DataSetWriter {
            name: self.name.clone(),
            dataset_writer_id: self.dataset_writer_id,
            field_content_mask: self.field_content_mask,
            key_frame_count: self.key_frame_count,
            message_content_mask: self.message_content_mask,
            delta_frame_counter: 0,
            config_version: ConfigurationVersionDataType {
                major_version: 0,
                minor_version: 0,
            },
            sequence_no: 0,
            dataset_name: self.dataset_name.clone(),
        }
    }
}

pub struct DataSetWriter {
    pub name: UAString,
    pub dataset_writer_id: u16,
    pub dataset_name: UAString,
    field_content_mask: DataSetFieldContentFlags,
    message_content_mask: UadpDataSetMessageContentFlags,
    key_frame_count: u32,
    delta_frame_counter: u32,
    config_version: ConfigurationVersionDataType,
    sequence_no: u16,
}

impl DataSetWriter {
    pub fn generate_header(&self, msg: &mut UadpDataSetMessage) {
        msg.header.valid = true;
        if self
            .message_content_mask
            .contains(UadpDataSetMessageContentFlags::MAJORVERSION)
        {
            msg.header.cfg_major_version = Some(self.config_version.major_version);
        }
        if self
            .message_content_mask
            .contains(UadpDataSetMessageContentFlags::MINORVERSION)
        {
            msg.header.cfg_minor_version = Some(self.config_version.minor_version);
        }
        if self
            .message_content_mask
            .contains(UadpDataSetMessageContentFlags::PICOSECONDS)
        {
            // Pico seconds are not supported
            msg.header.pico_seconds = Some(0);
        }
        if self
            .message_content_mask
            .contains(UadpDataSetMessageContentFlags::SEQUENCENUMBER)
        {
            msg.header.sequence_no = Some(self.sequence_no);
        }
        if self
            .message_content_mask
            .contains(UadpDataSetMessageContentFlags::STATUS)
        {
            //@TODO what todo with the status code?
            // 0 represents StatusCode Good
            msg.header.status = Some(0_u16);
        }
        if self
            .message_content_mask
            .contains(UadpDataSetMessageContentFlags::TIMESTAMP)
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
            let timediff = chrono::Duration::milliseconds(
                (publishing_interval * self.delta_frame_counter as f64) as i64,
            );
            let offset: DateTime = (Utc::now() - timediff).into();
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
                                d.value.unwrap_or(Variant::StatusCode(
                                    d.status.unwrap_or(StatusCode::BadUnexpectedError),
                                )),
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
            // Generate a DeltaFrame with rawdata
            else if self
                .field_content_mask
                .contains(DataSetFieldContentFlags::RAWDATA)
            {
                None // @TODO Atm raw transport is not supported
            }
            // Generate a deltaframe with dataValue
            else {
                let mut cnt: u16 = 0;
                let mut vec = Vec::new();
                for (_, mut d) in ds {
                    cnt += 1;
                    if let Some(dt) = &d.source_timestamp {
                        if dt.ticks() > offset.ticks() {
                            if !self
                                .field_content_mask
                                .contains(DataSetFieldContentFlags::SOURCETIMESTAMP)
                            {
                                d.server_timestamp = None;
                            }
                            if !self
                                .field_content_mask
                                .contains(DataSetFieldContentFlags::SERVERTIMESTAMP)
                            {
                                d.source_timestamp = None;
                            }
                            if !self
                                .field_content_mask
                                .contains(DataSetFieldContentFlags::STATUSCODE)
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
                    let v = d.value.unwrap_or(Variant::StatusCode(
                        d.status.unwrap_or(StatusCode::BadUnexpectedError),
                    ));
                    vec.push(v);
                }
                Some(UadpMessageType::KeyFrameVariant(vec))
            }
            // Generate Keyframe with raw value
            else if self
                .field_content_mask
                .contains(DataSetFieldContentFlags::RAWDATA)
            {
                None // @TODO Not supported at the moment
            }
            // Generate KeyFram with DataValue
            else {
                let mut vec = Vec::new();
                for (_, mut d) in ds {
                    if !self
                        .field_content_mask
                        .contains(DataSetFieldContentFlags::SOURCETIMESTAMP)
                    {
                        d.server_timestamp = None;
                    }
                    if !self
                        .field_content_mask
                        .contains(DataSetFieldContentFlags::SERVERTIMESTAMP)
                    {
                        d.source_timestamp = None;
                    }
                    if !self
                        .field_content_mask
                        .contains(DataSetFieldContentFlags::STATUSCODE)
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
    message_settings: UadpNetworkMessageContentFlags,
    sequence_no: u16,
    writer: Vec<DataSetWriter>,
    group_version: u32,
    last_action: DateTime,
}

impl WriterGroup {
    // Check if action is required
    pub fn tick(&mut self) -> bool {
        //@TODO check keepalive
        // Convert publishing_interval from Milliseconds + fraction to 100 nano Seconds
        (self.last_action.ticks() + (self.publishing_interval * 1000.0) as i64)
            < DateTime::now().ticks()
    }

    /// Calculate next time the writer has to write data
    pub fn next_tick(&self) -> std::time::Duration {
        // calc point in 100ns where next action takes place
        let next = self.last_action.ticks() + (self.publishing_interval * 1000.0) as i64;
        std::time::Duration::from_micros(((next - DateTime::now().ticks()) / 10) as u64)
    }

    pub fn generate_message<T: DataSetInfo>(
        &mut self,
        network_no: u16,
        publisher_id: &Variant,
        ds: &T,
    ) -> Option<UadpNetworkMessage> {
        //@TODO do keep alive
        let mut message = UadpNetworkMessage::new();
        if self
            .message_settings
            .contains(UadpNetworkMessageContentFlags::DATASETCLASSID)
        {
            message.header.dataset_class_id = Some(Guid::null());
        }
        if self
            .message_settings
            .contains(UadpNetworkMessageContentFlags::PUBLISHERID)
        {
            message.header.publisher_id = Some(publisher_id.clone());
        }
        if self
            .message_settings
            .contains(UadpNetworkMessageContentFlags::GROUPHEADER)
        {
            let ver = if self
                .message_settings
                .contains(UadpNetworkMessageContentFlags::GROUPVERSION)
            {
                Some(self.group_version)
            } else {
                None
            };
            let net_no = if self
                .message_settings
                .contains(UadpNetworkMessageContentFlags::NETWORKMESSAGENUMBER)
            {
                Some(network_no)
            } else {
                None
            };
            let seq_no = if self
                .message_settings
                .contains(UadpNetworkMessageContentFlags::SEQUENCENUMBER)
            {
                Some(self.sequence_no)
            } else {
                None
            };
            let wg_id = if self
                .message_settings
                .contains(UadpNetworkMessageContentFlags::WRITERGROUPID)
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
            .contains(UadpNetworkMessageContentFlags::PICOSECONDS)
        {
            message.picoseconds = Some(0);
        }

        if self
            .message_settings
            .contains(UadpNetworkMessageContentFlags::TIMESTAMP)
        {
            message.timestamp = Some(DateTime::now());
        }
        let sz = self.writer.len();
        for w in self.writer.iter_mut() {
            let vals = ds.collect_values(&w.dataset_name);
            // Only send Promoted Fields if 1 Dataset is send
            if sz == 1 {
                if self
                    .message_settings
                    .contains(UadpNetworkMessageContentFlags::PROMOTEDFIELDS)
                {
                    for (promoted, val) in vals.iter() {
                        if promoted.0 {
                            if let Some(v) = &val.value {
                                message.promoted_fields.push(v.clone());
                            }
                        }
                    }
                }
            }
            let cfg = ds.get_config_version(&w.dataset_name);
            let dataset = w.generate_message(vals, &cfg, self.publishing_interval);
            if let Some(ds) = dataset {
                message.dataset.push(ds);
                if self
                    .message_settings
                    .contains(UadpNetworkMessageContentFlags::PAYLOADHEADER)
                {
                    message.dataset_payload.push(w.dataset_writer_id);
                }
            }
        }
        // don't send a message with empty dataset
        if message.dataset.is_empty() {
            None
        } else {
            self.sequence_no = self.sequence_no.wrapping_add(1);
            Some(message)
        }
    }

    pub fn add_dataset_writer(&mut self, dsw: DataSetWriter) {
        self.group_version = generate_version_time();
        self.writer.push(dsw);
    }
}

pub struct WriterGroupBuilder {
    name: UAString,
    group_id: u16,
    publish_interval: Duration,
    keep_alive_time: f64,
    message_settings: UadpNetworkMessageContentFlags,
}

impl WriterGroupBuilder {
    pub fn new() -> Self {
        WriterGroupBuilder {
            name: "WriterGroup".into(),
            group_id: 12345,
            publish_interval: 1000.0,
            keep_alive_time: 10000.0,
            message_settings: UadpNetworkMessageContentFlags::PAYLOADHEADER
                | UadpNetworkMessageContentFlags::PUBLISHERID
                | UadpNetworkMessageContentFlags::WRITERGROUPID
                | UadpNetworkMessageContentFlags::GROUPHEADER,
        }
    }

    pub fn set_name<'a>(&'a mut self, name: UAString) -> &'a mut Self {
        self.name = name;
        self
    }

    pub fn set_keep_alive_time<'a>(&'a mut self, keep_alive_time: f64) -> &'a mut Self {
        self.keep_alive_time = keep_alive_time;
        self
    }

    pub fn set_publish_interval<'a>(&'a mut self, publish_interval: Duration) -> &'a mut Self {
        self.publish_interval = publish_interval;
        self
    }

    pub fn set_group_id<'a>(&'a mut self, id: u16) -> &'a mut Self {
        self.group_id = id;
        self
    }

    pub fn set_message_setting<'a>(
        &'a mut self,
        mask: UadpNetworkMessageContentFlags,
    ) -> &'a mut Self {
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
        }
    }
}
