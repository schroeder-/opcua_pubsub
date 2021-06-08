// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use chrono::{Duration, Utc};
use opcua_server::prelude::StatusCode;
use opcua_types::{
    ConfigurationVersionDataType, DataSetMetaDataType, EndpointDescription, UAString, Variant,
};
use std::convert::TryFrom;

use crate::{
    message::{
        InformationType, ResponseType, UadpDataSetMetaDataResp, UadpDataSetWriterResp,
        UadpDiscoveryRequest, UadpDiscoveryResponse, UadpNetworkMessage, UadpPayload,
        UadpPublisherEndpointsResp,
    },
    network::TransportSettings,
    prelude::PublishedDataSet,
    writer::{DataSetWriter, WriterGroup},
};

/// Handles all discovery request because they have to be delayed from 100..500ms to limit traffic
pub struct DiscoveryHandler {
    send_next_endpoint: Option<chrono::DateTime<Utc>>,
    send_next_dataset_writer_groups: Vec<(u16, chrono::DateTime<Utc>)>,
    send_next_dataset_meta: Vec<(u16, chrono::DateTime<Utc>)>,
    delay: Duration,
    discovery_network_message_no: u16,
}

impl DiscoveryHandler {
    pub fn new() -> Self {
        DiscoveryHandler {
            send_next_endpoint: None,
            send_next_dataset_writer_groups: Vec::new(),
            send_next_dataset_meta: Vec::new(),
            delay: Duration::milliseconds(300),
            discovery_network_message_no: 0,
        }
    }

    /// Store request and groupe them
    pub fn handle_request(&mut self, req: &UadpDiscoveryRequest) {
        // the specs say that discovery responses should be delayed 100-500 ms
        // and grouped into one if multiple arrive in this time frame. This is to limitation traffic
        match req.information_type() {
            InformationType::PublisherEndpoints => {
                if self.send_next_endpoint.is_none() {
                    self.send_next_endpoint = Some(Utc::now() + self.delay);
                }
            }
            InformationType::DataSetMetaData => {
                if let Some(ids) = req.dataset_writer_ids() {
                    ids.iter().for_each(|id| {
                        self.send_next_dataset_writer_groups
                            .push((*id, Utc::now() + self.delay))
                    })
                } else {
                    self.send_next_dataset_writer_groups
                        .push((0, Utc::now() + self.delay))
                }
            }
            InformationType::DataSetWriter => {
                if let Some(ids) = req.dataset_writer_ids() {
                    ids.iter().for_each(|id| {
                        self.send_next_dataset_meta
                            .push((*id, Utc::now() + self.delay))
                    })
                }
            }
        }
    }

    /// One discovery endpoint response is pending
    pub fn has_endpoint_response(&self) -> bool {
        let now = Utc::now();
        if let Some(send_next) = self.send_next_endpoint {
            if send_next < now {
                return true;
            }
        }
        false
    }

    /// Metadata has to be send
    pub fn has_meta_response(&self) -> bool {
        let now = Utc::now();
        self.send_next_dataset_meta
            .iter()
            .find(|(_id, dt)| *dt < now)
            .is_some()
    }

    /// Writer configuration has to be send
    pub fn has_writer_response(&self) -> bool {
        let now = Utc::now();
        self.send_next_dataset_writer_groups
            .iter()
            .find(|(_id, dt)| *dt < now)
            .is_some()
    }

    /// Generates Discovery Responses
    pub fn generate_endpoint_response(
        &mut self,
        publisher_id: Variant,
        endps: Option<Vec<EndpointDescription>>,
    ) -> UadpNetworkMessage {
        self.send_next_endpoint = None;
        let mut msg = UadpNetworkMessage::new();
        msg.header.publisher_id = Some(publisher_id);
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
        msg
    }

    /// Generates all pending meta responses
    pub fn generate_meta_responses<'a>(
        &mut self,
        publisher_id: Variant,
        writers: Vec<&'a DataSetWriter>,
        dss: &[PublishedDataSet],
    ) -> Vec<(UadpNetworkMessage, &'a TransportSettings)> {
        let offset = 0_u16;
        let datasets = self
            .send_next_dataset_meta
            .iter()
            .map(|(id, _)| (id, writers.iter().find(|w| w.dataset_writer_id == *id)));
        let ret: Vec<(UadpNetworkMessage, &TransportSettings)> = datasets
            .map(|(id, ds_writer)| {
                let mut msg = UadpNetworkMessage::new();
                msg.header.publisher_id = Some(publisher_id.clone());
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
                let response = UadpDataSetMetaDataResp::new(*id, meta_data, status);
                msg.payload = UadpPayload::DiscoveryResponse(Box::new(UadpDiscoveryResponse::new(
                    InformationType::DataSetMetaData,
                    self.discovery_network_message_no.wrapping_add(offset),
                    ResponseType::DataSetMetaData(response),
                )));
                (msg, transport)
            })
            .collect();
        self.discovery_network_message_no = self
            .discovery_network_message_no
            .wrapping_add(u16::try_from(ret.len()).unwrap_or(0));
        self.send_next_dataset_writer_groups.clear();
        ret
    }
    /// Generates all pending writer settings
    pub fn generate_writer_response<'a>(
        &mut self,
        publisher_id: &Variant,
        writer: Vec<&'a WriterGroup>,
    ) -> Vec<(UadpNetworkMessage, &'a TransportSettings)> {
        // @TODO filter out not used configs
        let ret: Vec<(UadpNetworkMessage, &TransportSettings)> = writer
            .iter()
            .enumerate()
            .map(|(offset, writer_g)| {
                let (cfg, writer) = writer_g.generate_info();
                let mut msg = UadpNetworkMessage::new();
                msg.header.publisher_id = Some(publisher_id.clone());
                let status = vec![StatusCode::Good; writer.len()];
                let response = UadpDataSetWriterResp::new(Some(writer), cfg, Some(status));
                msg.payload = UadpPayload::DiscoveryResponse(Box::new(UadpDiscoveryResponse::new(
                    InformationType::DataSetWriter,
                    self.discovery_network_message_no
                        .wrapping_add(u16::try_from(offset).unwrap_or(0)),
                    ResponseType::DataSetWriter(response),
                )));
                (msg, writer_g.transport_settings())
            })
            .collect();
        self.discovery_network_message_no = self
            .discovery_network_message_no
            .wrapping_add(u16::try_from(ret.len()).unwrap_or(0));
        ret
    }

    pub fn get_next_time(&self, next: std::time::Duration) -> std::time::Duration {
        let now = Utc::now();
        let mut ret = next;
        if let Some(enp) = self.send_next_endpoint{
            ret = (enp - now).to_std().unwrap_or(ret).min(ret);
        };
        let meta = self.send_next_dataset_meta.iter().min_by(|(_, a), (_, b)| a.cmp(b));
        let writer = self.send_next_dataset_writer_groups.iter().min_by(|(_, a), (_, b)| a.cmp(b));
        if let Some((_, dt)) = meta{
            ret = (*dt - now).to_std().unwrap_or(ret).min(ret);
        } 
        if let Some((_, dt)) = writer{
            ret = (*dt - now).to_std().unwrap_or(ret).min(ret);
        }
        ret
    }
}

impl Default for DiscoveryHandler {
    fn default() -> Self {
        Self::new()
    }
}
