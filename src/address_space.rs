// OPC UA Pubsub implementation  node_id: (), attribute_id: (), index_range: (), data_encoding: () for Ru node_id: (), attribute_id: (), index_range: (), data_encoding: () st
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use crate::dataset::{DataSetTarget, PubSubFieldMetaData};
use log::{error, trace};
use opcua_types::{
    status_code::StatusCode, DataSetFieldFlags, DataValue, DateTime, EventFilter, FieldMetaData,
    Guid, NodeId, Variant, VariantTypeId,
};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{Arc, RwLock};

/// Trait to implement a DataSource from where the DataSetWriter get there values
/// and also where a DataSetSource writes the values to. A DataSetSource is feed from a
/// DatSetReader
pub trait DataSource {
    /// Reads a variable for DataSetWriter
    fn get_pubsub_values(&mut self, nid: &[NodeId]) -> Vec<Result<DataValue, StatusCode>>;
    /// Sets a variable
    fn set_pubsub_values(&mut self, data: Vec<(&DataSetTarget, DataValue, &PubSubFieldMetaData)>);

    /// Get metadata for Variable
    fn get_pubsub_meta(&mut self, nid: &NodeId) -> Result<FieldMetaData, StatusCode>;
    /// Gets events since before with EventFilter
    fn get_events(
        &self,
        nid: &NodeId,
        ef: &EventFilter,
        before: DateTime,
    ) -> Result<Vec<Vec<Variant>>, StatusCode>;
}

/// Wraps the Datasource
pub struct PubSubDataSource {
    source: Arc<RwLock<dyn DataSource + Sync + Send>>,
}

impl PubSubDataSource {
    pub fn new(source: Arc<RwLock<dyn DataSource + Sync + Send>>) -> Self {
        Self { source }
    }

    pub fn new_arc(source: Arc<RwLock<dyn DataSource + Sync + Send>>) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self::new(source)))
    }

    /// Reads a variables for DataSetWriter
    pub fn get_pubsub_values(&mut self, nid: &[NodeId]) -> Vec<Result<DataValue, StatusCode>> {
        let mut source = self.source.write().unwrap();
        source.get_pubsub_values(nid)
    }
    /// Sets a variables
    pub fn set_pubsub_values(
        &mut self,
        data: Vec<(&DataSetTarget, DataValue, &PubSubFieldMetaData)>,
    ) {
        let mut source = self.source.write().unwrap();
        source.set_pubsub_values(data)
    }

    /// Get metadata for Variable
    pub fn get_pubsub_meta(&mut self, nid: &NodeId) -> Result<FieldMetaData, StatusCode> {
        let mut source = self.source.write().unwrap();
        source.get_pubsub_meta(nid)
    }
    /// Gets events since before with EventFilter
    pub fn get_events(
        &self,
        nid: &NodeId,
        ef: &EventFilter,
        before: DateTime,
    ) -> Result<Vec<Vec<Variant>>, StatusCode> {
        let source = self.source.write().unwrap();
        source.get_events(nid, ef, before)
    }
}

/// Simple implementation mimic the opcua_server::address_space::AddressSpace.
/// Its used for standalone application as simple solution. For advanced applications
/// implementing the trait PubSubDataSource is recommended.
pub struct SimpleAddressSpace {
    node_map: HashMap<NodeId, DataValue>,
}

impl SimpleAddressSpace {
    /// Generates a empty AddressSpace
    pub fn new() -> Self {
        Self {
            node_map: HashMap::new(),
        }
    }
    /// Sets a value with the NodeId nid
    pub fn set_value(&mut self, nid: &NodeId, dv: DataValue) {
        self.node_map.insert(nid.clone(), dv);
    }
    /// Removes a value
    pub fn remove_value(&mut self, nid: &NodeId) {
        self.node_map.remove(nid);
    }
    /// Reads a value with NodeId nid
    pub fn get_value(&self, nid: &NodeId) -> Option<&DataValue> {
        self.node_map.get(nid)
    }
    /// get a boxed empty addressspace
    pub fn new_arc_lock() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self::new()))
    }
}

impl Default for SimpleAddressSpace {
    fn default() -> Self {
        Self::new()
    }
}

impl DataSource for SimpleAddressSpace {
    /// Just get the datavalue or return DataSource
    fn get_pubsub_values(&mut self, nids: &[NodeId]) -> Vec<Result<DataValue, StatusCode>> {
        nids.iter()
            .map(|n| {
                trace!("SimpleAddressSpace: Get {}", n);
                match self.node_map.get(n) {
                Some(v) => Ok(v.clone()),
                None => Err(StatusCode::BadNodeIdUnknown),
            }})
            .collect()
    }
    /// Just set the datavalue for nid
    fn set_pubsub_values(&mut self, data: Vec<(&DataSetTarget, DataValue, &PubSubFieldMetaData)>) {
        for (target, dv, _) in data {
            let nid = &target.0.target_node_id;
            trace!("SimpleAddressSpace: Update nid: {}, {:?}", nid, dv);
            match &target.update_dv(dv) {
                Ok(res) => {
                    self.node_map.insert(nid.clone(), res.clone());
                }
                Err(err) => error!("Couldn't update variable {} -> {}", nid, err),
            };
        }
    }
    //@TODO implement
    fn get_events(
        &self,
        _nid: &NodeId,
        _ef: &EventFilter,
        _before: DateTime,
    ) -> Result<Vec<Vec<Variant>>, StatusCode> {
        Err(StatusCode::BadNotImplemented)
    }
    //@TODO implement
    fn get_pubsub_meta(&mut self, _nid: &NodeId) -> Result<FieldMetaData, StatusCode> {
        Err(StatusCode::BadNotImplemented)
    }
}

#[cfg(feature = "client-integration")]
use opcua_client::prelude::*;
#[cfg(feature = "client-integration")]
impl DataSource for opcua_client::prelude::Session {
    fn get_pubsub_values(&mut self, nid: &[NodeId]) -> Vec<Result<DataValue, StatusCode>> {
        let v: Vec<_> = nid.iter().map(ReadValueId::from).collect();
        match self.read(&v) {
            Ok(v) => v.into_iter().map(Ok).collect(),
            Err(e) => vec![Err(e)],
        }
    }

    fn set_pubsub_values(&mut self, data: Vec<(&DataSetTarget, DataValue, &PubSubFieldMetaData)>) {
        
        let v: Vec<_> = data
            .into_iter()
            .map(|(target, dv, _)| WriteValue {
                node_id: target.0.target_node_id.clone(),
                attribute_id: target.0.attribute_id,
                index_range: target.0.write_index_range.clone(),
                value: dv,
            })
            .collect();
        if let Err(e) = self.write(&v) {
            error!("WriteValue: {}", e);
        }
    }

    fn get_pubsub_meta(&mut self, nid: &NodeId) -> Result<FieldMetaData, StatusCode> {
        let v = vec![
            ReadValueId {
                node_id: nid.clone(),
                attribute_id: opcua_types::AttributeId::DisplayName as u32,
                index_range: "".into(),
                data_encoding: QualifiedName::null(),
            },
            ReadValueId {
                node_id: nid.clone(),
                attribute_id: opcua_types::AttributeId::Description as u32,
                index_range: "".into(),
                data_encoding: QualifiedName::null(),
            },
            ReadValueId {
                node_id: nid.clone(),
                attribute_id: opcua_types::AttributeId::DataType as u32,
                index_range: "".into(),
                data_encoding: QualifiedName::null(),
            },
            ReadValueId {
                node_id: nid.clone(),
                attribute_id: opcua_types::AttributeId::ValueRank as u32,
                index_range: "".into(),
                data_encoding: QualifiedName::null(),
            },
            ReadValueId {
                node_id: nid.clone(),
                attribute_id: opcua_types::AttributeId::ValueRank as u32,
                index_range: "".into(),
                data_encoding: QualifiedName::null(),
            },
        ];
        match self.read(&v) {
            Err(e) => Err(e),
            Ok(mut v) => {
                let name = if let Variant::String(s) = v.remove(0).value.unwrap_or(Variant::Empty) {
                    s
                } else {
                    "".into()
                };
                let description = if let Variant::LocalizedText(s) =
                    v.remove(0).value.unwrap_or(Variant::Empty)
                {
                    *s
                } else {
                    LocalizedText::null()
                };
                let (data_type, built_in_type) = if let Variant::NodeId(dt) =
                    v.remove(0).value.unwrap_or(Variant::Empty)
                {
                    let built_in_type: u8 = if let Ok(t) = VariantTypeId::try_from(dt.as_ref()) {
                        t.encoding_mask()
                    } else {
                        0
                    };
                    (*dt, built_in_type)
                } else {
                    (NodeId::null(), 0)
                };
                let value_rank =
                    if let Variant::Int32(vr) = v.remove(0).value.unwrap_or(Variant::Empty) {
                        vr
                    } else {
                        -1
                    };
                let array_dimensions: Option<Vec<u32>> =
                    if let Variant::Array(ar) = v.remove(0).value.unwrap_or(Variant::Empty) {
                        if values_are_of_type(&ar.values, VariantTypeId::UInt32) {
                            Some(
                                ar.values
                                    .into_iter()
                                    .filter_map(|x| match x {
                                        Variant::UInt32(x) => Some(x),
                                        _ => None,
                                    })
                                    .collect(),
                            )
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                Ok(FieldMetaData {
                    name,
                    description,
                    field_flags: DataSetFieldFlags::None,
                    built_in_type,
                    data_type,
                    value_rank,
                    array_dimensions,
                    max_string_length: 0,
                    data_set_field_id: Guid::null(),
                    properties: None,
                })
            }
        }
    }
    //@TODO implement
    fn get_events(
        &self,
        _nid: &NodeId,
        _ef: &EventFilter,
        _before: DateTime,
    ) -> Result<Vec<Vec<Variant>>, StatusCode> {
        Err(StatusCode::BadNotImplemented)
    }
}

#[cfg(feature = "server-integration")]
use opcua_server::address_space::AddressSpace;
#[cfg(feature = "server-integration")]
use opcua_server::prelude::NodeBase;

#[cfg(feature = "server-integration")]
/// Makes the AddressSpace usable as Datasource for PubSub
impl DataSource for AddressSpace {
    fn get_pubsub_values(&mut self, nids: &[NodeId]) -> Vec<Result<DataValue, StatusCode>> {
        nids.iter()
            .map(|n| match self.get_variable_value(n) {
                Ok(n) => Ok(n),
                Err(_) => Err(StatusCode::BadNodeIdUnknown),
            })
            .collect()
    }
    /// Just set the datavalue for nid
    fn set_pubsub_values(&mut self, data: Vec<(&DataSetTarget, DataValue, &PubSubFieldMetaData)>) {
        for (target, dv, meta) in data {
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

    fn get_events(
        &self,
        nid: &NodeId,
        ef: &EventFilter,
        before: DateTime,
    ) -> Result<Vec<Vec<Variant>>, StatusCode> {
        if self.find_node(nid).is_some() {
            if let Some(evs) =
                opcua_server::events::event_filter::evaluate(nid, &ef, self, &before.as_chrono(), 0)
            {
                Ok(evs.into_iter().filter_map(|e| e.event_fields).collect())
            } else {
                Ok(Vec::new())
            }
        } else {
            Err(StatusCode::BadNodeIdUnknown)
        }
    }

    fn get_pubsub_meta(&mut self, nid: &NodeId) -> Result<FieldMetaData, StatusCode> {
        if let Some(v) = self.find_variable(nid) {
            let buildin: u8 = if let Ok(t) = VariantTypeId::try_from(&v.data_type()) {
                t.encoding_mask()
            } else {
                0
            };
            Ok(FieldMetaData {
                name: v.display_name().text,
                description: v.description().unwrap_or_default(),
                field_flags: DataSetFieldFlags::None,
                built_in_type: buildin,
                data_type: v.data_type(),
                value_rank: v.value_rank(),
                array_dimensions: v.array_dimensions(),
                max_string_length: 0,
                data_set_field_id: Guid::null(),
                properties: None,
            })
        } else {
            Err(StatusCode::BadNodeIdUnknown)
        }
    }
}
