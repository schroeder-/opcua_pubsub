// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use crate::dataset::{DataSetTarget, PubSubFieldMetaData};
use log::error;
use opcua_types::{DataValue, NodeId};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Err from DataSource
#[derive(Debug)]
pub struct DataSourceErr;
/// Shorthand
pub type PubSubDataSourceT = dyn PubSubDataSource + Sync + Send;
/// Trait to implement a DataSource from where the DataSetWriter get there values
/// and also where a DataSetSource writes the values to. A DataSetSource is feed from a
/// DatSetReader
pub trait PubSubDataSource {
    /// Reads a variable for DataSetWriter
    fn get_pubsub_value(&self, nid: &NodeId) -> Result<DataValue, DataSourceErr>;
    /// Sets a variable
    fn set_pubsub_value(
        &mut self,
        target: &DataSetTarget,
        dv: DataValue,
        meta: &PubSubFieldMetaData,
    );
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

impl PubSubDataSource for SimpleAddressSpace {
    /// Just get the datavalue or return DataSource
    fn get_pubsub_value(&self, nid: &NodeId) -> Result<DataValue, DataSourceErr> {
        match self.node_map.get(nid) {
            Some(v) => Ok(v.clone()),
            None => Err(DataSourceErr),
        }
    }
    /// Just set the datavalue for nid
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
/// Makes the AddressSpace usable as Datasource for PubSub
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
