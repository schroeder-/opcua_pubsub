// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use crate::{
    connection::{PubSubConnection, PubSubConnectionId},
    dataset::PublishedDataSetId,
    prelude::{PubSubDataSource, PublishedDataSet},
    until::decode_extension,
};
use core::panic;
use log::{error, info, warn};
use opcua_types::{
    status_code::StatusCode, BinaryEncoder, DecodingOptions, ExtensionObject, ObjectId,
    PubSubConfigurationDataType, UABinaryFileDataType, Variant,
};
use std::sync::{Arc, RwLock};
use std::{fs, path::Path};
use tokio::task::JoinHandle;

/// Represents the PubSubApplication
/// TopLevel of PubSub
pub struct PubSubApp {
    /// connections controlled by this object
    connections: Vec<PubSubConnection>,
    /// the datasets contained in pubsub app
    datasets: Vec<PublishedDataSet>,
    /// Id counter gets incremented with each new connection
    con_id: u32,
    /// Id counter gets incremented with each dataset
    dataset_id: u32,
}

impl PubSubApp {
    pub const fn new() -> Self {
        Self {
            connections: Vec::new(),
            datasets: Vec::new(),
            // start with 1 because zero => not set
            con_id: 1,
            dataset_id: 1,
        }
    }

    /// Adds a connection and return its Id
    pub fn add_connection(
        &mut self,
        mut connection: PubSubConnection,
    ) -> Result<PubSubConnectionId, StatusCode> {
        connection.is_valid()?;
        self.con_id += 1;
        let id = PubSubConnectionId(self.con_id);
        connection.set_id(id.clone());
        self.connections.push(connection);
        Ok(id)
    }
    /// Get the reference to connection
    pub fn get_connection(&self, id: &PubSubConnectionId) -> Option<&PubSubConnection> {
        self.connections.iter().find(|p| p.id() == id)
    }
    /// Get the reference to connection
    pub fn get_connection_mut(&mut self, id: &PubSubConnectionId) -> Option<&mut PubSubConnection> {
        self.connections.iter_mut().find(|p| p.id() == id)
    }

    /// Removes a connection from its id
    /// If not found returns Err(BadInvalidArgument)
    pub async fn remove_connection(
        &mut self,
        connection_id: PubSubConnectionId,
    ) -> Result<(), StatusCode> {
        if let Some(idx) = self
            .connections
            .iter()
            .position(|c| c.id() == &connection_id)
        {
            let con = &self.connections[idx];
            con.disable().await;
            self.connections.remove(idx);
            Ok(())
        } else {
            error!("removing unknown connection: {:?}", connection_id);
            Err(StatusCode::BadInvalidArgument)
        }
    }

    /// Add a new PublishedDataset
    pub fn add_dataset(
        &mut self,
        mut pds: PublishedDataSet,
    ) -> Result<PublishedDataSetId, StatusCode> {
        self.con_id += 1;
        let id = PublishedDataSetId(self.dataset_id);
        pds.set_id(id.clone());
        self.datasets.push(pds);
        Ok(id)
    }

    /// Get the reference to connection
    pub fn get_dataset(&mut self, id: &PublishedDataSetId) -> Option<&mut PublishedDataSet> {
        self.datasets.iter_mut().find(|p| p.id() == id)
    }

    /// Removes a connection from its id
    /// If not found returns Err(BadInvalidArgument)
    pub fn remove_dataset(&mut self, dataset_id: PublishedDataSetId) -> Result<(), StatusCode> {
        if let Some(idx) = self.datasets.iter().position(|c| c.id() == &dataset_id) {
            self.datasets.remove(idx);
            Ok(())
        } else {
            error!("removing unknown connection: {:?}", dataset_id);
            Err(StatusCode::BadInvalidArgument)
        }
    }

    pub fn run_thread(_pubsub: Arc<RwLock<Self>>) -> std::thread::JoinHandle<()> {
        todo!("Impl");
    }

    /// Runs Code Async
    pub async fn run_async(pubsub: Arc<tokio::sync::RwLock<Self>>) -> Vec<JoinHandle<()>> {
        {
            let pubsub = pubsub.clone();
            let mut ps = pubsub.write().await;
            for con in ps.connections.iter_mut() {
                con.enable().await.unwrap();
            }
            let dt = Arc::new(tokio::sync::Mutex::new(ps.datasets.to_vec()));
            //@TODO improve
            ps.connections
                .iter_mut()
                .map(|con| {
                    let ds = dt.clone();
                    tokio::task::spawn(async {
                        con.run_loop(ds).await;
                    })
                })
                .collect()
        }
    }
    pub async fn drive_writer(&mut self, id: &PubSubConnectionId) -> std::time::Duration {
        let con = self.connections.iter_mut().find(|x| x.id() == id);
        if let Some(con) = con {
            con.drive_writer(&self.datasets).await
        } else {
            panic!("shouldn't happen")
        }
    }
    /// runs the pubsub forever
    pub fn run(self) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let s = Arc::new(tokio::sync::RwLock::new(self));
        let future = Self::run_async(s.clone());
        let ths = rt.block_on(future);
        for th in ths {
            rt.block_on(th).unwrap();
        }
        rt.block_on(async {
            let s = s.write().await;
            for con in s.connections.iter() {
                con.disable().await;
            }
        });
    }

    ///  Loads configuration for pubsub from binary file
    pub fn new_from_bin_file(
        path: &Path,
        ds: Option<Arc<RwLock<PubSubDataSource>>>,
    ) -> Result<Self, StatusCode> {
        let buffer = match fs::read(path) {
            Ok(b) => b,
            Err(err) => {
                error!("Error reading file {} - {}", path.display(), err);
                return Err(StatusCode::BadInvalidArgument);
            }
        };
        Self::new_from_binary(&mut buffer.as_slice(), ds)
    }
    /// Load from PubSubConfiguration DataType
    fn new_from_cfg(
        cfg: PubSubConfigurationDataType,
        ds: Option<Arc<RwLock<PubSubDataSource>>>,
    ) -> Result<Self, StatusCode> {
        let mut obj = Self::new();
        obj.update(cfg, ds)?;
        Ok(obj)
    }

    pub fn shutdown(&mut self){
        self.connections.clear();
    }   

    /// Loads configuration form binary data
    pub fn new_from_binary<Stream: std::io::Read>(
        buf: &mut Stream,
        ds: Option<Arc<RwLock<PubSubDataSource>>>,
    ) -> Result<Self, StatusCode> {
        // Read as extension object
        
        let dec_opts = DecodingOptions::default();
        let eobj = match ExtensionObject::decode(buf, &dec_opts) {
            Ok(b) => b,
            Err(err) => {
                error!("Error invalid data in configuration");
                return Err(err);
            }
        };
        let bin = match decode_extension::<UABinaryFileDataType>(
            &eobj,
            ObjectId::UABinaryFileDataType_Encoding_DefaultBinary,
            &dec_opts,
        ) {
            Ok(bin) => bin,
            Err(err) => {
                error!("Couldn't read UABinaryFileDataType");
                return Err(err);
            }
        };
        if bin.body.is_array() {
            error!("Only one PubSubConnection per file is allowed!");
            Err(StatusCode::BadNotImplemented)
        } else {
            match bin.body {
                Variant::ExtensionObject(ex) => {
                    match decode_extension::<PubSubConfigurationDataType>(
                        &ex,
                        ObjectId::PubSubConfigurationDataType_Encoding_DefaultBinary,
                        &dec_opts,
                    ) {
                        Ok(config) => Self::new_from_cfg(config, ds),
                        Err(err) => {
                            error!("BinaryFileDataType body doesn't contain ExtensionObject containing PubSubConnectionConfig!");
                            Err(err)
                        }
                    }
                }
                _ => {
                    error!(
                        "File doesn't contain ExtensionObject containing PubSubConnectionConfig!"
                    );
                    Err(StatusCode::BadTypeMismatch)
                }
            }
        }
    }

    /// Update PubSubConfiguration
    fn update(
        &mut self,
        cfg: PubSubConfigurationDataType,
        ds: Option<Arc<RwLock<PubSubDataSource>>>,
    ) -> Result<(), StatusCode> {
        if let Some(pds_cfgs) = cfg.published_data_sets {
            for pds in pds_cfgs {
                if let Some(v) = self.datasets.iter_mut().find(|x| x.name() == pds.name) {
                    v.update(&pds)?;
                } else {
                    self.datasets.push(PublishedDataSet::from_cfg(&pds)?);
                }
            }
        }
        if let Some(con_cfg) = cfg.connections {
            for con in con_cfg {
                if let Some(v) = self
                    .connections
                    .iter_mut()
                    .find(|x| x.publisher_id() == &con.publisher_id)
                {
                    v.update(&con)?;
                } else {
                    self.add_connection(PubSubConnection::from_cfg(&con, ds.clone())?)?;
                }
            }
        }
        Ok(())
    }
    /// Generates the configuration to save to file or build an a DataSet model
    pub fn generate_cfg(&self) -> Result<PubSubConfigurationDataType, StatusCode> {
        let mut pds = Vec::new();
        for ds in self.datasets.iter() {
            pds.push(ds.generate_cfg()?);
        }
        let mut cons = Vec::new();
        for c in self.connections.iter() {
            cons.push(c.generate_cfg()?);
        }
        Ok(PubSubConfigurationDataType {
            published_data_sets: Some(pds),
            connections: Some(cons),
            enabled: true,
        })
    }

    /// Save PubSubConfiguration to writer
    pub fn save_configuration<Stream: std::io::Write>(
        &self,
        stream: &mut Stream,
    ) -> Result<usize, StatusCode> {
        let cfg = self.generate_cfg()?;
        let eobj = ExtensionObject::from_encodable(
            ObjectId::PubSubConfigurationDataType_Encoding_DefaultBinary,
            &cfg,
        );
        let bin = UABinaryFileDataType {
            namespaces: None,
            structure_data_types: None,
            enum_data_types: None,
            simple_data_types: None,
            schema_location: "en".into(),
            file_header: None,
            body: eobj.into(),
        };
        let ebin = ExtensionObject::from_encodable(
            ObjectId::UABinaryFileDataType_Encoding_DefaultBinary,
            &bin,
        );
        ebin.encode(stream)
    }
}

impl Default for PubSubApp {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for PubSubApp{
    fn drop(&mut self) {
        // Drop all connections
        self.connections.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    #[tokio::test]
    async fn test_binary_config() -> Result<(), StatusCode> {
        std::env::set_var("RUST_OPCUA_LOG", "trace");
        opcua_console_logging::init();
        let data = include_bytes!("../test_data/test_publisher.bin");
        let mut c = Cursor::new(data);
        let mut p  = PubSubApp::new_from_binary(&mut c, None)?;
        let cfg = p.generate_cfg()?;
        info!("{}", cfg.enabled);
        p.shutdown();
        Ok(())
    }
}
