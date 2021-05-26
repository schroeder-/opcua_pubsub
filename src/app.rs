// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use crate::{
    connection::{ConnectionAction, PubSubConnection, PubSubConnectionId},
    dataset::PublishedDataSetId,
    prelude::PublishedDataSet,
};
use core::panic;
use log::error;
use opcua_types::status_code::StatusCode;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::{
    sync::{Arc, RwLock},
    thread::JoinHandle,
};

/// Represents the PubSubApplication
/// TopLevel of PubSub
pub struct PubSubApp {
    /// connections controlled by this object
    connections: Vec<PubSubConnection>,
    /// the datasets contained in pubsub app
    datasets: Vec<PublishedDataSet>,

    /// Id counter gets incremented with each new connection
    con_id: u32,
    // Id counter gets incremented with each dataset
    dataset_id: u32,
}

impl PubSubApp {
    pub fn new() -> Self {
        PubSubApp {
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
    pub fn remove_connection(
        &mut self,
        connection_id: PubSubConnectionId,
    ) -> Result<(), StatusCode> {
        if let Some(idx) = self
            .connections
            .iter()
            .position(|c| c.id() == &connection_id)
        {
            let con = &self.connections[idx];
            con.disable();
            self.connections.remove(idx);
            Ok(())
        } else {
            error!("removeing unkown connection: {:?}", connection_id);
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
            error!("removeing unkown connection: {:?}", dataset_id);
            Err(StatusCode::BadInvalidArgument)
        }
    }

    /// runs pubs in a new thread
    /// currently 2 threads per connection are used
    /// this will change happen under the hood
    pub fn run_thread(pubsub: Arc<RwLock<Self>>) -> Vec<JoinHandle<()>> {
        {
            let ps = pubsub.write().unwrap();
            for con in ps.connections.iter() {
                con.enable();
            }
        }
        let mut vec = Vec::new();
        let ids: Vec<PubSubConnectionId> = {
            let ps = pubsub.write().unwrap();
            ps.connections.iter().map(|c| c.id().clone()).collect()
        };
        // @Hack implement a correcter loop
        // for new its ok to get startet 2 threads per connection
        let (input_tx, input_rx): (Sender<ConnectionAction>, Receiver<ConnectionAction>) =
            mpsc::channel();
        for id in ids.iter() {
            let receiver = {
                let p = pubsub.write().unwrap();
                let c = p.get_connection(id).unwrap();
                c.create_receiver().expect("Error creating reciver")
            };
            let id1 = id.clone();
            let inp = input_tx.clone();
            vec.push(std::thread::spawn(move || {
                receiver.recv_to_channel(inp, &id1);
            }));
            let inst = pubsub.clone();
            let id2 = id.clone();
            vec.push(std::thread::spawn(move || loop {
                let delay = {
                    let mut ps = inst.write().unwrap();
                    ps.drive_writer(&id2)
                };
                std::thread::sleep(delay);
            }));
        }
        vec.push(std::thread::spawn(move || loop {
            match input_rx.recv() {
                Ok(action) => match action {
                    ConnectionAction::GotUadp(id, topic, msg) => {
                        let ps = pubsub.write().unwrap();
                        let con = ps.get_connection(&id).unwrap();
                        con.handle_message(&topic.into(), msg);
                    }
                    ConnectionAction::DoLoop(_id) => {}
                },
                Err(err) => panic!("error {}", err),
            }
        }));
        return vec;
    }
    pub fn drive_writer(&mut self, id: &PubSubConnectionId) -> std::time::Duration {
        let con = self.connections.iter_mut().find(|x| x.id() == id);
        if let Some(con) = con {
            con.drive_writer(&self.datasets)
        } else {
            panic!("Shoudln't happen")
        }
    }
    /// runs the pubsub forever
    pub fn run(self) {
        let s = Arc::new(RwLock::new(self));
        let ths = Self::run_thread(s.clone());
        for th in ths {
            th.join().unwrap();
        }
        let s = s.write().unwrap();
        for con in s.connections.iter() {
            con.disable();
        }
    }
}

impl Default for PubSubApp {
    fn default() -> Self {
        PubSubApp::new()
    }
}
