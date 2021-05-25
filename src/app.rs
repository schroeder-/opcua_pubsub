// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use crate::connection::{PubSubConnection, PubSubConnectionId};
use log::error;
use opcua_types::status_code::StatusCode;

/// Represents the PubSubApplication
/// TopLevel of PubSub
pub struct PubSubApp {
    /// connections controlled by this object
    connections: Vec<PubSubConnection>,
    /// Id counter gets incremented with each new connection
    con_id: u32,
}

impl PubSubApp {
    pub fn new() -> Self {
        PubSubApp {
            connections: Vec::new(),
            // start with 1 because zero => not set
            con_id: 1,
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
}

impl Default for PubSubApp {
    fn default() -> Self {
        PubSubApp::new()
    }
}
