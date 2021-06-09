// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
#![warn(clippy::all)] //, clippy::pedantic, clippy::nursery)]

pub mod address_space;
pub mod app;
pub mod callback;
pub mod connection;
pub mod constants;
pub mod dataset;
mod discovery;
pub mod message;
mod network;
pub mod reader;
pub mod until;
pub mod writer;
pub mod prelude {
    pub use crate::address_space::{PubSubDataSource, SimpleAddressSpace};
    pub use crate::app::PubSubApp;
    pub use crate::callback::*;
    pub use crate::connection::{PubSubConnection, PubSubConnectionBuilder};
    pub use crate::constants::*;
    pub use crate::dataset::{
        DataSetFieldBuilder, DataSetTargetBuilder, PubSubFieldMetaDataBuilder, PublishedDataSet,
        UpdateTarget,
    };
    pub use crate::network::configuration::*;
    pub use crate::reader::{DataSetReader, DataSetReaderBuilder, ReaderGroup};
    pub use crate::until;
    pub use crate::writer::{DataSetWriterBuilder, WriterGroupBuilder};
    pub use opcua_types::status_code::StatusCode;
    pub use opcua_types::string::UAString;
    pub use opcua_types::{
        BrokerTransportQualityOfService, DataSetFieldContentMask, DataTypeId, DataValue, DateTime,
        NodeId, OverrideValueHandling, UadpDataSetMessageContentMask,
        UadpNetworkMessageContentMask, Variant,
    };
}

use opcua_types::DataSetFieldContentMask;
use opcua_types::UadpDataSetMessageContentMask;
use opcua_types::UadpNetworkMessageContentMask;
