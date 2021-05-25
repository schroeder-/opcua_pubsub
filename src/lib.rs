// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
pub mod address_space;
pub mod app;
pub mod callback;
pub mod connection;
pub mod constants;
pub mod dataset;
pub mod message;
mod network;
pub mod reader;
pub mod writer;
pub mod prelude {
    pub use crate::address_space::{PubSubDataSource, SimpleAddressSpace};
    pub use crate::callback::OnReceiveValueFn;
    pub use crate::connection::{PubSubConnection, PubSubConnectionBuilder};
    pub use crate::constants::*;
    pub use crate::dataset::{
        DataSetFieldBuilder, DataSetTargetBuilder, PubSubFieldMetaDataBuilder, PublishedDataSet,
        UpdateTarget,
    };
    pub use crate::network::configuration::*;
    pub use crate::reader::{DataSetReader, DataSetReaderBuilder, ReaderGroup};
    pub use crate::writer::{DataSetWriterBuilder, WriterGroupBuilder};
    pub use crate::{
        DataSetFieldContentFlags, UadpDataSetMessageContentFlags, UadpNetworkMessageContentFlags,
    };
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
#[allow(non_upper_case_globals)]

bitflags::bitflags! {
    pub struct DataSetFieldContentFlags: u32 {
         const NONE = DataSetFieldContentMask::None as u32;
         const STATUSCODE = DataSetFieldContentMask::StatusCode as u32;
         const SOURCETIMESTAMP = DataSetFieldContentMask::SourceTimestamp as u32;
         const SERVERTIMESTAMP = DataSetFieldContentMask::ServerTimestamp as u32;
         const SOURCEPICOSECONDS = DataSetFieldContentMask::SourcePicoSeconds as u32;
         const SERVERPICOSECONDS = DataSetFieldContentMask::ServerPicoSeconds as u32;
         const RAWDATA = DataSetFieldContentMask::RawData as u32;
    }
}

bitflags::bitflags! {
    pub struct UadpDataSetMessageContentFlags: u32 {
        const NONE = UadpDataSetMessageContentMask::None as u32;
        const TIMESTAMP = UadpDataSetMessageContentMask::Timestamp as u32;
        const PICOSECONDS = UadpDataSetMessageContentMask::PicoSeconds as u32;
        const STATUS = UadpDataSetMessageContentMask::Status as u32;
        const MAJORVERSION = UadpDataSetMessageContentMask::MajorVersion as u32;
        const MINORVERSION = UadpDataSetMessageContentMask::MinorVersion as u32;
        const SEQUENCENUMBER = UadpDataSetMessageContentMask::SequenceNumber as u32;
    }
}

bitflags::bitflags! {
    pub struct UadpNetworkMessageContentFlags: u32 {
        const NONE = UadpNetworkMessageContentMask::None as u32;
        const PUBLISHERID = UadpNetworkMessageContentMask::PublisherId as u32;
        const GROUPHEADER = UadpNetworkMessageContentMask::GroupHeader as u32;
        const WRITERGROUPID = UadpNetworkMessageContentMask::WriterGroupId as u32;
        const GROUPVERSION = UadpNetworkMessageContentMask::GroupVersion as u32;
        const NETWORKMESSAGENUMBER = UadpNetworkMessageContentMask::NetworkMessageNumber as u32;
        const SEQUENCENUMBER = UadpNetworkMessageContentMask::SequenceNumber as u32;
        const PAYLOADHEADER = UadpNetworkMessageContentMask::PayloadHeader as u32;
        const TIMESTAMP = UadpNetworkMessageContentMask::Timestamp as u32;
        const PICOSECONDS = UadpNetworkMessageContentMask::PicoSeconds as u32;
        const DATASETCLASSID = UadpNetworkMessageContentMask::DataSetClassId as u32;
        const PROMOTEDFIELDS = UadpNetworkMessageContentMask::PromotedFields as u32;
    }
}
