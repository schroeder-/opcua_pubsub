// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
mod network;
pub mod message;
pub mod connection;
pub mod pubdataset;
pub mod writer;
pub mod reader;
pub mod prelude {
    pub use opcua_types::status_code::StatusCode;
    pub use opcua_types::{NodeId, DataTypeId, DataValue, Variant, DateTime, UadpNetworkMessageContentMask, DataSetFieldContentMask, UadpDataSetMessageContentMask};
    pub use opcua_types::string::UAString;
    pub use crate::connection::{PubSubConnection, PubSubConnectionBuilder};
    pub use crate::connection::{PubSubDataSource, SimpleAddressSpace};
    pub use crate::pubdataset::{PublishedDataSet, DataSetFieldBuilder, PubSubFieldMetaDataBuilder};
    pub use crate::writer::{DataSetWriterBuilder, WriterGroupBuilder};
    pub use crate::reader::{ReaderGroup, DataSetReaderBuilder};
    pub use crate::{UadpNetworkMessageContentFlags, DataSetFieldContentFlags, UadpDataSetMessageContentFlags};
}


use bitflags;
use opcua_types::DataSetFieldContentMask;
use opcua_types::UadpNetworkMessageContentMask;
use opcua_types::UadpDataSetMessageContentMask;
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

