// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode

use log::{error, trace, warn};
use opcua_types::byte_len_array;
use opcua_types::read_array;
use opcua_types::status_code::StatusCode;
use opcua_types::write_array;
use opcua_types::DataSetMetaDataType;
use opcua_types::EncodingResult;
use opcua_types::EndpointDescription;
use opcua_types::WriterGroupDataType;
use opcua_types::{
    guid::Guid, string::UAString, BinaryEncoder, DataValue, DateTime, DecodingOptions, Variant,
};
use opcua_types::{process_decode_io_result, read_u16, read_u32, read_u64, read_u8};
use opcua_types::{process_encode_io_result, write_u16, write_u32, write_u8};
use std::convert::TryFrom;
use std::io::{Read, Write};
/// Uadp Message flags See OPC Unified Architecture, Part 14 7.2.2.2.2
#[derive(PartialEq, Debug, Clone, Copy)]
pub(super) struct MessageHeaderFlags(u32);

#[allow(dead_code)]
impl MessageHeaderFlags {
    const NONE: u32 = 0;
    // Flags
    const PUBLISHER_ID_EN: u32 = 0b00010000;
    /// If the PublisherId is enabled, the type of PublisherId is indicated in the ExtendedFlags1 field.
    const GROUP_HEADER_EN: u32 = 0b00100000;
    const PAYLOAD_HEADER_EN: u32 = 0b01000000;
    const EXTENDED_FLAGS_1: u32 = 0b10000000;
    // FlagsExtend1
    // When No PublisherId ist set then Id is Byte!
    const PUBLISHER_ID_UINT16: u32 = 0b0000000100000000;
    const PUBLISHER_ID_UINT32: u32 = 0b0000001000000000;
    const PUBLISHER_ID_UINT64: u32 = 0b0000011000000000;
    const PUBLISHER_ID_STRING: u32 = 0b0000010000000000;
    const DATACLASS_SET_EN: u32 = 0b0000100000000000;
    const SECURITY_MODE_EN: u32 = 0b0001000000000000; //   If the SecurityMode is SIGN_1 or SIGNANDENCRYPT_2, this flag is set, message security is enabled and the SecurityHeader is contained in the NetworkMessage header.
                                                      //   If this flag is not set, the SecurityHeader is omitted.
    const TIMESTAMP_EN: u32 = 0b0010000000000000;
    const PICO_SECONDS_EN: u32 = 0b0100000000000000;
    const EXTENDED_FLAGS_2: u32 = 0b1000000000000000;
    // FlagsExtend2
    const CHUNK: u32 = 0b000010000000000000000;
    const PROMOTEDFIELDS: u32 = 0b000100000000000000000; //Promoted fields can only be sent if the NetworkMessage contains only one DataSetMessage.
    const DISCOVERYREQUEST: u32 = 0b001000000000000000000;
    const DISCOVERYRESPONSE: u32 = 0b010000000000000000000;
    fn contains(&self, val: u32) -> bool {
        self.0 & val == val
    }
}

/// Uadp group header flags See OPC Unified Architecture, Part 14 7.2.2.2.2
struct MessageGroupHeaderFlags;
impl MessageGroupHeaderFlags {
    const WRITER_GROUP_ID_EN: u8 = 0b0001;
    const GROUP_VERSION_EN: u8 = 0b0010;
    const NETWORK_MESSAGE_NUMBER_EN: u8 = 0b0100;
    const SEQUENCE_NUMBER_EN: u8 = 0b1000;
}

/// Uadp dataset header flags See OPC Unified Architecture, Part 14 7.2.2.3.2
struct MessageDataSetFlags(u16);
#[allow(dead_code)]
impl MessageDataSetFlags {
    //Byte 1
    const VALID: u16 = 0b00000001;
    const RAW_DATA: u16 = 0b00000010;
    const DATA_VALUE: u16 = 0b00000100;
    const SEQUENCE_NUMBER_EN: u16 = 0b00001000;
    const STATUS: u16 = 0b00010000;
    const CFG_MAJOR_VERSION: u16 = 0b00100000;
    const CFG_MINOR_VERSION: u16 = 0b01000000;
    const FLAGS2: u16 = 0b10000000;
    // Byte 2
    const DELTA_FRAME: u16 = 0b0000000100000000;
    const EVENT: u16 = 0b0000001000000000;
    const KEEP_ALIVE: u16 = 0b0000001100000000;
    const TIMESTAMP: u16 = 0b0001000000000000;
    const PICOSECONDS: u16 = 0b0010000000000000;
    fn contains(&self, val: u16) -> bool {
        self.0 & val == val
    }
}

/// Header of an Uadp  Message
#[derive(Debug, Clone)]
pub struct UadpHeader {
    pub publisher_id: Option<Variant>,
    pub dataset_class_id: Option<Guid>,
    pub(super) flags: MessageHeaderFlags,
}

/// Ignore Flags
impl PartialEq for UadpHeader {
    fn eq(&self, other: &Self) -> bool {
        self.publisher_id == other.publisher_id && self.dataset_class_id == other.dataset_class_id
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct UadpChunk {
    message_sequence_no: u16,
    chunk_offset: u32,
    total_size: u32,
    chunk_data: Vec<u8>,
}

impl UadpChunk {
    pub fn byte_len(&self) -> usize {
        let mut sz = 2;
        sz += 4;
        sz += 4;
        sz += self.chunk_data.len();
        sz
    }

    pub fn encode<S: Write>(&self, stream: &mut S) -> EncodingResult<usize> {
        let mut sz = write_u16(stream, self.message_sequence_no)?;
        sz += write_u32(stream, self.chunk_offset)?;
        sz += write_u32(stream, self.total_size)?;
        sz += process_encode_io_result(stream.write(&self.chunk_data))?;
        Ok(sz)
    }

    pub fn decode<S: Read>(
        stream: &mut S,
        _decoding_opts: &DecodingOptions,
    ) -> EncodingResult<Self> {
        let message_sequence_no = read_u16(stream)?;
        let chunk_offset = read_u32(stream)?;
        let total_size = read_u32(stream)?;
        let mut chunk_data = Vec::new();
        process_decode_io_result(stream.read_to_end(&mut chunk_data))?;
        Ok(UadpChunk {
            message_sequence_no,
            chunk_offset,
            total_size,
            chunk_data,
        })
    }
}

/// https://reference.opcfoundation.org/v104/Core/docs/Part4/7.38/
/// UInt32 as seconds since the year 2000. It is used for representing Version changes
type VersionTime = u32;
type UadpDataSetPayload = [u16];

/// Header of group part of an uadp message
#[derive(PartialEq, Debug, Clone)]
pub struct UadpGroupHeader {
    pub writer_group_id: Option<u16>,
    pub group_version: Option<VersionTime>,
    pub network_message_no: Option<u16>,
    pub sequence_no: Option<u16>,
}
#[derive(PartialEq, Debug, Clone)]
pub struct UadpDataSetMessageHeader {
    pub valid: bool,
    pub sequence_no: Option<u16>,
    pub time_stamp: Option<DateTime>,
    pub pico_seconds: Option<u16>,
    pub status: Option<u16>,
    pub cfg_major_version: Option<VersionTime>,
    pub cfg_minor_version: Option<VersionTime>,
}

/// different Messages types
#[derive(PartialEq, Debug, Clone)]
pub enum UadpMessageType {
    /// a vector of variants
    KeyFrameVariant(Vec<Variant>),
    /// a vector of DataValues
    KeyFrameDataValue(Vec<DataValue>),
    /// raw data, encoding needs a description of the structured data
    KeyFrameRaw(Vec<Vec<u8>>),
    /// only changed variants are reported, u16 represents the position in dataset
    KeyDeltaFrameVariant(Vec<(u16, Variant)>),
    /// only changed DataValues are reported
    KeyDeltaFrameValue(Vec<(u16, DataValue)>),
    /// only raw changed elements of dataset
    KeyDeltaFrameRaw(Vec<(u16, Vec<u8>)>),
    /// an event which contains the data as variants
    Event(Vec<Variant>),
    /// keep alive message of the publisher
    KeepAlive,
}

/// Type of information to be send
#[derive(PartialEq, Debug, Clone, Copy)]
pub enum InformationType {
    PublisherEndpoints = 1,
    DataSetMetaData = 2,
    DataSetWriter = 3,
}
/// Response with the Endpoint of the publisher
#[derive(PartialEq, Debug, Clone)]
pub struct UadpPublisherEndpointsResp {
    endpoints: Option<Vec<EndpointDescription>>,
    status: StatusCode,
}

impl UadpPublisherEndpointsResp {
    pub fn new(endpoints: Option<Vec<EndpointDescription>>, status: StatusCode) -> Self {
        Self { endpoints, status }
    }

    pub fn byte_len(&self) -> usize {
        let mut sz = byte_len_array(&self.endpoints);
        sz += self.status.byte_len();
        sz
    }

    pub fn encode<S: Write>(&self, stream: &mut S) -> EncodingResult<usize> {
        let mut sz = write_array(stream, &self.endpoints)?;
        sz += self.status.encode(stream)?;
        Ok(sz)
    }

    pub fn decode<S: Read>(
        stream: &mut S,
        decoding_opts: &DecodingOptions,
    ) -> EncodingResult<Self> {
        let endpoints = read_array(stream, decoding_opts)?;
        let status = StatusCode::decode(stream, decoding_opts)?;
        Ok(Self { endpoints, status })
    }
}

/// Response with the MetaData of an Datasetwriter
#[derive(PartialEq, Debug, Clone)]
pub struct UadpDataSetMetaDataResp {
    dataset_writer_id: u16,
    meta_data: DataSetMetaDataType,
    status: StatusCode,
}

impl UadpDataSetMetaDataResp {
    pub fn new(dataset_writer_id: u16, meta_data: DataSetMetaDataType, status: StatusCode) -> Self {
        Self {
            dataset_writer_id,
            meta_data,
            status,
        }
    }

    pub fn byte_len(&self) -> usize {
        let mut sz = 2;
        sz += self.meta_data.byte_len();
        sz += self.status.byte_len();
        sz
    }

    pub fn encode<S: Write>(&self, stream: &mut S) -> EncodingResult<usize> {
        let mut sz = write_u16(stream, self.dataset_writer_id)?;
        sz += self.meta_data.encode(stream)?;
        sz += self.status.encode(stream)?;
        Ok(sz)
    }

    pub fn decode<S: Read>(
        stream: &mut S,
        decoding_opts: &DecodingOptions,
    ) -> EncodingResult<Self> {
        let dataset_writer_id = read_u16(stream)?;
        let meta_data = DataSetMetaDataType::decode(stream, decoding_opts)?;
        let status = StatusCode::decode(stream, decoding_opts)?;
        Ok(Self {
            dataset_writer_id,
            meta_data,
            status,
        })
    }
}

/// Response with the DataSetWriters of a Writer Group
#[derive(PartialEq, Debug, Clone)]
pub struct UadpDataSetWriterResp {
    dataset_writer_ids: Option<Vec<u16>>,
    dataset_writer_config: WriterGroupDataType,
    status: Option<Vec<StatusCode>>,
}

impl UadpDataSetWriterResp {
    pub fn new(
        dataset_writer_ids: Option<Vec<u16>>,
        dataset_writer_config: WriterGroupDataType,
        status: Option<Vec<StatusCode>>,
    ) -> Self {
        Self {
            dataset_writer_ids,
            dataset_writer_config,
            status,
        }
    }

    pub fn byte_len(&self) -> usize {
        let mut sz = byte_len_array(&self.dataset_writer_ids);
        sz += self.dataset_writer_config.byte_len();
        sz += byte_len_array(&self.status);
        sz
    }

    pub fn encode<S: Write>(&self, stream: &mut S) -> EncodingResult<usize> {
        let mut sz = write_array(stream, &self.dataset_writer_ids)?;
        sz += self.dataset_writer_config.encode(stream)?;
        sz += write_array(stream, &self.status)?;
        Ok(sz)
    }

    pub fn decode<S: Read>(
        stream: &mut S,
        decoding_opts: &DecodingOptions,
    ) -> EncodingResult<Self> {
        let dataset_writer_ids = read_array(stream, decoding_opts)?;
        let dataset_writer_config = WriterGroupDataType::decode(stream, decoding_opts)?;
        let status = read_array(stream, decoding_opts)?;
        Ok(Self {
            dataset_writer_ids,
            dataset_writer_config,
            status,
        })
    }
}

/// Response Information
#[derive(PartialEq, Debug, Clone)]
pub enum ResponseType {
    PublisherEndpoints(UadpPublisherEndpointsResp),
    DataSetMetaData(UadpDataSetMetaDataResp),
    DataSetWriter(UadpDataSetWriterResp),
}

/// Struct to request meta infos
#[derive(PartialEq, Debug, Clone)]
pub struct UadpDiscoveryRequest {
    /// Which type of discovery message
    information_type: InformationType,
    /// Dataset ids can be null
    dataset_writer_ids: Option<Vec<u16>>,
}
/// Struct to send meta infos
#[derive(PartialEq, Debug, Clone)]
pub struct UadpDiscoveryResponse {
    /// Which type of discovery message
    information_type: InformationType,
    /// Sequence number for responses, should be incremented for each discovery response from the connection
    sequence_number: u16,
    /// the specific response
    response: ResponseType,
}

impl UadpDiscoveryResponse {
    pub fn new(
        information_type: InformationType,
        sequence_number: u16,
        response: ResponseType,
    ) -> Self {
        Self {
            information_type,
            sequence_number,
            response,
        }
    }

    pub fn byte_len(&self) -> usize {
        let mut sz = 1;
        sz += self.sequence_number.byte_len();
        sz += match &self.response {
            ResponseType::PublisherEndpoints(d) => d.byte_len(),
            ResponseType::DataSetMetaData(d) => d.byte_len(),
            ResponseType::DataSetWriter(d) => d.byte_len(),
        };
        sz
    }

    pub fn encode<S: Write>(&self, stream: &mut S) -> EncodingResult<usize> {
        let mut sz = write_u8(stream, self.information_type as u8)?;
        sz += write_u16(stream, self.sequence_number)?;
        sz += match &self.response {
            ResponseType::PublisherEndpoints(d) => d.encode(stream)?,
            ResponseType::DataSetMetaData(d) => d.encode(stream)?,
            ResponseType::DataSetWriter(d) => d.encode(stream)?,
        };
        Ok(sz)
    }

    pub fn decode<S: Read>(
        stream: &mut S,
        decoding_opts: &DecodingOptions,
    ) -> EncodingResult<Self> {
        let information_type = match read_u8(stream)? {
            1 => InformationType::PublisherEndpoints,
            2 => InformationType::DataSetMetaData,
            3 => InformationType::DataSetWriter,
            x => {
                warn!("Discovery InformationType {} not supported", x);
                return Err(StatusCode::BadRequestTypeInvalid);
            }
        };
        let sequence_number = read_u16(stream)?;
        let response = match information_type {
            InformationType::PublisherEndpoints => ResponseType::PublisherEndpoints(
                UadpPublisherEndpointsResp::decode(stream, decoding_opts)?,
            ),
            InformationType::DataSetMetaData => ResponseType::DataSetMetaData(
                UadpDataSetMetaDataResp::decode(stream, decoding_opts)?,
            ),
            InformationType::DataSetWriter => {
                ResponseType::DataSetWriter(UadpDataSetWriterResp::decode(stream, decoding_opts)?)
            }
        };
        Ok(Self {
            information_type,
            sequence_number,
            response,
        })
    }
}

impl UadpDiscoveryRequest {
    pub fn new(information_type: InformationType, dataset_writer_ids: Option<Vec<u16>>) -> Self {
        Self {
            information_type,
            dataset_writer_ids,
        }
    }

    pub fn byte_len(&self) -> usize {
        let mut sz = 2;
        sz += byte_len_array(&self.dataset_writer_ids);
        sz
    }

    pub fn encode<S: Write>(&self, stream: &mut S) -> EncodingResult<usize> {
        // 1 => Publisher information request message
        let mut sz = write_u8(stream, 1)?;
        sz += write_u8(stream, self.information_type as u8)?;
        sz += write_array(stream, &self.dataset_writer_ids.clone())?;
        Ok(sz)
    }

    pub fn decode<S: Read>(
        stream: &mut S,
        decoding_options: &DecodingOptions,
    ) -> EncodingResult<Self> {
        let req_type = read_u8(stream)?;
        if req_type != 1 {
            warn!("Discovery RequestType {} not supported", req_type);
            return Err(StatusCode::BadRequestTypeInvalid);
        }
        let information_type = match read_u8(stream)? {
            1 => InformationType::PublisherEndpoints,
            2 => InformationType::DataSetMetaData,
            3 => InformationType::DataSetWriter,
            x => {
                warn!("Discovery InformationType {} not supported", x);
                return Err(StatusCode::BadRequestTypeInvalid);
            }
        };
        let dataset_writer_ids = read_array(stream, decoding_options)?;
        Ok(Self {
            information_type,
            dataset_writer_ids,
        })
    }

    /// Get a reference to the uadp discovery request's information type.
    pub fn information_type(&self) -> &InformationType {
        &self.information_type
    }

    /// Get a reference to the uadp discovery request's dataset writer ids.
    pub fn dataset_writer_ids(&self) -> Option<&Vec<u16>> {
        self.dataset_writer_ids.as_ref()
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct UadpDataSetMessage {
    pub header: UadpDataSetMessageHeader,
    pub data: UadpMessageType,
}

impl UadpHeader {
    /// Check what payload to send
    fn payload_flag(msg: &UadpNetworkMessage) -> EncodingResult<u32> {
        match msg.payload {
            UadpPayload::DataSets(_) => Ok(0),
            UadpPayload::DiscoveryRequest(_) => Ok(MessageHeaderFlags::DISCOVERYREQUEST),
            UadpPayload::DiscoveryResponse(_) => Ok(MessageHeaderFlags::DISCOVERYRESPONSE),
            UadpPayload::Chunk(_) => Ok(MessageHeaderFlags::CHUNK),
            UadpPayload::None => {
                error!("Empty Dataset");
                Err(StatusCode::BadInvalidState)
            }
        }
    }

    fn generate_flags(&self, msg: &UadpNetworkMessage) -> EncodingResult<u32> {
        let mut f: u32 = 0x01; // Uadp Version 1
        if self.publisher_id.is_some() {
            f |= MessageHeaderFlags::PUBLISHER_ID_EN;
            match self.publisher_id {
                Some(Variant::UInt16(_)) => f |= MessageHeaderFlags::PUBLISHER_ID_UINT16,
                Some(Variant::UInt32(_)) => f |= MessageHeaderFlags::PUBLISHER_ID_UINT32,
                Some(Variant::Byte(_)) => {} // U8 is Zero
                Some(Variant::UInt64(_)) => f |= MessageHeaderFlags::PUBLISHER_ID_UINT64,
                Some(Variant::String(_)) => f |= MessageHeaderFlags::PUBLISHER_ID_STRING,
                _ => return Err(StatusCode::BadTypeMismatch),
            }
        }
        if self.dataset_class_id.is_some() {
            f |= MessageHeaderFlags::DATACLASS_SET_EN;
        }
        if msg.timestamp.is_some() {
            f |= MessageHeaderFlags::TIMESTAMP_EN;
        }
        if msg.picoseconds.is_some() {
            f |= MessageHeaderFlags::PICO_SECONDS_EN;
        }
        if !msg.promoted_fields.is_empty() {
            f |= MessageHeaderFlags::PROMOTEDFIELDS;
        }
        if msg.group_header.is_some() {
            f |= MessageHeaderFlags::GROUP_HEADER_EN;
        }
        if !msg.dataset_payload.is_empty() {
            f |= MessageHeaderFlags::PAYLOAD_HEADER_EN;
        }
        f |= Self::payload_flag(msg)?;
        if f > 0xFF {
            f |= MessageHeaderFlags::EXTENDED_FLAGS_1;
        }
        if f > 0xFFFF {
            f |= MessageHeaderFlags::EXTENDED_FLAGS_2;
        }
        Ok(f)
    }

    pub fn byte_len(&self, msg: &UadpNetworkMessage) -> usize {
        let f = self.generate_flags(msg).unwrap_or(0xFFFFF);
        let mut sz = match f {
            0..=0xFF => 1,
            0x0100..=0xFFFF => 2,
            _ => 3,
        };
        if let Some(f) = &self.publisher_id {
            sz += match f {
                Variant::Byte(d) => d.byte_len(),
                Variant::UInt16(d) => d.byte_len(),
                Variant::UInt32(d) => d.byte_len(),
                Variant::UInt64(d) => d.byte_len(),
                Variant::String(d) => d.byte_len(),
                _ => 0,
            };
        }
        if let Some(f) = &self.dataset_class_id {
            sz += f.byte_len();
        }
        sz
    }

    pub fn encode<S: Write>(
        &self,
        stream: &mut S,
        msg: &UadpNetworkMessage,
    ) -> EncodingResult<usize> {
        let f = self.generate_flags(msg)?;
        let b = f.to_le_bytes();
        let mut sz: usize = 0;
        sz += write_u8(stream, b[0])?;
        if f > 0xFF {
            sz += write_u8(stream, b[1])?;
        }
        if f > 0xFFFF {
            sz += write_u8(stream, b[2])?;
        }
        sz += if let Some(v) = &self.publisher_id {
            match v {
                Variant::Byte(d) => d.encode(stream)?,
                Variant::UInt16(d) => d.encode(stream)?,
                Variant::UInt32(d) => d.encode(stream)?,
                Variant::UInt64(d) => d.encode(stream)?,
                Variant::String(d) => d.encode(stream)?,
                _ => 0,
            }
        } else {
            0
        };
        if let Some(v) = &self.dataset_class_id {
            sz += v.encode(stream)?;
        }
        Ok(sz)
    }

    fn decode<S: Read>(c: &mut S, limits: &DecodingOptions) -> EncodingResult<UadpHeader> {
        let h1 = read_u8(c)?;
        let bitmaskv: u8 = 1 << 0 | 1 << 1 | 1 << 2 | 1 << 3;
        let version: u8 = h1 & bitmaskv;
        if version != 1 {
            warn!("Uadp: UadpMessage Version doesnt match compatible Version!");
        }
        let mut f = MessageHeaderFlags((h1 & !bitmaskv) as u32);
        if f.contains(MessageHeaderFlags::EXTENDED_FLAGS_1) {
            let h2 = read_u8(c)?;
            f.0 += (h2 as u32) << 8;
            if f.contains(MessageHeaderFlags::EXTENDED_FLAGS_2) {
                let h3 = read_u8(c)?;
                f.0 += (h3 as u32) << 16;
            }
        }
        let publisher_id = if f.contains(MessageHeaderFlags::PUBLISHER_ID_EN) {
            if f.contains(MessageHeaderFlags::PUBLISHER_ID_STRING) {
                Some(Variant::String(UAString::decode(c, limits)?))
            } else if f.contains(MessageHeaderFlags::PUBLISHER_ID_UINT64) {
                Some(Variant::UInt64(read_u64(c)?))
            } else if f.contains(MessageHeaderFlags::PUBLISHER_ID_UINT32) {
                Some(Variant::UInt32(read_u32(c)?))
            } else if f.contains(MessageHeaderFlags::PUBLISHER_ID_UINT16) {
                Some(Variant::UInt16(read_u16(c)?))
            } else {
                Some(Variant::Byte(read_u8(c)?))
            }
        } else {
            None
        };
        let dataset_class_id = if f.contains(MessageHeaderFlags::DATACLASS_SET_EN) {
            Some(Guid::decode(c, limits)?)
        } else {
            None
        };
        Ok(UadpHeader {
            publisher_id,
            dataset_class_id,
            flags: f,
        })
    }
}

impl Default for UadpGroupHeader {
    fn default() -> Self {
        Self::new()
    }
}

impl UadpGroupHeader {
    pub fn new() -> Self {
        UadpGroupHeader {
            group_version: None,
            network_message_no: None,
            sequence_no: None,
            writer_group_id: None,
        }
    }

    fn byte_len(&self) -> usize {
        let mut sz = 1;
        sz += match self.writer_group_id {
            Some(_) => 2,
            None => 0,
        };
        sz += match self.group_version {
            Some(_) => 4,
            None => 0,
        };
        sz += match self.network_message_no {
            Some(_) => 2,
            None => 0,
        };
        sz += match self.sequence_no {
            Some(_) => 2,
            None => 0,
        };
        sz
    }

    fn encode<S: Write>(&self, stream: &mut S) -> EncodingResult<usize> {
        let mut flags: u8 = 0;
        if self.writer_group_id.is_some() {
            flags |= MessageGroupHeaderFlags::WRITER_GROUP_ID_EN;
        }
        if self.group_version.is_some() {
            flags |= MessageGroupHeaderFlags::GROUP_VERSION_EN;
        }
        if self.network_message_no.is_some() {
            flags |= MessageGroupHeaderFlags::NETWORK_MESSAGE_NUMBER_EN;
        }
        if self.sequence_no.is_some() {
            flags |= MessageGroupHeaderFlags::SEQUENCE_NUMBER_EN;
        }
        let mut sz = write_u8(stream, flags)?;
        sz += match self.writer_group_id {
            Some(v) => write_u16(stream, v)?,
            None => 0,
        };
        sz += match self.group_version {
            Some(v) => write_u32(stream, v)?,
            None => 0,
        };
        sz += match self.network_message_no {
            Some(v) => write_u16(stream, v)?,
            None => 0,
        };
        sz += match self.sequence_no {
            Some(v) => write_u16(stream, v)?,
            None => 0,
        };
        Ok(sz)
    }

    fn decode<S: Read>(c: &mut S, _limits: &DecodingOptions) -> EncodingResult<UadpGroupHeader> {
        let flags = read_u8(c)?;
        let writer_group_id = if flags & MessageGroupHeaderFlags::WRITER_GROUP_ID_EN != 0 {
            Some(read_u16(c)?)
        } else {
            None
        };
        let group_version = if flags & MessageGroupHeaderFlags::GROUP_VERSION_EN != 0 {
            Some(read_u32(c)?)
        } else {
            None
        };
        let network_message_no = if flags & MessageGroupHeaderFlags::NETWORK_MESSAGE_NUMBER_EN != 0
        {
            Some(read_u16(c)?)
        } else {
            None
        };
        let sequence_no = if flags & MessageGroupHeaderFlags::SEQUENCE_NUMBER_EN != 0 {
            Some(read_u16(c)?)
        } else {
            None
        };
        Ok(UadpGroupHeader {
            writer_group_id,
            group_version,
            network_message_no,
            sequence_no,
        })
    }
}

impl UadpDataSetMessageHeader {
    fn generate_flags(&self, dt: &UadpMessageType) -> u16 {
        let mut flags: u16 = if self.valid {
            MessageDataSetFlags::VALID
        } else {
            0
        };
        flags += match dt {
            UadpMessageType::Event(_) => MessageDataSetFlags::EVENT,
            UadpMessageType::KeepAlive => MessageDataSetFlags::KEEP_ALIVE,
            UadpMessageType::KeyDeltaFrameRaw(_) => {
                MessageDataSetFlags::DELTA_FRAME | MessageDataSetFlags::RAW_DATA
            }
            UadpMessageType::KeyDeltaFrameValue(_) => {
                MessageDataSetFlags::DELTA_FRAME | MessageDataSetFlags::DATA_VALUE
            }
            UadpMessageType::KeyDeltaFrameVariant(_) => MessageDataSetFlags::DELTA_FRAME,
            UadpMessageType::KeyFrameRaw(_) => MessageDataSetFlags::RAW_DATA,
            UadpMessageType::KeyFrameDataValue(_) => MessageDataSetFlags::DATA_VALUE,
            UadpMessageType::KeyFrameVariant(_) => 0,
        };
        if self.sequence_no.is_some() {
            flags += MessageDataSetFlags::SEQUENCE_NUMBER_EN;
        }
        if self.time_stamp.is_some() {
            flags += MessageDataSetFlags::TIMESTAMP;
        }
        if self.pico_seconds.is_some() {
            flags += MessageDataSetFlags::PICOSECONDS;
        }
        if self.cfg_minor_version.is_some() {
            flags += MessageDataSetFlags::CFG_MINOR_VERSION;
        }
        if self.cfg_major_version.is_some() {
            flags += MessageDataSetFlags::CFG_MAJOR_VERSION;
        }
        if flags > 0xFF {
            flags += MessageDataSetFlags::FLAGS2;
        }
        flags
    }

    fn byte_len(&self, dt: &UadpMessageType) -> usize {
        let flags = self.generate_flags(dt);
        let mut sz = if flags <= 0xFF { 1 } else { 2 };
        sz += match self.sequence_no {
            Some(_) => 2,
            None => 0,
        };
        sz += match &self.time_stamp {
            Some(v) => v.byte_len(),
            None => 0,
        };
        sz += match self.pico_seconds {
            Some(_) => 2,
            None => 0,
        };
        sz += match self.status {
            Some(_) => 2,
            None => 0,
        };
        sz += match self.cfg_major_version {
            Some(_) => 4,
            None => 0,
        };
        sz += match self.cfg_minor_version {
            Some(_) => 4,
            None => 0,
        };
        sz
    }

    fn encode<S: Write>(&self, stream: &mut S, dt: &UadpMessageType) -> EncodingResult<usize> {
        let flags = self.generate_flags(dt);
        let f = flags.to_le_bytes();
        let mut sz = write_u8(stream, f[0])?;
        if flags > 0xFF {
            sz += write_u8(stream, f[1])?;
        }
        sz += match self.sequence_no {
            Some(v) => write_u16(stream, v)?,
            None => 0,
        };
        sz += match &self.time_stamp {
            Some(v) => v.encode(stream)?,
            None => 0,
        };
        sz += match self.pico_seconds {
            Some(v) => write_u16(stream, v)?,
            None => 0,
        };
        sz += match self.status {
            Some(v) => write_u16(stream, v)?,
            None => 0,
        };
        sz += match self.cfg_major_version {
            Some(v) => write_u32(stream, v)?,
            None => 0,
        };
        sz += match self.cfg_minor_version {
            Some(v) => write_u32(stream, v)?,
            None => 0,
        };
        Ok(sz)
    }

    fn decode<S: Read>(
        c: &mut S,
        decoding_options: &DecodingOptions,
    ) -> EncodingResult<(UadpDataSetMessageHeader, MessageDataSetFlags)> {
        let mut flags = read_u8(c)? as u16;
        if flags & MessageDataSetFlags::FLAGS2 != 0 {
            flags += (read_u8(c)? as u16) << 8;
        }
        let valid = flags & MessageDataSetFlags::VALID != 0;
        let sequence_no = if flags & MessageDataSetFlags::SEQUENCE_NUMBER_EN != 0 {
            Some(read_u16(c)?)
        } else {
            None
        };
        let time_stamp = if flags & MessageDataSetFlags::TIMESTAMP != 0 {
            Some(opcua_types::DateTime::decode(c, &decoding_options)?)
        } else {
            None
        };
        let pico_seconds = if flags & MessageDataSetFlags::PICOSECONDS != 0 {
            Some(read_u16(c)?)
        } else {
            None
        };
        let status = if flags & MessageDataSetFlags::STATUS != 0 {
            Some(read_u16(c)?)
        } else {
            None
        };
        let cfg_major_version = if flags & MessageDataSetFlags::CFG_MAJOR_VERSION != 0 {
            Some(read_u32(c)?)
        } else {
            None
        };
        let cfg_minor_version = if flags & MessageDataSetFlags::CFG_MINOR_VERSION != 0 {
            Some(read_u32(c)?)
        } else {
            None
        };
        Ok((
            UadpDataSetMessageHeader {
                valid,
                sequence_no,
                time_stamp,
                pico_seconds,
                status,
                cfg_major_version,
                cfg_minor_version,
            },
            MessageDataSetFlags(flags),
        ))
    }
}

impl UadpMessageType {
    fn byte_len(&self) -> usize {
        match self {
            UadpMessageType::KeyFrameVariant(x) => {
                x.iter().map(|f| f.byte_len()).sum::<usize>() + 2
            }
            UadpMessageType::KeyFrameDataValue(x) => {
                2 + x.iter().map(|f| f.byte_len()).sum::<usize>()
            }
            UadpMessageType::KeyFrameRaw(x) => 2 + x.iter().map(|f| f.len()).sum::<usize>(),
            UadpMessageType::KeyDeltaFrameVariant(x) => {
                2 + x.iter().map(|(_z, f)| 2 + f.byte_len()).sum::<usize>()
            }
            UadpMessageType::KeyDeltaFrameValue(x) => {
                2 + x.iter().map(|(_z, f)| 2 + f.byte_len()).sum::<usize>()
            }
            UadpMessageType::KeyDeltaFrameRaw(x) => {
                2 + x.iter().map(|(_z, f)| 2 + f.len()).sum::<usize>()
            }
            UadpMessageType::Event(x) => 2 + x.iter().map(|f| f.byte_len()).sum::<usize>(),
            UadpMessageType::KeepAlive => 0,
        }
    }

    fn encode<S: Write>(&self, stream: &mut S) -> EncodingResult<usize> {
        match self {
            UadpMessageType::KeyFrameVariant(data) => {
                let mut sz = write_u16(stream, data.len() as u16)?;
                for v in data.iter() {
                    sz += v.encode(stream)?;
                }
                Ok(sz)
            }
            UadpMessageType::KeyFrameDataValue(data) => {
                let mut sz = write_u16(stream, data.len() as u16)?;
                for v in data.iter() {
                    sz += v.encode(stream)?;
                }
                Ok(sz)
            }
            UadpMessageType::KeyFrameRaw(data) => {
                let mut sz = write_u16(stream, data.len() as u16)?;
                for v in data.iter() {
                    sz += process_encode_io_result(stream.write(v))?;
                }
                Ok(sz)
            }
            UadpMessageType::KeyDeltaFrameRaw(data) => {
                let mut sz = write_u16(stream, data.len() as u16)?;
                for (id, v) in data.iter() {
                    sz += write_u16(stream, *id)?;
                    sz += process_encode_io_result(stream.write(v))?;
                }
                Ok(sz)
            }
            UadpMessageType::KeyDeltaFrameValue(data) => {
                let mut sz = write_u16(stream, data.len() as u16)?;
                for (id, v) in data.iter() {
                    sz += write_u16(stream, *id)?;
                    sz += v.encode(stream)?;
                }
                Ok(sz)
            }
            UadpMessageType::KeyDeltaFrameVariant(data) => {
                let mut sz = write_u16(stream, data.len() as u16)?;
                for (id, v) in data.iter() {
                    sz += write_u16(stream, *id)?;
                    sz += v.encode(stream)?;
                }
                Ok(sz)
            }
            UadpMessageType::KeepAlive => Ok(0),
            UadpMessageType::Event(data) => {
                let mut sz = write_u16(stream, data.len() as u16)?;
                for v in data.iter() {
                    sz += v.encode(stream)?;
                }
                Ok(sz)
            }
        }
    }

    fn decode<S: Read>(
        c: &mut S,
        decoding_options: &DecodingOptions,
        flags: MessageDataSetFlags,
        pay_head: &UadpDataSetPayload,
    ) -> EncodingResult<Self> {
        if flags.contains(MessageDataSetFlags::DELTA_FRAME) {
            let count = read_u16(c)? as usize;
            // Remove clippy warning because it's mut data is different vector type
            #[allow(clippy::branches_sharing_code)]
            if flags.contains(MessageDataSetFlags::DATA_VALUE) {
                let mut data = Vec::with_capacity(count);
                for _ in 0..count {
                    data.push((read_u16(c)?, DataValue::decode(c, decoding_options)?));
                }
                Ok(UadpMessageType::KeyDeltaFrameValue(data))
            } else if flags.contains(MessageDataSetFlags::RAW_DATA) {
                let mut data = Vec::with_capacity(count);
                for x in 0..count as usize {
                    let id = read_u16(c)?;
                    let sz = if pay_head.len() < x {
                        pay_head[x]
                    } else {
                        error!("No PayHead found for raw header!");
                        1500_u16
                    };
                    let mut raw = vec![0u8; sz as usize];
                    let result = c.read_exact(&mut raw);
                    process_decode_io_result(result)?;
                    data.push((id, raw));
                }
                Ok(UadpMessageType::KeyDeltaFrameRaw(data))
            } else {
                let mut data = Vec::with_capacity(count);
                for _ in 0..count {
                    data.push((read_u16(c)?, Variant::decode(c, decoding_options)?));
                }
                Ok(UadpMessageType::KeyDeltaFrameVariant(data))
            }
        } else if flags.contains(MessageDataSetFlags::EVENT) {
            let count = read_u16(c)? as usize;
            let mut data = Vec::with_capacity(count);
            for _ in 0..count {
                data.push(Variant::decode(c, decoding_options)?);
            }
            Ok(UadpMessageType::Event(data))
        } else if flags.contains(MessageDataSetFlags::KEEP_ALIVE) {
            trace!("UadpKeepAlive Message");
            Ok(UadpMessageType::KeepAlive)
        } else if flags.contains(MessageDataSetFlags::DATA_VALUE) {
            let count = read_u16(c)? as usize;
            let mut data = Vec::with_capacity(count);
            for _ in 0..count {
                data.push(DataValue::decode(c, decoding_options)?);
            }
            Ok(UadpMessageType::KeyFrameDataValue(data))
        } else if flags.contains(MessageDataSetFlags::RAW_DATA) {
            let count = read_u16(c)? as usize;
            let mut data = Vec::with_capacity(count);
            for x in 0..count as usize {
                let sz = if pay_head.len() < x {
                    pay_head[x]
                } else {
                    error!("No PayHead found for raw header!");
                    1500_u16
                };
                let mut raw = vec![0u8; sz as usize];
                let result = c.read_exact(&mut raw);
                process_decode_io_result(result)?;
                data.push(raw);
            }
            Ok(UadpMessageType::KeyFrameRaw(data))
        } else {
            let count = read_u16(c)? as usize;
            let mut data = Vec::with_capacity(count);
            for _ in 0..count {
                data.push(Variant::decode(c, decoding_options)?);
            }
            Ok(UadpMessageType::KeyFrameVariant(data))
        }
    }
}

impl UadpDataSetMessage {
    /// creates a new dataset message that is valid and doesn't contain an value
    pub fn new(message: UadpMessageType) -> Self {
        let header = UadpDataSetMessageHeader {
            valid: true,
            sequence_no: None,
            time_stamp: None,
            pico_seconds: None,
            status: None,
            cfg_major_version: None,
            cfg_minor_version: None,
        };
        UadpDataSetMessage {
            header,
            data: message,
        }
    }

    fn byte_len(&self) -> usize {
        let mut sz = self.header.byte_len(&self.data);
        sz += self.data.byte_len();
        sz
    }

    fn encode<S: Write>(&self, stream: &mut S) -> Result<usize, StatusCode> {
        let mut sz = self.header.encode(stream, &self.data)?;
        sz += self.data.encode(stream)?;
        Ok(sz)
    }

    fn decode<S: Read>(
        c: &mut S,
        decoding_options: &DecodingOptions,
        pay_head: &UadpDataSetPayload,
    ) -> EncodingResult<UadpDataSetMessage> {
        let (header, flags) = UadpDataSetMessageHeader::decode(c, decoding_options)?;
        let data = UadpMessageType::decode(c, decoding_options, flags, pay_head)?;
        Ok(UadpDataSetMessage { header, data })
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum UadpPayload {
    DataSets(Vec<UadpDataSetMessage>),
    DiscoveryRequest(Box<UadpDiscoveryRequest>),
    DiscoveryResponse(Box<UadpDiscoveryResponse>),
    Chunk(UadpChunk),
    None,
}

#[derive(PartialEq, Debug, Clone)]
pub struct UadpNetworkMessage {
    pub header: UadpHeader,
    pub group_header: Option<UadpGroupHeader>,
    pub dataset_payload: Vec<u16>,
    pub timestamp: Option<opcua_types::DateTime>,
    pub picoseconds: Option<u16>,
    pub promoted_fields: Vec<Variant>,
    pub payload: UadpPayload,
}

impl UadpNetworkMessage {
    /// creates an empty uadp network message
    pub fn new() -> Self {
        let promoted_fields = Vec::new();
        let picoseconds = None;
        let timestamp = None;
        let dataset_payload = Vec::new();
        let group_header = None;
        let header = UadpHeader {
            dataset_class_id: None,
            publisher_id: None,
            flags: MessageHeaderFlags(0),
        };
        UadpNetworkMessage {
            header,
            group_header,
            dataset_payload,
            timestamp,
            picoseconds,
            promoted_fields,
            payload: UadpPayload::None,
        }
    }

    pub fn is_chunk(&self) -> bool {
        matches!(&self.payload, UadpPayload::Chunk(_))
    }

    pub fn is_discovery(&self) -> bool {
        match &self.payload {
            UadpPayload::DataSets(_) => false,
            UadpPayload::DiscoveryRequest(_) | UadpPayload::DiscoveryResponse(_) => true,
            UadpPayload::Chunk(_) => {
                self.header
                    .flags
                    .contains(MessageHeaderFlags::DISCOVERYREQUEST)
                    || self
                        .header
                        .flags
                        .contains(MessageHeaderFlags::DISCOVERYRESPONSE)
            }
            UadpPayload::None => false,
        }
    }

    /// Calcs the size of the static message size, in chunking this value is constant
    pub fn body_byte_len(&self) -> usize {
        let mut sz = self.header.byte_len(self);
        if let Some(v) = &self.group_header {
            sz += v.byte_len();
        }
        if !self.dataset_payload.is_empty() {
            sz += 1;
            sz += 2 * self.dataset_payload.len();
        }
        if let Some(v) = &self.timestamp {
            sz += v.byte_len();
        }
        if self.picoseconds.is_some() {
            sz += 2;
        }
        sz
    }

    pub fn byte_len(&self) -> usize {
        let mut sz = self.body_byte_len();
        if !self.promoted_fields.is_empty() {
            match &self.payload {
                UadpPayload::DataSets(ds) => {
                    sz += 2;
                    sz += ds.len() * 2;
                }
                UadpPayload::DiscoveryRequest(_)
                | UadpPayload::DiscoveryResponse(_)
                | UadpPayload::None => {}
                UadpPayload::Chunk(_) => {
                    sz += 2;
                }
            }
        }
        match &self.payload {
            UadpPayload::DataSets(dataset) => {
                // Don't write payload len if only one dataset is contained via payload header
                if self.dataset_payload.len() > 1 {
                    sz += 2;
                }
                for v in dataset.iter() {
                    sz += v.byte_len();
                }
            }
            UadpPayload::DiscoveryRequest(req) => {
                sz += req.byte_len();
            }
            UadpPayload::DiscoveryResponse(res) => {
                sz += res.byte_len();
            }
            UadpPayload::Chunk(chunk) => {
                sz += chunk.byte_len();
            }
            UadpPayload::None => {}
        }
        sz
    }

    pub fn encode<S: Write>(&self, stream: &mut S) -> EncodingResult<usize> {
        let mut sz = self.header.encode(stream, self)?;
        if let Some(v) = &self.group_header {
            sz += v.encode(stream)?;
        }
        if !self.dataset_payload.is_empty() {
            sz += write_u8(stream, self.dataset_payload.len() as u8)?;
            for v in self.dataset_payload.iter() {
                sz += v.encode(stream)?;
            }
        }
        if let Some(v) = &self.timestamp {
            sz += v.encode(stream)?;
        }
        if let Some(v) = self.picoseconds {
            sz += v.encode(stream)?;
        }
        if !self.promoted_fields.is_empty() {
            match &self.payload {
                UadpPayload::DataSets(_) => {
                    sz += 2;
                    for v in self.dataset_payload.iter() {
                        sz += write_u16(stream, *v)?;
                    }
                }
                UadpPayload::DiscoveryRequest(_)
                | UadpPayload::DiscoveryResponse(_)
                | UadpPayload::None => {}
                UadpPayload::Chunk(_) => {
                    sz += 2;
                    // Chunk are only allowed to have 1 dataset
                    let id = self.dataset_payload.first().unwrap_or(&0);
                    sz += write_u16(stream, *id)?;
                }
            }
        }
        match &self.payload {
            UadpPayload::DataSets(ds) => {
                // Don't write payload len if only one dataset is contained via payload header
                if self.dataset_payload.len() > 1 {
                    sz += write_u16(stream, ds.len() as u16)?;
                }
                for v in ds.iter() {
                    sz += v.encode(stream)?;
                }
            }
            UadpPayload::DiscoveryRequest(req) => {
                sz += req.encode(stream)?;
            }
            UadpPayload::DiscoveryResponse(res) => {
                sz += res.encode(stream)?;
            }
            UadpPayload::Chunk(chunk) => {
                sz += chunk.encode(stream)?;
            }
            UadpPayload::None => {}
        }
        Ok(sz)
    }

    pub fn decode<S: Read>(c: &mut S, decoding_options: &DecodingOptions) -> EncodingResult<Self> {
        let header = UadpHeader::decode(c, decoding_options)?;
        let flags = header.flags;
        let group_header = if flags.contains(MessageHeaderFlags::GROUP_HEADER_EN) {
            Some(UadpGroupHeader::decode(c, decoding_options)?)
        } else {
            None
        };
        let dataset_payload = if flags.contains(MessageHeaderFlags::PAYLOAD_HEADER_EN) {
            let count = read_u8(c)? as usize;
            let mut data_ids = Vec::with_capacity(count);
            for _ in 0..count {
                data_ids.push(read_u16(c)?)
            }
            data_ids
        } else {
            Vec::new()
        };
        let timestamp = if flags.contains(MessageHeaderFlags::TIMESTAMP_EN) {
            Some(opcua_types::DateTime::decode(c, decoding_options)?)
        } else {
            None
        };
        let picoseconds = if flags.contains(MessageHeaderFlags::PICO_SECONDS_EN) {
            Some(read_u16(c)?)
        } else {
            None
        };
        let promoted_fields = if flags.contains(MessageHeaderFlags::PROMOTEDFIELDS) {
            let sz = read_u16(c)? as usize;
            let mut v = Vec::with_capacity(sz);
            for _ in 0..sz {
                v.push(Variant::decode(c, decoding_options)?);
            }
            v
        } else {
            Vec::new()
        };
        let payload = if flags.contains(MessageHeaderFlags::CHUNK) {
            UadpPayload::Chunk(UadpChunk::decode(c, decoding_options)?)
        } else if flags.contains(MessageHeaderFlags::DISCOVERYRESPONSE) {
            UadpPayload::DiscoveryResponse(Box::new(UadpDiscoveryResponse::decode(
                c,
                decoding_options,
            )?))
        } else if flags.contains(MessageHeaderFlags::DISCOVERYREQUEST) {
            UadpPayload::DiscoveryRequest(Box::new(UadpDiscoveryRequest::decode(
                c,
                decoding_options,
            )?))
        } else {
            let sz = if flags.contains(MessageHeaderFlags::PAYLOAD_HEADER_EN) {
                match dataset_payload.len() {
                    1 => 1_usize,
                    _ => read_u16(c)? as usize,
                }
            } else {
                1_usize
            };
            let mut v = Vec::with_capacity(sz);
            if sz > 0 {
                for _ in 0..sz {
                    v.push(UadpDataSetMessage::decode(
                        c,
                        decoding_options,
                        &dataset_payload,
                    )?);
                }
            }
            UadpPayload::DataSets(v)
        };
        Ok(UadpNetworkMessage {
            header,
            group_header,
            dataset_payload,
            timestamp,
            picoseconds,
            promoted_fields,
            payload,
        })
    }
    // Generates a chunked message
    pub fn chunk(self, max_size: usize, network_no: u16) -> Result<Vec<Self>, StatusCode> {
        let sz = self.byte_len();
        if sz > max_size {
            let body_sz = self.body_byte_len();
            let mut data = Vec::new();
            let (payload_header, network_no) = match &self.payload {
                UadpPayload::DataSets(r) => {
                    if r.len() != 1 {
                        error!("Chunking for multiple datasetmessages not supported");
                        return Err(StatusCode::BadNotSupported);
                    }
                    r[0].encode(&mut data)?;
                    if self.dataset_payload.is_empty() {
                        error!("Chunking needs dataset payload header");
                        return Err(StatusCode::BadInvalidArgument);
                    }
                    (
                        self.dataset_payload[0],
                        r[0].header.sequence_no.unwrap_or(network_no),
                    )
                }
                UadpPayload::DiscoveryRequest(r) => {
                    r.encode(&mut data)?;
                    (0, network_no)
                }
                UadpPayload::DiscoveryResponse(r) => {
                    r.encode(&mut data)?;
                    (0, network_no)
                }
                UadpPayload::Chunk(_) => {
                    error!("Chunking a chunk is not supported");
                    return Err(StatusCode::BadNotSupported);
                }
                UadpPayload::None => (0, 0),
            };
            // Worst case 10 Bytes for the Chunkdataheader + 1 Byte for longer header
            let overhead_sz = body_sz + 11;
            if overhead_sz > max_size {
                error!(
                    "Cant chunk message because headers cant go blow {}",
                    overhead_sz
                );
                return Err(StatusCode::BadEncodingLimitsExceeded);
            }
            let chunk_sz = max_size - overhead_sz;
            let total = data.len() as u32;
            let mut vec = Vec::new();
            for (i, chunk) in data.chunks(chunk_sz).enumerate() {
                vec.push(UadpNetworkMessage {
                    header: self.header.clone(),
                    group_header: self.group_header.clone(),
                    dataset_payload: vec![payload_header],
                    timestamp: self.timestamp,
                    picoseconds: self.picoseconds,
                    promoted_fields: self.promoted_fields.clone(),
                    payload: UadpPayload::Chunk(UadpChunk {
                        message_sequence_no: network_no,
                        chunk_offset: (i * chunk_sz) as u32,
                        total_size: total,
                        chunk_data: chunk.to_vec(),
                    }),
                });
            }
            Ok(vec)
        } else {
            Ok(vec![self])
        }
    }

    fn dechunk_internal(mut self, mut parts: Vec<UadpChunk>) -> Result<Self, StatusCode> {
        let flags = self.header.flags;
        let net_no = parts.first().unwrap().message_sequence_no;
        // Sort alle parts
        if !parts.iter().all(|c| net_no == c.message_sequence_no) {
            error!("Not all message numbers are the same");
            return Err(StatusCode::BadInvalidState);
        }
        parts.sort_by(|a, b| a.chunk_offset.cmp(&b.chunk_offset));
        let data = parts.into_iter().fold(Vec::new(), |mut a, d| {
            a.extend(d.chunk_data);
            a
        });
        let decoding_opts = DecodingOptions::default();
        self.payload = if flags.contains(MessageHeaderFlags::DISCOVERYREQUEST) {
            UadpPayload::DiscoveryRequest(Box::new(UadpDiscoveryRequest::decode(
                &mut data.as_slice(),
                &decoding_opts,
            )?))
        } else if flags.contains(MessageHeaderFlags::DISCOVERYRESPONSE) {
            UadpPayload::DiscoveryResponse(Box::new(UadpDiscoveryResponse::decode(
                &mut data.as_slice(),
                &decoding_opts,
            )?))
        } else {
            UadpPayload::DataSets(vec![UadpDataSetMessage::decode(
                &mut data.as_slice(),
                &decoding_opts,
                &self.dataset_payload,
            )?])
        };
        Ok(self)
    }
    // Dechunk from a msg and uadpchunks
    pub fn dechunk_msg(&self, parts: Vec<UadpChunk>) -> Result<Self, StatusCode> {
        let ret = UadpNetworkMessage {
            header: self.header.clone(),
            group_header: self.group_header.clone(),
            dataset_payload: self.dataset_payload.clone(),
            timestamp: self.timestamp,
            picoseconds: self.picoseconds,
            promoted_fields: self.promoted_fields.clone(),
            payload: UadpPayload::None,
        };
        ret.dechunk_internal(parts)
    }

    /// Transforms multiple chunked messages into one message
    pub fn dechunk(mut msgs: Vec<Self>) -> Result<Self, StatusCode> {
        match msgs.len() {
            0 => {
                error!("No message found");
                return Err(StatusCode::BadNoData);
            }
            1 => match msgs[0].payload {
                UadpPayload::Chunk(_) => {}
                _ => return Ok(msgs.pop().unwrap()),
            },
            _ => {
                if msgs
                    .iter()
                    .filter(|a| !matches!(&a.payload, UadpPayload::Chunk(_)))
                    .count()
                    > 0
                {
                    error!("Messages contains non chunked messages");
                    return Err(StatusCode::BadDecodingError);
                }
            }
        }
        let msg1 = msgs.first().unwrap();
        let ret = UadpNetworkMessage {
            header: msg1.header.clone(),
            group_header: msg1.group_header.clone(),
            dataset_payload: msg1.dataset_payload.clone(),
            timestamp: msg1.timestamp,
            picoseconds: msg1.picoseconds,
            promoted_fields: msg1.promoted_fields.clone(),
            payload: UadpPayload::None,
        };
        let parts: Vec<UadpChunk> = msgs
            .into_iter()
            .map(|m| match m.payload {
                UadpPayload::Chunk(c) => c,
                _ => panic!("Cant get chunks"),
            })
            .collect();
        ret.dechunk_internal(parts)
    }
}

impl Default for UadpNetworkMessage {
    fn default() -> Self {
        Self::new()
    }
}

/// Combines incoming Messages Chunks into complete Messages
pub struct UadpMessageChunkManager {
    messages: Vec<UadpChunk>,
    last_sequence_no: u16,
    dataset_id: u16,
}

fn is_sequence_newer(sequence_no: u16, last_sequence_no: u16) -> bool {
    let v = (65535_u32 + u32::from(sequence_no) - u32::from(last_sequence_no)) % 65536;
    v < 16384
}

impl UadpMessageChunkManager {
    pub fn new(dataset_id: u16) -> Self {
        UadpMessageChunkManager {
            messages: Vec::new(),
            last_sequence_no: u16::MAX,
            dataset_id,
        }
    }

    fn try_build_message(&mut self, msg: &UadpNetworkMessage) -> Option<UadpNetworkMessage> {
        // Sort Messages
        self.messages.sort_by(|a, b| {
            if a.message_sequence_no == b.message_sequence_no {
                a.chunk_offset.cmp(&b.chunk_offset)
            } else {
                b.message_sequence_no.cmp(&a.message_sequence_no)
            }
        });
        // @FIXME messy, maybe do some iterator magic here
        // Find out if a sequence is completed. Alle elements are sorted seq_no descending and chunk_offset ascending
        //   seq_no: chunk_offset
        // | 123: 0 | 123: 444 | 123: 666 | 122: 0 | 122: 1000 | ....
        let mut seq = self.last_sequence_no;
        let mut p = 0;
        let mut beg = 0;
        let mut ret = usize::MAX;
        for (i, msg) in self.messages.iter().enumerate() {
            if msg.message_sequence_no != seq {
                seq = msg.message_sequence_no;
                p = 0;
                beg = i;
            }
            if msg.chunk_offset != p {
                p = 0;
            } else {
                p += u32::try_from(msg.chunk_data.len()).unwrap();
                if p == msg.total_size {
                    ret = i;
                    break;
                }
            }
        }
        if ret != usize::MAX {
            let chunks = self.messages.drain(beg..=ret).collect();
            let res = msg.dechunk_msg(chunks);
            if res.is_ok() {
                // Remove older messages and set last_sequence_no
                self.last_sequence_no = seq;
                let cur_seq = self.last_sequence_no;
                self.messages
                    .retain(|x| is_sequence_newer(x.message_sequence_no, cur_seq));
            }
            res.ok()
        } else {
            None
        }
    }

    /// Adds a new chunk message. If the message is a completed return a dechunked message
    pub fn add_chunk(&mut self, msg: &UadpNetworkMessage) -> Option<UadpNetworkMessage> {
        let dataset_id = msg.dataset_payload.first();
        let mut added = false;
        if let UadpPayload::Chunk(chunk) = &msg.payload {
            if let Some(id) = dataset_id {
                if *id == self.dataset_id {
                    // Filter out older messages
                    if is_sequence_newer(chunk.message_sequence_no, self.last_sequence_no) {
                        self.messages.push(chunk.clone());
                        added = true;
                    }
                }
            } else {
                warn!("Message without dataset_id");
            }
        }
        if added {
            return self.try_build_message(msg);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use opcua_types::{ConfigurationVersionDataType, LocalizedText};

    use super::*;
    use std::io::Cursor;
    #[test]
    fn encode_decode_test() -> Result<(), StatusCode> {
        let mut msg = UadpNetworkMessage::new();
        msg.timestamp = Some(opcua_types::DateTime::now());
        let var = vec![Variant::from("Test123"), Variant::from(64)];
        msg.payload = UadpPayload::DataSets(vec![UadpDataSetMessage::new(
            UadpMessageType::KeyFrameVariant(var),
        )]);
        let mut data = Vec::new();
        let sz = msg.byte_len();
        let msg_sz = msg.encode(&mut data)?;
        assert_eq!(sz, msg_sz);
        let mut c = Cursor::new(data);
        let dec = match UadpNetworkMessage::decode(&mut c, &DecodingOptions::default()) {
            Ok(d) => d,
            Err(err) => panic!("decode failed {}", err),
        };
        assert_eq!(dec.timestamp, msg.timestamp);
        assert_eq!(dec.payload, msg.payload);
        assert_eq!(dec, msg);
        Ok(())
    }
    #[test]
    fn test_parts() -> Result<(), StatusCode> {
        let mut msg = UadpNetworkMessage::new();
        let var = vec![Variant::from("Test123"), Variant::from(64)];
        let mut ds = UadpDataSetMessage::new(UadpMessageType::KeyFrameVariant(var));
        ds.header.cfg_major_version = Some(1234);
        ds.header.cfg_minor_version = Some(12345);
        ds.header.time_stamp = Some(DateTime::now());
        msg.payload = UadpPayload::DataSets(vec![ds]);

        let mut data = Vec::new();
        let sz = msg.byte_len();
        let msg_sz = msg.encode(&mut data)?;
        assert_eq!(sz, msg_sz);
        let mut c = Cursor::new(data);
        let dec = match UadpNetworkMessage::decode(&mut c, &DecodingOptions::default()) {
            Ok(d) => d,
            Err(err) => panic!("decode failed {}", err),
        };
        assert_eq!(dec.timestamp, msg.timestamp);
        assert_eq!(dec.payload, msg.payload);
        assert_eq!(dec, msg);
        Ok(())
    }

    #[test]
    fn test_header() -> Result<(), StatusCode> {
        let mut msg = UadpNetworkMessage::new();
        msg.header.publisher_id = Some(12345_u16.into());
        msg.header.dataset_class_id = Some(Guid::new());
        msg.dataset_payload.push(1234);
        let mut gp = UadpGroupHeader::new();
        gp.group_version = Some(1234553_u32);
        gp.network_message_no = Some(123_u16);
        gp.sequence_no = Some(13_u16);
        gp.writer_group_id = Some(555_u16);
        msg.group_header = Some(gp);
        msg.timestamp = Some(opcua_types::DateTime::now());
        let var = vec![Variant::from("Test123"), Variant::from(64)];
        let mut ds = UadpDataSetMessage::new(UadpMessageType::KeyFrameVariant(var));
        ds.header.cfg_major_version = Some(1234);
        ds.header.cfg_minor_version = Some(12345);
        ds.header.time_stamp = Some(DateTime::now());
        msg.payload = UadpPayload::DataSets(vec![ds]);
        let mut data = Vec::new();
        let sz = msg.byte_len();
        let msg_sz = msg.encode(&mut data)?;
        assert_eq!(sz, msg_sz);
        let mut c = Cursor::new(data);
        let dec = UadpNetworkMessage::decode(&mut c, &DecodingOptions::default())?;
        assert_eq!(dec, msg);
        assert_eq!(dec.timestamp, msg.timestamp);
        assert_eq!(dec.payload, msg.payload);
        Ok(())
    }
    #[test]
    /// Test som discovery messages encode/decodes
    fn test_discovery_messages() -> Result<(), StatusCode> {
        {
            let mut msg = UadpNetworkMessage::new();
            msg.header.publisher_id = Some(1234_u16.into());
            msg.payload = UadpPayload::DiscoveryRequest(Box::new(UadpDiscoveryRequest::new(
                InformationType::DataSetMetaData,
                Some(vec![12u16, 13u16]),
            )));
            let mut data = Vec::new();
            let sz = msg.byte_len();
            let enc_sz = msg.encode(&mut data)?;
            assert_eq!(sz, enc_sz);
            let mut c = Cursor::new(data);
            let dec = UadpNetworkMessage::decode(&mut c, &DecodingOptions::default())?;
            assert_eq!(dec, msg);
        }
        {
            let mut msg = UadpNetworkMessage::new();
            msg.header.publisher_id = Some(1234_u16.into());
            msg.payload = UadpPayload::DiscoveryRequest(Box::new(UadpDiscoveryRequest::new(
                InformationType::DataSetWriter,
                Some(vec![12u16]),
            )));
            let mut data = Vec::new();
            let sz = msg.byte_len();
            let enc_sz = msg.encode(&mut data)?;
            assert_eq!(sz, enc_sz);
            let mut c = Cursor::new(data);
            let dec = UadpNetworkMessage::decode(&mut c, &DecodingOptions::default())?;
            assert_eq!(dec, msg);
        }
        {
            let mut msg = UadpNetworkMessage::new();
            msg.header.publisher_id = Some(1234_u16.into());
            msg.payload = UadpPayload::DiscoveryRequest(Box::new(UadpDiscoveryRequest::new(
                InformationType::PublisherEndpoints,
                None,
            )));
            let mut data = Vec::new();
            let sz = msg.byte_len();
            let enc_sz = msg.encode(&mut data)?;
            assert_eq!(sz, enc_sz);
            let mut c = Cursor::new(data);
            let dec = UadpNetworkMessage::decode(&mut c, &DecodingOptions::default())?;
            assert_eq!(dec, msg);
        }
        {
            let mut msg = UadpNetworkMessage::new();
            msg.header.publisher_id = Some(1234_u16.into());
            let resp = ResponseType::DataSetMetaData(UadpDataSetMetaDataResp {
                dataset_writer_id: 12u16,
                meta_data: DataSetMetaDataType {
                    namespaces: None,
                    structure_data_types: None,
                    enum_data_types: None,
                    simple_data_types: None,
                    name: "Abc".into(),
                    description: LocalizedText::new("test", "en"),
                    fields: None,
                    data_set_class_id: Guid::new(),
                    configuration_version: ConfigurationVersionDataType {
                        major_version: 123,
                        minor_version: 123,
                    },
                },
                status: StatusCode::GoodDataIgnored,
            });
            msg.payload = UadpPayload::DiscoveryResponse(Box::new(UadpDiscoveryResponse::new(
                InformationType::DataSetMetaData,
                1,
                resp,
            )));
            let mut data = Vec::new();
            let sz = msg.byte_len();
            let enc_sz = msg.encode(&mut data)?;
            assert_eq!(sz, enc_sz);
            let mut c = Cursor::new(data);
            let dec = UadpNetworkMessage::decode(&mut c, &DecodingOptions::default())?;
            assert_eq!(dec, msg);
        }
        Ok(())
    }
    #[test]
    fn chunking_test() -> Result<(), StatusCode> {
        let mut msg = UadpNetworkMessage::new();
        msg.header.publisher_id = Some(12345_u16.into());
        msg.header.dataset_class_id = Some(Guid::new());
        msg.dataset_payload.push(1234);
        let mut gp = UadpGroupHeader::new();
        gp.group_version = Some(1234553_u32);
        gp.network_message_no = Some(123_u16);
        gp.sequence_no = Some(13_u16);
        gp.writer_group_id = Some(555_u16);
        msg.group_header = Some(gp);
        msg.timestamp = Some(opcua_types::DateTime::now());
        let var = vec![Variant::from("Superlongstringtotriggerchunking and stuff"); 100];
        let mut ds = UadpDataSetMessage::new(UadpMessageType::KeyFrameVariant(var));
        ds.header.cfg_major_version = Some(1234);
        ds.header.cfg_minor_version = Some(12345);
        ds.header.time_stamp = Some(DateTime::now());
        msg.payload = UadpPayload::DataSets(vec![ds]);
        let org_msg = msg.clone();
        let msgs = msg.chunk(1000, 1)?;
        let ne_msg = UadpNetworkMessage::dechunk(msgs)?;
        assert_eq!(ne_msg.header, org_msg.header);
        assert_eq!(ne_msg.group_header, org_msg.group_header);
        if let UadpPayload::DataSets(ne) = &ne_msg.payload {
            if let UadpPayload::DataSets(org) = &org_msg.payload {
                assert_eq!(ne.len(), org.len());
                let ne_d = ne.first().unwrap();
                let org_d = org.first().unwrap();
                assert_eq!(ne_d.header, org_d.header);
                if let UadpMessageType::KeyFrameVariant(n) = &ne_d.data {
                    if let UadpMessageType::KeyFrameVariant(o) = &org_d.data {
                        assert_eq!(n.len(), o.len());
                        assert_eq!(n, o);
                    }
                }
            }
        }
        assert_eq!(ne_msg.dataset_payload, org_msg.dataset_payload);
        assert_eq!(ne_msg, org_msg);
        Ok(())
    }
}
