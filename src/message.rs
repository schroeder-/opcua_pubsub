// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use log::{error, trace, warn};
use opcua_types::status_code::StatusCode;
use opcua_types::EncodingResult;
use opcua_types::{
    guid::Guid, string::UAString, BinaryEncoder, DataValue, DateTime, DecodingLimits, Variant,
};
use opcua_types::{process_decode_io_result, read_u16, read_u32, read_u64, read_u8};
use opcua_types::{process_encode_io_result, write_u16, write_u32, write_u8};
use std::io::{Read, Write};
/// Uadp Message flags See OPC Unified Architecture, Part 14 7.2.2.2.2
struct MessageHeaderFlags(u32);
#[allow(dead_code)]
impl MessageHeaderFlags {
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
#[derive(PartialEq, Debug)]
pub struct UadpHeader {
    pub publisher_id: Option<Variant>,
    pub dataset_class_id: Option<Guid>,
}

/// https://reference.opcfoundation.org/v104/Core/docs/Part4/7.38/
/// UInt32 as seconds since the year 2000. It is used for representing Version changes
type VersionTime = u32;
type UadpDataSetPayload = [u16];

/// Header of group part of an uadp message
#[derive(PartialEq, Debug)]
pub struct UadpGroupHeader {
    pub writer_group_id: Option<u16>,
    pub group_version: Option<VersionTime>,
    pub network_message_no: Option<u16>,
    pub sequence_no: Option<u16>,
}
#[derive(PartialEq, Debug)]
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
#[derive(PartialEq, Debug)]
pub enum UadpMessageType {
    /// a vector of variants
    KeyFrameVariant(Vec<Variant>),
    /// a vector of datavalues
    KeyFrameDataValue(Vec<DataValue>),
    /// raw data, encoding needs a description of the structured data
    KeyFrameRaw(Vec<Vec<u8>>),
    /// only changed variants are reported, u16 represents the position in dataset
    KeyDeltaFrameVariant(Vec<(u16, Variant)>),
    /// only changed datavalues are reported
    KeyDeltaFrameValue(Vec<(u16, DataValue)>),
    /// only raw changed elements of dataset
    KeyDeltaFrameRaw(Vec<(u16, Vec<u8>)>),
    /// an event which contains the data as variants
    Event(Vec<Variant>),
    /// keep alive message of the publisher
    KeepAlive,
}

#[derive(PartialEq, Debug)]
pub struct UadpDataSetMessage {
    pub header: UadpDataSetMessageHeader,
    pub data: UadpMessageType,
}

impl UadpHeader {
    fn encode<S: Write>(&self, stream: &mut S, msg: &UadpNetworkMessage) -> EncodingResult<usize> {
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
        if f > 0xFF {
            f |= MessageHeaderFlags::EXTENDED_FLAGS_1;
        }
        if f > 0xFFFF {
            f |= MessageHeaderFlags::EXTENDED_FLAGS_2;
        }
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

    fn decode<S: Read>(
        c: &mut S,
        limits: &DecodingLimits,
    ) -> EncodingResult<(UadpHeader, MessageHeaderFlags)> {
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
        Ok((
            UadpHeader {
                publisher_id,
                dataset_class_id,
            },
            f,
        ))
    }
}

impl UadpGroupHeader {
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

    fn decode<S: Read>(c: &mut S, _limits: &DecodingLimits) -> EncodingResult<UadpGroupHeader> {
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
    fn encode<S: Write>(&self, stream: &mut S, dt: &UadpMessageType) -> EncodingResult<usize> {
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
        decoding_options: &DecodingLimits,
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
        decoding_options: &DecodingLimits,
        flags: MessageDataSetFlags,
        pay_head: &UadpDataSetPayload,
    ) -> EncodingResult<Self> {
        if flags.contains(MessageDataSetFlags::DELTA_FRAME) {
            if flags.contains(MessageDataSetFlags::DATA_VALUE) {
                let count = read_u16(c)? as usize;
                let mut data = Vec::with_capacity(count);
                for _ in 0..count {
                    data.push((read_u16(c)?, DataValue::decode(c, decoding_options)?));
                }
                Ok(UadpMessageType::KeyDeltaFrameValue(data))
            } else if flags.contains(MessageDataSetFlags::RAW_DATA) {
                let count = read_u16(c)? as usize;
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
                let count = read_u16(c)? as usize;
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

    fn encode<S: Write>(&self, stream: &mut S) -> Result<usize, StatusCode> {
        let mut sz = self.header.encode(stream, &self.data)?;
        sz += self.data.encode(stream)?;
        Ok(sz)
    }

    fn decode<S: Read>(
        c: &mut S,
        decoding_options: &DecodingLimits,
        pay_head: &UadpDataSetPayload,
    ) -> EncodingResult<UadpDataSetMessage> {
        let (header, flags) = UadpDataSetMessageHeader::decode(c, decoding_options)?;
        let data = UadpMessageType::decode(c, decoding_options, flags, pay_head)?;
        Ok(UadpDataSetMessage { header, data })
    }
}

#[derive(PartialEq, Debug)]
pub struct UadpNetworkMessage {
    pub header: UadpHeader,
    pub group_header: Option<UadpGroupHeader>,
    pub dataset_payload: Vec<u16>,
    pub timestamp: Option<opcua_types::DateTime>,
    pub picoseconds: Option<u16>,
    pub promoted_fields: Vec<Variant>,
    pub dataset: Vec<UadpDataSetMessage>,
}

impl UadpNetworkMessage {
    /// creates an empty uadp network message
    pub fn new() -> Self {
        let dataset = Vec::new();
        let promoted_fields = Vec::new();
        let picoseconds = None;
        let timestamp = None;
        let dataset_payload = Vec::new();
        let group_header = None;
        let header = UadpHeader {
            dataset_class_id: None,
            publisher_id: None,
        };
        UadpNetworkMessage {
            header,
            group_header,
            dataset_payload,
            timestamp,
            picoseconds,
            promoted_fields,
            dataset,
        }
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
            sz += write_u16(stream, self.promoted_fields.len() as u16)?;
            for v in self.dataset.iter() {
                sz += v.encode(stream)?;
            }
        }
        if !self.dataset_payload.is_empty() {
            sz += write_u16(stream, self.dataset.len() as u16)?;
        }
        for v in self.dataset.iter() {
            sz += v.encode(stream)?;
        }
        Ok(sz)
    }

    pub fn decode<S: Read>(c: &mut S, decoding_options: &DecodingLimits) -> EncodingResult<Self> {
        let (header, flags) = UadpHeader::decode(c, decoding_options)?;
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
        let dataset = {
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
            v
        };
        Ok(UadpNetworkMessage {
            header,
            group_header,
            dataset_payload,
            timestamp,
            picoseconds,
            promoted_fields,
            dataset,
        })
    }
}

impl Default for UadpNetworkMessage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    #[test]
    fn encode_decode_test() -> Result<(), StatusCode> {
        let mut msg = UadpNetworkMessage::new();
        msg.timestamp = Some(opcua_types::DateTime::now());
        let var = vec![Variant::from("Test123"), Variant::from(64)];
        msg.dataset
            .push(UadpDataSetMessage::new(UadpMessageType::KeyFrameVariant(
                var,
            )));
        let mut data = Vec::new();

        msg.encode(&mut data)?;
        let mut c = Cursor::new(data);
        let dec = UadpNetworkMessage::decode(&mut c, &DecodingLimits::default())?;
        assert_eq!(dec.timestamp, msg.timestamp);
        assert_eq!(dec.dataset, msg.dataset);
        Ok(())
    }
    #[test]
    fn test_parts() -> Result<(), StatusCode> {
        let mut msg = UadpNetworkMessage::new();
        msg.timestamp = Some(opcua_types::DateTime::now());
        let var = vec![Variant::from("Test123"), Variant::from(64)];
        let mut ds = UadpDataSetMessage::new(UadpMessageType::KeyFrameVariant(var));
        ds.header.cfg_major_version = Some(1234);
        ds.header.cfg_minor_version = Some(12345);
        ds.header.time_stamp = Some(DateTime::now());
        msg.dataset.push(ds);

        let mut data = Vec::new();
        msg.encode(&mut data)?;
        let mut c = Cursor::new(data);
        let dec = UadpNetworkMessage::decode(&mut c, &DecodingLimits::default())?;
        assert_eq!(dec.timestamp, msg.timestamp);
        assert_eq!(dec.dataset, msg.dataset);
        Ok(())
    }
}
