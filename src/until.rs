// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use opcua_types::status_code::StatusCode;
use opcua_types::{BinaryEncoder, DecodingOptions, ExtensionObject, ObjectId};

/// Decode Object from ExtensionObject helper function
pub fn decode_extension<T: BinaryEncoder<T>>(
    eobj: &ExtensionObject,
    objid: ObjectId,
    dec_opts: &DecodingOptions,
) -> Result<T, StatusCode> {
    if let Ok(obj) = eobj.object_id() {
        if obj == objid {
            eobj.decode_inner::<T>(dec_opts)
        } else {
            Err(StatusCode::BadDecodingError)
        }
    } else {
        Err(StatusCode::BadDecodingError)
    }
}


pub (crate) fn is_sequence_newer(sequence_no: u16, last_sequence_no: u16) -> bool {
    let v = (65535_u32 + u32::from(sequence_no) - u32::from(last_sequence_no)) % 65536;
    v < 16384
}