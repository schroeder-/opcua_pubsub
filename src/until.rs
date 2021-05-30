// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use opcua_types::status_code::StatusCode;
use opcua_types::{BinaryEncoder, DecodingOptions, ExtensionObject, ObjectId};
/// Decode Object from ExtensionObject helper function
/// ```
/// let ex = ExtensionObject::null();
/// let set = decode_extension::<opcua_types::UadpWriterGroupMessageDataType>(ex, opcua_types::ObjectId::UadpWriterGroupMessageDataType_Encoding_DefaultBinary, &DecodingOptions::default())?;
/// ```
pub fn decode_extension<T: BinaryEncoder<T>>(
    eobj: &ExtensionObject,
    objid: ObjectId,
    dec_opts: &DecodingOptions,
) -> Result<T, StatusCode> {
    if let Ok(obj) = eobj.object_id() {
        if obj == objid {
            eobj.decode_inner::<T>(&dec_opts)
        } else {
            Err(StatusCode::BadDecodingError)
        }
    } else {
        Err(StatusCode::BadDecodingError)
    }
}
