// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2020 Alexander Schrode
use opcua_pubsub::message::{UadpDataSetMessage, UadpMessageType, UadpNetworkMessage, UadpPayload};
use opcua_pubsub::prelude::*;
use std::{thread, time};

/// This example implements publishing data via uadpmessage with out application logic.
/// If just need to send fixed data to an opc ua udap subcriber, this is all you need.
/// Requires knowledge of the underlying protocol so you don't craft malformed messages.
fn main() -> Result<(), StatusCode> {
    opcua_console_logging::init();
    let url = "opc.udp://239.0.0.1:4840";
    // create a dummy datasource not need in this configuration
    let data_source = SimpleAddressSpace::new_arc_lock();
    let uadp_config = UadpConfig::new(url.into());
    let pubsub = PubSubConnection::new(
        uadp_config.into(),
        Variant::UInt16(1002),
        PubSubDataSource::new_arc(data_source),
        None,
    )?;
    let strs = vec![
        "ALFA", "BRAVO", "CHARLIE", "DELTA", "ECHO", "FOXTROT", "GOLF", "HOTEL", "INDIA",
        "JULIETT", "KILO", "LIMA", "MIKE", "NOVEMBER", "OSCAR", "PAPA", "QUEBEC", "ROMEO",
        "SIERRA", "TANGO", "UNIFORM", "VICTOR", "WHISKEY", "X-RAY", "YANKEE", "ZULU",
    ];
    let mut p: usize = 0;
    // Generate a message every second with random data
    loop {
        let mut msg = UadpNetworkMessage::new();
        msg.timestamp = Some(opcua_types::DateTime::now());
        let var = vec![Variant::from(strs[p % strs.len()]), Variant::from(p as u64)];
        msg.payload = UadpPayload::DataSets(vec![UadpDataSetMessage::new(
            UadpMessageType::KeyFrameVariant(var),
        )]);
        pubsub.send(&mut msg)?;
        p = p.wrapping_add(1);
        thread::sleep(time::Duration::from_millis(1000));
    }
}
