// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use opcua_pubsub::message::{UadpMessageType, UadpNetworkMessage};
use opcua_pubsub::prelude::*;

/// In this example messages a subscribe and print out.
/// The handling of the recevied message is up to the user, no logic is provided

fn got_message(msg: UadpNetworkMessage) {
    println!("Got new Message: ");
    if let Some(pid) = &msg.header.publisher_id {
        println!("PublisherId={}", pid);
    }
    if let Some(gp) = &msg.group_header {
        println!(
            "Group Header: WriterGroup={}",
            gp.writer_group_id.unwrap_or(0)
        );
    }
    let iter = msg.dataset_payload.iter();
    for it in iter {
        println!("Dataset {}", it);
    }
    if let Some(timestamp) = &msg.timestamp {
        println!("Timestamp: {}", timestamp);
        println!("Picoseconds: {}", msg.picoseconds.unwrap_or(0_u16));
    }

    for p in msg.promoted_fields.iter() {
        println!("Promotedfields: {}", p);
    }

    for ds in msg.dataset.iter() {
        match &ds.data {
            UadpMessageType::KeyFrameVariant(v) => {
                println!("# DataVariant");
                for vv in v.iter() {
                    println!("var: {}", vv);
                }
            }
            UadpMessageType::KeyFrameDataValue(v) => {
                println!("# DataValue");
                for vv in v.iter() {
                    println!("val: {:?}", vv);
                }
            }
            UadpMessageType::KeyFrameRaw(raw) => {
                println!("# DataRaw");
                for vv in raw.iter() {
                    println!("raw: {}", vv.len());
                }
            }
            UadpMessageType::KeyDeltaFrameVariant(dv) => {
                println!("# DeltaVariant");
                for (id, vv) in dv.iter() {
                    println!("id: {} var: {:?}", id, vv);
                }
            }
            UadpMessageType::KeyDeltaFrameValue(v) => {
                println!("# DeltaValue");
                for (id, vv) in v.iter() {
                    println!("id: {} val: {:?}", id, vv);
                }
            }
            UadpMessageType::KeyDeltaFrameRaw(_) => {
                println!("# DeltaRaw");
            }
            UadpMessageType::Event(v) => {
                println!("# Event");
                for vv in v.iter() {
                    println!("var: {:?}", vv);
                }
            }
            UadpMessageType::KeepAlive => {
                println!("# KeepAlive")
            }
        }
    }
}

fn main() -> Result<(), StatusCode> {
    opcua_console_logging::init();
    let url = "opc.udp://224.0.0.22:4840";
    let data_source = SimpleAddressSpace::new_arc_lock();
    let pubsub = PubSubConnection::new(url.to_string(), Variant::UInt16(1002), data_source, None)?;
    let receiver = pubsub.create_receiver()?;
    loop {
        match receiver.receive_msg() {
            Ok((_, msg)) => got_message(msg),
            Err(e) => return Err(e),
        };
    }
}
