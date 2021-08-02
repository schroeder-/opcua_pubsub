// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use core::time;
use opcua_pubsub::prelude::*;
use rand::prelude::*;
use std::io::Cursor;

use std::sync::Arc;
use tokio::sync::RwLock;

// Read the configuration from binary file instead to configure everything
// @TODO Move to sync
#[tokio::main]
async fn main() -> Result<(), StatusCode> {
    let data = include_bytes!("../test_data/test_publisher.bin");
    let mut c = Cursor::new(data);
    let data_source = SimpleAddressSpace::new_arc_lock();
    let p =
        PubSubApp::new_from_binary(&mut c, Some(PubSubDataSource::new_arc(data_source.clone())))?;
    // Spawn a pubsub connection
    PubSubApp::run_async(Arc::new(RwLock::new(p))).await;
    // Simulate a working loop where data is produced
    let mut rng = rand::thread_rng();
    let mut i = 0_u32;
    loop {
        {
            let mut ds = data_source.write().unwrap();
            ds.set_value(
                &NodeId::new(1, "DateTime"),
                DataValue::new_now(DateTime::now()),
            );
            ds.set_value(&NodeId::new(0, 2258), DataValue::new_now(DateTime::now()));
            if i % 100 == 0 {
                ds.set_value(
                    &NodeId::new(1, "BoolToggle"),
                    DataValue::new_now(rng.gen::<bool>()),
                );
                ds.set_value(
                    &NodeId::new(1, "Int32"),
                    DataValue::new_now(rng.gen::<i32>()),
                );
                ds.set_value(
                    &NodeId::new(1, "Int32Fast"),
                    DataValue::new_now(rng.gen::<i32>()),
                );
                ds.set_value(
                    &NodeId::new(1, "DateTime"),
                    DataValue::new_now(DateTime::now()),
                )
            }
        }
        i = i.wrapping_add(1);
        tokio::time::sleep(time::Duration::from_millis(100)).await;
    }
}
