// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use opcua_pubsub::prelude::*;
use rand::prelude::*;
use std::sync::{Arc, RwLock};
use std::{thread, time};

/// In this example a publisher without a server is create
/// This you to fill an simple address space with opc ua values.
/// The values are automatic published via the pubsubconnection,
/// if they are in a published dataset

// Generates the Publisher
fn generate_pubsub(
    ns: u16,
    addr: &Arc<RwLock<SimpleAddressSpace>>,
) -> Result<Arc<RwLock<PubSubApp>>, StatusCode> {
    let url = "opc.udp://224.0.0.22:4840";
    // Create App
    let mut pubsub = PubSubApp::new();
    // Create a pubsub connection
    let mut connection = PubSubConnectionBuilder::new()
        .uadp(UadpConfig::new(url.into()))
        .publisher_id(Variant::UInt16(2234))
        .build(addr.clone())?;
    // Create a Published Dataset with the fields to publish
    let dataset_name = "Dataset 1".into();
    let mut dataset = PublishedDataSet::new(dataset_name);
    // add fields to the dataset
    DataSetFieldBuilder::new()
        .set_target_variable(NodeId::new(ns, 0))
        .set_alias("ServerTime".into())
        .insert(&mut dataset);
    DataSetFieldBuilder::new()
        .set_target_variable(NodeId::new(ns, 1))
        .set_alias("Int32".into())
        .insert(&mut dataset);
    DataSetFieldBuilder::new()
        .set_target_variable(NodeId::new(ns, 2))
        .set_alias("Int64".into())
        .insert(&mut dataset);
    DataSetFieldBuilder::new()
        .set_target_variable(NodeId::new(ns, 3))
        .set_alias("BoolToggle".into())
        .insert(&mut dataset);
    // Configure a Writer Group which is responsable for sending the messages
    let msg_settings: UadpNetworkMessageContentMask = UadpNetworkMessageContentMask::PublisherId
        | UadpNetworkMessageContentMask::GroupHeader
        | UadpNetworkMessageContentMask::WriterGroupId
        | UadpNetworkMessageContentMask::PayloadHeader;
    let mut wg = WriterGroupBuilder::new()
        .set_name("WriterGroup1".into())
        .set_group_id(100)
        .set_message_setting(msg_settings)
        .set_publish_interval(1000.0)
        .build();
    // Glue the writer group and published dataset together with a
    // dataset writer
    let dsw = DataSetWriterBuilder::new(&dataset)
        .key_frame_count(1)
        .dataset_writer_id(62541)
        .name("DataSetWriter1".into())
        .build();
    wg.add_dataset_writer(dsw);
    connection.add_writer_group(wg);
    pubsub.add_dataset(dataset)?;
    pubsub.add_connection(connection)?;
    Ok(Arc::new(RwLock::new(pubsub)))
}

fn main() -> Result<(), StatusCode> {
    opcua_console_logging::init();
    let data_source = SimpleAddressSpace::new_arc_lock();
    let nodes: Vec<NodeId> = (0..3).map(|i| NodeId::new(0, i as u32)).collect();
    // Generating a pubsubconnection
    let pubsub = generate_pubsub(0, &data_source)?;
    // Spawn a pubsub connection
    PubSubApp::run_thread(pubsub);
    // Simulate a working loop where data is produced
    let mut rng = rand::thread_rng();
    let mut i = 0_u32;
    loop {
        // Update values Time every 100ms all others all 10 seconds
        {
            let mut ds = data_source.write().unwrap();
            ds.set_value(&nodes[0], DataValue::new_now(DateTime::now()));
            if i % 100 == 0 {
                ds.set_value(&nodes[1], DataValue::new_now(rng.gen::<i32>()));
                ds.set_value(&nodes[2], DataValue::new_now(rng.gen::<i64>()));
                ds.set_value(&nodes[3], DataValue::new_now(rng.gen::<bool>()));
            }
        }
        i = i.wrapping_add(1);
        thread::sleep(time::Duration::from_millis(100));
    }
}
