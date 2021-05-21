// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode

// Example subscribes to values from pubsub and add them to server variables
use opcua_pubsub::prelude::*;
use opcua_server::prelude::*;
use std::env;
use std::sync::{Arc, RwLock};

fn create_server() -> Server {
    let port = 4850;
    ServerBuilder::new_anonymous("UadpSubscriber")
        .application_name("UadpSubscriber")
        .application_uri("urn:uadsubscriber")
        .product_uri("urn:uadpsubriber Test")
        .create_sample_keypair(true)
        .host_and_port("127.0.0.1", port)
        .discovery_urls(vec!["/".into()])
        .server()
        .unwrap()
}

fn generate_namespace(server: &Server) -> u16 {
    let address_space = server.address_space();
    let mut address_space = address_space.write().unwrap();
    let ns = address_space
        .register_namespace("urn:pubsubSubscriber")
        .unwrap();
    let folder_id = address_space
        .add_folder(
            "SubscribedVariables",
            "SubscribedVariables",
            &NodeId::objects_folder_id(),
        )
        .unwrap();
    let v0_node = NodeId::new(ns, 4);
    let v1_node = NodeId::new(ns, 1);
    let v2_node = NodeId::new(ns, 2);
    let v3_node = NodeId::new(ns, 3);
    let vars = vec![
        Variable::new(&v0_node, "Time", "Time", DateTime::null()),
        Variable::new(&v1_node, "Int32Var", "Int32Var", 4444 as i32),
        Variable::new(&v2_node, "Int64Var", "Int64Var", 12345 as i64),
        Variable::new(&v3_node, "BoolToggle", "BoolToggle", false),
    ];
    let _ = address_space.add_variables(vars, &folder_id);
    return ns;
}

// Generates the subscriber
fn generate_pubsub(ns: u16, server: &Server) -> Result<Arc<RwLock<PubSubConnection>>, StatusCode> {
    let url = "224.0.0.22:4840"; // "opc.udp://224.0.0.22:4840/";
                                 // Create a pubsub connection
    let pubsub = Arc::new(RwLock::new(
        PubSubConnectionBuilder::new()
            .set_url(url.into())
            .set_publisher_id(Variant::UInt16(2234))
            .build(server.address_space())?,
    ));
    // create a reader group to handle incoming messages
    let mut rg = ReaderGroup::new("Reader Group 1".into());
    // build the dataset reader to receive values.
    // publisherid, writergroupid and datasetwriterid are to target publisher and dataset
    let mut dsr = DataSetReaderBuilder::new()
        .name("DataSet Reader 1".into())
        .publisher_id(2234_u16.into())
        .writer_group_id(100)
        .dataset_writer_id(62541)
        .build();
    // Add the expected fields as MetaData
    let fields = [
        PubSubFieldMetaDataBuilder::new()
            .data_type(&DataTypeId::DateTime)
            .name("DateTime".into())
            .insert(&mut dsr),
        PubSubFieldMetaDataBuilder::new()
            .data_type(&DataTypeId::Int32)
            .name("Int32".into())
            .insert(&mut dsr),
        PubSubFieldMetaDataBuilder::new()
            .data_type(&DataTypeId::Int64)
            .name("Int64".into())
            .insert(&mut dsr),
        PubSubFieldMetaDataBuilder::new()
            .data_type(&DataTypeId::Boolean)
            .name("ToogleBool".into())
            .insert(&mut dsr),
    ];
    for j in 0..4 {
        let i = if j == 0 { 4 } else { j };
        let x = &fields[j];
        // Finally target server variables as destination for the values
        DataSetTargetBuilder::new_from_guid(x.clone())
            .target_node_id(&NodeId::new(ns, i as u32))
            .insert(&mut dsr);
    }
    rg.add_dataset_reader(dsr);
    {
        let mut ps = pubsub.write().unwrap();
        ps.add_reader_group(rg);
    }
    Ok(pubsub)
}

fn main() -> Result<(), StatusCode> {
    env::set_var("RUST_OPCUA_LOG", "INFO");
    opcua_console_logging::init();
    let server = create_server();
    let ns = generate_namespace(&server);
    let pubsub = generate_pubsub(ns, &server)?;
    // Run the pubsub
    PubSubConnection::run_thread(pubsub);
    // Run the server. This does not ordinarily exit so you must Ctrl+C to terminate
    server.run();
    Ok(())
}
