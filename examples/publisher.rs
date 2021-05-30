// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use opcua_pubsub::prelude::*;
use opcua_server::prelude::*;
use std::env;
use std::sync::{Arc, RwLock};

fn create_server() -> Server {
    let port = 4855;
    ServerBuilder::new_anonymous("UadpPublisher")
        .application_name("UadpPublisher")
        .application_uri("urn:uadppublisher")
        .product_uri("urn:uadppublisher Test")
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
        .register_namespace("urn:pubsubPublisher")
        .unwrap();
    let folder_id = address_space
        .add_folder("Variables", "Variables", &NodeId::objects_folder_id())
        .unwrap();
    let v1_node = NodeId::new(ns, 1);
    let v2_node = NodeId::new(ns, 2);
    let v3_node = NodeId::new(ns, 3);
    let mut vars = vec![
        Variable::new(&v1_node, "Int32Var", "Int32Var", 4444_i32),
        Variable::new(&v2_node, "Int64Var", "Int64Var", 12345_i64),
        Variable::new(&v3_node, "BoolToggle", "BoolToggle", false),
    ];
    vars.iter_mut().for_each(|v| {
        v.set_writable(true);
        v.set_user_access_level(UserAccessLevel::CURRENT_WRITE | UserAccessLevel::CURRENT_READ);
    });
    let _ = address_space.add_variables(vars, &folder_id);
    ns
}

// Generates the Publisher
fn generate_pubsub(ns: u16, server: &Server) -> Result<Arc<RwLock<PubSubApp>>, StatusCode> {
    let url = "opc.udp://224.0.0.22:4840";
    // Create a pubsub connection
    let pubsub = Arc::new(RwLock::new(PubSubApp::new()));
    let mut connection = PubSubConnectionBuilder::new()
        .uadp(UadpConfig::new(url.into()))
        .publisher_id(Variant::UInt16(2234))
        .build(server.address_space())?;
    // Create a Published Dataset with the fields to publish
    let dataset_name = "Dataset 1".into();
    let mut dataset = PublishedDataSet::new(dataset_name);
    // add fields to the dataset
    DataSetFieldBuilder::new()
        .set_target_variable(VariableId::Server_ServerStatus_CurrentTime.into())
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
    {
        let mut ps = pubsub.write().unwrap();
        ps.add_dataset(dataset)?;
        ps.add_connection(connection)?;
    }
    Ok(pubsub)
}

fn main() -> Result<(), StatusCode> {
    env::set_var("RUST_OPCUA_LOG", "INFO");
    opcua_console_logging::init();
    let server = create_server();
    let ns = generate_namespace(&server);
    let pubsub = generate_pubsub(ns, &server)?;
    // Run pubsub
    PubSubApp::run_thread(pubsub);
    // Run the server. This does not ordinarily exit so you must Ctrl+C to terminate
    server.run();
    Ok(())
}
