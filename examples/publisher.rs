use opcua_pubsub::prelude::*;
use opcua_server::prelude::*;
use std::sync::{Arc, RwLock};
use std::env;

fn create_server() -> Server{
    let port = 4855;
    ServerBuilder::new_anonymous("UadpPublisher")
        .application_name("UadpPublisher")
        .application_uri("urn:uadppublisher")
        .product_uri("urn:uadppublisher Test")
        .create_sample_keypair(true)
        .host_and_port("127.0.0.1", port)
        .discovery_urls(vec![
            "/".into()
        ])
        .server().unwrap()
}


fn generate_namespace(server: &Server) -> u16{
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
        Variable::new(&v1_node, "Int32Var", "Int32Var", 4444 as i32),
        Variable::new(&v2_node, "Int64Var", "Int64Var", 12345 as i64),
        Variable::new(&v3_node, "BoolToggle", "BoolToggle", false),
    ];
    vars.iter_mut().for_each(|v| {
        v.set_writable(true);
        v.set_user_access_level(UserAccessLevel::CURRENT_WRITE | UserAccessLevel::CURRENT_READ);
    });
    let _ = address_space.add_variables(
        vars,
        &folder_id,
    );
    return ns;
}

// Generates the Publisher
fn generate_pubsub(ns: u16, server: &Server) -> Result<Arc<RwLock<PubSubConnection>>, StatusCode>{
    let url = "224.0.0.22:4840"; // "opc.udp://224.0.0.22:4840/";
    // Create a pubsub connection
    let pubsub = Arc::new(RwLock::new(PubSubConnectionBuilder::new()
        .set_url(url.into())
        .set_publisher_id(Variant::UInt16(2234))
        .build(Some(server.address_space()))?));
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
    // Configure a Writer Groupe which is responsable for sending the messages  
    let msg_settings: UadpNetworkMessageContentFlags = 
        UadpNetworkMessageContentFlags::PUBLISHERID |
        UadpNetworkMessageContentFlags::GROUPHEADER |
        UadpNetworkMessageContentFlags::WRITERGROUPID |
        UadpNetworkMessageContentFlags::PAYLOADHEADER;
    let mut wg = WriterGroupeBuilder::new()
        .set_name("WriterGroupe1".into())
        .set_groupe_id(100)
        .set_message_setting(msg_settings)
        .set_publish_interval(1000.0)
        .build();
    // Glue the writer groupe and published dataset together with a 
    // dataset writer
    let dsw = DataSetWriterBuilder::new(&dataset)
        .set_key_frame_count(1)
        .set_dataset_writer_id(62541)
        .set_name("DataSetWriter1".into())
        .build();
    wg.add_dataset_writer(dsw);
    { 
        let mut ps = pubsub.write().unwrap();
        ps.add_writer_groupe(wg);
        ps.add_dataset(dataset);
    }
    Ok(pubsub)
}

fn main() -> Result<(), StatusCode> {
    env::set_var("RUST_OPCUA_LOG", "INFO");
    opcua_console_logging::init();
    let mut server = create_server();
    let ns = generate_namespace(&server);
    let pubsub = generate_pubsub(ns, &server)?;
    let polling_time_ms = 1000;
    // Atm drive the pubsub via polling action
    server.add_polling_action(polling_time_ms, move || {
        let mut ps = pubsub.write().unwrap();
        ps.poll(polling_time_ms);
    });
    // Run the server. This does not ordinarily exit so you must Ctrl+C to terminate
    server.run();
    Ok(())
}

