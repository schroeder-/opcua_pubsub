// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode

// In this integration test the uadp communication is tested
use core::time;
use opcua_pubsub::connection::PubSubConnection;
use opcua_pubsub::message::{UadpDataSetMessage, UadpMessageType, UadpNetworkMessage, UadpPayload};
use opcua_pubsub::prelude::*;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::sync::{Arc, RwLock};
use std::{iter, thread};

#[test]
fn uadp_message_test() -> Result<(), StatusCode> {
    let strs = vec![
        "ALFA", "BRAVO", "CHARLIE", "DELTA", "ECHO", "FOXTROT", "GOLF", "HOTEL", "INDIA",
        "JULIETT", "KILO", "LIMA", "MIKE", "NOVEMBER", "OSCAR", "PAPA", "QUEBEC", "ROMEO",
        "SIERRA", "TANGO", "UNIFORM", "VICTOR", "WHISKEY", "X-RAY", "YANKEE", "ZULU",
    ];
    let mut sended = Vec::new();
    let url = "opc.udp://239.0.0.1:4840";
    let data_source = SimpleAddressSpace::new_arc_lock();
    let pubsub = PubSubConnection::new(
        UadpConfig::new(url.into()).into(),
        Variant::UInt16(1002),
        data_source,
        None,
    )?;
    const CNT: usize = 100;
    let recv = pubsub.create_receiver()?;
    let handler = thread::spawn(move || -> Result<Vec<UadpNetworkMessage>, StatusCode> {
        let mut recived = Vec::new();
        for _ in 0..CNT {
            let (_, msg) = recv.receive_msg()?;
            recived.push(msg);
        }
        Ok(recived)
    });

    for p in 0..CNT {
        let mut msg = UadpNetworkMessage::new();
        msg.timestamp = Some(opcua_types::DateTime::now());
        let var = vec![Variant::from(strs[p % strs.len()]), Variant::from(p as u64)];
        msg.payload = UadpPayload::DataSets(vec![UadpDataSetMessage::new(
            UadpMessageType::KeyFrameVariant(var),
        )]);
        pubsub.send(&mut msg)?;
        sended.push(msg);
    }
    let recived = handler.join().expect("Thread got error")?;
    assert_eq!(recived.len(), sended.len());
    for x in 0..CNT {
        assert_eq!(recived[x], sended[x]);
    }
    Ok(())
}

// Generates the Publisher
fn generate_pubsub(
    ns: u16,
    addr: &Arc<RwLock<SimpleAddressSpace>>,
) -> Result<Arc<RwLock<PubSubApp>>, StatusCode> {
    let url: UAString = "opc.udp://237.0.0.1:4840".into();
    // Create Application
    let mut pubsub = PubSubApp::new();
    // Create a pubsub connection
    let mut connection = PubSubConnectionBuilder::new()
        .uadp(UadpConfig::new(url))
        .publisher_id(Variant::UInt16(2234))
        .build(addr.clone())?;
    // Create a Published Dataset with the fields to publish
    let dataset_name = "Dataset 1".into();
    let mut dataset = PublishedDataSet::new(dataset_name);
    // add fields to the dataset
    DataSetFieldBuilder::new()
        .set_target_variable(NodeId::new(ns, 0))
        .set_alias("ExtraLongString".into())
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
    let fields = [PubSubFieldMetaDataBuilder::new()
        .data_type(&DataTypeId::String)
        .name("ExtraLongString".into())
        .insert(&mut dsr)];
    for (j, x) in fields.iter().enumerate() {
        let i = j + 4;
        // Finally target server variables as destination for the values
        DataSetTargetBuilder::new_from_guid(x.clone())
            .target_node_id(&NodeId::new(ns, i as u32))
            .insert(&mut dsr);
    }
    rg.add_dataset_reader(dsr);
    connection.add_reader_group(rg);
    pubsub.add_connection(connection)?;
    Ok(Arc::new(RwLock::new(pubsub)))
}

#[test]
fn uadp_chunk_test() -> Result<(), StatusCode> {
    std::env::set_var("RUST_OPCUA_LOG", "trace");
    opcua_console_logging::init();
    let data_source = SimpleAddressSpace::new_arc_lock();
    let nodes: Vec<NodeId> = (0..8).map(|i| NodeId::new(0, i as u32)).collect();
    // Generating a pubsubconnection
    let pubsub = generate_pubsub(0, &data_source)?;
    // Spawn a pubsub connection
    PubSubApp::run_thread(pubsub);
    // Simulate a working loop where data is produced
    let mut rng = rand::thread_rng();
    let dist = rand::distributions::Uniform::new_inclusive(3000, 5000);
    for _ in 0..30 {
        {
            let mut ds = data_source.write().unwrap();
            let sz = rng.sample(dist);
            let strs: String = iter::repeat(())
                .map(|()| rng.sample(Alphanumeric))
                .map(char::from)
                .take(sz)
                .collect();
            ds.set_value(&nodes[0], DataValue::new_now(UAString::from(strs)));
        }
        thread::sleep(time::Duration::from_millis(1000));
    }
    thread::sleep(time::Duration::from_secs(5));
    let ds = data_source.write().unwrap();
    for i in 0..1 {
        // Only check values because timestamps can differ
        assert_eq!(
            ds.get_value(&nodes[i]).unwrap().value,
            ds.get_value(&nodes[i + 4]).unwrap().value
        )
    }
    Ok(())
}
