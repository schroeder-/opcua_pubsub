// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use opcua_pubsub::prelude::*;
use rand::prelude::*;
use std::sync::{Arc, RwLock};
use std::{thread, time};

/// Test Mqtt

// Generates the Publisher
fn generate_pubsub(
    ns: u16,
    addr: &Arc<RwLock<SimpleAddressSpace>>,
) -> Result<Arc<RwLock<PubSubConnection>>, StatusCode> {
    let topic: UAString = "OPCUA_TEST/Data".into();
    let topic_meta: UAString = "OPCUA_TEST/Meta".into();
    const META_INTERVAL: f64 = 0.0;
    const QOS: BrokerTransportQualityOfService = BrokerTransportQualityOfService::BestEffort;
    let broker: UAString = "mqtt://localhost:1883".into();

    // Create a pubsub connection
    let mut pubsub = PubSubConnectionBuilder::new()
        .mqtt(MqttConfig::new(broker))
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
    let msg_settings: UadpNetworkMessageContentFlags = UadpNetworkMessageContentFlags::PUBLISHERID
        | UadpNetworkMessageContentFlags::GROUPHEADER
        | UadpNetworkMessageContentFlags::WRITERGROUPID
        | UadpNetworkMessageContentFlags::PAYLOADHEADER;
    let mut wg = WriterGroupBuilder::new_for_broker(&topic, &QOS)
        .set_name("WriterGroup1".into())
        .set_group_id(100)
        .set_message_setting(msg_settings)
        .set_publish_interval(1000.0)
        .build();
    // Glue the writer group and published dataset together with a
    // dataset writer
    let dsw = DataSetWriterBuilder::new_for_broker(&dataset, META_INTERVAL, &topic_meta, &QOS)
        .set_key_frame_count(1)
        .set_dataset_writer_id(62541)
        .set_name("DataSetWriter1".into())
        .build();
    wg.add_dataset_writer(dsw);
    pubsub.add_writer_group(wg);
    pubsub.add_dataset(dataset);

    // create a reader group to handle incoming messages
    let mut rg = ReaderGroup::new("Reader Group 1".into());
    // build the dataset reader to receive values.
    // publisherid, writergroupid and datasetwriterid are to target publisher and dataset
    let mut dsr = DataSetReaderBuilder::new_for_broker(&topic, &topic_meta, QOS)
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
    for (j, x) in fields.iter().enumerate() {
        let i = j + 4;
        // Finally target server variables as destination for the values
        DataSetTargetBuilder::new_from_guid(x.clone())
            .target_node_id(&NodeId::new(ns, i as u32))
            .insert(&mut dsr);
    }
    rg.add_dataset_reader(dsr);
    pubsub.add_reader_group(rg);
    Ok(Arc::new(RwLock::new(pubsub)))
}

#[test]
fn test_mqtt() -> Result<(), StatusCode> {
    opcua_console_logging::init();
    let data_source = SimpleAddressSpace::new_arc_lock();
    let nodes: Vec<NodeId> = (0..8).map(|i| NodeId::new(0, i as u32)).collect();
    // Generating a pubsubconnection
    let pubsub = generate_pubsub(0, &data_source)?;
    // Spawn a pubsub connection
    PubSubConnection::run_thread(pubsub);
    // Simulate a working loop where data is produced
    let mut rng = rand::thread_rng();

    for i in 0..100 {
        // Update values Time every 100ms all others all 10 seconds
        {
            let mut ds = data_source.write().unwrap();
            ds.set_value(&nodes[0], DataValue::new_now(DateTime::now()));
            if i % 10 == 0 {
                ds.set_value(&nodes[1], DataValue::new_now(rng.gen::<i32>()));
                ds.set_value(&nodes[2], DataValue::new_now(rng.gen::<i64>()));
                ds.set_value(&nodes[3], DataValue::new_now(rng.gen::<bool>()));
            }
        }
        thread::sleep(time::Duration::from_millis(100));
    }
    thread::sleep(time::Duration::from_secs(5));
    let ds = data_source.write().unwrap();
    for i in 0..4 {
        // Only check values because timestamps can differ
        assert_eq!(
            ds.get_value(&nodes[i]).unwrap().value,
            ds.get_value(&nodes[i + 4]).unwrap().value
        )
    }
    Ok(())
}
