// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use opcua_pubsub::prelude::*;
use rand::prelude::*;
use std::sync::{Arc, Mutex, RwLock};
use std::time;

/// Example uses Mqtt with Uadp encoding
/// The difference to uadp is, that you have to configure
/// datasets with broker settings

// Generates the Publisher
fn generate_pubsub(
    ns: u16,
    addr: &Arc<RwLock<SimpleAddressSpace>>,
    cb: Arc<Mutex<dyn OnPubSubReceiveValues + Send>>,
) -> Result<PubSubApp, StatusCode> {
    let topic: UAString = "OPCUA_TEST/Data".into();
    let topic_meta: UAString = "OPCUA_TEST/Meta".into();
    const META_INTERVAL: f64 = 0.0;
    const QOS: BrokerTransportQualityOfService = BrokerTransportQualityOfService::BestEffort;
    let broker: UAString = "mqtt://localhost:1883".into();
    // Create Application Object
    let mut pubsub = PubSubApp::new();
    // Create a pubsub connection
    let mut connection = PubSubConnectionBuilder::new()
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
    // Configure a Writer Group which is responsible for sending the messages
    let mut wg = WriterGroupBuilder::new_for_broker(&topic, &QOS)
        .set_name("WriterGroup1".into())
        .set_group_id(100)
        .set_publish_interval(1000.0)
        .build();
    // Glue the writer group and published dataset together with a
    // dataset writer
    let dsw = DataSetWriterBuilder::new_for_broker(&dataset, META_INTERVAL, &topic_meta, &QOS)
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
    // Publisher id, writer group id and dataset writer id are to target publisher and dataset
    let mut dsr = DataSetReaderBuilder::new_for_broker(&topic, &topic_meta, QOS)
        .name("DataSet Reader 1".into())
        .publisher_id(2234_u16.into())
        .writer_group_id(100)
        .dataset_writer_id(62541)
        .build();
    // Add the expected fields as MetaData
    PubSubFieldMetaDataBuilder::new()
        .data_type(&DataTypeId::DateTime)
        .name("DateTime".into())
        .insert(&mut dsr);
    PubSubFieldMetaDataBuilder::new()
        .data_type(&DataTypeId::Int32)
        .name("Int32".into())
        .insert(&mut dsr);
    PubSubFieldMetaDataBuilder::new()
        .data_type(&DataTypeId::Int64)
        .name("Int64".into())
        .insert(&mut dsr);
    PubSubFieldMetaDataBuilder::new()
        .data_type(&DataTypeId::Boolean)
        .name("ToggleBool".into())
        .insert(&mut dsr);

    rg.add_dataset_reader(dsr);
    connection.add_reader_group(rg);
    connection.set_data_value_recv(Some(cb));
    pubsub.add_connection(connection)?;

    Ok(pubsub)
}

fn on_value(reader: &DataSetReader, dataset: &[UpdateTarget]) {
    println!("#### Got Dataset from reader: {}", reader.name());
    for UpdateTarget(_, dv, meta) in dataset {
        println!("#### Variable: {} Value: {:?}", meta.name(), dv);
    }
}

// @TODO Move to sync
#[tokio::main]
async fn main() -> Result<(), StatusCode> {
    opcua_console_logging::init();
    let data_source = SimpleAddressSpace::new_arc_lock();
    let nodes: Vec<NodeId> = (0..8).map(|i| NodeId::new(0, i as u32)).collect();
    // Generating a PubSubApp
    let cb = OnReceiveValueFn::new_boxed(on_value);
    let pubsub = generate_pubsub(0, &data_source, cb)?;

    // Spawn a pubsub connection
    PubSubApp::run_async(Arc::new(RwLock::new(pubsub))).await;
    // Simulate a working loop where data is produced
    let mut rng = rand::thread_rng();
    let mut i = 0_usize;
    loop {
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
        tokio::time::sleep(time::Duration::from_millis(100)).await;
        i = i.wrapping_add(1);
    }
}
