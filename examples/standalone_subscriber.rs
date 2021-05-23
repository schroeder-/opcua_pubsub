// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode

use opcua_pubsub::prelude::*;
/// In this example the subscriber gets a notify can when a Subscribed Dataset changed
/// and can use this information


// Generates the subscriber
fn generate_pubsub(ns: u16) -> Result<PubSubConnection, StatusCode> {
    let url = "opc.udp://224.0.0.22:4840"; 
    // Create a pubsub connection
    let mut pubsub = PubSubConnectionBuilder::new()
            .set_url(url.into())
            .set_publisher_id(Variant::UInt16(2234))
            .build(SimpleAddressSpace::new_arc_lock())?;
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
    pubsub.add_reader_group(rg);
    Ok(pubsub)
}

fn on_value(reader: &DataSetReader, dataset: Vec<UpdateTarget>){
    println!("#### Got Dataset from reader: {}", reader.name());
    for UpdateTarget(_, dv, meta) in dataset{
        println!("#### Variable: {} Value: {:?}", meta.name(), dv);
    }
    
}

fn main() -> Result<(), StatusCode> {
    opcua_console_logging::init();
    // Generating a pubsubconnection
    let mut pubsub = generate_pubsub(0)?;
    let cb = OnReceiveValueFn::new_boxed(on_value);
    pubsub.set_datavalue_recv(Some(cb));
    // Spawn a pubsub connection
    pubsub.run();
    Ok(())
}
