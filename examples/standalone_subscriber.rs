use opcua_pubsub::prelude::*;
use opcua_pubsub::connection::PubSubConnection;
use opcua_pubsub::message::{UadpNetworkMessage, UadpMessageType};

fn got_message(msg: UadpNetworkMessage){
    println!("Got new Message: ");
    if let Some(pid) = &msg.header.publisher_id{
        println!("PublisherId={}", pid);
    }
    if let Some(gp) = &msg.group_header {
        println!(
            "Groupe Header: WriterGroupe={}",
            gp.writer_group_id.unwrap_or(0)
        );
    }
    let iter = msg.dataset_payload.iter();
    for it in iter {
        println!("Dataset {}", it);
    }
    if let Some(timestamp) = &msg.timestamp{
        println!(
            "Timestamp: {}",
            timestamp
        );
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
    let url = "239.0.0.1:4840"; // opc.udp://239.0.0.1:4840/
                                //let url = "224.0.0.22:4840";
    let pubsub = PubSubConnection::new(url.to_string(), Variant::UInt16(1002))?;
    let reciver = pubsub.create_reciver()?;
    loop{
        match reciver.recive_msg() {
            Ok(msg) => got_message(msg),
            Err(e) => return Err(e)
        };
    }
}
