
use opcua_pubsub::prelude::*;
use opcua_pubsub::pubsubconnection::{PubSubConnection};
use opcua_pubsub::pubsubmessage::{UadpNetworkMessage, UadpDataSetMessage, UadpMessageType};
use std::{thread, time};

fn main() -> Result<(), StatusCode> {
    opcua_console_logging::init();
    let url = "239.0.0.1:4840"; // opc.udp://239.0.0.1:4840/
                                //let url = "224.0.0.22:4840";
    let pubsub = PubSubConnection::new(url.to_string(), Variant::UInt16(1002))?;
    let strs = vec!{
        "ALFA",
        "BRAVO",
        "CHARLIE",
        "DELTA",
        "ECHO",
        "FOXTROT",
        "GOLF",
        "HOTEL",
        "INDIA",
        "JULIETT",
        "KILO",
        "LIMA",
        "MIKE",
        "NOVEMBER",
        "OSCAR",
        "PAPA",
        "QUEBEC",
        "ROMEO",
        "SIERRA",
        "TANGO",
        "UNIFORM",
        "VICTOR",
        "WHISKEY",
        "X-RAY",
        "YANKEE",
        "ZULU"     
    };
    let mut p: usize = 0;
    loop{
        let mut msg = UadpNetworkMessage::new();
        msg.timestamp = Some(opcua_types::DateTime::now());
        let var = vec!{ Variant::from(strs[p % strs.len()]), Variant::from(p as u64)};
        msg.dataset.push(UadpDataSetMessage::new(UadpMessageType::KeyFrameVariant(var)));
        pubsub.send(&mut msg)?;
        p += 1;
        thread::sleep(time::Duration::from_millis(1000));
    }
}

