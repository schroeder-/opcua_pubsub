use opcua_pubsub::prelude::*;
use opcua_pubsub::pubsubconnection::PubSubConnection;
use opcua_pubsub::pubsubmessage::{UadpNetworkMessage, UadpDataSetMessage, UadpMessageType};
use std::{thread};

#[test]
fn uadp_message_test() -> Result<(), StatusCode>{
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
    let mut sended = Vec::new();
    let url = "239.0.0.1:4840";
    let pubsub = PubSubConnection::new(url.to_string(),  Variant::UInt16(1002))?;
    const CNT: usize = 100;
    let recv = pubsub.create_reciver()?;
    let handler = thread::spawn(move || -> Result<Vec::<UadpNetworkMessage>, StatusCode>{
        let mut recived = Vec::new();
        for _ in 0 .. CNT{
            recived.push(recv.recive_msg()?);
        }
        Ok(recived)
    });
    
    for p in 0 .. CNT{
        let mut msg = UadpNetworkMessage::new();
        msg.timestamp = Some(opcua_types::DateTime::now());
        let var = vec!{ Variant::from(strs[p % strs.len()]), Variant::from(p as u64)};
        msg.dataset.push(UadpDataSetMessage::new(UadpMessageType::KeyFrameVariant(var)));
        pubsub.send(&mut msg)?;
        sended.push(msg);
    }
    let recived = handler.join().expect("Thread got error")?;
    assert_eq!(recived.len(), sended.len());
    for x in 0 .. CNT{
        assert_eq!(recived[x], sended[x]);
    }
    Ok(())
}