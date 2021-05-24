// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2020 Alexander Schrode
use crate::network::{ReaderTransportSettings, TransportSettings};
use opcua_types::status_code::StatusCode;
use std::io;

#[cfg(not(feature = "mqtt"))]
mod dummy {
    use super::*;
    pub struct MqttConnection {}

    pub struct MqttReceiver {}

    impl MqttReceiver {
        pub fn new() -> Self {
            MqttReceiver {}
        }

        pub fn receive_msg(&self) -> Result<(String, Vec<u8>), StatusCode> {
            Err(StatusCode::BadNotImplemented)
        }
    }

    impl MqttConnection {
        pub fn new(_url: &str) -> Result<MqttConnection, StatusCode> {
            Err(StatusCode::BadNotImplemented)
        }
        pub fn publish(&self, _b: &[u8], _cfg: &TransportSettings) -> io::Result<usize> {
            Ok(0)
        }

        pub fn subscribe(&self, _cfg: &ReaderTransportSettings) -> Result<(), StatusCode> {
            Ok(())
        }

        pub fn unsubscribe(&self, _cfg: &ReaderTransportSettings) -> Result<(), StatusCode> {
            Ok(())
        }

        pub fn create_receiver(&self) -> MqttReceiver {
            MqttReceiver::new()
        }
    }
}
#[cfg(feature = "mqtt")]
mod paho {
    use super::*;
    use log::{error, info, warn};
    use opcua_types::BrokerTransportQualityOfService;
    use std::sync::mpsc;
    use std::sync::{Arc, RwLock};
    use std::thread;
    use std::time::Duration;
    extern crate paho_mqtt as mqtt;
    pub struct MqttConnection {
        cli: Arc<RwLock<mqtt::Client>>,
    }

    pub struct MqttReceiver {
        rx: mpsc::Receiver<Option<mqtt::Message>>,
        cli: Arc<RwLock<mqtt::Client>>,
    }

    impl MqttReceiver {
        /// try to receive on message, return topic and data   
        pub fn receive_msg(&self) -> Result<(String, Vec<u8>), StatusCode> {
            loop {
                match self.rx.recv() {
                    Ok(msg) => {
                        if let Some(msg) = msg {
                            return Ok((msg.topic().to_string(), msg.payload().to_vec()));
                        }
                    }
                    Err(err) => {
                        warn!("recv err: {}", err);
                        let cli = self.cli.write().unwrap();
                        if cli.is_connected() || !try_reconnect(&cli) {
                            return Err(StatusCode::BadCommunicationError);
                        }
                    }
                }
            }
        }
    }

    impl MqttConnection {
        pub fn new(url: &str) -> Result<MqttConnection, StatusCode> {
            // pharo doesn't support mqtt scheme so change to tcp:// and mqtt://
            let url = url.replace("mqtt://", "tcp://");
            let url = url.replace("mqtts://", "ssl://");
            let mut cli = match mqtt::Client::new(url.clone()) {
                Ok(cli) => cli,
                Err(err) => {
                    error!("crating client from url: {} - {}", url, err);
                    return Err(StatusCode::BadCommunicationError);
                }
            };
            cli.set_timeout(Duration::from_secs(5));
            // Connect and wait for it to complete or fail
            match cli.connect(None) {
                Err(e) => {
                    error!("Unable to connect: {:?}", e);
                    return Err(StatusCode::BadCommunicationError);
                }
                Ok(rsp) => {
                    if let Some(conn_rsp) = rsp.connect_response() {
                        info!(
                            "Connected to: '{}' with MQTT version {}",
                            conn_rsp.server_uri, conn_rsp.mqtt_version
                        );
                    }
                }
            }
            Ok(MqttConnection {
                cli: Arc::new(RwLock::new(cli)),
            })
        }

        pub fn subscribe(&self, cfg: &ReaderTransportSettings) -> Result<(), StatusCode> {
            let (topic, meta, qos) = match cfg {
                ReaderTransportSettings::BrokerDataSetReader(b) => (
                    &b.queue_name,
                    &b.meta_data_queue_name,
                    qos_from(b.requested_delivery_guarantee),
                ),
                _ => return Err(StatusCode::BadInvalidArgument),
            };
            let cli = self.cli.write().unwrap();
            let res = match topic.value() {
                Some(t) => {
                    if let Err(err) = cli.subscribe(t, qos) {
                        error!("subscribe: {} - {}", t, err);
                        Err(StatusCode::BadCommunicationError)
                    } else {
                        Ok(())
                    }
                }
                None => return Err(StatusCode::BadInvalidArgument),
            };
            match meta.value() {
                Some(t) => {
                    if let Err(err) = cli.subscribe(t, qos) {
                        error!("subscribe: {} - {}", t, err);
                        return Err(StatusCode::BadCommunicationError);
                    }
                }
                None => {}
            }
            res
        }

        pub fn unsubscribe(&self, cfg: &ReaderTransportSettings) -> Result<(), StatusCode> {
            let (topic, meta) = match cfg {
                ReaderTransportSettings::BrokerDataSetReader(b) => {
                    (&b.queue_name, &b.meta_data_queue_name)
                }
                _ => return Err(StatusCode::BadInvalidArgument),
            };
            let cli = self.cli.write().unwrap();
            let res_topic = match topic.value() {
                Some(t) => {
                    if let Err(err) = cli.unsubscribe(t) {
                        error!("subscribe: {} - {}", t, err);
                        Err(StatusCode::BadCommunicationError)
                    } else {
                        Ok(())
                    }
                }
                None => Err(StatusCode::BadInvalidArgument),
            };
            match meta.value() {
                Some(t) => {
                    if let Err(err) = cli.unsubscribe(t) {
                        error!("subscribe: {} - {}", t, err);
                        return Err(StatusCode::BadInvalidArgument);
                    }
                }
                None => {}
            };
            //@TODO error handling
            res_topic
        }

        pub fn publish(&self, b: &[u8], cfg: &TransportSettings) -> io::Result<usize> {
            let def = opcua_types::UAString::from("OpcUADefault");
            let (topic, qos) = match cfg {
                TransportSettings::BrokerDataSetWrite(b) => (
                    &b.meta_data_queue_name,
                    qos_from(b.requested_delivery_guarantee),
                ),
                TransportSettings::BrokerWrite(b) => {
                    (&b.queue_name, qos_from(b.requested_delivery_guarantee))
                }
                _ => (&def, 1),
            };
            let topic: &str = match topic.value() {
                Some(t) => &*t,
                None => "OpcUADefault",
            };
            let msg = mqtt::Message::from((topic, b, qos, false));
            let cli = self.cli.write().unwrap();
            if let Err(err) = cli.publish(msg) {
                warn!("error publishing {} - {}", topic, err);
            }
            Ok(0)
        }

        pub fn create_receiver(&self) -> MqttReceiver {
            let rx = {
                let mut cli = self.cli.write().unwrap();
                cli.start_consuming()
            };
            MqttReceiver {
                rx,
                cli: self.cli.clone(),
            }
        }
    }

    impl Drop for MqttConnection {
        fn drop(&mut self) {
            // Disconnect from the broker
            if let Err(err) = self
                .cli
                .write()
                .unwrap()
                .disconnect(mqtt::DisconnectOptions::new())
            {
                warn!("disconnect failed {}", err)
            }
        }
    }
    fn qos_from(requested_delivery_guarantee: BrokerTransportQualityOfService) -> i32 {
        match requested_delivery_guarantee {
            BrokerTransportQualityOfService::AtLeastOnce => 1,
            BrokerTransportQualityOfService::AtMostOnce => 0,
            BrokerTransportQualityOfService::BestEffort => 2,
            BrokerTransportQualityOfService::ExactlyOnce => 2,
            BrokerTransportQualityOfService::NotSpecified => 1,
        }
    }

    fn try_reconnect(cli: &mqtt::Client) -> bool {
        warn!("Lost Connection");
        loop {
            thread::sleep(Duration::from_millis(5000));
            if cli.reconnect().is_ok() {
                info!("Successfully reconnected");
                return true;
            }
        }
    }
}

#[cfg(not(feature = "mqtt"))]
pub(crate) use dummy::*;
#[cfg(feature = "mqtt")]
pub(crate) use paho::*;
