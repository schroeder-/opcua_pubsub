// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2020 Alexander Schrode
use crate::network::configuration::MqttConfig;
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
        pub fn new(_url: &MqttConfig) -> Result<MqttConnection, StatusCode> {
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
    use crate::network::configuration::MqttVersion;
    use futures::stream::StreamExt;
    use log::{error, info, warn};
    use mqtt::ConnectOptionsBuilder;
    use opcua_types::BrokerTransportQualityOfService;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;
    extern crate paho_mqtt as mqtt;
    pub struct MqttConnection {
        init_token: mqtt::Token,
        cli: Arc<RwLock<mqtt::AsyncClient>>,
        rx: Option<futures::channel::mpsc::Receiver<Option<mqtt::Message>>>,
    }

    pub struct MqttReceiver {
        rx: futures::channel::mpsc::Receiver<Option<mqtt::Message>>,
        cli: Arc<RwLock<mqtt::AsyncClient>>,
    }

    impl MqttReceiver {
        /// try to receive on message, return topic and data   
        pub async fn receive_msg(&mut self) -> Result<(String, Vec<u8>), StatusCode> {
            loop {
                match self.rx.next().await {
                    Some(msg) => {
                        if let Some(msg) = msg {
                            return Ok((msg.topic().to_string(), msg.payload().to_vec()));
                        } else {
                            let cli = self.cli.write().unwrap();
                            if cli.is_connected() || !try_reconnect(&cli).await {
                                return Err(StatusCode::BadCommunicationError);
                            }
                        }
                    }
                    None => {
                        panic!("await failed");
                    }
                }
            }
        }
    }

    impl MqttConnection {
        pub fn new(cfg: &MqttConfig) -> Result<Self, StatusCode> {
            let org_url = cfg.url().to_string();
            // pharo doesn't support mqtt scheme so change to tcp:// and mqtt://
            let url = org_url.replace("mqtt://", "tcp://");
            let url = url.replace("mqtts://", "ssl://");
            let mut opts = ConnectOptionsBuilder::new();
            if cfg.clean_session {
                opts.clean_session(true);
            }
            if !cfg.password.is_empty() {
                opts.password(cfg.password.to_string());
            }
            if !cfg.username.is_empty() {
                opts.user_name(cfg.username.to_string());
            }
            if cfg.version != MqttVersion::All {
                opts.mqtt_version(cfg.version as u32);
            }
            let o = opts.finalize();
            let mut cli = match mqtt::AsyncClient::new(url) {
                Ok(cli) => cli,
                Err(err) => {
                    error!("crating client from url: {} - {}", org_url, err);
                    return Err(StatusCode::BadCommunicationError);
                }
            };
            let init_token = cli.connect(o);

            let rx = cli.get_stream(2500);
            Ok(Self {
                init_token,
                rx: Some(rx),
                cli: Arc::new(RwLock::new(cli)),
            })
        }

        pub async fn start(&mut self) -> Result<(), StatusCode> {
            let t = &mut self.init_token;
            match t.await {
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
            Ok(())
        }

        pub async fn subscribe(&self, cfg: &ReaderTransportSettings) -> Result<(), StatusCode> {
            let (topic, meta, qos) = match cfg {
                ReaderTransportSettings::BrokerDataSetReader(b) => (
                    &b.queue_name,
                    &b.meta_data_queue_name,
                    qos_from(b.requested_delivery_guarantee),
                ),
                ReaderTransportSettings::None => return Err(StatusCode::BadInvalidArgument),
            };
            let cli = self.cli.write().unwrap();
            let res = match topic.value() {
                Some(t) => {
                    if let Err(err) = cli.subscribe(t, qos).await {
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
                    if let Err(err) = cli.subscribe(t, qos).await {
                        error!("subscribe: {} - {}", t, err);
                        return Err(StatusCode::BadCommunicationError);
                    }
                }
                None => {}
            }
            res
        }

        pub async fn unsubscribe(&self, cfg: &ReaderTransportSettings) -> Result<(), StatusCode> {
            let (topic, meta) = match cfg {
                ReaderTransportSettings::BrokerDataSetReader(b) => {
                    (&b.queue_name, &b.meta_data_queue_name)
                }
                &ReaderTransportSettings::None => return Err(StatusCode::BadInvalidArgument),
            };
            let cli = self.cli.write().unwrap();
            let res_topic = match topic.value() {
                Some(t) => {
                    if let Err(err) = cli.unsubscribe(t).await {
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
                    if let Err(err) = cli.unsubscribe(t).await {
                        error!("subscribe: {} - {}", t, err);
                        return Err(StatusCode::BadInvalidArgument);
                    }
                }
                None => {}
            };
            //@TODO error handling
            res_topic
        }

        pub async fn publish(&self, b: &[u8], cfg: &TransportSettings) -> io::Result<usize> {
            let def = opcua_types::UAString::from("OpcUADefault");
            let (topic, qos) = match cfg {
                TransportSettings::BrokerDataSetWrite(b) => (
                    &b.meta_data_queue_name,
                    qos_from(b.requested_delivery_guarantee),
                ),
                TransportSettings::BrokerWrite(b) => {
                    (&b.queue_name, qos_from(b.requested_delivery_guarantee))
                }
                TransportSettings::None => (&def, 1),
            };
            let topic: &str = match topic.value() {
                Some(t) => &*t,
                None => "OpcUADefault",
            };
            let msg = mqtt::Message::from((topic, b, qos, false));
            let cli = self.cli.write().unwrap();
            if let Err(err) = cli.publish(msg).await {
                warn!("error publishing {} - {}", topic, err);
            }
            Ok(0)
        }
        /// Creates Reciver only one is supported
        pub fn create_receiver(&mut self) -> MqttReceiver {
            let mut rx = None;
            std::mem::swap(&mut self.rx, &mut rx);
            MqttReceiver {
                rx: rx.unwrap(),
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
                .wait_for(std::time::Duration::from_millis(100))
            {
                warn!("disconnect failed {}", err)
            }
        }
    }
    const fn qos_from(requested_delivery_guarantee: BrokerTransportQualityOfService) -> i32 {
        match requested_delivery_guarantee {
            BrokerTransportQualityOfService::NotSpecified
            | BrokerTransportQualityOfService::AtLeastOnce => 1,
            BrokerTransportQualityOfService::AtMostOnce => 0,
            BrokerTransportQualityOfService::ExactlyOnce
            | BrokerTransportQualityOfService::BestEffort => 2,
        }
    }

    async fn try_reconnect(_cli: &mqtt::AsyncClient) -> bool {
        warn!("Lost Connection");
        loop {
            tokio::time::sleep(Duration::from_millis(5000)).await;
            panic!("Reconnect not implemented")
        }
    }
}

#[cfg(not(feature = "mqtt"))]
pub(crate) use dummy::*;
#[cfg(feature = "mqtt")]
pub use paho::*;
