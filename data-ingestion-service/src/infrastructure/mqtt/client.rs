use rumqttc::{AsyncClient, MqttOptions, QoS, EventLoop};
use std::time::Duration;
use std::error::Error;

pub struct MqttClient {
    client: AsyncClient,
}

pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub keep_alive: u64,
}

impl MqttClient {
    pub fn new(config: MqttConfig) -> (Self, EventLoop) {
        let mut mqtt_options = MqttOptions::new(config.client_id, config.host, config.port);
        mqtt_options.set_keep_alive(Duration::from_secs(config.keep_alive));

        let (client, eventloop) = AsyncClient::new(mqtt_options, 10);

        (Self { client }, eventloop)
    }

    pub async fn publish(&self, topic: &str, payload: Vec<u8>) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.client
            .publish(topic, QoS::AtLeastOnce, false, payload)
            .await?;
        Ok(())
    }

    pub async fn subscribe(&self, topic: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.client
            .subscribe(topic, QoS::AtLeastOnce)
            .await?;
        Ok(())
    }
}
