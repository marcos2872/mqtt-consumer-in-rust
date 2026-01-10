use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;
// use tokio::{task, time};

pub async fn client() {
    let mut mqttoptions = MqttOptions::new("data-ingestion-client", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    client
        .subscribe("iot/sensors", QoS::AtMostOnce)
        .await
        .unwrap();

    loop {
        match eventloop.poll().await {
            Ok(notification) => match notification {
                Event::Incoming(Packet::Publish(publish)) => {
                    println!("Payload: {:?}", std::str::from_utf8(&publish.payload));
                }
                _ => println!("Received = {:?}", notification),
            },
            Err(e) => {
                eprintln!("Connection error: {:?}", e);
                break;
            }
        }
    }
}
