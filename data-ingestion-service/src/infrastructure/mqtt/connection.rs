use rumqttc::{Event, EventLoop, Packet};
use std::time::Duration;
use tokio::time::sleep;

pub struct ConnectionManager;

impl ConnectionManager {
    pub async fn run(mut eventloop: EventLoop) {
        loop {
            match eventloop.poll().await {
                Ok(notification) => {
                    match notification {
                        Event::Incoming(Packet::Publish(publish)) => {
                            println!("Received = {:?}, Payload = {:?}", publish.topic, publish.payload);
                            // TODO: Dispatch to handler
                        }
                        Event::Incoming(Packet::ConnAck(_)) => {
                            println!("Connected to broker");
                        }
                        _ => {
                            // Handle other events or ignore
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Connection error: {:?}. Retrying...", e);
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
