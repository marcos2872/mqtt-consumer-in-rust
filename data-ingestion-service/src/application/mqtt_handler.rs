use crate::application::buffering::ReadingBuffer;
use crate::application::message_parser::MessageParser;
use rumqttc::{Event, EventLoop, Packet};
use std::time::Duration;
use tokio::time::sleep;

pub struct MqttHandler {
    buffer: ReadingBuffer,
}

impl MqttHandler {
    pub fn new(buffer: ReadingBuffer) -> Self {
        Self { buffer }
    }

    pub async fn run(&self, mut eventloop: EventLoop) {
        let mut i = 0;
        loop {
            match eventloop.poll().await {
                Ok(notification) => {
                    match notification {
                        Event::Incoming(Packet::Publish(publish)) => {
                            let topic = publish.topic;
                            let payload = publish.payload;

                            match MessageParser::parse(&topic, &payload) {
                                Ok(readings) => {
                                    for reading in &readings {
                                        self.buffer.push(reading.clone());
                                    }
                                    i += readings.len();
                                    println!("MqttHandler receive {} readings", i);
                                }
                                Err(e) => {
                                    eprintln!("Failed to parse message from {}: {}", topic, e);
                                }
                            }
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
