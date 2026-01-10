use rumqttc::{Event, EventLoop, Packet};
use std::time::Duration;
use tokio::time::sleep;
use crate::application::message_parser::MessageParser;
use crate::application::buffering::ReadingBuffer;

pub struct MqttHandler {
    buffer: ReadingBuffer,
}

impl MqttHandler {
    pub fn new(buffer: ReadingBuffer) -> Self {
        Self { buffer }
    }

    pub async fn run(&self, mut eventloop: EventLoop) {
        loop {
            match eventloop.poll().await {
                Ok(notification) => {
                    match notification {
                        Event::Incoming(Packet::Publish(publish)) => {
                            let topic = publish.topic;
                            let payload = publish.payload;
                            
                            match MessageParser::parse(&topic, &payload) {
                                Ok(reading) => {
                                    self.buffer.push(reading);
                                    // println!("Pushed reading to buffer. Size: {}", self.buffer.len());
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
