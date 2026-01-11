mod infrastructure;
mod domain;
mod application;

use infrastructure::mqtt::client::{MqttClient, MqttConfig};
use infrastructure::database::postgres;
use infrastructure::database::migration;
use infrastructure::database::repository::SensorRepository;
use application::buffering::ReadingBuffer;
use application::batch_processor::BatchProcessor;
use application::mqtt_handler::MqttHandler;
use tokio::task;
use std::env;
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("Starting Data Ingestion Service...");

    // 1. Setup Database
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/db".to_string());
    
    let pool = postgres::create_pool(&database_url).await.expect("Failed to create database pool");
    let repository = SensorRepository::new(pool.clone());
    
    // 2. Run Migrations
    println!("Running migrations...");
    migration::run_migrations(&pool).await.expect("Failed to run migrations");
    println!("Migrations applied successfully.");

    // 3. Setup Application State
    // Buffer capacity 10000, Batch size 1000, Interval 1s
    let buffer = ReadingBuffer::new(5_000_000); 
    let batch_processor = BatchProcessor::new(
        buffer.clone(), 
        repository, 
        15000, 
        Duration::from_millis(1)
    );

    // Spawn Batch Processor
    task::spawn(async move {
        batch_processor.run().await;
    });

    // 4. Setup MQTT
    let config = MqttConfig {
        host: "localhost".to_string(),
        port: 1883,
        client_id: "data-ingestion-service".to_string(),
        keep_alive: 30,
    };

    let (client, eventloop) = MqttClient::new(config);
    let mqtt_handler = MqttHandler::new(buffer);

    task::spawn(async move {
        mqtt_handler.run(eventloop).await;
    });

    client.subscribe("machines/+/data").await.unwrap();

    // Keep main thread alive
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
