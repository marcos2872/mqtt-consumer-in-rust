mod infrastructure;

use infrastructure::mqtt::client::{MqttClient, MqttConfig};
use infrastructure::mqtt::connection::ConnectionManager;
use infrastructure::database::postgres;
use infrastructure::database::migration;
use tokio::task;
use std::env;

#[tokio::main]
async fn main() {
    // 1. Setup Database
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/db".to_string());
    
    let pool = postgres::create_pool(&database_url).await.expect("Failed to create database pool");
    
    // 2. Run Migrations
    println!("Running migrations...");
    migration::run_migrations(&pool).await.expect("Failed to run migrations");
    println!("Migrations applied successfully.");

    // 3. Setup MQTT
    let config = MqttConfig {
        host: "localhost".to_string(),
        port: 1883,
        client_id: "data-ingestion-service".to_string(),
        keep_alive: 5,
    };

    let (client, eventloop) = MqttClient::new(config);

    task::spawn(async move {
        ConnectionManager::run(eventloop).await;
    });

    client.subscribe("machines/+/sensors/+/data").await.unwrap();

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
