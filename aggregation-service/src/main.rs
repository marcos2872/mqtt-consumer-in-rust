mod domain;
mod application;
mod infrastructure;

use application::aggregator::Aggregator;
use application::scheduler::Scheduler;
use infrastructure::database::postgres;
use infrastructure::database::repository::AggregationRepository;
use infrastructure::http;
use infrastructure::redis::{client as redis_client, publisher::RedisPublisher};
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    
    // Initialize tracing (logs)
    tracing_subscriber::fmt::init();
    
    println!("Starting Aggregation Service...");

    // 1. Infrastructure Setup
    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    let redis_url = env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://localhost:6379".to_string());

    println!("Connecting to Database...");
    let db_pool = postgres::create_pool(&database_url).await?;
    let repository = AggregationRepository::new(db_pool);

    println!("Connecting to Redis...");
    let redis_client = redis_client::create_client(&redis_url).await?;
    let publisher = RedisPublisher::new(redis_client);

    // 2. Application Setup
    let aggregator = Arc::new(Aggregator::new(repository, publisher));
    let scheduler = Scheduler::new(aggregator.clone());

    // 3. Start Scheduler
    println!("Starting Scheduler...");
    tokio::spawn(async move {
        scheduler.start().await;
    });

    // 4. Start HTTP Server
    let app = http::create_router();
    let addr = SocketAddr::from(([0, 0, 0, 0], 3002));
    println!("Listening on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
