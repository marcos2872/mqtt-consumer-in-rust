use crate::application::aggregator::Aggregator;
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

pub struct Scheduler {
    aggregator: Arc<Aggregator>,
}

impl Scheduler {
    pub fn new(aggregator: Arc<Aggregator>) -> Self {
        Self { aggregator }
    }

    pub async fn start(&self) {
        let mut interval = time::interval(Duration::from_secs(60)); // Run every minute

        loop {
            interval.tick().await;
            println!("Starting scheduled aggregation...");
            
            let now = Utc::now();
            let window_start = now - chrono::Duration::minutes(1);
            let window_end = now;

            // Fetch active machine/sensor pairs from the last hour (to catch frequent updaters)
            let lookback = now - chrono::Duration::hours(1);
            
            match self.aggregator.get_active_sensors(lookback).await {
                Ok(active_pairs) => {
                    println!("Found {} active sensor streams", active_pairs.len());
                    for (machine_id, sensor_id) in active_pairs {
                         let aggregator = self.aggregator.clone();
                         let m_id = machine_id;
                         let s_id = sensor_id;
                         let w_start = window_start;
                         let w_end = window_end;
                         
                         tokio::spawn(async move {
                             if let Err(e) = aggregator.process_aggregation(m_id, s_id, w_start, w_end).await {
                                 eprintln!("Error processing aggregation: {}", e);
                             }
                         });
                    }
                }
                Err(e) => {
                    eprintln!("Failed to fetch active sensors: {}", e);
                }
            }
        }
    }
}
