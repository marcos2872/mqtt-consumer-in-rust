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

            // In a real app, we would iterate over all active machines/sensors.
            // fetching them from DB or Redis.
            // For this demo, I'll hardcode a few test IDs or valid ones.
            let machines = vec!["machine_001"]; 
            let sensors = vec!["temperature", "pressure", "vibration"];

            for machine_id in &machines {
                for sensor_id in &sensors {
                     let aggregator = self.aggregator.clone();
                     let m_id = machine_id.to_string();
                     let s_id = sensor_id.to_string();
                     
                     tokio::spawn(async move {
                         if let Err(e) = aggregator.process_aggregation(m_id, s_id, window_start, window_end).await {
                             eprintln!("Error processing aggregation: {}", e);
                         }
                     });
                }
            }
        }
    }
}
