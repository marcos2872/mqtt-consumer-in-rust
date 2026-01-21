use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Aggregation {
    pub machine_id: String,
    pub sensor_id: String,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub avg_value: f64,
    pub min_value: f64,
    pub max_value: f64,
    pub count: i64,
    pub p99_value: Option<f64>,
    pub timestamp: DateTime<Utc>,
}

impl Aggregation {
    pub fn new(
        machine_id: String,
        sensor_id: String,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        avg_value: f64,
        min_value: f64,
        max_value: f64,
        count: i64,
        p99_value: Option<f64>,
    ) -> Self {
        Self {
            machine_id,
            sensor_id,
            window_start,
            window_end,
            avg_value,
            min_value,
            max_value,
            count,
            p99_value,
            timestamp: Utc::now(),
        }
    }
}
