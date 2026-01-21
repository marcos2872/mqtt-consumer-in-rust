use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Anomaly {
    pub id: Uuid,
    pub machine_id: String,
    pub sensor_id: String,
    pub timestamp: DateTime<Utc>,
    pub detected_value: f64,
    pub expected_range_min: f64,
    pub expected_range_max: f64,
    pub z_score: f64,
    pub description: String,
}

impl Anomaly {
    pub fn new(
        machine_id: String,
        sensor_id: String,
        detected_value: f64,
        expected_range_min: f64,
        expected_range_max: f64,
        z_score: f64,
        description: String,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            machine_id,
            sensor_id,
            timestamp: Utc::now(),
            detected_value,
            expected_range_min,
            expected_range_max,
            z_score,
            description,
        }
    }
}
