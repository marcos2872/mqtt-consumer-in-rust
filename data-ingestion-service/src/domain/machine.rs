use serde::{Deserialize, Serialize};
use super::sensor_reading::SensorReading;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Machine {
    pub id: String,
    pub name: String,
    pub location: Option<String>,
    pub sensors: Vec<SensorConfig>,
    pub last_readings: Vec<SensorReading>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorConfig {
    pub id: String,
    pub r#type: String,
}
