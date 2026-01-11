use crate::domain::error::DomainError;
use crate::domain::sensor_reading::SensorReading;
use crate::domain::validation_rules::Validator;
use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Deserialize)]
struct RawSensorReading {
    timestamp: DateTime<Utc>,
    machine_id: String,
    sensor_id: String,
    value: f64,
}

pub struct MessageParser;

impl MessageParser {
    pub fn parse(topic: &str, payload: &[u8]) -> Result<Vec<SensorReading>, DomainError> {
        // Expected topic: machines/{machine_id}/data
        let topic_parts: Vec<&str> = topic.split('/').collect();
        if topic_parts.len() != 3 || topic_parts[2] != "data" {
            return Err(DomainError::ParserError(format!(
                "Invalid topic format: {}",
                topic
            )));
        }

        let _topic_machine_id = topic_parts[1]; // We could validate this against payload if needed

        let raw_readings: Vec<RawSensorReading> =
            serde_json::from_slice(payload).map_err(|e| DomainError::ParserError(e.to_string()))?;

        let mut readings = Vec::with_capacity(raw_readings.len());

        for raw in raw_readings {
            let reading =
                SensorReading::new(raw.machine_id, raw.sensor_id, raw.value, raw.timestamp);

            reading.validate()?;
            readings.push(reading);
        }

        Ok(readings)
    }
}
