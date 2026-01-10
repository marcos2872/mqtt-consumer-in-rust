use crate::domain::sensor_reading::SensorReading;
use crate::domain::error::DomainError;
use crate::domain::validation_rules::Validator;
use serde::Deserialize;

#[derive(Deserialize)]
struct RawSensorReading {
    value: f64,
    unit: Option<String>,
    timestamp: Option<i64>, // Unix timestamp
}

pub struct MessageParser;

impl MessageParser {
    pub fn parse(
        topic: &str,
        payload: &[u8],
    ) -> Result<SensorReading, DomainError> {
        let topic_parts: Vec<&str> = topic.split('/').collect();
        // Expected format: machines/{machine_id}/sensors/{sensor_id}/data
        if topic_parts.len() != 5 || topic_parts[4] != "data" {
             return Err(DomainError::ParserError(format!("Invalid topic format: {}", topic)));
        }

        let machine_id = topic_parts[1].to_string();
        let sensor_id = topic_parts[3].to_string();
        // Assuming sensor_type is inferred from sensor_id or passed in payload. 
        // For simplicity, let's assume sensor_id contains the type or valid type is required.
        // Let's infer type from sensor_id string for now (e.g. temp-01)
        let sensor_type = if sensor_id.contains("temp") {
            "temperature".to_string()
        } else if sensor_id.contains("pres") {
            "pressure".to_string()
        } else {
            "unknown".to_string()
        };

        let raw: RawSensorReading = serde_json::from_slice(payload)
            .map_err(|e| DomainError::ParserError(e.to_string()))?;

        let timestamp = raw.timestamp
            .map(|ts| chrono::DateTime::from_timestamp(ts, 0).unwrap_or(chrono::Utc::now()))
            .unwrap_or_else(chrono::Utc::now);

        let reading = SensorReading::new(
            machine_id,
            sensor_id,
            sensor_type,
            raw.value,
            raw.unit,
            timestamp,
        );

        reading.validate()?;

        Ok(reading)
    }
}
