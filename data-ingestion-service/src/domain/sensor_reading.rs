use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use super::error::DomainError;
use super::validation_rules::{Validator, ValidationRules};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorReading {
    pub machine_id: String,
    pub sensor_id: String,
    pub sensor_type: String,
    pub value: f64,
    pub unit: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub status: Option<String>,
}

impl SensorReading {
    pub fn new(
        machine_id: String,
        sensor_id: String,
        sensor_type: String,
        value: f64,
        unit: Option<String>,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            machine_id,
            sensor_id,
            sensor_type,
            value,
            unit,
            timestamp,
            status: None,
        }
    }
}

impl Validator for SensorReading {
    fn validate(&self) -> Result<(), DomainError> {
        match self.sensor_type.as_str() {
            "temperature" => ValidationRules::validate_temperature(self.value)?,
            "pressure" => ValidationRules::validate_pressure(self.value)?,
            _ => {} // Unknown types are accepted without specific range validation for now
        }
        Ok(())
    }
}
