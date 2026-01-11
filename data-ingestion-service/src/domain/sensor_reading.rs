use super::error::DomainError;
use super::validation_rules::Validator;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SensorReading {
    pub machine_id: String,
    pub sensor_id: String,
    pub value: f64,
    pub timestamp: DateTime<Utc>,
}

impl SensorReading {
    pub fn new(
        machine_id: String,
        sensor_id: String,
        value: f64,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            machine_id,
            sensor_id,
            value,
            timestamp,
        }
    }
}

impl Validator for SensorReading {
    fn validate(&self) -> Result<(), DomainError> {
        // match self.sensor_type.as_str() {
        //     "temperature" => ValidationRules::validate_temperature(self.value)?,
        //     "pressure" => ValidationRules::validate_pressure(self.value)?,
        //     _ => {} // Unknown types are accepted without specific range validation for now
        // }
        Ok(())
    }
}
