use super::error::DomainError;

pub trait Validator {
    fn validate(&self) -> Result<(), DomainError>;
}

pub struct ValidationRules;

impl ValidationRules {
    pub const TEMPERATURE_MIN: f64 = -50.0;
    pub const TEMPERATURE_MAX: f64 = 150.0;
    pub const PRESSURE_MIN: f64 = 0.0;
    pub const PRESSURE_MAX: f64 = 200.0; // bar
    
    pub fn validate_temperature(value: f64) -> Result<(), DomainError> {
        if !(Self::TEMPERATURE_MIN..=Self::TEMPERATURE_MAX).contains(&value) {
            return Err(DomainError::ValidationError(format!(
                "Temperature {} out of range [{}, {}]",
                value, Self::TEMPERATURE_MIN, Self::TEMPERATURE_MAX
            )));
        }
        Ok(())
    }

    pub fn validate_pressure(value: f64) -> Result<(), DomainError> {
         if !(Self::PRESSURE_MIN..=Self::PRESSURE_MAX).contains(&value) {
            return Err(DomainError::ValidationError(format!(
                "Pressure {} out of range [{}, {}]",
                value, Self::PRESSURE_MIN, Self::PRESSURE_MAX
            )));
        }
        Ok(())
    }
}
