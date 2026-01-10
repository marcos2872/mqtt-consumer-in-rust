use thiserror::Error;

#[derive(Error, Debug)]
pub enum DomainError {
    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Parser error: {0}")]
    ParserError(String),

    #[error("Infrastructure error: {0}")]
    InfrastructureError(String),
}
