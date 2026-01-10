pub const SENSOR_DATA_TOPIC_PATTERN: &str = "machines/+/sensors/+/data";

pub enum MqttTopic {
    SensorData { machine_id: String, sensor_id: String },
}

impl MqttTopic {
    pub fn to_string(&self) -> String {
        match self {
            MqttTopic::SensorData { machine_id, sensor_id } => {
                format!("machines/{}/sensors/{}/data", machine_id, sensor_id)
            }
        }
    }
}

pub fn parse_topic(topic: &str) -> Option<MqttTopic> {
    let parts: Vec<&str> = topic.split('/').collect();
    if parts.len() == 5 && parts[0] == "machines" && parts[2] == "sensors" && parts[4] == "data" {
        return Some(MqttTopic::SensorData {
            machine_id: parts[1].to_string(),
            sensor_id: parts[3].to_string(),
        });
    }
    None
}
