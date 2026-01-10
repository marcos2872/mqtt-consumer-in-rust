use crate::domain::sensor_reading::SensorReading;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct ReadingBuffer {
    buffer: Arc<Mutex<VecDeque<SensorReading>>>,
    capacity: usize,
}

impl ReadingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
        }
    }

    pub fn push(&self, reading: SensorReading) {
        let mut buffer = self.buffer.lock().unwrap();
        if buffer.len() >= self.capacity {
            // Buffer full, drop oldest or log warning. 
            // For IoT it's often better to drop oldest to keep fresh data.
            buffer.pop_front(); 
        }
        buffer.push_back(reading);
    }

    pub fn pop_batch(&self, batch_size: usize) -> Vec<SensorReading> {
        let mut buffer = self.buffer.lock().unwrap();
        let count = std::cmp::min(buffer.len(), batch_size);
        buffer.drain(0..count).collect()
    }

    pub fn is_empty(&self) -> bool {
        let buffer = self.buffer.lock().unwrap();
        buffer.is_empty()
    }
    
    pub fn len(&self) -> usize {
        let buffer = self.buffer.lock().unwrap();
        buffer.len()
    }
}
