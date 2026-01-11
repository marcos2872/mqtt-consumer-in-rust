use crate::domain::sensor_reading::SensorReading;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
pub struct ReadingBuffer {
    buffer: Arc<Mutex<VecDeque<SensorReading>>>,
    dropped_count: Arc<AtomicUsize>,
    capacity: usize,
}

impl ReadingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            dropped_count: Arc::new(AtomicUsize::new(0)),
            capacity,
        }
    }

    pub fn push(&self, reading: SensorReading) {
        let mut buffer = self.buffer.lock().unwrap();
        if buffer.len() >= self.capacity {
            buffer.pop_front();
            let drops = self.dropped_count.fetch_add(1, Ordering::Relaxed) + 1;
            if drops % 1000 == 0 {
                eprintln!("WARN: Buffer full. Total items dropped: {}", drops);
            }
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
