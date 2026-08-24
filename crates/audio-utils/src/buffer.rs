

use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;

pub struct RingBuffer {
    buffer: VecDeque<u8>,
    max_size: usize,
}

impl RingBuffer {
    pub fn new(max_size: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(max_size),
            max_size,
        }
    }

    pub fn push(&mut self, data: &[u8]) {
        for &byte in data {
            if self.buffer.len() >= self.max_size {
                self.buffer.pop_front();
            }
            self.buffer.push_back(byte);
        }
    }

    pub fn read(&mut self, size: usize) -> Option<Bytes> {
        if self.buffer.len() < size {
            return None;
        }

        let mut data = BytesMut::with_capacity(size);
        for _ in 0..size {
            if let Some(byte) = self.buffer.pop_front() {
                data.extend_from_slice(&[byte]);
            }
        }

        Some(data.freeze())
    }

    pub fn peek(&self, size: usize) -> Option<Bytes> {
        if self.buffer.len() < size {
            return None;
        }

        let data: Vec<u8> = self.buffer.iter().take(size).copied().collect();
        Some(Bytes::from(data))
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

pub struct FrameBuffer {
    frames: VecDeque<Bytes>,
    max_frames: usize,
    total_size: usize,
}

impl FrameBuffer {
    pub fn new(max_frames: usize) -> Self {
        Self {
            frames: VecDeque::with_capacity(max_frames),
            max_frames,
            total_size: 0,
        }
    }

    pub fn push_frame(&mut self, frame: Bytes) {
        if self.frames.len() >= self.max_frames {
            if let Some(old_frame) = self.frames.pop_front() {
                self.total_size -= old_frame.len();
            }
        }

        self.total_size += frame.len();
        self.frames.push_back(frame);
    }

    pub fn pop_frame(&mut self) -> Option<Bytes> {
        self.frames.pop_front().inspect(|frame| {
            self.total_size -= frame.len();
        })
    }

    pub fn get_concatenated(&self) -> Bytes {
        let mut result = BytesMut::with_capacity(self.total_size);
        for frame in &self.frames {
            result.extend_from_slice(frame);
        }
        result.freeze()
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn total_bytes(&self) -> usize {
        self.total_size
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub fn clear(&mut self) {
        self.frames.clear();
        self.total_size = 0;
    }
}