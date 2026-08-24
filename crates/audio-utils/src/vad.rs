

use std::collections::VecDeque;
use tracing::debug;

pub struct VoiceActivityDetector {
    threshold: f32,
    frame_size: usize,
    energy_history: VecDeque<f32>,
    noise_floor: f32,
    adaptation_rate: f32,
}

impl VoiceActivityDetector {
    pub fn new(threshold: f32) -> Self {
        Self {
            threshold,
            frame_size: 160,
            energy_history: VecDeque::with_capacity(50),
            noise_floor: 0.0,
            adaptation_rate: 0.01,
        }
    }

    pub fn detect_voice_activity(&mut self, pcm_data: &[u8]) -> bool {
        if pcm_data.len() < self.frame_size * 2 {
            return false;
        }

        let energy = self.calculate_energy(pcm_data);
        self.update_noise_floor(energy);

        let normalized_energy = if self.noise_floor > 0.0 {
            energy / self.noise_floor
        } else {
            energy
        };

        let is_voice = normalized_energy > self.threshold;

        self.energy_history.push_back(energy);
        if self.energy_history.len() > 50 {
            self.energy_history.pop_front();
        }

        debug!(
            "VAD: energy={:.2}, noise_floor={:.2}, normalized={:.2}, is_voice={}",
            energy, self.noise_floor, normalized_energy, is_voice
        );

        is_voice
    }

    fn calculate_energy(&self, pcm_data: &[u8]) -> f32 {
        let mut energy = 0.0;
        let mut sample_count = 0;

        for chunk in pcm_data.chunks_exact(2) {
            if chunk.len() == 2 {
                let sample = i16::from_le_bytes([chunk[0], chunk[1]]) as f32;
                energy += sample * sample;
                sample_count += 1;
            }
        }

        if sample_count > 0 {
            (energy / sample_count as f32).sqrt()
        } else {
            0.0
        }
    }

    fn update_noise_floor(&mut self, current_energy: f32) {
        if self.noise_floor == 0.0 {
            self.noise_floor = current_energy;
        } else {
            let adaptive_threshold = self.noise_floor * 2.0;
            if current_energy < adaptive_threshold {
                self.noise_floor = (1.0 - self.adaptation_rate) * self.noise_floor
                    + self.adaptation_rate * current_energy;
            }
        }
    }

    pub fn set_threshold(&mut self, threshold: f32) {
        self.threshold = threshold;
    }

    pub fn get_noise_floor(&self) -> f32 {
        self.noise_floor
    }
}