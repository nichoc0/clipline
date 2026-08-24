

use audio_codec_algorithms::encode_ulaw;
use rubato::{
    SincFixedIn, Resampler, SincInterpolationType, SincInterpolationParameters, WindowFunction,
};
use std::sync::Mutex;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CodecError {
    #[error("Invalid audio data length: expected multiple of {expected}, got {actual}")]
    InvalidLength { expected: usize, actual: usize },
    #[error("Sample rate conversion failed: {message}")]
    ResampleError { message: String },
    #[error("Unsupported format conversion")]
    UnsupportedFormat,
}

pub struct CodecConverter {
    resampler_8k_to_16k: Mutex<Option<SincFixedIn<f32>>>,
    sample_buffer: Mutex<Vec<f32>>,

    resampler_16k_to_8k: Mutex<Option<SincFixedIn<f32>>>,
    sample_buffer_16k_to_8k: Mutex<Vec<f32>>,
}

impl Default for CodecConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl CodecConverter {
    pub fn new() -> Self {
        Self {
            resampler_8k_to_16k: Mutex::new(None),
            sample_buffer: Mutex::new(Vec::new()),
            resampler_16k_to_8k: Mutex::new(None),
            sample_buffer_16k_to_8k: Mutex::new(Vec::new()),
        }
    }

    pub fn mulaw_to_pcm16(&self, mulaw_data: &[u8]) -> Result<Vec<u8>, CodecError> {
        let mut pcm_data = Vec::with_capacity(mulaw_data.len() * 2);

        for &mulaw_byte in mulaw_data {
            let pcm_sample = self.mulaw_to_linear(mulaw_byte);
            pcm_data.extend_from_slice(&pcm_sample.to_le_bytes());
        }

        Ok(pcm_data)
    }

    pub fn pcm16_to_mulaw(&self, pcm_data: &[u8]) -> Result<Vec<u8>, CodecError> {
        if pcm_data.len() % 2 != 0 {
            return Err(CodecError::InvalidLength {
                expected: 2,
                actual: pcm_data.len(),
            });
        }

        let mut mulaw_data = Vec::with_capacity(pcm_data.len() / 2);

        for chunk in pcm_data.chunks_exact(2) {
            let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
            let mulaw_byte = encode_ulaw(sample);
            mulaw_data.push(mulaw_byte);
        }

        Ok(mulaw_data)
    }

    pub fn pcm16_to_alaw(&self, pcm_data: &[u8]) -> Result<Vec<u8>, CodecError> {
        if pcm_data.len() % 2 != 0 {
            return Err(CodecError::InvalidLength {
                expected: 2,
                actual: pcm_data.len(),
            });
        }

        let mut alaw_data = Vec::with_capacity(pcm_data.len() / 2);

        for chunk in pcm_data.chunks_exact(2) {
            let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
            let alaw_byte = self.linear_to_alaw(sample);
            alaw_data.push(alaw_byte);
        }

        Ok(alaw_data)
    }

    fn mulaw_to_linear(&self, mulaw: u8) -> i16 {
        let mulaw = !mulaw;

        let sign = if (mulaw & 0x80) != 0 { -1i16 } else { 1i16 };

        let exponent = ((mulaw >> 4) & 0x07) as i16;
        let mantissa = (mulaw & 0x0F) as i16;

        let mut linear = ((33 + (mantissa << 1)) << exponent) - 33;

        linear = sign.saturating_mul(linear);

        linear << 2
    }


    fn linear_to_alaw(&self, sample: i16) -> u8 {
        const SEG_SHIFT: u8 = 4;
        const QUANT_MASK: i16 = 0x0F;

        let mut pcm_val = sample;
        let mask: u8;

        if pcm_val >= 0 {
            mask = 0xD5;
        } else {
            mask = 0x55;
            pcm_val = pcm_val.saturating_neg().saturating_sub(1);
        }

        pcm_val = pcm_val >> 3;

        if pcm_val > 4095 {
            pcm_val = 4095;
        }

        let seg = if pcm_val < 256 {
            pcm_val as u8 >> 4
        } else {
            let mut s = 1u8;
            let mut val = pcm_val;
            while val > 0x1FF {
                val >>= 1;
                s += 1;
            }
            s
        };

        let aval = if seg < 2 {
            (pcm_val >> 1) & QUANT_MASK
        } else {
            (pcm_val >> seg) & QUANT_MASK
        };

        ((seg << SEG_SHIFT) | aval as u8) ^ mask
    }

    pub fn resample_8k_to_16k(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        if data.len() % 2 != 0 {
            return Err(CodecError::InvalidLength {
                expected: 2,
                actual: data.len(),
            });
        }

        const CHUNK_SIZE: usize = 160;

        let num_samples = data.len() / 2;
        let mut incoming_samples = Vec::with_capacity(num_samples);
        for chunk in data.chunks_exact(2) {
            let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
            incoming_samples.push(sample as f32 / 32768.0);
        }

        let mut buffer = self.sample_buffer.lock().unwrap();
        buffer.extend_from_slice(&incoming_samples);

        let mut resampler_guard = self.resampler_8k_to_16k.lock().unwrap();
        if resampler_guard.is_none() {
            let params = SincInterpolationParameters {
                sinc_len: 256,
                f_cutoff: 0.95,
                interpolation: SincInterpolationType::Linear,
                oversampling_factor: 256,
                window: WindowFunction::BlackmanHarris2,
            };

            *resampler_guard = Some(SincFixedIn::<f32>::new(
                2.0,
                2.0,
                params,
                CHUNK_SIZE,
                1,
            ).map_err(|e| CodecError::ResampleError {
                message: format!("Failed to create resampler: {:?}", e),
            })?);
        }

        let resampler = resampler_guard.as_mut().unwrap();

        let mut output = Vec::new();

        while buffer.len() >= CHUNK_SIZE {
            let chunk: Vec<f32> = buffer.drain(..CHUNK_SIZE).collect();

            let waves_in = vec![chunk];
            let waves_out = resampler
                .process(&waves_in, None)
                .map_err(|e| CodecError::ResampleError {
                    message: format!("Resampling failed: {:?}", e),
                })?;

            for &sample in &waves_out[0] {
                let sample_i16 = (sample * 32768.0).clamp(-32768.0, 32767.0) as i16;
                output.extend_from_slice(&sample_i16.to_le_bytes());
            }
        }

        Ok(output)
    }

    pub fn resample_16k_to_8k(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        if data.len() % 2 != 0 {
            return Err(CodecError::InvalidLength {
                expected: 2,
                actual: data.len(),
            });
        }

        const CHUNK_SIZE_16K: usize = 320;

        let num_samples = data.len() / 2;
        let mut incoming_samples = Vec::with_capacity(num_samples);
        for chunk in data.chunks_exact(2) {
            let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
            incoming_samples.push(sample as f32 / 32768.0);
        }

        let mut buffer = self.sample_buffer_16k_to_8k.lock().unwrap();
        buffer.extend_from_slice(&incoming_samples);

        let mut resampler_guard = self.resampler_16k_to_8k.lock().unwrap();
        if resampler_guard.is_none() {
            let params = SincInterpolationParameters {
                sinc_len: 64,
                f_cutoff: 0.42,
                interpolation: SincInterpolationType::Linear,
                oversampling_factor: 256,
                window: WindowFunction::Blackman,
            };

            *resampler_guard = Some(SincFixedIn::<f32>::new(
                0.5,
                2.0,
                params,
                CHUNK_SIZE_16K,
                1,
            ).map_err(|e| CodecError::ResampleError {
                message: format!("Failed to create 16k→8k resampler: {:?}", e),
            })?);
        }

        let resampler = resampler_guard.as_mut().unwrap();

        let mut output = Vec::new();

        while buffer.len() >= CHUNK_SIZE_16K {
            let chunk: Vec<f32> = buffer.drain(..CHUNK_SIZE_16K).collect();

            let waves_in = vec![chunk];
            let waves_out = resampler
                .process(&waves_in, None)
                .map_err(|e| CodecError::ResampleError {
                    message: format!("16k→8k resampling failed: {:?}", e),
                })?;

            for &sample in &waves_out[0] {
                let sample_i16 = (sample * 32768.0).clamp(-32768.0, 32767.0) as i16;
                output.extend_from_slice(&sample_i16.to_le_bytes());
            }
        }

        Ok(output)
    }

    pub fn resample_22050_to_16k(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        if data.len() % 2 != 0 {
            return Err(CodecError::InvalidLength {
                expected: 2,
                actual: data.len(),
            });
        }

        let num_input_samples = data.len() / 2;
        let num_output_samples = (num_input_samples as f64 * 16000.0 / 22050.0) as usize;
        let mut output = Vec::with_capacity(num_output_samples * 2);

        for i in 0..num_output_samples {
            let input_idx = (i as f64 * 22050.0 / 16000.0) as usize;
            let byte_idx = input_idx * 2;

            if byte_idx + 1 < data.len() {
                output.push(data[byte_idx]);
                output.push(data[byte_idx + 1]);
            }
        }

        Ok(output)
    }
}