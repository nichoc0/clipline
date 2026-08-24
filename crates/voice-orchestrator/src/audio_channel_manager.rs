

use bytes::Bytes;
use flume::{Receiver, Sender};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::info;

pub struct AudioChannelPair {
    pub tx: Sender<Bytes>,
    rx: Mutex<Option<Receiver<Bytes>>>,
}

struct AudioChannelManager {
    channels: HashMap<String, Arc<AudioChannelPair>>,
}

impl AudioChannelManager {
    fn new() -> Self {
        Self {
            channels: HashMap::new(),
        }
    }

    fn create_channel_pair(&mut self, call_sid: &str) -> Arc<AudioChannelPair> {
        if let Some(existing) = self.channels.get(call_sid) {
            info!("AudioChannelManager returning existing channel pair for {}", call_sid);
            return existing.clone();
        }

        let (tx, rx) = flume::bounded::<Bytes>(500);
        info!("AudioChannelManager created channel pair for {}: TX: {:p}, RX: {:p}", call_sid, &tx, &rx);

        let pair = Arc::new(AudioChannelPair { tx, rx: Mutex::new(Some(rx)) });
        self.channels.insert(call_sid.to_string(), pair.clone());
        pair
    }

    fn get_channel_pair(&self, call_sid: &str) -> Option<Arc<AudioChannelPair>> {
        self.channels.get(call_sid).cloned()
    }

    fn remove_channel_pair(&mut self, call_sid: &str) {
        if self.channels.remove(call_sid).is_some() {
            info!("AudioChannelManager removed channel pair for {}", call_sid);
        }
    }
}

lazy_static::lazy_static! {
    static ref CHANNEL_MANAGER: Mutex<AudioChannelManager> = Mutex::new(AudioChannelManager::new());
}

pub fn create_audio_channel(call_sid: &str) -> Arc<AudioChannelPair> {
    let mut manager = CHANNEL_MANAGER.lock().unwrap();
    manager.create_channel_pair(call_sid)
}

pub fn get_audio_channel(call_sid: &str) -> Option<Arc<AudioChannelPair>> {
    let manager = CHANNEL_MANAGER.lock().unwrap();
    manager.get_channel_pair(call_sid)
}

pub fn remove_audio_channel(call_sid: &str) {
    let mut manager = CHANNEL_MANAGER.lock().unwrap();
    manager.remove_channel_pair(call_sid);
}

pub fn get_sender(call_sid: &str) -> Option<Sender<Bytes>> {
    get_audio_channel(call_sid).map(|pair| {
        let sender = pair.tx.clone();
        info!("AudioChannelManager returning sender for {}: {:p}, capacity: {}, len: {}", call_sid, &sender, sender.capacity().unwrap_or(0), sender.len());
        sender
    })
}

pub fn get_receiver(call_sid: &str) -> Option<Receiver<Bytes>> {
    let manager = CHANNEL_MANAGER.lock().unwrap();
    manager.get_channel_pair(call_sid).and_then(|pair| {
        let mut rx_guard = pair.rx.lock().unwrap();
        if let Some(rx) = rx_guard.take() {
            info!("AudioChannelManager moved receiver out for {}: {:p}, capacity: {}, len: {}", call_sid, &rx, rx.capacity().unwrap_or(0), rx.len());
            Some(rx)
        } else {
            info!("AudioChannelManager receiver already taken for {}", call_sid);
            None
        }
    })
}