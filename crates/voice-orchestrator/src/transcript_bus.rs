
use dashmap::DashMap;
use serde::Serialize;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

#[derive(Clone, Copy, Debug, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TranscriptDirection {
    Inbound,
    Outbound,
}

#[derive(Clone, Debug, Serialize)]
pub struct TranscriptFrame {
    pub direction: TranscriptDirection,
    pub text: String,
    pub is_final: bool,
    pub confidence: Option<f32>,
    pub timestamp_ms: u64,
}

const HISTORY_CAP: usize = 200;

struct CallChannel {
    tx: broadcast::Sender<TranscriptFrame>,
    history: parking_lot::Mutex<std::collections::VecDeque<TranscriptFrame>>,
}

pub struct TranscriptBus {
    channels: DashMap<String, std::sync::Arc<CallChannel>>,
}

impl TranscriptBus {
    fn new() -> Self {
        Self { channels: DashMap::new() }
    }

    fn entry_for(&self, call_sid: &str) -> std::sync::Arc<CallChannel> {
        self.channels
            .entry(call_sid.to_string())
            .or_insert_with(|| {
                let (tx, _rx) = broadcast::channel::<TranscriptFrame>(128);
                std::sync::Arc::new(CallChannel {
                    tx,
                    history: parking_lot::Mutex::new(
                        std::collections::VecDeque::with_capacity(HISTORY_CAP),
                    ),
                })
            })
            .clone()
    }

    pub fn publish(&self, call_sid: &str, frame: TranscriptFrame) {
        let entry = self.entry_for(call_sid);
        {
            let mut h = entry.history.lock();
            if h.len() == HISTORY_CAP {
                h.pop_front();
            }
            h.push_back(frame.clone());
        }
        let _ = entry.tx.send(frame);
    }

    pub fn subscribe_with_history(
        &self,
        call_sid: &str,
    ) -> (Vec<TranscriptFrame>, broadcast::Receiver<TranscriptFrame>) {
        let entry = self.entry_for(call_sid);
        let snapshot: Vec<TranscriptFrame> = entry.history.lock().iter().cloned().collect();
        let rx = entry.tx.subscribe();
        (snapshot, rx)
    }

    pub fn subscribe(&self, call_sid: &str) -> broadcast::Receiver<TranscriptFrame> {
        self.subscribe_with_history(call_sid).1
    }

    pub fn snapshot(&self, call_sid: &str) -> Vec<TranscriptFrame> {
        match self.channels.get(call_sid) {
            Some(entry) => entry.history.lock().iter().cloned().collect(),
            None => Vec::new(),
        }
    }

    pub fn close(&self, call_sid: &str) {
        self.channels.remove(call_sid);
    }
}

static GLOBAL: OnceLock<TranscriptBus> = OnceLock::new();

pub fn global() -> &'static TranscriptBus {
    GLOBAL.get_or_init(TranscriptBus::new)
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}
