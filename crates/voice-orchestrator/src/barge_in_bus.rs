
use dashmap::DashMap;
use std::sync::OnceLock;
use tokio::sync::broadcast;

pub struct BargeInBus {
    channels: DashMap<String, broadcast::Sender<()>>,
}

impl BargeInBus {
    fn new() -> Self {
        Self { channels: DashMap::new() }
    }

    pub fn signal(&self, call_sid: &str) {
        let tx = self
            .channels
            .entry(call_sid.to_string())
            .or_insert_with(|| broadcast::channel::<()>(8).0)
            .clone();
        let _ = tx.send(());
    }

    pub fn subscribe(&self, call_sid: &str) -> broadcast::Receiver<()> {
        let entry = self
            .channels
            .entry(call_sid.to_string())
            .or_insert_with(|| broadcast::channel::<()>(8).0);
        entry.subscribe()
    }

    pub fn close(&self, call_sid: &str) {
        self.channels.remove(call_sid);
    }
}

static GLOBAL: OnceLock<BargeInBus> = OnceLock::new();

pub fn global() -> &'static BargeInBus {
    GLOBAL.get_or_init(BargeInBus::new)
}
