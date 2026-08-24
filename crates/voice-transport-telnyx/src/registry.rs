
use dashmap::DashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, info};

#[derive(Clone, Debug)]
pub struct SessionEntry {
    pub call_control_id: String,
    pub started_at: SystemTime,
}

#[derive(Clone, Default)]
pub struct SessionRegistryHandle {
    inner: Arc<DashMap<String, SessionEntry>>,
}

impl SessionRegistryHandle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, call_control_id: impl Into<String>) -> SessionEntry {
        let id: String = call_control_id.into();
        let entry = SessionEntry {
            call_control_id: id.clone(),
            started_at: SystemTime::now(),
        };
        self.inner.insert(id.clone(), entry.clone());
        info!("Registered Telnyx session for call {}", id);
        entry
    }

    pub fn remove(&self, call_control_id: &str) -> bool {
        let removed = self.inner.remove(call_control_id).is_some();
        if removed {
            info!("Removed Telnyx session for call {}", call_control_id);
        } else {
            debug!("No Telnyx session found to remove for call {}", call_control_id);
        }
        removed
    }

    pub fn get(&self, call_control_id: &str) -> Option<SessionEntry> {
        self.inner.get(call_control_id).map(|e| (*e).clone())
    }

    pub fn active_count(&self) -> usize {
        self.inner.len()
    }
}

pub type SessionRegistry = SessionRegistryHandle;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_remove_count() {
        let r = SessionRegistryHandle::new();
        assert_eq!(r.active_count(), 0);
        r.insert("call-1");
        r.insert("call-2");
        assert_eq!(r.active_count(), 2);
        assert!(r.get("call-1").is_some());
        assert!(r.remove("call-1"));
        assert_eq!(r.active_count(), 1);
        assert!(!r.remove("call-1"));
    }
}
