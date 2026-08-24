

use flume::{Receiver, Sender, TrySendError};
use metrics::counter;
use tracing::warn;

pub struct ChannelWithDropPolicy<T> {
    tx: Sender<T>,
    rx: Receiver<T>,
}

impl<T> ChannelWithDropPolicy<T> {
    pub fn new(tx: Sender<T>, rx: Receiver<T>) -> Self {
        Self { tx, rx }
    }

    pub fn sender(&self) -> &Sender<T> {
        &self.tx
    }

    pub fn receiver(&self) -> &Receiver<T> {
        &self.rx
    }

    pub fn send_or_drop_oldest(&self, item: T, metric_name: &str) -> Result<(), TrySendError<T>> {
        match self.tx.try_send(item) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(item)) => {
                if let Ok(_dropped) = self.rx.try_recv() {
                    warn!("Dropped oldest item from channel: {}", metric_name);
                    counter!("pipeline_drops_total").increment(1);
                    counter!("pipeline_drops_total", "channel" => metric_name.to_string()).increment(1);
                    self.tx.try_send(item)
                } else {
                    Err(TrySendError::Full(item))
                }
            }
            Err(e) => Err(e),
        }
    }
}

pub trait NonBlockingSend<T> {
    fn try_send_or_drop(&self, item: T, metric_name: &str) -> Result<(), TrySendError<T>>;
}

impl<T> NonBlockingSend<T> for Sender<T> {
    fn try_send_or_drop(&self, item: T, metric_name: &str) -> Result<(), TrySendError<T>> {
        match self.try_send(item) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(_item)) => {
                warn!("Channel full, dropping newest item: {}", metric_name);
                counter!("pipeline_drops_total").increment(1);
                counter!("pipeline_drops_total", "channel" => metric_name.to_string()).increment(1);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}