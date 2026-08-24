
use crate::channel_utils::NonBlockingSend;
use crate::providers::groq::GroqProvider;
use crate::transcript_bus::{global as transcript_bus, now_ms, TranscriptDirection, TranscriptFrame};
use anyhow::anyhow;
use flume::{Receiver, Sender};
use metrics::gauge;
use std::collections::VecDeque;
use tracing::{debug, info, warn};
use voice_protocols::{PipelineEvent, WorkerEvent};

#[derive(Clone, Debug)]
pub struct ConversationTurn {
    pub user_text: String,
    pub assistant_response: String,
    pub timestamp: std::time::Instant,
}

pub struct LlmStage {
    provider: Box<dyn LlmProvider + Send + Sync>,
    conversation_history: VecDeque<ConversationTurn>,
    current_user_input: String,
    max_history_turns: usize,
    max_context_chars: usize,
    last_response: Option<String>,
    was_interrupted: bool,
    worker_tx: Option<Sender<WorkerEvent>>,
    session_id: String,
    call_sid: String,
    buffered_response: Option<String>,
    buffered_response_time: Option<std::time::Instant>,
    release_delay_ms: u64,
    last_final_word_count: usize,
    agent_speaking_until: Option<std::time::Instant>,
    vad_on_since: Option<std::time::Instant>,
    last_vad_speech_at: Option<std::time::Instant>,
    vad_barge_in_fired: bool,
    last_partial: Option<(String, std::time::Instant)>,
    last_stt_activity_at: Option<std::time::Instant>,
    last_response_at: Option<std::time::Instant>,
}

#[async_trait::async_trait]
pub trait LlmProvider {
    async fn generate_response(&mut self, prompt: &str) -> Result<String, anyhow::Error>;
    async fn stream_tokens(&mut self, prompt: &str) -> Result<Vec<String>, anyhow::Error>;
    async fn generate_response_with_context(&mut self, prompt: &str, history: &VecDeque<ConversationTurn>) -> Result<String, anyhow::Error>;

    fn inject_context(&mut self, context: &str);

    fn set_system_prompt(&mut self, prompt: String) {
        self.inject_context(&prompt);
    }
}

impl LlmStage {
    pub async fn new() -> Result<Self, anyhow::Error> {
        Self::new_with_session_type("telnyx", None, "default_session".to_string(), None).await
    }

    pub async fn new_with_session_type(
        session_type: &str,
        worker_tx: Option<Sender<WorkerEvent>>,
        call_sid: String,
        _caller_phone: Option<String>,
    ) -> Result<Self, anyhow::Error> {
        let provider_type = std::env::var("VOICE_LLM_PROVIDER")
            .map_err(|_| anyhow!("VOICE_LLM_PROVIDER environment variable must be set"))?;

        let provider: Box<dyn LlmProvider + Send + Sync> = match provider_type.as_str() {
            "groq" => {
                Box::new(GroqProvider::new_for_session_type(session_type)?)
            }
            "openai" => {
                use crate::providers::openai::OpenAiProvider;
                Box::new(OpenAiProvider::new()?)
            }
            "repl" => Box::new(crate::providers::repl::ReplProvider::new()?),
            _ => {
                return Err(anyhow!("Unsupported LLM provider '{}'. Supported: groq, openai, repl", provider_type));
            }
        };

        Ok(Self {
            provider,
            conversation_history: VecDeque::new(),
            current_user_input: String::new(),
            max_history_turns: 10,
            max_context_chars: 2000,
            last_response: None,
            was_interrupted: false,
            worker_tx,
            session_id: call_sid.clone(),
            call_sid,
            buffered_response: None,
            buffered_response_time: None,
            release_delay_ms: 200,
            last_final_word_count: 0,
            agent_speaking_until: None,
            vad_on_since: None,
            last_vad_speech_at: None,
            vad_barge_in_fired: false,
            last_partial: None,
            last_stt_activity_at: None,
            last_response_at: None,
        })
    }

    pub async fn run(
        &mut self,
        stt_rx: Receiver<PipelineEvent>,
        llm_tx: Sender<PipelineEvent>,
        shutdown_rx: Receiver<()>,
    ) {
        debug!("LLM stage started");

        self.send_initial_greeting(&llm_tx).await;

        loop {
            tokio::select! {
                _ = shutdown_rx.recv_async() => {
                    debug!("LLM stage shutting down");

                    if let Some(ref worker_tx) = self.worker_tx {
                        let full_transcript = self.conversation_history
                            .iter()
                            .map(|turn| format!("User: {}\nAssistant: {}",
                                               turn.user_text, turn.assistant_response))
                            .collect::<Vec<_>>()
                            .join("\n\n");

                        let worker_event = WorkerEvent::ConversationComplete {
                            session_id: self.session_id.clone(),
                            call_sid: self.call_sid.clone(),
                            full_transcript,
                        };

                        if let Err(e) = worker_tx.try_send(worker_event) {
                            debug!("Failed to send conversation complete to worker: {}", e);
                        }
                    }

                    break;
                }
                Ok(event) = stt_rx.recv_async() => {
                    self.process_single_event(event, &stt_rx, &llm_tx, &shutdown_rx).await;

                    for _ in 0..50 {
                        match stt_rx.try_recv() {
                            Ok(event) => {
                                self.process_single_event(event, &stt_rx, &llm_tx, &shutdown_rx).await;
                            }
                            Err(_) => break,
                        }
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    if let (Some(response), Some(buffer_time)) = (&self.buffered_response, self.buffered_response_time) {
                        let elapsed = buffer_time.elapsed().as_millis() as u64;
                        if elapsed >= self.release_delay_ms {
                            info!("Releasing buffered response after {}ms delay: '{}'", elapsed, response);
                            let response_text = response.clone();
                            let event = PipelineEvent::LlmComplete { response: response_text.clone() };
                            gauge!("pipeline_queue_depth", "stage" => "llm_to_tts").set(llm_tx.len() as f64);

                            if llm_tx.try_send_or_drop(event, "llm_to_tts").is_ok() {
                                self.mark_agent_speaking(&response_text);
                                self.buffered_response = None;
                                self.buffered_response_time = None;
                            }
                        }
                    }
                }
            }
        }

        debug!("LLM stage finished");
    }

    async fn process_single_event(
        &mut self,
        event: PipelineEvent,
        _stt_rx: &Receiver<PipelineEvent>,
        llm_tx: &Sender<PipelineEvent>,
        _shutdown_rx: &Receiver<()>,
    ) {
        match event {
            PipelineEvent::SttFinal { ref text, confidence } => {
                info!("LLM received SttFinal: '{}' (confidence: {:.2})", text, confidence);
                self.last_stt_activity_at = Some(std::time::Instant::now());
                if self.agent_currently_speaking() {
                    if std::env::var("VOICE_HALF_DUPLEX").as_deref() != Ok("0") {
                        debug!("Half-duplex: dropping '{}' while agent speaking", text);
                        return;
                    }
                    let total_words = text.split_whitespace().count();
                    let novel = count_novel_words(text, self.last_response.as_deref());
                    let real_interrupt =
                        total_words >= 4 && novel >= 3 && confidence >= 0.85;
                    if real_interrupt {
                        info!(
                            "Echo-gate: SttFinal '{}' ({} total / {} novel / conf {:.2}) — treating as barge-in",
                            text, total_words, novel, confidence
                        );
                        self.agent_speaking_until = None;
                        self.buffered_response = None;
                        self.buffered_response_time = None;
                        self.was_interrupted = true;
                        crate::barge_in_bus::global().signal(&self.call_sid);
                        return;
                    } else {
                        info!(
                            "Echo-gate: dropping SttFinal '{}' (total={} novel={} conf={:.2}) — looks like echo or noise",
                            text, total_words, novel, confidence
                        );
                        return;
                    }
                }
                self.last_final_word_count = text.split_whitespace().count();
                transcript_bus().publish(&self.call_sid, TranscriptFrame {
                    direction: TranscriptDirection::Inbound,
                    text: text.clone(),
                    is_final: true,
                    confidence: Some(confidence),
                    timestamp_ms: now_ms(),
                });
                let min_conf: f32 = std::env::var("VOICE_STT_MIN_CONFIDENCE")
                    .ok().and_then(|s| s.parse().ok()).unwrap_or(0.4);
                if confidence > min_conf {
                    debug!("SttFinal passed confidence check, appending to current_user_input");
                    self.current_user_input.push_str(text);
                    self.current_user_input.push(' ');

                    let prompt = if self.was_interrupted && self.last_response.is_some() {
                        debug!("Building prompt with interruption context");
                        format!("[You were interrupted while saying: \"{}\"]\nUser said: {}",
                            self.last_response.as_ref().unwrap(),
                            self.current_user_input)
                    } else {
                        debug!("Building prompt without interruption context");
                        self.current_user_input.clone()
                    };

                    debug!("Calling LLM with prompt: '{}'", prompt);
                    if !self.was_interrupted
                        && std::env::var("VOICE_BACKCHANNEL").as_deref() != Ok("0")
                    {
                        const FILLERS: [&str; 6] = [
                            "Mm-hmm.", "Sure.", "Right, okay.",
                            "Okay, let me see.", "Got it.", "One sec.",
                        ];
                        let filler = FILLERS[self.conversation_history.len() % FILLERS.len()].to_string();
                        info!("Backchannel filler: '{}'", filler);
                        let _ = llm_tx.try_send_or_drop(
                            PipelineEvent::LlmComplete { response: filler.clone() },
                            "llm_to_tts",
                        );
                        self.mark_agent_speaking(&filler);
                    }
                    match self.provider.generate_response_with_context(&prompt, &self.conversation_history).await {
                        Ok(response) => {
                            info!("LLM response generated: '{}'", response);
                            self.last_response_at = Some(std::time::Instant::now());
                            transcript_bus().publish(&self.call_sid, TranscriptFrame {
                                direction: TranscriptDirection::Outbound,
                                text: response.clone(),
                                is_final: true,
                                confidence: None,
                                timestamp_ms: now_ms(),
                            });
                            let turn = ConversationTurn {
                                user_text: self.current_user_input.clone(),
                                assistant_response: response.clone(),
                                timestamp: std::time::Instant::now(),
                            };
                            self.add_conversation_turn(turn);
                            self.current_user_input.clear();
                            self.last_partial = None;

                            self.last_response = Some(response.clone());
                            self.was_interrupted = false;

                            info!("Sending LLM response to TTS: '{}'", response);
                            let _ = llm_tx.try_send_or_drop(
                                PipelineEvent::LlmComplete { response: response.clone() },
                                "llm_to_tts",
                            );
                            self.mark_agent_speaking(&response);
                        }
                        Err(e) => {
                            warn!("LLM generation failed: {}", e);
                        }
                    }
                } else {
                    debug!("SttFinal below confidence threshold, ignoring");
                }
            }
            PipelineEvent::SttPartial { ref text, confidence } => {
                debug!("LLM received SttPartial: '{}' (confidence: {:.2}, len: {})", text, confidence, text.len());
                if !text.trim().is_empty() && confidence >= 0.7 {
                    let now = std::time::Instant::now();
                    self.last_partial = Some((text.to_string(), now));
                    self.last_stt_activity_at = Some(now);
                }
                if self.agent_currently_speaking() {
                    if std::env::var("VOICE_HALF_DUPLEX").as_deref() != Ok("0") {
                        debug!("Half-duplex: dropping '{}' while agent speaking", text);
                        return;
                    }
                    let total_words = text.split_whitespace().count();
                    let novel = count_novel_words(text, self.last_response.as_deref());
                    let real_interrupt =
                        total_words >= 3 && novel >= 2 && confidence >= 0.85;
                    if real_interrupt {
                        info!(
                            "Partial barge-in: '{}' ({} total / {} novel / conf {:.2}) — firing interrupt",
                            text, total_words, novel, confidence
                        );
                        self.agent_speaking_until = None;
                        self.buffered_response = None;
                        self.buffered_response_time = None;
                        self.was_interrupted = true;
                        crate::barge_in_bus::global().signal(&self.call_sid);
                    } else {
                        debug!(
                            "Echo-gate: dropping SttPartial '{}' (total={} novel={} conf={:.2})",
                            text, total_words, novel, confidence
                        );
                    }
                    return;
                }
                transcript_bus().publish(&self.call_sid, TranscriptFrame {
                    direction: TranscriptDirection::Inbound,
                    text: text.clone(),
                    is_final: false,
                    confidence: Some(confidence),
                    timestamp_ms: now_ms(),
                });

                if self.buffered_response.is_some() {
                    let partial_words = text.split_whitespace().count();
                    let is_continuation = partial_words >= self.last_final_word_count
                        && partial_words >= 2;
                    if is_continuation {
                        info!(
                            "User still speaking ({} words ≥ last final {}); cancelling buffered response",
                            partial_words, self.last_final_word_count
                        );
                        self.buffered_response = None;
                        self.buffered_response_time = None;
                    } else {
                        debug!(
                            "Ignoring likely phantom partial '{}' ({} words < last final {} words); keeping buffered response",
                            text, partial_words, self.last_final_word_count
                        );
                    }
                }
            }
            PipelineEvent::Interrupt => {
                info!("LLM received interrupt - marking for natural acknowledgment");
                self.was_interrupted = true;

                if let Some(ref interrupted_response) = self.last_response {
                    let interrupt_turn = ConversationTurn {
                        user_text: "[User interrupted while assistant was speaking]".to_string(),
                        assistant_response: format!("[Was saying: \"{}\"]", interrupted_response),
                        timestamp: std::time::Instant::now(),
                    };
                    self.add_conversation_turn(interrupt_turn);
                    debug!("Added interrupt context to conversation history");
                }
            }
            PipelineEvent::VadDetected { is_speech } => {
                debug!("LLM forwarding VAD event to TTS: is_speech={}", is_speech);
                let now = std::time::Instant::now();
                if is_speech {
                    if self.vad_on_since.is_none() {
                        self.vad_on_since = Some(now);
                        self.vad_barge_in_fired = false;
                    }
                    self.last_vad_speech_at = Some(now);
                } else {
                    self.vad_on_since = None;
                    self.vad_barge_in_fired = false;
                }
                let _ = llm_tx.try_send(PipelineEvent::VadDetected { is_speech });
            }
            _ => {}
        }
    }

    fn mark_agent_speaking(&mut self, text: &str) {
        let words = text.split_whitespace().count().max(1) as u64;
        let dur_ms = words * 330 + 500;
        let new_until = std::time::Instant::now() + std::time::Duration::from_millis(dur_ms);
        self.agent_speaking_until = Some(match self.agent_speaking_until {
            Some(prev) if prev > new_until => prev,
            _ => new_until,
        });
        debug!("Agent speaking gate set: ~{}ms ({} words)", dur_ms, words);
    }

    fn agent_currently_speaking(&self) -> bool {
        match self.agent_speaking_until {
            Some(until) => until > std::time::Instant::now(),
            None => false,
        }
    }

    async fn send_initial_greeting(&mut self, llm_tx: &Sender<PipelineEvent>) {
        {
            use std::sync::{Mutex, OnceLock};
            static GREETED: OnceLock<Mutex<std::collections::HashSet<String>>> = OnceLock::new();
            let mut set = GREETED
                .get_or_init(|| Mutex::new(std::collections::HashSet::new()))
                .lock()
                .unwrap();
            if !set.insert(self.call_sid.clone()) {
                info!(call_sid = %self.call_sid, "Skipping duplicate initial greeting");
                return;
            }
        }
        info!("Generating initial greeting for call");

        let intro_prompt = std::env::var("VOICE_TELNYX_INTRO").unwrap_or_else(|_| {
            "You are a helpful voice assistant. Please introduce yourself \
             briefly and ask how you can help.".to_string()
        });

        match self.provider.generate_response(&intro_prompt).await {
            Ok(greeting) => {
                info!("Generated initial greeting: {}", greeting);
                transcript_bus().publish(&self.call_sid, TranscriptFrame {
                    direction: TranscriptDirection::Outbound,
                    text: greeting.clone(),
                    is_final: true,
                    confidence: None,
                    timestamp_ms: now_ms(),
                });

                let turn = ConversationTurn {
                    user_text: String::new(),
                    assistant_response: greeting.clone(),
                    timestamp: std::time::Instant::now(),
                };
                self.add_conversation_turn(turn);

                self.last_response = Some(greeting.clone());

                self.mark_agent_speaking(&greeting);
                let event = PipelineEvent::LlmComplete { response: greeting };

                gauge!("pipeline_queue_depth", "stage" => "llm_to_tts").set(llm_tx.len() as f64);

                if let Err(_) = llm_tx.try_send_or_drop(event, "llm_to_tts") {
                    warn!("Failed to send initial greeting - LLM output channel closed");
                }
            }
            Err(e) => {
                warn!("Failed to generate initial greeting: {}", e);
                let fallback_greeting = "Hello! I'm your voice assistant. How can I help you today?";
                let event = PipelineEvent::LlmComplete { response: fallback_greeting.to_string() };

                if let Err(_) = llm_tx.try_send_or_drop(event, "llm_to_tts") {
                    warn!("Failed to send fallback greeting - LLM output channel closed");
                }
            }
        }
    }

    fn add_conversation_turn(&mut self, turn: ConversationTurn) {
        self.conversation_history.push_back(turn);

        while self.conversation_history.len() > self.max_history_turns {
            self.conversation_history.pop_front();
        }

        while self.get_context_size() > self.max_context_chars && !self.conversation_history.is_empty() {
            self.conversation_history.pop_front();
        }
    }

    fn get_context_size(&self) -> usize {
        self.conversation_history.iter()
            .map(|turn| turn.user_text.len() + turn.assistant_response.len())
            .sum()
    }
}

fn count_novel_words(text: &str, agent_last_response: Option<&str>) -> usize {
    let normalize = |s: &str| -> std::collections::HashSet<String> {
        s.split(|c: char| !c.is_alphanumeric())
            .filter(|t| !t.is_empty())
            .map(|t| t.to_lowercase())
            .collect()
    };
    let theirs = normalize(text);
    let mine = match agent_last_response {
        Some(s) => normalize(s),
        None => return 0,
    };
    theirs.iter().filter(|w| !mine.contains(*w)).count()
}
