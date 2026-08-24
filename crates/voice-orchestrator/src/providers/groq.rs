

use crate::agent_config::{AgentConfig, AgentType};
use crate::stages::llm::{LlmProvider, ConversationTurn};
use anyhow::{anyhow, Result};
use reqwest::Client;
use reqwest_eventsource::{Event, EventSource};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use async_trait::async_trait;
use futures::StreamExt;
use metrics::{counter, histogram};
use std::time::Instant;
use tracing::{debug, warn, info};

#[derive(Serialize, Deserialize)]
struct GroqMessage {
    role: String,
    content: String,
}

#[derive(Serialize)]
struct GroqRequest {
    model: String,
    messages: Vec<GroqMessage>,
    temperature: f32,
    max_tokens: u32,
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct GroqResponse {
    choices: Vec<GroqChoice>,
}

#[derive(Deserialize)]
struct GroqChoice {
    message: GroqResponseMessage,
}

#[derive(Deserialize)]
struct GroqResponseMessage {
    content: String,
}

#[derive(Deserialize)]
struct GroqStreamResponse {
    choices: Vec<GroqStreamChoice>,
}

#[derive(Deserialize)]
struct GroqStreamChoice {
    delta: GroqDelta,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct GroqDelta {
    content: Option<String>,
}

pub struct GroqProvider {
    client: Client,
    api_key: String,
    model: String,
    system_prompt: String,
    agent_type: AgentType,
    intro_prompt: String,
}

impl GroqProvider {
    pub fn new() -> Result<Self> {
        let api_key = std::env::var("VOICE_GROQ_API_KEY")
            .map_err(|_| anyhow!("VOICE_GROQ_API_KEY environment variable not set"))?;

        let model = std::env::var("VOICE_GROQ_MODEL")
            .unwrap_or_else(|_| "llama-3.1-8b-instant".to_string());

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        let system_prompt = std::env::var("VOICE_SYSTEM_PROMPT")
            .unwrap_or_else(|_| "You are Claude, answering a phone call. Keep replies short and plain.".to_string());

        Ok(Self {
            client,
            api_key,
            model,
            system_prompt,
            agent_type: AgentType::Assistant,
            intro_prompt: AgentConfig::for_agent(AgentType::Assistant).get_intro_prompt(),
        })
    }

    pub fn new_with_system_prompt(custom_prompt: String) -> Result<Self> {
        let api_key = std::env::var("VOICE_GROQ_API_KEY")
            .map_err(|_| anyhow!("VOICE_GROQ_API_KEY environment variable not set"))?;

        let model = std::env::var("VOICE_GROQ_MODEL")
            .unwrap_or_else(|_| "llama-3.1-8b-instant".to_string());

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        Ok(Self {
            client,
            api_key,
            model,
            system_prompt: custom_prompt,
            agent_type: AgentType::Assistant,
            intro_prompt: AgentConfig::for_agent(AgentType::Assistant).get_intro_prompt(),
        })
    }

    pub fn new_for_agent(agent_type: AgentType) -> Result<Self> {
        let api_key = std::env::var("VOICE_GROQ_API_KEY")
            .map_err(|_| anyhow!("VOICE_GROQ_API_KEY environment variable not set"))?;

        let model = std::env::var("VOICE_GROQ_MODEL")
            .unwrap_or_else(|_| "llama-3.1-8b-instant".to_string());

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;

        let config = AgentConfig::for_agent(agent_type);
        info!("Initializing GroqProvider for agent: {} ({})", config.display_name, config.name);

        let intro_prompt = config.get_intro_prompt();

        Ok(Self {
            client,
            api_key,
            model,
            system_prompt: config.system_prompt,
            agent_type,
            intro_prompt,
        })
    }

    pub fn new_for_session_type(_session_type: &str) -> Result<Self> {
        Self::new()
    }

    pub fn get_intro_prompt(&self) -> &str {
        &self.intro_prompt
    }

    pub fn agent_type(&self) -> AgentType {
        self.agent_type
    }

    pub fn inject_context(&mut self, context: &str) {
        info!("Injecting client context ({} chars) into system prompt", context.len());
        self.system_prompt = format!("{}\n[AGENT ROLE]\n{}", context, self.system_prompt);
    }

    pub fn set_system_prompt(&mut self, prompt: String) {
        info!("Replacing system prompt ({} chars)", prompt.len());
        self.system_prompt = prompt;
    }

    pub fn get_system_prompt(&self) -> &str {
        &self.system_prompt
    }
}

#[async_trait]
impl LlmProvider for GroqProvider {
    async fn generate_response(&mut self, prompt: &str) -> Result<String> {
        let start_time = Instant::now();

        let messages = vec![
            GroqMessage {
                role: "system".to_string(),
                content: self.system_prompt.clone(),
            },
            GroqMessage {
                role: "user".to_string(),
                content: prompt.to_string(),
            },
        ];

        let request = GroqRequest {
            model: self.model.clone(),
            messages,
            temperature: 0.7,
            max_tokens: 1000,
            reasoning_effort: Some("low".to_string()),
            stop: Some(vec!["\n\n".to_string(), "Assistant:".to_string(), "Agent:".to_string(), "[Agent]".to_string()]),
            stream: false,
        };

        let response = self.client
            .post("https://api.groq.com/openai/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Groq API error {}: {}", status, error_text));
        }

        let groq_response: GroqResponse = response.json().await?;
        let content = groq_response
            .choices
            .first()
            .ok_or_else(|| anyhow!("No choices in Groq response"))?
            .message
            .content
            .clone();

        let duration = start_time.elapsed();
        histogram!("llm_response_time_ms", "provider" => "groq").record(duration.as_millis() as f64);
        counter!("llm_requests_total", "provider" => "groq").increment(1);

        debug!("Groq response generated in {}ms: {}", duration.as_millis(), content);
        Ok(content)
    }

    async fn stream_tokens(&mut self, prompt: &str) -> Result<Vec<String>> {
        let start_time = Instant::now();

        let messages = vec![
            GroqMessage {
                role: "system".to_string(),
                content: self.system_prompt.clone(),
            },
            GroqMessage {
                role: "user".to_string(),
                content: prompt.to_string(),
            },
        ];

        let request = GroqRequest {
            model: self.model.clone(),
            messages,
            temperature: 0.7,
            max_tokens: 1000,
            reasoning_effort: Some("low".to_string()),
            stop: Some(vec!["\n\n".to_string(), "Assistant:".to_string(), "Agent:".to_string(), "[Agent]".to_string()]),
            stream: true,
        };

        let request_builder = self.client
            .post("https://api.groq.com/openai/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request);

        let mut event_source = EventSource::new(request_builder)?;
        let mut tokens = Vec::new();
        let mut first_token = true;

        while let Some(event) = event_source.next().await {
            match event {
                Ok(Event::Message(message)) => {
                    if message.data == "[DONE]" {
                        break;
                    }

                    match serde_json::from_str::<GroqStreamResponse>(&message.data) {
                        Ok(response) => {
                            for choice in response.choices {
                                if let Some(content) = choice.delta.content {
                                    if first_token {
                                        let ttft = start_time.elapsed();
                                        histogram!("llm_ttft_ms", "provider" => "groq").record(ttft.as_millis() as f64);
                                        debug!("Groq TTFT: {}ms", ttft.as_millis());
                                        first_token = false;
                                    }

                                    if !content.trim().is_empty() {
                                        tokens.push(content);
                                    }
                                }

                                if choice.finish_reason.is_some() {
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse Groq streaming response: {}", e);
                        }
                    }
                }
                Ok(Event::Open) => {
                    debug!("Groq SSE connection opened");
                }
                Err(e) => {
                    warn!("Groq streaming error: {}", e);
                    break;
                }
            }
        }

        let total_time = start_time.elapsed();
        histogram!("llm_stream_time_ms", "provider" => "groq").record(total_time.as_millis() as f64);
        counter!("llm_stream_requests_total", "provider" => "groq").increment(1);

        debug!("Groq streamed {} tokens in {}ms", tokens.len(), total_time.as_millis());
        Ok(tokens)
    }

    async fn generate_response_with_context(&mut self, prompt: &str, history: &VecDeque<ConversationTurn>) -> Result<String> {
        let start_time = Instant::now();

        let mut messages = vec![
            GroqMessage {
                role: "system".to_string(),
                content: self.system_prompt.clone(),
            }
        ];

        let recent_turns: Vec<_> = history.iter().rev().take(6).collect();
        for turn in recent_turns.iter().rev() {
            messages.push(GroqMessage {
                role: "user".to_string(),
                content: turn.user_text.clone(),
            });
            messages.push(GroqMessage {
                role: "assistant".to_string(),
                content: turn.assistant_response.clone(),
            });
        }

        messages.push(GroqMessage {
            role: "user".to_string(),
            content: prompt.to_string(),
        });

        let request = GroqRequest {
            model: self.model.clone(),
            messages,
            temperature: 0.7,
            max_tokens: 1000,
            reasoning_effort: Some("low".to_string()),
            stop: Some(vec!["\n\n".to_string(), "Assistant:".to_string(), "Agent:".to_string(), "[Agent]".to_string()]),
            stream: false,
        };

        let response = self.client
            .post("https://api.groq.com/openai/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Groq API error {}: {}", status, error_text));
        }

        let groq_response: GroqResponse = response.json().await?;
        let content = groq_response
            .choices
            .first()
            .ok_or_else(|| anyhow!("No choices in Groq response"))?
            .message
            .content
            .clone();

        let duration = start_time.elapsed();
        histogram!("llm_context_response_time_ms", "provider" => "groq").record(duration.as_millis() as f64);
        counter!("llm_context_requests_total", "provider" => "groq").increment(1);

        debug!("Groq contextual response generated in {}ms: {} (history: {} turns)",
               duration.as_millis(), content, history.len());
        Ok(content)
    }

    fn inject_context(&mut self, context: &str) {
        info!("Injecting client context ({} chars) into system prompt", context.len());
        self.system_prompt = format!("{}\\n[AGENT ROLE]\\n{}", context, self.system_prompt);
    }

    fn set_system_prompt(&mut self, prompt: String) {
        info!("set_system_prompt: replacing system prompt with {} chars (was {} chars)",
              prompt.len(), self.system_prompt.len());
        self.system_prompt = prompt;
    }
}

impl GroqProvider {
    pub async fn extract_contact_info(&mut self, transcript: &str) -> Result<ExtractedContactData> {
        let extraction_prompt = format!(
            r#"Extract contact information from this conversation transcript.
Return ONLY valid JSON in this exact format:
{{
    "name": "string or null",
    "email": "string or null",
    "phone": "string or null"
}}

Rules:
- name: Full name if mentioned, otherwise null
- email: Email address if mentioned, otherwise null
- phone: Phone number if mentioned (any format), otherwise null
- Do NOT include any text outside the JSON object
- Use null for missing fields, not empty strings

Transcript:
{}
"#,
            transcript
        );

        let messages = vec![
            GroqMessage {
                role: "system".to_string(),
                content: "You are a data extraction assistant. Extract contact information and return ONLY JSON.".to_string(),
            },
            GroqMessage {
                role: "user".to_string(),
                content: extraction_prompt,
            },
        ];

        let request = GroqRequest {
            model: self.model.clone(),
            messages,
            temperature: 0.0,
            max_tokens: 400,
            reasoning_effort: Some("low".to_string()),
            stop: Some(vec!["\n\n".to_string(), "Assistant:".to_string(), "Agent:".to_string(), "[Agent]".to_string()]),
            stream: false,
        };

        let response = self.client
            .post("https://api.groq.com/openai/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Groq extraction API error {}: {}", status, error_text));
        }

        let groq_response: GroqResponse = response.json().await?;
        let content = groq_response
            .choices
            .first()
            .ok_or_else(|| anyhow!("No choices in Groq response"))?
            .message
            .content
            .clone();

        debug!("Groq extraction raw response: {}", content);

        let extracted: ExtractedContactData = serde_json::from_str(&content.trim())
            .map_err(|e| anyhow!("Failed to parse extraction JSON: {}. Response: {}", e, content))?;

        Ok(extracted)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedContactData {
    pub name: Option<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
}
