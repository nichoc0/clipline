

use crate::stages::llm::{LlmProvider, ConversationTurn};
use anyhow::{anyhow, Result};
use async_openai::{
    config::OpenAIConfig,
    types::{
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
        ChatCompletionRequestUserMessageArgs, ChatCompletionRequestAssistantMessageArgs,
        CreateChatCompletionRequestArgs,
    },
    Client,
};
use std::collections::VecDeque;
use async_trait::async_trait;
use futures::StreamExt;
use metrics::{counter, histogram};
use std::time::Instant;
use tracing::{debug, warn};

pub struct OpenAiProvider {
    client: Client<OpenAIConfig>,
    model: String,
    system_prompt: String,
}

impl OpenAiProvider {
    pub fn new() -> Result<Self> {
        let base_url = std::env::var("VOICE_OPENAI_BASE_URL").ok();
        let api_key = match std::env::var("VOICE_OPENAI_API_KEY") {
            Ok(k) => k,
            Err(_) if base_url.is_some() => "sk-noauth".to_string(),
            Err(_) => return Err(anyhow!("VOICE_OPENAI_API_KEY environment variable not set")),
        };

        let model = std::env::var("VOICE_OPENAI_MODEL")
            .unwrap_or_else(|_| "gpt-4o-mini".to_string());

        let mut config = OpenAIConfig::new().with_api_key(api_key);
        if let Some(ref b) = base_url {
            config = config.with_api_base(b.as_str());
            tracing::info!("OpenAiProvider: using custom base URL {}", b);
        }
        let client = Client::with_config(config);

        let system_prompt = std::env::var("VOICE_SYSTEM_PROMPT")
            .unwrap_or_else(|_| "You are Claude, answering a phone call. Keep replies short and plain.".to_string());

        Ok(Self {
            client,
            model,
            system_prompt,
        })
    }
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    async fn generate_response(&mut self, prompt: &str) -> Result<String> {
        let start_time = Instant::now();

        let messages = vec![
            ChatCompletionRequestMessage::System(
                ChatCompletionRequestSystemMessageArgs::default()
                    .content(&self.system_prompt)
                    .build()?,
            ),
            ChatCompletionRequestMessage::User(
                ChatCompletionRequestUserMessageArgs::default()
                    .content(prompt)
                    .build()?,
            ),
        ];

        let request = CreateChatCompletionRequestArgs::default()
            .max_tokens(150u16)
            .model(&self.model)
            .messages(messages)
            .temperature(0.7)
            .build()?;

        let response = self.client.chat().create(request).await?;

        let content = response
            .choices
            .first()
            .and_then(|choice| choice.message.content.as_ref())
            .ok_or_else(|| anyhow!("No response content from OpenAI"))?;

        let duration = start_time.elapsed();
        histogram!("llm_response_time_ms", "provider" => "openai").record(duration.as_millis() as f64);
        counter!("llm_requests_total", "provider" => "openai").increment(1);

        debug!("OpenAI response generated in {}ms: {}", duration.as_millis(), content);
        Ok(content.clone())
    }

    async fn stream_tokens(&mut self, prompt: &str) -> Result<Vec<String>> {
        let start_time = Instant::now();

        let messages = vec![
            ChatCompletionRequestMessage::System(
                ChatCompletionRequestSystemMessageArgs::default()
                    .content(&self.system_prompt)
                    .build()?,
            ),
            ChatCompletionRequestMessage::User(
                ChatCompletionRequestUserMessageArgs::default()
                    .content(prompt)
                    .build()?,
            ),
        ];

        let request = CreateChatCompletionRequestArgs::default()
            .max_tokens(150u16)
            .model(&self.model)
            .messages(messages)
            .temperature(0.7)
            .stream(true)
            .build()?;

        let mut stream = self.client.chat().create_stream(request).await?;
        let mut tokens = Vec::new();
        let mut first_token = true;

        while let Some(result) = stream.next().await {
            match result {
                Ok(response) => {
                    for choice in response.choices {
                        if let Some(delta) = choice.delta.content {
                            if first_token {
                                let ttft = start_time.elapsed();
                                histogram!("llm_ttft_ms", "provider" => "openai").record(ttft.as_millis() as f64);
                                debug!("OpenAI TTFT: {}ms", ttft.as_millis());
                                first_token = false;
                            }

                            if !delta.trim().is_empty() {
                                tokens.push(delta);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("OpenAI streaming error: {}", e);
                    break;
                }
            }
        }

        let total_time = start_time.elapsed();
        histogram!("llm_stream_time_ms", "provider" => "openai").record(total_time.as_millis() as f64);
        counter!("llm_stream_requests_total", "provider" => "openai").increment(1);

        debug!("OpenAI streamed {} tokens in {}ms", tokens.len(), total_time.as_millis());
        Ok(tokens)
    }

    async fn generate_response_with_context(&mut self, prompt: &str, history: &VecDeque<ConversationTurn>) -> Result<String> {
        let start_time = Instant::now();

        let mut messages = vec![
            ChatCompletionRequestMessage::System(
                ChatCompletionRequestSystemMessageArgs::default()
                    .content(&self.system_prompt)
                    .build()?,
            )
        ];

        let recent_turns: Vec<_> = history.iter().rev().take(3).collect();
        for turn in recent_turns.iter().rev() {
            messages.push(ChatCompletionRequestMessage::User(
                ChatCompletionRequestUserMessageArgs::default()
                    .content(turn.user_text.clone())
                    .build()?,
            ));
            messages.push(ChatCompletionRequestMessage::Assistant(
                ChatCompletionRequestAssistantMessageArgs::default()
                    .content(turn.assistant_response.clone())
                    .build()?,
            ));
        }

        messages.push(ChatCompletionRequestMessage::User(
            ChatCompletionRequestUserMessageArgs::default()
                .content(prompt)
                .build()?,
        ));

        let request = CreateChatCompletionRequestArgs::default()
            .max_tokens(150u16)
            .model(&self.model)
            .messages(messages)
            .temperature(0.7)
            .build()?;

        let response = self.client.chat().create(request).await?;

        let content = response
            .choices
            .first()
            .and_then(|choice| choice.message.content.as_ref())
            .ok_or_else(|| anyhow!("No response content from OpenAI"))?;

        let duration = start_time.elapsed();
        histogram!("llm_context_response_time_ms", "provider" => "openai").record(duration.as_millis() as f64);
        counter!("llm_context_requests_total", "provider" => "openai").increment(1);

        debug!("OpenAI contextual response generated in {}ms: {} (history: {} turns)",
               duration.as_millis(), content, history.len());
        Ok(content.clone())
    }

    fn inject_context(&mut self, context: &str) {
        self.system_prompt = format!("{}\\n[AGENT ROLE]\\n{}", context, self.system_prompt);
    }
}