#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AgentType {
    Assistant,
}

impl AgentType {
    pub fn from_str(_s: &str) -> Self {
        AgentType::Assistant
    }

    pub fn as_str(&self) -> &'static str {
        "assistant"
    }
}

pub struct AgentConfig {
    pub agent_type: AgentType,
    pub name: &'static str,
    pub display_name: &'static str,
    pub system_prompt: String,
    pub voice_id: &'static str,
    pub greeting_style: &'static str,
}

impl AgentConfig {
    pub fn for_agent(agent_type: AgentType) -> Self {
        let system_prompt = std::env::var("VOICE_SYSTEM_PROMPT").unwrap_or_else(|_| {
            "You are Claude, answering a phone call. Keep replies short and plain.".to_string()
        });
        Self {
            agent_type,
            name: "assistant",
            display_name: "Assistant",
            system_prompt,
            voice_id: "21m00Tcm4TlvDq8ikWAM",
            greeting_style: "friendly",
        }
    }

    pub fn get_intro_prompt(&self) -> String {
        "Greet the caller in one short sentence and ask how you can help.".to_string()
    }
}
