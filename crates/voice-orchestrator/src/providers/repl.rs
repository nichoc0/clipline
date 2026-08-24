use crate::stages::llm::{ConversationTurn, LlmProvider};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::process::Command;
use tokio::time::{sleep, Instant};
use tracing::{info, warn};

const SPINNERS: [&str; 7] = [
    "esc to interrupt", "Cogitating", "Synthesizing", "Pondering",
    "Marinating", "Brewing", "Shenaniganing",
];
const MODAL: &str = " 1. Yes";
const POLL: Duration = Duration::from_millis(150);
const MAX_WAIT: Duration = Duration::from_secs(45);
const BOOT_WAIT: Duration = Duration::from_secs(6);
const PRESEND_IDLE_WAIT: Duration = Duration::from_secs(3);

struct Config {
    claude: String,
    model: String,
    workdir: String,
    tools: bool,
    system_prompt: String,
    session: Option<String>,
}

impl Config {
    fn from_env() -> Self {
        let tools = std::env::var("CLIPLINE_TOOLS").map(|v| v != "0").unwrap_or(false);
        let system_prompt = std::env::var("VOICE_SYSTEM_PROMPT")
            .unwrap_or_else(|_| "You are Claude, answering a phone call. Keep replies short and plain.".to_string());
        Config {
            claude: std::env::var("CLIPLINE_CLAUDE_BIN").unwrap_or_else(|_| "claude".to_string()),
            model: std::env::var("CLIPLINE_MODEL")
                .unwrap_or_else(|_| "claude-haiku-4-5-20251001".to_string()),
            workdir: std::env::var("CLIPLINE_WORKDIR")
                .unwrap_or_else(|_| std::env::current_dir().map(|p| p.display().to_string()).unwrap_or_else(|_| ".".to_string())),
            tools,
            system_prompt,
            session: std::env::var("CLIPLINE_SESSION").ok().filter(|s| !s.is_empty()),
        }
    }

    fn contract(&self) -> &'static str {
        if self.tools {
            "\n\nIf the question needs it, use your tools first (read files, search, run shell \
             commands in the working directory) before you answer. Your final output must be your \
             spoken line wrapped [SPK]your line[/SPK] then a space then the SENTINEL token, with \
             nothing after it. Keep the spoken line short and plain, like talking on a phone."
        } else {
            "\n\nReply with only your next spoken line (one short sentence). Wrap it \
             [SPK]your line[/SPK] then a space then the SENTINEL token. Nothing else."
        }
    }
}

struct Session {
    name: String,
    turn: tokio::sync::Mutex<()>,
    ready: Mutex<bool>,
}

fn pool() -> &'static Mutex<HashMap<String, Arc<Session>>> {
    static POOL: OnceLock<Mutex<HashMap<String, Arc<Session>>>> = OnceLock::new();
    POOL.get_or_init(|| Mutex::new(HashMap::new()))
}

fn session_key(system_prompt: &str) -> String {
    let mut h = DefaultHasher::new();
    system_prompt.hash(&mut h);
    format!("clipline-{:08x}", (h.finish() as u32))
}

fn short_id(tag: &str) -> String {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{}{:x}{:06x}", tag, std::process::id(), n & 0xffffff)
}

async fn tmux(args: &[&str]) -> String {
    match Command::new("tmux").args(args).output().await {
        Ok(o) => String::from_utf8_lossy(&o.stdout).to_string(),
        Err(_) => String::new(),
    }
}

async fn has_session(name: &str) -> bool {
    Command::new("tmux")
        .args(["has-session", "-t", name])
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn capture(name: &str, lines: i64) -> String {
    tmux(&["capture-pane", "-p", "-t", name, "-S", &format!("-{}", lines)]).await
}

fn spinning(pane: &str) -> bool {
    SPINNERS.iter().any(|s| pane.contains(s))
}

async fn spawn_and_warm(name: &str, cfg: &Config) -> Result<()> {
    tmux(&["kill-session", "-t", name]).await;
    let prompt_file = std::env::temp_dir().join(format!("{}.txt", name));
    std::fs::write(&prompt_file, format!("{}{}", cfg.system_prompt, cfg.contract()))?;
    let tools_flag = if cfg.tools { "" } else { "--allowedTools ''" };
    let launch = format!(
        "env -u CLAUDECODE -u CLAUDE_CODE_ENTRYPOINT -u CLAUDE_CODE_EXECPATH \
         -u CLAUDE_CODE_SESSION_ID -u CLAUDE_CODE_CHILD_SESSION -u CLAUDE_EFFORT \
         -u AI_AGENT -u ANTHROPIC_API_KEY {claude} --dangerously-skip-permissions \
         --strict-mcp-config --mcp-config '{{\"mcpServers\":{{}}}}' {tools} \
         --exclude-dynamic-system-prompt-sections \
         --model {model} --append-system-prompt \"$(cat {file})\"",
        claude = cfg.claude,
        tools = tools_flag,
        model = cfg.model,
        file = prompt_file.display(),
    );
    tmux(&["new-session", "-d", "-s", name, "-x", "200", "-y", "50", "-c", &cfg.workdir, &launch]).await;
    info!("clipline spawned {}", name);
    sleep(BOOT_WAIT).await;
    for _ in 0..4 {
        tmux(&["send-keys", "-t", name, "Enter"]).await;
        sleep(Duration::from_millis(1200)).await;
    }
    let sid = short_id("WARM");
    let warm = format!("The other person just said: \"Hello?\". Reply now. SENTINEL={}", sid);
    tmux(&["send-keys", "-t", name, "-l", &warm]).await;
    sleep(Duration::from_millis(500)).await;
    tmux(&["send-keys", "-t", name, "Enter"]).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let pane = capture(name, 400).await;
        if pane.matches(&sid).count() >= 2 && pane.contains("[SPK]") {
            info!("clipline {} ready", name);
            return Ok(());
        }
        if pane.contains("Login expired") || pane.contains("Please run /login") {
            return Err(anyhow!("claude session needs login, run: {}", cfg.claude));
        }
        sleep(POLL).await;
    }
    warn!("clipline {} did not warm cleanly", name);
    Ok(())
}

async fn prime_existing(name: &str, cfg: &Config) -> Result<()> {
    if !has_session(name).await {
        return Err(anyhow!(
            "tmux session '{name}' not found. Start Claude Code in it first: tmux new-session -s {name} claude, /resume your session, then run clipline"
        ));
    }
    let deadline = Instant::now() + PRESEND_IDLE_WAIT;
    while Instant::now() < deadline {
        if !spinning(&capture(name, 40).await) {
            break;
        }
        sleep(POLL).await;
    }
    let sid = short_id("WARM");
    let msg = format!(
        "You are now also on a phone call with someone.{} Acknowledge now. SENTINEL={}",
        cfg.contract(),
        sid
    );
    tmux(&["send-keys", "-t", name, "C-u"]).await;
    sleep(Duration::from_millis(50)).await;
    tmux(&["send-keys", "-t", name, "-l", &msg]).await;
    sleep(Duration::from_millis(120)).await;
    tmux(&["send-keys", "-t", name, "Enter"]).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let pane = capture(name, 400).await;
        if pane.matches(&sid).count() >= 2 && pane.contains("[SPK]") {
            info!("clipline primed existing session {}", name);
            return Ok(());
        }
        if pane.contains("Login expired") || pane.contains("Please run /login") {
            return Err(anyhow!("claude session needs login"));
        }
        sleep(POLL).await;
    }
    warn!("clipline priming of {} did not confirm", name);
    Ok(())
}

async fn ensure_session(cfg: &Config) -> Result<Arc<Session>> {
    let key = cfg
        .session
        .clone()
        .unwrap_or_else(|| session_key(&cfg.system_prompt));
    let existing = pool().lock().unwrap().get(&key).cloned();
    let session = match existing {
        Some(s) => s,
        None => {
            let s = Arc::new(Session {
                name: key.clone(),
                turn: tokio::sync::Mutex::new(()),
                ready: Mutex::new(false),
            });
            pool().lock().unwrap().insert(key.clone(), s.clone());
            s
        }
    };
    let already = *session.ready.lock().unwrap();
    if !already {
        let _guard = session.turn.lock().await;
        if !*session.ready.lock().unwrap() {
            if cfg.session.is_some() {
                prime_existing(&session.name, cfg).await?;
            } else {
                spawn_and_warm(&session.name, cfg).await?;
            }
            *session.ready.lock().unwrap() = true;
        }
    }
    Ok(session)
}

async fn send_turn(name: &str, sid: &str, last_user: &str) {
    let last = if last_user.trim().is_empty() {
        "(the call just connected, you speak first)".to_string()
    } else {
        last_user.replace(['\n', '\r'], " ")
    };
    let prompt = format!("The other person just said: \"{}\". Reply with your next spoken line now. SENTINEL={}", last, sid);
    let deadline = Instant::now() + PRESEND_IDLE_WAIT;
    while Instant::now() < deadline {
        if !spinning(&capture(name, 40).await) {
            break;
        }
        sleep(POLL).await;
    }
    tmux(&["send-keys", "-t", name, "C-u"]).await;
    sleep(Duration::from_millis(50)).await;
    tmux(&["send-keys", "-t", name, "-l", &prompt]).await;
    sleep(Duration::from_millis(120)).await;
    tmux(&["send-keys", "-t", name, "Enter"]).await;
}

fn extract_spk(pane: &str) -> Option<String> {
    let joined: String = pane.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut last = None;
    let mut rest = joined.as_str();
    while let Some(open) = rest.find("[SPK]") {
        let after = &rest[open + 5..];
        if let Some(close) = after.find("[/SPK]") {
            last = Some(after[..close].trim().to_string());
            rest = &after[close + 6..];
        } else {
            break;
        }
    }
    last.filter(|l| !l.is_empty() && l.to_lowercase() != "your spoken line")
}

async fn run_turn(session: &Session, last_user: &str) -> Result<String> {
    if !has_session(&session.name).await {
        return Err(anyhow!("claude session gone"));
    }
    let sid = short_id("VT");
    let _guard = session.turn.lock().await;
    send_turn(&session.name, &sid, last_user).await;
    let deadline = Instant::now() + MAX_WAIT;
    let mut saw_spinner = false;
    let mut retries = 0;
    let mut last_retry = Instant::now();
    while Instant::now() < deadline {
        sleep(POLL).await;
        let pane = capture(&session.name, 400).await;
        if pane.contains(MODAL) {
            return Err(anyhow!("claude session blocked on a prompt"));
        }
        if pane.contains("Login expired") {
            return Err(anyhow!("claude session needs login"));
        }
        let spin = spinning(&pane);
        saw_spinner = saw_spinner || spin;
        let count = pane.matches(&sid).count();
        if count < 2 && !spin && !saw_spinner && retries < 5 && last_retry.elapsed() > Duration::from_secs(1) {
            tmux(&["send-keys", "-t", &session.name, "Enter"]).await;
            retries += 1;
            last_retry = Instant::now();
            continue;
        }
        if count >= 2 && !spin {
            if let Some(line) = extract_spk(&pane) {
                return Ok(line);
            }
        }
    }
    Err(anyhow!("claude session timed out"))
}

pub struct ReplProvider {
    cfg: Config,
}

impl ReplProvider {
    pub fn new() -> Result<Self> {
        Ok(Self { cfg: Config::from_env() })
    }
}

#[async_trait]
impl LlmProvider for ReplProvider {
    async fn generate_response(&mut self, prompt: &str) -> Result<String> {
        let session = ensure_session(&self.cfg).await?;
        run_turn(&session, prompt).await
    }

    async fn stream_tokens(&mut self, _prompt: &str) -> Result<Vec<String>> {
        Ok(vec![])
    }

    async fn generate_response_with_context(
        &mut self,
        prompt: &str,
        _history: &VecDeque<ConversationTurn>,
    ) -> Result<String> {
        let session = ensure_session(&self.cfg).await?;
        run_turn(&session, prompt).await
    }

    fn inject_context(&mut self, context: &str) {
        self.cfg.system_prompt = format!("{}\n{}", context, self.cfg.system_prompt);
    }
}
