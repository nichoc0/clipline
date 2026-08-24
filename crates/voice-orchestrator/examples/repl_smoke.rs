use std::collections::VecDeque;
use voice_orchestrator::providers::repl::ReplProvider;
use voice_orchestrator::stages::llm::LlmProvider;

#[tokio::main]
async fn main() {
    let mut p = ReplProvider::new().expect("new");
    let g = p.generate_response("(call connected)").await;
    println!("greeting: {:?}", g);
    let h = VecDeque::new();
    for q in ["How many rust files are here?", "And how many toml files?", "What's in the README, one line?"] {
        let t = std::time::Instant::now();
        let r = p.generate_response_with_context(q, &h).await;
        println!("[{:?}] {:?} -> {:?}", t.elapsed(), q, r);
    }
}
