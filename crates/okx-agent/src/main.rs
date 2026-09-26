use std::{
    fs,
    io::{self, Read},
    path::PathBuf,
};

use clap::{Parser, Subcommand};
use okx_agent::{
    AgentResult,
    config::{AgentConfig, default_root},
    identity::{
        default_key_id, initialize_native_identity, load_native_identity, load_native_private_key,
    },
    once::process_once_now,
    runtime::run_until_shutdown,
};
use okx_protocol::MailboxEnvelope;
use zeroize::Zeroize;

#[derive(Debug, Parser)]
#[command(name = "okx-agent")]
#[command(about = "Native long-lived OKX observation agent runtime")]
struct Cli {
    #[arg(long, global = true, default_value_os_t = default_root())]
    root: PathBuf,

    #[arg(long, global = true, default_value = default_key_id())]
    key_id: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create the long-lived X25519 agent identity in the native Windows credential store.
    InitKey,

    /// Print only the public agent identity.
    Identity,

    /// Process one encrypted mailbox envelope from a file or stdin.
    Once {
        #[arg(long)]
        input: Option<PathBuf>,
    },

    /// Start the native Tokio lifecycle shell. No inbound listener is opened.
    Run,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run(Cli::parse()).await?;
    Ok(())
}

async fn run(cli: Cli) -> AgentResult<()> {
    let config = AgentConfig::new(cli.root, cli.key_id.clone());

    match cli.command {
        Command::InitKey => {
            let identity = initialize_native_identity(&config.key_id)?;
            println!("{}", serde_json::to_string_pretty(&identity)?);
        }
        Command::Identity => {
            let identity = load_native_identity(&config.key_id)?;
            println!("{}", serde_json::to_string_pretty(&identity)?);
        }
        Command::Once { input } => {
            let payload = read_input(input)?;
            let envelope: MailboxEnvelope = serde_json::from_str(&payload)?;
            let mut private_key = load_native_private_key(&config.key_id)?;
            let response = process_once_now(&envelope, &config.key_id, &private_key);
            private_key.zeroize();
            println!("{}", serde_json::to_string_pretty(&response?)?);
        }
        Command::Run => {
            let identity = load_native_identity(&config.key_id)?;
            run_until_shutdown(&config, &identity).await?;
        }
    }

    Ok(())
}

fn read_input(input: Option<PathBuf>) -> AgentResult<String> {
    match input {
        Some(path) => Ok(fs::read_to_string(path)?),
        None => {
            let mut payload = String::new();
            io::stdin().read_to_string(&mut payload)?;
            Ok(payload)
        }
    }
}
