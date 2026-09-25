use std::str::FromStr;

use clap::Parser;
use okx_api::{
    AccountApi, Credentials, MarginMode, OkxEnvironment, OkxRestClient, Region,
};

#[derive(Debug, Parser)]
#[command(name = "okx-capabilities")]
#[command(about = "Read-only probe of actual OKX account capabilities")]
struct Args {
    #[arg(long, default_value = "global")]
    region: String,

    #[arg(long)]
    demo: bool,

    #[arg(long, default_value = "BTC-USDT-SWAP")]
    instrument: String,

    #[arg(long, default_value = "cross")]
    margin: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let region = Region::from_str(&args.region)?;
    let margin = MarginMode::from_str(&args.margin)?;
    let credentials = Credentials::from_env()?;

    let client = OkxRestClient::new(OkxEnvironment::new(region, args.demo), credentials)?;
    let account = AccountApi::new(client);
    let capabilities = account
        .probe_capabilities(&args.instrument, margin)
        .await?;

    println!("{}", serde_json::to_string_pretty(&capabilities)?);
    Ok(())
}
