use std::{str::FromStr, time::{Duration, Instant}};

use clap::Parser;
use okx_api::{AccountApi, Credentials, MarginMode, OkxEnvironment, OkxRestClient, Region, TradeApi};

#[derive(Debug, Parser)]
#[command(name = "okx-capabilities")]
#[command(about = "Read-only probe of actual OKX account capabilities")]
struct Args {
    #[arg(long, default_value = "global")]
    region: String,

    #[arg(long)]
    demo: bool,

    #[arg(long, help = "Read authenticated opening-risk inventory using GET only; no trading")]
    opening_inventory: bool,

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
    if args.opening_inventory {
        // No broad "account flat" claim: all returned position rows are blockers,
        // including zero-size rows. This is an authenticated, bounded derivative
        // position/order/algo census; any missing or saturated source fails closed.
        let account = AccountApi::new(client.clone());
        let positions = account.positions().await?;
        let orders = account.pending_orders().await?;
        let algos = TradeApi::new(client)
            .pending_derivative_risk_algos_until(Instant::now() + Duration::from_secs(30))
            .await?;
        let complete = algos.complete_within_bound && algos.scopes_checked == 16;
        let empty = positions.is_empty() && orders.is_empty() && algos.rows == 0;
        let state = if !complete {
            "INCOMPLETE"
        } else if empty {
            "DERIVATIVES_EXPOSURE_EMPTY"
        } else {
            "EXPOSURE_PRESENT"
        };
        // No UID, key, instrument, order ID, financial amounts or samples.
        println!("{}", serde_json::json!({
            "schema": "okx.readonly-opening-inventory/v1",
            "state": state,
            "complete": complete,
            "position_rows": positions.len(),
            "pending_order_rows": orders.len(),
            "pending_derivative_algo_rows": algos.rows,
            "algo_scopes_checked": algos.scopes_checked,
            "algo_received_at_ms": algos.received_at_ms,
            "authenticated_reads_only": true
        }));
        return Ok(());
    }
    let account = AccountApi::new(client);
    let capabilities = account.probe_capabilities(&args.instrument, margin).await?;

    println!("{}", serde_json::to_string_pretty(&capabilities)?);
    Ok(())
}
