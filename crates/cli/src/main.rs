use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand, ValueEnum};
use client::{JsonRpcClient, JwtSigner, RestClient};
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    io::{self, IsTerminal, Write},
    path::Path,
    process::Command as ProcessCommand,
    time::Instant,
};
use tokio::{
    fs,
    time::{sleep, timeout, Duration},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Parser, Debug)]
#[command(name = "boom")]
#[command(about = "High-performance Ethereum RPC benchmarking and comparison CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start an interactive CLI menu for common workflows.
    Run {
        #[arg(value_name = "RPC")]
        rpc: Option<String>,
    },
    /// Probe endpoint health, metadata, and method availability.
    Probe {
        #[arg(value_name = "RPC")]
        rpc_positional: Option<String>,
        #[arg(long)]
        rpc: Option<String>,
        #[arg(long)]
        jwt: Option<String>,
        #[arg(long, default_value = "10s")]
        timeout: String,
    },
    /// Run a single-endpoint benchmark.
    Bench(BenchArgs),
    /// Discover and catalog supported JSON-RPC methods.
    Catalog {
        #[arg(value_name = "RPC")]
        rpc_positional: Option<String>,
        #[arg(long)]
        rpc: Option<String>,
        #[arg(long)]
        jwt: Option<String>,
        #[arg(long, default_value = "10s")]
        timeout: String,
        /// Include heavier debug, trace, txpool, and engine method probes.
        #[arg(long)]
        all: bool,
        /// Extra JSON-RPC methods to probe, comma separated.
        #[arg(long)]
        methods: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Compare two endpoints with the same workload.
    Compare {
        left: String,
        right: String,
        #[arg(long, default_value = "runs/compare")]
        out: String,
        #[command(flatten)]
        bench: BenchWorkloadArgs,
        #[arg(long)]
        open: bool,
    },
    /// Probe authenticated JSON-RPC Engine API capabilities.
    Engine {
        #[arg(long)]
        rpc: String,
        #[arg(long)]
        jwt: String,
        #[arg(long, default_value = "10s")]
        timeout: String,
    },
    /// Call every built-in Engine REST/SSZ endpoint and print a compatibility table.
    EngineSszSuite {
        #[arg(long)]
        base: String,
        #[arg(long)]
        jwt: String,
        #[arg(long, default_value = "prague")]
        fork: String,
        #[arg(long, default_value = "10s")]
        timeout: String,
        #[arg(long, default_value = "application/json")]
        accept: String,
    },
    /// Call an authenticated Engine REST/SSZ endpoint from execution-apis PR #793.
    EngineSsz {
        #[arg(long)]
        base: String,
        #[arg(long)]
        jwt: String,
        #[arg(long, value_enum)]
        endpoint: EngineSszEndpoint,
        #[arg(long, default_value = "prague")]
        fork: String,
        #[arg(long)]
        payload_id: Option<String>,
        #[arg(long)]
        body: Option<String>,
        #[arg(long, default_value = "10s")]
        timeout: String,
        #[arg(long, default_value = "application/octet-stream")]
        accept: String,
    },
    /// Generate charts and readable summaries from completed run artifacts.
    Report {
        #[arg(long)]
        run: String,
        #[arg(long)]
        out: Option<String>,
        #[arg(long)]
        open: bool,
        #[arg(long)]
        print: bool,
        #[arg(long)]
        prompt: bool,
        #[arg(long)]
        no_prompt: bool,
    },
    /// Export a completed run as Prometheus/OpenMetrics text.
    Metrics {
        #[arg(long)]
        run: String,
        #[arg(long)]
        out: Option<String>,
        #[arg(long)]
        print: bool,
    },
    /// Run a benchmark with a live terminal dashboard.
    Live(BenchArgs),
    /// Benchmark JSON-RPC over WebSocket.
    WsBench {
        ws: String,
        #[arg(long, default_value = "30s")]
        duration: String,
        #[arg(long, default_value_t = 64)]
        concurrency: usize,
        #[arg(long, default_value = "10s")]
        timeout: String,
        #[arg(long, default_value = "eth_blockNumber")]
        method: String,
        #[arg(long, default_value = "[]")]
        params: String,
        #[arg(long, default_value = "runs/ws")]
        out: String,
        #[arg(long)]
        json: bool,
    },
    /// Find the highest request rate that stays inside latency and error budgets.
    FindLimit {
        rpc: String,
        #[arg(long, default_value = "runs/limit")]
        out: String,
        #[arg(long, default_value_t = 50.0)]
        start_rps: f64,
        #[arg(long, default_value_t = 5000.0)]
        max_rps: f64,
        #[arg(long, default_value_t = 1.5)]
        step: f64,
        #[arg(long, default_value_t = 250)]
        target_p95: u128,
        #[arg(long, default_value_t = 1.0)]
        max_error_rate: f64,
        #[command(flatten)]
        bench: BenchWorkloadArgs,
    },
}

#[derive(Parser, Debug)]
struct BenchArgs {
    #[arg(value_name = "RPC")]
    rpc: Option<String>,
    #[arg(long)]
    config: Option<String>,
    #[arg(long, default_value = "runs/local-001")]
    out: String,
    #[command(flatten)]
    workload: BenchWorkloadArgs,
    #[arg(long)]
    json: bool,
    #[arg(long)]
    no_prompt: bool,
}

#[derive(Parser, Debug, Clone)]
struct BenchWorkloadArgs {
    #[arg(long, default_value = "30s")]
    duration: String,
    #[arg(long, default_value = "0s")]
    warmup: String,
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    #[arg(long, default_value = "10s")]
    timeout: String,
    #[arg(long, default_value_t = 1)]
    batch_size: usize,
    #[arg(long)]
    rps: Option<f64>,
    /// Linear ramp as START:END requests/sec, for example 100:1000.
    #[arg(long)]
    ramp: Option<String>,
    #[arg(long)]
    scenario: Option<String>,
    #[arg(long)]
    eth: bool,
    #[arg(long)]
    debug: bool,
    #[arg(long)]
    trace: bool,
    #[arg(long)]
    txpool: bool,
    #[arg(long)]
    net: bool,
    #[arg(long)]
    web3: bool,
    #[arg(long)]
    all: bool,
}

#[derive(Clone, Debug, ValueEnum)]
enum EngineSszEndpoint {
    Capabilities,
    Identity,
    NewPayload,
    Forkchoice,
    GetPayload,
    BodiesByHash,
    BodiesByRange,
    BlobsV1,
    BlobsV2,
    BlobsV3,
    BlobsV4,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run { rpc } => run_menu(rpc).await?,
        Command::Probe { rpc_positional, rpc, jwt, timeout } => {
            let rpc = rpc.or(rpc_positional).ok_or_else(|| anyhow!("RPC required"))?;
            let timeout = common::parse_duration(&timeout)?;
            let mut client = JsonRpcClient::new(rpc.clone(), timeout)?;
            if let Some(jwt) = jwt {
                client = client.with_jwt(JwtSigner::from_file_or_hex(&jwt)?);
            }
            let report = discovery::probe_jsonrpc(rpc, &client).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Bench(args) => {
            let config = match args.config {
                Some(path) => common::load_config(path)?,
                None => build_config(
                    args.rpc.ok_or_else(|| anyhow!("RPC required unless --config is set"))?,
                    &args.workload,
                )?,
            };
            let out_dir = args.out.clone();
            let (target_name, rpc) = common::first_rpc_target(&config)?;
            let summary = runner::run_bench(target_name, rpc, config, args.out).await?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&summary)?);
            } else {
                println!("bench complete: {out_dir}");
                println!(
                    "requests: {} | rps: {:.2} | p95: {} ms | errors: {}",
                    summary.total_requests,
                    summary.requests_per_second,
                    summary.latency.p95_ms,
                    summary.rpc_errors + summary.transport_errors + summary.timeouts
                );
                if !args.no_prompt &&
                    prompt_yes_no_default_yes("Show detailed summary in terminal? [Y/n] ")?
                {
                    print!("{}", report::render_terminal_summary(&summary));
                }
                if !args.no_prompt &&
                    prompt_yes_no_default_yes(
                        "Generate and open HTML report in browser? [Y/n] ",
                    )?
                {
                    let artifacts = report::write_report(&out_dir, None).await?;
                    println!("html: {}", artifacts.html.display());
                    open_report(&artifacts.html)?;
                }
            }
        }
        Command::Catalog { rpc_positional, rpc, jwt, timeout, all, methods, json } => {
            let rpc = rpc.or(rpc_positional).ok_or_else(|| anyhow!("RPC required"))?;
            let timeout = common::parse_duration(&timeout)?;
            let mut client = JsonRpcClient::new(rpc.clone(), timeout)?;
            if let Some(jwt) = jwt {
                client = client.with_jwt(JwtSigner::from_file_or_hex(&jwt)?);
            }
            let extra = methods
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|method| !method.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            let catalog = discovery::catalog_jsonrpc(rpc, &client, all, extra).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&catalog)?);
            } else {
                println!("method,status,latency_ms,detail");
                for method in catalog.methods {
                    println!(
                        "{},{:?},{},{}",
                        method.method,
                        method.status,
                        method.latency_ms,
                        method.detail.unwrap_or_default().replace(',', ";")
                    );
                }
            }
        }
        Command::Compare { left, right, out, bench, open } => {
            let left_dir = format!("{out}/left");
            let right_dir = format!("{out}/right");
            let left_config = build_config(left, &bench)?;
            let right_config = build_config(right, &bench)?;
            let (left_name, left_rpc) = common::first_rpc_target(&left_config)?;
            let (right_name, right_rpc) = common::first_rpc_target(&right_config)?;
            let left_summary =
                runner::run_bench(left_name, left_rpc, left_config, &left_dir).await?;
            let right_summary =
                runner::run_bench(right_name, right_rpc, right_config, &right_dir).await?;
            let report = compare::build_compare(left_summary, right_summary);
            let artifacts = compare::write_compare(&report, &out)?;
            println!("compare: {}", artifacts.markdown.display());
            println!("html: {}", artifacts.html.display());
            if open {
                open_report(&artifacts.html)?;
            }
        }
        Command::Engine { rpc, jwt, timeout } => {
            let timeout = common::parse_duration(&timeout)?;
            let client =
                JsonRpcClient::new(rpc, timeout)?.with_jwt(JwtSigner::from_file_or_hex(&jwt)?);
            let capabilities = discovery::engine_exchange_capabilities(&client).await?;
            println!("{}", serde_json::to_string_pretty(&capabilities)?);
        }
        Command::EngineSszSuite { base, jwt, fork, timeout, accept } => {
            let timeout = common::parse_duration(&timeout)?;
            let client =
                RestClient::new(base, timeout)?.with_jwt(JwtSigner::from_file_or_hex(&jwt)?);
            let endpoints = [
                EngineSszEndpoint::Capabilities,
                EngineSszEndpoint::Identity,
                EngineSszEndpoint::NewPayload,
                EngineSszEndpoint::Forkchoice,
                EngineSszEndpoint::GetPayload,
                EngineSszEndpoint::BodiesByHash,
                EngineSszEndpoint::BodiesByRange,
                EngineSszEndpoint::BlobsV1,
                EngineSszEndpoint::BlobsV2,
                EngineSszEndpoint::BlobsV3,
                EngineSszEndpoint::BlobsV4,
            ];
            println!("endpoint,status,content_type,bytes");
            for endpoint in endpoints {
                let name = format!("{endpoint:?}");
                match call_engine_ssz(&client, endpoint, &fork, None, &accept, Vec::new()).await {
                    Ok(response) => println!(
                        "{},{},{},{}",
                        name,
                        response.status,
                        response.content_type.unwrap_or_default(),
                        response.bytes.len()
                    ),
                    Err(error) => println!("{name},error,,{}", error.to_string().replace(',', ";")),
                }
            }
        }
        Command::EngineSsz { base, jwt, endpoint, fork, payload_id, body, timeout, accept } => {
            let timeout = common::parse_duration(&timeout)?;
            let client =
                RestClient::new(base, timeout)?.with_jwt(JwtSigner::from_file_or_hex(&jwt)?);
            let request_body = match body {
                Some(path) => fs::read(path).await?,
                None => Vec::new(),
            };
            let response = call_engine_ssz(
                &client,
                endpoint,
                &fork,
                payload_id.as_deref(),
                &accept,
                request_body,
            )
            .await?;
            println!("status: {}", response.status);
            if let Some(content_type) = response.content_type {
                println!("content-type: {content_type}");
            }
            println!("bytes: {}", response.bytes.len());
            if let Ok(text) = std::str::from_utf8(&response.bytes) {
                if !text.trim().is_empty() {
                    println!("{text}");
                }
            }
        }
        Command::Report { run, out, open, print, prompt, no_prompt } => {
            let interactive = prompt || (!no_prompt && !open && !print && is_interactive());
            let artifacts = report::write_report(&run, out).await?;
            println!("summary: {}", artifacts.summary_md.display());
            println!("html: {}", artifacts.html.display());
            let print = print ||
                (interactive && prompt_yes_no_default_yes("Show summary in terminal? [Y/n] ")?);
            if print {
                let markdown = std::fs::read_to_string(&artifacts.summary_md)?;
                println!("\n{markdown}");
            }
            let open = open ||
                (interactive &&
                    prompt_yes_no_default_yes("Open HTML report in browser? [Y/n] ")?);
            if open {
                open_report(&artifacts.html)?;
            }
        }
        Command::Metrics { run, out, print } => {
            let metrics = report::write_openmetrics(&run, out)?;
            println!("metrics: {}", metrics.display());
            if print {
                println!("{}", std::fs::read_to_string(metrics)?);
            }
        }
        Command::Live(args) => run_live(args).await?,
        Command::WsBench { ws, duration, concurrency, timeout, method, params, out, json } => {
            let params: Value = serde_json::from_str(&params)?;
            let summary =
                run_ws_bench(ws, duration, concurrency, timeout, method, params, &out).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&summary)?);
            } else {
                println!("websocket bench complete: {out}");
                println!(
                    "requests: {} | rps: {:.2} | p95: {} ms | errors: {}",
                    summary.total_requests,
                    summary.requests_per_second,
                    summary.latency.p95_ms,
                    summary.rpc_errors + summary.transport_errors + summary.timeouts
                );
            }
        }
        Command::FindLimit {
            rpc,
            out,
            start_rps,
            max_rps,
            step,
            target_p95,
            max_error_rate,
            bench,
        } => {
            let result = run_find_limit(LimitOptions {
                rpc,
                out,
                start_rps,
                max_rps,
                step,
                target_p95,
                max_error_rate,
                bench,
            })
            .await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
    }
    Ok(())
}

async fn run_menu(initial_rpc: Option<String>) -> Result<()> {
    if !is_interactive() {
        return Err(anyhow!("boom run requires an interactive terminal"));
    }

    println!("{}", cyan("boom"));
    println!("{}", dim("interactive RPC operator menu"));
    println!();
    println!("{} Probe endpoint health", green("1"));
    println!("{} Catalog supported methods", green("2"));
    println!("{} Run ETH benchmark", green("3"));
    println!("{} Run comprehensive benchmark", green("4"));
    println!("{} Open/generate report", green("5"));
    println!("{} Find saturation limit", green("6"));
    println!("{} WebSocket benchmark", green("7"));
    println!();

    let choice = prompt_line("Choose workflow [1-7]: ")?;
    match choice.trim() {
        "1" => {
            let rpc = initial_rpc
                .or_else(|| prompt_optional("RPC URL: "))
                .ok_or_else(|| anyhow!("RPC required"))?;
            let timeout = common::parse_duration("10s")?;
            let client = JsonRpcClient::new(rpc.clone(), timeout)?;
            let probe = discovery::probe_jsonrpc(rpc, &client).await?;
            println!("{}", serde_json::to_string_pretty(&probe)?);
        }
        "2" => {
            let rpc = initial_rpc
                .or_else(|| prompt_optional("RPC URL: "))
                .ok_or_else(|| anyhow!("RPC required"))?;
            let all = prompt_yes_no_default_yes("Include extended catalog? [Y/n] ")?;
            let timeout = common::parse_duration("10s")?;
            let client = JsonRpcClient::new(rpc.clone(), timeout)?;
            let catalog = discovery::catalog_jsonrpc(rpc, &client, all, Vec::new()).await?;
            println!("{}", serde_json::to_string_pretty(&catalog)?);
        }
        "3" | "4" => {
            let rpc = initial_rpc
                .or_else(|| prompt_optional("RPC URL: "))
                .ok_or_else(|| anyhow!("RPC required"))?;
            let duration = prompt_with_default("Duration", "30s")?;
            let concurrency = prompt_with_default("Concurrency", "64")?.parse()?;
            let rps = prompt_optional("RPS limit, blank for open loop: ")
                .map(|value| value.parse())
                .transpose()?;
            let out = prompt_with_default("Output dir", "runs/menu")?;
            let mut workload = BenchWorkloadArgs {
                duration,
                warmup: "0s".to_string(),
                concurrency,
                timeout: "10s".to_string(),
                batch_size: 1,
                rps,
                ramp: None,
                scenario: None,
                eth: choice.trim() == "3",
                debug: false,
                trace: false,
                txpool: false,
                net: false,
                web3: false,
                all: choice.trim() == "4",
            };
            if choice.trim() == "3" {
                workload.eth = true;
            }
            let config = build_config(rpc, &workload)?;
            let (target_name, target_rpc) = common::first_rpc_target(&config)?;
            let summary = runner::run_bench(target_name, target_rpc, config, &out).await?;
            println!("{}", green("benchmark complete"));
            print!("{}", report::render_terminal_summary(&summary));
            if prompt_yes_no_default_yes("Generate and open HTML report? [Y/n] ")? {
                let artifacts = report::write_report(&out, None).await?;
                println!("html: {}", artifacts.html.display());
                open_report(&artifacts.html)?;
            }
        }
        "5" => {
            let run = prompt_with_default("Run dir", "runs/local-001")?;
            let artifacts = report::write_report(&run, None).await?;
            println!("summary: {}", artifacts.summary_md.display());
            println!("html: {}", artifacts.html.display());
            if prompt_yes_no_default_yes("Show summary in terminal? [Y/n] ")? {
                println!("{}", std::fs::read_to_string(&artifacts.summary_md)?);
            }
            if prompt_yes_no_default_yes("Open HTML report? [Y/n] ")? {
                open_report(&artifacts.html)?;
            }
        }
        "6" => {
            let rpc = initial_rpc
                .or_else(|| prompt_optional("RPC URL: "))
                .ok_or_else(|| anyhow!("RPC required"))?;
            let out = prompt_with_default("Output dir", "runs/limit")?;
            let bench = BenchWorkloadArgs {
                duration: prompt_with_default("Attempt duration", "30s")?,
                warmup: "0s".to_string(),
                concurrency: prompt_with_default("Concurrency", "64")?.parse()?,
                timeout: "10s".to_string(),
                batch_size: 1,
                rps: None,
                ramp: None,
                scenario: Some(prompt_with_default("Scenario", "explorer")?),
                eth: false,
                debug: false,
                trace: false,
                txpool: false,
                net: false,
                web3: false,
                all: false,
            };
            let result = run_find_limit(LimitOptions {
                rpc,
                out,
                start_rps: prompt_with_default("Start RPS", "50")?.parse()?,
                max_rps: prompt_with_default("Max RPS", "1000")?.parse()?,
                step: prompt_with_default("Step", "1.5")?.parse()?,
                target_p95: prompt_with_default("Target p95 ms", "250")?.parse()?,
                max_error_rate: prompt_with_default("Max error rate %", "1")?.parse()?,
                bench,
            })
            .await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        "7" => {
            let ws = prompt_with_default("WebSocket URL", "ws://localhost:8546")?;
            let out = prompt_with_default("Output dir", "runs/ws")?;
            let summary = run_ws_bench(
                ws,
                prompt_with_default("Duration", "30s")?,
                prompt_with_default("Concurrency", "64")?.parse()?,
                "10s".to_string(),
                prompt_with_default("Method", "eth_blockNumber")?,
                Value::Array(Vec::new()),
                &out,
            )
            .await?;
            print!("{}", report::render_terminal_summary(&summary));
        }
        other => return Err(anyhow!("unknown menu choice '{other}'")),
    }
    Ok(())
}

async fn run_live(args: BenchArgs) -> Result<()> {
    let config = match args.config {
        Some(path) => common::load_config(path)?,
        None => build_config(
            args.rpc.ok_or_else(|| anyhow!("RPC required unless --config is set"))?,
            &args.workload,
        )?,
    };
    let out_dir = args.out.clone();
    let (target_name, rpc) = common::first_rpc_target(&config)?;
    let started = Instant::now();
    let handle = tokio::spawn(runner::run_bench(target_name, rpc, config, out_dir.clone()));
    loop {
        if handle.is_finished() {
            break;
        }
        print!(
            "\rboom live | elapsed {:>4}s | writing {} | press Ctrl-C to stop",
            started.elapsed().as_secs(),
            out_dir
        );
        io::stdout().flush()?;
        sleep(Duration::from_secs(1)).await;
    }
    println!();
    let summary = handle.await??;
    print!("{}", report::render_terminal_summary(&summary));
    let artifacts = report::write_report(&out_dir, None).await?;
    println!("summary: {}", artifacts.summary_md.display());
    println!("html: {}", artifacts.html.display());
    Ok(())
}

#[derive(Debug, Serialize)]
struct LimitAttempt {
    rps: f64,
    out: String,
    observed_rps: f64,
    p95_ms: u128,
    error_rate: f64,
    passed: bool,
}

#[derive(Debug, Serialize)]
struct LimitResult {
    rpc: String,
    target_p95_ms: u128,
    max_error_rate: f64,
    best_rps: Option<f64>,
    breaking_rps: Option<f64>,
    attempts: Vec<LimitAttempt>,
}

struct LimitOptions {
    rpc: String,
    out: String,
    start_rps: f64,
    max_rps: f64,
    step: f64,
    target_p95: u128,
    max_error_rate: f64,
    bench: BenchWorkloadArgs,
}

async fn run_find_limit(options: LimitOptions) -> Result<LimitResult> {
    let LimitOptions { rpc, out, start_rps, max_rps, step, target_p95, max_error_rate, mut bench } =
        options;
    anyhow::ensure!(start_rps > 0.0, "--start-rps must be greater than zero");
    anyhow::ensure!(max_rps >= start_rps, "--max-rps must be >= --start-rps");
    anyhow::ensure!(step > 1.0, "--step must be greater than 1.0");
    fs::create_dir_all(&out).await?;
    let mut attempts = Vec::new();
    let mut best_rps = None;
    let mut breaking_rps = None;
    let mut current = start_rps;
    while current <= max_rps {
        bench.rps = Some(current);
        bench.ramp = None;
        let config = build_config(rpc.clone(), &bench)?;
        let attempt_dir = format!("{out}/rps-{current:.0}");
        let (target_name, target_rpc) = common::first_rpc_target(&config)?;
        let summary = runner::run_bench(target_name, target_rpc, config, &attempt_dir).await?;
        let errors = summary.rpc_errors + summary.transport_errors + summary.timeouts;
        let error_rate = if summary.total_requests == 0 {
            100.0
        } else {
            errors as f64 / summary.total_requests as f64 * 100.0
        };
        let passed = summary.latency.p95_ms <= target_p95 && error_rate <= max_error_rate;
        attempts.push(LimitAttempt {
            rps: current,
            out: attempt_dir,
            observed_rps: summary.requests_per_second,
            p95_ms: summary.latency.p95_ms,
            error_rate,
            passed,
        });
        if passed {
            best_rps = Some(current);
            current *= step;
        } else {
            breaking_rps = Some(current);
            break;
        }
    }
    let result = LimitResult {
        rpc,
        target_p95_ms: target_p95,
        max_error_rate,
        best_rps,
        breaking_rps,
        attempts,
    };
    let json_path = format!("{out}/limit.json");
    fs::write(&json_path, serde_json::to_vec_pretty(&result)?).await?;
    fs::write(format!("{out}/limit.md"), render_limit_markdown(&result)).await?;
    Ok(result)
}

fn render_limit_markdown(result: &LimitResult) -> String {
    let mut out = String::new();
    out.push_str("# boom saturation result\n\n");
    out.push_str(&format!("- rpc: `{}`\n", result.rpc));
    out.push_str(&format!("- latency budget: p95 <= {} ms\n", result.target_p95_ms));
    out.push_str(&format!("- error budget: <= {:.2}%\n", result.max_error_rate));
    out.push_str(&format!("- best rps: {:?}\n", result.best_rps));
    out.push_str(&format!("- breaking rps: {:?}\n\n", result.breaking_rps));
    out.push_str("| target rps | observed rps | p95 | error rate | result | artifact |\n");
    out.push_str("|---:|---:|---:|---:|---|---|\n");
    for attempt in &result.attempts {
        out.push_str(&format!(
            "| {:.2} | {:.2} | {} ms | {:.2}% | {} | `{}` |\n",
            attempt.rps,
            attempt.observed_rps,
            attempt.p95_ms,
            attempt.error_rate,
            if attempt.passed { "pass" } else { "break" },
            attempt.out
        ));
    }
    out
}

async fn run_ws_bench(
    ws: String,
    duration: String,
    concurrency: usize,
    timeout_arg: String,
    method: String,
    params: Value,
    out: &str,
) -> Result<common::BenchSummary> {
    let duration = common::parse_duration(&duration)?;
    let request_timeout = common::parse_duration(&timeout_arg)?;
    let deadline = Instant::now() + duration;
    let started = Instant::now();
    let stats = std::sync::Arc::new(std::sync::Mutex::new(WsStats::default()));
    let tasks = (0..concurrency.max(1)).map(|worker| {
        let ws = ws.clone();
        let method = method.clone();
        let params = params.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            let Ok((mut socket, _)) = connect_async(&ws).await else {
                let mut stats = stats.lock().expect("ws stats mutex poisoned");
                stats.transport_errors += 1;
                return Result::<()>::Ok(());
            };
            let mut id = worker as u64;
            while Instant::now() < deadline {
                id += concurrency as u64;
                let request = common::JsonRpcRequest {
                    jsonrpc: "2.0",
                    id,
                    method: method.clone(),
                    params: params.clone(),
                };
                let started = Instant::now();
                let send = socket.send(Message::Text(serde_json::to_string(&request)?)).await;
                if let Err(error) = send {
                    record_ws_transport(&stats, started.elapsed().as_millis(), error.to_string());
                    continue;
                }
                let response = timeout(request_timeout, socket.next()).await;
                let latency = started.elapsed().as_millis();
                match response {
                    Ok(Some(Ok(Message::Text(text)))) => {
                        match serde_json::from_str::<common::JsonRpcResponse>(&text) {
                            Ok(response) if response.error.is_none() => {
                                record_ws_success(&stats, latency)
                            }
                            Ok(response) => record_ws_rpc_error(
                                &stats,
                                latency,
                                response.error.map(|e| e.message).unwrap_or_default(),
                            ),
                            Err(error) => record_ws_transport(&stats, latency, error.to_string()),
                        }
                    }
                    Ok(Some(Ok(Message::Binary(bytes)))) => {
                        match serde_json::from_slice::<common::JsonRpcResponse>(&bytes) {
                            Ok(response) if response.error.is_none() => {
                                record_ws_success(&stats, latency)
                            }
                            Ok(response) => record_ws_rpc_error(
                                &stats,
                                latency,
                                response.error.map(|e| e.message).unwrap_or_default(),
                            ),
                            Err(error) => record_ws_transport(&stats, latency, error.to_string()),
                        }
                    }
                    Ok(Some(Ok(_))) => record_ws_transport(
                        &stats,
                        latency,
                        "unexpected websocket message".to_string(),
                    ),
                    Ok(Some(Err(error))) => record_ws_transport(&stats, latency, error.to_string()),
                    Ok(None) => {
                        record_ws_transport(&stats, latency, "websocket closed".to_string())
                    }
                    Err(_) => record_ws_timeout(&stats, latency),
                }
            }
            Result::<()>::Ok(())
        })
    });
    for task in futures::future::join_all(tasks).await {
        task??;
    }
    let summary = build_ws_summary(ws, method, started.elapsed().as_millis(), stats);
    write_ws_artifacts(out, &summary).await?;
    Ok(summary)
}

#[derive(Default)]
struct WsStats {
    successes: u64,
    rpc_errors: u64,
    transport_errors: u64,
    timeouts: u64,
    latencies: Vec<u128>,
}

fn record_ws_success(stats: &std::sync::Arc<std::sync::Mutex<WsStats>>, latency: u128) {
    let mut stats = stats.lock().expect("ws stats mutex poisoned");
    stats.successes += 1;
    stats.latencies.push(latency);
}

fn record_ws_rpc_error(
    stats: &std::sync::Arc<std::sync::Mutex<WsStats>>,
    latency: u128,
    _detail: String,
) {
    let mut stats = stats.lock().expect("ws stats mutex poisoned");
    stats.rpc_errors += 1;
    stats.latencies.push(latency);
}

fn record_ws_transport(
    stats: &std::sync::Arc<std::sync::Mutex<WsStats>>,
    latency: u128,
    _detail: String,
) {
    let mut stats = stats.lock().expect("ws stats mutex poisoned");
    stats.transport_errors += 1;
    stats.latencies.push(latency);
}

fn record_ws_timeout(stats: &std::sync::Arc<std::sync::Mutex<WsStats>>, latency: u128) {
    let mut stats = stats.lock().expect("ws stats mutex poisoned");
    stats.timeouts += 1;
    stats.latencies.push(latency);
}

fn build_ws_summary(
    target: String,
    method: String,
    duration_ms: u128,
    stats: std::sync::Arc<std::sync::Mutex<WsStats>>,
) -> common::BenchSummary {
    let stats = stats.lock().expect("ws stats mutex poisoned");
    let total = stats.successes + stats.rpc_errors + stats.transport_errors + stats.timeouts;
    let mut latencies = stats.latencies.clone();
    let latency = common::summarize_latencies(&mut latencies);
    let mut methods = BTreeMap::new();
    methods.insert(
        method,
        common::MethodSummary {
            requests: total,
            successes: stats.successes,
            errors: stats.rpc_errors + stats.transport_errors + stats.timeouts,
            p50_ms: latency.p50_ms,
            p90_ms: latency.p90_ms,
            p95_ms: latency.p95_ms,
            p99_ms: latency.p99_ms,
        },
    );
    common::BenchSummary {
        target,
        duration_ms,
        total_requests: total,
        successes: stats.successes,
        rpc_errors: stats.rpc_errors,
        transport_errors: stats.transport_errors,
        timeouts: stats.timeouts,
        requests_per_second: total as f64 / (duration_ms as f64 / 1000.0).max(0.001),
        latency,
        histogram: common::latency_histogram(&stats.latencies),
        samples: Vec::new(),
        methods,
    }
}

async fn write_ws_artifacts(out: &str, summary: &common::BenchSummary) -> Result<()> {
    fs::create_dir_all(out).await?;
    fs::write(format!("{out}/run.json"), serde_json::to_vec_pretty(summary)?).await?;
    fs::write(format!("{out}/summary.md"), report::render_markdown(summary)).await?;
    fs::write(format!("{out}/openmetrics.prom"), report::render_openmetrics(summary)).await?;
    Ok(())
}

fn build_config(rpc: String, args: &BenchWorkloadArgs) -> Result<common::Config> {
    let bench = common::BenchConfig {
        duration: args.duration.clone(),
        warmup: Some(args.warmup.clone()),
        concurrency: args.concurrency,
        timeout: args.timeout.clone(),
        batch_size: args.batch_size,
        seed: None,
        rps: args.rps,
        ramp: args.ramp.clone(),
    };
    let workload = if let Some(scenario) = &args.scenario {
        common::scenario_workload(scenario)?
    } else {
        common::workload_presets(
            args.eth,
            args.debug,
            args.trace,
            args.txpool,
            args.net,
            args.web3,
            args.all,
        )
    };
    Ok(common::config_for_rpc(rpc, bench, workload))
}

async fn call_engine_ssz(
    client: &RestClient,
    endpoint: EngineSszEndpoint,
    fork: &str,
    payload_id: Option<&str>,
    accept: &str,
    body: Vec<u8>,
) -> Result<client::RestResponse> {
    let ssz = Some("application/octet-stream");
    let accept = Some(accept);
    Ok(match endpoint {
        EngineSszEndpoint::Capabilities => client.get("/capabilities", accept).await?,
        EngineSszEndpoint::Identity => client.get("/identity", accept).await?,
        EngineSszEndpoint::NewPayload => {
            client.post(&format!("/{fork}/payloads"), ssz, accept, body).await?
        }
        EngineSszEndpoint::Forkchoice => {
            client.post(&format!("/{fork}/forkchoice"), ssz, accept, body).await?
        }
        EngineSszEndpoint::GetPayload => {
            let payload_id = payload_id.unwrap_or("0x0000000000000000");
            client.get(&format!("/{fork}/payloads/{payload_id}"), accept).await?
        }
        EngineSszEndpoint::BodiesByHash => {
            client.post(&format!("/{fork}/bodies/hash"), ssz, accept, body).await?
        }
        EngineSszEndpoint::BodiesByRange => {
            client.get(&format!("/{fork}/bodies?from=1&count=1"), accept).await?
        }
        EngineSszEndpoint::BlobsV1 => client.post("/blobs/v1", ssz, accept, body).await?,
        EngineSszEndpoint::BlobsV2 => client.post("/blobs/v2", ssz, accept, body).await?,
        EngineSszEndpoint::BlobsV3 => client.post("/blobs/v3", ssz, accept, body).await?,
        EngineSszEndpoint::BlobsV4 => client.post("/blobs/v4", ssz, accept, body).await?,
    })
}

fn open_report(path: &Path) -> Result<()> {
    #[cfg(target_os = "windows")]
    {
        ProcessCommand::new("cmd")
            .args(["/C", "start", "", &path.display().to_string()])
            .spawn()?;
    }
    #[cfg(target_os = "macos")]
    {
        ProcessCommand::new("open").arg(path).spawn()?;
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        ProcessCommand::new("xdg-open").arg(path).spawn()?;
    }
    Ok(())
}

fn prompt_yes_no_default_yes(prompt: &str) -> Result<bool> {
    if !is_interactive() {
        return Ok(false);
    }
    print!("{}", yellow(prompt));
    io::stdout().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    Ok(!matches!(answer.trim(), "n" | "N" | "no" | "NO" | "No"))
}

fn prompt_line(prompt: &str) -> Result<String> {
    print!("{}", cyan(prompt));
    io::stdout().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    Ok(answer.trim().to_string())
}

fn prompt_optional(prompt: &str) -> Option<String> {
    prompt_line(prompt).ok().filter(|value| !value.trim().is_empty())
}

fn prompt_with_default(label: &str, default: &str) -> Result<String> {
    let value = prompt_line(&format!("{label} [{default}]: "))?;
    if value.trim().is_empty() {
        Ok(default.to_string())
    } else {
        Ok(value)
    }
}

fn is_interactive() -> bool {
    io::stdin().is_terminal() && io::stdout().is_terminal()
}

fn cyan(input: &str) -> String {
    format!("\x1b[36m{input}\x1b[0m")
}

fn green(input: &str) -> String {
    format!("\x1b[32m{input}\x1b[0m")
}

fn yellow(input: &str) -> String {
    format!("\x1b[33m{input}\x1b[0m")
}

fn dim(input: &str) -> String {
    format!("\x1b[2m{input}\x1b[0m")
}
