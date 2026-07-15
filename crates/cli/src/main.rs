use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use client::{JsonRpcClient, JwtSigner, RestClient};
use futures::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    io::{self, IsTerminal, Write},
    path::Path,
    process::Command as ProcessCommand,
    time::Instant,
};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    time::{sleep, timeout, Duration},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Parser, Debug)]
#[command(name = "boom")]
#[command(version)]
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
    /// Compare a completed run against a saved baseline and fail on regression.
    Gate {
        #[arg(long)]
        baseline: String,
        #[arg(long)]
        run: String,
        #[arg(long)]
        out: Option<String>,
        #[arg(long, default_value_t = 10.0)]
        max_p95_regression: f64,
        #[arg(long, default_value_t = 1.0)]
        max_error_rate_delta: f64,
        #[arg(long, default_value_t = 0.95)]
        min_throughput_ratio: f64,
        #[arg(long)]
        json: bool,
    },
    /// Execute a configured multi-step JSON-RPC scenario.
    Scenario {
        #[arg(long)]
        config: String,
        #[arg(long)]
        scenario: String,
        #[arg(long)]
        target: Option<String>,
        #[arg(long, default_value = "runs/scenario")]
        out: String,
        #[arg(long)]
        iterations: Option<usize>,
        #[arg(long)]
        json: bool,
    },
    /// Validate a TOML benchmark config without sending requests.
    ConfigCheck {
        #[arg(long)]
        config: String,
        #[arg(long)]
        json: bool,
    },
    /// Serve a completed run as a Prometheus scrape endpoint for Grafana.
    ServeMetrics {
        #[arg(long)]
        run: String,
        #[arg(long, default_value = "127.0.0.1:9464")]
        listen: String,
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
        /// Minimum percentage of the requested RPS that must actually be delivered.
        #[arg(long, default_value_t = 95.0)]
        min_achieved_rate: f64,
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
    /// Named target to use when a config contains more than one RPC target.
    #[arg(long)]
    target: Option<String>,
    #[arg(long, default_value = "runs/local-001")]
    out: String,
    #[command(flatten)]
    workload: BenchWorkloadArgs,
    #[arg(long)]
    json: bool,
    #[arg(long)]
    no_prompt: bool,
    /// Validate and print the plan without sending traffic.
    #[arg(long)]
    dry_run: bool,
    /// Bind a live Prometheus endpoint while this benchmark is running.
    #[arg(long)]
    live_metrics: Option<String>,
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
    #[arg(long, conflicts_with = "ramp")]
    rps: Option<f64>,
    /// Linear ramp as START:END requests/sec, for example 100:1000.
    #[arg(long, conflicts_with = "rps")]
    ramp: Option<String>,
    /// Hard request budget for this run.
    #[arg(long)]
    max_requests: Option<u64>,
    /// Hard duration cap for this run.
    #[arg(long)]
    max_duration: Option<String>,
    /// Hard cap for fixed or ramped request rates.
    #[arg(long)]
    max_rps: Option<f64>,
    /// Explicitly permit traffic to public/non-private endpoints.
    #[arg(long)]
    allow_public: bool,
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
            let mut config = match args.config.as_deref() {
                Some(path) => common::load_config(path)?,
                None => build_config(
                    args.rpc
                        .clone()
                        .ok_or_else(|| anyhow!("RPC required unless --config is set"))?,
                    &args.workload,
                )?,
            };
            let out_dir = args.out.clone();
            if args.dry_run || config.bench.dry_run {
                let plan = json!({
                    "dry_run": true,
                    "targets": config.targets.keys().collect::<Vec<_>>(),
                    "duration": config.bench.duration,
                    "warmup": config.bench.warmup,
                    "concurrency": config.bench.concurrency,
                    "batch_size": config.bench.batch_size,
                    "rps": config.bench.rps,
                    "ramp": config.bench.ramp,
                    "max_requests": config.bench.max_requests,
                    "max_duration": config.bench.max_duration,
                    "max_rps": config.bench.max_rps,
                    "allow_public": config.bench.allow_public,
                    "methods": config.json_rpc.keys().collect::<Vec<_>>(),
                });
                if args.json {
                    println!("{}", serde_json::to_string_pretty(&plan)?);
                } else {
                    println!("dry run: no requests sent\n{}", serde_json::to_string_pretty(&plan)?);
                }
                return Ok(());
            }
            let (target_name, rpc) = common::rpc_target(&config, args.target.as_deref())?;
            if !config.bench.allow_public && !runner::is_private_endpoint(&rpc) {
                anyhow::ensure!(
                    !args.no_prompt && is_interactive(),
                    "refusing public endpoint {rpc}; rerun interactively to confirm or pass --allow-public"
                );
                anyhow::ensure!(
                    prompt_yes_no_default_yes(&format!(
                        "Endpoint {rpc} is not private. Send benchmark traffic? [Y/n] "
                    ))?,
                    "public endpoint benchmark cancelled"
                );
                config.bench.allow_public = true;
            }
            let live = args
                .live_metrics
                .as_ref()
                .map(|_| runner::LiveMetrics::new(target_name.clone(), &config))
                .transpose()?;
            let (shutdown, server) =
                if let (Some(listen), Some(live)) = (args.live_metrics.as_deref(), live.clone()) {
                    let listener = TcpListener::bind(listen).await?;
                    println!("live Prometheus metrics at http://{listen}/metrics");
                    let (shutdown, receiver) = oneshot::channel();
                    let server = tokio::spawn(serve_live_metrics(listener, live, receiver));
                    (Some(shutdown), Some(server))
                } else {
                    (None, None)
                };
            let result =
                runner::run_bench_with_live(target_name, rpc, config, args.out, live).await;
            if let Some(shutdown) = shutdown {
                let _ = shutdown.send(());
            }
            if let Some(server) = server {
                server.await??;
            }
            let summary = result?;
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
            let mut left_config = build_config(left, &bench)?;
            let mut right_config = build_config(right, &bench)?;
            left_config.targets.get_mut("target").expect("generated target").label =
                Some("left".to_string());
            right_config.targets.get_mut("target").expect("generated target").label =
                Some("right".to_string());
            let (left_name, left_rpc) = common::first_rpc_target(&left_config)?;
            let (right_name, right_rpc) = common::first_rpc_target(&right_config)?;
            let (left_summary, right_summary) = tokio::try_join!(
                runner::run_bench(left_name, left_rpc, left_config, &left_dir),
                runner::run_bench(right_name, right_rpc, right_config, &right_dir),
            )?;
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
        Command::Gate {
            baseline,
            run,
            out,
            max_p95_regression,
            max_error_rate_delta,
            min_throughput_ratio,
            json,
        } => {
            anyhow::ensure!(
                max_p95_regression.is_finite() && max_p95_regression >= 0.0,
                "--max-p95-regression must be finite and non-negative"
            );
            anyhow::ensure!(
                max_error_rate_delta.is_finite() && max_error_rate_delta >= 0.0,
                "--max-error-rate-delta must be finite and non-negative"
            );
            anyhow::ensure!(
                min_throughput_ratio.is_finite() && (0.0..=1.0).contains(&min_throughput_ratio),
                "--min-throughput-ratio must be between 0 and 1"
            );
            let baseline = read_summary(&baseline)?;
            let current = read_summary(&run)?;
            let gate = compare::build_regression(
                &baseline,
                &current,
                compare::RegressionLimits {
                    max_p95_regression_pct: max_p95_regression,
                    max_error_rate_delta_pct: max_error_rate_delta,
                    min_throughput_ratio,
                },
            );
            if let Some(out) = out {
                let out = Path::new(&out);
                std::fs::create_dir_all(out)?;
                std::fs::write(out.join("regression.json"), serde_json::to_vec_pretty(&gate)?)?;
                std::fs::write(
                    out.join("regression.md"),
                    compare::render_regression_markdown(&gate),
                )?;
            }
            if json {
                println!("{}", serde_json::to_string_pretty(&gate)?);
            } else {
                print!("{}", compare::render_regression_markdown(&gate));
            }
            anyhow::ensure!(gate.passed, "regression gate failed");
        }
        Command::Scenario { config, scenario, target, out, iterations, json } => {
            let config_value = common::load_config(&config)?;
            let (target_name, rpc) = common::rpc_target(&config_value, target.as_deref())?;
            let summary =
                runner::run_scenario(target_name, rpc, config_value, scenario, &out, iterations)
                    .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&summary)?);
            } else {
                println!("scenario complete: {out}");
                println!(
                    "steps: {} | successes: {} | errors: {}",
                    summary.total_steps, summary.successes, summary.errors
                );
            }
        }
        Command::ConfigCheck { config, json } => {
            let config = common::load_config(&config)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&config)?);
            } else {
                let rpc_targets =
                    config.targets.values().filter(|target| target.rpc.is_some()).count();
                let runnable_methods =
                    config.json_rpc.values().filter(|method| method.weight > 0).count();
                println!(
                    "config valid: {rpc_targets} RPC target(s), {runnable_methods} configured method(s), {} scenario(s)",
                    config.scenarios.len()
                );
            }
        }
        Command::ServeMetrics { run, listen } => serve_metrics(&run, &listen).await?,
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
            min_achieved_rate,
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
                min_achieved_rate,
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
                max_requests: None,
                max_duration: None,
                max_rps: None,
                allow_public: false,
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
                max_requests: None,
                max_duration: None,
                max_rps: None,
                allow_public: false,
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
                min_achieved_rate: prompt_with_default("Minimum achieved rate %", "95")?.parse()?,
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
    let config = match args.config.as_deref() {
        Some(path) => common::load_config(path)?,
        None => build_config(
            args.rpc.clone().ok_or_else(|| anyhow!("RPC required unless --config is set"))?,
            &args.workload,
        )?,
    };
    let out_dir = args.out.clone();
    let (target_name, rpc) = common::rpc_target(&config, args.target.as_deref())?;
    let live = args
        .live_metrics
        .as_ref()
        .map(|_| runner::LiveMetrics::new(target_name.clone(), &config))
        .transpose()?;
    let (shutdown, server) =
        if let (Some(listen), Some(live)) = (args.live_metrics.as_deref(), live.clone()) {
            let listener = TcpListener::bind(listen).await?;
            println!("live Prometheus metrics at http://{listen}/metrics");
            let (shutdown, receiver) = oneshot::channel();
            let server = tokio::spawn(serve_live_metrics(listener, live, receiver));
            (Some(shutdown), Some(server))
        } else {
            (None, None)
        };
    let started = Instant::now();
    let handle =
        tokio::spawn(runner::run_bench_with_live(target_name, rpc, config, out_dir.clone(), live));
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
    let result = handle.await?;
    if let Some(shutdown) = shutdown {
        let _ = shutdown.send(());
    }
    if let Some(server) = server {
        server.await??;
    }
    let summary = result?;
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
    achieved_rate_pct: f64,
    dropped_requests: u64,
    passed: bool,
}

#[derive(Debug, Serialize)]
struct LimitResult {
    rpc: String,
    target_p95_ms: u128,
    max_error_rate: f64,
    min_achieved_rate: f64,
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
    min_achieved_rate: f64,
    bench: BenchWorkloadArgs,
}

async fn run_find_limit(options: LimitOptions) -> Result<LimitResult> {
    let LimitOptions {
        rpc,
        out,
        start_rps,
        max_rps,
        step,
        target_p95,
        max_error_rate,
        min_achieved_rate,
        mut bench,
    } = options;
    anyhow::ensure!(start_rps > 0.0, "--start-rps must be greater than zero");
    anyhow::ensure!(max_rps >= start_rps, "--max-rps must be >= --start-rps");
    anyhow::ensure!(step > 1.0, "--step must be greater than 1.0");
    anyhow::ensure!(
        (0.0..=100.0).contains(&max_error_rate),
        "--max-error-rate must be between 0 and 100"
    );
    anyhow::ensure!(
        (0.0..=100.0).contains(&min_achieved_rate),
        "--min-achieved-rate must be between 0 and 100"
    );
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
        let achieved_rate_pct = summary.achieved_rate_ratio.unwrap_or(0.0) * 100.0;
        let passed = summary.latency.p95_ms <= target_p95 &&
            error_rate <= max_error_rate &&
            achieved_rate_pct >= min_achieved_rate &&
            summary.dropped_requests == 0;
        attempts.push(LimitAttempt {
            rps: current,
            out: attempt_dir,
            observed_rps: summary.requests_per_second,
            p95_ms: summary.latency.p95_ms,
            error_rate,
            achieved_rate_pct,
            dropped_requests: summary.dropped_requests,
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
        min_achieved_rate,
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
    out.push_str(&format!("- minimum achieved rate: {:.2}%\n", result.min_achieved_rate));
    out.push_str(&format!("- best rps: {:?}\n", result.best_rps));
    out.push_str(&format!("- breaking rps: {:?}\n\n", result.breaking_rps));
    out.push_str("| target rps | observed rps | achieved | dropped | p95 | error rate | result | artifact |\n");
    out.push_str("|---:|---:|---:|---:|---:|---:|---|---|\n");
    for attempt in &result.attempts {
        out.push_str(&format!(
            "| {:.2} | {:.2} | {:.2}% | {} | {} ms | {:.2}% | {} | `{}` |\n",
            attempt.rps,
            attempt.observed_rps,
            attempt.achieved_rate_pct,
            attempt.dropped_requests,
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
                    record_ws_transport(&stats, started.elapsed().as_nanos(), error.to_string());
                    break;
                }
                let response = timeout(request_timeout, socket.next()).await;
                let latency = started.elapsed().as_nanos();
                match response {
                    Ok(Some(Ok(Message::Text(text)))) => {
                        match serde_json::from_str::<common::JsonRpcResponse>(&text) {
                            Ok(response)
                                if response.id.as_ref().and_then(Value::as_u64) == Some(id) &&
                                    response.error.is_none() =>
                            {
                                record_ws_success(&stats, latency)
                            }
                            Ok(response)
                                if response.id.as_ref().and_then(Value::as_u64) == Some(id) =>
                            {
                                record_ws_rpc_error(
                                    &stats,
                                    latency,
                                    response.error.map(|e| e.message).unwrap_or_default(),
                                )
                            }
                            Ok(_) => {
                                record_ws_transport(
                                    &stats,
                                    latency,
                                    "websocket response ID mismatch".to_string(),
                                );
                                break;
                            }
                            Err(error) => {
                                record_ws_transport(&stats, latency, error.to_string());
                                break;
                            }
                        }
                    }
                    Ok(Some(Ok(Message::Binary(bytes)))) => {
                        match serde_json::from_slice::<common::JsonRpcResponse>(&bytes) {
                            Ok(response)
                                if response.id.as_ref().and_then(Value::as_u64) == Some(id) &&
                                    response.error.is_none() =>
                            {
                                record_ws_success(&stats, latency)
                            }
                            Ok(response)
                                if response.id.as_ref().and_then(Value::as_u64) == Some(id) =>
                            {
                                record_ws_rpc_error(
                                    &stats,
                                    latency,
                                    response.error.map(|e| e.message).unwrap_or_default(),
                                )
                            }
                            Ok(_) => {
                                record_ws_transport(
                                    &stats,
                                    latency,
                                    "websocket response ID mismatch".to_string(),
                                );
                                break;
                            }
                            Err(error) => {
                                record_ws_transport(&stats, latency, error.to_string());
                                break;
                            }
                        }
                    }
                    Ok(Some(Ok(Message::Ping(payload)))) => {
                        socket.send(Message::Pong(payload)).await?;
                    }
                    Ok(Some(Ok(Message::Pong(_)))) => {}
                    Ok(Some(Ok(Message::Close(_)))) => break,
                    Ok(Some(Ok(_))) => {
                        record_ws_transport(
                            &stats,
                            latency,
                            "unexpected websocket message".to_string(),
                        );
                        break;
                    }
                    Ok(Some(Err(error))) => {
                        record_ws_transport(&stats, latency, error.to_string());
                        break;
                    }
                    Ok(None) => {
                        record_ws_transport(&stats, latency, "websocket closed".to_string());
                        break;
                    }
                    Err(_) => {
                        record_ws_timeout(&stats, latency);
                        break;
                    }
                }
            }
            Result::<()>::Ok(())
        })
    });
    for task in futures::future::join_all(tasks).await {
        task??;
    }
    let summary = build_ws_summary(
        ws,
        method,
        started.elapsed().as_nanos(),
        duration,
        concurrency.max(1),
        stats,
    );
    write_ws_artifacts(out, &summary).await?;
    Ok(summary)
}

struct WsStats {
    successes: u64,
    rpc_errors: u64,
    transport_errors: u64,
    timeouts: u64,
    latency: Histogram<u64>,
    histogram: common::LatencyHistogram,
    sum_ns: u128,
}

impl Default for WsStats {
    fn default() -> Self {
        Self {
            successes: 0,
            rpc_errors: 0,
            transport_errors: 0,
            timeouts: 0,
            latency: Histogram::new_with_bounds(1, 86_400_000_000_000, 3)
                .expect("valid websocket latency histogram bounds"),
            histogram: common::LatencyHistogram::default(),
            sum_ns: 0,
        }
    }
}

fn record_ws_latency(stats: &mut WsStats, latency_ns: u128) {
    let bounded = latency_ns.clamp(1, 86_400_000_000_000) as u64;
    let _ = stats.latency.record(bounded);
    stats.sum_ns = stats.sum_ns.saturating_add(latency_ns);
    match common::ns_to_ms_ceil(latency_ns) {
        0..=5 => stats.histogram.le_5_ms += 1,
        6..=10 => stats.histogram.le_10_ms += 1,
        11..=25 => stats.histogram.le_25_ms += 1,
        26..=50 => stats.histogram.le_50_ms += 1,
        51..=100 => stats.histogram.le_100_ms += 1,
        101..=250 => stats.histogram.le_250_ms += 1,
        251..=500 => stats.histogram.le_500_ms += 1,
        501..=1000 => stats.histogram.le_1000_ms += 1,
        _ => stats.histogram.gt_1000_ms += 1,
    }
}

fn record_ws_success(stats: &std::sync::Arc<std::sync::Mutex<WsStats>>, latency: u128) {
    let mut stats = stats.lock().expect("ws stats mutex poisoned");
    stats.successes += 1;
    record_ws_latency(&mut stats, latency);
}

fn record_ws_rpc_error(
    stats: &std::sync::Arc<std::sync::Mutex<WsStats>>,
    latency: u128,
    _detail: String,
) {
    let mut stats = stats.lock().expect("ws stats mutex poisoned");
    stats.rpc_errors += 1;
    record_ws_latency(&mut stats, latency);
}

fn record_ws_transport(
    stats: &std::sync::Arc<std::sync::Mutex<WsStats>>,
    latency: u128,
    _detail: String,
) {
    let mut stats = stats.lock().expect("ws stats mutex poisoned");
    stats.transport_errors += 1;
    record_ws_latency(&mut stats, latency);
}

fn record_ws_timeout(stats: &std::sync::Arc<std::sync::Mutex<WsStats>>, latency: u128) {
    let mut stats = stats.lock().expect("ws stats mutex poisoned");
    stats.timeouts += 1;
    record_ws_latency(&mut stats, latency);
}

fn build_ws_summary(
    target: String,
    method: String,
    duration_ns: u128,
    requested_duration: Duration,
    concurrency: usize,
    stats: std::sync::Arc<std::sync::Mutex<WsStats>>,
) -> common::BenchSummary {
    let stats = stats.lock().expect("ws stats mutex poisoned");
    let total = stats.successes + stats.rpc_errors + stats.transport_errors + stats.timeouts;
    let latency = if total == 0 {
        common::LatencySummary::default()
    } else {
        let min_ns = stats.latency.min() as u128;
        let p50_ns = stats.latency.value_at_quantile(0.50) as u128;
        let p90_ns = stats.latency.value_at_quantile(0.90) as u128;
        let p95_ns = stats.latency.value_at_quantile(0.95) as u128;
        let p99_ns = stats.latency.value_at_quantile(0.99) as u128;
        let max_ns = stats.latency.max() as u128;
        common::LatencySummary {
            min_ms: common::ns_to_ms(min_ns),
            p50_ms: common::ns_to_ms(p50_ns),
            p90_ms: common::ns_to_ms(p90_ns),
            p95_ms: common::ns_to_ms(p95_ns),
            p99_ms: common::ns_to_ms(p99_ns),
            max_ms: common::ns_to_ms(max_ns),
            min_ns,
            p50_ns,
            p90_ns,
            p95_ns,
            p99_ns,
            max_ns,
            mean_ns: stats.sum_ns / total as u128,
        }
    };
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
            p50_ns: latency.p50_ns,
            p90_ns: latency.p90_ns,
            p95_ns: latency.p95_ns,
            p99_ns: latency.p99_ns,
        },
    );
    common::BenchSummary {
        schema_version: 2,
        boom_version: env!("CARGO_PKG_VERSION").to_string(),
        target,
        duration_ms: common::ns_to_ms(duration_ns),
        duration_ns,
        requested_duration_ns: requested_duration.as_nanos(),
        started_unix_ms: 0,
        requested_rps: None,
        offered_requests: total,
        dropped_requests: 0,
        achieved_rate_ratio: None,
        concurrency,
        batch_size: 1,
        seed: Some(1),
        skipped_methods: Vec::new(),
        total_requests: total,
        successes: stats.successes,
        rpc_errors: stats.rpc_errors,
        transport_errors: stats.transport_errors,
        timeouts: stats.timeouts,
        requests_per_second: total as f64 / (requested_duration.as_secs_f64()).max(0.001),
        latency,
        histogram: stats.histogram.clone(),
        samples: Vec::new(),
        methods,
    }
}

async fn write_ws_artifacts(out: &str, summary: &common::BenchSummary) -> Result<()> {
    let out_dir = Path::new(out);
    fs::create_dir_all(out_dir).await?;
    anyhow::ensure!(
        !out_dir.join("run.json").exists(),
        "output directory {} already contains run.json; choose a new --out directory",
        out_dir.display()
    );
    write_cli_artifact(out_dir.join("run.json"), serde_json::to_vec_pretty(summary)?).await?;
    write_cli_artifact(out_dir.join("summary.md"), report::render_markdown(summary).into_bytes())
        .await?;
    write_cli_artifact(
        out_dir.join("openmetrics.prom"),
        report::render_openmetrics(summary).into_bytes(),
    )
    .await?;
    let manifest = json!({
        "schema_version": 1,
        "state": "complete",
        "transport": "websocket",
        "target": summary.target,
        "method": summary.methods.keys().next(),
        "concurrency": summary.concurrency,
        "requested_duration_ns": summary.requested_duration_ns,
        "duration_ns": summary.duration_ns,
        "result": {
            "total_requests": summary.total_requests,
            "successes": summary.successes,
            "errors": summary.rpc_errors + summary.transport_errors + summary.timeouts,
        },
    });
    write_cli_artifact(out_dir.join("manifest.json"), serde_json::to_vec_pretty(&manifest)?)
        .await?;
    Ok(())
}

async fn write_cli_artifact(path: std::path::PathBuf, bytes: Vec<u8>) -> Result<()> {
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, bytes).await?;
    match fs::rename(&temporary, &path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            fs::remove_file(&path).await?;
            fs::rename(&temporary, &path).await?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

async fn serve_metrics(run: &str, listen: &str) -> Result<()> {
    let run_path = Path::new(run);
    let run_json =
        if run_path.is_dir() { run_path.join("run.json") } else { run_path.to_path_buf() };
    let raw = fs::read_to_string(&run_json).await?;
    let summary: common::BenchSummary = serde_json::from_str(&raw)?;
    let metrics = std::sync::Arc::new(report::render_openmetrics(&summary));
    let listener = TcpListener::bind(listen).await?;
    println!("serving Prometheus metrics at http://{listen}/metrics");
    loop {
        tokio::select! {
            connection = listener.accept() => {
                let (mut stream, _) = connection?;
                let metrics = metrics.clone();
                tokio::spawn(async move {
                    let mut request = [0_u8; 4096];
                    let read = stream.read(&mut request).await.unwrap_or(0);
                    let first_line = String::from_utf8_lossy(&request[..read]);
                    let path = first_line.split_whitespace().nth(1).unwrap_or("/");
                    let (status, content_type, body) = match path {
                        "/metrics" => ("200 OK", "application/openmetrics-text; version=1.0.0; charset=utf-8", metrics.as_str()),
                        "/healthz" => ("200 OK", "text/plain; charset=utf-8", "ok\n"),
                        _ => ("404 Not Found", "text/plain; charset=utf-8", "not found\n"),
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len(),
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                break;
            }
        }
    }
    Ok(())
}

async fn serve_live_metrics(
    listener: TcpListener,
    live: runner::LiveMetrics,
    mut shutdown: oneshot::Receiver<()>,
) -> Result<()> {
    loop {
        tokio::select! {
            connection = listener.accept() => {
                let (mut stream, _) = connection?;
                let live = live.clone();
                tokio::spawn(async move {
                    let mut request = [0_u8; 4096];
                    let read = stream.read(&mut request).await.unwrap_or(0);
                    let first_line = String::from_utf8_lossy(&request[..read]);
                    let path = first_line.split_whitespace().nth(1).unwrap_or("/");
                    let (status, content_type, body) = match path {
                        "/metrics" => (
                            "200 OK",
                            "application/openmetrics-text; version=1.0.0; charset=utf-8",
                            report::render_openmetrics(&live.snapshot()),
                        ),
                        "/healthz" => {
                            let state = live.state();
                            let status = if state == "failed" { "503 Service Unavailable" } else { "200 OK" };
                            (status, "text/plain; charset=utf-8", format!("{state}\n"))
                        }
                        _ => ("404 Not Found", "text/plain; charset=utf-8", "not found\n".to_string()),
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len(),
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
            _ = &mut shutdown => break,
        }
    }
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
        max_requests: args.max_requests,
        max_duration: args.max_duration.clone(),
        max_rps: args.max_rps,
        dry_run: false,
        allow_public: args.allow_public,
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
    let config = common::config_for_rpc(rpc, bench, workload);
    common::validate_config(&config)?;
    Ok(config)
}

fn read_summary(path: &str) -> Result<common::BenchSummary> {
    let path = Path::new(path);
    let run_json = if path.is_dir() { path.join("run.json") } else { path.to_path_buf() };
    let raw = std::fs::read_to_string(&run_json)
        .with_context(|| format!("reading run summary {}", run_json.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("parsing run summary {}", run_json.display()))
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
