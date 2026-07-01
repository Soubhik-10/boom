use anyhow::{Context, Result};
const MAX_ERROR_RECORDS: usize = 1_000;
use client::JsonRpcClient;
use common::{summarize_latencies, BenchSummary, Config, MethodSummary, TimeSample};
use futures::future::join_all;
use serde::Serialize;
use serde_json::{Map, Value};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::{
    fs,
    time::{sleep, Duration},
};

#[derive(Debug, Clone)]
struct WorkItem {
    method: String,
    params: Value,
}

#[derive(Debug, Clone, Copy)]
struct RateSchedule {
    start_rps: f64,
    end_rps: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct SeedData {
    latest_block: Option<String>,
    block_hash: Option<String>,
    tx_hash: Option<String>,
    address: Option<String>,
    call_to: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ErrorRecord {
    method: String,
    kind: String,
    detail: String,
    latency_ms: u128,
}

#[derive(Default)]
struct SampleBucket {
    requests: u64,
    successes: u64,
    errors: u64,
    latencies: Vec<u128>,
}

#[derive(Default)]
struct SharedMetrics {
    total: u64,
    successes: u64,
    rpc_errors: u64,
    transport_errors: u64,
    timeouts: u64,
    latencies: Vec<u128>,
    method_latencies: BTreeMap<String, Vec<u128>>,
    method_successes: BTreeMap<String, u64>,
    method_errors: BTreeMap<String, u64>,
    samples: BTreeMap<u64, SampleBucket>,
    errors: Vec<ErrorRecord>,
}

pub async fn run_bench(
    target_name: String,
    rpc: String,
    config: Config,
    out_dir: impl AsRef<Path>,
) -> Result<BenchSummary> {
    let timeout = common::parse_duration(&config.bench.timeout)?;
    let duration = common::parse_duration(&config.bench.duration)?;
    let warmup = config
        .bench
        .warmup
        .as_deref()
        .map(common::parse_duration)
        .transpose()?
        .unwrap_or(Duration::ZERO);
    let rate = rate_schedule(config.bench.rps, config.bench.ramp.as_deref())?;
    let batch_size = config.bench.batch_size.max(1);
    let client = Arc::new(JsonRpcClient::new(rpc, timeout)?);
    let seed = fetch_seed_data(&client).await.unwrap_or_default();
    let workload = Arc::new(expand_workload(&config, &seed));
    anyhow::ensure!(
        !workload.is_empty(),
        "no runnable JSON-RPC workload after resolving live seed data"
    );
    let metrics = Arc::new(Mutex::new(SharedMetrics::default()));

    fs::create_dir_all(&out_dir).await?;
    fs::write(out_dir.as_ref().join("seed.json"), serde_json::to_vec_pretty(&seed)?).await?;

    if !warmup.is_zero() {
        run_phase(
            client.clone(),
            workload.clone(),
            config.bench.concurrency.clamp(1, 16),
            warmup,
            batch_size,
            rate,
            None,
        )
        .await?;
    }

    let started = Instant::now();
    run_phase(
        client,
        workload,
        config.bench.concurrency.max(1),
        duration,
        batch_size,
        rate,
        Some((started, metrics.clone())),
    )
    .await?;
    let elapsed = started.elapsed();

    let summary = build_summary(target_name, elapsed.as_millis(), metrics.clone());
    write_artifacts(out_dir, &summary, metrics).await?;
    Ok(summary)
}

fn rate_schedule(rps: Option<f64>, ramp: Option<&str>) -> Result<Option<RateSchedule>> {
    if let Some(ramp) = ramp {
        let (start, end) = ramp.split_once(':').ok_or_else(|| {
            anyhow::anyhow!("ramp must look like START:END, for example 100:1000")
        })?;
        return Ok(Some(RateSchedule { start_rps: start.parse()?, end_rps: end.parse()? }));
    }
    Ok(rps.map(|rps| RateSchedule { start_rps: rps, end_rps: rps }))
}

async fn fetch_seed_data(client: &JsonRpcClient) -> Result<SeedData> {
    let latest_block = rpc_value(client, 9_000_001, "eth_blockNumber", Value::Array(vec![]))
        .await
        .ok()
        .and_then(|v| v.as_str().map(str::to_string));
    let block_param = latest_block
        .clone()
        .map(Value::String)
        .unwrap_or_else(|| Value::String("latest".to_string()));
    let block = rpc_value(
        client,
        9_000_002,
        "eth_getBlockByNumber",
        Value::Array(vec![block_param.clone(), Value::Bool(false)]),
    )
    .await
    .ok();
    let full_block = rpc_value(
        client,
        9_000_003,
        "eth_getBlockByNumber",
        Value::Array(vec![block_param, Value::Bool(true)]),
    )
    .await
    .ok();

    let block_hash = block.as_ref().and_then(|v| str_field(v, "hash"));
    let tx_hash = first_tx_hash(block.as_ref()).or_else(|| first_tx_hash(full_block.as_ref()));
    let address = first_tx_address(full_block.as_ref())
        .unwrap_or_else(|| "0x0000000000000000000000000000000000000000".to_string());
    let call_to = first_tx_to(full_block.as_ref()).unwrap_or_else(|| address.clone());

    Ok(SeedData {
        latest_block,
        block_hash,
        tx_hash,
        address: Some(address),
        call_to: Some(call_to),
    })
}

async fn rpc_value(client: &JsonRpcClient, id: u64, method: &str, params: Value) -> Result<Value> {
    let response = client.call(id, method, params).await?;
    if let Some(error) = response.error {
        anyhow::bail!("{}: {}", error.code, error.message);
    }
    Ok(response.result.unwrap_or(Value::Null))
}

async fn run_phase(
    client: Arc<JsonRpcClient>,
    workload: Arc<Vec<WorkItem>>,
    concurrency: usize,
    duration: Duration,
    batch_size: usize,
    rate: Option<RateSchedule>,
    metrics: Option<(Instant, Arc<Mutex<SharedMetrics>>)>,
) -> Result<()> {
    let deadline = Instant::now() + duration;
    let phase_started = Instant::now();
    let tasks = (0..concurrency).map(|worker| {
        let client = client.clone();
        let workload = workload.clone();
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let mut index = worker % workload.len();
            let mut id = worker as u64;
            while Instant::now() < deadline {
                if let Some(rate) = rate {
                    let elapsed = phase_started.elapsed().as_secs_f64();
                    let total = duration.as_secs_f64().max(0.001);
                    let progress = (elapsed / total).clamp(0.0, 1.0);
                    let current_rps = rate.start_rps + ((rate.end_rps - rate.start_rps) * progress);
                    let interval = concurrency as f64 / current_rps.max(0.001);
                    sleep(Duration::from_secs_f64(interval)).await;
                }

                let started = Instant::now();
                if batch_size == 1 {
                    let item = &workload[index];
                    index = (index + concurrency) % workload.len();
                    id += concurrency as u64;
                    let result = client.call(id, item.method.clone(), item.params.clone()).await;
                    let latency_ms = started.elapsed().as_millis();
                    if let Some((bench_started, metrics)) = &metrics {
                        record(metrics, *bench_started, item.method.clone(), result, latency_ms);
                    }
                } else {
                    let mut batch = Vec::with_capacity(batch_size);
                    let mut methods = Vec::with_capacity(batch_size);
                    for _ in 0..batch_size {
                        let item = &workload[index];
                        index = (index + concurrency) % workload.len();
                        id += concurrency as u64;
                        methods.push(item.method.clone());
                        batch.push(common::JsonRpcRequest {
                            jsonrpc: "2.0",
                            id,
                            method: item.method.clone(),
                            params: item.params.clone(),
                        });
                    }
                    let result = client.call_batch(&batch).await;
                    let latency_ms = started.elapsed().as_millis();
                    if let Some((bench_started, metrics)) = &metrics {
                        match result {
                            Ok(responses) => {
                                for (method, response) in methods.into_iter().zip(responses) {
                                    record(
                                        metrics,
                                        *bench_started,
                                        method,
                                        Ok(response),
                                        latency_ms,
                                    );
                                }
                            }
                            Err(error) => {
                                let detail = error.to_string();
                                for method in methods {
                                    record_transport_error(
                                        metrics,
                                        *bench_started,
                                        method,
                                        detail.clone(),
                                        latency_ms,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        })
    });
    for task in join_all(tasks).await {
        task?;
    }
    sleep(Duration::from_millis(1)).await;
    Ok(())
}

fn record(
    metrics: &Arc<Mutex<SharedMetrics>>,
    bench_started: Instant,
    method: String,
    result: Result<common::JsonRpcResponse>,
    latency_ms: u128,
) {
    match result {
        Ok(response) if response.error.is_none() => {
            let mut metrics = metrics.lock().expect("metrics mutex poisoned");
            update_common(&mut metrics, bench_started, &method, latency_ms);
            metrics.successes += 1;
            *metrics.method_successes.entry(method).or_default() += 1;
            let sample = metrics.samples.entry(bench_started.elapsed().as_secs()).or_default();
            sample.successes += 1;
        }
        Ok(response) => {
            let detail =
                response.error.map(|e| format!("{}: {}", e.code, e.message)).unwrap_or_default();
            let mut metrics = metrics.lock().expect("metrics mutex poisoned");
            update_common(&mut metrics, bench_started, &method, latency_ms);
            metrics.rpc_errors += 1;
            *metrics.method_errors.entry(method.clone()).or_default() += 1;
            metrics.samples.entry(bench_started.elapsed().as_secs()).or_default().errors += 1;
            push_error(
                &mut metrics,
                ErrorRecord { method, kind: "rpc_error".to_string(), detail, latency_ms },
            );
        }
        Err(error) => {
            record_transport_error(metrics, bench_started, method, error.to_string(), latency_ms)
        }
    }
}

fn record_transport_error(
    metrics: &Arc<Mutex<SharedMetrics>>,
    bench_started: Instant,
    method: String,
    detail: String,
    latency_ms: u128,
) {
    let mut metrics = metrics.lock().expect("metrics mutex poisoned");
    update_common(&mut metrics, bench_started, &method, latency_ms);
    if detail.to_ascii_lowercase().contains("timeout") {
        metrics.timeouts += 1;
    } else {
        metrics.transport_errors += 1;
    }
    *metrics.method_errors.entry(method.clone()).or_default() += 1;
    metrics.samples.entry(bench_started.elapsed().as_secs()).or_default().errors += 1;
    push_error(
        &mut metrics,
        ErrorRecord { method, kind: "transport_error".to_string(), detail, latency_ms },
    );
}

fn update_common(
    metrics: &mut SharedMetrics,
    bench_started: Instant,
    method: &str,
    latency_ms: u128,
) {
    metrics.total += 1;
    metrics.latencies.push(latency_ms);
    metrics.method_latencies.entry(method.to_string()).or_default().push(latency_ms);
    let sample = metrics.samples.entry(bench_started.elapsed().as_secs()).or_default();
    sample.requests += 1;
    sample.latencies.push(latency_ms);
}

fn push_error(metrics: &mut SharedMetrics, error: ErrorRecord) {
    if metrics.errors.len() < MAX_ERROR_RECORDS {
        metrics.errors.push(error);
    }
}
fn expand_workload(config: &Config, seed: &SeedData) -> Vec<WorkItem> {
    let mut out = Vec::new();
    for (method, params, weight) in common::methods_or_default(config) {
        let Some(params) = resolve_placeholders(params, seed) else {
            continue;
        };
        for _ in 0..weight.max(1) {
            out.push(WorkItem { method: method.clone(), params: params.clone() });
        }
    }
    out
}

fn resolve_placeholders(value: Value, seed: &SeedData) -> Option<Value> {
    match value {
        Value::String(s) if s.starts_with('$') => placeholder(&s, seed).map(Value::String),
        Value::Array(values) => values
            .into_iter()
            .map(|value| resolve_placeholders(value, seed))
            .collect::<Option<Vec<_>>>()
            .map(Value::Array),
        Value::Object(values) => {
            let mut out = Map::new();
            for (key, value) in values {
                out.insert(key, resolve_placeholders(value, seed)?);
            }
            Some(Value::Object(out))
        }
        other => Some(other),
    }
}

fn placeholder(name: &str, seed: &SeedData) -> Option<String> {
    match name {
        "$latest_block" => seed.latest_block.clone(),
        "$block_hash" => seed.block_hash.clone(),
        "$tx_hash" => seed.tx_hash.clone(),
        "$address" => seed.address.clone(),
        "$call_to" => seed.call_to.clone(),
        _ => None,
    }
}

fn str_field(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_string)
}

fn first_tx_hash(block: Option<&Value>) -> Option<String> {
    let tx = block?.get("transactions")?.as_array()?.first()?;
    if let Some(hash) = tx.as_str() {
        return Some(hash.to_string());
    }
    str_field(tx, "hash")
}

fn first_tx_address(block: Option<&Value>) -> Option<String> {
    let tx = block?.get("transactions")?.as_array()?.iter().find(|tx| tx.is_object())?;
    str_field(tx, "from").or_else(|| str_field(tx, "to"))
}

fn first_tx_to(block: Option<&Value>) -> Option<String> {
    let tx = block?.get("transactions")?.as_array()?.iter().find(|tx| tx.is_object())?;
    str_field(tx, "to")
}

fn build_summary(
    target: String,
    duration_ms: u128,
    metrics: Arc<Mutex<SharedMetrics>>,
) -> BenchSummary {
    let metrics = metrics.lock().expect("metrics mutex poisoned");
    let mut latencies = metrics.latencies.clone();
    let latency = summarize_latencies(&mut latencies);
    let histogram = common::latency_histogram(&metrics.latencies);
    let mut methods = BTreeMap::new();
    for (method, method_latencies) in &metrics.method_latencies {
        let mut copy = method_latencies.clone();
        let summary = summarize_latencies(&mut copy);
        let successes = *metrics.method_successes.get(method).unwrap_or(&0);
        let errors = *metrics.method_errors.get(method).unwrap_or(&0);
        methods.insert(
            method.clone(),
            MethodSummary {
                requests: successes + errors,
                successes,
                errors,
                p50_ms: summary.p50_ms,
                p90_ms: summary.p90_ms,
                p95_ms: summary.p95_ms,
                p99_ms: summary.p99_ms,
            },
        );
    }
    let samples = metrics
        .samples
        .iter()
        .map(|(second, sample)| {
            let mut latencies = sample.latencies.clone();
            let summary = summarize_latencies(&mut latencies);
            TimeSample {
                second: *second,
                requests: sample.requests,
                successes: sample.successes,
                errors: sample.errors,
                p50_ms: summary.p50_ms,
                p95_ms: summary.p95_ms,
                p99_ms: summary.p99_ms,
            }
        })
        .collect();
    let seconds = (duration_ms as f64 / 1000.0).max(0.001);
    BenchSummary {
        target,
        duration_ms,
        total_requests: metrics.total,
        successes: metrics.successes,
        rpc_errors: metrics.rpc_errors,
        transport_errors: metrics.transport_errors,
        timeouts: metrics.timeouts,
        requests_per_second: metrics.total as f64 / seconds,
        latency,
        histogram,
        samples,
        methods,
    }
}

async fn write_artifacts(
    out_dir: impl AsRef<Path>,
    summary: &BenchSummary,
    metrics: Arc<Mutex<SharedMetrics>>,
) -> Result<()> {
    let out_dir = out_dir.as_ref();
    fs::write(out_dir.join("run.json"), serde_json::to_vec_pretty(summary)?).await?;
    let mut csv = String::from("method,requests,successes,errors,p50_ms,p90_ms,p95_ms,p99_ms\n");
    for (method, summary) in &summary.methods {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{}\n",
            method,
            summary.requests,
            summary.successes,
            summary.errors,
            summary.p50_ms,
            summary.p90_ms,
            summary.p95_ms,
            summary.p99_ms
        ));
    }
    fs::write(out_dir.join("metrics.csv"), csv).await?;
    let mut samples_csv = String::from("second,requests,successes,errors,p50_ms,p95_ms,p99_ms\n");
    for sample in &summary.samples {
        samples_csv.push_str(&format!(
            "{},{},{},{},{},{},{}\n",
            sample.second,
            sample.requests,
            sample.successes,
            sample.errors,
            sample.p50_ms,
            sample.p95_ms,
            sample.p99_ms
        ));
    }
    fs::write(out_dir.join("samples.csv"), samples_csv).await?;
    let errors = {
        let metrics = metrics.lock().expect("metrics mutex poisoned");
        metrics
            .errors
            .iter()
            .map(serde_json::to_string)
            .collect::<std::result::Result<Vec<_>, _>>()?
            .join("\n")
    };
    fs::write(out_dir.join("errors.jsonl"), errors).await.context("writing errors.jsonl")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn resolves_nested_placeholders() {
        let seed = SeedData {
            latest_block: Some("0x10".to_string()),
            block_hash: Some("0xabc".to_string()),
            tx_hash: Some("0xdef".to_string()),
            address: Some("0x0000000000000000000000000000000000000001".to_string()),
            call_to: Some("0x0000000000000000000000000000000000000002".to_string()),
        };
        let value = json!([{ "to": "$call_to", "nested": ["$latest_block"] }, "$tx_hash"]);
        let resolved = resolve_placeholders(value, &seed).unwrap();
        assert_eq!(resolved[0]["to"], "0x0000000000000000000000000000000000000002");
        assert_eq!(resolved[0]["nested"][0], "0x10");
        assert_eq!(resolved[1], "0xdef");
    }

    #[test]
    fn unresolved_placeholder_skips_method() {
        let seed = SeedData::default();
        assert!(resolve_placeholders(json!(["$tx_hash"]), &seed).is_none());
    }

    #[test]
    fn caps_error_records() {
        let mut metrics = SharedMetrics::default();
        for i in 0..(MAX_ERROR_RECORDS + 10) {
            push_error(
                &mut metrics,
                ErrorRecord {
                    method: "eth_blockNumber".to_string(),
                    kind: "rpc_error".to_string(),
                    detail: i.to_string(),
                    latency_ms: 1,
                },
            );
        }
        assert_eq!(metrics.errors.len(), MAX_ERROR_RECORDS);
    }
}
