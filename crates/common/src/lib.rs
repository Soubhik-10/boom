use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeMap, fs, path::Path, time::Duration};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub targets: BTreeMap<String, TargetConfig>,
    #[serde(default)]
    pub bench: BenchConfig,
    #[serde(default)]
    pub json_rpc: BTreeMap<String, MethodConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TargetConfig {
    #[serde(default)]
    pub rpc: Option<String>,
    #[serde(default)]
    pub engine: Option<String>,
    #[serde(default)]
    pub jwt: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BenchConfig {
    #[serde(default = "default_duration")]
    pub duration: String,
    #[serde(default)]
    pub warmup: Option<String>,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_timeout")]
    pub timeout: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default)]
    pub seed: Option<u64>,
    #[serde(default)]
    pub rps: Option<f64>,
    #[serde(default)]
    pub ramp: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MethodConfig {
    #[serde(default = "default_weight")]
    pub weight: usize,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub compare: Option<String>,
    #[serde(default)]
    pub readonly: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: &'static str,
    pub id: u64,
    pub method: String,
    pub params: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JsonRpcErrorObject {
    pub code: i64,
    pub message: String,
    #[serde(default)]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: Option<String>,
    pub id: Option<Value>,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub error: Option<JsonRpcErrorObject>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MethodProbe {
    pub method: String,
    pub status: ProbeStatus,
    pub latency_ms: u128,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeStatus {
    Supported,
    Unsupported,
    AuthRequired,
    InvalidParams,
    Timeout,
    ServerError,
    TransportError,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BenchSummary {
    pub target: String,
    pub duration_ms: u128,
    pub total_requests: u64,
    pub successes: u64,
    pub rpc_errors: u64,
    pub transport_errors: u64,
    pub timeouts: u64,
    pub requests_per_second: f64,
    pub latency: LatencySummary,
    #[serde(default)]
    pub histogram: LatencyHistogram,
    #[serde(default)]
    pub samples: Vec<TimeSample>,
    pub methods: BTreeMap<String, MethodSummary>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct LatencyHistogram {
    pub le_5_ms: u64,
    pub le_10_ms: u64,
    pub le_25_ms: u64,
    pub le_50_ms: u64,
    pub le_100_ms: u64,
    pub le_250_ms: u64,
    pub le_500_ms: u64,
    pub le_1000_ms: u64,
    pub gt_1000_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct TimeSample {
    pub second: u64,
    pub requests: u64,
    pub successes: u64,
    pub errors: u64,
    pub p50_ms: u128,
    pub p95_ms: u128,
    pub p99_ms: u128,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct MethodSummary {
    pub requests: u64,
    pub successes: u64,
    pub errors: u64,
    pub p50_ms: u128,
    pub p90_ms: u128,
    pub p95_ms: u128,
    pub p99_ms: u128,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct LatencySummary {
    pub min_ms: u128,
    pub p50_ms: u128,
    pub p90_ms: u128,
    pub p95_ms: u128,
    pub p99_ms: u128,
    pub max_ms: u128,
}

pub fn load_config(path: impl AsRef<Path>) -> Result<Config> {
    let path = path.as_ref();
    let raw =
        fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    toml::from_str(&raw).with_context(|| format!("parsing config {}", path.display()))
}

pub fn first_rpc_target(config: &Config) -> Result<(String, String)> {
    config
        .targets
        .iter()
        .find_map(|(name, target)| target.rpc.as_ref().map(|rpc| (name.clone(), rpc.clone())))
        .ok_or_else(|| anyhow!("config does not define any target with an rpc URL"))
}

pub fn methods_or_default(config: &Config) -> Vec<(String, Value, usize)> {
    if config.json_rpc.is_empty() {
        return default_eth_workload();
    }
    config
        .json_rpc
        .iter()
        .filter(|(_, cfg)| cfg.weight > 0)
        .map(|(method, cfg)| (method.clone(), cfg.params.clone(), cfg.weight))
        .collect()
}

pub fn config_for_rpc(
    rpc: String,
    bench: BenchConfig,
    workload: Vec<(String, Value, usize)>,
) -> Config {
    let mut targets = BTreeMap::new();
    targets.insert(
        "target".to_string(),
        TargetConfig { rpc: Some(rpc), engine: None, jwt: None, label: None },
    );

    let mut json_rpc = BTreeMap::new();
    for (method, params, weight) in workload {
        json_rpc
            .insert(method, MethodConfig { weight, params, compare: None, readonly: Some(true) });
    }

    Config { targets, bench, json_rpc }
}

pub fn workload_presets(
    eth: bool,
    debug: bool,
    trace: bool,
    txpool: bool,
    net: bool,
    web3: bool,
    all: bool,
) -> Vec<(String, Value, usize)> {
    let mut out = Vec::new();
    let include_eth = eth || all || !(debug || trace || txpool || net || web3);
    if include_eth {
        out.extend(eth_workload());
    }
    if debug || all {
        out.extend(debug_workload());
    }
    if trace || all {
        out.extend(trace_workload());
    }
    if txpool || all {
        out.extend(txpool_workload());
    }
    if net || all {
        out.extend(net_workload());
    }
    if web3 || all {
        out.extend(web3_workload());
    }
    dedupe_workload(out)
}

pub fn eth_workload() -> Vec<(String, Value, usize)> {
    vec![
        ("eth_chainId".to_string(), json!([]), 4),
        ("eth_blockNumber".to_string(), json!([]), 10),
        ("eth_syncing".to_string(), json!([]), 1),
        ("eth_gasPrice".to_string(), json!([]), 3),
        ("eth_maxPriorityFeePerGas".to_string(), json!([]), 2),
        ("eth_feeHistory".to_string(), json!([4, "latest", [25, 50, 75]]), 2),
        ("eth_getBlockByNumber".to_string(), json!(["$latest_block", false]), 8),
        ("eth_getBlockByHash".to_string(), json!(["$block_hash", false]), 4),
        ("eth_getBlockTransactionCountByNumber".to_string(), json!(["$latest_block"]), 2),
        ("eth_getBlockTransactionCountByHash".to_string(), json!(["$block_hash"]), 2),
        ("eth_getTransactionByHash".to_string(), json!(["$tx_hash"]), 6),
        ("eth_getTransactionReceipt".to_string(), json!(["$tx_hash"]), 6),
        ("eth_getBalance".to_string(), json!(["$address", "latest"]), 5),
        ("eth_getTransactionCount".to_string(), json!(["$address", "latest"]), 3),
        ("eth_getCode".to_string(), json!(["$address", "latest"]), 3),
        ("eth_getStorageAt".to_string(), json!(["$address", "0x0", "latest"]), 2),
        ("eth_call".to_string(), json!([{ "to": "$call_to", "data": "0x" }, "latest"]), 4),
        ("eth_estimateGas".to_string(), json!([{ "to": "$call_to", "data": "0x" }]), 2),
        (
            "eth_getLogs".to_string(),
            json!([{ "fromBlock": "$latest_block", "toBlock": "$latest_block" }]),
            2,
        ),
    ]
}

pub fn simulate_workload() -> Vec<(String, Value, usize)> {
    vec![(
        "eth_simulateV1".to_string(),
        json!([{ "blockStateCalls": [{ "calls": [] }] }, "latest"]),
        1,
    )]
}

pub fn debug_workload() -> Vec<(String, Value, usize)> {
    vec![
        (
            "debug_traceTransaction".to_string(),
            json!(["$tx_hash", { "tracer": "callTracer", "timeout": "10s" }]),
            1,
        ),
        (
            "debug_traceCall".to_string(),
            json!([{ "to": "$call_to", "data": "0x" }, "latest", { "tracer": "callTracer", "timeout": "10s" }]),
            1,
        ),
        (
            "debug_traceBlockByNumber".to_string(),
            json!(["$latest_block", { "tracer": "callTracer", "timeout": "10s" }]),
            1,
        ),
    ]
}

pub fn trace_workload() -> Vec<(String, Value, usize)> {
    vec![
        ("trace_block".to_string(), json!(["$latest_block"]), 1),
        ("trace_transaction".to_string(), json!(["$tx_hash"]), 1),
        (
            "trace_call".to_string(),
            json!([{ "to": "$call_to", "data": "0x" }, ["trace"], "latest"]),
            1,
        ),
    ]
}

pub fn txpool_workload() -> Vec<(String, Value, usize)> {
    vec![
        ("txpool_status".to_string(), json!([]), 1),
        ("txpool_content".to_string(), json!([]), 1),
        ("txpool_inspect".to_string(), json!([]), 1),
    ]
}

pub fn net_workload() -> Vec<(String, Value, usize)> {
    vec![
        ("net_version".to_string(), json!([]), 1),
        ("net_peerCount".to_string(), json!([]), 1),
        ("net_listening".to_string(), json!([]), 1),
    ]
}

pub fn web3_workload() -> Vec<(String, Value, usize)> {
    vec![
        ("web3_clientVersion".to_string(), json!([]), 1),
        ("web3_sha3".to_string(), json!(["0x68656c6c6f"]), 1),
    ]
}

fn dedupe_workload(workload: Vec<(String, Value, usize)>) -> Vec<(String, Value, usize)> {
    let mut out: BTreeMap<String, (Value, usize)> = BTreeMap::new();
    for (method, params, weight) in workload {
        out.entry(method)
            .and_modify(|(_, existing_weight)| *existing_weight += weight)
            .or_insert((params, weight));
    }
    out.into_iter().map(|(method, (params, weight))| (method, params, weight)).collect()
}
pub fn default_eth_workload() -> Vec<(String, Value, usize)> {
    vec![
        ("web3_clientVersion".to_string(), json!([]), 1),
        ("net_version".to_string(), json!([]), 1),
        ("eth_chainId".to_string(), json!([]), 4),
        ("eth_blockNumber".to_string(), json!([]), 10),
        ("eth_syncing".to_string(), json!([]), 1),
        ("eth_gasPrice".to_string(), json!([]), 3),
        ("eth_maxPriorityFeePerGas".to_string(), json!([]), 2),
        ("eth_feeHistory".to_string(), json!([4, "latest", [25, 50, 75]]), 2),
        ("eth_getBlockByNumber".to_string(), json!(["$latest_block", false]), 8),
        ("eth_getBlockByHash".to_string(), json!(["$block_hash", false]), 4),
        ("eth_getBlockTransactionCountByNumber".to_string(), json!(["$latest_block"]), 2),
        ("eth_getBlockTransactionCountByHash".to_string(), json!(["$block_hash"]), 2),
        ("eth_getTransactionByHash".to_string(), json!(["$tx_hash"]), 6),
        ("eth_getTransactionReceipt".to_string(), json!(["$tx_hash"]), 6),
        ("eth_getBalance".to_string(), json!(["$address", "latest"]), 5),
        ("eth_getTransactionCount".to_string(), json!(["$address", "latest"]), 3),
        ("eth_getCode".to_string(), json!(["$address", "latest"]), 3),
        ("eth_getStorageAt".to_string(), json!(["$address", "0x0", "latest"]), 2),
        ("eth_call".to_string(), json!([{ "to": "$call_to", "data": "0x" }, "latest"]), 4),
        ("eth_estimateGas".to_string(), json!([{ "to": "$call_to", "data": "0x" }]), 2),
        (
            "eth_getLogs".to_string(),
            json!([{ "fromBlock": "$latest_block", "toBlock": "$latest_block" }]),
            2,
        ),
        (
            "debug_traceTransaction".to_string(),
            json!(["$tx_hash", { "tracer": "callTracer", "timeout": "10s" }]),
            1,
        ),
    ]
}

pub fn scenario_workload(name: &str) -> Result<Vec<(String, Value, usize)>> {
    Ok(match name {
        "light" => vec![
            ("eth_chainId".to_string(), json!([]), 1),
            ("eth_blockNumber".to_string(), json!([]), 8),
            ("eth_getBlockByNumber".to_string(), json!(["$latest_block", false]), 3),
        ],
        "explorer" => {
            let mut out = eth_workload();
            out.extend(vec![
                (
                    "eth_getLogs".to_string(),
                    json!([{ "fromBlock": "$latest_block", "toBlock": "$latest_block" }]),
                    8,
                ),
                ("eth_getTransactionReceipt".to_string(), json!(["$tx_hash"]), 8),
            ]);
            dedupe_workload(out)
        }
        "archive" => vec![
            ("eth_getBalance".to_string(), json!(["$address", "$latest_block"]), 4),
            ("eth_getCode".to_string(), json!(["$address", "$latest_block"]), 4),
            ("eth_getStorageAt".to_string(), json!(["$address", "0x0", "$latest_block"]), 4),
            (
                "eth_call".to_string(),
                json!([{ "to": "$call_to", "data": "0x" }, "$latest_block"]),
                4,
            ),
        ],
        "debug-heavy" => debug_workload(),
        "simulate" => simulate_workload(),
        "txpool" => txpool_workload(),
        "all" => workload_presets(false, false, false, false, false, false, true),
        other => return Err(anyhow!("unknown scenario '{other}'")),
    })
}
pub fn parse_duration(input: &str) -> Result<Duration> {
    let trimmed = input.trim();
    if let Some(value) = trimmed.strip_suffix("ms") {
        return Ok(Duration::from_millis(value.parse()?));
    }
    if let Some(value) = trimmed.strip_suffix('s') {
        return Ok(Duration::from_secs(value.parse()?));
    }
    if let Some(value) = trimmed.strip_suffix('m') {
        return Ok(Duration::from_secs(value.parse::<u64>()? * 60));
    }
    Ok(Duration::from_secs(trimmed.parse()?))
}

pub fn summarize_latencies(latencies: &mut [u128]) -> LatencySummary {
    if latencies.is_empty() {
        return LatencySummary::default();
    }
    latencies.sort_unstable();
    LatencySummary {
        min_ms: latencies[0],
        p50_ms: percentile(latencies, 50.0),
        p90_ms: percentile(latencies, 90.0),
        p95_ms: percentile(latencies, 95.0),
        p99_ms: percentile(latencies, 99.0),
        max_ms: *latencies.last().unwrap_or(&0),
    }
}

pub fn latency_histogram(latencies: &[u128]) -> LatencyHistogram {
    let mut histogram = LatencyHistogram::default();
    for latency in latencies {
        match *latency {
            0..=5 => histogram.le_5_ms += 1,
            6..=10 => histogram.le_10_ms += 1,
            11..=25 => histogram.le_25_ms += 1,
            26..=50 => histogram.le_50_ms += 1,
            51..=100 => histogram.le_100_ms += 1,
            101..=250 => histogram.le_250_ms += 1,
            251..=500 => histogram.le_500_ms += 1,
            501..=1000 => histogram.le_1000_ms += 1,
            _ => histogram.gt_1000_ms += 1,
        }
    }
    histogram
}
fn percentile(sorted: &[u128], pct: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((pct / 100.0) * ((sorted.len() - 1) as f64)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn default_duration() -> String {
    "30s".to_string()
}
fn default_timeout() -> String {
    "10s".to_string()
}
fn default_concurrency() -> usize {
    64
}
fn default_batch_size() -> usize {
    1
}
fn default_weight() -> usize {
    1
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            duration: default_duration(),
            warmup: Some("0s".to_string()),
            concurrency: default_concurrency(),
            timeout: default_timeout(),
            batch_size: default_batch_size(),
            seed: None,
            rps: None,
            ramp: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_duration_units() {
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("3s").unwrap(), Duration::from_secs(3));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("7").unwrap(), Duration::from_secs(7));
    }

    #[test]
    fn latency_summary_uses_expected_percentiles() {
        let mut latencies = vec![10, 50, 20, 40, 30];
        let summary = summarize_latencies(&mut latencies);
        assert_eq!(summary.min_ms, 10);
        assert_eq!(summary.p50_ms, 30);
        assert_eq!(summary.p90_ms, 50);
        assert_eq!(summary.max_ms, 50);
    }

    #[test]
    fn default_workload_contains_realistic_debug_call() {
        let workload = default_eth_workload();
        assert!(workload.iter().any(|(method, _, _)| method == "debug_traceTransaction"));
        assert!(workload.iter().any(|(_, params, _)| params.to_string().contains("$tx_hash")));
    }

    #[test]
    fn explicit_zero_weight_disables_method() {
        let config: Config = toml::from_str(
            r#"
            [targets.local]
            rpc = "http://localhost:8545"

            [json_rpc.eth_simulateV1]
            weight = 0
            params = []
        "#,
        )
        .unwrap();
        assert!(methods_or_default(&config).is_empty());
    }

    #[test]
    fn eth_workload_does_not_include_simulate() {
        let workload = eth_workload();
        assert!(!workload.iter().any(|(method, _, _)| method == "eth_simulateV1"));
    }

    #[test]
    fn simulate_workload_does_not_pin_stale_block_number() {
        let workload = simulate_workload();
        let simulate_params = workload
            .iter()
            .find_map(|(method, params, _)| (method == "eth_simulateV1").then_some(params))
            .expect("eth_simulateV1 workload exists");
        let encoded = simulate_params.to_string();
        assert!(!encoded.contains("blockOverrides"));
        assert!(!encoded.contains("$latest_block"));
    }
}
