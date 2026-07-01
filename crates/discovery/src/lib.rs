use anyhow::Result;
use client::JsonRpcClient;
use common::{MethodProbe, ProbeStatus};
use serde::Serialize;
use serde_json::{json, Value};
use std::time::Instant;

#[derive(Debug, Serialize)]
pub struct ProbeReport {
    pub rpc: String,
    pub client_version: Option<Value>,
    pub chain_id: Option<Value>,
    pub block_number: Option<Value>,
    pub methods: Vec<MethodProbe>,
}

pub async fn probe_jsonrpc(rpc: String, client: &JsonRpcClient) -> Result<ProbeReport> {
    let client_version = call_value(client, 1, "web3_clientVersion", json!([])).await.ok();
    let chain_id = call_value(client, 2, "eth_chainId", json!([])).await.ok();
    let block_number = call_value(client, 3, "eth_blockNumber", json!([])).await.ok();

    let methods = vec![
        probe_method(client, 10, "web3_clientVersion", json!([])).await,
        probe_method(client, 11, "net_version", json!([])).await,
        probe_method(client, 12, "eth_chainId", json!([])).await,
        probe_method(client, 13, "eth_blockNumber", json!([])).await,
        probe_method(client, 14, "eth_syncing", json!([])).await,
        probe_method(client, 15, "rpc.discover", json!([])).await,
    ];

    Ok(ProbeReport { rpc, client_version, chain_id, block_number, methods })
}

pub async fn catalog_jsonrpc(
    rpc: String,
    client: &JsonRpcClient,
    all: bool,
    extra_methods: Vec<String>,
) -> Result<ProbeReport> {
    let client_version = call_value(client, 1, "web3_clientVersion", json!([])).await.ok();
    let chain_id = call_value(client, 2, "eth_chainId", json!([])).await.ok();
    let block_number = call_value(client, 3, "eth_blockNumber", json!([])).await.ok();

    let mut methods = catalog_methods(all);
    methods.extend(extra_methods.into_iter().map(|method| (method, json!([]))));
    methods.sort_by(|a, b| a.0.cmp(&b.0));
    methods.dedup_by(|a, b| a.0 == b.0);

    let mut probes = Vec::with_capacity(methods.len());
    for (index, (method, params)) in methods.into_iter().enumerate() {
        probes.push(probe_method(client, 1_000 + index as u64, &method, params).await);
    }

    Ok(ProbeReport { rpc, client_version, chain_id, block_number, methods: probes })
}

pub async fn engine_exchange_capabilities(client: &JsonRpcClient) -> Result<Value> {
    call_value(client, 1, "engine_exchangeCapabilities", json!([[]])).await
}

async fn call_value(client: &JsonRpcClient, id: u64, method: &str, params: Value) -> Result<Value> {
    let response = client.call(id, method, params).await?;
    if let Some(error) = response.error {
        anyhow::bail!("{}: {}", error.code, error.message);
    }
    Ok(response.result.unwrap_or(Value::Null))
}

async fn probe_method(client: &JsonRpcClient, id: u64, method: &str, params: Value) -> MethodProbe {
    let started = Instant::now();
    let result = client.call(id, method, params).await;
    let latency_ms = started.elapsed().as_millis();
    match result {
        Ok(response) if response.error.is_none() => MethodProbe {
            method: method.to_string(),
            status: ProbeStatus::Supported,
            latency_ms,
            detail: None,
        },
        Ok(response) => {
            let error = response.error.expect("checked above");
            let status = classify_rpc_error(error.code, &error.message);
            MethodProbe {
                method: method.to_string(),
                status,
                latency_ms,
                detail: Some(format!("{}: {}", error.code, error.message)),
            }
        }
        Err(error) => MethodProbe {
            method: method.to_string(),
            status: ProbeStatus::TransportError,
            latency_ms,
            detail: Some(error.to_string()),
        },
    }
}

fn classify_rpc_error(code: i64, message: &str) -> ProbeStatus {
    let lower = message.to_ascii_lowercase();
    if code == -32601 || lower.contains("not found") || lower.contains("does not exist") {
        ProbeStatus::Unsupported
    } else if code == -32602 {
        ProbeStatus::InvalidParams
    } else if lower.contains("auth") || lower.contains("jwt") || lower.contains("unauthorized") {
        ProbeStatus::AuthRequired
    } else {
        ProbeStatus::ServerError
    }
}

fn catalog_methods(all: bool) -> Vec<(String, Value)> {
    let mut methods = vec![
        ("web3_clientVersion".to_string(), json!([])),
        ("web3_sha3".to_string(), json!(["0x68656c6c6f"])),
        ("net_version".to_string(), json!([])),
        ("net_listening".to_string(), json!([])),
        ("net_peerCount".to_string(), json!([])),
        ("eth_chainId".to_string(), json!([])),
        ("eth_blockNumber".to_string(), json!([])),
        ("eth_syncing".to_string(), json!([])),
        ("eth_gasPrice".to_string(), json!([])),
        ("eth_maxPriorityFeePerGas".to_string(), json!([])),
        ("eth_feeHistory".to_string(), json!([1, "latest", []])),
        ("eth_getBlockByNumber".to_string(), json!(["latest", false])),
        ("eth_getBlockTransactionCountByNumber".to_string(), json!(["latest"])),
        (
            "eth_getBalance".to_string(),
            json!(["0x0000000000000000000000000000000000000000", "latest"]),
        ),
        (
            "eth_getTransactionCount".to_string(),
            json!(["0x0000000000000000000000000000000000000000", "latest"]),
        ),
        (
            "eth_getCode".to_string(),
            json!(["0x0000000000000000000000000000000000000000", "latest"]),
        ),
        (
            "eth_getStorageAt".to_string(),
            json!(["0x0000000000000000000000000000000000000000", "0x0", "latest"]),
        ),
        (
            "eth_call".to_string(),
            json!([{ "to": "0x0000000000000000000000000000000000000000", "data": "0x" }, "latest"]),
        ),
        (
            "eth_estimateGas".to_string(),
            json!([{ "to": "0x0000000000000000000000000000000000000000", "data": "0x" }]),
        ),
        ("eth_getLogs".to_string(), json!([{ "fromBlock": "latest", "toBlock": "latest" }])),
        ("eth_simulateV1".to_string(), json!([{ "blockStateCalls": [{ "calls": [] }] }, "latest"])),
        ("rpc.discover".to_string(), json!([])),
    ];
    if all {
        methods.extend([
            ("debug_traceCall".to_string(), json!([{ "to": "0x0000000000000000000000000000000000000000", "data": "0x" }, "latest", { "tracer": "callTracer", "timeout": "10s" }])),
            ("debug_traceBlockByNumber".to_string(), json!(["latest", { "tracer": "callTracer", "timeout": "10s" }])),
            ("trace_block".to_string(), json!(["latest"])),
            ("trace_call".to_string(), json!([{ "to": "0x0000000000000000000000000000000000000000", "data": "0x" }, ["trace"], "latest"])),
            ("txpool_status".to_string(), json!([])),
            ("txpool_content".to_string(), json!([])),
            ("txpool_inspect".to_string(), json!([])),
            ("engine_exchangeCapabilities".to_string(), json!([[]])),
            ("engine_getPayloadV1".to_string(), json!(["0x0000000000000000"])),
            ("engine_getPayloadV2".to_string(), json!(["0x0000000000000000"])),
            ("engine_getPayloadV3".to_string(), json!(["0x0000000000000000"])),
            ("engine_getPayloadV4".to_string(), json!(["0x0000000000000000"])),
        ]);
    }
    methods
}
