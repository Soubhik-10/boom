# Boom Config Guide

This guide documents Boom `config.toml` files: target definitions, benchmark settings, method weights, params, placeholders, and review practices.

Use a config file when a benchmark needs to be reviewed, repeated, or shared. CLI flags are good for quick runs; TOML is better for production benchmark plans because it records the target, load profile, method mix, params, and method weights in one file.

## Minimal Config

```toml
[targets.local]
rpc = "http://localhost:8545"
label = "Local execution RPC"

[bench]
duration = "30s"
warmup = "3s"
concurrency = 64
timeout = "10s"
batch_size = 1
rps = 250

[json_rpc.eth_blockNumber]
weight = 10
params = []
readonly = true

[json_rpc.eth_getBlockByNumber]
weight = 4
params = ["$latest_block", false]
readonly = true
```

Run it with:

```bash
boom bench --config configs/examples/mini-eth.toml --out runs/mini-eth
```

## Top-Level Sections

| Section | Required | Purpose |
|---|---:|---|
| `[targets.<name>]` | Yes | Defines one named RPC/Engine target. `boom bench --config` uses the first target with an `rpc` URL. |
| `[bench]` | No | Defines duration, warmup, concurrency, timeout, batching, and rate controls. Defaults are used when omitted. |
| `[json_rpc.<method>]` | No | Defines custom JSON-RPC methods, params, weights, and metadata. If omitted, Boom uses the default ETH workload. |

## Target Fields

| Field | Type | Purpose |
|---|---|---|
| `rpc` | string | HTTP JSON-RPC endpoint used by `boom bench`. |
| `engine` | string | Engine API endpoint used by Engine-oriented workflows. |
| `jwt` | string | Hex JWT secret or path to a JWT secret file. |
| `label` | string | Human-readable target label. |

## Bench Fields

| Field | Type | Default | Purpose |
|---|---|---:|---|
| `duration` | duration string | `30s` | Measured benchmark duration. Supports `ms`, `s`, `m`, or bare seconds. |
| `warmup` | duration string | `0s` | Warmup period before measured samples are recorded. |
| `concurrency` | integer | `64` | Number of async workers issuing requests. |
| `timeout` | duration string | `10s` | Per-request timeout. |
| `batch_size` | integer | `1` | Number of logical JSON-RPC calls per HTTP batch request. |
| `seed` | integer | unset | Reserved for deterministic workload generation. |
| `rps` | number | unset | Fixed logical request rate target. |
| `ramp` | string | unset | Linear rate ramp, formatted as `START:END`, for example `100:1000`. |

## Method Fields

| Field | Type | Default | Purpose |
|---|---|---:|---|
| `weight` | integer | `1` | Relative frequency. A method with weight `10` is scheduled roughly twice as often as one with weight `5`. Set `0` to disable a method without deleting it. |
| `params` | TOML value | `null` | JSON-RPC params. Use TOML arrays/tables to describe JSON arrays/objects. |
| `compare` | string | unset | Reserved metadata for comparison behavior. |
| `readonly` | boolean | unset | Documentation/intent flag. Boom does not mutate requests based on this flag. |

## Live Placeholders

| Placeholder | Resolved From |
|---|---|
| `$latest_block` | `eth_blockNumber`. |
| `$block_hash` | `eth_getBlockByNumber($latest_block, false).hash`. |
| `$tx_hash` | First transaction hash found from seeded block data. |
| `$address` | Address found from seeded transaction data, or zero address fallback. |
| `$call_to` | Transaction recipient from seeded data, or address fallback. |

Placeholder rules:

- Placeholders can appear anywhere inside `params`, including nested tables and arrays.
- If a placeholder cannot be resolved, that method is skipped for the run.
- Use placeholders for methods that need real block, transaction, or account data, such as `eth_getTransactionByHash`, `eth_getTransactionReceipt`, `debug_traceTransaction`, and `trace_transaction`.

## Weighted ETH Example

```toml
[targets.local]
rpc = "http://localhost:8545"

[bench]
duration = "2m"
warmup = "10s"
concurrency = 128
rps = 500
timeout = "15s"
batch_size = 1

[json_rpc.eth_blockNumber]
weight = 20
params = []
readonly = true

[json_rpc.eth_getBlockByNumber]
weight = 12
params = ["$latest_block", false]
readonly = true

[json_rpc.eth_getTransactionReceipt]
weight = 10
params = ["$tx_hash"]
readonly = true

[json_rpc.eth_call]
weight = 8
params = [{ to = "$call_to", data = "0x" }, "latest"]
readonly = true

[json_rpc.eth_getLogs]
weight = 2
params = [{ fromBlock = "$latest_block", toBlock = "$latest_block" }]
readonly = true
```

## Archive-Style Example

```toml
[targets.archive]
rpc = "http://localhost:8545"
label = "Archive node"

[bench]
duration = "5m"
warmup = "30s"
concurrency = 256
ramp = "100:1000"
timeout = "30s"

[json_rpc.eth_getBalance]
weight = 5
params = ["$address", "$latest_block"]
readonly = true

[json_rpc.eth_getCode]
weight = 5
params = ["$address", "$latest_block"]
readonly = true

[json_rpc.eth_getStorageAt]
weight = 5
params = ["$address", "0x0", "$latest_block"]
readonly = true
```

## Debug/Trace Example

```toml
[targets.debug]
rpc = "http://localhost:8545"

[bench]
duration = "60s"
concurrency = 32
timeout = "30s"
rps = 20

[json_rpc.debug_traceTransaction]
weight = 1
params = ["$tx_hash", { tracer = "callTracer", timeout = "10s" }]
readonly = true

[json_rpc.debug_traceCall]
weight = 1
params = [{ to = "$call_to", data = "0x" }, "latest", { tracer = "callTracer", timeout = "10s" }]
readonly = true
```

## Engine Target Metadata Example

```toml
[targets.engine_local]
rpc = "http://localhost:8545"
engine = "http://localhost:8551"
jwt = "./jwt.hex"
label = "Local execution and Engine APIs"
```

## Review Checklist

- Keep public or shared RPC endpoints out of committed configs unless you have permission to benchmark them.
- Prefer `rps` for reproducible capacity tests; use pure `concurrency` mode when you intentionally want open-loop pressure.
- Set `timeout` long enough for slow debug/archive methods, but short enough to expose degraded behavior.
- Keep `debug_*` and `trace_*` workloads separate from normal ETH read workloads when you want clean numbers.
- Use `weight = 0` to keep a method documented but disabled.
- Store generated outputs in a unique `--out` directory for each run.
