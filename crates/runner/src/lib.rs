use anyhow::{Context, Result};
use client::{JsonRpcClient, JwtSigner};
use common::{BenchSummary, Config, LatencyHistogram, LatencySummary, MethodSummary, TimeSample};
use hdrhistogram::Histogram;
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::{
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{fs, task::JoinSet, time::Duration};

const MAX_ERROR_RECORDS: usize = 1_000;
const MAX_LATENCY_NS: u64 = 86_400_000_000_000;

#[derive(Debug, Clone)]
struct WorkItem {
    method: String,
    params: Value,
    weight: u64,
}

#[derive(Debug, Clone)]
struct WeightedWorkload {
    items: Vec<WorkItem>,
    cumulative_weights: Vec<u64>,
    total_weight: u64,
    skipped_methods: Vec<String>,
}

impl WeightedWorkload {
    fn select(&self, sequence: u64, seed: u64) -> WorkItem {
        let choice = splitmix64(sequence ^ seed) % self.total_weight;
        let index = self.cumulative_weights.partition_point(|weight| *weight <= choice);
        self.items[index].clone()
    }

    fn batch(&self, sequence: u64, size: usize, seed: u64) -> Vec<WorkItem> {
        (0..size).map(|offset| self.select(sequence.wrapping_add(offset as u64), seed)).collect()
    }

    fn manifest_methods(&self) -> Vec<Value> {
        self.items
            .iter()
            .map(|item| json!({ "method": item.method, "weight": item.weight }))
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
struct RateSchedule {
    start_rps: f64,
    end_rps: f64,
}

impl RateSchedule {
    fn at(self, progress: f64) -> f64 {
        self.start_rps + ((self.end_rps - self.start_rps) * progress.clamp(0.0, 1.0))
    }

    fn average(self) -> f64 {
        (self.start_rps + self.end_rps) / 2.0
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SeedData {
    pub latest_block: Option<String>,
    pub block_hash: Option<String>,
    pub tx_hash: Option<String>,
    pub address: Option<String>,
    pub call_to: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ErrorRecord {
    method: String,
    kind: String,
    detail: String,
    latency_ns: u128,
    latency_ms: u128,
}

#[derive(Clone)]
struct LatencyRecorder {
    histogram: Histogram<u64>,
    sum_ns: u128,
    count: u64,
}

impl Default for LatencyRecorder {
    fn default() -> Self {
        Self {
            histogram: Histogram::new_with_bounds(1, MAX_LATENCY_NS, 3)
                .expect("valid latency histogram bounds"),
            sum_ns: 0,
            count: 0,
        }
    }
}

impl LatencyRecorder {
    fn record(&mut self, latency_ns: u128) {
        let bounded = latency_ns.clamp(1, MAX_LATENCY_NS as u128) as u64;
        let _ = self.histogram.record(bounded);
        self.sum_ns = self.sum_ns.saturating_add(latency_ns);
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        self.histogram.add(&other.histogram).expect("matching latency histograms");
        self.sum_ns = self.sum_ns.saturating_add(other.sum_ns);
        self.count += other.count;
    }

    fn summary(&self) -> LatencySummary {
        if self.count == 0 {
            return LatencySummary::default();
        }
        let min_ns = self.histogram.min() as u128;
        let p50_ns = self.histogram.value_at_quantile(0.50) as u128;
        let p90_ns = self.histogram.value_at_quantile(0.90) as u128;
        let p95_ns = self.histogram.value_at_quantile(0.95) as u128;
        let p99_ns = self.histogram.value_at_quantile(0.99) as u128;
        let max_ns = self.histogram.max() as u128;
        LatencySummary {
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
            mean_ns: self.sum_ns / self.count as u128,
        }
    }
}

#[derive(Default, Clone)]
struct MethodMetrics {
    successes: u64,
    errors: u64,
    latency: LatencyRecorder,
}

#[derive(Default, Clone)]
struct SampleBucket {
    requests: u64,
    successes: u64,
    errors: u64,
    latency: LatencyRecorder,
}

#[derive(Default, Clone)]
struct Metrics {
    total: u64,
    successes: u64,
    rpc_errors: u64,
    transport_errors: u64,
    timeouts: u64,
    latency: LatencyRecorder,
    histogram: LatencyHistogram,
    methods: BTreeMap<String, MethodMetrics>,
    samples: BTreeMap<u64, SampleBucket>,
    errors: Vec<ErrorRecord>,
}

/// Shared, low-contention snapshot used by the live Prometheus endpoint.
#[derive(Clone)]
pub struct LiveMetrics {
    inner: Arc<Mutex<LiveState>>,
    started: Instant,
    started_unix_ms: u128,
    target: Arc<Mutex<String>>,
    requested_duration_ns: u128,
    requested_rps: Option<f64>,
    concurrency: usize,
    batch_size: usize,
}

struct LiveState {
    state: String,
    offered_requests: u64,
    dropped_requests: u64,
    total_requests: u64,
    successes: u64,
    rpc_errors: u64,
    transport_errors: u64,
    timeouts: u64,
    latency: LatencyRecorder,
    histogram: LatencyHistogram,
}

impl LiveMetrics {
    pub fn new(target: String, config: &Config) -> Result<Self> {
        let duration = effective_duration(config)?;
        let requested_rps =
            rate_schedule(config.bench.rps, config.bench.ramp.as_deref(), config.bench.max_rps)?
                .map(RateSchedule::average);
        Ok(Self {
            inner: Arc::new(Mutex::new(LiveState {
                state: "created".to_string(),
                offered_requests: 0,
                dropped_requests: 0,
                total_requests: 0,
                successes: 0,
                rpc_errors: 0,
                transport_errors: 0,
                timeouts: 0,
                latency: LatencyRecorder::default(),
                histogram: LatencyHistogram::default(),
            })),
            started: Instant::now(),
            started_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            target: Arc::new(Mutex::new(target)),
            requested_duration_ns: duration.as_nanos(),
            requested_rps,
            concurrency: config.bench.concurrency,
            batch_size: config.bench.batch_size,
        })
    }

    pub fn set_target(&self, target: String) {
        *self.target.lock().expect("live target mutex poisoned") = target;
    }

    pub fn state(&self) -> String {
        self.inner.lock().expect("live metrics mutex poisoned").state.clone()
    }

    fn mark_running(&self) {
        self.inner.lock().expect("live metrics mutex poisoned").state = "running".to_string();
    }

    fn mark_complete(&self) {
        self.inner.lock().expect("live metrics mutex poisoned").state = "complete".to_string();
    }

    fn mark_failed(&self) {
        self.inner.lock().expect("live metrics mutex poisoned").state = "failed".to_string();
    }

    fn add_offered(&self, count: u64) {
        self.inner.lock().expect("live metrics mutex poisoned").offered_requests += count;
    }

    fn add_dropped(&self, count: u64) {
        self.inner.lock().expect("live metrics mutex poisoned").dropped_requests += count;
    }

    fn record(&self, observation: &Observation) {
        let mut state = self.inner.lock().expect("live metrics mutex poisoned");
        state.total_requests += 1;
        state.latency.record(observation.latency_ns);
        record_histogram(&mut state.histogram, observation.latency_ns);
        match &observation.outcome {
            Outcome::Success => state.successes += 1,
            Outcome::RpcError(_) => state.rpc_errors += 1,
            Outcome::TransportError(detail)
                if detail.to_ascii_lowercase().starts_with("timeout:") =>
            {
                state.timeouts += 1;
            }
            Outcome::TransportError(_) => state.transport_errors += 1,
        }
    }

    pub fn snapshot(&self) -> BenchSummary {
        let state = self.inner.lock().expect("live metrics mutex poisoned");
        let elapsed = self.started.elapsed();
        let elapsed_seconds = elapsed.as_secs_f64().max(1e-9);
        let requests_per_second = state.total_requests as f64 / elapsed_seconds;
        BenchSummary {
            schema_version: 2,
            boom_version: env!("CARGO_PKG_VERSION").to_string(),
            target: self.target.lock().expect("live target mutex poisoned").clone(),
            duration_ms: elapsed.as_millis(),
            duration_ns: elapsed.as_nanos(),
            requested_duration_ns: self.requested_duration_ns,
            started_unix_ms: self.started_unix_ms,
            requested_rps: self.requested_rps,
            offered_requests: state.offered_requests,
            dropped_requests: state.dropped_requests,
            achieved_rate_ratio: self
                .requested_rps
                .map(|requested| requests_per_second / requested),
            concurrency: self.concurrency,
            batch_size: self.batch_size,
            seed: None,
            skipped_methods: Vec::new(),
            total_requests: state.total_requests,
            successes: state.successes,
            rpc_errors: state.rpc_errors,
            transport_errors: state.transport_errors,
            timeouts: state.timeouts,
            requests_per_second,
            latency: state.latency.summary(),
            histogram: state.histogram.clone(),
            samples: Vec::new(),
            methods: BTreeMap::new(),
        }
    }
}

impl Metrics {
    fn merge(&mut self, other: Self) {
        self.total += other.total;
        self.successes += other.successes;
        self.rpc_errors += other.rpc_errors;
        self.transport_errors += other.transport_errors;
        self.timeouts += other.timeouts;
        self.latency.merge(&other.latency);
        merge_histogram(&mut self.histogram, &other.histogram);
        for (name, source) in other.methods {
            let target = self.methods.entry(name).or_default();
            target.successes += source.successes;
            target.errors += source.errors;
            target.latency.merge(&source.latency);
        }
        for (second, source) in other.samples {
            let target = self.samples.entry(second).or_default();
            target.requests += source.requests;
            target.successes += source.successes;
            target.errors += source.errors;
            target.latency.merge(&source.latency);
        }
        for error in other.errors {
            if self.errors.len() == MAX_ERROR_RECORDS {
                break;
            }
            self.errors.push(error);
        }
    }

    fn record(&mut self, bench_started: Instant, observation: Observation) {
        let second = bench_started.elapsed().as_secs();
        self.total += 1;
        self.latency.record(observation.latency_ns);
        record_histogram(&mut self.histogram, observation.latency_ns);
        self.methods
            .entry(observation.method.clone())
            .or_default()
            .latency
            .record(observation.latency_ns);
        {
            let sample = self.samples.entry(second).or_default();
            sample.requests += 1;
            sample.latency.record(observation.latency_ns);
        }

        match observation.outcome {
            Outcome::Success => {
                self.successes += 1;
                self.methods.entry(observation.method).or_default().successes += 1;
                self.samples.entry(second).or_default().successes += 1;
            }
            Outcome::RpcError(detail) => {
                self.rpc_errors += 1;
                self.methods.entry(observation.method.clone()).or_default().errors += 1;
                self.samples.entry(second).or_default().errors += 1;
                self.push_error(observation.method, "rpc_error", detail, observation.latency_ns);
            }
            Outcome::TransportError(detail) => {
                if detail.to_ascii_lowercase().starts_with("timeout:") {
                    self.timeouts += 1;
                    self.push_error(
                        observation.method.clone(),
                        "timeout",
                        detail,
                        observation.latency_ns,
                    );
                } else {
                    self.transport_errors += 1;
                    self.push_error(
                        observation.method.clone(),
                        "transport_error",
                        detail,
                        observation.latency_ns,
                    );
                }
                self.methods.entry(observation.method).or_default().errors += 1;
                self.samples.entry(second).or_default().errors += 1;
            }
        }
    }

    fn push_error(&mut self, method: String, kind: &str, detail: String, latency_ns: u128) {
        if self.errors.len() < MAX_ERROR_RECORDS {
            self.errors.push(ErrorRecord {
                method,
                kind: kind.to_string(),
                detail: truncate(detail, 8_192),
                latency_ns,
                latency_ms: common::ns_to_ms(latency_ns),
            });
        }
    }
}

struct PhaseResult {
    metrics: Metrics,
    offered: u64,
    dropped: u64,
}

#[derive(Clone)]
struct RequestBudget {
    max_requests: Option<u64>,
    reserved: Arc<AtomicU64>,
}

impl RequestBudget {
    fn new(max_requests: Option<u64>) -> Self {
        Self { max_requests, reserved: Arc::new(AtomicU64::new(0)) }
    }

    fn reserve(&self, requested: usize) -> usize {
        let Some(max_requests) = self.max_requests else {
            return requested;
        };
        loop {
            let current = self.reserved.load(Ordering::Relaxed);
            if current >= max_requests {
                return 0;
            }
            let allowed = requested.min((max_requests - current) as usize);
            if self
                .reserved
                .compare_exchange(
                    current,
                    current + allowed as u64,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return allowed;
            }
        }
    }
}

struct BatchObservation {
    observations: Vec<Observation>,
}

struct Observation {
    method: String,
    outcome: Outcome,
    latency_ns: u128,
}

enum Outcome {
    Success,
    RpcError(String),
    TransportError(String),
}

pub async fn run_bench(
    target_name: String,
    rpc: String,
    config: Config,
    out_dir: impl AsRef<Path>,
) -> Result<BenchSummary> {
    run_bench_with_live(target_name, rpc, config, out_dir, None).await
}

pub async fn run_bench_with_live(
    target_name: String,
    rpc: String,
    config: Config,
    out_dir: impl AsRef<Path>,
    live: Option<LiveMetrics>,
) -> Result<BenchSummary> {
    common::validate_config(&config)?;
    anyhow::ensure!(
        !config.bench.dry_run,
        "bench.dry_run is enabled; inspect the plan with the CLI instead of running traffic"
    );
    let out_dir = out_dir.as_ref();
    fs::create_dir_all(out_dir).await?;
    anyhow::ensure!(
        !out_dir.join("run.json").exists(),
        "output directory {} already contains run.json; choose a new --out directory",
        out_dir.display()
    );

    let timeout = common::parse_duration(&config.bench.timeout)?;
    let duration = effective_duration(&config)?;
    let warmup = config
        .bench
        .warmup
        .as_deref()
        .map(common::parse_duration)
        .transpose()?
        .unwrap_or(Duration::ZERO);
    let rate = rate_schedule(config.bench.rps, config.bench.ramp.as_deref(), config.bench.max_rps)?;
    let budget = RequestBudget::new(config.bench.max_requests);
    let batch_size = config.bench.batch_size;
    let seed_value = config.bench.seed.unwrap_or(1);
    let started_unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();

    let target = config
        .targets
        .get(&target_name)
        .ok_or_else(|| anyhow::anyhow!("unknown target '{target_name}'"))?;
    anyhow::ensure!(
        config.bench.allow_public || is_private_endpoint(&rpc),
        "refusing public endpoint {rpc}; set --allow-public or bench.allow_public = true to opt in"
    );
    let target_label = target.label.clone().unwrap_or_else(|| target_name.clone());
    if let Some(live) = &live {
        live.set_target(target_label.clone());
        live.mark_running();
    }
    let client = Arc::new(build_client(&rpc, timeout, target)?);
    let seed = fetch_seed_data(&client).await.unwrap_or_default();
    write_atomic(out_dir.join("seed.json"), serde_json::to_vec_pretty(&seed)?).await?;
    let workload = Arc::new(build_workload(&config, &seed)?);

    write_manifest(out_dir, "running", &target_label, started_unix_ms, &config, &workload, None)
        .await?;

    if !warmup.is_zero() {
        if let Err(error) = run_phase(
            client.clone(),
            workload.clone(),
            config.bench.concurrency,
            warmup,
            batch_size,
            rate,
            seed_value,
            None,
            RequestBudget::new(None),
        )
        .await
        {
            let _ = write_manifest(
                out_dir,
                "failed",
                &target_label,
                started_unix_ms,
                &config,
                &workload,
                None,
            )
            .await;
            if let Some(live) = &live {
                live.mark_failed();
            }
            return Err(error);
        }
    }

    let started = Instant::now();
    let phase = match run_phase(
        client,
        workload.clone(),
        config.bench.concurrency,
        duration,
        batch_size,
        rate,
        seed_value,
        live.clone(),
        budget,
    )
    .await
    {
        Ok(phase) => phase,
        Err(error) => {
            let _ = write_manifest(
                out_dir,
                "failed",
                &target_label,
                started_unix_ms,
                &config,
                &workload,
                None,
            )
            .await;
            if let Some(live) = &live {
                live.mark_failed();
            }
            return Err(error);
        }
    };
    let elapsed = started.elapsed();
    let summary = build_summary(
        target_label.clone(),
        started_unix_ms,
        duration,
        elapsed,
        &config,
        rate,
        phase.offered,
        phase.dropped,
        workload.skipped_methods.clone(),
        &phase.metrics,
    );
    write_artifacts(out_dir, &summary, &phase.metrics).await?;
    write_manifest(
        out_dir,
        "complete",
        &target_label,
        started_unix_ms,
        &config,
        &workload,
        Some(&summary),
    )
    .await?;
    if let Some(live) = &live {
        live.mark_complete();
    }
    Ok(summary)
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioSummary {
    pub schema_version: u32,
    pub boom_version: String,
    pub scenario: String,
    pub target: String,
    pub iterations: usize,
    pub total_steps: u64,
    pub successes: u64,
    pub errors: u64,
    pub duration_ns: u128,
    pub steps: BTreeMap<String, ScenarioStepSummary>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ScenarioStepSummary {
    pub requests: u64,
    pub successes: u64,
    pub errors: u64,
    pub p50_ns: u128,
    pub p95_ns: u128,
    pub p99_ns: u128,
    pub last_error: Option<String>,
}

struct ScenarioStepState {
    requests: u64,
    successes: u64,
    errors: u64,
    latency: LatencyRecorder,
    last_error: Option<String>,
}

pub async fn run_scenario(
    target_name: String,
    rpc: String,
    config: Config,
    scenario_name: String,
    out_dir: impl AsRef<Path>,
    iterations_override: Option<usize>,
) -> Result<ScenarioSummary> {
    common::validate_config(&config)?;
    anyhow::ensure!(!config.bench.dry_run, "scenario cannot run while bench.dry_run is enabled");
    let scenario = config
        .scenarios
        .get(&scenario_name)
        .ok_or_else(|| anyhow::anyhow!("unknown scenario '{scenario_name}'"))?
        .clone();
    let iterations = iterations_override.unwrap_or(scenario.iterations);
    anyhow::ensure!(iterations > 0, "scenario iterations must be greater than zero");
    let target = config
        .targets
        .get(&target_name)
        .ok_or_else(|| anyhow::anyhow!("unknown target '{target_name}'"))?;
    anyhow::ensure!(
        config.bench.allow_public || is_private_endpoint(&rpc),
        "refusing public endpoint {rpc}; set --allow-public or bench.allow_public = true to opt in"
    );
    let target_label = target.label.clone().unwrap_or(target_name);
    let timeout = common::parse_duration(&config.bench.timeout)?;
    let client = build_client(&rpc, timeout, target)?;
    let started = Instant::now();
    let mut next_id = 1_u64;
    let mut total_steps = 0_u64;
    let mut successes = 0_u64;
    let mut errors = 0_u64;
    let mut completed_iterations = 0_usize;
    let mut steps = BTreeMap::<String, ScenarioStepState>::new();
    let max_duration = effective_duration(&config)?;

    'iterations: for _ in 0..iterations {
        let mut captures = BTreeMap::<String, Value>::new();
        for step in &scenario.steps {
            if started.elapsed() >= max_duration ||
                config.bench.max_requests.is_some_and(|max| total_steps >= max)
            {
                break 'iterations;
            }
            let params = resolve_scenario_value(step.params.clone(), &captures)
                .ok_or_else(|| anyhow::anyhow!("scenario capture is missing for {}", step.method));
            let params = match params {
                Ok(params) => params,
                Err(error) => {
                    let state =
                        steps.entry(step.method.clone()).or_insert_with(|| ScenarioStepState {
                            requests: 0,
                            successes: 0,
                            errors: 0,
                            latency: LatencyRecorder::default(),
                            last_error: None,
                        });
                    state.errors += 1;
                    state.last_error = Some(error.to_string());
                    errors += 1;
                    if !step.optional {
                        break;
                    }
                    continue;
                }
            };
            let step_started = Instant::now();
            let response = client.call(next_id, step.method.clone(), params).await;
            next_id = next_id.wrapping_add(1);
            let latency_ns = step_started.elapsed().as_nanos();
            total_steps += 1;
            let state = steps.entry(step.method.clone()).or_insert_with(|| ScenarioStepState {
                requests: 0,
                successes: 0,
                errors: 0,
                latency: LatencyRecorder::default(),
                last_error: None,
            });
            state.requests += 1;
            state.latency.record(latency_ns);
            match response {
                Ok(response) if response.error.is_none() => {
                    successes += 1;
                    state.successes += 1;
                    let response_value = serde_json::to_value(&response)?;
                    for (name, path) in &step.capture {
                        let value = value_at_path(&response_value, path).ok_or_else(|| {
                            anyhow::anyhow!("capture '{name}' path '{path}' was not found")
                        });
                        match value {
                            Ok(value) => {
                                captures.insert(name.clone(), value);
                            }
                            Err(error) => {
                                state.errors += 1;
                                state.last_error = Some(error.to_string());
                                errors += 1;
                                if !step.optional {
                                    break;
                                }
                            }
                        }
                    }
                }
                Ok(response) => {
                    errors += 1;
                    state.errors += 1;
                    state.last_error =
                        response.error.map(|error| format!("{}: {}", error.code, error.message));
                    if !step.optional {
                        break;
                    }
                }
                Err(error) => {
                    errors += 1;
                    state.errors += 1;
                    state.last_error = Some(error.to_string());
                    if !step.optional {
                        break;
                    }
                }
            }
        }
        completed_iterations += 1;
    }
    let step_summaries = steps
        .into_iter()
        .map(|(method, state)| {
            let latency = state.latency.summary();
            (
                method,
                ScenarioStepSummary {
                    requests: state.requests,
                    successes: state.successes,
                    errors: state.errors,
                    p50_ns: latency.p50_ns,
                    p95_ns: latency.p95_ns,
                    p99_ns: latency.p99_ns,
                    last_error: state.last_error,
                },
            )
        })
        .collect();
    let summary = ScenarioSummary {
        schema_version: 1,
        boom_version: env!("CARGO_PKG_VERSION").to_string(),
        scenario: scenario_name,
        target: target_label,
        iterations: completed_iterations,
        total_steps,
        successes,
        errors,
        duration_ns: started.elapsed().as_nanos(),
        steps: step_summaries,
    };
    let out_dir = out_dir.as_ref();
    fs::create_dir_all(out_dir).await?;
    write_atomic(out_dir.join("scenario.json"), serde_json::to_vec_pretty(&summary)?).await?;
    write_atomic(out_dir.join("scenario.md"), render_scenario_markdown(&summary).into_bytes())
        .await?;
    Ok(summary)
}

fn resolve_scenario_value(value: Value, captures: &BTreeMap<String, Value>) -> Option<Value> {
    match value {
        Value::String(value) if value.starts_with('$') => {
            let name = value.trim_start_matches('$');
            let (capture, path) =
                name.split_once('.').map_or((name, None), |(capture, path)| (capture, Some(path)));
            let value = captures.get(capture)?.clone();
            path.map_or(Some(value.clone()), |path| value_at_path(&value, path))
        }
        Value::Array(values) => values
            .into_iter()
            .map(|value| resolve_scenario_value(value, captures))
            .collect::<Option<Vec<_>>>()
            .map(Value::Array),
        Value::Object(values) => values
            .into_iter()
            .map(|(key, value)| resolve_scenario_value(value, captures).map(|value| (key, value)))
            .collect::<Option<Map<_, _>>>()
            .map(Value::Object),
        other => Some(other),
    }
}

fn value_at_path(value: &Value, path: &str) -> Option<Value> {
    let mut current = value;
    for segment in path.trim_matches('.').split('.') {
        if segment.is_empty() {
            continue;
        }
        current = current
            .get(segment)
            .or_else(|| segment.parse::<usize>().ok().and_then(|index| current.get(index)))?;
    }
    Some(current.clone())
}

fn render_scenario_markdown(summary: &ScenarioSummary) -> String {
    let mut out = format!(
        "# boom scenario: {}\n\n- target: {}\n- iterations: {}\n- steps: {}\n- successes: {}\n- errors: {}\n\n| method | requests | successes | errors | p50 | p95 | p99 |\n|---|---:|---:|---:|---:|---:|---:|\n",
        summary.scenario,
        summary.target,
        summary.iterations,
        summary.total_steps,
        summary.successes,
        summary.errors,
    );
    for (method, step) in &summary.steps {
        out.push_str(&format!(
            "| `{method}` | {} | {} | {} | {} ns | {} ns | {} ns |\n",
            step.requests, step.successes, step.errors, step.p50_ns, step.p95_ns, step.p99_ns
        ));
    }
    out
}

fn build_client(
    rpc: &str,
    timeout: Duration,
    target: &common::TargetConfig,
) -> Result<JsonRpcClient> {
    let mut headers = target.headers.clone();
    for (header, variable) in &target.header_env {
        let value = env::var(variable).with_context(|| {
            format!("reading environment variable {variable} for header {header}")
        })?;
        headers.insert(header.clone(), value);
    }
    let mut client = JsonRpcClient::new(rpc.to_string(), timeout)?.with_headers(&headers)?;
    let jwt = if let Some(variable) = &target.jwt_env {
        Some(
            env::var(variable)
                .with_context(|| format!("reading JWT environment variable {variable}"))?,
        )
    } else {
        target.jwt.clone()
    };
    if let Some(jwt) = jwt {
        client = client.with_jwt(JwtSigner::from_file_or_hex(&jwt)?);
    }
    Ok(client)
}

pub fn is_private_endpoint(endpoint: &str) -> bool {
    let authority = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest)
        .split('/')
        .next()
        .unwrap_or_default()
        .rsplit('@')
        .next()
        .unwrap_or_default();
    let host = authority
        .strip_prefix('[')
        .and_then(|value| value.split(']').next())
        .unwrap_or_else(|| authority.split(':').next().unwrap_or(authority))
        .to_ascii_lowercase();
    host == "localhost" ||
        host.ends_with(".local") ||
        host == "::1" ||
        host.starts_with("127.") ||
        host.starts_with("10.") ||
        host.starts_with("192.168.") ||
        host.starts_with("172.16.") ||
        host.starts_with("172.17.") ||
        host.starts_with("172.18.") ||
        host.starts_with("172.19.") ||
        host.starts_with("172.2") ||
        host.starts_with("172.30.") ||
        host.starts_with("172.31.") ||
        host.starts_with("fc") ||
        host.starts_with("fd") ||
        host.starts_with("fe80:")
}

fn effective_duration(config: &Config) -> Result<Duration> {
    let duration = common::parse_duration(&config.bench.duration)?;
    let max_duration =
        config.bench.max_duration.as_deref().map(common::parse_duration).transpose()?;
    Ok(max_duration.map_or(duration, |max| duration.min(max)))
}

fn rate_schedule(
    rps: Option<f64>,
    ramp: Option<&str>,
    max_rps: Option<f64>,
) -> Result<Option<RateSchedule>> {
    let cap = max_rps.unwrap_or(f64::INFINITY);
    if let Some(ramp) = ramp {
        let (start_rps, end_rps) = common::parse_ramp(ramp)?;
        return Ok(Some(RateSchedule { start_rps: start_rps.min(cap), end_rps: end_rps.min(cap) }));
    }
    Ok(rps.map(|rps| {
        let rps = rps.min(cap);
        RateSchedule { start_rps: rps, end_rps: rps }
    }))
}

async fn fetch_seed_data(client: &JsonRpcClient) -> Result<SeedData> {
    let latest_block = rpc_value(client, 9_000_001, "eth_blockNumber", Value::Array(vec![]))
        .await
        .ok()
        .and_then(|value| value.as_str().map(str::to_string));
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

    let block_hash = block.as_ref().and_then(|value| str_field(value, "hash"));
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

#[allow(clippy::too_many_arguments)]
async fn run_phase(
    client: Arc<JsonRpcClient>,
    workload: Arc<WeightedWorkload>,
    concurrency: usize,
    duration: Duration,
    batch_size: usize,
    rate: Option<RateSchedule>,
    seed: u64,
    live: Option<LiveMetrics>,
    budget: RequestBudget,
) -> Result<PhaseResult> {
    if let Some(rate) = rate {
        run_open_loop(client, workload, concurrency, duration, batch_size, rate, seed, live, budget)
            .await
    } else {
        run_closed_loop(client, workload, concurrency, duration, batch_size, seed, live, budget)
            .await
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_open_loop(
    client: Arc<JsonRpcClient>,
    workload: Arc<WeightedWorkload>,
    concurrency: usize,
    duration: Duration,
    batch_size: usize,
    rate: RateSchedule,
    seed: u64,
    live: Option<LiveMetrics>,
    budget: RequestBudget,
) -> Result<PhaseResult> {
    let started = Instant::now();
    let deadline = started + duration;
    let mut next_due = started;
    let mut sequence = 0_u64;
    let mut offered = 0_u64;
    let mut dropped = 0_u64;
    let mut metrics = Metrics::default();
    let mut tasks = JoinSet::new();

    while next_due < deadline {
        if next_due > Instant::now() {
            tokio::time::sleep_until(tokio::time::Instant::from_std(next_due)).await;
        }
        if Instant::now() >= deadline {
            break;
        }
        drain_ready(&mut tasks, &mut metrics, started, live.as_ref())?;
        let mut items = workload.batch(sequence, batch_size, seed);
        let allowed = budget.reserve(items.len());
        if allowed == 0 {
            break;
        }
        items.truncate(allowed);
        offered += items.len() as u64;
        if let Some(live) = &live {
            live.add_offered(items.len() as u64);
        }
        if tasks.len() < concurrency {
            let client = client.clone();
            let base_id = sequence.saturating_add(1);
            tasks.spawn(async move { execute_batch(client, items, base_id).await });
        } else {
            dropped += items.len() as u64;
            if let Some(live) = &live {
                live.add_dropped(items.len() as u64);
            }
        }
        sequence = sequence.wrapping_add(batch_size as u64);

        let progress = started.elapsed().as_secs_f64() / duration.as_secs_f64().max(1e-9);
        let logical_rps = rate.at(progress);
        let interval = batch_size as f64 / logical_rps;
        next_due += Duration::from_secs_f64(interval);
    }
    while let Some(result) = tasks.join_next().await {
        let batch = result??;
        for observation in batch.observations {
            if let Some(live) = &live {
                live.record(&observation);
            }
            metrics.record(started, observation);
        }
    }
    Ok(PhaseResult { metrics, offered, dropped })
}

fn drain_ready(
    tasks: &mut JoinSet<Result<BatchObservation>>,
    metrics: &mut Metrics,
    started: Instant,
    live: Option<&LiveMetrics>,
) -> Result<()> {
    while let Some(result) = tasks.try_join_next() {
        let batch = result??;
        for observation in batch.observations {
            if let Some(live) = live {
                live.record(&observation);
            }
            metrics.record(started, observation);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_closed_loop(
    client: Arc<JsonRpcClient>,
    workload: Arc<WeightedWorkload>,
    concurrency: usize,
    duration: Duration,
    batch_size: usize,
    seed: u64,
    live: Option<LiveMetrics>,
    budget: RequestBudget,
) -> Result<PhaseResult> {
    let started = Instant::now();
    let deadline = started + duration;
    let mut tasks = JoinSet::new();
    for worker in 0..concurrency {
        let client = client.clone();
        let workload = workload.clone();
        let live = live.clone();
        let budget = budget.clone();
        tasks.spawn(async move {
            let mut sequence = worker as u64 * batch_size as u64;
            let mut metrics = Metrics::default();
            let mut offered = 0_u64;
            while Instant::now() < deadline {
                let mut items = workload.batch(sequence, batch_size, seed);
                let allowed = budget.reserve(items.len());
                if allowed == 0 {
                    break;
                }
                items.truncate(allowed);
                offered += items.len() as u64;
                if let Some(live) = &live {
                    live.add_offered(items.len() as u64);
                }
                let batch =
                    execute_batch(client.clone(), items, sequence.saturating_add(1)).await?;
                for observation in batch.observations {
                    if let Some(live) = &live {
                        live.record(&observation);
                    }
                    metrics.record(started, observation);
                }
                sequence = sequence.wrapping_add(concurrency as u64 * batch_size as u64);
            }
            Result::<_>::Ok((metrics, offered))
        });
    }
    let mut metrics = Metrics::default();
    let mut offered = 0_u64;
    while let Some(result) = tasks.join_next().await {
        let (worker_metrics, worker_offered) = result??;
        metrics.merge(worker_metrics);
        offered += worker_offered;
    }
    Ok(PhaseResult { metrics, offered, dropped: 0 })
}

async fn execute_batch(
    client: Arc<JsonRpcClient>,
    items: Vec<WorkItem>,
    base_id: u64,
) -> Result<BatchObservation> {
    let started = Instant::now();
    if items.len() == 1 {
        let item = &items[0];
        let result = client.call(base_id, item.method.clone(), item.params.clone()).await;
        let latency_ns = started.elapsed().as_nanos();
        return Ok(BatchObservation {
            observations: vec![observation(item.method.clone(), result, latency_ns)],
        });
    }

    let requests = items
        .iter()
        .enumerate()
        .map(|(offset, item)| common::JsonRpcRequest {
            jsonrpc: "2.0",
            id: base_id.saturating_add(offset as u64),
            method: item.method.clone(),
            params: item.params.clone(),
        })
        .collect::<Vec<_>>();
    let result = client.call_batch(&requests).await;
    let latency_ns = started.elapsed().as_nanos();
    let observations = match result {
        Ok(responses) => items
            .into_iter()
            .zip(responses)
            .map(|(item, response)| observation(item.method, Ok(response), latency_ns))
            .collect(),
        Err(error) => {
            let detail = error.to_string();
            items
                .into_iter()
                .map(|item| Observation {
                    method: item.method,
                    outcome: Outcome::TransportError(detail.clone()),
                    latency_ns,
                })
                .collect()
        }
    };
    Ok(BatchObservation { observations })
}

fn observation(
    method: String,
    result: Result<common::JsonRpcResponse>,
    latency_ns: u128,
) -> Observation {
    let outcome = match result {
        Ok(response) => match response.error {
            None => Outcome::Success,
            Some(error) => Outcome::RpcError(format!("{}: {}", error.code, error.message)),
        },
        Err(error) => Outcome::TransportError(error.to_string()),
    };
    Observation { method, outcome, latency_ns }
}

fn build_workload(config: &Config, seed: &SeedData) -> Result<WeightedWorkload> {
    let mut items = Vec::new();
    let mut cumulative_weights = Vec::new();
    let mut total_weight = 0_u64;
    let mut skipped_methods = Vec::new();
    for (method, params, weight) in common::methods_or_default(config) {
        let Some(params) = resolve_placeholders(params, seed) else {
            skipped_methods.push(method);
            continue;
        };
        let weight = weight as u64;
        if weight == 0 {
            continue;
        }
        total_weight = total_weight
            .checked_add(weight)
            .ok_or_else(|| anyhow::anyhow!("workload weight overflow"))?;
        items.push(WorkItem { method, params, weight });
        cumulative_weights.push(total_weight);
    }
    anyhow::ensure!(
        total_weight > 0,
        "no runnable JSON-RPC workload after resolving live seed data"
    );
    Ok(WeightedWorkload { items, cumulative_weights, total_weight, skipped_methods })
}

fn resolve_placeholders(value: Value, seed: &SeedData) -> Option<Value> {
    match value {
        Value::String(value) if value.starts_with('$') => {
            placeholder(&value, seed).map(Value::String)
        }
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
    let transaction = block?.get("transactions")?.as_array()?.first()?;
    transaction.as_str().map(str::to_string).or_else(|| str_field(transaction, "hash"))
}

fn first_tx_address(block: Option<&Value>) -> Option<String> {
    let transaction =
        block?.get("transactions")?.as_array()?.iter().find(|value| value.is_object())?;
    str_field(transaction, "from").or_else(|| str_field(transaction, "to"))
}

fn first_tx_to(block: Option<&Value>) -> Option<String> {
    let transaction =
        block?.get("transactions")?.as_array()?.iter().find(|value| value.is_object())?;
    str_field(transaction, "to")
}

#[allow(clippy::too_many_arguments)]
fn build_summary(
    target: String,
    started_unix_ms: u128,
    requested_duration: Duration,
    elapsed: Duration,
    config: &Config,
    rate: Option<RateSchedule>,
    offered_requests: u64,
    dropped_requests: u64,
    skipped_methods: Vec<String>,
    metrics: &Metrics,
) -> BenchSummary {
    let latency = metrics.latency.summary();
    let methods = metrics
        .methods
        .iter()
        .map(|(name, metrics)| {
            let latency = metrics.latency.summary();
            (
                name.clone(),
                MethodSummary {
                    requests: metrics.successes + metrics.errors,
                    successes: metrics.successes,
                    errors: metrics.errors,
                    p50_ms: latency.p50_ms,
                    p90_ms: latency.p90_ms,
                    p95_ms: latency.p95_ms,
                    p99_ms: latency.p99_ms,
                    p50_ns: latency.p50_ns,
                    p90_ns: latency.p90_ns,
                    p95_ns: latency.p95_ns,
                    p99_ns: latency.p99_ns,
                },
            )
        })
        .collect();
    let samples = metrics
        .samples
        .iter()
        .map(|(second, sample)| {
            let latency = sample.latency.summary();
            TimeSample {
                second: *second,
                requests: sample.requests,
                successes: sample.successes,
                errors: sample.errors,
                p50_ms: latency.p50_ms,
                p95_ms: latency.p95_ms,
                p99_ms: latency.p99_ms,
                p50_ns: latency.p50_ns,
                p95_ns: latency.p95_ns,
                p99_ns: latency.p99_ns,
            }
        })
        .collect();
    let requested_seconds = requested_duration.as_secs_f64().max(1e-9);
    let requests_per_second = metrics.total as f64 / requested_seconds;
    let requested_rps = rate.map(RateSchedule::average);
    let achieved_rate_ratio = requested_rps.map(|requested| requests_per_second / requested);
    BenchSummary {
        schema_version: 2,
        boom_version: env!("CARGO_PKG_VERSION").to_string(),
        target,
        duration_ms: elapsed.as_millis(),
        duration_ns: elapsed.as_nanos(),
        requested_duration_ns: requested_duration.as_nanos(),
        started_unix_ms,
        requested_rps,
        offered_requests,
        dropped_requests,
        achieved_rate_ratio,
        concurrency: config.bench.concurrency,
        batch_size: config.bench.batch_size,
        seed: config.bench.seed,
        skipped_methods,
        total_requests: metrics.total,
        successes: metrics.successes,
        rpc_errors: metrics.rpc_errors,
        transport_errors: metrics.transport_errors,
        timeouts: metrics.timeouts,
        requests_per_second,
        latency,
        histogram: metrics.histogram.clone(),
        samples,
        methods,
    }
}

async fn write_artifacts(out_dir: &Path, summary: &BenchSummary, metrics: &Metrics) -> Result<()> {
    write_atomic(out_dir.join("run.json"), serde_json::to_vec_pretty(summary)?).await?;
    let mut csv = String::from(
        "method,requests,successes,errors,p50_ns,p90_ns,p95_ns,p99_ns,p50_ms,p90_ms,p95_ms,p99_ms\n",
    );
    for (method, summary) in &summary.methods {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{}\n",
            csv_escape(method),
            summary.requests,
            summary.successes,
            summary.errors,
            summary.p50_ns,
            summary.p90_ns,
            summary.p95_ns,
            summary.p99_ns,
            summary.p50_ms,
            summary.p90_ms,
            summary.p95_ms,
            summary.p99_ms,
        ));
    }
    write_atomic(out_dir.join("metrics.csv"), csv.into_bytes()).await?;

    let mut samples = String::from(
        "second,requests,successes,errors,p50_ns,p95_ns,p99_ns,p50_ms,p95_ms,p99_ms\n",
    );
    for sample in &summary.samples {
        samples.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{}\n",
            sample.second,
            sample.requests,
            sample.successes,
            sample.errors,
            sample.p50_ns,
            sample.p95_ns,
            sample.p99_ns,
            sample.p50_ms,
            sample.p95_ms,
            sample.p99_ms,
        ));
    }
    write_atomic(out_dir.join("samples.csv"), samples.into_bytes()).await?;
    let errors = metrics
        .errors
        .iter()
        .map(serde_json::to_string)
        .collect::<std::result::Result<Vec<_>, _>>()?
        .join("\n");
    write_atomic(out_dir.join("errors.jsonl"), errors.into_bytes()).await?;
    Ok(())
}

async fn write_manifest(
    out_dir: &Path,
    state: &str,
    target: &str,
    started_unix_ms: u128,
    config: &Config,
    workload: &WeightedWorkload,
    summary: Option<&BenchSummary>,
) -> Result<()> {
    let manifest = json!({
        "schema_version": 1,
        "state": state,
        "boom_version": env!("CARGO_PKG_VERSION"),
        "target": target,
        "started_unix_ms": started_unix_ms,
        "effective_bench": config.bench,
        "workload": workload.manifest_methods(),
        "skipped_methods": workload.skipped_methods,
        "result": summary.map(|summary| json!({
            "total_requests": summary.total_requests,
            "offered_requests": summary.offered_requests,
            "dropped_requests": summary.dropped_requests,
            "requests_per_second": summary.requests_per_second,
            "achieved_rate_ratio": summary.achieved_rate_ratio,
            "duration_ns": summary.duration_ns,
        })),
    });
    write_atomic(out_dir.join("manifest.json"), serde_json::to_vec_pretty(&manifest)?).await
}

async fn write_atomic(path: PathBuf, bytes: Vec<u8>) -> Result<()> {
    let extension = path.extension().and_then(|value| value.to_str()).unwrap_or("artifact");
    let temporary = path.with_extension(format!("{extension}.tmp"));
    fs::write(&temporary, bytes)
        .await
        .with_context(|| format!("writing temporary artifact {}", temporary.display()))?;
    match fs::rename(&temporary, &path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            // Windows does not replace an existing destination on rename. The
            // benchmark output directory is private to this run, so replacing
            // the prior manifest is safe and keeps the same helper portable.
            fs::remove_file(&path)
                .await
                .with_context(|| format!("replacing artifact {}", path.display()))?;
            fs::rename(&temporary, &path)
                .await
                .with_context(|| format!("committing artifact {}", path.display()))
        }
        Err(error) => Err(error).with_context(|| format!("committing artifact {}", path.display())),
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E3779B97F4A7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D049BB133111EB);
    value ^ (value >> 31)
}

fn record_histogram(histogram: &mut LatencyHistogram, latency_ns: u128) {
    match common::ns_to_ms_ceil(latency_ns) {
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

fn merge_histogram(target: &mut LatencyHistogram, source: &LatencyHistogram) {
    target.le_5_ms += source.le_5_ms;
    target.le_10_ms += source.le_10_ms;
    target.le_25_ms += source.le_25_ms;
    target.le_50_ms += source.le_50_ms;
    target.le_100_ms += source.le_100_ms;
    target.le_250_ms += source.le_250_ms;
    target.le_500_ms += source.le_500_ms;
    target.le_1000_ms += source.le_1000_ms;
    target.gt_1000_ms += source.gt_1000_ms;
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn truncate(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
    value.push('…');
    value
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
        let resolved = resolve_placeholders(value, &seed).expect("placeholders resolve");
        assert_eq!(resolved[0]["to"], "0x0000000000000000000000000000000000000002");
        assert_eq!(resolved[0]["nested"][0], "0x10");
        assert_eq!(resolved[1], "0xdef");
    }

    #[test]
    fn unresolved_placeholder_skips_method() {
        assert!(resolve_placeholders(json!(["$tx_hash"]), &SeedData::default()).is_none());
    }

    #[test]
    fn deterministic_selector_is_stable() {
        let workload = WeightedWorkload {
            items: vec![
                WorkItem { method: "a".into(), params: json!([]), weight: 1 },
                WorkItem { method: "b".into(), params: json!([]), weight: 3 },
            ],
            cumulative_weights: vec![1, 4],
            total_weight: 4,
            skipped_methods: Vec::new(),
        };
        let first = (0..100).map(|index| workload.select(index, 42).method).collect::<Vec<_>>();
        let second = (0..100).map(|index| workload.select(index, 42).method).collect::<Vec<_>>();
        assert_eq!(first, second);
        assert!(first.iter().filter(|method| method.as_str() == "b").count() > 50);
    }

    #[test]
    fn latency_recorder_preserves_sub_millisecond_values() {
        let mut recorder = LatencyRecorder::default();
        recorder.record(123_456);
        let summary = recorder.summary();
        assert!(summary.p50_ns >= 123_000);
        assert_eq!(summary.p50_ms, 0);
    }

    #[test]
    fn caps_error_records() {
        let mut metrics = Metrics::default();
        for index in 0..(MAX_ERROR_RECORDS + 10) {
            metrics.push_error("eth_blockNumber".into(), "rpc_error", index.to_string(), 1);
        }
        assert_eq!(metrics.errors.len(), MAX_ERROR_RECORDS);
    }

    #[test]
    fn live_snapshot_tracks_partial_results() {
        let config = common::config_for_rpc(
            "http://localhost:8545".into(),
            common::BenchConfig::default(),
            Vec::new(),
        );
        let live = LiveMetrics::new("target".into(), &config).unwrap();
        live.add_offered(2);
        live.record(&Observation {
            method: "eth_blockNumber".into(),
            outcome: Outcome::Success,
            latency_ns: 123_456,
        });
        live.mark_complete();
        let snapshot = live.snapshot();
        assert_eq!(snapshot.offered_requests, 2);
        assert_eq!(snapshot.successes, 1);
        assert!((123_000..=124_000).contains(&snapshot.latency.p50_ns));
        assert_eq!(live.state(), "complete");
    }

    #[test]
    fn scenario_capture_resolves_nested_values() {
        let captures = BTreeMap::from([("block".to_string(), json!({"number": "0x10"}))]);
        let value = resolve_scenario_value(json!(["$block.number"]), &captures).unwrap();
        assert_eq!(value, json!(["0x10"]));
        assert_eq!(
            value_at_path(&json!({"result":{"hash":"0xabc"}}), "result.hash"),
            Some(json!("0xabc"))
        );
    }

    #[test]
    fn public_endpoint_guard_distinguishes_private_hosts() {
        assert!(is_private_endpoint("http://localhost:8545"));
        assert!(is_private_endpoint("http://192.168.1.10:8545"));
        assert!(!is_private_endpoint("https://rpc.example.com"));
    }
}
