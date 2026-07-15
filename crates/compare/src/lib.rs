use anyhow::Result;
use common::BenchSummary;
use serde::Serialize;
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize)]
pub struct CompareReport {
    pub left: BenchSummary,
    pub right: BenchSummary,
    pub rps_delta_pct: f64,
    pub p95_delta_pct: f64,
    pub success_delta_pct: f64,
    pub methods: Vec<MethodCompare>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MethodCompare {
    pub method: String,
    pub left_p95_ms: u128,
    pub right_p95_ms: u128,
    pub left_p95_ns: u128,
    pub right_p95_ns: u128,
    pub p95_delta_pct: Option<f64>,
    pub left_success_pct: Option<f64>,
    pub right_success_pct: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct CompareArtifacts {
    pub json: PathBuf,
    pub markdown: PathBuf,
    pub html: PathBuf,
}

#[derive(Debug, Clone, Copy)]
pub struct RegressionLimits {
    pub max_p95_regression_pct: f64,
    pub max_error_rate_delta_pct: f64,
    pub min_throughput_ratio: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegressionReport {
    pub baseline_target: String,
    pub current_target: String,
    pub baseline_requests_per_second: f64,
    pub current_requests_per_second: f64,
    pub throughput_ratio: f64,
    pub baseline_p95_ns: u128,
    pub current_p95_ns: u128,
    pub p95_regression_pct: f64,
    pub baseline_error_rate_pct: f64,
    pub current_error_rate_pct: f64,
    pub error_rate_delta_pct: f64,
    pub limits: RegressionLimitsReport,
    pub passed: bool,
    pub failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegressionLimitsReport {
    pub max_p95_regression_pct: f64,
    pub max_error_rate_delta_pct: f64,
    pub min_throughput_ratio: f64,
}

pub fn build_regression(
    baseline: &BenchSummary,
    current: &BenchSummary,
    limits: RegressionLimits,
) -> RegressionReport {
    let baseline_p95_ns = effective_ns(baseline.latency.p95_ns, baseline.latency.p95_ms);
    let current_p95_ns = effective_ns(current.latency.p95_ns, current.latency.p95_ms);
    let p95_regression_pct = regression_pct(baseline_p95_ns as f64, current_p95_ns as f64);
    let throughput_ratio = if baseline.requests_per_second > 0.0 {
        current.requests_per_second / baseline.requests_per_second
    } else if current.requests_per_second > 0.0 {
        f64::INFINITY
    } else {
        1.0
    };
    let baseline_error_rate_pct = error_pct(baseline);
    let current_error_rate_pct = error_pct(current);
    let error_rate_delta_pct = current_error_rate_pct - baseline_error_rate_pct;
    let mut failures = Vec::new();
    if p95_regression_pct > limits.max_p95_regression_pct {
        failures.push(format!(
            "p95 regressed by {p95_regression_pct:.2}% (limit {:.2}%)",
            limits.max_p95_regression_pct
        ));
    }
    if error_rate_delta_pct > limits.max_error_rate_delta_pct {
        failures.push(format!(
            "error rate increased by {error_rate_delta_pct:.2} percentage points (limit {:.2})",
            limits.max_error_rate_delta_pct
        ));
    }
    if throughput_ratio < limits.min_throughput_ratio {
        failures.push(format!(
            "throughput is {:.2}% of baseline (minimum {:.2}%)",
            throughput_ratio * 100.0,
            limits.min_throughput_ratio * 100.0
        ));
    }
    RegressionReport {
        baseline_target: baseline.target.clone(),
        current_target: current.target.clone(),
        baseline_requests_per_second: baseline.requests_per_second,
        current_requests_per_second: current.requests_per_second,
        throughput_ratio,
        baseline_p95_ns,
        current_p95_ns,
        p95_regression_pct,
        baseline_error_rate_pct,
        current_error_rate_pct,
        error_rate_delta_pct,
        limits: RegressionLimitsReport {
            max_p95_regression_pct: limits.max_p95_regression_pct,
            max_error_rate_delta_pct: limits.max_error_rate_delta_pct,
            min_throughput_ratio: limits.min_throughput_ratio,
        },
        passed: failures.is_empty(),
        failures,
    }
}

pub fn render_regression_markdown(report: &RegressionReport) -> String {
    let status = if report.passed { "PASS" } else { "FAIL" };
    let mut out = format!(
        "# boom regression gate: {status}\n\n- baseline: {}\n- current: {}\n- throughput: {:.2}% of baseline\n- p95 regression: {:.2}%\n- error-rate delta: {:.2} percentage points\n",
        report.baseline_target,
        report.current_target,
        report.throughput_ratio * 100.0,
        report.p95_regression_pct,
        report.error_rate_delta_pct,
    );
    if !report.failures.is_empty() {
        out.push_str("\nFailures:\n");
        for failure in &report.failures {
            out.push_str(&format!("- {failure}\n"));
        }
    }
    out
}

fn error_pct(summary: &BenchSummary) -> f64 {
    let errors = summary.rpc_errors + summary.transport_errors + summary.timeouts;
    if summary.total_requests == 0 {
        0.0
    } else {
        errors as f64 / summary.total_requests as f64 * 100.0
    }
}

fn regression_pct(baseline: f64, current: f64) -> f64 {
    if baseline <= 0.0 {
        if current > 0.0 {
            100.0
        } else {
            0.0
        }
    } else {
        ((current - baseline) / baseline * 100.0).max(0.0)
    }
}

pub fn build_compare(left: BenchSummary, right: BenchSummary) -> CompareReport {
    let rps_delta_pct = delta_pct(left.requests_per_second, right.requests_per_second);
    let p95_delta_pct = delta_pct(
        effective_ns(left.latency.p95_ns, left.latency.p95_ms) as f64,
        effective_ns(right.latency.p95_ns, right.latency.p95_ms) as f64,
    );
    let success_delta_pct = delta_pct(success_pct(&left), success_pct(&right));
    let mut method_names = BTreeSet::new();
    method_names.extend(left.methods.keys().cloned());
    method_names.extend(right.methods.keys().cloned());
    let methods = method_names
        .into_iter()
        .map(|method| {
            let l = left.methods.get(&method).cloned().unwrap_or_default();
            let r = right.methods.get(&method).cloned().unwrap_or_default();
            let left_present = left.methods.contains_key(&method) && l.requests > 0;
            let right_present = right.methods.contains_key(&method) && r.requests > 0;
            let left_ns = effective_ns(l.p95_ns, l.p95_ms);
            let right_ns = effective_ns(r.p95_ns, r.p95_ms);
            MethodCompare {
                method,
                left_p95_ms: l.p95_ms,
                right_p95_ms: r.p95_ms,
                left_p95_ns: left_ns,
                right_p95_ns: right_ns,
                p95_delta_pct: (left_present && right_present)
                    .then(|| delta_pct(left_ns as f64, right_ns as f64)),
                left_success_pct: left_present.then(|| pct(l.successes, l.requests)),
                right_success_pct: right_present.then(|| pct(r.successes, r.requests)),
            }
        })
        .collect();
    CompareReport { left, right, rps_delta_pct, p95_delta_pct, success_delta_pct, methods }
}

pub fn write_compare(
    report: &CompareReport,
    out_dir: impl AsRef<Path>,
) -> Result<CompareArtifacts> {
    let out_dir = out_dir.as_ref();
    fs::create_dir_all(out_dir)?;
    let json = out_dir.join("compare.json");
    let markdown = out_dir.join("compare.md");
    let html = out_dir.join("compare.html");
    fs::write(&json, serde_json::to_vec_pretty(report)?)?;
    fs::write(&markdown, render_markdown(report))?;
    fs::write(&html, render_html(report))?;
    Ok(CompareArtifacts { json, markdown, html })
}

fn render_markdown(report: &CompareReport) -> String {
    let mut out = String::new();
    out.push_str(&format!("# boom compare: {} vs {}\n\n", report.left.target, report.right.target));
    out.push_str(&format!("- RPS delta: {:.2}%\n", report.rps_delta_pct));
    out.push_str(&format!("- p95 delta: {:.2}%\n", report.p95_delta_pct));
    out.push_str(&format!("- success delta: {:.2}%\n\n", report.success_delta_pct));
    out.push_str("| method | left p95 | right p95 | p95 delta | left success | right success |\n");
    out.push_str("|---|---:|---:|---:|---:|---:|\n");
    for method in &report.methods {
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} |\n",
            method.method,
            format_latency(method.left_p95_ns),
            format_latency(method.right_p95_ns),
            format_optional_pct(method.p95_delta_pct),
            format_optional_pct(method.left_success_pct),
            format_optional_pct(method.right_success_pct),
        ));
    }
    out
}

fn render_html(report: &CompareReport) -> String {
    let rows = report.methods.iter().map(|m| {
        format!("<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>", escape(&m.method), format_latency(m.left_p95_ns), format_latency(m.right_p95_ns), format_optional_pct(m.p95_delta_pct), format_optional_pct(m.left_success_pct), format_optional_pct(m.right_success_pct))
    }).collect::<Vec<_>>().join("\n");
    format!(
        r#"<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>boom compare</title><style>body{{font-family:Inter,ui-sans-serif,system-ui;margin:0;background:#f5f5f2;color:#171817}}main{{max-width:1180px;margin:0 auto;padding:28px}}.cards{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin:18px 0}}.card,table{{background:#fff;border:1px solid #d8ddd5;border-radius:8px}}.card{{padding:16px}}.card span{{color:#697069;font-size:12px;text-transform:uppercase}}.card strong{{display:block;font-size:28px;margin-top:8px}}table{{border-collapse:collapse;width:100%;overflow:hidden}}th,td{{padding:11px 12px;border-bottom:1px solid #eceee9;text-align:right}}th:first-child,td:first-child{{text-align:left}}code{{font-family:Consolas,monospace;font-size:12px}}@media(max-width:800px){{.cards{{grid-template-columns:1fr}}main{{padding:16px}}}}</style></head><body><main><h1>boom compare</h1><p>{left} vs {right}</p><section class="cards"><div class="card"><span>RPS delta</span><strong>{rps:.2}%</strong></div><div class="card"><span>p95 delta</span><strong>{p95:.2}%</strong></div><div class="card"><span>success delta</span><strong>{success:.2}%</strong></div></section><table><thead><tr><th>method</th><th>left p95</th><th>right p95</th><th>p95 delta</th><th>left success</th><th>right success</th></tr></thead><tbody>{rows}</tbody></table></main></body></html>"#,
        left = escape(&report.left.target),
        right = escape(&report.right.target),
        rps = report.rps_delta_pct,
        p95 = report.p95_delta_pct,
        success = report.success_delta_pct,
        rows = rows,
    )
}

fn success_pct(summary: &BenchSummary) -> f64 {
    pct(summary.successes, summary.total_requests)
}
fn pct(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 / total as f64 * 100.0
    }
}
fn delta_pct(left: f64, right: f64) -> f64 {
    if left == 0.0 {
        0.0
    } else {
        (right - left) / left * 100.0
    }
}
fn effective_ns(ns: u128, ms: u128) -> u128 {
    if ns == 0 && ms > 0 {
        ms * 1_000_000
    } else {
        ns
    }
}
fn format_latency(ns: u128) -> String {
    if ns == 0 {
        "n/a".to_string()
    } else if ns < 1_000_000 {
        format!("{:.2} \u{00b5}s", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2} s", ns as f64 / 1_000_000_000.0)
    }
}
fn format_optional_pct(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_string(), |value| format!("{value:.2}%"))
}
fn escape(input: &str) -> String {
    input.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}
