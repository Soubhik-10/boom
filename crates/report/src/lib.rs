use anyhow::{Context, Result};
use common::{BenchSummary, MethodSummary};
use serde::Deserialize;
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone)]
pub struct ReportArtifacts {
    pub summary_md: PathBuf,
    pub html: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
struct ErrorSample {
    method: String,
    kind: String,
    detail: String,
    latency_ms: u128,
    #[serde(default)]
    latency_ns: u128,
}

pub async fn write_report(run: impl AsRef<Path>, out: Option<String>) -> Result<ReportArtifacts> {
    write_report_sync(run, out)
}

pub fn write_openmetrics(run: impl AsRef<Path>, out: Option<String>) -> Result<PathBuf> {
    let run = run.as_ref();
    let run_json = if run.is_dir() { run.join("run.json") } else { run.to_path_buf() };
    let raw = fs::read_to_string(&run_json)
        .with_context(|| format!("reading run summary {}", run_json.display()))?;
    let summary: BenchSummary = serde_json::from_str(&raw)
        .with_context(|| format!("parsing run summary {}", run_json.display()))?;
    let out_path = out.map(PathBuf::from).unwrap_or_else(|| {
        if run.is_dir() {
            run.join("openmetrics.prom")
        } else {
            run.with_extension("prom")
        }
    });
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&out_path, render_openmetrics(&summary))?;
    Ok(out_path)
}

fn write_report_sync(run: impl AsRef<Path>, out: Option<String>) -> Result<ReportArtifacts> {
    let run = run.as_ref();
    let run_json = if run.is_dir() { run.join("run.json") } else { run.to_path_buf() };
    let raw = fs::read_to_string(&run_json)
        .with_context(|| format!("reading run summary {}", run_json.display()))?;
    let summary: BenchSummary = serde_json::from_str(&raw)
        .with_context(|| format!("parsing run summary {}", run_json.display()))?;
    let errors =
        if run.is_dir() { read_error_samples(&run.join("errors.jsonl"))? } else { Vec::new() };

    let out_dir = out.map(PathBuf::from).unwrap_or_else(|| {
        if run.is_dir() {
            run.to_path_buf()
        } else {
            run.parent().unwrap_or_else(|| Path::new(".")).to_path_buf()
        }
    });
    fs::create_dir_all(&out_dir)?;

    let summary_md = out_dir.join("summary.md");
    let html = out_dir.join("report.html");
    fs::write(&summary_md, render_markdown(&summary))?;
    fs::write(&html, render_html(&summary, &errors))?;
    Ok(ReportArtifacts { summary_md, html })
}

fn read_error_samples(path: &Path) -> Result<Vec<ErrorSample>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(raw
        .lines()
        .filter(|line| !line.trim().is_empty())
        .take(200)
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect())
}

pub fn render_markdown(summary: &BenchSummary) -> String {
    let mut out = String::new();
    out.push_str(&format!("# boom report: {}\n\n", summary.target));
    out.push_str(&format!("- requests: {}\n", summary.total_requests));
    out.push_str(&format!("- successes: {}\n", summary.successes));
    out.push_str(&format!("- rpc errors: {}\n", summary.rpc_errors));
    out.push_str(&format!("- transport errors: {}\n", summary.transport_errors));
    out.push_str(&format!("- timeouts: {}\n", summary.timeouts));
    out.push_str(&format!("- rps: {:.2}\n", summary.requests_per_second));
    out.push_str(&format!(
        "- p50/p95/p99: {}/{}/{}\n",
        format_latency(summary.latency.p50_ns, summary.latency.p50_ms),
        format_latency(summary.latency.p95_ns, summary.latency.p95_ms),
        format_latency(summary.latency.p99_ns, summary.latency.p99_ms),
    ));
    if let Some(target_rps) = summary.requested_rps {
        out.push_str(&format!(
            "- target delivery: {:.2}% ({:.2} requested RPS, {} dropped)\n",
            summary.achieved_rate_ratio.unwrap_or(0.0) * 100.0,
            target_rps,
            summary.dropped_requests,
        ));
    }
    if !summary.skipped_methods.is_empty() {
        out.push_str(&format!("- skipped methods: {}\n", summary.skipped_methods.join(", ")));
    }
    out.push('\n');
    out.push_str("| method | requests | ok | errors | success | p50 | p95 | p99 |\n");
    out.push_str("|---|---:|---:|---:|---:|---:|---:|---:|\n");
    for (method, m) in method_rows(summary) {
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {:.2}% | {} | {} | {} |\n",
            method,
            m.requests,
            m.successes,
            m.errors,
            percent(m.successes, m.requests),
            format_latency(m.p50_ns, m.p50_ms),
            format_latency(m.p95_ns, m.p95_ms),
            format_latency(m.p99_ns, m.p99_ms),
        ));
    }
    out
}

pub fn render_terminal_summary(summary: &BenchSummary) -> String {
    let success_rate = percent(summary.successes, summary.total_requests);
    let error_total = summary.rpc_errors + summary.transport_errors + summary.timeouts;
    let mut out = String::new();
    out.push_str("\nboom results\n");
    out.push_str("============\n");
    out.push_str(&format!("target: {}\n", summary.target));
    out.push_str(&format!(
        "requests: {} total, {} ok, {} failed ({:.2}% success)\n",
        summary.total_requests, summary.successes, error_total, success_rate
    ));
    out.push_str(&format!("throughput: {:.2} req/s\n", summary.requests_per_second));
    out.push_str(&format!(
        "latency: min {} | p50 {} | p95 {} | p99 {} | max {}\n",
        format_latency(summary.latency.min_ns, summary.latency.min_ms),
        format_latency(summary.latency.p50_ns, summary.latency.p50_ms),
        format_latency(summary.latency.p95_ns, summary.latency.p95_ms),
        format_latency(summary.latency.p99_ns, summary.latency.p99_ms),
        format_latency(summary.latency.max_ns, summary.latency.max_ms),
    ));
    out.push_str(&format!(
        "errors: rpc {} | transport {} | timeout {}\n",
        summary.rpc_errors, summary.transport_errors, summary.timeouts
    ));
    if let Some(requested_rps) = summary.requested_rps {
        out.push_str(&format!(
            "load delivery: {:.2}% of {:.2} req/s | offered {} | dropped {}\n",
            summary.achieved_rate_ratio.unwrap_or(0.0) * 100.0,
            requested_rps,
            summary.offered_requests,
            summary.dropped_requests,
        ));
    }
    if !summary.skipped_methods.is_empty() {
        out.push_str(&format!("skipped methods: {}\n", summary.skipped_methods.join(", ")));
    }

    let slow = slowest_methods(summary, 5);
    if !slow.is_empty() {
        out.push_str("\nslowest methods by p95\n");
        for (method, m) in &slow {
            out.push_str(&format!(
                "  {:32} p50 {:>10}  p95 {:>10}  p99 {:>10}\n",
                method,
                format_latency(m.p50_ns, m.p50_ms),
                format_latency(m.p95_ns, m.p95_ms),
                format_latency(m.p99_ns, m.p99_ms),
            ));
        }
    }

    let noisy = error_methods(summary, 5);
    if !noisy.is_empty() {
        out.push_str("\nerror hotspots\n");
        for (method, m) in &noisy {
            out.push_str(&format!(
                "  {:32} {:>5} errors / {:>5} requests ({:.2}%)\n",
                method,
                m.errors,
                m.requests,
                percent(m.errors, m.requests)
            ));
        }
    }

    out
}

pub fn render_openmetrics(summary: &BenchSummary) -> String {
    let mut out = String::new();
    let target = label_value(&summary.target);
    out.push_str("# HELP boom_requests_total Completed logical JSON-RPC requests in the run.\n");
    out.push_str("# TYPE boom_requests_total counter\n");
    out.push_str(&format!(
        "boom_requests_total{{target=\"{target}\",status=\"success\"}} {}\n",
        summary.successes
    ));
    out.push_str(&format!(
        "boom_requests_total{{target=\"{target}\",status=\"rpc_error\"}} {}\n",
        summary.rpc_errors
    ));
    out.push_str(&format!(
        "boom_requests_total{{target=\"{target}\",status=\"transport_error\"}} {}\n",
        summary.transport_errors
    ));
    out.push_str(&format!(
        "boom_requests_total{{target=\"{target}\",status=\"timeout\"}} {}\n",
        summary.timeouts
    ));
    out.push_str("# HELP boom_offered_requests Total logical requests offered by the scheduler.\n");
    out.push_str("# TYPE boom_offered_requests gauge\n");
    out.push_str(&format!(
        "boom_offered_requests{{target=\"{target}\"}} {}\n",
        summary.offered_requests
    ));
    out.push_str("# HELP boom_dropped_requests Logical requests dropped because the concurrency limit was saturated.\n");
    out.push_str("# TYPE boom_dropped_requests gauge\n");
    out.push_str(&format!(
        "boom_dropped_requests{{target=\"{target}\"}} {}\n",
        summary.dropped_requests
    ));
    if let Some(ratio) = summary.achieved_rate_ratio {
        out.push_str("# HELP boom_achieved_rate_ratio Observed logical RPS divided by requested logical RPS.\n");
        out.push_str("# TYPE boom_achieved_rate_ratio gauge\n");
        out.push_str(&format!("boom_achieved_rate_ratio{{target=\"{target}\"}} {ratio:.9}\n"));
    }
    out.push_str(
        "# HELP boom_requests_per_second Completed logical requests per requested test second.\n",
    );
    out.push_str("# TYPE boom_requests_per_second gauge\n");
    out.push_str(&format!(
        "boom_requests_per_second{{target=\"{target}\"}} {:.6}\n",
        summary.requests_per_second
    ));
    out.push_str("# HELP boom_latency_seconds End-to-end logical request latency by quantile.\n");
    out.push_str("# TYPE boom_latency_seconds gauge\n");
    for (quantile, value_ns) in [
        ("0.50", summary.latency.p50_ns),
        ("0.90", summary.latency.p90_ns),
        ("0.95", summary.latency.p95_ns),
        ("0.99", summary.latency.p99_ns),
    ] {
        out.push_str(&format!(
            "boom_latency_seconds{{target=\"{target}\",quantile=\"{quantile}\"}} {:.9}\n",
            effective_ns(
                value_ns,
                match quantile {
                    "0.50" => summary.latency.p50_ms,
                    "0.90" => summary.latency.p90_ms,
                    "0.95" => summary.latency.p95_ms,
                    _ => summary.latency.p99_ms,
                }
            ) as f64 /
                1_000_000_000.0,
        ));
    }
    append_histogram(&mut out, summary, &target);
    out.push_str(
        "# HELP boom_method_requests_total Completed logical requests by method and status.\n",
    );
    out.push_str("# TYPE boom_method_requests_total counter\n");
    out.push_str("# HELP boom_method_latency_seconds End-to-end latency by method and quantile.\n");
    out.push_str("# TYPE boom_method_latency_seconds gauge\n");
    for (method, metrics) in &summary.methods {
        let method = label_value(method);
        out.push_str(&format!(
            "boom_method_requests_total{{target=\"{target}\",method=\"{method}\",status=\"success\"}} {}\n",
            metrics.successes
        ));
        out.push_str(&format!(
            "boom_method_requests_total{{target=\"{target}\",method=\"{method}\",status=\"error\"}} {}\n",
            metrics.errors
        ));
        for (quantile, value_ns) in [
            ("0.50", metrics.p50_ns),
            ("0.90", metrics.p90_ns),
            ("0.95", metrics.p95_ns),
            ("0.99", metrics.p99_ns),
        ] {
            out.push_str(&format!(
                "boom_method_latency_seconds{{target=\"{target}\",method=\"{method}\",quantile=\"{quantile}\"}} {:.9}\n",
                effective_ns(value_ns, match quantile {
                    "0.50" => metrics.p50_ms,
                    "0.90" => metrics.p90_ms,
                    "0.95" => metrics.p95_ms,
                    _ => metrics.p99_ms,
                }) as f64 / 1_000_000_000.0,
            ));
        }
    }
    out.push_str("# EOF\n");
    out
}

fn append_histogram(out: &mut String, summary: &BenchSummary, target: &str) {
    out.push_str("# HELP boom_latency_histogram_seconds Coarse cumulative latency histogram.\n");
    out.push_str("# TYPE boom_latency_histogram_seconds histogram\n");
    let histogram = &summary.histogram;
    let mut cumulative = 0_u64;
    for (le, count) in [
        ("0.005", histogram.le_5_ms),
        ("0.010", histogram.le_10_ms),
        ("0.025", histogram.le_25_ms),
        ("0.050", histogram.le_50_ms),
        ("0.100", histogram.le_100_ms),
        ("0.250", histogram.le_250_ms),
        ("0.500", histogram.le_500_ms),
        ("1.000", histogram.le_1000_ms),
    ] {
        cumulative += count;
        out.push_str(&format!(
            "boom_latency_histogram_seconds_bucket{{target=\"{target}\",le=\"{le}\"}} {cumulative}\n"
        ));
    }
    out.push_str(&format!(
        "boom_latency_histogram_seconds_bucket{{target=\"{target}\",le=\"+Inf\"}} {}\n",
        summary.total_requests
    ));
    out.push_str(&format!(
        "boom_latency_histogram_seconds_sum{{target=\"{target}\"}} {:.9}\n",
        summary.latency.mean_ns as f64 * summary.total_requests as f64 / 1_000_000_000.0,
    ));
    out.push_str(&format!(
        "boom_latency_histogram_seconds_count{{target=\"{target}\"}} {}\n",
        summary.total_requests
    ));
}

fn render_html(summary: &BenchSummary, errors: &[ErrorSample]) -> String {
    let success_rate = percent(summary.successes, summary.total_requests);
    let error_total = summary.rpc_errors + summary.transport_errors + summary.timeouts;
    let method_count = summary.methods.len();
    let table_rows = method_rows(summary)
        .into_iter()
        .map(|(method, m)| method_table_row(method, m, summary.total_requests))
        .collect::<Vec<_>>()
        .join("\n");
    let error_reasons = error_reasons(errors);
    let error_rows = error_rows(errors);

    format!(
        r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>boom report - {target}</title>
<style>
:root {{ color-scheme: dark; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: radial-gradient(circle at top left, #152033 0, #080b10 34%, #050608 100%); color: #eff6ff; }}
main {{ max-width: 1280px; margin: 0 auto; padding: 28px; }}
header {{ display: flex; justify-content: space-between; gap: 18px; align-items: flex-start; margin-bottom: 20px; }}
h1 {{ margin: 0 0 8px; font-size: 34px; letter-spacing: 0; }}
h2 {{ margin: 0 0 14px; font-size: 16px; letter-spacing: 0; }}
.target {{ color: #8fb3d9; overflow-wrap: anywhere; }}
.badge {{ display: inline-flex; align-items: center; border: 1px solid #1f6feb; border-radius: 999px; padding: 7px 12px; background: rgba(21, 32, 51, 0.82); color: #bfe4ff; font-size: 13px; white-space: nowrap; box-shadow: 0 0 22px rgba(47, 111, 237, 0.22); }}
.band {{ display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 10px; margin-bottom: 16px; }}
.card, .panel {{ border: 1px solid #1d3147; border-radius: 8px; background: rgba(12, 18, 27, 0.92); box-shadow: 0 18px 45px rgba(0, 0, 0, 0.35), inset 0 1px 0 rgba(255, 255, 255, 0.04); }}
.card {{ padding: 13px 14px; min-height: 84px; }}
.card span {{ display: block; color: #7dd3fc; font-size: 11px; text-transform: uppercase; }}
.card strong {{ display: block; margin-top: 7px; font-size: 24px; color: #f8fbff; }}
.card small {{ display: block; margin-top: 4px; color: #8497ad; }}
.layout {{ display: grid; grid-template-columns: 1.05fr 1fr; gap: 16px; margin-bottom: 16px; }}
.triple {{ display: grid; grid-template-columns: 0.85fr 1.1fr 1.05fr; gap: 16px; margin-bottom: 16px; }}
.panel {{ padding: 16px; overflow: hidden; }}
.score {{ font-size: 50px; line-height: 1; font-weight: 800; margin-bottom: 8px; color: #5eead4; text-shadow: 0 0 24px rgba(94, 234, 212, 0.35); }}
.scoreline {{ height: 10px; background: #142235; border-radius: 999px; overflow: hidden; margin: 12px 0; }}
.scoreline div {{ height: 100%; background: linear-gradient(90deg, #14b8a6, #38bdf8); border-radius: inherit; }}
.finding {{ display: flex; gap: 10px; align-items: flex-start; padding: 9px 0; border-bottom: 1px solid #1a293b; }}
.finding:last-child {{ border-bottom: 0; }}
.dot {{ width: 8px; height: 8px; border-radius: 50%; margin-top: 7px; background: #38bdf8; box-shadow: 0 0 14px rgba(56, 189, 248, 0.8); flex: 0 0 auto; }}
.kv {{ display: grid; grid-template-columns: 1fr auto; gap: 8px 16px; font-size: 13px; }}
.kv span:nth-child(odd) {{ color: #b6c7db; overflow: hidden; text-overflow: ellipsis; }}
.kv span:nth-child(even) {{ font-weight: 700; }}
svg {{ width: 100%; height: auto; display: block; }}
.label {{ font-size: 12px; fill: #dcecff; }}
.muted {{ fill: #8fa2b7; color: #8fa2b7; }}
.axis {{ stroke: #20334a; stroke-width: 1; }}
.table-wrap {{ overflow-x: auto; border: 1px solid #1d3147; border-radius: 8px; background: rgba(6, 10, 16, 0.62); }}
table {{ border-collapse: collapse; width: 100%; min-width: 860px; }}
th, td {{ padding: 11px 12px; border-bottom: 1px solid #152236; text-align: right; white-space: nowrap; }}
th {{ color: #86b7e7; font-size: 12px; text-transform: uppercase; background: #0c1522; }}
th:first-child, td:first-child {{ text-align: left; }}
code {{ font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace; font-size: 12px; }}
.pill {{ display: inline-flex; min-width: 58px; justify-content: center; border-radius: 999px; padding: 3px 8px; background: rgba(20, 184, 166, 0.16); color: #5eead4; }}
.warn {{ background: rgba(245, 158, 11, 0.16); color: #fbbf24; }}
.bad {{ background: rgba(248, 81, 73, 0.16); color: #ff8b83; }}
.error-grid {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }}
.error-card {{ border: 1px solid #3b1d2a; border-radius: 8px; background: rgba(36, 12, 22, 0.55); padding: 12px; }}
.error-card strong {{ color: #ff8b83; }}
.error-card code {{ display: block; color: #ffd6d2; margin-top: 6px; white-space: normal; overflow-wrap: anywhere; }}
@media (max-width: 1080px) {{ .band {{ grid-template-columns: repeat(3, 1fr); }} .layout, .triple {{ grid-template-columns: 1fr; }} }}
@media (max-width: 680px) {{ main {{ padding: 16px; }} header {{ display: block; }} .badge {{ margin-top: 12px; }} .band {{ grid-template-columns: repeat(2, 1fr); }} h1 {{ font-size: 28px; }} .error-grid {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body><main>
<header>
  <div><h1>boom report</h1><div class="target">{target}</div></div>
  <div class="badge">{method_count} methods benchmarked</div>
</header>
<section class="band">
  <div class="card"><span>Total Requests</span><strong>{requests}</strong><small>{offered} offered / {dropped} dropped</small></div>
  <div class="card"><span>Throughput</span><strong>{rps:.2}</strong><small>{rate_detail}</small></div>
  <div class="card"><span>Success Rate</span><strong>{success_rate:.2}%</strong><small>{successes} successful</small></div>
  <div class="card"><span>Total Errors</span><strong>{error_total}</strong><small>rpc + transport + timeout</small></div>
  <div class="card"><span>P95 Latency</span><strong>{p95}</strong><small>p50 {p50}</small></div>
  <div class="card"><span>P99 Latency</span><strong>{p99}</strong><small>max {max_latency}</small></div>
</section>
<section class="triple">
  <div class="panel"><h2>Observed Reliability</h2><div class="score">{health_score:.1}%</div><div class="scoreline"><div style="width:{health_score:.0}%"></div></div><div class="muted">Observed success rate. Apply your own SLO for pass/fail decisions.</div></div>
  <div class="panel"><h2>Findings</h2>{findings}</div>
  <div class="panel"><h2>Hotspots</h2>{hotspots}</div>
</section>
<section class="layout">
  <div class="panel"><h2>Status Split</h2>{status_chart}</div>
  <div class="panel"><h2>Request Volume By Method</h2>{volume_chart}</div>
</section>
<section class="layout">
  <div class="panel"><h2>Latency By Method</h2>{latency_chart}</div>
  <div class="panel"><h2>Error Rate By Method</h2>{error_chart}</div>
</section>
<section class="panel" style="margin-bottom:16px;">
  <h2>Run Timeline</h2>
  {timeline}
</section>
<section class="panel" style="margin-bottom:16px;">
  <h2>Error Reasons</h2>
  {error_reasons}
</section>
<section class="panel" style="padding:0;">
  <div style="padding:16px 16px 0;"><h2>Method Detail</h2></div>
  <div class="table-wrap"><table>
    <thead><tr><th>Method</th><th>Share</th><th>Requests</th><th>OK</th><th>Errors</th><th>Success</th><th>P50</th><th>P95</th><th>P99</th></tr></thead>
    <tbody>{table_rows}</tbody>
  </table></div>
</section>
<section class="panel" style="padding:0; margin-top:16px;">
  <div style="padding:16px 16px 0;"><h2>Error Samples</h2></div>
  <div class="table-wrap"><table>
    <thead><tr><th>Method</th><th>Kind</th><th>Latency</th><th>Reason</th></tr></thead>
    <tbody>{error_rows}</tbody>
  </table></div>
</section>
</main></body></html>"##,
        target = escape(&summary.target),
        method_count = method_count,
        requests = summary.total_requests,
        offered = summary.offered_requests,
        dropped = summary.dropped_requests,
        rps = summary.requests_per_second,
        rate_detail = rate_detail(summary),
        success_rate = success_rate,
        successes = summary.successes,
        error_total = error_total,
        p50 = format_latency(summary.latency.p50_ns, summary.latency.p50_ms),
        p95 = format_latency(summary.latency.p95_ns, summary.latency.p95_ms),
        p99 = format_latency(summary.latency.p99_ns, summary.latency.p99_ms),
        max_latency = format_latency(summary.latency.max_ns, summary.latency.max_ms),
        health_score = health_score(summary),
        findings = findings(summary),
        hotspots = hotspots(summary),
        status_chart = status_chart(summary),
        volume_chart = bar_chart(summary, |m| m.requests as f64, "req", "#2f6fed", 10),
        latency_chart = grouped_latency_chart(summary),
        error_chart = bar_chart(summary, |m| percent(m.errors, m.requests), "%", "#c43f32", 10),
        timeline = timeline_chart(summary),
        error_reasons = error_reasons,
        table_rows = table_rows,
        error_rows = error_rows,
    )
}

fn method_table_row(method: &str, m: &MethodSummary, total_requests: u64) -> String {
    let success = percent(m.successes, m.requests);
    let error_class = if success >= 99.0 {
        "pill"
    } else if success >= 90.0 {
        "pill warn"
    } else {
        "pill bad"
    };
    format!(
        "<tr><td><code>{}</code></td><td>{:.1}%</td><td>{}</td><td>{}</td><td>{}</td><td><span class=\"{}\">{:.1}%</span></td><td>{}</td><td>{}</td><td>{}</td></tr>",
        escape(method),
        percent(m.requests, total_requests),
        m.requests,
        m.successes,
        m.errors,
        error_class,
        success,
        format_latency(m.p50_ns, m.p50_ms),
        format_latency(m.p95_ns, m.p95_ms),
        format_latency(m.p99_ns, m.p99_ms),
    )
}

fn error_reasons(errors: &[ErrorSample]) -> String {
    if errors.is_empty() {
        return "<div class=\"muted\">No error samples were recorded for this run.</div>"
            .to_string();
    }
    let mut grouped = std::collections::BTreeMap::<(String, String, String), (usize, u128)>::new();
    for error in errors {
        let key = (error.method.clone(), error.kind.clone(), error.detail.clone());
        let entry = grouped.entry(key).or_default();
        entry.0 += 1;
        entry.1 = entry.1.max(error.latency_ms);
    }
    let mut rows = grouped.into_iter().collect::<Vec<_>>();
    rows.sort_by_key(|(_, (count, max_latency))| {
        (std::cmp::Reverse(*count), std::cmp::Reverse(*max_latency))
    });
    let cards = rows
        .into_iter()
        .take(6)
        .map(|((method, kind, detail), (count, max_latency))| {
            format!(
                "<div class=\"error-card\"><strong>{} x {}</strong><div class=\"muted\">{} | max {} ms</div><code>{}</code></div>",
                count,
                escape(&kind),
                escape(&method),
                max_latency,
                escape(&detail),
            )
        })
        .collect::<Vec<_>>()
        .join("");
    format!("<div class=\"error-grid\">{cards}</div>")
}

fn error_rows(errors: &[ErrorSample]) -> String {
    if errors.is_empty() {
        return "<tr><td colspan=\"4\" style=\"text-align:left;\">No error samples recorded.</td></tr>"
            .to_string();
    }
    errors
        .iter()
        .take(100)
        .map(|error| {
            format!(
                "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td style=\"text-align:left; white-space:normal;\"><code>{}</code></td></tr>",
                escape(&error.method),
                escape(&error.kind),
                format_latency(error.latency_ns, error.latency_ms),
                escape(&error.detail),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn status_chart(summary: &BenchSummary) -> String {
    let total = summary.total_requests.max(1) as f64;
    let ok = summary.successes as f64 / total;
    let rpc = summary.rpc_errors as f64 / total;
    let transport = summary.transport_errors as f64 / total;
    let timeout = summary.timeouts as f64 / total;
    let ok_len = 251.2 * ok;
    let rpc_len = 251.2 * rpc;
    let transport_len = 251.2 * transport;
    let timeout_len = 251.2 * timeout;
    format!(
        r##"<svg viewBox="0 0 440 238" role="img" aria-label="status split chart">
<circle cx="120" cy="112" r="44" fill="none" stroke="#e3e7e0" stroke-width="24"/>
<circle cx="120" cy="112" r="44" fill="none" stroke="#21855b" stroke-width="24" stroke-dasharray="{ok_len:.2} 251.2" transform="rotate(-90 120 112)"/>
<circle cx="120" cy="112" r="44" fill="none" stroke="#d28b19" stroke-width="24" stroke-dasharray="{rpc_len:.2} 251.2" stroke-dashoffset="-{ok_len:.2}" transform="rotate(-90 120 112)"/>
<circle cx="120" cy="112" r="44" fill="none" stroke="#c43f32" stroke-width="24" stroke-dasharray="{transport_len:.2} 251.2" stroke-dashoffset="-{rpc_offset:.2}" transform="rotate(-90 120 112)"/>
<circle cx="120" cy="112" r="44" fill="none" stroke="#6b5fd6" stroke-width="24" stroke-dasharray="{timeout_len:.2} 251.2" stroke-dashoffset="-{timeout_offset:.2}" transform="rotate(-90 120 112)"/>
<text x="120" y="108" text-anchor="middle" class="label" font-size="20">{ok_pct:.1}%</text>
<text x="120" y="128" text-anchor="middle" class="muted" font-size="11">success</text>
{legend}
</svg>"##,
        ok_len = ok_len,
        rpc_len = rpc_len,
        transport_len = transport_len,
        timeout_len = timeout_len,
        rpc_offset = ok_len + rpc_len,
        timeout_offset = ok_len + rpc_len + transport_len,
        ok_pct = ok * 100.0,
        legend = legend(&[
            ("success", summary.successes, "#21855b"),
            ("rpc error", summary.rpc_errors, "#d28b19"),
            ("transport", summary.transport_errors, "#c43f32"),
            ("timeout", summary.timeouts, "#6b5fd6"),
        ]),
    )
}

fn bar_chart(
    summary: &BenchSummary,
    value: fn(&MethodSummary) -> f64,
    unit: &str,
    color: &str,
    limit: usize,
) -> String {
    let methods = top_methods(summary, limit);
    let max = methods.iter().map(|(_, m)| value(m)).fold(0.0, f64::max).max(1.0);
    let height = 42 + methods.len() as i32 * 30;
    let bars = methods
        .iter()
        .enumerate()
        .map(|(index, (method, m))| {
            let y = 32 + index as i32 * 30;
            let val = value(m);
            let width = (val / max * 258.0).max(1.0);
            format!(
                r#"<text x="0" y="{label_y}" class="label">{method}</text><rect x="156" y="{bar_y}" width="{width:.2}" height="16" rx="5" fill="{color}"/><text x="{value_x:.2}" y="{label_y}" class="muted">{val:.2} {unit}</text>"#,
                label_y = y + 13,
                method = trim_method(method, 25),
                bar_y = y,
                width = width,
                color = color,
                value_x = 164.0 + width,
                val = val,
                unit = unit,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("<svg viewBox=\"0 0 500 {height}\">{bars}</svg>")
}

fn grouped_latency_chart(summary: &BenchSummary) -> String {
    let methods = slowest_methods(summary, 8);
    let max = methods
        .iter()
        .map(|(_, metrics)| effective_ns(metrics.p99_ns, metrics.p99_ms))
        .max()
        .unwrap_or(1)
        .max(1) as f64;
    let height = 54 + methods.len() as i32 * 38;
    let rows = methods
        .iter()
        .enumerate()
        .map(|(index, (method, m))| {
            let y = 38 + index as i32 * 38;
            let p50 = (effective_ns(m.p50_ns, m.p50_ms) as f64 / max * 230.0).max(1.0);
            let p95 = (effective_ns(m.p95_ns, m.p95_ms) as f64 / max * 230.0).max(1.0);
            let p99 = (effective_ns(m.p99_ns, m.p99_ms) as f64 / max * 230.0).max(1.0);
            format!(
                r##"<text x="0" y="{label_y}" class="label">{method}</text>
<rect x="156" y="{p99_y}" width="{p99:.2}" height="8" rx="4" fill="#d85826"/>
<rect x="156" y="{p95_y}" width="{p95:.2}" height="8" rx="4" fill="#f0a13a"/>
<rect x="156" y="{p50_y}" width="{p50:.2}" height="8" rx="4" fill="#21855b"/>
<text x="{value_x:.2}" y="{label_y}" class="muted">{p50v}/{p95v}/{p99v}</text>"##,
                label_y = y + 18,
                method = trim_method(method, 25),
                p99_y = y,
                p95_y = y + 10,
                p50_y = y + 20,
                p50 = p50,
                p95 = p95,
                p99 = p99,
                value_x = 164.0 + p99,
                p50v = format_latency(m.p50_ns, m.p50_ms),
                p95v = format_latency(m.p95_ns, m.p95_ms),
                p99v = format_latency(m.p99_ns, m.p99_ms),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "<svg viewBox=\"0 0 520 {height}\"><text x=\"156\" y=\"14\" class=\"muted\">p99 / p95 / p50, slowest methods first</text>{rows}</svg>"
    )
}

fn legend(items: &[(&str, u64, &str)]) -> String {
    items
        .iter()
        .enumerate()
        .map(|(index, (label, value, color))| {
            let y = 58 + index as i32 * 32;
            format!(
                r#"<rect x="230" y="{y}" width="12" height="12" rx="3" fill="{color}"/><text x="250" y="{text_y}" class="label">{label}</text><text x="358" y="{text_y}" class="muted">{value}</text>"#,
                y = y,
                color = color,
                text_y = y + 11,
                label = label,
                value = value,
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn health_score(summary: &BenchSummary) -> f64 {
    percent(summary.successes, summary.total_requests)
}

fn timeline_chart(summary: &BenchSummary) -> String {
    if summary.samples.is_empty() {
        return "<div class=\"muted\">No per-second samples are available for this run.</div>"
            .to_string();
    }
    let width = 920.0;
    let height = 230.0;
    let plot_height = 170.0;
    let max_requests =
        summary.samples.iter().map(|sample| sample.requests).max().unwrap_or(1).max(1);
    let max_latency = summary
        .samples
        .iter()
        .map(|sample| effective_ns(sample.p95_ns, sample.p95_ms))
        .max()
        .unwrap_or(1)
        .max(1);
    let count = summary.samples.len().max(1) as f64;
    let bar_width = (width / count).max(1.0);
    let bars = summary
        .samples
        .iter()
        .enumerate()
        .map(|(index, sample)| {
            let x = index as f64 * bar_width;
            let bar_height = sample.requests as f64 / max_requests as f64 * plot_height;
            format!(
                "<rect x=\"{x:.2}\" y=\"{y:.2}\" width=\"{bar_width:.2}\" height=\"{bar_height:.2}\" fill=\"#17365f\"/><title>second {}: {} requests</title>",
                sample.second,
                sample.requests,
                y = plot_height - bar_height,
            )
        })
        .collect::<Vec<_>>()
        .join("");
    let points = summary
        .samples
        .iter()
        .enumerate()
        .map(|(index, sample)| {
            let x = (index as f64 + 0.5) * bar_width;
            let value = effective_ns(sample.p95_ns, sample.p95_ms);
            let y = plot_height - value as f64 / max_latency as f64 * plot_height;
            format!("{x:.2},{y:.2}")
        })
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "<svg viewBox=\"0 0 {width} {height}\" role=\"img\" aria-label=\"requests and p95 latency over time\">{bars}<polyline points=\"{points}\" fill=\"none\" stroke=\"#5eead4\" stroke-width=\"3\"/><text x=\"0\" y=\"205\" class=\"muted\">blue bars: requests/sec · green line: p95 latency · max {}</text></svg>",
        format_latency(max_latency, common::ns_to_ms(max_latency)),
    )
}

fn findings(summary: &BenchSummary) -> String {
    let success_rate = percent(summary.successes, summary.total_requests);
    let mut items = Vec::new();
    items.push(finding(
        "Reliability",
        &format!("{success_rate:.2}% success rate across {} requests.", summary.total_requests),
    ));
    items.push(finding(
        "Throughput",
        &format!(
            "{:.2} requests per second over {} ms.",
            summary.requests_per_second, summary.duration_ms
        ),
    ));
    items.push(finding(
        "Latency",
        &format!(
            "p50 {}, p95 {}, p99 {}.",
            format_latency(summary.latency.p50_ns, summary.latency.p50_ms),
            format_latency(summary.latency.p95_ns, summary.latency.p95_ms),
            format_latency(summary.latency.p99_ns, summary.latency.p99_ms),
        ),
    ));
    if let Some(requested_rps) = summary.requested_rps {
        items.push(finding(
            "Load delivery",
            &format!(
                "{:.2}% of {:.2} requested RPS; {} scheduler drops.",
                summary.achieved_rate_ratio.unwrap_or(0.0) * 100.0,
                requested_rps,
                summary.dropped_requests,
            ),
        ));
    }
    if !summary.skipped_methods.is_empty() {
        items.push(finding("Skipped workload", &summary.skipped_methods.join(", ")));
    }
    if summary.rpc_errors + summary.transport_errors + summary.timeouts == 0 {
        items.push(finding("Errors", "No RPC, transport, or timeout errors recorded."));
    } else {
        items.push(finding(
            "Errors",
            &format!(
                "{} RPC, {} transport, {} timeout errors.",
                summary.rpc_errors, summary.transport_errors, summary.timeouts
            ),
        ));
    }
    items.join("")
}

fn finding(title: &str, body: &str) -> String {
    format!(
        "<div class=\"finding\"><span class=\"dot\"></span><div><strong>{}</strong><br><span class=\"muted\">{}</span></div></div>",
        escape(title),
        escape(body)
    )
}

fn hotspots(summary: &BenchSummary) -> String {
    let slow = slowest_methods(summary, 4);
    let noisy = error_methods(summary, 4);
    let mut out = String::from("<div class=\"kv\">");
    for (method, m) in &slow {
        out.push_str(&format!(
            "<span>{}</span><span>p95 {}</span>",
            escape(method),
            format_latency(m.p95_ns, m.p95_ms),
        ));
    }
    for (method, m) in &noisy {
        out.push_str(&format!(
            "<span>{}</span><span>{:.1}% errors</span>",
            escape(method),
            percent(m.errors, m.requests)
        ));
    }
    if slow.is_empty() && noisy.is_empty() {
        out.push_str("<span>no method hotspots</span><span>clean</span>");
    }
    out.push_str("</div>");
    out
}

fn method_rows(summary: &BenchSummary) -> Vec<(&String, &MethodSummary)> {
    let mut methods = summary.methods.iter().collect::<Vec<_>>();
    methods.sort_by_key(|(_, metrics)| {
        (
            std::cmp::Reverse(effective_ns(metrics.p95_ns, metrics.p95_ms)),
            std::cmp::Reverse(metrics.requests),
        )
    });
    methods
}

fn slowest_methods(summary: &BenchSummary, limit: usize) -> Vec<(&String, &MethodSummary)> {
    let mut methods = summary.methods.iter().filter(|(_, m)| m.requests > 0).collect::<Vec<_>>();
    methods.sort_by_key(|(_, metrics)| {
        std::cmp::Reverse(effective_ns(metrics.p95_ns, metrics.p95_ms))
    });
    methods.truncate(limit);
    methods
}

fn error_methods(summary: &BenchSummary, limit: usize) -> Vec<(&String, &MethodSummary)> {
    let mut methods = summary.methods.iter().filter(|(_, m)| m.errors > 0).collect::<Vec<_>>();
    methods.sort_by_key(|(_, m)| std::cmp::Reverse(m.errors));
    methods.truncate(limit);
    methods
}

fn top_methods(summary: &BenchSummary, limit: usize) -> Vec<(&String, &MethodSummary)> {
    let mut methods = summary.methods.iter().collect::<Vec<_>>();
    methods.sort_by_key(|(_, m)| std::cmp::Reverse(m.requests));
    methods.truncate(limit);
    methods
}

fn percent(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        part as f64 / total as f64 * 100.0
    }
}

fn trim_method(method: &str, max_len: usize) -> String {
    let escaped = escape(method);
    if escaped.chars().count() > max_len {
        format!("{}...", escaped.chars().take(max_len.saturating_sub(3)).collect::<String>())
    } else {
        escaped
    }
}

fn escape(input: &str) -> String {
    input.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

fn label_value(input: &str) -> String {
    input.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n").replace('\r', "\\r")
}

fn effective_ns(ns: u128, legacy_ms: u128) -> u128 {
    if ns == 0 && legacy_ms > 0 {
        legacy_ms * 1_000_000
    } else {
        ns
    }
}

fn format_latency(ns: u128, legacy_ms: u128) -> String {
    let ns = effective_ns(ns, legacy_ms);
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.2} \u{00b5}s", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2} s", ns as f64 / 1_000_000_000.0)
    }
}

fn rate_detail(summary: &BenchSummary) -> String {
    summary.requested_rps.map_or_else(
        || "closed-loop requests per second".to_string(),
        |requested| {
            format!(
                "target {:.2} · {:.1}% delivered",
                requested,
                summary.achieved_rate_ratio.unwrap_or(0.0) * 100.0,
            )
        },
    )
}
