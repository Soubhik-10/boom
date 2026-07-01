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
    pub p95_delta_pct: f64,
    pub left_success_pct: f64,
    pub right_success_pct: f64,
}

#[derive(Debug, Clone)]
pub struct CompareArtifacts {
    pub json: PathBuf,
    pub markdown: PathBuf,
    pub html: PathBuf,
}

pub fn build_compare(left: BenchSummary, right: BenchSummary) -> CompareReport {
    let rps_delta_pct = delta_pct(left.requests_per_second, right.requests_per_second);
    let p95_delta_pct = delta_pct(left.latency.p95_ms as f64, right.latency.p95_ms as f64);
    let success_delta_pct = delta_pct(success_pct(&left), success_pct(&right));
    let mut method_names = BTreeSet::new();
    method_names.extend(left.methods.keys().cloned());
    method_names.extend(right.methods.keys().cloned());
    let methods = method_names
        .into_iter()
        .map(|method| {
            let l = left.methods.get(&method).cloned().unwrap_or_default();
            let r = right.methods.get(&method).cloned().unwrap_or_default();
            MethodCompare {
                method,
                left_p95_ms: l.p95_ms,
                right_p95_ms: r.p95_ms,
                p95_delta_pct: delta_pct(l.p95_ms as f64, r.p95_ms as f64),
                left_success_pct: pct(l.successes, l.requests),
                right_success_pct: pct(r.successes, r.requests),
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
            "| `{}` | {} | {} | {:.2}% | {:.2}% | {:.2}% |\n",
            method.method,
            method.left_p95_ms,
            method.right_p95_ms,
            method.p95_delta_pct,
            method.left_success_pct,
            method.right_success_pct
        ));
    }
    out
}

fn render_html(report: &CompareReport) -> String {
    let rows = report.methods.iter().map(|m| {
        format!("<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{:.2}%</td><td>{:.2}%</td><td>{:.2}%</td></tr>", escape(&m.method), m.left_p95_ms, m.right_p95_ms, m.p95_delta_pct, m.left_success_pct, m.right_success_pct)
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
fn escape(input: &str) -> String {
    input.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}
