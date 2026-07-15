# Prometheus and Grafana

Expose a completed run from the host:

```bash
boom serve-metrics --run runs/example --listen 0.0.0.0:9464
```

Configure Prometheus with `prometheus.yml`. If Prometheus runs directly on the host, replace
`host.docker.internal` with `127.0.0.1`. On Linux Docker, add the host-gateway mapping if your
Docker installation does not provide `host.docker.internal` automatically.

Import `grafana/boom-dashboard.json` into Grafana and select the Prometheus data source. The
dashboard displays completed-run throughput, reliability, rate delivery, scheduler drops,
latency quantiles, histogram buckets, and per-method results.

The endpoint also exposes `GET /healthz`. Stop it cleanly with Ctrl-C.
