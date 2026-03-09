# SiMM Metrics

This module exposes lightweight, Prometheus-compatible HTTP metrics for SiMM components.
It is intended for high-level monitoring (dashboards/alerts) and complements tracing
(see [`src/common/trace/README.md`](../trace/README.md)).

## What you get

Typical metrics include:

- **Request latency** (summary)
- **Request totals** (counter)
- **Error totals** (counter)
- **Bytes read/written totals** (counter)

Metric name patterns (module-prefixed):

- `<module>_request_duration_microseconds{instance="<ip>", type="<op>"}`
- `<module>_requests_total{instance="<ip>", type="<op>"}`
- `<module>_errors_total{instance="<ip>", type="<op>"}`
- `<module>_read_bytes_total{instance="<ip>"}`
- `<module>_written_bytes_total{instance="<ip>"}`

## Enable metrics (build-time)

Metrics are compiled in only when `ENABLE_METRICS=ON`.

Using the build helper script:

```bash
# Build release binaries with metrics enabled
./build.sh --mode release --metric

# Build debug binaries with metrics enabled
./build.sh --mode debug --metric
```

## Scrape the metrics endpoint

When enabled, SiMM starts an HTTP exposer bound to:

- `0.0.0.0:<metrics_port>`

Prometheus scrapes the default path:

- `http://<host>:<metrics_port>/metrics`

### Quick checks (curl)

```bash
# Fetch the full metrics page
curl -s http://127.0.0.1:<metrics_port>/metrics | head

# Search for a specific module prefix (example: data_server)
curl -s http://127.0.0.1:<metrics_port>/metrics | grep -E '^data_server_'
```

### Run-time configuration

The port is controlled by the gflag `metrics_port`. Currently changing this flag cannot change the port of the server.

You can inspect gflags using the admin tool (see [`docs/admin_tool.md`](../../../docs/admin_tool.md)):

```bash
# Get metrics_port
./build/debug/bin/tools/simmctl --ip <IP> gflag get metrics_port
```

## Related docs

- Observability entry points: [`docs/observability_fuctionality.md`](../../../docs/observability_fuctionality.md)
- Admin tool: [`docs/admin_tool.md`](../../../docs/admin_tool.md)