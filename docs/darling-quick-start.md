# Quick Start: Darling on Linux and the web dashboard

Audience: operators.

Part of the [Performance Monitor documentation](../README.md#documentation).

**Darling** is the always-on edition for teams that want 24/7 collection without a desktop app driving it: a service on Windows or Linux collects from your SQL Server and PostgreSQL servers around the clock into a central PostgreSQL store (TimescaleDB is detected and adopted automatically for compression and chunk-based retention), and a detached WPF viewer reads that store from any Windows seat. It runs the same monitoring brain as Lite — collectors, alert engine, and analysis pipeline shared at the library level — with alerts over email and webhooks (Teams, Slack, PagerDuty, and generic HTTP POST) and the same MCP tool surface available on request.

The optional built-in web dashboard is off by default and uses its own port, 5153. It serves the fleet overview, per-server drill-down, FinOps, the scheduled fleet sweep reports, alert history, job history, and an Admin page to any browser. It reads the collected data. A seat that is allowed to make changes can also do these things:

- save views, and edit alert rules and mute rules
- dismiss alerts
- manage server tags, including the order of sibling tags (Move Up and Move Down in the viewer, Move up and Move down on the web page)
- add or edit a monitored server

Operators can watch the fleet without installing the viewer. See [Tabs](tabs.md#tabs) for the pages.

## Install on Windows

The Windows install has its own page: [Install Darling](darling/install.md). It covers the download and hash check, the install folder `C:\Program Files\PerformanceMonitorDarling`, `install-darling.ps1`, and the rule that `darling.json` is only a seed. This page does not repeat those steps. To run the viewer on another machine, read [The WPF viewer](darling/viewer.md).

For a PostgreSQL server as a monitored target, set `"engine": "postgres"` on its entry in `darling.json` (see [PostgreSQL Targets](../Darling/README.md#postgresql-targets)).

## Turn on the web dashboard or MCP

Both surfaces are off by default. After the service collects, turn them on with `--enable-web` (the browser dashboard on port 5153) or `--enable-mcp` (the MCP endpoint on port 5152). Both take effect live, with no restart. The steps and proof points are in [The web dashboard](darling/web-dashboard.md) and [Darling MCP setup](darling/mcp.md).

## Install on Linux

On Linux, run the service under Docker Compose with the official TimescaleDB image. You can also run it under systemd with a PostgreSQL server that you already run, version 16 or later. The bundled store is Windows-only.

On Linux, the service keeps a key pair and seals secrets to it, so it can save and use them. The secrets are a SQL login password, the SMTP password, webhook URLs, the PagerDuty key, and generic webhook headers. `--reset-password-key` makes a new key. See [Run on Linux](../Darling/README.md#run-on-linux-docker-compose-or-systemd-1804) in the operator guide.

Configuration is a single JSON file with no schedule knobs. One exception: a collector that runs once a day or less often can have a run time. That is a time of day on the monitored server's own clock. Set it in the viewer's Collector Schedules window (the **Run at** column) or with `--set-collector-run-at`. See the **[Darling operator guide](../Darling/README.md)** for the configuration reference, permissions, and operations.
