# Quick Start — Darling (headless)

Part of the [Performance Monitor documentation](../README.md#documentation).

**Darling** is the always-on edition for teams that want 24/7 collection without a desktop app driving it: a service on Windows or Linux collects from your SQL Server and PostgreSQL servers around the clock into a central PostgreSQL store (TimescaleDB is detected and adopted automatically for compression and chunk-based retention), and a detached WPF viewer reads that store from any Windows seat. It runs the same monitoring brain as Lite — collectors, alert engine, and analysis pipeline shared at the library level — with alerts over email and webhooks (Teams, Slack, PagerDuty, and generic HTTP POST) and the same MCP tool surface available on request.

The optional built-in web dashboard is off by default and uses its own port, 5153. It serves the fleet overview, per-server drill-down, FinOps, the scheduled fleet sweep reports, alert history, job history, and an Admin page to any browser. It reads the collected data. A seat that is allowed to make changes can also do these things:

- save views, and edit alert rules and mute rules
- dismiss alerts
- manage server tags, including the order of sibling tags (Move Up and Move Down in the viewer, Move up and Move down on the web page)
- add or edit a monitored server

Operators can watch the fleet without installing the viewer. See [Tabs](tabs.md#tabs) for the pages.

1. Download **`PerformanceMonitorDarling-<version>.zip`** from the [latest release](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest) — the signed service and viewer with the bundled PostgreSQL + TimescaleDB runtime beside the service exe, so a from-zero install needs no database provisioning. Both are self-contained, so the machine needs no .NET runtime installed.
2. Copy `darling.sample.json` to `darling.json` and add your servers (and optional SMTP / webhook delivery). In managed mode the service unpacks and runs its own PostgreSQL — no external database to set up. For a PostgreSQL server, set `"engine": "postgres"` on its entry (see [PostgreSQL Targets](../Darling/README.md#postgresql-targets)).
3. Run the service (console for a trial, or install it as a Windows service). It seeds the store and begins collecting on the same default cadences and retention horizons as a fresh Lite install.
4. Start the **Darling Viewer**. It is in the same zip, under `viewer\`, and `install-darling.ps1` leaves a Desktop shortcut. On the service host there is nothing to point at anything: it finds the same `darling.json`, derives the store connection, and opens on the fleet. For a seat on another machine, run `--export-viewer-config` on the service host and copy the folder it writes.
5. Optionally turn on the two off-by-default surfaces: `--enable-web` serves the browser dashboard on port 5153, `--enable-mcp` serves the MCP endpoint on 5152. Both take effect live, no restart.

These steps are for Windows. On Linux, run the service under Docker Compose with the official TimescaleDB image. You can also run it under systemd with a PostgreSQL server that you already run, version 16 or later. The bundled store is Windows-only.

On Linux, the service keeps a key pair and seals secrets to it, so it can save and use them. The secrets are a SQL login password, the SMTP password, webhook URLs, the PagerDuty key, and generic webhook headers. `--reset-password-key` makes a new key. See [Run on Linux](../Darling/README.md#run-on-linux-docker-compose-or-systemd-1804) in the operator guide.

**Never done this before?** [**docs/uat-onboarding.md**](uat-onboarding.md) is the ordered path from a downloaded zip to all three surfaces — the WPF viewer, the web dashboard, and MCP — with the log line, HTTP response, or screen that proves each step worked, and the handful of things that reliably catch people out.

Configuration is a single JSON file with no schedule knobs. One exception: a collector that runs once a day or less often can have a run time. That is a time of day on the monitored server's own clock. Set it in the viewer's Collector Schedules window (the **Run at** column) or with `--set-collector-run-at`. See the **[Darling operator guide](../Darling/README.md)** for the configuration reference, permissions, and operations.
