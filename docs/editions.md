# Choosing an edition

Audience: operators.

Part of the [Performance Monitor documentation](../README.md#documentation).

## Editions

Pick by how you want collection to run — the monitoring brain (collectors, alert engine, plan analysis, MCP tools) is shared across all three at the library level.

| | **[Lite](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest)** — flagship | **[Darling](../Darling/README.md)** — headless | **[Dashboard](../deprecated/Dashboard/README.md)** — *deprecated* |
|---|---|---|---|
| **How it runs** | Single desktop app monitors remotely, on demand | Service on Windows or Linux collects 24/7 into a central store. The Windows viewer and the web dashboard read it from any seat | SQL-Server-installed database + Agent collectors, separate viewer app |
| **Installs on your server?** | No | No | Yes (a `PerformanceMonitor` database) |
| **Stores data** | Local DuckDB + Parquet | PostgreSQL + TimescaleDB (bundled on Windows) | In the target SQL Server |
| **Best for** | Quick triage, Azure SQL DB, locked-down servers, consultants, firefighting | Always-on monitoring of many SQL Server and PostgreSQL servers from one service | *Existing installs only — new deployments should use Lite or Darling* |
| **Requires** | Four server grants: `VIEW SERVER STATE`, `CONNECT ANY DATABASE`, `VIEW ANY DEFINITION` and `ALTER ANY EVENT SESSION` ([permissions](permissions.md#permissions)) | The same four grants (SQL Server) or [these grants](darling/postgresql-targets.md#permissions-on-a-postgresql-target) (PostgreSQL), plus a place to run the service | SQL Agent ([Dashboard docs](../deprecated/Dashboard/README.md)) |

> **⚠️ The "Full" Dashboard edition is deprecated.** Existing installs keep working and remain on bug-fix support, but it is no longer the recommended path and — as of v3.3.0 — the Dashboard and its CLI installer are **no longer included in release assets**. The last shipped builds are in the [v3.2.0 release](https://github.com/erikdarlingdata/PerformanceMonitor/releases/tag/v3.2.0), and both remain buildable from the repo. New deployments should use **Lite** or **Darling**. Its docs live with the code: **[deprecated/Dashboard/README.md](../deprecated/Dashboard/README.md)** (the app, tabs, permissions) and **[deprecated/Installer/README.md](../deprecated/Installer/README.md)** (the CLI database installer).

**👉 Not sure? [Start with Lite.](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest)** One download, nothing installed on your server, data flowing in under 5 minutes.

All editions include real-time alerts (email + webhooks), charts and graphs, Dark, Light and Cool Breeze themes, CSV export, and a built-in MCP server for AI-powered analysis with tools like Claude. Lite and Dashboard also show alerts in the system tray. All release binaries are digitally signed via [SignPath](https://signpath.io) — no more Windows SmartScreen warnings.

## Edition Comparison

| Capability | Lite | Darling | Dashboard *(deprecated)* |
|---|---|---|---|
| Target server installation | None | None | Required |
| Runs collection | On-demand desktop app | 24/7 service (Windows or Linux) | SQL Agent on the target |
| Multi-server from one seat | Built-in | Built-in (central store) | Per-server install |
| Data storage | DuckDB + Parquet (local) | PostgreSQL + TimescaleDB (bundled on Windows) | SQL Server (on target) |
| Azure SQL Database | Supported | Supported | Not supported |
| Azure SQL MI / AWS RDS | Supported | Supported | Supported |
| PostgreSQL (self-hosted, AWS RDS, Aurora) | Not supported | Supported | Not supported |
| Graphical plan viewer | Built-in, 37-rule PlanAnalyzer | Built-in, 37-rule PlanAnalyzer | Built-in, 37-rule PlanAnalyzer |
| Standalone plan viewer | Open/paste/drag `.sqlplan` | Open/paste/drag `.sqlplan` | Open/paste/drag `.sqlplan` |
| Alerts (tray + email + webhooks) | Yes | Email + webhooks (headless) | Yes |
| Themes | Dark, Light and Cool Breeze, with per-color overrides | Dark, Light and Cool Breeze, with per-color overrides (viewer) | Dark, Light and Cool Breeze |
| Web dashboard | Not available | Optional, off by default (port 5153) | Not available |
| Portability | Single executable | Portable service + viewer zip (Windows), service tarball (Linux) | Server-bound |
| SQL Server collectors | More than 40 (the long-running-query trace is opt-in) | Lite's set, plus a forced-plan collector, a Query Store hint collector, an identity column capacity collector, a database protection collector (backups and CHECKDB) and an Agent alerts collector | Its own set, installed on the target |
| PostgreSQL collectors | None | More than 25 | None |
| Agent investigations (your own AI agent picks up an alert and reports) | Level 1: the agent reads and reports | Level 1 to 3: reports, plus agent-made alert changes and server actions, each behind its own switch | Not available |
| Agent changes on a monitored server (force or unforce a Query Store plan, evict a cached plan, recompile a module, create or drop a plan guide, kill a session) | Not available | Yes, one switch per server and action, all off until you turn them on | Not available |
| Query tags (name a group of queries and see its resource use) | Yes | Yes | Not available |
| Notification routes limited to a server or a tag | Yes | Yes | Not available |
| MCP server (LLM integration) | Built-in (more than 100 tools) | On request (more than 200 tools) | Built into Dashboard (more than 60 tools) |
