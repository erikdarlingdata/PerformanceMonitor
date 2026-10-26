# Performance Monitor Darling — Headless Edition

Darling is the headless, centralized edition of Performance Monitor: a 24/7 service (a Windows service, or Linux with systemd or Docker Compose) that collects from your SQL Servers into a central PostgreSQL (optionally TimescaleDB) store, plus a detached desktop viewer that reads that store. No desktop app has to stay open for collection to happen, and every viewer seat reads the same central data.

**Running an install day to day?** [Running Darling day to day](../docs/darling/admin.md) has the steps to add or remove a server, change a password, rotate a token, pause collection, upgrade and uninstall.

It runs the **same monitoring brain as the Lite edition** — one shared codebase, two storage engines:

- `PerformanceMonitor.Collectors` owns every collector definition (more than 40 for SQL Server and more than 25 for PostgreSQL): the exact query sent to monitored servers, the result-row mappings, the delta rules, the default cadences and retention horizons, and the ignored-wait-types list. Lite writes those rows to DuckDB; Darling writes the same rows to PostgreSQL via binary COPY. Each definition declares which engine it targets, and a collector never runs against the other one — see [PostgreSQL targets](../docs/darling/postgresql-targets.md#).
- `PerformanceMonitor.Alerting` owns the shared alert engine — the same thresholds, edge-trigger gates, cooldowns, and dedup fingerprints Lite uses.
- The analysis/recommendations pipeline (the same inference engine behind both apps' Recommendations tabs and the `analyze_server` MCP tool) runs on a schedule inside the service.

A collector, alert, or analysis change lands once in the shared libraries and both editions get it. A Darling install monitoring a server even derives the **same `server_id`** Lite would for that server, because the identity rule (`host[:database][:RO]`, hashed) is shared too.

---

## When to Choose Darling vs. Lite

| | **Lite** | **Darling** |
|---|---|---|
| Collection runs | While the desktop app is open (or in the tray) | 24/7 as a Windows service, or on Linux with systemd or Docker Compose |
| Data lives | Locally per seat (DuckDB + Parquet) | Centrally (PostgreSQL / TimescaleDB) |
| Execution plans | Not stored (fetched live when you view a query) | Captured and stored, TOAST-compressed (`capturePlans`, default on) |
| Viewers | The app is the viewer | Any number of viewer seats read the central store |
| Setup | Download and run | On Windows, extract the release zip and run `install-darling.ps1` (on Linux, see [Run on Linux](../docs/darling-quick-start.md#run-on-linux-docker-compose-or-systemd)). The shipped default is the bundled store (managed mode), so you provision no database. To use your own PostgreSQL, provision it and set its connection string. |
| Best for | Quick triage, consultants, a handful of servers | Always-on team monitoring, larger estates, one shared store |
| Configuration | Settings UI | `darling.json` seeds the first start. After that, the Viewer edits servers, alert settings and notification routes in the store. The web dashboard's Admin page edits a server in place and the agent channel of a notification route, for a sign-in that can make changes. Its Alert Settings tab is read-only. |

Nothing is installed on the monitored SQL Servers by either edition beyond two lightweight Extended Events ring-buffer sessions and, when it is unset, a one-time `blocked process threshold` bootstrap (see [What the Service Does on Monitored Servers](../docs/darling/monitored-servers.md#what-the-service-does-on-monitored-servers)).

---

## Quick Start

> **Installing on Windows?** Follow [`docs/darling/install.md`](../docs/darling/install.md). It is the ordered path from a downloaded zip to a working install, with a proof point at every step.
>
> **Installing on Linux, or turning on the web dashboard?** Read [`docs/darling-quick-start.md`](../docs/darling-quick-start.md).

The install reference that was here moved to these pages:

- [Install Darling](../docs/darling/install.md#install-reference): prerequisites, `darling.json`, the pre-flight, the Windows service, the install location, the service account, building from source and the CET error.
- [The Darling quick start](../docs/darling-quick-start.md#run-on-linux-docker-compose-or-systemd): Docker Compose and systemd on Linux.
- [Running Darling day to day](../docs/darling/admin.md#upgrading-an-existing-install): upgrade and roll back.
- [Darling and your monitored servers](../docs/darling/monitored-servers.md): what the service does on each server, its Extended Events sessions and the permissions it needs.

The headings below keep the old links working.

### Prerequisites

This section moved to [Install Darling](../docs/darling/install.md#prerequisites).

### Configure darling.json

This section moved to [Install Darling](../docs/darling/install.md#configure-darlingjson).

### Validate the Config (Pre-flight)

This section moved to [Install Darling](../docs/darling/install.md#validate-the-config-pre-flight).

### Check Host/Store Sizing Settings

This section moved to [Install Darling](../docs/darling/install.md#check-hoststore-sizing-settings).

### Diagnostics Bundle

This section moved to [The command line reference](../docs/darling/cli.md#diagnostics-bundle).

<a id="run-on-linux-docker-compose-or-systemd-1804"></a>
### Run on Linux (Docker Compose or systemd)

This section moved to [the Darling quick start](../docs/darling-quick-start.md#run-on-linux-docker-compose-or-systemd).

### Install as a Windows Service

This section moved to [Install Darling](../docs/darling/install.md#install-as-a-windows-service).

#### Upgrading an existing install

This section moved to [Running Darling day to day](../docs/darling/admin.md#upgrading-an-existing-install).

##### Rolling back to an older build

This section moved to [Running Darling day to day](../docs/darling/admin.md#rolling-back-to-an-older-build).

#### Upgrading from a build that predates the script

This section moved to [Running Darling day to day](../docs/darling/admin.md#upgrading-from-a-build-that-predates-the-script).

#### The install location has to be machine-scoped

This section moved to [Install Darling](../docs/darling/install.md#the-install-location-has-to-be-machine-scoped).

#### Run the service as a domain account or gMSA

This section moved to [Install Darling](../docs/darling/install.md#run-the-service-as-a-domain-account-or-gmsa).

### Build from source

This section moved to [Install Darling](../docs/darling/install.md#build-from-source).

### Windows stops at start: the CET error

This section moved to [Install Darling](../docs/darling/install.md#windows-stops-at-start-the-cet-error).

### What the Service Does on Monitored Servers

This section moved to [Darling and your monitored servers](../docs/darling/monitored-servers.md#what-the-service-does-on-monitored-servers).

### The Extended Events Sessions the Service Creates

This section moved to [Darling and your monitored servers](../docs/darling/monitored-servers.md#the-extended-events-sessions-the-service-creates).

### Permissions on Monitored Servers

This section moved to [Darling and your monitored servers](../docs/darling/monitored-servers.md#permissions-on-monitored-servers).

---

## Configuration Reference

This section covered every block of `darling.json` and the collector settings. It moved to these pages:

- [Configuration Reference](../docs/darling/configuration.md): where each setting lives, `postgres`, `servers`, the PostgreSQL actions, the per-server plan options, `peers`, `heartbeat` and `reads`.
- [Alert and notification settings](../docs/darling/alerts-and-notifications.md): `alerts`, `analysis`, `smtp`, `webhooks` and the notification routes.
- [Agents and alert sources](../docs/darling/agents-and-alert-sources.md): hand an alert to your own agent, practice requests, the export of judged cases, and the logins for systems that send alerts.
- [MCP configuration](../docs/darling/configuration-mcp.md): the `mcp` block.
- [Web dashboard configuration](../docs/darling/configuration-web.md): the `web` block.
- [Collector schedules and switches](../docs/darling/collector-settings.md): why there are no schedule settings, and the verbs and tools that change a collector.

The headings below keep the old links working.

### Where each setting lives: first-run seed or file only

This section moved to [Configuration Reference](../docs/darling/configuration.md#where-each-setting-lives-first-run-seed-or-file-only).

### postgres

This section moved to [Configuration Reference](../docs/darling/configuration.md#postgres).

### servers (array, at least one entry)

This section moved to [Configuration Reference](../docs/darling/configuration.md#servers-array-at-least-one-entry).

### Testing an index on PostgreSQL

This section moved to [Configuration Reference](../docs/darling/configuration.md#testing-an-index-on-postgresql).

### allowedAwsRoles (array, optional)

This section moved to [Configuration Reference](../docs/darling/configuration.md#allowedawsroles-array-optional).

### alerts

This section moved to [Alert and notification settings](../docs/darling/alerts-and-notifications.md#alerts).

#### Which engine each built-in alert and health band covers

This section moved to [Alert and notification settings](../docs/darling/alerts-and-notifications.md#which-engine-each-built-in-alert-and-health-band-covers).

#### Notification routes — sending alert families to different channels

This section moved to [Alert and notification settings](../docs/darling/alerts-and-notifications.md#notification-routes--sending-alert-families-to-different-channels).

### Export the judged cases to test your own agent (#6236)

This section moved to [Agents and alert sources](../docs/darling/agents-and-alert-sources.md#export-the-judged-cases-to-test-your-own-agent-6236).

### mcp

This section moved to [MCP configuration](../docs/darling/configuration-mcp.md#mcp).

### web

This section moved to [Web dashboard configuration](../docs/darling/configuration-web.md#web).

### peers

This section moved to [Configuration Reference](../docs/darling/configuration.md#peers).

### Enable or disable a collector headlessly (`--enable-collector` / `--disable-collector`)

This section moved to [Collector schedules and switches](../docs/darling/collector-settings.md#enable-or-disable-a-collector-headlessly---enable-collector----disable-collector).

### Make a new password key (`--reset-password-key`)

This section moved to [Collector schedules and switches](../docs/darling/collector-settings.md#make-a-new-password-key---reset-password-key).

### Set the time of day a daily collector runs (`--set-collector-run-at`)

This section moved to [Collector schedules and switches](../docs/darling/collector-settings.md#set-the-time-of-day-a-daily-collector-runs---set-collector-run-at).

### Drop the Extended Events sessions a removed server left behind (`--drop-xe-sessions`)

This section moved to [Collector schedules and switches](../docs/darling/collector-settings.md#drop-the-extended-events-sessions-a-removed-server-left-behind---drop-xe-sessions).

### Make a login for a system that sends alerts (`--add-alert-source` and friends)

This section moved to [Agents and alert sources](../docs/darling/agents-and-alert-sources.md#make-a-login-for-a-system-that-sends-alerts---add-alert-source-and-friends).

#### Alert sources on another machine

This section moved to [Agents and alert sources](../docs/darling/agents-and-alert-sources.md#alert-sources-on-another-machine).

---

## PostgreSQL Targets

This section covered PostgreSQL servers as monitored targets. It moved to [PostgreSQL Targets](../docs/darling/postgresql-targets.md).

### Permissions on a PostgreSQL target

This section moved to [PostgreSQL Targets](../docs/darling/postgresql-targets.md#permissions-on-a-postgresql-target).

### Oldest PostgreSQL version per collector

This section moved to [PostgreSQL Targets](../docs/darling/postgresql-targets.md#oldest-postgresql-version-per-collector).

---

## Operations

This section moved to [Operations](../docs/darling/operations.md). It covered the store, TimescaleDB, the background workers, retention, the logs, the Viewer, restarts and how to watch the service from outside.

### The Store

This section moved to [Operations](../docs/darling/operations.md#the-store).

### Background workers: sizing an unmanaged store, and what happens if you don't

This section moved to [Operations](../docs/darling/operations.md#background-workers-sizing-an-unmanaged-store-and-what-happens-if-you-dont).

### Retention

This section moved to [Operations](../docs/darling/operations.md#retention).

### Logs

This section moved to [Operations](../docs/darling/operations.md#logs).

### Watching the service from outside

This section moved to [Operations](../docs/darling/operations.md#watching-the-service-from-outside).

---

## Agent Changes: List, Undo and the Switch

This section moved to [Security and agent changes](../docs/darling/security.md#agent-changes-list-undo-and-the-switch).

## Connect a Remote Viewer

This section covered a viewer seat on another machine. It moved to [The WPF viewer](../docs/darling/viewer.md#connect-a-remote-viewer).

### 1. Export the handoff folder (on the service host)

This section moved to [The WPF viewer](../docs/darling/viewer.md#1-export-the-handoff-folder-on-the-service-host).

### Manual configuration (fallback)

This section moved to [The WPF viewer](../docs/darling/viewer.md#manual-configuration-fallback).

---

## Troubleshooting

This section moved to [Darling troubleshooting](../docs/darling/troubleshooting.md).

---

## How It Runs (Reference)

This section listed the fixed cadences of the service. It moved to [Running Darling day to day](../docs/darling/admin.md#how-it-runs-reference).

---

## Managed Bundled PostgreSQL

This section covered the store that the service runs for itself in managed mode. It moved to [Managed Bundled PostgreSQL](../docs/darling/managed-postgresql.md).

## Security & Least-Privilege Roles

This section moved to [Security and agent changes](../docs/darling/security.md#security--least-privilege-roles).

## Opt-in Network Endpoints (LAN)

This section moved to [Opt-in Network Endpoints (LAN)](../docs/darling/network-endpoints.md).

### Guided setup (`--configure-network`)

This section moved to [Opt-in Network Endpoints (LAN)](../docs/darling/network-endpoints.md#guided-setup---configure-network).

### Firewall rules (`--configure-firewall`)

This section moved to [Opt-in Network Endpoints (LAN)](../docs/darling/network-endpoints.md#firewall-rules---configure-firewall).

### Headless enable/disable + firewall (`--enable-mcp` / `--enable-web`)

This section moved to [Opt-in Network Endpoints (LAN)](../docs/darling/network-endpoints.md#headless-enabledisable--firewall---enable-mcp----enable-web).

### Store endpoint (viewer over the LAN)

This section moved to [Opt-in Network Endpoints (LAN)](../docs/darling/network-endpoints.md#store-endpoint-viewer-over-the-lan).

### MCP endpoint (assistant over the LAN)

This section moved to [Opt-in Network Endpoints (LAN)](../docs/darling/network-endpoints.md#mcp-endpoint-assistant-over-the-lan).

### Web endpoint (browser over the LAN)

This section moved to [Opt-in Network Endpoints (LAN)](../docs/darling/network-endpoints.md#web-endpoint-browser-over-the-lan).

## Level 2: agents change mute rules and custom alert rules

This section moved to [Security and agent changes](../docs/darling/security.md#level-2-agents-change-mute-rules-and-custom-alert-rules).

## Per-server threshold overrides

This section covered a threshold value for one server or one database. It moved to [Alerts & Notifications](../docs/alerts.md#per-server-threshold-overrides).
