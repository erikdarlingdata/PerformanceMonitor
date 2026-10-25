# Documentation index

Audience: everyone. This page lists every page in `docs/`, grouped by who it is for.

Each page starts with an `Audience:` line under its title. The line tells you if the page is for operators, contributors or maintainers. The [root README](../README.md#documentation) lists the main operator pages.

## Install

- [Choosing an edition](editions.md): Lite, Darling and Dashboard side by side.
- [Lite guide](lite-guide.md): install, upgrade and configure Lite.
- [Darling guides](darling/README.md): the reading order for all Darling pages.
- [Install Darling](darling/install.md): from a downloaded zip to a service that collects data.
- [Darling quick start](darling-quick-start.md): the short path for Windows and Linux, plus the web dashboard.
- [Darling install guide (moved)](uat-onboarding.md): a pointer from the old install guide to the new pages.
- [PostgreSQL target runbook](postgres-first-target-runbook.md): point Darling at a PostgreSQL target, with a proof point at each step.

## Run

- [Running Darling day to day](darling/admin.md): the routine tasks after the first week.
- [Command line reference](darling/cli.md): every verb that the Darling service exe accepts.
- [The WPF viewer](darling/viewer.md): open the desktop viewer on the service host or on another machine.
- [The web dashboard](darling/web-dashboard.md): turn on the browser view and reach it from another machine.
- [MCP server](mcp-server.md#darling): turn on the Darling MCP endpoint for an LLM client.
- [Darling operations](darling/operations.md): the store, TimescaleDB, retention, the logs, restarts and watching the service from outside.
- [Darling troubleshooting](darling/troubleshooting.md): every Darling service log line and what it means.
- [Back up, restore and move the store](darling-store-backup.md): protect and relocate a Darling store.
- [Darling store sizing](darling-store-sizing.md): disk, RAM and vCPU for a store host.
- [Clearing a Retention Held alert](retention-hold-runbook.md): steps to resume a paused retention policy.
- [Backfilling the successor dailies](runbooks/a6-successor-daily-backfill.md): a runbook to backfill the interval-honest daily aggregates.

## Configure

- [Authentication](authentication.md): authentication types and credential profiles.
- [Permissions](permissions.md): the grants that each monitored server needs.
- [Platform notes](platform-notes.md): settings for AWS RDS and Aurora.
- [Alerts and notifications](alerts.md): alert types, channels, routes and email.
- [Agent dispatch](darling-agent-dispatch.md): hand an alert to your own AI agent.
- [Signed webhooks](signed-webhooks.md): check that a generic webhook post came from Darling.
- [MCP server](mcp-server.md): the MCP tools, setup and client notes for both editions.
- [Lite Extended Events sessions](lite-extended-events.md): the sessions Lite creates, and how to remove them.
- [Darling and your monitored servers](darling/monitored-servers.md): what the Darling service does on each SQL Server, and the permissions it needs.
- [Darling PostgreSQL targets](darling/postgresql-targets.md): monitor a PostgreSQL server with Darling.
- [Managed bundled PostgreSQL](darling/managed-postgresql.md): the store that the Darling service runs for itself.
- [Darling configuration reference](darling/configuration.md): where each `darling.json` setting lives, `postgres`, `servers`, `peers`, `heartbeat` and `reads`.
- [Darling alert and notification settings](darling/alerts-and-notifications.md): the `alerts`, `analysis`, `smtp` and `webhooks` blocks, and the notification routes.
- [Darling agents and alert sources](darling/agents-and-alert-sources.md): hand an alert to your own agent, practice requests, and logins for systems that send alerts.
- [Darling MCP configuration](darling/configuration-mcp.md): the `mcp` block of `darling.json`.
- [Darling web dashboard configuration](darling/configuration-web.md): the `web` block of `darling.json`.
- [Darling collector schedules and switches](darling/collector-settings.md): the verbs and tools that turn a collector on or off and set when it runs.
- [Darling security and agent changes](darling/security.md): the store's schemas and roles, the agent change list, and Level 2.
- [Darling network endpoints](darling/network-endpoints.md): reach the store, MCP and the web dashboard over the LAN.

## Monitor your servers

- [Features and screenshots](features.md): what you get, with screenshots.
- [Tabs](tabs.md): what each tab in Lite and the Darling viewer shows.
- [Agent job monitoring](agent-job-monitoring.md): running SQL Agent jobs and jobs that run too long.
- [Performance impact](performance-impact.md): the load on monitored servers and on the local host.
- [Troubleshooting](troubleshooting.md): common problems for Lite and Darling.

## Reference

- [How collection works](how-collection-works.md): a tour of the collection pipeline.
- [Support and sponsorship](support.md): commercial support and sponsorship.
- [Third-party components](third-party-components.md): components that ship with the product, and their licenses.
- [Changelog files](changelog/): the release notes for each version, one file per version.

## Design notes (for contributors)

These pages are kept as history. They record the reasoning at the time. Do not follow them as procedures.

- [PostgreSQL alerting scoping note](design/postgres-alerting-design-note.md): why the PostgreSQL alert thresholds are constants.
- [PostgreSQL blocking chains design note](design/postgres-blocking-design-note.md): how PostgreSQL blocking collection was designed and what building it changed.

## Maintainers

- [Building from source](building-from-source.md): folder structure and build commands.
- [Release upgrade validation](releasing.md): how to test an upgrade from a released build.
