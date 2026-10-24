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
- [Darling MCP setup](darling/mcp.md): turn on the MCP endpoint for an LLM client.
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

- [PostgreSQL alerting scoping note](postgres-alerting-design-note.md): why the PostgreSQL alert thresholds are constants.
- [PostgreSQL blocking chains design note](postgres-blocking-design-note.md): how PostgreSQL blocking collection was designed and what building it changed.

## Maintainers

- [Building from source](building-from-source.md): folder structure and build commands.
- [Release upgrade validation](releasing.md): how to test an upgrade from a released build.
