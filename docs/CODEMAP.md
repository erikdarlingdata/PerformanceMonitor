# Code map

Audience: contributors.

This page is generated from the repository by `tools/codemap/codemap.py`. Do not edit it by hand. The area text comes from `tools/codemap/areas.json`. The same content is in `docs/codemap.json` for tools and in `docs/codemap.html` as one static page.

## Overview

This repository holds two products you can build and run. Darling is a headless collector service that keeps its data in PostgreSQL with TimescaleDB, and it comes with a WPF viewer and a web dashboard. Lite is a portable WPF app that keeps its data in DuckDB.

Both products collect data from SQL Server and answer questions through an MCP server for AI tools. They share code for collectors, alerting, notifications, analysis and plan analysis.

The Full Dashboard and the CLI installer are frozen in a zip file. Nothing in the repository builds them.

In numbers:

- 23 projects (4 app, 10 library, 5 test, 4 tool).
- 320 Darling MCP tools and 130 Lite MCP tools.
- 318 store migrations. The newest is version 319.
- 4998 test classes in 5 test projects.
- 20 CI workflows.

To build, run these from the repository root:

```
dotnet build Lite/PerformanceMonitorLite.csproj
dotnet build Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj
dotnet build Darling/PerformanceMonitor.Darling.Viewer/PerformanceMonitor.Darling.Viewer.csproj
```

## Areas

Each area lists the folders it covers and the files to open first.

### Darling service (collectors, scheduler)

The Darling service is the headless process that collects data. At start it reads darling.json, bootstraps the store, and then runs each collector on its schedule. Collectors that are shared with Lite live in a separate project, and the service runs them through its collector runner. The same process hosts the MCP endpoint and the web dashboard.

Folders: [`Darling/PerformanceMonitor.Darling.Service`](../Darling/PerformanceMonitor.Darling.Service/), [`PerformanceMonitor.Collectors`](../PerformanceMonitor.Collectors/)

Start here:

- [`Darling/PerformanceMonitor.Darling.Service/Program.cs`](../Darling/PerformanceMonitor.Darling.Service/Program.cs): The host. Builds the service and starts it.
- [`Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs`](../Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs): The main loop, split across the DarlingWorker.*.cs files.
- [`Darling/PerformanceMonitor.Darling.Service/DarlingWorker.Collection.cs`](../Darling/PerformanceMonitor.Darling.Service/DarlingWorker.Collection.cs): How a collection pass is scheduled.
- [`Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs`](../Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs): Runs one collector against one monitored server.
- [`Darling/PerformanceMonitor.Darling.Service/DarlingConfig.cs`](../Darling/PerformanceMonitor.Darling.Service/DarlingConfig.cs): The settings that darling.json holds.
- [`Darling/PerformanceMonitor.Darling.Service/DarlingCliCommands.cs`](../Darling/PerformanceMonitor.Darling.Service/DarlingCliCommands.cs): The command line verbs of the service exe.

### Darling storage (PostgreSQL store, migrations, rollups)

This project owns the PostgreSQL store. It holds the numbered migrations and the schema generator. It also holds the store classes that read and write each table, and the rollups and retention tiers that keep the store small. A change to a table starts with a new migration in the list. The Darling export tool also lives here, because it reads the store and writes Parquet files.

Folders: [`Darling/PerformanceMonitor.Darling.Storage`](../Darling/PerformanceMonitor.Darling.Storage/), [`Exporter/PerformanceMonitor.Exporter`](../Exporter/PerformanceMonitor.Exporter/)

Start here:

- [`Darling/PerformanceMonitor.Darling.Storage/PgMigrations.cs`](../Darling/PerformanceMonitor.Darling.Storage/PgMigrations.cs): The numbered migration list. The newest rung is at the end of Scripts.
- [`Darling/PerformanceMonitor.Darling.Storage/PgSchemaGenerator.cs`](../Darling/PerformanceMonitor.Darling.Storage/PgSchemaGenerator.cs): Builds the collector tables for migration 1 and later table changes.
- [`Darling/PerformanceMonitor.Darling.Storage/RetentionTierRouter.cs`](../Darling/PerformanceMonitor.Darling.Storage/RetentionTierRouter.cs): Decides which retention tier a read or a prune uses.
- [`Darling/PerformanceMonitor.Darling.Storage/QueryTagRollupSql.cs`](../Darling/PerformanceMonitor.Darling.Storage/QueryTagRollupSql.cs): An example of a rollup and its SQL.
- [`Darling/PerformanceMonitor.Darling.Storage/IntervalRollupCountGuard.cs`](../Darling/PerformanceMonitor.Darling.Storage/IntervalRollupCountGuard.cs): A guard that checks interval rollup counts.
- [`Exporter/README.md`](../Exporter/README.md): The darling-export tool, which writes Parquet files from the store.

### Darling MCP

Darling answers MCP clients from the service process. Each tool is a public static method that carries the McpServerTool attribute. The web endpoints call the same methods, so the two surfaces return the same data. A write policy decides which tools can change anything, and each change is off until an operator turns it on.

Folders: [`Darling/PerformanceMonitor.Darling.Service/Mcp`](../Darling/PerformanceMonitor.Darling.Service/Mcp/), [`PerformanceMonitor.Common/Mcp`](../PerformanceMonitor.Common/Mcp/)

Start here:

- [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHostService.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHostService.cs): Starts the MCP endpoint and registers the tool classes.
- [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingCoreToolProfile.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingCoreToolProfile.cs): The small set of core tools.
- [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs): A first tool class to read.
- [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWritePolicy.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWritePolicy.cs): The rules for tools that change things.
- [`PerformanceMonitor.Common/Mcp/McpSchemaCompat.cs`](../PerformanceMonitor.Common/Mcp/McpSchemaCompat.cs): Builds tool schemas that more MCP clients accept. Both products use it.

### Darling web UI and front door

The web dashboard is plain JavaScript pages in wwwroot that call the service over HTTP. The endpoint files in the service answer those calls. The front door lets one Darling service route to other Darling services, so a team can use one address.

Folders: [`Darling/PerformanceMonitor.Darling.Service/wwwroot`](../Darling/PerformanceMonitor.Darling.Service/wwwroot/), [`Darling/PerformanceMonitor.Darling.Service/FrontDoor`](../Darling/PerformanceMonitor.Darling.Service/FrontDoor/)

Start here:

- [`Darling/PerformanceMonitor.Darling.Service/DarlingWebEndpoints.cs`](../Darling/PerformanceMonitor.Darling.Service/DarlingWebEndpoints.cs): The /api endpoints. Other DarlingWebEndpoints.*.cs files add more.
- [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingWebHostService.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingWebHostService.cs): Starts the web host.
- [`Darling/PerformanceMonitor.Darling.Service/wwwroot/index.html`](../Darling/PerformanceMonitor.Darling.Service/wwwroot/index.html): The page shell.
- [`Darling/PerformanceMonitor.Darling.Service/wwwroot/js/app.js`](../Darling/PerformanceMonitor.Darling.Service/wwwroot/js/app.js): The browser app that loads each page.
- [`Darling/PerformanceMonitor.Darling.Service/FrontDoor/DarlingFrontDoorRouting.cs`](../Darling/PerformanceMonitor.Darling.Service/FrontDoor/DarlingFrontDoorRouting.cs): How the front door picks a service.
- [`Darling/PerformanceMonitor.Darling.Service/FrontDoor/DarlingFrontDoorTool.cs`](../Darling/PerformanceMonitor.Darling.Service/FrontDoor/DarlingFrontDoorTool.cs): The MCP tools of the front door.

### Darling Viewer (WPF)

The Viewer is a desktop app that reads from a Darling service. It shows the fleet, the server tabs, alerts and the alert rule editors. It can run on the service host or on another machine.

Folders: [`Darling/PerformanceMonitor.Darling.Viewer`](../Darling/PerformanceMonitor.Darling.Viewer/)

Start here:

- [`Darling/PerformanceMonitor.Darling.Viewer/Program.cs`](../Darling/PerformanceMonitor.Darling.Viewer/Program.cs): The entry point.
- [`Darling/PerformanceMonitor.Darling.Viewer/App.xaml.cs`](../Darling/PerformanceMonitor.Darling.Viewer/App.xaml.cs): App start and shared resources.
- [`Darling/PerformanceMonitor.Darling.Viewer/MainWindow.xaml.cs`](../Darling/PerformanceMonitor.Darling.Viewer/MainWindow.xaml.cs): The main window.
- [`Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.cs`](../Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.cs): How the Viewer reads its data.

### Lite (WPF, DuckDB, archive and compaction)

Lite is one WPF app that collects, stores and shows the data. A background service runs the collectors on a timer and writes to DuckDB. Older data moves to Parquet files, which a compaction step merges. A retention service deletes archive files that are past their age limit.

Folders: [`Lite`](../Lite/)

Start here:

- [`Lite/Program.cs`](../Lite/Program.cs): The entry point, including the headless mode.
- [`Lite/App.xaml.cs`](../Lite/App.xaml.cs): App start.
- [`Lite/MainWindow.xaml.cs`](../Lite/MainWindow.xaml.cs): The main window.
- [`Lite/Services/CollectionBackgroundService.cs`](../Lite/Services/CollectionBackgroundService.cs): The timer that runs collection, archive and retention.
- [`Lite/Services/ArchiveService.cs`](../Lite/Services/ArchiveService.cs): Moves old rows from DuckDB to Parquet.
- [`Lite/Services/ParquetCompaction.cs`](../Lite/Services/ParquetCompaction.cs): Merges small Parquet files.

### Lite MCP

Lite has its own MCP server with the same tool style as Darling. Each tool class sits in Lite/Mcp and reads from the local DuckDB data. Lite also has an investigation store that agents can read and write.

Folders: [`Lite/Mcp`](../Lite/Mcp/), [`Lite/Investigations`](../Lite/Investigations/)

Start here:

- [`Lite/Mcp/McpHostService.cs`](../Lite/Mcp/McpHostService.cs): Starts the MCP endpoint.
- [`Lite/Mcp/McpHealthTools.cs`](../Lite/Mcp/McpHealthTools.cs): A first tool class to read.
- [`Lite/Mcp/McpWritePolicy.cs`](../Lite/Mcp/McpWritePolicy.cs): The rules for tools that change things.
- [`Lite/Mcp/McpToolGuideTools.cs`](../Lite/Mcp/McpToolGuideTools.cs): The tool that tells a client which tool to use.

### Shared code

Both products use these projects. They hold the alert engine, the notification channels, the analysis engines and the plan analyzer. They also hold the charts and controls that both WPF apps use, and the types that the other projects share. A change here reaches both products.

Folders: [`PerformanceMonitor.Alerting`](../PerformanceMonitor.Alerting/), [`PerformanceMonitor.Analysis`](../PerformanceMonitor.Analysis/), [`PerformanceMonitor.Common`](../PerformanceMonitor.Common/), [`PerformanceMonitor.Notifications`](../PerformanceMonitor.Notifications/), [`PerformanceMonitor.PlanAnalysis`](../PerformanceMonitor.PlanAnalysis/), [`PerformanceMonitor.Ui`](../PerformanceMonitor.Ui/), [`Darling/PerformanceMonitor.Darling.Analysis`](../Darling/PerformanceMonitor.Darling.Analysis/), [`Darling/PerformanceMonitor.Darling.Remediation`](../Darling/PerformanceMonitor.Darling.Remediation/)

Start here:

- [`PerformanceMonitor.Alerting/AlertEngine.cs`](../PerformanceMonitor.Alerting/AlertEngine.cs): The alert engine that both products use.
- [`PerformanceMonitor.Notifications/AlertDelivery.cs`](../PerformanceMonitor.Notifications/AlertDelivery.cs): What was sent for an alert, and with what result.
- [`PerformanceMonitor.PlanAnalysis/ActualPlanExecutor.cs`](../PerformanceMonitor.PlanAnalysis/ActualPlanExecutor.cs): Runs the plan analyzer.
- [`Darling/PerformanceMonitor.Darling.Analysis/DarlingAnalysisService.cs`](../Darling/PerformanceMonitor.Darling.Analysis/DarlingAnalysisService.cs): Darling's analysis service.
- [`Darling/PerformanceMonitor.Darling.Remediation/PostgresRemediationExecutor.cs`](../Darling/PerformanceMonitor.Darling.Remediation/PostgresRemediationExecutor.cs): Runs an approved change on a PostgreSQL target.

### Tests

Darling.Tests and Lite.Tests hold nearly all of the tests. Each test class is one file whose name ends in Tests.cs. The Exporter has its own small test project. Tests tagged Stage=Guard run first in CI.

Folders: [`Darling/Darling.Tests`](../Darling/Darling.Tests/), [`Darling/Darling.Tests.Standin`](../Darling/Darling.Tests.Standin/), [`Lite.Tests`](../Lite.Tests/), [`Exporter/PerformanceMonitor.Exporter.Tests`](../Exporter/PerformanceMonitor.Exporter.Tests/), [`Exporter/PerformanceMonitor.Exporter.TestChild`](../Exporter/PerformanceMonitor.Exporter.TestChild/)

Start here:

- [`Darling/Darling.Tests/Darling.Tests.csproj`](../Darling/Darling.Tests/Darling.Tests.csproj): The Darling test project.
- [`Lite.Tests/Lite.Tests.csproj`](../Lite.Tests/Lite.Tests.csproj): The Lite test project.
- [`.github/scripts/ci-select.py`](../.github/scripts/ci-select.py): Picks which test classes CI runs for a change.
- [`.github/scripts/pre-push.sh`](../.github/scripts/pre-push.sh): Runs the first CI gates on your machine.

### CI and scripts (.github)

The workflows in .github/workflows run the checks and build the releases. The scripts in .github/scripts hold the rules for what each run does, so a rule lives in one place. The pull request workflow is the first one to read.

Folders: [`.github`](../.github/)

Start here:

- [`.github/workflows/pr.yml`](../.github/workflows/pr.yml): The pull request workflow.
- [`.github/scripts/ci-plan.py`](../.github/scripts/ci-plan.py): Decides what a run does.
- [`.github/scripts/ci-select.py`](../.github/scripts/ci-select.py): Picks the test jobs.
- [`.github/workflows/nightly.yml`](../.github/workflows/nightly.yml): The nightly build and checks.
- [`.github/workflows/build.yml`](../.github/workflows/build.yml): The release build.

### Tools

The tools folder holds small programs and scripts for contributors. They include measurement repros, the web UI smoke check, the docs publisher, the changelog archiver and this code map. None of them ship in a release.

Folders: [`tools`](../tools/), [`Darling/tools`](../Darling/tools/)

Start here:

- [`tools/codemap/codemap.py`](../tools/codemap/codemap.py): Builds this code map.
- [`tools/ui-smoke/README.md`](../tools/ui-smoke/README.md): The web UI smoke check.
- [`tools/docs-site/README.md`](../tools/docs-site/README.md): How the Darling docs are published.
- [`tools/pg-verification-rig/README.md`](../tools/pg-verification-rig/README.md): A PostgreSQL rig for checks.
- [`Darling/tools/install-darling.ps1`](../Darling/tools/install-darling.ps1): The Darling install script.

### The deprecated freeze

The Full Dashboard and the CLI installer are frozen in one zip file. Nothing builds, tests or releases them. The install and upgrades folders hold the T-SQL that the frozen installer deploys, and those scripts must stay idempotent.

Folders: [`deprecated`](../deprecated/), [`install`](../install/), [`upgrades`](../upgrades/)

Start here:

- [`deprecated/README.md`](../deprecated/README.md): What the zip holds and how to build it at the freeze commit.
- [`install/01_install_database.sql`](../install/01_install_database.sql): The first install script.

## Projects

| Project | Kind | Target framework |
|---|---|---|
| [`Darling/Darling.Tests.Standin/Darling.Tests.Standin.csproj`](../Darling/Darling.Tests.Standin/Darling.Tests.Standin.csproj) | Test | net10.0 |
| [`Darling/Darling.Tests/Darling.Tests.csproj`](../Darling/Darling.Tests/Darling.Tests.csproj) | Test | net10.0, net10.0-windows |
| [`Darling/PerformanceMonitor.Darling.Analysis/PerformanceMonitor.Darling.Analysis.csproj`](../Darling/PerformanceMonitor.Darling.Analysis/PerformanceMonitor.Darling.Analysis.csproj) | Library | net10.0 |
| [`Darling/PerformanceMonitor.Darling.Remediation/PerformanceMonitor.Darling.Remediation.csproj`](../Darling/PerformanceMonitor.Darling.Remediation/PerformanceMonitor.Darling.Remediation.csproj) | Library | net10.0 |
| [`Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj`](../Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj) | App | net10.0 |
| [`Darling/PerformanceMonitor.Darling.Storage/PerformanceMonitor.Darling.Storage.csproj`](../Darling/PerformanceMonitor.Darling.Storage/PerformanceMonitor.Darling.Storage.csproj) | Library | net10.0 |
| [`Darling/PerformanceMonitor.Darling.Viewer/PerformanceMonitor.Darling.Viewer.csproj`](../Darling/PerformanceMonitor.Darling.Viewer/PerformanceMonitor.Darling.Viewer.csproj) | App | net10.0-windows |
| [`Darling/tools/generate-ladder-fixture/generate-ladder-fixture.csproj`](../Darling/tools/generate-ladder-fixture/generate-ladder-fixture.csproj) | Tool | net10.0 |
| [`Exporter/PerformanceMonitor.Exporter.TestChild/PerformanceMonitor.Exporter.TestChild.csproj`](../Exporter/PerformanceMonitor.Exporter.TestChild/PerformanceMonitor.Exporter.TestChild.csproj) | Test | net10.0 |
| [`Exporter/PerformanceMonitor.Exporter.Tests/PerformanceMonitor.Exporter.Tests.csproj`](../Exporter/PerformanceMonitor.Exporter.Tests/PerformanceMonitor.Exporter.Tests.csproj) | Test | net10.0 |
| [`Exporter/PerformanceMonitor.Exporter/PerformanceMonitor.Exporter.csproj`](../Exporter/PerformanceMonitor.Exporter/PerformanceMonitor.Exporter.csproj) | App | net10.0 |
| [`Lite.Tests/Lite.Tests.csproj`](../Lite.Tests/Lite.Tests.csproj) | Test | net10.0-windows |
| [`Lite/PerformanceMonitorLite.csproj`](../Lite/PerformanceMonitorLite.csproj) | App | net10.0-windows |
| [`PerformanceMonitor.Alerting/PerformanceMonitor.Alerting.csproj`](../PerformanceMonitor.Alerting/PerformanceMonitor.Alerting.csproj) | Library | net10.0 |
| [`PerformanceMonitor.Analysis/PerformanceMonitor.Analysis.csproj`](../PerformanceMonitor.Analysis/PerformanceMonitor.Analysis.csproj) | Library | net10.0 |
| [`PerformanceMonitor.Collectors/PerformanceMonitor.Collectors.csproj`](../PerformanceMonitor.Collectors/PerformanceMonitor.Collectors.csproj) | Library | net10.0 |
| [`PerformanceMonitor.Common/PerformanceMonitor.Common.csproj`](../PerformanceMonitor.Common/PerformanceMonitor.Common.csproj) | Library | net10.0 |
| [`PerformanceMonitor.Notifications/PerformanceMonitor.Notifications.csproj`](../PerformanceMonitor.Notifications/PerformanceMonitor.Notifications.csproj) | Library | net10.0 |
| [`PerformanceMonitor.PlanAnalysis/PerformanceMonitor.PlanAnalysis.csproj`](../PerformanceMonitor.PlanAnalysis/PerformanceMonitor.PlanAnalysis.csproj) | Library | net10.0 |
| [`PerformanceMonitor.Ui/PerformanceMonitor.Ui.csproj`](../PerformanceMonitor.Ui/PerformanceMonitor.Ui.csproj) | Library | net10.0-windows |
| [`tools/CompactionRepro/CompactionRepro.csproj`](../tools/CompactionRepro/CompactionRepro.csproj) | Tool | net10.0 |
| [`tools/SliceRepairRepro/SliceRepairRepro.csproj`](../tools/SliceRepairRepro/SliceRepairRepro.csproj) | Tool | net10.0 |
| [`tools/ui-smoke-wpf/UiSmokeWpf.csproj`](../tools/ui-smoke-wpf/UiSmokeWpf.csproj) | Tool | net10.0-windows |

## Store migrations

Darling's PostgreSQL store has 318 migrations. The newest is version 319. The list is in [`Darling/PerformanceMonitor.Darling.Storage/PgMigrations.cs`](../Darling/PerformanceMonitor.Darling.Storage/PgMigrations.cs). Version numbers are not gapless, so the count is lower than the newest version.

## MCP tools

A tool is a method that carries the `McpServerTool` attribute. Each row names the tool and the file that declares it.

### Darling MCP tools (320)

| Tool | Declared in |
|---|---|
| `add_alert_source` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertSourceTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertSourceTools.cs) |
| `add_servers` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerAdminTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerAdminTools.cs) |
| `analyze_now` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpIncidentActionTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpIncidentActionTools.cs) |
| `analyze_plan_xml` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs) |
| `analyze_procedure_plan` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs) |
| `analyze_query_plan` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs) |
| `analyze_query_store_plan` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs) |
| `analyze_server` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs) |
| `approve_investigation_note` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationAdminTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationAdminTools.cs) |
| `assign_query_tag` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagTools.cs) |
| `assign_server_tag` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerTagTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerTagTools.cs) |
| `audit_config` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs) |
| `backtest_custom_alert_rule` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs) |
| `call_tool` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingLeanToolProfile.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingLeanToolProfile.cs) |
| `claim_investigation_request` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationTools.cs) |
| `clear_change_window` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpChangeWindowWriteTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpChangeWindowWriteTools.cs) |
| `clear_quiet_window` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQuietWindowTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQuietWindowTools.cs) |
| `close_investigation_request` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationAdminTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationAdminTools.cs) |
| `compare_analysis` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs) |
| `create_custom_alert_rule` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs) |
| `create_custom_view` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs) |
| `create_known_pattern` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpKnownPatternWriteTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpKnownPatternWriteTools.cs) |
| `create_mute_rule` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs) |
| `create_notification_route` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.RouteAdmin.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.RouteAdmin.cs) |
| `create_plan_guide` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForceTools.PlanGuide.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForceTools.PlanGuide.cs) |
| `create_query_tag` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagTools.cs) |
| `create_query_tag_rule` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagRuleTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagRuleTools.cs) |
| `create_server_tag` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerTagTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerTagTools.cs) |
| `create_web_access_group` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWebAccessTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWebAccessTools.cs) |
| `declare_change_window` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpChangeWindowWriteTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpChangeWindowWriteTools.cs) |
| `delete_custom_alert_rule` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs) |
| `delete_custom_view` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs) |
| `delete_known_pattern` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpKnownPatternWriteTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpKnownPatternWriteTools.cs) |
| `delete_mute_rule` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs) |
| `delete_notification_route` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs) |
| `delete_query_tag` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagTools.cs) |
| `delete_query_tag_rule` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagRuleTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagRuleTools.cs) |
| `delete_server_tag` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerTagTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerTagTools.cs) |
| `delete_web_access_group` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWebAccessTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWebAccessTools.cs) |
| `describe_custom_view_catalog` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs) |
| `disable_collector` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs) |
| `disable_server` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs) |
| `dismiss_alert_history` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.RouteAdmin.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.RouteAdmin.cs) |
| `drop_plan_guide` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForceTools.PlanGuide.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForceTools.PlanGuide.cs) |
| `edit_server` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerAdminTools.Edit.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerAdminTools.Edit.cs) |
| `enable_collector` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs) |
| `enable_server` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs) |
| `evict_cached_plan` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForceTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForceTools.cs) |
| `find_database` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFindDatabaseTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFindDatabaseTools.cs) |
| `find_queries_by_text` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFindQueriesByTextTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFindQueriesByTextTools.cs) |
| `find_schema_objects` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSchemaTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSchemaTools.cs) |
| `find_tools` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpToolSearchTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpToolSearchTools.cs) |
| `force_query_store_plan` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForceTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForceTools.cs) |
| `generate_webhook_signing_key` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.SigningKeys.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.SigningKeys.cs) |
| `get_active_queries` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSessionTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSessionTools.cs) |
| `get_active_query_plan_xml` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs) |
| `get_ag_health` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAgTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAgTools.cs) |
| `get_agent_alerts` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAgentAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAgentAlertTools.cs) |
| `get_agent_changes` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpIncidentActionTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpIncidentActionTools.cs) |
| `get_alert_history` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs) |
| `get_alert_settings` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs) |
| `get_analysis_facts` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs) |
| `get_analysis_findings` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs) |
| `get_blocked_process_xml` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs) |
| `get_blocking` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs) |
| `get_blocking_plan_xml` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs) |
| `get_blocking_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_blocking_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs) |
| `get_change_watches` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpChangeWatchTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpChangeWatchTools.cs) |
| `get_change_windows` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpChangeWindowTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpChangeWindowTools.cs) |
| `get_client_timeouts` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpClientTimeoutTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpClientTimeoutTools.cs) |
| `get_collection_health` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_collection_log` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_collection_state` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs) |
| `get_collector_cost` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorCostTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorCostTools.cs) |
| `get_collector_readiness` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorReadinessTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorReadinessTools.cs) |
| `get_collector_schedules` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs) |
| `get_collector_stall_probes` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStallProbeTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStallProbeTools.cs) |
| `get_command_result` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpIncidentActionTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpIncidentActionTools.cs) |
| `get_compiles_after_evict` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCompilesAfterEvictTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCompilesAfterEvictTools.cs) |
| `get_cpu_by_database` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWhoDroveItTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWhoDroveItTools.cs) |
| `get_cpu_scheduler_pressure` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanCacheSchedulerTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanCacheSchedulerTools.cs) |
| `get_cpu_utilization` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_current_waits_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_custom_alert_rule` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs) |
| `get_custom_view` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs) |
| `get_daily_health_report` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDailyReportTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDailyReportTools.cs) |
| `get_daily_summary` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthTools.cs) |
| `get_daily_summary_range` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthTools.cs) |
| `get_database_config` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigTools.cs) |
| `get_database_config_changes` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigHistoryTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigHistoryTools.cs) |
| `get_database_protection` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDatabaseProtectionTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDatabaseProtectionTools.cs) |
| `get_database_scoped_config` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigHistoryTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigHistoryTools.cs) |
| `get_database_sizes` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpObjectStatsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpObjectStatsTools.cs) |
| `get_database_state_expectations` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDatabaseStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDatabaseStateTools.cs) |
| `get_deadlock_detail` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs) |
| `get_deadlock_plan_xml` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs) |
| `get_deadlock_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs) |
| `get_deadlocks` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs) |
| `get_default_trace_events` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDefaultTraceTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDefaultTraceTools.cs) |
| `get_file_io_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_file_io_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs) |
| `get_finops` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFinOpsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFinOpsTools.cs) |
| `get_finops_inventory` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFinOpsInventoryTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFinOpsInventoryTools.cs) |
| `get_finops_recommendations` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFinOpsRecommendationsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFinOpsRecommendationsTools.cs) |
| `get_fleet_overview` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFleetTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFleetTools.cs) |
| `get_forced_plans` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpForcedPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpForcedPlanTools.cs) |
| `get_health_parser_cpu_tasks` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs) |
| `get_health_parser_io_issues` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs) |
| `get_health_parser_memory_broker` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs) |
| `get_health_parser_memory_conditions` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs) |
| `get_health_parser_memory_node_oom` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs) |
| `get_health_parser_scheduler_issues` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs) |
| `get_health_parser_severe_errors` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs) |
| `get_health_parser_significant_waits` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs) |
| `get_health_parser_system_health` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthParserTools.cs) |
| `get_identity_capacity` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpIdentityCapacityTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpIdentityCapacityTools.cs) |
| `get_index_usage` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpObjectStatsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpObjectStatsTools.cs) |
| `get_investigation_evidence` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationTools.cs) |
| `get_investigation_reports` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationTools.cs) |
| `get_job_history` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpJobTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpJobTools.cs) |
| `get_known_patterns` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpKnownPatternTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpKnownPatternTools.cs) |
| `get_latch_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpLatchSpinlockTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpLatchSpinlockTools.cs) |
| `get_load_by_caller` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWhoDroveItTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWhoDroveItTools.cs) |
| `get_lock_wait_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpBlockingTools.cs) |
| `get_long_query_completions` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpLongQueryTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpLongQueryTools.cs) |
| `get_memory_clerks` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_memory_grants` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpMemoryGrantTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpMemoryGrantTools.cs) |
| `get_memory_pressure_events` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpMemoryGrantTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpMemoryGrantTools.cs) |
| `get_memory_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_memory_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs) |
| `get_mute_rules` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs) |
| `get_notification_routes` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs) |
| `get_object_definition` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSchemaTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSchemaTools.cs) |
| `get_object_locking` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpObjectStatsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpObjectStatsTools.cs) |
| `get_object_usage` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpObjectUsageTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpObjectUsageTools.cs) |
| `get_oversized_plan_backlog` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpOversizedPlanBacklogTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpOversizedPlanBacklogTools.cs) |
| `get_people` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPeopleTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPeopleTools.cs) |
| `get_perfmon_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_perfmon_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs) |
| `get_personal_tokens` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPersonalTokenTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPersonalTokenTools.cs) |
| `get_pg_autovacuum_health` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgAutovacuumTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgAutovacuumTools.cs) |
| `get_pg_blocking` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgBlockingTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgBlockingTools.cs) |
| `get_pg_buffer_usage` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgServerStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgServerStateTools.cs) |
| `get_pg_column_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgIndexTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgIndexTools.cs) |
| `get_pg_cpu_utilization` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgCpuUtilizationTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgCpuUtilizationTools.cs) |
| `get_pg_database_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgDatabaseTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgDatabaseTools.cs) |
| `get_pg_database_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgTrendTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgTrendTools.cs) |
| `get_pg_deadlock_detail` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgDeadlockTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgDeadlockTools.cs) |
| `get_pg_deadlocks` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgDeadlockTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgDeadlockTools.cs) |
| `get_pg_extensions` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgServerStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgServerStateTools.cs) |
| `get_pg_index_bloat` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgIndexTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgIndexTools.cs) |
| `get_pg_index_usage` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgIndexUsageTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgIndexUsageTools.cs) |
| `get_pg_io_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgIoTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgIoTools.cs) |
| `get_pg_io_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgTrendTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgTrendTools.cs) |
| `get_pg_kernel_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgKernelStatsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgKernelStatsTools.cs) |
| `get_pg_lock_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgServerStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgServerStateTools.cs) |
| `get_pg_log_events` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgLogEventTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgLogEventTools.cs) |
| `get_pg_logging_audit` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgLoggingAuditTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgLoggingAuditTools.cs) |
| `get_pg_plan_capture_readiness` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgPlanTools.cs) |
| `get_pg_plans` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgPlanTools.cs) |
| `get_pg_predicate_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgPredicateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgPredicateTools.cs) |
| `get_pg_query_duration_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgTrendTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgTrendTools.cs) |
| `get_pg_replication_slots` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgSlotTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgSlotTools.cs) |
| `get_pg_replication_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgReplicationStatsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgReplicationStatsTools.cs) |
| `get_pg_server_config` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgServerStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgServerStateTools.cs) |
| `get_pg_server_config_changes` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgServerStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgServerStateTools.cs) |
| `get_pg_session_states` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgSessionStatesTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgSessionStatesTools.cs) |
| `get_pg_table_bloat` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgTableBloatTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgTableBloatTools.cs) |
| `get_pg_top_queries` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgStatementTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgStatementTools.cs) |
| `get_pg_wait_sampling` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgWaitSamplingTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgWaitSamplingTools.cs) |
| `get_pg_wait_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgWaitTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgWaitTools.cs) |
| `get_pg_wait_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgTrendTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgTrendTools.cs) |
| `get_pg_wraparound_risk` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgWraparoundTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgWraparoundTools.cs) |
| `get_pg_write_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgServerStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgServerStateTools.cs) |
| `get_pg_xmin_horizon` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgXminTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPgXminTools.cs) |
| `get_plan_cache_bloat` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanCacheSchedulerTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanCacheSchedulerTools.cs) |
| `get_plan_corrections` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanCorrectionTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanCorrectionTools.cs) |
| `get_plan_force_precheck` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForcePrecheckTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForcePrecheckTools.cs) |
| `get_plan_xml` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs) |
| `get_procedure_duration_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs) |
| `get_procedure_plan_xml` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs) |
| `get_pvs_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPvsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPvsTools.cs) |
| `get_query_duration_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs) |
| `get_query_fleet_summary` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryFleetSummaryTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryFleetSummaryTools.cs) |
| `get_query_heatmap` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryHeatmapTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryHeatmapTools.cs) |
| `get_query_pileups` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWhoDroveItTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWhoDroveItTools.cs) |
| `get_query_repro_script` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs) |
| `get_query_store_aborts` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryStoreAbortsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryStoreAbortsTools.cs) |
| `get_query_store_clutter` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryStoreClutterTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryStoreClutterTools.cs) |
| `get_query_store_duration_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs) |
| `get_query_store_health` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigHistoryTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigHistoryTools.cs) |
| `get_query_store_hints` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpForcedPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpForcedPlanTools.cs) |
| `get_query_store_plan_xml` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanTools.cs) |
| `get_query_store_query_history` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryStoreHistoryTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryStoreHistoryTools.cs) |
| `get_query_store_regressions` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryStoreRegressionTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryStoreRegressionTools.cs) |
| `get_query_store_top` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_query_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs) |
| `get_read_latency` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpReadLatencyTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpReadLatencyTools.cs) |
| `get_resource_semaphore` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpMemoryGrantTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpMemoryGrantTools.cs) |
| `get_resource_use_by_tag` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagRollupTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagRollupTools.cs) |
| `get_running_jobs` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpJobTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpJobTools.cs) |
| `get_schema_changes` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSchemaTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSchemaTools.cs) |
| `get_server_actions` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerActionTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerActionTools.cs) |
| `get_server_config` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigTools.cs) |
| `get_server_config_changes` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigHistoryTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigHistoryTools.cs) |
| `get_server_properties` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_server_settings` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs) |
| `get_server_summary` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpHealthTools.cs) |
| `get_server_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerTrendTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerTrendTools.cs) |
| `get_service_starts` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServiceStartTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServiceStartTools.cs) |
| `get_session_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSessionTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSessionTools.cs) |
| `get_slow_reads` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSlowReadTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSlowReadTools.cs) |
| `get_spinlock_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpLatchSpinlockTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpLatchSpinlockTools.cs) |
| `get_store_checkpoints` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreCheckpointTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreCheckpointTools.cs) |
| `get_store_compression_health` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreCompressionHealthTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreCompressionHealthTools.cs) |
| `get_store_connections` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreConnectionsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreConnectionsTools.cs) |
| `get_store_host` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreHostTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreHostTools.cs) |
| `get_store_job_runs` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreJobRunsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreJobRunsTools.cs) |
| `get_store_lock_waits` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreLockWaitTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreLockWaitTools.cs) |
| `get_store_log` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreLogTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreLogTools.cs) |
| `get_store_maintenance_readiness` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreMaintenanceReadinessTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreMaintenanceReadinessTools.cs) |
| `get_store_metrics` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreMetricsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreMetricsTools.cs) |
| `get_store_pass_steps` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStorePassStepsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStorePassStepsTools.cs) |
| `get_store_query_history` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreQueryHistoryTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreQueryHistoryTools.cs) |
| `get_store_query_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreQueryStatsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreQueryStatsTools.cs) |
| `get_store_setting_changes` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreSettingChangesTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreSettingChangesTools.cs) |
| `get_store_table_health` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreTableHealthTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreTableHealthTools.cs) |
| `get_sweep_reports` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFleetSweepTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFleetSweepTools.cs) |
| `get_table_index_sizes` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpObjectStatsTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpObjectStatsTools.cs) |
| `get_tempdb_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_tool_guide` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpToolGuideTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpToolGuideTools.cs) |
| `get_top_procedures_by_cpu` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_top_queries_by_cpu` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_trace_flag_changes` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigHistoryTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigHistoryTools.cs) |
| `get_trace_flags` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpConfigTools.cs) |
| `get_wait_stats` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_wait_trend` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_wait_types` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `get_waiting_tasks` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSessionTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSessionTools.cs) |
| `get_web_access_groups` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWebAccessTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWebAccessTools.cs) |
| `kill_session` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForceTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForceTools.cs) |
| `list_alert_sources` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertSourceTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertSourceTools.cs) |
| `list_custom_alert_rules` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs) |
| `list_custom_alert_templates` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs) |
| `list_custom_views` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs) |
| `list_investigation_requests` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationTools.cs) |
| `list_query_tag_rules` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagRuleTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagRuleTools.cs) |
| `list_query_tags` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagTools.cs) |
| `list_servers` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs) |
| `measure_store_table` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreTableMeasureTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpStoreTableMeasureTools.cs) |
| `merge_deadlock_signatures` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSignatureMergeTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpSignatureMergeTools.cs) |
| `mint_personal_token` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPersonalTokenTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPersonalTokenTools.cs) |
| `mute_analysis_finding` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs) |
| `new_password_key` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPasswordKeyTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPasswordKeyTools.cs) |
| `pause_collection` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs) |
| `post_investigation_message` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationTools.Chat.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationTools.Chat.cs) |
| `purge_expired_mute_rules` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs) |
| `purge_now` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpIncidentActionTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpIncidentActionTools.cs) |
| `recompile_module` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForceTools.RecompileModule.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForceTools.RecompileModule.cs) |
| `record_investigation_feedback` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationFeedbackTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationFeedbackTools.cs) |
| `reject_investigation_note` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationAdminTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationAdminTools.cs) |
| `reload_config` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs) |
| `remove_alert_source` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertSourceTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertSourceTools.cs) |
| `remove_server` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerAdminTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerAdminTools.cs) |
| `remove_webhook_signing_key` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.SigningKeys.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.SigningKeys.cs) |
| `reset_collector_schedules` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs) |
| `reset_password_key` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPasswordKeyTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPasswordKeyTools.cs) |
| `restore_alert_history` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.RouteAdmin.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.RouteAdmin.cs) |
| `resume_collection` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs) |
| `revoke_personal_token` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPersonalTokenTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPersonalTokenTools.cs) |
| `rotate_alert_source` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertSourceTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertSourceTools.cs) |
| `rotate_web_access_group_token` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWebAccessTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWebAccessTools.cs) |
| `run_collectors_now` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorRunTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorRunTools.cs) |
| `run_custom_view_panel` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs) |
| `save_environment_profile` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationFeedbackTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationFeedbackTools.cs) |
| `send_daily_health_report_now` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDailyHealthReportTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDailyHealthReportTools.cs) |
| `send_test_notification` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.TestNotification.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.TestNotification.cs) |
| `set_advanced_web_access` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWebAccessTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWebAccessTools.cs) |
| `set_agent_actions_enabled` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAgentChangeAdminTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAgentChangeAdminTools.cs) |
| `set_alert_threshold_override` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpThresholdOverrideTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpThresholdOverrideTools.cs) |
| `set_collector_databases` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs) |
| `set_collector_frequency` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs) |
| `set_collector_retention` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs) |
| `set_collector_run_at` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectorScheduleTools.cs) |
| `set_database_state_expected` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDatabaseStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDatabaseStateTools.cs) |
| `set_mute_rule_enabled` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs) |
| `set_notification_route_enabled` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs) |
| `set_notification_route_fallback` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs) |
| `set_quiet_window` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQuietWindowTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQuietWindowTools.cs) |
| `set_server_action` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerActionTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerActionTools.cs) |
| `set_server_excluded_databases` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs) |
| `set_server_xe_session_databases` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCollectionStateTools.cs) |
| `start_practice_request` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationFeedbackTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationFeedbackTools.cs) |
| `submit_investigation_report` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationTools.cs) |
| `test_connection` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpIncidentActionTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpIncidentActionTools.cs) |
| `test_custom_alert_rule` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs) |
| `test_query_tag_rule` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagRuleTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagRuleTools.cs) |
| `unassign_query_tag` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagTools.cs) |
| `unassign_server_tag` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerTagTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerTagTools.cs) |
| `undo_agent_change` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAgentChangeAdminTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAgentChangeAdminTools.cs) |
| `undo_plan_change` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanUndoTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanUndoTools.cs) |
| `unforce_query_store_plan` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForceTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpPlanForceTools.cs) |
| `unmute_analysis_finding` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTools.cs) |
| `update_alert_settings` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs) |
| `update_custom_alert_rule` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs) |
| `update_custom_view` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs) |
| `update_known_pattern` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpKnownPatternWriteTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpKnownPatternWriteTools.cs) |
| `update_mute_rule` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs) |
| `update_notification_channels` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.NotificationChannels.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.NotificationChannels.cs) |
| `update_notification_route` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.RouteAdmin.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.RouteAdmin.cs) |
| `update_query_tag` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagTools.cs) |
| `update_query_tag_rule` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagRuleTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpQueryTagRuleTools.cs) |
| `update_server_tag` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerTagTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServerTagTools.cs) |
| `update_service_flags` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServiceFlagTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpServiceFlagTools.cs) |
| `update_web_access_group` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWebAccessTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpWebAccessTools.cs) |
| `validate_custom_alert_rule` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomAlertTools.cs) |
| `validate_custom_view` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs) |
| `wait_for_investigation_requests` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationTools.cs) |
| `write_investigation_note` | [`Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationAdminTools.cs`](../Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpInvestigationAdminTools.cs) |

### Lite MCP tools (130)

| Tool | Declared in |
|---|---|
| `add_servers` | [`Lite/Mcp/McpServerRegistryTools.cs`](../Lite/Mcp/McpServerRegistryTools.cs) |
| `analyze_plan_xml` | [`Lite/Mcp/McpPlanTools.cs`](../Lite/Mcp/McpPlanTools.cs) |
| `analyze_procedure_plan` | [`Lite/Mcp/McpPlanTools.cs`](../Lite/Mcp/McpPlanTools.cs) |
| `analyze_query_plan` | [`Lite/Mcp/McpPlanTools.cs`](../Lite/Mcp/McpPlanTools.cs) |
| `analyze_query_store_plan` | [`Lite/Mcp/McpPlanTools.cs`](../Lite/Mcp/McpPlanTools.cs) |
| `analyze_server` | [`Lite/Mcp/McpAnalysisTools.cs`](../Lite/Mcp/McpAnalysisTools.cs) |
| `assign_query_tag` | [`Lite/Mcp/McpQueryTagTools.cs`](../Lite/Mcp/McpQueryTagTools.cs) |
| `audit_config` | [`Lite/Mcp/McpAnalysisTools.cs`](../Lite/Mcp/McpAnalysisTools.cs) |
| `claim_investigation_request` | [`Lite/Mcp/McpInvestigationTools.cs`](../Lite/Mcp/McpInvestigationTools.cs) |
| `compare_analysis` | [`Lite/Mcp/McpAnalysisTools.cs`](../Lite/Mcp/McpAnalysisTools.cs) |
| `create_mute_rule` | [`Lite/Mcp/McpAlertTools.MuteRules.cs`](../Lite/Mcp/McpAlertTools.MuteRules.cs) |
| `create_notification_route` | [`Lite/Mcp/McpAlertTools.RouteAdmin.cs`](../Lite/Mcp/McpAlertTools.RouteAdmin.cs) |
| `create_query_tag` | [`Lite/Mcp/McpQueryTagTools.cs`](../Lite/Mcp/McpQueryTagTools.cs) |
| `create_query_tag_rule` | [`Lite/Mcp/McpQueryTagTools.cs`](../Lite/Mcp/McpQueryTagTools.cs) |
| `delete_mute_rule` | [`Lite/Mcp/McpAlertTools.MuteRules.cs`](../Lite/Mcp/McpAlertTools.MuteRules.cs) |
| `delete_notification_route` | [`Lite/Mcp/McpAlertTools.cs`](../Lite/Mcp/McpAlertTools.cs) |
| `delete_query_tag` | [`Lite/Mcp/McpQueryTagTools.cs`](../Lite/Mcp/McpQueryTagTools.cs) |
| `delete_query_tag_rule` | [`Lite/Mcp/McpQueryTagTools.cs`](../Lite/Mcp/McpQueryTagTools.cs) |
| `edit_server` | [`Lite/Mcp/McpServerAdminTools.cs`](../Lite/Mcp/McpServerAdminTools.cs) |
| `find_database` | [`Lite/Mcp/McpDiscoveryTools.cs`](../Lite/Mcp/McpDiscoveryTools.cs) |
| `find_tools` | [`Lite/Mcp/McpToolSearchTools.cs`](../Lite/Mcp/McpToolSearchTools.cs) |
| `get_active_queries` | [`Lite/Mcp/McpSessionTools.cs`](../Lite/Mcp/McpSessionTools.cs) |
| `get_alert_history` | [`Lite/Mcp/McpAlertTools.cs`](../Lite/Mcp/McpAlertTools.cs) |
| `get_alert_settings` | [`Lite/Mcp/McpAlertTools.cs`](../Lite/Mcp/McpAlertTools.cs) |
| `get_analysis_facts` | [`Lite/Mcp/McpAnalysisTools.cs`](../Lite/Mcp/McpAnalysisTools.cs) |
| `get_analysis_findings` | [`Lite/Mcp/McpAnalysisTools.cs`](../Lite/Mcp/McpAnalysisTools.cs) |
| `get_blocked_process_reports` | [`Lite/Mcp/McpBlockingTools.cs`](../Lite/Mcp/McpBlockingTools.cs) |
| `get_blocked_process_xml` | [`Lite/Mcp/McpBlockingTools.cs`](../Lite/Mcp/McpBlockingTools.cs) |
| `get_blocking_stats` | [`Lite/Mcp/McpHealthTools.cs`](../Lite/Mcp/McpHealthTools.cs) |
| `get_blocking_trend` | [`Lite/Mcp/McpBlockingTools.cs`](../Lite/Mcp/McpBlockingTools.cs) |
| `get_client_timeouts` | [`Lite/Mcp/McpClientTimeoutTools.cs`](../Lite/Mcp/McpClientTimeoutTools.cs) |
| `get_collection_health` | [`Lite/Mcp/McpHealthTools.cs`](../Lite/Mcp/McpHealthTools.cs) |
| `get_collection_log` | [`Lite/Mcp/McpHealthTools.cs`](../Lite/Mcp/McpHealthTools.cs) |
| `get_collector_schedules` | [`Lite/Mcp/McpCollectorScheduleTools.cs`](../Lite/Mcp/McpCollectorScheduleTools.cs) |
| `get_cpu_by_database` | [`Lite/Mcp/McpWhoDroveItTools.cs`](../Lite/Mcp/McpWhoDroveItTools.cs) |
| `get_cpu_scheduler_pressure` | [`Lite/Mcp/McpPlanCacheSchedulerTools.cs`](../Lite/Mcp/McpPlanCacheSchedulerTools.cs) |
| `get_cpu_utilization` | [`Lite/Mcp/McpCpuTools.cs`](../Lite/Mcp/McpCpuTools.cs) |
| `get_current_waits_trend` | [`Lite/Mcp/McpHealthTools.cs`](../Lite/Mcp/McpHealthTools.cs) |
| `get_daily_summary` | [`Lite/Mcp/McpHealthTools.cs`](../Lite/Mcp/McpHealthTools.cs) |
| `get_daily_summary_range` | [`Lite/Mcp/McpHealthTools.cs`](../Lite/Mcp/McpHealthTools.cs) |
| `get_database_config` | [`Lite/Mcp/McpConfigTools.cs`](../Lite/Mcp/McpConfigTools.cs) |
| `get_database_config_changes` | [`Lite/Mcp/McpConfigHistoryTools.cs`](../Lite/Mcp/McpConfigHistoryTools.cs) |
| `get_database_scoped_config` | [`Lite/Mcp/McpConfigTools.cs`](../Lite/Mcp/McpConfigTools.cs) |
| `get_database_sizes` | [`Lite/Mcp/McpServerInfoTools.cs`](../Lite/Mcp/McpServerInfoTools.cs) |
| `get_deadlock_detail` | [`Lite/Mcp/McpBlockingTools.cs`](../Lite/Mcp/McpBlockingTools.cs) |
| `get_deadlock_trend` | [`Lite/Mcp/McpBlockingTools.cs`](../Lite/Mcp/McpBlockingTools.cs) |
| `get_deadlocks` | [`Lite/Mcp/McpBlockingTools.cs`](../Lite/Mcp/McpBlockingTools.cs) |
| `get_default_trace_events` | [`Lite/Mcp/McpDefaultTraceTools.cs`](../Lite/Mcp/McpDefaultTraceTools.cs) |
| `get_file_io_stats` | [`Lite/Mcp/McpIoTools.cs`](../Lite/Mcp/McpIoTools.cs) |
| `get_file_io_trend` | [`Lite/Mcp/McpIoTools.cs`](../Lite/Mcp/McpIoTools.cs) |
| `get_health_parser_cpu_tasks` | [`Lite/Mcp/McpHealthParserTools.cs`](../Lite/Mcp/McpHealthParserTools.cs) |
| `get_health_parser_io_issues` | [`Lite/Mcp/McpHealthParserTools.cs`](../Lite/Mcp/McpHealthParserTools.cs) |
| `get_health_parser_memory_broker` | [`Lite/Mcp/McpHealthParserTools.cs`](../Lite/Mcp/McpHealthParserTools.cs) |
| `get_health_parser_memory_conditions` | [`Lite/Mcp/McpHealthParserTools.cs`](../Lite/Mcp/McpHealthParserTools.cs) |
| `get_health_parser_memory_node_oom` | [`Lite/Mcp/McpHealthParserTools.cs`](../Lite/Mcp/McpHealthParserTools.cs) |
| `get_health_parser_scheduler_issues` | [`Lite/Mcp/McpHealthParserTools.cs`](../Lite/Mcp/McpHealthParserTools.cs) |
| `get_health_parser_severe_errors` | [`Lite/Mcp/McpHealthParserTools.cs`](../Lite/Mcp/McpHealthParserTools.cs) |
| `get_health_parser_significant_waits` | [`Lite/Mcp/McpHealthParserTools.cs`](../Lite/Mcp/McpHealthParserTools.cs) |
| `get_health_parser_system_health` | [`Lite/Mcp/McpHealthParserTools.cs`](../Lite/Mcp/McpHealthParserTools.cs) |
| `get_index_usage` | [`Lite/Mcp/McpObjectStatsTools.cs`](../Lite/Mcp/McpObjectStatsTools.cs) |
| `get_investigation_reports` | [`Lite/Mcp/McpInvestigationTools.cs`](../Lite/Mcp/McpInvestigationTools.cs) |
| `get_latch_stats` | [`Lite/Mcp/McpLatchSpinlockTools.cs`](../Lite/Mcp/McpLatchSpinlockTools.cs) |
| `get_load_by_caller` | [`Lite/Mcp/McpWhoDroveItTools.cs`](../Lite/Mcp/McpWhoDroveItTools.cs) |
| `get_lock_wait_trend` | [`Lite/Mcp/McpBlockingTools.cs`](../Lite/Mcp/McpBlockingTools.cs) |
| `get_long_query_completions` | [`Lite/Mcp/McpLongQueryTools.cs`](../Lite/Mcp/McpLongQueryTools.cs) |
| `get_memory_clerks` | [`Lite/Mcp/McpMemoryTools.cs`](../Lite/Mcp/McpMemoryTools.cs) |
| `get_memory_grants` | [`Lite/Mcp/McpMemoryTools.cs`](../Lite/Mcp/McpMemoryTools.cs) |
| `get_memory_pressure_events` | [`Lite/Mcp/McpMemoryTools.cs`](../Lite/Mcp/McpMemoryTools.cs) |
| `get_memory_stats` | [`Lite/Mcp/McpMemoryTools.cs`](../Lite/Mcp/McpMemoryTools.cs) |
| `get_memory_trend` | [`Lite/Mcp/McpMemoryTools.cs`](../Lite/Mcp/McpMemoryTools.cs) |
| `get_mute_rules` | [`Lite/Mcp/McpAlertTools.cs`](../Lite/Mcp/McpAlertTools.cs) |
| `get_notification_routes` | [`Lite/Mcp/McpAlertTools.cs`](../Lite/Mcp/McpAlertTools.cs) |
| `get_object_locking` | [`Lite/Mcp/McpObjectStatsTools.cs`](../Lite/Mcp/McpObjectStatsTools.cs) |
| `get_object_usage` | [`Lite/Mcp/McpObjectUsageTools.cs`](../Lite/Mcp/McpObjectUsageTools.cs) |
| `get_perfmon_stats` | [`Lite/Mcp/McpPerfmonTools.cs`](../Lite/Mcp/McpPerfmonTools.cs) |
| `get_perfmon_trend` | [`Lite/Mcp/McpPerfmonTools.cs`](../Lite/Mcp/McpPerfmonTools.cs) |
| `get_plan_cache_bloat` | [`Lite/Mcp/McpPlanCacheSchedulerTools.cs`](../Lite/Mcp/McpPlanCacheSchedulerTools.cs) |
| `get_plan_corrections` | [`Lite/Mcp/McpPlanCorrectionTools.cs`](../Lite/Mcp/McpPlanCorrectionTools.cs) |
| `get_plan_xml` | [`Lite/Mcp/McpPlanTools.cs`](../Lite/Mcp/McpPlanTools.cs) |
| `get_procedure_duration_trend` | [`Lite/Mcp/McpQueryTools.cs`](../Lite/Mcp/McpQueryTools.cs) |
| `get_pvs_stats` | [`Lite/Mcp/McpPvsTools.cs`](../Lite/Mcp/McpPvsTools.cs) |
| `get_query_duration_trend` | [`Lite/Mcp/McpQueryTools.cs`](../Lite/Mcp/McpQueryTools.cs) |
| `get_query_heatmap` | [`Lite/Mcp/McpQueryTools.cs`](../Lite/Mcp/McpQueryTools.cs) |
| `get_query_pileups` | [`Lite/Mcp/McpWhoDroveItTools.cs`](../Lite/Mcp/McpWhoDroveItTools.cs) |
| `get_query_store_duration_trend` | [`Lite/Mcp/McpQueryTools.cs`](../Lite/Mcp/McpQueryTools.cs) |
| `get_query_store_health` | [`Lite/Mcp/McpConfigTools.cs`](../Lite/Mcp/McpConfigTools.cs) |
| `get_query_store_regressions` | [`Lite/Mcp/McpQueryTools.cs`](../Lite/Mcp/McpQueryTools.cs) |
| `get_query_store_top` | [`Lite/Mcp/McpQueryTools.cs`](../Lite/Mcp/McpQueryTools.cs) |
| `get_query_trend` | [`Lite/Mcp/McpQueryTools.cs`](../Lite/Mcp/McpQueryTools.cs) |
| `get_resource_semaphore` | [`Lite/Mcp/McpMemoryTools.cs`](../Lite/Mcp/McpMemoryTools.cs) |
| `get_resource_use_by_tag` | [`Lite/Mcp/McpQueryTagUsageTools.cs`](../Lite/Mcp/McpQueryTagUsageTools.cs) |
| `get_running_jobs` | [`Lite/Mcp/McpJobTools.cs`](../Lite/Mcp/McpJobTools.cs) |
| `get_server_config` | [`Lite/Mcp/McpConfigTools.cs`](../Lite/Mcp/McpConfigTools.cs) |
| `get_server_config_changes` | [`Lite/Mcp/McpConfigHistoryTools.cs`](../Lite/Mcp/McpConfigHistoryTools.cs) |
| `get_server_properties` | [`Lite/Mcp/McpServerInfoTools.cs`](../Lite/Mcp/McpServerInfoTools.cs) |
| `get_server_summary` | [`Lite/Mcp/McpHealthTools.cs`](../Lite/Mcp/McpHealthTools.cs) |
| `get_session_stats` | [`Lite/Mcp/McpSessionTools.cs`](../Lite/Mcp/McpSessionTools.cs) |
| `get_slow_mcp_calls` | [`Lite/Mcp/McpSlowCallTools.cs`](../Lite/Mcp/McpSlowCallTools.cs) |
| `get_spinlock_stats` | [`Lite/Mcp/McpLatchSpinlockTools.cs`](../Lite/Mcp/McpLatchSpinlockTools.cs) |
| `get_store_health` | [`Lite/Mcp/McpStoreHealthTools.cs`](../Lite/Mcp/McpStoreHealthTools.cs) |
| `get_table_index_sizes` | [`Lite/Mcp/McpObjectStatsTools.cs`](../Lite/Mcp/McpObjectStatsTools.cs) |
| `get_tempdb_trend` | [`Lite/Mcp/McpTempDbTools.cs`](../Lite/Mcp/McpTempDbTools.cs) |
| `get_tool_guide` | [`Lite/Mcp/McpToolGuideTools.cs`](../Lite/Mcp/McpToolGuideTools.cs) |
| `get_top_procedures_by_cpu` | [`Lite/Mcp/McpQueryTools.cs`](../Lite/Mcp/McpQueryTools.cs) |
| `get_top_queries_by_cpu` | [`Lite/Mcp/McpQueryTools.cs`](../Lite/Mcp/McpQueryTools.cs) |
| `get_trace_flag_changes` | [`Lite/Mcp/McpConfigHistoryTools.cs`](../Lite/Mcp/McpConfigHistoryTools.cs) |
| `get_trace_flags` | [`Lite/Mcp/McpConfigTools.cs`](../Lite/Mcp/McpConfigTools.cs) |
| `get_wait_stats` | [`Lite/Mcp/McpWaitTools.cs`](../Lite/Mcp/McpWaitTools.cs) |
| `get_wait_trend` | [`Lite/Mcp/McpWaitTools.cs`](../Lite/Mcp/McpWaitTools.cs) |
| `get_wait_types` | [`Lite/Mcp/McpWaitTools.cs`](../Lite/Mcp/McpWaitTools.cs) |
| `get_waiting_tasks` | [`Lite/Mcp/McpWaitTools.cs`](../Lite/Mcp/McpWaitTools.cs) |
| `list_investigation_requests` | [`Lite/Mcp/McpInvestigationTools.cs`](../Lite/Mcp/McpInvestigationTools.cs) |
| `list_query_tag_rules` | [`Lite/Mcp/McpQueryTagTools.cs`](../Lite/Mcp/McpQueryTagTools.cs) |
| `list_query_tags` | [`Lite/Mcp/McpQueryTagTools.cs`](../Lite/Mcp/McpQueryTagTools.cs) |
| `list_servers` | [`Lite/Mcp/McpDiscoveryTools.cs`](../Lite/Mcp/McpDiscoveryTools.cs) |
| `mute_analysis_finding` | [`Lite/Mcp/McpAnalysisTools.cs`](../Lite/Mcp/McpAnalysisTools.cs) |
| `remove_server` | [`Lite/Mcp/McpServerRegistryTools.cs`](../Lite/Mcp/McpServerRegistryTools.cs) |
| `reset_collector_schedules` | [`Lite/Mcp/McpCollectorScheduleTools.cs`](../Lite/Mcp/McpCollectorScheduleTools.cs) |
| `set_mute_rule_enabled` | [`Lite/Mcp/McpAlertTools.MuteRules.cs`](../Lite/Mcp/McpAlertTools.MuteRules.cs) |
| `set_notification_route_enabled` | [`Lite/Mcp/McpAlertTools.cs`](../Lite/Mcp/McpAlertTools.cs) |
| `set_server_excluded_databases` | [`Lite/Mcp/McpServerAdminTools.cs`](../Lite/Mcp/McpServerAdminTools.cs) |
| `submit_investigation_report` | [`Lite/Mcp/McpInvestigationTools.cs`](../Lite/Mcp/McpInvestigationTools.cs) |
| `test_query_tag_rule` | [`Lite/Mcp/McpQueryTagTools.cs`](../Lite/Mcp/McpQueryTagTools.cs) |
| `unassign_query_tag` | [`Lite/Mcp/McpQueryTagTools.cs`](../Lite/Mcp/McpQueryTagTools.cs) |
| `unmute_analysis_finding` | [`Lite/Mcp/McpAnalysisTools.cs`](../Lite/Mcp/McpAnalysisTools.cs) |
| `update_alert_settings` | [`Lite/Mcp/McpAlertSettingsWriteTools.cs`](../Lite/Mcp/McpAlertSettingsWriteTools.cs) |
| `update_mute_rule` | [`Lite/Mcp/McpAlertTools.MuteRules.cs`](../Lite/Mcp/McpAlertTools.MuteRules.cs) |
| `update_notification_route` | [`Lite/Mcp/McpAlertTools.RouteAdmin.cs`](../Lite/Mcp/McpAlertTools.RouteAdmin.cs) |
| `update_query_tag` | [`Lite/Mcp/McpQueryTagTools.cs`](../Lite/Mcp/McpQueryTagTools.cs) |
| `update_query_tag_rule` | [`Lite/Mcp/McpQueryTagTools.cs`](../Lite/Mcp/McpQueryTagTools.cs) |

## Tests

A test class is a file whose name ends in `Tests.cs`.

| Test project | Test classes |
|---|---|
| [`Darling/Darling.Tests.Standin/Darling.Tests.Standin.csproj`](../Darling/Darling.Tests.Standin/Darling.Tests.Standin.csproj) | 0 |
| [`Darling/Darling.Tests/Darling.Tests.csproj`](../Darling/Darling.Tests/Darling.Tests.csproj) | 3675 |
| [`Exporter/PerformanceMonitor.Exporter.TestChild/PerformanceMonitor.Exporter.TestChild.csproj`](../Exporter/PerformanceMonitor.Exporter.TestChild/PerformanceMonitor.Exporter.TestChild.csproj) | 0 |
| [`Exporter/PerformanceMonitor.Exporter.Tests/PerformanceMonitor.Exporter.Tests.csproj`](../Exporter/PerformanceMonitor.Exporter.Tests/PerformanceMonitor.Exporter.Tests.csproj) | 75 |
| [`Lite.Tests/Lite.Tests.csproj`](../Lite.Tests/Lite.Tests.csproj) | 1248 |

## CI

| Workflow | Name | Runs on |
|---|---|---|
| [`.github/workflows/build.yml`](../.github/workflows/build.yml) | Build | `release` |
| [`.github/workflows/check-pr-branch.yml`](../.github/workflows/check-pr-branch.yml) | Check pull request target branch | `pull_request` |
| [`.github/workflows/check-version-bump.yml`](../.github/workflows/check-version-bump.yml) | Check version bump | `pull_request` |
| [`.github/workflows/claude-review.yml`](../.github/workflows/claude-review.yml) | Claude Auto Review | `pull_request` |
| [`.github/workflows/darling-helm-release.yml`](../.github/workflows/darling-helm-release.yml) | Darling Helm release | `workflow_dispatch`, `workflow_run` |
| [`.github/workflows/darling-helm.yml`](../.github/workflows/darling-helm.yml) | Darling Helm chart | `pull_request`, `workflow_call`, `workflow_dispatch` |
| [`.github/workflows/darling-linux-store.yml`](../.github/workflows/darling-linux-store.yml) | Darling Linux store | `pull_request`, `workflow_call`, `workflow_dispatch` |
| [`.github/workflows/darling-linux-tests.yml`](../.github/workflows/darling-linux-tests.yml) | Darling Linux tests | `pull_request`, `workflow_call`, `workflow_dispatch` |
| [`.github/workflows/darling-msi.yml`](../.github/workflows/darling-msi.yml) | Darling MSI | `pull_request`, `workflow_call`, `workflow_dispatch` |
| [`.github/workflows/darling-ui-smoke.yml`](../.github/workflows/darling-ui-smoke.yml) | Darling UI smoke | `workflow_call`, `workflow_dispatch` |
| [`.github/workflows/darling-upgrade-shapes.yml`](../.github/workflows/darling-upgrade-shapes.yml) | Darling upgrade shapes | `pull_request`, `workflow_call`, `workflow_dispatch` |
| [`.github/workflows/description-drift.yml`](../.github/workflows/description-drift.yml) | Description drift | `pull_request` |
| [`.github/workflows/fork-ci-result.yml`](../.github/workflows/fork-ci-result.yml) | PR CI fork result | `workflow_dispatch`, `workflow_run` |
| [`.github/workflows/nightly.yml`](../.github/workflows/nightly.yml) | Nightly Build | `schedule`, `workflow_dispatch` |
| [`.github/workflows/pr.yml`](../.github/workflows/pr.yml) | PR CI | `pull_request`, `push`, `schedule`, `workflow_dispatch` |
| [`.github/workflows/probe-vpk-signing.yml`](../.github/workflows/probe-vpk-signing.yml) | Probe vpk signing invocations | `pull_request`, `workflow_dispatch` |
| [`.github/workflows/release-signatures.yml`](../.github/workflows/release-signatures.yml) | Release signatures | `pull_request`, `workflow_dispatch` |
| [`.github/workflows/runtime-currency.yml`](../.github/workflows/runtime-currency.yml) | Runtime currency | `schedule`, `workflow_dispatch` |
| [`.github/workflows/sql-validation.yml`](../.github/workflows/sql-validation.yml) | SQL Validation | `pull_request`, `push` |
| [`.github/workflows/wpf-smoke.yml`](../.github/workflows/wpf-smoke.yml) | WPF smoke | `workflow_call`, `workflow_dispatch` |

---

Generated from commit `ae7176e58`. To refresh it, run `python tools/codemap/codemap.py`.
