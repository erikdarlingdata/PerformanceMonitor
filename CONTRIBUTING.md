# Contributing to SQL Server Performance Monitor

Thank you for your interest in contributing to the SQL Server Performance Monitor! This guide will help you understand the project structure, set up your development environment, and submit high-quality contributions.

## Table of Contents

1. [Project Overview](#project-overview)
2. [Development Setup](#development-setup)
3. [Architecture Overview](#architecture-overview)
4. [Contribution Paths](#contribution-paths)
5. [Code Style Guidelines](#code-style-guidelines)
6. [Pull Request Process](#pull-request-process)
7. [Testing Guidelines](#testing-guidelines)

---

## Project Overview

This repository contains three editions of the SQL Server Performance Monitor:

**Full Edition** — server-installed collectors with a separate dashboard:

| Folder | Description |
|--------|-------------|
| `install/` | 50+ T-SQL scripts that create the PerformanceMonitor database |
| `Installer/` | CLI installer for the Full Edition database and collectors |
| `Installer.Core/` | Shared installation library (used by CLI installer and Dashboard) |
| `Dashboard/` | WPF dashboard — connects to PerformanceMonitor database, can also install/upgrade via Add Server |

**Lite Edition** — standalone desktop app, nothing installed on the target server:

| Folder | Description |
|--------|-------------|
| `Lite/` | Standalone WPF app with embedded DuckDB, collects directly from DMVs over the network |

**Darling Edition** — headless collector service with a central PostgreSQL store:

| Folder | Description |
|--------|-------------|
| `Darling/PerformanceMonitor.Darling.Service/` | Windows service — collector runner, alert engine host, scheduled analysis, MCP and web dashboard endpoints |
| `Darling/PerformanceMonitor.Darling.Storage/` | The PostgreSQL store — the migration ladder (`PgMigrations.cs`), the schema generator, writers and readers |
| `Darling/PerformanceMonitor.Darling.Analysis/` | Binds the shared analysis pipeline to the store |
| `Darling/PerformanceMonitor.Darling.Viewer/` | WPF viewer — reads the store directly, any number of seats |

`Darling/README.md` is the operator-facing document (configuration, installation, what the service does on
monitored servers). This file covers the contributor-facing conventions.

Lite and Darling share their libraries, all targeting `net10.0`: `PerformanceMonitor.Collectors` (every
collector definition — the query sent to the monitored server, the row mappings, the delta rules, the
cadences), `PerformanceMonitor.Alerting` (the alert engine), plus `.Analysis`, `.Notifications`,
`.PlanAnalysis`, `.Common` and `.Ui`. Only the storage layer differs: Lite writes DuckDB, Darling writes
PostgreSQL.

---

## Development Setup

### Prerequisites

- **Windows 10/11** (required for WPF)
- **.NET 10.0 SDK** ([download](https://dotnet.microsoft.com/download/dotnet/10.0))
- **Visual Studio 2022** or **VS Code** with C# extension
- **SQL Server** (2016 or later) for testing
- **Git** for version control

### Building from Source

```cmd
# Clone the repository
git clone https://github.com/erikdarlingdata/PerformanceMonitor.git
cd PerformanceMonitor

# Build Full Dashboard
dotnet build Dashboard/Dashboard.csproj

# Build Lite Edition
dotnet build Lite/PerformanceMonitorLite.csproj

# Build CLI Installer (self-contained)
dotnet publish Installer/PerformanceMonitorInstaller.csproj -c Release

# Build Darling (headless service, then the viewer)
dotnet build Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj
dotnet build Darling/PerformanceMonitor.Darling.Viewer/PerformanceMonitor.Darling.Viewer.csproj

```

### Running the Applications

**Full Dashboard:**
1. Install the database on a SQL Server instance using the installer
2. Run `Dashboard/bin/Debug/net10.0-windows/Dashboard.exe`
3. Add your server connection and start monitoring

**Lite Edition:**
1. Run `Lite/bin/Debug/net10.0-windows/PerformanceMonitorLite.exe`
2. Add a SQL Server connection (requires VIEW SERVER STATE permission)
3. Data collection begins automatically

**Darling:**
1. Provision a PostgreSQL store — or let the service run its own bundled one — and write `darling.json`
2. Install and start the service; it applies any pending schema migrations on startup
3. Point the viewer at the same store. `Darling/README.md` has the full procedure

---

## Architecture Overview

### Full Dashboard Architecture

The Full Dashboard has a clean separation between data collection and display:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Monitored SQL Server                         │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  PerformanceMonitor Database                             │   │
│  │  ├── collect.* tables (raw collected data)               │   │
│  │  ├── dbo.collect_* procedures (29 collectors)            │   │
│  │  ├── report.* views (reporting layer)                    │   │
│  │  └── config.* tables (schedules, retention, logs)        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           ▲                                     │
│                           │ SQL Agent Jobs (every 1 min)        │
└───────────────────────────┼─────────────────────────────────────┘
                            │
                            │ SQL queries
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Dashboard Application                        │
│  ├── Services/DatabaseService.cs (data access layer)           │
│  ├── Models/*.cs (data transfer objects)                       │
│  ├── ServerTab.xaml (main monitoring UI)                       │
│  └── Mcp/*.cs (MCP server for LLM integration)                 │
└─────────────────────────────────────────────────────────────────┘
```

**Key insight:** T-SQL collectors run independently from the C# Dashboard. You can modify collectors without touching C# code, and vice versa.

### Lite Edition Architecture

**Warning:** Lite has significantly more architectural complexity. A single new collector touches multiple layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Remote SQL Server                            │
│  (No installation required - just VIEW SERVER STATE)            │
└───────────────────────────────────────────────────────────────┬─┘
                            │ DMV queries                       │
                            ▼                                   │
┌─────────────────────────────────────────────────────────────────┐
│                    Lite Application                             │
│                                                                 │
│  1. RemoteCollectorService.*.cs                                │
│     └── Queries SQL Server DMVs, stores in DuckDB              │
│                                                                 │
│  2. Database/Schema.cs                                         │
│     └── DuckDB table definitions (must match collector output) │
│                                                                 │
│  3. LocalDataService.*.cs                                      │
│     └── Reads from DuckDB for UI and MCP                       │
│                                                                 │
│  4. Controls/ServerTab.xaml.cs                                 │
│     └── UI display, charts, data grids                         │
│                                                                 │
│  5. Services/ArchiveService.cs + RetentionService.cs           │
│     └── Parquet archival and data cleanup                      │
│                                                                 │
│  6. ScheduleManager.cs                                         │
│     └── Controls when collectors run                           │
│                                                                 │
│  7. Mcp/*.cs                                                   │
│     └── Exposes data to LLM clients                            │
└─────────────────────────────────────────────────────────────────┘
```

**There is no clean single-layer contribution path for Lite collectors.** Adding a new collector requires changes across 5-7 files with careful coordination.

### Darling Edition Architecture

Darling puts a process boundary between collection and display: a headless Windows service collects 24/7
into a central PostgreSQL store, and any number of viewer seats read that store.

```
┌─────────────────────────────────────────────────────────────────┐
│              Monitored SQL Server or PostgreSQL                 │
│  (No install beyond two Extended Events sessions on SQL Server) │
└─────────────────────────────────────────────────────────────────┘
                            │ DMV / catalog queries
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│      PerformanceMonitor.Darling.Service (Windows service)       │
│                                                                 │
│  1. DarlingCollectorRunner.cs                                   │
│     └── Runs the shared collector definitions, 24/7             │
│                                                                 │
│  2. PerformanceMonitor.Alerting + .Notifications                │
│     └── Shared alert engine, cooldowns, delivery                │
│                                                                 │
│  3. PerformanceMonitor.Darling.Analysis                         │
│     └── Scheduled analysis / recommendations pass               │
│                                                                 │
│  4. Mcp/*.cs + DarlingWebEndpoints.cs                           │
│     └── MCP server and web dashboard                            │
└─────────────────────────────────────────────────────────────────┘
                            │ binary COPY / SQL (Npgsql)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│     PostgreSQL store (TimescaleDB optional, auto-detected)      │
│  ├── collect.* tables (raw collected data)                      │
│  ├── config.* tables (control plane, schedules, alert settings) │
│  └── darling_schema_version (what the migration ladder stamps)  │
└─────────────────────────────────────────────────────────────────┘
                            │ SQL queries
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│     PerformanceMonitor.Darling.Viewer (WPF, one per seat)       │
│  └── ViewerDataService.*.cs reads the store directly            │
└─────────────────────────────────────────────────────────────────┘
```

**Key insight:** the viewer talks to the store, not to the service, so both are versioned against the
schema rather than against each other. `ViewerDataService.RequiredStoreSchemaVersion` is
`StorageVersion.SchemaVersion`, and the viewer refuses at connect time to open a store below it
(`MainWindow.xaml.cs`) instead of failing later on a column that is not there yet.

**The schema is a ladder, not a schema file.** `PgMigrations.Scripts` is an ordered list of
`Migration(version, name, sql)`. On startup the service reads `MAX(version)` from `darling_schema_version`
and applies every rung above it, each in its own transaction, stamping the table as it goes. V1 is
generated from the collector definitions (`PgSchemaGenerator.GenerateFullSchema()`); later rungs are
appended, never edited, because a store that already ran a rung will never read it again. TimescaleDB
conversion is deliberately *not* on the ladder — it is runtime setup (`TimescaleSupport`) applied only when
the extension is present, so the same store works on plain PostgreSQL.

---

## Contribution Paths

### Where Contributions Are Welcome

| Area | Complexity | Notes |
|------|------------|-------|
| **Full Dashboard T-SQL collectors** | Low | Clean, self-contained stored procedures |
| **Full Dashboard reporting views** | Low | SQL views in `install/47_create_reporting_views.sql` |
| **Dashboard UI improvements** | Medium | WPF/C# in `Dashboard/ServerTab.xaml*` |
| **Lite UI improvements** | Medium | WPF/C# in `Lite/Controls/ServerTab.xaml*` |
| **Bug fixes (either edition)** | Varies | Always welcome |
| **Documentation** | Low | README, comments, troubleshooting guides |
| **MCP tool improvements** | Medium | `Dashboard/Mcp/` or `Lite/Mcp/` |
| **New Lite collectors** | **High** | See warning below |
| **Darling schema migrations** | **High** | Four coordinated edits — see below |

### Adding a New Full Dashboard Collector

This is the cleanest contribution path. You need to:

1. **Create the collection table** in `install/02_create_tables.sql`
2. **Create the collector procedure** in a new file `install/XX_collect_your_collector.sql`
3. **Register in the schedule** (handled automatically by `install/41_schedule_management.sql`)
4. **Optionally** add reporting views in `install/47_create_reporting_views.sql`

See the existing collectors as templates. Each collector is self-contained.

### Adding a New Lite Collector (Advanced)

**Be aware:** This is a significant undertaking. You must modify:

1. `Lite/Database/Schema.cs` - Add DuckDB table definition
2. `Lite/Services/RemoteCollectorService.{YourCollector}.cs` - SQL query + DuckDB insert logic
3. `Lite/Services/LocalDataService.{YourCollector}.cs` - Query methods for UI/MCP
4. `Lite/Controls/ServerTab.xaml` and `.cs` - UI elements, charts, grids
5. `Lite/Models/` - Any new data transfer objects
6. `Lite/config/collection_schedule.json` - Default schedule entry
7. `Lite/Mcp/Mcp{Category}Tools.cs` - MCP tool exposure (if applicable)

All of these must be coordinated. The schema must match what the collector inserts, the LocalDataService must query what the schema defines, the UI must display what LocalDataService returns, etc.

If you want to contribute a new Lite collector, please **open an issue first** to discuss the approach.

### Adding a Darling Migration Rung

Every Darling schema change is a numbered rung on the ladder, and it takes four coordinated edits. CI is red
until all four are there.

1. **The rung.** Add `new Migration(N, "kebab-name", VNSql)` at the end of `PgMigrations.Scripts`, with a
   matching `private const string VNSql`. Schema-qualify every object in it (`collect.`, `config.`) — the
   migrate session runs under `search_path = collect, config, public`, so a bare `CREATE TABLE` silently
   lands in `collect` with `collect`'s ACL whether or not that is where you meant it.
2. **The version.** Set `StorageVersion.SchemaVersion = N`.
3. **The pins.** These are symbolic: the tests assert against `StorageVersion.SchemaVersion`, not against a
   literal, so they follow the bump rather than needing one (`ScaffoldTests` pins it to
   `PgMigrations.Scripts[^1].Version`, `MigrationLadderPins` pins the ladder's shape, and each rung's own
   store test pins the ladder max). The literals a rung's own test does carry are its **own** ordinals,
   which never move: its version number, its probe ordinal, and the version its sentinel maps to. Write
   those the way the newest existing rung's test writes them, and leave earlier rungs' literals alone — a
   demoted rung whose test still asserts it is the newest is how the next rung's build goes red.
4. **The viewer probe.** `ViewerDataService.StoreSchemaProbeSql` reads `information_schema` (plus
   `pg_indexes` / `pg_extension` where a sentinel is not a table or column) for one sentinel per rung, and
   `MapProbedSchemaVersion` maps them newest-first. A rung needs all four parts: a sentinel line in the
   probe SQL, one more `reader.GetBoolean(<next ordinal>)` in `GetStoreSchemaVersionAsync`, one more
   trailing `bool hasThing = false` parameter, and an arm `if (hasThing) return N;` placed **above** the
   previous one. Miss it and a fully-migrated store probes one rung short, so the connect-time gate refuses
   a store that is in fact current — permanently, because no later upgrade changes the answer.

#### Rung numbering: `max(dev) + 1`, never pre-reserved

The applier ascends and skips anything already stamped
(`if (migration.Version <= currentVersion) continue;`), which makes the two failure modes wildly asymmetric:

- A **collision** is loud. Two branches take the same N and the second one gets a rebase conflict.
- A **gap** is silent and unrepairable. Take N+1 while N is unclaimed, merge first, and stores stamp N+1;
  the branch that later lands N has its rung skipped forever — the objects never exist, every reader of
  them fails permanently, and no upgrade repairs it. That is why V45 is permanently absent and has a
  comment in its place rather than a rung.

So take `max(dev) + 1`, never reserve a number above an unmerged sibling's, and when two PRs want the same
number the first to actually merge keeps it while the other renumbers — rung and `StorageVersion` together —
at its own merge. `MigrationLadderPins` enforces both halves in CI:
`TheLadder_IsStrictlyOrdered_WithNoDuplicates` and `TheLadder_IsDenseAboveTheHistoricalGap`, with V45 as the
one sanctioned hole.

### Two-Store Parity

The shared libraries are written against seams, and every seam has one implementation per store —
`PgAlertStateStore` for Darling, `LiteAlertStateStore` for Lite, and so on. Two rules keep them from
drifting:

- **Declare interface members as required, not defaulted.** `IAlertStateStore`, `IAlertEngineSettings` and
  `IAlertDeliverer` give no member a default implementation, so adding one makes the compiler name every
  implementer *and* every test fake (`AlertEngineTests.FakeStateStore`, `DarlingSelfAlertTests.StubStateStore`,
  `LiteAlertForwardingTests.InMemoryStateStore`). A default implementation compiles and leaves the ones you
  forgot quietly doing nothing.
- **State added to one store reads as permanently empty on the other, and nothing fails to build.** A
  Darling migration rung plus a reader for it has a Lite twin: bump `DuckDbInitializer.CurrentSchemaVersion`,
  add an idempotent `if (fromVersion < N)` upgrade block, register the table in
  `Schema.GetAllTableStatements()`, and update the table-count assertion in `DuckDbSchemaTests`. Without it
  the shared engine keeps asking Lite a question its store can never answer.

---

## Code Style Guidelines

### T-SQL Style

All T-SQL code must follow the project's coding standards. Key points:

- **Keywords**: UPPERCASE (`SELECT`, `FROM`, `WHERE`)
- **Data types**: lowercase, never abbreviated (`integer` not `int`, `nvarchar(max)` not `nvarchar(MAX)`)
- **Object names**: Use `sysname` for SQL Server identifiers
- **Indentation**: 4 spaces (never tabs)
- **Table aliases**: Always use `AS` keyword (`FROM dbo.table AS t`)
- **Column aliases**: Use `column_name = expression` pattern
- **Commas**: Trailing commas on multi-line lists
- **Comments**: Use `/* ... */` block comments, never `--`
- **Functions**: Use `COUNT_BIG()` not `COUNT()`, `ROWCOUNT_BIG()` not `@@ROWCOUNT`

Example:

```sql
SELECT
    database_name = d.name,
    index_count = COUNT_BIG(i.index_id),
    total_size_mb = SUM(a.total_pages) * 8 / 1024
FROM sys.databases AS d
JOIN sys.indexes AS i
  ON i.database_id = d.database_id
WHERE d.database_id > 4
AND   d.state_desc = N'ONLINE'
GROUP BY
    d.name
ORDER BY
    total_size_mb DESC
OPTION(RECOMPILE);
```

A few more rules that come up in review, and the reasoning behind the ones that are
not obvious:

- **Unicode literals**: prefix with `N` (`N'ONLINE'`, not `'ONLINE'`).
- **`ON` continues its `JOIN` at two spaces**, so the join graph reads down the left edge.
- **`AND` / `OR` align their predicates** (`AND   d.state_desc = N'ONLINE'`), so a `WHERE`
  clause reads as a list rather than as prose.
- **`GROUP BY` / `ORDER BY` put each term on its own indented line**, so adding one is a
  one-line diff.
- **Never suggest missing-index DMV recommendations.** `sys.dm_db_missing_index_*` output
  is not used in this project and changes proposing it will not be accepted.
- **No full-text search.**

Collector queries specifically:

- **`OPTION(RECOMPILE)` on collector queries.** These run with parameters whose selectivity
  varies enormously between a first-run catch-up window and a steady-state minute, and a plan
  cached from one is wrong for the other. A statement added to an existing batch needs its own
  hint — one on a neighbouring statement does not cover it.
- **Comments explain WHY, at length.** This codebase's comments carry measurements, issue
  numbers, and the failure the line prevents. A comment restating the code is noise; one
  recording "this threshold was 300s and the fleet's median gap is 299s, so it discarded half
  of every sweep" is what stops the next person undoing it.

Darling's PostgreSQL store (not T-SQL — the Darling service stores to PostgreSQL/TimescaleDB):

- **Schema-qualify every object in a migration** (`collect.*`, `config.*`). The migrate session's
  `search_path` resolves bare names to a different schema, so an unqualified `CREATE` or `ALTER`
  can land an object in the wrong one silently.
- **Timestamps are naive UTC.** Columns are declared `timestamp`, never `timestamptz`, and SQL that stamps
  its own time uses `now() AT TIME ZONE 'UTC'`.
- **Strip the `DateTimeKind` before binding a timestamp parameter** —
  `DateTime.SpecifyKind(value, DateTimeKind.Unspecified)`. Npgsql infers `timestamptz` from a `Utc` or
  `Local` kind, PostgreSQL then converts into the session's time zone on the way into a naive column, and
  the row lands at the wrong hour with no error anywhere. Several files keep a one-line `Naive()` helper
  for exactly this; use it rather than binding a `DateTime` straight through.

### C# Style

Follow standard C# conventions:

- **Naming**: PascalCase for public members, _camelCase for private fields
- **Async/await**: Use for all I/O operations
- **Null handling**: Use nullable reference types, check for null appropriately
- **Comments**: XML documentation for public APIs
- **File organization**: One class per file, partial classes for large services

WPF-specific:
- Use data binding where practical
- Keep code-behind focused on UI logic
- Services handle data access and business logic

---

## Pull Request Process

### Before You Start

1. **Check existing issues** - Your idea may already be discussed
2. **Open an issue first** for significant changes
3. **Fork the repository** and create a feature branch

### Branch Naming

Use descriptive branch names:
- `feature/add-memory-pressure-collector`
- `fix/dashboard-connection-timeout`
- `docs/update-readme-lite-section`

### Commit Messages

Write clear, descriptive commit messages:

```
Add memory pressure event collector

- Created collect.memory_pressure_events table
- Added dbo.collect_memory_pressure procedure
- Queries sys.dm_os_ring_buffers for memory broker events
- Runs every 5 minutes on default schedule
```

### PR Checklist

Before submitting:

- [ ] Code follows the style guidelines above
- [ ] T-SQL has been tested with `@debug = 1`
- [ ] C# code compiles without warnings
- [ ] No hardcoded paths or credentials
- [ ] Changes are documented (comments, README if applicable)
- [ ] New files have copyright headers

### Review Process

1. Submit your PR with a clear description
2. Maintainers will review within a few days
3. Address any feedback
4. Once approved, your PR will be merged

---

## Testing Guidelines

### Testing T-SQL Collectors

Run collectors with debug output:

```sql
/* Test the collector */
EXECUTE dbo.collect_your_collector
    @debug = 1;

/* Verify data was collected */
SELECT TOP (10) *
FROM collect.your_table
ORDER BY collection_time DESC;

/* Check collection log for errors */
SELECT TOP (10) *
FROM config.collection_log
WHERE collector_name = N'your_collector'
ORDER BY collection_time DESC;
```

Use the troubleshooting scripts:
- `install/99_installer_troubleshooting.sql` - Quick health check
- `install/99_user_troubleshooting.sql` - Detailed diagnostics

### Testing Dashboard Changes

1. Build and run the Dashboard
2. Connect to a test SQL Server with PerformanceMonitor installed
3. Verify your changes appear correctly
4. Test with different time ranges
5. Check that CSV export still works (right-click any grid)

### Testing Lite Changes

1. Build and run the Lite application
2. Connect to a test SQL Server (VIEW SERVER STATE permission required)
3. Wait for initial collection (1-5 minutes)
4. Verify data appears in the appropriate tab
5. Test auto-refresh functionality
6. If you added MCP tools, test with Claude Code:
   ```
   claude mcp add --transport http --scope user sql-monitor http://localhost:5151/
   ```

### Testing Darling Changes

`Darling.Tests` targets `net10.0-windows`, so it builds anywhere but only runs on Windows. Three CI jobs
cover it, and which one goes red tells you where to look:

- **Darling Linux build** — it does not compile. The fastest signal available.
- **Darling PostgreSQL tests** — the full `Darling.Tests` suite against a real PostgreSQL with TimescaleDB.
  `DARLING_TEST_PG` lights up the `[Collection("live-postgres")]` classes, so this is the job that actually
  applies the ladder and reads the probe back. A migration mistake surfaces here.
- **build** (Windows) — runs `Lite.Tests` and `Darling.Tests` without a live store. This is where a
  two-store parity gap surfaces.

### SQL Server Versions

Test against multiple versions if possible:
- SQL Server 2016 (minimum supported)
- SQL Server 2019
- SQL Server 2022
- Azure SQL Database (Lite only)

Some DMVs behave differently across versions. Handle version differences gracefully.

---

## Questions?

- **Bug reports**: Open a GitHub issue using the bug report template
- **Feature requests**: Open a GitHub issue using the feature request template
- **General questions**: Start a GitHub Discussion

Thank you for contributing!
