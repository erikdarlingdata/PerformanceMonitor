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

This repository contains two editions of the SQL Server Performance Monitor:

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

---

## Development Setup

### Prerequisites

- **Windows 10/11** (required for WPF)
- **.NET 8.0 SDK** ([download](https://dotnet.microsoft.com/download/dotnet/8.0))
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

```

### Running the Applications

**Full Dashboard:**
1. Install the database on a SQL Server instance using the installer
2. Run `Dashboard/bin/Debug/net8.0-windows/Dashboard.exe`
3. Add your server connection and start monitoring

**Lite Edition:**
1. Run `Lite/bin/Debug/net8.0-windows/PerformanceMonitorLite.exe`
2. Add a SQL Server connection (requires VIEW SERVER STATE permission)
3. Data collection begins automatically

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

The full T-SQL style guide is in [CLAUDE.md](CLAUDE.md).

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
