# Upgrade Scripts

This folder contains version-specific upgrade scripts for the Performance Monitor system.

## Structure

Each upgrade is stored in a folder named `{from_version}-to-{to_version}/`:

```
upgrades/
├── 1.0.0-to-1.1.0/
│   ├── 01_add_new_columns.sql
│   ├── 02_create_new_collector.sql
│   └── upgrade.txt
├── 1.1.0-to-1.2.0/
│   └── ...
```

## upgrade.txt Format

Each upgrade folder must contain an `upgrade.txt` file listing the SQL files to execute in order:

```
01_add_new_columns.sql
02_create_new_collector.sql
```

## How Upgrades Work

The installer:
1. Detects current installed version from `config.installation_history`
2. Determines which upgrade folders to apply
3. Executes upgrade folders in sequence
4. Updates all stored procedures, views, and functions to the new version
5. Logs the upgrade in `config.installation_history`

## Upgrade Script Guidelines

1. **Start from `_template.sql`**: Copy the template for every new upgrade script — it has the required SET options and `USE PerformanceMonitor` that the installer depends on
2. **Always check before altering**: Use `IF NOT EXISTS` / `IF EXISTS` checks before adding or modifying columns/indexes
3. **Be idempotent**: Scripts should be safe to run multiple times
4. **Preserve data**: Never DROP tables with data (use ALTER/UPDATE instead)
5. **Add comments**: Document why each change is being made
6. **Test upgrade paths**: Test upgrading from each previous version

## Example Upgrade Script

```sql
/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 1.0.0 to 1.1.0
Adds execution context tracking to query_stats
*/

SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;
SET IMPLICIT_TRANSACTIONS OFF;
SET STATISTICS TIME, IO OFF;
GO

USE PerformanceMonitor;
GO

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.query_stats')
    AND   name = N'execution_context'
)
BEGIN
    ALTER TABLE collect.query_stats
        ADD execution_context nvarchar(128) NULL;

    PRINT 'Added execution_context column to collect.query_stats';
END;
GO
```

## Version History

- **1.0.0**: Initial release
- **1.1.0**: Remove invalid query_hash column from trace_analysis table; fix trace_analysis_collector to properly query sys.traces for file paths; add PerformanceMonitor database exclusion filter to trace; make trace START action idempotent
- **1.2.0**: Current Configuration tabs, Default Trace DynamicResource fix, alert badge, chart tooltips, drill-down sizing
- **1.3.0**: Add total_physical_memory_mb and committed_target_memory_mb to memory_stats collector

- **2.0.0**: Add server_start_time and delta columns to memory_grant_stats for delta framework; drop unused warning columns; new Memory Grants charts

Future upgrade folders will be added here as new versions are released.
