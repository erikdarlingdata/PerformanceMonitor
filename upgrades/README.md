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

1. **Always check before altering**: Use `IF NOT EXISTS` checks before adding columns/indexes
2. **Be idempotent**: Scripts should be safe to run multiple times
3. **Preserve data**: Never DROP tables with data (use ALTER/UPDATE instead)
4. **Add comments**: Document why each change is being made
5. **Test upgrade paths**: Test upgrading from each previous version

## Example Upgrade Script

```sql
/*
Upgrade from 1.0.0 to 1.1.0
Adds execution context tracking to query_stats
*/

-- Add new column if it doesn't exist
IF NOT EXISTS (
    SELECT 1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID('collect.query_stats')
    AND name = 'execution_context'
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

Future upgrade folders will be added here as new versions are released.
