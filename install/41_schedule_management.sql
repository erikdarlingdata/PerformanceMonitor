/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

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

/*
Schedule management procedures
Provides easy configuration of collection frequencies and settings
*/

/*
Update collector frequency
*/
IF OBJECT_ID(N'config.update_collector_frequency', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE config.update_collector_frequency AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    config.update_collector_frequency
(
    @collector_name sysname,
    @frequency_minutes integer,
    @enabled bit = NULL,
    @max_duration_minutes integer = NULL
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @rows_updated bigint = 0;

    BEGIN TRY
        UPDATE
            config.collection_schedule
        SET
            frequency_minutes = @frequency_minutes,
            enabled = ISNULL(@enabled, enabled),
            max_duration_minutes = ISNULL(@max_duration_minutes, max_duration_minutes),
            modified_date = SYSDATETIME(),
            next_run_time =
                CASE
                    WHEN ISNULL(@enabled, enabled) = 1
                    THEN SYSDATETIME()
                    ELSE next_run_time
                END
        WHERE collector_name = @collector_name;

        SET @rows_updated = ROWCOUNT_BIG();

        IF @rows_updated = 0
        BEGIN
            RAISERROR(N'Collector "%s" not found in schedule', 16, 1, @collector_name);
            RETURN;
        END;

        PRINT 'Updated ' + @collector_name + ' frequency to ' + CONVERT(varchar(10), @frequency_minutes) + ' minutes';

    END TRY
    BEGIN CATCH
        DECLARE @error_message nvarchar(4000) = ERROR_MESSAGE();
        RAISERROR(N'Error updating collector frequency: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

/*
Enable/disable collector
*/
IF OBJECT_ID(N'config.set_collector_enabled', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE config.set_collector_enabled AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    config.set_collector_enabled
(
    @collector_name sysname,
    @enabled bit
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @rows_updated bigint = 0;

    BEGIN TRY
        UPDATE
            config.collection_schedule
        SET
            enabled = @enabled,
            modified_date = SYSDATETIME(),
            next_run_time =
                CASE
                    WHEN @enabled = 1
                    THEN SYSDATETIME()
                    ELSE NULL
                END
        WHERE collector_name = @collector_name;

        SET @rows_updated = ROWCOUNT_BIG();

        IF @rows_updated = 0
        BEGIN
            RAISERROR(N'Collector "%s" not found in schedule', 16, 1, @collector_name);
            RETURN;
        END;

        PRINT @collector_name + ' collector ' +
              CASE WHEN @enabled = 1 THEN 'enabled' ELSE 'disabled' END;

    END TRY
    BEGIN CATCH
        DECLARE @error_message nvarchar(4000) = ERROR_MESSAGE();
        RAISERROR(N'Error setting collector enabled status: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

/*
Apply a named collection preset

Valid preset names: Off, Aggressive, Balanced, Low-Impact

Off sets enabled = 0 on every row in config.collection_schedule and changes
nothing else — no frequency edits. The other three set enabled = 1 on every
row (so switching back from Off reliably reactivates collection) and update
frequency_minutes for the collectors they list. Daily/on-load collectors
that aren't in the preset keep their existing frequency.

Heads up: applying a non-Off preset overrides any manual
UPDATE config.collection_schedule SET enabled = 0 on a specific collector.
If that matters in your environment, re-disable it after switching presets.
*/
IF OBJECT_ID(N'config.apply_collection_preset', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE config.apply_collection_preset AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    config.apply_collection_preset
(
    @preset_name sysname,
    @debug bit = 0
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @rows_updated bigint = 0;

    BEGIN TRY
        IF @preset_name NOT IN (N'Off', N'Aggressive', N'Balanced', N'Low-Impact')
        BEGIN
            RAISERROR(N'Invalid preset name "%s". Valid presets: Off, Aggressive, Balanced, Low-Impact', 16, 1, @preset_name);
            RETURN;
        END;

        /*
        Off disables every collector and exits. No frequency table needed.
        Pair with a second Agent job that applies a non-Off preset at the
        start of your active window to resume collection.
        */
        IF @preset_name = N'Off'
        BEGIN
            UPDATE
                config.collection_schedule
            SET
                enabled = 0,
                modified_date = SYSDATETIME();

            SET @rows_updated = ROWCOUNT_BIG();

            IF @debug = 1
            BEGIN
                RAISERROR(N'Applied "Off" preset — %I64d collectors disabled', 0, 1, @rows_updated) WITH NOWAIT;
            END;

            PRINT 'Applied "Off" collection preset (' + CONVERT(varchar(10), @rows_updated) + ' collectors disabled)';
            RETURN;
        END;

        DECLARE
            @preset TABLE
            (
                collector_name sysname NOT NULL,
                frequency_minutes integer NOT NULL
            );

        IF @preset_name = N'Aggressive'
        BEGIN
            INSERT INTO
                @preset
            (
                collector_name,
                frequency_minutes
            )
            VALUES
                (N'wait_stats_collector', 1),
                (N'query_stats_collector', 1),
                (N'memory_stats_collector', 1),
                (N'memory_pressure_events_collector', 1),
                (N'system_health_collector', 2),
                (N'blocked_process_xml_collector', 1),
                (N'deadlock_xml_collector', 1),
                (N'process_blocked_process_xml', 2),
                (N'blocking_deadlock_analyzer', 2),
                (N'process_deadlock_xml', 2),
                (N'query_store_collector', 2),
                (N'procedure_stats_collector', 1),
                (N'query_snapshots_collector', 1),
                (N'file_io_stats_collector', 1),
                (N'memory_grant_stats_collector', 1),
                (N'cpu_scheduler_stats_collector', 1),
                (N'memory_clerks_stats_collector', 2),
                (N'perfmon_stats_collector', 1),
                (N'cpu_utilization_stats_collector', 1),
                (N'trace_analysis_collector', 1),
                (N'default_trace_collector', 2),
                (N'configuration_issues_analyzer', 1),
                (N'latch_stats_collector', 1),
                (N'spinlock_stats_collector', 1),
                (N'tempdb_stats_collector', 1),
                (N'plan_cache_stats_collector', 2),
                (N'session_stats_collector', 1),
                (N'waiting_tasks_collector', 1),
                (N'running_jobs_collector', 2);
        END;

        IF @preset_name = N'Balanced'
        BEGIN
            INSERT INTO
                @preset
            (
                collector_name,
                frequency_minutes
            )
            VALUES
                (N'wait_stats_collector', 1),
                (N'query_stats_collector', 2),
                (N'memory_stats_collector', 1),
                (N'memory_pressure_events_collector', 1),
                (N'system_health_collector', 5),
                (N'blocked_process_xml_collector', 1),
                (N'deadlock_xml_collector', 1),
                (N'process_blocked_process_xml', 5),
                (N'blocking_deadlock_analyzer', 5),
                (N'process_deadlock_xml', 5),
                (N'query_store_collector', 2),
                (N'procedure_stats_collector', 2),
                (N'query_snapshots_collector', 1),
                (N'file_io_stats_collector', 1),
                (N'memory_grant_stats_collector', 1),
                (N'cpu_scheduler_stats_collector', 1),
                (N'memory_clerks_stats_collector', 5),
                (N'perfmon_stats_collector', 5),
                (N'cpu_utilization_stats_collector', 1),
                (N'trace_analysis_collector', 2),
                (N'default_trace_collector', 5),
                (N'configuration_issues_analyzer', 1),
                (N'latch_stats_collector', 1),
                (N'spinlock_stats_collector', 1),
                (N'tempdb_stats_collector', 1),
                (N'plan_cache_stats_collector', 5),
                (N'session_stats_collector', 1),
                (N'waiting_tasks_collector', 1),
                (N'running_jobs_collector', 1);
        END;

        IF @preset_name = N'Low-Impact'
        BEGIN
            INSERT INTO
                @preset
            (
                collector_name,
                frequency_minutes
            )
            VALUES
                (N'wait_stats_collector', 5),
                (N'query_stats_collector', 10),
                (N'memory_stats_collector', 10),
                (N'memory_pressure_events_collector', 5),
                (N'system_health_collector', 15),
                (N'blocked_process_xml_collector', 5),
                (N'deadlock_xml_collector', 5),
                (N'process_blocked_process_xml', 10),
                (N'blocking_deadlock_analyzer', 10),
                (N'process_deadlock_xml', 10),
                (N'query_store_collector', 30),
                (N'procedure_stats_collector', 10),
                (N'query_snapshots_collector', 5),
                (N'file_io_stats_collector', 10),
                (N'memory_grant_stats_collector', 5),
                (N'cpu_scheduler_stats_collector', 5),
                (N'memory_clerks_stats_collector', 30),
                (N'perfmon_stats_collector', 5),
                (N'cpu_utilization_stats_collector', 5),
                (N'trace_analysis_collector', 10),
                (N'default_trace_collector', 15),
                (N'configuration_issues_analyzer', 5),
                (N'latch_stats_collector', 5),
                (N'spinlock_stats_collector', 5),
                (N'tempdb_stats_collector', 5),
                (N'plan_cache_stats_collector', 15),
                (N'session_stats_collector', 5),
                (N'waiting_tasks_collector', 5),
                (N'running_jobs_collector', 30);
        END;

        /*
        Re-enable every collector first so a switch from Off → named preset
        reliably resumes collection, including daily/on-load collectors that
        aren't in the preset frequency table.
        */
        UPDATE
            config.collection_schedule
        SET
            enabled = 1,
            modified_date = SYSDATETIME()
        WHERE
            enabled = 0;

        /*
        Apply the preset frequencies to the collectors it lists.
        Daily/on-load collectors not in the preset keep their existing frequency.
        */
        UPDATE
            cs
        SET
            cs.frequency_minutes = p.frequency_minutes,
            cs.next_run_time = SYSDATETIME(),
            cs.modified_date = SYSDATETIME()
        FROM config.collection_schedule AS cs
        JOIN @preset AS p
          ON p.collector_name = cs.collector_name;

        SET @rows_updated = ROWCOUNT_BIG();

        IF @debug = 1
        BEGIN
            RAISERROR(N'Applied "%s" preset to %I64d collectors', 0, 1, @preset_name, @rows_updated) WITH NOWAIT;

            SELECT
                cs.collector_name,
                cs.enabled,
                cs.frequency_minutes,
                cs.next_run_time,
                cs.description
            FROM config.collection_schedule AS cs
            WHERE cs.frequency_minutes < 1440
            ORDER BY
                cs.collector_name;
        END;

        PRINT 'Applied "' + @preset_name + '" collection preset (' + CONVERT(varchar(10), @rows_updated) + ' collectors updated)';

    END TRY
    BEGIN CATCH
        DECLARE
            @error_message nvarchar(4000) = ERROR_MESSAGE();

        RAISERROR(N'Error applying collection preset: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

/*
Drop legacy profile procedures replaced by config.apply_collection_preset
*/
IF OBJECT_ID(N'config.enable_realtime_monitoring', N'P') IS NOT NULL
BEGIN
    DROP PROCEDURE config.enable_realtime_monitoring;
END;
GO

IF OBJECT_ID(N'config.enable_consulting_analysis', N'P') IS NOT NULL
BEGIN
    DROP PROCEDURE config.enable_consulting_analysis;
END;
GO

IF OBJECT_ID(N'config.enable_baseline_monitoring', N'P') IS NOT NULL
BEGIN
    DROP PROCEDURE config.enable_baseline_monitoring;
END;
GO

/*
Show current schedule status
*/
IF OBJECT_ID(N'config.show_collection_schedule', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE config.show_collection_schedule AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    config.show_collection_schedule
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    SELECT
        collector_name,
        enabled,
        frequency_minutes,
        next_run_time,
        minutes_until_next_run =
            CASE
                WHEN enabled = 1 AND next_run_time IS NOT NULL
                THEN DATEDIFF(MINUTE, SYSDATETIME(), next_run_time)
                ELSE NULL
            END,
        last_run_time,
        max_duration_minutes,
        description
    FROM config.collection_schedule
    ORDER BY
        enabled DESC,
        next_run_time;
END;
GO

PRINT 'Schedule management procedures created successfully';
PRINT '';
PRINT 'Available procedures:';
PRINT '- config.update_collector_frequency - Change frequency for specific collector';
PRINT '- config.set_collector_enabled - Enable/disable specific collector';
PRINT '- config.apply_collection_preset - Apply a named preset (Off, Aggressive, Balanced, Low-Impact)';
PRINT '- config.show_collection_schedule - Display current schedule';
PRINT '';
PRINT 'Examples:';
PRINT '  EXECUTE config.apply_collection_preset @preset_name = N''Aggressive'', @debug = 1;';
PRINT '  EXECUTE config.apply_collection_preset @preset_name = N''Balanced'';';
PRINT '  EXECUTE config.apply_collection_preset @preset_name = N''Low-Impact'';';
PRINT '  EXECUTE config.apply_collection_preset @preset_name = N''Off'';  -- disables all collectors';
GO
