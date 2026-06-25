/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.11.0 to 3.0.0
Issue #1048: Linux host CPU pinned at 100%.

On SQL Server on Linux the RING_BUFFER_SCHEDULER_MONITOR ring buffer reports
SystemIdle = 0 (a documented platform limitation — Microsoft's own sample query
carries the comment "SystemIdle on Linux will be 0"). The collector derives
other_process_cpu_utilization as 100 - SystemIdle - ProcessUtilization, so on
Linux that becomes 100 - 0 - sqlcpu and the host total (sqlserver + other) pins
at 100% forever. SQL Server's own CPU number (ProcessUtilization) is correct;
only the host/other figure is fabricated. No DMV exposes true host CPU on Linux,
so the collector now stores NULL for other_process_cpu_utilization on Linux and
the Dashboard renders the host figure as unavailable rather than a false 100%.

That requires other_process_cpu_utilization to be nullable. It cannot be ALTERed
in place because the PERSISTED computed column total_cpu_utilization references
it (ALTER COLUMN is blocked while a computed column depends on the column), so we
drop the computed column, widen the base column to NULL, then re-add the computed
column. NULL in the base column propagates to NULL in total_cpu_utilization.

Idempotent and partial-failure safe: each step is guarded independently, so a
re-run (or a re-run after an interrupted run) converges to the target shape.
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

IF OBJECT_ID(N'collect.cpu_utilization_stats', N'U') IS NULL
BEGIN
    PRINT 'collect.cpu_utilization_stats does not exist — no action taken';
END;
ELSE
BEGIN
    DECLARE
        @is_nullable bit =
        (
            SELECT
                c.is_nullable
            FROM sys.columns AS c
            WHERE c.object_id = OBJECT_ID(N'collect.cpu_utilization_stats')
            AND   c.name = N'other_process_cpu_utilization'
        );

    IF @is_nullable = 0
    BEGIN
        /*
        1. Drop the persisted computed column that depends on the base column.
           ALTER COLUMN below is blocked while this dependency exists.
        */
        IF COL_LENGTH(N'collect.cpu_utilization_stats', N'total_cpu_utilization') IS NOT NULL
        BEGIN
            ALTER TABLE
                collect.cpu_utilization_stats
                DROP COLUMN total_cpu_utilization;

            PRINT 'Dropped computed column collect.cpu_utilization_stats.total_cpu_utilization';
        END;

        /*
        2. Widen the base column to allow NULL (Linux host CPU is not derivable).
        */
        ALTER TABLE
            collect.cpu_utilization_stats
            ALTER COLUMN other_process_cpu_utilization integer NULL;

        PRINT 'Made collect.cpu_utilization_stats.other_process_cpu_utilization nullable';
    END;
    ELSE
    BEGIN
        PRINT 'collect.cpu_utilization_stats.other_process_cpu_utilization already nullable — no action taken';
    END;

    /*
    3. Ensure the computed column exists. Guarded separately so an interrupted
       prior run (computed dropped, then a failure before re-add) still recovers.
       NULL in other_process_cpu_utilization propagates to NULL in total.
    */
    IF COL_LENGTH(N'collect.cpu_utilization_stats', N'total_cpu_utilization') IS NULL
    BEGIN
        ALTER TABLE
            collect.cpu_utilization_stats
            ADD total_cpu_utilization AS (sqlserver_cpu_utilization + other_process_cpu_utilization) PERSISTED;

        PRINT 'Re-added computed column collect.cpu_utilization_stats.total_cpu_utilization';
    END;
END;
GO
