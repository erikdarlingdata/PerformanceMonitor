/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 1.3.0 to 2.0.0
Adds server_start_time and delta columns to collect.memory_grant_stats
for proper delta framework integration.
Drops unused warning columns from the inline delta approach.
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

/* Add server_start_time for delta framework restart detection */
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.memory_grant_stats')
    AND   name = N'server_start_time'
)
BEGIN
    ALTER TABLE
        collect.memory_grant_stats
    ADD
        server_start_time datetime2(7) NULL;

    PRINT 'Added server_start_time to collect.memory_grant_stats';
END;
GO

/* Add timeout_error_count_delta for delta framework */
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.memory_grant_stats')
    AND   name = N'timeout_error_count_delta'
)
BEGIN
    ALTER TABLE
        collect.memory_grant_stats
    ADD
        timeout_error_count_delta bigint NULL;

    PRINT 'Added timeout_error_count_delta to collect.memory_grant_stats';
END;
GO

/* Add forced_grant_count_delta for delta framework */
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.memory_grant_stats')
    AND   name = N'forced_grant_count_delta'
)
BEGIN
    ALTER TABLE
        collect.memory_grant_stats
    ADD
        forced_grant_count_delta bigint NULL;

    PRINT 'Added forced_grant_count_delta to collect.memory_grant_stats';
END;
GO

/* Add sample_interval_seconds for delta framework */
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.memory_grant_stats')
    AND   name = N'sample_interval_seconds'
)
BEGIN
    ALTER TABLE
        collect.memory_grant_stats
    ADD
        sample_interval_seconds integer NULL;

    PRINT 'Added sample_interval_seconds to collect.memory_grant_stats';
END;
GO

/* Drop unused warning columns from the old inline delta approach */
IF EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.memory_grant_stats')
    AND   name = N'available_memory_pressure_warning'
)
BEGIN
    ALTER TABLE
        collect.memory_grant_stats
    DROP COLUMN
        available_memory_pressure_warning;

    PRINT 'Dropped available_memory_pressure_warning from collect.memory_grant_stats';
END;
GO

IF EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.memory_grant_stats')
    AND   name = N'waiter_count_warning'
)
BEGIN
    ALTER TABLE
        collect.memory_grant_stats
    DROP COLUMN
        waiter_count_warning;

    PRINT 'Dropped waiter_count_warning from collect.memory_grant_stats';
END;
GO

IF EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.memory_grant_stats')
    AND   name = N'timeout_error_warning'
)
BEGIN
    ALTER TABLE
        collect.memory_grant_stats
    DROP COLUMN
        timeout_error_warning;

    PRINT 'Dropped timeout_error_warning from collect.memory_grant_stats';
END;
GO

IF EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.memory_grant_stats')
    AND   name = N'forced_grant_warning'
)
BEGIN
    ALTER TABLE
        collect.memory_grant_stats
    DROP COLUMN
        forced_grant_warning;

    PRINT 'Dropped forced_grant_warning from collect.memory_grant_stats';
END;
GO
