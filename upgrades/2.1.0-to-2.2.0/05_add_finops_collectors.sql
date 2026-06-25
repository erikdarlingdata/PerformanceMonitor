/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.1.0 to 2.2.0
Adds FinOps collector schedule entries for existing installations.
Tables self-heal via ensure_collection_table; views use CREATE OR ALTER.
Only the schedule entries need explicit insertion for upgrades.
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
    FROM config.collection_schedule
    WHERE collector_name = N'database_size_stats_collector'
)
BEGIN
    INSERT INTO
        config.collection_schedule
    (
        collector_name,
        enabled,
        frequency_minutes,
        max_duration_minutes,
        retention_days,
        description
    )
    VALUES
    (
        N'database_size_stats_collector',
        1,
        60,
        10,
        90,
        N'Database file sizes for growth trending and capacity planning'
    );

    PRINT 'Added database_size_stats_collector to collection schedule';
END;
GO

IF NOT EXISTS
(
    SELECT
        1/0
    FROM config.collection_schedule
    WHERE collector_name = N'server_properties_collector'
)
BEGIN
    INSERT INTO
        config.collection_schedule
    (
        collector_name,
        enabled,
        frequency_minutes,
        max_duration_minutes,
        retention_days,
        description
    )
    VALUES
    (
        N'server_properties_collector',
        1,
        1440,
        5,
        365,
        N'Server edition, licensing, CPU/memory hardware metadata for license audit'
    );

    PRINT 'Added server_properties_collector to collection schedule';
END;
GO
