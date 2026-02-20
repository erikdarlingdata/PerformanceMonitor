/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 1.2.0 to 1.3.0
Adds total_physical_memory_mb and committed_target_memory_mb to collect.memory_stats
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
    WHERE object_id = OBJECT_ID(N'collect.memory_stats')
    AND   name = N'total_physical_memory_mb'
)
BEGIN
    ALTER TABLE
        collect.memory_stats
    ADD
        total_physical_memory_mb decimal(19,2) NULL;

    PRINT 'Added total_physical_memory_mb to collect.memory_stats';
END;
GO

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.memory_stats')
    AND   name = N'committed_target_memory_mb'
)
BEGIN
    ALTER TABLE
        collect.memory_stats
    ADD
        committed_target_memory_mb decimal(19,2) NULL;

    PRINT 'Added committed_target_memory_mb to collect.memory_stats';
END;
GO
