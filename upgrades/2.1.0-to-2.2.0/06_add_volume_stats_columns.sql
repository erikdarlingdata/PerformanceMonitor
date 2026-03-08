/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.1.0 to 2.2.0
Adds volume-level drive space columns to database_size_stats.
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
    WHERE object_id = OBJECT_ID(N'collect.database_size_stats', N'U')
    AND   name = N'volume_mount_point'
)
BEGIN
    ALTER TABLE
        collect.database_size_stats
    ADD
        volume_mount_point nvarchar(256) NULL,
        volume_total_mb decimal(19,2) NULL,
        volume_free_mb decimal(19,2) NULL;

    PRINT 'Added volume_mount_point, volume_total_mb, volume_free_mb to collect.database_size_stats';
END;
GO
