/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 1.3.0 to 2.0.0
Removes session_wait_stats collector (zero UI, never surfaced in Dashboard or Lite).
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

/* Remove reporting view */
IF OBJECT_ID(N'report.session_wait_analysis', N'V') IS NOT NULL
BEGIN
    DROP VIEW
        report.session_wait_analysis;

    PRINT 'Dropped report.session_wait_analysis';
END;
GO

/* Remove collector procedure */
IF OBJECT_ID(N'collect.session_wait_stats_collector', N'P') IS NOT NULL
BEGIN
    DROP PROCEDURE
        collect.session_wait_stats_collector;

    PRINT 'Dropped collect.session_wait_stats_collector';
END;
GO

/* Remove collection table */
IF OBJECT_ID(N'collect.session_wait_stats', N'U') IS NOT NULL
BEGIN
    DROP TABLE
        collect.session_wait_stats;

    PRINT 'Dropped collect.session_wait_stats';
END;
GO

/* Remove schedule entry */
IF OBJECT_ID(N'config.collection_schedule', N'U') IS NOT NULL
BEGIN
    DELETE FROM
        config.collection_schedule
    WHERE
        collector_name = N'session_wait_stats_collector';

    PRINT 'Removed session_wait_stats_collector from config.collection_schedule';
END;
GO
