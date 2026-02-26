/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 1.3.0 to 2.0.0
Adds duration_us and end_time columns to collect.default_trace_events
for autogrow duration tracking and event completion times.
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

/* Add duration_us for autogrow/shrink I/O stall duration (microseconds) */
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.default_trace_events')
    AND   name = N'duration_us'
)
BEGIN
    ALTER TABLE
        collect.default_trace_events
    ADD
        duration_us bigint NULL;

    PRINT 'Added duration_us to collect.default_trace_events';
END;
GO

/* Add end_time for event completion timestamp */
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'collect.default_trace_events')
    AND   name = N'end_time'
)
BEGIN
    ALTER TABLE
        collect.default_trace_events
    ADD
        end_time datetime2(7) NULL;

    PRINT 'Added end_time to collect.default_trace_events';
END;
GO
