/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Add duration_us and end_time columns to collect.default_trace_events
These columns were added in the 2.1.0 install scripts for the default trace collector
*/

USE PerformanceMonitor;
GO

IF NOT EXISTS
(
    SELECT
        1/0
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = N'collect'
    AND   TABLE_NAME = N'default_trace_events'
    AND   COLUMN_NAME = N'duration_us'
)
BEGIN
    ALTER TABLE collect.default_trace_events
        ADD duration_us bigint NULL;
END;
GO

IF NOT EXISTS
(
    SELECT
        1/0
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = N'collect'
    AND   TABLE_NAME = N'default_trace_events'
    AND   COLUMN_NAME = N'end_time'
)
BEGIN
    ALTER TABLE collect.default_trace_events
        ADD end_time datetime2(7) NULL;
END;
GO
