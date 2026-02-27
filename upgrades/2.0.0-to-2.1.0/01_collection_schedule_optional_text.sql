/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.0.0 to 2.1.0
Adds collect_query and collect_plan columns to config.collection_schedule
for optional query text and execution plan collection (#337).
Both default to 1 (enabled) to preserve existing behavior.
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

/* Add collect_query column for optional query text collection */
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'config.collection_schedule')
    AND   name = N'collect_query'
)
BEGIN
    ALTER TABLE
        config.collection_schedule
    ADD
        collect_query bit NOT NULL
            CONSTRAINT DF_collection_schedule_collect_query
            DEFAULT CONVERT(bit, 'true');

    PRINT 'Added collect_query to config.collection_schedule';
END;
GO

/* Add collect_plan column for optional execution plan collection */
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'config.collection_schedule')
    AND   name = N'collect_plan'
)
BEGIN
    ALTER TABLE
        config.collection_schedule
    ADD
        collect_plan bit NOT NULL
            CONSTRAINT DF_collection_schedule_collect_plan
            DEFAULT CONVERT(bit, 'true');

    PRINT 'Added collect_plan to config.collection_schedule';
END;
GO
