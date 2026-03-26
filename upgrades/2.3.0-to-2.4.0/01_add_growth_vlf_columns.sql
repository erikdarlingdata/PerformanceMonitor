/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.3.0 to 2.4.0
Re-applies growth/VLF columns for servers that upgraded to 2.3.0 before PR #625 shipped
Adds growth settings and VLF count columns to collect.database_size_stats:

database_size_stats:
  - is_percent_growth: new column (bit NULL) — true when auto-growth is percent-based
  - growth_pct:        new column (integer NULL) — raw growth percent value (set when is_percent_growth = 1)
  - vlf_count:         new column (integer NULL) — VLF count for log files (NULL for data files)
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
database_size_stats: add growth settings and VLF count columns
*/
IF OBJECT_ID(N'collect.database_size_stats', N'U') IS NOT NULL
BEGIN
    PRINT 'Checking collect.database_size_stats columns...';

    IF NOT EXISTS
    (
        SELECT
            1/0
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = N'collect'
        AND   TABLE_NAME = N'database_size_stats'
        AND   COLUMN_NAME = N'is_percent_growth'
    )
    BEGIN
        ALTER TABLE collect.database_size_stats ADD is_percent_growth bit NULL;
        PRINT '  is_percent_growth: added (bit NULL)';
    END;

    IF NOT EXISTS
    (
        SELECT
            1/0
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = N'collect'
        AND   TABLE_NAME = N'database_size_stats'
        AND   COLUMN_NAME = N'growth_pct'
    )
    BEGIN
        ALTER TABLE collect.database_size_stats ADD growth_pct integer NULL;
        PRINT '  growth_pct: added (integer NULL)';
    END;

    IF NOT EXISTS
    (
        SELECT
            1/0
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = N'collect'
        AND   TABLE_NAME = N'database_size_stats'
        AND   COLUMN_NAME = N'vlf_count'
    )
    BEGIN
        ALTER TABLE collect.database_size_stats ADD vlf_count integer NULL;
        PRINT '  vlf_count: added (integer NULL)';
    END;

    PRINT 'database_size_stats complete.';
END;
ELSE
BEGIN
    PRINT 'collect.database_size_stats not found — skipping.';
END;
GO

PRINT 'Upgrade 03_add_growth_vlf_columns complete.';
GO
