/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.2.0 to 2.3.0
Widens query_stats columns to match sys.dm_exec_query_stats DMV types:
  - min_dop, max_dop: smallint -> bigint
  - min_reserved_threads, max_reserved_threads: integer -> bigint
  - min_used_threads, max_used_threads: integer -> bigint
Fixes arithmetic overflow error on INSERT (#547)
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

IF OBJECT_ID(N'collect.query_stats', N'U') IS NOT NULL
BEGIN
    PRINT 'Widening collect.query_stats columns to match DMV types...';

    IF EXISTS
    (
        SELECT
            1/0
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = N'collect'
        AND   TABLE_NAME = N'query_stats'
        AND   COLUMN_NAME = N'min_dop'
        AND   DATA_TYPE = N'smallint'
    )
    BEGIN
        ALTER TABLE collect.query_stats ALTER COLUMN min_dop bigint NOT NULL;
        ALTER TABLE collect.query_stats ALTER COLUMN max_dop bigint NOT NULL;
        PRINT '  min_dop, max_dop: smallint -> bigint';
    END;

    IF EXISTS
    (
        SELECT
            1/0
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = N'collect'
        AND   TABLE_NAME = N'query_stats'
        AND   COLUMN_NAME = N'min_reserved_threads'
        AND   DATA_TYPE = N'int'
    )
    BEGIN
        ALTER TABLE collect.query_stats ALTER COLUMN min_reserved_threads bigint NOT NULL;
        ALTER TABLE collect.query_stats ALTER COLUMN max_reserved_threads bigint NOT NULL;
        ALTER TABLE collect.query_stats ALTER COLUMN min_used_threads bigint NOT NULL;
        ALTER TABLE collect.query_stats ALTER COLUMN max_used_threads bigint NOT NULL;
        PRINT '  min/max_reserved_threads, min/max_used_threads: int -> bigint';
    END;

    PRINT 'Column widening complete.';
END;
ELSE
BEGIN
    PRINT 'Table collect.query_stats does not exist, skipping.';
END;
GO
