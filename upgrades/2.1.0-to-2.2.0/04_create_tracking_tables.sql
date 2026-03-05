/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.1.0 to 2.2.0
Creates deduplication tracking tables for the three compressed collectors.
Each table holds one row per natural key with the latest row_hash,
allowing collectors to skip unchanged rows without scanning full history.
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

IF OBJECT_ID(N'collect.query_stats_latest_hash', N'U') IS NULL
BEGIN
    CREATE TABLE
        collect.query_stats_latest_hash
    (
        sql_handle varbinary(64) NOT NULL,
        statement_start_offset integer NOT NULL,
        statement_end_offset integer NOT NULL,
        plan_handle varbinary(64) NOT NULL,
        row_hash binary(32) NOT NULL,
        last_seen datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        CONSTRAINT
            PK_query_stats_latest_hash
        PRIMARY KEY CLUSTERED
            (sql_handle, statement_start_offset,
             statement_end_offset, plan_handle)
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.query_stats_latest_hash';
END;
ELSE
BEGIN
    PRINT 'collect.query_stats_latest_hash already exists — skipping.';
END;
GO

IF OBJECT_ID(N'collect.procedure_stats_latest_hash', N'U') IS NULL
BEGIN
    CREATE TABLE
        collect.procedure_stats_latest_hash
    (
        database_name sysname NOT NULL,
        object_id integer NOT NULL,
        plan_handle varbinary(64) NOT NULL,
        row_hash binary(32) NOT NULL,
        last_seen datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        CONSTRAINT
            PK_procedure_stats_latest_hash
        PRIMARY KEY CLUSTERED
            (database_name, object_id, plan_handle)
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.procedure_stats_latest_hash';
END;
ELSE
BEGIN
    PRINT 'collect.procedure_stats_latest_hash already exists — skipping.';
END;
GO

IF OBJECT_ID(N'collect.query_store_data_latest_hash', N'U') IS NULL
BEGIN
    CREATE TABLE
        collect.query_store_data_latest_hash
    (
        database_name sysname NOT NULL,
        query_id bigint NOT NULL,
        plan_id bigint NOT NULL,
        row_hash binary(32) NOT NULL,
        last_seen datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        CONSTRAINT
            PK_query_store_data_latest_hash
        PRIMARY KEY CLUSTERED
            (database_name, query_id, plan_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.query_store_data_latest_hash';
END;
ELSE
BEGIN
    PRINT 'collect.query_store_data_latest_hash already exists — skipping.';
END;
GO
