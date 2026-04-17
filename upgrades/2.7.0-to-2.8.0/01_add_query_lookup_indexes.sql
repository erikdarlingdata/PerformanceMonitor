/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.7.0 to 2.8.0

Add nonclustered indexes on collect.query_stats, collect.procedure_stats, and
collect.query_store_data to support OUTER APPLY lookups in the Dashboard's
grid queries. Without these, the optimizer builds an Eager Index Spool over
the entire table to service the lookups, which can take minutes on large
installations (see #835).

ONLINE = ON is only supported on Enterprise/Developer/Azure editions. The
options string is built dynamically based on SERVERPROPERTY('EngineEdition').
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

DECLARE @online_option nvarchar(20) =
    CASE
        WHEN CAST(SERVERPROPERTY(N'EngineEdition') AS integer) IN (3, 5, 8)
        THEN N', ONLINE = ON'
        ELSE N''
    END;

DECLARE @index_sql nvarchar(max);

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'collect.query_stats')
    AND   name = N'IX_query_stats_hash_lookup'
)
BEGIN
    SET @index_sql = N'
    CREATE NONCLUSTERED INDEX
        IX_query_stats_hash_lookup
    ON collect.query_stats
        (query_hash, database_name, collection_time DESC)
    WITH
        (SORT_IN_TEMPDB = ON, DATA_COMPRESSION = PAGE' + @online_option + N');';
    EXEC sys.sp_executesql @index_sql;
    PRINT 'Created collect.query_stats.IX_query_stats_hash_lookup index';
END;

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'collect.procedure_stats')
    AND   name = N'IX_procedure_stats_name_lookup'
)
BEGIN
    SET @index_sql = N'
    CREATE NONCLUSTERED INDEX
        IX_procedure_stats_name_lookup
    ON collect.procedure_stats
        (database_name, schema_name, object_name, collection_time DESC)
    WITH
        (SORT_IN_TEMPDB = ON, DATA_COMPRESSION = PAGE' + @online_option + N');';
    EXEC sys.sp_executesql @index_sql;
    PRINT 'Created collect.procedure_stats.IX_procedure_stats_name_lookup index';
END;

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'collect.query_store_data')
    AND   name = N'IX_query_store_data_id_lookup'
)
BEGIN
    SET @index_sql = N'
    CREATE NONCLUSTERED INDEX
        IX_query_store_data_id_lookup
    ON collect.query_store_data
        (database_name, query_id, collection_time DESC)
    WITH
        (SORT_IN_TEMPDB = ON, DATA_COMPRESSION = PAGE' + @online_option + N');';
    EXEC sys.sp_executesql @index_sql;
    PRINT 'Created collect.query_store_data.IX_query_store_data_id_lookup index';
END;
