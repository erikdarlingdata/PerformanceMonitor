/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

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
Views that format query plans from the collection tables
These provide the formatting logic that was previously in computed columns
*/

/*
View for collect.query_stats with formatted query plans
*/
CREATE OR ALTER VIEW
    report.query_stats_with_formatted_plans
AS
SELECT
    *,
    query_plan_formatted =
        CASE
            WHEN TRY_CAST(qs.query_plan_text AS xml) IS NOT NULL
            THEN TRY_CAST(qs.query_plan_text AS xml)
            WHEN TRY_CAST(qs.query_plan_text AS xml) IS NULL
            THEN
                (
                    SELECT
                        [processing-instruction(query_plan)] =
                            N'-- ' + NCHAR(13) + NCHAR(10) +
                            N'-- This is a huge query plan.' + NCHAR(13) + NCHAR(10) +
                            N'-- Remove the headers and footers, save it as a .sqlplan file, and re-open it.' + NCHAR(13) + NCHAR(10) +
                            NCHAR(13) + NCHAR(10) +
                            REPLACE(qs.query_plan_text, N'<RelOp', NCHAR(13) + NCHAR(10) + N'<RelOp') +
                            NCHAR(13) + NCHAR(10) COLLATE Latin1_General_Bin2
                    FOR XML
                        PATH(N''),
                        TYPE
                )
        END
FROM collect.query_stats AS qs;
GO


/*
View for collect.procedure_stats with formatted query plans
*/
CREATE OR ALTER VIEW
    report.procedure_stats_with_formatted_plans
AS
SELECT
    *,
    query_plan_formatted =
        CASE
            WHEN TRY_CAST(ps.query_plan_text AS xml) IS NOT NULL
            THEN TRY_CAST(ps.query_plan_text AS xml)
            WHEN TRY_CAST(ps.query_plan_text AS xml) IS NULL
            THEN
                (
                    SELECT
                        [processing-instruction(query_plan)] =
                            N'-- ' + NCHAR(13) + NCHAR(10) +
                            N'-- This is a huge query plan.' + NCHAR(13) + NCHAR(10) +
                            N'-- Remove the headers and footers, save it as a .sqlplan file, and re-open it.' + NCHAR(13) + NCHAR(10) +
                            NCHAR(13) + NCHAR(10) +
                            REPLACE(ps.query_plan_text, N'<RelOp', NCHAR(13) + NCHAR(10) + N'<RelOp') +
                            NCHAR(13) + NCHAR(10) COLLATE Latin1_General_Bin2
                    FOR XML
                        PATH(N''),
                        TYPE
                )
        END
FROM collect.procedure_stats AS ps;
GO


/*
View for collect.query_store_data with formatted query plans
*/
CREATE OR ALTER VIEW
    report.query_store_stats_with_formatted_plans
AS
SELECT
    qsd.*,
    query_plan_formatted =
        CASE
            WHEN TRY_CAST(qsd.query_plan_text AS xml) IS NOT NULL
            THEN TRY_CAST(qsd.query_plan_text AS xml)
            WHEN TRY_CAST(qsd.query_plan_text AS xml) IS NULL
            THEN
                (
                    SELECT
                        [processing-instruction(query_plan)] =
                            N'-- ' + NCHAR(13) + NCHAR(10) +
                            N'-- This is a huge query plan.' + NCHAR(13) + NCHAR(10) +
                            N'-- Remove the headers and footers, save it as a .sqlplan file, and re-open it.' + NCHAR(13) + NCHAR(10) +
                            NCHAR(13) + NCHAR(10) +
                            REPLACE(qsd.query_plan_text, N'<RelOp', NCHAR(13) + NCHAR(10) + N'<RelOp') +
                            NCHAR(13) + NCHAR(10) COLLATE Latin1_General_Bin2
                    FOR XML
                        PATH(N''),
                        TYPE
                )
        END
FROM collect.query_store_data AS qsd
GO

/*
NOTE: query_snapshots views are auto-managed by collect.query_snapshots_create_views
      See report.query_snapshots and report.query_snapshots_blocking views
*/


/*
Expensive Queries Today - Top resource consumers from all sources
Combines query_stats (ad-hoc), procedure_stats (procs/triggers/functions), and query_store_data
Properly grouped by query to avoid duplicates
*/
CREATE OR ALTER VIEW
    report.expensive_queries_today
AS
WITH
    all_queries AS
(
    /*Ad-hoc queries from query_stats - grouped by query_hash*/
    SELECT
        qs.*
    FROM
    (
        SELECT TOP (20)
            source = N'Query Stats',
            database_name = qs.database_name,
            object_identifier = CONVERT(nvarchar(20), qs.query_hash, 1),
            object_name =
                CASE qs.object_type
                     WHEN 'STATEMENT'
                     THEN 'Adhoc'
                     ELSE QUOTENAME(qs.schema_name) + N'.' + QUOTENAME(qs.object_name)
                END,
            first_execution_time = MIN(qs.creation_time),
            last_execution_time = MAX(qs.last_execution_time),
            execution_count = SUM(qs.execution_count),
            total_worker_time = SUM(qs.total_worker_time),
            total_elapsed_time = SUM(qs.total_elapsed_time),
            total_logical_reads = SUM(qs.total_logical_reads),
            total_logical_writes = SUM(qs.total_logical_writes),
            total_physical_reads = SUM(qs.total_physical_reads),
            max_grant_kb = MAX(qs.max_grant_kb),
            query_text_sample = CONVERT(nvarchar(4000), MAX(qs.query_text)),
            query_plan_xml = MAX(qs.query_plan_text)
        FROM collect.query_stats AS qs
        GROUP BY
            qs.database_name,
            qs.query_hash,
            CASE qs.object_type
                 WHEN 'STATEMENT'
                 THEN 'Adhoc'
                 ELSE QUOTENAME(qs.schema_name) +
                      N'.' +
                      QUOTENAME(qs.object_name)
            END
        ORDER BY
            SUM(qs.total_worker_time) / SUM(qs.execution_count) DESC
    ) AS qs

    UNION ALL

    /*Stored procedures, triggers, functions from procedure_stats - grouped by object*/
    SELECT
        ps.*
    FROM
    (
        SELECT TOP (20)
            source =
                CASE
                    WHEN ps.object_type = N'STORED_PROCEDURE' THEN N'Stored Procedure'
                    WHEN ps.object_type = N'SQL_TRIGGER' THEN N'Trigger'
                    WHEN ps.object_type = N'SQL_SCALAR_FUNCTION' THEN N'Scalar Function'
                    WHEN ps.object_type = N'SQL_TABLE_VALUED_FUNCTION' THEN N'Table Function'
                    ELSE N'Procedure'
                END,
            database_name = ps.database_name,
            object_identifier = CONVERT(nvarchar(20), MAX(ps.object_id)),
            object_name =
                QUOTENAME(ps.schema_name) +
                N'.' +
                QUOTENAME(ps.object_name),
            first_execution_time = MIN(ps.cached_time),
            last_execution_time = MAX(ps.last_execution_time),
            execution_count = SUM(ps.execution_count),
            total_worker_time = SUM(ps.total_worker_time),
            total_elapsed_time = SUM(ps.total_elapsed_time),
            total_logical_reads = SUM(ps.total_logical_reads),
            total_logical_writes = SUM(ps.total_logical_writes),
            total_physical_reads = SUM(ps.total_physical_reads),
            max_grant_kb = NULL,
            query_text_sample =
                QUOTENAME(ps.database_name) +
                N'.' +
                QUOTENAME(ps.schema_name) +
                N'.' +
                QUOTENAME(ps.object_name),
            query_plan_xml = MAX(ps.query_plan_text)
        FROM collect.procedure_stats AS ps
        GROUP BY
            ps.database_name,
            ps.schema_name,
            ps.object_name,
            ps.object_type
        ORDER BY
            SUM(ps.total_worker_time) / SUM(ps.execution_count) DESC
    ) AS ps

    UNION ALL

    /*Query Store data - grouped by query_id*/
    SELECT DISTINCT
        qsd.source,
        qsd.database_name,
        qsd.object_identifier,
        qsd.object_name,
        qsd.first_execution_time,
        qsd.last_execution_time,
        qsd.execution_count,
        qsd.total_worker_time,
        qsd.total_elapsed_time,
        qsd.total_logical_reads,
        qsd.total_logical_writes,
        qsd.total_physical_reads,
        max_grant_kb = qsd.max_query_max_used_memory,
        query_text_sample = CONVERT(nvarchar(4000), qsd2.query_sql_text),
        query_plan_xml = qsd2.query_plan_text
    FROM
    (
        SELECT TOP (20)
            source = N'Query Store',
            database_name = qsd.database_name,
            object_identifier = CONVERT(nvarchar(20), qsd.query_id),
            qsd.query_id,
            object_name = ISNULL(qsd.module_name, N'Ad Hoc'),
            first_execution_time = MIN(qsd.server_first_execution_time),
            last_execution_time = MAX(qsd.server_last_execution_time),
            execution_count = SUM(qsd.count_executions),
            /*All sources store time in microseconds*/
            total_worker_time = SUM(qsd.avg_cpu_time * qsd.count_executions),
            total_elapsed_time = SUM(qsd.avg_duration * qsd.count_executions),
            total_logical_reads = SUM(qsd.avg_logical_io_reads * qsd.count_executions),
            total_logical_writes = SUM(qsd.avg_logical_io_writes * qsd.count_executions),
            total_physical_reads = SUM(qsd.avg_physical_io_reads * qsd.count_executions),
            max_query_max_used_memory = MAX(qsd.max_query_max_used_memory)
        FROM collect.query_store_data AS qsd
        GROUP BY
            qsd.database_name,
            qsd.query_id,
            qsd.module_name
        ORDER BY
            SUM(qsd.avg_cpu_time) DESC
    ) AS qsd
    JOIN collect.query_store_data AS qsd2
      ON  qsd2.database_name = qsd.database_name
      AND qsd2.query_id = qsd.query_id
      AND qsd2.server_first_execution_time = qsd.first_execution_time
      AND qsd2.server_last_execution_time = qsd.last_execution_time
)
SELECT TOP (20)
    source,
    database_name,
    object_identifier,
    object_name,
    first_execution_time,
    last_execution_time,
    execution_count,
    total_worker_time_sec = total_worker_time / 1000000.0,
    avg_worker_time_ms = total_worker_time / 1000.0 / NULLIF(execution_count, 0),
    total_elapsed_time_sec = total_elapsed_time / 1000000.0,
    avg_elapsed_time_ms = total_elapsed_time / 1000.0 / NULLIF(execution_count, 0),
    total_logical_reads,
    avg_logical_reads = total_logical_reads / NULLIF(execution_count, 0),
    total_logical_writes,
    avg_logical_writes = total_logical_writes / NULLIF(execution_count, 0),
    total_physical_reads,
    avg_physical_reads = total_physical_reads / NULLIF(execution_count, 0),
    max_grant_mb = max_grant_kb / 1024.0,
    query_text_sample,
    query_plan_xml
FROM all_queries
ORDER BY
    avg_worker_time_ms DESC;
GO


/*
Query Stats Summary - Aggregated ad-hoc query stats grouped by query_hash
Used by Dashboard Query Stats tab (main grid)
Drill down uses raw collect.query_stats data
*/
CREATE OR ALTER VIEW
    report.query_stats_summary
AS
SELECT
    database_name = qs.database_name,
    query_hash = CONVERT(nvarchar(20), qs.query_hash, 1),
    object_type = MAX(qs.object_type),
    object_name =
        CASE MAX(qs.object_type)
            WHEN 'STATEMENT'
            THEN N'Adhoc'
            ELSE QUOTENAME(MAX(qs.schema_name)) + N'.' + QUOTENAME(MAX(qs.object_name))
        END,
    first_execution_time = MIN(qs.creation_time),
    last_execution_time = MAX(qs.last_execution_time),
    execution_count = MAX(qs.execution_count),
    total_worker_time = MAX(qs.total_worker_time),
    avg_worker_time_ms = MAX(qs.total_worker_time) / 1000.0 / NULLIF(MAX(qs.execution_count), 0),
    min_worker_time_ms = MIN(qs.min_worker_time) / 1000.0,
    max_worker_time_ms = MAX(qs.max_worker_time) / 1000.0,
    total_elapsed_time = MAX(qs.total_elapsed_time),
    avg_elapsed_time_ms = MAX(qs.total_elapsed_time) / 1000.0 / NULLIF(MAX(qs.execution_count), 0),
    min_elapsed_time_ms = MIN(qs.min_elapsed_time) / 1000.0,
    max_elapsed_time_ms = MAX(qs.max_elapsed_time) / 1000.0,
    total_logical_reads = MAX(qs.total_logical_reads),
    avg_logical_reads = MAX(qs.total_logical_reads) / NULLIF(MAX(qs.execution_count), 0),
    total_logical_writes = MAX(qs.total_logical_writes),
    avg_logical_writes = MAX(qs.total_logical_writes) / NULLIF(MAX(qs.execution_count), 0),
    total_physical_reads = MAX(qs.total_physical_reads),
    avg_physical_reads = MAX(qs.total_physical_reads) / NULLIF(MAX(qs.execution_count), 0),
    total_rows = MAX(qs.total_rows),
    avg_rows = MAX(qs.total_rows) / NULLIF(MAX(qs.execution_count), 0),
    min_rows = MIN(qs.min_rows),
    max_rows = MAX(qs.max_rows),
    min_dop = MIN(qs.min_dop),
    max_dop = MAX(qs.max_dop),
    min_grant_kb = MIN(qs.min_grant_kb),
    max_grant_kb = MAX(qs.max_grant_kb),
    total_spills = MAX(qs.total_spills),
    min_spills = MIN(qs.min_spills),
    max_spills = MAX(qs.max_spills),
    query_text = CONVERT(nvarchar(max), MAX(qs.query_text)),
    query_plan_xml = MAX(qs.query_plan_text),
    query_plan_hash = CONVERT(nvarchar(20), MAX(qs.query_plan_hash), 1),
    sql_handle = CONVERT(nvarchar(130), MAX(qs.sql_handle), 1),
    plan_handle = CONVERT(nvarchar(130), MAX(qs.plan_handle), 1)
FROM collect.query_stats AS qs
GROUP BY
    qs.database_name,
    qs.query_hash;
GO


/*
Procedure Stats Summary - Aggregated procedure stats grouped by object
Used by Dashboard Procedure Stats tab (main grid)
Drill down uses raw collect.procedure_stats data
*/
CREATE OR ALTER VIEW
    report.procedure_stats_summary
AS
SELECT
    database_name = ps.database_name,
    object_id = MAX(ps.object_id),
    object_name = QUOTENAME(ps.schema_name) + N'.' + QUOTENAME(ps.object_name),
    schema_name = ps.schema_name,
    procedure_name = ps.object_name,
    object_type = MAX(ps.object_type),
    type_desc = MAX(ps.type_desc),
    first_cached_time = MIN(ps.cached_time),
    last_execution_time = MAX(ps.last_execution_time),
    execution_count = MAX(ps.execution_count),
    total_worker_time = MAX(ps.total_worker_time),
    avg_worker_time_ms = MAX(ps.total_worker_time) / 1000.0 / NULLIF(MAX(ps.execution_count), 0),
    min_worker_time_ms = MIN(ps.min_worker_time) / 1000.0,
    max_worker_time_ms = MAX(ps.max_worker_time) / 1000.0,
    total_elapsed_time = MAX(ps.total_elapsed_time),
    avg_elapsed_time_ms = MAX(ps.total_elapsed_time) / 1000.0 / NULLIF(MAX(ps.execution_count), 0),
    min_elapsed_time_ms = MIN(ps.min_elapsed_time) / 1000.0,
    max_elapsed_time_ms = MAX(ps.max_elapsed_time) / 1000.0,
    total_logical_reads = MAX(ps.total_logical_reads),
    avg_logical_reads = MAX(ps.total_logical_reads) / NULLIF(MAX(ps.execution_count), 0),
    min_logical_reads = MIN(ps.min_logical_reads),
    max_logical_reads = MAX(ps.max_logical_reads),
    total_logical_writes = MAX(ps.total_logical_writes),
    avg_logical_writes = MAX(ps.total_logical_writes) / NULLIF(MAX(ps.execution_count), 0),
    min_logical_writes = MIN(ps.min_logical_writes),
    max_logical_writes = MAX(ps.max_logical_writes),
    total_physical_reads = MAX(ps.total_physical_reads),
    avg_physical_reads = MAX(ps.total_physical_reads) / NULLIF(MAX(ps.execution_count), 0),
    min_physical_reads = MIN(ps.min_physical_reads),
    max_physical_reads = MAX(ps.max_physical_reads),
    total_spills = MAX(ps.total_spills),
    avg_spills = MAX(ps.total_spills) / NULLIF(MAX(ps.execution_count), 0),
    min_spills = MIN(ps.min_spills),
    max_spills = MAX(ps.max_spills),
    query_plan_xml = MAX(ps.query_plan_text),
    sql_handle = CONVERT(nvarchar(130), MAX(ps.sql_handle), 1),
    plan_handle = CONVERT(nvarchar(130), MAX(ps.plan_handle), 1)
FROM collect.procedure_stats AS ps
GROUP BY
    ps.database_name,
    ps.schema_name,
    ps.object_name;
GO


/*
Query Store Summary - Aggregated Query Store data grouped by query_id
Used by Dashboard Query Store tab (main grid)
Drill down uses raw collect.query_store_data data (includes plan_id)
*/
CREATE OR ALTER VIEW
    report.query_store_summary
AS
SELECT
    database_name = qsd.database_name,
    query_id = qsd.query_id,
    execution_type_desc = MAX(qsd.execution_type_desc),
    module_name = MAX(qsd.module_name),
    first_execution_time = MIN(qsd.server_first_execution_time),
    last_execution_time = MAX(qsd.server_last_execution_time),
    execution_count = SUM(qsd.count_executions),
    plan_count = COUNT_BIG(DISTINCT qsd.plan_id),
    /*Duration in microseconds, convert to ms for display*/
    avg_duration_ms = SUM(qsd.avg_duration * qsd.count_executions) / 1000.0 / NULLIF(SUM(qsd.count_executions), 0),
    min_duration_ms = MIN(qsd.min_duration) / 1000.0,
    max_duration_ms = MAX(qsd.max_duration) / 1000.0,
    /*CPU time in microseconds, convert to ms*/
    avg_cpu_time_ms = SUM(qsd.avg_cpu_time * qsd.count_executions) / 1000.0 / NULLIF(SUM(qsd.count_executions), 0),
    min_cpu_time_ms = MIN(qsd.min_cpu_time) / 1000.0,
    max_cpu_time_ms = MAX(qsd.max_cpu_time) / 1000.0,
    /*Logical reads*/
    avg_logical_reads = SUM(qsd.avg_logical_io_reads * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
    min_logical_reads = MIN(qsd.min_logical_io_reads),
    max_logical_reads = MAX(qsd.max_logical_io_reads),
    /*Logical writes*/
    avg_logical_writes = SUM(qsd.avg_logical_io_writes * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
    min_logical_writes = MIN(qsd.min_logical_io_writes),
    max_logical_writes = MAX(qsd.max_logical_io_writes),
    /*Physical reads*/
    avg_physical_reads = SUM(qsd.avg_physical_io_reads * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
    min_physical_reads = MIN(qsd.min_physical_io_reads),
    max_physical_reads = MAX(qsd.max_physical_io_reads),
    /*DOP*/
    min_dop = MIN(qsd.min_dop),
    max_dop = MAX(qsd.max_dop),
    /*Memory (8KB pages)*/
    avg_memory_pages = SUM(qsd.avg_query_max_used_memory * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
    min_memory_pages = MIN(qsd.min_query_max_used_memory),
    max_memory_pages = MAX(qsd.max_query_max_used_memory),
    /*Rowcount*/
    avg_rowcount = SUM(qsd.avg_rowcount * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
    min_rowcount = MIN(qsd.min_rowcount),
    max_rowcount = MAX(qsd.max_rowcount),
    /*Tempdb (8KB pages) - may be NULL on SQL 2016*/
    avg_tempdb_pages = SUM(ISNULL(qsd.avg_tempdb_space_used, 0) * qsd.count_executions) / NULLIF(SUM(qsd.count_executions), 0),
    min_tempdb_pages = MIN(qsd.min_tempdb_space_used),
    max_tempdb_pages = MAX(qsd.max_tempdb_space_used),
    /*Plan info - take from most recent collection*/
    plan_type = MAX(qsd.plan_type),
    is_forced_plan = MAX(CONVERT(tinyint, qsd.is_forced_plan)),
    compatibility_level = MAX(qsd.compatibility_level),
    /*Query text and plan - take sample*/
    query_sql_text = CONVERT(nvarchar(max), MAX(qsd.query_sql_text)),
    query_plan_xml = MAX(qsd.query_plan_text),
    query_plan_hash = CONVERT(nvarchar(20), MAX(qsd.query_plan_hash), 1)
FROM collect.query_store_data AS qsd
GROUP BY
    qsd.database_name,
    qsd.query_id;
GO


PRINT 'Query plan formatting views created successfully';
PRINT 'Summary views created: report.query_stats_summary, report.procedure_stats_summary, report.query_store_summary';
GO
