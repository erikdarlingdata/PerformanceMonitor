/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Point-in-time snapshot of currently running queries. Extracted verbatim from Lite's
/// RemoteCollectorService.QuerySnapshots.cs, including: the CDC-capture detection (job-id parse
/// from the Agent program_name with the text-match fallback when msdb.dbo.cdc_jobs is
/// unreadable), the live-query-plan gate (2016 SP1+/MI; disabled on Azure SQL DB where DB-scoped
/// logins trip error 300 — #857), the Azure #req-staging variant that keeps the sql_text/plan
/// DMFs away from master-scoped rows (#857), the #1286 memory-grant/tempdb/transaction triage
/// columns, and the exclusion clause injected ahead of the ORDER BY.
/// <see cref="BuildSnapshotQuery"/> is public because Lite's live-snapshot button builds the
/// same SQL — the templates are the single source for both paths.
/// </summary>
public sealed class QuerySnapshotsCollector : CollectorDefinitionBase<QuerySnapshotsCollector.Row>
{
    public static QuerySnapshotsCollector Instance { get; } = new();

    private QuerySnapshotsCollector()
    {
    }

    public sealed class Row
    {
        public int SessionId { get; set; }
        public string? DatabaseName { get; set; }
        public string? ElapsedTimeFormatted { get; set; }
        public string? QueryText { get; set; }
        public string? QueryPlan { get; set; }
        public string? LiveQueryPlan { get; set; }
        public string? Status { get; set; }
        public int BlockingSessionId { get; set; }
        public string? WaitType { get; set; }
        public long WaitTimeMs { get; set; }
        public string? WaitResource { get; set; }
        public long CpuTimeMs { get; set; }
        public long TotalElapsedTimeMs { get; set; }
        public long Reads { get; set; }
        public long Writes { get; set; }
        public long LogicalReads { get; set; }
        public decimal GrantedQueryMemoryGb { get; set; }
        public string? TransactionIsolationLevel { get; set; }
        public int Dop { get; set; }
        public int ParallelWorkerCount { get; set; }
        public string? LoginName { get; set; }
        public string? HostName { get; set; }
        public string? ProgramName { get; set; }
        public int OpenTransactionCount { get; set; }
        public decimal PercentComplete { get; set; }
        public bool IsCdcCapture { get; set; }
        public string? QueryHash { get; set; }
        public double? RequestedMemoryMb { get; set; }
        public double? UsedMemoryMb { get; set; }
        public double? MaxUsedMemoryMb { get; set; }
        public double? TempdbCurrentMb { get; set; }
        public double? TempdbAllocationsMb { get; set; }
        public double? TranLogUsedMb { get; set; }
        public DateTime? TranStartTime { get; set; }
        public int RequestId { get; set; }
    }

    private const string SnapshotsBase = """

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 1000;

DECLARE @cdc_capture_jobs TABLE (job_id uniqueidentifier PRIMARY KEY);
DECLARE @cdc_readable bit = 0;
BEGIN TRY
    /* Existence pre-guard, not just the TRY/CATCH: msdb.dbo.cdc_jobs is created lazily on first CDC
       configuration, so on a never-CDC server the probe raised error 208 every cycle. The CATCH absorbed
       the failure (collection proceeded on the text fallback), but TRY/CATCH suppresses the failure, not
       the server-side error_reported EVENT - fleet error monitoring watching those events saw a
       once-per-cycle "Invalid object name" from every no-CDC server. OBJECT_ID also returns NULL when the
       table exists but the login lacks metadata visibility; that lands on the same text fallback the
       CATCH would have. The TRY/CATCH stays as the belt for denied SELECTs and races. */
    IF OBJECT_ID(N'msdb.dbo.cdc_jobs') IS NOT NULL
    BEGIN
        INSERT @cdc_capture_jobs (job_id)
        EXEC sys.sp_executesql N'SELECT cj.job_id FROM msdb.dbo.cdc_jobs AS cj WHERE cj.job_type = N''capture'';';
        SET @cdc_readable = 1;
    END;
END TRY
BEGIN CATCH
    SET @cdc_readable = 0;
END CATCH;

SELECT /* PerformanceMonitorLite */
    der.session_id,
    database_name = d.name,
    elapsed_time_formatted =
        CASE
            WHEN der.total_elapsed_time < 0
            THEN '00 00:00:00.000'
            ELSE RIGHT(REPLICATE('0', 2) + CONVERT(varchar(10), der.total_elapsed_time / 86400000), 2) +
                 ' ' + RIGHT(CONVERT(varchar(30), DATEADD(second, der.total_elapsed_time / 1000, 0), 120), 9) +
                 '.' + RIGHT('000' + CONVERT(varchar(3), der.total_elapsed_time % 1000), 3)
        END,
    query_text = SUBSTRING(dest.text, (der.statement_start_offset / 2) + 1,
        ((CASE der.statement_end_offset WHEN -1 THEN DATALENGTH(dest.text)
          ELSE der.statement_end_offset END - der.statement_start_offset) / 2) + 1),
    query_plan = TRY_CAST(deqp.query_plan AS nvarchar(max)),
    {0}
    der.status,
    der.blocking_session_id,
    der.wait_type,
    wait_time_ms = CONVERT(bigint, der.wait_time),
    der.wait_resource,
    cpu_time_ms = CONVERT(bigint, der.cpu_time),
    total_elapsed_time_ms = CONVERT(bigint, der.total_elapsed_time),
    der.reads,
    der.writes,
    der.logical_reads,
    granted_query_memory_gb = CONVERT(decimal(38, 2), (der.granted_query_memory / 128. / 1024.)),
    transaction_isolation_level =
        CASE der.transaction_isolation_level
            WHEN 0 THEN 'Unspecified'
            WHEN 1 THEN 'Read Uncommitted'
            WHEN 2 THEN 'Read Committed'
            WHEN 3 THEN 'Repeatable Read'
            WHEN 4 THEN 'Serializable'
            WHEN 5 THEN 'Snapshot'
            ELSE '???'
        END,
    der.dop,
    der.parallel_worker_count,
    des.login_name,
    des.host_name,
    des.program_name,
    des.open_transaction_count,
    der.percent_complete,
    is_cdc_capture =
        CONVERT(bit,
            CASE
                WHEN @cdc_readable = 1
                     AND des.program_name LIKE N'SQLAgent - TSQL JobStep (Job 0x%'
                     AND TRY_CONVERT(uniqueidentifier, TRY_CONVERT(binary(16), SUBSTRING(des.program_name, 32, 32), 2))
                         IN (SELECT j.job_id FROM @cdc_capture_jobs AS j)
                THEN 1
                WHEN @cdc_readable = 0
                     AND dest.text IS NOT NULL
                     AND (dest.text LIKE N'%sp_MScdc_capture_job%' OR dest.text LIKE N'%sp_cdc_scan%')
                THEN 1
                ELSE 0
            END),
    query_hash = CONVERT(varchar(18), der.query_hash, 1), /* #1140 long-running-query dedup key */
    requested_memory_mb = CONVERT(decimal(38, 2), deqmg.requested_memory_kb / 1024.),
    used_memory_mb = CONVERT(decimal(38, 2), deqmg.used_memory_kb / 1024.),
    max_used_memory_mb = CONVERT(decimal(38, 2), deqmg.max_used_memory_kb / 1024.),
    tempdb_current_mb = tsu.tempdb_current_mb,
    tempdb_allocations_mb = tsu.tempdb_allocations_mb,
    tran_log_used_mb = trn.tran_log_used_mb,
    tran_start_time = trn.tran_start_time,
    der.request_id
FROM sys.dm_exec_requests AS der
JOIN sys.dm_exec_sessions AS des
    ON des.session_id = der.session_id
OUTER APPLY sys.dm_exec_sql_text(COALESCE(der.sql_handle, der.plan_handle)) AS dest
OUTER APPLY sys.dm_exec_text_query_plan(der.plan_handle, der.statement_start_offset, der.statement_end_offset) AS deqp
LEFT JOIN sys.databases AS d
  ON d.database_id = der.database_id
LEFT JOIN sys.dm_exec_query_memory_grants AS deqmg
  ON  deqmg.session_id = der.session_id
  AND deqmg.request_id = der.request_id
OUTER APPLY
(
    SELECT
        tempdb_current_mb =
            CONVERT(decimal(38, 2),
                CASE
                    WHEN SUM(x.alloc) - SUM(x.dealloc) < 0
                    THEN 0.
                    ELSE (SUM(x.alloc) - SUM(x.dealloc)) * 8. / 1024.
                END),
        tempdb_allocations_mb = CONVERT(decimal(38, 2), SUM(x.alloc) * 8. / 1024.)
    FROM
    (
        SELECT
            alloc = dssu.user_objects_alloc_page_count + dssu.internal_objects_alloc_page_count,
            dealloc = dssu.user_objects_dealloc_page_count + dssu.internal_objects_dealloc_page_count
        FROM sys.dm_db_session_space_usage AS dssu
        WHERE dssu.session_id = der.session_id

        UNION ALL

        SELECT
            dtsu.user_objects_alloc_page_count + dtsu.internal_objects_alloc_page_count,
            dtsu.user_objects_dealloc_page_count + dtsu.internal_objects_dealloc_page_count
        FROM sys.dm_db_task_space_usage AS dtsu
        WHERE dtsu.session_id = der.session_id
        AND   dtsu.request_id = der.request_id
    ) AS x
) AS tsu
OUTER APPLY
(
    SELECT
        tran_log_used_mb = CONVERT(decimal(38, 2), SUM(ddt.database_transaction_log_bytes_used) / 1048576.),
        tran_start_time = MIN(dat.transaction_begin_time)
    FROM sys.dm_tran_session_transactions AS dst
    JOIN sys.dm_tran_active_transactions AS dat
      ON dat.transaction_id = dst.transaction_id
    LEFT JOIN sys.dm_tran_database_transactions AS ddt
      ON ddt.transaction_id = dst.transaction_id
    WHERE dst.session_id = der.session_id
) AS trn
{1}
WHERE der.session_id <> @@SPID
AND   der.session_id >= 50
AND   dest.text IS NOT NULL
AND   der.database_id <> ISNULL(DB_ID(N'PerformanceMonitor'), 0)
ORDER BY der.cpu_time DESC, der.parallel_worker_count DESC
OPTION(MAXDOP 1, RECOMPILE);
""";

    private static readonly CompositeFormat s_snapshotsBaseFormat = CompositeFormat.Parse(SnapshotsBase);

    /*
     * On Azure SQL Database with a contained / DB-scoped login (e.g. D365FO),
     * the OUTER APPLY to sys.dm_exec_sql_text / sys.dm_exec_text_query_plan
     * will be evaluated against master-scoped rows from sys.dm_exec_requests
     * before the WHERE predicate applies, tripping error 300 (VIEW SERVER
     * PERFORMANCE STATE denied). Materialising the filtered request rows
     * into a #temp table first guarantees the DMFs only see handles from
     * the current database. See #857.
     */
    private const string SnapshotsAzureBase = """

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 1000;

IF OBJECT_ID(N'tempdb..#req') IS NOT NULL DROP TABLE #req;

SELECT
    der.session_id,
    der.database_id,
    der.sql_handle,
    der.plan_handle,
    der.statement_start_offset,
    der.statement_end_offset,
    der.status,
    der.blocking_session_id,
    der.wait_type,
    der.wait_time,
    der.wait_resource,
    der.cpu_time,
    der.total_elapsed_time,
    der.reads,
    der.writes,
    der.logical_reads,
    der.granted_query_memory,
    der.transaction_isolation_level,
    der.dop,
    der.parallel_worker_count,
    der.percent_complete,
    der.query_hash,
    der.request_id
INTO #req
FROM sys.dm_exec_requests AS der
WHERE der.session_id <> @@SPID
AND   der.session_id >= 50
AND   der.database_id = DB_ID()
AND   der.database_id <> ISNULL(DB_ID(N'PerformanceMonitor'), 0);

SELECT /* PerformanceMonitorLite */
    der.session_id,
    database_name = d.name,
    elapsed_time_formatted =
        CASE
            WHEN der.total_elapsed_time < 0
            THEN '00 00:00:00.000'
            ELSE RIGHT(REPLICATE('0', 2) + CONVERT(varchar(10), der.total_elapsed_time / 86400000), 2) +
                 ' ' + RIGHT(CONVERT(varchar(30), DATEADD(second, der.total_elapsed_time / 1000, 0), 120), 9) +
                 '.' + RIGHT('000' + CONVERT(varchar(3), der.total_elapsed_time % 1000), 3)
        END,
    query_text = SUBSTRING(dest.text, (der.statement_start_offset / 2) + 1,
        ((CASE der.statement_end_offset WHEN -1 THEN DATALENGTH(dest.text)
          ELSE der.statement_end_offset END - der.statement_start_offset) / 2) + 1),
    query_plan = TRY_CAST(deqp.query_plan AS nvarchar(max)),
    {0}
    der.status,
    der.blocking_session_id,
    der.wait_type,
    wait_time_ms = CONVERT(bigint, der.wait_time),
    der.wait_resource,
    cpu_time_ms = CONVERT(bigint, der.cpu_time),
    total_elapsed_time_ms = CONVERT(bigint, der.total_elapsed_time),
    der.reads,
    der.writes,
    der.logical_reads,
    granted_query_memory_gb = CONVERT(decimal(38, 2), (der.granted_query_memory / 128. / 1024.)),
    transaction_isolation_level =
        CASE der.transaction_isolation_level
            WHEN 0 THEN 'Unspecified'
            WHEN 1 THEN 'Read Uncommitted'
            WHEN 2 THEN 'Read Committed'
            WHEN 3 THEN 'Repeatable Read'
            WHEN 4 THEN 'Serializable'
            WHEN 5 THEN 'Snapshot'
            ELSE '???'
        END,
    der.dop,
    der.parallel_worker_count,
    des.login_name,
    des.host_name,
    des.program_name,
    des.open_transaction_count,
    der.percent_complete,
    /* Azure SQL Database has no SQL Agent / msdb.dbo.cdc_jobs (CDC there is scheduler-based), so no capture job to exclude. */
    is_cdc_capture = CONVERT(bit, 0),
    query_hash = CONVERT(varchar(18), der.query_hash, 1), /* #1140 long-running-query dedup key */
    requested_memory_mb = CONVERT(decimal(38, 2), deqmg.requested_memory_kb / 1024.),
    used_memory_mb = CONVERT(decimal(38, 2), deqmg.used_memory_kb / 1024.),
    max_used_memory_mb = CONVERT(decimal(38, 2), deqmg.max_used_memory_kb / 1024.),
    tempdb_current_mb = tsu.tempdb_current_mb,
    tempdb_allocations_mb = tsu.tempdb_allocations_mb,
    tran_log_used_mb = trn.tran_log_used_mb,
    tran_start_time = trn.tran_start_time,
    der.request_id
FROM #req AS der
JOIN sys.dm_exec_sessions AS des
    ON des.session_id = der.session_id
OUTER APPLY sys.dm_exec_sql_text(COALESCE(der.sql_handle, der.plan_handle)) AS dest
OUTER APPLY sys.dm_exec_text_query_plan(der.plan_handle, der.statement_start_offset, der.statement_end_offset) AS deqp
LEFT JOIN sys.databases AS d
  ON d.database_id = der.database_id
LEFT JOIN sys.dm_exec_query_memory_grants AS deqmg
  ON  deqmg.session_id = der.session_id
  AND deqmg.request_id = der.request_id
OUTER APPLY
(
    SELECT
        tempdb_current_mb =
            CONVERT(decimal(38, 2),
                CASE
                    WHEN SUM(x.alloc) - SUM(x.dealloc) < 0
                    THEN 0.
                    ELSE (SUM(x.alloc) - SUM(x.dealloc)) * 8. / 1024.
                END),
        tempdb_allocations_mb = CONVERT(decimal(38, 2), SUM(x.alloc) * 8. / 1024.)
    FROM
    (
        SELECT
            alloc = dssu.user_objects_alloc_page_count + dssu.internal_objects_alloc_page_count,
            dealloc = dssu.user_objects_dealloc_page_count + dssu.internal_objects_dealloc_page_count
        FROM sys.dm_db_session_space_usage AS dssu
        WHERE dssu.session_id = der.session_id

        UNION ALL

        SELECT
            dtsu.user_objects_alloc_page_count + dtsu.internal_objects_alloc_page_count,
            dtsu.user_objects_dealloc_page_count + dtsu.internal_objects_dealloc_page_count
        FROM sys.dm_db_task_space_usage AS dtsu
        WHERE dtsu.session_id = der.session_id
        AND   dtsu.request_id = der.request_id
    ) AS x
) AS tsu
OUTER APPLY
(
    SELECT
        tran_log_used_mb = CONVERT(decimal(38, 2), SUM(ddt.database_transaction_log_bytes_used) / 1048576.),
        tran_start_time = MIN(dat.transaction_begin_time)
    FROM sys.dm_tran_session_transactions AS dst
    JOIN sys.dm_tran_active_transactions AS dat
      ON dat.transaction_id = dst.transaction_id
    LEFT JOIN sys.dm_tran_database_transactions AS ddt
      ON ddt.transaction_id = dst.transaction_id
    WHERE dst.session_id = der.session_id
) AS trn
{1}
WHERE dest.text IS NOT NULL
ORDER BY der.cpu_time DESC, der.parallel_worker_count DESC
OPTION(MAXDOP 1, RECOMPILE);

DROP TABLE #req;
""";

    private static readonly CompositeFormat s_snapshotsAzureBaseFormat = CompositeFormat.Parse(SnapshotsAzureBase);

    /// <summary>
    /// Builds the query snapshots SQL with or without live query plan support — the single
    /// source for both the scheduled collector and Lite's live snapshot button (#857 for the
    /// Azure #req staging).
    /// </summary>
    public static string BuildSnapshotQuery(bool supportsLiveQueryPlan, bool isAzureSqlDatabase)
    {
        var liveQueryPlanColumn = supportsLiveQueryPlan
            ? "live_query_plan = deqs.query_plan,"
            : "live_query_plan = CONVERT(xml, NULL),";
        var liveQueryPlanApply = supportsLiveQueryPlan
            ? "OUTER APPLY sys.dm_exec_query_statistics_xml(der.session_id) AS deqs"
            : "";
        var template = isAzureSqlDatabase ? s_snapshotsAzureBaseFormat : s_snapshotsBaseFormat;
        return string.Format(null, template, liveQueryPlanColumn, liveQueryPlanApply);
    }

    /// <summary>The live-plan gate: 2016 SP1+/version-unknown/MI; never on Azure SQL DB (#857).</summary>
    public static bool SupportsLiveQueryPlan(CollectorTargetInfo target)
        => !target.IsAzureSqlDb
           && (target.SqlMajorVersion >= 13 || target.SqlMajorVersion == 0 || target.IsAzureManagedInstance);

    public override string Name => "query_snapshots";

    public override string TargetTable => "query_snapshots";

    /// <summary>
    /// Both query variants open with <c>SET LOCK_TIMEOUT 1000</c>: a point-in-time snapshot sweep
    /// must never join a blocking chain, so after 1 second it yields instead. The data cost is
    /// near zero (the next sweep sees current state; nothing cumulative or watermarked is lost),
    /// which is why hosts record the resulting 1222 as a <c>YIELDED</c> row rather than an error —
    /// a 1222 here is evidence about the TARGET's lock contention, not a monitoring failure (#1805).
    /// </summary>
    public override bool YieldsOnLockTimeout => true;

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        var query = BuildSnapshotQuery(SupportsLiveQueryPlan(context.Target), context.Target.IsAzureSqlDb);

        /* Append the per-database exclusion filter to the WHERE clause. The base query joins
           sys.databases AS d, so we filter on d.name. When ExcludedDatabases is empty the
           clause is "" so nothing changes. */
        var (exclusionClause, exclusionParameters) = DatabaseExclusionFilter.Build(context.ExcludedDatabases, "d.name");
        if (!string.IsNullOrEmpty(exclusionClause))
        {
            /* Inject just before the OPTION clause so it lands inside the WHERE. */
            query = query.Replace(
                "ORDER BY der.cpu_time DESC",
                exclusionClause + "\nORDER BY der.cpu_time DESC",
                StringComparison.Ordinal);
        }

        return new CollectorQuery(query, exclusionParameters);
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("session_id", CollectorColumnType.Integer),
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("elapsed_time_formatted", CollectorColumnType.Varchar),
        new CollectorColumn("query_text", CollectorColumnType.Varchar),
        new CollectorColumn("query_plan", CollectorColumnType.Varchar),
        new CollectorColumn("live_query_plan", CollectorColumnType.Varchar),
        new CollectorColumn("status", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_session_id", CollectorColumnType.Integer),
        new CollectorColumn("wait_type", CollectorColumnType.Varchar),
        new CollectorColumn("wait_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("wait_resource", CollectorColumnType.Varchar),
        new CollectorColumn("cpu_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("total_elapsed_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("reads", CollectorColumnType.BigInt),
        new CollectorColumn("writes", CollectorColumnType.BigInt),
        new CollectorColumn("logical_reads", CollectorColumnType.BigInt),
        new CollectorColumn("granted_query_memory_gb", CollectorColumnType.Decimal, 18, 2),
        new CollectorColumn("transaction_isolation_level", CollectorColumnType.Varchar),
        new CollectorColumn("dop", CollectorColumnType.Integer),
        new CollectorColumn("parallel_worker_count", CollectorColumnType.Integer),
        new CollectorColumn("login_name", CollectorColumnType.Varchar),
        new CollectorColumn("host_name", CollectorColumnType.Varchar),
        new CollectorColumn("program_name", CollectorColumnType.Varchar),
        new CollectorColumn("open_transaction_count", CollectorColumnType.Integer),
        new CollectorColumn("percent_complete", CollectorColumnType.Decimal, 5, 2),
        new CollectorColumn("is_cdc_capture", CollectorColumnType.Boolean),
        new CollectorColumn("query_hash", CollectorColumnType.Varchar),
        new CollectorColumn("requested_memory_mb", CollectorColumnType.Double),
        new CollectorColumn("used_memory_mb", CollectorColumnType.Double),
        new CollectorColumn("max_used_memory_mb", CollectorColumnType.Double),
        new CollectorColumn("tempdb_current_mb", CollectorColumnType.Double),
        new CollectorColumn("tempdb_allocations_mb", CollectorColumnType.Double),
        new CollectorColumn("tran_log_used_mb", CollectorColumnType.Double),
        new CollectorColumn("tran_start_time", CollectorColumnType.Timestamp),
        new CollectorColumn("request_id", CollectorColumnType.Integer),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        /* #4348: the statement text and both plans are judged where they first enter a Row, so the stored rows and
           the live Active Queries read (which shares this ReadAsync) both carry the filtered values. */
        var scrub = context.BeginStatementScrub();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row
            {
                SessionId = Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture),
                DatabaseName = reader.IsDBNull(1) ? null : reader.GetString(1),
                ElapsedTimeFormatted = reader.IsDBNull(2) ? null : reader.GetString(2),
                QueryText = reader.IsDBNull(3) ? null : scrub.Text(reader.GetString(3)),
                QueryPlan = reader.IsDBNull(4) ? null : scrub.Xml(reader.GetString(4)),
                LiveQueryPlan = reader.IsDBNull(5) ? null : scrub.Xml(reader.GetValue(5)?.ToString()),
                Status = reader.IsDBNull(6) ? null : reader.GetString(6),
                BlockingSessionId = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture),
                /* Trimmed at the read (see WaitTypeName): this is the live Active Queries fetch's read too. */
                WaitType = reader.IsDBNull(8) ? null : WaitTypeName.Trim(reader.GetString(8)),
                WaitTimeMs = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9), CultureInfo.InvariantCulture),
                WaitResource = reader.IsDBNull(10) ? null : reader.GetString(10),
                CpuTimeMs = reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11), CultureInfo.InvariantCulture),
                TotalElapsedTimeMs = reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12), CultureInfo.InvariantCulture),
                Reads = reader.IsDBNull(13) ? 0L : Convert.ToInt64(reader.GetValue(13), CultureInfo.InvariantCulture),
                Writes = reader.IsDBNull(14) ? 0L : Convert.ToInt64(reader.GetValue(14), CultureInfo.InvariantCulture),
                LogicalReads = reader.IsDBNull(15) ? 0L : Convert.ToInt64(reader.GetValue(15), CultureInfo.InvariantCulture),
                GrantedQueryMemoryGb = reader.IsDBNull(16) ? 0m : reader.GetDecimal(16),
                TransactionIsolationLevel = reader.IsDBNull(17) ? null : reader.GetString(17),
                Dop = reader.IsDBNull(18) ? 0 : Convert.ToInt32(reader.GetValue(18), CultureInfo.InvariantCulture),
                ParallelWorkerCount = reader.IsDBNull(19) ? 0 : Convert.ToInt32(reader.GetValue(19), CultureInfo.InvariantCulture),
                LoginName = reader.IsDBNull(20) ? null : reader.GetString(20),
                HostName = reader.IsDBNull(21) ? null : reader.GetString(21),
                ProgramName = reader.IsDBNull(22) ? null : reader.GetString(22),
                OpenTransactionCount = reader.IsDBNull(23) ? 0 : Convert.ToInt32(reader.GetValue(23), CultureInfo.InvariantCulture),
                PercentComplete = reader.IsDBNull(24) ? 0m : Convert.ToDecimal(reader.GetValue(24), CultureInfo.InvariantCulture),
                IsCdcCapture = !reader.IsDBNull(25) && Convert.ToBoolean(reader.GetValue(25), CultureInfo.InvariantCulture),
                QueryHash = reader.IsDBNull(26) ? null : reader.GetString(26),
                RequestedMemoryMb = reader.IsDBNull(27) ? null : Convert.ToDouble(reader.GetValue(27), CultureInfo.InvariantCulture),
                UsedMemoryMb = reader.IsDBNull(28) ? null : Convert.ToDouble(reader.GetValue(28), CultureInfo.InvariantCulture),
                MaxUsedMemoryMb = reader.IsDBNull(29) ? null : Convert.ToDouble(reader.GetValue(29), CultureInfo.InvariantCulture),
                TempdbCurrentMb = reader.IsDBNull(30) ? null : Convert.ToDouble(reader.GetValue(30), CultureInfo.InvariantCulture),
                TempdbAllocationsMb = reader.IsDBNull(31) ? null : Convert.ToDouble(reader.GetValue(31), CultureInfo.InvariantCulture),
                TranLogUsedMb = reader.IsDBNull(32) ? null : Convert.ToDouble(reader.GetValue(32), CultureInfo.InvariantCulture),
                TranStartTime = reader.IsDBNull(33) ? null : reader.GetDateTime(33),
                RequestId = reader.IsDBNull(34) ? 0 : Convert.ToInt32(reader.GetValue(34), CultureInfo.InvariantCulture),
            });
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.SessionId)
            .Value(row.DatabaseName)
            .Value(row.ElapsedTimeFormatted)
            .Value(row.QueryText)
            .Value(row.QueryPlan)
            .Value(row.LiveQueryPlan)
            .Value(row.Status)
            .Value(row.BlockingSessionId)
            .Value(row.WaitType)
            .Value(row.WaitTimeMs)
            .Value(row.WaitResource)
            .Value(row.CpuTimeMs)
            .Value(row.TotalElapsedTimeMs)
            .Value(row.Reads)
            .Value(row.Writes)
            .Value(row.LogicalReads)
            .Value(row.GrantedQueryMemoryGb)
            .Value(row.TransactionIsolationLevel)
            .Value(row.Dop)
            .Value(row.ParallelWorkerCount)
            .Value(row.LoginName)
            .Value(row.HostName)
            .Value(row.ProgramName)
            .Value(row.OpenTransactionCount)
            .Value(row.PercentComplete)
            .Value(row.IsCdcCapture)
            .Value(row.QueryHash)
            .Value(row.RequestedMemoryMb)
            .Value(row.UsedMemoryMb)
            .Value(row.MaxUsedMemoryMb)
            .Value(row.TempdbCurrentMb)
            .Value(row.TempdbAllocationsMb)
            .Value(row.TranLogUsedMb)
            .Value(row.TranStartTime)
            .Value(row.RequestId);
    }
}
