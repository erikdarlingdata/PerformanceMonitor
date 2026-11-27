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
using PerformanceMonitor.Common.QueryTags;

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
/// same SQL — the templates are the single source for both paths. The button reads its query itself, so it builds
/// the unbounded form (the default), in which every request renders its own plans.
///
/// <para><b>Plans are bounded and reused (#9529).</b> The collector's form (<c>boundPlans</c>) renders at most
/// <see cref="MaxPlansPerRun"/> distinct estimated plans and as many live plans, highest CPU first, and it does not render an estimated plan
/// the host already holds in <see cref="CollectorContext.SnapshotPlanCache"/>: the read fills that row from the
/// held copy, so the stored row carries the same plan XML a fetch would have returned. Requests that share one
/// plan key in a run have it rendered once, on the first of them by CPU, and the read gives the others that
/// same plan (<see cref="PlansSharedMeasurement"/>). The live plan is a reading of the request at that minute
/// (its operator row counts move), so it is never reused, only capped. A row past the cap is stored with a NULL
/// plan, and the run's note counts those rows (<see cref="PlansOverCapMeasurement"/>,
/// <see cref="LivePlansOverCapMeasurement"/>), which is what tells a plan the cap skipped from a request that
/// has none.</para>
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

    /// <summary>
    /// The most estimated plans, and the most live plans, one run asks the target to render (#9529). Each plan is
    /// rendered on the monitored server and shipped whole, and with no bound a server running many large queries
    /// paid for every one of them every minute: in the field about three quarters of the slowest runs was the
    /// row drain, at roughly 0.8 MB of plan XML per row. Twenty covers every request on a server that is not
    /// saturated, and on one that is it keeps the plans of the requests using the most CPU, which are the ones a
    /// reader opens. Estimated plans the host already holds do not count against it, so on the next run the cap
    /// moves on to requests that have none yet.
    /// </summary>
    public const int MaxPlansPerRun = 20;

    /// <summary>Rows whose estimated plan came from the host's held copy instead of the target this run (#9529).</summary>
    public const string PlansReusedMeasurement = "plans_reused";

    /// <summary>
    /// Rows whose estimated plan is the one another request with the same plan key fetched in this same run
    /// (#9529). Kept apart from <see cref="PlansReusedMeasurement"/>, so the note still shows whether the host's
    /// held copies are being used across runs.
    /// </summary>
    public const string PlansSharedMeasurement = "plans_shared";

    /// <summary>
    /// Rows whose estimated plan was not rendered this run because their plan key is past
    /// <see cref="MaxPlansPerRun"/> distinct keys (#9529).
    /// </summary>
    public const string PlansOverCapMeasurement = "plans_over_cap";

    /// <summary>Rows past <see cref="MaxPlansPerRun"/> whose live plan was not rendered this run (#9529).</summary>
    public const string LivePlansOverCapMeasurement = "live_plans_over_cap";

    /* The plan_state values the query returns at PlanStateOrdinal. */
    private const int PlanStateFetched = 0;
    private const int PlanStateHeld = 1;
    private const int PlanStateOverCap = 2;
    private const int PlanStateShared = 4;

    /* The three columns the query returns after the payload. They are read, never stored. */
    private const int PlanStateOrdinal = 35;
    private const int LivePlanOverCapOrdinal = 36;
    private const int PlanKeyOrdinal = 37;

    /* A query built with no held plans: the predicate is constant, so plan_state is never 1 and no parameter is
       bound. This is the predicate of the form Lite's live snapshot button runs and of the on-demand live reads. */
    private const string NoHeldPlansPredicate = "1 = 0";

    /* The held-plan predicate always names every parameter, the unused ones bound NULL, so the batch text is the
       same on every run and the target keeps one cached batch instead of one per set of keys. */
    private static readonly string[] s_heldPlanParameterNames = BuildHeldPlanParameterNames();

    private static readonly string s_heldPlansPredicate =
        "pk.plan_key IN (" + string.Join(", ", s_heldPlanParameterNames) + ")";

    private static string[] BuildHeldPlanParameterNames()
    {
        var names = new string[QuerySnapshotPlanCache.MaxKnownPlansPerServer];
        for (var i = 0; i < names.Length; i++)
        {
            names[i] = "@pm_held_plan_" + i.ToString(CultureInfo.InvariantCulture);
        }

        return names;
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
        EXEC sys.sp_executesql N'SELECT /* PerformanceMonitorLite */ cj.job_id FROM msdb.dbo.cdc_jobs AS cj WHERE cj.job_type = N''capture'';';
        SET @cdc_readable = 1;
    END;
END TRY
BEGIN CATCH
    SET @cdc_readable = 0;
END CATCH;

SELECT /* PerformanceMonitorLite */
    der.session_id,
    der.database_name,
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
    login_name = COALESCE(NULLIF(des.login_name, N''), des.original_login_name),
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
    der.request_id,
    plan_state = CASE WHEN der.plan_state = 0 AND der.plan_rank > {3} THEN 2 ELSE der.plan_state END,
    live_plan_over_cap = {5},
    der.plan_key
FROM
(
    /* #9529: which requests get a plan this run. The estimated plan of one (plan_handle, statement offsets,
       query_plan_hash) does not change while it stays cached, so plan_key names it, and a request whose key
       the host already holds (plan_state 1) is not rendered again: its plan handle reaches
       sys.dm_exec_text_query_plan as NULL, which returns no row. In a pileup many requests share one key, so
       only the first request of each key by CPU (key_ordinal 1, plan_state 0) is ranked and rendered: the cap
       counts distinct plans, and every other request of that key (plan_state 4) takes the plan its first
       request fetched, on the host, from this same result. The live plan is rendered for the requests inside
       the same cap, by CPU. Without the bound, every request has both plans rendered and shipped every minute,
       which is the form BuildSnapshotQuery builds by default: its cap is the largest int and no request is
       plan_state 4, so every request renders its own plans. A request with no plan handle (plan_state 3: a
       background task, or a statement still compiling) has no plan to render and is not counted by either
       rank, and the session and database filters sit in this set, so none of those ever takes a plan from a
       request that is stored. Both ranks are running counts
       over one order, which is also the order of the result, and they are computed after key_ordinal, so the
       last sort of the request rows is the result's order and no sort carries the statement text or a plan. */
    SELECT
        rk.session_id,
        rk.request_id,
        rk.database_id,
        rk.sql_handle,
        rk.plan_handle,
        rk.statement_start_offset,
        rk.statement_end_offset,
        rk.status,
        rk.blocking_session_id,
        rk.wait_type,
        rk.wait_time,
        rk.wait_resource,
        rk.cpu_time,
        rk.total_elapsed_time,
        rk.reads,
        rk.writes,
        rk.logical_reads,
        rk.granted_query_memory,
        rk.transaction_isolation_level,
        rk.dop,
        rk.parallel_worker_count,
        rk.percent_complete,
        rk.query_hash,
        rk.database_name,
        rk.plan_key,
        plan_state = {6},
        plan_rank =
            SUM(CASE WHEN rk.key_state = 0 AND rk.key_ordinal = 1 THEN 1 ELSE 0 END) OVER
            (
                ORDER BY
                    rk.cpu_time DESC,
                    rk.parallel_worker_count DESC,
                    rk.session_id,
                    rk.request_id
                ROWS UNBOUNDED PRECEDING
            ),
        live_plan_rank =
            SUM(CASE WHEN rk.plan_handle IS NOT NULL THEN 1 ELSE 0 END) OVER
            (
                ORDER BY
                    rk.cpu_time DESC,
                    rk.parallel_worker_count DESC,
                    rk.session_id,
                    rk.request_id
                ROWS UNBOUNDED PRECEDING
            )
    FROM
    (
        SELECT
            r.session_id,
            r.request_id,
            r.database_id,
            r.sql_handle,
            r.plan_handle,
            r.statement_start_offset,
            r.statement_end_offset,
            r.status,
            r.blocking_session_id,
            r.wait_type,
            r.wait_time,
            r.wait_resource,
            r.cpu_time,
            r.total_elapsed_time,
            r.reads,
            r.writes,
            r.logical_reads,
            r.granted_query_memory,
            r.transaction_isolation_level,
            r.dop,
            r.parallel_worker_count,
            r.percent_complete,
            r.query_hash,
            database_name = d.name,
            pk.plan_key,
            key_state = ps.plan_state,
            key_ordinal =
                ROW_NUMBER() OVER
                (
                    PARTITION BY
                        pk.plan_key
                    ORDER BY
                        r.cpu_time DESC,
                        r.parallel_worker_count DESC,
                        r.session_id,
                        r.request_id
                )
        FROM sys.dm_exec_requests AS r
        LEFT JOIN sys.databases AS d
          ON d.database_id = r.database_id
        CROSS APPLY
        (
            SELECT
                plan_key =
                    CONVERT(nvarchar(130), r.plan_handle, 1) + N'|' +
                    CONVERT(nvarchar(11), r.statement_start_offset) + N'|' +
                    CONVERT(nvarchar(11), r.statement_end_offset) + N'|' +
                    ISNULL(CONVERT(nvarchar(18), r.query_plan_hash, 1), N'')
        ) AS pk
        CROSS APPLY
        (
            SELECT
                plan_state =
                    CASE
                        WHEN r.plan_handle IS NULL
                        THEN 3
                        WHEN {2}
                        THEN 1
                        ELSE 0
                    END
        ) AS ps
        WHERE r.session_id <> @@SPID
        AND   r.session_id >= 50
        AND   r.database_id <> ISNULL(DB_ID(N'PerformanceMonitor'), 0)
        {4}
    ) AS rk
) AS der
JOIN sys.dm_exec_sessions AS des
    ON des.session_id = der.session_id
OUTER APPLY sys.dm_exec_sql_text(COALESCE(der.sql_handle, der.plan_handle)) AS dest
OUTER APPLY
(
    /* The WHERE decides which requests have a plan, so a request past the cap or already held reads NULL
       whatever the function returns for it. The NULL handle is what keeps the function from rendering a
       plan for a row the WHERE is about to drop. */
    SELECT
        tqp.query_plan
    FROM sys.dm_exec_text_query_plan
    (
        CASE WHEN der.plan_state = 0 AND der.plan_rank <= {3} THEN der.plan_handle END,
        der.statement_start_offset,
        der.statement_end_offset
    ) AS tqp
    WHERE der.plan_state = 0
    AND   der.plan_rank <= {3}
) AS deqp
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
OPTION({7});
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
    rk.session_id,
    rk.database_id,
    rk.sql_handle,
    rk.plan_handle,
    rk.statement_start_offset,
    rk.statement_end_offset,
    rk.status,
    rk.blocking_session_id,
    rk.wait_type,
    rk.wait_time,
    rk.wait_resource,
    rk.cpu_time,
    rk.total_elapsed_time,
    rk.reads,
    rk.writes,
    rk.logical_reads,
    rk.granted_query_memory,
    rk.transaction_isolation_level,
    rk.dop,
    rk.parallel_worker_count,
    rk.percent_complete,
    rk.query_hash,
    rk.request_id,
    /* #9529: which requests get a plan this run. The estimated plan of one (plan_handle, statement offsets,
       query_plan_hash) does not change while it stays cached, so plan_key names it, and a request whose key
       the host already holds (plan_state 1) is not rendered again: its plan handle reaches
       sys.dm_exec_text_query_plan as NULL, which returns no row. In a pileup many requests share one key, so
       only the first request of each key by CPU (key_ordinal 1, plan_state 0) is ranked and rendered: the cap
       counts distinct plans, and every other request of that key (plan_state 4) takes the plan its first
       request fetched, on the host, from this same result. Without the bound, every request has its plan
       rendered and shipped every minute, which is the form BuildSnapshotQuery builds by default. A request
       with no plan handle (plan_state 3) has no plan to render and is not counted by either rank, so it never
       takes a plan from a request that is stored. */
    rk.plan_key,
    plan_state = {6},
    plan_rank =
        SUM(CASE WHEN rk.key_state = 0 AND rk.key_ordinal = 1 THEN 1 ELSE 0 END) OVER
        (
            ORDER BY
                rk.cpu_time DESC,
                rk.parallel_worker_count DESC,
                rk.session_id,
                rk.request_id
            ROWS UNBOUNDED PRECEDING
        ),
    live_plan_rank =
        SUM(CASE WHEN rk.plan_handle IS NOT NULL THEN 1 ELSE 0 END) OVER
        (
            ORDER BY
                rk.cpu_time DESC,
                rk.parallel_worker_count DESC,
                rk.session_id,
                rk.request_id
            ROWS UNBOUNDED PRECEDING
        )
INTO #req
FROM
(
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
        der.request_id,
        pk.plan_key,
        key_state = ps.plan_state,
        key_ordinal =
            ROW_NUMBER() OVER
            (
                PARTITION BY
                    pk.plan_key
                ORDER BY
                    der.cpu_time DESC,
                    der.parallel_worker_count DESC,
                    der.session_id,
                    der.request_id
            )
    FROM sys.dm_exec_requests AS der
    CROSS APPLY
    (
        SELECT
            plan_key =
                CONVERT(nvarchar(130), der.plan_handle, 1) + N'|' +
                CONVERT(nvarchar(11), der.statement_start_offset) + N'|' +
                CONVERT(nvarchar(11), der.statement_end_offset) + N'|' +
                ISNULL(CONVERT(nvarchar(18), der.query_plan_hash, 1), N'')
    ) AS pk
    CROSS APPLY
    (
        SELECT
            plan_state =
                CASE
                    WHEN der.plan_handle IS NULL
                    THEN 3
                    WHEN {2}
                    THEN 1
                    ELSE 0
                END
    ) AS ps
    WHERE der.session_id <> @@SPID
    AND   der.session_id >= 50
    AND   der.database_id = DB_ID()
    AND   der.database_id <> ISNULL(DB_ID(N'PerformanceMonitor'), 0)
) AS rk
OPTION(RECOMPILE);

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
    login_name = COALESCE(NULLIF(des.login_name, N''), des.original_login_name),
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
    der.request_id,
    plan_state = CASE WHEN der.plan_state = 0 AND der.plan_rank > {3} THEN 2 ELSE der.plan_state END,
    live_plan_over_cap = {5},
    der.plan_key
FROM #req AS der
JOIN sys.dm_exec_sessions AS des
    ON des.session_id = der.session_id
OUTER APPLY sys.dm_exec_sql_text(COALESCE(der.sql_handle, der.plan_handle)) AS dest
OUTER APPLY
(
    /* The WHERE decides which requests have a plan, so a request past the cap or already held reads NULL
       whatever the function returns for it. The NULL handle is what keeps the function from rendering a
       plan for a row the WHERE is about to drop. */
    SELECT
        tqp.query_plan
    FROM sys.dm_exec_text_query_plan
    (
        CASE WHEN der.plan_state = 0 AND der.plan_rank <= {3} THEN der.plan_handle END,
        der.statement_start_offset,
        der.statement_end_offset
    ) AS tqp
    WHERE der.plan_state = 0
    AND   der.plan_rank <= {3}
) AS deqp
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
{4}
ORDER BY der.cpu_time DESC, der.parallel_worker_count DESC
OPTION(MAXDOP 1, RECOMPILE);

DROP TABLE #req;
""";

    private static readonly CompositeFormat s_snapshotsAzureBaseFormat = CompositeFormat.Parse(SnapshotsAzureBase);

    /* The live plan of the first {cap} requests by CPU. The WHERE decides which requests have one. It has to:
       inside an APPLY, sys.dm_exec_query_statistics_xml given a NULL session id answers with the plan of the last
       session it was asked for, so the NULL alone would store another session's plan on the row. The NULL is
       still passed, because the function renders nothing for it. */
    private const string LivePlanApply = """
OUTER APPLY
(
    SELECT
        qsx.query_plan
    FROM sys.dm_exec_query_statistics_xml(CASE WHEN der.plan_handle IS NOT NULL AND der.live_plan_rank <= {cap} THEN der.session_id END) AS qsx
    WHERE der.plan_handle IS NOT NULL
    AND   der.live_plan_rank <= {cap}
) AS deqs
""";

    /* The sharing expression of the bounded form: a request that shares its plan key with an earlier request
       renders nothing, and the read gives it the plan that request fetched. */
    private const string SharedPlanState = "CASE WHEN rk.key_state = 0 AND rk.key_ordinal > 1 THEN 4 ELSE rk.key_state END";

    /// <summary>
    /// Builds the query snapshots SQL with or without live query plan support, the single source for both the
    /// scheduled collector and Lite's live snapshot button (#857 for the Azure #req staging).
    ///
    /// <para><paramref name="boundPlans"/> false, the default, is the query as the live snapshot button runs it:
    /// every request renders its own estimated plan and its own live plan, with no cap and no sharing, and a reader
    /// that takes the payload columns as they come (the button's own) has every plan in them. True is the
    /// collector's form (#9529): at most <see cref="MaxPlansPerRun"/> plans of each kind, one estimated plan for all
    /// the requests that share a plan key, and the plan_state, live_plan_over_cap and plan_key columns that
    /// <see cref="ReadAsync"/> reads to give those requests the plan the target did not send.</para>
    /// </summary>
    public static string BuildSnapshotQuery(bool supportsLiveQueryPlan, bool isAzureSqlDatabase, bool boundPlans = false)
        => BuildSnapshotQuery(supportsLiveQueryPlan, isAzureSqlDatabase, boundPlans, NoHeldPlansPredicate, exclusionClause: "");

    private static string BuildSnapshotQuery(
        bool supportsLiveQueryPlan, bool isAzureSqlDatabase, bool boundPlans, string heldPlansPredicate, string exclusionClause)
    {
        var cap = (boundPlans ? MaxPlansPerRun : int.MaxValue).ToString(CultureInfo.InvariantCulture);
        var liveQueryPlanColumn = supportsLiveQueryPlan
            ? "live_query_plan = deqs.query_plan,"
            : "live_query_plan = CONVERT(xml, NULL),";
        var liveQueryPlanApply = !supportsLiveQueryPlan
            ? ""
            : boundPlans
                ? LivePlanApply.Replace("{cap}", cap, StringComparison.Ordinal)
                : "OUTER APPLY sys.dm_exec_query_statistics_xml(der.session_id) AS deqs";
        var livePlanOverCap = supportsLiveQueryPlan && boundPlans
            ? "CONVERT(bit, CASE WHEN der.plan_handle IS NOT NULL AND der.live_plan_rank > " + cap + " THEN 1 ELSE 0 END)"
            : "CONVERT(bit, 0)";
        var planState = boundPlans ? SharedPlanState : "rk.key_state";
        var template = isAzureSqlDatabase ? s_snapshotsAzureBaseFormat : s_snapshotsBaseFormat;
        return string.Format(
            null, template, liveQueryPlanColumn, liveQueryPlanApply, heldPlansPredicate, cap, exclusionClause, livePlanOverCap, planState,
            exclusionClause.Length == 0 ? OnPremCachedOptions : OnPremRecompileOptions);
    }

    /* #9683: the on-prem statement carries no OPTION(RECOMPILE) when no database is excluded, the one parameterized
       statement CONTRIBUTING.md lets drop the hint. The held-plan parameters sit in a CASE and an IN list that pick a
       plan_state value, never in a seek or a row filter, so a plan compiled for one set of keys returns the same rows
       for any other. The estimates for sys.dm_exec_requests and the other DMVs are fixed, so a compile at idle and a
       compile under load read the same estimates. Measured against the hinted text, 200 runs each over pooled
       connections in alternating blocks of 50, on SQL Server 2022 and 2025 (emulated x64 on arm64, so read the ratios):
       idle, 5 requests, 40 requests of one plan key, 24 distinct plans, 120 requests, a server with CDC capture jobs,
       and 0 or 20 held keys. Server CPU per run is 50 to 92% lower and the first result arrives 71 to 92% sooner. The
       hinted text spends 23 ms of CPU compiling at idle on 2022 against 2 ms to run, and the plan compiled at idle and
       reused under each load costs less than the hinted text in every state. Back to back hinted and unhinted runs return
       the same rows in 2100 of 2100 pairs on 2022 and 2088 of 2100 on 2025. The 12 that differ are one boundary row or
       cell each, under 24 or 40 requests that start in the same instant, and the pre-#9554 text differs the same way.
       3000 repeat pairs on a fresh or aged load found none. The statement recompiles once on its first run, which
       defers the compile of the @cdc_capture_jobs table variable, and then reuses its plan. A statement with excluded databases binds their
       names as parameters and keeps the hint, as does the Azure SQL Database text, which was not measured. */
    private const string OnPremCachedOptions = "MAXDOP 1";

    private const string OnPremRecompileOptions = "MAXDOP 1, RECOMPILE";

    /// <summary>The live-plan gate: 2016 SP1+/version-unknown/MI; never on Azure SQL DB (#857).</summary>
    public static bool SupportsLiveQueryPlan(CollectorTargetInfo target)
        => !target.IsAzureSqlDb
           && (target.SqlMajorVersion >= 13 || target.SqlMajorVersion == 0 || target.IsAzureManagedInstance);

    /// <summary>
    /// Whole-collector version floor (#9099): the SQL below reads columns or objects that SQL Server 2014 and older do not have
    /// (dm_exec_requests.dop and parallel_worker_count), so on a target below the supported floor the collector does not run and reads as not collected, instead of failing
    /// every cycle. Unknown version (0) assumes newest, and Azure SQL Database and Managed Instance are never version-gated.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) =>
        target.MeetsMinimumVersion(CollectorTargetInfo.SupportedFloorMajorVersion, unknownIsNewest: true);

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
        /* The per-database exclusion filter. Both variants join sys.databases AS d, so it filters on d.name, and
           an empty ExcludedDatabases gives an empty clause. On a boxed target it sits inside the ranked request
           set, so an excluded database's request neither takes a plan from a stored one nor has a plan rendered.
           On Azure SQL Database the staged set is one database, and the clause sits in the final WHERE. */
        var (exclusionClause, parameters) = DatabaseExclusionFilter.Build(context.ExcludedDatabases, "d.name");

        var heldPlansPredicate = NoHeldPlansPredicate;
        if (context.SnapshotPlanCache is { } cache)
        {
            /* #9529: name the plans the host holds, so the target renders none of them. The same set goes on the
               context for the read, which fills those rows from it. */
            var held = cache.Known(context.ServerId, context.CollectionTime);
            context.SnapshotKnownPlans = held;
            heldPlansPredicate = s_heldPlansPredicate;

            var bound = 0;
            foreach (var planKey in held.Keys)
            {
                if (bound == s_heldPlanParameterNames.Length)
                {
                    break;
                }

                parameters.Add(new CollectorParameter(s_heldPlanParameterNames[bound++], planKey, CollectorParameterType.NVarChar260));
            }

            for (; bound < s_heldPlanParameterNames.Length; bound++)
            {
                parameters.Add(new CollectorParameter(s_heldPlanParameterNames[bound], null, CollectorParameterType.NVarChar260));
            }
        }

        var query = BuildSnapshotQuery(
            SupportsLiveQueryPlan(context.Target), context.Target.IsAzureSqlDb, boundPlans: true, heldPlansPredicate, exclusionClause);

        return new CollectorQuery(query, parameters);
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

        /* #9529: the plan bookkeeping columns follow the payload. A reader without them is read as before. */
        var hasPlanState = reader.FieldCount > PlanKeyOrdinal;
        var cache = context.SnapshotPlanCache;
        var held = context.SnapshotKnownPlans;
        long plansReused = 0;
        long plansShared = 0;
        long plansOverCap = 0;
        long livePlansOverCap = 0;

        /* The plans this run fetched and the keys it left past the cap, by plan key, and the rows that share a
           key with an earlier request (plan_state 4). Those rows are settled after the last row is read, so
           they do not depend on where their first request sits in the result. */
        Dictionary<string, string>? fetchedThisRun = null;
        HashSet<string>? overCapKeys = null;
        List<(int RowIndex, string PlanKey)>? sharedRows = null;

        while (await reader.ReadAsync(cancellationToken))
        {
            var queryPlan = reader.IsDBNull(4) ? null : reader.GetString(4);
            if (hasPlanState)
            {
                var planState = reader.IsDBNull(PlanStateOrdinal)
                    ? PlanStateFetched
                    : Convert.ToInt32(reader.GetValue(PlanStateOrdinal), CultureInfo.InvariantCulture);
                var planKey = reader.IsDBNull(PlanKeyOrdinal) ? null : reader.GetString(PlanKeyOrdinal);

                if (planState == PlanStateHeld)
                {
                    /* The target rendered nothing for this row because the query named its plan as held. The
                       held copy is the plan an earlier run fetched for the same key, so the row is stored with
                       the plan it would have carried. */
                    if (planKey is not null && held is not null && held.TryGetValue(planKey, out var heldPlan))
                    {
                        queryPlan = heldPlan;
                        plansReused++;
                        cache?.Touch(context.ServerId, planKey, context.CollectionTime);
                    }
                }
                else if (planState == PlanStateOverCap)
                {
                    plansOverCap++;
                    if (planKey is not null)
                    {
                        (overCapKeys ??= new HashSet<string>(StringComparer.Ordinal)).Add(planKey);
                    }
                }
                else if (planState == PlanStateShared)
                {
                    if (planKey is not null)
                    {
                        (sharedRows ??= []).Add((rows.Count, planKey));
                    }
                }
                else if (planState == PlanStateFetched && queryPlan is not null && planKey is not null)
                {
                    /* Held as fetched, before the statement filter: the filter judges it on every run, as it
                       does a plan the target sent. */
                    cache?.Store(context.ServerId, planKey, queryPlan, context.CollectionTime);
                    (fetchedThisRun ??= new Dictionary<string, string>(StringComparer.Ordinal))[planKey] = queryPlan;
                }

                if (!reader.IsDBNull(LivePlanOverCapOrdinal)
                    && Convert.ToBoolean(reader.GetValue(LivePlanOverCapOrdinal), CultureInfo.InvariantCulture))
                {
                    livePlansOverCap++;
                }
            }

            rows.Add(new Row
            {
                SessionId = Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture),
                DatabaseName = reader.IsDBNull(1) ? null : reader.GetString(1),
                ElapsedTimeFormatted = reader.IsDBNull(2) ? null : reader.GetString(2),
                QueryText = reader.IsDBNull(3) ? null : scrub.Text(reader.GetString(3)),
                QueryPlan = queryPlan is null ? null : scrub.Xml(queryPlan),
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

        if (sharedRows is not null)
        {
            foreach (var (rowIndex, planKey) in sharedRows)
            {
                if (fetchedThisRun is not null && fetchedThisRun.TryGetValue(planKey, out var sharedPlan))
                {
                    /* The plan the first request of this key fetched, judged by the statement filter for this
                       row as a plan the target sent would be. */
                    rows[rowIndex].QueryPlan = scrub.Xml(sharedPlan);
                    plansShared++;
                }
                else if (overCapKeys is not null && overCapKeys.Contains(planKey))
                {
                    /* Its key is past the cap, so no request of that key was rendered. */
                    plansOverCap++;
                }
            }
        }

        /* Only a run that reused or skipped something says so: a run that fetched every plan keeps a NULL note. */
        if (plansReused > 0)
        {
            context.Measure(PlansReusedMeasurement, plansReused);
        }

        if (plansShared > 0)
        {
            context.Measure(PlansSharedMeasurement, plansShared);
        }

        if (plansOverCap > 0)
        {
            context.Measure(PlansOverCapMeasurement, plansOverCap);
        }

        if (livePlansOverCap > 0)
        {
            context.Measure(LivePlansOverCapMeasurement, livePlansOverCap);
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

    /// <summary>
    /// #5980: one hash sighting for an active-query row, carrying the application, login, host and database it
    /// ran under and a sample of its text. Null when the row has no query_hash.
    /// </summary>
    public override IReadOnlyList<QueryTagSighting>? DescribeQueryTagSightings(Row row)
        => QueryTagSightings.Collect(QueryTagSightings.Hash(
            row.QueryHash, row.DatabaseName, null, row.ProgramName, row.LoginName, row.HostName, row.QueryText));
}
