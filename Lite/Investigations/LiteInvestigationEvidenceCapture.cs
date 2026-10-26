/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Investigations;

/// <summary>
/// The evidence capture of Lite's investigations (#6010, the DuckDB twin of Darling's #6001 and #6002): right after a request
/// opens, a pass freezes the sections an agent would read first from Lite's already collected rows. <b>It opens no connection
/// to a monitored server</b>: it reads only Lite's own DuckDB, so Lite stays advise-only. Sections are rows in
/// <c>investigation_evidence</c>: High CPU brings <c>top_statements</c>, <c>active_requests</c>, <c>query_store</c>, <c>jobs</c>
/// and <c>waits</c>; <c>Deadlocks Detected</c> brings <c>deadlocks</c>; <c>Blocking Detected</c> and <c>Blocking Wait Time</c>
/// bring <c>blocking_chains</c> and <c>head_blocker</c>; the <c>query_performance</c> family brings <c>regression_targets</c>.
///
/// <para>A section is <c>ready</c> when the collector that feeds it has COMPLETED a run at or after the window end (read from
/// <c>collection_log</c>; a run that found nothing is still a run). It is then FROZEN: the store never rewrites it. Until that
/// run it is <c>pending</c>, and after its grace (<see cref="GraceFor"/>) it is <c>not_collected</c>. The store journals
/// <c>evidence_ready</c> once per request. Slow collectors, the section cap, and the per-section deadline follow Darling's
/// rules, so the two products freeze the same evidence.</para>
///
/// <para>The reads are DuckDB's: Darling's SQL ported over Lite's <c>v_</c> views. The pure shaping (the section cap, deadlock
/// parties, blocking chains and head blockers, report XML fitting) is Darling's code, copied, because Darling's capture lives
/// in a project Lite cannot reference.</para>
/// </summary>
public sealed class LiteInvestigationEvidenceCapture
{
    public const int MaxSectionBytes = InvestigationToolCore.MaxEvidenceBytes;
    public const string HighCpuMetric = AlertEngine.CpuPersistenceMetric;
    public const string DeadlockMetric = AlertEngine.DeadlockWatermarkMetric;

    /// <summary>The Client Timeouts alert's metric (#6553): a request opened on it gets the <c>client_timeouts</c> section.</summary>
    public const string ClientTimeoutsMetric = AttentionEvaluator.MetricName;

    /// <summary>Characters of a top statement's sample text kept in <c>client_timeouts</c>.</summary>
    public const int ClientTimeoutStatementMaxChars = 1000;
    public static readonly TimeSpan BreachLeadIn = TimeSpan.FromMinutes(15);
    public static readonly TimeSpan BreachTail = TimeSpan.FromMinutes(1);
    public static readonly TimeSpan PendingGrace = TimeSpan.FromMinutes(20);
    public const int SlowCollectorGraceMultiplier = 2;
    public static readonly TimeSpan CaptureHorizon = TimeSpan.FromHours(6);
    public static readonly TimeSpan HorizonMargin = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan RequestDeadline = TimeSpan.FromMinutes(2);
    public const int TopStatements = 10;
    public const int SectionRows = 10;
    public const int QueryStoreDatabasesPerCapture = 10;
    public const int CandidatesPerTick = 100;
    public const int PlansPerStatement = 3;
    public const int DeadlockStatementMaxChars = 1000;
    public const int DeadlockPartiesMax = 8;
    public const int DeadlockObjectsMax = 20;
    public static readonly TimeSpan RegressionLookback = TimeSpan.FromDays(1);
    public const int RegressionFactor = PlanInputComparison.PerRowFactor;
    public static readonly TimeSpan StatementLookback = TimeSpan.FromDays(1);
    public static readonly TimeSpan RequestStartTolerance = TimeSpan.FromSeconds(30);
    private const int BatchRequests = 25;

    public static readonly IReadOnlyList<string> Sections = new[] { "top_statements", "active_requests", "query_store", "jobs", "waits" };

    public static readonly IReadOnlyList<string> BlockingMetrics = new[] { AlertEngine.BlockingWatermarkMetric, AlertEngine.BlockingWaitPersistenceMetric };

    /// <summary>The sections each scope key brings: a linked metric, or <c>family:&lt;name&gt;</c>.</summary>
    public static readonly IReadOnlyDictionary<string, IReadOnlyList<string>> SectionsByKey = BuildSectionsByKey();

    private static Dictionary<string, IReadOnlyList<string>> BuildSectionsByKey()
    {
        var blocking = new[] { "blocking_chains", "head_blocker" };
        var map = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal)
        {
            [HighCpuMetric] = Sections,
            [DeadlockMetric] = new[] { "deadlocks" },
            [ClientTimeoutsMetric] = new[] { "client_timeouts" },
            ["family:" + InvestigationFamily.QueryPerformance] = new[] { "regression_targets" },
        };
        foreach (var metric in BlockingMetrics)
            map[metric] = blocking;
        return map;
    }

    /// <summary>The collector (<c>collection_log.collector_name</c>) that feeds each section.</summary>
    public static readonly IReadOnlyDictionary<string, string> SectionCollectors = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["top_statements"] = "query_stats",
        ["active_requests"] = "query_snapshots",
        ["query_store"] = "query_store_health",
        ["jobs"] = "running_jobs",
        ["waits"] = "wait_stats",
        ["deadlocks"] = "deadlocks",
        ["blocking_chains"] = "blocked_process_report",
        ["head_blocker"] = "blocked_process_report",
        ["regression_targets"] = "query_store",
        ["client_timeouts"] = AttentionWindowSql.CollectorName,
    };

    internal const string CollectorRunsSql = @"
SELECT DISTINCT collector_name
FROM v_collection_log
WHERE server_id = $1
  AND status = 'SUCCESS'
  AND collection_time >= $2";

    internal const string TopStatementsSql = @"
SELECT database_name, query_hash, query_plan_hash,
       CAST(SUM(delta_worker_time) AS BIGINT) AS cpu_us,
       CAST(SUM(delta_execution_count) AS BIGINT) AS executions,
       CAST(SUM(delta_elapsed_time) AS BIGINT) AS elapsed_us,
       CAST(SUM(delta_logical_reads) AS BIGINT) AS reads,
       MIN(collection_time) AS first_seen
FROM v_query_stats
WHERE server_id = $1
  AND collection_time >= $2
  AND collection_time <= $3
  AND sample_interval_seconds IS DISTINCT FROM 0
GROUP BY database_name, query_hash, query_plan_hash";

    internal const string ActiveRequestsSql = @"
WITH snaps AS (
    SELECT host_name, program_name, login_name, session_id, COALESCE(request_id, 0) AS request_id, collection_time, cpu_time_ms,
           collection_time - to_milliseconds(CAST(COALESCE(total_elapsed_time_ms, 0) AS BIGINT)) AS started_at
    FROM v_query_snapshots
    WHERE server_id = $1
      AND collection_time >= $2
      AND collection_time <= $3
), marked AS (
    SELECT s.*,
           CASE WHEN LAG(s.collection_time) OVER w IS NULL THEN 1
                WHEN s.cpu_time_ms < LAG(s.cpu_time_ms) OVER w THEN 1
                WHEN s.started_at > LAG(s.started_at) OVER w + INTERVAL 30 SECOND THEN 1
                ELSE 0 END AS is_new
    FROM snaps AS s
    WINDOW w AS (PARTITION BY s.session_id, s.request_id ORDER BY s.collection_time)
), numbered AS (
    SELECT m.*, SUM(m.is_new) OVER (PARTITION BY m.session_id, m.request_id ORDER BY m.collection_time) AS run_no
    FROM marked AS m
), per_request AS (
    SELECT host_name, program_name, login_name, session_id, request_id, run_no, count(*) AS samples, MAX(cpu_time_ms) AS cpu_ms
    FROM numbered
    GROUP BY host_name, program_name, login_name, session_id, request_id, run_no
)
SELECT host_name, program_name, login_name, CAST(SUM(samples) AS BIGINT) AS samples, CAST(count(DISTINCT session_id) AS BIGINT) AS sessions,
       CAST(count(*) AS BIGINT) AS requests, CAST(SUM(cpu_ms) AS BIGINT) AS total_cpu_ms, CAST(MAX(cpu_ms) AS BIGINT) AS max_cpu_ms
FROM per_request
GROUP BY host_name, program_name, login_name
ORDER BY SUM(cpu_ms) DESC NULLS LAST, host_name, program_name, login_name
LIMIT 10";

    internal const string JobsSql = @"
SELECT job_name, start_time, current_duration_seconds, avg_duration_seconds, is_running_long
FROM (
    SELECT DISTINCT ON (job_name) job_name, start_time, current_duration_seconds, avg_duration_seconds, is_running_long, collection_time
    FROM v_running_jobs
    WHERE server_id = $1
      AND collection_time >= $2
      AND collection_time <= $3
    ORDER BY job_name, collection_time DESC
) AS j
ORDER BY current_duration_seconds DESC NULLS LAST, job_name
LIMIT 10";

    internal const string WaitsSql = @"
SELECT rtrim(wait_type) AS wait_type,
       CAST(SUM(waiting_tasks) AS BIGINT) AS total_waiting_tasks,
       CAST(SUM(wait_time_ms) AS BIGINT) AS total_wait_time_ms,
       CAST(SUM(signal_wait_time_ms) AS BIGINT) AS total_signal_wait_time_ms
FROM (
    SELECT wait_type,
           SUM(delta_waiting_tasks) AS waiting_tasks,
           SUM(delta_wait_time_ms) AS wait_time_ms,
           SUM(delta_signal_wait_time_ms) AS signal_wait_time_ms
    FROM v_wait_stats
    WHERE server_id = $1
      AND collection_time >= $2
      AND collection_time <= $3
    GROUP BY wait_type
) AS per_spelling
GROUP BY rtrim(wait_type)
ORDER BY SUM(wait_time_ms) DESC
LIMIT 10";

    internal const string WindowCpuByDatabaseSql = @"
SELECT database_name, CAST(SUM(delta_worker_time) AS BIGINT) AS window_cpu_us
FROM v_query_stats
WHERE server_id = $1
  AND collection_time >= $2
  AND collection_time <= $3
  AND sample_interval_seconds IS DISTINCT FROM 0
GROUP BY database_name";

    internal const string QueryStoreHealthSql = @"
SELECT database_name, actual_state, desired_state, readonly_reason, capture_time
FROM v_query_store_health
WHERE server_id = $1
QUALIFY row_number() OVER (PARTITION BY database_name ORDER BY capture_time DESC) = 1
ORDER BY database_name";

    /* Each stored event once (StoredEventCopies): a later batch can store a copy of the same event again. The window
       filter is the read's own, with no collection_time lower bound. */
    internal static readonly string DeadlocksSql = @"
SELECT deadlock_time, database_name, victim_process_id, deadlock_graph_xml
FROM " + StoredEventCopies.Deadlocks(
        "server_id = $1 AND deadlock_time >= $2 AND deadlock_time <= $3 AND deadlock_graph_xml IS NOT NULL AND deadlock_graph_xml <> ''") + @" AS dl
ORDER BY deadlock_time DESC
LIMIT 10";

    internal static readonly string BlockedReportsSql = @"
SELECT event_time, database_name, blocked_spid, blocked_ecid, blocking_spid, blocking_ecid, wait_time_ms, wait_resource, lock_mode,
       contentious_object, blocked_sql_text, blocking_sql_text, blocked_login_name, blocking_login_name, blocked_host_name,
       blocking_host_name, blocking_client_app, blocking_status, blocked_process_report_xml
FROM " + StoredEventCopies.BlockedProcessReports("server_id = $1 AND event_time >= $2 AND event_time <= $3 AND " + PerformanceMonitor.Common.BlockingWaits.NotSelfWaitSql) + @" AS ev
ORDER BY event_time DESC, wait_time_ms DESC
LIMIT 500";

    internal const string StatementByOffsetSql = @"
SELECT query_text
FROM v_query_stats
WHERE server_id = $1
  AND sql_handle = $2
  AND statement_start_offset = $3
  AND statement_end_offset = $4
  AND collection_time >= $5
  AND query_text IS NOT NULL
  AND trim(query_text) <> ''
ORDER BY collection_time DESC
LIMIT 1";

    internal const string RegressionTargetsSql = @"
WITH deduped AS (
    SELECT database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
           (array_agg(query_plan_hash ORDER BY collection_time DESC, execution_count DESC))[1] AS query_plan_hash,
           (array_agg(execution_count ORDER BY collection_time DESC, execution_count DESC))[1] AS execution_count,
           (array_agg(avg_cpu_time_us ORDER BY collection_time DESC, execution_count DESC))[1] AS avg_cpu_time_us,
           (array_agg(last_execution_time ORDER BY collection_time DESC, execution_count DESC))[1] AS last_execution_time
    FROM v_query_store_stats
    WHERE server_id = $1
      AND execution_type_desc = 'Regular'
      AND last_execution_time >= $2
      AND collection_time >= $2
      AND first_execution_time <= $4
    GROUP BY database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time
), plans AS (
    SELECT database_name, query_id, plan_id, any_value(query_plan_hash) AS query_plan_hash,
           SUM(execution_count) AS executions,
           CAST(SUM(avg_cpu_time_us * execution_count) AS DOUBLE) / NULLIF(SUM(execution_count), 0) AS cpu_per_exec_us,
           MAX(last_execution_time) AS last_execution_time,
           COALESCE(SUM(execution_count) FILTER (WHERE last_execution_time >= $3), 0) AS window_executions
    FROM deduped
    GROUP BY database_name, query_id, plan_id
), ranked AS (
    SELECT p.*,
           ROW_NUMBER() OVER (PARTITION BY database_name, query_id ORDER BY last_execution_time DESC, plan_id DESC) AS newest,
           ROW_NUMBER() OVER (PARTITION BY database_name, query_id ORDER BY cpu_per_exec_us ASC NULLS LAST, plan_id) AS cheapest,
           MAX(last_execution_time) OVER (PARTITION BY database_name, query_id) AS query_last_execution,
           SUM(window_executions) OVER (PARTITION BY database_name, query_id) AS query_window_executions
    FROM plans AS p
    WHERE p.executions > 0
)
SELECT b.database_name, b.query_id,
       b.cpu_per_exec_us / g.cpu_per_exec_us AS regression_factor,
       b.query_last_execution AS last_execution_time,
       CAST(b.query_window_executions AS BIGINT) AS window_executions,
       g.plan_id AS good_plan_id, g.query_plan_hash AS good_plan_hash, g.cpu_per_exec_us AS good_cpu_per_exec_us,
       CAST(g.executions AS BIGINT) AS good_executions,
       b.plan_id AS bad_plan_id, b.query_plan_hash AS bad_plan_hash, b.cpu_per_exec_us AS bad_cpu_per_exec_us,
       CAST(b.executions AS BIGINT) AS bad_executions
FROM ranked AS b
JOIN ranked AS g
  ON g.database_name = b.database_name
 AND g.query_id = b.query_id
 AND g.cheapest = 1
WHERE b.newest = 1
  AND g.plan_id <> b.plan_id
  AND g.cpu_per_exec_us > 0
  AND b.cpu_per_exec_us >= $5 * g.cpu_per_exec_us
  -- #6548: SQL Server own housekeeping statements (change tracking cleanup and the like) are not application code. Left
  -- out BEFORE the row cap, so they cannot use up its rows. Any row of the query carrying the text is enough.
  AND NOT EXISTS (SELECT 1 FROM v_query_store_stats AS x
                  WHERE x.server_id = $1
                    AND x.database_name = b.database_name
                    AND x.query_id = b.query_id
                    AND x.last_execution_time >= $2
                    AND regexp_matches(lower(x.query_text), '" + PerformanceMonitor.Common.InternalStatements.LowerTextPattern + @"'))
ORDER BY b.cpu_per_exec_us / g.cpu_per_exec_us DESC, b.database_name, b.query_id
LIMIT $6";

    internal const string PlanTextSql = @"
SELECT plan_id, query_plan_text
FROM v_query_store_stats
WHERE server_id = $1
  AND database_name = $2
  AND plan_id IN ($3, $4)
  AND query_plan_text IS NOT NULL
ORDER BY collection_time DESC";

    private readonly DuckDbInitializer _duckDb;
    private readonly DuckDbInvestigationStore _requests;
    private readonly ILogger? _logger;
    private readonly Func<DateTime> _utcNow;
    private readonly Func<string, int, int> _collectorIntervalMinutes;
    private readonly TimeSpan _sectionDeadline;

    /// <param name="duckDb">Lite's database: the only data the capture reads.</param>
    /// <param name="requests">The request store over the same database (it should share <paramref name="utcNow"/>).</param>
    /// <param name="logger">Where a failure is logged.</param>
    /// <param name="utcNow">The one clock the grace is judged by.</param>
    /// <param name="collectorIntervalMinutes">A collector's configured interval in minutes on one server, (collector name, server id);
    /// the code defaults without it.</param>
    /// <param name="sectionDeadline">How long one section's reads may take (default <see cref="RequestDeadline"/>).</param>
    public LiteInvestigationEvidenceCapture(
        DuckDbInitializer duckDb, DuckDbInvestigationStore requests, ILogger? logger = null, Func<DateTime>? utcNow = null,
        Func<string, int, int>? collectorIntervalMinutes = null, TimeSpan? sectionDeadline = null)
    {
        _duckDb = duckDb ?? throw new ArgumentNullException(nameof(duckDb));
        _requests = requests ?? throw new ArgumentNullException(nameof(requests));
        _logger = logger;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _collectorIntervalMinutes = (collector, serverId) =>
        {
            var configured = collectorIntervalMinutes?.Invoke(collector, serverId) ?? -1;
            return configured > 0 ? configured : DefaultIntervalMinutes(collector, serverId);
        };
        _sectionDeadline = sectionDeadline ?? RequestDeadline;
    }

    private static int DefaultIntervalMinutes(string collectorName, int serverId) =>
        CollectorScheduleDefaults.All.TryGetValue(collectorName, out var entry)
            ? CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(entry.FrequencyMinutes)
            : 5;

    /// <summary>How long after the request opened a section may wait for its collector's run before it is <c>not_collected</c>.</summary>
    public TimeSpan GraceFor(string section, int serverId)
    {
        var interval = TimeSpan.FromMinutes(_collectorIntervalMinutes(SectionCollectors[section], serverId));
        var slow = TimeSpan.FromTicks(interval.Ticks * SlowCollectorGraceMultiplier);
        var grace = slow > PendingGrace ? slow : PendingGrace;
        return grace > CaptureHorizon ? CaptureHorizon : grace;
    }

    /// <summary>One pass: every request still needing evidence gets its sections looked at. A failed request is logged and the next
    /// is tried. Returns how many requests it recorded.</summary>
    public async Task<int> CaptureOnceAsync(CancellationToken cancellationToken)
    {
        var now = _utcNow();
        var recorded = 0;
        var page = await _requests.ListEvidenceCandidatesAsync(now - CaptureHorizon, CandidatesPerTick, SectionsByKey, cancellationToken);
        foreach (var candidate in page)
        {
            try
            {
                await CaptureAsync(candidate, now, cancellationToken);
                recorded++;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Evidence capture failed for investigation {RequestId}; it is tried again next pass", candidate.RequestId);
            }
        }

        return recorded;
    }

    private async Task CaptureAsync(LiteEvidenceCandidate request, DateTime now, CancellationToken cancellationToken)
    {
        var existing = await _requests.ReadEvidenceAsync(request.RequestId, cancellationToken);
        var settled = new HashSet<string>(StringComparer.Ordinal);
        foreach (var row in existing)
        {
            if (row.State != "pending")
                settled.Add(row.Section);
        }

        var writes = new List<LiteEvidenceWrite>();
        var slow = new HashSet<string>(StringComparer.Ordinal);
        var runsByWindowEnd = new Dictionary<DateTime, HashSet<string>>();
        var finalPass = now >= request.OpenedAtUtc + CaptureHorizon - HorizonMargin;
        foreach (var (section, anchor) in SectionsOf(request))
        {
            if (settled.Contains(section))
                continue;

            var from = anchor - BreachLeadIn;
            var to = anchor + BreachTail;
            var graceStart = anchor > request.OpenedAtUtc ? anchor : request.OpenedAtUtc;
            var age = now - graceStart;
            var grace = CappedGrace(GraceFor(section, request.ServerId), graceStart, request.OpenedAtUtc);
            var over = finalPass || age >= grace;

            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            deadline.CancelAfter(_sectionDeadline);
            var readToken = deadline.Token;
            try
            {
                if (!runsByWindowEnd.TryGetValue(to, out var ran))
                {
                    ran = await ReadCollectorRunsAsync(request.ServerId, to, readToken);
                    runsByWindowEnd[to] = ran;
                }

                if (ran.Contains(SectionCollectors[section]))
                {
                    var rows = await ReadSectionAsync(section, request, from, to, readToken);
                    var scrubbed = ScrubRows(rows.Rows, readToken);
                    writes.Add(new LiteEvidenceWrite(section, "ready", rows.ReportXml is null
                        ? Fit(from, to, scrubbed, rows.DatabasesDropped)
                        : FitReportXml(from, to, scrubbed, rows.ReportXml)));
                }
                else
                {
                    if (grace > PendingGrace)
                        slow.Add(section);
                    writes.Add(new LiteEvidenceWrite(section, over ? "not_collected" : "pending", null));
                }
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                var timedOut = deadline.IsCancellationRequested;
                _logger?.LogWarning(timedOut ? null : ex, "Evidence section {Section} for investigation {RequestId} {Outcome}",
                    section, request.RequestId, timedOut ? "ran past its deadline" : "failed");
                if (grace > PendingGrace)
                    slow.Add(section);
                writes.Add(over
                    ? new LiteEvidenceWrite(section, "ready", Unavailable(from, to, timedOut ? new TimeoutException() : ex))
                    : new LiteEvidenceWrite(section, "pending", null));
            }
        }

        if (writes.Count > 0)
        {
            await _requests.RecordEvidenceAsync(
                request.RequestId, request.ServerId, writes, slow, now - request.OpenedAtUtc >= PendingGrace, cancellationToken);
        }
    }

    private sealed record SectionRead(List<JsonObject> Rows, int? DatabasesDropped = null, IReadOnlyList<string?>? ReportXml = null);

    private async Task<T> WithConnectionAsync<T>(Func<DuckDBConnection, Task<T>> read, CancellationToken cancellationToken)
    {
        using (_duckDb.AcquireReadLock(cancellationToken))
        {
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return await read(connection).ConfigureAwait(false);
        }
    }

    private static DuckDBCommand Command(DuckDBConnection connection, string sql, params object?[] values)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var value in values)
            command.Parameters.Add(new DuckDBParameter { Value = value ?? DBNull.Value });
        return command;
    }

    private static async Task<List<JsonObject>> ReadRowsAsync(DuckDBCommand command, CancellationToken cancellationToken)
    {
        var rows = new List<JsonObject>();
        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var row = new JsonObject();
            for (var i = 0; i < reader.FieldCount; i++)
                row[reader.GetName(i)] = Value(reader.IsDBNull(i) ? null : reader.GetValue(i));
            rows.Add(row);
        }

        return rows;
    }

    private Task<List<JsonObject>> ReadAsync(string sql, int serverId, DateTime from, DateTime to, CancellationToken cancellationToken) =>
        WithConnectionAsync(async connection =>
        {
            using var command = Command(connection, sql, serverId, from, to);
            return await ReadRowsAsync(command, cancellationToken).ConfigureAwait(false);
        }, cancellationToken);

    private Task<HashSet<string>> ReadCollectorRunsAsync(int serverId, DateTime since, CancellationToken cancellationToken) =>
        WithConnectionAsync(async connection =>
        {
            var ran = new HashSet<string>(StringComparer.Ordinal);
            using var command = Command(connection, CollectorRunsSql, serverId, since);
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                ran.Add(reader.GetString(0));
            return ran;
        }, cancellationToken);

    private async Task<SectionRead> ReadSectionAsync(
        string section, LiteEvidenceCandidate request, DateTime from, DateTime to, CancellationToken cancellationToken) =>
        section switch
        {
            "top_statements" => new SectionRead(await ReadTopStatementsAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "active_requests" => new SectionRead(await ReadAsync(ActiveRequestsSql, request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "query_store" => await ReadQueryStoreAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false),
            "jobs" => new SectionRead(await ReadAsync(JobsSql, request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "waits" => new SectionRead(await ReadAsync(WaitsSql, request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "deadlocks" => new SectionRead(await ReadDeadlocksAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "blocking_chains" => await ReadBlockingChainsAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false),
            "head_blocker" => new SectionRead(await ReadHeadBlockersAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "client_timeouts" => new SectionRead(await ReadClientTimeoutsAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            "regression_targets" => new SectionRead(await ReadRegressionTargetsAsync(request.ServerId, from, to, cancellationToken).ConfigureAwait(false)),
            _ => new SectionRead(new List<JsonObject>()),
        };

    /// <summary>The heaviest statements by CPU, each with the plans that ran it (the heaviest <see cref="PlansPerStatement"/> by CPU
    /// and the cheapest per execution), ranked here over one read of the per-plan sums (Darling ranks the same in SQL).</summary>
    private async Task<List<JsonObject>> ReadTopStatementsAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        var plans = await WithConnectionAsync(async connection =>
        {
            var list = new List<(string? Db, string? Hash, string? PlanHash, long Cpu, long Exec, long Elapsed, long Reads, DateTime FirstSeen)>();
            using var command = Command(connection, TopStatementsSql, serverId, from, to);
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                long L(int i) => reader.IsDBNull(i) ? 0L : Convert.ToInt64(reader.GetValue(i), CultureInfo.InvariantCulture);
                list.Add((reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2), L(3), L(4), L(5), L(6), reader.GetDateTime(7)));
            }

            return list;
        }, cancellationToken).ConfigureAwait(false);

        var top = plans.GroupBy(p => (p.Db, p.Hash))
            .Select(g => (g.Key, Cpu: g.Sum(p => p.Cpu), Exec: g.Sum(p => p.Exec), Elapsed: g.Sum(p => p.Elapsed), Reads: g.Sum(p => p.Reads),
                          PlanHash: g.Select(p => p.PlanHash).OrderBy(h => h, StringComparer.Ordinal).LastOrDefault(), Plans: g.ToList()))
            .Where(g => g.Cpu > 0)
            .OrderByDescending(g => g.Cpu).ThenBy(g => g.Key.Db, StringComparer.Ordinal).ThenBy(g => g.Key.Hash, StringComparer.Ordinal)
            .Take(TopStatements)
            .ToList();
        var rows = new List<JsonObject>();
        foreach (var t in top)
        {
            var heaviest = t.Plans.OrderByDescending(p => p.Cpu).ThenBy(p => p.PlanHash, StringComparer.Ordinal).Take(PlansPerStatement).ToList();
            var cheapest = t.Plans.OrderBy(p => p.Exec == 0 ? double.MaxValue : (double)p.Cpu / p.Exec).ThenBy(p => p.PlanHash, StringComparer.Ordinal).First();
            var kept = heaviest.Contains(cheapest) ? heaviest : heaviest.Append(cheapest).ToList();
            var planArray = new JsonArray();
            foreach (var p in kept.OrderBy(p => p.FirstSeen).ThenBy(p => p.PlanHash, StringComparer.Ordinal))
            {
                planArray.Add(new JsonObject
                {
                    ["query_plan_hash"] = p.PlanHash,
                    ["executions"] = p.Exec,
                    ["cpu_us"] = p.Cpu,
                    ["cpu_per_exec_us"] = p.Exec == 0 ? null : Math.Round((double)p.Cpu / p.Exec, 1),
                    ["first_seen"] = p.FirstSeen.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
                });
            }

            rows.Add(new JsonObject
            {
                ["database_name"] = t.Key.Db,
                ["query_hash"] = t.Key.Hash,
                ["query_plan_hash"] = t.PlanHash,
                ["total_cpu_us"] = t.Cpu,
                ["executions"] = t.Exec,
                ["total_elapsed_us"] = t.Elapsed,
                ["total_reads"] = t.Reads,
                ["plans"] = planArray,
            });
        }

        return rows;
    }

    private async Task<SectionRead> ReadQueryStoreAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        var windowCpu = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var row in await ReadAsync(WindowCpuByDatabaseSql, serverId, from, to, cancellationToken).ConfigureAwait(false))
        {
            if (row["database_name"] is JsonValue name && row["window_cpu_us"] is JsonValue cpu)
                windowCpu[name.GetValue<string>()] = cpu.GetValue<long>();
        }

        return await WithConnectionAsync(async connection =>
        {
            var all = new List<(string Name, long Cpu, JsonObject Row)>();
            using var command = Command(connection, QueryStoreHealthSql, serverId);
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var capturedAt = reader.GetDateTime(4);
                if (capturedAt < from - TimeSpan.FromHours(2))
                    return new SectionRead(new List<JsonObject>(), 0);
                var name = reader.IsDBNull(0) ? "" : reader.GetString(0);
                var cpu = windowCpu.TryGetValue(name, out var micros) ? micros : 0L;
                all.Add((name, cpu, new JsonObject
                {
                    ["database_name"] = reader.IsDBNull(0) ? null : name,
                    ["actual_state"] = reader.IsDBNull(1) ? null : reader.GetString(1),
                    ["desired_state"] = reader.IsDBNull(2) ? null : reader.GetString(2),
                    ["readonly_reason"] = reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture),
                    ["window_cpu_us"] = cpu,
                    ["capture_time"] = capturedAt.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
                }));
            }

            var kept = all.OrderByDescending(d => d.Cpu).ThenBy(d => d.Name, StringComparer.Ordinal)
                .Take(QueryStoreDatabasesPerCapture).Select(d => d.Row).ToList();
            return new SectionRead(kept, all.Count - kept.Count);
        }, cancellationToken).ConfigureAwait(false);
    }

    private Task<List<JsonObject>> ReadDeadlocksAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken) =>
        WithConnectionAsync(async connection =>
        {
            var rows = new List<JsonObject>();
            using var command = Command(connection, DeadlocksSql, serverId, from, to);
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var graph = SensitiveStatements.Xml(reader.GetString(3)) ?? "";
                var model = DeadlockGraphParser.Parse(graph);
                var parties = new JsonArray();
                var kept = model.Processes.OrderByDescending(p => p.IsVictim).Take(DeadlockPartiesMax).ToList();
                foreach (var p in kept)
                {
                    parties.Add(new JsonObject
                    {
                        ["process_id"] = p.Id,
                        ["spid"] = p.Spid,
                        ["is_victim"] = p.IsVictim,
                        ["database_name"] = p.DatabaseName,
                        ["proc_name"] = p.ProcName,
                        ["lock_mode"] = p.LockMode,
                        ["wait_resource"] = p.WaitResource,
                        ["contentious_object"] = p.ContentiousObject,
                        ["isolation_level"] = p.IsolationLevel,
                        ["login_name"] = p.LoginName,
                        ["host_name"] = p.HostName,
                        ["client_app"] = p.ClientApp,
                        ["sql_text"] = Cut(p.SqlText, DeadlockStatementMaxChars),
                    });
                }

                var objects = DeadlockObjects(graph);
                var objectsDropped = Math.Max(0, objects.Count - DeadlockObjectsMax);
                for (var i = 0; i < objectsDropped; i++)
                    objects.RemoveAt(objects.Count - 1);
                var row = new JsonObject
                {
                    ["deadlock_time"] = Value(reader.GetDateTime(0)),
                    ["database_name"] = reader.IsDBNull(1) ? null : reader.GetString(1),
                    ["victim_process_id"] = reader.IsDBNull(2) ? null : reader.GetString(2),
                    ["is_parallel"] = model.IsParallel,
                    ["objects"] = objects,
                    ["parties"] = parties,
                };
                if (objectsDropped > 0)
                    row["objects_dropped"] = objectsDropped;
                if (model.Processes.Count > kept.Count)
                    row["parties_dropped"] = model.Processes.Count - kept.Count;
                rows.Add(row);
            }

            return rows;
        }, cancellationToken);

    private Task<List<BlockedReport>> ReadBlockedReportsAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken) =>
        WithConnectionAsync(async connection =>
        {
            var reports = new List<BlockedReport>();
            using var command = Command(connection, BlockedReportsSql, serverId, from, to);
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                string? S(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
                int I(int i) => reader.IsDBNull(i) ? 0 : Convert.ToInt32(reader.GetValue(i), CultureInfo.InvariantCulture);
                reports.Add(new BlockedReport(
                    reader.GetDateTime(0), S(1), I(2), I(3), I(4), I(5),
                    reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture),
                    S(7), S(8), S(9), S(10), S(11), S(12), S(13), S(14), S(15), S(16), S(17), S(18)));
            }

            return reports;
        }, cancellationToken);

    private async Task<SectionRead> ReadBlockingChainsAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        var reports = await ReadBlockedReportsAsync(serverId, from, to, cancellationToken).ConfigureAwait(false);
        var blockedBy = BlockedBy(reports);
        var chains = reports
            .GroupBy(r => (r.BlockedSpid, r.BlockingSpid))
            .Select(g => g.OrderByDescending(r => r.WaitMs).ThenByDescending(r => r.EventTime).First())
            .OrderByDescending(r => r.WaitMs).ThenBy(r => r.BlockedSpid).ThenBy(r => r.BlockingSpid)
            .Take(SectionRows)
            .ToList();
        var rows = chains.ConvertAll(r => new JsonObject
        {
            ["event_time"] = Value(r.EventTime),
            ["database_name"] = r.Database,
            ["blocked_spid"] = r.BlockedSpid,
            ["blocked_ecid"] = r.BlockedEcid,
            ["blocking_spid"] = r.BlockingSpid,
            ["blocking_ecid"] = r.BlockingEcid,
            ["head_blocker_spid"] = HeadOf(r.BlockedSpid, blockedBy),
            ["wait_time_ms"] = r.WaitMs,
            ["wait_resource"] = r.WaitResource,
            ["lock_mode"] = r.LockMode,
            ["contentious_object"] = r.ContentiousObject,
            ["blocked_sql_text"] = r.BlockedSql,
            ["blocking_sql_text"] = r.BlockingSql,
            ["blocked_login_name"] = r.BlockedLogin,
            ["blocking_login_name"] = r.BlockingLogin,
            ["blocked_host_name"] = r.BlockedHost,
            ["blocking_host_name"] = r.BlockingHost,
            ["report_xml"] = null,
        });
        return new SectionRead(rows, ReportXml: chains.ConvertAll(r => SensitiveStatements.Xml(r.ReportXml)));
    }

    private async Task<List<JsonObject>> ReadHeadBlockersAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        var reports = await ReadBlockedReportsAsync(serverId, from, to, cancellationToken).ConfigureAwait(false);
        var blockedBy = BlockedBy(reports);
        var heads = reports
            .GroupBy(r => HeadOf(r.BlockedSpid, blockedBy))
            .Select(g => (Newest: reports.Where(r => r.BlockingSpid == g.Key).OrderByDescending(r => r.EventTime).FirstOrDefault(),
                          Blocked: g.Count(),
                          MaxWait: g.Max(r => r.WaitMs)))
            .Where(h => h.Newest is not null && h.Newest.BlockingSpid != 0)
            .OrderByDescending(h => h.Blocked).ThenByDescending(h => h.MaxWait).ThenBy(h => h.Newest!.BlockingSpid)
            .Take(SectionRows)
            .Select(h => (Newest: h.Newest!, h.Blocked, h.MaxWait,
                          Process: BlockingProcess(SensitiveStatements.Xml(h.Newest!.ReportXml))))
            .ToList();

        var statements = new Dictionary<(string Handle, int Start, int End), string>();
        foreach (var frame in heads.Where(h => h.Process.SqlHandle is not null).Select(h => h.Process).Distinct())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var text = await WithConnectionAsync(async connection =>
            {
                using var command = Command(connection, StatementByOffsetSql, serverId, frame.SqlHandle, frame.StmtStart, frame.StmtEnd, from - StatementLookback);
                return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string;
            }, cancellationToken).ConfigureAwait(false);
            if (text is not null)
                statements[(frame.SqlHandle!, frame.StmtStart, frame.StmtEnd)] = text;
        }

        var rows = new List<JsonObject>();
        foreach (var (newest, blocked, maxWait, process) in heads)
        {
            string? statement = null;
            if (process.SqlHandle is not null)
                statements.TryGetValue((process.SqlHandle, process.StmtStart, process.StmtEnd), out statement);

            var exact = statement is not null;
            rows.Add(new JsonObject
            {
                ["spid"] = newest.BlockingSpid,
                ["ecid"] = newest.BlockingEcid,
                ["database_name"] = newest.Database,
                ["reports_in_chain"] = blocked,
                ["max_wait_time_ms"] = maxWait,
                ["status"] = newest.BlockingStatus ?? process.Status,
                ["login_name"] = newest.BlockingLogin,
                ["host_name"] = newest.BlockingHost,
                ["client_app"] = newest.BlockingApp,
                ["sql_handle"] = process.SqlHandle,
                ["stmtstart"] = process.SqlHandle is null ? null : process.StmtStart,
                ["stmtend"] = process.SqlHandle is null ? null : process.StmtEnd,
                ["statement_source"] = exact ? "exact" : "inferred",
                ["statement_text"] = exact ? statement : process.InputBuffer ?? newest.BlockingSql,
                ["input_buffer"] = process.InputBuffer,
                ["report_time"] = Value(newest.EventTime),
            });
        }

        return rows;
    }

    private async Task<List<JsonObject>> ReadRegressionTargetsAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken)
    {
        var targets = await WithConnectionAsync(async connection =>
        {
            using var command = Command(connection, RegressionTargetsSql, serverId, to - RegressionLookback, from, to, (double)RegressionFactor, SectionRows);
            return await ReadRowsAsync(command, cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);

        foreach (var target in targets)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var database = target["database_name"]?.GetValue<string>() ?? "";
            var good = target["good_plan_id"]!.GetValue<long>();
            var bad = target["bad_plan_id"]!.GetValue<long>();
            var plans = await WithConnectionAsync(async connection =>
            {
                var found = new Dictionary<long, string?>();
                using var command = Command(connection, PlanTextSql, serverId, database, good, bad);
                using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    found.TryAdd(Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture), reader.GetString(1));
                return found;
            }, cancellationToken).ConfigureAwait(false);
            target["good_compiled"] = Compiled(SensitiveStatements.Xml(plans.GetValueOrDefault(good)));
            cancellationToken.ThrowIfCancellationRequested();
            target["bad_compiled"] = Compiled(SensitiveStatements.Xml(plans.GetValueOrDefault(bad)));
            target["stopped_running"] = target["window_executions"]?.GetValue<long>() == 0;
        }

        return targets;
    }

    /// <summary>A section's grace, ended no later than <see cref="HorizonMargin"/> before the request's
    /// <see cref="CaptureHorizon"/>: the grace counts from <paramref name="graceStart"/>, which is after the open for a metric
    /// that joined late (#6002 review).</summary>
    internal static TimeSpan CappedGrace(TimeSpan grace, DateTime graceStart, DateTime openedAt)
    {
        var latest = openedAt + CaptureHorizon - HorizonMargin - graceStart;
        if (latest < TimeSpan.Zero)
            latest = TimeSpan.Zero;
        return grace > latest ? latest : grace;
    }

    /// <summary>The content of a section whose read failed past its grace: no rows, <c>status: unavailable</c>, and the error's
    /// class name only.</summary>
    internal static string Unavailable(DateTime from, DateTime to, Exception error) => new JsonObject
    {
        ["window_start"] = from.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
        ["window_end"] = to.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
        ["rows"] = new JsonArray(),
        ["status"] = "unavailable",
        ["error"] = error.GetType().Name,
    }.ToJsonString();

    /// <summary>
    /// #4348 (#6002 review): each row through <see cref="DerivedStoreScrub.Json"/> BEFORE it is fitted, so the bytes the cap
    /// measures are the bytes stored (the store runs the same scrub at the write, and finds these rows clean). A row the filter
    /// cannot read is kept as <c>{"withheld": true}</c>, so the rows stay aligned with any report XML beside them.
    /// </summary>
    internal static List<JsonObject> ScrubRows(List<JsonObject> rows, CancellationToken cancellationToken = default) =>
        rows.ConvertAll(row =>
        {
            /* #6002 review round 2: the statement filter is synchronous work (up to its own 10 s per call), so a section past its
               deadline stops between rows instead of finishing every row first. */
            cancellationToken.ThrowIfCancellationRequested();
            var json = row.ToJsonString();
            var scrubbed = DerivedStoreScrub.Json(json);
            if (ReferenceEquals(scrubbed, json))
                return row;
            return scrubbed is null ? new JsonObject { ["withheld"] = true } : JsonNode.Parse(scrubbed)!.AsObject();
        });

    /// <summary>
    /// The sections <paramref name="request"/> gets (#6002), each with the alert time its window is built around: the sections of
    /// every linked metric in <see cref="SectionsByKey"/> (at that metric's first linked alert; the opening metric at the
    /// request's first alert) and of its family (at the first alert). A section two keys bring (the two blocking metrics) is
    /// listed once, at the earlier alert. High CPU's sections come first, in their written order.
    /// </summary>
    internal static IReadOnlyList<(string Section, DateTime Anchor)> SectionsOf(LiteEvidenceCandidate request)
    {
        var anchors = new Dictionary<string, DateTime>(StringComparer.Ordinal);
        var order = new List<string>();
        void Add(string key, DateTime anchor)
        {
            if (!SectionsByKey.TryGetValue(key, out var sections))
                return;
            foreach (var section in sections)
            {
                if (!anchors.TryGetValue(section, out var seen))
                {
                    order.Add(section);
                    anchors[section] = anchor;
                }
                else if (anchor < seen)
                {
                    anchors[section] = anchor;
                }
            }
        }

        Add(request.FirstMetric, request.FirstAlertAtUtc);
        foreach (var key in SectionsByKey.Keys)
        {
            if (request.Metrics.TryGetValue(key, out var first))
                Add(key, string.Equals(key, request.FirstMetric, StringComparison.Ordinal) ? request.FirstAlertAtUtc : first);
        }

        Add("family:" + request.Family, request.FirstAlertAtUtc);
        return order.ConvertAll(s => (s, anchors[s]));
    }

    private static JsonValue? Value(object? value) => value switch
    {
        null => null,
        string text => JsonValue.Create(text),
        bool flag => JsonValue.Create(flag),
        byte or sbyte or short or ushort or int or uint or long or ulong => JsonValue.Create(Convert.ToInt64(value, CultureInfo.InvariantCulture)),
        float or double or decimal => JsonValue.Create(Convert.ToDouble(value, CultureInfo.InvariantCulture)),
        DateTime time => JsonValue.Create(time.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture)),
        byte[] bytes => JsonValue.Create(Convert.ToHexString(bytes)),
        System.Numerics.BigInteger big => JsonValue.Create((long)big),
        _ => JsonValue.Create(Convert.ToString(value, CultureInfo.InvariantCulture)),
    };

    /// <summary>The bytes <paramref name="json"/> takes as PostgreSQL prints a jsonb (<c>octet_length(content::text)</c>, which is
    /// what the table's check holds to the cap): PostgreSQL puts a space after every structural colon and comma, so each
    /// colon and comma is counted twice, strings included, which only over-counts.</summary>
    internal static int StoredBytes(string json)
    {
        var extra = 0;
        foreach (var c in json)
        {
            if (c == ':' || c == ',')
                extra++;
        }

        return Encoding.UTF8.GetByteCount(json) + extra;
    }

    /// <summary>
    /// The section's content as JSON held to <see cref="MaxSectionBytes"/>: <c>window_start</c>, <c>window_end</c>, the rows and,
    /// when rows had to go, <c>rows_dropped</c>. Rows come off the tail (the lowest-ranked) until it fits. A section with no rows
    /// is a true "nothing found": an empty list. <paramref name="databasesDropped"/> (the <c>query_store</c> section's count of
    /// databases the cap left out) is written as <c>databases_dropped</c>, 0 included, when the section has one.
    /// </summary>
    internal static string Fit(DateTime from, DateTime to, List<JsonObject> rows, int? databasesDropped = null)
    {
        var dropped = 0;
        while (true)
        {
            var content = new JsonObject
            {
                ["window_start"] = from.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
                ["window_end"] = to.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture),
                ["rows"] = new JsonArray(rows.GetRange(0, rows.Count - dropped).ConvertAll(r => (JsonNode?)r.DeepClone()).ToArray()),
            };
            /* #6002: a ready section that found nothing says so: a frozen true negative, never a dropped section. */
            if (rows.Count == 0)
                content["status"] = "empty";
            if (dropped > 0)
                content["rows_dropped"] = dropped;
            if (databasesDropped is not null)
                content["databases_dropped"] = databasesDropped;
            var json = content.ToJsonString();
            if (StoredBytes(json) <= MaxSectionBytes || dropped >= rows.Count)
                return json;
            dropped++;
        }
    }

    /// <summary>The objects and indexes a deadlock graph's resources name, once each: (resource kind, object name, index
    /// name, lock mode), in graph order. A graph that does not parse names none.</summary>
    internal static JsonArray DeadlockObjects(string? graphXml)
    {
        var objects = new JsonArray();
        var root = ParseXml(graphXml);
        if (root is null)
            return objects;

        var seen = new HashSet<(string, string?, string?)>();
        foreach (var list in root.DescendantsAndSelf().Where(e => e.Name.LocalName == "resource-list"))
        {
            foreach (var resource in list.Elements())
            {
                var objectName = (string?)resource.Attribute("objectname");
                var indexName = (string?)resource.Attribute("indexname");
                if (string.IsNullOrWhiteSpace(objectName) && string.IsNullOrWhiteSpace(indexName))
                    continue;
                var kind = resource.Name.LocalName;
                if (!seen.Add((kind, objectName, indexName)))
                    continue;
                objects.Add(new JsonObject
                {
                    ["kind"] = kind,
                    ["object_name"] = objectName,
                    ["index_name"] = indexName,
                    ["mode"] = (string?)resource.Attribute("mode"),
                });
            }
        }

        return objects;
    }

    /// <summary>One blocked-process report as the two blocking sections read it.</summary>
    private sealed record BlockedReport(
        DateTime EventTime, string? Database, int BlockedSpid, int BlockedEcid, int BlockingSpid, int BlockingEcid, long WaitMs,
        string? WaitResource, string? LockMode, string? ContentiousObject, string? BlockedSql, string? BlockingSql,
        string? BlockedLogin, string? BlockingLogin, string? BlockedHost, string? BlockingHost, string? BlockingApp,
        string? BlockingStatus, string? ReportXml);

    /// <summary>The session at the head of <paramref name="spid"/>'s chain: follow blocked to blocking through the window's
    /// reports (the newest report per blocked session) until a session that is not itself blocked; a cycle stops where it
    /// closes.</summary>
    private static int HeadOf(int spid, Dictionary<int, int> blockedBy)
    {
        var seen = new HashSet<int> { spid };
        var head = spid;
        while (blockedBy.TryGetValue(head, out var next) && next != 0 && seen.Add(next))
            head = next;
        return head;
    }

    /// <summary>
    /// The <c>blocking_chains</c> content (#6002): the rows are placed first, by <see cref="Fit"/> with no report XML, and each kept
    /// row's report XML then takes what is left of <see cref="MaxSectionBytes"/>, in row order (longest wait first). A report that
    /// does not fit whole is cut at the cap and its row says <c>report_xml_truncated: true</c>; once the cap is spent, every later
    /// report is cut to nothing and says so. A report is never left out silently and never cut at a fixed length. A cut report no
    /// longer parses as XML, so under <c>maskLiterals</c> or <c>metadataOnly</c> the masker withholds it whole.
    /// <para>#6002 review: every row with a report first takes its floor, <c>report_xml: ""</c> and <c>report_xml_truncated:
    /// true</c>, so a report placed earlier can never leave a later row's flag over the cap (the store refuses the whole write
    /// then). When the floors themselves do not fit, rows come off the tail, counted in <c>rows_dropped</c>.</para>
    /// </summary>
    internal static string FitReportXml(DateTime from, DateTime to, List<JsonObject> rows, IReadOnlyList<string?> reportXml)
    {
        var content = JsonNode.Parse(Fit(from, to, rows))!.AsObject();
        var kept = content["rows"]!.AsArray();
        for (var i = 0; i < kept.Count && i < reportXml.Count; i++)
        {
            if (string.IsNullOrEmpty(reportXml[i]))
                continue;
            kept[i]!.AsObject()["report_xml"] = "";
            kept[i]!.AsObject()["report_xml_truncated"] = true;
        }

        while (kept.Count > 0 && FittedBytes(content) > MaxSectionBytes)
        {
            kept.RemoveAt(kept.Count - 1);
            content["rows_dropped"] = (content["rows_dropped"]?.GetValue<int>() ?? 0) + 1;
        }

        for (var i = 0; i < kept.Count && i < reportXml.Count; i++)
        {
            var xml = reportXml[i];
            if (string.IsNullOrEmpty(xml))
                continue;
            var row = kept[i]!.AsObject();
            row.Remove("report_xml_truncated");
            row["report_xml"] = xml;
            if (FittedBytes(content) <= MaxSectionBytes)
                continue;

            /* The longest prefix that fits, by halving: the bytes a prefix takes grow with its length. The floor ("") is known
               to fit, so the search always ends inside the cap. */
            row["report_xml_truncated"] = true;
            int low = 0, high = xml.Length - 1;
            while (low < high)
            {
                var mid = (low + high + 1) / 2;
                row["report_xml"] = Prefix(xml, mid);
                if (FittedBytes(content) <= MaxSectionBytes)
                    low = mid;
                else
                    high = mid - 1;
            }

            row["report_xml"] = Prefix(xml, low);
        }

        return content.ToJsonString(Relaxed);
    }

    /// <summary>XML kept as written (<c>&lt;</c>, not its <c><</c> escape), as the jsonb column prints it, so the cap counts
    /// what is stored.</summary>
    private static readonly JsonSerializerOptions Relaxed = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    private static int FittedBytes(JsonObject content) => StoredBytes(content.ToJsonString(Relaxed));

    /// <summary>The first <paramref name="length"/> characters, one fewer when that would split a surrogate pair.</summary>
    private static string Prefix(string text, int length) =>
        length > 0 && char.IsHighSurrogate(text[length - 1]) ? text[..(length - 1)] : text[..length];

    /// <summary>The newest report's blocking session per blocked session.</summary>
    private static Dictionary<int, int> BlockedBy(IEnumerable<BlockedReport> reports)
    {
        var blockedBy = new Dictionary<int, int>();
        foreach (var r in reports.OrderByDescending(r => r.EventTime))
            blockedBy.TryAdd(r.BlockedSpid, r.BlockingSpid);
        return blockedBy;
    }

    /// <summary>What the head blocker's statement is resolved from: the blocking process's top execution frame (its sql
    /// handle in the collector's spelling, and the offsets, 0 and -1 when the frame leaves them out) and its input buffer.</summary>
    internal sealed record BlockingProcessFrame(string? SqlHandle, int StmtStart, int StmtEnd, string? InputBuffer, string? Status);

    /// <summary>Reads <see cref="BlockingProcessFrame"/> from a blocked-process report. A frame without a usable handle (absent,
    /// or all zeros as SQL Server writes for a frame it cannot name) counts as no frame.</summary>
    internal static BlockingProcessFrame BlockingProcess(string? reportXml)
    {
        var root = ParseXml(reportXml);
        var process = root?.DescendantsAndSelf().FirstOrDefault(e => e.Name.LocalName == "blocking-process")?
            .Elements().FirstOrDefault(e => e.Name.LocalName == "process");
        if (process is null)
            return new BlockingProcessFrame(null, 0, -1, null, null);

        var inputBuffer = process.Elements().FirstOrDefault(e => e.Name.LocalName == "inputbuf")?.Value.Trim();
        var status = (string?)process.Attribute("status");
        var frame = process.Elements().FirstOrDefault(e => e.Name.LocalName == "executionStack")?
            .Elements().FirstOrDefault(e => e.Name.LocalName == "frame");
        var handle = NormalizeHandle((string?)frame?.Attribute("sqlhandle"));
        var start = int.TryParse((string?)frame?.Attribute("stmtstart"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var s) ? s : 0;
        var end = int.TryParse((string?)frame?.Attribute("stmtend"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var e2) ? e2 : -1;
        return new BlockingProcessFrame(handle, start, end, string.IsNullOrEmpty(inputBuffer) ? null : inputBuffer, status);
    }

    /// <summary>A report's sql handle (<c>0x0200…</c>, lower-case hex) in the spelling <c>query_stats</c> stores
    /// (<c>CONVERT(varchar, sql_handle, 1)</c>: <c>0x</c> then upper-case hex); null for no handle or an all-zero one.</summary>
    internal static string? NormalizeHandle(string? handle)
    {
        if (string.IsNullOrWhiteSpace(handle) || !handle.StartsWith("0x", StringComparison.OrdinalIgnoreCase) || handle.Length <= 2)
            return null;
        var hex = handle[2..];
        if (hex.All(c => c == '0'))
            return null;
        return "0x" + hex.ToUpperInvariant();
    }

    /// <summary>A plan's compiled values as <c>[{parameter, compiled_value}]</c>, or null when the plan is missing or unreadable.</summary>
    private static JsonArray? Compiled(string? planXml)
    {
        var values = PlanInputComparison.CompiledValues(planXml);
        if (values is null)
            return null;
        var array = new JsonArray();
        foreach (var (parameter, value) in values)
            array.Add(new JsonObject { ["parameter"] = parameter, ["compiled_value"] = value });
        return array;
    }

    /// <summary>
    /// <c>client_timeouts</c> (#6553), the twin of Darling's section: the count of the window's event_counter rows (exact), and the
    /// <c>top_client_apps</c>, <c>top_hosts</c>, <c>top_databases</c> and <c>top_statements</c> (a sample text each, cut to
    /// <see cref="ClientTimeoutStatementMaxChars"/>, field <c>sql_text</c> so the statement-class masking covers it) from the shared
    /// <see cref="AttentionWindowSql"/> statements over <c>v_attention_events</c>. One row; a window with no count and no sampled
    /// event is a true negative: an empty list.
    /// </summary>
    private Task<List<JsonObject>> ReadClientTimeoutsAsync(int serverId, DateTime from, DateTime to, CancellationToken cancellationToken) =>
        WithConnectionAsync(async connection =>
        {
            async Task<List<T>> Query<T>(string sql, Func<DuckDBDataReader, T> map)
            {
                using var command = Command(connection, sql, serverId, from, to);
                var result = new List<T>();
                using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    result.Add(map((DuckDBDataReader)reader));
                return result;
            }

            var count = (await Query(AttentionWindowSql.Counts("v_attention_events", bounded: true), r => r.GetInt64(1)).ConfigureAwait(false)).Sum();
            var apps = await Query(AttentionWindowSql.TopBy("v_attention_events", "client_app_name", bounded: true), r => (Name: r.GetString(0), Events: r.GetInt64(1))).ConfigureAwait(false);
            var hosts = await Query(AttentionWindowSql.TopBy("v_attention_events", "client_hostname", bounded: true), r => (Name: r.GetString(0), Events: r.GetInt64(1))).ConfigureAwait(false);
            var databases = await Query(AttentionWindowSql.TopBy("v_attention_events", "database_name", bounded: true), r => (Name: r.GetString(0), Events: r.GetInt64(1))).ConfigureAwait(false);
            var statements = await Query(AttentionWindowSql.TopStatements("v_attention_events", bounded: true),
                r => (Fingerprint: r.GetString(0), Events: r.GetInt64(1), Text: r.IsDBNull(2) ? null : r.GetString(2))).ConfigureAwait(false);
            if (count == 0 && apps.Count == 0 && hosts.Count == 0 && databases.Count == 0 && statements.Count == 0)
                return new List<JsonObject>();

            static JsonArray Named(IEnumerable<(string Name, long Events)> items, string key) =>
                new(items.Select(i => (JsonNode?)new JsonObject { [key] = i.Name, ["sampled_events"] = i.Events }).ToArray());

            return new List<JsonObject>
            {
                new()
                {
                    ["count"] = count,
                    ["top_client_apps"] = Named(apps, "client_app_name"),
                    ["top_hosts"] = Named(hosts, "client_hostname"),
                    ["top_databases"] = Named(databases, "database_name"),
                    ["top_statements"] = new JsonArray(statements.Select(st => (JsonNode?)new JsonObject
                    {
                        ["statement_fingerprint"] = st.Fingerprint,
                        ["sampled_events"] = st.Events,
                        ["sql_text"] = Cut(st.Text, ClientTimeoutStatementMaxChars),
                    }).ToArray()),
                    ["note"] = "count is exact (the session's event_counter). The lists come from sampled ring-buffer events, a subset: they show who and what was timing out, never how many.",
                },
            };
        }, cancellationToken);

    /* #6579: cut at a text-element boundary, like ClientTimeoutsRead, so a surrogate pair or combining sequence is never split. */
    private static string? Cut(string? text, int max) =>
        string.IsNullOrEmpty(text) ? null : text.Length <= max ? text : text[..McpHelpers.TextElementCutLength(text, max)];

    /// <summary>Parses collected XML with DTDs refused; null for none or XML that does not parse.</summary>
    private static XElement? ParseXml(string? xml)
    {
        if (string.IsNullOrWhiteSpace(xml))
            return null;
        try
        {
            using var text = new StringReader(xml);
            using var reader = XmlReader.Create(text, new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit, XmlResolver = null });
            return XElement.Load(reader);
        }
        catch (XmlException)
        {
            return null;
        }
    }
}
