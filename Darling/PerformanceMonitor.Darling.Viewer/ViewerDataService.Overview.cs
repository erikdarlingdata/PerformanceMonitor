/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Media;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Overview server-cards read (W2a viewer copy-parity), copied from Lite's
/// <c>LocalDataService.Overview.cs</c> (GetServerSummaryAsync + ServerSummaryItem) and rewired to the
/// Darling Postgres store, then ENRICHED toward the Dashboard's richer <c>ServerHealthCard</c>
/// (Dashboard/Controls/ServerHealthCard.xaml + Dashboard/Models/ServerHealthStatus.cs). Alongside Lite's
/// original five per-server reads — latest CPU (SQL + other), latest total server memory, blocking count
/// in the last hour (XE blocked-process reports, falling back to the always-on DMV snapshot when the XE
/// count is zero), deadlock count in the last hour, and the last collection time — the card now also
/// surfaces the Dashboard's <b>Threads</b> row (worker-thread pressure from the latest
/// <c>cpu_scheduler_stats</c> snapshot), <b>Collectors</b> row (healthy / failing counts reusing the
/// viewer's own <see cref="CollectorHealthRow.HealthStatus"/> banding), a richer <b>Memory</b> signal
/// (resource-semaphore waiters / timeouts / forced grants from <c>memory_grant_stats</c>, not just total
/// MB), and a <b>Blocking</b> duration (max wait in the window). Every metric drives a per-row severity
/// band that mirrors <c>ServerHealthStatus</c>'s deterministic CASE logic. All reads run over the same
/// <c>v_*</c> passthrough views the other viewer tabs read; the SQL lives in public constants so tests can
/// pin the load-bearing clauses without a live Postgres.
///
/// <para><b>The viewer adaptations (#1262 headless plan).</b> (1) Lite's <c>IsOnline</c> comes from a live
/// per-server connection ping; the viewer has no live connection to the monitored servers, so it derives
/// the card's status from COLLECTION FRESHNESS instead — how old the newest <c>v_collection_log</c> row
/// is (see <see cref="ServerSummaryItem.ClassifyFreshness"/>): fresh → Online (green), stale (older than
/// twice the fastest collector's cadence) → Warning (amber), and no collection / long-dead → Offline
/// (the red overlay). (2) The Dashboard's health card live-queries currently-blocked sessions / current
/// scheduler state; the viewer reads the newest COLLECTED snapshot instead (the freshness band already
/// answers "is this server reporting"). The severity BANDS themselves are reproduced verbatim from
/// <c>ServerHealthStatus</c> so a viewer card colours a metric exactly as the Dashboard would.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>Latest SQL + other-process CPU for one server (newest ring-buffer sample). $1 server_id.</summary>
    public const string ServerSummaryCpuSql = @"
SELECT sqlserver_cpu_utilization, other_process_cpu_utilization
FROM v_cpu_utilization_stats
WHERE server_id = $1
ORDER BY sample_time DESC
LIMIT 1";

    /// <summary>Latest total server memory + buffer pool (MB) for one server. $1 server_id.</summary>
    public const string ServerSummaryMemorySql = @"
SELECT
    CAST(total_server_memory_mb AS double precision),
    CAST(buffer_pool_mb AS double precision)
FROM v_memory_stats
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>
    /// The latest resource-semaphore pressure for one server — the workspace-memory signal the Dashboard's
    /// <c>get_resource_semaphore</c> / <c>ServerHealthStatus.MemorySeverity</c> reads: grant waiters,
    /// timeout-error and forced-grant deltas, and total granted MB, summed across every pool at the newest
    /// collection instant. The Dashboard live-sums <c>sys.dm_exec_query_resource_semaphores.waiter_count</c>
    /// (WHERE max_target_memory_kb IS NOT NULL, which the <c>memory_grant_stats</c> collector already
    /// applies at capture); the viewer sums the collected per-pool rows at MAX(collection_time). $1 server_id.
    /// </summary>
    public const string ServerSummaryMemoryPressureSql = @"
SELECT
    CAST(COALESCE(SUM(waiter_count), 0) AS bigint),
    CAST(COALESCE(SUM(timeout_error_count_delta), 0) AS bigint),
    CAST(COALESCE(SUM(forced_grant_count_delta), 0) AS bigint),
    CAST(COALESCE(SUM(granted_memory_mb), 0) AS double precision)
FROM v_memory_grant_stats
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM v_memory_grant_stats WHERE server_id = $1)";

    /// <summary>
    /// The latest worker-thread pressure for one server — the Dashboard's Threads row inputs
    /// (<c>ServerHealthStatus.ThreadsSeverity</c>) from the newest <c>cpu_scheduler_stats</c> snapshot:
    /// total worker ceiling (<c>max_workers_count</c>), workers in use (<c>total_current_workers_count</c> —
    /// available = ceiling − in-use, the same figure the collector's own worker-thread-exhaustion warning
    /// bands on at 90%), runnable tasks waiting for CPU (<c>total_runnable_tasks_count</c>), and requests
    /// starved of a worker (<c>total_work_queue_count</c>). A point-in-time snapshot collector, so the
    /// newest row is the current state. NULL/absent on Azure SQL DB (the collector does not apply there),
    /// which the card renders as "--". $1 server_id.
    /// </summary>
    public const string ServerSummaryThreadsSql = @"
SELECT
    max_workers_count,
    total_current_workers_count,
    total_runnable_tasks_count,
    total_work_queue_count
FROM v_cpu_scheduler_stats
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>
    /// Blocking in the window with its worst wait, plus the newest blocking event ever — XE blocked-process
    /// reports preferred, the always-on DMV blocking snapshot as fallback (AWS RDS has no XE), keeping
    /// Lite's XE→DMV fallback but returning the COUNT and the MAX wait (ms) from the SAME source so the
    /// card's count and its "max: Ns" duration can never come from different feeds. The last two columns are
    /// each source's newest event_time (unbounded — the same <c>MAX(event_time) WHERE server_id</c> read the
    /// collector runs every cycle for its watermark), for the Dashboard's "Last: N ago" detail when the
    /// window is clear. The caller applies the fallback in C# (identical to Lite's
    /// <c>COALESCE(NULLIF(xe,0), dmv)</c>: use XE when it has any row, else DMV). $1 server_id, $2 window
    /// start (naive UTC).
    /// </summary>
    public const string ServerSummaryBlockingSql = @"
SELECT
    (SELECT COUNT(*)          FROM v_blocked_process_reports WHERE server_id = $1 AND event_time >= $2),
    (SELECT MAX(wait_time_ms) FROM v_blocked_process_reports WHERE server_id = $1 AND event_time >= $2),
    (SELECT COUNT(*)          FROM v_dmv_blocking_snapshots   WHERE server_id = $1 AND event_time >= $2),
    (SELECT MAX(wait_time_ms) FROM v_dmv_blocking_snapshots   WHERE server_id = $1 AND event_time >= $2),
    (SELECT MAX(event_time)   FROM v_blocked_process_reports WHERE server_id = $1),
    (SELECT MAX(event_time)   FROM v_dmv_blocking_snapshots   WHERE server_id = $1)";

    /// <summary>
    /// Deadlock count in the window plus the newest deadlock ever — the windowed count for the card value,
    /// and the unbounded MAX(deadlock_time) for the Dashboard's "Last: N ago" detail. $1 server_id, $2
    /// window start (naive UTC).
    /// </summary>
    public const string ServerSummaryDeadlockSql = @"
SELECT
    (SELECT COUNT(*)           FROM v_deadlocks WHERE server_id = $1 AND deadlock_time >= $2),
    (SELECT MAX(deadlock_time) FROM v_deadlocks WHERE server_id = $1)";

    /// <summary>Newest collection time across all collectors for one server. $1 server_id.</summary>
    public const string ServerSummaryLastCollectionSql = @"
SELECT MAX(collection_time)
FROM v_collection_log
WHERE server_id = $1";

    /// <summary>
    /// One server's Overview-card summary — Lite's <c>GetServerSummaryAsync</c> ported to Postgres and
    /// enriched toward the Dashboard's <c>ServerHealthCard</c>. The caller sets
    /// <see cref="ServerSummaryItem.ServerName"/> and applies the freshness-derived status
    /// (<see cref="ServerSummaryItem.ApplyFreshness"/>) after the read, exactly where Lite set IsOnline
    /// from the live ping. Blocking / deadlock counts use a one-hour window (Lite's window); the Threads /
    /// Memory-pressure reads take the newest snapshot; the Collectors row REUSES the viewer's 7-day
    /// <see cref="GetCollectionHealthAsync"/> banding.
    /// </summary>
    public async Task<ServerSummaryItem> GetServerSummaryAsync(int serverId, string displayName, CancellationToken cancellationToken = default)
    {
        var nowUtc = DateTime.UtcNow;
        var windowStart = DateTime.SpecifyKind(nowUtc.AddHours(-1), DateTimeKind.Unspecified);

        double? cpuPercent = null;
        double? otherProcessCpuPercent = null;
        double? memoryMb = null;
        double? bufferPoolMb = null;
        var blockingCount = 0;
        long maxBlockingWaitMs = 0;
        int? lastBlockingMinutesAgo = null;
        var deadlockCount = 0;
        int? lastDeadlockMinutesAgo = null;
        DateTime? lastCollection = null;

        int? totalThreads = null;
        int? currentWorkers = null;
        var threadsWaitingForCpu = 0;
        long requestsWaitingForThreads = 0;

        long memoryWaiterCount = 0;
        long memoryTimeoutCount = 0;
        long memoryForcedCount = 0;
        double? grantedMemoryMb = null;

        /* Latest CPU — SQL and other-process, so the card can show total non-idle CPU with the SQL-only
           number alongside (Lite's headline). */
        await using (var command = _dataSource.CreateCommand(ServerSummaryCpuSql))
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                cpuPercent = reader.IsDBNull(0) ? null : Convert.ToDouble(reader.GetValue(0));
                otherProcessCpuPercent = reader.IsDBNull(1) ? null : Convert.ToDouble(reader.GetValue(1));
            }
        }

        /* Latest total server memory + buffer pool. */
        await using (var command = _dataSource.CreateCommand(ServerSummaryMemorySql))
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                memoryMb = reader.IsDBNull(0) ? null : Convert.ToDouble(reader.GetValue(0));
                bufferPoolMb = reader.IsDBNull(1) ? null : Convert.ToDouble(reader.GetValue(1));
            }
        }

        /* Latest resource-semaphore pressure (grant waiters / timeouts / forced grants + granted MB). */
        await using (var command = _dataSource.CreateCommand(ServerSummaryMemoryPressureSql))
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                memoryWaiterCount = reader.IsDBNull(0) ? 0 : Convert.ToInt64(reader.GetValue(0));
                memoryTimeoutCount = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1));
                memoryForcedCount = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2));
                grantedMemoryMb = reader.IsDBNull(3) ? null : Convert.ToDouble(reader.GetValue(3));
            }
        }

        /* Latest worker-thread pressure (max / in-use / runnable-waiting / work-queue). Absent on Azure. */
        await using (var command = _dataSource.CreateCommand(ServerSummaryThreadsSql))
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                totalThreads = reader.IsDBNull(0) ? null : Convert.ToInt32(reader.GetValue(0));
                currentWorkers = reader.IsDBNull(1) ? null : Convert.ToInt32(reader.GetValue(1));
                threadsWaitingForCpu = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2));
                requestsWaitingForThreads = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3));
            }
        }

        /* Blocking count + worst wait in the last hour (XE preferred, DMV fallback — same source for both). */
        await using (var command = _dataSource.CreateCommand(ServerSummaryBlockingSql))
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                var xeCount = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
                var xeMaxWait = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
                var dmvCount = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2));
                var dmvMaxWait = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3));

                /* Lite's fallback: use XE when it has any row this window, else the DMV snapshot. Both the
                   count and the max wait come from whichever source wins, so they never disagree. */
                if (xeCount > 0)
                {
                    blockingCount = xeCount;
                    maxBlockingWaitMs = xeMaxWait;
                }
                else
                {
                    blockingCount = dmvCount;
                    maxBlockingWaitMs = dmvMaxWait;
                }

                /* Newest blocking event across both sources (unbounded) → "Last: N ago" when the window is
                   clear. Stored times are naive UTC; the tick subtraction against UtcNow is a true elapsed. */
                DateTime? lastBlocking = reader.IsDBNull(4) ? null : reader.GetDateTime(4);
                var dmvLast = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5);
                if (dmvLast.HasValue && (!lastBlocking.HasValue || dmvLast.Value > lastBlocking.Value))
                {
                    lastBlocking = dmvLast;
                }
                lastBlockingMinutesAgo = MinutesAgo(lastBlocking, nowUtc);
            }
        }

        /* Deadlock count in the last hour + the newest deadlock ever (for "Last: N ago"). */
        await using (var command = _dataSource.CreateCommand(ServerSummaryDeadlockSql))
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                deadlockCount = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
                lastDeadlockMinutesAgo = MinutesAgo(reader.IsDBNull(1) ? null : reader.GetDateTime(1), nowUtc);
            }
        }

        /* Newest collection time across all collectors — drives the freshness status. */
        await using (var command = _dataSource.CreateCommand(ServerSummaryLastCollectionSql))
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            var result = await command.ExecuteScalarAsync(cancellationToken);
            if (result is not null && result != DBNull.Value)
            {
                lastCollection = Convert.ToDateTime(result);
            }
        }

        /* Collectors row — REUSE the viewer's own 7-day per-collector health banding (the same STALE /
           FAILING / NEVER_RUN / HEALTHY logic the Collection Health tab renders), mirroring the Dashboard's
           SUM(CASE health_status = 'HEALTHY' / 'FAILING') over report.collection_health. */
        var (healthyCollectors, failingCollectors) = await GetCollectorHealthCountsAsync(serverId, cancellationToken);

        return new ServerSummaryItem
        {
            DisplayName = displayName,
            ServerId = serverId,
            CpuPercent = cpuPercent,
            OtherProcessCpuPercent = otherProcessCpuPercent,
            MemoryMb = memoryMb,
            BufferPoolMb = bufferPoolMb,
            GrantedMemoryMb = grantedMemoryMb,
            MemoryWaiterCount = memoryWaiterCount,
            MemoryTimeoutCount = memoryTimeoutCount,
            MemoryForcedCount = memoryForcedCount,
            BlockingCount = blockingCount,
            MaxBlockingWaitMs = maxBlockingWaitMs,
            LastBlockingMinutesAgo = lastBlockingMinutesAgo,
            DeadlockCount = deadlockCount,
            LastDeadlockMinutesAgo = lastDeadlockMinutesAgo,
            TotalThreads = totalThreads,
            CurrentWorkers = currentWorkers,
            ThreadsWaitingForCpu = threadsWaitingForCpu,
            RequestsWaitingForThreads = requestsWaitingForThreads,
            HealthyCollectorCount = healthyCollectors,
            FailedCollectorCount = failingCollectors,
            LastCollectionTime = lastCollection,
        };
    }

    /// <summary>
    /// Healthy / failing collector counts for one server, derived from the viewer's own 7-day collection
    /// health banding (<see cref="GetCollectionHealthAsync"/> → <see cref="CollectorHealthRow.HealthStatus"/>).
    /// Mirrors the Dashboard's <c>GetCollectorStatusAsync</c> (SUM of HEALTHY / FAILING over
    /// <c>report.collection_health</c>) — HEALTHY and FAILING are the two bands the card surfaces; the
    /// STALE / WARNING / NO_PERMISSIONS / NEVER_RUN rows count as neither (a failing collector is one the
    /// banding calls FAILING: no success in over 24h).
    /// </summary>
    private async Task<(int Healthy, int Failing)> GetCollectorHealthCountsAsync(int serverId, CancellationToken cancellationToken)
    {
        var rows = await GetCollectionHealthAsync(serverId, cancellationToken);
        var healthy = rows.Count(r => r.HealthStatus == "HEALTHY");
        var failing = rows.Count(r => r.HealthStatus == "FAILING");
        return (healthy, failing);
    }

    /// <summary>Whole minutes elapsed from a stored naive-UTC instant to now (UTC), floored at 0, or null
    /// when there is no instant. Both sides are UTC instants, so the tick subtraction is a true elapsed
    /// regardless of Kind (the same reasoning the freshness classification documents).</summary>
    private static int? MinutesAgo(DateTime? instantUtc, DateTime nowUtc) =>
        instantUtc.HasValue ? Math.Max(0, (int)(nowUtc - instantUtc.Value).TotalMinutes) : null;
}

/* The card's status discriminant used to be declared here, as ServerCardStatus. It is now
   PerformanceMonitor.Common's ServerCollectionStatus, because three OTHER places derived the same ladder and
   two of them are in the headless service, which cannot reference WPF (#2473). The argument for having one
   discriminant at all is unchanged and is written down on the enum; what changed is how far "one" reaches.

   The rename is not cosmetic. Lite has its own ServerCardStatus meaning a CONNECTION check, and #2457 kept
   the two axes apart on purpose; giving the shared type the collection name (beside Common's existing
   ServerConnectionStatus) means the two can no longer be confused for each other by a reader or by a
   using directive. */

/// <summary>
/// One Overview server card's view-model — copied from Lite's <c>ServerSummaryItem</c>
/// (Lite/Services/LocalDataService.Overview.cs) and enriched toward the Dashboard's
/// <c>ServerHealthStatus</c> (Threads / Collectors rows, the resource-semaphore Memory signal, blocking
/// duration, and a per-metric severity band for each row's dot). Two viewer adaptations carry over from
/// the headless plan (#1262): <see cref="CpuPercentForAlert"/> is always total non-idle CPU (the viewer
/// has no per-app <c>CpuAlertMode</c> preference — total is what the alert engine evaluates by default),
/// and the connection status is derived from collection freshness rather than a live ping (see
/// <see cref="ClassifyFreshness"/> / <see cref="ApplyFreshness"/>). Every severity BAND reproduces
/// <c>ServerHealthStatus</c>'s deterministic CASE logic so a viewer card colours a metric exactly as the
/// Dashboard would; every display / brush is otherwise a pure format of the stored values.
/// </summary>
public sealed class ServerSummaryItem
{
    /* Freshness + per-metric severity thresholds live once in PerformanceMonitor.Common's
       ServerHealthThresholds / ServerHealthClassifier (#1562); this card keeps only the brush mapping. */

    /* Per-severity dot / value brushes, frozen once (the viewer's dark-theme palette). */
    private static readonly SolidColorBrush s_criticalBrush = MakeBrush("#E57373");
    private static readonly SolidColorBrush s_warningBrush = MakeBrush("#FFD54F");
    private static readonly SolidColorBrush s_healthyBrush = MakeBrush("#81C784");
    private static readonly SolidColorBrush s_unknownBrush = MakeBrush("#888888");

    public string DisplayName { get; set; } = "";
    public string ServerName { get; set; } = "";
    public int ServerId { get; set; }
    public bool? IsOnline { get; set; }

    /// <summary>The server's tags as coloured pills, empty when it has none. Stamped by the Overview loader
    /// from the loaded tag list (the summary carries no tag query of its own), so it survives a re-sort —
    /// which reuses these item instances rather than rebuilding them.</summary>
    public System.Collections.Generic.IReadOnlyList<ServerTagPill> TagPills { get; set; } =
        System.Array.Empty<ServerTagPill>();

    /// <summary>Warning (amber) state — in the viewer this means the collection has gone stale.</summary>
    public bool HasCollectorErrors { get; set; }

    /// <summary>
    /// True when no collection has EVER landed for this server (<see cref="ServerFreshness.NeverCollected"/>):
    /// the service hasn't reached it yet — a registered-but-queued server during bootstrap, not a dead one.
    /// Drives the amber "Awaiting first collection" status instead of the red Offline overlay.
    /// </summary>
    public bool AwaitingFirstCollection { get; set; }

    /// <summary>SQL Server scheduler ProcessUtilization from sys.dm_os_ring_buffers. NULL on Azure SQL DB.</summary>
    public double? CpuPercent { get; set; }

    /// <summary>Non-SQL-Server CPU on the host (100 - SystemIdle - ProcessUtilization). NULL on Azure SQL DB.</summary>
    public double? OtherProcessCpuPercent { get; set; }

    /// <summary>Total non-idle CPU on the host = sql_server + other_process. Tracks OS user+system counters.</summary>
    public double? TotalCpuPercent =>
        CpuPercent.HasValue ? CpuPercent.Value + (OtherProcessCpuPercent ?? 0) : null;

    /// <summary>
    /// The CPU value the headline display / colour band uses. The viewer has no per-app CpuAlertMode, so
    /// it always uses total non-idle CPU (falling back to SQL-only when other-process is unavailable) —
    /// what the alert engine evaluates by default.
    /// </summary>
    public double? CpuPercentForAlert => TotalCpuPercent ?? CpuPercent;

    public double? MemoryMb { get; set; }

    /// <summary>Latest buffer-pool MB (v_memory_stats.buffer_pool_mb) — the "BP" figure in the Memory detail.</summary>
    public double? BufferPoolMb { get; set; }

    /// <summary>Total granted query-memory MB across all pools at the newest grant snapshot — the "QMG" figure.</summary>
    public double? GrantedMemoryMb { get; set; }

    /// <summary>Grant waiters at the newest snapshot — the primary resource-semaphore pressure signal.</summary>
    public long MemoryWaiterCount { get; set; }

    /// <summary>Grant timeout-error delta at the newest snapshot (a query gave up waiting for memory).</summary>
    public long MemoryTimeoutCount { get; set; }

    /// <summary>Forced-grant delta at the newest snapshot (a grant was forced through under pressure).</summary>
    public long MemoryForcedCount { get; set; }

    public int BlockingCount { get; set; }

    /// <summary>The worst blocking wait (ms) observed in the window — the "max: Ns" detail + Critical band input.</summary>
    public long MaxBlockingWaitMs { get; set; }

    /// <summary>Minutes since the most recent blocking event ever — the "Last: N ago" detail when the window is clear.</summary>
    public int? LastBlockingMinutesAgo { get; set; }

    public int DeadlockCount { get; set; }

    /// <summary>Minutes since the most recent deadlock ever — the "Last: N ago" deadlock detail.</summary>
    public int? LastDeadlockMinutesAgo { get; set; }

    /// <summary>Worker-thread ceiling (max_workers_count). NULL = no scheduler snapshot (e.g. Azure SQL DB).</summary>
    public int? TotalThreads { get; set; }

    /// <summary>Workers in use (total_current_workers_count); available = ceiling − in-use.</summary>
    public int? CurrentWorkers { get; set; }

    /// <summary>Runnable tasks waiting for a CPU (total_runnable_tasks_count).</summary>
    public int ThreadsWaitingForCpu { get; set; }

    /// <summary>Requests starved of a worker thread (total_work_queue_count) — thread-pool starvation.</summary>
    public long RequestsWaitingForThreads { get; set; }

    /// <summary>Available worker threads = ceiling − in-use, or NULL when there is no scheduler snapshot.</summary>
    public int? AvailableThreads =>
        TotalThreads.HasValue ? TotalThreads.Value - (CurrentWorkers ?? 0) : null;

    /// <summary>Collectors whose 7-day band is HEALTHY (mirrors report.collection_health).</summary>
    public int HealthyCollectorCount { get; set; }

    /// <summary>Collectors whose 7-day band is FAILING (no success in over 24h).</summary>
    public int FailedCollectorCount { get; set; }

    public DateTime? LastCollectionTime { get; set; }

    /// <summary>
    /// Headline CPU display: total non-idle CPU prominently with the SQL-only number alongside, e.g.
    /// "64% (SQL 60%)". Falls back to a single number when only one value is available.
    /// </summary>
    public string CpuDisplay
    {
        get
        {
            if (!CpuPercent.HasValue) return "--";
            if (!OtherProcessCpuPercent.HasValue) return $"{CpuPercent:F0}%";
            return $"{TotalCpuPercent:F0}% (SQL {CpuPercent:F0}%)";
        }
    }

    /// <summary>The non-SQL host CPU alongside the headline (the Dashboard's CPU detail), when known.</summary>
    public string CpuDetail => OtherProcessCpuPercent.HasValue ? $"Other: {OtherProcessCpuPercent:F0}%" : "";

    public string MemoryDisplay => MemoryMb.HasValue ? $"{MemoryMb / 1024.0:F1} GB" : "--";

    /// <summary>
    /// The Memory detail: under resource-semaphore pressure it names the pressure (grant waiters, then
    /// timeouts / forced grants when present) — mirroring the Dashboard's "N waiting"; when calm it shows
    /// the buffer-pool and query-memory-grant sizes (the Dashboard's "BP: x, QMG: y").
    /// </summary>
    public string MemoryDetail
    {
        get
        {
            if (HasMemoryPressure)
            {
                var parts = new List<string>();
                if (MemoryWaiterCount > 0) parts.Add($"{MemoryWaiterCount} grant waiter{(MemoryWaiterCount == 1 ? "" : "s")}");
                if (MemoryTimeoutCount > 0) parts.Add($"{MemoryTimeoutCount} timeout{(MemoryTimeoutCount == 1 ? "" : "s")}");
                if (MemoryForcedCount > 0) parts.Add($"{MemoryForcedCount} forced");
                return string.Join(", ", parts);
            }

            var sizes = new List<string>();
            if (BufferPoolMb.HasValue) sizes.Add($"BP {BufferPoolMb.Value / 1024.0:F1}");
            if (GrantedMemoryMb is > 0) sizes.Add($"QMG {GrantedMemoryMb.Value / 1024.0:F1}");
            return sizes.Count > 0 ? string.Join(", ", sizes) + " GB" : "";
        }
    }

    public string BlockingDisplay => BlockingCount > 0 ? BlockingCount.ToString() : "0";

    /// <summary>
    /// The blocking detail (Dashboard's BlockingDetailText): the worst wait while blocking is present in
    /// the window ("max: 42s"), else how long since the last blocking event ever ("Last: 3h ago"), else blank.
    /// </summary>
    public string BlockingDetail
    {
        get
        {
            if (BlockingCount > 0) return $"max: {MaxBlockedSeconds:F0}s";
            if (LastBlockingMinutesAgo.HasValue) return $"Last: {FormatMinutesAgo(LastBlockingMinutesAgo.Value)}";
            return "";
        }
    }

    /// <summary>The worst blocking wait in the window, in seconds.</summary>
    public double MaxBlockedSeconds => MaxBlockingWaitMs / 1000.0;

    public string DeadlockDisplay => DeadlockCount > 0 ? DeadlockCount.ToString() : "0";

    /// <summary>The deadlock detail — how long since the last deadlock ever ("Last: N ago"), else blank.</summary>
    public string DeadlockDetail =>
        LastDeadlockMinutesAgo.HasValue ? $"Last: {FormatMinutesAgo(LastDeadlockMinutesAgo.Value)}" : "";

    /// <summary>Threads value — the pressure headline (Dashboard's ThreadsDisplayText), or "--" with no snapshot.</summary>
    public string ThreadsDisplay
    {
        get
        {
            if (!TotalThreads.HasValue) return "--";
            if (RequestsWaitingForThreads > 0) return $"{RequestsWaitingForThreads} starved";
            if (ThreadsWaitingForCpu >= 20) return $"{ThreadsWaitingForCpu} runnable";
            if (TotalThreads.Value > 0 && AvailableThreads < TotalThreads.Value * 0.10) return "Low";
            return "OK";
        }
    }

    /// <summary>Threads detail — "Available: in/ceiling" (Dashboard's ThreadsDetailText); blank with no snapshot.</summary>
    public string ThreadsDetail =>
        TotalThreads is > 0 ? $"Available: {AvailableThreads}/{TotalThreads}" : "";

    /// <summary>Collectors value — "N failed" or "OK" (Dashboard's CollectorDisplayText).</summary>
    public string CollectorDisplay => FailedCollectorCount > 0 ? $"{FailedCollectorCount} failed" : "OK";

    /// <summary>Collectors detail — "Healthy: N, Failing: M" (Dashboard's CollectorDetailText).</summary>
    public string CollectorDetail => $"Healthy: {HealthyCollectorCount}, Failing: {FailedCollectorCount}";

    /// <summary>
    /// The stored collection_time is naive UTC; the viewer shows it in the viewer machine's local time
    /// (the viewer convention — Lite used its per-server offset helper instead).
    /// </summary>
    public string LastCollectionDisplay => LastCollectionTime.HasValue
        ? ViewerTimeHelper.ForDisplay(LastCollectionTime.Value).ToString("HH:mm:ss")
        : "Never";

    /* Collection status. The (IsOnline, HasCollectorErrors, AwaitingFirstCollection) triple is resolved by
       ServerCollectionStatusRules.Classify and nowhere else in the viewer — the sidebar row's dot carried its
       own four-state copy until #2473, which is how a never-collected server got a grey "Unknown" dot beside
       this card's amber "Awaiting first collection". Everything downstream — the word, the colour, the
       ranking's reason, the tooltip, the sidebar dot — renders the resulting ServerCollectionStatus, so no
       two of them can land on different answers for the same server. See ServerCollectionStatus for the
       contradictions that motivated collapsing it.

       The null arm distinguishes "not reached yet" (bootstrap) from a legacy unknown. */
    public ServerCollectionStatus CardStatus =>
        ServerCollectionStatusRules.Classify(IsOnline, HasCollectorErrors, AwaitingFirstCollection);

    public string StatusDisplay => CardStatus.Word();

    /* The palette stays here rather than moving to the rules class: these are the viewer's dark-theme hexes,
       and the sidebar dot paints the same states from the THEME dictionaries instead (a DynamicResource, so
       it follows the light / cool-breeze themes the cards do not). The states agree; only the colour source
       differs, and ViewerSidebarDotRendersTheCardStatusTests pins that every state the card paints has a
       trigger on the dot. */
    public SolidColorBrush StatusBrush => MakeBrush(CardStatus switch
    {
        ServerCollectionStatus.Stale => "#FFD54F",  // amber — stale collection
        ServerCollectionStatus.Online => "#81C784",
        ServerCollectionStatus.Offline => "#E57373",
        ServerCollectionStatus.AwaitingFirstCollection => "#FFD54F",  // amber — queued, not dead
        _ => "#888888",
    });

    /// <summary>
    /// What the status word MEANS on this card, for the tooltip the status line carries (#2422). A colour and
    /// a one-word band were the whole answer the Overview gave, and the reporter's question — "what is it that
    /// this text warns me about?" — is one the card could already answer: <see cref="FleetRollup.BuildReason"/>
    /// builds the sentence out of THIS card's own metric displays, and until now only the Needs Attention
    /// ranking got to see it.
    ///
    /// <para>Delegated rather than reimplemented on purpose: two independent derivations of "why is this amber"
    /// would eventually disagree, and the one place they would disagree is a card the reader is staring at.</para>
    /// </summary>
    public string StatusTooltip => FleetRollup.BuildStatusTooltip(this);

    public bool IsOffline => IsOnline == false;

    // ── Per-metric severity bands (delegated to the SHARED ServerHealthClassifier — one place for the
    //    thresholds; this card keeps only the brush mapping) ────────────────────────────────────────────

    /// <summary>CPU band — total non-idle CPU: >= 95% Critical, >= 80% Warning.</summary>
    public HealthSeverity CpuSeverity => ServerHealthClassifier.CpuSeverity(CpuPercentForAlert);

    /// <summary>True when the resource semaphore shows grant waiters, timeouts, or forced grants.</summary>
    public bool HasMemoryPressure => MemoryWaiterCount > 0 || MemoryTimeoutCount > 0 || MemoryForcedCount > 0;

    /// <summary>Memory band — Critical on any resource-semaphore pressure, else Healthy.</summary>
    public HealthSeverity MemorySeverity => ServerHealthClassifier.MemorySeverity(HasMemoryPressure);

    /// <summary>Blocking band — >= 60s max wait or >= 5 events Critical; >= 10s, >= 2 events, or any blocking Warning.</summary>
    public HealthSeverity BlockingSeverity => ServerHealthClassifier.BlockingSeverity(BlockingCount, MaxBlockedSeconds);

    /// <summary>Deadlock band — any deadlock in the window is Critical.</summary>
    public HealthSeverity DeadlockSeverity => ServerHealthClassifier.DeadlockSeverity(DeadlockCount);

    /// <summary>Threads band — work-queue starvation Critical; >= 20 runnable-waiting or under 10% available Warning; no snapshot Unknown.</summary>
    public HealthSeverity ThreadsSeverity =>
        ServerHealthClassifier.ThreadsSeverity(TotalThreads, AvailableThreads, ThreadsWaitingForCpu, RequestsWaitingForThreads);

    /// <summary>Collectors band — any FAILING collector is Warning.</summary>
    public HealthSeverity CollectorSeverity => ServerHealthClassifier.CollectorSeverity(FailedCollectorCount);

    /// <summary>The card's worst metric band (offline handled separately by the border / overlay).</summary>
    public HealthSeverity OverallMetricSeverity => ServerHealthClassifier.OverallMetricSeverity(ToHealthMetrics());

    /// <summary>The card's raw per-metric inputs, for the shared classifier (banding + fleet score).</summary>
    public ServerHealthMetrics ToHealthMetrics() => new()
    {
        CpuPercentForAlert = CpuPercentForAlert,
        HasMemoryPressure = HasMemoryPressure,
        BlockingCount = BlockingCount,
        MaxBlockedSeconds = MaxBlockedSeconds,
        DeadlockCount = DeadlockCount,
        TotalThreads = TotalThreads,
        AvailableThreads = AvailableThreads,
        ThreadsWaitingForCpu = ThreadsWaitingForCpu,
        RequestsWaitingForThreads = RequestsWaitingForThreads,
        FailedCollectorCount = FailedCollectorCount,
    };

    // ── Per-metric dot / value brushes ───────────────────────────────────────────────────────────────

    public SolidColorBrush CpuSeverityBrush => SeverityBrush(CpuSeverity);
    public SolidColorBrush MemorySeverityBrush => SeverityBrush(MemorySeverity);
    public SolidColorBrush BlockingSeverityBrush => SeverityBrush(BlockingSeverity);
    public SolidColorBrush DeadlockSeverityBrush => SeverityBrush(DeadlockSeverity);
    public SolidColorBrush ThreadsSeverityBrush => SeverityBrush(ThreadsSeverity);
    public SolidColorBrush CollectorSeverityBrush => SeverityBrush(CollectorSeverity);

    public bool HasAlerts => BlockingCount > 0 || DeadlockCount > 0;

    /// <summary>
    /// The card border reflects the worst signal: offline (red) &gt; a Critical metric (red) &gt; a Warning
    /// metric (amber-orange) &gt; a stale collection (amber) &gt; calm (dark). Enriches Lite's border (which
    /// only knew CPU / blocking / deadlock) with the added Threads / Memory / Collectors bands via
    /// <see cref="OverallMetricSeverity"/>.
    /// </summary>
    public SolidColorBrush CardBorderBrush
    {
        get
        {
            if (IsOnline == false) return s_criticalBrush;
            return OverallMetricSeverity switch
            {
                HealthSeverity.Critical => s_criticalBrush,
                HealthSeverity.Warning => s_warningBrush,
                _ => HasCollectorErrors || AwaitingFirstCollection ? MakeBrush("#FFD54F") : MakeBrush("#2a2d35"),
            };
        }
    }

    /// <summary>
    /// The viewer's status derivation (#1262): classify how fresh the newest collection is. Pure over
    /// (last-collection, now) so it can be pinned without a store. Both instants are UTC (the store is
    /// naive UTC; <paramref name="nowUtc"/> is <see cref="DateTime.UtcNow"/>), so the subtraction is a
    /// true elapsed-time regardless of Kind.
    /// </summary>
    public static ServerFreshness ClassifyFreshness(DateTime? lastCollectionUtc, DateTime nowUtc) =>
        ServerHealthClassifier.ClassifyFreshness(lastCollectionUtc, nowUtc);

    /// <summary>
    /// Maps the freshness band onto the card's three status flags, taking the live-ping's place: Fresh →
    /// Online, Stale → the amber Warning state, Offline → the red Offline overlay, NeverCollected → the amber
    /// "Awaiting first collection" state (IsOnline stays null: the truth is "unknown, not reached yet", not
    /// "was up and died").
    ///
    /// <para>The mapping itself is <see cref="ServerCollectionStatusRules.FlagsFor"/>, shared with the sidebar
    /// row and the service's fleet reader. It was written out here in longhand, and the sidebar's longhand
    /// copy set two of the three flags and dropped <c>AwaitingFirstCollection</c> — an omission that is
    /// invisible in a block of assignments and impossible when the three arrive together (#2473).</para>
    /// </summary>
    public void ApplyFreshness(DateTime nowUtc)
    {
        var flags = ServerCollectionStatusRules.FlagsFor(ClassifyFreshness(LastCollectionTime, nowUtc));
        IsOnline = flags.IsOnline;
        HasCollectorErrors = flags.HasCollectorErrors;
        AwaitingFirstCollection = flags.AwaitingFirstCollection;
    }

    private static SolidColorBrush SeverityBrush(HealthSeverity severity) => severity switch
    {
        HealthSeverity.Critical => s_criticalBrush,
        HealthSeverity.Warning => s_warningBrush,
        HealthSeverity.Healthy => s_healthyBrush,
        _ => s_unknownBrush,
    };

    /// <summary>Human "N ago" rendering of an elapsed minutes count — verbatim from ServerHealthStatus.</summary>
    private static string FormatMinutesAgo(int minutes)
    {
        if (minutes < 1) return "just now";
        if (minutes < 60) return $"{minutes}m ago";
        if (minutes < 1440) return $"{minutes / 60}h ago";   // 24 hours
        if (minutes < 10080) return $"{minutes / 1440}d ago"; // 7 days
        return $"{minutes / 10080}w ago";
    }

    private static SolidColorBrush MakeBrush(string hex)
    {
        var color = (Color)ColorConverter.ConvertFromString(hex);
        var brush = new SolidColorBrush(color);
        brush.Freeze();
        return brush;
    }
}

/// <summary>One tag pill on an Overview card (#2008 stage 2a): the tag name plus the brushes to render it,
/// resolved once from the stored colour via <see cref="TagColorBrushes"/> (neutral when the tag has no
/// colour). The label brush is a constant readable light, so a pill looks the same in either theme.</summary>
public sealed class ServerTagPill
{
    public ServerTagPill(string name, string? colour)
    {
        Name = name;
        Fill = TagColorBrushes.Fill(colour);
    }

    public string Name { get; }

    public System.Windows.Media.Brush Fill { get; }

    public System.Windows.Media.Brush TextBrush => TagColorBrushes.PillText;
}
