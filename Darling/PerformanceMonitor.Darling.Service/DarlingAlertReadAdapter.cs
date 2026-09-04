/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Darling's <see cref="IAlertReadAdapter"/> (Phase-5 slice B): the seven collected alert feeds
/// read from Postgres — each query is Lite's DuckDB read ported dialect-for-dialect against the
/// same table names and columns (the generated PG schema mirrors Lite's — see PgSchemaGenerator),
/// with three deliberate adjustments:
/// (1) no <c>v_</c> views exist here — the raw tables are queried directly;
/// (2) Lite's long-running-query read uses DuckDB's bare <c>NOW()</c>; the PG twin binds a
///     parameterized naive-UTC now instead (the poison-wait read's parameterization style),
///     because a bare <c>now()</c> is <c>timestamptz</c> and comparing it against the naive-UTC
///     <c>timestamp</c> columns would resolve in the server's time zone;
/// (3) DuckDB's <c>N'...'</c> literals become plain <c>'...'</c> (Postgres has no N-prefix).
/// All bound timestamps are naive-UTC Kind-Unspecified, matching the COPY writer's storage
/// discipline. The blocking read reproduces Lite's XE-preferred + DMV-fallback merge via the
/// shared <see cref="BlockedProcessReportMerge"/>.
/// <para>
/// <c>serverKey</c> is the deterministic storage-name hash rendered as a string (the engine's
/// identity across the Phase-5 seams), parsed back to the <c>server_id</c> int here.
/// </para>
/// </summary>
public sealed class DarlingAlertReadAdapter : IAlertReadAdapter
{
    /// <summary>
    /// The explicit command deadline for EVERY read in the alert evaluation pass (#2874).
    ///
    /// <para>All forty-nine commands across the seven alert-pass types ran with no
    /// <c>CommandTimeout</c>, so every one inherited Npgsql's undocumented 30 s default. Nobody chose
    /// 30 s; it was simply what happened. On 2026-09-04 the forced-plan read failed five times on the
    /// production store, each time surfacing as "Exception while reading from stream" — which is how
    /// Npgsql renders its OWN deadline, and which read literally says the network broke (the same
    /// misdiagnosis #2826 exists to prevent).</para>
    ///
    /// <para><b>Why this pass needed its own number rather than the 60 s #2810 and #2871 chose.</b>
    /// Those two sit under <c>DarlingWorker.s_analysisTimeout</c>, a 120 s <c>CancelAfter</c> that
    /// bounds the whole pass however long an individual command runs. <b>This pass has no enclosing
    /// budget at all</b> — <c>EvaluateAlertsAsync</c> is called with the plain stopping token — so the
    /// per-command deadline IS the pass budget, multiplied by however many reads run in sequence.
    /// At the inherited 30 s that is 49 x 30 s of worst-case exposure while the body holds one of only
    /// <see cref="DarlingWorker.MaxConcurrentServerSweeps"/> fleet permits, and the sweep skips
    /// relaunch for that server the whole time. Copying 60 s here would have doubled it.</para>
    ///
    /// <para><b>Bounded below</b> by measurement: the shipped queries were timed against the
    /// production store on its three busiest servers, cold and warm. The whole pass is dominated by
    /// one read — the forced-plan check at <b>1,744.9 ms</b> cold, scanning ~6.0 GB of
    /// <c>query_store_stats</c>; every other read in the family lands under 3 ms. Ten seconds is 5.7x
    /// that worst case, so it absorbs a substantial stall rather than only the happy path.</para>
    ///
    /// <para><b>Bounded above</b> by the cadence this pass runs on: <c>s_alertSweepInterval</c> is
    /// 30 s, so one stalled read must still leave the pass able to finish inside the interval that
    /// will start it again. Ten seconds keeps a single stall well inside that, and caps the unbudgeted
    /// worst case at 49 x 10 s instead of 49 x 30 s.</para>
    ///
    /// <para><b>The asymmetry is why erring SHORT is right here, and it is the reverse of #2810.</b>
    /// A read that exceeds this deadline skips one alert check and logs it; the next pass runs 30 s
    /// later, so the cost is one cycle of delay on one alert. A read that runs long holds a fleet
    /// sweep permit and delays collection for every other server queued behind it. The recoverable
    /// failure is strictly cheaper than the unrecoverable one, so this is the first value in the
    /// family set BELOW what it inherited rather than above it.</para>
    ///
    /// <para><b>What the data cannot say.</b> Every observed failure was killed AT the 30 s ceiling,
    /// so the record is right-censored: nothing here establishes whether a stalled read wanted 35 s or
    /// 300 s. Ten seconds is chosen from the measured cost and the cadence above, NOT fitted to the
    /// failure distribution — a number claiming to fit that data would be invented.</para>
    /// </summary>
    internal const int AlertPassCommandTimeoutSeconds = 10;

    private readonly NpgsqlDataSource _postgres;
    private readonly Func<int, int>? _runningJobsCadenceMinutes;
    private readonly Func<int, int>? _blockingSnapshotCadenceMinutes;

    /// <param name="runningJobsCadenceMinutes">
    /// Resolves a server's EFFECTIVE running_jobs collection cadence (minutes) for the #1812
    /// snapshot-freshness bound — the worker supplies its own schedule resolution (the same
    /// <c>StoreConfigProvider.ResolveSchedule</c> the sweep runs on). Null (test call sites) or a
    /// non-positive answer falls back to the shared <see cref="CollectorScheduleDefaults"/> cadence.
    /// </param>
    /// <param name="blockingSnapshotCadenceMinutes">
    /// The same resolver for the dmv_blocking_snapshot cadence, behind #1839's freshness bound.
    /// </param>
    public DarlingAlertReadAdapter(
        NpgsqlDataSource postgres,
        Func<int, int>? runningJobsCadenceMinutes = null,
        Func<int, int>? blockingSnapshotCadenceMinutes = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _runningJobsCadenceMinutes = runningJobsCadenceMinutes;
        _blockingSnapshotCadenceMinutes = blockingSnapshotCadenceMinutes;
    }

    /* ---------------- blocking (XE-preferred + DMV fallback) ---------------- */

    /// <summary>
    /// The XE blocked-process-report read — Lite's query with the column list trimmed to the
    /// shared alert row's fields (same WHERE / ORDER BY event_time DESC / LIMIT 200 semantics).
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string BlockedProcessReportsSql = @"
SELECT
    event_time,
    database_name,
    blocked_spid,
    blocking_spid,
    wait_time_ms,
    lock_mode,
    blocked_sql_text,
    blocking_sql_text,
    blocked_process_report_xml,
    contentious_object
FROM blocked_process_reports
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY event_time DESC
LIMIT 200";

    /// <summary>
    /// The always-on DMV blocking-snapshot fallback read — Lite's query trimmed the same way
    /// (dmv_blocking_snapshots has no report XML column). Same parameters as
    /// <see cref="BlockedProcessReportsSql"/>.
    /// </summary>
    public const string DmvBlockingSnapshotsSql = @"
SELECT
    event_time,
    database_name,
    blocked_spid,
    blocking_spid,
    wait_time_ms,
    lock_mode,
    blocked_sql_text,
    blocking_sql_text,
    contentious_object
FROM dmv_blocking_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY event_time DESC
LIMIT 200";

    public async Task<List<BlockedProcessAlertRow>> GetRecentBlockedProcessReportsAsync(
        string serverKey, int hoursBack, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        var (startTime, endTime) = Window(hoursBack);

        var items = new List<BlockedProcessAlertRow>();
        var dmvItems = new List<BlockedProcessAlertRow>();

        await using (var connection = await _postgres.OpenConnectionAsync(cancellationToken))
        {
            using (var command = new NpgsqlCommand(BlockedProcessReportsSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
            {
                command.Parameters.AddWithValue(serverId);
                command.Parameters.AddWithValue(startTime);
                command.Parameters.AddWithValue(endTime);

                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    items.Add(new BlockedProcessAlertRow
                    {
                        EventTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                        DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        BlockedSpid = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                        BlockingSpid = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                        WaitTimeMs = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                        LockMode = reader.IsDBNull(5) ? "" : reader.GetString(5),
                        BlockedSqlText = reader.IsDBNull(6) ? "" : reader.GetString(6),
                        BlockingSqlText = reader.IsDBNull(7) ? "" : reader.GetString(7),
                        BlockedProcessReportXml = reader.IsDBNull(8) ? "" : reader.GetString(8),
                        ContentiousObject = reader.IsDBNull(9) ? "" : reader.GetString(9)
                    });
                }
            }

            using (var command = new NpgsqlCommand(DmvBlockingSnapshotsSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
            {
                command.Parameters.AddWithValue(serverId);
                command.Parameters.AddWithValue(startTime);
                command.Parameters.AddWithValue(endTime);

                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    dmvItems.Add(new BlockedProcessAlertRow
                    {
                        EventTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                        DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                        BlockedSpid = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                        BlockingSpid = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                        WaitTimeMs = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                        LockMode = reader.IsDBNull(5) ? "" : reader.GetString(5),
                        BlockedSqlText = reader.IsDBNull(6) ? "" : reader.GetString(6),
                        BlockingSqlText = reader.IsDBNull(7) ? "" : reader.GetString(7),
                        ContentiousObject = reader.IsDBNull(8) ? "" : reader.GetString(8),
                        Source = BlockedProcessAlertRow.DmvSnapshotSource
                    });
                }
            }
        }

        /* Lite's XE-preferred fallback semantics, verbatim via the shared merge: keep all BPR
           rows; append a DMV row only where no BPR covers the same SPID pair in the same minute;
           re-cap to the 200 newest. */
        BlockedProcessReportMerge.AppendDmvFallbackRows(items, dmvItems);

        return items;
    }

    /* ---------------- current blocking wait (#1839) ---------------- */

    /// <summary>
    /// Lite's latest-blocking-snapshot sum, ported dialect-for-dialect: ONE snapshot selected by
    /// <c>collection_time = MAX(collection_time)</c> (never a window — see
    /// <see cref="CurrentBlockingWaitResult"/>), its <c>wait_time_ms</c> summed and its distinct
    /// blocked SPIDs counted. $1 server_id, used twice.
    /// <para>
    /// ONE statement, deliberately: Npgsql fails SILENTLY on multi-statement commands with positional
    /// parameters, so the freshness probe cannot be batched onto this — the snapshot time comes back as
    /// a column of this same aggregate instead, which is one round trip rather than two anyway.
    /// </para>
    /// </summary>
    public const string CurrentBlockingWaitSql = @"
SELECT
    collection_time,
    CAST(COALESCE(SUM(wait_time_ms), 0) AS bigint) AS total_wait_ms,
    CAST(COUNT(DISTINCT blocked_spid) AS integer) AS blocked_sessions
FROM dmv_blocking_snapshots
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM dmv_blocking_snapshots
    WHERE server_id = $1
)
GROUP BY collection_time";

    public async Task<CurrentBlockingWaitResult?> GetCurrentBlockingWaitAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);

        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(CurrentBlockingWaitSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        /* The SQL casts pin both aggregates to bigint/integer, so these read directly — PG's SUM(bigint)
           is numeric and COUNT is bigint, neither of which Npgsql would hand back as long/int untyped. */
        var snapshotTime = reader.GetDateTime(0);
        var totalWaitMs = reader.IsDBNull(1) ? 0L : reader.GetInt64(1);
        var blockedSessions = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);

        /* #1812 freshness, same rule as Lite's adapter — parity is the point. The stored times are
           naive UTC, so they compare directly against DateTime.UtcNow. */
        var cadence = ResolveCadence(_blockingSnapshotCadenceMinutes, serverId, "dmv_blocking_snapshot");
        bool isFresh = DateTime.UtcNow - snapshotTime <= CurrentBlockingWaitResult.MaxSnapshotAge(cadence);

        return new CurrentBlockingWaitResult(snapshotTime, totalWaitMs, blockedSessions, isFresh);
    }

    /* ---------------- deadlocks ---------------- */

    /// <summary>Lite's deadlock read (column list trimmed to the shared alert row's fields).</summary>
    public const string DeadlocksSql = @"
SELECT
    victim_process_id,
    victim_sql_text,
    deadlock_graph_xml
FROM deadlocks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY deadlock_time DESC
LIMIT 50";

    public async Task<List<DeadlockAlertRow>> GetRecentDeadlocksAsync(
        string serverKey, int hoursBack, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        var (startTime, endTime) = Window(hoursBack);

        var items = new List<DeadlockAlertRow>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(DeadlocksSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(startTime);
        command.Parameters.AddWithValue(endTime);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new DeadlockAlertRow
            {
                VictimProcessId = reader.IsDBNull(0) ? "" : reader.GetString(0),
                VictimSqlText = reader.IsDBNull(1) ? "" : reader.GetString(1),
                DeadlockGraphXml = reader.IsDBNull(2) ? "" : reader.GetString(2)
            });
        }

        return items;
    }

    /* ---------------- poison waits ---------------- */

    /// <summary>
    /// Lite's poison-wait read verbatim (wait_stats table instead of the v_ view). $2 is the
    /// parameterized naive-UTC "now minus 10 minutes" — the parameterization style the
    /// long-running-query twin copies.
    /// </summary>
    public const string PoisonWaitsSql = @"
SELECT
    wait_type,
    delta_wait_time_ms AS delta_ms,
    delta_waiting_tasks AS delta_tasks,
    CASE WHEN delta_waiting_tasks > 0
    THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / delta_waiting_tasks
    ELSE 0 END AS avg_ms_per_wait,
    collection_time
FROM wait_stats
WHERE server_id = $1
AND wait_type IN ('THREADPOOL', 'RESOURCE_SEMAPHORE', 'RESOURCE_SEMAPHORE_QUERY_COMPILE')
AND delta_waiting_tasks > 0
AND collection_time >= $2
ORDER BY collection_time DESC
LIMIT 3";

    public async Task<List<PoisonWaitDelta>> GetPoisonWaitDeltasAsync(
        string serverKey, double thresholdMs, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);

        var items = new List<PoisonWaitDelta>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(PoisonWaitsSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(NaiveUtcNow().AddMinutes(-10));

        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                items.Add(new PoisonWaitDelta
                {
                    WaitType = reader.GetString(0),
                    DeltaMs = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                    DeltaTasks = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                    AvgMsPerWait = reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                    CollectionTime = reader.GetDateTime(4)
                });
            }
        }

        /* Fetch-then-filter, exactly like Lite's loop: the 3-row window is selected before
           thresholding (see the IAlertReadAdapter contract). */
        return items.FindAll(w => w.AvgMsPerWait >= thresholdMs);
    }

    /* ---------------- long-running queries ---------------- */

    /// <summary>
    /// Lite's long-running-query read with the two PG dialect adjustments: DuckDB's bare
    /// <c>NOW() - INTERVAL '10 MINUTES'</c> becomes the parameterized naive-UTC $4 (a bare
    /// <c>now()</c> is timestamptz — wrong basis against naive-UTC timestamp columns), and the
    /// N'' literals in the opt-out filters (<c>{0}</c> placeholder) lose their N prefix.
    /// $1 server_id, $2 elapsed-ms threshold, $3 max results, $4 staleness floor.
    /// </summary>
    public const string LongRunningQueriesSqlTemplate = @"
SELECT
    r.session_id,
    r.database_name,
    SUBSTRING(r.query_text, 1, 300) AS query_text,
    r.total_elapsed_time_ms / 1000 AS elapsed_seconds,
    r.cpu_time_ms,
    r.reads,
    r.writes,
    r.wait_type,
    r.blocking_session_id,
    r.query_hash,
    r.program_name
FROM query_snapshots AS r
WHERE r.server_id = $1
    AND r.collection_time = (SELECT MAX(vqs.collection_time) FROM query_snapshots AS vqs WHERE vqs.server_id = $1)
    AND r.collection_time >= $4
    AND r.session_id > 50
    {0}
    AND r.total_elapsed_time_ms >= $2
ORDER BY r.total_elapsed_time_ms DESC
LIMIT $3";

    /// <summary>The five opt-out noise filters — Lite's clauses with the N'' prefixes dropped.</summary>
    /* sp_server_diagnostics (the AG/FCI health-check session) usually sits in SP_SERVER_DIAGNOSTICS_SLEEP, but it
       also does Extended Events work, so it can be captured in a different wait (e.g. PREEMPTIVE_XE_GETTARGETSTATE)
       where the wait-type match alone misses it and the Long-Running Query alert fires anyway. The query-text
       match (case-insensitive, NULL-safe) catches it regardless of the wait it happens to be in at capture time. */
    public const string SpServerDiagnosticsFilter =
        "AND r.wait_type NOT LIKE '%SP_SERVER_DIAGNOSTICS%'\n    AND (r.query_text IS NULL OR r.query_text NOT ILIKE '%sp_server_diagnostics%')";
    public const string WaitForFilter = "AND r.wait_type NOT IN ('WAITFOR', 'BROKER_RECEIVE_WAITFOR')";
    public const string BackupsFilter = "AND r.wait_type NOT IN ('BACKUPTHREAD', 'BACKUPIO')";
    public const string MiscWaitsFilter = "AND r.wait_type NOT IN ('XE_LIVE_TARGET_TVF')";
    public const string CdcFilter = "AND COALESCE(r.is_cdc_capture, FALSE) = FALSE";

    public async Task<List<LongRunningQueryInfo>> GetLongRunningQueriesAsync(
        string serverKey,
        int thresholdMinutes,
        int maxResults,
        bool excludeSpServerDiagnostics,
        bool excludeWaitFor,
        bool excludeBackups,
        bool excludeMiscWaits,
        bool excludeCdc,
        IReadOnlyList<string> excludedDatabases,
        CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        var thresholdMs = (long)thresholdMinutes * 60 * 1000;
        maxResults = Math.Clamp(maxResults, 1, 1000);

        var filters = string.Join("\n    ", new[]
        {
            excludeSpServerDiagnostics ? SpServerDiagnosticsFilter : "",
            excludeWaitFor ? WaitForFilter : "",
            excludeBackups ? BackupsFilter : "",
            excludeMiscWaits ? MiscWaitsFilter : "",
            excludeCdc ? CdcFilter : ""
        }.Where(f => f.Length > 0));

        var sql = LongRunningQueriesSqlTemplate.Replace("{0}", filters);

        var items = new List<LongRunningQueryInfo>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(thresholdMs);
        command.Parameters.AddWithValue(maxResults);
        command.Parameters.AddWithValue(NaiveUtcNow().AddMinutes(-10));

        using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                items.Add(new LongRunningQueryInfo
                {
                    SessionId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                    DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    QueryText = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    ElapsedSeconds = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                    CpuTimeMs = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                    Reads = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                    Writes = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                    WaitType = reader.IsDBNull(7) ? null : reader.GetString(7),
                    BlockingSessionId = reader.IsDBNull(8) ? null : (int?)reader.GetInt32(8),
                    QueryHash = reader.IsDBNull(9) ? null : reader.GetString(9),
                    ProgramName = reader.IsDBNull(10) ? "" : reader.GetString(10)
                });
            }
        }

        if (excludedDatabases is { Count: > 0 })
        {
            items = items
                .Where(q => string.IsNullOrEmpty(q.DatabaseName) ||
                    !excludedDatabases.Any(e =>
                        string.Equals(e, q.DatabaseName, StringComparison.OrdinalIgnoreCase)))
                .ToList();
        }

        return items;
    }

    /* ---------------- database file growth (#2349) ---------------- */

    /// <summary>
    /// Per-file current size, growth over the lookback window, and the file's volume.
    ///
    /// <para><b>Newest per file, and a baseline from the window's far edge.</b> <c>DISTINCT ON</c> takes the
    /// current row per (database, file); the baseline join takes the OLDEST sample inside the window for the
    /// same key. Growth is the difference, and is 0 when the window holds a single sample — which reads as "no
    /// rise observed" rather than as a rise of the whole file, the wrong answer for a server that just started
    /// collecting.</para>
    ///
    /// <para>Both sides are bounded on <c>collection_time</c>, the partitioning column, so the window prunes
    /// chunks rather than scanning retention. The reported window width is measured rather than assumed, so a
    /// gap in collection cannot make a slow rise look fast.</para>
    ///
    /// <para>$1 server_id, $2 window start (naive UTC).</para>
    /// </summary>
    public const string DatabaseFileGrowthSql = @"
WITH current_files AS (
    SELECT DISTINCT ON (database_name, file_name)
        database_name, file_name, physical_name, file_type_desc, collection_time,
        total_size_mb, auto_growth_mb, is_percent_growth, growth_pct, max_size_mb,
        volume_mount_point, volume_total_mb, volume_free_mb
    FROM database_size_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    ORDER BY database_name, file_name, collection_time DESC
),
baseline AS (
    SELECT DISTINCT ON (database_name, file_name)
        database_name, file_name, collection_time, total_size_mb
    FROM database_size_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    ORDER BY database_name, file_name, collection_time ASC
)
SELECT
    c.database_name,
    c.file_name,
    COALESCE(c.physical_name, '') AS physical_name,
    COALESCE(c.file_type_desc, '') AS file_type_desc,
    COALESCE(c.total_size_mb, 0) AS total_size_mb,
    COALESCE(c.total_size_mb, 0) - COALESCE(b.total_size_mb, c.total_size_mb, 0) AS growth_mb,
    COALESCE(EXTRACT(EPOCH FROM (c.collection_time - b.collection_time)) / 60.0, 0) AS growth_window_minutes,
    COALESCE(c.volume_mount_point, '') AS volume_mount_point,
    COALESCE(c.volume_total_mb, 0) AS volume_total_mb,
    COALESCE(c.volume_free_mb, 0) AS volume_free_mb,
    c.auto_growth_mb,
    COALESCE(c.is_percent_growth, false) AS is_percent_growth,
    c.growth_pct,
    c.max_size_mb
FROM current_files c
LEFT JOIN baseline b
  ON  b.database_name = c.database_name
  AND b.file_name = c.file_name
WHERE c.total_size_mb IS NOT NULL
ORDER BY c.database_name, c.file_name";

    public async Task<List<DatabaseFileGrowthInfo>> GetDatabaseFileGrowthAsync(
        string serverKey, int lookbackMinutes, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        var windowStart = DateTime.SpecifyKind(
            DateTime.UtcNow.AddMinutes(-Math.Max(1, lookbackMinutes)), DateTimeKind.Unspecified);

        var items = new List<DatabaseFileGrowthInfo>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(DatabaseFileGrowthSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(windowStart);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new DatabaseFileGrowthInfo
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                FileName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                PhysicalName = reader.GetString(2),
                FileTypeDesc = reader.GetString(3),
                TotalSizeMb = Convert.ToDouble(reader.GetValue(4)),
                GrowthMb = Convert.ToDouble(reader.GetValue(5)),
                GrowthWindowMinutes = Convert.ToDouble(reader.GetValue(6)),
                VolumeMountPoint = reader.GetString(7),
                VolumeTotalMb = Convert.ToDouble(reader.GetValue(8)),
                VolumeFreeMb = Convert.ToDouble(reader.GetValue(9)),
                AutoGrowthMb = reader.IsDBNull(10) ? null : Convert.ToDouble(reader.GetValue(10)),
                IsPercentGrowth = !reader.IsDBNull(11) && reader.GetBoolean(11),
                GrowthPct = reader.IsDBNull(12) ? null : Convert.ToDouble(reader.GetValue(12)),
                MaxSizeMb = reader.IsDBNull(13) ? null : Convert.ToDouble(reader.GetValue(13)),
            });
        }

        return items;
    }

    /* ---------------- volume free space ---------------- */

    /// <summary>Lite's per-volume free-space read verbatim (database_size_stats table).</summary>
    public const string VolumeFreeSpaceSql = @"
SELECT
    volume_mount_point,
    MAX(volume_total_mb) AS volume_total_mb,
    MIN(volume_free_mb) AS volume_free_mb
FROM database_size_stats
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM database_size_stats
    WHERE server_id = $1
)
AND   volume_mount_point IS NOT NULL
AND   volume_total_mb > 0
GROUP BY volume_mount_point
ORDER BY MIN(volume_free_mb) / MAX(volume_total_mb)";

    public async Task<List<VolumeFreeSpaceInfo>> GetVolumeFreeSpaceAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);

        var items = new List<VolumeFreeSpaceInfo>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(VolumeFreeSpaceSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new VolumeFreeSpaceInfo
            {
                MountPoint = reader.IsDBNull(0) ? "" : reader.GetString(0),
                TotalMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                FreeMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2))
            });
        }

        return items;
    }

    /* ---------------- persistent version store (#1984) ---------------- */

    /// <summary>
    /// The newest pvs_stats snapshot's ADR databases, worst (highest PVS share) first — the
    /// latest-collection convention <see cref="VolumeFreeSpaceSql"/> uses. ADR-OFF rows are
    /// excluded here rather than engine-side: a database that cannot have a PVS cannot breach,
    /// and every collected server carries system databases with ADR off.
    /// </summary>
    public const string PvsPressureSql = @"
SELECT
    database_name,
    persistent_version_store_size_mb,
    database_data_size_mb,
    current_aborted_transaction_count,
    oldest_active_transaction_id,
    oldest_aborted_transaction_id,
    aborted_version_cleaner_start_time,
    aborted_version_cleaner_end_time
FROM pvs_stats
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM pvs_stats
    WHERE server_id = $1
)
AND   is_accelerated_database_recovery_on
ORDER BY
    CASE WHEN database_data_size_mb > 0
         THEN persistent_version_store_size_mb / database_data_size_mb
         ELSE 0 END DESC";

    public async Task<List<PvsPressureInfo>> GetPvsPressureAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);

        var items = new List<PvsPressureInfo>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(PvsPressureSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new PvsPressureInfo
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                PvsSizeMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                DatabaseDataSizeMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                CurrentAbortedTransactionCount = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                OldestActiveTransactionId = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                OldestAbortedTransactionId = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                /* MS's documented shape for "cleanup is ongoing": a start time with no end time. */
                AbortedCleanupOngoing = !reader.IsDBNull(6) && reader.IsDBNull(7)
            });
        }

        return items;
    }

    /* ---------------- tempdb ---------------- */

    /// <summary>Lite's latest-tempdb-snapshot read verbatim (tempdb_stats table).</summary>
    public const string TempDbSpaceSql = @"
SELECT
    total_reserved_mb,
    unallocated_mb,
    user_object_reserved_mb,
    internal_object_reserved_mb,
    version_store_reserved_mb,
    top_session_tempdb_mb,
    top_session_id,
    max_size_mb
FROM tempdb_stats
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

    public async Task<TempDbSpaceInfo?> GetTempDbSpaceAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);

        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(TempDbSpaceSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new TempDbSpaceInfo
            {
                TotalReservedMb = reader.IsDBNull(0) ? 0 : ToDouble(reader.GetValue(0)),
                UnallocatedMb = reader.IsDBNull(1) ? 0 : ToDouble(reader.GetValue(1)),
                UserObjectReservedMb = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                InternalObjectReservedMb = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                VersionStoreReservedMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                TopConsumerMb = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                TopConsumerSessionId = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                /* NULL on every row collected before the V81 rung, and 0 is what "no ceiling measured"
                   is spelled as — so history keeps reporting the percentage it always did rather than
                   dividing by a zero cap. */
                MaxSizeMb = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7))
            };
        }

        return null;
    }

    /* ---------------- anomalous jobs ---------------- */

    /// <summary>
    /// Lite's anomalous-jobs read verbatim (running_jobs table). $2 is the threshold percent
    /// (multiplier x 100, as numeric — percent_of_average is numeric(10,1)).
    /// </summary>
    public const string AnomalousJobsSql = @"
SELECT
    job_name,
    job_id,
    current_duration_seconds,
    avg_duration_seconds,
    p95_duration_seconds,
    percent_of_average,
    start_time
FROM running_jobs
WHERE server_id = $1
AND collection_time = (SELECT MAX(collection_time) FROM running_jobs WHERE server_id = $1)
AND avg_duration_seconds >= 60
AND percent_of_average >= $2
ORDER BY percent_of_average DESC
LIMIT 5";

    public async Task<AnomalousJobsResult> GetAnomalousJobsAsync(
        string serverKey, int multiplier, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);
        var thresholdPercent = (decimal)(multiplier * 100);

        var items = new List<AnomalousJobInfo>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

        /* #1812: the latest snapshot is only evidence when fresh — a stopped collector, missed cycles,
           or lost msdb access leaves a stale "latest" that would otherwise read as NOW, and the engine's
           per-run cooldown key expires each pass, so a stale snapshot re-fires the same historical run
           every cooldown, forever. Same rule as Lite's adapter; parity is the point. */
        using (var snapshotProbe = new NpgsqlCommand(
            "SELECT MAX(collection_time) FROM running_jobs WHERE server_id = $1", connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
        {
            snapshotProbe.Parameters.AddWithValue(serverId);
            var snapshot = await snapshotProbe.ExecuteScalarAsync(cancellationToken);
            var cadence = ResolveRunningJobsCadence(serverId);
            if (snapshot is not DateTime snapshotTime
                || DateTime.UtcNow - snapshotTime > AnomalousJobsResult.MaxSnapshotAge(cadence))
            {
                return AnomalousJobsResult.Stale;
            }
        }

        using var command = new NpgsqlCommand(AnomalousJobsSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(thresholdPercent);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new AnomalousJobInfo
            {
                JobName = reader.GetString(0),
                JobId = reader.IsDBNull(1) ? "" : reader.GetString(1),
                CurrentDurationSeconds = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                AvgDurationSeconds = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                P95DurationSeconds = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                PercentOfAverage = reader.IsDBNull(5) ? null : reader.GetDecimal(5),
                StartTime = reader.IsDBNull(6) ? DateTime.MinValue : reader.GetDateTime(6)
            });
        }

        return new AnomalousJobsResult(SnapshotIsFresh: true, items);
    }

    /* ---------------- database state (baseline deviation) ---------------- */

    /// <summary>
    /// Seeds a first-observation baseline for any database in the latest snapshot that has none
    /// (insert-if-absent; never overwrites an existing baseline or user override). A first observation in
    /// an integrity or transient state is deliberately NOT baselined
    /// (<see cref="DatabaseStateTokens.NeverBaselinedSqlList"/>): onboarding a server mid-outage or
    /// mid-restore must not learn that state as expected — such a database stays pending (no row) until it
    /// settles into a steady state, and the deviation read alerts meanwhile only if the state is critical.
    /// config schema qualified explicitly; database_states resolves to collect through the search_path.
    /// $1 server_id.
    /// </summary>
    public const string SeedDatabaseStateExpectedSql = $@"
INSERT INTO config.database_state_expected (server_id, database_name, expected_state, is_user_override, updated_at)
SELECT $1, ds.database_name, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END, false, (now() AT TIME ZONE 'UTC')
FROM database_states ds
WHERE ds.server_id = $1
AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
AND   ds.state_desc IS NOT NULL
AND   (CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END) NOT IN ({DatabaseStateTokens.NeverBaselinedSqlList})
ON CONFLICT (server_id, database_name) DO NOTHING";

    /// <summary>
    /// #2189: re-learns an ILLEGITIMATE inferred baseline as ONLINE once the database reaches ONLINE. The
    /// rule is the seed's own, applied after the fact — an expectation recording a state the seed would
    /// refuse to learn (<see cref="DatabaseStateTokens.NeverBaselinedSqlList"/>) is not a baseline at all,
    /// it is a snapshot of a database mid-something, and the moment that database is demonstrably healthy
    /// the honest move is to learn the steady state rather than page about the improvement forever.
    ///
    /// <para>This is what heals the rows the old seed already poisoned, which the widened exclusion above
    /// cannot: a database baselined RESTORING mid-restore deviated by being healthy forever, and the only
    /// escape was an operator noticing and re-baselining by hand. It also covers the route that is still
    /// open and always will be — "reset to current" pressed during a restore, or during an outage, records
    /// whatever it sees with no state filter at all, and this un-writes it on the next sweep.</para>
    ///
    /// <para>Two gates, and both matter more than they look.</para>
    ///
    /// <para><c>is_user_override = false</c>: an operator who declared an expected state MEANT it, and
    /// #2166's composition contract depends on that — a database parked at expected OFFLINE stays silent
    /// while parked and still alerts the moment it comes back ONLINE. Only the machine's own inference is
    /// second-guessed, never the operator's.</para>
    ///
    /// <para>The state list, which is deliberately NOT "anything that is not ONLINE". OFFLINE and STANDBY
    /// are steady states the seed is happy to learn, and leaving one is real news that must still fire.
    /// A STANDBY secondary that turns up ONLINE has stopped being a secondary — somebody recovered it, log
    /// shipping is broken, and healing it would replace that alert with silence and then fire the moment
    /// the operator FIXED it. An auto-baselined OFFLINE database brought up for an hour and re-parked would
    /// come back deviating forever against a baseline it never had. Both are the reported bug's own shape,
    /// which is why the heal only ever touches states that were never a legitimate baseline.</para>
    ///
    /// <para>Reads the EFFECTIVE state, not <c>state_desc</c> — the same CASE as the seed and the deviation
    /// read. That is load-bearing rather than cosmetic: a standby log-shipping secondary reports
    /// <c>state_desc = 'ONLINE'</c> with <c>is_in_standby</c> set, so matching on the raw column would
    /// re-baseline every such secondary to ONLINE and then alert it forever for being STANDBY — the very
    /// bug being fixed, re-created for the one database family #1986 went out of its way to keep quiet.</para>
    ///
    /// <para>The alerted-state memory is dropped with the baseline it described (#2166). A memory saying
    /// "the operator was told about ONLINE" only meant anything against the stale expectation; carried
    /// past it, it would judge the next episode against an announcement about a baseline that no longer
    /// exists. Clearing is the safe direction — it can cost an extra alert, never a missed one. $1
    /// server_id.</para>
    /// </summary>
    public const string HealDatabaseStateBaselineToOnlineSql = $@"
UPDATE config.database_state_expected e
SET expected_state = 'ONLINE',
    updated_at = (now() AT TIME ZONE 'UTC'),
    last_alerted_state = NULL,
    last_alerted_at = NULL
WHERE e.server_id = $1
AND   e.is_user_override = false
AND   e.expected_state IN ({DatabaseStateTokens.NeverBaselinedSqlList})
AND   EXISTS (
    SELECT 1
    FROM database_states ds
    WHERE ds.server_id = $1
    AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
    AND   ds.database_name = e.database_name
    AND   (CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END) = 'ONLINE'
)";

    /// <summary>
    /// Tidies auto-baselines for databases no longer in the newest snapshot (dropped/renamed); user
    /// overrides are kept. $1 server_id.
    /// </summary>
    public const string PruneDatabaseStateExpectedSql = @"
DELETE FROM config.database_state_expected e
WHERE e.server_id = $1
AND   e.is_user_override = false
AND   NOT EXISTS (
    SELECT 1 FROM database_states ds
    WHERE ds.server_id = $1
    AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
    AND   ds.database_name = e.database_name
)";

    /// <summary>
    /// #2166: clears the alerted-state memory for any database the store now shows back AT its expected
    /// state. Runs beside the seed and the prune, on the same connection, for the same reason they do — it
    /// is store maintenance derived from what the store holds, not from anything a process observed.
    ///
    /// <para>That distinction is the whole point. The engine also clears on the falling edge it witnesses,
    /// but that path is reachable only through its in-memory active set, which empties on every restart. A
    /// service restart landing between an alert and the recovery therefore left the persisted
    /// <c>last_alerted_state</c> sticky forever: the database was never in <c>active</c> to be noticed as
    /// recovered, so the next parking read as already-announced and was swallowed. This statement cannot
    /// have that gap, because it asks the store rather than remembering. The engine's clear stays as the
    /// immediate path — a recovery inside one process should not wait for the next cycle's sweep — and this
    /// is what actually owns the invariant.</para>
    ///
    /// <para>One sample at expected is enough, deliberately, where the DEVIATION rule needs two: clearing is
    /// the safe direction (it can only cause an extra alert, never a missed one), and a flap cannot exploit
    /// it because a flap does not survive the two-sample deviation test to alert in the first place. The
    /// "(ignore)" sentinel clears too — an operator silencing a database should not leave a memory behind
    /// that outlives the silence. $1 server_id.</para>
    /// </summary>
    public const string ClearRecoveredDatabaseStateAlertsSql = @"
UPDATE config.database_state_expected e
SET last_alerted_state = NULL,
    last_alerted_at = NULL
WHERE e.server_id = $1
AND   e.last_alerted_state IS NOT NULL
AND   (e.expected_state = '(ignore)'
       OR EXISTS (
           SELECT 1
           FROM database_states ds
           WHERE ds.server_id = $1
           AND   ds.collection_time = (SELECT MAX(collection_time) FROM database_states WHERE server_id = $1)
           AND   ds.database_name = e.database_name
           AND   (CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END) = e.expected_state
       ))";

    /// <summary>
    /// The databases whose state deviates from their expected state in BOTH of the two most recent
    /// collections (a two-sample rule that absorbs restart transients — RECOVERY_PENDING / RECOVERING — and
    /// a standby secondary's per-restore RESTORING flicker), plus databases with no baseline yet whose
    /// effective state is critical in both samples (a pending critical first observation). Uses the
    /// effective state (STANDBY for a log-shipping secondary, else state_desc). Skips the "(ignore)"
    /// sentinel; each row carries current + expected (expected is empty for a pending row). Lite's DuckDB
    /// read ported to Postgres. $1 server_id.
    /// </summary>
    public const string DatabaseStateDeviationsSql = $@"
WITH newest AS (
    SELECT MAX(collection_time) AS t FROM database_states WHERE server_id = $1
),
prev AS (
    SELECT MAX(collection_time) AS t FROM database_states
    WHERE server_id = $1 AND collection_time < (SELECT t FROM newest)
),
latest AS (
    SELECT ds.database_name, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END AS eff
    FROM database_states ds
    WHERE ds.server_id = $1 AND ds.collection_time = (SELECT t FROM newest)
),
previous AS (
    SELECT ds.database_name, CASE WHEN ds.is_in_standby THEN 'STANDBY' ELSE ds.state_desc END AS eff
    FROM database_states ds
    WHERE ds.server_id = $1 AND ds.collection_time = (SELECT t FROM prev)
)
SELECT l.database_name, l.eff, COALESCE(e.expected_state, ''), COALESCE(e.last_alerted_state, '')
FROM latest l
JOIN previous p
  ON p.database_name = l.database_name
LEFT JOIN config.database_state_expected e
  ON  e.server_id = $1
  AND e.database_name = l.database_name
WHERE (e.expected_state IS NULL
        AND l.eff IN ({DatabaseStateTokens.CriticalSqlList})
        AND p.eff IN ({DatabaseStateTokens.CriticalSqlList}))
   OR (e.expected_state IS NOT NULL AND e.expected_state <> '(ignore)'
        AND l.eff IS DISTINCT FROM e.expected_state
        AND p.eff IS DISTINCT FROM e.expected_state)
ORDER BY l.database_name";

    public async Task<List<DatabaseStateInfo>> GetDatabaseStatesAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);

        var items = new List<DatabaseStateInfo>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

        using (var seed = new NpgsqlCommand(SeedDatabaseStateExpectedSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
        {
            seed.Parameters.AddWithValue(serverId);
            await seed.ExecuteNonQueryAsync(cancellationToken);
        }

        /* Beside the seed because it is the same job from the other end (#2189): the seed learns a baseline
           for a database that has none, this un-learns one the database has since outgrown. Both run before
           the read, so a poisoned expectation is corrected on the cycle that notices it rather than firing
           once more first. */
        using (var heal = new NpgsqlCommand(HealDatabaseStateBaselineToOnlineSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
        {
            heal.Parameters.AddWithValue(serverId);
            await heal.ExecuteNonQueryAsync(cancellationToken);
        }

        using (var prune = new NpgsqlCommand(PruneDatabaseStateExpectedSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
        {
            prune.Parameters.AddWithValue(serverId);
            await prune.ExecuteNonQueryAsync(cancellationToken);
        }

        /* Before the read, so this cycle judges against a memory the store has already healed rather than
           one carried over from a restart (#2166). A database cleared here is one that is back at its
           expected state, so it cannot appear in the deviation read below either way — the ordering matters
           for the NEXT deviation, not this one. */
        using (var clearRecovered = new NpgsqlCommand(ClearRecoveredDatabaseStateAlertsSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
        {
            clearRecovered.Parameters.AddWithValue(serverId);
            await clearRecovered.ExecuteNonQueryAsync(cancellationToken);
        }

        using (var command = new NpgsqlCommand(DatabaseStateDeviationsSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds })
        {
            command.Parameters.AddWithValue(serverId);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                items.Add(new DatabaseStateInfo
                {
                    DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                    StateDesc = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    ExpectedState = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    LastAlertedState = reader.IsDBNull(3) ? "" : reader.GetString(3)
                });
            }
        }

        return items;
    }

    /// <summary>
    /// Forced plans whose failure counter ROSE between the two most recent collections that carried the
    /// plan (#2157). $1 server_id.
    ///
    /// <para>Shape notes: query_store_stats holds one row per plan PER INTERVAL per collection, and the
    /// forcing columns are plan-level attributes repeated across those rows — so the CTE collapses each
    /// (plan, collection_time) to one value with MAX before any comparison. The two-hour window bounds
    /// the hypertable scan; a plan not collected within it is by definition not failing right now, and
    /// Query Store's own flush cadence (900s) means an active plan appears several times inside it.</para>
    ///
    /// <para>The <c>&gt;</c> comparison is what makes this a delta read: equal counters are silence, and a
    /// LOWER counter (unforce/re-force reset) is silence too rather than a negative delta.</para>
    /// </summary>
    public const string ForcePlanFailuresSql = @"
WITH per_collection AS (
    SELECT
        qs.database_name,
        qs.query_id,
        qs.plan_id,
        qs.collection_time,
        MAX(COALESCE(qs.force_failure_count, 0)) AS failures,
        MAX(CASE WHEN qs.is_forced_plan THEN 1 ELSE 0 END) AS forced,
        MAX(COALESCE(qs.plan_forcing_type, '')) AS forcing_type,
        MAX(COALESCE(qs.last_force_failure_reason, '')) AS reason
    FROM query_store_stats AS qs
    WHERE qs.server_id = $1
    AND   qs.collection_time > now() - interval '2 hours'
    GROUP BY qs.database_name, qs.query_id, qs.plan_id, qs.collection_time
),
ranked AS (
    SELECT
        pc.*,
        ROW_NUMBER() OVER (PARTITION BY pc.database_name, pc.query_id, pc.plan_id ORDER BY pc.collection_time DESC) AS rn
    FROM per_collection AS pc
)
SELECT
    n.database_name,
    n.query_id,
    n.plan_id,
    n.forcing_type,
    n.reason,
    n.failures - p.failures AS failure_delta,
    n.failures AS total_failures
FROM ranked AS n
JOIN ranked AS p
  ON  p.database_name = n.database_name
  AND p.query_id = n.query_id
  AND p.plan_id = n.plan_id
  AND p.rn = 2
WHERE n.rn = 1
AND   n.forced = 1
AND   n.failures > p.failures
ORDER BY n.database_name, n.query_id, n.plan_id";

    public async Task<List<ForcePlanFailureInfo>> GetForcePlanFailuresAsync(
        string serverKey, CancellationToken cancellationToken = default)
    {
        var serverId = ParseServerKey(serverKey);

        var items = new List<ForcePlanFailureInfo>();
        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
        using var command = new NpgsqlCommand(ForcePlanFailuresSql, connection) { CommandTimeout = AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new ForcePlanFailureInfo
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                QueryId = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                PlanId = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                ForcingType = reader.IsDBNull(3) ? "" : reader.GetString(3),
                FailureReason = reader.IsDBNull(4) ? "" : reader.GetString(4),
                FailureDelta = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                TotalFailures = reader.IsDBNull(6) ? 0 : reader.GetInt64(6)
            });
        }

        return items;
    }

    private int ResolveRunningJobsCadence(int serverId) =>
        ResolveCadence(_runningJobsCadenceMinutes, serverId, "running_jobs");

    /// <summary>
    /// A server's effective cadence for one collector: the worker's resolver when it answers usefully,
    /// otherwise the shipped default, otherwise 2 minutes (an unregistered collector name).
    /// </summary>
    private static int ResolveCadence(Func<int, int>? resolver, int serverId, string collectorName)
    {
        var resolved = resolver?.Invoke(serverId) ?? 0;
        if (resolved > 0)
        {
            return resolved;
        }

        return PerformanceMonitor.Collectors.CollectorScheduleDefaults.All.TryGetValue(collectorName, out var schedule)
            ? schedule.FrequencyMinutes
            : 2;
    }

    /* ---------------- helpers ---------------- */

    /// <summary>Naive-UTC now, Kind-Unspecified — the product's PG timestamp discipline.</summary>
    private static DateTime NaiveUtcNow() =>
        DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

    /// <summary>The (start, end) collection_time window Lite's GetTimeRange produces for hoursBack.</summary>
    private static (DateTime StartTime, DateTime EndTime) Window(int hoursBack)
    {
        var endTime = NaiveUtcNow();
        return (endTime.AddHours(-hoursBack), endTime);
    }

    private static int ParseServerKey(string serverKey) =>
        int.Parse(serverKey, CultureInfo.InvariantCulture);

    /// <summary>numeric(p,s) columns read back as decimal — coerce like Lite's ToDouble.</summary>
    private static double ToDouble(object value) =>
        Convert.ToDouble(value, CultureInfo.InvariantCulture);
}
