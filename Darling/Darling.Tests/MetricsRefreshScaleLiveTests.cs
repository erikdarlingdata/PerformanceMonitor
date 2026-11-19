/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Metrics;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9013: the measured cost of the /metrics background refresh on a large store, which the port was approved on. Each
/// case seeds one scratch store with many SQL Servers and 6 hours of 1-minute history (a big <c>file_io_stats</c> among
/// it), runs one warm-up refresh and ten timed ones, and writes the fleet and server-set percentiles, each server-set
/// statement's own median and the plan of the disk statement as Markdown. Opt-in through <c>DARLING_METRICS_SCALE=1</c>:
/// the seed takes minutes and gigabytes, so CI never runs it. It asserts only that every refresh fits the cache's
/// refresh timeout; the numbers are the output.
/// </summary>
/* #1776 own-store: each case mints its own scratch database through ScratchPostgres and seeds only that database, so
   this class is deliberately NOT [Collection("live-postgres")]; it is in the timing collection because it times work,
   and a class is in one collection only. The scratch database is dropped on disposal, which is the cleanup. */
[Collection("timing")]
public sealed class MetricsRefreshScaleLiveTests
{
    private const int HistoryMinutes = 360;
    private const int SliceMinutes = 30;
    private const int FilesPerCollection = 272;
    private const int WaitTypes = 150;
    private const int TimedRefreshes = 10;
    private const int StatementRuns = 10;
    private const int SeedTimeoutSeconds = 3600;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Theory]
    [InlineData(50)]
    [InlineData(200)]
    public async Task RefreshDuration_AtScale_IsReported(int servers)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the refresh scale measurement.");
        Assert.SkipUnless(Environment.GetEnvironmentVariable("DARLING_METRICS_SCALE") == "1",
            "Set DARLING_METRICS_SCALE=1 to run the refresh scale measurement: it seeds millions of rows per case.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* #9013: the store's clock is pinned to a fixed instant inside one chunk, so the seed's newest collection is 30 s
           old whatever the seed took, and every refresh reads the same steady-state window. Durations still come from the
           real timestamp clock. */
        var anchor = LiveClock.AnchoredEnd();
        string layout;
        long fileIoRows;
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            layout = await PrepareStoreAsync(connection, scratch.TimescaleEnabled, ct);
            await SeedAsync(connection, servers, anchor, ct);
            fileIoRows = await ScalarAsync<long>(connection, "SELECT count(*) FROM file_io_stats", ct);
        }

        var refresher = new DarlingMetricsRefresher(
            _ => Task.FromResult<NpgsqlDataSource?>(postgres), new ReadAdmission(), registry: null, Collecting(),
            new PinnedNow(new DateTimeOffset(DateTime.SpecifyKind(anchor, DateTimeKind.Utc))), new CapturingTestLogger());

        var warm = await refresher.RefreshAsync(ct);
        var reporting = warm.Samples.Where(s => s.Name == "darling_disk_read_latency_ms")
            .Select(s => s.Labels[0].Value).Distinct(StringComparer.Ordinal).Count();
        Assert.True(reporting == servers,
            $"the plant is meant to give every server a fresh disk reading, but {reporting} of {servers} have one; the numbers below would not measure a full server set");

        var fleet = new List<double>();
        var serverSet = new List<double>();
        var total = new List<double>();
        for (var i = 0; i < TimedRefreshes; i++)
        {
            var started = Stopwatch.GetTimestamp();
            await refresher.RefreshAsync(ct);
            var elapsed = Stopwatch.GetElapsedTime(started);
            Assert.True(elapsed < DarlingMetricsSnapshotCache.RefreshTimeout,
                $"refresh {i + 1} took {elapsed.TotalMilliseconds:F0} ms, past the cache's {DarlingMetricsSnapshotCache.RefreshTimeout.TotalSeconds:F0} s refresh timeout");
            total.Add(elapsed.TotalMilliseconds);
            fleet.Add(refresher.LastFleetDuration!.Value.TotalMilliseconds);
            serverSet.Add(refresher.LastServerSetDuration!.Value.TotalMilliseconds);
        }

        var statements = new List<(string Name, int Rows, double P50)>();
        string fileIoPlan;
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            foreach (var (name, sql) in ServerSetStatements())
            {
                statements.Add(await TimeStatementAsync(connection, name, sql, anchor, ct));
            }

            fileIoPlan = await ExplainAsync(connection, DarlingMetricsServerSetReader.FileIoSql, anchor, ct);
        }

        var markdown = Markdown(servers, fileIoRows, layout, fleet, serverSet, total, statements, fileIoPlan);
        TestContext.Current.SendDiagnosticMessage(markdown);
        File.WriteAllText(Path.Combine(Path.GetTempPath(), $"darling-metrics-scale-{servers}.md"), markdown);
    }

    /// <summary>The service's own store layout: every collector table a hypertable, as the conversion step leaves it,
    /// with the scratch database's background jobs stopped so no policy runs during the timing. Returns a note for the
    /// table.</summary>
    private static async Task<string> PrepareStoreAsync(NpgsqlConnection connection, bool timescale, CancellationToken ct)
    {
        if (!timescale)
        {
            return "plain PostgreSQL tables (TimescaleDB is not enabled on this cluster)";
        }

        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        var converted = await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        return $"TimescaleDB hypertables ({converted} collector tables converted)";
    }

    /// <summary>
    /// Every server's history, server-side: the registry, then the 1-minute tables in 30-minute slices, oldest slice
    /// first and in time order inside each, so the heap is laid out as collection writes it (time major, not server
    /// major); then the 5-minute sessions and the hourly sizes. <c>ANALYZE</c> last.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, int servers, DateTime anchor, CancellationToken ct)
    {
        await ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, engine_kind)
SELECT sv.id, 'scale-' || sv.id, 'scale-' || sv.id, TRUE, 3, $2
FROM generate_series(1, $1) AS sv (id)", ct, servers, MonitoredEngineKind.SqlServer);

        for (var newest = HistoryMinutes - SliceMinutes; newest >= 0; newest -= SliceMinutes)
        {
            object[] slice = [anchor, servers, newest, newest + SliceMinutes - 1];
            foreach (var sql in OneMinuteSeeds)
            {
                await ExecAsync(connection, sql, ct, slice);
            }
        }

        await ExecAsync(connection, SessionsSeed, ct, anchor, servers);
        await ExecAsync(connection, DatabaseSizeSeed, ct, anchor, servers);
        await ExecAsync(connection, "ANALYZE", ct);
    }

    /* The 1-minute seeds share their parameters: $1 the pinned now, $2 the server count, $3 and $4 the newest and oldest
       minute of the slice (k minutes back from the newest collection, which is 30 s before now). */
    private const string SliceTimes = @"
FROM generate_series($4::int, $3::int, -1) AS k
CROSS JOIN generate_series(1, $2::int) AS sv (id)";

    private const string CollectionTime = "$1::timestamp - interval '30 seconds' - k * interval '1 minute'";

    private static readonly string[] OneMinuteSeeds =
    {
        /* 272 files: tempdb's 8 data files and its log, then 66 user databases of 3 data files and a log each. */
        $@"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
    num_of_reads, num_of_writes, read_bytes, write_bytes, io_stall_read_ms, io_stall_write_ms,
    delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
SELECT (sv.id::bigint * 1000 + k) * 1000 + f, {CollectionTime}, sv.id, 'scale-' || sv.id,
       d.database_name, d.database_name || '_' || f, d.file_type, 'D:\Data\' || d.database_name || '_' || f || '.mdf', 1024 + f,
       10000000 - k * 300 + f, 5000000 - k * 200 + f, (10000000 - k * 300 + f)::bigint * 65536, (5000000 - k * 200 + f)::bigint * 8192,
       40000000 - k * 900 + f, 9000000 - k * 400 + f,
       (f % 17) * 10 + k % 7, (f % 13) * 5 + k % 5, ((f % 17) * 10 + k % 7) * 65536, ((f % 13) * 5 + k % 5) * 8192,
       ((f % 17) * 10 + k % 7) * (1 + f % 9), ((f % 13) * 5 + k % 5) * (1 + f % 4), 60
{SliceTimes}
CROSS JOIN generate_series(0, {FilesPerCollection - 1}) AS f
CROSS JOIN LATERAL (
    SELECT CASE WHEN f < 9 THEN 'tempdb' ELSE 'Db' || ((f - 9) / 4) END AS database_name,
           CASE WHEN f = 8 OR (f >= 9 AND (f - 9) % 4 = 3) THEN 'LOG' ELSE 'ROWS' END AS file_type
) AS d
ORDER BY k DESC, sv.id, f",

        /* 150 wait types: ten real names, then numbered variants of them. */
        $@"
INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
    delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
SELECT (sv.id::bigint * 1000 + k) * 1000 + w, {CollectionTime}, sv.id, 'scale-' || sv.id,
       (ARRAY['PAGEIOLATCH_SH','LCK_M_X','CXPACKET','WRITELOG','SOS_SCHEDULER_YIELD','ASYNC_NETWORK_IO','RESOURCE_SEMAPHORE','LATCH_EX','PAGELATCH_EX','HADR_SYNC_COMMIT'])[1 + w % 10]
           || CASE WHEN w < 10 THEN '' ELSE '_' || w END,
       100000 - k * 10 + w, 9000000 - k * 500 + w, 900000 - k * 50 + w,
       1 + w % 7, 1 + (w * 37 + k) % 5000, (w * 37 + k) % 500, 60
{SliceTimes}
CROSS JOIN generate_series(0, {WaitTypes - 1}) AS w
ORDER BY k DESC, sv.id, w",

        /* The 12 counters the statement reads; the Databases and Locks objects carry 20 instances and _Total. */
        $@"
INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value,
    delta_cntr_value, sample_interval_seconds, cntr_type)
SELECT (sv.id::bigint * 1000 + k) * 1000 + c.n * 30 + i.n, {CollectionTime}, sv.id, 'scale-' || sv.id, c.object_name, c.counter_name, i.instance_name,
       CASE WHEN c.cntr_type = 65792 THEN (k + sv.id) % 4 ELSE 50000000 - k * 6000 + i.n END,
       CASE WHEN c.cntr_type = 65792 THEN NULL ELSE 6000 + (k * 13 + i.n) % 900 END,
       CASE WHEN c.cntr_type = 65792 THEN NULL ELSE 60 END,
       c.cntr_type
{SliceTimes}
CROSS JOIN (
    VALUES (1, 'SQLServer:SQL Statistics', 'Batch Requests/sec', false, 272696576),
           (2, 'SQLServer:SQL Statistics', 'SQL Compilations/sec', false, 272696576),
           (3, 'SQLServer:SQL Statistics', 'SQL Re-Compilations/sec', false, 272696576),
           (4, 'SQLServer:Databases', 'Transactions/sec', true, 272696576),
           (5, 'SQLServer:Databases', 'Log Flushes/sec', true, 272696576),
           (6, 'SQLServer:Databases', 'Log Bytes Flushed/sec', true, 272696576),
           (7, 'SQLServer:Locks', 'Lock Waits/sec', true, 272696576),
           (8, 'SQLServer:General Statistics', 'Processes blocked', false, 65792),
           (9, 'SQLServer:Buffer Manager', 'Lazy writes/sec', false, 272696576),
           (10, 'SQLServer:Buffer Manager', 'Page reads/sec', false, 272696576),
           (11, 'SQLServer:Buffer Manager', 'Page writes/sec', false, 272696576),
           (12, 'SQLServer:Buffer Manager', 'Free list stalls/sec', false, 272696576)
) AS c (n, object_name, counter_name, has_total, cntr_type)
CROSS JOIN LATERAL (
    SELECT g AS n, CASE WHEN g = 20 THEN '_Total' ELSE 'Db' || g END AS instance_name
    FROM generate_series(0, 20) AS g
    WHERE c.has_total
    UNION ALL
    SELECT 0, ''
    WHERE NOT c.has_total
) AS i
ORDER BY k DESC, sv.id, c.n, i.n",

        $@"
INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization,
    other_process_cpu_utilization, sample_time_utc)
SELECT sv.id::bigint * 1000 + k, {CollectionTime}, sv.id, 'scale-' || sv.id, {CollectionTime}, (k * 7 + sv.id) % 90, (k + sv.id) % 5, {CollectionTime}
{SliceTimes}
ORDER BY k DESC, sv.id",

        $@"
INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
    total_page_file_mb, available_page_file_mb, system_memory_state, sql_memory_model, target_server_memory_mb, total_server_memory_mb,
    buffer_pool_mb, plan_cache_mb, max_workers_count, current_workers_count)
SELECT sv.id::bigint * 1000 + k, {CollectionTime}, sv.id, 'scale-' || sv.id, 131072, 8192 + k % 100, 140000, 20000,
       'Available physical memory is high', 'CONVENTIONAL', 114688, 114000 - k % 50, 98000, 4096 + k % 30, 1472, 300 + k % 40
{SliceTimes}
ORDER BY k DESC, sv.id",

        /* Two semaphores of two pools each, as the default and internal pools report. */
        $@"
INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id, target_memory_mb,
    max_target_memory_mb, total_memory_mb, available_memory_mb, granted_memory_mb, used_memory_mb, grantee_count, waiter_count,
    timeout_error_count, forced_grant_count, sample_interval_seconds)
SELECT (sv.id::bigint * 1000 + k) * 10 + g.n, {CollectionTime}, sv.id, 'scale-' || sv.id, g.semaphore, g.pool, 80000, 80000, 80000,
       60000, 20000 + k % 100, 15000, 3 + k % 4, k % 2, 0, 0, 60
{SliceTimes}
CROSS JOIN (VALUES (1, 0::smallint, 1), (2, 0::smallint, 2), (3, 1::smallint, 1), (4, 1::smallint, 2)) AS g (n, semaphore, pool)
ORDER BY k DESC, sv.id, g.n",

        $@"
INSERT INTO tempdb_stats (collection_id, collection_time, server_id, server_name, user_object_reserved_mb, internal_object_reserved_mb,
    version_store_reserved_mb, total_reserved_mb, unallocated_mb, total_sessions_using_tempdb, top_session_id, top_session_tempdb_mb,
    max_size_mb, log_total_mb, log_used_mb, log_used_percent, log_reuse_wait_desc)
SELECT sv.id::bigint * 1000 + k, {CollectionTime}, sv.id, 'scale-' || sv.id, 120 + k % 10, 40 + k % 5, 300, 460, 64000, 12, 77, 90,
       65536, 8192, 1200, 14.65, 'NOTHING'
{SliceTimes}
ORDER BY k DESC, sv.id",

        $@"
INSERT INTO cpu_scheduler_stats (collection_id, collection_time, server_id, server_name, max_workers_count, scheduler_count, cpu_count,
    total_runnable_tasks_count, total_work_queue_count, total_current_workers_count, avg_runnable_tasks_count, total_active_request_count,
    total_queued_request_count, total_blocked_task_count, runnable_request_count, total_request_count, runnable_percent)
SELECT sv.id::bigint * 1000 + k, {CollectionTime}, sv.id, 'scale-' || sv.id, 1472, 32, 32, (k + sv.id) % 9, 0, 280, 0.2, 40, 0,
       k % 3, (k + sv.id) % 9, 40, 2.5
{SliceTimes}
ORDER BY k DESC, sv.id",

        /* One collection_log row per 1-minute collector per collection. */
        $@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT (sv.id::bigint * 1000 + k) * 100 + c.n, sv.id, 'scale-' || sv.id, c.name, {CollectionTime}, 40 + (k + c.n) % 200, 'SUCCESS', c.row_count
{SliceTimes}
CROSS JOIN (
    VALUES (1, 'file_io_stats', {FilesPerCollection}), (2, 'wait_stats', {WaitTypes}), (3, 'perfmon_stats', 92), (4, 'cpu_utilization', 1),
           (5, 'memory_stats', 1), (6, 'memory_grant_stats', 4), (7, 'tempdb_stats', 1), (8, 'cpu_scheduler_stats', 1)
) AS c (n, name, row_count)
ORDER BY k DESC, sv.id, c.n",
    };

    /// <summary>Every 5 minutes for the 6 hours, with its collection_log row. $1 the pinned now, $2 the server count.</summary>
    private const string SessionsSeed = @"
WITH s AS (
    INSERT INTO session_summary_stats (collection_id, collection_time, server_id, server_name, total_sessions, running_sessions, sleeping_sessions,
        background_sessions, dormant_sessions, idle_sessions_over_30min, sessions_waiting_for_memory, databases_with_connections,
        top_application_name, top_application_connections, top_host_name, top_host_connections)
    SELECT sv.id::bigint * 1000 + k, $1::timestamp - interval '30 seconds' - k * interval '5 minutes', sv.id, 'scale-' || sv.id,
           400, 10 + k % 7, 300, 60, 30 - k % 7, 40, 0, 25, 'scale app', 120, 'scale host', 90
    FROM generate_series(71, 0, -1) AS k
    CROSS JOIN generate_series(1, $2::int) AS sv (id)
    ORDER BY k DESC, sv.id
    RETURNING collection_id, collection_time, server_id, server_name
)
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT 900000000000 + collection_id, server_id, server_name, 'session_summary_stats', collection_time, 30, 'SUCCESS', 1
FROM s";

    /// <summary>Hourly, 40 files on two volumes, with its collection_log row. $1 the pinned now, $2 the server count.</summary>
    private const string DatabaseSizeSeed = @"
WITH d AS (
    INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc,
        file_name, physical_name, total_size_mb, used_size_mb, auto_growth_mb, max_size_mb, recovery_model_desc, compatibility_level, state_desc,
        volume_mount_point, volume_total_mb, volume_free_mb, is_percent_growth, growth_pct)
    SELECT (sv.id::bigint * 1000 + k) * 100 + f, $1::timestamp - interval '30 seconds' - k * interval '1 hour', sv.id, 'scale-' || sv.id,
           'Db' || (f / 2), 5 + f / 2, 1 + f % 2, CASE WHEN f % 2 = 1 THEN 'LOG' ELSE 'ROWS' END,
           'Db' || (f / 2) || '_' || f, CASE WHEN f % 2 = 1 THEN 'L:\Logs\' ELSE 'D:\Data\' END || 'Db' || (f / 2) || '_' || f || '.mdf',
           2048 + f, 1500 + f, 256, -1, 'FULL', 160, 'ONLINE',
           CASE WHEN f % 2 = 1 THEN 'L:\' ELSE 'D:\' END, CASE WHEN f % 2 = 1 THEN 512000 ELSE 2048000 END,
           CASE WHEN f % 2 = 1 THEN 300000 ELSE 900000 - k * 100 END, false, 0
    FROM generate_series(5, 0, -1) AS k
    CROSS JOIN generate_series(1, $2::int) AS sv (id)
    CROSS JOIN generate_series(0, 39) AS f
    ORDER BY k DESC, sv.id, f
    RETURNING collection_id, collection_time, server_id, server_name
)
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT 800000000000 + (collection_id / 100), server_id, server_name, 'database_size_stats', collection_time, 120, 'SUCCESS', 40
FROM d
WHERE collection_id % 100 = 0";

    /// <summary>The ten statements, in the order the reader runs them, by the name of their constant.</summary>
    private static IEnumerable<(string Name, string Sql)> ServerSetStatements() =>
    [
        (nameof(DarlingMetricsServerSetReader.CpuSql), DarlingMetricsServerSetReader.CpuSql),
        (nameof(DarlingMetricsServerSetReader.SchedulerSql), DarlingMetricsServerSetReader.SchedulerSql),
        (nameof(DarlingMetricsServerSetReader.PerfmonSql), DarlingMetricsServerSetReader.PerfmonSql),
        (nameof(DarlingMetricsServerSetReader.SessionsSql), DarlingMetricsServerSetReader.SessionsSql),
        (nameof(DarlingMetricsServerSetReader.MemorySql), DarlingMetricsServerSetReader.MemorySql),
        (nameof(DarlingMetricsServerSetReader.GrantsSql), DarlingMetricsServerSetReader.GrantsSql),
        (nameof(DarlingMetricsServerSetReader.FileIoSql), DarlingMetricsServerSetReader.FileIoSql),
        (nameof(DarlingMetricsServerSetReader.VolumeSql), DarlingMetricsServerSetReader.VolumeSql),
        (nameof(DarlingMetricsServerSetReader.WaitsSql), DarlingMetricsServerSetReader.WaitsSql),
        (nameof(DarlingMetricsServerSetReader.TempdbSql), DarlingMetricsServerSetReader.TempdbSql),
    ];

    /// <summary>One warm-up run, then <see cref="StatementRuns"/> timed ones, each read to the end as the reader reads it.</summary>
    private static async Task<(string Name, int Rows, double P50)> TimeStatementAsync(
        NpgsqlConnection connection, string name, string sql, DateTime now, CancellationToken ct)
    {
        var rows = await RunAsync(connection, sql, now, ct);
        var times = new List<double>();
        for (var i = 0; i < StatementRuns; i++)
        {
            var started = Stopwatch.GetTimestamp();
            await RunAsync(connection, sql, now, ct);
            times.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
        }

        return (name, rows, Percentile(times, 0.50));
    }

    private static async Task<int> RunAsync(NpgsqlConnection connection, string sql, DateTime now, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(TimestampParameter(now));
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = 0;
        while (await reader.ReadAsync(ct))
        {
            rows++;
        }

        return rows;
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, string sql, DateTime now, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS) " + sql, connection);
        command.Parameters.Add(TimestampParameter(now));
        await using var reader = await command.ExecuteReaderAsync(ct);
        var plan = new StringBuilder();
        while (await reader.ReadAsync(ct))
        {
            plan.AppendLine(reader.GetString(0));
        }

        return plan.ToString();
    }

    /// <summary>The reader binds <c>$1</c> as a naive <c>timestamp</c>; so does every run here.</summary>
    private static NpgsqlParameter TimestampParameter(DateTime now) =>
        new() { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(now, DateTimeKind.Unspecified) };

    /// <summary>Nearest rank: of ten samples the p50 is the 5th fastest and the p95 the slowest.</summary>
    private static double Percentile(List<double> samples, double p)
    {
        var sorted = samples.OrderBy(x => x).ToList();
        var rank = (int)Math.Ceiling(p * sorted.Count);
        return sorted[Math.Max(rank, 1) - 1];
    }

    private static string Markdown(
        int servers, long fileIoRows, string layout, List<double> fleet, List<double> serverSet, List<double> total,
        List<(string Name, int Rows, double P50)> statements, string fileIoPlan)
    {
        static string Ms(double value) => value.ToString("F1", CultureInfo.InvariantCulture);

        var md = new StringBuilder();
        md.AppendLine(CultureInfo.InvariantCulture, $"### /metrics refresh: {servers} servers, 6 h of 1-minute history");
        md.AppendLine();
        md.AppendLine("| servers | file_io_stats rows | fleet p50 ms | fleet p95 ms | server set p50 ms | server set p95 ms | whole refresh p50 ms | whole refresh p95 ms |");
        md.AppendLine("|---:|---:|---:|---:|---:|---:|---:|---:|");
        md.AppendLine(CultureInfo.InvariantCulture,
            $"| {servers} | {fileIoRows:N0} | {Ms(Percentile(fleet, 0.50))} | {Ms(Percentile(fleet, 0.95))} | {Ms(Percentile(serverSet, 0.50))} | {Ms(Percentile(serverSet, 0.95))} | {Ms(Percentile(total, 0.50))} | {Ms(Percentile(total, 0.95))} |");
        md.AppendLine();
        md.AppendLine("| server-set statement | rows returned | p50 ms |");
        md.AppendLine("|---|---:|---:|");
        foreach (var (name, rows, p50) in statements)
        {
            md.AppendLine(CultureInfo.InvariantCulture, $"| {name} | {rows} | {Ms(p50)} |");
        }

        md.AppendLine();
        md.AppendLine("Notes: " + TimedRefreshes + " timed refreshes after one warm-up; percentiles are nearest-rank, so the p95 of ten is the slowest. "
            + "Each statement ran once to warm, then " + StatementRuns + " times on one connection. Store: " + layout + ". "
            + "Per server and collection: 272 files, 150 wait types, the 12 perfmon counters (20 database instances and _Total on the Databases and Locks objects), "
            + "4 memory grant rows and one row each of CPU, scheduler, memory and tempdb; sessions every 5 minutes, database sizes hourly with 40 files, "
            + "a collection_log row per collector per collection. No chunk is compressed: the history is 6 hours, and the store compresses chunks older than a day.");
        md.AppendLine();
        md.AppendLine("EXPLAIN (ANALYZE, BUFFERS) of FileIoSql:");
        md.AppendLine();
        md.AppendLine("```");
        md.Append(fileIoPlan);
        md.AppendLine("```");
        return md.ToString();
    }

    private static CollectorRuntimeState Collecting()
    {
        var state = new CollectorRuntimeState();
        state.PublishCollecting();
        return state;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = SeedTimeoutSeconds };
        foreach (var p in parameters)
        {
            command.Parameters.AddWithValue(p is DateTime dt ? DateTime.SpecifyKind(dt, DateTimeKind.Unspecified) : p);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = SeedTimeoutSeconds };
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>A store clock pinned to one instant; timestamps (and so every measured duration) stay real.</summary>
    private sealed class PinnedNow(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
