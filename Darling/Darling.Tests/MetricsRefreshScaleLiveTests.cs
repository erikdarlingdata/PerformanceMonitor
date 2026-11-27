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
/// case seeds one scratch store with many SQL Servers, 6 hours of 1-minute history in the wide collector tables (a big
/// <c>file_io_stats</c> among them) and 7 days of <c>collection_log</c>, which the fleet read's collection-health
/// rollup reads. It runs one warm-up refresh and ten timed ones, each 61 s after the last on the store clock so each
/// one pays the collection-health read as the metrics host does, and writes the fleet and server-set percentiles, each
/// statement's own percentiles (the collection-health read among them) and two plans as Markdown. Opt-in through
/// <c>DARLING_METRICS_SCALE=1</c>: the seed takes minutes and gigabytes, so CI never runs it. It asserts that every
/// refresh fits the cache's refresh timeout and that the plant is what the numbers claim; the numbers are the output.
/// </summary>
/* #1776 own-store: each case mints its own scratch database through ScratchPostgres and seeds only that database, so
   this class is deliberately NOT [Collection("live-postgres")]; it is in the timing collection because it times work,
   and a class is in one collection only. The scratch database is dropped on disposal, which is the cleanup. */
[Collection("timing")]
public sealed class MetricsRefreshScaleLiveTests
{
    private const int HistoryMinutes = 360;
    private const int SliceMinutes = 30;
    /* #9013: every refresh pays the collection-health read, which spans 7 days, so collection_log holds 7 days and the hour
       the oldest timed window starts in. */
    private const int HealthHistoryMinutes = 7 * 1440 + 60;
    private const int HealthSliceMinutes = 1440;
    private static readonly TimeSpan RefreshStep = TimeSpan.FromSeconds(61);
    private const int FilesPerCollection = 272;
    private const int WaitTypes = 150;
    private const int TimedRefreshes = 10;
    private const int StatementRuns = 10;
    private const int SeedTimeoutSeconds = 3600;
    /* #9013: one server more than the case's count stopped collecting this long before the newest collection: past every
       freshness bound (3 intervals of a 1-minute collector, the 15 and 30 minute floors, the 3 hour floor of the hourly
       sizes), so each statement's per-server probe finds no fresh row for it, the probe a stopped collector costs. */
    private const int StoppedMinutes = 240;

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

        /* #9013: the store's clock starts at a fixed instant inside one chunk, so the seed's newest collection is 30 s
           old whatever the seed took. Durations still come from the real timestamp clock. */
        var anchor = LiveClock.AnchoredEnd();
        string layout;
        var seedClock = Stopwatch.GetTimestamp();
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            layout = await PrepareStoreAsync(connection, scratch.TimescaleEnabled, ct);
            await SeedAsync(connection, servers + 1, anchor, ct);
            await SeedSchedulesAsync(connection, servers + 1, ct);
        }

        var seedSeconds = Stopwatch.GetElapsedTime(seedClock).TotalSeconds;
        string rollupNote;
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            rollupNote = await BuildRollupAsync(connection, scratch.TimescaleEnabled, anchor, ct);
            await ExecAsync(connection, "ANALYZE", ct);
        }

        var rowCounts = new List<(string Table, long Rows)>();
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            foreach (var table in SeededTables)
            {
                rowCounts.Add((table, await ScalarAsync<long>(connection, $"SELECT count(*) FROM {table}", ct)));
            }
        }

        var clock = new PinnedNow(new DateTimeOffset(DateTime.SpecifyKind(anchor, DateTimeKind.Utc)));
        var refresher = new DarlingMetricsRefresher(
            _ => Task.FromResult<NpgsqlDataSource?>(postgres), new ReadAdmission(), registry: null, Collecting(),
            clock, new CapturingTestLogger());

        /* #9013 (C1): the fleet read memoises its 7-day collection-health read per data source for 60 s of the caller's
           clock. The metrics host owns its own data source and refreshes at least 55 s apart, so nearly every refresh
           pays that read; each timed refresh here is 61 s after the last on the store clock so it pays it too, and the
           memo's own count of the reads it started proves each one did. */
        var memo = DarlingFleetReader.CollectionHealthMemoFor(postgres);
        var scansBefore = memo.ScansStarted;
        var warm = await refresher.RefreshAsync(ct);
        AssertPlant(warm, servers, "the warm-up refresh");
        Assert.True(memo.ScansStarted == scansBefore + 1,
            $"the warm-up refresh should have run the collection-health read once, but the memo started {memo.ScansStarted - scansBefore}");

        var fleet = new List<double>();
        var serverSet = new List<double>();
        var total = new List<double>();
        var timedNows = new List<DateTime>();
        var scansPerRefresh = new List<int>();
        MetricsSnapshot? last = null;
        for (var i = 0; i < TimedRefreshes; i++)
        {
            clock.Now += RefreshStep;
            timedNows.Add(clock.Now.UtcDateTime);
            var scansAtStart = memo.ScansStarted;
            var started = Stopwatch.GetTimestamp();
            last = await refresher.RefreshAsync(ct);
            var elapsed = Stopwatch.GetElapsedTime(started);
            scansPerRefresh.Add(memo.ScansStarted - scansAtStart);
            Assert.True(elapsed < DarlingMetricsSnapshotCache.RefreshTimeout,
                $"refresh {i + 1} took {elapsed.TotalMilliseconds:F0} ms, past the cache's {DarlingMetricsSnapshotCache.RefreshTimeout.TotalSeconds:F0} s refresh timeout");
            total.Add(elapsed.TotalMilliseconds);
            fleet.Add(refresher.LastFleetDuration!.Value.TotalMilliseconds);
            serverSet.Add(refresher.LastServerSetDuration!.Value.TotalMilliseconds);
        }

        Assert.True(scansPerRefresh.All(n => n == 1),
            $"every timed refresh should have run the collection-health read once (a memo miss), but the memo started [{string.Join(", ", scansPerRefresh)}]");
        /* The last refresh reads the store clock 10 min 10 s on from the seed's newest rows, still inside every
           freshness bound (15 min, 30 min for sessions, 3 h for volumes), and the plant must still be whole there. */
        AssertPlant(last!, servers, "the last timed refresh");

        var statements = new List<(string Name, int Rows, double P50, double P95)>();
        string fileIoPlan;
        long schedules;
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            foreach (var (name, sql) in ServerSetStatements())
            {
                statements.Add(await TimeStatementAsync(connection, name, sql, anchor, ct));
            }

            fileIoPlan = await ExplainAsync(connection, DarlingMetricsServerSetReader.FileIoSql, anchor, ct);
            schedules = await ScalarAsync<long>(connection, "SELECT count(*) FROM config.config_collector_schedules", ct);
        }

        var health = await TimeCollectionHealthAsync(postgres, timedNows, raw: false, ct);
        var rawHealth = await TimeCollectionHealthAsync(postgres, timedNows, raw: true, ct);
        statements.Add(("collection-health", health.Rows, health.P50, health.P95));
        var healthPlan = await ExplainCollectionHealthAsync(postgres, timedNows[^1], ct);

        var markdown = Markdown(servers, rowCounts, layout, rollupNote, seedSeconds, fleet, serverSet, total, statements,
            health.Path, rawHealth, scansPerRefresh, schedules, fileIoPlan, healthPlan);
        TestContext.Current.SendDiagnosticMessage(markdown);
        File.WriteAllText(Path.Combine(Path.GetTempPath(), $"darling-metrics-scale-{servers}.md"), markdown);
    }

    /// <summary>The plant check: every collecting server carries a fresh series from each freshness bound (the 1-minute
    /// disk and CPU readings, the 5-minute sessions, the hourly volumes) and a collector count from the collection-health
    /// read, so fast numbers cannot come from a window that went stale; the stopped server carries none of those series
    /// but is still in the snapshot, so its probes ran and found nothing.</summary>
    private static void AssertPlant(MetricsSnapshot snapshot, int servers, string which)
    {
        var stopped = "scale-" + (servers + 1).ToString(CultureInfo.InvariantCulture);
        foreach (var series in new[] { "darling_disk_read_latency_ms", "darling_cpu_sql_percent", "darling_sessions", "darling_disk_volume_min_free_percent" })
        {
            var reporting = snapshot.Samples.Where(s => s.Name == series)
                .Select(s => s.Labels[0].Value).Distinct(StringComparer.Ordinal).ToList();
            Assert.True(!reporting.Contains(stopped, StringComparer.Ordinal),
                $"after {which} the stopped server {stopped} should have no fresh {series}, but it has one; its probe would not measure the no-fresh-row case");
            Assert.True(reporting.Count == servers,
                $"after {which} the plant is meant to give every collecting server a fresh {series}, but {reporting.Count} of {servers} have one; the numbers would not measure a full server set");
        }

        Assert.True(snapshot.Samples.Any(s => s.Name == "darling_server_health_band" && s.Labels[0].Value == stopped),
            $"after {which} the stopped server {stopped} should still be in the snapshot through its health band");
        var counted = snapshot.Samples.Count(s => s.Name == "darling_server_collectors" && s.Value > 0 && s.Labels[0].Value != stopped);
        Assert.True(counted == servers,
            $"after {which} every collecting server should carry collectors from the collection-health read, but {counted} of {servers} do");
    }

    private static readonly string[] SeededTables =
    {
        "file_io_stats", "wait_stats", "perfmon_stats", "cpu_utilization_stats", "memory_stats", "memory_grant_stats",
        "tempdb_stats", "cpu_scheduler_stats", "session_summary_stats", "database_size_stats", "collection_log",
    };

    /// <summary>
    /// The collection-health rollup as the service leaves it: the continuous aggregates created by the service's own
    /// ensure, then the one refresh of the collection-health window the service runs on an empty rollup (the policy's
    /// window), then collection_log's chunks older than a day compressed as its compression policy leaves them.
    /// Returns a note for the table.
    /// </summary>
    private static async Task<string> BuildRollupAsync(NpgsqlConnection connection, bool timescale, DateTime anchor, CancellationToken ct)
    {
        if (!timescale)
        {
            return "no rollup: plain PostgreSQL, so the collection-health read is the raw 7-day scan";
        }

        var started = Stopwatch.GetTimestamp();
        var aggregates = await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.WarmCollectionHealthHourlyAsync(connection, null, anchor, ct),
            "the collection-health rollup should have been refreshed once over its window");
        var refreshSeconds = Stopwatch.GetElapsedTime(started).TotalSeconds;

        started = Stopwatch.GetTimestamp();
        await ExecAsync(connection, @"
SELECT compress_chunk(ch, if_not_compressed => true)
FROM show_chunks('collection_log', older_than => $1::timestamp) AS ch", ct, anchor.AddDays(-TimescaleSupport.CompressAfterDays));
        var compressSeconds = Stopwatch.GetElapsedTime(started).TotalSeconds;
        var chunks = await ScalarAsync<long>(connection,
            "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'collection_log' AND is_compressed", ct);
        var watermark = await ScalarAsync<object>(connection, CollectionHealthRollupSupport.WatermarkSql, ct);
        return string.Create(CultureInfo.InvariantCulture,
            $"{aggregates} continuous aggregates created by the service's ensure; collection_health_hourly refreshed once over its 8-day window as the service does on an empty rollup (with the ensure, {refreshSeconds:F1} s), watermark {watermark:yyyy-MM-dd HH:mm}; {chunks} collection_log chunks older than a day compressed ({compressSeconds:F1} s)");
    }

    /// <summary>
    /// The fleet read's collection-health statement alone, at each timed refresh's store clock: the rollup guard picks
    /// the composed statement (hourly rollup + raw head slice + any hole hours) or the raw 7-day scan, as the reader
    /// does, and the statement runs in the reader's own work-memory transaction. <paramref name="raw"/> forces the raw
    /// scan, the path a store without the rollup takes. One warm-up run at the first instant, then one timed run at each.
    /// </summary>
    private static async Task<(int Rows, double P50, double P95, string Path)> TimeCollectionHealthAsync(
        NpgsqlDataSource postgres, List<DateTime> nows, bool raw, CancellationToken ct)
    {
        var paths = new HashSet<string>(StringComparer.Ordinal);
        var rows = 0;
        var times = new List<double>();
        for (var i = -1; i < nows.Count; i++)
        {
            var now = nows[Math.Max(i, 0)];
            var (sql, parameters, path) = await CollectionHealthStatementAsync(postgres, now, raw, ct);
            await using var scope = await CollectionHealthRollupSupport.OpenWorkMemReadAsync(postgres, 600, ct);
            await using var command = scope.CreateCommand(sql);
            command.CommandTimeout = 600;
            command.Parameters.AddRange(parameters);
            var started = Stopwatch.GetTimestamp();
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                rows = 0;
                while (await reader.ReadAsync(ct))
                {
                    rows++;
                }
            }

            if (i >= 0)
            {
                times.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
                paths.Add(path);
            }
        }

        return (rows, Percentile(times, 0.50), Percentile(times, 0.95), string.Join("; ", paths));
    }

    private static async Task<string> ExplainCollectionHealthAsync(NpgsqlDataSource postgres, DateTime now, CancellationToken ct)
    {
        var (sql, parameters, path) = await CollectionHealthStatementAsync(postgres, now, raw: false, ct);
        await using var scope = await CollectionHealthRollupSupport.OpenWorkMemReadAsync(postgres, 600, ct);
        await using var command = scope.CreateCommand("EXPLAIN (ANALYZE, BUFFERS) " + sql);
        command.CommandTimeout = 600;
        command.Parameters.AddRange(parameters);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var plan = new StringBuilder().AppendLine("-- path: " + path);
        while (await reader.ReadAsync(ct))
        {
            plan.AppendLine(reader.GetString(0));
        }

        return plan.ToString();
    }

    /// <summary>The statement and parameters the reader would run at <paramref name="now"/>, chosen by the same guard.</summary>
    private static async Task<(string Sql, NpgsqlParameter[] Parameters, string Path)> CollectionHealthStatementAsync(
        NpgsqlDataSource postgres, DateTime now, bool raw, CancellationToken ct)
    {
        var windowStart = DateTime.SpecifyKind(now.AddDays(-7), DateTimeKind.Unspecified);
        var headEnd = CollectionHealthRollupSupport.CeilingHour(windowStart);
        var plan = raw ? CollectionHealthRollupSupport.RollupPlan.Unusable : await CollectionHealthRollupSupport.RollupPlanAsync(postgres, headEnd, ct);
        if (!plan.Usable)
        {
            return (DarlingFleetReader.FleetCollectionHealthSql, [Timestamp(windowStart)], raw ? "raw 7-day scan (forced)" : "raw 7-day scan (the rollup guard failed)");
        }

        var parameters = new List<NpgsqlParameter> { Timestamp(windowStart), Timestamp(headEnd) };
        if (plan.HoleHours.Count > 0)
        {
            parameters.Add(new NpgsqlParameter<DateTime[]> { TypedValue = plan.HoleHours.ToArray() });
        }

        return (CollectionHealthRollupSupport.ComposeFleetSql(DarlingFleetReader.FleetCollectionHealthSql, plan.HoleHours), parameters.ToArray(),
            $"composed: hourly rollup + raw head slice, {plan.HoleHours.Count} hole hours read raw");
    }

    private static NpgsqlParameter<DateTime> Timestamp(DateTime value) =>
        new() { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) };

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
        /* #9013: collection_log is outside the collector catalog; the service converts it on its own pass. */
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct),
            "collection_log should be a hypertable, as the service's own pass leaves it");
        return $"TimescaleDB hypertables ({converted} collector tables and collection_log converted)";
    }

    /// <summary>
    /// Every server's history, server-side: the registry, then the 1-minute tables in 30-minute slices, oldest slice
    /// first and in time order inside each, so the heap is laid out as collection writes it (time major, not server
    /// major); then the 5-minute sessions and the hourly sizes; then 7 days of collection_log in daily slices, oldest
    /// first.
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

        for (var oldest = HealthHistoryMinutes - 1; oldest >= 0; oldest -= HealthSliceMinutes)
        {
            await ExecAsync(connection, CollectionLogSeed, ct, anchor, servers, Math.Max(oldest - HealthSliceMinutes + 1, 0), oldest);
        }
    }

    /// <summary>
    /// #9013: the freshness cutoff looks up each collector's interval per server (the server's row, then the fleet-wide
    /// row), so the store carries both: a fleet-wide row for each of the ten collectors and a row per collector on every
    /// second server, each at the collector's shipped interval, so every lookup finds rows and the bounds stay the
    /// seeded cadence's.
    /// </summary>
    private static async Task SeedSchedulesAsync(NpgsqlConnection connection, int servers, CancellationToken ct)
    {
        var names = new[]
        {
            "cpu_utilization", "cpu_scheduler_stats", "perfmon_stats", "session_summary_stats", "memory_stats",
            "memory_grant_stats", "file_io_stats", "database_size_stats", "wait_stats", "tempdb_stats",
        };
        var minutes = names.Select(n => CollectorScheduleDefaults.ResolveFrequencyMinutes(n, null, null)).ToArray();
        await ExecAsync(connection, @"
INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, enabled)
SELECT NULL, c.name, c.minutes, TRUE
FROM unnest($1::text[], $2::int[]) AS c (name, minutes)
UNION ALL
SELECT sv.id, c.name, c.minutes, TRUE
FROM unnest($1::text[], $2::int[]) AS c (name, minutes)
CROSS JOIN generate_series(2, $3::int, 2) AS sv (id)", ct, names, minutes, servers);
    }

    /* The 1-minute seeds share their parameters: $1 the pinned now, $2 the server count, $3 and $4 the newest and oldest
       minute of the slice (k minutes back from the newest collection, which is 30 s before now). The last server, $2, has
       no rows in the newest StoppedMinutes. */
    private static readonly string SliceTimes = $@"
FROM generate_series($4::int, $3::int, -1) AS k
JOIN generate_series(1, $2::int) AS sv (id) ON NOT (sv.id = $2::int AND k < {StoppedMinutes})";

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
    };

    /// <summary>Every 5 minutes for the 6 hours. $1 the pinned now, $2 the server count; the last server stopped.</summary>
    private static readonly string SessionsSeed = $@"
INSERT INTO session_summary_stats (collection_id, collection_time, server_id, server_name, total_sessions, running_sessions, sleeping_sessions,
    background_sessions, dormant_sessions, idle_sessions_over_30min, sessions_waiting_for_memory, databases_with_connections,
    top_application_name, top_application_connections, top_host_name, top_host_connections)
SELECT sv.id::bigint * 1000 + k, $1::timestamp - interval '30 seconds' - k * interval '5 minutes', sv.id, 'scale-' || sv.id,
       400, 10 + k % 7, 300, 60, 30 - k % 7, 40, 0, 25, 'scale app', 120, 'scale host', 90
FROM generate_series(71, 0, -1) AS k
JOIN generate_series(1, $2::int) AS sv (id) ON NOT (sv.id = $2::int AND k < {StoppedMinutes / 5})
ORDER BY k DESC, sv.id";

    /// <summary>Hourly, 40 files on two volumes. $1 the pinned now, $2 the server count; the last server stopped.</summary>
    private static readonly string DatabaseSizeSeed = $@"
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
JOIN generate_series(1, $2::int) AS sv (id) ON NOT (sv.id = $2::int AND k < {StoppedMinutes / 60})
CROSS JOIN generate_series(0, 39) AS f
ORDER BY k DESC, sv.id, f";

    /// <summary>
    /// #9013: a collection_log row per collector per collection at each collector's real cadence (the 1-minute
    /// collectors every minute, sessions every 5 minutes, database sizes hourly) over <see cref="HealthHistoryMinutes"/>,
    /// the span the fleet read's 7-day collection-health read covers. Narrow rows, so 7 days fit where 7 days of the wide
    /// tables would not. $1 the pinned now, $2 the server count, $3 and $4 the newest and oldest minute of the slice.
    /// </summary>
    private static readonly string CollectionLogSeed = $@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT (sv.id::bigint * 20000 + k) * 16 + c.n, sv.id, 'scale-' || sv.id, c.name, {CollectionTime}, 40 + (k + c.n) % 200, 'SUCCESS', c.row_count
{SliceTimes}
JOIN (
    VALUES (1, 'file_io_stats', {FilesPerCollection}, 1), (2, 'wait_stats', {WaitTypes}, 1), (3, 'perfmon_stats', 92, 1),
           (4, 'cpu_utilization', 1, 1), (5, 'memory_stats', 1, 1), (6, 'memory_grant_stats', 4, 1), (7, 'tempdb_stats', 1, 1),
           (8, 'cpu_scheduler_stats', 1, 1), (9, 'session_summary_stats', 1, 5), (10, 'database_size_stats', 40, 60)
) AS c (n, name, row_count, every_minutes) ON k % c.every_minutes = 0
ORDER BY k DESC, sv.id, c.n";

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
    private static async Task<(string Name, int Rows, double P50, double P95)> TimeStatementAsync(
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

        return (name, rows, Percentile(times, 0.50), Percentile(times, 0.95));
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
        int servers, List<(string Table, long Rows)> rowCounts, string layout, string rollupNote, double seedSeconds,
        List<double> fleet, List<double> serverSet, List<double> total, List<(string Name, int Rows, double P50, double P95)> statements,
        string healthPath, (int Rows, double P50, double P95, string Path) rawHealth, List<int> scansPerRefresh,
        long schedules, string fileIoPlan, string healthPlan)
    {
        static string Ms(double value) => value.ToString("F1", CultureInfo.InvariantCulture);

        var fileIoRows = rowCounts.First(r => r.Table == "file_io_stats").Rows;
        var md = new StringBuilder();
        md.AppendLine(CultureInfo.InvariantCulture, $"### /metrics refresh: {servers} collecting servers and 1 stopped, 6 h of 1-minute history, 7 days of collection_log");
        md.AppendLine();
        md.AppendLine("| servers | file_io_stats rows | fleet p50 ms | fleet p95 ms | server set p50 ms | server set p95 ms | whole refresh p50 ms | whole refresh p95 ms |");
        md.AppendLine("|---:|---:|---:|---:|---:|---:|---:|---:|");
        md.AppendLine(CultureInfo.InvariantCulture,
            $"| {servers} | {fileIoRows:N0} | {Ms(Percentile(fleet, 0.50))} | {Ms(Percentile(fleet, 0.95))} | {Ms(Percentile(serverSet, 0.50))} | {Ms(Percentile(serverSet, 0.95))} | {Ms(Percentile(total, 0.50))} | {Ms(Percentile(total, 0.95))} |");
        md.AppendLine();
        md.AppendLine("| statement | rows returned | p50 ms | p95 ms |");
        md.AppendLine("|---|---:|---:|---:|");
        foreach (var (name, rows, p50, p95) in statements)
        {
            md.AppendLine(CultureInfo.InvariantCulture, $"| {name} | {rows} | {Ms(p50)} | {Ms(p95)} |");
        }

        md.AppendLine();
        md.AppendLine("| table | rows seeded |");
        md.AppendLine("|---|---:|");
        foreach (var (table, rows) in rowCounts)
        {
            md.AppendLine(CultureInfo.InvariantCulture, $"| {table} | {rows:N0} |");
        }

        md.AppendLine();
        md.AppendLine("Notes: " + TimedRefreshes + " timed refreshes after one warm-up, each " + RefreshStep.TotalSeconds + " s after the last on the store clock, "
            + "so each misses the fleet read's 60 s collection-health memo as the metrics host's refreshes (at least 55 s apart, on a data source of its own) do; "
            + "the memo started [" + string.Join(", ", scansPerRefresh) + "] collection-health reads in the timed refreshes, so the fleet column includes that read every time. "
            + "Percentiles are nearest-rank, so the p95 of ten is the slowest. Each server-set statement ran once to warm, then " + StatementRuns + " times on one connection. "
            + "The collection-health row is the fleet read's 7-day statement alone, chosen by the reader's own rollup guard and run in its work-memory transaction "
            + "at each timed refresh's store clock (one warm-up run first); path: " + healthPath + ". "
            + string.Create(CultureInfo.InvariantCulture, $"For reference, the raw 7-day scan a store without the rollup runs: p50 {Ms(rawHealth.P50)} ms, p95 {Ms(rawHealth.P95)} ms ({rawHealth.Rows} rows). ")
            + string.Create(CultureInfo.InvariantCulture, $"One more server, scale-{servers + 1}, stopped collecting {StoppedMinutes} minutes before the newest collection, past every freshness bound, so every statement probes it and finds no fresh row. ")
            + string.Create(CultureInfo.InvariantCulture, $"config_collector_schedules: {schedules} rows, a fleet-wide row per collector and a row per collector on every second server, each at the shipped interval. ")
            + "Store: " + layout + "; " + rollupNote + ". "
            + string.Create(CultureInfo.InvariantCulture, $"Seed: {seedSeconds:F0} s. ")
            + "Per server and collection: 272 files, 150 wait types, the 12 perfmon counters (20 database instances and _Total on the Databases and Locks objects), "
            + "4 memory grant rows and one row each of CPU, scheduler, memory and tempdb; sessions every 5 minutes, database sizes hourly with 40 files, "
            + "all over 6 hours and uncompressed (the store compresses chunks older than a day). collection_log: a row per collector per collection at each "
            + "collector's cadence over 7 days and 1 hour, chunks older than a day compressed.");
        md.AppendLine();
        md.AppendLine("EXPLAIN (ANALYZE, BUFFERS) of FileIoSql:");
        md.AppendLine();
        md.AppendLine("```");
        md.Append(fileIoPlan);
        md.AppendLine("```");
        md.AppendLine();
        md.AppendLine("EXPLAIN (ANALYZE, BUFFERS) of the collection-health statement at the last timed refresh's store clock:");
        md.AppendLine();
        md.AppendLine("```");
        md.Append(healthPlan);
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

    /// <summary>A store clock that moves only when the test moves it; timestamps (and so every measured duration) stay
    /// real.</summary>
    private sealed class PinnedNow(DateTimeOffset now) : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = now;

        public override DateTimeOffset GetUtcNow() => Now;
    }
}
