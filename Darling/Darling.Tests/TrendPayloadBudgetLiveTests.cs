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
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3897: what the five bucketed trends put on the wire, measured on a week of one-minute collections — the
/// payload-size census <see cref="TrendBuckets"/>' caps are pinned against.
///
/// <para>The defect was size by construction: each read returned every collection, so an answer was the window
/// times the collection cadence times the series. On DARLING01 a default (24-hour) <c>get_file_io_trend</c> was
/// 12,500 points and 1.4 MB, a week 9.8 MB; <c>get_lock_wait_trend</c> 349 KB and 2 MB; the PostgreSQL pair 448 KB
/// and 294 KB at a day. On this fixture the old reads would have answered the same way — a day of twelve files is
/// 14,400 rows — and every assertion below would fail on them.</para>
///
/// <para>Two properties, over every window a caller is likely to pass: an answer left to size itself stays near the
/// 200-point budget and under <see cref="DefaultCeilingBytes"/>, and the LARGEST answer a caller can ask for — the
/// narrowest width each read's cap admits over its longest window — stays under <see cref="CapCeilingBytes"/>, the
/// "near 256 KB" the caps were sized to. The fixture's names and magnitudes are production-shaped (real database
/// and file names, four- and five-digit counters), so bytes per point are what a real server's are.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class TrendPayloadBudgetLiveTests
{
    private const string ServerName = "trend-payload-budget";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /// <summary>What any default answer may weigh: the heaviest measured on DARLING01 after the change was 63 KB.</summary>
    private const int DefaultCeilingBytes = 80 * 1024;

    /// <summary>#4193: get_pg_cpu_utilization's own default ceiling. Its row is the widest in the family by
    /// design (<see cref="TrendBuckets.PgCpuMaxPoints"/>'s own doc comment: "about 500 bytes/point" for the
    /// CPU/ACU pair with peaks, the capacity trio and six V136 host-memory columns), so the SAME 200-point
    /// auto-sizing target (<see cref="TrendBuckets.McpPointBudget"/>) every trend tool shares costs it
    /// roughly 100 KB at the widest auto-sized window, not the ~63 KB the narrower rows in
    /// <see cref="DefaultCeilingBytes"/> top out at. Measured 84,556 bytes / 169 points at 168h on this
    /// fixture; sized with headroom above that rather than raising the shared ceiling and weakening what it
    /// guards for the other ten tools.</summary>
    private const int PgCpuDefaultCeilingBytes = 100 * 1024;

    /// <summary>What the largest answer a caller can ask for may weigh.</summary>
    private const int CapCeilingBytes = 320 * 1024;

    private const int WeekMinutes = 7 * 24 * 60;
    private const int RawMinutes = 72 * 60;

    /// <summary>#3960: the one PostgreSQL statement seeded for <c>get_pg_query_duration_trend</c>'s census, as
    /// both the SQL literal (<see cref="SeedAsync"/>) and the tool's string parameter.</summary>
    private const long PgQueryId = 987654321L;
    private const string PgQueryIdText = "987654321";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task EveryDefaultAnswer_StaysNearTheBudget_AndTheLargestAnswerStaysUnderTheCap()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live trend payload census.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var now = DateTime.UtcNow;
            var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0).AddMinutes(-1);
            await SeedAsync(connection, end, ct);

            var measured = new List<string>();

            /* ── left to size themselves ── */
            foreach (var hours in new[] { 1, 4, 24, 72, 168 })
            {
                Measure(measured, "get_file_io_trend", hours, await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, hours), "trend", DefaultCeilingBytes, TrendBuckets.McpPointBudget);
                Measure(measured, "get_lock_wait_trend", hours, await DarlingMcpBlockingTools.GetLockWaitTrend(postgres, ServerName, hours), "trend", DefaultCeilingBytes, TrendBuckets.McpPointBudget);
                Measure(measured, "get_pg_io_trend", hours, await DarlingMcpPgTrendTools.GetPgIoTrend(postgres, ServerName, hours_back: hours), "points", DefaultCeilingBytes, TrendBuckets.McpPointBudget);
                Measure(measured, "get_pg_database_trend", hours, await DarlingMcpPgTrendTools.GetPgDatabaseTrend(postgres, ServerName, hours_back: hours), "points", DefaultCeilingBytes, TrendBuckets.McpPointBudget);
                /* #4193: get_pg_cpu_utilization joined the TrendBuckets family; roster it the same as its
                   SQL Server twin get_cpu_utilization below. */
                Measure(measured, "get_pg_cpu_utilization", hours, await DarlingMcpPgCpuUtilizationTools.GetPgCpuUtilization(postgres, ServerName, hours_back: hours), "samples", PgCpuDefaultCeilingBytes, TrendBuckets.McpPointBudget);
                if (hours <= 72)
                {
                    Measure(measured, "get_query_duration_trend", hours, await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName, hours_back: hours), "trend", DefaultCeilingBytes, TrendBuckets.McpPointBudget);
                }

                /* #3960: the rest of the trend family, on the same fixture. get_wait_trend rides the LCK_M_S
                   series wait_stats already carries for get_lock_wait_trend above — one more read over rows that
                   exist regardless, not a reason to seed a second wait_type. */
                Measure(measured, "get_wait_trend", hours, await DarlingMcpDataTools.GetWaitTrend(postgres, "LCK_M_S", ServerName, hours), "trend", DefaultCeilingBytes, TrendBuckets.McpPointBudget);
                Measure(measured, "get_cpu_utilization", hours, await DarlingMcpDataTools.GetCpuUtilization(postgres, ServerName, hours), "samples", DefaultCeilingBytes, TrendBuckets.McpPointBudget);
                Measure(measured, "get_tempdb_trend", hours, await DarlingMcpDataTools.GetTempDbTrend(postgres, ServerName, hours), "trend", DefaultCeilingBytes, TrendBuckets.McpPointBudget);
                Measure(measured, "get_memory_trend", hours, await DarlingMcpTrendTools.GetMemoryTrend(postgres, ServerName, hours), "trend", DefaultCeilingBytes, TrendBuckets.McpPointBudget);
                Measure(measured, "get_perfmon_trend", hours, await DarlingMcpTrendTools.GetPerfmonTrend(postgres, "Batch Requests/sec", ServerName, hours), "trend", DefaultCeilingBytes, TrendBuckets.McpPointBudget);
                Measure(measured, "get_pg_query_duration_trend", hours, await DarlingMcpPgTrendTools.GetPgQueryDurationTrend(postgres, ServerName, PgQueryIdText, hours), "points", DefaultCeilingBytes, TrendBuckets.McpPointBudget);
            }

            /* The large-server shape the issue extrapolated to 30 MB: the last four hours also hold 300 tenant
               databases' 600 files. 609 series are active and ranked, and the answer is still five lines and
               the fold — the payload does not grow with the number of files. */
            var crowded = JsonDocument.Parse(await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 4)).RootElement;
            Assert.Equal(609, crowded.GetProperty("series_active").GetInt32());
            Assert.Equal(604, crowded.GetProperty("series_folded").GetInt32());
            Assert.Equal(6, crowded.GetProperty("series").GetArrayLength());

            /* A day at the budget is a real series, not a degenerate one: 145 ten-minute points on one line. */
            var lockDay = JsonDocument.Parse(await DarlingMcpBlockingTools.GetLockWaitTrend(postgres, ServerName, 24)).RootElement;
            Assert.InRange(lockDay.GetProperty("trend").GetArrayLength(), 140, 145);
            Assert.Equal(6, lockDay.GetProperty("wait_types").GetArrayLength());
            Assert.Equal(6, lockDay.GetProperty("wait_types_idle").GetInt32());

            /* ── the largest answer each read can give: its cap's narrowest width over its longest window ── */
            var fileIoWidth = TrendBuckets.NarrowestFitting(WeekMinutes, 6, TrendBuckets.FileIoMaxPoints);
            Measure(measured, "get_file_io_trend@" + fileIoWidth, 168,
                await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 168, bucket_minutes: fileIoWidth), "trend", CapCeilingBytes, TrendBuckets.FileIoMaxPoints);

            var lockWidth = TrendBuckets.NarrowestFitting(WeekMinutes, 1, TrendBuckets.LockWaitMaxPoints);
            Measure(measured, "get_lock_wait_trend@" + lockWidth, 168,
                await DarlingMcpBlockingTools.GetLockWaitTrend(postgres, ServerName, 168, bucket_minutes: lockWidth), "trend", CapCeilingBytes, TrendBuckets.LockWaitMaxPoints);

            var durationWidth = TrendBuckets.NarrowestFitting(RawMinutes, 1, TrendBuckets.DurationMaxPoints);
            Measure(measured, "get_query_duration_trend@" + durationWidth, 72,
                await DarlingMcpTrendTools.GetQueryDurationTrend(postgres, ServerName, hours_back: 72, bucket_minutes: durationWidth), "trend", CapCeilingBytes, TrendBuckets.DurationMaxPoints);

            var pgIoWidth = TrendBuckets.NarrowestFitting(WeekMinutes, 1, TrendBuckets.PgIoMaxPoints);
            Measure(measured, "get_pg_io_trend@" + pgIoWidth, 168,
                await DarlingMcpPgTrendTools.GetPgIoTrend(postgres, ServerName, hours_back: 168, bucket_minutes: pgIoWidth), "points", CapCeilingBytes, TrendBuckets.PgIoMaxPoints);

            var pgDatabaseWidth = TrendBuckets.NarrowestFitting(WeekMinutes, 1, TrendBuckets.PgDatabaseMaxPoints);
            Measure(measured, "get_pg_database_trend@" + pgDatabaseWidth, 168,
                await DarlingMcpPgTrendTools.GetPgDatabaseTrend(postgres, ServerName, hours_back: 168, bucket_minutes: pgDatabaseWidth), "points", CapCeilingBytes, TrendBuckets.PgDatabaseMaxPoints);

            var pgCpuWidth = TrendBuckets.NarrowestFitting(WeekMinutes, 1, TrendBuckets.PgCpuMaxPoints);
            Measure(measured, "get_pg_cpu_utilization@" + pgCpuWidth, 168,
                await DarlingMcpPgCpuUtilizationTools.GetPgCpuUtilization(postgres, ServerName, hours_back: 168, bucket_minutes: pgCpuWidth), "samples", CapCeilingBytes, TrendBuckets.PgCpuMaxPoints);

            /* #3960: the rest of the family's largest answers, same recipe. */
            var waitWidth = TrendBuckets.NarrowestFitting(WeekMinutes, 1, TrendBuckets.WaitMaxPoints);
            Measure(measured, "get_wait_trend@" + waitWidth, 168,
                await DarlingMcpDataTools.GetWaitTrend(postgres, "LCK_M_S", ServerName, 168, bucket_minutes: waitWidth), "trend", CapCeilingBytes, TrendBuckets.WaitMaxPoints);

            var cpuWidth = TrendBuckets.NarrowestFitting(WeekMinutes, 1, TrendBuckets.CpuMaxPoints);
            Measure(measured, "get_cpu_utilization@" + cpuWidth, 168,
                await DarlingMcpDataTools.GetCpuUtilization(postgres, ServerName, 168, bucket_minutes: cpuWidth), "samples", CapCeilingBytes, TrendBuckets.CpuMaxPoints);

            var tempDbWidth = TrendBuckets.NarrowestFitting(WeekMinutes, 1, TrendBuckets.TempDbMaxPoints);
            Measure(measured, "get_tempdb_trend@" + tempDbWidth, 168,
                await DarlingMcpDataTools.GetTempDbTrend(postgres, ServerName, 168, bucket_minutes: tempDbWidth), "trend", CapCeilingBytes, TrendBuckets.TempDbMaxPoints);

            var memoryWidth = TrendBuckets.NarrowestFitting(WeekMinutes, 1, TrendBuckets.MemoryMaxPoints);
            Measure(measured, "get_memory_trend@" + memoryWidth, 168,
                await DarlingMcpTrendTools.GetMemoryTrend(postgres, ServerName, 168, bucket_minutes: memoryWidth), "trend", CapCeilingBytes, TrendBuckets.MemoryMaxPoints);

            var perfmonWidth = TrendBuckets.NarrowestFitting(WeekMinutes, 1, TrendBuckets.PerfmonMaxPoints);
            Measure(measured, "get_perfmon_trend@" + perfmonWidth, 168,
                await DarlingMcpTrendTools.GetPerfmonTrend(postgres, "Batch Requests/sec", ServerName, 168, bucket_minutes: perfmonWidth), "trend", CapCeilingBytes, TrendBuckets.PerfmonMaxPoints);

            var pgQueryDurationWidth = TrendBuckets.NarrowestFitting(WeekMinutes, 1, TrendBuckets.PgQueryDurationMaxPoints);
            Measure(measured, "get_pg_query_duration_trend@" + pgQueryDurationWidth, 168,
                await DarlingMcpPgTrendTools.GetPgQueryDurationTrend(postgres, ServerName, PgQueryIdText, 168, bucket_minutes: pgQueryDurationWidth), "points", CapCeilingBytes, TrendBuckets.PgQueryDurationMaxPoints);

            /* And one minute narrower than that is refused, not served. */
            Assert.True(McpHelpers.IsRefusalEnvelope(await DarlingMcpTrendTools.GetFileIoTrend(postgres, ServerName, 168, bucket_minutes: fileIoWidth - 1)));
            Assert.True(McpHelpers.IsRefusalEnvelope(await DarlingMcpPgTrendTools.GetPgIoTrend(postgres, ServerName, hours_back: 168, bucket_minutes: pgIoWidth - 1)));
            Assert.True(McpHelpers.IsRefusalEnvelope(await DarlingMcpDataTools.GetWaitTrend(postgres, "LCK_M_S", ServerName, 168, bucket_minutes: waitWidth - 1)));
            Assert.True(McpHelpers.IsRefusalEnvelope(await DarlingMcpPgTrendTools.GetPgQueryDurationTrend(postgres, ServerName, PgQueryIdText, 168, bucket_minutes: pgQueryDurationWidth - 1)));
            Assert.True(McpHelpers.IsRefusalEnvelope(await DarlingMcpPgCpuUtilizationTools.GetPgCpuUtilization(postgres, ServerName, 168, bucket_minutes: pgCpuWidth - 1)));

            TestContext.Current.TestOutputHelper?.WriteLine(string.Join(Environment.NewLine, measured));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /// <summary>One answer: a data envelope (not a miss or a refusal), at most <paramref name="maxPoints"/> points,
    /// under <paramref name="ceilingBytes"/> bytes of UTF-8.</summary>
    private static void Measure(List<string> measured, string tool, int hours, string answer, string pointsKey, int ceilingBytes, int maxPoints)
    {
        var root = JsonDocument.Parse(answer).RootElement;
        Assert.False(
            root.TryGetProperty("status", out var status) && status.GetString() is not ("io_trend" or "database_trend" or "query_duration_trend"),
            $"{tool} over {hours}h answered a {(root.TryGetProperty("status", out var s) ? s.GetString() : "?")} instead of data: {answer[..Math.Min(answer.Length, 400)]}");

        var points = root.GetProperty(pointsKey).GetArrayLength();
        var bytes = Encoding.UTF8.GetByteCount(answer);
        measured.Add($"{tool} {hours}h: {points} points, {bytes / 1024.0:F1} KB");

        Assert.True(points > 0, $"{tool} over {hours}h returned no points");
        Assert.True(points <= maxPoints, $"{tool} over {hours}h returned {points} points, over {maxPoints}");
        Assert.True(bytes <= ceilingBytes, $"{tool} over {hours}h is {bytes:N0} bytes, over the {ceilingBytes:N0}-byte ceiling ({points} points)");
    }

    /// <summary>
    /// A week of one-minute collections (the raw query-stats tier holds four days, so its three days): twelve
    /// files in five databases (nine (database, file type) series, so file I/O folds to six lines), twelve LCK
    /// types of which six ever wait, three cached queries, one pg_stat_io pair over two object types, and one
    /// database's pg_stat_database — counters cumulative where the collector stores them cumulative. And, over
    /// the last four hours only, 300 tenant databases with a data and a log file each: the 600-file server.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime end, CancellationToken ct)
    {
        var weekStart = end.AddMinutes(-WeekMinutes);
        var rawStart = end.AddMinutes(-RawMinutes);

        await PlantAsync(connection, ct, @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type,
     physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes,
     delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
SELECT $1 + n * 16 + f.i, $2 + n * interval '1 minute', $3, $4, f.db, f.file, f.kind,
       '/var/opt/mssql/data/' || f.file, 10240, 1000 + (n % 50) * 10 + f.i * 10, 200 + (n % 7) * 10,
       (1000 + (n % 50) * 10 + f.i * 10) * 8192, (200 + (n % 7) * 10) * 8192,
       (1000 + (n % 50) * 10 + f.i * 10) * (1 + f.i % 5), (200 + (n % 7) * 10) * 3, 60
FROM generate_series(0, $5) AS n
CROSS JOIN (VALUES
    (0, 'StackOverflow2013', 'StackOverflow2013.mdf', 'ROWS'),
    (1, 'StackOverflow2013', 'StackOverflow2013_log.ldf', 'LOG'),
    (2, 'AdventureWorks2022', 'AdventureWorks2022.mdf', 'ROWS'),
    (3, 'AdventureWorks2022', 'AdventureWorks2022_log.ldf', 'LOG'),
    (4, 'SalesReporting', 'SalesReporting.mdf', 'ROWS'),
    (5, 'SalesReporting', 'SalesReporting_2.ndf', 'ROWS'),
    (6, 'SalesReporting', 'SalesReporting_log.ldf', 'LOG'),
    (7, 'tempdb', 'tempdev.mdf', 'ROWS'),
    (8, 'tempdb', 'temp2.ndf', 'ROWS'),
    (9, 'tempdb', 'temp3.ndf', 'ROWS'),
    (10, 'tempdb', 'templog.ldf', 'LOG'),
    (11, 'msdb', 'MSDBData.mdf', 'ROWS')
) AS f(i, db, file, kind)", 1_000_000L, weekStart, WeekMinutes);

        await PlantAsync(connection, ct, @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type,
     physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes,
     delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
SELECT $1 + n * 1000 + d.i * 2 + k.j, $2 + n * interval '1 minute', $3, $4,
       'TenantDb' || lpad(d.i::text, 3, '0'), 'TenantDb' || lpad(d.i::text, 3, '0') || k.suffix, k.kind,
       '/var/opt/mssql/data/TenantDb' || lpad(d.i::text, 3, '0') || k.suffix, 2048,
       20 + (n + d.i) % 30, 5 + n % 5, (20 + (n + d.i) % 30) * 8192, (5 + n % 5) * 8192,
       (20 + (n + d.i) % 30) * (1 + d.i % 7), (5 + n % 5) * 2, 60
FROM generate_series(0, $5) AS n
CROSS JOIN generate_series(0, 299) AS d(i)
CROSS JOIN (VALUES (0, '.mdf', 'ROWS'), (1, '_log.ldf', 'LOG')) AS k(j, suffix, kind)", 6_000_000L, end.AddHours(-4), 240);

        await PlantAsync(connection, ct, @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     delta_wait_time_ms, delta_signal_wait_time_ms, delta_waiting_tasks, sample_interval_seconds)
SELECT $1 + n * 16 + w.i, $2 + n * interval '1 minute', $3, $4, w.wait_type,
       CASE WHEN w.i < 6 THEN (n % 13) * (w.i + 1) * 70 ELSE 0 END, 0, n % 3, 60
FROM generate_series(0, $5) AS n
CROSS JOIN (VALUES (0, 'LCK_M_S'), (1, 'LCK_M_U'), (2, 'LCK_M_X'), (3, 'LCK_M_IS'), (4, 'LCK_M_IU'), (5, 'LCK_M_IX'),
                   (6, 'LCK_M_SIU'), (7, 'LCK_M_SIX'), (8, 'LCK_M_UIX'), (9, 'LCK_M_BU'), (10, 'LCK_M_RS_S'), (11, 'LCK_M_RIn_NL')
) AS w(i, wait_type)", 2_000_000L, weekStart, WeekMinutes);

        await PlantAsync(connection, ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     sql_handle, plan_handle, query_text, execution_count, total_worker_time, total_elapsed_time,
     delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
SELECT $1 + n * 4 + q.i, $2 + n * interval '1 minute', $3, $4, 'StackOverflow2013', q.hash, '0x7F3A9C2B11D04E55', '0xSQLH', '0xPLANH',
       'SELECT TOP (100) p.Id FROM dbo.Posts AS p WHERE p.OwnerUserId = @UserId', 0, 0, 0,
       10 + (n % 17) * (q.i + 1), 0, (10 + (n % 17) * (q.i + 1)) * 2517, 60
FROM generate_series(0, $5) AS n
CROSS JOIN (VALUES (0, '0x1C8F0E7D55A2B391'), (1, '0x2D90F18E66B3C4A2'), (2, '0x3EA1029F77C4D5B3')) AS q(i, hash)", 3_000_000L, rawStart, RawMinutes);

        await PlantAsync(connection, ct, @"
WITH s AS (
    SELECT n, o.i, o.object_type, 1100 + (n % 13) * 57 AS r
    FROM generate_series(0, $5) AS n
    CROSS JOIN (VALUES (0, 'relation'), (1, 'temp relation')) AS o(i, object_type)
)
INSERT INTO pg_io_stats
    (collection_id, collection_time, server_id, server_name, backend_type, object_type, context,
     reads, read_time_ms, writes, write_time_ms, extends, op_bytes, hits, evictions, stats_reset)
SELECT $1 + n * 4 + i, $2 + n * interval '1 minute', $3, $4, 'client backend', object_type, 'normal',
       SUM(r) OVER w, SUM(r * 1.37) OVER w, SUM(r / 5) OVER w, SUM(r / 5 * 0.81) OVER w, SUM(r / 30) OVER w,
       8192, SUM(r * 83) OVER w, SUM(r / 50) OVER w, NULL::timestamp
FROM s
WINDOW w AS (PARTITION BY i ORDER BY n)", 4_000_000L, weekStart, WeekMinutes);

        await PlantAsync(connection, ct, @"
WITH s AS (
    SELECT n,
           6000 + (n % 11) * 217 AS commits,
           (n % 5) * 3 AS rollbacks,
           1000 + (n % 7) * 331 AS block_reads,
           90000 + (n % 13) * 1009 AS block_hits,
           CASE WHEN n % 97 = 0 THEN 3 ELSE 0 END AS spills,
           CASE WHEN n % 97 = 0 THEN 52428800 ELSE 0 END AS spill_bytes,
           CASE WHEN n % 1440 = 0 THEN 1 ELSE 0 END AS deadlock
    FROM generate_series(0, $5) AS n
)
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT $1 + n, $2 + n * interval '1 minute', $3, $4, 'StackOverflow2013',
       SUM(commits) OVER w, SUM(rollbacks) OVER w, SUM(block_reads) OVER w, SUM(block_hits) OVER w,
       SUM(spills) OVER w, SUM(spill_bytes) OVER w, SUM(deadlock) OVER w, NULL::timestamp
FROM s
WINDOW w AS (ORDER BY n)", 5_000_000L, weekStart, WeekMinutes);

        /* #3960: the rest of the trend family, on the same week. One series each is enough for a payload size
           and cap census -- get_wait_trend's own series count comes from wait_stats' LCK_M_S rows above. */
        await PlantAsync(connection, ct, @"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time,
     sqlserver_cpu_utilization, other_process_cpu_utilization)
SELECT $1 + n, $2 + n * interval '1 minute', $3, $4, $2 + n * interval '1 minute',
       40 + (n % 30), 10 + (n % 5)
FROM generate_series(0, $5) AS n", 7_000_000L, weekStart, WeekMinutes);

        await PlantAsync(connection, ct, @"
INSERT INTO tempdb_stats
    (collection_id, collection_time, server_id, server_name,
     user_object_reserved_mb, internal_object_reserved_mb, version_store_reserved_mb,
     total_reserved_mb, unallocated_mb, total_sessions_using_tempdb, top_session_id, top_session_tempdb_mb)
SELECT $1 + n, $2 + n * interval '1 minute', $3, $4,
       100 + (n % 50), 50 + (n % 20), 25 + (n % 10),
       175 + (n % 80), 825 - (n % 80), 5 + (n % 10), 55 + (n % 3), 12 + (n % 5)
FROM generate_series(0, $5) AS n", 8_000_000L, weekStart, WeekMinutes);

        await PlantAsync(connection, ct, @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, plan_cache_mb)
SELECT $1 + n, $2 + n * interval '1 minute', $3, $4,
       40000 + (n % 500), 49152, 35000 + (n % 400), 5000 + (n % 100)
FROM generate_series(0, $5) AS n", 9_000_000L, weekStart, WeekMinutes);

        await PlantAsync(connection, ct, @"
INSERT INTO memory_grant_stats
    (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id, granted_memory_mb)
SELECT $1 + n, $2 + n * interval '1 minute', $3, $4, 0, 2, 50 + (n % 30)
FROM generate_series(0, $5) AS n", 10_000_000L, weekStart, WeekMinutes);

        await PlantAsync(connection, ct, @"
INSERT INTO perfmon_stats
    (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name,
     cntr_value, delta_cntr_value, sample_interval_seconds)
SELECT $1 + n, $2 + n * interval '1 minute', $3, $4, 'SQLServer:SQL Statistics', 'Batch Requests/sec', '',
       5000000 + n * 900, 900 + (n % 50) * 3, 60
FROM generate_series(0, $5) AS n", 11_000_000L, weekStart, WeekMinutes);

        await PlantAsync(connection, ct, $@"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid,
     delta_calls, delta_total_exec_time_ms, sample_interval_seconds)
SELECT $1 + n, $2 + n * interval '1 minute', $3, $4, {PgQueryId},
       10 + (n % 20), (10 + (n % 20)) * 2.5, 60
FROM generate_series(0, $5) AS n", 12_000_000L, weekStart, WeekMinutes);

        /* #4193: get_pg_cpu_utilization's own table, one row/minute like the rest of the family, with the
           V136 host-memory columns populated (not left NULL) since those are what doubled the pre-bucketing
           row width the issue measured. */
        await PlantAsync(connection, ct, @"
INSERT INTO pg_cpu_utilization
    (collection_id, collection_time, server_id, server_name, sample_time,
     cpu_percent, acu_utilization_percent, serverless_capacity_acu, max_configured_acu,
     memory_total_bytes, memory_free_bytes, memory_cached_bytes, memory_buffers_bytes, memory_active_bytes, configured_memory_bytes)
SELECT $1 + n, $2 + n * interval '1 minute', $3, $4, $2 + n * interval '1 minute',
       30 + (n % 50), 25 + (n % 40), 4 + (n % 8), 16,
       17179869184, 2147483648 + (n % 1000) * 100000, 6442450944, 536870912, 8589934592 + (n % 1000) * 100000, 17179869184
FROM generate_series(0, $5) AS n", 13_000_000L, weekStart, WeekMinutes);
    }

    /// <summary>One generate_series plant: $1 an id base past anything the generator has handed out, $2 the first
    /// collection, $3/$4 the server, $5 the last minute's index.</summary>
    private static async Task PlantAsync(NpgsqlConnection connection, CancellationToken ct, string sql, long idOffset, DateTime first, int minutes)
    {
        using var plant = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        plant.Parameters.AddWithValue(CollectionIdGenerator.Next() + idOffset * 100);
        plant.Parameters.AddWithValue(DarlingMcpTestData.Naive(first));
        plant.Parameters.AddWithValue(ServerId);
        plant.Parameters.AddWithValue(ServerName);
        plant.Parameters.AddWithValue(minutes);
        await plant.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[]
        {
            "file_io_stats", "wait_stats", "query_stats", "pg_io_stats", "pg_database_stats",
            /* #3960 */
            "cpu_utilization_stats", "tempdb_stats", "memory_stats", "memory_grant_stats", "perfmon_stats", "pg_statement_stats",
            /* #4193 */
            "pg_cpu_utilization",
            "servers",
        })
        {
            using var delete = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1", connection) { CommandTimeout = 300 };
            delete.Parameters.AddWithValue(ServerId);
            await delete.ExecuteNonQueryAsync(ct);
        }
    }
}
