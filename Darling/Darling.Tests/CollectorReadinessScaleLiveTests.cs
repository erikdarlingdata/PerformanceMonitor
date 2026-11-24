/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #9417: <c>get_collector_readiness</c> on a log that has been filling for days. The old read aggregated every row since the start for every
/// pair in one scan, so on a service that has been up a while it read the whole log. The read now probes the first good cycle per pair and reads
/// errors and skips only for the pairs it shows. These tests prove the new reads give the old query's answers on a seeded log (with chunks
/// older than a day compressed the way the store's policy does), and that the waiting list is cut at 200 rows, oldest wait first, with the total
/// counted. The scale knobs <c>DARLING_READINESS_SERVERS</c> and <c>DARLING_READINESS_EXPLAIN_OUT</c> let a measurement run seed a fleet and
/// write the plans of both shapes to a file.
/// </summary>
public sealed class CollectorReadinessScaleLiveTests
{
    private const int SeedServerBase = 9_417_100;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The query this read shipped with: one scan of every row since the start, for every pair. It stays here as the reference the new reads must match.</summary>
    private static readonly string OldSinceStartSql = $@"
SELECT server_id, collector_name,
       MIN(collection_time) FILTER (WHERE status = 'SUCCESS' AND NOT COALESCE({EnumeratedCollectorDriver.AbandonedByNotePredicateSql}, false)) AS first_good,
       COUNT(*) FILTER (WHERE status IN ({CollectorFleetLiveness.FailureStatusSqlList})
                        OR (status = 'SUCCESS' AND COALESCE({EnumeratedCollectorDriver.AbandonedByNotePredicateSql}, false))) AS errors,
       COUNT(*) FILTER (WHERE status IS NULL OR status NOT IN ('SUCCESS', {CollectorFleetLiveness.FailureStatusSqlList})) AS skips,
       MAX(collection_time) FILTER (WHERE status IN ({CollectorFleetLiveness.FailureStatusSqlList})) AS last_error_at,
       (array_agg(left(error_message, {CollectorFleetLiveness.MaxMessageChars}) ORDER BY collection_time DESC)
            FILTER (WHERE status IN ({CollectorFleetLiveness.FailureStatusSqlList})))[1] AS last_error
FROM collection_log
WHERE collection_time >= $1
AND   collection_time <= $2
AND   ($3::int IS NULL OR server_id = $3::int)
GROUP BY server_id, collector_name";

    private sealed record OldRow(DateTime? FirstGood, long Errors, long Skips, DateTime? LastErrorAt, string? LastError);

    [Fact]
    public async Task FirstGoodAndWaitingDetail_MatchTheOldSinceStartScan_OnACompressedSeed()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live get_collector_readiness scale test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var serverCount = int.TryParse(Environment.GetEnvironmentVariable("DARLING_READINESS_SERVERS"), out var n) && n > 0 ? n : 5;
            var explainOut = Environment.GetEnvironmentVariable("DARLING_READINESS_EXPLAIN_OUT");
            var now = LiveClock.AnchoredEnd();
            now = new DateTime(now.Ticks - (now.Ticks % 10), DateTimeKind.Utc);
            var collectors = CollectorCatalog.All
                .Select(d => d.Name)
                .Where(name => CollectorScheduleDefaults.All.TryGetValue(name, out var e) && e.DefaultEnabled)
                .Take(30)
                .ToArray();
            Assert.True(collectors.Length >= 20, "The catalog should offer at least 20 enabled collectors to seed.");

            var report = new StringBuilder();
            await SeedAsync(connection, serverCount, collectors, now, ct);
            var compressed = await CompressOldChunksAsync(connection, now, ct);
            report.AppendLine($"seed: {serverCount} servers x {collectors.Length} collectors, {await ScalarAsync(connection, "SELECT count(*) FROM collection_log", ct)} log rows, chunks compressed: {compressed}");

            var pairs = new List<DarlingMcpCollectorReadinessTools.Pair>();
            for (var s = 1; s <= serverCount; s++)
            {
                for (var c = 0; c < collectors.Length; c++)
                {
                    pairs.Add(new DarlingMcpCollectorReadinessTools.Pair(SeedServerBase + s, collectors[c], IntervalOf(s, c), false));
                }
            }

            var windowStarts = new[] { now.AddDays(-2), now.AddMinutes(-10) };
            foreach (var start in windowStarts)
            {
                var label = start == windowStarts[0] ? "start 2 days back" : "start 10 minutes back";
                var oldRows = await ReadOldAsync(postgres, start, now, ct);
                var firstGood = await DarlingMcpCollectorReadinessTools.ReadFirstGoodAsync(postgres, pairs, start, now, ct);

                /* Same first good time for every pair, and no first good where the old scan had none. */
                foreach (var pair in pairs)
                {
                    oldRows.TryGetValue((pair.ServerId, pair.Collector), out var old);
                    firstGood.TryGetValue((pair.ServerId, pair.Collector), out var fresh);
                    Assert.Equal(old?.FirstGood, fresh == default ? null : fresh);
                }

                /* Same waiting set, and for the waiting pairs the same counts and newest error. */
                var waiting = pairs.Where(p => !firstGood.ContainsKey((p.ServerId, p.Collector))).ToList();
                var oldWaiting = pairs.Where(p => oldRows.GetValueOrDefault((p.ServerId, p.Collector))?.FirstGood is null).ToList();
                Assert.Equal(
                    oldWaiting.Select(p => (p.ServerId, p.Collector)).OrderBy(k => k).ToList(),
                    waiting.Select(p => (p.ServerId, p.Collector)).OrderBy(k => k).ToList());
                var detail = await DarlingMcpCollectorReadinessTools.ReadDetailAsync(postgres, waiting, start, now, ct);
                foreach (var pair in waiting)
                {
                    var old = oldRows.GetValueOrDefault((pair.ServerId, pair.Collector));
                    var fresh = detail[(pair.ServerId, pair.Collector)];
                    Assert.Equal(old?.Errors ?? 0, fresh.Errors);
                    Assert.Equal(old?.Skips ?? 0, fresh.Skips);
                    Assert.Equal(old?.LastError, fresh.LastError);
                    Assert.Equal(old?.LastErrorAt, fresh.LastErrorAt);
                }

                if (explainOut is not null)
                {
                    report.AppendLine($"== {label}: old scan");
                    report.Append(await ExplainAsync(connection, OldSinceStartSql, ct, start, now, DBNull.Value));
                    report.AppendLine($"== {label}: new first-good probe");
                    report.Append(await ExplainPairsAsync(connection, DarlingMcpCollectorReadinessTools.FirstGoodSql, pairs, start, now, ct));
                    report.AppendLine($"== {label}: new detail read for {Math.Min(waiting.Count, DarlingMcpCollectorReadinessTools.MaxWaitingRows)} shown waiting pairs");
                    report.Append(await ExplainPairsAsync(
                        connection, DarlingMcpCollectorReadinessTools.DetailSql, waiting.Take(DarlingMcpCollectorReadinessTools.MaxWaitingRows).ToList(), start, now, ct));
                    report.AppendLine($"== {label}: new last-run-before-start probe, floored at the worker's seeding lookback");
                    report.Append(await ExplainPairsAsync(
                        connection, DarlingMcpCollectorReadinessTools.LastRunBeforeStartSql, pairs, start, start - DarlingWorker.WatermarkFloorLookback, ct));
                }
            }

            if (explainOut is not null)
            {
                await File.WriteAllTextAsync(explainOut, report.ToString(), ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task WaitingList_IsCutAt200Rows_OldestWaitFirst_WithTheTotalCounted()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live get_collector_readiness cut test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var now = LiveClock.Now();
            var start = now.AddMinutes(-10);

            /* Enough servers that every due collector waiting from the start is well past 200 rows. */
            for (var i = 1; i <= 12; i++)
            {
                await RegisterServerAsync(connection, SeedServerBase + 500 + i, $"cut-{i:00}", ct);
            }

            /* "aa-late" sorts first by name, but its collector ran three minutes inside its interval before the start, so it came due after
               the start and is the youngest wait. With the list cut at 200 it must be the one cut, and the total must still count it. */
            var overrides = await DarlingDataReader.ReadScheduleOverridesAsync(postgres, serverId: null, ct, throwOnFailure: true);
            var schedules = StoreConfigProvider.MergeRunTimes(overrides, new List<RunTimeOverride>());
            string? late = null;
            var interval = 0;
            foreach (var name in CollectorCatalog.All.Select(d => d.Name).Where(CollectorScheduleDefaults.All.ContainsKey))
            {
                var schedule = StoreConfigProvider.ResolveSchedule(name, SeedServerBase + 600, schedules);
                var minutes = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(schedule.FrequencyMinutes);
                if (schedule.Enabled && schedule.RunAtMinute is null && minutes is >= 5 and < 1440)
                {
                    late = name;
                    interval = minutes;
                    break;
                }
            }

            Assert.NotNull(late);
            await RegisterServerAsync(connection, SeedServerBase + 600, "aa-late", ct);
            await InsertErrorAsync(connection, SeedServerBase + 600, "aa-late", late, start.AddMinutes(-(interval - 3)), ct);

            string waitingSince;
            using (var one = JsonDocument.Parse(await DarlingMcpCollectorReadinessTools.GetCollectorReadiness(postgres, "aa-late", start, now, ct)))
            {
                var row = one.RootElement.GetProperty("waiting").EnumerateArray().Single(w => w.GetProperty("collector").GetString() == late);
                waitingSince = row.GetProperty("waiting_since").GetString()!;
                Assert.NotEqual(DateTime.SpecifyKind(start, DateTimeKind.Utc).ToString("o"), waitingSince);
            }

            using var doc = JsonDocument.Parse(await DarlingMcpCollectorReadinessTools.GetCollectorReadiness(postgres, null, start, now, ct));
            var root = doc.RootElement;
            var shown = root.GetProperty("waiting").EnumerateArray().ToList();
            Assert.True(root.GetProperty("waiting_count").GetInt32() > DarlingMcpCollectorReadinessTools.MaxWaitingRows);
            Assert.Equal(DarlingMcpCollectorReadinessTools.MaxWaitingRows, shown.Count);
            Assert.True(root.GetProperty("truncated").GetBoolean());

            /* Oldest wait first, and the youngest wait is the one that fell off the end. */
            var stamps = shown.Select(w => DateTime.Parse(w.GetProperty("waiting_since").GetString()!, null, System.Globalization.DateTimeStyles.RoundtripKind)).ToList();
            Assert.Equal(stamps.OrderBy(t => t).ToList(), stamps);
            Assert.DoesNotContain(shown, w => w.GetProperty("server").GetString() == "aa-late" && w.GetProperty("collector").GetString() == late);
            Assert.All(shown, w => Assert.NotEqual(waitingSince, w.GetProperty("waiting_since").GetString()));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static int IntervalOf(int server, int collectorIndex) => 1 + (((server * 7) + (collectorIndex * 3)) % 15);

    /// <summary>
    /// Seeds the log for the fleet back two days from <paramref name="now"/> at 1 to 15 minute intervals per pair. Most rows are SUCCESS, some are
    /// errors or skips, some pairs only ever error, some only ever abandon their work, and some report a NULL row count.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, int serverCount, string[] collectors, DateTime now, CancellationToken ct)
    {
        var stamp = DateTime.SpecifyKind(now, DateTimeKind.Unspecified);
        using (var servers = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
SELECT $1 + s, 'scale-' || s, 'scale-' || s, TRUE, 15, $2, $2 FROM generate_series(1, $3) AS s", connection))
        {
            servers.Parameters.AddWithValue(SeedServerBase);
            servers.Parameters.AddWithValue(stamp);
            servers.Parameters.AddWithValue(serverCount);
            await servers.ExecuteNonQueryAsync(ct);
        }

        using var log = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected, error_message)
SELECT 9417000000000 + row_number() OVER (),
       $2 - (r.n * r.iv) * interval '1 minute' - ((r.s * 13 + r.c * 7) % 50) * interval '1 second',
       $1 + r.s, 'scale-' || r.s, r.name,
       CASE WHEN (r.s + r.c) % 97 = 0 THEN 'ERROR'
            WHEN (r.s + r.c) % 101 = 3 THEN 'SUCCESS'
            WHEN (r.s + r.c) % 103 = 5 THEN 'SUCCESS'
            WHEN r.n % 23 = 1 THEN 'ERROR'
            WHEN r.n % 31 = 2 THEN 'SKIPPED'
            ELSE 'SUCCESS' END,
       10,
       CASE WHEN (r.s + r.c) % 101 = 3 THEN 0 WHEN (r.s + r.c) % 103 = 5 THEN NULL ELSE 5 END,
       CASE WHEN (r.s + r.c) % 97 = 0 THEN 'simulated failure ' || r.n
            WHEN (r.s + r.c) % 101 = 3 THEN 'wall-clock budget (60s) reached; cycle abandoned'
            WHEN (r.s + r.c) % 103 = 5 THEN NULL
            WHEN r.n % 23 = 1 THEN 'simulated failure ' || r.n
            ELSE NULL END
FROM (
    SELECT s, c.ordinality::int - 1 AS c, c.name, 1 + ((s * 7 + (c.ordinality::int - 1) * 3) % 15) AS iv, n
    FROM generate_series(1, $3) AS s
    CROSS JOIN unnest($4::text[]) WITH ORDINALITY AS c (name, ordinality)
    CROSS JOIN LATERAL generate_series(0, (2 * 1440) / (1 + ((s * 7 + (c.ordinality::int - 1) * 3) % 15))) AS n
) r", connection);
        log.CommandTimeout = 600;
        log.Parameters.AddWithValue(SeedServerBase);
        log.Parameters.AddWithValue(stamp);
        log.Parameters.AddWithValue(serverCount);
        log.Parameters.AddWithValue(collectors);
        await log.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Compresses the chunks older than a day, as the store's compression policy does. Returns 0 on a store without TimescaleDB.</summary>
    private static async Task<long> CompressOldChunksAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        var hypertable = Convert.ToInt64(await ScalarAsync(connection,
            "SELECT count(*) FROM pg_extension e WHERE e.extname = 'timescaledb' AND EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'collection_log')", ct));
        if (hypertable == 0)
        {
            return 0;
        }

        using var compress = new NpgsqlCommand(
            "SELECT count(compress_chunk(ch, if_not_compressed => true)) FROM show_chunks('collection_log', older_than => $1::timestamp) AS ch", connection);
        compress.CommandTimeout = 600;
        compress.Parameters.AddWithValue(DateTime.SpecifyKind(now.AddDays(-1), DateTimeKind.Unspecified));
        var count = Convert.ToInt64(await compress.ExecuteScalarAsync(ct));
        using var analyze = new NpgsqlCommand("ANALYZE collection_log", connection);
        analyze.CommandTimeout = 600;
        await analyze.ExecuteNonQueryAsync(ct);
        return count;
    }

    private static async Task<Dictionary<(int, string), OldRow>> ReadOldAsync(NpgsqlDataSource postgres, DateTime start, DateTime now, CancellationToken ct)
    {
        var rows = new Dictionary<(int, string), OldRow>();
        await using var command = postgres.CreateCommand(OldSinceStartSql);
        command.CommandTimeout = 600;
        DarlingMcpReadParameters.AddTimestamp(command, start);
        DarlingMcpReadParameters.AddTimestamp(command, now);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = DBNull.Value });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows[(reader.GetInt32(0), reader.GetString(1))] = new OldRow(
                reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                reader.GetInt64(3), reader.GetInt64(4),
                reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                reader.IsDBNull(6) ? null : reader.GetString(6));
        }

        return rows;
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, string sql, CancellationToken ct, DateTime start, DateTime now, object serverId)
    {
        using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) " + sql, connection);
        command.CommandTimeout = 600;
        DarlingMcpReadParameters.AddTimestamp(command, start);
        DarlingMcpReadParameters.AddTimestamp(command, now);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = serverId });
        return await ReadPlanAsync(command, ct);
    }

    private static async Task<string> ExplainPairsAsync(
        NpgsqlConnection connection, string sql, List<DarlingMcpCollectorReadinessTools.Pair> pairs, DateTime start, DateTime now, CancellationToken ct)
    {
        using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) " + sql, connection);
        command.CommandTimeout = 600;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, Value = pairs.Select(p => p.ServerId).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, Value = pairs.Select(p => p.Collector).ToArray() });
        DarlingMcpReadParameters.AddTimestamp(command, start);
        DarlingMcpReadParameters.AddTimestamp(command, now);
        return await ReadPlanAsync(command, ct);
    }

    private static async Task<string> ReadPlanAsync(NpgsqlCommand command, CancellationToken ct)
    {
        var text = new StringBuilder();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            text.AppendLine(reader.GetString(0));
        }

        return text.ToString();
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task RegisterServerAsync(NpgsqlConnection connection, int serverId, string name, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 15, $3, $3)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertErrorAsync(NpgsqlConnection connection, int serverId, string serverName, string collector, DateTime at, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected, error_message)
VALUES ($1,$2,$3,$4,$5,'ERROR',10,0,'simulated failure')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(collector);
        await command.ExecuteNonQueryAsync(ct);
    }
}
