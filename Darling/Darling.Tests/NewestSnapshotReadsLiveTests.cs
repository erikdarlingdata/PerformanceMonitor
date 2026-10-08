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
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5516: the file I/O size fact and the perfmon rate facts used to number every row of the window to keep the
/// newest per key (49.0 M and 42.4 M blocks over 13 days on the audited store). They now stop inside the newest
/// snapshot. Text pins on the new shape.
/// </summary>
public sealed class NewestSnapshotReadsTextTests
{
    [Fact]
    public void ThePerfmonRead_TakesTheNewestUsableRowPerCounter_AndNoLongerNumbersTheWindow()
    {
        var sql = PgFactCollector.PerfmonSql;

        Assert.DoesNotContain("ROW_NUMBER", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
        Assert.Equal(3, System.Text.RegularExpressions.Regex.Matches(sql, "ORDER BY collection_time DESC").Count);
        foreach (var counter in new[] { "Batch Requests/sec", "SQL Compilations/sec", "SQL Re-Compilations/sec" })
        {
            Assert.Contains($"counter_name = '{counter}'", sql, StringComparison.Ordinal);
        }

        Assert.Contains("sample_interval_seconds > 0", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDatabaseSizeRead_TakesTheNewestSnapshotByEquality_AndOnlyDroppedOutFilesSendItToTheFullRead()
    {
        var sql = PgFactCollector.DatabaseSizeNewestSql;

        Assert.DoesNotContain("ROW_NUMBER", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("collection_time = (SELECT collection_time FROM newest)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("dropped_out", sql, StringComparison.Ordinal);

        var source = File.ReadAllText(Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Analysis", "PgFactCollector.Storage.cs"));
        var method = source[source.IndexOf("private async Task CollectDatabaseSizeFactAsync", StringComparison.Ordinal)..];
        method = method[..method.IndexOf("ReportCollectionFailure", StringComparison.Ordinal)];
        Assert.True(
            method.IndexOf("new NpgsqlCommand(DatabaseSizeNewestSql", StringComparison.Ordinal)
            < method.IndexOf("new NpgsqlCommand(DatabaseSizeSql", StringComparison.Ordinal),
            "the cheap read runs first; the full read is the fallback");
        Assert.Contains("if (droppedOut)", method, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile);
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}

/* #1776 own-store: the live class below mints its own scratch database through ScratchPostgres and never touches
   another test's rows, so it is deliberately NOT [Collection("live-postgres")]. The scratch database is dropped
   on disposal, which is the cleanup. */

/// <summary>
/// #5516: the answers must not change. Both reads can return a key's OLDER row when the newest snapshot lacks it,
/// so each scenario runs the shipped path and the pre-#5516 read (kept here as an oracle) over the same rows.
/// </summary>
public sealed class NewestSnapshotReadsLiveTests
{
    /// <summary>The perfmon read as shipped before #5516. An oracle, never run by the product.</summary>
    private const string WindowFunctionPerfmonSql = @"
WITH latest AS (
    SELECT counter_name, cntr_value, delta_cntr_value, sample_interval_seconds,
           ROW_NUMBER() OVER (PARTITION BY counter_name ORDER BY collection_time DESC) AS rn
    FROM perfmon_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   counter_name IN ('Batch Requests/sec', 'SQL Compilations/sec', 'SQL Re-Compilations/sec')
    AND   sample_interval_seconds > 0
)
SELECT counter_name, cntr_value, delta_cntr_value, sample_interval_seconds
FROM latest WHERE rn = 1";

    private static readonly string[] s_counters = ["Batch Requests/sec", "SQL Compilations/sec", "SQL Re-Compilations/sec"];

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime Now()
    {
        var now = DateTime.UtcNow;
        return DateTime.SpecifyKind(new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
    }

    private static AnalysisContext Context(int serverId, DateTime end) => new()
    {
        ServerId = serverId,
        ServerName = "newest-snapshot-e2e",
        TimeRangeStart = end.AddHours(-4),
        TimeRangeEnd = end,
        ServerUtcOffset = TimeSpan.Zero,
    };

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)> OpenAsync(CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        if (await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct))
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await using var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection);
            await stop.ExecuteNonQueryAsync(ct);
        }

        return (scratch, connection);
    }

    private static async Task FileIoAsync(NpgsqlConnection c, int serverId, DateTime at, string db, string file, decimal size, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(@"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
VALUES ($1, $2, $3, 'newest-snapshot-e2e', $4, $5, 'ROWS', $6)", c);
        cmd.Parameters.AddWithValue(CollectionIdGenerator.Next());
        cmd.Parameters.AddWithValue(at);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(db);
        cmd.Parameters.AddWithValue(file);
        cmd.Parameters.AddWithValue(size);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task PerfmonAsync(NpgsqlConnection c, int serverId, DateTime at, string counter, long delta, int interval, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(@"
INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name,
                           cntr_value, delta_cntr_value, sample_interval_seconds)
VALUES ($1, $2, $3, 'newest-snapshot-e2e', 'SQLServer:SQL Statistics', $4, '', $5, $6, $7)", c);
        cmd.Parameters.AddWithValue(CollectionIdGenerator.Next());
        cmd.Parameters.AddWithValue(at);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(counter);
        cmd.Parameters.AddWithValue(delta * 2);
        cmd.Parameters.AddWithValue(delta);
        cmd.Parameters.AddWithValue(interval);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>The oracle: the pre-#5516 full read, over the same bounds the product binds.</summary>
    private static async Task<double?> OldDatabaseSizeAsync(NpgsqlConnection c, AnalysisContext context, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(PgFactCollector.DatabaseSizeSql, c);
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(Naive(context.LatestValueStartFor("file_io_stats")));
        cmd.Parameters.AddWithValue(Naive(context.TimeRangeEnd));
        var value = await cmd.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToDouble(value);
    }

    private static async Task<(double? Total, bool DroppedOut)> NewestDatabaseSizeAsync(NpgsqlConnection c, AnalysisContext context, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(PgFactCollector.DatabaseSizeNewestSql, c);
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(Naive(context.LatestValueStartFor("file_io_stats")));
        cmd.Parameters.AddWithValue(Naive(context.TimeRangeEnd));
        using var reader = await cmd.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.IsDBNull(0) ? null : Convert.ToDouble(reader.GetValue(0)), reader.GetBoolean(1));
    }

    private static async Task<double?> ShippedDatabaseSizeFactAsync(NpgsqlDataSource postgres, AnalysisContext context)
    {
        var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
        return facts.SingleOrDefault(f => f.Key == "DATABASE_TOTAL_SIZE_MB")?.Value;
    }

    private static async Task<List<string>> PerfmonRowsAsync(NpgsqlConnection c, string sql, AnalysisContext context, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(sql, c);
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(Naive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(Naive(context.TimeRangeEnd));
        var rows = new List<string>();
        using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add($"{reader.GetString(0)}|{reader.GetValue(1)}|{reader.GetValue(2)}|{reader.GetValue(3)}");
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    /// <summary>
    /// Every way the newest snapshot can differ from the full read's answer, one server each, compared with the
    /// pre-#5516 read run over the same rows. A steady store takes the cheap path alone; a file missing from the
    /// newest snapshot (dropped, a failed per-database read, a size that stopped qualifying) is carried by the
    /// full read, so the sum is the old one in every scenario.
    /// </summary>
    [Fact]
    public async Task TheDatabaseSizeFact_EqualsTheFullRead_WhenFilesAreMissingFromTheNewestSnapshot_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live newest-snapshot size test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await OpenAsync(ct);
        await using var scratchLease = scratch;
        await using var connectionLease = c;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var end = Now();
        DateTime At(int minutesAgo) => end.AddMinutes(-minutesAgo);

        /* 1: steady - three files in every snapshot, sizes growing; the newest snapshot decides. */
        const int steady = -516_001;
        for (var m = 600; m >= 0; m -= 30)
        {
            await FileIoAsync(c, steady, At(m), "Db", "A", 100 + (600 - m), ct);
            await FileIoAsync(c, steady, At(m), "Db", "B", 200 + (600 - m), ct);
            await FileIoAsync(c, steady, At(m), "Db", "C", 300 + (600 - m), ct);
        }

        /* 2: file C missing from the newest snapshot only - the full read keeps its older row. */
        const int missingNewest = -516_002;
        for (var m = 600; m >= 30; m -= 30)
        {
            await FileIoAsync(c, missingNewest, At(m), "Db", "A", 100, ct);
            await FileIoAsync(c, missingNewest, At(m), "Db", "B", 200, ct);
            await FileIoAsync(c, missingNewest, At(m), "Db", "C", 300, ct);
        }

        await FileIoAsync(c, missingNewest, At(0), "Db", "A", 111, ct);
        await FileIoAsync(c, missingNewest, At(0), "Db", "B", 222, ct);

        /* 3: a database present only in the OLDEST snapshot of the lookback (dropped long ago, still inside it). */
        const int onlyOldest = -516_003;
        await FileIoAsync(c, onlyOldest, At(1380), "Gone", "G", 5000, ct);
        for (var m = 600; m >= 0; m -= 30)
        {
            await FileIoAsync(c, onlyOldest, At(m), "Db", "A", 100, ct);
        }

        /* 4: the newest row for a file stopped qualifying (size 0) - the old read returned its older positive row. */
        const int zeroNewest = -516_004;
        await FileIoAsync(c, zeroNewest, At(60), "Db", "A", 100, ct);
        await FileIoAsync(c, zeroNewest, At(60), "Db", "B", 400, ct);
        await FileIoAsync(c, zeroNewest, At(0), "Db", "A", 150, ct);
        await FileIoAsync(c, zeroNewest, At(0), "Db", "B", 0, ct);

        /* 5: a sample after the window's end never wins; 6: one snapshot; 7: nothing at all. */
        const int afterEnd = -516_005;
        await FileIoAsync(c, afterEnd, At(10), "Db", "A", 100, ct);
        await FileIoAsync(c, afterEnd, At(-30), "Db", "A", 7777, ct);
        const int single = -516_006;
        await FileIoAsync(c, single, At(5), "Db", "A", 10, ct);
        await FileIoAsync(c, single, At(5), "Db", "B", 20, ct);
        const int empty = -516_007;

        /* 8: a file absent from a MIDDLE snapshot (a failed per-database read) but present in the newest: the newest
           snapshot is still complete, so the cheap path stands. */
        const int flaky = -516_008;
        foreach (var m in new[] { 600, 300, 90, 60, 30, 0 })
        {
            await FileIoAsync(c, flaky, At(m), "Db", "A", 100, ct);
        }

        foreach (var m in new[] { 600, 300, 90, 30, 0 })
        {
            await FileIoAsync(c, flaky, At(m), "Db", "B", 200, ct);
        }

        var expectDroppedOut = new Dictionary<int, bool>
        {
            [steady] = false, [missingNewest] = true, [onlyOldest] = true, [zeroNewest] = true,
            [afterEnd] = false, [single] = false, [empty] = false, [flaky] = false,
        };
        foreach (var (serverId, dropped) in expectDroppedOut)
        {
            var context = Context(serverId, end);
            var old = await OldDatabaseSizeAsync(c, context, ct);
            var (cheap, droppedOut) = await NewestDatabaseSizeAsync(c, context, ct);
            var fact = await ShippedDatabaseSizeFactAsync(postgres, context);

            Assert.True(dropped == droppedOut, $"server {serverId}: expected dropped_out={dropped}, got {droppedOut}");
            Assert.Equal(old is > 0 ? old : null, fact);
            if (!droppedOut)
            {
                Assert.Equal(old, cheap);
            }
        }

        /* By value, so a vacuous pass on both sides cannot hide: the old read's older rows are the answer. */
        Assert.Equal(111 + 222 + 300, await ShippedDatabaseSizeFactAsync(postgres, Context(missingNewest, end)));
        Assert.Equal(5000 + 100, await ShippedDatabaseSizeFactAsync(postgres, Context(onlyOldest, end)));
        Assert.Equal(150 + 400, await ShippedDatabaseSizeFactAsync(postgres, Context(zeroNewest, end)));
        Assert.Equal(100, await ShippedDatabaseSizeFactAsync(postgres, Context(afterEnd, end)));
        Assert.Equal(30, await ShippedDatabaseSizeFactAsync(postgres, Context(single, end)));
        Assert.Equal(700 + 800 + 900, await ShippedDatabaseSizeFactAsync(postgres, Context(steady, end)));
        Assert.Null(await ShippedDatabaseSizeFactAsync(postgres, Context(empty, end)));
    }

    /// <summary>
    /// The perfmon read returns the same rows as the window function it replaced, including a counter that is
    /// absent from the newest snapshot or unusable there (interval 0): the older usable row is still the answer.
    /// </summary>
    [Fact]
    public async Task ThePerfmonRead_ReturnsTheWindowFunctionsRows_IncludingOlderRowsForCountersTheNewestSnapshotLacks_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live newest-snapshot perfmon test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await OpenAsync(ct);
        await using var scratchLease = scratch;
        await using var connectionLease = c;

        var end = Now();
        DateTime At(int minutesAgo) => end.AddMinutes(-minutesAgo);

        /* 1: steady - all three counters usable in every snapshot. */
        const int steady = -516_011;
        for (var m = 200; m >= 0; m -= 20)
        {
            foreach (var counter in s_counters)
            {
                await PerfmonAsync(c, steady, At(m), counter, 600 + m, 60, ct);
            }
        }

        /* 2: the newest snapshot lacks one counter outright. */
        const int absent = -516_012;
        for (var m = 200; m >= 20; m -= 20)
        {
            foreach (var counter in s_counters)
            {
                await PerfmonAsync(c, absent, At(m), counter, 600 + m, 60, ct);
            }
        }

        await PerfmonAsync(c, absent, At(0), s_counters[0], 1, 60, ct);
        await PerfmonAsync(c, absent, At(0), s_counters[2], 2, 60, ct);

        /* 3: the newest snapshot holds an unusable (interval 0) row for one counter - the older usable row wins. */
        const int unusable = -516_013;
        foreach (var counter in s_counters)
        {
            await PerfmonAsync(c, unusable, At(40), counter, 900, 60, ct);
        }

        await PerfmonAsync(c, unusable, At(0), s_counters[0], 0, 0, ct);
        await PerfmonAsync(c, unusable, At(0), s_counters[1], 5, 60, ct);

        /* 4: a counter with only unusable rows yields no row; unrelated counters are ignored; a row after the end is. */
        const int mixed = -516_014;
        await PerfmonAsync(c, mixed, At(40), s_counters[2], 0, 0, ct);
        await PerfmonAsync(c, mixed, At(40), "Page life expectancy", 77, 60, ct);
        await PerfmonAsync(c, mixed, At(30), s_counters[0], 10, 60, ct);
        await PerfmonAsync(c, mixed, At(-30), s_counters[0], 99, 60, ct);

        /* 5: nothing at all. */
        const int empty = -516_015;

        foreach (var serverId in new[] { steady, absent, unusable, mixed, empty })
        {
            var context = Context(serverId, end);
            var oldRows = await PerfmonRowsAsync(c, WindowFunctionPerfmonSql, context, ct);
            var newRows = await PerfmonRowsAsync(c, PgFactCollector.PerfmonSql, context, ct);
            Assert.Equal(oldRows, newRows);
        }

        /* By value, so a vacuous pass on both sides cannot hide: the absent counter keeps its older row. */
        var absentRows = await PerfmonRowsAsync(c, PgFactCollector.PerfmonSql, Context(absent, end), ct);
        Assert.Equal(3, absentRows.Count);
        Assert.Contains(absentRows, r => r.StartsWith("SQL Compilations/sec|", StringComparison.Ordinal) && r.EndsWith("|620|60", StringComparison.Ordinal));
        var unusableRows = await PerfmonRowsAsync(c, PgFactCollector.PerfmonSql, Context(unusable, end), ct);
        Assert.Contains(unusableRows, r => r.StartsWith("Batch Requests/sec|", StringComparison.Ordinal) && r.EndsWith("|900|60", StringComparison.Ordinal));
        Assert.Single(await PerfmonRowsAsync(c, PgFactCollector.PerfmonSql, Context(mixed, end), ct));
        Assert.Empty(await PerfmonRowsAsync(c, PgFactCollector.PerfmonSql, Context(empty, end), ct));
    }

    /// <summary>
    /// The point of #5516: a window of minute snapshots. The old shapes touch every snapshot's blocks; the new
    /// ones touch the newest. Block counts, not timings, so the assertion is stable.
    /// </summary>
    [Fact]
    public async Task TheNewestSnapshotReads_TouchAFractionOfTheBlocksTheFullWindowReadsDo_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live newest-snapshot block-count test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await OpenAsync(ct);
        await using var scratchLease = scratch;
        await using var connectionLease = c;

        const int serverId = -516_021;
        var end = Now();

        using (var seed = new NpgsqlCommand(@"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
SELECT 516_000_000_000 + (EXTRACT(EPOCH FROM t)::bigint / 60) * 100 + f, t, $1, 'newest-snapshot-e2e',
       'Db' || f, 'Db' || f || '_data', 'ROWS', 100 * f
FROM generate_series($2::timestamp, $3::timestamp, interval '1 minute') AS t CROSS JOIN generate_series(1, 20) AS f", c))
        {
            seed.Parameters.AddWithValue(serverId);
            seed.Parameters.AddWithValue(end.AddHours(-24));
            seed.Parameters.AddWithValue(end);
            await seed.ExecuteNonQueryAsync(ct);
        }

        using (var seed = new NpgsqlCommand(@"
INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name,
                           cntr_value, delta_cntr_value, sample_interval_seconds)
SELECT 516_100_000_000 + (EXTRACT(EPOCH FROM t)::bigint / 60) * 100 + n, t, $1, 'newest-snapshot-e2e',
       'SQLServer:SQL Statistics',
       CASE WHEN n = 1 THEN 'Batch Requests/sec' WHEN n = 2 THEN 'SQL Compilations/sec' WHEN n = 3 THEN 'SQL Re-Compilations/sec'
            ELSE 'Filler counter ' || n END,
       '', 1000, 60, 60
FROM generate_series($2::timestamp, $3::timestamp, interval '1 minute') AS t CROSS JOIN generate_series(1, 60) AS n", c))
        {
            seed.Parameters.AddWithValue(serverId);
            seed.Parameters.AddWithValue(end.AddHours(-4));
            seed.Parameters.AddWithValue(end);
            await seed.ExecuteNonQueryAsync(ct);
        }

        foreach (var table in new[] { "file_io_stats", "perfmon_stats" })
        {
            using var analyze = new NpgsqlCommand($"ANALYZE {table}", c);
            await analyze.ExecuteNonQueryAsync(ct);
        }

        var context = Context(serverId, end);
        var lookback = context.LatestValueStartFor("file_io_stats");

        var oldSize = await BlocksAsync(c, PgFactCollector.DatabaseSizeSql, [serverId, lookback, end], ct);
        var newSize = await BlocksAsync(c, PgFactCollector.DatabaseSizeNewestSql, [serverId, lookback, end], ct);
        var oldPerfmon = await BlocksAsync(c, WindowFunctionPerfmonSql, [serverId, context.TimeRangeStart, end], ct);
        var newPerfmon = await BlocksAsync(c, PgFactCollector.PerfmonSql, [serverId, context.TimeRangeStart, end], ct);

        var plans = new System.Text.StringBuilder();
        foreach (var (sql, ps) in new (string, object[])[]
        {
            (PgFactCollector.DatabaseSizeNewestSql, [serverId, lookback, end]),
            (PgFactCollector.PerfmonSql, [serverId, context.TimeRangeStart, end]),
        })
        {
            using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) " + sql, c);
            foreach (var p in ps)
            {
                explain.Parameters.AddWithValue(p is DateTime dt ? Naive(dt) : p);
            }

            using var reader = await explain.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                plans.AppendLine(reader.GetString(0));
            }
        }

        var report = $"{plans}{Environment.NewLine}file_io old={oldSize} new={newSize}; perfmon old={oldPerfmon} new={newPerfmon}";
        Assert.True(newSize * 3 <= oldSize, "the database-size read should touch a third of the blocks or less: " + report);
        Assert.True(newPerfmon * 3 <= oldPerfmon, "the perfmon read should touch a third of the blocks or less: " + report);
    }

    private static async Task<long> BlocksAsync(NpgsqlConnection c, string sql, object[] parameters, CancellationToken ct)
    {
        using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, c);
        foreach (var p in parameters)
        {
            explain.Parameters.AddWithValue(p is DateTime dt ? Naive(dt) : p);
        }

        var json = (string)(await explain.ExecuteScalarAsync(ct))!;
        using var doc = JsonDocument.Parse(json);
        var plan = doc.RootElement[0].GetProperty("Plan");
        return plan.GetProperty("Shared Hit Blocks").GetInt64()
             + (plan.TryGetProperty("Shared Read Blocks", out var read) ? read.GetInt64() : 0);
    }
}
