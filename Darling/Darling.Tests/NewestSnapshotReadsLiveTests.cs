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

    /// <summary>
    /// DatabaseSizeNewestSql as shipped before #5794: the two edge timestamps in one `edge` CTE, probed with
    /// `IN (SELECT ...)`. An oracle, never run by the product. The new statement must return exactly these rows.
    /// </summary>
    private const string EdgeInOracleSql = @"
WITH newest AS (
    SELECT collection_time
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   size_mb > 0
    ORDER BY collection_time DESC
    LIMIT 1
),
cur AS (
    SELECT DISTINCT ON (database_name, file_name) database_name, file_name, size_mb
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time = (SELECT collection_time FROM newest)
    AND   size_mb > 0
    ORDER BY database_name, file_name
),
edge AS (
    (
        SELECT collection_time
        FROM file_io_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time < (SELECT collection_time FROM newest)
        AND   size_mb > 0
        ORDER BY collection_time DESC
        LIMIT 1
    )
    UNION
    (
        SELECT collection_time
        FROM file_io_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time < (SELECT collection_time FROM newest)
        AND   size_mb > 0
        ORDER BY collection_time ASC
        LIMIT 1
    )
)
SELECT
    (SELECT SUM(size_mb) FROM cur) AS total_size_mb,
    EXISTS (
        SELECT 1
        FROM file_io_stats AS f
        WHERE f.server_id = $1
        AND   f.collection_time IN (SELECT collection_time FROM edge)
        AND   f.size_mb > 0
        AND   NOT EXISTS (
            SELECT 1
            FROM cur AS c
            WHERE c.database_name = f.database_name
            AND   c.file_name = f.file_name
        )
    ) AS dropped_out";

    private static readonly string[] s_counters = ["Batch Requests/sec", "SQL Compilations/sec", "SQL Re-Compilations/sec"];

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* #5608: the end of every window is the fixed anchor, never the clock. The perfmon read's block count depends on where
       the day-sized raw chunk boundary falls in the seeded window: the 4-hour window straddles it between 00:00 and 04:00
       UTC, and the newest read then walks a different number of rows inside the newest chunk, so a 3x margin that held at
       midday failed at 00:50 (perfmon old=264 new=120). The anchor keeps the window inside one chunk at any hour. */
    private static DateTime Now() => LiveClock.AnchoredEnd();

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
    /// The one accepted difference from the full read (#5516): a file seen only in snapshots strictly between the
    /// two probed ones (the oldest in the lookback and the one just before the newest) is not carried by the cheap
    /// path, where the full read carried it as a ghost for up to 24 hours. Pinned so a change to it is deliberate:
    /// if the probe ever closes the gap the cheap total will include the file and this test will say so.
    /// </summary>
    [Fact]
    public async Task TheDatabaseSizeFact_DoesNotCarryAFileSeenOnlyBetweenTheProbedSnapshots_AgainstDevPostgres()
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

        /* File A is in every snapshot, including the oldest in the lookback (23 h ago) and the second newest. File G
           lives in two middle snapshots only: in neither probed snapshot, and not in the newest. */
        const int hole = -516_009;
        await FileIoAsync(c, hole, At(1380), "Db", "A", 100, ct);
        foreach (var m in new[] { 720, 690, 600, 300, 30, 0 })
        {
            await FileIoAsync(c, hole, At(m), "Db", "A", 100, ct);
        }

        foreach (var m in new[] { 720, 690 })
        {
            await FileIoAsync(c, hole, At(m), "Staging", "G", 5000, ct);
        }

        var context = Context(hole, end);
        var old = await OldDatabaseSizeAsync(c, context, ct);
        var (cheap, droppedOut) = await NewestDatabaseSizeAsync(c, context, ct);

        Assert.Equal(5000 + 100, old);
        Assert.Equal(100, cheap);
        Assert.False(droppedOut, "the probe sees neither snapshot with the file, so the full read is not asked");
        Assert.Equal(100, await ShippedDatabaseSizeFactAsync(postgres, context));
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
    /// The rows seeded here are uncompressed: what the older COMPRESSED chunks cost the old shapes is not measured.
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

    private static async Task FileIoRowAsync(NpgsqlConnection c, int serverId, DateTime at, string db, string file, decimal? size, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(@"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
VALUES ($1, $2, $3, 'newest-snapshot-e2e', $4, $5, 'ROWS', $6)", c);
        cmd.Parameters.AddWithValue(CollectionIdGenerator.Next());
        cmd.Parameters.AddWithValue(at);
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.AddWithValue(db);
        cmd.Parameters.AddWithValue(file);
        cmd.Parameters.Add(new NpgsqlParameter { Value = size.HasValue ? size.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Numeric });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Compresses every raw chunk that ends on or before the start of <paramref name="end"/>'s day; the end's own day stays raw.</summary>
    private static async Task<(long Compressed, long Raw)> CompressOlderChunksAsync(NpgsqlConnection c, DateTime end, CancellationToken ct)
    {
        using (var enable = new NpgsqlCommand(TimescaleSupport.EnableCompressionSql("file_io_stats"), c))
        {
            await enable.ExecuteNonQueryAsync(ct);
        }

        using (var compress = new NpgsqlCommand(
            "SELECT compress_chunk(ch, if_not_compressed => true) FROM show_chunks('file_io_stats', older_than => $1::timestamp) AS ch", c))
        {
            compress.Parameters.AddWithValue(Naive(end.Date));
            await compress.ExecuteNonQueryAsync(ct);
        }

        using var count = new NpgsqlCommand(@"
SELECT COUNT(*) FILTER (WHERE is_compressed), COUNT(*) FILTER (WHERE NOT is_compressed)
FROM timescaledb_information.chunks WHERE hypertable_name = 'file_io_stats'", c);
        using var reader = await count.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    /* Edge times used by the #5794 scenarios, minutes before the window's end. The anchored end is 22:00 UTC, so Old
       (23.5 h back) and PrevCompressed (23 h back) land in the previous UTC day's chunk, which the helper compresses, and Mid,
       Prev and New land in the end's own day, which stays raw. */
    private const int Old = 1410, PrevCompressed = 1380, Mid = 720, Prev = 30, New = 0;

    /// <summary>
    /// Seeds every case the edge probe can meet, one server each: (server, name, expected dropped_out).
    /// </summary>
    private static async Task<List<(int ServerId, string Name, bool DroppedOut)>> SeedEdgeScenariosAsync(NpgsqlConnection c, DateTime end, CancellationToken ct)
    {
        DateTime At(int minutesAgo) => end.AddMinutes(-minutesAgo);
        var scenarios = new List<(int, string, bool)>();
        var next = -579_400;

        async Task<int> Server(string name, bool dropped, params (int Minutes, string File, decimal? Size)[] rows)
        {
            var id = next--;
            foreach (var (minutes, file, size) in rows)
            {
                await FileIoRowAsync(c, id, At(minutes), "Db", file, size, ct);
            }

            scenarios.Add((id, name, dropped));
            return id;
        }

        (int, string, decimal?)[] Steady(params int[] minutes) =>
            minutes.SelectMany(m => new[] { (m, "A", (decimal?)100m), (m, "B", (decimal?)200m), (m, "C", (decimal?)300m) }).ToArray();

        await Server("steady", false, Steady(Old, Mid, Prev, New));

        /* A file that disappears between the two edges: present in the oldest edge only, in the previous-snapshot edge
           only, and in both (newest lacks it in each). The oldest-edge case is the one a dropped second probe loses. */
        await Server("vanished-after-oldest-edge", true,
            Steady(Old, Mid, Prev, New).Where(r => !(r.Item2 == "C" && (r.Item1 == Prev || r.Item1 == New))).ToArray());
        await Server("vanished-at-previous-edge-only", true,
            [(Old, "A", 100m), (Prev, "A", 100m), (Prev, "D", 40m), (New, "A", 100m)]);
        await Server("vanished-before-newest-in-both-edges", true,
            [(Old, "A", 100m), (Old, "C", 300m), (Prev, "A", 100m), (Prev, "C", 300m), (New, "A", 100m)]);

        /* A new file: only in the newest snapshot, then in the last two. Nothing dropped out. */
        await Server("new-file-in-newest", false, [(Old, "A", 100m), (Prev, "A", 100m), (New, "A", 100m), (New, "N", 70m)]);
        await Server("new-file-in-previous-and-newest", false,
            [(Old, "A", 100m), (Prev, "A", 100m), (Prev, "N", 70m), (New, "A", 100m), (New, "N", 70m)]);

        /* A file seen only strictly between the edges is the documented unseen case: neither read sees it. */
        await Server("only-between-edges", false, [(Old, "A", 100m), (Mid, "A", 100m), (Mid, "C", 300m), (Prev, "A", 100m), (New, "A", 100m)]);

        /* NULL and zero sizes: a newest row that stopped qualifying drops out; an older row that never qualified does not. */
        await Server("zero-in-newest", true, [(Old, "A", 100m), (Old, "B", 400m), (Prev, "A", 100m), (Prev, "B", 400m), (New, "A", 150m), (New, "B", 0m)]);
        await Server("null-in-newest", true, [(Old, "A", 100m), (Old, "B", 400m), (Prev, "A", 100m), (New, "A", 150m), (New, "B", null)]);
        await Server("zero-only-in-edges", false, [(Old, "A", 100m), (Old, "B", 0m), (Prev, "A", 100m), (Prev, "B", 0m), (New, "A", 100m)]);
        await Server("null-only-in-edges", false, [(Old, "A", 100m), (Old, "B", null), (Prev, "A", 100m), (Prev, "B", null), (New, "A", 100m)]);
        await Server("negative-in-edge", false, [(Old, "A", 100m), (Old, "B", -5m), (New, "A", 100m)]);

        /* Captures: one only, two only (both edges are the same snapshot), nothing, only after the end, only before the lookback. */
        await Server("single-capture", false, [(New, "A", 10m), (New, "B", 20m)]);
        await Server("two-captures-same-edge-dropped", true, [(Prev, "A", 100m), (Prev, "C", 300m), (New, "A", 100m)]);
        await Server("two-captures-same-edge-steady", false, [(Prev, "A", 100m), (New, "A", 100m)]);
        scenarios.Add((next--, "no-rows", false));
        await Server("after-end-only", false, [(-30, "A", 7777m)]);
        await Server("before-lookback-only", false, [(1800, "A", 900m)]);
        await Server("after-end-newer-than-window", false, [(Prev, "A", 100m), (-30, "A", 7777m), (-30, "C", 5m)]);

        /* A file seen only before the lookback starts is not an edge. */
        await Server("edge-before-lookback-ignored", false, [(1800, "X", 55m), (Prev, "A", 100m), (New, "A", 100m)]);

        /* Ties on collection_time: duplicate rows at the same timestamp, equal and unequal sizes, in the newest and an edge. */
        await Server("duplicate-rows-at-newest-and-edge", true,
            [(Old, "A", 100m), (Old, "A", 100m), (Old, "C", 300m), (Old, "C", 300m), (New, "A", 100m), (New, "A", 100m), (New, "B", 200m), (New, "B", 200m)]);
        await Server("duplicate-rows-nothing-dropped", false,
            [(Old, "A", 100m), (Old, "A", 100m), (Prev, "A", 100m), (New, "A", 100m), (New, "A", 100m)]);

        /* Both edges inside the compressed chunk (the previous raw day), and the previous-snapshot edge compressed while the newest is raw. */
        await Server("compressed-edges-vanished", true,
            [(Old, "A", 100m), (Old, "C", 300m), (PrevCompressed, "A", 100m), (PrevCompressed, "C", 300m), (New, "A", 100m)]);
        await Server("compressed-previous-edge-steady", false,
            [(Old, "A", 100m), (PrevCompressed, "A", 100m), (New, "A", 100m)]);

        return scenarios;
    }

    private static async Task<(double? Total, bool DroppedOut)> RunEdgeStatementAsync(NpgsqlConnection c, string sql, AnalysisContext context, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(sql, c);
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(Naive(context.LatestValueStartFor("file_io_stats")));
        cmd.Parameters.AddWithValue(Naive(context.TimeRangeEnd));
        using var reader = await cmd.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.IsDBNull(0) ? null : Convert.ToDouble(reader.GetValue(0)), reader.GetBoolean(1));
    }

    private static async Task<List<string>> EdgeMismatchesAsync(
        NpgsqlConnection c, string sql, DateTime end, IEnumerable<(int ServerId, string Name, bool DroppedOut)> scenarios, CancellationToken ct)
    {
        var mismatches = new List<string>();
        foreach (var (serverId, name, _) in scenarios)
        {
            var context = Context(serverId, end);
            var oracle = await RunEdgeStatementAsync(c, EdgeInOracleSql, context, ct);
            var candidate = await RunEdgeStatementAsync(c, sql, context, ct);
            if (oracle != candidate)
            {
                mismatches.Add($"{name}: oracle={oracle} candidate={candidate}");
            }
        }

        return mismatches;
    }

    /// <summary>
    /// #5794: the probe now compares to two scalar timestamps instead of an IN set. Its rows must be the old
    /// statement's rows in every case the probe can meet, across compressed and raw chunks.
    /// </summary>
    [Fact]
    public async Task TheDatabaseSizeNewestRead_ReturnsTheInSetShapesRows_InEveryEdgeCase_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live newest-snapshot edge parity test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await OpenAsync(ct);
        await using var scratchLease = scratch;
        await using var connectionLease = c;

        var end = Now();
        var scenarios = await SeedEdgeScenariosAsync(c, end, ct);
        if (scratch.TimescaleEnabled)
        {
            var (compressed, raw) = await CompressOlderChunksAsync(c, end, ct);
            Assert.True(compressed >= 1 && raw >= 1, $"the fixture must hold both compressed and raw chunks (compressed={compressed}, raw={raw})");
        }

        Assert.Empty(await EdgeMismatchesAsync(c, PgFactCollector.DatabaseSizeNewestSql, end, scenarios, ct));

        /* By value, so identical-but-vacuous answers cannot pass: every scenario's dropped_out is what the case says. */
        foreach (var (serverId, name, dropped) in scenarios)
        {
            var (total, droppedOut) = await RunEdgeStatementAsync(c, PgFactCollector.DatabaseSizeNewestSql, Context(serverId, end), ct);
            Assert.True(dropped == droppedOut, $"{name}: expected dropped_out={dropped}, got {droppedOut} (total {total})");
        }

        var steadyTotal = (await RunEdgeStatementAsync(c, PgFactCollector.DatabaseSizeNewestSql, Context(scenarios.Single(s => s.Name == "steady").ServerId, end), ct)).Total;
        Assert.Equal(600d, steadyTotal);
        var dupTotal = (await RunEdgeStatementAsync(c, PgFactCollector.DatabaseSizeNewestSql, Context(scenarios.Single(s => s.Name == "duplicate-rows-at-newest-and-edge").ServerId, end), ct)).Total;
        Assert.Equal(300d, dupTotal);
        var empty = await RunEdgeStatementAsync(c, PgFactCollector.DatabaseSizeNewestSql, Context(scenarios.Single(s => s.Name == "no-rows").ServerId, end), ct);
        Assert.Equal((null, false), empty);
    }

    /// <summary>
    /// The parity check above must be able to fail: a statement that probes only one edge, or that matches no edge
    /// at all, returns different rows in the scenarios, so a regression of either kind is caught.
    /// </summary>
    [Fact]
    public async Task TheEdgeParityCheck_CatchesAProbeThatDropsAnEdge_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live newest-snapshot edge parity test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await OpenAsync(ct);
        await using var scratchLease = scratch;
        await using var connectionLease = c;

        var end = Now();
        var scenarios = await SeedEdgeScenariosAsync(c, end, ct);
        if (scratch.TimescaleEnabled)
        {
            await CompressOlderChunksAsync(c, end, ct);
        }

        var shipped = PgFactCollector.DatabaseSizeNewestSql;
        var dropsOldest = System.Text.RegularExpressions.Regex.Replace(
            shipped, @"\s+OR\s+f\.collection_time = \(SELECT collection_time FROM old_t\)", string.Empty);
        var dropsPrevious = System.Text.RegularExpressions.Regex.Replace(
            shipped, @"f\.collection_time = \(SELECT collection_time FROM prev_t\)\s+OR\s+", string.Empty);
        Assert.NotEqual(shipped, dropsOldest);
        Assert.NotEqual(shipped, dropsPrevious);

        var oldestMisses = await EdgeMismatchesAsync(c, dropsOldest, end, scenarios, ct);
        var previousMisses = await EdgeMismatchesAsync(c, dropsPrevious, end, scenarios, ct);
        Assert.Contains(oldestMisses, m => m.StartsWith("vanished-after-oldest-edge:", StringComparison.Ordinal));
        Assert.Contains(previousMisses, m => m.StartsWith("vanished-at-previous-edge-only:", StringComparison.Ordinal));
    }

    /// <summary>
    /// The point of #5794: against raw and compressed chunks over several days, the probe opens only the chunks that
    /// hold the two edge snapshots. The IN-set shape appended every chunk of the table (including the compressed
    /// ones) to find two timestamps. Block counts, not timings, so the assertion is stable.
    /// </summary>
    [Fact]
    public async Task TheDatabaseSizeNewestRead_ProbesOnlyTheEdgeChunks_NotEveryChunk_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live newest-snapshot chunk-exclusion test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await OpenAsync(ct);
        await using var scratchLease = scratch;
        await using var connectionLease = c;
        Assert.SkipUnless(scratch.TimescaleEnabled, "chunk exclusion needs TimescaleDB");

        const int serverId = -579_390;
        var end = Now();

        /* Six days of five-minute snapshots, 20 files each, ending at the window's end: the lookback holds the last
           day (two raw days' chunks) and the older four days sit in chunks the lookback never reaches. */
        using (var seed = new NpgsqlCommand(@"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
SELECT 579_000_000_000 + (EXTRACT(EPOCH FROM t)::bigint / 300) * 100 + f, t, $1, 'newest-snapshot-e2e',
       'Db' || f, 'Db' || f || '_data', 'ROWS', 100 * f
FROM generate_series($2::timestamp, $3::timestamp, interval '5 minutes') AS t CROSS JOIN generate_series(1, 20) AS f", c))
        {
            seed.Parameters.AddWithValue(serverId);
            seed.Parameters.AddWithValue(Naive(end.AddDays(-6)));
            seed.Parameters.AddWithValue(Naive(end));
            await seed.ExecuteNonQueryAsync(ct);
        }

        var (compressed, raw) = await CompressOlderChunksAsync(c, end, ct);
        Assert.True(compressed >= 3 && raw >= 1, $"the fixture must hold several compressed chunks and a raw one (compressed={compressed}, raw={raw})");
        using (var analyze = new NpgsqlCommand("ANALYZE file_io_stats", c))
        {
            await analyze.ExecuteNonQueryAsync(ct);
        }

        var context = Context(serverId, end);
        var lookback = context.LatestValueStartFor("file_io_stats");
        var oracle = await RunEdgeStatementAsync(c, EdgeInOracleSql, context, ct);
        var shipped = await RunEdgeStatementAsync(c, PgFactCollector.DatabaseSizeNewestSql, context, ct);
        Assert.Equal(oracle, shipped);
        Assert.Equal((21000d, false), shipped);

        var oldBlocks = await BlocksAsync(c, EdgeInOracleSql, [serverId, lookback, end], ct);
        var newBlocks = await BlocksAsync(c, PgFactCollector.DatabaseSizeNewestSql, [serverId, lookback, end], ct);
        Assert.True(newBlocks * 2 <= oldBlocks,
            $"the edge probe should touch half the blocks of the IN-set shape or fewer: old={oldBlocks} new={newBlocks}");
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
