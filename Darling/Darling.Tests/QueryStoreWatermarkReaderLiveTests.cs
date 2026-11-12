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
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live pins for the Query Store watermark seed read's side-table path (#5684, V178), READ side. The oracle is the raw read the
/// product used before: <see cref="DarlingCollectorRunner.BuildServerWatermarkWithWitnessForDatabaseSql"/> against
/// <c>collect.query_store_stats</c>. Every batch goes through the product's own write path, so the side rows are the ones a
/// production COPY writes.
///
/// <para><b>#1776 own-store:</b> every test here creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so it is not in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class QueryStoreWatermarkReaderLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to run the Query Store watermark reader's live pins (each mints its own scratch database).";

    private static readonly DateTime T0 = new(2026, 1, 5, 10, 0, 0, DateTimeKind.Unspecified);

    private static readonly string RawSql = DarlingCollectorRunner.BuildServerWatermarkWithWitnessForDatabaseSql(
        "collect.query_store_stats", "last_execution_time", "database_name");

    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }

        public required NpgsqlDataSource DataSource { get; init; }

        public required NpgsqlConnection Connection { get; init; }

        public required DarlingCollectorRunner Runner { get; init; }

        public required ServerRuntime Server { get; init; }

        public required CollectorContext Context { get; init; }

        public static async Task<Rig> OpenAsync(int serverId, string serverName, CancellationToken ct)
        {
            var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
            var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var deltas = new CollectorDeltaCalculator();
            var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            return new Rig
            {
                Scratch = scratch,
                DataSource = dataSource,
                Connection = connection,
                Runner = new DarlingCollectorRunner(dataSource, deltas),
                Context = new CollectorContext { ServerId = serverId, ServerName = serverName, CollectionTime = DateTime.UtcNow, Deltas = deltas },
                Server = new ServerRuntime
                {
                    Config = new MonitoredServer { Name = serverName, Host = serverName },
                    ConnectionString = "Server=" + serverName,
                    Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
                    StorageName = serverName,
                    ServerId = serverId,
                    EngineEdition = 3,
                },
            };
        }

        public Task<int> WriteAsync(DateTime collectionTime, IReadOnlyList<QueryStoreCollector.Row> rows) =>
            Runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, rows.ToList(), Server, collectionTime, Context, TestContext.Current.CancellationToken);

        /// <summary>One batch for another server of the same store.</summary>
        public Task<int> WriteForAsync(int serverId, DateTime collectionTime, IReadOnlyList<QueryStoreCollector.Row> rows)
        {
            var name = "qswm-5684-s" + serverId.ToString(CultureInfo.InvariantCulture);
            var server = new ServerRuntime
            {
                Config = new MonitoredServer { Name = name, Host = name },
                ConnectionString = "Server=" + name,
                Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
                StorageName = name,
                ServerId = serverId,
                EngineEdition = 3,
            };
            var context = new CollectorContext { ServerId = serverId, ServerName = name, CollectionTime = DateTime.UtcNow, Deltas = Context.Deltas };
            return Runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, rows.ToList(), server, collectionTime, context, TestContext.Current.CancellationToken);
        }

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            await DataSource.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private static QueryStoreCollector.Row Row(string database, long queryId, long planId, DateTime? last) => new()
    {
        DatabaseName = database,
        QueryId = queryId,
        PlanId = planId,
        ExecutionTypeDesc = "Regular",
        FirstExecutionTime = T0,
        LastExecutionTime = last,
        QueryHash = "0x" + queryId.ToString("X8", CultureInfo.InvariantCulture),
        QueryPlanHash = "0x" + planId.ToString("X8", CultureInfo.InvariantCulture),
        ExecutionCount = 1,
        AvgCpuTimeUs = 10,
        AvgDurationUs = 20,
        IsForcedPlan = false,
        ForceFailureCount = 0,
        RuntimeStatsIntervalId = 100,
    };

    private static string Stamp(DateTime value) => "'" + value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "'::timestamp";

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    /// <summary>Moves the one coverage row, so a test controls which batches count as written by a side-table build.</summary>
    private static Task SetCoverageAsync(NpgsqlConnection connection, DateTime coveredSince, CancellationToken ct) =>
        ExecAsync(connection, "UPDATE collect.query_store_watermarks_coverage SET covered_since = " + Stamp(coveredSince), ct);

    /// <summary>The raw read, exactly as the product ran it before the side table.</summary>
    private static async Task<(DateTime? Value, DateTime? Witness)> RawAsync(
        NpgsqlConnection connection, int serverId, string database, DateTime floor, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(RawSql, connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(floor, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct) && reader.GetValue(0) is DateTime value)
        {
            return (value, reader.GetValue(1) as DateTime?);
        }

        return (null, null);
    }

    private const string VerifyIndexName = "idx_query_store_stats_server_db_query_plan_time";

    /// <summary>
    /// True when <paramref name="scanned"/> is the verify index: its full name, or a hypertable chunk's copy, _hyper_N_M_chunk_ plus the
    /// full name cut to PostgreSQL's 63-byte identifier limit. The cut is built from the prefix found on the scanned name, not a length.
    /// </summary>
    internal static bool IsVerifyIndexName(string scanned)
    {
        if (string.Equals(scanned, VerifyIndexName, StringComparison.Ordinal))
        {
            return true;
        }

        var prefix = Regex.Match(scanned, @"^_hyper_\d+_\d+_chunk_");
        if (!prefix.Success)
        {
            return false;
        }

        var expected = prefix.Value + VerifyIndexName;
        if (expected.Length > 63)
        {
            expected = expected[..63];
        }

        return string.Equals(scanned, expected, StringComparison.Ordinal);
    }

    /// <summary>
    /// The chunk-index name check holds for chunk prefixes of 18, 19 and 20 characters (chunk ids 2 and 10, and a three-digit
    /// hypertable id), and still rejects another index, a name cut shorter than PostgreSQL's limit, and one with a different tail.
    /// </summary>
    [Theory]
    [InlineData("_hyper_31_2_chunk_")]
    [InlineData("_hyper_31_10_chunk_")]
    [InlineData("_hyper_131_10_chunk_")]
    public void TheChunkIndexNameCheck_FollowsThePrefixLength(string prefix)
    {
        var cut = (prefix + VerifyIndexName)[..63];
        Assert.True(IsVerifyIndexName(cut), cut);
        Assert.False(IsVerifyIndexName(cut[..^1]), "a name shorter than the 63-byte cut is not the cut");
        Assert.False(IsVerifyIndexName(prefix + "idx_query_store_stats_other_index"));
        Assert.True(IsVerifyIndexName(VerifyIndexName));
        Assert.False(IsVerifyIndexName(VerifyIndexName + "x"));
    }

    private static string Show((DateTime? Value, DateTime? Witness) r) =>
        (r.Value?.ToString("o", CultureInfo.InvariantCulture) ?? "null") + " @ " + (r.Witness?.ToString("o", CultureInfo.InvariantCulture) ?? "null");

    private sealed class CapturingLogger : ILogger
    {
        public List<string> Warnings { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                Warnings.Add(formatter(state, exception));
            }
        }
    }

    /// <summary>
    /// Random batches across several (server, database) pairs, with NULL last_execution_time, ties in the maximum across
    /// collection times, a batch written twice at one stamp, and backdated (backfill-shaped) batches on both sides of the coverage
    /// start. The stitched read equals the raw read, value AND witness, for every pair at every floor, including a floor exactly at a
    /// batch stamp (strict) and one exactly at the coverage start. Run twice: with every side row kept (a store whose backfill ran on
    /// the new build) and with the side rows before the coverage start removed (the shape right after the upgrade).
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheStitchedRead_EqualsTheRawRead_ForEveryPairAndFloor(bool dropSideRowsBeforeCoverage)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverA = -568411;
        const int serverB = -568412;

        await using var rig = await Rig.OpenAsync(serverA, "qswm-5684-r1", ct);
        var bodySucceeded = false;
        try
        {
            var covered = T0.AddMinutes(100);
            var random = new Random(5684);
            long id = 1;
            var databases = new[] { "dbA", "dbB", "dbC" };
            /* A small value set so the maximum ties across collection times. */
            var values = Enumerable.Range(0, 6).Select(i => T0.AddMinutes(i * 7).AddTicks(10 * i)).ToArray();
            var servers = new[] { serverA, serverB };
            for (var batch = 0; batch < 60; batch++)
            {
                var server = servers[random.Next(servers.Length)];
                var stamp = T0.AddMinutes(random.Next(0, 30) * 10);
                var rows = new List<QueryStoreCollector.Row>();
                foreach (var database in databases.Where(_ => random.Next(4) != 0))
                {
                    for (var n = random.Next(1, 5); n > 0; n--)
                    {
                        DateTime? last = random.Next(4) == 0 ? null : values[random.Next(values.Length)];
                        rows.Add(Row(database, id, id + 1000, last));
                        id++;
                    }
                }

                await rig.WriteForAsync(server, stamp, rows);
            }

            await SetCoverageAsync(rig.Connection, covered, ct);
            if (dropSideRowsBeforeCoverage)
            {
                await ExecAsync(rig.Connection, "DELETE FROM collect.query_store_watermarks WHERE collection_time < " + Stamp(covered), ct);
            }

            Assert.True(await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_stats", ct) > 100);
            var reader = new QueryStoreWatermarkReader(null);
            var floors = new List<DateTime> { T0.AddMinutes(-1), covered, covered.AddTicks(-10), covered.AddMinutes(1), T0.AddMinutes(300) };
            floors.AddRange(Enumerable.Range(0, 30).Select(i => T0.AddMinutes(i * 10)));
            var compared = 0;
            foreach (var server in servers)
            {
                foreach (var database in databases.Append("dbMissing"))
                {
                    foreach (var floor in floors)
                    {
                        var expected = await RawAsync(rig.Connection, server, database, floor, ct);
                        var actual = await reader.ReadAsync(rig.Connection, server, database, floor, RawSql, 60, ct);
                        Assert.True(expected == actual, $"server {server} {database} floor {floor:o}: raw {Show(expected)} vs stitched {Show(actual)}");
                        compared++;
                    }
                }
            }

            Assert.Equal(2 * 4 * floors.Count, compared);
            Assert.Equal(0, reader.Fallbacks);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A deleted raw max row makes verification fail: the read falls back to the full raw read, so the result is the raw answer
    /// (the next-best stored value), never the side row's now-orphaned value. The fallback is logged once per pair per window.
    /// </summary>
    [Fact]
    public async Task ADeletedRawMaxRow_FailsVerification_AndFallsBackToRaw_LoggedOncePerPair()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568421;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-r2", ct);
        var bodySucceeded = false;
        try
        {
            var high = T0.AddMinutes(50).AddTicks(1230);
            await rig.WriteAsync(T0.AddMinutes(10), [Row("dbA", 1, 11, T0.AddMinutes(5)), Row("dbA", 2, 22, T0.AddMinutes(6))]);
            await rig.WriteAsync(T0.AddMinutes(20), [Row("dbA", 3, 33, high)]);
            await SetCoverageAsync(rig.Connection, T0, ct);
            var floor = T0.AddMinutes(-60);
            var now = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc);
            var logger = new CapturingLogger();
            var reader = new QueryStoreWatermarkReader(logger, () => now);

            Assert.Equal((high, T0.AddMinutes(20)), await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct));
            Assert.Equal(0, reader.Fallbacks);

            await ExecAsync(rig.Connection, "DELETE FROM collect.query_store_stats WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture) + " AND query_id = 3", ct);
            var expected = await RawAsync(rig.Connection, serverId, "dbA", floor, ct);
            Assert.Equal((T0.AddMinutes(6), T0.AddMinutes(10)), expected);
            Assert.Equal(expected, await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct));
            Assert.Equal(expected, await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct));
            Assert.Equal(2, reader.Fallbacks);
            Assert.Single(logger.Warnings);

            /* An hour later the pair logs again. */
            now = now.AddHours(1).AddSeconds(1);
            Assert.Equal(expected, await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct));
            Assert.Equal(2, logger.Warnings.Count);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The per-pair fallback warning also fires when the clock has moved backwards past the last warning (#5707): a clock that
    /// steps back would otherwise silence it until the clock caught up.
    /// </summary>
    [Fact]
    public async Task TheFallbackWarning_FiresAgain_WhenTheClockMovesBackwards()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568481;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-r9", ct);
        var bodySucceeded = false;
        try
        {
            await rig.WriteAsync(T0.AddMinutes(10), [Row("dbA", 1, 11, T0.AddMinutes(5)), Row("dbA", 2, 22, T0.AddMinutes(6))]);
            await rig.WriteAsync(T0.AddMinutes(20), [Row("dbA", 3, 33, T0.AddMinutes(50))]);
            await SetCoverageAsync(rig.Connection, T0, ct);
            var floor = T0.AddMinutes(-60);
            var now = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);
            var logger = new CapturingLogger();
            var reader = new QueryStoreWatermarkReader(logger, () => now);

            await ExecAsync(rig.Connection, "DELETE FROM collect.query_store_stats WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture) + " AND query_id = 3", ct);
            var expected = await RawAsync(rig.Connection, serverId, "dbA", floor, ct);
            Assert.Equal(expected, await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct));
            Assert.Single(logger.Warnings);

            now = now.AddMinutes(10);
            Assert.Equal(expected, await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct));
            Assert.Single(logger.Warnings);

            /* The clock steps back an hour: the next fallback warns instead of waiting for the clock to catch up. */
            now = now.AddHours(-1);
            Assert.Equal(expected, await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct));
            Assert.Equal(2, logger.Warnings.Count);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A missing side row (a savepoint failure) lowers the answer and never raises it: with the newest batch's side row gone, the
    /// read returns the next side row's value, which is lower than the raw maximum; with a tie's twin row gone, it is equal.
    /// </summary>
    [Fact]
    public async Task AMissingSideRow_GivesALowerOrEqualValue_NeverHigher()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568431;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-r3", ct);
        var bodySucceeded = false;
        try
        {
            var tied = T0.AddMinutes(40);
            await rig.WriteAsync(T0.AddMinutes(10), [Row("dbA", 1, 11, tied)]);
            await rig.WriteAsync(T0.AddMinutes(20), [Row("dbA", 2, 22, tied)]);
            await rig.WriteAsync(T0.AddMinutes(30), [Row("dbA", 3, 33, T0.AddMinutes(45))]);
            await SetCoverageAsync(rig.Connection, T0, ct);
            var floor = T0.AddMinutes(-60);
            var reader = new QueryStoreWatermarkReader(null);

            Assert.Equal(await RawAsync(rig.Connection, serverId, "dbA", floor, ct), await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct));

            /* The newest batch has no side row: lower. */
            await ExecAsync(rig.Connection, "DELETE FROM collect.query_store_watermarks WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture) + " AND collection_time = " + Stamp(T0.AddMinutes(30)), ct);
            var raw = await RawAsync(rig.Connection, serverId, "dbA", floor, ct);
            var lowered = await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct);
            Assert.True(lowered.Value < raw.Value, Show(lowered) + " vs raw " + Show(raw));
            Assert.Equal((tied, T0.AddMinutes(20)), lowered);

            /* A tie's newest twin is gone: the value holds, the witness falls to the older twin (an older witness is the safe side). */
            await ExecAsync(rig.Connection, "DELETE FROM collect.query_store_watermarks WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture) + " AND collection_time = " + Stamp(T0.AddMinutes(20)), ct);
            var twin = await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct);
            Assert.Equal((tied, T0.AddMinutes(10)), twin);

            /* Every side row gone: the read is the raw read, NOT an empty answer (#5707). The caller maps null to now-60min and
               records no backfill gap, so an empty answer where raw holds a value loses the rows between silently. */
            await ExecAsync(rig.Connection, "DELETE FROM collect.query_store_watermarks WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture), ct);
            Assert.Equal((T0.AddMinutes(45), T0.AddMinutes(30)), await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct));
            Assert.Equal(await RawAsync(rig.Connection, serverId, "dbA", floor, ct), await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct));
            Assert.Equal(2, reader.EmptySideReads);
            Assert.Equal(0, reader.Fallbacks);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The H1 variant (#5707, review round 1): pairs with raw rows but NO side row above the floor, after coverage (a downgrade to a
    /// build that does not write them, an old instance still collecting, repeated savepoint faults, a manual cleanup). An empty
    /// answer is not "lower": the caller turns null into now-60min and records no backfill gap. So at every floor a pair with no side
    /// row equals the raw read, and a pair with only SOME side rows may be lower but is never above raw and is null only where raw
    /// is null.
    /// </summary>
    [Fact]
    public async Task APairWithRawRowsButNoSideRowsAfterCoverage_ReadsRaw_AndNeverAnswersEmptyWhereRawHasAValue()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568471;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-r7", ct);
        var bodySucceeded = false;
        try
        {
            var random = new Random(56841);
            long id = 1;
            var databases = new[] { "dbA", "dbB", "dbC" };
            var values = Enumerable.Range(0, 6).Select(i => T0.AddMinutes(i * 7).AddTicks(10 * i)).ToArray();
            for (var batch = 0; batch < 40; batch++)
            {
                var stamp = T0.AddMinutes(random.Next(0, 30) * 10);
                var rows = new List<QueryStoreCollector.Row>();
                foreach (var database in databases.Where(_ => random.Next(4) != 0))
                {
                    for (var n = random.Next(1, 5); n > 0; n--)
                    {
                        DateTime? last = random.Next(4) == 0 ? null : values[random.Next(values.Length)];
                        rows.Add(Row(database, id, id + 1000, last));
                        id++;
                    }
                }

                await rig.WriteAsync(stamp, rows);
            }

            /* Every batch is after coverage. dbB has no side row at all; dbC lost every other 10-minute batch's. */
            await SetCoverageAsync(rig.Connection, T0.AddMinutes(-120), ct);
            await ExecAsync(rig.Connection, "DELETE FROM collect.query_store_watermarks WHERE database_name = 'dbB'", ct);
            await ExecAsync(
                rig.Connection,
                "DELETE FROM collect.query_store_watermarks WHERE database_name = 'dbC' AND (extract(minute from collection_time)::int / 10) % 2 = 0", ct);

            var reader = new QueryStoreWatermarkReader(null);
            var floors = new List<DateTime> { T0.AddMinutes(-1), T0.AddMinutes(300) };
            floors.AddRange(Enumerable.Range(0, 30).Select(i => T0.AddMinutes(i * 10)));
            var rawHadValueWithoutSide = 0;
            foreach (var database in databases.Append("dbMissing"))
            {
                foreach (var floor in floors)
                {
                    var expected = await RawAsync(rig.Connection, serverId, database, floor, ct);
                    var actual = await reader.ReadAsync(rig.Connection, serverId, database, floor, RawSql, 60, ct);
                    if (database != "dbC")
                    {
                        Assert.True(expected == actual, $"{database} floor {floor:o}: raw {Show(expected)} vs stitched {Show(actual)}");
                    }

                    Assert.True(actual.Value is null || (expected.Value is { } rawValue && actual.Value <= rawValue), $"{database} floor {floor:o}: raw {Show(expected)} vs stitched {Show(actual)}");
                    Assert.True(actual.Value is not null || expected.Value is null, $"{database} floor {floor:o}: an empty answer where raw has {Show(expected)}");
                    if (database == "dbB" && expected.Value is not null)
                    {
                        rawHadValueWithoutSide++;
                    }
                }
            }

            Assert.True(rawHadValueWithoutSide > 0);
            Assert.True(reader.EmptySideReads >= rawHadValueWithoutSide);
            Assert.Equal(0, reader.Fallbacks);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// An idle pair (no rows in side or raw) is normal, so the empty-side read is never a Warning; a pair whose side row is missing
    /// but whose raw rows exist logs the throttled Warning once per hour (the data really is missing its side row).
    /// </summary>
    [Fact]
    public async Task TheEmptySideRead_NeverWarnsForAnIdlePair_AndWarnsOncePerHourWhenRawHoldsRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568472;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-r8", ct);
        var bodySucceeded = false;
        try
        {
            await rig.WriteAsync(T0.AddMinutes(10), [Row("dbA", 1, 11, T0.AddMinutes(5))]);
            await SetCoverageAsync(rig.Connection, T0.AddMinutes(-120), ct);
            await ExecAsync(rig.Connection, "DELETE FROM collect.query_store_watermarks WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture), ct);
            var now = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc);
            var logger = new CapturingLogger();
            var reader = new QueryStoreWatermarkReader(logger, () => now);
            var floor = T0.AddMinutes(-60);

            for (var i = 0; i < 3; i++)
            {
                Assert.Equal((null, null), await reader.ReadAsync(rig.Connection, serverId, "dbIdle", floor, RawSql, 60, ct));
            }

            Assert.Empty(logger.Warnings);
            Assert.Equal(3, reader.EmptySideReads);

            Assert.Equal((T0.AddMinutes(5), T0.AddMinutes(10)), await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct));
            Assert.Equal((T0.AddMinutes(5), T0.AddMinutes(10)), await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct));
            Assert.Single(logger.Warnings);
            now = now.AddHours(1).AddSeconds(1);
            await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct);
            Assert.Equal(2, logger.Warnings.Count);
            Assert.Equal(0, reader.Fallbacks);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The floor straddles the coverage start. Before the start only raw knows the pair (the side table is empty), so a floor below
    /// it runs the raw arm, once per read, and finds the pre-coverage maximum; a floor at or above it never runs the raw arm (the
    /// counter stays put) and the answer still equals raw. Strict bounds: a row stamped exactly at the floor is excluded by both reads.
    /// </summary>
    [Fact]
    public async Task TheRawArm_RunsOnlyWhileTheFloorIsBelowCoverage_AndNeverAfter()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568441;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-r4", ct);
        var bodySucceeded = false;
        try
        {
            var covered = T0.AddMinutes(60);
            await rig.WriteAsync(T0.AddMinutes(10), [Row("dbA", 1, 11, T0.AddMinutes(30))]);
            await rig.WriteAsync(T0.AddMinutes(70), [Row("dbA", 2, 22, T0.AddMinutes(65))]);
            await SetCoverageAsync(rig.Connection, covered, ct);
            await ExecAsync(rig.Connection, "DELETE FROM collect.query_store_watermarks WHERE collection_time < " + Stamp(covered), ct);
            var reader = new QueryStoreWatermarkReader(null);

            /* Below coverage, the older batch is reachable only through the raw arm; the newer side row wins on value. */
            var below = await reader.ReadAsync(rig.Connection, serverId, "dbA", T0, RawSql, 60, ct);
            Assert.Equal(await RawAsync(rig.Connection, serverId, "dbA", T0, ct), below);
            Assert.Equal(1, reader.PreCoverageReads);

            /* Only the pre-coverage batch is left and the side table cannot see it: the raw arm supplies it. */
            await ExecAsync(rig.Connection, "DELETE FROM collect.query_store_stats WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture) + " AND query_id = 2", ct);
            await ExecAsync(rig.Connection, "DELETE FROM collect.query_store_watermarks WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture), ct);
            var onlyRaw = await reader.ReadAsync(rig.Connection, serverId, "dbA", T0, RawSql, 60, ct);
            Assert.Equal((T0.AddMinutes(30), T0.AddMinutes(10)), onlyRaw);
            Assert.Equal(2, reader.PreCoverageReads);
            Assert.Equal(0, reader.Fallbacks);

            /* Strict: a floor exactly at the pre-coverage stamp excludes it. */
            Assert.Equal((null, null), await reader.ReadAsync(rig.Connection, serverId, "dbA", T0.AddMinutes(10), RawSql, 60, ct));

            /* At and above the coverage start the raw arm never runs. */
            var before = reader.PreCoverageReads;
            foreach (var floor in new[] { covered, covered.AddTicks(10), T0.AddMinutes(500) })
            {
                Assert.Equal(await RawAsync(rig.Connection, serverId, "dbA", floor, ct), await reader.ReadAsync(rig.Connection, serverId, "dbA", floor, RawSql, 60, ct));
            }

            Assert.Equal(before, reader.PreCoverageReads);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The product's own seed read (<see cref="DarlingCollectorRunner.ReadLastCollectedTimeAndWitnessForDatabaseAsync"/>) takes the
    /// stitched path for the Query Store shape, returns the same pair the raw read does, and counts no raw-arm run past coverage.
    /// </summary>
    [Fact]
    public async Task TheRunnersSeedRead_TakesTheStitchedPath_AndMatchesRaw()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -568451;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-r5", ct);
        var bodySucceeded = false;
        try
        {
            await rig.WriteAsync(T0.AddMinutes(10), [Row("dbA", 1, 11, T0.AddMinutes(30)), Row("dbA", 2, 22, null)]);
            await SetCoverageAsync(rig.Connection, T0.AddMinutes(-120), ct);
            var floor = T0.AddMinutes(-60);
            var (value, witness, ok) = await rig.Runner.ReadLastCollectedTimeAndWitnessForDatabaseAsync(
                serverId, "query_store_stats", "last_execution_time", "database_name", "dbA", floor, ct);
            Assert.True(ok);
            Assert.Equal(await RawAsync(rig.Connection, serverId, "dbA", floor, ct), (value, witness));
            Assert.Equal(0, rig.Runner.QueryStoreWatermarkSeedReader.PreCoverageReads);
            Assert.Equal(0, rig.Runner.QueryStoreWatermarkSeedReader.Fallbacks);

            /* A store without the side tables reads raw (SQLSTATE 42P01), not a failed seed. */
            await ExecAsync(rig.Connection, "DROP TABLE collect.query_store_watermarks_coverage", ct);
            var (v2, w2, ok2) = await rig.Runner.ReadLastCollectedTimeAndWitnessForDatabaseAsync(
                serverId, "query_store_stats", "last_execution_time", "database_name", "dbA", floor, ct);
            Assert.True(ok2);
            Assert.Equal((value, witness), (v2, w2));
            Assert.Equal(1, rig.Runner.QueryStoreWatermarkSeedReader.Fallbacks);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static long TopBuffers(string plan)
    {
        var line = plan.Split('\n').First(l => l.Contains("Buffers:", StringComparison.Ordinal));
        long total = 0;
        foreach (Match m in Regex.Matches(line, @"(hit|read)=(\d+)"))
        {
            total += long.Parse(m.Groups[2].Value, CultureInfo.InvariantCulture);
        }

        return total;
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var text = new StringBuilder();
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF) " + sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            text.AppendLine(reader.GetString(0));
        }

        return text.ToString();
    }

    private static string Literal(string sql, int serverId, string database, DateTime floor, long? queryId = null, long? planId = null, DateTime? stamp = null, DateTime? value = null)
    {
        string Iso(DateTime d) => "'" + d.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "'::timestamp";
        sql = sql.Replace("$1", serverId.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal)
            .Replace("$2", "'" + database + "'", StringComparison.Ordinal)
            .Replace("$3", queryId?.ToString(CultureInfo.InvariantCulture) ?? Iso(floor), StringComparison.Ordinal);
        if (planId is not null)
        {
            sql = sql.Replace("$4", planId.Value.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal)
                .Replace("$5", Iso(stamp!.Value), StringComparison.Ordinal)
                .Replace("$6", Iso(value!.Value), StringComparison.Ordinal);
        }

        return sql;
    }

    /// <summary>
    /// The EXPLAIN pin. One realistic pair (30,000 raw rows over 36 collection times, 36 side rows) after coverage: the side
    /// statement reads the side table only, the verification is one raw index probe on
    /// <c>idx_query_store_stats_server_db_query_plan_time</c> with no sequential or bitmap scan, and together they touch a small
    /// fraction of the buffers the raw read does.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AfterCoverage_TheReadTouchesTheSideTableAndOneRawIndexProbe_NoRawRangeScan(bool hypertable)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var serverId = hypertable ? -568462 : -568461;

        await using var rig = await Rig.OpenAsync(serverId, "qswm-5684-r6" + (hypertable ? "h" : "p"), ct);
        if (hypertable)
        {
            /* #5707: the production shape. query_store_stats a TimescaleDB hypertable with a COMPRESSED chunk (an old batch), so the
               plan names chunk indexes (_hyper_N_M_chunk_idx_...) and the read has to exclude the compressed chunk. */
            Assert.SkipWhen(!await LiveTimescaleProbe.TryEnableAsync(rig.Scratch.ConnectionString, ct), "TimescaleDB is not available on this server.");
            await TimescaleSupport.ConvertToHypertablesAsync(rig.Connection, null, ct);
            await ExecAsync(rig.Connection, TimescaleSupport.EnableCompressionSql("collect.query_store_stats"), ct);
            await rig.WriteAsync(T0.AddDays(-30), [Row("dbBig", -1, -2, T0.AddDays(-30)), Row("dbBig", -3, -4, T0.AddDays(-30))]);
            await ExecAsync(rig.Connection, "SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('collect.query_store_stats', older_than => " + Stamp(T0.AddDays(-10)) + ") AS c", ct);
            Assert.True(
                await CountAsync(rig.Connection, "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'query_store_stats' AND is_compressed", ct) > 0,
                "the old chunk must be compressed");
        }

        var bodySucceeded = false;
        try
        {
            var random = new Random(5685);
            long id = 1;
            DateTime maxStamp = default;
            DateTime maxValue = default;
            for (var batch = 0; batch < 36; batch++)
            {
                var stamp = T0.AddMinutes(batch * 5);
                var rows = new List<QueryStoreCollector.Row>();
                for (var n = 0; n < 834; n++)
                {
                    var last = stamp.AddSeconds(-random.Next(0, 300));
                    if (last > maxValue || (last == maxValue && stamp > maxStamp))
                    {
                        maxValue = last;
                        maxStamp = stamp;
                    }

                    rows.Add(Row("dbBig", id, id + 500000, last));
                    id++;
                }

                await rig.WriteAsync(stamp, rows);
            }

            await SetCoverageAsync(rig.Connection, T0.AddMinutes(-60), ct);
            /* The tuning pass owns the verification's index (PgTableTuning), applied on every production store. */
            await PgTableTuning.ApplyAsync(rig.Connection, null, ct);
            await ExecAsync(rig.Connection, "ANALYZE collect.query_store_stats", ct);
            await ExecAsync(rig.Connection, "ANALYZE collect.query_store_watermarks", ct);
            Assert.True(await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_stats WHERE database_name = 'dbBig'", ct) >= 30000);
            Assert.Equal(hypertable ? 37L : 36L, await CountAsync(rig.Connection, "SELECT count(*) FROM collect.query_store_watermarks WHERE database_name = 'dbBig'", ct));

            var floor = T0.AddMinutes(-30);
            var rawPlan = await ExplainAsync(rig.Connection, Literal(RawSql, serverId, "dbBig", floor), ct);
            var sidePlan = await ExplainAsync(rig.Connection, Literal(QueryStoreWatermarkReader.SideSql, serverId, "dbBig", floor), ct);

            /* The side row the read picks, to point-probe exactly as the product does. */
            await using var pick = new NpgsqlCommand(
                "SELECT max_query_id, max_plan_id, collection_time, batch_max FROM collect.query_store_watermarks WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture)
                + " AND database_name = 'dbBig' AND batch_max IS NOT NULL ORDER BY batch_max DESC, collection_time DESC LIMIT 1", rig.Connection);
            long queryId, planId;
            DateTime sideStamp, sideValue;
            await using (var r = await pick.ExecuteReaderAsync(ct))
            {
                Assert.True(await r.ReadAsync(ct));
                queryId = r.GetInt64(0);
                planId = r.GetInt64(1);
                sideStamp = r.GetDateTime(2);
                sideValue = r.GetDateTime(3);
            }

            Assert.Equal((maxValue, maxStamp), (sideValue, sideStamp));
            var verifyPlan = await ExplainAsync(
                rig.Connection, Literal(QueryStoreWatermarkReader.VerifySql, serverId, "dbBig", floor, queryId, planId, sideStamp, sideValue), ct);

            Assert.DoesNotContain("query_store_stats", sidePlan, StringComparison.Ordinal);
            Assert.Contains("query_store_watermarks", sidePlan, StringComparison.Ordinal);
            /* The verify read uses the (server, database, query, plan, time) index. A hypertable chunk's copy is named
               _hyper_N_M_chunk_ + the name, cut to PostgreSQL's 63 bytes, so the visible tail of the name shrinks by one character for
               each digit the chunk ids gain (#5707). The name is captured from the plan and checked against the cut built from its own prefix. */
            Assert.DoesNotContain("Seq Scan", verifyPlan, StringComparison.Ordinal);
            Assert.DoesNotContain("Bitmap", verifyPlan, StringComparison.Ordinal);
            var scans = Regex.Matches(verifyPlan, @"Index (?:Only )?Scan using (?<name>\S+)");
            Assert.Single(scans);
            var scannedIndex = scans[0].Groups["name"].Value;
            Assert.True(IsVerifyIndexName(scannedIndex), scannedIndex + "\n" + verifyPlan);
            Assert.Matches(@"rows=1\b", verifyPlan);

            var rawBuffers = TopBuffers(rawPlan);
            var afterBuffers = TopBuffers(sidePlan) + TopBuffers(verifyPlan);
            Assert.True(afterBuffers * 5 < rawBuffers, $"side+verify {afterBuffers} buffers vs raw {rawBuffers}\n--raw--\n{rawPlan}\n--side--\n{sidePlan}\n--verify--\n{verifyPlan}");

            var reader = new QueryStoreWatermarkReader(null);
            Assert.Equal((maxValue, maxStamp), await reader.ReadAsync(rig.Connection, serverId, "dbBig", floor, RawSql, 60, ct));
            Assert.Equal(await RawAsync(rig.Connection, serverId, "dbBig", floor, ct), (maxValue, maxStamp));
            Assert.Equal(0, reader.PreCoverageReads);

            /* The empty-side read (#5707): a database with no side row above the floor runs the full raw read. For an INACTIVE
               database that must stay cheap: an index range read of that pair (an index condition on server_id and database_name),
               no sequential or bitmap scan, and a small fraction of the busy pair's buffers. */
            var idlePlan = await ExplainAsync(rig.Connection, Literal(RawSql, serverId, "dbIdle", floor), ct);
            Assert.Matches(@"Index (Only )?Scan (Backward )?using \S*idx_query_store_stats_\S+", idlePlan);
            Assert.DoesNotContain("Seq Scan", idlePlan, StringComparison.Ordinal);
            Assert.DoesNotContain("Bitmap", idlePlan, StringComparison.Ordinal);
            Assert.True(idlePlan.Contains("database_name = 'dbIdle'", StringComparison.Ordinal), idlePlan);
            Assert.Matches(@"rows=0\b", idlePlan);
            Assert.True(TopBuffers(idlePlan) * 20 < rawBuffers, $"idle raw read {TopBuffers(idlePlan)} buffers vs busy raw {rawBuffers}\n{idlePlan}");
            Assert.Equal((null, null), await reader.ReadAsync(rig.Connection, serverId, "dbIdle", floor, RawSql, 60, ct));
            Assert.Equal(1, reader.EmptySideReads);
            Assert.Equal(0, reader.Fallbacks);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
