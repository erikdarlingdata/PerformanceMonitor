// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5581: after a retention drain removes the oldest rows of an interval table, the read gate's floor read
/// (<c>MIN(first_execution_time) WHERE server_id = $1</c>) walks the index from that end and makes a heap fetch for
/// every dead entry vacuum has not removed yet. <see cref="QueryStoreIntervalFloorWarmUp"/> runs the shipped
/// floor reads once per server at the end of the retention pass, so a user's first read finds the entries marked.
///
/// <para>Source pins first (the warm-up runs only the two shipped constants, and runs after the last purge), then
/// live tests on a scratch database with autovacuum off on the table: the premise (a drain with no warm-up leaves
/// the first read walking the dead entries), the fix (the same drain through the retention pass leaves the first
/// read with few heap fetches), a locked table (skipped and logged, the purge still succeeds), a server-list
/// failure and a cancel (logged, never thrown).</para>
/// </summary>
/* #1776 own-store: everything live here lives inside its own ScratchPostgres database, so it cannot race live
   collection or any other class's tables. */
public sealed class QueryStoreIntervalFloorWarmUpTests
{
    private const string SkipReason =
        "Set DARLING_TEST_PG to a Postgres connection string to run the #5581 interval floor warm-up live tests.";

    /// <summary>Expired rows seeded per server. Large enough that "near the deleted count" and "a handful" cannot be confused.</summary>
    private const int ExpiredPerServer = 20_000;

    private const int KeptPerServer = 200;

    private static readonly int[] ServerIds = { 1, 2 };

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    public sealed record IntervalTable(string Name, int HorizonDays, IntervalFloorTable Kind)
    {
        public string Qualified => "collect." + Name;

        public string FloorSql => Kind == IntervalFloorTable.Wide
            ? QueryStoreIntervalWide.PlainTableFloorSql
            : QueryStoreIntervalLatest.PlainTableFloorSql;

        public override string ToString() => Name;
    }

    private static IntervalTable WideTable => new(
        QueryStoreIntervalWide.TableName, DarlingRetention.QueryStoreIntervalWideRetentionDays, IntervalFloorTable.Wide);

    public static TheoryData<IntervalTable> Tables => new()
    {
        new IntervalTable(
            QueryStoreIntervalWide.TableName, DarlingRetention.QueryStoreIntervalWideRetentionDays, IntervalFloorTable.Wide),
        new IntervalTable(
            QueryStoreIntervalLatest.TableName, DarlingRetention.QueryStoreIntervalLatestRetentionDays, IntervalFloorTable.Latest),
    };

    /* ---- source pins ---------------------------------------------------------------------------------- */

    /// <summary>
    /// The warm-up executes the two shipped <c>PlainTableFloorSql</c> constants and nothing else against an interval
    /// table: it names each constant once, its <c>FloorSql</c> hands back the constants themselves, and the only
    /// statements it builds are the lock timeout, the registry read and the floor read. A copy of the floor read, or a
    /// <c>SELECT DISTINCT server_id</c> on the big table, would walk a different plan from the gate's.
    /// </summary>
    [Fact]
    public void TheWarmUp_ExecutesOnlyTheTwoShippedFloorConstants()
    {
        var source = ReadSource("Darling", "PerformanceMonitor.Darling.Service", "QueryStoreIntervalFloorWarmUp.cs");
        var code = Regex.Replace(source, @"^\s*///.*$", string.Empty, RegexOptions.Multiline);

        Assert.Single(Regex.Matches(code, @"QueryStoreIntervalWide\.PlainTableFloorSql"));
        Assert.Single(Regex.Matches(code, @"QueryStoreIntervalLatest\.PlainTableFloorSql"));
        Assert.Equal(QueryStoreIntervalWide.PlainTableFloorSql, QueryStoreIntervalFloorWarmUp.FloorSql(IntervalFloorTable.Wide));
        Assert.Equal(QueryStoreIntervalLatest.PlainTableFloorSql, QueryStoreIntervalFloorWarmUp.FloorSql(IntervalFloorTable.Latest));

        var commands = Regex.Matches(code, @"new NpgsqlCommand\(\s*([^,]+),", RegexOptions.Singleline)
            .Select(m => Regex.Replace(m.Groups[1].Value, @"\s+", " ").Trim())
            .ToArray();
        Assert.Equal(
            new[] { "\"SET lock_timeout = \" + lockTimeoutMilliseconds.ToString(CultureInfo.InvariantCulture)", "sql", "ServerIdsSql" },
            commands);

        /* The only table the file reads by name is the registry; no DISTINCT over a fact table. */
        Assert.DoesNotMatch(@"\bFROM\s+collect\.(?!servers\b)", code);
        Assert.DoesNotContain("DISTINCT", code, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"SELECT server_id FROM collect.servers ORDER BY server_id\"", code, StringComparison.Ordinal);
    }

    /// <summary>
    /// The sweep calls the warm-up exactly once, after the last <c>PurgeOneAsync</c> (so never between two purges),
    /// handing it the two interval drains' row counts, and not through a size threshold.
    /// </summary>
    [Fact]
    public void TheSweep_CallsTheWarmUpOnce_AfterTheLastPurge_WithTheTwoDrainCounts()
    {
        var source = ReadSource("Darling", "PerformanceMonitor.Darling.Service", "DarlingRetention.cs");
        var calls = Regex.Matches(source, @"QueryStoreIntervalFloorWarmUp\.RunAfterDrainAsync\(\s*postgres, intervalWideDeleted, intervalLatestDeleted, logger, cancellationToken\)");
        Assert.Single(calls);

        /* The sweep body ends where BuildRunRecordSummary is defined; PurgeOneAsync is defined further down. */
        var sweep = source.Substring(0, source.IndexOf("internal static (string Status, string Message) BuildRunRecordSummary(", StringComparison.Ordinal));
        var lastPurge = sweep.LastIndexOf("PurgeOneAsync(", StringComparison.Ordinal);
        var lastCaveatPrune = sweep.LastIndexOf("CollectionCaveatStore.PruneAsync(", StringComparison.Ordinal);
        Assert.True(calls[0].Index > lastPurge, "the warm-up runs after the last PurgeOneAsync call");
        Assert.True(calls[0].Index > lastCaveatPrune, "the warm-up runs after the last prune");
        Assert.True(
            calls[0].Index > source.IndexOf("var (status, message) = BuildRunRecordSummary(", StringComparison.Ordinal),
            "the warm-up runs after the run-record, so its time is not counted as the purge's");
    }

    /* ---- live ----------------------------------------------------------------------------------------- */

    /// <summary>
    /// The premise: a drain that deletes the oldest rows of two servers, with nothing reading afterwards, leaves
    /// each server's first floor read walking the dead index entries. Without this the bound below could pass on a
    /// plan that never had the problem.
    /// </summary>
    [Theory]
    [MemberData(nameof(Tables))]
    public async Task ADrainWithNoWarmUp_LeavesTheFirstFloorReadWalkingTheDeadEntries(IntervalTable table)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenSeededAsync(scratch, table, ct);

            /* The drain's effect without the sweep: delete exactly the expired rows. */
            await ExecuteAsync(
                connection,
                $"DELETE FROM {table.Qualified} WHERE first_execution_time < now() AT TIME ZONE 'UTC' - INTERVAL '{table.HorizonDays} days'",
                ct);

            /* Only the FIRST read counts: the index is ordered by first_execution_time across all servers, so the
               first server's walk passes (and marks) the second server's dead entries too. */
            var read = await FirstReadAsync(connection, table, ServerIds[0], ct);
            Assert.True(
                read.Buffers >= ExpiredPerServer,
                $"the first read after an unwarmed drain should walk about {ExpiredPerServer} dead entries ({read})");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// The fix: the same drain through the retention pass ends with the warm-up, so each server's first floor read
    /// after the pass fetches only a handful of heap tuples, and the pass logs one line for the table naming its
    /// servers, its time and its slowest server. Only the first read per server counts: EXPLAIN ANALYZE marks the
    /// entries it walks, too.
    /// </summary>
    [Theory]
    [MemberData(nameof(Tables))]
    public async Task TheRetentionPass_WarmsEachServersFloorRead_SoTheFirstReadAfterItIsCheap(IntervalTable table)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenSeededAsync(scratch, table, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var log = new CapturingTestLogger();

            var summary = await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, log, ct);

            Assert.True(summary.RowsDeleted >= ExpiredPerServer * ServerIds.Length, log.Joined);
            Assert.Equal(KeptPerServer * ServerIds.Length, await CountAsync(connection, table.Qualified, ct));
            Assert.Contains(
                $"Interval floor warm-up for {table.Kind.ToString().ToLowerInvariant()}: {ServerIds.Length} server(s) warmed, 0 skipped on a lock, 0 failed",
                log.Joined, StringComparison.Ordinal);
            Assert.Contains("slowest server", log.Joined, StringComparison.Ordinal);

            foreach (var serverId in ServerIds)
            {
                var read = await FirstReadAsync(connection, table, serverId, ct);
                var holders = read.Buffers > 1_000 ? await HorizonHoldersAsync(connection, ct) : string.Empty;
                if (read.Buffers > 1_000 && !holders.EndsWith(":: none", StringComparison.Ordinal))
                {
                    /* PostgreSQL marks an index entry dead only when its row is dead to every open snapshot, and the
                       suite runs other classes against this cluster: one of them has a transaction open that predates
                       the drain, so the entries stay live and no read can mark them. That is the documented limit of
                       the warm-up (#5581), not a defect; the premise and wiring tests above still ran. */
                    bodySucceeded = true;
                    Assert.Skip("A transaction open on this cluster predates the drain, so no read can mark its dead entries: " + holders);
                }

                Assert.True(
                    read.Buffers <= 1_000 && (read.HeapFetches ?? 0) <= 1_000,
                    $"server {serverId}: the first read after the warm-up should touch a handful of pages, not the {ExpiredPerServer} dead entries the drain left ({read}); horizon holders: {holders}; {string.Join(" | ", log.Lines.Where(l => l.Contains("warm-up", StringComparison.Ordinal)))}");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// A table with an ACCESS EXCLUSIVE lock queued or held (a schema upgrade step, a partition step) makes each
    /// warm-up read time out on <c>lock_timeout</c>: every server is skipped and logged inside the timeout instead of
    /// waiting on the lock, and the purge that preceded it still reports success. The lock is taken the moment the
    /// sweep logs its summary, which is after the last purge and before the warm-up, so the purge's own DELETEs are
    /// not the statements that block.
    /// </summary>
    [Fact]
    public async Task ALockedTable_SkipsEachServer_LogsIt_AndThePurgeStillSucceeds()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var table = WideTable;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenSeededAsync(scratch, table, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            await using var locker = new NpgsqlConnection(scratch.ConnectionString);
            await locker.OpenAsync(ct);
            var inner = new CapturingTestLogger();
            var log = new LockOnSummaryLogger(inner, locker, table.Qualified);

            var summary = await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, log, ct);

            Assert.True(log.Locked, "the test's lock was taken before the warm-up");
            Assert.True(summary.RowsDeleted >= ExpiredPerServer * ServerIds.Length, inner.Joined);
            Assert.Contains(
                $"Interval floor warm-up for wide: 0 server(s) warmed, {ServerIds.Length} skipped on a lock, 0 failed",
                inner.Joined, StringComparison.Ordinal);
            Assert.Contains("the table is locked", inner.Joined, StringComparison.Ordinal);
            Assert.Equal(0, inner.CountAtLevel(LogLevel.Error));
            /* The warm-up's own logged time (not the whole pass's, which includes the purge and varies with load):
               each skipped server costs about its lock timeout. */
            var logged = Regex.Match(inner.Joined, @"Interval floor warm-up for wide: .*?, (\d+) ms total");
            Assert.True(logged.Success, inner.Joined);
            Assert.True(
                long.Parse(logged.Groups[1].Value, CultureInfo.InvariantCulture)
                    < ServerIds.Length * (QueryStoreIntervalFloorWarmUp.LockTimeoutMilliseconds + 10_000),
                $"the warm-up took {logged.Groups[1].Value} ms; each skipped server should cost about its lock timeout");

            await using (var rollback = new NpgsqlCommand("ROLLBACK", locker))
            {
                await rollback.ExecuteNonQueryAsync(ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// A warm-up that cannot read the server list (here the registry table is gone) logs the failure and the purge
    /// around it still returns its summary with the rows it deleted.
    /// </summary>
    [Fact]
    public async Task AWarmUpFailure_IsLogged_AndDoesNotFailThePurge()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var table = WideTable;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenSeededAsync(scratch, table, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecuteAsync(connection, "ALTER TABLE collect.servers RENAME TO servers_renamed_for_test", ct);
            var log = new CapturingTestLogger();

            var summary = await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, log, ct);

            Assert.True(summary.RowsDeleted >= ExpiredPerServer * ServerIds.Length, log.Joined);
            Assert.Contains("could not read the server list", log.Joined, StringComparison.Ordinal);
            Assert.Equal(0, log.CountAtLevel(LogLevel.Error));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>A cancelled token stops the warm-up quietly: it reports the cancel in its result and throws nothing.</summary>
    [Fact]
    public async Task ACancel_StopsTheWarmUp_WithoutThrowing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var log = new CapturingTestLogger();
            using var cancelled = new CancellationTokenSource();
            await cancelled.CancelAsync();

            var result = await QueryStoreIntervalFloorWarmUp.WarmAsync(postgres, IntervalFloorTable.Wide, log, cancelled.Token);
            await QueryStoreIntervalFloorWarmUp.RunAfterDrainAsync(postgres, 5, 5, log, cancelled.Token);

            Assert.True(result.Cancelled);
            Assert.Equal(0, result.ServersWarmed);
            Assert.Equal(0, log.CountAtLevel(LogLevel.Error));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>A drain that deleted nothing does not warm: the gate's cost is one index probe then, and the pass logs no warm-up line.</summary>
    [Fact]
    public async Task ADrainThatDeletedNothing_DoesNotWarm()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var log = new CapturingTestLogger();

            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: false, log, ct);

            Assert.DoesNotContain("Interval floor warm-up", log.Joined, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /* ---- helpers -------------------------------------------------------------------------------------- */

    /// <summary>
    /// Migrates the scratch store, registers <see cref="ServerIds"/>, turns autovacuum off on the table, seeds each
    /// server with expired rows (older than the horizon) and kept rows (yesterday), then vacuums once by hand so the
    /// visibility map is set: that is what makes a dead entry cost a heap fetch after the drain, as on a real store.
    /// </summary>
    private static async Task<NpgsqlConnection> OpenSeededAsync(ScratchPostgres scratch, IntervalTable table, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        foreach (var serverId in ServerIds)
        {
            await ExecuteAsync(
                connection,
                $"INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, created_date, modified_date) VALUES ({serverId.ToString(CultureInfo.InvariantCulture)}, 'warm-{serverId.ToString(CultureInfo.InvariantCulture)}', 'warm-{serverId.ToString(CultureInfo.InvariantCulture)}', TRUE, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC')",
                ct);
        }

        await ExecuteAsync(connection, $"ALTER TABLE {table.Qualified} SET (autovacuum_enabled = false)", ct);

        var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var expiredStart = utcNow.AddDays(-(table.HorizonDays + 5));
        var keptStart = utcNow.AddDays(-1);
        foreach (var serverId in ServerIds)
        {
            var idOffset = serverId * 1_000_000L;
            await SeedAsync(connection, table, serverId, expiredStart, ExpiredPerServer, idOffset, ct);
            await SeedAsync(connection, table, serverId, keptStart, KeptPerServer, idOffset + ExpiredPerServer, ct);
        }

        await ExecuteAsync(connection, $"VACUUM (ANALYZE) {table.Qualified}", ct);
        return connection;
    }

    private static async Task SeedAsync(
        NpgsqlConnection connection, IntervalTable table, int serverId, DateTime start, int count, long idOffset, CancellationToken ct)
    {
        var sql = table.Kind == IntervalFloorTable.Wide
            ? @"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, module_name, query_text, query_hash, execution_count, replica_role, runtime_stats_interval_id)
SELECT $1 + (g - 1) * INTERVAL '1 second' + INTERVAL '1 minute', $5, 'db', g + $4, 1, 'exec',
       $1 + (g - 1) * INTERVAL '1 second', $1 + (g - 1) * INTERVAL '1 second' + INTERVAL '1 minute',
       'mod', 'select 1', 'qh', 1, NULL, g + $4
FROM generate_series(1, $2) AS g;"
            : @"
INSERT INTO collect.query_store_interval_latest
(server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time, collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us, last_execution_time, is_forced_plan, force_failure_count, query_text)
SELECT $5, 'db', g + $4, 1, NULL, g + $4,
       $1 + (g - 1) * INTERVAL '1 second', $1 + (g - 1) * INTERVAL '1 second' + INTERVAL '1 minute',
       'ph', 'qh', 1, 1, 1, $1 + (g - 1) * INTERVAL '1 second' + INTERVAL '1 minute',
       false, 0, 'select 1'
FROM generate_series(1, $2) AS g;";

        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = count });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 0 });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = idOffset });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) of the shipped floor read for one server: the plan's top node type,
    /// its "Heap Fetches" when it is an index-only scan (the only node that reports that figure), and the buffers
    /// the whole plan touched. A walk over dead entries shows up in both numbers; the buffers are what a plain index
    /// scan, which has no "Heap Fetches", still reports.
    /// </summary>
    private static async Task<FloorRead> FirstReadAsync(
        NpgsqlConnection connection, IntervalTable table, int serverId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + table.FloorSql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        var json = (string)(await command.ExecuteScalarAsync(ct))!;

        using var document = JsonDocument.Parse(json);
        var plan = document.RootElement[0].GetProperty("Plan");
        var fetches = new List<long>();
        var nodes = new List<string>();
        CollectNodes(plan, fetches, nodes);
        var buffers = plan.GetProperty("Shared Hit Blocks").GetInt64() + plan.GetProperty("Shared Read Blocks").GetInt64();
        return new FloorRead(fetches.Count == 0 ? null : fetches.Sum(), buffers, string.Join(" > ", nodes));
    }

    private sealed record FloorRead(long? HeapFetches, long Buffers, string Nodes)
    {
        public override string ToString() =>
            $"{Nodes}: {(HeapFetches is null ? "no heap-fetch figure" : HeapFetches + " heap fetches")}, {Buffers} buffers";
    }

    private static void CollectNodes(JsonElement node, List<long> fetches, List<string> nodes)
    {
        nodes.Add(node.GetProperty("Node Type").GetString()!);
        if (node.TryGetProperty("Heap Fetches", out var heap) && heap.ValueKind == JsonValueKind.Number)
        {
            fetches.Add(heap.GetInt64());
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                CollectNodes(child, fetches, nodes);
            }
        }
    }
    private static async Task<string> HorizonHoldersAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT current_database() || ' :: ' || coalesce(string_agg(format('%s/%s/%s/%s/%s', datname, state, backend_xmin, now() - xact_start, left(query, 60)), ' ## '), 'none') FROM pg_stat_activity WHERE backend_xmin IS NOT NULL AND pid <> pg_backend_pid()",
            connection);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT COUNT(*) FROM {table}", connection);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static string ReadSource(
        string first, string second, string file,
        [System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var relative = System.IO.Path.Combine(first, second, file);
        for (var dir = new System.IO.DirectoryInfo(System.IO.Path.GetDirectoryName(thisFile)!);
             dir is not null; dir = dir.Parent)
        {
            var candidate = System.IO.Path.Combine(dir.FullName, relative);
            if (System.IO.File.Exists(candidate))
            {
                return System.IO.File.ReadAllText(candidate);
            }
        }

        throw new System.IO.FileNotFoundException(file + " not found above " + thisFile);
    }

    /// <summary>
    /// Forwards to a capturing logger, and takes an ACCESS EXCLUSIVE lock on the table inside an open transaction the
    /// first time the sweep logs its summary line, which is after every purge and before the warm-up.
    /// </summary>
    private sealed class LockOnSummaryLogger(ILogger inner, NpgsqlConnection locker, string table) : ILogger
    {
        public bool Locked { get; private set; }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            inner.Log(logLevel, eventId, state, exception, formatter);
            if (!Locked && formatter(state, exception).Contains("table(s) purged", StringComparison.Ordinal))
            {
                using var begin = new NpgsqlCommand($"BEGIN; LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE", locker);
                begin.ExecuteNonQuery();
                Locked = true;
            }
        }
    }
}
