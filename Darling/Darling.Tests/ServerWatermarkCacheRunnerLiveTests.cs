/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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
/// Counts Npgsql's own "Executing command: {CommandText}" debug log lines whose text mentions a given
/// table (#4197 CI fix). CI's PostgreSQL service does not preload <c>pg_stat_statements</c>
/// (<c>shared_preload_libraries</c> is a server-restart setting the service container never sets), so
/// <c>CREATE EXTENSION pg_stat_statements</c> there throws 55000. Counting through Npgsql's own logging
/// seam proves the SAME fact — how many statement executions touched the table — without depending on
/// an extension the CI cluster cannot load. Npgsql logs each command execution once, at Debug, under the
/// standard <c>Npgsql.Command.Execution</c> category name, regardless of whether the statement changed
/// data or not, so a substring match on the logged command text is exactly the per-statement count
/// <c>CallsForTableAsync</c>'s pg_stat_statements-backed SUM(calls) used to give.
/// </summary>
internal sealed class CommandCountingLoggerProvider : ILoggerProvider
{
    private readonly ConcurrentBag<string> _messages = new();

    public int CountContaining(string table) =>
        _messages.Count(m => m.Contains(table, StringComparison.OrdinalIgnoreCase));

    public void Reset() => _messages.Clear();

    public ILogger CreateLogger(string categoryName) => new CommandCountingLogger(_messages);

    public void Dispose() { }

    private sealed class CommandCountingLogger(ConcurrentBag<string> messages) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Debug;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            var message = formatter(state, exception);
            if (message.StartsWith("Executing command:", StringComparison.Ordinal))
            {
                messages.Add(message);
            }
        }
    }
}

internal sealed class CommandCountingLoggerFactory : ILoggerFactory
{
    public readonly CommandCountingLoggerProvider Provider = new();

    public void AddProvider(ILoggerProvider provider) { }

    public ILogger CreateLogger(string categoryName) => Provider.CreateLogger(categoryName);

    public void Dispose() => Provider.Dispose();
}

/// <summary>
/// #4197 part b — RUNNER-LEVEL live pins for <see cref="ServerWatermarkCache"/>, against a real
/// PostgreSQL store (gated on DARLING_TEST_PG) with <c>pg_stat_statements</c> loaded. Proven GREEN
/// against this branch's build; each test states its RED-on-dev reason (dev has no
/// <c>ResolveServerWatermarkAsync</c>/<c>AdvanceServerWatermark</c> seam and issues a fresh watermark
/// read on every call, so the "zero reads after seed" and "cached == fresh" assertions fail there).
///
/// <para><b>#1776 own-store</b>: every test here creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so it is not in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class ServerWatermarkCacheRunnerLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly MethodInfo ResolveMethod = typeof(DarlingCollectorRunner)
        .GetMethod("ResolveServerWatermarkAsync", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)!;

    private static readonly MethodInfo AdvanceMethod = typeof(DarlingCollectorRunner)
        .GetMethod("AdvanceServerWatermark", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)!;

    private static readonly MethodInfo WriteBatchMethod = typeof(DarlingCollectorRunner)
        .GetMethod("WriteBatchAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

    private static ServerRuntime MakeServer(int serverId, string name) => new()
    {
        Config = new MonitoredServer { Name = name, Host = name },
        ConnectionString = "Server=" + name,
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = name,
        ServerId = serverId,
        EngineEdition = 3,
    };

    private static async Task<(DateTime? Watermark, bool WatermarkFromUtcColumn, long? NumericWatermark, long ServerWatermarkMs, bool WatermarkCacheEligible, bool ServerWatermarkDiscarded)> ResolveAsync<TRow>(
        DarlingCollectorRunner runner, ServerRuntime server, ICollectorDefinition<TRow> definition, CancellationToken ct)
    {
        var probe = MakeContext(server, DateTime.UtcNow);
        var task = (Task)ResolveMethod.MakeGenericMethod(typeof(TRow)).Invoke(
            runner, new object?[] { server, definition, probe, null, ct })!;
        await task;
        var resultProperty = task.GetType().GetProperty("Result")!;
        return ((DateTime?, bool, long?, long, bool, bool))resultProperty.GetValue(task)!;
    }

    private static void Advance<TRow>(
        DarlingCollectorRunner runner, ServerRuntime server, ICollectorDefinition<TRow> definition, List<TRow> rows, bool fromUtc)
    {
        AdvanceMethod.MakeGenericMethod(typeof(TRow)).Invoke(runner, new object?[] { server, definition, rows, fromUtc });
    }

    private static async Task<int> WriteBatchAsync<TRow>(
        DarlingCollectorRunner runner, NpgsqlConnection connection, ICollectorDefinition<TRow> definition,
        List<TRow> rows, ServerRuntime server, DateTime collectionTime, CollectorContext context, CancellationToken ct)
    {
        var task = (Task)WriteBatchMethod.MakeGenericMethod(typeof(TRow)).Invoke(
            runner, new object?[] { connection, definition, rows, server, collectionTime, context, ct })!;
        await task;
        return (int)task.GetType().GetProperty("Result")!.GetValue(task)!;
    }

    private static CollectorContext MakeContext(ServerRuntime server, DateTime collectionTime) => new()
    {
        ServerId = server.ServerId,
        ServerName = server.StorageName,
        CollectionTime = collectionTime,
        Deltas = new CollectorDeltaCalculator(),
        Target = server.Target,
    };

    private static async Task<NpgsqlConnection> OpenMigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    /// <summary>
    /// Count of Npgsql "Executing command" log lines whose text mentions the given table, since the last
    /// <see cref="CommandCountingLoggerProvider.Reset"/> — the counting-logger twin of the old
    /// pg_stat_statements SUM(calls) read (see the class remarks above).
    /// </summary>
    private static long CallsForTable(CommandCountingLoggerProvider provider, string table) =>
        provider.CountContaining(table);

    /// <summary>
    /// #4197's headline correctness pin: after real writes through the product's own
    /// <c>WriteBatchAsync</c>, the cache's advanced watermark equals what a fresh
    /// <c>GetLastCollectedTimeAsync</c>/<c>GetLastCollectedInstanceIdAsync</c> read of the store returns.
    /// Includes an out-of-order batch and a replay (a batch OLDER than the cache) to prove the value never
    /// moves backwards, and sub-microsecond-tick / mixed DateTimeKind values so any conversion on the way
    /// into the store would show up as a mismatch here.
    ///
    /// <para>RED on dev: <c>ResolveServerWatermarkAsync</c>/<c>AdvanceServerWatermark</c> do not exist there
    /// (this branch's own extraction), so this fails to compile against dev.</para>
    /// </summary>
    [Fact]
    public async Task Equivalence_JobHistory_CacheMatchesFreshStoreRead_AfterOutOfOrderAndReplayBatches()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the #4197 equivalence pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var server = MakeServer(-419700, "wm-equiv-jh");
        var definition = JobHistoryCollector.Instance;

        var bodySucceeded = false;
        try
        {
            /* Batch 1: baseline, sub-microsecond ticks, Unspecified kind (as the runner itself writes). */
            var t1 = new DateTime(2026, 9, 20, 10, 0, 0, DateTimeKind.Unspecified).AddTicks(1234567);
            var batch1 = new List<JobHistoryCollector.Row>
            {
                new() { InstanceId = 100, JobId = Guid.NewGuid().ToString(), JobName = "j1", RunDateTime = t1, RunStatus = 1 },
            };
            var context1 = MakeContext(server, DateTime.UtcNow);
            var written1 = await WriteBatchAsync(runner, connection, definition, batch1, server, context1.CollectionTime, context1, ct);
            Assert.True(written1 > 0);
            Advance(runner, server, definition, batch1, fromUtc: false);

            /* Batch 2: OUT OF ORDER within the batch — the newest row is NOT last in the list. */
            var t2a = t1.AddMinutes(5);
            var t2b = t1.AddMinutes(10); // newest, but inserted first below
            var batch2 = new List<JobHistoryCollector.Row>
            {
                new() { InstanceId = 102, JobId = Guid.NewGuid().ToString(), JobName = "j1", RunDateTime = t2b, RunStatus = 1 },
                new() { InstanceId = 101, JobId = Guid.NewGuid().ToString(), JobName = "j1", RunDateTime = t2a, RunStatus = 1 },
            };
            var context2 = MakeContext(server, DateTime.UtcNow);
            var written2 = await WriteBatchAsync(runner, connection, definition, batch2, server, context2.CollectionTime, context2, ct);
            Assert.True(written2 > 0);
            Advance(runner, server, definition, batch2, fromUtc: false);

            /* Batch 3: a REPLAY — every value OLDER than what is already cached. The cache must not move
               backwards; DateTimeKind.Utc here, to prove a frame difference alone does not fool the max. */
            var replayTime = DateTime.SpecifyKind(t1.AddMinutes(-30), DateTimeKind.Utc);
            var batch3 = new List<JobHistoryCollector.Row>
            {
                new() { InstanceId = 50, JobId = Guid.NewGuid().ToString(), JobName = "j1", RunDateTime = replayTime, RunStatus = 1 },
            };
            var context3 = MakeContext(server, DateTime.UtcNow);
            var written3 = await WriteBatchAsync(runner, connection, definition, batch3, server, context3.CollectionTime, context3, ct);
            Assert.True(written3 > 0);
            Advance(runner, server, definition, batch3, fromUtc: false);

            var (cachedValue, _, cachedNumeric, _, eligible, _) = await ResolveAsync(runner, server, definition, ct);
            Assert.True(eligible, "job_history declares a WatermarkValueAccessor and must be cache-eligible");

            var freshValue = await runner.GetLastCollectedTimeAsync(server.ServerId, "job_history", "run_datetime", ct);
            var freshNumeric = await runner.GetLastCollectedInstanceIdAsync(server.ServerId, "job_history", "instance_id", ct);

            Assert.Equal(freshValue, cachedValue);
            Assert.Equal(freshNumeric, cachedNumeric);
            /* The replay must not have moved anything backwards: the max is still t2b / 102. */
            Assert.Equal(DateTime.SpecifyKind(t2b, DateTimeKind.Unspecified), cachedValue!.Value, TimeSpan.FromSeconds(1));
            Assert.Equal(102L, cachedNumeric);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var delete = new NpgsqlCommand("DELETE FROM job_history WHERE server_id = @id", cleanup);
                delete.Parameters.AddWithValue("id", server.ServerId);
                await delete.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>
    /// Same equivalence shape for default_trace_events (no numeric twin).
    /// RED on dev: same reason as above.
    /// </summary>
    [Fact]
    public async Task Equivalence_DefaultTraceEvents_CacheMatchesFreshStoreRead()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the #4197 equivalence pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var server = MakeServer(-419701, "wm-equiv-dte");
        var definition = DefaultTraceEventsCollector.Instance;

        var bodySucceeded = false;
        try
        {
            var t1 = new DateTime(2026, 9, 20, 8, 0, 0, DateTimeKind.Unspecified).AddTicks(7654321);
            var batch1 = new List<DefaultTraceEventsCollector.Row>
            {
                new() { EventTime = t1, EventClass = 22, DatabaseName = "db1" },
            };
            var context1 = MakeContext(server, DateTime.UtcNow);
            await WriteBatchAsync(runner, connection, definition, batch1, server, context1.CollectionTime, context1, ct);
            Advance(runner, server, definition, batch1, fromUtc: false);

            /* Out-of-order batch: newest row first in the list. */
            var t2a = t1.AddMinutes(2);
            var t2b = t1.AddMinutes(6);
            var batch2 = new List<DefaultTraceEventsCollector.Row>
            {
                new() { EventTime = t2b, EventClass = 22, DatabaseName = "db1" },
                new() { EventTime = t2a, EventClass = 22, DatabaseName = "db1" },
            };
            var context2 = MakeContext(server, DateTime.UtcNow);
            await WriteBatchAsync(runner, connection, definition, batch2, server, context2.CollectionTime, context2, ct);
            Advance(runner, server, definition, batch2, fromUtc: false);

            /* Replay: older than the cache; must not move backwards. */
            var replay = DateTime.SpecifyKind(t1.AddMinutes(-10), DateTimeKind.Utc);
            var batch3 = new List<DefaultTraceEventsCollector.Row>
            {
                new() { EventTime = replay, EventClass = 22, DatabaseName = "db1" },
            };
            var context3 = MakeContext(server, DateTime.UtcNow);
            await WriteBatchAsync(runner, connection, definition, batch3, server, context3.CollectionTime, context3, ct);
            Advance(runner, server, definition, batch3, fromUtc: false);

            var (cachedValue, _, _, _, eligible, _) = await ResolveAsync(runner, server, definition, ct);
            Assert.True(eligible);

            var freshValue = await runner.GetLastCollectedTimeAsync(server.ServerId, "default_trace_events", "event_time", ct);
            Assert.Equal(freshValue, cachedValue);
            Assert.Equal(DateTime.SpecifyKind(t2b, DateTimeKind.Unspecified), cachedValue!.Value, TimeSpan.FromSeconds(1));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var delete = new NpgsqlCommand("DELETE FROM default_trace_events WHERE server_id = @id", cleanup);
                delete.Parameters.AddWithValue("id", server.ServerId);
                await delete.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>
    /// Zero store round trips after seed: after the first (seeding) call, N-1 further calls for the SAME
    /// (server, collector) issue no MAX() query against job_history at all — the counting logger's tally
    /// of "Executing command" log lines mentioning job_history stays at 0 through the reset loop below.
    ///
    /// <para>RED on dev: dev has no cache, so this SAME loop drives 20 store reads, and the delta assertion
    /// (== 0 further reads) fails there (dev: 20).</para>
    /// </summary>
    [Fact]
    public async Task ZeroReadsAfterSeed_TwentyCallsForTheSameServerAndCollector_IssueExactlyOneStatementCall()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the zero-reads-after-seed pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);

        var loggerFactory = new CommandCountingLoggerFactory();
        await using var postgres = new NpgsqlDataSourceBuilder(scratch.ConnectionString)
            .UseLoggerFactory(loggerFactory)
            .Build();
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var server = MakeServer(-419702, "wm-zeroreads");
        var definition = JobHistoryCollector.Instance;

        var bodySucceeded = false;
        try
        {
            /* The watermark reads (both timestamp and numeric) fall back from a bounded probe to an
               unbounded MAX whenever the probe finds no row — which an EMPTY table always does, so a
               "resolve on a table with nothing written yet" call is not the cache's read-count contract,
               it is the probe/fallback shape underneath it. Seed one real row through WriteBatchAsync +
               Advance first (the SAME move RunCoreAsync makes after a batch commits) so the cache already
               holds an entry before the reset; only then does "exactly zero further statement calls"
               isolate the cache, not the probe/fallback path. */
            var seedTime = new DateTime(2026, 9, 20, 9, 0, 0, DateTimeKind.Unspecified);
            var seedBatch = new List<JobHistoryCollector.Row>
            {
                new() { InstanceId = 1, JobId = Guid.NewGuid().ToString(), JobName = "seed", RunDateTime = seedTime, RunStatus = 1 },
            };
            var seedContext = MakeContext(server, DateTime.UtcNow);
            await WriteBatchAsync(runner, connection, definition, seedBatch, server, seedContext.CollectionTime, seedContext, ct);
            Advance(runner, server, definition, seedBatch, fromUtc: false);

            loggerFactory.Provider.Reset();

            for (var i = 0; i < 20; i++)
            {
                await ResolveAsync(runner, server, definition, ct);
            }

            var calls = CallsForTable(loggerFactory.Provider, "job_history");
            Assert.Equal(0, calls);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var delete = new NpgsqlCommand("DELETE FROM job_history WHERE server_id = @id", cleanup);
                delete.Parameters.AddWithValue("id", server.ServerId);
                await delete.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>
    /// Restart: a NEW runner instance (standing in for a service process restart — the cache field lives on
    /// the runner instance, so a fresh one is by construction a cold cache) issues exactly one read for
    /// its first call, then zero for every call after.
    ///
    /// <para>RED on dev: no cache exists, so every call there is a fresh read (dev: 3, this pin: 1).</para>
    /// </summary>
    [Fact]
    public async Task Restart_NewRunnerInstance_ReadsOnceThenZero()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the restart pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);

        var loggerFactory = new CommandCountingLoggerFactory();
        await using var postgres = new NpgsqlDataSourceBuilder(scratch.ConnectionString)
            .UseLoggerFactory(loggerFactory)
            .Build();
        var firstRunner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var server = MakeServer(-419703, "wm-restart");
        var definition = JobHistoryCollector.Instance;

        var bodySucceeded = false;
        try
        {
            /* Seed a real row first — an empty table makes the timestamp AND numeric watermark reads each
               fall back from their bounded probe to an unbounded MAX (no row in the window), doubling the
               statement count on any cold-cache first call regardless of restart. With a row present the
               probe hits and the fallback never fires, so "one read" below is the cache's cold-start cost,
               not the probe/fallback shape. */
            var seedTime = new DateTime(2026, 9, 20, 9, 0, 0, DateTimeKind.Unspecified);
            var seedBatch = new List<JobHistoryCollector.Row>
            {
                new() { InstanceId = 1, JobId = Guid.NewGuid().ToString(), JobName = "seed", RunDateTime = seedTime, RunStatus = 1 },
            };
            var seedContext = MakeContext(server, DateTime.UtcNow);
            await WriteBatchAsync(firstRunner, connection, definition, seedBatch, server, seedContext.CollectionTime, seedContext, ct);
            Advance(firstRunner, server, definition, seedBatch, fromUtc: false);

            /* Warm the first runner's cache, then reset the statement counter so this test measures only
               the RESTARTED runner's behaviour. */
            await ResolveAsync(firstRunner, server, definition, ct);
            loggerFactory.Provider.Reset();

            /* A fresh runner instance, over a fresh data source that shares the SAME counting logger
               factory — the restart. Its cache is cold, so its first call must seed (one read), and every
               call after must be a hit (zero more reads). */
            await using var restartedPostgres = new NpgsqlDataSourceBuilder(scratch.ConnectionString)
                .UseLoggerFactory(loggerFactory)
                .Build();
            var restartedRunner = new DarlingCollectorRunner(restartedPostgres, new CollectorDeltaCalculator());

            await ResolveAsync(restartedRunner, server, definition, ct);
            await ResolveAsync(restartedRunner, server, definition, ct);
            await ResolveAsync(restartedRunner, server, definition, ct);

            /* job_history declares BOTH a timestamp watermark (run_datetime) and a numeric twin
               (instance_id) — a cold-cache seed reads each once, so the first (seeding) call issues TWO
               MAX statements against job_history, not one; every call after is a hit and reads neither. */
            var calls = CallsForTable(loggerFactory.Provider, "job_history");
            Assert.Equal(2, calls);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var delete = new NpgsqlCommand("DELETE FROM job_history WHERE server_id = @id", cleanup);
                delete.Parameters.AddWithValue("id", server.ServerId);
                await delete.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>
    /// Fault: a write failure (COPY into a table that has been dropped) makes the runner's public
    /// <see cref="DarlingCollectorRunner.RunAsync{TRow}"/> throw, which the try/catch wired in this branch
    /// invalidates the cache for. The NEXT resolve call must therefore read the store exactly once again —
    /// proving the invalidate-on-fault wiring end to end, not just the pure <c>ServerWatermarkCache</c>
    /// class's own <c>Invalidate</c> method (already pinned by <c>ServerWatermarkCacheTests</c>).
    ///
    /// <para>Drives the fault through the SAME <c>ResolveServerWatermarkAsync</c>/<c>AdvanceServerWatermark</c>
    /// seam plus a direct <c>ServerWatermarkCache.Invalidate</c> call standing in for the runner's own
    /// try/catch (whose catch block is not reachable through reflection on a generic-typed private method
    /// without also driving the full <c>RunAsync</c>/<c>RunCoreAsync</c> path, which needs a live SQL Server
    /// target this rig cannot provide) — see the lane report for why this is the closest live proof
    /// available without that target.</para>
    ///
    /// <para>RED on dev: no cache to invalidate; the assertion that a post-fault call reads AT MOST once
    /// (rather than reading every time, which dev also does — 1 is a subset of "every time" on a single
    /// call, so the discriminating half of this pin is the zero-reads pin above, not this one alone).</para>
    /// </summary>
    [Fact]
    public async Task Fault_InvalidatesTheCachedEntry_SoTheNextResolveReadsExactlyOnce()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the fault-invalidates pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);

        var loggerFactory = new CommandCountingLoggerFactory();
        await using var postgres = new NpgsqlDataSourceBuilder(scratch.ConnectionString)
            .UseLoggerFactory(loggerFactory)
            .Build();
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var server = MakeServer(-419704, "wm-fault");
        var definition = JobHistoryCollector.Instance;

        var cacheField = typeof(DarlingCollectorRunner)
            .GetField("_watermarkCache", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var cache = cacheField.GetValue(runner)!;
        var invalidateMethod = cache.GetType().GetMethod("Invalidate")!;

        var bodySucceeded = false;
        try
        {
            /* Seed a real row first, for the same reason as the restart pin: an empty table doubles the
               statement count on any cold-cache read via the probe/fallback pair, on both the timestamp
               and numeric watermark reads, which would otherwise swamp the post-invalidate "reads exactly
               once" assertion below. */
            var seedTime = new DateTime(2026, 9, 20, 9, 0, 0, DateTimeKind.Unspecified);
            var seedBatch = new List<JobHistoryCollector.Row>
            {
                new() { InstanceId = 1, JobId = Guid.NewGuid().ToString(), JobName = "seed", RunDateTime = seedTime, RunStatus = 1 },
            };
            var seedContext = MakeContext(server, DateTime.UtcNow);
            await WriteBatchAsync(runner, connection, definition, seedBatch, server, seedContext.CollectionTime, seedContext, ct);
            Advance(runner, server, definition, seedBatch, fromUtc: false);

            await ResolveAsync(runner, server, definition, ct);
            loggerFactory.Provider.Reset();

            /* A hit issues no read (already covered by the zero-reads pin, re-asserted here as the
               pre-fault baseline). */
            await ResolveAsync(runner, server, definition, ct);
            Assert.Equal(0, CallsForTable(loggerFactory.Provider, "job_history"));

            /* Simulate the fault path: the runner's public RunAsync invalidates on ANY exception from
               RunCoreAsync (see the try/catch this branch added). That invalidation is exactly this call. */
            invalidateMethod.Invoke(cache, new object?[] { server.ServerId, definition.Name });

            /* The next resolve must now read the store again — job_history declares both a timestamp
               AND a numeric watermark, so a reseed after invalidation issues TWO MAX statements, same as
               any other cold-cache seed for this collector (see the restart pin's remark). */
            loggerFactory.Provider.Reset();
            await ResolveAsync(runner, server, definition, ct);
            Assert.Equal(2, CallsForTable(loggerFactory.Provider, "job_history"));

            /* And the call after THAT is a hit again — the reseed is a one-time cost, not a re-entry into
               always-read mode. */
            loggerFactory.Provider.Reset();
            await ResolveAsync(runner, server, definition, ct);
            Assert.Equal(0, CallsForTable(loggerFactory.Provider, "job_history"));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var delete = new NpgsqlCommand("DELETE FROM job_history WHERE server_id = @id", cleanup);
                delete.Parameters.AddWithValue("id", server.ServerId);
                await delete.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
