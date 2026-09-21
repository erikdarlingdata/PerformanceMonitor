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
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The single-flight memo over <c>get_fleet_overview</c>'s 7-day collection-health rollup (#3735). The
/// production photo: one caller raced three overview calls with three <c>hours_back</c> values, and because
/// that rollup does not depend on the window, the store ran three identical copies of one 7-day aggregate
/// concurrently inside the collectors' flush band and the slowest crossed the role's 15 s
/// <c>statement_timeout</c>. These pins hold the four promises the fix makes — one scan however many callers
/// race, a minute of memory, a failed scan never memoized, and the age published beside the payload — and the
/// one it makes about cancellation, which is the riskiest line in the change: a caller's token releases that
/// caller and nobody else.
///
/// <para>The memo is exercised through its <c>Func</c> seam with a counting scan rather than through a mocked
/// Npgsql, and the seam is the SAME object the reader routes through, so the count here IS the number of
/// statements the store would have seen. The gated live test at the bottom closes the loop against a real
/// store with three racing <c>GetFleetOverviewAsync</c> calls.</para>
/// </summary>
public sealed class FleetCollectionHealthMemoTests
{
    private static readonly DateTime T0 = new(2026, 9, 19, 13, 36, 3, DateTimeKind.Utc);

    /// <summary>A scan that counts how often it was invoked, records the token it was handed, and completes
    /// only when the test says so — so N callers can be parked on it before it produces anything, which is
    /// the only way to prove they shared it rather than merely ran in sequence.</summary>
    private sealed class CountingScan
    {
        private int _calls;
        public TaskCompletionSource<Dictionary<int, DarlingFleetReader.CollectorCounts>> Gate { get; private set; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public List<CancellationToken> TokensSeen { get; } = new();
        public int Calls => Volatile.Read(ref _calls);

        public Task<Dictionary<int, DarlingFleetReader.CollectorCounts>> Run(DateTime now, CancellationToken token)
        {
            Interlocked.Increment(ref _calls);
            lock (TokensSeen)
            {
                TokensSeen.Add(token);
            }

            return Gate.Task;
        }

        public void Release(int healthyForServerOne = 1)
        {
            var gate = Gate;
            Gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
            gate.SetResult(new Dictionary<int, DarlingFleetReader.CollectorCounts>
            {
                /* #3819 appended Regressed after Total; this fixture's subject is the memo, not the band. */
                [1] = new(healthyForServerOne, 0, healthyForServerOne, 0),
            });
        }

        public void Fail(Exception ex)
        {
            var gate = Gate;
            Gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
            gate.SetException(ex);
        }
    }

    /// <summary>(a) Five concurrent callers, all parked before the scan produces anything, cost the store ONE
    /// statement — and every one of them gets the same reading at age 0, because the scan was cut at the
    /// instant they all asked for.</summary>
    [Fact]
    public async Task ConcurrentCallers_ShareOneScan()
    {
        var memo = new DarlingFleetReader.CollectionHealthMemo();
        var scan = new CountingScan();

        var callers = Enumerable.Range(0, 5)
            .Select(_ => memo.GetAsync(T0, scan.Run, CancellationToken.None))
            .ToArray();

        /* Every caller is in flight and the statement has run exactly once before anything completes. */
        Assert.All(callers, c => Assert.False(c.IsCompleted));
        Assert.Equal(1, scan.Calls);
        Assert.Equal(1, memo.ScansStarted);

        scan.Release();
        var results = await Task.WhenAll(callers);

        Assert.Equal(1, scan.Calls);
        Assert.All(results, r => Assert.Equal(0, r.AgeSeconds));
        Assert.All(results, r => Assert.Same(results[0].Counts, r.Counts));
        Assert.Equal(1, results[0].Counts[1].Healthy);
    }

    /// <summary>(b) and (d) together, because they are one clock: a reading is served for 60 seconds with its
    /// age beside it, and re-read at 60 — the lifetime is the fastest collector cadence, asserted as a value
    /// so a change to it has to change this line too.</summary>
    [Fact]
    public async Task AReadingUnderAMinuteOld_IsServedWithItsAge_AndReReadAtSixty()
    {
        Assert.Equal(TimeSpan.FromSeconds(60), DarlingFleetReader.CollectionHealthMemoLifetime);

        var memo = new DarlingFleetReader.CollectionHealthMemo();
        var scan = new CountingScan();

        var first = memo.GetAsync(T0, scan.Run, CancellationToken.None);
        scan.Release(healthyForServerOne: 7);
        var fresh = await first;
        Assert.Equal(0, fresh.AgeSeconds);
        Assert.Equal(7, fresh.Counts[1].Healthy);

        /* 30 s later: a hit, age 30, no statement. 59 s later: still a hit. */
        var hit = await memo.GetAsync(T0.AddSeconds(30), scan.Run, CancellationToken.None);
        Assert.Equal(30, hit.AgeSeconds);
        Assert.Same(fresh.Counts, hit.Counts);
        var lastHit = await memo.GetAsync(T0.AddSeconds(59.9), scan.Run, CancellationToken.None);
        Assert.Equal(59, lastHit.AgeSeconds);
        Assert.Equal(1, scan.Calls);

        /* At 60 s the reading is re-read: a second statement, a new dictionary, age back to 0. */
        var expiredCall = memo.GetAsync(T0.AddSeconds(60), scan.Run, CancellationToken.None);
        Assert.Equal(2, scan.Calls);
        scan.Release(healthyForServerOne: 8);
        var reread = await expiredCall;
        Assert.Equal(0, reread.AgeSeconds);
        Assert.Equal(8, reread.Counts[1].Healthy);
        Assert.NotSame(fresh.Counts, reread.Counts);
        Assert.Equal(2, memo.ScansStarted);
    }

    /// <summary>A caller whose <c>now</c> reads BEHIND the memo's — two hosts' clocks, or a test's fixed
    /// instant — is not holding a reading from the future; it is holding the freshest one there is, and the
    /// age says 0 rather than going negative or forcing a pointless re-read.</summary>
    [Fact]
    public async Task ACallerWhoseClockIsBehindTheMemo_IsServedAtAgeZero()
    {
        var memo = new DarlingFleetReader.CollectionHealthMemo();
        var scan = new CountingScan();

        var first = memo.GetAsync(T0, scan.Run, CancellationToken.None);
        scan.Release();
        await first;

        var behind = await memo.GetAsync(T0.AddSeconds(-5), scan.Run, CancellationToken.None);
        Assert.Equal(0, behind.AgeSeconds);
        Assert.Equal(1, scan.Calls);
    }

    /// <summary>(c) A failed scan is not memoized: every waiter present sees the failure — the ORIGINAL
    /// exception, not a wrapper — and the very next caller at the same instant runs a fresh statement rather
    /// than being handed the failure again or an empty reading.</summary>
    [Fact]
    public async Task AFailedScan_IsNotMemoized_TheNextCallReRuns()
    {
        var memo = new DarlingFleetReader.CollectionHealthMemo();
        var scan = new CountingScan();

        var a = memo.GetAsync(T0, scan.Run, CancellationToken.None);
        var b = memo.GetAsync(T0, scan.Run, CancellationToken.None);
        Assert.Equal(1, scan.Calls);

        scan.Fail(new InvalidOperationException("57014: canceling statement due to statement timeout"));

        var exA = await Assert.ThrowsAsync<InvalidOperationException>(() => a);
        var exB = await Assert.ThrowsAsync<InvalidOperationException>(() => b);
        Assert.Contains("57014", exA.Message, StringComparison.Ordinal);
        Assert.Same(exA, exB);

        /* Nothing was memoized: the next caller, same instant, starts statement #2 and gets its reading. */
        var retry = memo.GetAsync(T0, scan.Run, CancellationToken.None);
        Assert.Equal(2, scan.Calls);
        scan.Release(healthyForServerOne: 3);
        var reading = await retry;
        Assert.Equal(0, reading.AgeSeconds);
        Assert.Equal(3, reading.Counts[1].Healthy);
    }

    /// <summary>A failure does not evict a GOOD reading either: while the last successful scan is under a
    /// minute old it keeps being served, and only a caller past the minute pays for the re-read. Pinned
    /// because the tempting simplification — "on failure clear everything" — would turn one timed-out
    /// statement into a burst of retries from every caller of the next minute.</summary>
    [Fact]
    public async Task AFailedReRead_DoesNotEvictTheLastGoodReading()
    {
        var memo = new DarlingFleetReader.CollectionHealthMemo();
        var scan = new CountingScan();

        var first = memo.GetAsync(T0, scan.Run, CancellationToken.None);
        scan.Release(healthyForServerOne: 5);
        var good = await first;

        /* 61 s on: the re-read fails. Its caller sees the failure. */
        var failing = memo.GetAsync(T0.AddSeconds(61), scan.Run, CancellationToken.None);
        Assert.Equal(2, scan.Calls);
        scan.Fail(new TimeoutException("simulated"));
        await Assert.ThrowsAsync<TimeoutException>(() => failing);

        /* A caller whose now is still inside the good reading's minute gets the good reading. */
        var stillGood = await memo.GetAsync(T0.AddSeconds(45), scan.Run, CancellationToken.None);
        Assert.Same(good.Counts, stillGood.Counts);
        Assert.Equal(45, stillGood.AgeSeconds);
        Assert.Equal(2, scan.Calls);

        /* And a caller past it starts statement #3 rather than inheriting the failure. */
        var third = memo.GetAsync(T0.AddSeconds(62), scan.Run, CancellationToken.None);
        Assert.Equal(3, scan.Calls);
        scan.Release(healthyForServerOne: 6);
        Assert.Equal(6, (await third).Counts[1].Healthy);
    }

    /// <summary>
    /// The riskiest line in the change, stated as behaviour: when the FIRST caller cancels while two others
    /// wait, that caller is released at once with a cancellation, the statement keeps running for the two,
    /// and its result is memoized for everyone who arrives inside the next minute. The scan itself never sees
    /// a cancellable token — it runs bounded by the command deadline and the role's statement_timeout, not by
    /// any one caller's patience — so a departing browser tab cannot turn one scan into cancel-and-restart
    /// churn.
    /// </summary>
    [Fact]
    public async Task TheFirstCallersCancellation_ReleasesOnlyThatCaller_AndTheScanCompletesForTheRest()
    {
        var memo = new DarlingFleetReader.CollectionHealthMemo();
        var scan = new CountingScan();

        using var firstCallersToken = new CancellationTokenSource();
        var first = memo.GetAsync(T0, scan.Run, firstCallersToken.Token);
        var second = memo.GetAsync(T0, scan.Run, CancellationToken.None);
        var third = memo.GetAsync(T0, scan.Run, CancellationToken.None);
        Assert.Equal(1, scan.Calls);

        /* The statement was handed a token that cannot cancel — the first caller's never reached it. */
        Assert.Single(scan.TokensSeen);
        Assert.False(scan.TokensSeen[0].CanBeCanceled);

        firstCallersToken.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);

        /* The other two are still waiting on a scan that is still running — nothing was torn down. */
        Assert.False(second.IsCompleted);
        Assert.False(third.IsCompleted);
        Assert.Equal(1, scan.Calls);

        scan.Release(healthyForServerOne: 9);
        var results = await Task.WhenAll(second, third);
        Assert.All(results, r => Assert.Equal(9, r.Counts[1].Healthy));
        Assert.All(results, r => Assert.Equal(0, r.AgeSeconds));

        /* And the departed caller's cancellation cost nobody a statement: a fourth caller 20 s on is a hit. */
        var fourth = await memo.GetAsync(T0.AddSeconds(20), scan.Run, CancellationToken.None);
        Assert.Equal(20, fourth.AgeSeconds);
        Assert.Same(results[0].Counts, fourth.Counts);
        Assert.Equal(1, scan.Calls);
        Assert.Equal(1, memo.ScansStarted);
    }

    /// <summary>The other half of that promise: a scan EVERY waiter abandoned still completes and is still
    /// memoized — the statement was already paid for, and the next tick should not pay for it again.</summary>
    [Fact]
    public async Task AScanEveryWaiterAbandoned_StillCompletes_AndIsStillMemoized()
    {
        var memo = new DarlingFleetReader.CollectionHealthMemo();
        var scan = new CountingScan();

        using var cts = new CancellationTokenSource();
        var a = memo.GetAsync(T0, scan.Run, cts.Token);
        var b = memo.GetAsync(T0, scan.Run, cts.Token);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => a);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => b);

        scan.Release(healthyForServerOne: 4);

        /* The orphaned scan's result landed: a later caller inside the minute is a hit on it. The memo's
           write happens on the scan's continuation, so give it the one yield it needs. */
        var later = await memo.GetAsync(T0.AddSeconds(10), scan.Run, CancellationToken.None);
        Assert.Equal(1, scan.Calls);
        Assert.Equal(10, later.AgeSeconds);
        Assert.Equal(4, later.Counts[1].Healthy);
    }

    /// <summary>The memo is per data source, not per process: the MCP host and the web host each build their
    /// own <see cref="NpgsqlDataSource"/>, so each holds its own memo and "one scan per minute per HOST" is the
    /// bound — and the same instance always maps to the same memo, or the single-flight would be no flight
    /// at all.</summary>
    [Fact]
    public void TheMemo_IsOnePerDataSource()
    {
        using var mcpHost = NpgsqlDataSource.Create("Host=localhost;Username=mcp;Database=darling");
        using var webHost = NpgsqlDataSource.Create("Host=localhost;Username=viewer;Database=darling");

        var mcpMemo = DarlingFleetReader.CollectionHealthMemoFor(mcpHost);
        Assert.Same(mcpMemo, DarlingFleetReader.CollectionHealthMemoFor(mcpHost));
        Assert.NotSame(mcpMemo, DarlingFleetReader.CollectionHealthMemoFor(webHost));
    }

    /// <summary>The age travels: <c>BuildRollup</c> carries it onto the payload, defaults it to 0 for the pure
    /// reduction tests that build cards by hand, and both JSON surfaces (the MCP tool and <c>/api/fleet</c>
    /// serialize the same object through the same options) spell it <c>collection_health_age_seconds</c>,
    /// trailing.</summary>
    [Fact]
    public void TheAge_RidesTheRollup_AndSerializesTrailing()
    {
        var fresh = DarlingFleetReader.BuildRollup(Array.Empty<FleetServerCard>(), T0, T0.AddHours(-1), T0);
        Assert.Equal(0, fresh.CollectionHealthAgeSeconds);

        var served = DarlingFleetReader.BuildRollup(
            Array.Empty<FleetServerCard>(), T0, T0.AddHours(-1), T0, collectionHealthAgeSeconds: 42);
        Assert.Equal(42, served.CollectionHealthAgeSeconds);

        var json = JsonSerializer.Serialize(served, DarlingFleetReader.JsonOptions);
        JsonAssert.Contains("\"collection_health_age_seconds\": 42", json);
        /* Trailing: nothing an existing consumer indexes by position moved. */
        Assert.True(
            json.IndexOf("\"collection_health_age_seconds\"", StringComparison.Ordinal)
            > json.IndexOf("\"tags\"", StringComparison.Ordinal));
        Assert.EndsWith("\"collection_health_age_seconds\":42}", json, StringComparison.Ordinal);
    }

    /// <summary>The reader reaches the 7-day statement ONLY through the memo — a second direct call site would
    /// silently restore one copy of the fan-out per caller and no behavioural test against a fake scan could
    /// see it. Source-anchored on the reader's own text.</summary>
    [Fact]
    public void TheReader_RunsTheCollectionHealthStatement_OnlyThroughTheMemo()
    {
        var reader = RepoFile.ReadRepoFileLf(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingFleetReader.cs");

        Assert.Contains(
            "await CollectionHealthMemoFor(postgres).GetAsync(",
            reader,
            StringComparison.Ordinal);

        /* Every call of the statement's reader is the one inside the memo's scan lambda. */
        var callSites = Regex.Matches(reader, @"ReadFailingCollectorCountsAsync\(postgres,")
            .Select(m => m.Index)
            .ToList();
        Assert.Single(callSites);
        var lineStart = reader.LastIndexOf('\n', callSites[0]) + 1;
        var line = reader[lineStart..reader.IndexOf('\n', callSites[0])];
        Assert.Contains("(scanNow, scanToken) => ReadFailingCollectorCountsAsync(postgres, scanNow, scanToken)", line, StringComparison.Ordinal);

        /* And the statement text and its deadline are untouched by this change — the plan was never the problem. */
        Assert.Contains("FROM v_collection_log\nWHERE collection_time >= $1\nAND   server_id <> 0\nGROUP BY server_id, collector_name", reader, StringComparison.Ordinal);
        Assert.Contains(
            "await using var command = postgres.CreateCommand(FleetCollectionHealthSql);\n        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;",
            reader,
            StringComparison.Ordinal);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) end-to-end: three <see cref="DarlingFleetReader.GetFleetOverviewAsync"/> calls raced
/// against one real data source with three different windows — the exact shape of the #3735 photo — cost the
/// store ONE collection-health statement, and the age travels through the whole read. Plants nothing: the
/// assertions are about the memo the reader routed through and the age on the payload, neither of which
/// depends on what the store holds, so there is no sentinel row to scope to or clean up.
/// </summary>
[Collection("live-postgres")]
public sealed class FleetCollectionHealthMemoLivePostgresTests
{
    [Fact]
    public async Task ThreeRacedOverviewCalls_CostOneCollectionHealthScan_AndTheAgeTravels()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet-memo test.");

        var ct = TestContext.Current.CancellationToken;
        using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var memo = DarlingFleetReader.CollectionHealthMemoFor(postgres);
        Assert.Equal(0, memo.ScansStarted);

        /* The photo: one caller, three windows, in parallel. */
        var now = DateTime.UtcNow;
        var raced = await Task.WhenAll(
            DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-1), now, now, cancellationToken: ct),
            DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-6), now, now, cancellationToken: ct),
            DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-24), now, now, cancellationToken: ct));

        Assert.Equal(1, memo.ScansStarted);
        Assert.All(raced, r => Assert.Equal(0, r.CollectionHealthAgeSeconds));

        /* Half a minute on, still one statement, and the payload says how old that half of it is. */
        var later = now.AddSeconds(30);
        var hit = await DarlingFleetReader.GetFleetOverviewAsync(postgres, later.AddHours(-1), later, later, cancellationToken: ct);
        Assert.Equal(1, memo.ScansStarted);
        Assert.Equal(30, hit.CollectionHealthAgeSeconds);

        /* Past the minute: the second statement of the test, age back to 0. */
        var expired = now.AddSeconds(61);
        var reread = await DarlingFleetReader.GetFleetOverviewAsync(postgres, expired.AddHours(-1), expired, expired, cancellationToken: ct);
        Assert.Equal(2, memo.ScansStarted);
        Assert.Equal(0, reread.CollectionHealthAgeSeconds);

        /* A second data source over the SAME store is the web host beside the MCP host: its own memo, its own
           first statement, nothing borrowed from the first. */
        await using var otherHost = NpgsqlDataSource.Create(connectionString!);
        var other = await DarlingFleetReader.GetFleetOverviewAsync(otherHost, now.AddHours(-1), now, now, cancellationToken: ct);
        Assert.Equal(0, other.CollectionHealthAgeSeconds);
        Assert.Equal(1, DarlingFleetReader.CollectionHealthMemoFor(otherHost).ScansStarted);
        Assert.Equal(2, memo.ScansStarted);
    }
}
