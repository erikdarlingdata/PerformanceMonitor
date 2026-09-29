/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4772: the Query Store backfill's candidate read and stored-floor read turn an error into "no work", so the
/// tails and outage holes they should fill stop filling. They used to say so only at Debug, which the default
/// level drops. The first failure of a run now logs one Warning, the repeats stay quiet, and a read that
/// completes ends the run so the next failure warns again.
///
/// <para>Most of this runs without a store: a data source that cannot connect is a read that fails every time.
/// Only the "a completed read ends the run" wiring needs a store that works and then stops working, so that one
/// test mints its own scratch database.</para>
///
/// <para><b>#1776 own-store</b> — the one live test mints its own scratch database (via
/// <see cref="ScratchPostgres"/>) and renames the query_store_stats table away and back to break and repair the
/// candidate read; it never touches a shared fixture.</para>
/// </summary>
public sealed class QueryStoreBackfillReadFailureWarningTests
{
    private const int TestServerId = -477201;
    private const string ServerLabel = "backfill-read-failure-test";
    private const string DatabaseA = "aaa_db";
    private const string DatabaseB = "bbb_db";

    /// <summary>A store nothing listens on: a Unix-domain-socket directory that does not exist, so every connection
    /// attempt fails at once instead of waiting out a network timeout, which is a read that fails.</summary>
    private const string UnreachableStore = "Host=/no-such-directory-4772;Username=x;Password=x;Database=x;Timeout=2";

    /// <summary>Unspecified kind, as the reads bind it: a Utc-kind <c>DateTime</c> against a <c>timestamp</c>
    /// column throws in Npgsql.</summary>
    private static readonly DateTime FloorLimit = new(2026, 6, 15, 9, 0, 0, DateTimeKind.Unspecified);

    private static readonly Dictionary<string, string> NoState = new(StringComparer.Ordinal);

    [Fact]
    public void TheRuns_WarnOncePerRun_PerServerAndDatabase_AndACompletedReadEndsTheRun()
    {
        var runs = new QueryStoreBackfillReadFailureRuns();

        Assert.True(runs.RecordFailure(1), "the first failure of a run is the one to log");
        Assert.False(runs.RecordFailure(1));
        Assert.False(runs.RecordFailure(1));

        /* A run belongs to its server, and a per-database read has runs of its own. */
        Assert.True(runs.RecordFailure(2));
        Assert.True(runs.RecordFailure(1, "a"));
        Assert.False(runs.RecordFailure(1, "a"));
        Assert.True(runs.RecordFailure(1, "b"));
        Assert.True(runs.RecordFailure(2, "a"));

        /* A completed read ends only its own run, and the next failure is a new run. */
        runs.RecordSuccess(1);
        Assert.True(runs.RecordFailure(1));
        Assert.False(runs.RecordFailure(1, "a"));
        runs.RecordSuccess(1, "a");
        Assert.True(runs.RecordFailure(1, "a"));
        Assert.False(runs.RecordFailure(1, "b"));

        /* A success for a read that was not failing changes nothing. */
        runs.RecordSuccess(3);
        runs.RecordSuccess(3, "a");
        Assert.True(runs.RecordFailure(3));
    }

    [Fact]
    public async Task ARunOfFailedCandidateReads_WarnsOnce()
    {
        var ct = TestContext.Current.CancellationToken;
        var log = new WarningLog();
        await using var postgres = NpgsqlDataSource.Create(UnreachableStore);
        var backfill = NewBackfill(postgres, log);

        for (var i = 0; i < 3; i++)
        {
            Assert.Empty(await backfill.GetCandidateDatabasesAsync(TestServerId, FloorLimit, NoState, ct, ServerLabel));
        }

        var warning = Assert.Single(log.Warnings);
        Assert.Contains(ServerLabel, warning, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARunOfFailedFloorReads_WarnsOncePerDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        var log = new WarningLog();
        await using var postgres = NpgsqlDataSource.Create(UnreachableStore);
        var backfill = NewBackfill(postgres, log);

        for (var i = 0; i < 3; i++)
        {
            Assert.Null(await backfill.GetStoredFloorAsync(TestServerId, DatabaseA, FloorLimit, ct, ServerLabel));
        }

        var first = Assert.Single(log.Warnings);
        Assert.Contains(DatabaseA, first, StringComparison.Ordinal);
        Assert.Contains(ServerLabel, first, StringComparison.Ordinal);

        /* Another database's failure is its own run. */
        Assert.Null(await backfill.GetStoredFloorAsync(TestServerId, DatabaseB, FloorLimit, ct, ServerLabel));
        Assert.Equal(2, log.Warnings.Count);
        Assert.Contains(DatabaseB, log.Warnings[1], StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACompletedCandidateRead_EndsTheRun_SoTheNextFailureWarnsAgain()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4772 backfill read-failure test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        var log = new WarningLog();
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var backfill = NewBackfill(postgres, log);

        /* A store that works: the read completes and there is nothing to warn about. */
        Assert.Empty(await backfill.GetCandidateDatabasesAsync(TestServerId, FloorLimit, NoState, ct, ServerLabel));
        Assert.Empty(log.Warnings);

        /* The table goes away: three failing reads are one run, and one Warning. */
        await RenameStatsTableAsync(scratch.ConnectionString, "query_store_stats", "query_store_stats_renamed", ct);
        for (var i = 0; i < 3; i++)
        {
            Assert.Empty(await backfill.GetCandidateDatabasesAsync(TestServerId, FloorLimit, NoState, ct, ServerLabel));
        }

        Assert.Single(log.Warnings);

        /* It comes back: a read that completes ends the run, and adds nothing to the log. */
        await RenameStatsTableAsync(scratch.ConnectionString, "query_store_stats_renamed", "query_store_stats", ct);
        Assert.Empty(await backfill.GetCandidateDatabasesAsync(TestServerId, FloorLimit, NoState, ct, ServerLabel));
        Assert.Single(log.Warnings);

        /* The next failure is a new run, so it warns again. */
        await RenameStatsTableAsync(scratch.ConnectionString, "query_store_stats", "query_store_stats_renamed", ct);
        Assert.Empty(await backfill.GetCandidateDatabasesAsync(TestServerId, FloorLimit, NoState, ct, ServerLabel));
        Assert.Equal(2, log.Warnings.Count);
    }

    private static QueryStoreBackfill NewBackfill(NpgsqlDataSource postgres, ILogger logger)
        => new(postgres, new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator()), new CollectorDeltaCalculator(), logger);

    private static async Task RenameStatsTableAsync(string connectionString, string from, string to, System.Threading.CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand($"ALTER TABLE collect.{from} RENAME TO {to}", connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Keeps the Warning-and-above messages, formatted, so a test can count them.</summary>
    private sealed class WarningLog : ILogger
    {
        public List<string> Warnings { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Warning)
            {
                Warnings.Add(formatter(state, exception));
            }
        }
    }
}
