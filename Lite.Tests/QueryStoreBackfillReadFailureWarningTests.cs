/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4772: the Query Store backfill's candidate read and stored-floor read turn an error into "no work", so the
/// tails and outage holes they should fill stop filling. They used to say so only at Debug, which the default
/// level drops. The first failure of a run now logs one Warning, the repeats stay quiet, and a read that
/// completes ends the run so the next failure warns again. The Lite twin of Darling's pins.
///
/// <para>Each test owns a DuckDB file and creates or drops <c>query_store_stats</c> between reads: with the
/// table absent the read fails the way a broken store does, with it present the read completes.</para>
/// </summary>
public sealed class QueryStoreBackfillReadFailureWarningTests : IDisposable
{
    private const int ServerId = -4772;
    private const string ServerLabel = "backfill-read-failure-test";
    private const string DatabaseA = "aaa_db";
    private const string DatabaseB = "bbb_db";

    private static readonly DateTime FloorLimit = new(2026, 6, 15, 9, 0, 0, DateTimeKind.Utc);

    private readonly string _directory;
    private readonly DuckDbInitializer _duckDb;
    private readonly WarningLog _log = new();
    private readonly Harness _harness;

    public QueryStoreBackfillReadFailureWarningTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), "qs-read-failure-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_directory);
        _duckDb = new DuckDbInitializer(Path.Combine(_directory, "read-failure.duckdb"));
        _harness = new Harness(_duckDb, new ServerManager(_directory), new ScheduleManager(_directory), _log);
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        try
        {
            Directory.Delete(_directory, recursive: true);
        }
        catch (IOException)
        {
            /* Best effort: a leftover temp directory is harmless. */
        }
    }

    [Fact]
    public async Task ARunOfFailedCandidateReads_WarnsOnce_AndACompletedReadEndsTheRun()
    {
        for (var i = 0; i < 3; i++)
        {
            Assert.Empty(await _harness.CandidatesAsync());
        }

        var first = Assert.Single(_log.Warnings);
        Assert.Contains(ServerLabel, first, StringComparison.Ordinal);

        /* A read that completes ends the run: no new Warning for it, and the next failure is a new run. */
        await ExecuteAsync(CreateTableSql);
        Assert.Empty(await _harness.CandidatesAsync());
        Assert.Single(_log.Warnings);

        await ExecuteAsync("DROP TABLE query_store_stats");
        Assert.Empty(await _harness.CandidatesAsync());
        Assert.Equal(2, _log.Warnings.Count);
        Assert.All(_log.Warnings, w => Assert.Contains(ServerLabel, w, StringComparison.Ordinal));
    }

    [Fact]
    public async Task ARunOfFailedFloorReads_WarnsOncePerDatabase_AndACompletedReadEndsThatDatabasesRun()
    {
        for (var i = 0; i < 3; i++)
        {
            Assert.Null(await _harness.FloorAsync(DatabaseA));
        }

        var first = Assert.Single(_log.Warnings);
        Assert.Contains(DatabaseA, first, StringComparison.Ordinal);
        Assert.Contains(ServerLabel, first, StringComparison.Ordinal);

        /* Another database's failure is its own run. */
        Assert.Null(await _harness.FloorAsync(DatabaseB));
        Assert.Equal(2, _log.Warnings.Count);
        Assert.Contains(DatabaseB, _log.Warnings[1], StringComparison.Ordinal);

        await ExecuteAsync(CreateTableSql);
        Assert.Null(await _harness.FloorAsync(DatabaseA));
        Assert.Equal(2, _log.Warnings.Count);

        await ExecuteAsync("DROP TABLE query_store_stats");
        Assert.Null(await _harness.FloorAsync(DatabaseA));
        Assert.Equal(3, _log.Warnings.Count);
        Assert.Contains(DatabaseA, _log.Warnings[2], StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACandidateReadThatIsCancelled_IsNotAFailureToWarnAbout()
    {
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        Assert.Empty(await _harness.CandidatesAsync(cancelled.Token));
        Assert.Empty(_log.Warnings);

        /* It did not start a run either: the first real failure still warns. */
        Assert.Empty(await _harness.CandidatesAsync());
        Assert.Single(_log.Warnings);
    }

    private const string CreateTableSql =
        "CREATE TABLE query_store_stats (server_id INTEGER, database_name VARCHAR, collection_time TIMESTAMP, last_execution_time TIMESTAMP)";

    private async Task ExecuteAsync(string sql)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private sealed class Harness(DuckDbInitializer duckDb, ServerManager servers, ScheduleManager schedules, ILogger<RemoteCollectorService> logger)
        : RemoteCollectorService(duckDb, servers, schedules, logger)
    {
        public Task<List<string>> CandidatesAsync(CancellationToken cancellationToken = default) =>
            GetBackfillCandidateDatabasesAsync(ServerId, FloorLimit, new Dictionary<string, string>(StringComparer.Ordinal), cancellationToken, ServerLabel);

        public Task<DateTime?> FloorAsync(string database) =>
            GetMinCollectedTimeForDatabaseAsync(
                ServerId, "query_store_stats", "last_execution_time", "database_name", database, FloorLimit, CancellationToken.None, ServerLabel);
    }

    /// <summary>Keeps the Warning-and-above messages, formatted, so a test can count them.</summary>
    private sealed class WarningLog : ILogger<RemoteCollectorService>
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
