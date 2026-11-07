/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8055: the lock-stats denominator. A <c>pg_lock_stats</c> run that finds no lock writes no row to
/// <c>pg_lock_stats</c>, so the table alone cannot say how many times the collector looked. The reader takes
/// <c>total_captures</c> from the collector's successful runs in <c>collection_log</c>, the same source the
/// blocking read borrows its denominator from, so a lock seen in 3 of 60 runs reads as present in 60 captures,
/// not 3. Where the log holds no run for the window, the denominator falls back to the capture times the lock
/// rows themselves show, so it never reads below the numerator.
/// </summary>
[Collection("live-postgres")]
public sealed class PgLockStatsCaptureDenominatorLiveTests
{
    private const int ServerId = -8055101;
    private const string ServerName = "pg-lock-stats-denominator";

    /// <summary>The window of the issue's repro: one hour, sixty one-minute runs from 00:01 to 01:00.</summary>
    private static readonly DateTime WindowStart = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(1);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task SixtyRuns_ThreeWithALock_ReportSixtyTotalCaptures_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the lock-stats denominator test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        await SeedRunsAsync(connection, ct, "pg_lock_stats", "SUCCESS", WindowStart, 1, 60, lockRuns: [10, 11, 12]);

        await using var dataSource = StoreDataSource(scratch);
        var rows = await DarlingPgLockStatsReader.GetPgLockStatsAsync(dataSource, ServerId, WindowStart, WindowEnd, 50, ct);

        var row = Assert.Single(rows);
        Assert.Equal("orders", row.RelationName);
        Assert.False(row.Granted);
        Assert.Equal(3, row.Captures);
        Assert.Equal(60, row.TotalCaptures);
    }

    [Fact]
    public async Task TheTool_ReportsTheRunsAsTotalCaptures_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the lock-stats denominator test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        await SeedRunsAsync(connection, ct, "pg_lock_stats", "SUCCESS", WindowStart, 1, 60, lockRuns: [10, 11, 12]);

        await using var dataSource = StoreDataSource(scratch);
        var json = await DarlingMcpPgServerStateTools.GetPgLockStats(
            dataSource, ServerName, hours_back: 1, limit: 25, as_of: WebDataStartNote.FormatWindowEnd(WindowEnd), cancellationToken: ct);

        Assert.False(McpHelpers.IsErrorEnvelope(json), $"tool returned an error: {json}");
        using var doc = JsonDocument.Parse(json);
        var locks = doc.RootElement.GetProperty("locks").EnumerateArray().ToList();
        var @lock = Assert.Single(locks);
        Assert.Equal(3, @lock.GetProperty("captures").GetInt64());
        Assert.Equal(60, @lock.GetProperty("total_captures").GetInt64());
    }

    [Fact]
    public async Task OtherCollectors_OtherStatuses_AndRunsOutsideTheWindow_AreNotCaptures_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the lock-stats denominator test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        /* The 60 runs that count, and four kinds that must not: another collector's successful runs in the window, this
           collector's failed and skipped runs in the window, and this collector's successful runs the hour before. */
        await SeedRunsAsync(connection, ct, "pg_lock_stats", "SUCCESS", WindowStart, 1, 60, lockRuns: [10, 11, 12]);
        await SeedRunsAsync(connection, ct, "pg_blocking", "SUCCESS", WindowStart, 1, 60, lockRuns: [], logIdBase: 1_000);
        await SeedRunsAsync(connection, ct, "pg_lock_stats", "ERROR", WindowStart.AddSeconds(30), 1, 10, lockRuns: [], logIdBase: 2_000);
        await SeedRunsAsync(connection, ct, "pg_lock_stats", "SKIPPED", WindowStart.AddSeconds(45), 1, 10, lockRuns: [], logIdBase: 3_000);
        await SeedRunsAsync(connection, ct, "pg_lock_stats", "SUCCESS", WindowStart.AddHours(-1), 1, 59, lockRuns: [], logIdBase: 4_000);

        await using var dataSource = StoreDataSource(scratch);
        var rows = await DarlingPgLockStatsReader.GetPgLockStatsAsync(dataSource, ServerId, WindowStart, WindowEnd, 50, ct);

        var row = Assert.Single(rows);
        Assert.Equal(3, row.Captures);
        Assert.Equal(60, row.TotalCaptures);
    }

    [Fact]
    public async Task WithNoRunLogged_TheDenominatorIsTheCaptureTimesTheLockRowsShow_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the lock-stats denominator test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        /* Lock rows from three captures and no collection_log row at all: a store whose log has a gap over the window. The
           denominator cannot be zero with a numerator of three, so it falls back to the three capture times the rows show. */
        await SeedLockRowsAsync(connection, ct, WindowStart, [10, 11, 12]);

        await using var dataSource = StoreDataSource(scratch);
        var rows = await DarlingPgLockStatsReader.GetPgLockStatsAsync(dataSource, ServerId, WindowStart, WindowEnd, 50, ct);

        var row = Assert.Single(rows);
        Assert.Equal(3, row.Captures);
        Assert.Equal(3, row.TotalCaptures);
    }

    /// <summary>
    /// A seed session on the scratch store with the store's <c>search_path</c> SET, never inherited (the #8013 trap): a
    /// cloned scratch database does not carry the template's database-default <c>search_path</c>, and a pooled physical
    /// session keeps the path it was opened with for its whole life, so on either the bare table names the seeds and
    /// <see cref="DarlingMcpTestData.RegisterServerAsync"/> use resolve to nothing (42P01).
    /// </summary>
    private static async Task<NpgsqlConnection> OpenStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection);
        await setPath.ExecuteNonQueryAsync(ct);
        return connection;
    }

    /// <summary>The reader's data source over the scratch store, with the store's <c>search_path</c> on every session it
    /// opens, as the service's own pool has it: the reader's SQL names its tables bare.</summary>
    private static NpgsqlDataSource StoreDataSource(ScratchPostgres scratch) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath });

    /// <summary>
    /// The issue's seed: <paramref name="count"/> runs of <paramref name="collector"/> one <paramref name="stepMinutes"/> apart
    /// from <paramref name="first"/> + 1 step, logged with <paramref name="status"/>, and for the runs in <paramref name="lockRuns"/>
    /// one ungranted lock row at the run's time, with <c>rows_collected = 1</c>; every other run logs <c>rows_collected = 0</c>.
    /// </summary>
    private static async Task SeedRunsAsync(
        NpgsqlConnection connection, CancellationToken ct, string collector, string status, DateTime first, int stepMinutes, int count,
        int[] lockRuns, long logIdBase = 0)
    {
        for (var s = 1; s <= count; s++)
        {
            var at = DarlingMcpTestData.Naive(first.AddMinutes(s * stepMinutes));
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
VALUES ($1, $2, $3, $4, $5, 5, $6, $7)",
                logIdBase + s, ServerId, ServerName, collector, at, status, lockRuns.Contains(s) ? 1 : 0);
        }

        await SeedLockRowsAsync(connection, ct, first, lockRuns);
    }

    private static async Task SeedLockRowsAsync(NpgsqlConnection connection, CancellationToken ct, DateTime first, int[] lockRuns)
    {
        foreach (var s in lockRuns)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_lock_stats (collection_id, collection_time, server_id, server_name, database_name, lock_type, mode, granted, relation_oid, relation_name, backend_count, oldest_wait_ms)
VALUES ($1, $2, $3, $4, 'appdb', 'relation', 'AccessExclusiveLock', false, 16385, 'orders', 2, 4000)",
                (long)s, DarlingMcpTestData.Naive(first.AddMinutes(s)), ServerId, ServerName);
        }
    }
}
