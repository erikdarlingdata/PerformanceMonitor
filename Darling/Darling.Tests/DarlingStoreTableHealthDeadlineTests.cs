/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A store table health read that runs past its limit (#8806) reads as a timeout on the MCP tool (which the web page also shows) and
/// in the Viewer (which shows the exception's message), and a part that timed out does not take the other part with it.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class DarlingStoreTableHealthDeadlineTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>A store connection whose every statement runs under a 1 ms server-side limit, so any read of the catalogs trips it.</summary>
    private static NpgsqlDataSource TrippingSource(string connectionString) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString) { Options = "-c statement_timeout=1" }.ConnectionString);

    /// <summary>
    /// A store connection whose statements wait on a lock another connection holds, so the read trips a short server-side limit
    /// because it is blocked, not because it is slow. The lock is ACCESS EXCLUSIVE on <c>pg_class</c> in the scratch database only,
    /// taken when the source opens its first physical connection (after the reads that used other sources are done) and held until
    /// this is disposed. Every statement the store tool runs reads <c>pg_class</c>, so every one of them waits.
    /// </summary>
    private sealed class BlockedSource : IAsyncDisposable
    {
        private readonly NpgsqlConnection _locker;
        private NpgsqlTransaction? _transaction;

        public NpgsqlDataSource Source { get; }

        public BlockedSource(string connectionString)
        {
            _locker = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(connectionString) { Options = "-c lock_timeout=20000" }.ConnectionString);
            var builder = new NpgsqlDataSourceBuilder(new NpgsqlConnectionStringBuilder(connectionString) { Options = "-c statement_timeout=400" }.ConnectionString);
            builder.UsePhysicalConnectionInitializer(
                connection => HoldTheLockAsync(CancellationToken.None).GetAwaiter().GetResult(),
                connection => HoldTheLockAsync(CancellationToken.None));
            Source = builder.Build();
        }

        private async Task HoldTheLockAsync(CancellationToken cancellationToken)
        {
            if (_transaction is not null)
            {
                return;
            }

            await _locker.OpenAsync(cancellationToken);
            _transaction = await _locker.BeginTransactionAsync(cancellationToken);
            await using var command = new NpgsqlCommand("LOCK TABLE pg_catalog.pg_class IN ACCESS EXCLUSIVE MODE", _locker, _transaction);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        public async ValueTask DisposeAsync()
        {
            if (_transaction is not null)
            {
                await _transaction.RollbackAsync();
                await _transaction.DisposeAsync();
            }

            await _locker.DisposeAsync();
            await Source.DisposeAsync();
        }
    }

    private static void AssertReadsAsATimeout(string message)
    {
        Assert.Contains("-second limit", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Exception while reading from stream", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ATableReadOrIndexReadPastItsLimit_SaysTheLimitTripped_OnTheToolAndAsTheReadersMessage()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8806 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var tripping = TrippingSource(scratch.ConnectionString);

        var tables = await Assert.ThrowsAsync<StoreTableHealthTimeoutException>(() => DarlingStoreTableHealthReader.GetAsync(tripping, ct));
        AssertReadsAsATimeout(tables.Message);
        var indexes = await Assert.ThrowsAsync<StoreTableHealthTimeoutException>(() => DarlingStoreTableHealthReader.GetIndexesAsync(tripping, ct));
        AssertReadsAsATimeout(indexes.Message);

        foreach (var section in new[] { "tables", "indexes", "all" })
        {
            var reply = await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(tripping, section: section, cancellationToken: ct);
            using var json = JsonDocument.Parse(reply);
            Assert.Equal("timed_out", json.RootElement.GetProperty("status").GetString());
            AssertReadsAsATimeout(json.RootElement.GetProperty("message").GetString()!);
            Assert.Equal(StorageCommandDeadlines.McpReadSeconds, json.RootElement.GetProperty("hints").GetProperty("timeout_seconds").GetInt32());
        }
    }

    [Fact]
    public async Task AnIndexReadPastItsLimit_LeavesTheIndexPartOut_AndTheTableRowsStillReturn()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8806 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var healthy = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var tripping = TrippingSource(scratch.ConnectionString);

        var reply = await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(healthy, 1000, false, "all", 0, null, ct, indexSource: tripping);
        using var json = JsonDocument.Parse(reply);
        var root = json.RootElement;

        Assert.False(root.TryGetProperty("status", out _), "the reply is the table answer, not a whole-read failure: " + reply);
        Assert.True(root.GetProperty("tables_returned").GetInt32() > 0, "the table rows still come back");
        Assert.Equal(0, root.GetProperty("indexes_returned").GetInt32());
        AssertReadsAsATimeout(root.GetProperty("indexes_left_out_note").GetString()!);
        Assert.StartsWith("The index part was left out", root.GetProperty("index_header").GetString(), StringComparison.Ordinal);
        Assert.False(root.TryGetProperty("tables_left_out_note", out _));
        /* #8823 meets #8806: a fresh store has never had its statistics reset, so the table rows carry the rates note beside the index note. */
        Assert.False(root.GetProperty("rates_known").GetBoolean());
        Assert.Contains("never reset", root.GetProperty("rates_note").GetString()!, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("all")]
    [InlineData("indexes")]
    public async Task ACoverageReadPastItsLimit_KeepsTheIndexRows_AndLeavesOutOnlyTheCoverage(string section)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8806 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var healthy = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var blocked = new BlockedSource(scratch.ConnectionString);

        var reply = await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(healthy, 1000, false, section, 0, null, ct, indexSource: healthy, coverageSource: blocked.Source);
        using var json = JsonDocument.Parse(reply);
        var root = json.RootElement;

        Assert.False(root.TryGetProperty("status", out _), "the index rows were read, so the reply is not a whole-read failure: " + reply);
        var returned = root.GetProperty("indexes_returned").GetInt32();
        Assert.True(returned > 0, "the index rows that were read still come back");
        Assert.Equal(returned, root.GetProperty("indexes").GetArrayLength());
        Assert.Equal(returned, root.GetProperty("indexes_total").GetInt32());
        Assert.False(root.TryGetProperty("indexes_left_out_note", out _));
        Assert.DoesNotContain("The index part was left out", root.GetProperty("index_header").GetString()!, StringComparison.Ordinal);
        AssertReadsAsATimeout(root.GetProperty("coverage_left_out_note").GetString()!);
        Assert.Contains("coverage was left out", root.GetProperty("index_header").GetString()!, StringComparison.Ordinal);
        Assert.False(root.TryGetProperty("partition_index_coverage", out _));
        Assert.False(root.TryGetProperty("tables_left_out_note", out _));
        if (section == "all")
        {
            Assert.True(root.GetProperty("tables_returned").GetInt32() > 0, "the table rows still come back");
            Assert.False(root.GetProperty("rates_known").GetBoolean());
            Assert.Contains("never reset", root.GetProperty("rates_note").GetString()!, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task ATableReadPastItsLimit_LeavesTheTablePartOut_AndTheIndexRowsStillReturn()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8806 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var healthy = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var tripping = TrippingSource(scratch.ConnectionString);

        var reply = await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(tripping, 1000, false, "all", 0, null, ct, indexSource: healthy);
        using var json = JsonDocument.Parse(reply);
        var root = json.RootElement;

        Assert.False(root.TryGetProperty("status", out _), reply);
        Assert.Equal(0, root.GetProperty("tables_returned").GetInt32());
        Assert.True(root.GetProperty("indexes_returned").GetInt32() > 0, "the index rows still come back");
        AssertReadsAsATimeout(root.GetProperty("tables_left_out_note").GetString()!);
        Assert.False(root.TryGetProperty("indexes_left_out_note", out _));
        /* The table part was left out, so there are no rows to judge the rates by: no rates note is built from none. */
        Assert.False(root.TryGetProperty("rates_note", out _));
        Assert.False(root.TryGetProperty("rates_known", out _));
    }

    [Fact]
    public void BothShapesOfADeadlineTrip_AreOne_AndACallersOwnCancelIsNot()
    {
        var clientDeadline = new NpgsqlException("Exception while reading from stream", new TimeoutException());
        var serverDeadline = new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014");
        var other = new PostgresException("relation does not exist", "ERROR", "ERROR", "42P01");

        Assert.True(StoreTableHealthTimeoutException.IsDeadlineTrip(clientDeadline, CancellationToken.None));
        Assert.True(StoreTableHealthTimeoutException.IsDeadlineTrip(serverDeadline, CancellationToken.None));
        Assert.False(StoreTableHealthTimeoutException.IsDeadlineTrip(other, CancellationToken.None));
        Assert.False(StoreTableHealthTimeoutException.IsDeadlineTrip(new NpgsqlException("broken pipe"), CancellationToken.None));

        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        Assert.False(StoreTableHealthTimeoutException.IsDeadlineTrip(clientDeadline, cancelled.Token));
        Assert.False(StoreTableHealthTimeoutException.IsDeadlineTrip(serverDeadline, cancelled.Token));

        var wrapped = new StoreTableHealthTimeoutException(clientDeadline);
        Assert.Same(clientDeadline, wrapped.InnerException);
        AssertReadsAsATimeout(wrapped.Message);
    }

    [Fact]
    public async Task ACallersOwnCancel_StillCancels_AndIsNotATimeoutSentence()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8806 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var healthy = NpgsqlDataSource.Create(scratch.ConnectionString);
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => DarlingStoreTableHealthReader.GetAsync(healthy, cancelled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => DarlingMcpStoreTableHealthTools.GetStoreTableHealth(healthy, cancellationToken: cancelled.Token));
    }
}
