/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
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
/// #5718: <c>get_store_host</c> failed every time, after 10 s, on a store whose live size reads were slow
/// (<c>pg_database_size</c> measured 5.8 s mean / 30.7 s max in the hourly sweep, the per-hypertable size insert
/// 11.8 s mean / 49.4 s max). The live half forces a real client-side command timeout with <c>pg_sleep</c>
/// behind <see cref="DarlingStoreHostProfile.StoreFactsReads"/> and proves each slow fact fails ALONE; the
/// polled read takes the sweep's stored size and reports its age; the pure half pins the error text and the
/// call sites (MCP, web and CLI).
/// </summary>
[Collection("live-postgres")]
public sealed class StoreHostSlowSizeLiveTests
{
    private const string SleepingSizeSql = "SELECT count(*) FROM (SELECT pg_sleep(8)) s";
    private const string SleepingChunksSql = "SELECT count(*), 1 FROM (SELECT pg_sleep(8)) s";
    private const string ProbeObjectName = "store_5718_probe";

    private static string? ConnectionString() => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ASlowSizeRead_MarksOnlyTheSizeUnavailable_AndTheRestOfTheProfileReturns_Live()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the #5718 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        var facts = await DarlingStoreHostProfile.GatherStoreFactsAsync(
            connection, ct, reads: new DarlingStoreHostProfile.StoreFactsReads { SlowReadSeconds = 1, StoreSizeSql = SleepingSizeSql, UncompressedChunkSizeSql = "SELECT 5::bigint, 2::integer" });

        Assert.Null(facts.StoreSizeBytes);
        Assert.Equal("timed out after 1 s", facts.StoreSizeUnavailableReason);
        Assert.NotEmpty(facts.PostgresVersion);
        Assert.NotNull(facts.BufferHitRatioPercent);
        Assert.Equal(5L, facts.UncompressedChunkBytes);
        Assert.Equal(2, facts.UncompressedChunkCount);
        Assert.Null(facts.UncompressedUnavailableReason);
    }

    [Fact]
    public async Task ASlowUncompressedChunkRead_MarksOnlyThatFactUnavailable_AndTheSizeStillReads_Live()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the #5718 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        var facts = await DarlingStoreHostProfile.GatherStoreFactsAsync(
            connection, ct, reads: new DarlingStoreHostProfile.StoreFactsReads { SlowReadSeconds = 1, StoreSizeSql = "SELECT 42::bigint", UncompressedChunkSizeSql = SleepingChunksSql });

        Assert.Null(facts.UncompressedChunkBytes);
        Assert.Null(facts.UncompressedChunkCount);
        Assert.Equal("timed out after 1 s", facts.UncompressedUnavailableReason);
        Assert.Equal(42L, facts.StoreSizeBytes);
        Assert.Null(facts.StoreSizeUnavailableReason);
        Assert.NotNull(facts.BufferHitRatioPercent);

        /* Npgsql leaves the connection open and reusable after a client deadline (measured, see
           AlertReadRetrySeamTests): the settings read that follows in GatherAsync depends on it. */
        Assert.Equal(System.Data.ConnectionState.Open, connection.State);
        await using var after = new NpgsqlCommand("SELECT 1", connection);
        Assert.Equal(1, await after.ExecuteScalarAsync(ct));
    }

    [Fact]
    public async Task TheCallersOwnCancellation_IsNeverSwallowedAsASlowFact_Live()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the #5718 live tests.");

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromMilliseconds(500));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => DarlingStoreHostProfile.GatherStoreFactsAsync(
            connection, cts.Token, reads: new DarlingStoreHostProfile.StoreFactsReads { SlowReadSeconds = 30, StoreSizeSql = SleepingSizeSql }));
    }

    [Fact]
    public async Task APolledRead_TakesTheStoredSweepSize_AndReportsItsAge_NeverReadingPgDatabaseSize_Live()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the #5718 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using (var migrate = new NpgsqlConnection(cs))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        /* A figure no live pg_database_size could ever return, recorded 7 minutes ago: if the tool reports it,
           it read the stored row. */
        const long storedBytes = 7_654_321_987_123L;
        var recordedAt = DateTime.UtcNow.AddMinutes(-7);

        var bodySucceeded = false;
        try
        {
            await using (var seed = new NpgsqlConnection(cs))
            {
                await seed.OpenAsync(ct);
                await using var insert = new NpgsqlCommand(
                    "INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, total_bytes) "
                    + "VALUES ($1, $2, 'store', $3)", seed);
                insert.Parameters.Add(new NpgsqlParameter { Value = DateTime.SpecifyKind(recordedAt, DateTimeKind.Unspecified) });
                insert.Parameters.Add(new NpgsqlParameter { Value = ProbeObjectName });
                insert.Parameters.Add(new NpgsqlParameter { Value = storedBytes });
                await insert.ExecuteNonQueryAsync(ct);
            }

            await using var postgres = NpgsqlDataSource.Create(cs!);
            var cache = new StoreHostProfileCache(TimeSpan.FromMinutes(5));
            var json = await DarlingMcpStoreHostTools.GetStoreHost(postgres, new PostgresConfig { Managed = false }, cache, ct);

            using var doc = JsonDocument.Parse(json);
            var store = doc.RootElement.GetProperty("store");
            Assert.Equal(storedBytes, store.GetProperty("size_bytes").GetInt64());
            var asOf = store.GetProperty("size_as_of").GetDateTime();
            Assert.True(Math.Abs((asOf - recordedAt).TotalSeconds) < 1, $"size_as_of {asOf:O} is not the row's metric_time {recordedAt:O}");
            var age = store.GetProperty("size_age_seconds").GetInt64();
            Assert.InRange(age, 7 * 60 - 5, 7 * 60 + 120);
            Assert.Equal(JsonValueKind.Null, store.GetProperty("size_unavailable_reason").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var delete = new NpgsqlCommand(
                    "DELETE FROM collect.store_metrics WHERE object_name = $1", cleanup);
                delete.Parameters.Add(new NpgsqlParameter { Value = ProbeObjectName });
                await delete.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    [Fact]
    public async Task APolledReadWithNoStoredFigure_SaysSoInsteadOfFallingBackToTheLiveRead_Live()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the #5718 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        var facts = await DarlingStoreHostProfile.GatherStoreFactsAsync(
            connection, ct, useStoredStoreSize: true,
            reads: new DarlingStoreHostProfile.StoreFactsReads { StoredStoreSizeSql = "SELECT 1::bigint, now()::timestamp WHERE false" });

        Assert.Null(facts.StoreSizeBytes);
        Assert.Null(facts.StoreSizeAsOfUtc);
        Assert.Equal("no hourly sweep has recorded the store size yet", facts.StoreSizeUnavailableReason);
    }
}

/// <summary>#5718, no database needed: the error text, the timeout classifier and the call-site pins.</summary>
public sealed class StoreHostSlowSizeTests
{
    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }

    private static string Source(params string[] relative) => File.ReadAllText(Path.Combine(RepoRoot(), Path.Combine(relative)));

    [Fact]
    public void AnEscapedTimeout_ReadsAsATimeoutWithItsDuration_NotAsAStreamError()
    {
        var npgsqlShape = new NpgsqlException("Exception while reading from stream", new TimeoutException("Timeout during reading attempt"));

        var json = McpHelpers.FormatError("get_store_host", DarlingMcpStoreHostTools.FailureFor(npgsqlShape));

        using var doc = JsonDocument.Parse(json);
        Assert.Equal("error", doc.RootElement.GetProperty("status").GetString());
        var message = doc.RootElement.GetProperty("message").GetString()!;
        Assert.Contains("timed out after 10 s", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Exception while reading from stream", message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnOrdinaryFailure_KeepsItsOwnMessage()
    {
        var json = McpHelpers.FormatError("get_store_host", DarlingMcpStoreHostTools.FailureFor(new InvalidOperationException("boom")));

        Assert.Contains("Error during get_store_host: boom", json, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTimeoutClassifier_AcceptsADeadline_AndRefusesCancellationAndOrdinaryErrors()
    {
        Assert.True(DarlingStoreHostProfile.IsReadTimeout(
            new NpgsqlException("Exception while reading from stream", new TimeoutException())));
        Assert.True(DarlingStoreHostProfile.IsReadTimeout(
            new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", PostgresErrorCodes.QueryCanceled)));
        Assert.False(DarlingStoreHostProfile.IsReadTimeout(new OperationCanceledException()));
        Assert.False(DarlingStoreHostProfile.IsReadTimeout(new TaskCanceledException("x", new TimeoutException())));
        Assert.False(DarlingStoreHostProfile.IsReadTimeout(
            new PostgresException("permission denied", "ERROR", "ERROR", PostgresErrorCodes.InsufficientPrivilege)));
        Assert.False(DarlingStoreHostProfile.IsReadTimeout(new NpgsqlException("Failed to connect")));
    }

    [Fact]
    public void TheStoredSizeSql_IsTheRecordedSizeSqlWithTheTimeAdded()
    {
        Assert.Equal(
            StoreSelfMetrics.LatestStoreSizeSql,
            StoreSelfMetrics.LatestStoreSizeWithTimeSql.Replace("SELECT total_bytes, metric_time", "SELECT total_bytes", StringComparison.Ordinal));
    }

    [Fact]
    public void TheMcpTool_AndThereforeTheWebMap_ReadTheStoredSize_AndCheckSettingsKeepsTheLiveRead()
    {
        var tool = Source("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpStoreHostTools.cs");
        Assert.Matches(new Regex(@"DarlingStoreHostProfile\.GatherAsync\(\s*postgresConfig, connection, token, useStoredStoreSize: true\)"), tool);
        Assert.Contains("McpHelpers.FormatError(\"get_store_host\", FailureFor(ex))", tool, StringComparison.Ordinal);
        Assert.Contains("size_as_of", tool, StringComparison.Ordinal);
        Assert.Contains("size_age_seconds", tool, StringComparison.Ordinal);

        /* The web map calls the SAME method, so the web read shows the same fields and the same stored size. */
        var web = Source("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        Assert.Matches(new Regex(@"\[""get_store_host""\] = \(c, pg, an\) => DarlingMcpStoreHostTools\.GetStoreHost\("), web);

        var cli = Source("Darling", "PerformanceMonitor.Darling.Service", "DarlingCliCommands.cs");
        Assert.Contains("DarlingStoreHostProfile.GatherAsync(postgres, connection, cancellationToken)", cli, StringComparison.Ordinal);
        Assert.DoesNotContain("useStoredStoreSize", cli, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWebCard_ShowsTheSizeAge_AndAnUnavailableFactsReason()
    {
        var js = Source("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "sweeps.js");
        Assert.Contains("store.size_age_seconds", js, StringComparison.Ordinal);
        Assert.Contains("store.size_unavailable_reason", js, StringComparison.Ordinal);
        Assert.Contains("store.uncompressed_unavailable_reason", js, StringComparison.Ordinal);
    }
}
