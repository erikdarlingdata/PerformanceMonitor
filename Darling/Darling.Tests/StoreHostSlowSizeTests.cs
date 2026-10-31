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

    [Theory]
    [InlineData("SELECT 1::bigint, 1::integer FROM timescaledb_information.no_such_view_5718", "42P01")]
    [InlineData("SELECT 1::bigint, 1::integer FROM no_such_schema_5718.chunks", "42P01")]
    public async Task AStoreWithoutTimescaleDb_ReadsZeroChunks_NotUnavailable_Live(string sql, string expectedSqlState)
    {
        var cs = ConnectionString();
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the #5718 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        /* The statement raises what a database without TimescaleDB raises for the chunk catalog view
           (an undefined relation, SQLSTATE 42P01): the honest answer is "no chunks", not "unknown". */
        var ex = await Assert.ThrowsAsync<PostgresException>(async () =>
        {
            await using var probe = new NpgsqlCommand(sql, connection);
            await probe.ExecuteScalarAsync(ct);
        });
        Assert.Equal(expectedSqlState, ex.SqlState);

        var facts = await DarlingStoreHostProfile.GatherStoreFactsAsync(
            connection, ct, reads: new DarlingStoreHostProfile.StoreFactsReads { StoreSizeSql = "SELECT 42::bigint", UncompressedChunkSizeSql = sql });

        Assert.Equal(0L, facts.UncompressedChunkBytes);
        Assert.Equal(0, facts.UncompressedChunkCount);
        Assert.Null(facts.UncompressedUnavailableReason);
        Assert.Equal(42L, facts.StoreSizeBytes);
    }

    [Theory]
    [InlineData("DO $$ BEGIN RAISE EXCEPTION 'permission denied for view chunks' USING ERRCODE = '42501'; END $$", "42501")]
    [InlineData("SELECT (1 / (SELECT 0))::bigint, 1::integer", "22012")]
    public async Task ARefusedUncompressedChunkRead_IsUnknownWithItsSqlState_NotZeroChunks_Live(string sql, string expectedSqlState)
    {
        var cs = ConnectionString();
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the #5718 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);

        var facts = await DarlingStoreHostProfile.GatherStoreFactsAsync(
            connection, ct, reads: new DarlingStoreHostProfile.StoreFactsReads { StoreSizeSql = "SELECT 42::bigint", UncompressedChunkSizeSql = sql });

        /* Zero would be a fabricated "nothing is uncompressed" (and uncompressed_chunk_percent_of_ram would follow
           it to 0.0); a refusal is unknown, named by its SQLSTATE, and the rest of the profile still returns. */
        Assert.Null(facts.UncompressedChunkBytes);
        Assert.Null(facts.UncompressedChunkCount);
        Assert.Equal($"this role cannot read it (SQLSTATE {expectedSqlState})", facts.UncompressedUnavailableReason);
        Assert.Equal(42L, facts.StoreSizeBytes);
        Assert.NotNull(facts.BufferHitRatioPercent);

        /* The failed statement does not poison the connection: the reads that follow in GatherAsync depend on it. */
        await using var after = new NpgsqlCommand("SELECT 1", connection);
        Assert.Equal(1, await after.ExecuteScalarAsync(ct));
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

    private static string Source(params string[] relative) => RepoFile.ReadRepoFile(relative);

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
    public void TheStoredSizeSql_AndTheRecordedSizeSql_ShareOneTail_ThatIsDefinedOnce()
    {
        /* Both statements are built from ONE private const tail (the predicate and ordering), so they cannot
           drift apart; the select lists are the only difference (#5718 review). */
        const string tailStart = "FROM collect.store_metrics";
        var recorded = StoreSelfMetrics.LatestStoreSizeSql;
        var withTime = StoreSelfMetrics.LatestStoreSizeWithTimeSql;

        Assert.StartsWith("SELECT total_bytes", recorded, StringComparison.Ordinal);
        Assert.StartsWith("SELECT total_bytes, metric_time", withTime, StringComparison.Ordinal);
        Assert.Equal(recorded[recorded.IndexOf(tailStart, StringComparison.Ordinal)..], withTime[withTime.IndexOf(tailStart, StringComparison.Ordinal)..]);

        var source = Source("Darling", "PerformanceMonitor.Darling.Storage", "StoreSelfMetrics.cs");
        Assert.Single(Regex.Matches(source, @"AND\s+total_bytes IS NOT NULL\s+ORDER BY metric_time DESC\s+LIMIT 1"));
    }

    [Theory]
    [InlineData("42P01", true)]
    [InlineData("3F000", true)]
    [InlineData("42883", true)]
    [InlineData("42501", false)]
    [InlineData("22012", false)]
    [InlineData("57014", false)]
    [InlineData("XX000", false)]
    public void OnlyAnUndefinedRelationSchemaOrFunction_MeansTimescaleDbIsAbsent(string sqlState, bool absent)
    {
        Assert.Equal(absent, DarlingStoreHostProfile.IsTimescaleAbsent(new PostgresException("x", "ERROR", "ERROR", sqlState)));
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

    [Fact]
    public void TheWebCard_DerivesTheAgeFromSizeAsOfAtRenderTime_NotFromTheCachedServerAge()
    {
        var js = Source("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "sweeps.js");

        /* The card cell asks storeSizeText for the age as of NOW, so a poll tick that replays the cached payload
           still shows a true age; size_age_seconds is only the fallback inside storeSizeAgeSeconds. */
        Assert.Contains("cellText(storeSizeText(store, Date.now()))", js, StringComparison.Ordinal);
        Assert.Contains("parseUtc(store.size_as_of)", js, StringComparison.Ordinal);
        Assert.DoesNotContain("fmtMs(store.size_age_seconds", js, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheWebCardAge_KeepsMovingWhenTheCachedPayloadIsReplayed_Node()
    {
        var psi = new System.Diagnostics.ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(Path.Combine(RepoRoot(), "Darling", "Darling.Tests", "store-host-size-age-harness.mjs"));
        psi.ArgumentList.Add(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        System.Diagnostics.Process proc;
        try
        {
            proc = System.Diagnostics.Process.Start(psi)!;
        }
        catch (System.ComponentModel.Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            throw;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the store host size-age harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the store host size-age harness failed: " + await error);
            using var doc = JsonDocument.Parse(output);
            var r = doc.RootElement;

            /* One payload, three render times: the age follows the clock, which is what a replayed poll tick needs. */
            Assert.Equal("3.0 GB (hourly sweep, 10.0 min old)", r.GetProperty("atFetch").GetString());
            Assert.Equal("3.0 GB (hourly sweep, 11.0 min old)", r.GetProperty("onePollLater").GetString());
            Assert.Equal("3.0 GB (hourly sweep, 1.2 h old)", r.GetProperty("anHourLater").GetString());
            Assert.Equal("3.0 GB (hourly sweep, 0 ms old)", r.GetProperty("clockBehind").GetString());
            Assert.Equal("3.0 GB (hourly sweep, 10.0 min old)", r.GetProperty("noAsOfUsesServerAge").GetString());
            Assert.Equal("3.0 GB", r.GetProperty("noAge").GetString());
            Assert.Equal("unavailable (timed out after 10 s)", r.GetProperty("unavailable").GetString());
        }
    }

    [Fact]
    public void TheSupportBundle_StaysOnTheStoredSize_AndCarriesItsAgeAndReasons()
    {
        var bundle = Source("Darling", "PerformanceMonitor.Darling.Service", "DiagnosticsBundleRunner.cs");
        Assert.Contains("DarlingMcpStoreHostTools.GetStoreHost(postgres, config.Postgres", bundle, StringComparison.Ordinal);

        /* The bundle's host section is the tool's whole payload, which is what carries these. */
        var tool = Source("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpStoreHostTools.cs");
        foreach (var member in new[] { "size_as_of =", "size_age_seconds =", "size_unavailable_reason =", "uncompressed_unavailable_reason =" })
        {
            Assert.Contains(member, tool, StringComparison.Ordinal);
        }
    }
}
