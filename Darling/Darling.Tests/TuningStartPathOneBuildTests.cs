/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A service start builds at most one of the large start-path indexes: once one build has been issued, the other
/// missing ones wait for the next start and the log names each. Also pins how a start-path build that runs out
/// of time is told apart from any other failure, and the text of the warning it gets. Own scratch database per live test.
/// </summary>
/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres. */
public sealed class TuningStartPathOneBuildTests
{
    private static readonly string[] Deferred =
    {
        PgTableTuning.LegacyRowIndexName,
        PgTableTuning.QueryStatsRestartRowIndexName,
        PgTableTuning.ProcedureStatsRestartRowIndexName,
    };

    [Fact]
    public async Task AStartBuildsOneMissingIndex_DefersTheRest_AndTheNextStartsBuildThem()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the one-build-per-start live test.");

        var ct = TestContext.Current.CancellationToken;
        PgTableTuning.ResetHourlyMissingWarnings();
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var body = new NpgsqlConnection(scratch.ConnectionString);
        await body.OpenAsync(ct);
        await PgMigrations.MigrateAsync(body, ct);
        await TuningStartPasses.ConvergeAsync(body, ct);
        foreach (var name in Deferred)
        {
            Assert.True(await IndexExistsAsync(body, name, ct), name + " should exist after the start passes");
        }

        /* The hourly pass never builds any of them, however many are missing. */
        await Exec(body, "DROP INDEX collect." + PgTableTuning.QueryStatsRestartRowIndexName, ct);
        await Exec(body, "DROP INDEX collect." + PgTableTuning.ProcedureStatsRestartRowIndexName, ct);
        await PgTableTuning.ApplyAsync(body, new CapturingTestLogger(), hourly: true, ct);
        Assert.False(await IndexExistsAsync(body, PgTableTuning.QueryStatsRestartRowIndexName, ct));
        Assert.False(await IndexExistsAsync(body, PgTableTuning.ProcedureStatsRestartRowIndexName, ct));

        /* Start 1: builds exactly the first missing one in list order and defers the other, by name. */
        var start1 = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, start1, ct);
        Assert.True(await IndexExistsAsync(body, PgTableTuning.QueryStatsRestartRowIndexName, ct), start1.Joined);
        Assert.False(await IndexExistsAsync(body, PgTableTuning.ProcedureStatsRestartRowIndexName, ct), start1.Joined);
        Assert.Single(start1.Lines, l => l.StartsWith("Information:", StringComparison.Ordinal)
            && l.Contains(PgTableTuning.ProcedureStatsRestartRowIndexName, StringComparison.Ordinal)
            && l.Contains("built at the next start", StringComparison.Ordinal));
        Assert.DoesNotContain(start1.Lines, l => l.Contains(PgTableTuning.QueryStatsRestartRowIndexName, StringComparison.Ordinal)
            && l.Contains("built at the next start", StringComparison.Ordinal));

        /* Start 2: builds the other. */
        var start2 = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, start2, ct);
        Assert.True(await IndexExistsAsync(body, PgTableTuning.ProcedureStatsRestartRowIndexName, ct), start2.Joined);
        Assert.DoesNotContain(start2.Lines, l => l.Contains("built at the next start", StringComparison.Ordinal));

        /* Start 3: nothing is missing, nothing is built or deferred, and no index OID changes. */
        var oids = await OidsAsync(body, ct);
        var start3 = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, start3, ct);
        Assert.Equal(oids, await OidsAsync(body, ct));
        Assert.DoesNotContain(start3.Lines, l => l.Contains("built at the next start", StringComparison.Ordinal));
        Assert.Equal(0, start3.CountAtLevel(LogLevel.Warning));
    }

    /// <summary>The first-missing index in list order is the earliest start-path index, so the legacy-row index goes before the restart-row ones.</summary>
    [Fact]
    public void TheLegacyRowIndex_IsListedBeforeTheRestartRowIndexes_AndAllThreeShareTheBudget()
    {
        var order = PgTableTuning.Statements.Select((s, i) => (s, i)).ToList();
        int At(string name) => order.First(o => o.s.Contains(name + " ON", StringComparison.Ordinal)).i;
        Assert.True(At(PgTableTuning.LegacyRowIndexName) < At(PgTableTuning.QueryStatsRestartRowIndexName));
        Assert.True(At(PgTableTuning.QueryStatsRestartRowIndexName) < At(PgTableTuning.ProcedureStatsRestartRowIndexName));
        foreach (var name in Deferred)
        {
            Assert.True(PgTableTuning.IsOneBuildPerStart(name), name);
        }
    }

    [Fact]
    public void ABuildThatRanOutOfTime_IsToldApartFromAnyOtherFailure()
    {
        Assert.True(PgTableTuning.IsBuildTimeout(new NpgsqlException("Exception while reading from stream", new TimeoutException("Timeout during reading attempt"))));
        Assert.True(PgTableTuning.IsBuildTimeout(new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", PostgresErrorCodes.QueryCanceled)));
        Assert.False(PgTableTuning.IsBuildTimeout(new PostgresException("relation does not exist", "ERROR", "ERROR", PostgresErrorCodes.UndefinedTable)));
        Assert.False(PgTableTuning.IsBuildTimeout(new NpgsqlException("connection reset", new System.IO.IOException("reset"))));
        Assert.False(PgTableTuning.IsBuildTimeout(new InvalidOperationException("boom")));
    }

    [Fact]
    public void TheTimeoutWarning_NamesTheIndexTheTableTheElapsedTimeAndTheRetry()
    {
        var message = PgTableTuning.BuildTimeoutMessage("idx_demo", "collect.query_stats", 301.4);
        Assert.Equal(
            "Index collect.idx_demo on collect.query_stats was not built: the build ran out of time after 301 s (limit 300 s) and was rolled back; it is retried at the next start, and each try pauses collection at start for up to 300 s",
            message);
    }

    [Fact]
    public void TheStartPathCatch_UsesTheTimeoutWarning_OnlyForABuildThatTimedOut()
    {
        var src = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgTableTuning.cs").ReplaceLineEndings("\n");
        Assert.Contains("statement.StartsWith(CreateIndexPrefix, StringComparison.Ordinal) && IsBuildTimeout(ex)", src, StringComparison.Ordinal);
        Assert.Contains("BuildTimeoutMessage(", src, StringComparison.Ordinal);
        Assert.Contains("Composer performance-tuning statement failed", src, StringComparison.Ordinal);
    }

    private static async Task<string> OidsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT string_agg(c.relname || ':' || c.oid::text, ',' ORDER BY c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            + "WHERE n.nspname = 'collect' AND c.relname = ANY($1)", c);
        cmd.Parameters.AddWithValue(Deferred);
        return (string)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private static async Task Exec(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection c, string name, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = $1", c);
        cmd.Parameters.AddWithValue(name);
        return await cmd.ExecuteScalarAsync(ct) is not null;
    }
}
