/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5811: the raw-route refusal of a <c>query_stats</c> or <c>procedure_stats</c> panel. The limits come from the field (a 24 h fleet
/// panel of 12.2 M query_stats rows finishes in at most 35.9 s, and one of 19.7 M procedure_stats rows in at most 20.8 s), so a
/// window the field runs today is never refused, and the first refused window is the one that would have run to the timeout.
/// </summary>
public sealed class QueryStoreWideReadGuardRawRouteTests
{
    private static readonly DateTime Start = new(2026, 10, 1, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The refusal for a 43-server fleet storing <paramref name="dailyRows"/> a day, over a window of <paramref name="days"/>.</summary>
    private static string? Refuse(long dailyRows, string collector, int days) =>
        QueryStoreWideReadGuard.RawRefusal(
            new QueryStoreWideReadGuard.Estimate(dailyRows, 43), collector, Start, Start.AddDays(days), QueryStoreWideReadGuard.RawLimitFor(collector));

    [Fact]
    public void TheLimits_AreTheFieldsWorstCostPerRow_OverTheBudget()
    {
        /* 35.9 s over 12.2 M rows is about 2.9 us a row, and 45 s over that is about 15 M; 20.8 s over 19.7 M is about 1.1 us a row,
           and 45 s over that is about 40 M. */
        Assert.Equal(15_000_000, ComposeLimits.MaxRawQueryStatsRows);
        Assert.Equal(40_000_000, ComposeLimits.MaxRawProcedureStatsRows);
        Assert.InRange(45d / (35.9d / 12_200_000d), 15_000_000d, 15_500_000d);
        Assert.InRange(45d / (20.8d / 19_700_000d), 40_000_000d, 43_000_000d);
        Assert.Equal(ComposeLimits.MaxRawQueryStatsRows, QueryStoreWideReadGuard.RawLimitFor("query_stats"));
        Assert.Equal(ComposeLimits.MaxRawProcedureStatsRows, QueryStoreWideReadGuard.RawLimitFor("procedure_stats"));
    }

    [Theory]
    [InlineData("query_stats")]
    [InlineData("procedure_stats")]
    public void JustUnderOrAtTheLimit_Runs_AndJustOverIsRefused(string collector)
    {
        var limit = QueryStoreWideReadGuard.RawLimitFor(collector);

        Assert.Null(QueryStoreWideReadGuard.RawRefusal(new QueryStoreWideReadGuard.Estimate(limit - 1, 43), collector, Start, Start.AddDays(1), limit));
        Assert.Null(QueryStoreWideReadGuard.RawRefusal(new QueryStoreWideReadGuard.Estimate(limit, 43), collector, Start, Start.AddDays(1), limit));
        Assert.NotNull(QueryStoreWideReadGuard.RawRefusal(new QueryStoreWideReadGuard.Estimate(limit + 1, 43), collector, Start, Start.AddDays(1), limit));
    }

    [Fact]
    public void TheFieldsDailyFleetPanels_AreNeverRefused()
    {
        /* The week's highest day of each: 12.4 M query_stats rows and 20.5 M procedure_stats rows across 43 servers. */
        Assert.Null(Refuse(12_400_000, "query_stats", 1));
        Assert.Null(Refuse(20_500_000, "procedure_stats", 1));
        /* procedure_stats at the last full day's 19.7 M still fits two days. */
        Assert.Null(Refuse(19_700_000, "procedure_stats", 2));
    }

    [Fact]
    public void OverTheLimit_TheMessageStatesTheEstimate_TheLimit_AndTheLongestWindowThatFits()
    {
        Assert.Equal(
            "This panel needs about 24.4 million query statistics rows (43 servers, 2 days), over the limit of 15 million. "
            + "Choose fewer servers or a window of 29 hours or less.",
            Refuse(12_200_000, "query_stats", 2));
        Assert.Equal(
            "This panel needs about 59.1 million procedure statistics rows (43 servers, 3 days), over the limit of 40 million. "
            + "Choose fewer servers or a window of 2 days or less.",
            Refuse(19_700_000, "procedure_stats", 3));

        /* A day alone over the limit leaves no window of an hour or more that fits: the message asks for fewer servers only. */
        Assert.Equal(
            "This panel needs about 400 million query statistics rows (1 server, 1 day), over the limit of 15 million. Choose fewer servers.",
            QueryStoreWideReadGuard.RawRefusal(new QueryStoreWideReadGuard.Estimate(400_000_000, 1), "query_stats", Start, Start.AddDays(1), ComposeLimits.MaxRawQueryStatsRows));
    }

    [Fact]
    public void NoEstimate_OrNoServerWithALogRow_NeverRefuses()
    {
        Assert.Null(QueryStoreWideReadGuard.RawRefusal(null, "query_stats", Start, Start.AddDays(30), ComposeLimits.MaxRawQueryStatsRows));
        Assert.Null(QueryStoreWideReadGuard.RawRefusal(new QueryStoreWideReadGuard.Estimate(0, 0), "query_stats", Start, Start.AddDays(30), ComposeLimits.MaxRawQueryStatsRows));
        Assert.Null(QueryStoreWideReadGuard.RawRefusal(new QueryStoreWideReadGuard.Estimate(long.MaxValue, 0), "procedure_stats", Start, Start.AddDays(30), ComposeLimits.MaxRawProcedureStatsRows));
    }

    [Fact]
    public void TheGuard_AppliesToTheTwoRawTables_OnTheRawRouteOnly()
    {
        Assert.Equal("query_stats", QueryStoreWideReadGuard.RawCollectorFor("query_stats", ComposeRoute.Raw));
        Assert.Equal("procedure_stats", QueryStoreWideReadGuard.RawCollectorFor("procedure_stats", ComposeRoute.Raw));
        Assert.Null(QueryStoreWideReadGuard.RawCollectorFor("wait_stats", ComposeRoute.Raw));
        Assert.Null(QueryStoreWideReadGuard.RawCollectorFor("query_store_stats", ComposeRoute.Raw));
        foreach (var tier in new[] { ComposeSourceTier.Hourly, ComposeSourceTier.Daily, ComposeSourceTier.HourlyRawEdges })
        {
            Assert.Null(QueryStoreWideReadGuard.RawCollectorFor("query_stats", new ComposeRoute(tier, null)));
            Assert.Null(QueryStoreWideReadGuard.RawCollectorFor("procedure_stats", new ComposeRoute(tier, null)));
        }
    }
}

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and
   DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStoreWideReadGuardRawEstimateLiveTests
{
    private const int ServerA = -5811001;
    private const int ServerB = -5811002;
    private const int ServerNoLog = -5811003;

    private static readonly DateTime S = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-3), DateTimeKind.Unspecified);

    /// <summary>The estimate is each server's rows for the collector over the day that ends at its newest log row for that collector:
    /// older rows, other collectors and other servers do not count, a server with no log row adds nothing, and the scope narrows it.
    /// It reads the same on its own connection and inside an open transaction (the hourly-edges snapshot), where it leaves the
    /// transaction usable.</summary>
    [Fact]
    public async Task TheEstimate_IsEachServersLastDayOfTheCollector_InScope()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5811 raw estimate live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerA, "raw-est-a", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerB, "raw-est-b", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerNoLog, "raw-est-nolog", ct);

            var newestA = S.AddDays(2).AddHours(6);
            /* A: four rows inside the day ending at its newest row, one just outside it, and a procedure_stats row. */
            await LogAsync(connection, 1, ServerA, "raw-est-a", "query_stats", newestA, 100, ct);
            await LogAsync(connection, 2, ServerA, "raw-est-a", "query_stats", newestA.AddHours(-6), 200, ct);
            await LogAsync(connection, 3, ServerA, "raw-est-a", "query_stats", newestA.AddHours(-12), 300, ct);
            await LogAsync(connection, 4, ServerA, "raw-est-a", "query_stats", newestA.AddHours(-23), 400, ct);
            await LogAsync(connection, 5, ServerA, "raw-est-a", "query_stats", newestA.AddDays(-1), 9_000, ct);
            await LogAsync(connection, 6, ServerA, "raw-est-a", "procedure_stats", newestA, 77, ct);
            /* B: its own newest row, a day earlier than A's, anchors its own day. */
            await LogAsync(connection, 7, ServerB, "raw-est-b", "query_stats", newestA.AddDays(-1), 50, ct);
            await LogAsync(connection, 8, ServerB, "raw-est-b", "query_stats", newestA.AddDays(-1).AddHours(-1), 25, ct);

            var fleet = await QueryStoreWideReadGuard.EstimateRawAsync(postgres, null, null, "query_stats", null, ct);
            Assert.Equal(new QueryStoreWideReadGuard.Estimate(100 + 200 + 300 + 400 + 50 + 25, 2), fleet);

            var scoped = await QueryStoreWideReadGuard.EstimateRawAsync(postgres, null, new[] { "raw-est-b" }, "query_stats", null, ct);
            Assert.Equal(new QueryStoreWideReadGuard.Estimate(75, 1), scoped);

            var procs = await QueryStoreWideReadGuard.EstimateRawAsync(postgres, null, null, "procedure_stats", null, ct);
            Assert.Equal(new QueryStoreWideReadGuard.Estimate(77, 1), procs);

            var noLog = await QueryStoreWideReadGuard.EstimateRawAsync(postgres, null, new[] { "raw-est-nolog" }, "query_stats", null, ct);
            Assert.Equal(new QueryStoreWideReadGuard.Estimate(0, 0), noLog);
            Assert.Null(QueryStoreWideReadGuard.RawRefusal(noLog, "query_stats", S, S.AddDays(30), ComposeLimits.MaxRawQueryStatsRows));

            /* Inside an open transaction, as in the hourly-edges snapshot: the same answer, and the transaction still runs a statement. */
            await using (var transaction = await connection.BeginTransactionAsync(ct))
            {
                var inSnapshot = await QueryStoreWideReadGuard.EstimateRawAsync(postgres, connection, null, "query_stats", null, ct);
                Assert.Equal(fleet, inSnapshot);
                await using var probe = new NpgsqlCommand("SELECT 1", connection, transaction);
                Assert.Equal(1, await probe.ExecuteScalarAsync(ct));
                await transaction.RollbackAsync(ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private static async Task LogAsync(NpgsqlConnection connection, long logId, int serverId, string serverName, string collector, DateTime at, long rows, System.Threading.CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(
            "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) "
            + "VALUES ($1, $2, $3, $4, $5, 20, 'SUCCESS', $6)", connection);
        insert.Parameters.AddWithValue(9_581_100_000L + logId);
        insert.Parameters.AddWithValue(serverId);
        insert.Parameters.AddWithValue(serverName);
        insert.Parameters.AddWithValue(collector);
        insert.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
        insert.Parameters.AddWithValue(rows);
        await insert.ExecuteNonQueryAsync(ct);
    }
}
