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
            new QueryStoreWideReadGuard.RawEstimate(dailyRows * days, dailyRows, 43), collector, Start, Start.AddDays(days),
            QueryStoreWideReadGuard.RawLimitFor(collector));

    [Fact]
    public void TheLimits_AreTheMeasuredWorstCostPerRow_OverTheBudget()
    {
        /* #9357: procedure_stats was calibrated against a cheap day and now holds the same 15 M as query_stats. */
        Assert.Equal(15_000_000, ComposeLimits.MaxRawQueryStatsRows);
        Assert.Equal(15_000_000, ComposeLimits.MaxRawProcedureStatsRows);
        Assert.Equal(ComposeLimits.MaxRawQueryStatsRows, QueryStoreWideReadGuard.RawLimitFor("query_stats"));
        Assert.Equal(ComposeLimits.MaxRawProcedureStatsRows, QueryStoreWideReadGuard.RawLimitFor("procedure_stats"));
    }

    private static ComposeFilter FilterOn(string source, string dimension, ComposeFilterOp op, params string[] values) =>
        new(new ComposeDimension(source, dimension, dimension, false), op, new ComposeFilterValue(values, null));

    /// <summary>The refusal for an estimate of <paramref name="rows"/> over a day, judged as the panel runner does: by the collector and the
    /// statement's own filters.</summary>
    private static string? RefuseFiltered(long rows, string collector, params ComposeFilter[] filters) =>
        QueryStoreWideReadGuard.RawRefusal(
            new QueryStoreWideReadGuard.RawEstimate(rows, rows, 43), collector, Start, Start.AddDays(1),
            QueryStoreWideReadGuard.RawLimitFor(collector, filters));

    /// <summary>#9357: the estimate counts the whole collector and cannot see filters, so an estimate between the two limits refuses an
    /// unfiltered raw procedure_stats statement and runs one carrying an eq filter (one value, or a list: the in form). A filter that
    /// does not narrow by equality (neq, like, a range) does not count, and query_stats keeps one limit with or without a filter.</summary>
    [Fact]
    public void AnEqFilter_KeepsTheOldLimit_ForProcedureStatsOnly()
    {
        const long between = 25_000_000;
        Assert.True(between > ComposeLimits.MaxRawProcedureStatsRows && between <= ComposeLimits.MaxRawFilteredProcedureStatsRows);
        Assert.Equal(40_000_000, ComposeLimits.MaxRawFilteredProcedureStatsRows);

        Assert.NotNull(RefuseFiltered(between, "procedure_stats"));
        Assert.Null(RefuseFiltered(between, "procedure_stats", FilterOn("procedure_stats", "object_name", ComposeFilterOp.Eq, "usp_one")));
        Assert.Null(RefuseFiltered(between, "procedure_stats", FilterOn("procedure_stats", "database_name", ComposeFilterOp.Eq, "db_a", "db_b")));
        Assert.Null(RefuseFiltered(between, "procedure_stats",
            FilterOn("procedure_stats", "object_name", ComposeFilterOp.Neq, "usp_one"), FilterOn("procedure_stats", "database_name", ComposeFilterOp.Eq, "db_a")));

        Assert.NotNull(RefuseFiltered(between, "procedure_stats", FilterOn("procedure_stats", "object_name", ComposeFilterOp.Neq, "usp_one")));
        Assert.NotNull(RefuseFiltered(between, "procedure_stats", FilterOn("procedure_stats", "object_name", ComposeFilterOp.Like, "usp%")));

        /* A filtered statement still refuses past the old limit. */
        Assert.NotNull(RefuseFiltered(ComposeLimits.MaxRawFilteredProcedureStatsRows + 1, "procedure_stats", FilterOn("procedure_stats", "object_name", ComposeFilterOp.Eq, "usp_one")));

        Assert.NotNull(RefuseFiltered(between, "query_stats"));
        Assert.NotNull(RefuseFiltered(between, "query_stats", FilterOn("query_stats", "query_hash", ComposeFilterOp.Eq, "0x01")));
        Assert.Null(RefuseFiltered(ComposeLimits.MaxRawQueryStatsRows, "query_stats", FilterOn("query_stats", "query_hash", ComposeFilterOp.Eq, "0x01")));
        Assert.Equal(ComposeLimits.MaxRawQueryStatsRows, QueryStoreWideReadGuard.RawLimitFor("query_stats", new[] { FilterOn("query_stats", "query_hash", ComposeFilterOp.Eq, "0x01") }));
    }

    [Theory]
    [InlineData("query_stats")]
    [InlineData("procedure_stats")]
    public void JustUnderOrAtTheLimit_Runs_AndJustOverIsRefused(string collector)
    {
        var limit = QueryStoreWideReadGuard.RawLimitFor(collector);

        Assert.Null(QueryStoreWideReadGuard.RawRefusal(new QueryStoreWideReadGuard.RawEstimate(limit - 1, limit - 1, 43), collector, Start, Start.AddDays(1), limit));
        Assert.Null(QueryStoreWideReadGuard.RawRefusal(new QueryStoreWideReadGuard.RawEstimate(limit, limit, 43), collector, Start, Start.AddDays(1), limit));
        Assert.NotNull(QueryStoreWideReadGuard.RawRefusal(new QueryStoreWideReadGuard.RawEstimate(limit + 1, limit + 1, 43), collector, Start, Start.AddDays(1), limit));
    }

    [Fact]
    public void TheFieldsDailyFleetPanels_AreNeverRefused()
    {
        /* A day under the limit of each runs, and two days of a half-limit day still fit. */
        Assert.Null(Refuse(12_400_000, "query_stats", 1));
        Assert.Null(Refuse(14_900_000, "procedure_stats", 1));
        Assert.Null(Refuse(7_400_000, "procedure_stats", 2));
    }

    /// <summary>#9357: a procedure_stats day estimated between 15 M and the old 40 M used to run (to the statement timeout). It now
    /// refuses with the narrower-window text, and the window it names is the longest that fits the new limit.</summary>
    [Fact]
    public void AProcedureStatsDay_BetweenTheNewLimitAndTheOldOne_NowRefusesWithTheNarrowerWindowText()
    {
        Assert.Equal(
            "This panel needs about 20.5 million procedure statistics rows (43 servers, 1 day), over the limit of 15 million. "
            + "Choose fewer servers or a window of 17 hours or less.",
            Refuse(20_500_000, "procedure_stats", 1));
        Assert.Equal(
            "This panel needs about 30 million procedure statistics rows (43 servers, 1 day), over the limit of 15 million. "
            + "Choose fewer servers or a window of 12 hours or less.",
            Refuse(30_000_000, "procedure_stats", 1));
        Assert.Null(Refuse(15_000_000, "procedure_stats", 1));
    }

    [Fact]
    public void OverTheLimit_TheMessageStatesTheEstimate_TheLimit_AndTheLongestWindowThatFits()
    {
        Assert.Equal(
            "This panel needs about 24.4 million query statistics rows (43 servers, 2 days), over the limit of 15 million. "
            + "Choose fewer servers or a window of 29 hours or less.",
            Refuse(12_200_000, "query_stats", 2));
        Assert.Equal(
            "This panel needs about 59.1 million procedure statistics rows (43 servers, 3 days), over the limit of 15 million. "
            + "Choose fewer servers or a window of 18 hours or less.",
            Refuse(19_700_000, "procedure_stats", 3));

        /* A day alone over the limit leaves no window of an hour or more that fits: the message asks for fewer servers only. */
        Assert.Equal(
            "This panel needs about 400 million query statistics rows (1 server, 1 day), over the limit of 15 million. Choose fewer servers.",
            QueryStoreWideReadGuard.RawRefusal(new QueryStoreWideReadGuard.RawEstimate(400_000_000, 400_000_000, 1), "query_stats", Start, Start.AddDays(1), ComposeLimits.MaxRawQueryStatsRows));
    }

    [Fact]
    public void NoEstimate_OrNoServerWithALogRow_NeverRefuses()
    {
        Assert.Null(QueryStoreWideReadGuard.RawRefusal(null, "query_stats", Start, Start.AddDays(30), ComposeLimits.MaxRawQueryStatsRows));
        Assert.Null(QueryStoreWideReadGuard.RawRefusal(new QueryStoreWideReadGuard.RawEstimate(0, 0, 0), "query_stats", Start, Start.AddDays(30), ComposeLimits.MaxRawQueryStatsRows));
        Assert.Null(QueryStoreWideReadGuard.RawRefusal(new QueryStoreWideReadGuard.RawEstimate(long.MaxValue, long.MaxValue, 0), "procedure_stats", Start, Start.AddDays(30), ComposeLimits.MaxRawProcedureStatsRows));
    }

    /// <summary>A snapshot connection that throws on the savepoint (here one that is not open; live, one the store dropped) is a lookup
    /// that cannot answer: no verdict, so the panel runs, and the failed rollback does not replace that answer.</summary>
    [Fact]
    public async Task ASnapshotConnectionThatThrowsOnTheSavepoint_GivesNoVerdict_AndThePanelRuns()
    {
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Database=none");
        await using var notOpen = new NpgsqlConnection("Host=127.0.0.1;Port=1;Username=nobody;Database=none");
        var ct = TestContext.Current.CancellationToken;

        Assert.Null(await QueryStoreWideReadGuard.EstimateRawAsync(postgres, notOpen, null, "query_stats", Start, Start.AddDays(1), null, StorageCommandDeadlines.McpReadSeconds, ct));
        Assert.Null(await QueryStoreWideReadGuard.CheckRawAsync(postgres, notOpen, null, "query_stats", Start, Start.AddDays(30), null, StorageCommandDeadlines.McpReadSeconds, ct));
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
    /// The window here, S to S + 1 day, is one both servers cover in full, so each adds exactly its day.
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

            var (from, to) = (S, S.AddDays(1));
            var fleet = await QueryStoreWideReadGuard.EstimateRawAsync(postgres, null, null, "query_stats", from, to, null, StorageCommandDeadlines.McpReadSeconds, ct);
            Assert.Equal(new QueryStoreWideReadGuard.RawEstimate(1075, 100 + 200 + 300 + 400 + 50 + 25, 2), fleet);

            var scoped = await QueryStoreWideReadGuard.EstimateRawAsync(postgres, null, new[] { "raw-est-b" }, "query_stats", from, to, null, StorageCommandDeadlines.McpReadSeconds, ct);
            Assert.Equal(new QueryStoreWideReadGuard.RawEstimate(75, 75, 1), scoped);

            var procs = await QueryStoreWideReadGuard.EstimateRawAsync(postgres, null, null, "procedure_stats", from, to, null, StorageCommandDeadlines.McpReadSeconds, ct);
            Assert.Equal(new QueryStoreWideReadGuard.RawEstimate(77, 77, 1), procs);

            var noLog = await QueryStoreWideReadGuard.EstimateRawAsync(postgres, null, new[] { "raw-est-nolog" }, "query_stats", from, to, null, StorageCommandDeadlines.McpReadSeconds, ct);
            Assert.Equal(new QueryStoreWideReadGuard.RawEstimate(0, 0, 0), noLog);
            Assert.Null(QueryStoreWideReadGuard.RawRefusal(noLog, "query_stats", S, S.AddDays(30), ComposeLimits.MaxRawQueryStatsRows));

            /* Inside an open transaction, as in the hourly-edges snapshot: the same answer, and the transaction still runs a statement. */
            await using (var transaction = await connection.BeginTransactionAsync(ct))
            {
                var inSnapshot = await QueryStoreWideReadGuard.EstimateRawAsync(postgres, connection, null, "query_stats", from, to, null, StorageCommandDeadlines.McpReadSeconds, ct);
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

    /// <summary>A server counts only for the part of the window its collection covers (review of #5811): one that went dark before the
    /// window adds nothing, one that went dark halfway through adds half its day, and one whose rows are past raw retention adds
    /// nothing, so a 24 hour fleet panel the dark servers would push over the limit still runs, and a window that is too big is still
    /// refused.</summary>
    [Fact]
    public async Task AServerCounts_OnlyForThePartOfTheWindowItsCollectionCovers()
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
            var utcNow = DateTime.UtcNow;
            var end = DateTime.SpecifyKind(new DateTime(utcNow.Year, utcNow.Month, utcNow.Day, utcNow.Hour, 0, 0).AddHours(-1), DateTimeKind.Unspecified);
            var start = end.AddDays(-1);

            await DarlingMcpTestData.RegisterServerAsync(connection, ServerA, "raw-cover-live", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerB, "raw-cover-half", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerNoLog, "raw-cover-dark", ct);
            /* Live through the window's end: its whole day. */
            await LogAsync(connection, 11, ServerA, "raw-cover-live", "query_stats", end, 10_000_000, ct);
            /* Dark from the window's midpoint: half its day. */
            await LogAsync(connection, 12, ServerB, "raw-cover-half", "query_stats", start.AddHours(12), 4_000_000, ct);
            /* Dark since an hour before the window started, log rows still inside the collection log's retention: nothing. */
            await LogAsync(connection, 13, ServerNoLog, "raw-cover-dark", "query_stats", start.AddHours(-1), 10_000_000, ct);

            var day = await QueryStoreWideReadGuard.EstimateRawAsync(postgres, null, null, "query_stats", start, end, null, StorageCommandDeadlines.McpReadSeconds, ct);
            Assert.Equal(new QueryStoreWideReadGuard.RawEstimate(10_000_000 + 2_000_000, 14_000_000, 2), day);
            Assert.Null(await QueryStoreWideReadGuard.CheckRawAsync(postgres, null, null, "query_stats", start, end, null, StorageCommandDeadlines.McpReadSeconds, ct));

            var dark = await QueryStoreWideReadGuard.EstimateRawAsync(postgres, null, new[] { "raw-cover-dark" }, "query_stats", start, end, null, StorageCommandDeadlines.McpReadSeconds, ct);
            Assert.Equal(new QueryStoreWideReadGuard.RawEstimate(0, 0, 0), dark);

            /* Two days: the live server's two days, the half-dark one's day and a half, and the dark one's 23 hours, about 35.6 M rows. */
            Assert.NotNull(await QueryStoreWideReadGuard.CheckRawAsync(postgres, null, null, "query_stats", end.AddDays(-2), end, null, StorageCommandDeadlines.McpReadSeconds, ct));

            /* A window reaching past raw retention: a server whose newest log row is older than RawRetentionInterval holds no raw
               rows at all, however far back the window reaches. */
            var retired = end.AddDays(-5);
            await LogAsync(connection, 14, ServerNoLog, "raw-cover-dark", "procedure_stats", retired, 10_000_000, ct);
            var pastRetention = await QueryStoreWideReadGuard.EstimateRawAsync(
                postgres, null, new[] { "raw-cover-dark" }, "procedure_stats", retired.AddDays(-1), end, null, StorageCommandDeadlines.McpReadSeconds, ct);
            Assert.Equal(new QueryStoreWideReadGuard.RawEstimate(0, 0, 0), pastRetention);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The hourly-edges snapshot's connection, dropped by the store between the snapshot's last statement and the estimate's
    /// savepoint (a restart, a failover, a pooler reset, idle_in_transaction_session_timeout): no verdict, so the panel runs.</summary>
    [Fact]
    public async Task ASnapshotConnectionTheStoreDropped_GivesNoVerdict_AndThePanelRuns()
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
            await using var snapshot = new NpgsqlConnection(scratch.ConnectionString + ";Pooling=false");
            await snapshot.OpenAsync(ct);
            await using var transaction = await snapshot.BeginTransactionAsync(ct);
            await using (var pid = new NpgsqlCommand("SELECT pg_backend_pid()", snapshot, transaction))
            {
                var backend = (int)(await pid.ExecuteScalarAsync(ct))!;
                await using var terminate = new NpgsqlCommand("SELECT pg_terminate_backend($1)", connection);
                terminate.Parameters.AddWithValue(backend);
                Assert.Equal(true, await terminate.ExecuteScalarAsync(ct));
            }

            Assert.Null(await QueryStoreWideReadGuard.EstimateRawAsync(postgres, snapshot, null, "query_stats", S, S.AddDays(1), null, StorageCommandDeadlines.McpReadSeconds, ct));
            Assert.Null(await QueryStoreWideReadGuard.CheckRawAsync(postgres, snapshot, null, "query_stats", S, S.AddDays(30), null, StorageCommandDeadlines.McpReadSeconds, ct));
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
