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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5244 (PR3, lane R2): the waits-and-blocking reads take a SET of databases. Three reads, each with a reader overload that
/// takes a <see cref="DatabaseFilter"/> and a tool overload that does the same: get_blocking_stats (the BLOCKING series
/// only: deadlocks have no database here), get_current_waits_trend (the blocked-session series only) and get_waiting_tasks.
/// Each public method keeps passing one name, or all databases, until the W lane adds the parameter.
///
/// <para>The seed is three databases (A, B and C) per read, so [A, B] must return A's and B's rows and none of C's. The empty
/// paths are pinned too ([M4]): a filtered miss must not read as a clean bill for databases the read never looked at, and
/// "never collected" must stay <c>unavailable</c>. Skips without <c>DARLING_TEST_PG</c>, like its siblings.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class WaitsDatabaseFilterLiveTests
{
    /* #1776 own-store: this class owns the rows of its two deterministic servers, seeds them itself and removes them. */
    private const string ServerName = "darling-waits-dbfilter-e2e";
    private const string DmvOnlyServerName = "darling-waits-dbfilter-dmvonly-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly int DmvOnlyServerId = ServerIdHelper.GetDeterministicHashCode(DmvOnlyServerName);
    private const string MixedServerName = "darling-waits-dbfilter-mixed-e2e";
    private static readonly int MixedServerId = ServerIdHelper.GetDeterministicHashCode(MixedServerName);
    private const string DbA = "WaitsDbA";
    private const string DbB = "WaitsDbB";
    private const string DbC = "WaitsDbC";
    private const string DbD = "WaitsDbD";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the live waits database-filter test.";

    [Fact]
    public void ReaderSql_IsTheListForm_WithNoNameSplicedIn()
    {
        Assert.Contains("($5::text[] IS NULL OR database_name = ANY($5))", DarlingSessionReader.WaitingTasksSql, StringComparison.Ordinal);
        Assert.Contains("($4::text[] IS NULL OR database_name = ANY($4))", DarlingDataReader.BlockedSessionTrendSql, StringComparison.Ordinal);
        /* Both arms of the blocking series carry it, and the source choice reads the FILTERED bpr (Lite's and get_blocking_trend's rule). */
        var stats = DarlingDataReader.BlockingDurationStatsSql;
        Assert.Equal(2, CountOf(stats, "($5::text[] IS NULL OR database_name = ANY($5))"));
        Assert.Contains("NOT EXISTS (SELECT 1 FROM bpr)", stats, StringComparison.Ordinal);
        Assert.DoesNotContain("xe_any", stats, StringComparison.Ordinal);
        foreach (var sql in new[] { DarlingSessionReader.WaitingTasksSql, DarlingDataReader.BlockedSessionTrendSql, stats })
        {
            Assert.DoesNotContain("{{", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("::text IS NULL", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task WaitingTasks_Reader_TwoNames_ReturnsOnlyThoseDatabases_TheCapAppliesAfterTheFilter()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var seed = await SeedAsync(cs!, ct);
            var start = seed.End.AddHours(-1);

            var ab = await DarlingSessionReader.GetWaitingTasksAsync(postgres, ServerId, start, seed.End, 100, DatabaseFilter.Of([DbA, DbB]), ct);
            Assert.Equal(new[] { 51, 52, 71 }, ab.Select(r => r.SessionId).Order().ToArray());
            Assert.All(ab, r => Assert.Contains(r.DatabaseName, new[] { DbA, DbB }));

            /* The order of the names does not matter. */
            var ba = await DarlingSessionReader.GetWaitingTasksAsync(postgres, ServerId, start, seed.End, 100, DatabaseFilter.Of([DbB, DbA]), ct);
            Assert.Equal(ab.Select(r => r.SessionId), ba.Select(r => r.SessionId));

            /* One name, All and the unchanged public method: the same page as before the list. */
            var oneList = await DarlingSessionReader.GetWaitingTasksAsync(postgres, ServerId, start, seed.End, 100, DatabaseFilter.One(DbC), ct);
            Assert.Equal(new[] { 70, 90 }, oneList.Select(r => r.SessionId).Order().ToArray());
            var all = await DarlingSessionReader.GetWaitingTasksAsync(postgres, ServerId, start, seed.End, 100, DatabaseFilter.All, ct);
            var plain = await DarlingSessionReader.GetWaitingTasksAsync(postgres, ServerId, start, seed.End, 100, ct);
            Assert.Equal(5, all.Count);
            Assert.Equal(all.Select(r => r.SessionId), plain.Select(r => r.SessionId));

            /* The cap is applied AFTER the filter: the unfiltered top two are 90 (C, 4000 ms) and 51; [A, B] keeps its own top two (51, 52). */
            Assert.Equal(new[] { 90, 51 }, all.Take(2).Select(r => r.SessionId).ToArray());
            var capped = await DarlingSessionReader.GetWaitingTasksAsync(postgres, ServerId, start, seed.End, 2, DatabaseFilter.Of([DbA, DbB]), ct);
            Assert.Equal(new[] { 51, 52 }, capped.Select(r => r.SessionId).ToArray());

            /* A name no row carries, or a differently cased one, finds nothing: the compare is exact. */
            var none = await DarlingSessionReader.GetWaitingTasksAsync(postgres, ServerId, start, seed.End, 100, DatabaseFilter.Of(["NoSuchDb", DbA.ToLowerInvariant()]), ct);
            Assert.Empty(none);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task WaitingTasks_Tool_EchoesTheSet_APageIsTheNewestOfTheChosen_AMissIsEmptyForThemNotUnavailable()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var seed = await SeedAsync(cs!, ct);
            var asOf = seed.End.ToString("o");

            var ab = JsonDocument.Parse(await DarlingMcpSessionTools.GetWaitingTasks(
                postgres, ServerName, 1, 2, DatabaseFilter.Of([DbA, DbB]), asOf, null, ct)).RootElement;
            Assert.Equal("the chosen databases", ab.GetProperty("database_name").GetString());
            Assert.Equal(2, ab.GetProperty("tasks_returned").GetInt32());
            Assert.True(ab.GetProperty("truncated").GetBoolean());   /* 71 is the third row of A and B */
            Assert.Equal(new[] { 51, 52 }, ab.GetProperty("tasks").EnumerateArray().Select(t => t.GetProperty("session_id").GetInt32()).ToArray());

            /* One name echoes the name itself; none and the public method echo null and read every database. */
            var one = JsonDocument.Parse(await DarlingMcpSessionTools.GetWaitingTasks(
                postgres, ServerName, 1, 30, DatabaseFilter.One(DbC), asOf, null, ct)).RootElement;
            Assert.Equal(DbC, one.GetProperty("database_name").GetString());
            Assert.Equal(2, one.GetProperty("tasks_returned").GetInt32());
            Assert.False(one.GetProperty("truncated").GetBoolean());
            var plain = JsonDocument.Parse(await DarlingMcpSessionTools.GetWaitingTasks(postgres, ServerName, 1, 30, asOf, null, null, ct)).RootElement;
            Assert.Equal(JsonValueKind.Null, plain.GetProperty("database_name").ValueKind);
            Assert.Equal(5, plain.GetProperty("tasks_returned").GetInt32());

            /* [M4] The filtered miss: the collector HAS sampled this server, so the answer is empty, and it says the CHOSEN databases had none. */
            var oneMiss = JsonDocument.Parse(await DarlingMcpSessionTools.GetWaitingTasks(
                postgres, ServerName, 1, 30, DatabaseFilter.One("NoSuchDb"), asOf, null, ct)).RootElement;
            Assert.Equal("empty", oneMiss.GetProperty("status").GetString());
            Assert.Contains("for the database NoSuchDb", oneMiss.GetProperty("message").GetString(), StringComparison.Ordinal);
            var manyMiss = JsonDocument.Parse(await DarlingMcpSessionTools.GetWaitingTasks(
                postgres, ServerName, 1, 30, DatabaseFilter.Of(["NoSuchDb", "NorThisOne"]), asOf, null, ct)).RootElement;
            Assert.Equal("empty", manyMiss.GetProperty("status").GetString());
            var manyMessage = manyMiss.GetProperty("message").GetString()!;
            Assert.Contains("for the chosen databases", manyMessage, StringComparison.Ordinal);
            Assert.DoesNotContain("database '", manyMessage, StringComparison.Ordinal);

            /* A window with no waiting_tasks rows at all, filtered or not, keeps the unfiltered sentence: the store holds nothing to filter. */
            var farBack = JsonDocument.Parse(await DarlingMcpSessionTools.GetWaitingTasks(
                postgres, ServerName, 1, 30, DatabaseFilter.All, seed.End.AddDays(-30).ToString("o"), null, ct)).RootElement;
            Assert.Equal("No waiting tasks captured in the specified time range.", farBack.GetProperty("message").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task BlockedSessionTrend_Reader_TwoNames_ReturnsOnlyThoseDatabases_TheStringOverloadIsUnchanged()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var seed = await SeedAsync(cs!, ct);
            var start = seed.End.AddHours(-1);

            /* Blocked sessions in the seed: A has two (51, 52), C has one (70), B none. */
            var ac = await DarlingDataReader.GetBlockedSessionTrendAsync(postgres, ServerId, start, seed.End, DatabaseFilter.Of([DbA, DbC]), ct);
            Assert.Equal(new[] { (DbA, 2L), (DbC, 1L) }, ac.Select(r => (r.DatabaseName, r.BlockedCount)).OrderBy(r => r.DatabaseName).ToArray());

            var ab = await DarlingDataReader.GetBlockedSessionTrendAsync(postgres, ServerId, start, seed.End, DatabaseFilter.Of([DbA, DbB]), ct);
            Assert.Equal(new[] { (DbA, 2L) }, ab.Select(r => (r.DatabaseName, r.BlockedCount)).ToArray());

            var all = await DarlingDataReader.GetBlockedSessionTrendAsync(postgres, ServerId, start, seed.End, DatabaseFilter.All, ct);
            Assert.Equal(2, all.Count);

            /* The string overload: one name narrows, null and whitespace read every database, exactly as before. */
            var oneString = await DarlingDataReader.GetBlockedSessionTrendAsync(postgres, ServerId, start, seed.End, DbC, ct);
            Assert.Equal(new[] { (DbC, 1L) }, oneString.Select(r => (r.DatabaseName, r.BlockedCount)).ToArray());
            Assert.Equal(2, (await DarlingDataReader.GetBlockedSessionTrendAsync(postgres, ServerId, start, seed.End, null, ct)).Count);
            Assert.Equal(2, (await DarlingDataReader.GetBlockedSessionTrendAsync(postgres, ServerId, start, seed.End, "   ", ct)).Count);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task CurrentWaitsTrend_Tool_ListLimitsTheBlockedSeriesOnly_TheEchoNamesTheSet()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var seed = await SeedAsync(cs!, ct);
            var asOf = seed.End.ToString("o");

            var ac = JsonDocument.Parse(await DarlingMcpDataTools.GetCurrentWaitsTrend(
                postgres, ServerName, 1, DatabaseFilter.Of([DbA, DbC]), asOf, ct)).RootElement;
            Assert.Equal("the chosen databases", ac.GetProperty("database_name").GetString());
            Assert.Equal(new[] { DbA, DbC }, BlockedDatabases(ac));

            var ab = JsonDocument.Parse(await DarlingMcpDataTools.GetCurrentWaitsTrend(
                postgres, ServerName, 1, DatabaseFilter.Of([DbA, DbB]), asOf, ct)).RootElement;
            Assert.Equal(new[] { DbA }, BlockedDatabases(ab));

            /* The waiting-task series is per wait type and stays whole: the same under every filter, B's wait among them. */
            var waitTypes = WaitTypes(ab);
            Assert.Equal(WaitTypes(ac), waitTypes);
            Assert.Contains("PAGEIOLATCH_SH", waitTypes);

            /* A filter that matches no blocked session still answers with the whole waiting-task series, and an empty blocked series. */
            var bOnly = JsonDocument.Parse(await DarlingMcpDataTools.GetCurrentWaitsTrend(
                postgres, ServerName, 1, DatabaseFilter.One(DbB), asOf, ct)).RootElement;
            Assert.Equal(DbB, bOnly.GetProperty("database_name").GetString());
            Assert.Empty(BlockedDatabases(bOnly));
            Assert.Equal(waitTypes, WaitTypes(bOnly));

            /* The public method (a string name) passes the one-name filter through; no name echoes null and reads all. */
            var publicOne = JsonDocument.Parse(await DarlingMcpDataTools.GetCurrentWaitsTrend(postgres, ServerName, 1, DbC, asOf, ct)).RootElement;
            Assert.Equal(DbC, publicOne.GetProperty("database_name").GetString());
            Assert.Equal(new[] { DbC }, BlockedDatabases(publicOne));
            var publicAll = JsonDocument.Parse(await DarlingMcpDataTools.GetCurrentWaitsTrend(postgres, ServerName, 1, null, asOf, ct)).RootElement;
            Assert.Equal(JsonValueKind.Null, publicAll.GetProperty("database_name").ValueKind);
            Assert.Equal(new[] { DbA, DbC }, BlockedDatabases(publicAll));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }

        static string[] BlockedDatabases(JsonElement root) =>
            root.GetProperty("blocked_sessions").EnumerateArray().Select(b => b.GetProperty("database_name").GetString()!).Distinct().Order().ToArray();
        static string[] WaitTypes(JsonElement root) =>
            root.GetProperty("waiting_tasks").EnumerateArray().Select(w => w.GetProperty("wait_type").GetString()!).Distinct().Order().ToArray();
    }

    [Fact]
    public async Task BlockingStats_Reader_TwoNames_BucketsOnlyThoseDatabases_OnBothArms()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var seed = await SeedAsync(cs!, ct);
            var start = seed.End.AddHours(-1);

            /* XE arm. Minute m1: A 1000 + 3000, B 5000. Minute m2: C 7000. */
            var ab = await DarlingDataReader.GetBlockingDurationStatsAsync(postgres, ServerId, start, seed.End, DatabaseFilter.Of([DbA, DbB]), ct);
            var abRow = Assert.Single(ab);
            Assert.Equal(seed.M1, abRow.Time);
            Assert.Equal((3L, 9000L, 5000L), (abRow.EventCount, abRow.TotalDurationMs, abRow.MaxDurationMs));
            Assert.Equal(3000d, abRow.AvgDurationMs);

            var bc = await DarlingDataReader.GetBlockingDurationStatsAsync(postgres, ServerId, start, seed.End, DatabaseFilter.Of([DbB, DbC]), ct);
            Assert.Equal(new[] { (seed.M1, 1L, 5000L), (seed.M2, 1L, 7000L) }, bc.Select(r => (r.Time, r.EventCount, r.TotalDurationMs)).ToArray());

            /* All, One and the unchanged public method: the whole XE series, as before the list. */
            var all = await DarlingDataReader.GetBlockingDurationStatsAsync(postgres, ServerId, start, seed.End, DatabaseFilter.All, ct);
            var plain = await DarlingDataReader.GetBlockingDurationStatsAsync(postgres, ServerId, start, seed.End, ct);
            Assert.Equal(new[] { (seed.M1, 3L, 9000L), (seed.M2, 1L, 7000L) }, all.Select(r => (r.Time, r.EventCount, r.TotalDurationMs)).ToArray());
            Assert.Equal(all.Select(r => (r.Time, r.EventCount, r.TotalDurationMs)), plain.Select(r => (r.Time, r.EventCount, r.TotalDurationMs)));
            var oneC = await DarlingDataReader.GetBlockingDurationStatsAsync(postgres, ServerId, start, seed.End, DatabaseFilter.One(DbC), ct);
            Assert.Equal(new[] { (seed.M2, 1L, 7000L) }, oneC.Select(r => (r.Time, r.EventCount, r.TotalDurationMs)).ToArray());

            /* The DMV arm is read only when the XE arm has no rows for the CHOSEN databases (#5244, Lite's rule). D has DMV rows only,
               so D's series is the DMV row, while the unfiltered series (XE has rows) carries no DMV row. */
            var d = await DarlingDataReader.GetBlockingDurationStatsAsync(postgres, ServerId, start, seed.End, DatabaseFilter.One(DbD), ct);
            Assert.Equal(new[] { (seed.M3, 1L, 999L) }, d.Select(r => (r.Time, r.EventCount, r.TotalDurationMs)).ToArray());
            Assert.DoesNotContain(all, r => r.Time == seed.M3);

            /* DMV arm: a server with no XE row falls back to the DMV snapshot, and the list applies there too. */
            var dmvAll = await DarlingDataReader.GetBlockingDurationStatsAsync(postgres, DmvOnlyServerId, start, seed.End, DatabaseFilter.All, ct);
            Assert.Equal(new[] { (seed.M1, 3L, 600L) }, dmvAll.Select(r => (r.Time, r.EventCount, r.TotalDurationMs)).ToArray());
            var dmvAb = await DarlingDataReader.GetBlockingDurationStatsAsync(postgres, DmvOnlyServerId, start, seed.End, DatabaseFilter.Of([DbA, DbB]), ct);
            Assert.Equal(new[] { (seed.M1, 3L, 600L) }, dmvAb.Select(r => (r.Time, r.EventCount, r.TotalDurationMs)).ToArray());
            var dmvC = await DarlingDataReader.GetBlockingDurationStatsAsync(postgres, DmvOnlyServerId, start, seed.End, DatabaseFilter.One(DbC), ct);
            Assert.Empty(dmvC);
            var dmvB = await DarlingDataReader.GetBlockingDurationStatsAsync(postgres, DmvOnlyServerId, start, seed.End, DatabaseFilter.One(DbB), ct);
            Assert.Equal(new[] { (seed.M1, 1L, 300L) }, dmvB.Select(r => (r.Time, r.EventCount, r.TotalDurationMs)).ToArray());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task BlockingStats_Tool_EchoesTheSet_AFilteredEmptyAnswerSaysTheBlockingHalfWasFiltered()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var seed = await SeedAsync(cs!, ct);
            var asOf = seed.End.ToString("o");

            var ab = JsonDocument.Parse(await DarlingMcpDataTools.GetBlockingStats(
                postgres, ServerName, 1, DatabaseFilter.Of([DbA, DbB]), asOf, null, ct)).RootElement;
            Assert.Equal("the chosen databases", ab.GetProperty("database_name").GetString());
            Assert.Equal(1, ab.GetProperty("blocking_duration").GetArrayLength());
            Assert.Equal(3, ab.GetProperty("blocking_duration")[0].GetProperty("event_count").GetInt64());

            var one = JsonDocument.Parse(await DarlingMcpDataTools.GetBlockingStats(
                postgres, ServerName, 1, DatabaseFilter.One(DbC), asOf, null, ct)).RootElement;
            Assert.Equal(DbC, one.GetProperty("database_name").GetString());
            Assert.Equal(1, one.GetProperty("blocking_duration").GetArrayLength());

            var plain = JsonDocument.Parse(await DarlingMcpDataTools.GetBlockingStats(postgres, ServerName, 1, asOf, null, null, ct)).RootElement;
            Assert.Equal(JsonValueKind.Null, plain.GetProperty("database_name").ValueKind);
            Assert.Equal(2, plain.GetProperty("blocking_duration").GetArrayLength());

            /* [M4] A filtered blocking series with no deadlocks anywhere: the blocking collectors HAVE run, so the answer is empty, and the
               sentence says exactly which half was limited to the databases instead of "no blocking or deadlocks". */
            var oneMiss = JsonDocument.Parse(await DarlingMcpDataTools.GetBlockingStats(
                postgres, ServerName, 1, DatabaseFilter.One("NoSuchDb"), asOf, null, ct)).RootElement;
            Assert.Equal("empty", oneMiss.GetProperty("status").GetString());
            var oneMessage = oneMiss.GetProperty("message").GetString()!;
            Assert.Contains("No blocking for the database NoSuchDb (and no deadlocks, which are not limited by database)", oneMessage, StringComparison.Ordinal);
            var manyMiss = JsonDocument.Parse(await DarlingMcpDataTools.GetBlockingStats(
                postgres, ServerName, 1, DatabaseFilter.Of(["NoSuchDb", "NorThisOne"]), asOf, null, ct)).RootElement;
            Assert.Equal("empty", manyMiss.GetProperty("status").GetString());
            var manyMessage = manyMiss.GetProperty("message").GetString()!;
            Assert.Contains("No blocking for the chosen databases (and no deadlocks", manyMessage, StringComparison.Ordinal);
            Assert.DoesNotContain("database '", manyMessage, StringComparison.Ordinal);

            /* Unfiltered, a window with no rows keeps the sentence it always had. */
            var farBack = JsonDocument.Parse(await DarlingMcpDataTools.GetBlockingStats(
                postgres, ServerName, 1, DatabaseFilter.All, seed.End.AddDays(-30).ToString("o"), null, ct)).RootElement;
            Assert.Equal("empty", farBack.GetProperty("status").GetString());
            Assert.StartsWith($"No blocking or deadlocks recorded for {ServerName} in the last 1 hour(s)", farBack.GetProperty("message").GetString(), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task BlockingStats_AndBlockingTrend_ChooseTheSameSource_TheDmvArmOnlyWhenXeHasNoRowsForTheChosenDatabases()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var seed = await SeedAsync(cs!, ct);
            var start = seed.End.AddHours(-1);

            /* MixedServer: XE has a row for B only (m1, 5000), DMV has a row for A only (m2, 222). Filter [A] has no XE row, so BOTH tools
               answer from the DMV arm; [A, B] has an XE row, so BOTH answer from the XE arm (and never mix in A's DMV row). */
            async Task<((DateTime, long, long)[] Stats, (DateTime, int)[] Trend, string? StatsSource, string? TrendSource)> AskAsync(DatabaseFilter filter)
            {
                var stats = await DarlingDataReader.GetBlockingDurationStatsAsync(postgres, MixedServerId, start, seed.End, filter, ct);
                var trend = await DarlingBlockingTrendReader.GetBlockingTrendAsync(postgres, MixedServerId, start, seed.End, filter, ct);
                return (stats.Select(r => (r.Time, r.EventCount, r.TotalDurationMs)).ToArray(), trend.Select(p => (p.Time, p.Count)).ToArray(),
                    stats.Select(r => r.Source).Distinct().SingleOrDefault(), trend.Select(p => p.Source).Distinct().SingleOrDefault());
            }

            var a = await AskAsync(DatabaseFilter.Of([DbA]));
            Assert.Equal(new[] { (seed.M2, 1L, 222L) }, a.Stats);
            Assert.Equal(new[] { (seed.M2, 1) }, a.Trend);
            /* #5244 M1: the answer says which arm answered. [A] has no XE row, so the DMV snapshot. */
            Assert.Equal("DMV snapshot", a.StatsSource);
            Assert.Equal("DMV snapshot", a.TrendSource);

            var ab = await AskAsync(DatabaseFilter.Of([DbA, DbB]));
            Assert.Equal(new[] { (seed.M1, 1L, 5000L) }, ab.Stats);
            Assert.Equal(new[] { (seed.M1, 1) }, ab.Trend);
            /* Adding B to the filter moves the answer to the XE arm, and the source says so: A's DMV-only row is not mixed in. */
            Assert.Equal("blocked-process-report", ab.StatsSource);
            Assert.Equal("blocked-process-report", ab.TrendSource);

            /* The two tools' JSON carries the same word under a top-level source key. */
            var statsJson = JsonDocument.Parse(await DarlingMcpDataTools.GetBlockingStats(postgres, MixedServerName, 1, DatabaseFilter.Of([DbA]), seed.End.ToString("o"), null, ct)).RootElement;
            Assert.Equal("DMV snapshot", statsJson.GetProperty("source").GetString());
            var statsJsonAb = JsonDocument.Parse(await DarlingMcpDataTools.GetBlockingStats(postgres, MixedServerName, 1, DatabaseFilter.Of([DbA, DbB]), seed.End.ToString("o"), null, ct)).RootElement;
            Assert.Equal("blocked-process-report", statsJsonAb.GetProperty("source").GetString());

            /* N1: get_blocking_trend's own JSON, by exact tag for both filters, so a trend tool that hard-codes one source fails here
               even though the readers above are right. */
            var trendJsonA = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlockingTrend(postgres, MixedServerName, 1, seed.End.ToString("o"), DatabaseFilter.Of([DbA]), ct)).RootElement;
            Assert.Equal("DMV snapshot", trendJsonA.GetProperty("source").GetString());
            var trendJsonAb = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlockingTrend(postgres, MixedServerName, 1, seed.End.ToString("o"), DatabaseFilter.Of([DbA, DbB]), ct)).RootElement;
            Assert.Equal("blocked-process-report", trendJsonAb.GetProperty("source").GetString());

            var b = await AskAsync(DatabaseFilter.One(DbB));
            Assert.Equal(ab.Stats, b.Stats);
            Assert.Equal(ab.Trend, b.Trend);

            /* Unfiltered: XE has rows, so the XE arm, as always. */
            var all = await AskAsync(DatabaseFilter.All);
            Assert.Equal(ab.Stats, all.Stats);
            Assert.Equal(ab.Trend, all.Trend);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var i = text.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = text.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
            count++;
        return count;
    }

    /// <summary>The window end and the three event minutes the blocking seeds sit on.</summary>
    private sealed record Seed(DateTime End, DateTime M1, DateTime M2, DateTime M3);

    /// <summary>
    /// Seeds both servers and returns the window end (now, to the second) and the event minutes.
    /// <para>ServerName: ONE waiting-task capture ten minutes back of five tasks: 51 (A, 3000 ms, blocked by 90), 52 (A, 2000, blocked
    /// by 91), 71 (B, 1000), 70 (C, 500, blocked by 92) and 90 (C, 4000), the blocked ones on LCK_M_S and the rest on PAGEIOLATCH_SH. XE blocked-process
    /// reports: minute m1 has A 1000 and 3000 and B 5000, minute m2 has C 7000. DMV snapshots (which must never be read while XE has
    /// rows): D 999 at m3 and A 111 at m1. A collection_log row says the blocking collector ran.</para>
    /// <para>DmvOnlyServerName: no XE row; DMV snapshots at m1 of A 100, A 200 and B 300.</para>
    /// <para>MixedServerName: one XE report for B at m1 (5000) and one DMV snapshot for A at m2 (222).</para>
    /// </summary>
    private static async Task<Seed> SeedAsync(string cs, CancellationToken ct)
    {
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, DmvOnlyServerId, DmvOnlyServerName, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, MixedServerId, MixedServerName, ct);

        var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        var capture = DarlingMcpTestData.Naive(end.AddMinutes(-10));
        var minute = new DateTime(end.Ticks - (end.Ticks % TimeSpan.TicksPerMinute), DateTimeKind.Unspecified);
        var m1 = minute.AddMinutes(-20);
        var m2 = minute.AddMinutes(-19);
        var m3 = minute.AddMinutes(-18);

        var tasks = new (int Session, string Db, int Blocker, long WaitMs)[]
        {
            (51, DbA, 90, 3000), (52, DbA, 91, 2000), (71, DbB, 0, 1000), (70, DbC, 92, 500), (90, DbC, 0, 4000),
        };
        foreach (var t in tasks)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO waiting_tasks (collection_id, collection_time, server_id, server_name, session_id, wait_type, wait_duration_ms, blocking_session_id, resource_description, database_name)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                CollectionIdGenerator.Next(), capture, ServerId, ServerName, t.Session, t.Blocker > 0 ? "LCK_M_S" : "PAGEIOLATCH_SH", t.WaitMs, t.Blocker, null, t.Db);
        }

        foreach (var (at, db, wait, spid) in new[] { (m1, DbA, 1000L, 61), (m1, DbA, 3000L, 62), (m1, DbB, 5000L, 63), (m2, DbC, 7000L, 64) })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1,$2,$3,$4,$2,$5,60,$6,'suspended',$7)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, wait, spid, db);
        }

        foreach (var (at, db, wait, spid) in new[] { (m3, DbD, 999L, 71), (m1, DbA, 111L, 72) })
        {
            await InsertDmvAsync(connection, ServerId, ServerName, at, db, wait, spid, ct);
        }

        foreach (var (db, wait, spid) in new[] { (DbA, 100L, 81), (DbA, 200L, 82), (DbB, 300L, 83) })
        {
            await InsertDmvAsync(connection, DmvOnlyServerId, DmvOnlyServerName, m1, db, wait, spid, ct);
        }

        /* MixedServerName: XE has B only (m1, 5000) and DMV has A only (m2, 222), so which arm answers depends on the filter. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name) VALUES ($1,$2,$3,$4,$2,$5,60,$6,'suspended',$7)",
            CollectionIdGenerator.Next(), m1, MixedServerId, MixedServerName, 5000L, 91, DbB);
        await InsertDmvAsync(connection, MixedServerId, MixedServerName, m2, DbA, 222L, 92, ct);

        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) VALUES ($1,$2,$3,'blocked_process_report',$4,10,'SUCCESS',0)",
            CollectionIdGenerator.Next(), ServerId, ServerName, capture);

        return new Seed(end, m1, m2, m3);
    }

    private static Task InsertDmvAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, string db, long wait, int spid, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, blocking_status) VALUES ($1,$2,$3,$4,$2,$5,$6,60,$7,'suspended')",
            CollectionIdGenerator.Next(), at, serverId, serverName, db, spid, wait);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var id in new[] { ServerId, DmvOnlyServerId, MixedServerId })
        {
            using var cleanup = new NpgsqlCommand(
                $"DELETE FROM waiting_tasks WHERE server_id = {id}; DELETE FROM blocked_process_reports WHERE server_id = {id}; "
                + $"DELETE FROM dmv_blocking_snapshots WHERE server_id = {id}; DELETE FROM collection_log WHERE server_id = {id}; "
                + $"DELETE FROM servers WHERE server_id = {id};",
                connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
