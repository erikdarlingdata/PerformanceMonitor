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
/// #5245 (part of #5244), lane R2a: get_active_queries reads a SET of databases. The reader and the tool each gain an
/// internal overload that takes a <see cref="DatabaseFilter"/>; the public method keeps passing one name. This class pins
/// the overload over a seed of three databases (A, B and C): [A, B] returns only A and B rows, one name behaves as it did,
/// and every place the tool used the one name says something sensible for a list (the echoed <c>database_name</c>, the
/// empty-window sentence, <c>blocker_not_shown</c>, and the old trim of the name).
///
/// <para>The tool has ONE tier (the base <c>query_snapshots</c> table), so "every tier" is the one statement. Skips
/// without <c>DARLING_TEST_PG</c>, like its siblings.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ActiveQueriesDatabaseFilterLiveTests
{
    private const string ServerName = "darling-active-queries-dbfilter-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string DbA = "FilterDbA";
    private const string DbB = "FilterDbB";
    private const string DbC = "FilterDbC";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void ActiveQueriesSql_IsTheListForm_WithNoNameSplicedIn()
    {
        var sql = DarlingSessionReader.ActiveQueriesSql;
        Assert.Contains("($5::text[] IS NULL OR w.database_name = ANY($5))", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$5::text IS NULL", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("{{", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DescribeActiveQueryFilters_OneName_IsTheSentenceItAlwaysWas_ManyNamesSayTheChosenDatabases()
    {
        Assert.Equal("blocking_only", DarlingMcpSessionTools.DescribeActiveQueryFilters(DatabaseFilter.All, true));
        Assert.Equal("database_name 'X'", DarlingMcpSessionTools.DescribeActiveQueryFilters(DatabaseFilter.One("X"), false));
        Assert.Equal("database_name 'X' with blocking_only", DarlingMcpSessionTools.DescribeActiveQueryFilters(DatabaseFilter.One("X"), true));
        Assert.Equal("the chosen databases", DarlingMcpSessionTools.DescribeActiveQueryFilters(DatabaseFilter.Of(["X", "Y"]), false));
        Assert.Equal("the chosen databases with blocking_only", DarlingMcpSessionTools.DescribeActiveQueryFilters(DatabaseFilter.Of(["X", "Y"]), true));
        /* #5235: the wait_type clause comes last, and the order and wording match what Lite says. */
        Assert.Equal("wait_type 'LCK_M_S'", DarlingMcpSessionTools.DescribeActiveQueryFilters(DatabaseFilter.All, false, "LCK_M_S"));
        Assert.Equal("database_name 'X' with blocking_only with wait_type 'LCK_M_S'", DarlingMcpSessionTools.DescribeActiveQueryFilters(DatabaseFilter.One("X"), true, "LCK_M_S"));
        Assert.Equal("the chosen databases with wait_type 'LCK_M_S'", DarlingMcpSessionTools.DescribeActiveQueryFilters(DatabaseFilter.Of(["X", "Y"]), false, "LCK_M_S"));
    }

    [Fact]
    public async Task Reader_Overload_TwoNames_ReturnsOnlyThoseDatabases_OneNameAndAllBehaveAsBefore()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live active-queries database-filter test.");
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, ct);
            var start = end.AddHours(-1);

            var ab = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, start, end, 100, DatabaseFilter.Of([DbA, DbB]), false, null, ct);
            Assert.Equal(new[] { 51, 52, 71, 72, 91 }, ab.Rows.Select(r => r.SessionId).Order().ToArray());
            Assert.All(ab.Rows, r => Assert.Contains(r.DatabaseName, new[] { DbA, DbB }));
            Assert.Equal(5, ab.PopulationCount);

            /* The order of the names does not matter. */
            var ba = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, start, end, 100, DatabaseFilter.Of([DbB, DbA]), false, null, ct);
            Assert.Equal(ab.Rows.Select(r => r.SessionId), ba.Rows.Select(r => r.SessionId));

            /* One name: the overload and the string method that wraps it return the same page. */
            var oneList = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, start, end, 100, DatabaseFilter.One(DbC), false, null, ct);
            var oneString = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, start, end, 100, DbC, false, null, ct);
            Assert.Equal(new[] { 70, 90 }, oneList.Rows.Select(r => r.SessionId).Order().ToArray());
            Assert.Equal(oneList.Rows.Select(r => r.SessionId), oneString.Rows.Select(r => r.SessionId));
            Assert.Equal(2, oneList.PopulationCount);

            /* All: every database, and the empty filter is the same as no filter. */
            var all = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, start, end, 100, DatabaseFilter.All, false, null, ct);
            Assert.Equal(7, all.Rows.Count);
            Assert.Equal(7, all.PopulationCount);

            /* blocking_only composes with the list: victims in the set plus their same-capture head blockers (91; 90 is in C). */
            var blocking = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, start, end, 100, DatabaseFilter.Of([DbA, DbB]), true, null, ct);
            Assert.Equal(new[] { 51, 52, 91 }, blocking.Rows.Select(r => r.SessionId).Order().ToArray());

            /* #5235 beside #5245: the wait_type filter ANDs with the list (the seed's only waiters are the victims 51 and 52, in A, on LCK_M_S; the
               case is ignored), and a list that holds neither victim leaves nothing. */
            var waitInList = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, start, end, 100, DatabaseFilter.Of([DbA, DbB]), false, "lck_m_s", ct);
            Assert.Equal(new[] { 51, 52 }, waitInList.Rows.Select(r => r.SessionId).Order().ToArray());
            Assert.Equal(2, waitInList.PopulationCount);
            var waitOutOfList = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, start, end, 100, DatabaseFilter.Of([DbB, DbC]), false, "LCK_M_S", ct);
            Assert.Empty(waitOutOfList.Rows);
            var waitAlone = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, start, end, 100, DatabaseFilter.All, false, "LCK_M_S", ct);
            Assert.Equal(new[] { 51, 52 }, waitAlone.Rows.Select(r => r.SessionId).Order().ToArray());

            /* A name no row carries, or a name with different spelling, finds nothing: the compare is exact. */
            var none = await DarlingSessionReader.GetActiveQueriesAsync(postgres, ServerId, start, end, 100, DatabaseFilter.Of(["NoSuchDb", DbA.ToLowerInvariant()]), false, null, ct);
            Assert.Empty(none.Rows);
            Assert.Equal(0, none.PopulationCount);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task Tool_Overload_TwoNames_EchoesTheSet_AndAHeadBlockerOutsideItStaysOffThePage()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live active-queries database-filter test.");
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, ct);
            var asOf = end.ToString("o");

            var json = await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 1, DatabaseFilter.Of([DbA, DbB]), false, null, 25, 400, asOf, null, ct);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            /* The population is the set's rows only, counted in SQL. */
            Assert.Equal(5, root.GetProperty("total_snapshots").GetInt64());
            Assert.Equal(5, root.GetProperty("snapshots_returned").GetInt32());
            Assert.False(root.GetProperty("truncated").GetBoolean());
            var rows = root.GetProperty("queries").EnumerateArray().ToList();
            Assert.All(rows, r => Assert.Contains(r.GetProperty("database_name").GetString(), new[] { DbA, DbB }));

            /* [M4] the echoed database_name names the set, not a single database. */
            var filters = root.GetProperty("filters_applied");
            Assert.Equal("the chosen databases", filters.GetProperty("database_name").GetString());
            Assert.False(filters.GetProperty("blocking_only").GetBoolean());

            /* [M4] BlockerNotShown: session 51's head blocker (90) is in database C, outside the set, so it is not on the page
               and the victim says "filtered"; session 52's head blocker (91) is in B, inside the set, so it is on the page. */
            var s51 = rows.Single(r => r.GetProperty("session_id").GetInt32() == 51);
            Assert.Equal("filtered", s51.GetProperty("blocker_not_shown").GetString());
            var s52 = rows.Single(r => r.GetProperty("session_id").GetInt32() == 52);
            Assert.Equal(JsonValueKind.Null, s52.GetProperty("blocker_not_shown").ValueKind);
            var s91 = rows.Single(r => r.GetProperty("session_id").GetInt32() == 91);
            Assert.True(s91.GetProperty("is_head_blocker").GetBoolean());
            Assert.DoesNotContain(rows, r => r.GetProperty("session_id").GetInt32() == 90);

            /* Truncation is measured against the set's population: two of five rows, the page says more is held. */
            var cut = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(
                postgres, ServerName, 1, DatabaseFilter.Of([DbA, DbB]), false, null, 2, 400, asOf, null, ct)).RootElement;
            Assert.True(cut.GetProperty("truncated").GetBoolean());
            Assert.Equal(5, cut.GetProperty("total_snapshots").GetInt64());
            Assert.Equal(2, cut.GetProperty("snapshots_returned").GetInt32());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task Tool_OneName_KeepsTheEchoAndTheEmptyWindowSentence_AndTheNameIsNoLongerTrimmed()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live active-queries database-filter test.");
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var end = await SeedAsync(cs!, ct);
            var asOf = end.ToString("o");

            /* One name: the echo is the name itself, exactly as before the list. */
            var one = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(
                postgres, ServerName, 1, DbC, false, null, 25, 400, asOf, null, ct)).RootElement;
            Assert.Equal(DbC, one.GetProperty("filters_applied").GetProperty("database_name").GetString());
            Assert.Equal(2, one.GetProperty("total_snapshots").GetInt64());

            /* No name (null, and a whitespace-only one, which used to trim to null too): the echo is null and every row is read. */
            foreach (var blank in new string?[] { null, "   " })
            {
                var all = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(
                    postgres, ServerName, 1, blank, false, null, 25, 400, asOf, null, ct)).RootElement;
                Assert.Equal(JsonValueKind.Null, all.GetProperty("filters_applied").GetProperty("database_name").ValueKind);
                Assert.Equal(7, all.GetProperty("total_snapshots").GetInt64());
            }

            /* [M3] The name is no longer trimmed: ' FilterDbA' is a different name from 'FilterDbA' and finds nothing. */
            var padded = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(
                postgres, ServerName, 1, " " + DbA, false, null, 25, 400, asOf, null, ct)).RootElement;
            Assert.Equal("empty", padded.GetProperty("status").GetString());
            Assert.Contains($"matched database_name ' {DbA}'.", padded.GetProperty("message").GetString(), StringComparison.Ordinal);

            /* The empty-window sentence for one name is the one it always was; for a set it names "the chosen databases". */
            var oneMiss = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(
                postgres, ServerName, 1, "NoSuchDb", true, null, 25, 400, asOf, null, ct)).RootElement;
            Assert.Equal("empty", oneMiss.GetProperty("status").GetString());
            Assert.Contains("matched database_name 'NoSuchDb' with blocking_only. The filters were applied in SQL", oneMiss.GetProperty("message").GetString(), StringComparison.Ordinal);

            var manyMiss = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(
                postgres, ServerName, 1, DatabaseFilter.Of(["NoSuchDb", "NorThisOne"]), false, null, 25, 400, asOf, null, ct)).RootElement;
            Assert.Equal("empty", manyMiss.GetProperty("status").GetString());
            var manyMessage = manyMiss.GetProperty("message").GetString()!;
            Assert.Contains("matched the chosen databases. The filters were applied in SQL", manyMessage, StringComparison.Ordinal);
            Assert.DoesNotContain("database_name '", manyMessage, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// Seeds ONE capture (ten minutes before now) of seven sessions across databases A, B and C and returns the window end
    /// (now, a little after the capture). Sessions 51 and 52 are victims in A; 51 is blocked by 90 (a head blocker in C, outside
    /// [A, B]) and 52 by 91 (a head blocker in B, inside it). 70 (C), 71 (B) and 72 (A) are plain. CPU descends with the session
    /// number so the page order is fixed.
    /// </summary>
    private static async Task<DateTime> SeedAsync(string cs, CancellationToken ct)
    {
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        var capture = DarlingMcpTestData.Naive(end.AddMinutes(-10));
        var sessions = new (int Session, string Db, int Blocker, long Cpu)[]
        {
            (51, DbA, 90, 7000), (52, DbA, 91, 6000), (91, DbB, 0, 5000), (90, DbC, 0, 4000),
            (70, DbC, 0, 3000), (71, DbB, 0, 2000), (72, DbA, 0, 1000),
        };
        foreach (var s in sessions)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)",
                CollectionIdGenerator.Next(), capture, ServerId, ServerName, s.Session, s.Db, "00 00:00:05.000",
                $"SELECT {s.Session} FROM dbo.Posts", "running", s.Blocker, s.Blocker > 0 ? "LCK_M_S" : null, s.Blocker > 0 ? 500L : 0L,
                s.Cpu, s.Cpu, 100L, 0L, 2000L, 0.5m, "Read Committed", 1, 0, "sa", "APP01", "SSMS", 1, 0);
        }

        return end;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_snapshots WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
