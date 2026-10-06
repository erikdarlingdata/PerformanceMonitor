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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5244 (PR3, lane R1): the blocking reads take a SET of databases. <c>get_blocking</c> and its reader filter BOTH arms (the XE
/// reports and the DMV fallback), <c>get_blocked_process_xml</c> filters the XE-with-XML read, and <c>get_blocking_trend</c> filters
/// both arms of the per-minute trend. A call with [A, B] over a seed of A, B and C returns only A and B rows, and the cap is the
/// top N of the CHOSEN databases (the newest row is C's, so a cap taken before the filter would be spent on it). The one-name
/// consumers on the empty and miss paths say "for the chosen databases" instead of giving a verdict about a database the read
/// never looked at.
/// </summary>
[Collection("live-postgres")]
public sealed class BlockingDatabaseFilterLiveTests
{
    private const string ServerName = "darling-blocking-db-filter-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string SkipReason =
        "Set DARLING_TEST_PG to a Postgres connection string to run the live blocking database-filter test (it mints its own scratch database).";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task BlockedProcessReportsReader_TwoNames_ReturnsOnlyThoseDatabases_OnBothArms_AndCapsAfterTheFilter()
    {
        await RunAsync(async (postgres, now, ct) =>
        {
            var start = now.AddHours(-1);

            var all = await DarlingBlockingReader.GetRecentBlockedProcessReportsAsync(postgres, ServerId, start, now, 50, DatabaseFilter.All, ct);
            Assert.Equal(new[] { "A", "B", "C", "D" }, all.Select(r => r.DatabaseName).Distinct().OrderBy(n => n, StringComparer.Ordinal).ToArray());
            Assert.Equal(7, all.Count);

            var chosen = await DarlingBlockingReader.GetRecentBlockedProcessReportsAsync(postgres, ServerId, start, now, 50, DatabaseFilter.Of(["A", "B"]), ct);
            Assert.Equal(new[] { "A", "B" }, chosen.Select(r => r.DatabaseName).Distinct().OrderBy(n => n, StringComparer.Ordinal).ToArray());
            /* One XE row and one DMV row per chosen database: both arms were filtered. */
            Assert.Equal(4, chosen.Count);
            Assert.Equal(2, chosen.Count(r => r.Source != BlockedProcessAlertRow.DmvSnapshotSource));

            /* The overload's one-name form is the same predicate. */
            var one = await DarlingBlockingReader.GetRecentBlockedProcessReportsAsync(postgres, ServerId, start, now, 50, DatabaseFilter.One("B"), ct);
            Assert.All(one, r => Assert.Equal("B", r.DatabaseName));
            Assert.Equal(2, one.Count);

            /* The cap runs over the chosen databases: C holds the newest XE row, so the unfiltered top row is C's. */
            var topAll = await DarlingBlockingReader.GetRecentBlockedProcessReportsAsync(postgres, ServerId, start, now, 1, DatabaseFilter.All, ct);
            Assert.Equal("C", topAll.Single().DatabaseName);
            var topChosen = await DarlingBlockingReader.GetRecentBlockedProcessReportsAsync(postgres, ServerId, start, now, 1, DatabaseFilter.Of(["A", "B"]), ct);
            Assert.Equal("A", topChosen.Single().DatabaseName);

            /* The public-shaped overload (no filter) is still every database. */
            var bare = await DarlingBlockingReader.GetRecentBlockedProcessReportsAsync(postgres, ServerId, start, now, 50, ct);
            Assert.Equal(7, bare.Count);
        });
    }

    [Fact]
    public async Task BlockedProcessReportsWithXmlReader_TwoNames_ReturnsOnlyThoseDatabases_AndCapsAfterTheFilter()
    {
        await RunAsync(async (postgres, now, ct) =>
        {
            var start = now.AddHours(-1);

            var all = await DarlingBlockingReader.GetRecentBlockedProcessReportsWithXmlAsync(postgres, ServerId, start, now, 50, DatabaseFilter.All, ct);
            Assert.Equal(3, all.Count);

            var chosen = await DarlingBlockingReader.GetRecentBlockedProcessReportsWithXmlAsync(postgres, ServerId, start, now, 50, DatabaseFilter.Of(["A", "B"]), ct);
            Assert.Equal(new[] { "A", "B" }, chosen.Select(r => r.DatabaseName).OrderBy(n => n, StringComparer.Ordinal).ToArray());

            var top = await DarlingBlockingReader.GetRecentBlockedProcessReportsWithXmlAsync(postgres, ServerId, start, now, 1, DatabaseFilter.Of(["A", "B"]), ct);
            Assert.Equal("A", top.Single().DatabaseName);
        });
    }

    [Fact]
    public async Task BlockingTrendReader_TwoNames_CountsOnlyThoseDatabases_OnBothArms()
    {
        await RunAsync(async (postgres, now, ct) =>
        {
            var start = now.AddHours(-1);

            var all = await DarlingBlockingTrendReader.GetBlockingTrendAsync(postgres, ServerId, start, now, DatabaseFilter.All, ct);
            /* XE has A, B and C: the DMV arm stays out because the XE source has rows. */
            Assert.Equal(3, all.Sum(p => p.Count));

            var chosen = await DarlingBlockingTrendReader.GetBlockingTrendAsync(postgres, ServerId, start, now, DatabaseFilter.Of(["A", "B"]), ct);
            Assert.Equal(2, chosen.Sum(p => p.Count));

            /* D has DMV-captured blocking only. The fallback asks whether XE has rows for the CHOSEN databases, so D charts. */
            var dmvOnly = await DarlingBlockingTrendReader.GetBlockingTrendAsync(postgres, ServerId, start, now, DatabaseFilter.One("D"), ct);
            Assert.Equal(1, dmvOnly.Sum(p => p.Count));

            /* A set that includes a database with XE rows keeps the XE arm alone, as an unfiltered read does. */
            var mixed = await DarlingBlockingTrendReader.GetBlockingTrendAsync(postgres, ServerId, start, now, DatabaseFilter.Of(["A", "D"]), ct);
            Assert.Equal(1, mixed.Sum(p => p.Count));

            var none = await DarlingBlockingTrendReader.GetBlockingTrendAsync(postgres, ServerId, start, now, DatabaseFilter.One("Nope"), ct);
            Assert.Empty(none);

            var bare = await DarlingBlockingTrendReader.GetBlockingTrendAsync(postgres, ServerId, start, now, ct);
            Assert.Equal(3, bare.Sum(p => p.Count));
        });
    }

    [Fact]
    public async Task GetBlocking_TwoNames_ReturnsOnlyThoseDatabases_AndTheEmptyAndMissAnswersNameTheChosenDatabases()
    {
        await RunAsync(async (postgres, now, ct) =>
        {
            using (var doc = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(
                postgres, ServerName, 1, 50, null, false, null, DatabaseFilter.Of(["A", "B"]), 150, cancellationToken: ct)))
            {
                var events = doc.RootElement.GetProperty("events").EnumerateArray().ToList();
                Assert.Equal(new[] { "A", "B" }, events.Select(e => e.GetProperty("database_name").GetString()!).Distinct().OrderBy(n => n, StringComparer.Ordinal).ToArray());
                Assert.Equal(4, events.Count);
                Assert.False(doc.RootElement.GetProperty("truncated").GetBoolean());
            }

            /* The page is the newest `limit` of the chosen databases: limit 2 of A, B leaves the other two out and says so. */
            using (var doc = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(
                postgres, ServerName, 1, 2, null, false, null, DatabaseFilter.Of(["A", "B"]), 150, cancellationToken: ct)))
            {
                Assert.Equal(2, doc.RootElement.GetProperty("events").GetArrayLength());
                Assert.True(doc.RootElement.GetProperty("truncated").GetBoolean());
                Assert.DoesNotContain(doc.RootElement.GetProperty("events").EnumerateArray(), e => e.GetProperty("database_name").GetString() == "C");
            }

            /* Empty: the status word stays, and the sentence is about the chosen databases only. */
            var emptyOne = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(
                postgres, ServerName, 1, 50, null, false, null, DatabaseFilter.One("Nope"), 150, cancellationToken: ct)).RootElement;
            Assert.Equal("empty", emptyOne.GetProperty("status").GetString());
            Assert.Contains("for the database Nope", emptyOne.GetProperty("message").GetString(), StringComparison.Ordinal);

            var emptyMany = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(
                postgres, ServerName, 1, 50, null, false, null, DatabaseFilter.Of(["Nope", "Gone"]), 150, cancellationToken: ct)).RootElement;
            Assert.Equal("empty", emptyMany.GetProperty("status").GetString());
            Assert.Contains("for the chosen databases", emptyMany.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.DoesNotContain("Nope", emptyMany.GetProperty("message").GetString(), StringComparison.Ordinal);

            /* Unfiltered, the sentence is the one it always was. */
            var emptyAll = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(
                postgres, ServerName, 1, 50, null, false, now.AddDays(-30).ToString("o"), DatabaseFilter.All, 150, cancellationToken: ct)).RootElement;
            Assert.Equal("empty", emptyAll.GetProperty("status").GetString());
            Assert.Equal("No blocking events found in the specified time range.", emptyAll.GetProperty("message").GetString());

            /* A dedup_key from A's incident, asked over B alone: a miss that says the scan was limited to the chosen databases. */
            string aKey;
            using (var doc = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(
                postgres, ServerName, 1, 50, null, false, null, DatabaseFilter.One("A"), 150, cancellationToken: ct)))
            {
                aKey = doc.RootElement.GetProperty("events").EnumerateArray().First().GetProperty("dedup_key").GetString()!;
            }

            using (var hit = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(
                postgres, ServerName, 1, 50, aKey, false, null, DatabaseFilter.One("A"), 150, cancellationToken: ct)))
            {
                Assert.True(hit.RootElement.GetProperty("events").GetArrayLength() >= 1);
            }

            var miss = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(
                postgres, ServerName, 1, 50, aKey, false, null, DatabaseFilter.One("B"), 150, cancellationToken: ct)).RootElement;
            Assert.Equal("empty", miss.GetProperty("status").GetString());
            Assert.Contains("The scan was limited to the events for the database B", miss.GetProperty("message").GetString(), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task GetBlockedProcessXml_TwoNames_ReturnsOnlyThoseDatabases_AndTheEmptyAnswerNamesTheChosenDatabases()
    {
        await RunAsync(async (postgres, now, ct) =>
        {
            using (var doc = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlockedProcessXml(
                postgres, ServerName, 1, 50, null, DatabaseFilter.Of(["A", "B"]), cancellationToken: ct)))
            {
                var reports = doc.RootElement.GetProperty("reports").EnumerateArray().ToList();
                Assert.Equal(new[] { "A", "B" }, reports.Select(e => e.GetProperty("database_name").GetString()!).OrderBy(n => n, StringComparer.Ordinal).ToArray());
            }

            using (var doc = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlockedProcessXml(
                postgres, ServerName, 1, 1, null, DatabaseFilter.Of(["A", "B"]), cancellationToken: ct)))
            {
                Assert.Equal("A", doc.RootElement.GetProperty("reports")[0].GetProperty("database_name").GetString());
                Assert.True(doc.RootElement.GetProperty("truncated").GetBoolean());
            }

            var empty = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlockedProcessXml(
                postgres, ServerName, 1, 50, null, DatabaseFilter.Of(["Nope", "Gone"]), cancellationToken: ct)).RootElement;
            Assert.Equal("empty", empty.GetProperty("status").GetString());
            Assert.Contains("for the chosen databases", empty.GetProperty("message").GetString(), StringComparison.Ordinal);

            var emptyOne = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlockedProcessXml(
                postgres, ServerName, 1, 50, null, DatabaseFilter.One("Nope"), cancellationToken: ct)).RootElement;
            Assert.Contains("for the database Nope", emptyOne.GetProperty("message").GetString(), StringComparison.Ordinal);

            var emptyAll = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlockedProcessXml(
                postgres, ServerName, 1, 50, now.AddDays(-30).ToString("o"), DatabaseFilter.All, cancellationToken: ct)).RootElement;
            Assert.Equal("No blocked process report XML available in the specified time range.", emptyAll.GetProperty("message").GetString());
        });
    }

    [Fact]
    public async Task GetBlockingTrend_TwoNames_CountsOnlyThoseDatabases_AndTheEmptyAnswerNamesTheChosenDatabases()
    {
        await RunAsync(async (postgres, now, ct) =>
        {
            using (var doc = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlockingTrend(
                postgres, ServerName, 1, null, DatabaseFilter.Of(["A", "B"]), ct)))
            {
                Assert.Equal(2, doc.RootElement.GetProperty("trend").EnumerateArray().Sum(p => p.GetProperty("count").GetInt32()));
            }

            /* The collectors ran (seeded below) and none of the chosen databases' blocking was in the captures: an all-clear
               for THOSE databases, worded that way, with the status word unchanged. */
            var one = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlockingTrend(
                postgres, ServerName, 1, null, DatabaseFilter.One("Nope"), ct)).RootElement;
            Assert.Equal("empty", one.GetProperty("status").GetString());
            Assert.Contains("for the database Nope in the last 1 hour(s)", one.GetProperty("message").GetString(), StringComparison.Ordinal);

            var many = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlockingTrend(
                postgres, ServerName, 1, null, DatabaseFilter.Of(["Nope", "Gone"]), ct)).RootElement;
            Assert.Equal("empty", many.GetProperty("status").GetString());
            Assert.Contains("for the chosen databases in the last 1 hour(s)", many.GetProperty("message").GetString(), StringComparison.Ordinal);

            /* A window the collectors never ran in stays "unavailable" whatever the selection (no capture is no all-clear). */
            var gap = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlockingTrend(
                postgres, ServerName, 1, now.AddDays(-20).ToString("o"), DatabaseFilter.One("Nope"), ct)).RootElement;
            Assert.Equal("unavailable", gap.GetProperty("status").GetString());
        });
    }

    /// <summary>The database predicate is the list form on every statement the three reads route to (no live store needed): the XE reads at
    /// $6 (after the window floor at $5), the DMV fallback at $5, and BOTH arms of the trend at $5 (after the floor at $4), and
    /// none of them is spliced with a name.</summary>
    [Fact]
    public void EveryBlockingStatement_CarriesTheListPredicate_OnEveryArm()
    {
        var xe = DatabaseFilter.All.Clause("database_name", 6);
        Assert.Contains(xe, DarlingBlockingReader.BlockedProcessReportsSql, StringComparison.Ordinal);
        Assert.Contains(xe, DarlingBlockingReader.BlockedProcessReportsWithXmlSql, StringComparison.Ordinal);
        Assert.Contains(DatabaseFilter.All.Clause("database_name", 5), DarlingBlockingReader.DmvBlockingSnapshotsSql, StringComparison.Ordinal);

        var trend = DatabaseFilter.All.Clause("database_name", 5);
        var first = DarlingBlockingTrendReader.BlockingTrendSql.IndexOf(trend, StringComparison.Ordinal);
        Assert.True(first >= 0, "the XE arm of the trend");
        Assert.True(DarlingBlockingTrendReader.BlockingTrendSql.IndexOf(trend, first + trend.Length, StringComparison.Ordinal) > first, "the DMV arm of the trend");
    }

    /// <summary>Seeds A, B and C, then runs the body. XE rows (all with a report XML), newest first: C at -5 min, A at -10, B at -11.
    /// DMV rows, with spid pairs no XE row covers: one each for A, B and C, and a D whose only blocking is DMV-captured.</summary>
    private static async Task RunAsync(Func<NpgsqlDataSource, DateTime, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;

        // #1776 own-store: a scratch database, so no other class's chunks shape the store under test (#4650).
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var cs = scratch.ConnectionString;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PrepareStoreAsync(cs, connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            var xe = new (string Database, int Minutes, int Spid)[] { ("C", 5, 103), ("A", 10, 101), ("B", 11, 102) };
            foreach (var (database, minutes, spid) in xe)
            {
                var eventTime = DarlingMcpTestData.Naive(now.AddMinutes(-minutes));
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocked_ecid, blocking_spid, blocking_ecid, wait_time_ms, lock_mode,
     blocked_sql_text, blocking_sql_text, blocked_process_report_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,0,$8,0,1000,'X','SELECT 1','UPDATE t SET c = 1','<blocked-process-report/>')",
                    CollectionIdGenerator.Next(), eventTime.AddSeconds(30), ServerId, ServerName, eventTime, database, spid, 900 + spid);
            }

            var dmv = new (string Database, int Minutes, int Spid)[] { ("C", 20, 303), ("A", 21, 301), ("B", 22, 302), ("D", 23, 304) };
            foreach (var (database, minutes, spid) in dmv)
            {
                var at = DarlingMcpTestData.Naive(now.AddMinutes(-minutes));
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO dmv_blocking_snapshots
    (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, blocking_status)
VALUES ($1,$2,$3,$4,$2,$5,$6,60,12000,'suspended')",
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, database, spid);
            }

            /* Collector runs that looked at the last hour, so an empty trend is an all-clear and not a gap. */
            foreach (var collector in new[] { "blocked_process_report", "dmv_blocking_snapshot" })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 100, 'SUCCESS', NULL, 0, 80, 20)",
                    CollectionIdGenerator.Next(), ServerId, ServerName, collector, DarlingMcpTestData.Naive(now.AddMinutes(-30)));
            }

            await body(postgres, now, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task PrepareStoreAsync(string connectionString, NpgsqlConnection connection, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);
        if (await LiveTimescaleProbe.TryEnableAsync(connectionString, ct))
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await using var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection);
            await stop.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM blocked_process_reports WHERE server_id = {ServerId}; DELETE FROM dmv_blocking_snapshots WHERE server_id = {ServerId}; DELETE FROM collection_log WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
