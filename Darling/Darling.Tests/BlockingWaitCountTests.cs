/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5644 W1/W2: one blocking event is one blocked session, blocker and wait start, not one blocked-process report.
/// The reports below are built the way SQL Server writes them: one every 5 seconds while a session stays blocked,
/// each carrying the wait time so far, with a few milliseconds of jitter between the event timestamp and the wait
/// time (the wait start is derived, so it drifts by that much between reports of one wait).
/// </summary>
public sealed class BlockingWaitCountTests
{
    private static readonly DateTime T0 = new(2026, 10, 9, 9, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The reports of one wait that began at <paramref name="start"/>: one every 5 s, wait times 5.0 s,
    /// 10.0 s, ... with +/- 35 ms of jitter on the derived start.</summary>
    internal static List<BlockedProcessAlertRow> OneWait(
        int blocked, int blocker, DateTime start, int reports, string obj = "db.dbo.Users",
        string source = BlockedProcessAlertRow.XeReportSource)
    {
        var rows = new List<BlockedProcessAlertRow>();
        for (var k = 1; k <= reports; k++)
        {
            var jitter = ((k * 37) % 71) - 35;
            var wait = (5000L * k) + 900;
            rows.Add(new BlockedProcessAlertRow
            {
                EventTime = start.AddMilliseconds(wait + jitter),
                DatabaseName = "db",
                BlockedSpid = blocked,
                BlockingSpid = blocker,
                WaitTimeMs = wait,
                LockMode = "X",
                BlockedSqlText = "UPDATE Users SET x = 1",
                BlockingSqlText = "BEGIN TRAN UPDATE Users",
                ContentiousObject = obj,
                Source = source,
            });
        }

        return rows;
    }

    private static List<BlockedProcessAlertRow> Collapse(IEnumerable<BlockedProcessAlertRow> rows) =>
        BlockingWaits.Collapse(rows, r => r.Source, r => r.BlockedSpid, r => r.BlockingSpid, r => r.EventTime, r => r.WaitTimeMs);

    [Fact]
    public void OneMinuteWait_ElevenReports_IsOneEvent_WithTheLongestWait()
    {
        var rows = OneWait(102, 100, T0, 11);

        var waits = Collapse(rows);

        var wait = Assert.Single(waits);
        Assert.Equal(rows.Max(r => r.WaitTimeMs), wait.WaitTimeMs);
    }

    [Fact]
    public void IssueShape_FortyThreeReportsOfFourWaits_AreFourEvents()
    {
        /* The audit's table: blocked 102, blocker 100, 11 + 9 + 9 + 6 reports. */
        var rows = new List<BlockedProcessAlertRow>();
        rows.AddRange(OneWait(102, 100, T0, 11));
        rows.AddRange(OneWait(102, 100, T0.AddMinutes(10), 9));
        rows.AddRange(OneWait(102, 100, T0.AddMinutes(20), 9));
        rows.AddRange(OneWait(102, 100, T0.AddMinutes(30), 6));
        Assert.Equal(35, rows.Count);

        var waits = Collapse(rows);

        Assert.Equal(4, waits.Count);
        Assert.Equal(new[] { 55900L, 45900L, 45900L, 30900L }.OrderBy(x => x), waits.Select(w => w.WaitTimeMs).OrderBy(x => x));
    }

    [Fact]
    public void ReportOrder_DoesNotChangeTheCount()
    {
        var rows = new List<BlockedProcessAlertRow>();
        rows.AddRange(OneWait(102, 100, T0, 11));
        rows.AddRange(OneWait(102, 100, T0.AddMinutes(5), 4));

        Assert.Equal(2, Collapse(rows).Count);
        Assert.Equal(2, Collapse(Enumerable.Reverse(rows)).Count);
        Assert.Equal(2, Collapse(rows.OrderBy(r => r.WaitTimeMs % 7)).Count);
    }

    [Fact]
    public void BackToBackWaits_OfTheSamePair_AreTwoEvents()
    {
        /* The second wait starts the instant the first one ends; its first report is 5 s later. */
        var first = OneWait(102, 100, T0, 3);
        var secondStart = first[^1].EventTime!.Value;
        var second = OneWait(102, 100, secondStart, 3);

        Assert.Equal(2, Collapse(first.Concat(second)).Count);
    }

    [Fact]
    public void DifferentSessions_DifferentBlockers_AndDifferentSources_NeverMerge()
    {
        var rows = new List<BlockedProcessAlertRow>();
        rows.AddRange(OneWait(102, 100, T0, 4));
        rows.AddRange(OneWait(103, 100, T0, 4));
        rows.AddRange(OneWait(102, 101, T0, 4));
        rows.AddRange(OneWait(102, 100, T0, 4, source: BlockedProcessAlertRow.DmvSnapshotSource));

        Assert.Equal(4, Collapse(rows).Count);
    }

    [Fact]
    public void ARowWithNoEventTime_StaysAnEventOfItsOwn()
    {
        var rows = OneWait(102, 100, T0, 3);
        rows.Add(new BlockedProcessAlertRow { BlockedSpid = 102, BlockingSpid = 100, WaitTimeMs = 9000, EventTime = null });

        Assert.Equal(2, Collapse(rows).Count);
    }

    [Fact]
    public void AlertValue_IsTheSumOfTheChainCounts_NotOneHigher()
    {
        /* W2's shape: 42 waits of one chain and 1 of another, 5 reports each. The old value counted rows and the
           card showed "Blocking chain (x42)" beside it; now both come from BlockingWaitRows. */
        var rows = new List<BlockedProcessAlertRow>();
        for (var i = 0; i < 42; i++)
        {
            rows.AddRange(OneWait(200 + i, 100, T0.AddMinutes(i * 3), 5));
        }

        rows.AddRange(OneWait(900, 901, T0.AddHours(3), 5, obj: "db.dbo.Orders"));

        var counted = AlertContextBuilders.BlockingWaitRows(rows, Array.Empty<string>());
        var context = AlertContextBuilders.BuildBlockingContext("srv", rows, Array.Empty<string>());

        Assert.Equal(43, counted.Count);
        Assert.NotNull(context);
        var shown = context!.Details
            .Where(d => d.Heading.StartsWith("Blocking chain", StringComparison.Ordinal))
            .Select(d => d.Heading == "Blocking chain" ? 1 : int.Parse(d.Heading[(d.Heading.IndexOf("(x", StringComparison.Ordinal) + 2)..^1], CultureInfo.InvariantCulture))
            .ToList();
        Assert.Contains(42, shown);
        Assert.Equal(43, shown.Sum());
    }

    [Fact]
    public async Task Engine_FiresOnTheWaitCount_AndTheValueMatchesTheCard()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.BlockingEnabled = true;
        h.Settings.BlockingCountThreshold = 1;
        var engine = h.Build();

        /* One long wait, eleven reports; before #5644 this fired with a value of 11. */
        h.Adapter.Blocking.AddRange(OneWait(102, 100, T0, 11));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("1", fired.CurrentValue);
        Assert.Equal(1, fired.NumericCurrentValue);
        var chain = Assert.Single(fired.Context!.Details, d => d.Heading.StartsWith("Blocking chain", StringComparison.Ordinal));
        Assert.Equal("Blocking chain", chain.Heading);
        var waitRange = chain.Fields.Single(f => f.Label == "Wait Range").Value;
        Assert.Contains("55", waitRange, StringComparison.Ordinal);
    }

    /* ---- #5644 review (W1/W2): the XE-or-DMV choice is made BEFORE the excluded-database filter ---- */

    private static List<BlockedProcessAlertRow> InDb(List<BlockedProcessAlertRow> rows, string db)
    {
        foreach (var r in rows) r.DatabaseName = db;
        return rows;
    }

    private static readonly string[] ExcludedOnly = { "excl" };

    [Fact]
    public void XeReportsOnlyInAnExcludedDatabase_AreNotReplacedByDmvWaitsElsewhere()
    {
        var rows = InDb(OneWait(102, 100, T0, 5), "excl");
        rows.AddRange(InDb(OneWait(202, 200, T0.AddMinutes(5), 3, source: BlockedProcessAlertRow.DmvSnapshotSource), "other"));

        Assert.Empty(AlertContextBuilders.BlockingWaitRows(rows, ExcludedOnly));
        Assert.Null(AlertContextBuilders.BuildBlockingContext("srv", rows, ExcludedOnly));
    }

    [Fact]
    public void XeReportsInANonExcludedDatabase_AreTheSource_AndDmvWaitsAreIgnored()
    {
        var rows = InDb(OneWait(102, 100, T0, 5), "other");
        rows.AddRange(InDb(OneWait(202, 200, T0.AddMinutes(5), 3, source: BlockedProcessAlertRow.DmvSnapshotSource), "other"));
        rows.AddRange(InDb(OneWait(302, 300, T0.AddMinutes(9), 3), "excl"));

        var counted = AlertContextBuilders.BlockingWaitRows(rows, ExcludedOnly);

        var wait = Assert.Single(counted);
        Assert.Equal(BlockedProcessAlertRow.XeReportSource, wait.Source);
        Assert.Equal(102, wait.BlockedSpid);
    }

    [Fact]
    public void NoXeReportsAtAll_CountsTheDmvWaits_OutsideTheExcludedDatabases()
    {
        var rows = InDb(OneWait(102, 100, T0, 1, source: BlockedProcessAlertRow.DmvSnapshotSource), "other");
        rows.AddRange(InDb(OneWait(202, 200, T0.AddMinutes(5), 1, source: BlockedProcessAlertRow.DmvSnapshotSource), "excl"));

        var wait = Assert.Single(AlertContextBuilders.BlockingWaitRows(rows, ExcludedOnly));
        Assert.Equal(BlockedProcessAlertRow.DmvSnapshotSource, wait.Source);
        Assert.Equal(102, wait.BlockedSpid);
    }

    [Fact]
    public async Task Engine_DoesNotFire_WhenTheOnlyXeReportsAreExcluded_AndDmvWaitsSitElsewhere()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.BlockingEnabled = true;
        h.Settings.BlockingCountThreshold = 1;
        h.Settings.ExcludedDatabasesList.Add("excl");
        var engine = h.Build();

        h.Adapter.Blocking.AddRange(InDb(OneWait(102, 100, T0, 5), "excl"));
        h.Adapter.Blocking.AddRange(InDb(OneWait(202, 200, T0.AddMinutes(5), 3, source: BlockedProcessAlertRow.DmvSnapshotSource), "other"));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task Engine_Fires_OnXeWaits_WhenXeIsInANonExcludedDatabase()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.BlockingEnabled = true;
        h.Settings.BlockingCountThreshold = 1;
        h.Settings.ExcludedDatabasesList.Add("excl");
        var engine = h.Build();

        h.Adapter.Blocking.AddRange(InDb(OneWait(102, 100, T0, 5), "other"));
        h.Adapter.Blocking.AddRange(InDb(OneWait(202, 200, T0.AddMinutes(5), 3, source: BlockedProcessAlertRow.DmvSnapshotSource), "other"));
        await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("1", fired.CurrentValue);
    }

    /* ---- the SQL form of the same rule (PostgreSQL): the fleet tile, the analysis fact and the summaries run it ---- */

    private static string ValuesSource(IReadOnlyList<BlockedProcessAlertRow> rows, int server) =>
        "SELECT " + server.ToString(CultureInfo.InvariantCulture) + " AS server_id, v.blocked_spid, v.blocking_spid, v.event_time, v.wait_time_ms FROM (VALUES "
        + string.Join(",", rows.Select(r => string.Format(
            CultureInfo.InvariantCulture, "({0},{1},TIMESTAMP '{2:yyyy-MM-dd HH:mm:ss.ffffff}',{3}::bigint)",
            r.BlockedSpid, r.BlockingSpid, r.EventTime, r.WaitTimeMs)))
        + ") AS v(blocked_spid, blocking_spid, event_time, wait_time_ms)";

    [Fact]
    public async Task PostgresSql_AgreesWithTheCSharpRule_OnTheSameReports()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Set DARLING_TEST_PG to run the live wait-count SQL test.");

        var rows = new List<BlockedProcessAlertRow>();
        rows.AddRange(OneWait(102, 100, T0, 11));
        rows.AddRange(OneWait(102, 100, T0.AddMinutes(10), 9));
        rows.AddRange(OneWait(102, 100, T0.AddMinutes(20), 9));
        rows.AddRange(OneWait(102, 100, T0.AddMinutes(30), 6));
        rows.AddRange(OneWait(103, 100, T0, 4));
        rows.AddRange(OneWait(102, 101, T0.AddMinutes(1), 7));

        var sql = "WITH " + BlockingWaits.WaitsCteSql(
            BlockingWaits.SqlDialect.Postgres, "waits", ValuesSource(rows, 1), new[] { "server_id" })
            + " SELECT COUNT(*), MAX(wait_time_ms) FROM waits";

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        var expected = Collapse(rows);
        Assert.Equal(expected.Count, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(6, expected.Count);
        Assert.Equal(expected.Max(w => w.WaitTimeMs), Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture));
    }

    /* ── #5644 wave-1 review: sub-millisecond starts and NULL times, in C# and in the store's SQL ── */

    internal sealed record NullableReport(int Blocked, int Blocker, DateTime? Time, long? Wait);

    /// <summary>
    /// The edge cases both stores must agree with the C# rule on. Event times are offsets from <c>T0</c> in 100 ns
    /// ticks so a 2000.4 ms difference survives (a DateTime keeps it; PostgreSQL and DuckDB timestamps keep
    /// microseconds). Expected waits: pair 1/2 two (derived starts 2000.4 ms apart split), pair 3/4 one (exactly
    /// 2000.0 ms merges), pair 5/6 four (one dated report, then a NULL event time, a NULL wait time and both NULL,
    /// each a wait of its own), pair 7/8 one (a tie on wait time keeps the LATER report).
    /// </summary>
    internal static List<NullableReport> EdgeCaseReports()
    {
        DateTime At(long ticks) => T0.AddTicks(ticks);
        const long Second = TimeSpan.TicksPerSecond;
        return new List<NullableReport>
        {
            new(1, 2, At(10 * Second), 10000), new(1, 2, At(12 * Second + 4000), 10000),
            new(3, 4, At(10 * Second), 10000), new(3, 4, At(12 * Second), 10000),
            new(5, 6, At(10 * Second), 10000), new(5, 6, null, 5000), new(5, 6, At(20 * Second), null), new(5, 6, null, null),
            new(7, 8, At(10 * Second), 10000), new(7, 8, At(10 * Second + 4000000), 10000),
        };
    }

    internal static List<NullableReport> CollapseEdgeCases(IEnumerable<NullableReport> rows) =>
        BlockingWaits.Collapse(rows, _ => "xe", r => r.Blocked, r => r.Blocker, r => r.Time, r => r.Wait);

    internal static string EdgeKey(int blocked, int blocker, DateTime? time, long? wait) =>
        string.Format(CultureInfo.InvariantCulture, "{0}|{1}|{2:yyyy-MM-dd HH:mm:ss.ffffff}|{3}", blocked, blocker, time, wait);

    internal static string EdgeValuesSource(IEnumerable<NullableReport> rows) =>
        "SELECT v.blocked_spid, v.blocking_spid, v.event_time, v.wait_time_ms FROM (VALUES "
        + string.Join(",", rows.Select(r => string.Format(
            CultureInfo.InvariantCulture, "({0},{1},CAST({2} AS TIMESTAMP),CAST({3} AS BIGINT))",
            r.Blocked, r.Blocker,
            r.Time is null ? "NULL" : $"'{r.Time:yyyy-MM-dd HH:mm:ss.ffffff}'",
            r.Wait is null ? "NULL" : r.Wait.Value.ToString(CultureInfo.InvariantCulture))))
        + ") AS v(blocked_spid, blocking_spid, event_time, wait_time_ms)";

    [Fact]
    public void EdgeCases_SubMillisecondStartsAndNullTimes_GiveTheDocumentedWaits()
    {
        var waits = CollapseEdgeCases(EdgeCaseReports());

        Assert.Equal(2, waits.Count(w => w.Blocked == 1));
        Assert.Single(waits, w => w.Blocked == 3);
        Assert.Equal(4, waits.Count(w => w.Blocked == 5));
        var tie = Assert.Single(waits, w => w.Blocked == 7);
        Assert.Equal(T0.AddTicks((10 * TimeSpan.TicksPerSecond) + 4000000), tie.Time);
    }

    [Fact]
    public async Task PostgresSql_AgreesWithTheCSharpRule_OnSubMillisecondStartsAndNullTimes()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Set DARLING_TEST_PG to run the live wait-count SQL test.");

        var sql = "WITH " + BlockingWaits.WaitsCteSql(BlockingWaits.SqlDialect.Postgres, "waits", EdgeValuesSource(EdgeCaseReports()))
            + " SELECT blocked_spid, blocking_spid, event_time, wait_time_ms FROM waits";
        var fromSql = new List<string>();
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(sql, connection);
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                fromSql.Add(EdgeKey(
                    reader.GetInt32(0), reader.GetInt32(1),
                    reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                    reader.IsDBNull(3) ? null : reader.GetInt64(3)));
            }
        }

        var fromCSharp = CollapseEdgeCases(EdgeCaseReports()).Select(w => EdgeKey(w.Blocked, w.Blocker, w.Time, w.Wait));
        Assert.Equal(fromCSharp.OrderBy(k => k, StringComparer.Ordinal), fromSql.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(8, fromSql.Count);
    }
}
