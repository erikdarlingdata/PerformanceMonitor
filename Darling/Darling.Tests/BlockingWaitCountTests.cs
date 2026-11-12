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
[Collection("live-postgres")]
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

    /// <summary>
    /// #6645: the threads of one parallel query write reports with blocked spid = blocking spid (different ecid).
    /// That is the query waiting on itself, so it is no wait: the rule drops it and the alert rows follow.
    /// </summary>
    [Fact]
    public void ParallelQuerySelfWaits_AreNotWaits()
    {
        var rows = OneWait(59, 59, T0, 6);
        rows.AddRange(OneWait(59, 59, T0.AddMinutes(1), 3));
        var real = OneWait(102, 100, T0.AddMinutes(2), 5);
        rows.AddRange(real);

        var wait = Assert.Single(Collapse(rows));
        Assert.Equal(real.Max(r => r.WaitTimeMs), wait.WaitTimeMs);
        Assert.Equal(100, wait.BlockingSpid);

        var counted = AlertContextBuilders.BlockingWaitRows(rows, Array.Empty<string>());
        Assert.Equal(100, Assert.Single(counted).BlockingSpid);
        Assert.Empty(AlertContextBuilders.BlockingWaitRows(OneWait(59, 59, T0, 6), Array.Empty<string>()));

        /* A report that named no session (the collector stores 0, never NULL) is still a blocking event. */
        Assert.Single(AlertContextBuilders.BlockingWaitRows(OneWait(0, 0, T0, 1), Array.Empty<string>()));
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

    /// <summary>#6645 round 1: XE reports that are all a parallel query's self-waits are no XE reports, so the DMV
    /// waits beside them are the source, as in the SQL read surfaces.</summary>
    [Fact]
    public void XeReportsThatAreAllSelfWaits_FallBackToTheDmvWaits()
    {
        var rows = OneWait(59, 59, T0, 6);
        rows.AddRange(OneWait(202, 200, T0.AddMinutes(5), 3, source: BlockedProcessAlertRow.DmvSnapshotSource));

        var wait = Assert.Single(AlertContextBuilders.BlockingWaitRows(rows, Array.Empty<string>()));
        Assert.Equal(BlockedProcessAlertRow.DmvSnapshotSource, wait.Source);
        Assert.Equal(200, wait.BlockingSpid);
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
        /* #6645 round 1: a report that named no session is stored as (0, 0) and is a wait in the C# rule, so the SQL
           must keep it; a parallel query's equal non-zero spids are dropped by both. */
        rows.AddRange(OneWait(0, 0, T0.AddMinutes(40), 3));
        rows.AddRange(OneWait(59, 59, T0.AddMinutes(50), 4));

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
        Assert.Equal(7, expected.Count);
        Assert.Equal(expected.Max(w => w.WaitTimeMs), Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task PostgresSql_LeavesOutParallelQuerySelfWaits_AndCountsTheRealOne()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Set DARLING_TEST_PG to run the live wait-count SQL test.");

        var rows = new List<BlockedProcessAlertRow>();
        rows.AddRange(OneWait(59, 59, T0, 6));
        rows.AddRange(OneWait(59, 59, T0.AddMinutes(10), 4));
        rows.AddRange(OneWait(102, 100, T0.AddMinutes(20), 5));

        /* #6645: reportCount adds the per-wait report tally, which must count only the real wait's reports. */
        var sql = "WITH " + BlockingWaits.WaitsCteSql(
            BlockingWaits.SqlDialect.Postgres, "waits", ValuesSource(rows, 1), new[] { "server_id" }, reportCount: true)
            + " SELECT COUNT(*), COUNT(DISTINCT blocking_spid), MAX(wait_reports) FROM waits";

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        Assert.Equal(1, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(5, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
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

    /* ── #5743: DMV snapshot rows, where a wait of any length is caught ── */

    /// <summary>
    /// DMV snapshot shapes, milliseconds from <c>T0</c> (a row is event time and wait time; its derived start is
    /// event minus wait). Expected waits per blocked session are in <see cref="DmvExpectedWaits"/>.
    /// 21: the issue's example (wait 1.5 s seen at T, the pair blocks again 200 ms later and is seen 10 s later at
    /// 9.8 s: starts 1.7 s apart, under the 2 s tolerance, 200 ms after the first wait was last seen) is TWO waits.
    /// 22: a continuing wait first seen at 300 ms and again 10.04 s later at 10.3 s (start jitter +40 ms) is one.
    /// 23: first seen at 10 ms, again with a derived start +50 ms later (inside <see cref="BlockingWaits.WaitStartJitterMs"/>): one.
    /// 24: derived start exactly 150 ms after the earlier row's event time still merges (the comparison is strict): one.
    /// 25: 151 ms after it splits: two.
    /// 26: three rows of one continuing wait whose derived starts jitter by up to 120 ms and whose event order
    /// differs from the start order: one.
    /// 27: the running maximum covers ALL preceding rows, not only the previous one: the third row's start is 200 ms
    /// after the second row's event, but the first row (earlier start, later event) saw the wait alive: one.
    /// </summary>
    internal static List<NullableReport> DmvSnapshotCases()
    {
        DateTime At(long ms) => T0.AddMilliseconds(ms);
        const long T = 10_000;
        return new List<NullableReport>
        {
            new(21, 20, At(T), 1500), new(21, 20, At(T + 10_000), 9800),
            new(22, 20, At(T), 300), new(22, 20, At(T + 10_040), 10300),
            new(23, 20, At(T), 10), new(23, 20, At(T + 10_000), 9950),
            new(24, 20, At(T), 10), new(24, 20, At(T + 10_000), 9850),
            new(25, 20, At(T), 10), new(25, 20, At(T + 10_000), 9849),
            new(26, 20, At(T), 20), new(26, 20, At(T + 10_000), 9990), new(26, 20, At(T + 5_000), 4900),
            new(27, 20, At(T + 9_000), 10_000), new(27, 20, At(T - 800), 100), new(27, 20, At(T + 9_400), 10_000),
        };
    }

    internal static readonly IReadOnlyDictionary<int, int> DmvExpectedWaits = new Dictionary<int, int>
    {
        [21] = 2, [22] = 1, [23] = 1, [24] = 1, [25] = 2, [26] = 1, [27] = 1,
    };

    [Fact]
    public void DmvSnapshots_AWaitCannotStartBeforeThePreviousWasLastSeen()
    {
        var waits = CollapseEdgeCases(DmvSnapshotCases());

        foreach (var (blocked, expected) in DmvExpectedWaits)
        {
            Assert.Equal(expected, waits.Count(w => w.Blocked == blocked));
        }
    }

    [Fact]
    public async Task PostgresSql_AgreesWithTheCSharpRule_OnDmvSnapshotRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Set DARLING_TEST_PG to run the live wait-count SQL test.");

        var sql = "WITH " + BlockingWaits.WaitsCteSql(BlockingWaits.SqlDialect.Postgres, "waits", EdgeValuesSource(DmvSnapshotCases()))
            + " SELECT blocked_spid, blocking_spid, event_time, wait_time_ms FROM waits";
        var fromSql = new List<string>();
        var perPair = new Dictionary<int, int>();
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var command = new NpgsqlCommand(sql, connection);
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                fromSql.Add(EdgeKey(reader.GetInt32(0), reader.GetInt32(1), reader.GetDateTime(2), reader.GetInt64(3)));
                perPair[reader.GetInt32(0)] = perPair.GetValueOrDefault(reader.GetInt32(0)) + 1;
            }
        }

        var fromCSharp = CollapseEdgeCases(DmvSnapshotCases()).Select(w => EdgeKey(w.Blocked, w.Blocker, w.Time, w.Wait));
        Assert.Equal(fromCSharp.OrderBy(k => k, StringComparer.Ordinal), fromSql.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(DmvExpectedWaits.OrderBy(p => p.Key), perPair.OrderBy(p => p.Key));
    }

    /// <summary>
    /// #7028: the per-hour source choice (<see cref="BlockingWaits.HourlyChosenWaitsSql"/>, in its single-bucket form
    /// the alert's rolling hour) counts the same waits as the alert's own C# rule
    /// (<see cref="AlertContextBuilders.BlockingWaitRows"/>) on the same rows: an hour with blocked-process waits
    /// takes only those, an hour with DMV waits alone takes those, and a parallel query's self-wait is no report, so
    /// an hour whose only blocked-process rows are self-waits falls back to the DMV waits.
    /// </summary>
    [Fact]
    public async Task PostgresHourlyChooser_AgreesWithTheAlertsCSharpRule_OnXeOnlyDmvOnlyMixedAndSelfWaitHours()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Set DARLING_TEST_PG to run the live wait-count SQL test.");

        const string dmv = BlockedProcessAlertRow.DmvSnapshotSource;
        var cases = new (string Name, List<BlockedProcessAlertRow> Rows, int Expected)[]
        {
            ("XE only", Concat(OneWait(102, 100, T0, 6), OneWait(103, 100, T0.AddMinutes(5), 4)), 2),
            ("DMV only", Concat(OneWait(102, 100, T0, 6, source: dmv), OneWait(103, 100, T0.AddMinutes(5), 4, source: dmv), OneWait(104, 100, T0.AddMinutes(9), 3, source: dmv)), 3),
            ("a mix: the XE waits win", Concat(OneWait(102, 100, T0, 6), OneWait(103, 100, T0.AddMinutes(5), 4, source: dmv), OneWait(104, 100, T0.AddMinutes(9), 3, source: dmv), OneWait(105, 100, T0.AddMinutes(20), 3, source: dmv)), 1),
            ("self waits only in XE: the DMV waits count", Concat(OneWait(59, 59, T0, 5), OneWait(103, 100, T0.AddMinutes(5), 4, source: dmv), OneWait(104, 100, T0.AddMinutes(9), 3, source: dmv)), 2),
            ("a real XE wait beside a self wait", Concat(OneWait(59, 59, T0, 5), OneWait(102, 100, T0.AddMinutes(5), 4), OneWait(103, 100, T0.AddMinutes(9), 3, source: dmv)), 1),
            ("DMV self waits are no waits", Concat(OneWait(59, 59, T0, 5, source: dmv), OneWait(103, 100, T0.AddMinutes(9), 3, source: dmv)), 1),
        };

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        foreach (var (name, rows, expected) in cases)
        {
            var xe = rows.Where(r => r.Source == BlockedProcessAlertRow.XeReportSource).ToList();
            var dmvRows = rows.Where(r => r.Source == dmv).ToList();
            var sql = "WITH " + BlockingWaits.WaitsCteSql(BlockingWaits.SqlDialect.Postgres, "xe_waits", SourceOrEmpty(xe), new[] { "server_id" })
                + "," + BlockingWaits.WaitsCteSql(BlockingWaits.SqlDialect.Postgres, "dmv_waits", SourceOrEmpty(dmvRows), new[] { "server_id" })
                + "," + BlockingWaits.HourlyChosenWaitsSql("chosen", "xe_waits", "dmv_waits", "TIMESTAMP '2026-10-09 10:00:00'", singleBucket: true, new[] { "server_id" })
                + " SELECT COUNT(*), COALESCE(MAX(wait_time_ms), 0) FROM chosen";
            await using var command = new NpgsqlCommand(sql, connection);
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());

            var alertWaits = AlertContextBuilders.BlockingWaitRows(rows, Array.Empty<string>());
            Assert.True(expected == alertWaits.Count, $"{name}: the alert's C# rule counts {alertWaits.Count}, expected {expected}");
            Assert.True(
                alertWaits.Count == Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture),
                $"{name}: the SQL chooser counts {reader.GetValue(0)}, the alert's C# rule {alertWaits.Count}");
            Assert.Equal(alertWaits.Count == 0 ? 0 : alertWaits.Max(w => w.WaitTimeMs), Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture));
        }
    }

    private static string SourceOrEmpty(IReadOnlyList<BlockedProcessAlertRow> rows) => rows.Count > 0
        ? ValuesSource(rows, 1)
        : "SELECT 1 AS server_id, NULL::integer AS blocked_spid, NULL::integer AS blocking_spid, NULL::timestamp AS event_time, NULL::bigint AS wait_time_ms WHERE FALSE";

    private static List<BlockedProcessAlertRow> Concat(params List<BlockedProcessAlertRow>[] lists) => lists.SelectMany(l => l).ToList();

    /// <summary>
    /// Blocked-process reports are untouched by #5743: two waits whose starts are 6 s apart are two waits, one
    /// eleven-report wait is one, and <c>wait_reports</c> still counts each wait's reports (1 and 2, then 11).
    /// </summary>
    [Fact]
    public async Task BlockedProcessReports_KeepTheirWaitCounts_AndReportCounts()
    {
        var rows = new List<BlockedProcessAlertRow>();
        rows.AddRange(OneWait(102, 100, T0, 1));
        rows.AddRange(OneWait(102, 100, T0.AddSeconds(6), 2));
        rows.AddRange(OneWait(103, 100, T0, 11));

        var collapsed = Collapse(rows);
        Assert.Equal(2, collapsed.Count(w => w.BlockedSpid == 102));
        Assert.Equal(1, collapsed.Count(w => w.BlockedSpid == 103));

        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Set DARLING_TEST_PG to run the live wait-count SQL test.");

        var sql = "WITH " + BlockingWaits.WaitsCteSql(
            BlockingWaits.SqlDialect.Postgres, "waits", ValuesSource(rows, 1), new[] { "server_id" }, reportCount: true)
            + " SELECT blocked_spid, wait_reports FROM waits ORDER BY blocked_spid, wait_reports";
        var fromSql = new List<string>();
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            fromSql.Add(reader.GetInt32(0).ToString(CultureInfo.InvariantCulture) + ":"
                + Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture));
        }

        Assert.Equal(new[] { "102:1", "102:2", "103:11" }, fromSql);
    }
}
