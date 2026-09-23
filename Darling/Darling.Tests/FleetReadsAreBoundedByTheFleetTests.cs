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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3895: every statement the fleet overview runs is bounded by the FLEET — a per-server probe with its own
/// <c>LIMIT 1</c>, or a window on the partition column — and never by the retention.
///
/// <para><b>The defect.</b> Four "latest value per server" reads were <c>DISTINCT ON (server_id)</c> (or a
/// <c>MAX</c> joined back) over the whole table, which reads, decompresses and sorts every retained row to keep
/// one per server; two incident counts were bounded on the EVENT's timestamp, which no chunk is partitioned on,
/// so they opened every retained chunk to count the last hour. None of it showed at nine servers; all of it grew
/// with servers x retained days, and one overview on a 43-server store took 12.3 s cold. And the web viewer asked
/// for the whole overview twice per 60 s poll per tab.</para>
///
/// <para><b>What is pinned.</b> The rule the issue's verification named, over every public statement on the
/// reader and against the replaced shapes as positive controls; the event-table floor on each incident scan;
/// <see cref="EventWindowFloor"/>'s arithmetic; and the browser's side, where every <c>/api/fleet</c> GET has to
/// go through the one shared request. <see cref="FleetReadsAreBoundedLivePostgresTests"/> carries what a string
/// cannot: the rows, and the plan.</para>
/// </summary>
public sealed class FleetReadsAreBoundedByTheFleetTests
{
    /// <summary>The statements over the small registry and control-plane tables — servers, tags, the tag forest,
    /// the singleton settings row. They have no retention to be bounded by, so they are named here rather than
    /// exempted by a rule a new hypertable read could slip through.</summary>
    private static readonly string[] s_registryReads =
    {
        nameof(DarlingFleetReader.FleetServersSql),
        nameof(DarlingFleetReader.FleetTagsSql),
        nameof(DarlingFleetReader.FleetTagForestSql),
        nameof(DarlingFleetReader.FleetDeadlockRateThresholdSql),
    };

    [Fact]
    public void EveryFleetRead_IsBoundedByTheFleet_NotByTheRetention()
    {
        var statements = FleetStatements();
        Assert.True(statements.Count >= 13, $"found {statements.Count} fleet statements; the reflection sweep is broken, not the reader clean");

        foreach (var name in s_registryReads)
        {
            Assert.Contains(name, statements.Keys);
        }

        foreach (var (name, sql) in statements.Where(s => !s_registryReads.Contains(s.Key, StringComparer.Ordinal)))
        {
            Assert.True(IsBoundedByTheFleet(sql),
                $"{name} is bounded neither per server (a LATERAL probe with LIMIT 1) nor by a collection_time window, "
                + "so it reads every retained row of its table on every overview call (#3895):\n" + sql);
        }
    }

    /// <summary>
    /// The rule's positive controls: the four shapes #3895 replaced, verbatim, each of which it must refuse. A
    /// rule that accepted any of them would have passed on the tree this issue was filed against.
    /// </summary>
    [Theory]
    [InlineData("SELECT DISTINCT ON (server_id)\n    server_id,\n    sqlserver_cpu_utilization,\n    other_process_cpu_utilization\nFROM v_cpu_utilization_stats\nORDER BY server_id, collection_time DESC, sample_time DESC")]
    [InlineData("SELECT DISTINCT ON (server_id)\n    server_id,\n    max_workers_count\nFROM v_cpu_scheduler_stats\nORDER BY server_id, collection_time DESC")]
    [InlineData("SELECT\n    m.server_id,\n    CAST(COALESCE(SUM(m.waiter_count), 0) AS bigint)\nFROM v_memory_grant_stats m\nJOIN\n(\n    SELECT server_id, MAX(collection_time) AS max_collection_time\n    FROM v_memory_grant_stats\n    GROUP BY server_id\n) latest\n    ON m.server_id = latest.server_id\n    AND m.collection_time = latest.max_collection_time\nGROUP BY m.server_id")]
    [InlineData("SELECT server_id, COUNT(*) AS cnt, MAX(deadlock_time) AS last_seen\nFROM v_deadlocks\nWHERE deadlock_time >= $1\nAND   deadlock_time <= $2\nGROUP BY server_id")]
    public void TheRule_RefusesTheShapesThisIssueReplaced(string replaced)
    {
        Assert.False(IsBoundedByTheFleet(replaced) && EveryEventScanIsFloored(replaced));
    }

    /// <summary>
    /// Each scan of an event table carries the partition-column floor, in every statement that counts incidents
    /// over a window: the fleet card's two, the WPF viewer's fleet totals, which count the same thing for the
    /// same card, and <c>get_server_summary</c>'s per-server pair. A floor on ONE of a statement's scans is not
    /// enough — the other still opens every chunk. (The WPF card's per-server pair also carries two "newest
    /// event ever" reads, deliberately unbounded, so <c>ViewerW2aTests</c> pins it scan by scan instead.)
    /// </summary>
    [Fact]
    public void EveryEventTableScan_CarriesThePartitionFloor()
    {
        foreach (var (name, sql) in new[]
        {
            (nameof(DarlingFleetReader.FleetBlockingSql), DarlingFleetReader.FleetBlockingSql),
            (nameof(DarlingFleetReader.FleetDeadlockSql), DarlingFleetReader.FleetDeadlockSql),
            (nameof(ViewerDataService.FleetTotalsSql), ViewerDataService.FleetTotalsSql),
            ("DarlingHealthReader." + nameof(DarlingHealthReader.ServerSummaryBlockingSql), DarlingHealthReader.ServerSummaryBlockingSql),
            ("DarlingHealthReader." + nameof(DarlingHealthReader.ServerSummaryDeadlockSql), DarlingHealthReader.ServerSummaryDeadlockSql),
        })
        {
            Assert.True(EveryEventScanIsFloored(sql), $"{name} scans an event table without a collection_time floor:\n{sql}");
            Assert.True(EventScans(sql).Count >= 1, $"{name}: the scan finder found no event-table scan, so it proves nothing");
        }
    }

    [Fact]
    public void EventWindowFloor_IsOneChunkBeforeTheWindow_AsNaiveUtc()
    {
        Assert.Equal(TimeSpan.FromDays(TimescaleSupport.ChunkIntervalDays), EventWindowFloor.SkewAllowance);

        var start = new DateTime(2026, 9, 22, 21, 41, 47, DateTimeKind.Utc);
        var floor = EventWindowFloor.For(start);

        Assert.Equal(new DateTime(2026, 9, 21, 21, 41, 47), floor);
        /* Unspecified, the store's naive-UTC convention: a Kind=Utc value binds as timestamptz and the server
           zone-shifts it against the naive column. */
        Assert.Equal(DateTimeKind.Unspecified, floor.Kind);
    }

    /// <summary>
    /// The browser half: every <c>/api/fleet</c> GET in the web viewer goes through <c>apiGetFleet</c>, which
    /// shares one request among every caller that asks while it is in flight. The poll re-renders the sidebar
    /// and the current page in one synchronous pass, and both read the fleet — so a direct
    /// <c>apiGet("/api/fleet")</c> anywhere puts the second overview per tick straight back. Behaviour was
    /// verified by running the shipped modules under a DOM shim: one fleet request per tick on the fleet page,
    /// the server page and a saved view, where the previous modules sent two (see the PR).
    /// </summary>
    [Fact]
    public void EveryFleetFetchInTheBrowser_GoesThroughTheSharedRequest()
    {
        var jsRoot = RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js");
        var files = Directory.EnumerateFiles(jsRoot, "*.js", SearchOption.AllDirectories).ToList();
        Assert.True(files.Count >= 10, "the web module scan found almost nothing, so it proves nothing");

        var sharedCallers = 0;
        foreach (var file in files)
        {
            var js = File.ReadAllText(file);
            Assert.False(Regex.IsMatch(js, @"apiGet\(\s*[""'`]/api/fleet"),
                $"{Path.GetFileName(file)} fetches /api/fleet directly — every caller must go through apiGetFleet (#3895)");
            if (!file.EndsWith("util.js", StringComparison.Ordinal))
            {
                sharedCallers += Regex.Matches(js, @"\bapiGetFleet\(\)").Count;
            }
        }

        /* The sidebar, the fleet page, the server page, both composer loaders and both custom-view reads. */
        Assert.True(sharedCallers >= 7, $"only {sharedCallers} apiGetFleet() callers found — a fleet read has gone missing or around the helper");
    }

    /// <summary>
    /// And the helper's two promises, pinned on its text: it JOINS the request in flight rather than caching a
    /// response (the in-flight slot is cleared when the request settles, so a later caller always sends a fresh
    /// one and no page renders an older roll-up than it would have), and each caller classifies — so parses —
    /// the shared body itself, so no page can reach into cards another page was handed.
    /// </summary>
    [Fact]
    public void TheSharedRequest_IsJoinedInFlight_AndNeverOutlivesItsResponse()
    {
        var util = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "util.js");

        Assert.Contains("export async function apiGetFleet() {", util, StringComparison.Ordinal);
        Assert.Contains("if (!fleetRequest) {", util, StringComparison.Ordinal);
        Assert.Contains("fleetRequest = fetchBody(\"/api/fleet\").finally(() => {", util, StringComparison.Ordinal);
        Assert.Contains("fleetRequest = null;", util, StringComparison.Ordinal);
        Assert.Contains("return classifyResponse({ ok: shared.ok, status: shared.status, text: async () => shared.raw });", util, StringComparison.Ordinal);
    }

    /* ─────────────────────────── the rule ─────────────────────────── */

    private static readonly Regex s_partitionWindow = new(@"collection_time\s*>=\s*\$\d", RegexOptions.Compiled);
    private static readonly Regex s_perServerProbe = new(@"WHERE\s+(?:\w+\.)?server_id\s*=\s*s\.server_id", RegexOptions.Compiled);
    private static readonly Regex s_eventScan = new(@"FROM\s+(?:v_blocked_process_reports|v_dmv_blocking_snapshots|v_deadlocks)\b", RegexOptions.Compiled);

    /// <summary>Bounded by the fleet: a window on the partition column, or a per-server probe — a
    /// <c>LATERAL</c> keyed on the driving server with its own <c>LIMIT 1</c>. A <c>DISTINCT ON</c> with no
    /// window is refused outright whatever else the statement carries: it is the shape this issue removed.</summary>
    internal static bool IsBoundedByTheFleet(string sql)
    {
        var window = s_partitionWindow.IsMatch(sql);
        var probe = sql.Contains("CROSS JOIN LATERAL", StringComparison.Ordinal)
                    && s_perServerProbe.IsMatch(sql)
                    && sql.Contains("LIMIT 1", StringComparison.Ordinal);
        var unboundedDistinct = sql.Contains("DISTINCT ON (server_id)", StringComparison.Ordinal) && !window;

        return (window || probe) && !unboundedDistinct;
    }

    /// <summary>Every event-table scan's own predicate block — from its <c>FROM</c> to the <c>GROUP BY</c> or the
    /// closing parenthesis that ends it — carries a <c>collection_time</c> floor.</summary>
    internal static bool EveryEventScanIsFloored(string sql) =>
        EventScans(sql).All(block => s_partitionWindow.IsMatch(block));

    private static List<string> EventScans(string sql)
    {
        var blocks = new List<string>();
        foreach (Match match in s_eventScan.Matches(sql))
        {
            var rest = sql[match.Index..];
            var end = new[] { rest.IndexOf("GROUP BY", StringComparison.Ordinal), rest.IndexOf(')', StringComparison.Ordinal) }
                .Where(i => i > 0)
                .DefaultIfEmpty(rest.Length)
                .Min();
            blocks.Add(rest[..end]);
        }

        return blocks;
    }

    /// <summary>Every public SQL constant on the reader, by name — found by reflection so a statement added
    /// later is swept without anyone remembering to list it.</summary>
    private static Dictionary<string, string> FleetStatements() =>
        typeof(DarlingFleetReader)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name.EndsWith("Sql", StringComparison.Ordinal))
            .ToDictionary(f => f.Name, f => (string)f.GetRawConstantValue()!, StringComparer.Ordinal);
}

/// <summary>
/// Gated (DARLING_TEST_PG) live arm of #3895: that the bounded reads return the SAME rows the unbounded ones did —
/// the replaced statements are kept here verbatim as the oracle — and, where TimescaleDB is present, that the
/// planner actually stops: one chunk per collecting server for the newest-row probes, and only the window's
/// chunks for the incident counts. A string pin cannot show either, and the regression is quiet: a revert
/// returns identical rows on any store small enough for a test.
///
/// <para>Sentinel servers, negative ids, cleaned up in <c>finally</c>: two collecting SQL Servers (one with a
/// ring-buffer batch whose samples share a collection instant), one registered and never collected, one dark for
/// five days, one never stamped with an engine, one disabled, and one PostgreSQL target carrying a planted
/// SQL Server row it could never really have — which is how the driver's engine filter is shown to be real.
/// Each is registered the way the service registers one, with the first-connect instant #3935's fleet card
/// reads to tell the dark server from the never-collected one.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class FleetReadsAreBoundedLivePostgresTests
{
    private const int CollectingA = -958301;
    private const int CollectingB = -958302;
    private const int NeverCollected = -958303;
    private const int PostgresTarget = -958304;
    private const int Disabled = -958305;
    private const int DarkFiveDays = -958306;
    private const int Unstamped = -958307;

    private static readonly int[] s_all = { CollectingA, CollectingB, NeverCollected, PostgresTarget, Disabled, DarkFiveDays, Unstamped };

    /// <summary>The enabled, not-PostgreSQL sentinels — the population whose rows the old and new reads must
    /// agree on exactly (the cards exist for exactly these, among the sentinels).</summary>
    private static readonly int[] s_compared = { CollectingA, CollectingB, NeverCollected, DarkFiveDays, Unstamped };

    private static readonly string[] s_tables =
    {
        "cpu_utilization_stats", "memory_stats", "memory_grant_stats", "cpu_scheduler_stats",
        "blocked_process_reports", "dmv_blocking_snapshots", "deadlocks", "collection_log", "servers",
    };

    /* ── the replaced statements, verbatim: the oracle the bounded ones must match row for row ── */

    private const string OldCpuSql = @"
SELECT DISTINCT ON (server_id)
    server_id,
    sqlserver_cpu_utilization,
    other_process_cpu_utilization
FROM v_cpu_utilization_stats
ORDER BY server_id, collection_time DESC, sample_time DESC";

    private const string OldMemorySql = @"
SELECT DISTINCT ON (server_id)
    server_id,
    CAST(total_server_memory_mb AS double precision),
    CAST(buffer_pool_mb AS double precision)
FROM v_memory_stats
ORDER BY server_id, collection_time DESC";

    private const string OldMemoryPressureSql = @"
SELECT
    m.server_id,
    CAST(COALESCE(SUM(m.waiter_count), 0) AS bigint),
    CAST(COALESCE(SUM(m.timeout_error_count_delta), 0) AS bigint),
    CAST(COALESCE(SUM(m.forced_grant_count_delta), 0) AS bigint),
    CAST(COALESCE(SUM(m.granted_memory_mb), 0) AS double precision)
FROM v_memory_grant_stats m
JOIN
(
    SELECT server_id, MAX(collection_time) AS max_collection_time
    FROM v_memory_grant_stats
    GROUP BY server_id
) latest
    ON m.server_id = latest.server_id
    AND m.collection_time = latest.max_collection_time
GROUP BY m.server_id";

    private const string OldThreadsSql = @"
SELECT DISTINCT ON (server_id)
    server_id,
    max_workers_count,
    total_current_workers_count,
    total_runnable_tasks_count,
    total_work_queue_count
FROM v_cpu_scheduler_stats
ORDER BY server_id, collection_time DESC";

    private const string OldBlockingSql = @"
SELECT
    COALESCE(xe.server_id, dmv.server_id) AS server_id,
    COALESCE(xe.cnt, 0) AS xe_count,
    COALESCE(xe.max_wait, 0) AS xe_max_wait,
    COALESCE(dmv.cnt, 0) AS dmv_count,
    COALESCE(dmv.max_wait, 0) AS dmv_max_wait
FROM
(
    SELECT server_id, COUNT(*) AS cnt, MAX(wait_time_ms) AS max_wait
    FROM v_blocked_process_reports
    WHERE event_time >= $1
    AND   event_time <= $2
    GROUP BY server_id
) AS xe
FULL OUTER JOIN
(
    SELECT server_id, COUNT(*) AS cnt, MAX(wait_time_ms) AS max_wait
    FROM v_dmv_blocking_snapshots
    WHERE event_time >= $1
    AND   event_time <= $2
    GROUP BY server_id
) AS dmv ON xe.server_id = dmv.server_id";

    private const string OldDeadlockSql = @"
SELECT server_id, COUNT(*) AS cnt, MAX(deadlock_time) AS last_seen
FROM v_deadlocks
WHERE deadlock_time >= $1
AND   deadlock_time <= $2
GROUP BY server_id";

    private const string OldLastCollectionSql = @"
SELECT server_id, MAX(collection_time) AS last_collection_time
FROM v_collection_log
WHERE collection_time >= $1
AND   server_id <> 0
GROUP BY server_id";

    private const string OldViewerFreshnessSql = @"
SELECT server_id, MAX(collection_time)
FROM v_collection_log
WHERE server_id <> 0
GROUP BY server_id";

    /// <summary>
    /// The rows. Every newest-row read, the incident counts and both last-collection reads, old against new,
    /// over a week of history and every sentinel shape — including a CPU batch whose samples tie on the
    /// collection instant (the tiebreak must still pick the newest sample), grant pools summed at the newest
    /// instant, a server dark for five days (the unbounded probe must still find it; the 48-hour read must still
    /// leave its collection out, and since #3935 report it with an empty window and its registration), and
    /// incident rows collected late and collected ahead of their own event's clock.
    /// </summary>
    [Fact]
    public async Task TheBoundedReads_ReturnTheUnboundedReadsRows_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3895 read-equality test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await PrepareHypertablesAsync(connectionString!, connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelRowsAsync(connection, ct);
            var now = Micro(DateTime.UtcNow);
            await SeedAsync(connection, now, ct);

            /* The newest-row reads: identical rows for every compared sentinel. */
            foreach (var (label, oldSql, newSql) in new[]
            {
                ("cpu", OldCpuSql, DarlingFleetReader.FleetCpuSql),
                ("memory", OldMemorySql, DarlingFleetReader.FleetMemorySql),
                ("memory pressure", OldMemoryPressureSql, DarlingFleetReader.FleetMemoryPressureSql),
                ("threads", OldThreadsSql, DarlingFleetReader.FleetThreadsSql),
            })
            {
                var before = Scoped(await ReadRowsAsync(connection, oldSql, Array.Empty<object>(), ct), s_compared);
                var after = Scoped(await ReadRowsAsync(connection, newSql, Array.Empty<object>(), ct), s_compared);
                Assert.Equal(before, after);
                /* And the comparison is not vacuous: both collecting servers and the dark one answered. */
                Assert.Equal(3, after.Count(r => r.StartsWith(Id(CollectingA), StringComparison.Ordinal)
                                                 || r.StartsWith(Id(CollectingB), StringComparison.Ordinal)
                                                 || r.StartsWith(Id(DarkFiveDays), StringComparison.Ordinal)));
                Assert.DoesNotContain(after, r => r.StartsWith(Id(NeverCollected), StringComparison.Ordinal));

                /* The two populations the new driver leaves out on purpose: a disabled server has no card to
                   fill, and a PostgreSQL target never writes these tables (its planted row is the proof the
                   filter is real, not a reading any card was ever built from). */
                var unscoped = await ReadRowsAsync(connection, newSql, Array.Empty<object>(), ct);
                Assert.DoesNotContain(unscoped, r => r.StartsWith(Id(Disabled), StringComparison.Ordinal));
                Assert.DoesNotContain(unscoped, r => r.StartsWith(Id(PostgresTarget), StringComparison.Ordinal));
                if (label == "memory")
                {
                    /* The planted row is really there to be skipped: the oracle, which has no driver, finds it. */
                    Assert.Contains(await ReadRowsAsync(connection, oldSql, Array.Empty<object>(), ct), r => r.StartsWith(Id(PostgresTarget), StringComparison.Ordinal));
                }
            }

            /* The CPU tiebreak survived the reshape: the newest sample of the tied batch. */
            Assert.Contains(Id(CollectingB) + "77|7", await ReadRowsAsync(connection, DarlingFleetReader.FleetCpuSql, Array.Empty<object>(), ct));

            /* The incident counts over the default hour: the late-collected and the clock-ahead rows count
               in both; nothing differs. */
            var windowStart = now.AddHours(-1);
            var oldArgs = new object[] { windowStart, now };
            var newArgs = new object[] { windowStart, now, EventWindowFloor.For(windowStart) };
            Assert.Equal(
                Scoped(await ReadRowsAsync(connection, OldBlockingSql, oldArgs, ct), s_all),
                Scoped(await ReadRowsAsync(connection, DarlingFleetReader.FleetBlockingSql, newArgs, ct), s_all));
            Assert.Equal(
                Scoped(await ReadRowsAsync(connection, OldDeadlockSql, oldArgs, ct), s_all),
                Scoped(await ReadRowsAsync(connection, DarlingFleetReader.FleetDeadlockSql, newArgs, ct), s_all));
            /* Not vacuous: A's three in-window deadlocks — the ordinary one, the late-collected one and the
               clock-ahead one — are all in it. */
            Assert.Contains(Scoped(await ReadRowsAsync(connection, DarlingFleetReader.FleetDeadlockSql, newArgs, ct), s_all),
                r => r.StartsWith(Id(CollectingA) + "3|", StringComparison.Ordinal));

            /* The one row the floor refuses, pinned so the allowance cannot shrink unnoticed: an event whose
               collection is stamped MORE than a chunk before the event itself — a monitored clock a day ahead
               of the collector's. The oracle still counts it; nothing else differs. */
            await InsertDeadlockAsync(connection, CollectingB, now.AddMinutes(-10), now.AddMinutes(-10) - EventWindowFloor.SkewAllowance - TimeSpan.FromHours(3), ct);
            var oldDeadlocks = Scoped(await ReadRowsAsync(connection, OldDeadlockSql, oldArgs, ct), new[] { CollectingB });
            var newDeadlocks = Scoped(await ReadRowsAsync(connection, DarlingFleetReader.FleetDeadlockSql, newArgs, ct), new[] { CollectingB });
            Assert.Single(oldDeadlocks);
            Assert.Empty(newDeadlocks);

            /* Last collection: the 48 hours kept, so the five-days-dark server's collection is outside the window
               in both. #3935: the probe REPORTS that server, and the never-collected one, with an empty window
               and the registration beside it, where the oracle simply has no row; every collection the oracle
               finds, the probe finds identically. */
            var lastArgs = new object[] { DarlingFleetReader.LastCollectionWindowStart(now) };
            var oldLast = Scoped(await ReadRowsAsync(connection, OldLastCollectionSql, lastArgs, ct), s_compared);
            var newLast = Scoped(await ReadRowsAsync(connection, DarlingFleetReader.FleetLastCollectionSql, lastArgs, ct), s_compared);
            Assert.Equal(3, oldLast.Count);
            Assert.Equal(oldLast, newLast.Where(r => !r.Contains("|null|", StringComparison.Ordinal)).Select(WithoutLastCell).ToList());
            Assert.Equal(s_compared.Length, newLast.Count);
            Assert.Contains(Id(DarkFiveDays) + "null|" + Stamp(RegisteredLongAgo(now)), newLast);
            Assert.Contains(Id(NeverCollected) + "null|" + Stamp(RegisteredJustNow(now)), newLast);
            Assert.Contains(Id(CollectingA) + Stamp(now.AddMinutes(-1)) + "|" + Stamp(RegisteredLongAgo(now)), newLast);
            Assert.DoesNotContain(
                await ReadRowsAsync(connection, DarlingFleetReader.FleetLastCollectionSql, lastArgs, ct),
                r => r.StartsWith(Id(Disabled), StringComparison.Ordinal));

            /* The WPF viewer's sidebar freshness: every REGISTERED server, disabled included, unbounded — so the
               dark and the disabled servers keep their real last collection. */
            var registry = s_all;
            var oldFresh = Scoped(await ReadRowsAsync(connection, OldViewerFreshnessSql, Array.Empty<object>(), ct), registry);
            var newFresh = Scoped(await ReadRowsAsync(connection, ViewerDataService.ServerFreshnessSql, Array.Empty<object>(), ct), registry);
            Assert.Equal(oldFresh, newFresh);
            Assert.Contains(newFresh, r => r.StartsWith(Id(DarkFiveDays), StringComparison.Ordinal));
            Assert.Contains(newFresh, r => r.StartsWith(Id(Disabled), StringComparison.Ordinal));

            await using (var viewer = new ViewerDataService(connectionString!))
            {
                var freshness = await viewer.GetServerFreshnessAsync(ct);
                Assert.Equal(now.AddDays(-5).AddMinutes(1), freshness[DarkFiveDays]);
                Assert.Equal(now.AddMinutes(-1), freshness[CollectingA]);
                Assert.False(freshness.ContainsKey(NeverCollected));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelRowsAsync);
        }
    }

    /// <summary>
    /// The seam: the overview itself, through <see cref="DarlingFleetReader.GetFleetOverviewAsync"/>, puts the
    /// newest reading on each card — the newest sample of a tied batch, the pools summed at the newest instant,
    /// a dark server's last values — leaves a never-collected server's metrics empty, and ignores the reading
    /// planted under the PostgreSQL target. And (#3935) it bands the five-days-dark server Offline and the
    /// just-registered one awaiting its first collection, the same words the WPF viewer's unbounded read gives
    /// them.
    /// </summary>
    [Fact]
    public async Task TheOverview_BuildsEachCardFromItsNewestReading_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3895 overview test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await PrepareHypertablesAsync(connectionString!, connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelRowsAsync(connection, ct);
            var now = Micro(DateTime.UtcNow);
            await SeedAsync(connection, now, ct);

            var result = await DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-1), now, now, cancellationToken: ct);
            var cards = result.Cards.Where(c => s_all.Contains(c.ServerId)).ToDictionary(c => c.ServerId);

            Assert.False(cards.ContainsKey(Disabled));

            var a = cards[CollectingA];
            Assert.Equal(40, a.CpuPercent);
            Assert.Equal(4, a.OtherProcessCpuPercent);
            Assert.Equal(8000, a.MemoryMb);
            Assert.Equal(6000, a.BufferPoolMb);
            Assert.Equal(5, a.MemoryWaiterCount);          // 2 + 3, the two pools at the newest instant
            Assert.Equal(300, a.GrantedMemoryMb);
            Assert.Equal(512, a.TotalThreads);
            Assert.Equal(100, a.CurrentWorkers);
            Assert.Equal(3, a.DeadlockCount);              // the late-collected and the clock-ahead ones included
            Assert.Equal(2, a.BlockingCount);
            Assert.True(a.IsOnline);

            var b = cards[CollectingB];
            Assert.Equal(77, b.CpuPercent);                // the newest sample of the tied batch
            Assert.Equal(7, b.OtherProcessCpuPercent);

            var dark = cards[DarkFiveDays];
            Assert.Equal(11, dark.CpuPercent);             // five days old, still its newest
            Assert.Equal(1100, dark.MemoryMb);

            /* #3935: dark for five days, registered nine days ago — Offline, the viewer's word for it, and not
               "Awaiting first collection". Its last collection is outside the kept window, so the card carries
               none; status is what tells this null from the never-collected one below. */
            Assert.False(dark.IsOnline);
            Assert.Equal(FleetHealthBand.Offline, dark.Band);
            Assert.Equal(ServerCollectionStatus.Offline.Word(), dark.Status);
            Assert.False(dark.AwaitingFirstCollection);
            Assert.Null(dark.LastCollectionTime);

            var never = cards[NeverCollected];
            Assert.Null(never.CpuPercent);
            Assert.Null(never.MemoryMb);
            Assert.Null(never.TotalThreads);
            Assert.Null(never.GrantedMemoryMb);

            /* Registered ten minutes ago and nothing collected since: still the bootstrap state, never red. */
            Assert.Null(never.IsOnline);
            Assert.True(never.AwaitingFirstCollection);
            Assert.Equal(FleetHealthBand.Warning, never.Band);
            Assert.Equal(ServerCollectionStatus.AwaitingFirstCollection.Word(), never.Status);

            /* And both read what the WPF viewer reads for them: its sidebar freshness has no window, and each
               card's status is the word that freshness bands to. */
            await using (var viewer = new ViewerDataService(connectionString!))
            {
                var viewerFreshness = await viewer.GetServerFreshnessAsync(ct);
                foreach (var id in new[] { DarkFiveDays, NeverCollected, CollectingA })
                {
                    DateTime? viewerLast = viewerFreshness.TryGetValue(id, out var seen) ? seen : null;
                    Assert.Equal(
                        ServerCollectionStatusRules.FromFreshness(ServerHealthClassifier.ClassifyFreshness(viewerLast, now)).Word(),
                        cards[id].Status);
                }
            }

            var pg = cards[PostgresTarget];
            Assert.Null(pg.MemoryMb);                      // the planted SQL Server row is never read

            var unstamped = cards[Unstamped];
            Assert.Equal(55, unstamped.CpuPercent);        // no engine claim: probed like a SQL Server

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelRowsAsync);
        }
    }

    /// <summary>
    /// The plan, where TimescaleDB is present (the only place chunks exist to exclude). Over the seeded week of
    /// daily chunks: the newest-row probes read a handful of rows rather than every retained one and never sort
    /// the relation, and the floored incident count plans only the window's chunks where the event-bounded
    /// oracle plans every one.
    /// </summary>
    [Fact]
    public async Task TheBoundedReads_StopAtTheNewestChunk_AndPlanOnlyTheWindowsChunks_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3895 plan test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var timescale = await PrepareHypertablesAsync(connectionString!, connection, ct);
        Assert.SkipUnless(timescale, "TimescaleDB is not available on this store: there are no chunks to exclude.");

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelRowsAsync(connection, ct);
            var now = Micro(DateTime.UtcNow);
            await SeedAsync(connection, now, ct);
            using (var analyze = new NpgsqlCommand("ANALYZE collect.memory_stats; ANALYZE collect.deadlocks; ANALYZE collect.servers;", connection))
            {
                await analyze.ExecuteNonQueryAsync(ct);
            }

            /* Newest-row probe: a per-server Limit over an ordered ChunkAppend — no Unique and no sort of the
               relation by server — returning a handful of rows where the DISTINCT ON streamed every retained
               one through its sort. */
            var memoryPlan = await ExplainAsync(connection, "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) " + DarlingFleetReader.FleetMemorySql, Array.Empty<object>(), ct);
            Assert.DoesNotContain("Unique", memoryPlan, StringComparison.Ordinal);
            Assert.DoesNotMatch(new Regex(@"Sort Key: \S*server_id"), memoryPlan);
            Assert.Contains("Nested Loop", memoryPlan, StringComparison.Ordinal);
            Assert.Contains("Limit", memoryPlan, StringComparison.Ordinal);
            Assert.Matches(@"Order: \S*collection_time DESC", memoryPlan);

            var oldMemoryPlan = await ExplainAsync(connection, "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) " + OldMemorySql, Array.Empty<object>(), ct);
            Assert.True(LeafRows(memoryPlan) * 10 < LeafRows(oldMemoryPlan),
                $"the probes returned {LeafRows(memoryPlan)} rows against the DISTINCT ON's {LeafRows(oldMemoryPlan)} — they are not stopping at each server's newest row:\n{memoryPlan}");

            /* Floored incident count: the chunks older than the floor are gone from the plan, where the oracle
               plans every one. Counted relative to the oracle, because the shared test store can hold chunks
               other classes created on either side of the window; what this read controls is that the
               seeded week's older days are not among the ones it plans. */
            var windowStart = now.AddHours(-1);
            var floored = await ExplainAsync(connection, "EXPLAIN (COSTS OFF) " + DarlingFleetReader.FleetDeadlockSql, new object[] { windowStart, now, EventWindowFloor.For(windowStart) }, ct);
            var unfloored = await ExplainAsync(connection, "EXPLAIN (COSTS OFF) " + OldDeadlockSql, new object[] { windowStart, now }, ct);
            var flooredChunks = PlanChunkScans.Count(floored);
            var unflooredChunks = PlanChunkScans.Count(unfloored);
            Assert.True(unflooredChunks >= 7, $"the seeded week should put the oracle across at least seven chunks:\n{unfloored}");
            Assert.True(flooredChunks <= unflooredChunks - 5,
                $"the floored count planned {flooredChunks} of the oracle's {unflooredChunks} chunks for a one-hour window:\n{floored}");

            /* #3935 kept the last-collection read's window and told a dark server from a never-collected one
               with a registry column instead, because the same probe with the window taken out plans every
               retained chunk of collection_log on every call. Pinned at the plan, so a fix that drops the window
               goes red here: the windowed read plans only the window's chunks. */
            var windowedLast = await ExplainAsync(connection, "EXPLAIN (COSTS OFF) " + DarlingFleetReader.FleetLastCollectionSql, new object[] { DarlingFleetReader.LastCollectionWindowStart(now) }, ct);
            var unwindowedLast = await ExplainAsync(connection, "EXPLAIN (COSTS OFF) " + UnwindowedLastCollectionSql(), Array.Empty<object>(), ct);
            Assert.True(PlanChunkScans.Count(unwindowedLast) >= 7, $"the seeded week should put the unwindowed probe across at least seven chunks:\n{unwindowedLast}");
            Assert.True(PlanChunkScans.Count(windowedLast) <= PlanChunkScans.Count(unwindowedLast) - 5,
                $"the last-collection read planned {PlanChunkScans.Count(windowedLast)} of the unwindowed probe's {PlanChunkScans.Count(unwindowedLast)} chunks for a two-day window:\n{windowedLast}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelRowsAsync);
        }
    }

    /// <summary>
    /// The driver's engine filter, evaluated by PostgreSQL for every token and every spelling the C# decoder
    /// accepts, against <see cref="MonitoredEngineKind.IsPostgres"/>: a server is skipped exactly when the C#
    /// would call it PostgreSQL, so the SQL and the card agree about the same registry row.
    /// </summary>
    [Fact]
    public async Task TheEngineFilter_SkipsExactlyWhatTheDecoderCallsPostgres_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live engine-filter test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        var kinds = MonitoredEngineKind.All
            .Concat(new string?[] { null, "", "  ", " Postgres ", "AURORA-POSTGRES", "SqlServer", "mysql" })
            .ToList();

        foreach (var kind in kinds)
        {
            using var command = new NpgsqlCommand(
                "SELECT " + DarlingFleetReader.SqlServerCollectedTargetSql + " FROM (SELECT $1::text AS engine_kind) AS s", connection);
            command.Parameters.Add(new NpgsqlParameter<string?> { TypedValue = kind, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            var probed = (bool)(await command.ExecuteScalarAsync(ct))!;
            Assert.Equal(!MonitoredEngineKind.IsPostgres(kind), probed);
        }
    }

    /* ─────────────────────────── seeding ─────────────────────────── */

    /// <summary>
    /// A week of history, one row a day per collecting server (so every day is its own chunk on a hypertable
    /// store) plus the minute-old newest rows, and the incident rows the counts are asked about.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        /* Registration the way the service writes it (#3935): created_date is the first successful connect,
           and collection only follows one. Everything with history connected nine days ago, ahead of its
           eight days of it; the never-collected server and the PostgreSQL fixture connected ten minutes ago
           and have written no collection_log row since. */
        var longAgo = RegisteredLongAgo(now);
        var justNow = RegisteredJustNow(now);
        await InsertServerAsync(connection, CollectingA, "fleet3895-a", true, MonitoredEngineKind.SqlServer, longAgo, ct);
        await InsertServerAsync(connection, CollectingB, "fleet3895-b", true, MonitoredEngineKind.SqlServer, longAgo, ct);
        await InsertServerAsync(connection, NeverCollected, "fleet3895-never", true, MonitoredEngineKind.SqlServer, justNow, ct);
        await InsertServerAsync(connection, PostgresTarget, "fleet3895-pg", true, MonitoredEngineKind.Postgres, justNow, ct);
        await InsertServerAsync(connection, Disabled, "fleet3895-off", false, MonitoredEngineKind.SqlServer, longAgo, ct);
        await InsertServerAsync(connection, DarkFiveDays, "fleet3895-dark", true, MonitoredEngineKind.SqlServer, longAgo, ct);
        await InsertServerAsync(connection, Unstamped, "fleet3895-unstamped", true, null, longAgo, ct);

        /* History: every two hours from eight days back to a day back, older values that the newest must
           outrank — eight daily chunks on a hypertable store, and enough rows that a read which streams them
           all is unmistakable. One set-based INSERT per table. */
        foreach (var id in new[] { CollectingA, CollectingB, Disabled, Unstamped })
        {
            await SeedHistoryAsync(connection, id, now.AddDays(-8), now.AddDays(-1), ct);
        }

        /* The incident tables get a week of history too, outside any window asked about, so the event-bounded
           oracle has old chunks to open and the floored read has old chunks to skip. */
        for (var day = 8; day >= 2; day--)
        {
            await InsertDeadlockAsync(connection, Disabled, now.AddDays(-day), now.AddDays(-day).AddMinutes(1), ct);
        }

        /* The dark server's last day, five days ago. */
        var darkAt = now.AddDays(-5).AddMinutes(1);
        await InsertCpuAsync(connection, DarkFiveDays, darkAt, darkAt.AddHours(-5), 11, 1, ct);
        await InsertMemoryAsync(connection, DarkFiveDays, darkAt, 1100, 500, ct);
        await InsertGrantAsync(connection, DarkFiveDays, darkAt, 1, 10, ct);
        await InsertSchedulerAsync(connection, DarkFiveDays, darkAt, 128, 5, ct);
        await InsertCollectionLogAsync(connection, DarkFiveDays, darkAt, ct);

        /* The newest readings, a minute ago. */
        var newest = now.AddMinutes(-1);
        await InsertCpuAsync(connection, CollectingA, newest, newest.AddHours(-5), 40, 4, ct);
        /* B: one ring-buffer batch, three samples under ONE collection instant, inserted newest-sample-last so
           heap order does not hand back the right row by accident. */
        await InsertCpuAsync(connection, CollectingB, newest, newest.AddHours(-5).AddMinutes(-2), 70, 1, ct);
        await InsertCpuAsync(connection, CollectingB, newest, newest.AddHours(-5).AddMinutes(-1), 71, 2, ct);
        await InsertCpuAsync(connection, CollectingB, newest, newest.AddHours(-5), 77, 7, ct);
        await InsertCpuAsync(connection, Unstamped, newest, newest.AddHours(-5), 55, 5, ct);
        await InsertCpuAsync(connection, Disabled, newest, newest.AddHours(-5), 99, 9, ct);

        await InsertMemoryAsync(connection, CollectingA, newest, 8000, 6000, ct);
        await InsertMemoryAsync(connection, CollectingB, newest, 4000, 3000, ct);
        await InsertMemoryAsync(connection, Unstamped, newest, 2000, 1000, ct);
        /* The PostgreSQL target's planted row — a reading it can never really have. */
        await InsertMemoryAsync(connection, PostgresTarget, newest, 123456, 1, ct);

        /* A: two pools at the newest instant, summed. */
        await InsertGrantAsync(connection, CollectingA, newest, 2, 100, ct);
        await InsertGrantAsync(connection, CollectingA, newest, 3, 200, ct);
        await InsertGrantAsync(connection, CollectingB, newest, 0, 50, ct);

        await InsertSchedulerAsync(connection, CollectingA, newest, 512, 100, ct);
        await InsertSchedulerAsync(connection, CollectingB, newest, 256, 30, ct);

        foreach (var id in new[] { CollectingA, CollectingB, Unstamped })
        {
            await InsertCollectionLogAsync(connection, id, newest, ct);
        }

        /* Incidents for A in the default hour: an ordinary one, one collected 50 minutes after it happened (the
           catch-up after an outage), and one whose collection is stamped ten minutes BEFORE its own event (a
           monitored clock running ahead) — all three counted. Two XE blocking reports, one of them late. */
        await InsertDeadlockAsync(connection, CollectingA, now.AddMinutes(-20), now.AddMinutes(-19), ct);
        await InsertDeadlockAsync(connection, CollectingA, now.AddMinutes(-55), now.AddMinutes(-5), ct);
        await InsertDeadlockAsync(connection, CollectingA, now.AddMinutes(-30), now.AddMinutes(-40), ct);
        await InsertBlockedProcessAsync(connection, CollectingA, now.AddMinutes(-15), now.AddMinutes(-14), ct);
        await InsertBlockedProcessAsync(connection, CollectingA, now.AddMinutes(-58), now.AddMinutes(-2), ct);
        await InsertDmvBlockingAsync(connection, CollectingB, now.AddMinutes(-12), ct);
        /* And an incident a day old, which neither window wants. */
        await InsertDeadlockAsync(connection, CollectingA, now.AddDays(-1), now.AddDays(-1).AddMinutes(1), ct);
    }

    /// <summary>One server's history, a row every two hours in [<paramref name="from"/>, <paramref name="to"/>] in
    /// each of the four newest-row tables and the collection log, with values the newest rows outrank.</summary>
    private static async Task SeedHistoryAsync(NpgsqlConnection connection, int id, DateTime from, DateTime to, CancellationToken ct)
    {
        const string Series = " FROM generate_series($2::timestamp, $3::timestamp, interval '2 hours') AS t";
        foreach (var sql in new[]
        {
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) SELECT 1, t, $1, 'fleet3895', t - interval '5 hours', 1, 1" + Series,
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_server_memory_mb, buffer_pool_mb) SELECT 1, t, $1, 'fleet3895', 100, 50" + Series,
            "INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, waiter_count, timeout_error_count_delta, forced_grant_count_delta, granted_memory_mb) SELECT 1, t, $1, 'fleet3895', 0, 0, 0, 0" + Series,
            "INSERT INTO cpu_scheduler_stats (collection_id, collection_time, server_id, server_name, max_workers_count, total_current_workers_count, total_runnable_tasks_count, total_work_queue_count) SELECT 1, t, $1, 'fleet3895', 64, 1, 0, 0" + Series,
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) SELECT 1, $1, 'fleet3895', 'wait_stats', t, 'SUCCESS'" + Series,
        })
        {
            await ExecAsync(connection, sql, ct, id, from, to);
        }
    }

    private static async Task InsertServerAsync(NpgsqlConnection connection, int id, string name, bool enabled, string? engineKind, DateTime registeredAt, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, engine_kind, created_date, modified_date) VALUES ($1, $2, $2, $3, $4, $5, $6, $6)", connection);
        command.Parameters.AddWithValue(id);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(enabled);
        command.Parameters.Add(new NpgsqlParameter<int?> { TypedValue = engineKind == MonitoredEngineKind.Postgres ? 0 : 3 });
        command.Parameters.Add(new NpgsqlParameter<string?> { TypedValue = engineKind, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.AddWithValue(DateTime.SpecifyKind(registeredAt, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>When the servers with history first connected: nine days back, ahead of the eight days of it.</summary>
    private static DateTime RegisteredLongAgo(DateTime now) => now.AddDays(-9);

    /// <summary>When the never-collected server (and the PostgreSQL fixture) first connected: ten minutes back,
    /// well inside the last-collection read's two-day window.</summary>
    private static DateTime RegisteredJustNow(DateTime now) => now.AddMinutes(-10);

    private static Task InsertCpuAsync(NpgsqlConnection connection, int id, DateTime at, DateTime sampleTime, int sqlCpu, int otherCpu, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, 'fleet3895', $4, $5, $6)",
            ct, CollectionIdGenerator.Next(), at, id, sampleTime, sqlCpu, otherCpu);

    private static Task InsertMemoryAsync(NpgsqlConnection connection, int id, DateTime at, decimal total, decimal bufferPool, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_server_memory_mb, buffer_pool_mb) VALUES ($1, $2, $3, 'fleet3895', $4, $5)",
            ct, CollectionIdGenerator.Next(), at, id, total, bufferPool);

    private static Task InsertGrantAsync(NpgsqlConnection connection, int id, DateTime at, int waiters, decimal grantedMb, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, waiter_count, timeout_error_count_delta, forced_grant_count_delta, granted_memory_mb) VALUES ($1, $2, $3, 'fleet3895', $4, 0, 0, $5)",
            ct, CollectionIdGenerator.Next(), at, id, waiters, grantedMb);

    private static Task InsertSchedulerAsync(NpgsqlConnection connection, int id, DateTime at, int maxWorkers, int currentWorkers, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO cpu_scheduler_stats (collection_id, collection_time, server_id, server_name, max_workers_count, total_current_workers_count, total_runnable_tasks_count, total_work_queue_count) VALUES ($1, $2, $3, 'fleet3895', $4, $5, 0, 0)",
            ct, CollectionIdGenerator.Next(), at, id, maxWorkers, currentWorkers);

    private static Task InsertCollectionLogAsync(NpgsqlConnection connection, int id, DateTime at, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, 'fleet3895', 'wait_stats', $3, 'SUCCESS')",
            ct, CollectionIdGenerator.Next(), id, at);

    private static Task InsertDeadlockAsync(NpgsqlConnection connection, int id, DateTime deadlockTime, DateTime collectionTime, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time) VALUES ($1, $2, $3, 'fleet3895', $4)",
            ct, CollectionIdGenerator.Next(), collectionTime, id, deadlockTime);

    private static Task InsertBlockedProcessAsync(NpgsqlConnection connection, int id, DateTime eventTime, DateTime collectionTime, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms) VALUES ($1, $2, $3, 'fleet3895', $4, 1500)",
            ct, CollectionIdGenerator.Next(), collectionTime, id, eventTime);

    /// <summary>A DMV blocking snapshot, whose <c>event_time</c> IS its <c>collection_time</c> by construction.</summary>
    private static Task InsertDmvBlockingAsync(NpgsqlConnection connection, int id, DateTime at, CancellationToken ct) =>
        ExecAsync(connection,
            "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, wait_time_ms) VALUES ($1, $2, $3, 'fleet3895', $2, 900)",
            ct, CollectionIdGenerator.Next(), at, id);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] args)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.AddWithValue(arg is DateTime dt ? DateTime.SpecifyKind(dt, DateTimeKind.Unspecified) : arg);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    /* ─────────────────────────── reading ─────────────────────────── */

    /// <summary>Every row a statement returns, each rendered as <c>id|col|col...</c> in invariant culture, so
    /// two statements' answers compare as sets of strings whatever their column names.</summary>
    private static async Task<List<string>> ReadRowsAsync(NpgsqlConnection connection, string sql, object[] args, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.AddWithValue(arg is DateTime dt ? DateTime.SpecifyKind(dt, DateTimeKind.Unspecified) : arg);
        }

        var rows = new List<string>();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cells = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                /* Round-trip form for instants, so two reads that disagree below the second still disagree. */
                cells[i] = reader.IsDBNull(i) ? "null"
                    : reader.GetValue(i) is DateTime instant ? instant.ToString("O", CultureInfo.InvariantCulture)
                    : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture)!;
            }

            rows.Add(string.Join("|", cells));
        }

        return rows;
    }

    /// <summary>The rows whose first column is one of <paramref name="ids"/>, sorted — the population two reads
    /// are compared over, with everything else the shared store holds left out.</summary>
    private static List<string> Scoped(List<string> rows, int[] ids) =>
        rows.Where(r => ids.Any(id => r.StartsWith(Id(id), StringComparison.Ordinal))).OrderBy(r => r, StringComparer.Ordinal).ToList();

    private static string Id(int id) => id.ToString(CultureInfo.InvariantCulture) + "|";

    /// <summary>An instant as <see cref="ReadRowsAsync"/> renders it.</summary>
    private static string Stamp(DateTime instant) => instant.ToString("O", CultureInfo.InvariantCulture);

    /// <summary>A rendered row without its last cell — the last-collection probe's registration, which the
    /// oracle does not carry.</summary>
    private static string WithoutLastCell(string row) => row[..row.LastIndexOf('|')];

    /// <summary><see cref="DarlingFleetReader.FleetLastCollectionSql"/> with its window taken out — option 1 of
    /// #3935, derived rather than restated so it stays that statement minus one predicate.</summary>
    private static string UnwindowedLastCollectionSql()
    {
        var sql = Regex.Replace(DarlingFleetReader.FleetLastCollectionSql, @"\s+AND\s+collection_time >= \$1", "");
        Assert.NotEqual(DarlingFleetReader.FleetLastCollectionSql, sql);
        return sql;
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, string sql, object[] args, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.AddWithValue(arg is DateTime dt ? DateTime.SpecifyKind(dt, DateTimeKind.Unspecified) : arg);
        }

        var plan = new StringBuilder();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            plan.AppendLine(reader.GetString(0));
        }

        return plan.ToString();
    }

    /// <summary>The rows the plan's scans produced, summed over every scan node's <c>rows x loops</c>.</summary>
    private static double LeafRows(string plan) =>
        Regex.Matches(plan, @"Scan[^\n]*\(actual rows=([\d.]+) loops=(\d+)\)")
            .Sum(m => double.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture) * double.Parse(m.Groups[2].Value, CultureInfo.InvariantCulture));

    /// <summary>The store the way the service builds it: hypertables where TimescaleDB is present. #1922: the
    /// probe runs on its own connection.</summary>
    private static async Task<bool> PrepareHypertablesAsync(string connectionString, NpgsqlConnection connection, CancellationToken ct)
    {
        var timescale = await LiveTimescaleProbe.TryEnableAsync(connectionString, ct);
        if (timescale)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct);
        }

        return timescale;
    }

    /// <summary>PostgreSQL <c>timestamp</c> is microsecond-resolution; .NET ticks are 100 ns. Seeded instants
    /// are floored to the microsecond so the values read back compare equal to the ones written.</summary>
    private static DateTime Micro(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % 10)), DateTimeKind.Unspecified);

    private static async Task DeleteSentinelRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", s_all.Select(id => id.ToString(CultureInfo.InvariantCulture)));
        foreach (var table in s_tables)
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({ids})", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
