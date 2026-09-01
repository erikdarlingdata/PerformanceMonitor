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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Fleet-wide NOC roll-up's cross-server totals read (<see cref="ViewerDataService.FleetTotalsSql"/>)
/// against the Darling store contract — no live Postgres. This is the signature capability Darling's single
/// central store enables and the Dashboard/Lite physically cannot do: one aggregate over EVERY server's rows,
/// keyed by <c>server_id</c>. The pins are the load-bearing clauses — the two blocking sources counted per
/// server (a GROUP BY), the FULL OUTER JOIN lining them up, the per-server XE→DMV fallback CASE, the
/// cross-server deadlock COUNT, the two-sided window on every source, and the Pg dialect (no per-server
/// filter, positional params).
/// </summary>
public sealed class ViewerFleetRollupSqlTests
{
    [Fact]
    public void FleetTotalsSql_BlockingCountsBothSourcesPerServer_ThenAppliesXeDmvFallback()
    {
        var sql = ViewerDataService.FleetTotalsSql;

        /* Each source counted per server (a cross-server GROUP BY), FULL OUTER JOIN so a server present in
           only one source still appears, then Lite's per-server XE-preferred / DMV-fallback rule before the
           fleet SUM — so an RDS server with only DMV snapshots still counts and an XE server is never
           double-counted. */
        Assert.Contains("FROM v_blocked_process_reports", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_dmv_blocking_snapshots", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_id", sql, StringComparison.Ordinal);
        Assert.Contains("FULL OUTER JOIN", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(xe.server_id, dmv.server_id)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(CASE WHEN per_server.xe_count > 0 THEN per_server.xe_count ELSE per_server.dmv_count END)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void FleetTotalsSql_DeadlocksAreACrossServerCount_OverTheWindow()
    {
        var sql = ViewerDataService.FleetTotalsSql;
        Assert.Contains("FROM v_deadlocks", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*)", sql, StringComparison.Ordinal);
        Assert.Contains("deadlock_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains("deadlock_time <= $2", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void FleetTotalsSql_WindowsEverySource_OnBothBounds()
    {
        var sql = ViewerDataService.FleetTotalsSql;
        /* Both blocking sources and the deadlock read are bounded on BOTH sides so the totals honor a
           settable window exactly (the caller passes the same one-hour window the cards use). */
        Assert.Contains("event_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains("event_time <= $2", sql, StringComparison.Ordinal);
        Assert.Equal(2, CountOccurrences(sql, "event_time >= $1"));   // one per blocking source
        Assert.Equal(2, CountOccurrences(sql, "event_time <= $2"));
    }

    [Fact]
    public void FleetTotalsSql_IsPgDialect_PositionalWindowParams_NoBareNow_NoNLiterals_NoPerServerFilter()
    {
        var sql = ViewerDataService.FleetTotalsSql;
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.Contains("$2", sql, StringComparison.Ordinal);
        /* The fleet read spans ALL servers — no per-server scoping predicate (unlike the per-server card
           reads, whose signature is "WHERE server_id = $1"). */
        Assert.DoesNotContain("server_id = $", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void FleetTotals_ReferenceColumnsThatExistInTheGeneratedSourceTables()
    {
        /* The window is applied on event_time (both blocking sources) and deadlock_time (deadlocks); the
           per-server GROUP BY keys on server_id, the universal store key on every collect table. */
        Assert.Contains("event_time", PgSchemaGenerator.CreateTable(BlockedProcessReportCollector.Instance), StringComparison.Ordinal);
        Assert.Contains("event_time", PgSchemaGenerator.CreateTable(DmvBlockingSnapshotCollector.Instance), StringComparison.Ordinal);
        Assert.Contains("deadlock_time", PgSchemaGenerator.CreateTable(DeadlocksCollector.Instance), StringComparison.Ordinal);
    }

    [Fact]
    public void PassthroughViews_ForEveryFleetSource_ExistInTheMigrations()
    {
        var allMigrationSql = string.Concat(PgMigrations.Scripts.Select(m => m.Sql));
        foreach (var view in new[] { "v_blocked_process_reports", "v_dmv_blocking_snapshots", "v_deadlocks" })
        {
            Assert.Contains(view, allMigrationSql, StringComparison.Ordinal);
        }
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }
        return count;
    }
}

/// <summary>
/// The fleet roll-up's pure C# reduction (<see cref="FleetRollup.Build"/>) — no Postgres. Every band
/// decision REUSES #1426's card banding (<see cref="ServerSummaryItem.OverallMetricSeverity"/> + freshness),
/// so these tests prove the fleet counts / ranking never invent a new band: they classify the same cards the
/// Overview renders. Covers the band counts, the servers-with-collection-failures roll-up, the worst-first
/// ranking order (offline &gt; critical &gt; warning, then by how many metrics are bad), the cap + overflow,
/// the all-clear state, and the totals passthrough.
/// </summary>
public sealed class ViewerFleetRollupBuilderTests
{
    private static ServerSummaryItem Healthy(string name, int id) =>
        new() { DisplayName = name, ServerId = id, IsOnline = true };

    private static ServerSummaryItem Critical(string name, int id, int deadlocks = 1) =>
        new() { DisplayName = name, ServerId = id, IsOnline = true, DeadlockCount = deadlocks };

    private static ServerSummaryItem WarningCollectors(string name, int id, int failed = 1) =>
        new() { DisplayName = name, ServerId = id, IsOnline = true, FailedCollectorCount = failed };

    private static ServerSummaryItem Stale(string name, int id) =>
        new() { DisplayName = name, ServerId = id, IsOnline = true, HasCollectorErrors = true };

    private static ServerSummaryItem Offline(string name, int id) =>
        new() { DisplayName = name, ServerId = id, IsOnline = false };

    private static ServerSummaryItem Awaiting(string name, int id) =>
        new() { DisplayName = name, ServerId = id, IsOnline = null, AwaitingFirstCollection = true };

    private static readonly FleetTotals NoTotals = new();

    // ── ClassifyBand: the fleet band is the card's banding, collapsed to one label ─────────────────

    [Fact]
    public void ClassifyBand_OfflineCollection_IsOffline()
    {
        Assert.Equal(FleetHealthBand.Offline, FleetRollup.ClassifyBand(Offline("s", 1)));
    }

    [Fact]
    public void ClassifyBand_AwaitingFirstCollection_IsWarning_NeverOfflineOrHealthy()
    {
        /* A queued-during-bootstrap server is attention-worthy (never Healthy) but truthfully
           amber, never the red Offline overlay (24-server field incident, 2026-07-17). */
        Assert.Equal(FleetHealthBand.Warning, FleetRollup.ClassifyBand(Awaiting("s", 1)));
    }

    [Fact]
    public void ClassifyBand_CriticalMetric_IsCritical()
    {
        // A deadlock in the window is the card's Critical (DeadlockSeverity) → fleet Critical.
        Assert.Equal(FleetHealthBand.Critical, FleetRollup.ClassifyBand(Critical("s", 1)));
        // CPU >= 95 is also the card's Critical.
        Assert.Equal(FleetHealthBand.Critical,
            FleetRollup.ClassifyBand(new ServerSummaryItem { IsOnline = true, CpuPercent = 96 }));
    }

    [Fact]
    public void ClassifyBand_WarningMetric_IsWarning()
    {
        // A FAILING collector is the card's Warning (CollectorSeverity) — this is the reuse of
        // CollectorHealthRow.HealthStatus that flows through the card's FailedCollectorCount.
        Assert.Equal(FleetHealthBand.Warning, FleetRollup.ClassifyBand(WarningCollectors("s", 1)));
    }

    [Fact]
    public void ClassifyBand_StaleCollection_IsWarning()
    {
        // Online but collection has gone stale (HasCollectorErrors) → the card's amber → fleet Warning,
        // even with every metric calm.
        Assert.Equal(FleetHealthBand.Warning, FleetRollup.ClassifyBand(Stale("s", 1)));
    }

    [Fact]
    public void ClassifyBand_OnlineAndCalm_IsHealthy()
    {
        Assert.Equal(FleetHealthBand.Healthy, FleetRollup.ClassifyBand(Healthy("s", 1)));
    }

    // ── Band counts + servers-with-failures ────────────────────────────────────────────────────────

    [Fact]
    public void Build_CountsServersByBand()
    {
        var summaries = new[]
        {
            Healthy("h1", 1), Healthy("h2", 2), Healthy("h3", 3),
            WarningCollectors("w1", 4), Stale("w2", 5),
            Critical("c1", 6),
            Offline("o1", 7),
        };

        var rollup = FleetRollup.Build(summaries, NoTotals);

        Assert.Equal(7, rollup.TotalServers);
        Assert.Equal(3, rollup.HealthyCount);
        Assert.Equal(2, rollup.WarningCount);
        Assert.Equal(1, rollup.CriticalCount);
        Assert.Equal(1, rollup.OfflineCount);
    }

    [Fact]
    public void Build_CountsServersWithCollectionFailures_FromTheCardsFailingCount()
    {
        var summaries = new[]
        {
            WarningCollectors("w1", 1, failed: 2),   // 2 failing collectors → counts once
            Critical("c1", 2, deadlocks: 1),         // critical but no failing collector → not counted
            new ServerSummaryItem { DisplayName = "c2", ServerId = 3, IsOnline = true, DeadlockCount = 1, FailedCollectorCount = 3 }, // both
            Healthy("h1", 4),
        };

        var rollup = FleetRollup.Build(summaries, NoTotals);

        // Servers whose card FailedCollectorCount > 0: w1 and c2 → 2.
        Assert.Equal(2, rollup.ServersWithCollectionFailures);
    }

    // ── Worst-first ranking ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void Build_RanksProblemServersWorstFirst_OfflineThenCriticalThenWarning()
    {
        var summaries = new[]
        {
            Healthy("h1", 1),
            WarningCollectors("w1", 2),
            Critical("c1", 3),
            Offline("o1", 4),
        };

        var rollup = FleetRollup.Build(summaries, NoTotals);

        // Healthy is excluded from the "needs attention" ranking; problems ordered worst-first.
        Assert.Equal(3, rollup.WorstServers.Count);
        Assert.Equal(new[] { "o1", "c1", "w1" }, rollup.WorstServers.Select(w => w.DisplayName).ToArray());
        Assert.Equal(FleetHealthBand.Offline, rollup.WorstServers[0].Band);
        Assert.Equal(FleetHealthBand.Critical, rollup.WorstServers[1].Band);
        Assert.Equal(FleetHealthBand.Warning, rollup.WorstServers[2].Band);
        Assert.True(rollup.HasProblems);
    }

    [Fact]
    public void Build_WithinTheSameBand_RanksByHowManyMetricsAreBad()
    {
        // Two Critical servers: one with two critical metrics (CPU + deadlock), one with a single deadlock.
        var twoCritical = new ServerSummaryItem { DisplayName = "worse", ServerId = 1, IsOnline = true, CpuPercent = 96, DeadlockCount = 1 };
        var oneCritical = new ServerSummaryItem { DisplayName = "milder", ServerId = 2, IsOnline = true, DeadlockCount = 1 };

        var rollup = FleetRollup.Build(new[] { oneCritical, twoCritical }, NoTotals);

        Assert.Equal(new[] { "worse", "milder" }, rollup.WorstServers.Select(w => w.DisplayName).ToArray());
        Assert.True(FleetRollup.FleetHealthScore(twoCritical) > FleetRollup.FleetHealthScore(oneCritical));
    }

    [Fact]
    public void Build_CapsTheRankingAtWorstCount_AndReportsTheOverflow()
    {
        var summaries = Enumerable.Range(1, 8).Select(i => Critical($"c{i}", i, deadlocks: 1)).ToArray();

        var rollup = FleetRollup.Build(summaries, NoTotals, worstCount: 3);

        Assert.Equal(3, rollup.WorstServers.Count);
        Assert.Equal(5, rollup.AdditionalProblemCount);            // 8 problems − 3 shown
        Assert.Equal("+5 more need attention", rollup.AdditionalProblemText);
    }

    [Fact]
    public void Build_AllHealthy_HasNoProblems_AndAnAllClearLine()
    {
        var summaries = new[] { Healthy("h1", 1), Healthy("h2", 2) };

        var rollup = FleetRollup.Build(summaries, NoTotals);

        Assert.False(rollup.HasProblems);
        Assert.Empty(rollup.WorstServers);
        Assert.Equal(0, rollup.AdditionalProblemCount);
        Assert.Equal("", rollup.AdditionalProblemText);
        Assert.Equal("All 2 servers healthy", rollup.AllHealthyText);
    }

    [Fact]
    public void FleetHealthScore_BandRankDominates_AcrossBands()
    {
        var offline = FleetRollup.FleetHealthScore(Offline("o", 1));
        var critical = FleetRollup.FleetHealthScore(Critical("c", 2));
        var warning = FleetRollup.FleetHealthScore(WarningCollectors("w", 3));
        var healthy = FleetRollup.FleetHealthScore(Healthy("h", 4));

        Assert.True(offline > critical);
        Assert.True(critical > warning);
        Assert.True(warning > healthy);
    }

    // ── Totals passthrough + empty fleet ────────────────────────────────────────────────────────────

    [Fact]
    public void Build_PassesTheCrossServerTotalsThrough()
    {
        var totals = new FleetTotals { TotalBlockingEvents = 42, TotalDeadlocks = 7 };

        var rollup = FleetRollup.Build(new[] { Healthy("h1", 1) }, totals);

        Assert.Equal(42, rollup.TotalBlockingEvents);
        Assert.Equal(7, rollup.TotalDeadlocks);
    }

    [Fact]
    public void Build_TotalServers_UsesTheRegisteredCount_NotJustTheLoadedSummaries()
    {
        /* #2753: a transient per-server summary-read failure drops that server from `summaries` for THIS
           cycle only (MainWindow's LoadOverviewAsync catches the failure and returns null, then filters
           nulls out before calling Build). Deriving TotalServers from summaries.Count made the Overview's
           "Total Servers" wobble cycle to cycle while the sidebar — sourced from the stable server registry
           — never moved. TotalServers must equal the registered fleet size passed via totalServerCount,
           even when fewer summaries actually loaded this cycle. */
        var summaries = new[] { Healthy("h1", 1), Healthy("h2", 2) }; // 2 loaded...

        var rollup = FleetRollup.Build(summaries, NoTotals, totalServerCount: 5); // ...of 5 registered

        Assert.Equal(5, rollup.TotalServers);
        // The band counts still reflect only what actually loaded — that part is correct as-is.
        Assert.Equal(2, rollup.HealthyCount);
    }

    [Fact]
    public void Build_TotalServers_DefaultsToSummariesCount_WhenNotSpecified()
    {
        // Existing callers (every other test in this file) don't pass totalServerCount — must keep working
        // exactly as before: TotalServers falls back to summaries.Count.
        var rollup = FleetRollup.Build(new[] { Healthy("h1", 1), Healthy("h2", 2) }, NoTotals);

        Assert.Equal(2, rollup.TotalServers);
    }

    [Fact]
    public void Build_NoServers_StillCarriesTotals_ButHasNoRanking()
    {
        var totals = new FleetTotals { TotalBlockingEvents = 5, TotalDeadlocks = 2 };

        var rollup = FleetRollup.Build(Array.Empty<ServerSummaryItem>(), totals);

        Assert.Equal(0, rollup.TotalServers);
        Assert.Empty(rollup.WorstServers);
        Assert.False(rollup.HasProblems);
        Assert.Equal(5, rollup.TotalBlockingEvents);
        Assert.Equal(2, rollup.TotalDeadlocks);
    }

    [Fact]
    public void Build_EveryServerFailedToLoadThisCycle_StillReportsTheRegisteredTotal()
    {
        // #2753's worst case: every per-server summary read failed this one cycle (summaries is empty),
        // but the fleet is not — TotalServers must still be the registered count, not 0, so the Overview
        // panel doesn't collapse (FleetRollupContainer.Visibility keys off TotalServers > 0) just because
        // one bad sweep happened to fail everything.
        var rollup = FleetRollup.Build(Array.Empty<ServerSummaryItem>(), NoTotals, totalServerCount: 5);

        Assert.Equal(5, rollup.TotalServers);
        Assert.Empty(rollup.WorstServers);
    }

    // ── Reason text + ranked-row display ────────────────────────────────────────────────────────────

    [Fact]
    public void BuildReason_Offline_SaysSo()
    {
        Assert.Equal("Offline — no recent collection", FleetRollup.BuildReason(Offline("s", 1)));
    }

    [Fact]
    public void BuildReason_AwaitingFirstCollection_SaysSo()
    {
        Assert.Equal("Awaiting first collection", FleetRollup.BuildReason(Awaiting("s", 1)));
    }

    [Fact]
    public void BuildReason_NamesTheBadMetrics_FromTheCardsOwnDisplays()
    {
        var s = new ServerSummaryItem
        {
            IsOnline = true,
            CpuPercent = 96,                 // CPU 96%
            BlockingCount = 6, MaxBlockingWaitMs = 70000,
            DeadlockCount = 2,
            FailedCollectorCount = 1,
        };

        var reason = FleetRollup.BuildReason(s);

        Assert.Contains("CPU 96%", reason, StringComparison.Ordinal);
        Assert.Contains("Blocking 6", reason, StringComparison.Ordinal);
        Assert.Contains("Deadlocks 2", reason, StringComparison.Ordinal);
        Assert.Contains("1 collector failing", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildReason_StaleOnly_SaysCollectionStale()
    {
        Assert.Equal("collection stale", FleetRollup.BuildReason(Stale("s", 1)));
    }

    [Fact]
    public void RankedServer_LabelAndBrush_MatchTheBand()
    {
        var rollup = FleetRollup.Build(new[] { Critical("c1", 1), Offline("o1", 2), WarningCollectors("w1", 3) }, NoTotals);

        var offline = rollup.WorstServers.Single(w => w.DisplayName == "o1");
        var critical = rollup.WorstServers.Single(w => w.DisplayName == "c1");
        var warning = rollup.WorstServers.Single(w => w.DisplayName == "w1");

        Assert.Equal("Offline", offline.BandLabel);
        Assert.Equal("#FF888888", offline.BandBrush.Color.ToString());   // grey (the card's Unknown palette)
        Assert.Equal("Critical", critical.BandLabel);
        Assert.Equal("#FFE57373", critical.BandBrush.Color.ToString());  // red
        Assert.Equal("Warning", warning.BandLabel);
        Assert.Equal("#FFFFD54F", warning.BandBrush.Color.ToString());   // amber
    }

    // ── Header / total display helpers (singular / plural) ──────────────────────────────────────────

    [Fact]
    public void DisplayHelpers_Pluralize()
    {
        var one = FleetRollup.Build(new[] { Healthy("h1", 1) }, NoTotals);
        Assert.Equal("Monitoring 1 server", one.MonitoringText);
        Assert.Equal("All 1 server healthy", one.AllHealthyText);

        var many = FleetRollup.Build(new[]
        {
            Healthy("h1", 1),
            new ServerSummaryItem { DisplayName = "w1", ServerId = 2, IsOnline = true, FailedCollectorCount = 1 },
        }, NoTotals);
        Assert.Equal("Monitoring 2 servers", many.MonitoringText);
        Assert.Equal("1 server", many.CollectionFailuresText);

        var twoFailing = FleetRollup.Build(new[]
        {
            new ServerSummaryItem { DisplayName = "w1", ServerId = 1, IsOnline = true, FailedCollectorCount = 1 },
            new ServerSummaryItem { DisplayName = "w2", ServerId = 2, IsOnline = true, FailedCollectorCount = 2 },
        }, NoTotals);
        Assert.Equal("2 servers", twoFailing.CollectionFailuresText);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for the Fleet-wide totals read against planted rows across
/// MULTIPLE servers — the cross-server aggregate the Dashboard/Lite cannot run. Verifies the per-server
/// XE→DMV blocking fallback is applied PER server before the fleet SUM (an XE server counts its XE rows; a
/// DMV-only / RDS server counts its DMV rows) and that deadlocks total across servers. Uses a fixed
/// historical sentinel window so only the planted rows fall inside it, making the fleet-wide (un-scoped)
/// read deterministic regardless of any real data in the store; negative sentinel server_ids, cleaned up in
/// finally. Shares the serialized "live-postgres" collection.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerFleetRollupLivePostgresTests
{
    private const int XeServerId = -939301;   // has XE reports (XE preferred)
    private const int DmvServerId = -939302;  // DMV snapshots only (fallback)
    private const string ServerName = "viewer-fleet-e2e";

    /* A fixed historical window no real collected row occupies, so the fleet-wide read sees only the
       planted rows. */
    private static readonly DateTime WindowAnchor = new(2001, 3, 14, 9, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task FleetTotals_SumAcrossServers_WithPerServerXeDmvFallback_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet-totals test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteFleetRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var at = WindowAnchor;

            /* Server XE: two XE blocked-process reports + one DMV snapshot in the window → the fallback
               prefers XE, so this server contributes 2 (the DMV row is ignored). One deadlock. */
            await InsertBlockedProcessAsync(connection, XeServerId, at);
            await InsertBlockedProcessAsync(connection, XeServerId, at.AddMinutes(1));
            await InsertDmvBlockingAsync(connection, XeServerId, at);
            await InsertDeadlockAsync(connection, XeServerId, at);

            /* Server DMV: no XE reports, three DMV snapshots in the window → the fallback uses DMV, so this
               server contributes 3. Two deadlocks. */
            await InsertDmvBlockingAsync(connection, DmvServerId, at);
            await InsertDmvBlockingAsync(connection, DmvServerId, at.AddMinutes(1));
            await InsertDmvBlockingAsync(connection, DmvServerId, at.AddMinutes(2));
            await InsertDeadlockAsync(connection, DmvServerId, at);
            await InsertDeadlockAsync(connection, DmvServerId, at.AddMinutes(1));

            var totals = await viewer.GetFleetTotalsAsync(at.AddMinutes(-5), at.AddMinutes(30));

            /* Fleet blocking = 2 (XE server, XE-preferred) + 3 (DMV server, fallback) = 5. */
            Assert.Equal(5, totals.TotalBlockingEvents);
            /* Fleet deadlocks = 1 + 2 = 3. */
            Assert.Equal(3, totals.TotalDeadlocks);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteFleetRowsAsync(cleanup, cleanupCt));
        }
    }

    // ── planting helpers ─────────────────────────────────────────────────────────────

    private static async Task InsertBlockedProcessAsync(NpgsqlConnection connection, int serverId, DateTime eventTime)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time) VALUES ($1, $2, $3, $4, $5)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertDmvBlockingAsync(NpgsqlConnection connection, int serverId, DateTime eventTime)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time) VALUES ($1, $2, $3, $4, $5)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertDeadlockAsync(NpgsqlConnection connection, int serverId, DateTime deadlockTime)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time) VALUES ($1, $2, $3, $4, $5)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(deadlockTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(deadlockTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeleteFleetRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        foreach (var table in new[] { "blocked_process_reports", "dmv_blocking_snapshots", "deadlocks" })
        {
            using var cleanup = new NpgsqlCommand(
                $"DELETE FROM {table} WHERE server_id IN ({XeServerId}, {DmvServerId});", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
