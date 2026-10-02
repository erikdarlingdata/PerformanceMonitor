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
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pure pins for how the alert notebook finds an alert's resolution (#4755) and what a fleet-level store alert
/// reads when there is none (#4756). The live half, which needs a real <c>config_alert_log</c>, is
/// <see cref="AlertNotebookResolutionReadLiveTests"/>.
///
/// <para><b>#4755.</b> Status arms 1 and 2 used to scan a 24-hour, 200-row, newest-first, dismissed-excluded
/// window, so a resolution that landed later than 24 hours, sat behind 200 newer rows, or had been hidden by the
/// viewer's "Dismiss all" was invisible. They now read the FIRST resolution row and the FIRST later firing
/// straight from the store. The names the resolution read looks for come from
/// <see cref="AlertNotebookEndpoint.ResolutionRowNames"/>, pinned here against the two tables it folds.</para>
///
/// <para><b>#4756.</b> A fleet-level store alert has no collector: the process that evaluates it also serves the
/// page, so "Unknown (not collected since ...)" (which says an instrument is down) cannot be true of it.</para>
/// </summary>
public sealed class AlertNotebookResolutionReadTests
{
    private static readonly DateTime Anchor = new(2026, 9, 1, 12, 0, 0, DateTimeKind.Utc);

    private static DarlingAlertReader.AlertHistoryReadRow Row(DateTime alertTime, string metric, int serverId = 0) =>
        new(alertTime, serverId, "test-server", metric, 0, 0, false, "none", null, false, null, false);

    /* ═══════════════════════════ #4756: a fleet-level store alert with no resolution ═══════════════════════════ */

    [Fact]
    public void StoreSelfAlertMetric_IsFleetLevel_SoTheseTestsExerciseTheStorePath()
    {
        Assert.True(DarlingTriageEndpoint.IsFleetLevelStoreMetric(DarlingSelfAlertEvaluator.DiskPressureMetric));
    }

    [Fact]
    public async Task FleetLevelStoreAlert_NoResolutionRow_ReadsNoResolutionRecorded()
    {
        var status = await AlertNotebookEndpoint.ResolveStatusAsync(
            postgres: null!, serverId: null, fleetLevelStore: true, DarlingSelfAlertEvaluator.DiskPressureMetric,
            Anchor, Anchor.AddDays(2), new List<DarlingAlertReader.AlertHistoryReadRow>(),
            matchedRow: null, NullLogger.Instance, TestContext.Current.CancellationToken);

        Assert.Equal("No resolution recorded", status);
    }

    [Fact]
    public async Task FleetLevelStoreAlert_WithAResolutionRow_ReadsResolved()
    {
        var resolvedAt = Anchor.AddHours(30);
        var status = await AlertNotebookEndpoint.ResolveStatusAsync(
            postgres: null!, serverId: null, fleetLevelStore: true, DarlingSelfAlertEvaluator.DiskPressureMetric,
            Anchor, Anchor.AddDays(2),
            new List<DarlingAlertReader.AlertHistoryReadRow> { Row(resolvedAt, DarlingSelfAlertEvaluator.DiskPressureResolvedMetric) },
            matchedRow: null, NullLogger.Instance, TestContext.Current.CancellationToken);

        Assert.Equal("Resolved at 2026-09-02T18:00:00Z", status);
    }

    [Fact]
    public async Task FleetLevelStoreAlert_WithALaterFiring_ReadsFiredAgain()
    {
        var status = await AlertNotebookEndpoint.ResolveStatusAsync(
            postgres: null!, serverId: null, fleetLevelStore: true, DarlingSelfAlertEvaluator.DiskPressureMetric,
            Anchor, Anchor.AddDays(2),
            new List<DarlingAlertReader.AlertHistoryReadRow> { Row(Anchor.AddHours(3), DarlingSelfAlertEvaluator.DiskPressureMetric) },
            matchedRow: null, NullLogger.Instance, TestContext.Current.CancellationToken);

        Assert.Equal("Fired again at 2026-09-01T15:00:00Z", status);
    }

    [Fact]
    public async Task NonFleetAlert_WithNoServer_StillReadsUnknown()
    {
        /* The other half of the boundary: only a fleet-level STORE alert gets the new answer. A per-server
           alert whose server did not resolve is still "Unknown", because for it the instrument really may be
           down or unreachable. */
        var status = await AlertNotebookEndpoint.ResolveStatusAsync(
            postgres: null!, serverId: null, fleetLevelStore: false, "High CPU",
            Anchor, Anchor.AddDays(2), new List<DarlingAlertReader.AlertHistoryReadRow>(),
            matchedRow: null, NullLogger.Instance, TestContext.Current.CancellationToken);

        Assert.StartsWith("Unknown (not collected since ", status, StringComparison.Ordinal);
    }

    /* ═══════════════════════════ #4755: the names the resolution read looks for ═══════════════════════════ */

    [Fact]
    public void ResolutionRowNames_StoreAlert_IsItsResolvedAlias_ExactlyAsTheProductWritesIt()
    {
        var names = AlertNotebookEndpoint.ResolutionRowNames(DarlingSelfAlertEvaluator.DiskPressureMetric);

        Assert.Equal(new[] { DarlingSelfAlertEvaluator.DiskPressureResolvedMetric }, names);
    }

    [Fact]
    public void ResolutionRowNames_MatchesTheInputCaseInsensitively_ButReturnsTheProductsSpelling()
    {
        /* The store read compares with `=`, so the lookup has to be the product's own spelling; only the
           INPUT (a hand-typed link) is forgiven. */
        var names = AlertNotebookEndpoint.ResolutionRowNames("  store DISK pressure ");

        Assert.Equal(new[] { DarlingSelfAlertEvaluator.DiskPressureResolvedMetric }, names);
        Assert.Equal(new[] { "CPU Resolved" }, AlertNotebookEndpoint.ResolutionRowNames("high cpu"));
    }

    [Fact]
    public void ResolutionRowNames_AgAndServerFamilies_UseTheirRecoveryEdges()
    {
        Assert.Equal(new[] { "Server Restored" }, AlertNotebookEndpoint.ResolutionRowNames("Server Unreachable"));
        Assert.Equal(new[] { "AG Replica Reconnected" }, AlertNotebookEndpoint.ResolutionRowNames("AG Replica Disconnected"));
    }

    [Fact]
    public void ResolutionRowNames_MetricWithNoRecoveryEdge_IsEmpty()
    {
        Assert.Empty(AlertNotebookEndpoint.ResolutionRowNames("AG Failover"));
        Assert.Empty(AlertNotebookEndpoint.ResolutionRowNames("not a metric anyone fires"));
    }

    [Fact]
    public void ResolutionRowNames_CoversEveryAliasAndEveryRecoveryEdge()
    {
        /* Census: the read may not have a blind spot the classifier does not, so every alias must come back
           for its canonical metric and every recovery edge for its firing metric. */
        foreach (var (alias, canonical) in DarlingTriageEndpoint.ResolutionAliases)
        {
            Assert.Contains(alias, AlertNotebookEndpoint.ResolutionRowNames(canonical));
        }

        foreach (var (firing, recovery) in AlertNotebookEndpoint.NotebookRecoveryEdges)
        {
            Assert.Contains(recovery, AlertNotebookEndpoint.ResolutionRowNames(firing));
        }
    }

    [Fact]
    public void ResolutionRowNames_NeverReturnsAFiringName()
    {
        /* A firing name in the list would make the read return a later FIRING as the resolution. */
        var canonicals = DarlingTriageEndpoint.ResolutionAliases.Select(pair => pair.Canonical)
            .Concat(AlertNotebookEndpoint.NotebookRecoveryEdges.Select(pair => pair.Firing))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        foreach (var canonical in canonicals)
        {
            Assert.DoesNotContain(canonical, AlertNotebookEndpoint.ResolutionRowNames(canonical));
        }
    }

    /* ═══════════════════════════ #4755: the shape of the two targeted reads ═══════════════════════════ */

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void FirstAlertAfterSql_IsUnwindowed_DismissedIncluded_OneEarliestRow(bool serverScoped, bool excludesAlertTime)
    {
        var sql = DarlingAlertReader.FirstAlertAfterSql(serverScoped, excludesAlertTime);
        /* The select list names `dismissed` and `server_id` as columns; the absences below are about the
           predicate, which starts at WHERE. */
        var predicate = sql[sql.IndexOf("WHERE", StringComparison.Ordinal)..];

        Assert.Contains("FROM config_alert_log", sql, StringComparison.Ordinal);
        Assert.Contains("alert_time > $1", sql, StringComparison.Ordinal);
        Assert.Contains("alert_time <= $2", sql, StringComparison.Ordinal);
        Assert.Contains("metric_name = ANY($3)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY alert_time\nLIMIT 1", sql.Replace("\r\n", "\n"), StringComparison.Ordinal);

        /* The three defects, each as an absence: no `dismissed` filter (Dismiss all hides resolution rows too),
           no descending order, and no row cap beyond the one row wanted. */
        Assert.DoesNotContain("dismissed", predicate, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("DESC", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT 200", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("interval", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void FirstAlertAfterSql_ScopesToTheServerOnlyWhenOneIsKnown()
    {
        /* The display-name join names `s.server_id = a.server_id`; the scope is the predicate after WHERE. */
        var unscoped = DarlingAlertReader.FirstAlertAfterSql(serverScoped: false, excludesAlertTime: false);
        Assert.DoesNotContain("server_id =", unscoped[unscoped.IndexOf("WHERE", StringComparison.Ordinal)..], StringComparison.Ordinal);
        Assert.Contains("server_id = $4", DarlingAlertReader.FirstAlertAfterSql(serverScoped: true, excludesAlertTime: false), StringComparison.Ordinal);
    }

    [Fact]
    public void FirstAlertAfterSql_ExcludedAlertTime_TakesTheNextFreeParameterNumber()
    {
        Assert.Contains("alert_time <> $4", DarlingAlertReader.FirstAlertAfterSql(serverScoped: false, excludesAlertTime: true), StringComparison.Ordinal);
        Assert.Contains("alert_time <> $5", DarlingAlertReader.FirstAlertAfterSql(serverScoped: true, excludesAlertTime: true), StringComparison.Ordinal);
    }

    [Fact]
    public void TheHandler_ReadsStatusRowsThroughTheTargetedReads_NotTheWideWindow()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "AlertNotebookEndpoint.cs");

        Assert.Contains("await ReadStatusRowsAsync(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("statusUntil", source, StringComparison.Ordinal);
        Assert.Contains("DarlingAlertReader.GetFirstResolutionAfterAsync(", source, StringComparison.Ordinal);
        Assert.Contains("DarlingAlertReader.GetFirstRefireAfterAsync(", source, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadStatusRowsAsync_WhenTheAnswerCannotDependOnARow_MakesNoStoreRoundTrip()
    {
        /* A null data source would throw on first use, so a clean return proves no read was attempted. */
        var ct = TestContext.Current.CancellationToken;
        var now = Anchor.AddDays(1);

        Assert.Empty(await AlertNotebookEndpoint.ReadStatusRowsAsync(
            null!, serverId: 7, fleetLevelStore: false, "  ", Anchor, now, matchedRow: null, ct));
        Assert.Empty(await AlertNotebookEndpoint.ReadStatusRowsAsync(
            null!, serverId: null, fleetLevelStore: false, "High CPU", Anchor, now, matchedRow: null, ct));
        Assert.Empty(await AlertNotebookEndpoint.ReadStatusRowsAsync(
            null!, serverId: 7, fleetLevelStore: false, "High CPU", now, now, matchedRow: null, ct));
    }

    /* ═══════ the qualified select list needs the joined FROM: every SQL that selects it ═══════ */

    [Fact]
    public void EverySqlSelectingTheQualifiedAlertColumns_ReadsFromTheAliasedJoinedTable()
    {
        var all = new List<string>();
        foreach (var type in new[] { typeof(DarlingAlertReader), typeof(PerformanceMonitor.Darling.Viewer.ViewerDataService) })
        {
            foreach (var field in type.GetFields(System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic))
            {
                if (field.IsLiteral && field.GetValue(null) is string text && text.Contains("COALESCE(s.display_name, a.server_name)", StringComparison.Ordinal)
                    && text.Contains("FROM ", StringComparison.Ordinal))
                {
                    all.Add(text);
                }
            }
        }

        foreach (var scoped in new[] { false, true })
        {
            foreach (var excludes in new[] { false, true })
            {
                all.Add(DarlingAlertReader.FirstAlertAfterSql(scoped, excludes));
            }
        }

        Assert.True(all.Count >= 8, "expected the page reads, the viewer reads and the four first-row-after shapes");
        foreach (var sql in all)
        {
            Assert.Contains("FROM config_alert_log a", sql, StringComparison.Ordinal);
            Assert.Contains("LEFT JOIN servers s ON s.server_id = a.server_id", sql, StringComparison.Ordinal);
        }
    }
}
