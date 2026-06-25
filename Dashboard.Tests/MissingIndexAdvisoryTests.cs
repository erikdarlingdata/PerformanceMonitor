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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Controls;
using PerformanceMonitorDashboard.Services.Recommendations;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// WS4: missing-index advisory — the COPY-PASTE-ONLY recommendation. A MISSING_INDEX finding's
/// drill-down carries the SQL Server-suggested CREATE INDEX statement(s); the drill-down is
/// ephemeral (not persisted / not read back), so the statements ride the persisted
/// <see cref="RemediationAction"/> (FactKey "MISSING_INDEX") through to the reader — exactly like the
/// autogrowth advisory, but with NO handler so it never offers Apply. These tests cover the builder,
/// the persisted-action round-trip, the reader mapping (copy-paste set, Remediation null), and the
/// card affordances (Copy fix shown, Apply hidden, incident affordances preserved).
/// </summary>
public class MissingIndexAdvisoryTests
{
    private const string CreateStmt =
        "CREATE NONCLUSTERED INDEX [ix_Orders_CustomerId] ON [dbo].[Orders] ([CustomerId]) INCLUDE ([OrderDate]);";

    private static AnalysisFinding MissingIndexFinding(List<object>? rows = null) => new()
    {
        ServerId = 1,
        ServerName = "TestServer",
        Category = "missing_index",
        StoryPath = "MISSING_INDEX",
        StoryPathHash = "missingidx000001",
        RootFactKey = "MISSING_INDEX",
        Severity = 0.4,
        DrillDown = new Dictionary<string, object>
        {
            // SqlServerDrillDownCollector "missing_indexes": { table, impact, create_statement }.
            ["missing_indexes"] = rows ?? new List<object>
            {
                new { table = "dbo.Orders", impact = 87.3, create_statement = CreateStmt }
            }
        }
    };

    // ── Builder + extractor ───────────────────────────────────────────────────────

    [Fact]
    public void BuildMissingIndexAction_ProducesAdviseActionWithTargets()
    {
        var action = FactRemediation.BuildMissingIndexAction(MissingIndexFinding());

        Assert.NotNull(action);
        Assert.Equal("MISSING_INDEX", action!.FactKey);
        Assert.Equal("advise", action.Action);
        Assert.Empty(action.Targets);                       // not a force-plan action
        Assert.NotNull(action.MissingIndexTargets);
        var t = Assert.Single(action.MissingIndexTargets!);
        Assert.Equal("dbo.Orders", t.Table);
        Assert.Equal(87.3, t.Impact);
        Assert.Equal(CreateStmt, t.CreateStatement);
    }

    [Fact]
    public void BuildMissingIndexAction_WrongRootFactKey_ReturnsNull()
    {
        var finding = MissingIndexFinding();
        finding.RootFactKey = "PLAN_WARNING";               // a sibling advisory, not missing-index
        Assert.Null(FactRemediation.BuildMissingIndexAction(finding));
    }

    [Fact]
    public void BuildMissingIndexAction_NoDrillDownOrEmpty_ReturnsNull()
    {
        Assert.Null(FactRemediation.BuildMissingIndexAction(new AnalysisFinding { RootFactKey = "MISSING_INDEX" }));
        Assert.Null(FactRemediation.BuildMissingIndexAction(MissingIndexFinding(new List<object>())));
    }

    [Fact]
    public void ExtractMissingIndexTargets_SkipsRowsWithoutCreateStatement_AndCapsAtFive()
    {
        // A row with no create_statement has nothing to copy → skipped.
        var noStmt = FactRemediation.ExtractMissingIndexTargets(MissingIndexFinding(new List<object>
        {
            new { table = "dbo.A", impact = 10.0, create_statement = "" }
        }));
        Assert.Empty(noStmt);

        var many = Enumerable.Range(1, 8)
            .Select(i => (object)new { table = $"dbo.T{i}", impact = (double)i, create_statement = $"CREATE INDEX ix{i} ON dbo.T{i}(c);" })
            .ToList();
        Assert.Equal(5, FactRemediation.ExtractMissingIndexTargets(MissingIndexFinding(many)).Count);
    }

    // ── Persisted-action round-trip (the drill-down is ephemeral; the action must survive) ──

    [Fact]
    public void SerializeAction_RoundTrips_MissingIndexTargets()
    {
        var action = FactRemediation.BuildMissingIndexAction(MissingIndexFinding());
        var json = AlertContextSerializer.SerializeAction(action);
        var back = AlertContextSerializer.DeserializeAction(json);

        Assert.NotNull(back);
        Assert.Equal("MISSING_INDEX", back!.FactKey);
        var t = Assert.Single(back.MissingIndexTargets!);
        Assert.Equal("dbo.Orders", t.Table);
        Assert.Equal(87.3, t.Impact);
        Assert.Equal(CreateStmt, t.CreateStatement);
    }

    // ── Reader: copy-paste set, Remediation deliberately null (no Apply) ────────────

    [Fact]
    public void MapEngineFinding_MissingIndex_IsCopyOnly()
    {
        var finding = MissingIndexFinding();
        // Mirror AnalysisService: the built action is attached before persistence/read.
        finding.Remediation = FactRemediation.BuildMissingIndexAction(finding);

        var item = RecommendationsReader.MapEngineFinding(finding);

        Assert.Contains(CreateStmt, item.CopyPasteSql);     // the CREATE is the copy-paste payload
        Assert.Null(item.Remediation);                       // copy-only — never Apply
        Assert.True(item.IsMissingIndexAdvisory);
        Assert.Equal(RecommendationSetting.None, item.Setting);  // stays a non-de-duping incident
    }

    [Fact]
    public void MapEngineFinding_MissingIndex_JoinsMultipleCreateStatements()
    {
        var finding = MissingIndexFinding(new List<object>
        {
            new { table = "dbo.A", impact = 50.0, create_statement = "CREATE INDEX ixa ON dbo.A(c);" },
            new { table = "dbo.B", impact = 40.0, create_statement = "CREATE INDEX ixb ON dbo.B(c);" }
        });
        finding.Remediation = FactRemediation.BuildMissingIndexAction(finding);

        var item = RecommendationsReader.MapEngineFinding(finding);
        Assert.Contains("CREATE INDEX ixa ON dbo.A(c);", item.CopyPasteSql);
        Assert.Contains("CREATE INDEX ixb ON dbo.B(c);", item.CopyPasteSql);
    }

    // ── Card affordances: Copy fix shown, Apply hidden, incident affordances preserved ──

    [Fact]
    public void CardViewModel_MissingIndex_ShowsCopyFix_NotApply_StillIncident()
    {
        var finding = MissingIndexFinding();
        finding.Remediation = FactRemediation.BuildMissingIndexAction(finding);
        var vm = new RecommendationCardViewModel(RecommendationsReader.MapEngineFinding(finding));

        Assert.True(vm.ShowCopyFix);                 // the CREATE INDEX is copyable
        Assert.False(vm.ShowApply);                  // copy-only — no handler, no Apply button
        Assert.True(vm.IsIncident);                  // stays an incident...
        Assert.True(vm.ShowAskAi);                   // ...so Ask-AI...
        Assert.True(vm.ShowOpenInActiveQueries);     // ...and Open-in-Active-Queries remain
    }
}
