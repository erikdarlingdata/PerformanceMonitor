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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the W2b Recommendations re-skin's pure shaping: the finding-row -> advise-only card mapping,
/// the Copy-fix payload rebuilt from the PERSISTED remediation action (the viewer's read-back source,
/// mirroring the Dashboard reader), the Ask-AI MCP prompt text (ported verbatim from Lite), and the
/// affordance model. WPF-free — <see cref="RecommendationsViewModel"/> and its card/section DTOs carry
/// no WPF dependency, so the mapping + grouping are unit-testable directly.
/// </summary>
public sealed class ViewerRecommendationCardTests
{
    private static AnalysisFinding Finding(
        double severity = 1.0,
        string rootFactKey = "SOME_UNKNOWN_FACT_KEY",
        string storyText = "",
        string? database = null,
        string incidentId = "",
        RemediationAction? remediation = null,
        DateTime? windowStart = null,
        DateTime? windowEnd = null)
        => new()
        {
            FindingId = 1,
            AnalysisTime = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            ServerId = 1,
            ServerName = "SQL2022",
            DatabaseName = database,
            TimeRangeStart = windowStart,
            TimeRangeEnd = windowEnd,
            Severity = severity,
            Confidence = 0.9,
            Category = "cpu",
            StoryPath = "a -> b",
            StoryPathHash = "hash-" + rootFactKey + severity,
            StoryText = storyText,
            RootFactKey = rootFactKey,
            IncidentId = incidentId,
            FactCount = 1,
            Remediation = remediation,
        };

    private static ViewerFindingRow Row(AnalysisFinding finding, string? title = null)
        => new()
        {
            SeverityBand = ViewerDataService.SeverityBand(finding.Severity),
            RawSeverity = finding.Severity,
            Title = title ?? ViewerDataService.FindingTitle(finding),
            Category = finding.Category,
            DatabaseName = finding.DatabaseName ?? "",
            Finding = finding,
        };

    [Theory]
    [InlineData(2.0, RecommendationSeverity.Critical)]
    [InlineData(1.5, RecommendationSeverity.Critical)]
    [InlineData(1.49, RecommendationSeverity.Warning)]
    [InlineData(0.75, RecommendationSeverity.Warning)]
    [InlineData(0.74, RecommendationSeverity.Info)]
    [InlineData(0.0, RecommendationSeverity.Info)]
    public void ToSeverity_UsesTheSharedThreeBandCutoffs(double severity, RecommendationSeverity expected)
    {
        Assert.Equal(expected, RecommendationsViewModel.ToSeverity(severity));
    }

    [Fact]
    public void ComposeAdvice_JoinsRemediationThenInvestigation_NullWhenNeither()
    {
        Assert.Null(RecommendationsViewModel.ComposeAdvice(null));
        Assert.Null(RecommendationsViewModel.ComposeAdvice(new AdviceBlock("head", "", "")));

        Assert.Equal("do this",
            RecommendationsViewModel.ComposeAdvice(new AdviceBlock("head", "", "do this")));
        Assert.Equal("do this look here",
            RecommendationsViewModel.ComposeAdvice(new AdviceBlock("head", "look here", "do this")));
    }

    [Fact]
    public void BuildAskAiPrompt_PortsLitesExactText_ReferencingTheMcpTools()
    {
        var from = new DateTime(2026, 7, 1, 14, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2026, 7, 1, 15, 30, 0, DateTimeKind.Utc);

        var prompt = RecommendationsViewModel.BuildAskAiPrompt("SQL2022", "CPU is on fire", from, to);

        Assert.Equal(
            "Using the PerformanceMonitor MCP tools, investigate this finding on server \"SQL2022\": " +
            "\"CPU is on fire\". It was flagged around 2026-07-01 14:00–15:30. Call analyze_server / " +
            "get_analysis_findings and the relevant wait/blocking/memory tools, then tell me the " +
            "likely cause and what to do.",
            prompt);
    }

    [Fact]
    public void BuildCopyPasteSql_NullOrEmptyAction_ReturnsNull()
    {
        Assert.Null(RecommendationsViewModel.BuildCopyPasteSql(null));

        // An action that carries no renderable target (an empty force-plan list) yields null — there is
        // nothing to run. (A force-plan action WITH targets now renders; see the force-plan test below.)
        var empty = new RemediationAction("PLAN_REGRESSION", "force", Array.Empty<ForcePlanTarget>());
        Assert.Null(RecommendationsViewModel.BuildCopyPasteSql(empty));
    }

    // ── The four shapes that were previously copy-paste dead ends (viewer has no executor) ──────────

    [Fact]
    public void BuildCopyPasteSql_ForcePlanTargets_RenderUseThenForcePlanPerDatabase()
    {
        // sp_query_store_force_plan is DATABASE-scoped, so each target is a standalone USE + EXEC block
        // (the copy-paste must run without an ambient database). Blocks are separated by a blank line.
        var action = new RemediationAction(
            "PLAN_REGRESSION", "force",
            new[]
            {
                new ForcePlanTarget("StackOverflow", QueryId: 42, PlanId: 7),
                new ForcePlanTarget("Crazy]Name", QueryId: 100, PlanId: 200),
            });

        var sql = RecommendationsViewModel.BuildCopyPasteSql(action);

        Assert.Equal(
            "USE [StackOverflow];" + Environment.NewLine +
            "EXEC sys.sp_query_store_force_plan @query_id = 42, @plan_id = 7;" + Environment.NewLine +
            Environment.NewLine +
            "USE [Crazy]]Name];" + Environment.NewLine +
            "EXEC sys.sp_query_store_force_plan @query_id = 100, @plan_id = 200;",
            sql);
    }

    [Fact]
    public void BuildCopyPasteSql_ServerConfigTargets_RenderSpConfigureViaTheSharedBuilder()
    {
        // Every server-config setting renders sp_configure + RECONFIGURE via the shared builder,
        // INCLUDING the advise-only memory settings (the operator wants the scaffold regardless).
        var action = new RemediationAction(
            "SERVER_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            ServerConfigTargets: new[]
            {
                new ServerConfigTarget(ServerConfigSetting.Maxdop, 0, 8),
                new ServerConfigTarget(ServerConfigSetting.MaxServerMemory, 2147483647, 2147483647),
            });

        var sql = RecommendationsViewModel.BuildCopyPasteSql(action);

        Assert.Equal(
            FactRemediation.BuildSpConfigureStatement(ServerConfigSetting.Maxdop, 8) +
            Environment.NewLine + Environment.NewLine +
            FactRemediation.BuildSpConfigureStatement(ServerConfigSetting.MaxServerMemory, 2147483647),
            sql);
    }

    [Fact]
    public void BuildCopyPasteSql_RcsiTargets_RenderAlterWithTwoSidedDisclosureHeader()
    {
        // RCSI rides a DB_CONFIG action as a per-database RcsiTarget (with real inaction figures). It is
        // DESTRUCTIVE, so the copy-paste prepends the two-sided risk disclosure as a comment header.
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            RcsiTargets: new[] { new RcsiTarget("StackOverflow", new RcsiInactionFigures(50, 3, 70)) });

        var sql = RecommendationsViewModel.BuildCopyPasteSql(action);

        Assert.NotNull(sql);
        // Two-sided disclosure header, rendered as a T-SQL block comment ABOVE the runnable statement.
        Assert.StartsWith("/*", sql, StringComparison.Ordinal);
        Assert.Contains("Risks of MAKING this change:", sql!, StringComparison.Ordinal);
        Assert.Contains("Risks of NOT making this change:", sql!, StringComparison.Ordinal);
        // The real carried figures reach the inaction side (50 blocked-process events, reader/writer).
        Assert.Contains("50 blocked-process events", sql!, StringComparison.Ordinal);
        Assert.Contains("readers blocked by writers", sql!, StringComparison.Ordinal);
        // The runnable statement, after the closing comment.
        Assert.Contains("ALTER DATABASE [StackOverflow] SET READ_COMMITTED_SNAPSHOT ON;", sql!, StringComparison.Ordinal);
        Assert.True(
            sql!.IndexOf("*/", StringComparison.Ordinal) <
            sql.IndexOf("ALTER DATABASE", StringComparison.Ordinal),
            "the disclosure comment header must precede the ALTER statement");
    }

    [Fact]
    public void BuildCopyPasteSql_ClearPlanTargets_RenderResolveAndFreeScriptWithDisclosureHeader()
    {
        // Clear-plan is DESTRUCTIVE. The honest runnable form live-resolves the currently-cached plan
        // handle(s) for the abnormal query hash and frees each — with the two-sided disclosure header.
        var action = new RemediationAction(
            "CLEAR_PLAN", "clear", Array.Empty<ForcePlanTarget>(),
            ClearPlanTargets: new[]
            {
                new ClearPlanTarget(
                    "StackOverflow", "0xABCDEF0123456789",
                    CurrentCpuPerExecMs: 45.0, BaselineCpuPerExecMs: 9.0, AnomalyRatio: 5.0),
            },
            ClearPlanFigures: new ClearPlanFigures(45.0, 9.0, 5.0, 62, false, false));

        var sql = RecommendationsViewModel.BuildCopyPasteSql(action);

        Assert.NotNull(sql);
        Assert.StartsWith("/*", sql, StringComparison.Ordinal);
        Assert.Contains("Risks of MAKING this change:", sql!, StringComparison.Ordinal);
        Assert.Contains("Risks of NOT making this change:", sql!, StringComparison.Ordinal);
        // The real carried anomaly figure (62% of CPU over the window) reaches the inaction side.
        Assert.Contains("62% of CPU over the window", sql!, StringComparison.Ordinal);
        // The honest runnable form: resolve cached handles for the hash, then free each one.
        Assert.Contains("sys.dm_exec_query_stats", sql!, StringComparison.Ordinal);
        Assert.Contains("0xABCDEF0123456789", sql!, StringComparison.Ordinal);
        Assert.Contains("DBCC FREEPROCCACHE(@plan_handle);", sql!, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildCopyPasteSql_DbConfigWithRcsi_RendersSafeBareThenRcsiWithHeader()
    {
        // A DB_CONFIG action can carry BOTH always-safe settings AND per-db RCSI. The safe ALTER renders
        // bare (non-destructive, no header); the RCSI ALTER follows with its two-sided disclosure header.
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            DbConfigTargets: new[] { new DbConfigTarget("StackOverflow", DbConfigSetting.AutoShrinkOff) },
            RcsiTargets: new[] { new RcsiTarget("StackOverflow", new RcsiInactionFigures(50, 3, 70)) });

        var sql = RecommendationsViewModel.BuildCopyPasteSql(action);

        Assert.NotNull(sql);
        // Safe fix comes first, bare (no disclosure header above it).
        Assert.StartsWith("ALTER DATABASE [StackOverflow] SET AUTO_SHRINK OFF;", sql, StringComparison.Ordinal);
        // Then the destructive RCSI block, with its header.
        Assert.Contains("Risks of MAKING this change:", sql!, StringComparison.Ordinal);
        Assert.Contains("ALTER DATABASE [StackOverflow] SET READ_COMMITTED_SNAPSHOT ON;", sql!, StringComparison.Ordinal);
        Assert.True(
            sql!.IndexOf("AUTO_SHRINK OFF", StringComparison.Ordinal) <
            sql.IndexOf("/*", StringComparison.Ordinal),
            "the bare safe fix must precede the RCSI disclosure header");
    }

    [Fact]
    public void BuildCopyPasteSql_DbConfigTargets_RenderAlterDatabaseSetPerTarget()
    {
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            DbConfigTargets: new[]
            {
                new DbConfigTarget("StackOverflow", DbConfigSetting.AutoShrinkOff),
                new DbConfigTarget("Crazy]Name", DbConfigSetting.PageVerifyChecksum),
            });

        var sql = RecommendationsViewModel.BuildCopyPasteSql(action);

        Assert.Equal(
            "ALTER DATABASE [StackOverflow] SET AUTO_SHRINK OFF;" + Environment.NewLine +
            "ALTER DATABASE [Crazy]]Name] SET PAGE_VERIFY CHECKSUM;",
            sql);
    }

    [Fact]
    public void BuildCopyPasteSql_MissingIndexTargets_RenderTheSuggestedCreateVerbatim()
    {
        var action = new RemediationAction(
            "MISSING_INDEX", "advise", Array.Empty<ForcePlanTarget>(),
            MissingIndexTargets: new[]
            {
                new MissingIndexTarget("dbo.Posts", 92.5, "CREATE INDEX IX_Posts_1 ON dbo.Posts (OwnerUserId);"),
                new MissingIndexTarget("dbo.Users", 80.0, "CREATE INDEX IX_Users_1 ON dbo.Users (Reputation);"),
            });

        var sql = RecommendationsViewModel.BuildCopyPasteSql(action);

        Assert.Equal(
            "CREATE INDEX IX_Posts_1 ON dbo.Posts (OwnerUserId);" + Environment.NewLine +
            "CREATE INDEX IX_Users_1 ON dbo.Users (Reputation);",
            sql);
    }

    [Fact]
    public void BuildCopyPasteSql_FileGrowthTargets_RenderModifyFileViaTheSharedBuilder()
    {
        var target = new FileGrowthTarget("StackOverflow", "SO_data", CurrentSizeMb: 90000, CurrentGrowthPercent: 10, RecommendedGrowthMb: 1024);
        var action = new RemediationAction(
            "FILE_AUTOGROWTH_PERCENT", "set", Array.Empty<ForcePlanTarget>(),
            FileGrowthTargets: new[] { target });

        var sql = RecommendationsViewModel.BuildCopyPasteSql(action);

        // Byte-identical to the shared renderer the drill-down/Dashboard reader use.
        Assert.Equal(FactRemediation.BuildModifyFileStatement("StackOverflow", "SO_data", 1024), sql);
    }

    [Fact]
    public void MapItem_ShapesTheCardFromTheRowAndFinding()
    {
        var frozen = FactAdvice.SerializeForStoryText(
            new AdviceBlock("CPU is on fire", "check the top queries", "tune or cap them"));
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            DbConfigTargets: new[] { new DbConfigTarget("StackOverflow", DbConfigSetting.AutoShrinkOff) });
        var finding = Finding(
            severity: 1.6, storyText: frozen, database: "StackOverflow", incidentId: "inc-1",
            remediation: action,
            windowStart: new DateTime(2026, 7, 1, 10, 0, 0, DateTimeKind.Utc),
            windowEnd: new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));

        var item = RecommendationsViewModel.MapItem(Row(finding), "SQL2022");

        Assert.Equal(RecommendationSeverity.Critical, item.Severity);
        Assert.Equal(1.6, item.RawSeverity);
        Assert.Equal("StackOverflow", item.Database);
        Assert.Equal("CPU is on fire", item.Title);                         // frozen advice headline
        Assert.Equal("tune or cap them check the top queries", item.AdviceText);
        Assert.Equal("ALTER DATABASE [StackOverflow] SET AUTO_SHRINK OFF;", item.CopyPasteSql);
        Assert.Equal("inc-1", item.IncidentId);
        Assert.Equal("SQL2022", item.ServerName);
        Assert.Equal(1, item.ServerId);                                     // carried from the finding (drives the deep-link tab)
        Assert.Equal(DateTimeKind.Utc, item.WindowStartUtc!.Value.Kind);
        Assert.Equal(new DateTime(2026, 7, 1, 10, 0, 0), item.WindowStartUtc!.Value);
    }

    [Fact]
    public void MapItem_ServerScopedFinding_HasNullDatabase()
    {
        var item = RecommendationsViewModel.MapItem(Row(Finding(database: null)), "SQL2022");
        Assert.Null(item.Database);
    }

    [Fact]
    public void Card_AffordanceModel_CopyFixOnlyWhenSqlPresent_AskAiAlways()
    {
        var withSql = new RecommendationCardViewModel(new RecommendationItem
        {
            Severity = RecommendationSeverity.Warning,
            Title = "t",
            CopyPasteSql = "ALTER DATABASE [x] SET AUTO_SHRINK OFF;",
        });
        Assert.True(withSql.ShowCopyFix);
        Assert.True(withSql.ShowAskAi);

        var noSql = new RecommendationCardViewModel(new RecommendationItem
        {
            Severity = RecommendationSeverity.Warning,
            Title = "t",
            CopyPasteSql = null,
        });
        Assert.False(noSql.ShowCopyFix);
        Assert.True(noSql.ShowAskAi);
    }

    // ── "Open in Active Queries" deep-link affordance + window (Dashboard-parity port) ──────

    [Fact]
    public void Card_ShowOpenInActiveQueries_TrueOnlyWithBothATimeWindowAndAServerId()
    {
        var window = (new DateTime(2026, 7, 1, 10, 0, 0, DateTimeKind.Utc), new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));

        // Incident: has a time window AND a server id -> shown.
        var incident = Card(serverId: 7, windowStart: window.Item1, windowEnd: window.Item2);
        Assert.True(incident.ShowOpenInActiveQueries);
        Assert.Equal(7, incident.ServerId);

        // No time anchor (config-only finding read with no window) -> hidden.
        var noWindow = Card(serverId: 7, windowStart: null, windowEnd: null);
        Assert.False(noWindow.ShowOpenInActiveQueries);

        // Half a window is not enough (defensive; the reader always maps both or neither) -> hidden.
        Assert.False(Card(serverId: 7, windowStart: window.Item1, windowEnd: null).ShowOpenInActiveQueries);

        // No server anchor (a real server id is never 0) -> hidden even with a window.
        var noServer = Card(serverId: 0, windowStart: window.Item1, windowEnd: window.Item2);
        Assert.False(noServer.ShowOpenInActiveQueries);
    }

    [Fact]
    public void Card_DeepLinkWindowUtc_UsesTheFindingsOwnRangeWhenItCarriesOne()
    {
        var start = new DateTime(2026, 7, 1, 10, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

        var (from, to) = Card(serverId: 7, windowStart: start, windowEnd: end).DeepLinkWindowUtc();

        Assert.Equal(start, from);
        Assert.Equal(end, to);
    }

    [Fact]
    public void Card_DeepLinkWindowUtc_WidensADegeneratePointByTheDrillHalfWindow()
    {
        // start == end (a point) -> widen to +/- #1409's DrillDownHalfWindowMinutes so the read isn't empty.
        var point = new DateTime(2026, 7, 1, 11, 0, 0, DateTimeKind.Utc);

        var (from, to) = Card(serverId: 7, windowStart: point, windowEnd: point).DeepLinkWindowUtc();

        Assert.Equal(point.AddMinutes(-ViewerServerTab.DrillDownHalfWindowMinutes), from);
        Assert.Equal(point.AddMinutes(ViewerServerTab.DrillDownHalfWindowMinutes), to);
    }

    private static RecommendationCardViewModel Card(
        int serverId, DateTime? windowStart, DateTime? windowEnd, RemediationAction? remediation = null)
        => new(new RecommendationItem
        {
            Severity = RecommendationSeverity.Warning,
            Title = "CPU is on fire",
            ServerId = serverId,
            WindowStartUtc = windowStart,
            WindowEndUtc = windowEnd,
            Remediation = remediation,
        });

    // ── Incident-vs-config-fix gate on the deep-link (fixes the #1427 "shows on every finding" bug) ──

    [Fact]
    public void Card_ShowOpenInActiveQueries_HiddenForConfigFixFindings_EvenWithAWindowAndServer()
    {
        var start = new DateTime(2026, 7, 1, 10, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

        // A safe DB_CONFIG standing fix (AUTO_SHRINK) is a config fix, not an incident -> deep-link hidden
        // despite carrying a full window + a real server id.
        var dbConfig = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            DbConfigTargets: new[] { new DbConfigTarget("StackOverflow", DbConfigSetting.AutoShrinkOff) });
        var dbConfigCard = Card(serverId: 7, windowStart: start, windowEnd: end, remediation: dbConfig);
        Assert.True(dbConfigCard.IsConfigFix);
        Assert.False(dbConfigCard.IsIncident);
        Assert.False(dbConfigCard.ShowOpenInActiveQueries);

        // A DB_CONFIG action can carry ONLY per-db RCSI targets (no safe targets) — still a config fix.
        var rcsiOnly = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            RcsiTargets: new[] { new RcsiTarget("StackOverflow", new RcsiInactionFigures(50, 2, 60)) });
        Assert.False(Card(serverId: 7, windowStart: start, windowEnd: end, remediation: rcsiOnly).ShowOpenInActiveQueries);

        // Percent-autogrowth (FILE_AUTOGROWTH_PERCENT) is a config fix.
        var autogrowth = new RemediationAction(
            "FILE_AUTOGROWTH_PERCENT", "set", Array.Empty<ForcePlanTarget>(),
            FileGrowthTargets: new[] { new FileGrowthTarget("StackOverflow", "SO_data", 90000, 10, 1024) });
        Assert.False(Card(serverId: 7, windowStart: start, windowEnd: end, remediation: autogrowth).ShowOpenInActiveQueries);

        // Server-level config (MAXDOP) is a config fix.
        var serverConfig = new RemediationAction(
            "SERVER_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            ServerConfigTargets: new[] { new ServerConfigTarget(ServerConfigSetting.Maxdop, 0, 8) });
        Assert.False(Card(serverId: 7, windowStart: start, windowEnd: end, remediation: serverConfig).ShowOpenInActiveQueries);
    }

    [Fact]
    public void Card_ShowOpenInActiveQueries_ShownForIncidentFindings_WithAWindowAndServer()
    {
        var start = new DateTime(2026, 7, 1, 10, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);

        // A pure incident (CPU/blocking/waits) reads back with no remediation action -> shown.
        var bare = Card(serverId: 7, windowStart: start, windowEnd: end, remediation: null);
        Assert.True(bare.IsIncident);
        Assert.True(bare.ShowOpenInActiveQueries);

        // Plan-regression (force-plan) is Apply-able but stays an INCIDENT (no structured config targets) -> shown.
        var forcePlan = new RemediationAction(
            "PLAN_REGRESSION", "force", new[] { new ForcePlanTarget("StackOverflow", QueryId: 42, PlanId: 7) });
        Assert.True(Card(serverId: 7, windowStart: start, windowEnd: end, remediation: forcePlan).ShowOpenInActiveQueries);

        // MISSING_INDEX is copy-paste-only but deliberately stays an incident (Dashboard parity) -> shown.
        var missingIndex = new RemediationAction(
            "MISSING_INDEX", "advise", Array.Empty<ForcePlanTarget>(),
            MissingIndexTargets: new[] { new MissingIndexTarget("dbo.Posts", 92.5, "CREATE INDEX IX ON dbo.Posts (OwnerUserId);") });
        Assert.True(Card(serverId: 7, windowStart: start, windowEnd: end, remediation: missingIndex).ShowOpenInActiveQueries);
    }

    [Theory]
    [InlineData(RecommendationSeverity.Critical, "CRITICAL", "")]
    [InlineData(RecommendationSeverity.Warning, "WARNING", "")]
    [InlineData(RecommendationSeverity.Info, "INFO", "")]
    public void Card_SeverityLabelAndGlyph_MatchLite(RecommendationSeverity severity, string label, string glyph)
    {
        var card = new RecommendationCardViewModel(new RecommendationItem { Severity = severity, Title = "t" });
        Assert.Equal(label, card.SeverityLabel);
        Assert.Equal(glyph, card.SeverityGlyph);
    }

    [Fact]
    public void Card_DatabaseAndAdviceVisibilityFlags()
    {
        var withDb = new RecommendationCardViewModel(new RecommendationItem
        {
            Severity = RecommendationSeverity.Info, Title = "t", Database = "StackOverflow", AdviceText = "advice",
        });
        Assert.True(withDb.HasDatabase);
        Assert.Equal("[StackOverflow]", withDb.DatabaseBracketed);
        Assert.True(withDb.HasAdvice);

        var bare = new RecommendationCardViewModel(new RecommendationItem
        {
            Severity = RecommendationSeverity.Info, Title = "t", Database = null, AdviceText = null,
        });
        Assert.False(bare.HasDatabase);
        Assert.Equal("", bare.DatabaseBracketed);
        Assert.False(bare.HasAdvice);
    }

    [Fact]
    public void Card_AskAiPrompt_RendersTheWindowWithTheGivenOffset()
    {
        var item = new RecommendationItem
        {
            Severity = RecommendationSeverity.Warning,
            Title = "CPU is on fire",
            ServerName = "SQL2022",
            WindowStartUtc = new DateTime(2026, 7, 1, 14, 0, 0, DateTimeKind.Utc),
            WindowEndUtc = new DateTime(2026, 7, 1, 15, 30, 0, DateTimeKind.Utc),
        };

        // Offset 0 -> the UTC window verbatim (deterministic; the tab passes the machine-local offset).
        var utc = new RecommendationCardViewModel(item, utcOffsetMinutes: 0).AskAiPrompt;
        Assert.Contains("14:00–15:30", utc, StringComparison.Ordinal);

        // +60 shifts the rendered window an hour forward.
        var plusOne = new RecommendationCardViewModel(item, utcOffsetMinutes: 60).AskAiPrompt;
        Assert.Contains("15:00–16:30", plusOne, StringComparison.Ordinal);
    }
}

/// <summary>
/// Pins the W2b incident grouping + state selection: rows sharing an incident collapse into one
/// section headed by the primary finding, solo findings each get their own section, sections keep the
/// read's severity-desc order and expand unless Info-only, and the co-fired cross-reference is
/// appended across a multi-finding window. Mirrors Lite's GroupByIncident.
/// </summary>
public sealed class ViewerRecommendationGroupingTests
{
    private const string MidDot = "·";

    private static ViewerFindingRow Row(double severity, string title, string incidentId = "", string? database = null)
    {
        var finding = new AnalysisFinding
        {
            FindingId = 1,
            AnalysisTime = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            ServerId = 1,
            ServerName = "SQL2022",
            DatabaseName = database,
            Severity = severity,
            Confidence = 0.9,
            Category = "cpu",
            StoryPath = "a -> b",
            StoryPathHash = "hash-" + title,
            StoryText = "",
            RootFactKey = "FACT",
            IncidentId = incidentId,
            FactCount = 1,
        };
        return new ViewerFindingRow
        {
            SeverityBand = ViewerDataService.SeverityBand(severity),
            RawSeverity = severity,
            Title = title,
            Category = "cpu",
            DatabaseName = database ?? "",
            Finding = finding,
        };
    }

    [Fact]
    public void FromFindings_NoRows_YieldsEmptyState()
    {
        var vm = RecommendationsViewModel.FromFindings(Array.Empty<ViewerFindingRow>(), "SQL2022");
        Assert.Equal(RecommendationsState.Empty, vm.State);
        Assert.Empty(vm.Sections);
        Assert.Equal(0, vm.TotalCount);
    }

    // ── Insufficient-data vs all-clear state selection (V19 marker) ─────────────────────────────

    [Fact]
    public void FromFindings_ZeroFindings_NoInsufficientMarker_IsTheGenuineAllClear()
    {
        // data + zero findings -> Empty (the genuine all-clear); the message is empty.
        var vm = RecommendationsViewModel.FromFindings(
            Array.Empty<ViewerFindingRow>(), "SQL2022", utcOffsetMinutes: 0,
            insufficientData: false, insufficientDataMessage: null);

        Assert.Equal(RecommendationsState.Empty, vm.State);
        Assert.Empty(vm.Sections);
        Assert.Equal(string.Empty, vm.InsufficientDataMessage);
    }

    [Fact]
    public void FromFindings_ZeroFindings_InsufficientMarker_ShowsStillCollecting_NotAllClear()
    {
        // insufficient + zero findings -> InsufficientData ("still collecting"), NOT a false all-clear.
        var vm = RecommendationsViewModel.FromFindings(
            Array.Empty<ViewerFindingRow>(), "SQL2022", utcOffsetMinutes: 0,
            insufficientData: true,
            insufficientDataMessage: "Not enough data for reliable analysis. Need 1.0 days, have 3.0 hours.");

        Assert.Equal(RecommendationsState.InsufficientData, vm.State);
        Assert.Empty(vm.Sections);
        Assert.Equal("Not enough data for reliable analysis. Need 1.0 days, have 3.0 hours.", vm.InsufficientDataMessage);
    }

    [Fact]
    public void FromFindings_InsufficientMarker_ButFindingsPresent_FindingsWin_Loaded()
    {
        // A server with findings shows them regardless of the marker (the marker only decides zero-finding).
        var rows = new List<ViewerFindingRow> { Row(1.6, "CPU is on fire", incidentId: "a") };

        var vm = RecommendationsViewModel.FromFindings(
            rows, "SQL2022", utcOffsetMinutes: 0, insufficientData: true, insufficientDataMessage: "collecting");

        Assert.Equal(RecommendationsState.Loaded, vm.State);
        Assert.Single(vm.Sections);
        Assert.Equal(string.Empty, vm.InsufficientDataMessage);
    }

    [Fact]
    public void InsufficientData_UsesEngineMessageWhenPresent_ElseTheDefault()
    {
        Assert.Equal(
            RecommendationsViewModel.DefaultInsufficientDataMessage,
            RecommendationsViewModel.InsufficientData(null).InsufficientDataMessage);
        Assert.Equal(
            RecommendationsViewModel.DefaultInsufficientDataMessage,
            RecommendationsViewModel.InsufficientData("   ").InsufficientDataMessage);
        Assert.Equal(
            "engine says 3.0 hours",
            RecommendationsViewModel.InsufficientData("engine says 3.0 hours").InsufficientDataMessage);
        Assert.Equal(RecommendationsState.InsufficientData, RecommendationsViewModel.InsufficientData(null).State);
    }

    // ── Window-empty vs all-clear state selection (#3524/#3551, the marker's false-with-a-message shape) ──

    [Fact]
    public void FromFindings_ZeroFindings_WindowEmptyMarker_ShowsCollectionBroken_NotAllClear()
    {
        // window-empty + zero findings -> WindowEmpty ("collection appears broken"), NOT a false all-clear.
        var vm = RecommendationsViewModel.FromFindings(
            Array.Empty<ViewerFindingRow>(), "SQL2022", utcOffsetMinutes: 0,
            windowEmpty: true,
            windowEmptyMessage: "No facts were collected in the analysis window.");

        Assert.Equal(RecommendationsState.WindowEmpty, vm.State);
        Assert.Empty(vm.Sections);
        Assert.StartsWith("No facts were collected in the analysis window.", vm.WindowEmptyMessage);
        Assert.EndsWith(RecommendationsViewModel.WindowEmptyCollectionHealthPointer, vm.WindowEmptyMessage);
        Assert.Equal(string.Empty, vm.InsufficientDataMessage);
    }

    [Fact]
    public void FromFindings_WindowEmptyMarker_ButFindingsPresent_FindingsWin_Loaded()
    {
        // Same rule as the insufficient marker: it only decides the zero-finding case.
        var rows = new List<ViewerFindingRow> { Row(1.6, "CPU is on fire", incidentId: "a") };

        var vm = RecommendationsViewModel.FromFindings(
            rows, "SQL2022", utcOffsetMinutes: 0, windowEmpty: true, windowEmptyMessage: "window empty");

        Assert.Equal(RecommendationsState.Loaded, vm.State);
        Assert.Single(vm.Sections);
        Assert.Equal(string.Empty, vm.WindowEmptyMessage);
    }

    [Fact]
    public void WindowEmpty_UsesMarkerMessageWhenPresent_ElseTheDefault_AlwaysWithThePointer()
    {
        Assert.StartsWith(
            RecommendationsViewModel.DefaultWindowEmptyMessage,
            RecommendationsViewModel.WindowEmpty(null).WindowEmptyMessage);
        Assert.StartsWith(
            RecommendationsViewModel.DefaultWindowEmptyMessage,
            RecommendationsViewModel.WindowEmpty("   ").WindowEmptyMessage);
        Assert.StartsWith(
            "engine says the window held nothing",
            RecommendationsViewModel.WindowEmpty("engine says the window held nothing").WindowEmptyMessage);
        Assert.EndsWith(
            RecommendationsViewModel.WindowEmptyCollectionHealthPointer,
            RecommendationsViewModel.WindowEmpty(null).WindowEmptyMessage);
        Assert.Equal(RecommendationsState.WindowEmpty, RecommendationsViewModel.WindowEmpty(null).State);
    }

    [Fact]
    public void AnalysisStateMarker_WindowEmpty_IsExactlyTheFalseWithMessageShape()
    {
        // The marker encoding contract (#3551): false + message = window-empty, the shape only the
        // worker's window-empty arm writes. Every other persisted shape must NOT read as window-empty —
        // true + message is the span-gate miss, false + null/empty is a clean pass.
        var at = new DateTime(2026, 9, 1, 12, 0, 0, DateTimeKind.Utc);
        Assert.True(new AnalysisStateMarker(false, "window empty", at).WindowEmpty);
        Assert.False(new AnalysisStateMarker(false, null, at).WindowEmpty);
        Assert.False(new AnalysisStateMarker(false, "", at).WindowEmpty);
        Assert.False(new AnalysisStateMarker(true, "still collecting", at).WindowEmpty);
    }

    [Fact]
    public void FromFindings_BothMarkers_InsufficientWinsDefensively()
    {
        // The writer never sets both (the engine nulls both and sets at most one), but if a skewed
        // store ever did, the span-gate miss is the more fundamental answer.
        var vm = RecommendationsViewModel.FromFindings(
            Array.Empty<ViewerFindingRow>(), "SQL2022", utcOffsetMinutes: 0,
            insufficientData: true, insufficientDataMessage: "collecting",
            windowEmpty: true, windowEmptyMessage: "window empty");

        Assert.Equal(RecommendationsState.InsufficientData, vm.State);
    }

    [Fact]
    public void FromFindings_GroupsByIncident_HeaderNamesPrimaryPlusCount()
    {
        // Input arrives severity-desc (as the read returns it): incident A's primary first.
        var rows = new List<ViewerFindingRow>
        {
            Row(1.8, "Blocking chain", incidentId: "inc-A"),
            Row(1.2, "RCSI is off", incidentId: "inc-A"),
            Row(1.6, "CPU is on fire", incidentId: "inc-B"),
        };

        var vm = RecommendationsViewModel.FromFindings(rows, "SQL2022");

        Assert.Equal(RecommendationsState.Loaded, vm.State);
        Assert.Equal(2, vm.Sections.Count);
        Assert.Equal(3, vm.TotalCount);

        // inc-A: two cards, header names the primary + count + severity.
        var a = vm.Sections[0];
        Assert.Equal(2, a.Count);
        Assert.Equal($"Blocking chain {MidDot} 2 findings {MidDot} CRITICAL", a.Header);
        Assert.Equal("Blocking chain", a.Cards[0].Title);
        Assert.Equal("RCSI is off", a.Cards[1].Title);

        // inc-B: single card, header has no count.
        var b = vm.Sections[1];
        Assert.Equal(1, b.Count);
        Assert.Equal($"CPU is on fire {MidDot} CRITICAL", b.Header);
    }

    [Fact]
    public void FromFindings_SoloFindings_EachBecomeTheirOwnSection()
    {
        var rows = new List<ViewerFindingRow>
        {
            Row(1.6, "Finding one"),   // no incident id
            Row(1.4, "Finding two"),   // no incident id
        };

        var vm = RecommendationsViewModel.FromFindings(rows, "SQL2022");

        Assert.Equal(2, vm.Sections.Count);
        Assert.All(vm.Sections, s => Assert.Equal(1, s.Count));
        Assert.Equal($"Finding one {MidDot} CRITICAL", vm.Sections[0].Header);
        Assert.Equal($"Finding two {MidDot} WARNING", vm.Sections[1].Header);
    }

    [Fact]
    public void FromFindings_ExpandsCriticalAndWarning_CollapsesInfo()
    {
        var rows = new List<ViewerFindingRow>
        {
            Row(1.6, "Critical thing", incidentId: "c"),
            Row(1.0, "Warning thing", incidentId: "w"),
            Row(0.3, "Info thing", incidentId: "i"),
        };

        var vm = RecommendationsViewModel.FromFindings(rows, "SQL2022");

        Assert.True(vm.Sections[0].IsExpanded);   // Critical
        Assert.True(vm.Sections[1].IsExpanded);   // Warning
        Assert.False(vm.Sections[2].IsExpanded);  // Info-only -> collapsed
    }

    [Fact]
    public void FromFindings_AppendsCoFiredCrossReference_AcrossAMultiFindingWindow()
    {
        var rows = new List<ViewerFindingRow>
        {
            Row(1.6, "CPU is on fire", incidentId: "a"),
            Row(1.2, "Blocking chain", incidentId: "b"),
        };

        var vm = RecommendationsViewModel.FromFindings(rows, "SQL2022");

        // Each card's advice carries the "what else fired" line naming the OTHER finding.
        var cpu = vm.Sections[0].Cards[0];
        Assert.Contains("Also surfaced in this analysis window:", cpu.AdviceText, StringComparison.Ordinal);
        Assert.Contains("Blocking chain", cpu.AdviceText!, StringComparison.Ordinal);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip proving the W2b Copy-fix path end to end: a persisted
/// finding whose <c>remediation_action_json</c> carries a missing-index action deserializes through the
/// viewer read and yields a card whose Copy-fix payload is the suggested CREATE. Shares the serialized
/// "live-postgres" collection so the row churn can't race another class.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerRecommendationsLivePostgresTests
{
    private const int ServerId = -929292;
    private const string ServerName = "viewer-w2b-reco-e2e";
    private const string Hash = "v2b-reco-e2e-hash";

    [Fact]
    public async Task LatestFindings_WithPersistedMissingIndexAction_YieldACopyFixCard()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Recommendations card test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteFindingRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var store = new PgFindingStore(postgres);
        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var windowEnd = TruncateToSeconds(DateTime.UtcNow);
            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowEnd.AddHours(-4),
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var stories = new List<AnalysisStory>
            {
                new AnalysisStory
                {
                    Severity = 1.2,
                    Confidence = 0.8,
                    Category = "indexes",
                    StoryPath = "MISSING_INDEX",
                    StoryPathHash = Hash,
                    StoryText = "seed finding for the viewer Copy-fix round-trip",
                    RootFactKey = "MISSING_INDEX",
                    FactCount = 1,
                    DatabaseName = "StackOverflow",
                },
            };

            /* Two-phase write (the pipeline's shape): attach the BUILT action between the phases so it
               persists as remediation_action_json, exactly as the Darling analysis service does. */
            var survivors = await store.FilterMutedFindingsAsync(stories, context);
            var survivor = Assert.Single(survivors);
            survivor.Remediation = new RemediationAction(
                "MISSING_INDEX", "advise", Array.Empty<ForcePlanTarget>(),
                MissingIndexTargets: new[]
                {
                    new MissingIndexTarget("dbo.Posts", 92.5, "CREATE INDEX IX_Posts_1 ON dbo.Posts (OwnerUserId);"),
                });
            await store.InsertFindingsAsync(survivors, context);

            /* The viewer read hydrates the action; the pure shaping turns it into the card's Copy-fix. */
            var rows = await viewer.GetLatestFindingsAsync(ServerId);
            var vm = RecommendationsViewModel.FromFindings(rows, ServerName);

            var card = Assert.Single(vm.Sections).Cards.Single();
            Assert.True(card.ShowCopyFix);
            Assert.Equal("CREATE INDEX IX_Posts_1 ON dbo.Posts (OwnerUserId);", card.CopyPasteSql);
            Assert.True(card.ShowAskAi);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteFindingRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteFindingRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; DELETE FROM analysis_muted WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
