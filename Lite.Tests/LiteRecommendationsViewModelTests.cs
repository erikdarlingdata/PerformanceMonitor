using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitorLite.Analysis.Recommendations;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests for the Lite Recommendations view-model (WS1c): the pure, WPF-free grouping into
/// Critical / Warning / Info sections, the top-level state selection, the advise-only affordance
/// flags on a card, and the Ask-AI prompt interpolation.
/// </summary>
public class LiteRecommendationsViewModelTests
{
    private static LiteRecommendationItem Item(
        LiteRecommendationSeverity severity, double raw, string title = "T",
        string? database = null, string? sql = null, string? advice = "advice",
        string serverName = "SQL2022", string incidentId = "")
    {
        return new LiteRecommendationItem
        {
            Severity = severity,
            RawSeverity = raw,
            Title = title,
            Database = database,
            CopyPasteSql = sql,
            AdviceText = advice,
            RootFactKey = "CPU_SQL_PERCENT",
            IncidentId = incidentId,
            ServerName = serverName,
            WindowStartUtc = new DateTime(2026, 6, 1, 10, 0, 0, DateTimeKind.Utc),
            WindowEndUtc = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc)
        };
    }

    // ── states ──────────────────────────────────────────────────────────────────

    [Fact]
    public void Loading_State()
    {
        var vm = LiteRecommendationsViewModel.Loading();
        Assert.Equal(LiteRecommendationsState.Loading, vm.State);
        Assert.Empty(vm.Sections);
    }

    [Fact]
    public void InsufficientData_UsesEngineMessage()
    {
        var vm = LiteRecommendationsViewModel.InsufficientData("Need more data.");
        Assert.Equal(LiteRecommendationsState.InsufficientData, vm.State);
        Assert.Equal("Need more data.", vm.InsufficientDataMessage);
    }

    [Fact]
    public void InsufficientData_BlankMessage_FallsBackToDefault()
    {
        var vm = LiteRecommendationsViewModel.InsufficientData("   ");
        Assert.Equal(LiteRecommendationsViewModel.DefaultInsufficientDataMessage, vm.InsufficientDataMessage);
    }

    [Fact]
    public void FromItems_Empty_IsEmptyState()
    {
        var vm = LiteRecommendationsViewModel.FromItems(Array.Empty<LiteRecommendationItem>());
        Assert.Equal(LiteRecommendationsState.Empty, vm.State);
        Assert.Empty(vm.Sections);
        Assert.Equal(0, vm.TotalCount);
    }

    [Fact]
    public void FromItems_NonEmpty_IsLoadedState()
    {
        var vm = LiteRecommendationsViewModel.FromItems(new[] { Item(LiteRecommendationSeverity.Critical, 1.6) });
        Assert.Equal(LiteRecommendationsState.Loaded, vm.State);
        Assert.Equal(1, vm.TotalCount);
    }

    // ── grouping ────────────────────────────────────────────────────────────────

    [Fact]
    public void FromItems_GroupsByIncidentId()
    {
        // Reader-sorted (severity desc); two findings share incident "inc1", a third is its own.
        var items = new[]
        {
            Item(LiteRecommendationSeverity.Critical, 1.9, title: "c1", incidentId: "inc1"),
            Item(LiteRecommendationSeverity.Warning, 1.0, title: "w1", incidentId: "inc1"),
            Item(LiteRecommendationSeverity.Warning, 0.9, title: "w2", incidentId: "inc2"),
        };

        var vm = LiteRecommendationsViewModel.FromItems(items);

        Assert.Equal(2, vm.Sections.Count);                                       // two incidents
        Assert.Equal(2, vm.Sections[0].Count);                                    // inc1: c1 + w1
        Assert.Equal(LiteRecommendationSeverity.Critical, vm.Sections[0].Severity); // primary = c1
        Assert.Single(vm.Sections[1].Cards);                                      // inc2: w2 only
    }

    [Fact]
    public void Sections_ExpandedState_CriticalAndWarningOpen_InfoCollapsed()
    {
        var items = new[]
        {
            Item(LiteRecommendationSeverity.Critical, 1.6),
            Item(LiteRecommendationSeverity.Warning, 0.8),
            Item(LiteRecommendationSeverity.Info, 0.5),
        };

        var vm = LiteRecommendationsViewModel.FromItems(items);
        var byBand = vm.Sections.ToDictionary(s => s.Severity);

        Assert.True(byBand[LiteRecommendationSeverity.Critical].IsExpanded);
        Assert.True(byBand[LiteRecommendationSeverity.Warning].IsExpanded);
        Assert.False(byBand[LiteRecommendationSeverity.Info].IsExpanded);
    }

    [Fact]
    public void IncidentHeader_NamesPrimaryFindingCountAndSeverity()
    {
        var vm = LiteRecommendationsViewModel.FromItems(new[]
        {
            Item(LiteRecommendationSeverity.Critical, 1.9, title: "SQL CPU pegged", incidentId: "inc1"),
            Item(LiteRecommendationSeverity.Warning, 1.0, title: "Plan regression", incidentId: "inc1"),
        });

        Assert.Equal("SQL CPU pegged · 2 findings · CRITICAL", vm.Sections.Single().Header);
    }

    // ── card affordances (advise-only) ──────────────────────────────────────────

    [Fact]
    public void Card_WithSql_ShowsCopyFix()
    {
        var card = new LiteRecommendationCardViewModel(Item(LiteRecommendationSeverity.Warning, 0.8, sql: "ALTER ..."));
        Assert.True(card.ShowCopyFix);
    }

    [Fact]
    public void Card_WithoutSql_HidesCopyFix()
    {
        var card = new LiteRecommendationCardViewModel(Item(LiteRecommendationSeverity.Warning, 0.8, sql: null));
        Assert.False(card.ShowCopyFix);
    }

    [Fact]
    public void Card_AlwaysShowsAskAi()
    {
        var withSql = new LiteRecommendationCardViewModel(Item(LiteRecommendationSeverity.Info, 0.5, sql: "ALTER ..."));
        var noSql = new LiteRecommendationCardViewModel(Item(LiteRecommendationSeverity.Info, 0.5, sql: null));
        Assert.True(withSql.ShowAskAi);
        Assert.True(noSql.ShowAskAi);
    }

    [Fact]
    public void Card_DatabaseBracketed_AndHasDatabase()
    {
        var withDb = new LiteRecommendationCardViewModel(Item(LiteRecommendationSeverity.Info, 0.5, database: "MyDb"));
        Assert.True(withDb.HasDatabase);
        Assert.Equal("[MyDb]", withDb.DatabaseBracketed);

        var noDb = new LiteRecommendationCardViewModel(Item(LiteRecommendationSeverity.Info, 0.5, database: null));
        Assert.False(noDb.HasDatabase);
        Assert.Equal(string.Empty, noDb.DatabaseBracketed);
    }

    [Theory]
    [InlineData(LiteRecommendationSeverity.Critical, "CRITICAL")]
    [InlineData(LiteRecommendationSeverity.Warning, "WARNING")]
    [InlineData(LiteRecommendationSeverity.Info, "INFO")]
    public void Card_SeverityLabel(LiteRecommendationSeverity sev, string expected)
    {
        var card = new LiteRecommendationCardViewModel(Item(sev, 1.0));
        Assert.Equal(expected, card.SeverityLabel);
    }

    // ── Ask-AI prompt ───────────────────────────────────────────────────────────

    [Fact]
    public void AskAiPrompt_IncludesServerTitleAndWindow_InServerLocalTime()
    {
        // +60 min offset shifts the 10:00–12:00 UTC window to 11:00–13:00 server-local.
        var card = new LiteRecommendationCardViewModel(
            Item(LiteRecommendationSeverity.Critical, 1.6, title: "High CPU", serverName: "PRODSQL"),
            utcOffsetMinutes: 60);

        var prompt = card.AskAiPrompt;

        Assert.Contains("PRODSQL", prompt);
        Assert.Contains("High CPU", prompt);
        Assert.Contains("analyze_server", prompt);
        Assert.Contains("get_analysis_findings", prompt);
        Assert.Contains("2026-06-01 11:00–13:00", prompt);
    }

    [Fact]
    public void BuildAskAiPrompt_IsDeterministic()
    {
        var from = new DateTime(2026, 6, 1, 9, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2026, 6, 1, 9, 30, 0, DateTimeKind.Utc);
        var prompt = LiteRecommendationsViewModel.BuildAskAiPrompt("S1", "Title here", from, to);

        Assert.Contains("server \"S1\"", prompt);
        Assert.Contains("\"Title here\"", prompt);
        Assert.Contains("2026-06-01 09:00–09:30", prompt);
    }
}
