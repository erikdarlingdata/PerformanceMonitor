using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis.Recommendations;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests for the Lite Recommendations reader + view-model mapping (WS1c). These cover the pure,
/// database-free mapping logic: engine severity banding, latest-batch-only trim, advice
/// composition, copy-paste SQL derivation, sort order, the section grouping / state selection, and
/// the advise-only affordance model. No DuckDB or WPF is needed — the reader exposes the mapping as
/// <c>internal static</c> functions over finding lists.
/// </summary>
public class LiteRecommendationsReaderTests
{
    private const string ServerName = "SQL2022 (Read-Only)";

    private static AnalysisFinding Finding(
        string rootFactKey, double severity, string? database = null,
        string category = "waits", string storyText = "story", DateTime? analysisTime = null)
    {
        return new AnalysisFinding
        {
            FindingId = 1,
            AnalysisTime = analysisTime ?? DateTime.UtcNow,
            ServerId = 42,
            ServerName = ServerName,
            DatabaseName = database,
            TimeRangeStart = new DateTime(2026, 6, 1, 10, 0, 0, DateTimeKind.Utc),
            TimeRangeEnd = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc),
            Severity = severity,
            Confidence = 1.0,
            Category = category,
            StoryPath = rootFactKey,
            StoryPathHash = "hash_" + rootFactKey,
            StoryText = storyText,
            RootFactKey = rootFactKey,
            FactCount = 1
        };
    }

    // ── severity banding (cutoffs identical to the Dashboard reader) ───────────

    [Theory]
    [InlineData(2.0, LiteRecommendationSeverity.Critical)]
    [InlineData(1.5, LiteRecommendationSeverity.Critical)]
    [InlineData(1.49, LiteRecommendationSeverity.Warning)]
    [InlineData(0.75, LiteRecommendationSeverity.Warning)]
    [InlineData(0.74, LiteRecommendationSeverity.Info)]
    [InlineData(0.0, LiteRecommendationSeverity.Info)]
    public void SeverityBand_MapsAtCutoffs(double severity, LiteRecommendationSeverity expected)
    {
        Assert.Equal(expected, LiteRecommendationsReader.SeverityBand(severity));
    }

    // ── advice composition ─────────────────────────────────────────────────────

    [Fact]
    public void ComposeAdvice_NullBlock_ReturnsNull()
    {
        Assert.Null(LiteRecommendationsReader.ComposeAdvice(null));
    }

    [Fact]
    public void ComposeAdvice_JoinsRemediationThenInvestigation()
    {
        var block = new AdviceBlock("Headline", "Look here.", "Do this.");
        var composed = LiteRecommendationsReader.ComposeAdvice(block);
        Assert.Equal("Do this. Look here.", composed);
    }

    [Fact]
    public void ComposeAdvice_OnlyRemediation_NoTrailingSpace()
    {
        var block = new AdviceBlock("Headline", "", "Do this.");
        Assert.Equal("Do this.", LiteRecommendationsReader.ComposeAdvice(block));
    }

    // ── per-finding mapping ─────────────────────────────────────────────────────

    [Fact]
    public void MapFinding_UsesAdviceHeadlineAsTitle_WhenAdviceExists()
    {
        // SOS_SCHEDULER_YIELD has a first-class advice block with a headline.
        var item = LiteRecommendationsReader.MapFinding(Finding("SOS_SCHEDULER_YIELD", 1.6), ServerName);

        var expected = FactAdvice.GetForFactKey("SOS_SCHEDULER_YIELD");
        Assert.NotNull(expected);
        Assert.Equal(expected!.Headline, item.Title);
        Assert.Equal(LiteRecommendationSeverity.Critical, item.Severity);
        Assert.Equal(1.6, item.RawSeverity);
        Assert.Equal(ServerName, item.ServerName);
        Assert.Equal("SOS_SCHEDULER_YIELD", item.RootFactKey);
        Assert.False(string.IsNullOrEmpty(item.AdviceText));
    }

    [Fact]
    public void MapFinding_UnknownFactKey_FallsBackToRootFactKeyTitle_NoAdvice()
    {
        // StoryText now carries value-stated advice JSON (or empty), never human prose — so an
        // unknown key with no static advice block falls the title back to the fact key, with no
        // advice text, rather than echoing StoryText (which would dump JSON).
        var item = LiteRecommendationsReader.MapFinding(
            Finding("TOTALLY_UNKNOWN_KEY", 0.5, storyText: ""), ServerName);

        Assert.Equal("TOTALLY_UNKNOWN_KEY", item.Title);
        Assert.Null(item.AdviceText);
        Assert.Equal(LiteRecommendationSeverity.Info, item.Severity);
    }

    [Fact]
    public void MapFinding_ReadBackFinding_HasNoDrillDown_SoCopyPasteSqlIsNull()
    {
        // The store read path does not populate DrillDown; the SQL generators need it, so a
        // read-back DB_CONFIG finding (which WOULD produce ALTERs from a drill-down) yields no SQL.
        var item = LiteRecommendationsReader.MapFinding(Finding("DB_CONFIG", 1.0, database: "MyDb"), ServerName);
        Assert.Null(item.CopyPasteSql);
        Assert.Equal("MyDb", item.Database);
    }

    [Fact]
    public void MapFinding_WithDrillDown_PopulatesCopyPasteSqlFromSharedHelper()
    {
        // Generate-now path: an enriched finding carries drill-down, so the SHARED FactRemediation
        // helper produces the same copy-paste statement the Dashboard would.
        var finding = Finding("PLAN_REGRESSION", 1.6, database: "MyDb");
        finding.DrillDown = new Dictionary<string, object>
        {
            ["regressed_queries"] = new[]
            {
                new Dictionary<string, object>
                {
                    ["database"] = "MyDb",
                    ["query_id"] = 123L,
                    ["best_plan_id"] = 7L,
                    ["regression_factor"] = 4.2
                }
            }
        };

        var expectedSql = FactRemediation.GenerateForFinding(finding);
        Assert.False(string.IsNullOrEmpty(expectedSql)); // sanity: the helper produced SQL

        var item = LiteRecommendationsReader.MapFinding(finding, ServerName);
        Assert.Equal(expectedSql, item.CopyPasteSql);
    }

    [Fact]
    public void MapFinding_EmptyDatabase_NormalizesToNull()
    {
        var item = LiteRecommendationsReader.MapFinding(Finding("CPU_SQL_PERCENT", 1.0, database: ""), ServerName);
        Assert.Null(item.Database);
    }

    [Fact]
    public void MapFinding_StampsWindowKindUtc()
    {
        var item = LiteRecommendationsReader.MapFinding(Finding("CPU_SPIKE", 1.0), ServerName);
        Assert.NotNull(item.WindowStartUtc);
        Assert.NotNull(item.WindowEndUtc);
        Assert.Equal(DateTimeKind.Utc, item.WindowStartUtc!.Value.Kind);
        Assert.Equal(DateTimeKind.Utc, item.WindowEndUtc!.Value.Kind);
    }

    // ── latest-batch-only trim ──────────────────────────────────────────────────

    [Fact]
    public void LatestBatchOnly_KeepsOnlyMostRecentAnalysisTime()
    {
        var newer = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);
        var older = new DateTime(2026, 6, 1, 6, 0, 0, DateTimeKind.Utc);

        // Store returns analysis_time DESC, so the newer batch is first.
        var findings = new List<AnalysisFinding>
        {
            Finding("CPU_SQL_PERCENT", 1.6, analysisTime: newer),
            Finding("BLOCKING_EVENTS", 1.0, analysisTime: newer),
            Finding("CPU_SQL_PERCENT", 1.6, analysisTime: older),
            Finding("DEADLOCKS", 0.9, analysisTime: older),
        };

        var latest = LiteRecommendationsReader.LatestBatchOnly(findings);
        Assert.Equal(2, latest.Count);
        Assert.All(latest, f => Assert.Equal(newer, f.AnalysisTime));
    }

    [Fact]
    public void LatestBatchOnly_SingleOrEmpty_ReturnsInput()
    {
        Assert.Empty(LiteRecommendationsReader.LatestBatchOnly(Array.Empty<AnalysisFinding>()));

        var one = new List<AnalysisFinding> { Finding("CPU_SPIKE", 1.0) };
        Assert.Single(LiteRecommendationsReader.LatestBatchOnly(one));
    }

    // ── full map + sort ─────────────────────────────────────────────────────────

    [Fact]
    public void MapFindings_SortsBySeverityBandThenRawDescending()
    {
        var t = DateTime.UtcNow;
        var findings = new List<AnalysisFinding>
        {
            Finding("CPU_SPIKE", 0.5, analysisTime: t),            // Info
            Finding("CPU_SQL_PERCENT", 1.6, analysisTime: t),      // Critical
            Finding("BLOCKING_EVENTS", 0.8, analysisTime: t),      // Warning
            Finding("DEADLOCKS", 1.9, analysisTime: t),            // Critical (higher raw)
        };

        var items = LiteRecommendationsReader.MapFindings(findings, ServerName);

        Assert.Equal(4, items.Count);
        // Critical (1.9) -> Critical (1.6) -> Warning (0.8) -> Info (0.5)
        Assert.Equal(LiteRecommendationSeverity.Critical, items[0].Severity);
        Assert.Equal(1.9, items[0].RawSeverity);
        Assert.Equal(LiteRecommendationSeverity.Critical, items[1].Severity);
        Assert.Equal(1.6, items[1].RawSeverity);
        Assert.Equal(LiteRecommendationSeverity.Warning, items[2].Severity);
        Assert.Equal(LiteRecommendationSeverity.Info, items[3].Severity);
    }

    [Fact]
    public void MapFindings_AppliesLatestBatchOnly()
    {
        var newer = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);
        var older = new DateTime(2026, 6, 1, 6, 0, 0, DateTimeKind.Utc);
        var findings = new List<AnalysisFinding>
        {
            Finding("CPU_SQL_PERCENT", 1.6, analysisTime: newer),
            Finding("BLOCKING_EVENTS", 1.6, analysisTime: older),
        };

        var items = LiteRecommendationsReader.MapFindings(findings, ServerName);
        Assert.Single(items);
        Assert.Equal("CPU_SQL_PERCENT", items[0].RootFactKey);
    }

    [Fact]
    public void MapFindings_Empty_ReturnsEmpty()
    {
        Assert.Empty(LiteRecommendationsReader.MapFindings(Array.Empty<AnalysisFinding>(), ServerName));
    }

    // Correlate-and-focus slice 1 (review §1d): each card's advice gains a "what else fired in this
    // window" cross-reference listing the other findings, so the operator stops hunting across cards.
    [Fact]
    public void MapFindings_AppendsCoFiredCrossReference_WhenMultipleFindings()
    {
        var t = DateTime.UtcNow;
        var items = LiteRecommendationsReader.MapFindings(new List<AnalysisFinding>
        {
            Finding("CPU_SQL_PERCENT", 1.6, analysisTime: t),
            Finding("BLOCKING_EVENTS", 1.0, analysisTime: t),
        }, ServerName);

        Assert.Equal(2, items.Count);
        Assert.All(items, i => Assert.Contains("Also surfaced in this analysis window:", i.AdviceText!));
        // each card cross-references the OTHER finding's title
        Assert.Contains(items[1].Title, items[0].AdviceText!);
        Assert.Contains(items[0].Title, items[1].AdviceText!);
    }

    [Fact]
    public void MapFindings_NoCoFiredLine_ForSingleFinding()
    {
        var items = LiteRecommendationsReader.MapFindings(
            new List<AnalysisFinding> { Finding("CPU_SQL_PERCENT", 1.6) }, ServerName);

        Assert.Single(items);
        Assert.DoesNotContain("Also surfaced in this analysis window:", items[0].AdviceText ?? string.Empty);
    }
}
