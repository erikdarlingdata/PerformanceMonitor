using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;
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
    public void MapFinding_ReadBackFinding_NoActionAndNoDrillDown_CopyPasteSqlIsNull()
    {
        // A read-back finding that carries neither a persisted RemediationAction NOR a drill-down
        // (e.g. a pure incident) yields no command; the card renders advise-only.
        var item = LiteRecommendationsReader.MapFinding(Finding("DB_CONFIG", 1.0, database: "MyDb"), ServerName);
        Assert.Null(item.CopyPasteSql);
        Assert.Equal("MyDb", item.Database);
    }

    [Fact]
    public void MapFinding_ReadBackFinding_WithPersistedAction_RendersTheCopyPasteCommand()
    {
        // The read-back path (no drill-down) now carries the BUILT action deserialized from
        // remediation_action_json; MapFinding renders it via the SHARED renderer — the drift this fix
        // kills. A safe DB_CONFIG action renders the bare ALTER, byte-identical to the Darling viewer.
        var finding = Finding("DB_CONFIG", 1.0, database: "StackOverflow");
        finding.Remediation = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            DbConfigTargets: new[] { new DbConfigTarget("StackOverflow", DbConfigSetting.AutoShrinkOff) });

        var item = LiteRecommendationsReader.MapFinding(finding, ServerName);

        Assert.Equal("ALTER DATABASE [StackOverflow] SET AUTO_SHRINK OFF;", item.CopyPasteSql);
    }

    [Fact]
    public void MapFinding_PersistedActionWins_OverTheLiveDrillDownFallback()
    {
        // When BOTH a persisted action and a drill-down are present, the persisted-action render is the
        // PRIMARY source (the read-back path) and the drill-down generator is only the fallback.
        var finding = Finding("DB_CONFIG", 1.0, database: "StackOverflow");
        finding.Remediation = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            DbConfigTargets: new[] { new DbConfigTarget("StackOverflow", DbConfigSetting.AutoShrinkOff) });
        finding.DrillDown = new Dictionary<string, object>(); // present but empty

        var item = LiteRecommendationsReader.MapFinding(finding, ServerName);

        Assert.Equal("ALTER DATABASE [StackOverflow] SET AUTO_SHRINK OFF;", item.CopyPasteSql);
    }

    [Fact]
    public void MapFinding_WithDrillDown_PopulatesCopyPasteSqlFromSharedHelper()
    {
        // Generate-now fallback path: an enriched finding carries drill-down but NO persisted action,
        // so the SHARED FactRemediation generator produces the same copy-paste statement the Dashboard
        // would. (When an action IS persisted it wins; see MapFinding_PersistedActionWins.)
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
    public void ExtractPlanRegressionTargets_TwoReplicasOfOneQuery_YieldsOneForcePlanTarget()
    {
        /* #1850 made the plan-regression drill-down per-REPLICA: on an AG primary with Query Store for
           secondary replicas enabled, one query can regress on the primary AND on a secondary and arrive
           as two rows with their own best_plan_id. Both reaching the renderer would emit two
           sp_query_store_force_plan calls naming the same query with different plan ids — mutually
           exclusive instructions where whichever the operator runs last silently wins. This pins that
           they cannot both.

           WHICH one survives is settled by #1882 and pinned in ForcePlanReplicaScopeTests: the PRIMARY's
           row wins, because the rendered statement omits @replica_group_id and therefore forces on the
           primary by that argument's documented default. This fixture cannot tell the two rules apart —
           it seeds the primary with the WORSE regression, so #1850's "keep the first" and #1882's "prefer
           the primary" agree on it. That is why it kept passing through the #1882 change, and why the
           discriminating seed (a secondary that regressed HARDER) lives in the other suite. */
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
                    ["regression_factor"] = 12.0,
                    ["replica_role"] = "PRIMARY"
                },
                new Dictionary<string, object>
                {
                    ["database"] = "MyDb",
                    ["query_id"] = 123L,
                    ["best_plan_id"] = 9L,
                    ["regression_factor"] = 3.0,
                    ["replica_role"] = "SECONDARY"
                }
            }
        };

        var targets = FactRemediation.ExtractPlanRegressionTargets(finding);

        var target = Assert.Single(targets);
        Assert.Equal(7L, target.PlanId);
        Assert.Equal(12.0, target.RegressionFactor);

        /* And the rendered command names that plan once, not two plans for one query. Asserted on the
           whole EXEC statement rather than on the bare digits "7" and "9": a substring search for a
           single digit matches any cpu figure, regression factor or URL the renderer happens to emit,
           so it passes or fails for reasons that have nothing to do with plan ids. */
        var sql = FactRemediation.GenerateForFinding(finding);
        Assert.Contains(
            "EXEC sys.sp_query_store_force_plan @query_id = 123, @plan_id = 7;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@plan_id = 9;", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ExtractPlanRegressionTargets_DistinctQueries_AreAllKept()
    {
        /* The dedup is per (database, query_id) — it must not collapse different queries, nor the same
           query_id in a different database, which the cap of 5 would otherwise be doing silently. */
        var finding = Finding("PLAN_REGRESSION", 1.6, database: "MyDb");
        finding.DrillDown = new Dictionary<string, object>
        {
            ["regressed_queries"] = new[]
            {
                new Dictionary<string, object>
                {
                    ["database"] = "MyDb", ["query_id"] = 123L, ["best_plan_id"] = 7L, ["regression_factor"] = 12.0
                },
                new Dictionary<string, object>
                {
                    ["database"] = "MyDb", ["query_id"] = 124L, ["best_plan_id"] = 8L, ["regression_factor"] = 5.0
                },
                new Dictionary<string, object>
                {
                    ["database"] = "OtherDb", ["query_id"] = 123L, ["best_plan_id"] = 9L, ["regression_factor"] = 4.0
                }
            }
        };

        Assert.Equal(3, FactRemediation.ExtractPlanRegressionTargets(finding).Count);
    }

    // ── BuildCopyPasteSql: the persisted-action renderer for all seven remediation shapes ──────────
    // Lite delegates to the SAME shared FactRemediation.RenderCopyPasteCommand the Darling viewer uses,
    // so these mirror Darling's ViewerRecommendationsTests and prove Lite produces byte-identical
    // commands (ZERO drift). Lite renders a COPYABLE command only — there is no in-app executor.

    [Fact]
    public void BuildCopyPasteSql_NullOrEmptyAction_ReturnsNull()
    {
        Assert.Null(LiteRecommendationsReader.BuildCopyPasteSql(null));

        // An action with no renderable target (an empty force-plan list) yields null — nothing to run.
        var empty = new RemediationAction("PLAN_REGRESSION", "force", Array.Empty<ForcePlanTarget>());
        Assert.Null(LiteRecommendationsReader.BuildCopyPasteSql(empty));
    }

    [Fact]
    public void BuildCopyPasteSql_ForcePlanTargets_RenderUseThenForcePlanPerDatabase()
    {
        // sp_query_store_force_plan is DATABASE-scoped, so each target is a standalone USE + EXEC block.
        var action = new RemediationAction(
            "PLAN_REGRESSION", "force",
            new[]
            {
                new ForcePlanTarget("StackOverflow", QueryId: 42, PlanId: 7),
                new ForcePlanTarget("Crazy]Name", QueryId: 100, PlanId: 200),
            });

        var sql = LiteRecommendationsReader.BuildCopyPasteSql(action);

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
        var action = new RemediationAction(
            "SERVER_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            ServerConfigTargets: new[]
            {
                new ServerConfigTarget(ServerConfigSetting.Maxdop, 0, 8),
                new ServerConfigTarget(ServerConfigSetting.MaxServerMemory, 2147483647, 2147483647),
            });

        var sql = LiteRecommendationsReader.BuildCopyPasteSql(action);

        Assert.Equal(
            FactRemediation.BuildSpConfigureStatement(ServerConfigSetting.Maxdop, 8) +
            Environment.NewLine + Environment.NewLine +
            FactRemediation.BuildSpConfigureStatement(ServerConfigSetting.MaxServerMemory, 2147483647),
            sql);
    }

    [Fact]
    public void BuildCopyPasteSql_RcsiTargets_RenderAlterWithTwoSidedDisclosureHeader()
    {
        // RCSI rides a DB_CONFIG action as a per-database RcsiTarget. It is DESTRUCTIVE, so the
        // copy-paste prepends the two-sided risk disclosure as a comment header.
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            RcsiTargets: new[] { new RcsiTarget("StackOverflow", new RcsiInactionFigures(50, 3, 70)) });

        var sql = LiteRecommendationsReader.BuildCopyPasteSql(action);

        Assert.NotNull(sql);
        Assert.StartsWith("/*", sql, StringComparison.Ordinal);
        Assert.Contains("Risks of MAKING this change:", sql!, StringComparison.Ordinal);
        Assert.Contains("Risks of NOT making this change:", sql!, StringComparison.Ordinal);
        Assert.Contains("50 blocked-process events", sql!, StringComparison.Ordinal);
        Assert.Contains("ALTER DATABASE [StackOverflow] SET READ_COMMITTED_SNAPSHOT ON;", sql!, StringComparison.Ordinal);
        Assert.True(
            sql!.IndexOf("*/", StringComparison.Ordinal) <
            sql.IndexOf("ALTER DATABASE", StringComparison.Ordinal),
            "the disclosure comment header must precede the ALTER statement");
    }

    [Fact]
    public void BuildCopyPasteSql_ClearPlanTargets_RenderResolveAndFreeScriptWithDisclosureHeader()
    {
        // Clear-plan is DESTRUCTIVE: live-resolve the currently-cached plan handle(s) for the abnormal
        // query hash and free each — with the two-sided disclosure header.
        var action = new RemediationAction(
            "CLEAR_PLAN", "clear", Array.Empty<ForcePlanTarget>(),
            ClearPlanTargets: new[]
            {
                new ClearPlanTarget(
                    "StackOverflow", "0xABCDEF0123456789",
                    CurrentCpuPerExecMs: 45.0, BaselineCpuPerExecMs: 9.0, AnomalyRatio: 5.0),
            },
            ClearPlanFigures: new ClearPlanFigures(45.0, 9.0, 5.0, 62, false, false));

        var sql = LiteRecommendationsReader.BuildCopyPasteSql(action);

        Assert.NotNull(sql);
        Assert.StartsWith("/*", sql, StringComparison.Ordinal);
        Assert.Contains("Risks of MAKING this change:", sql!, StringComparison.Ordinal);
        Assert.Contains("Risks of NOT making this change:", sql!, StringComparison.Ordinal);
        Assert.Contains("sys.dm_exec_query_stats", sql!, StringComparison.Ordinal);
        Assert.Contains("0xABCDEF0123456789", sql!, StringComparison.Ordinal);
        Assert.Contains("DBCC FREEPROCCACHE(@plan_handle);", sql!, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildCopyPasteSql_DbConfigWithRcsi_RendersSafeBareThenRcsiWithHeader()
    {
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            DbConfigTargets: new[] { new DbConfigTarget("StackOverflow", DbConfigSetting.AutoShrinkOff) },
            RcsiTargets: new[] { new RcsiTarget("StackOverflow", new RcsiInactionFigures(50, 3, 70)) });

        var sql = LiteRecommendationsReader.BuildCopyPasteSql(action);

        Assert.NotNull(sql);
        Assert.StartsWith("ALTER DATABASE [StackOverflow] SET AUTO_SHRINK OFF;", sql, StringComparison.Ordinal);
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

        var sql = LiteRecommendationsReader.BuildCopyPasteSql(action);

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

        var sql = LiteRecommendationsReader.BuildCopyPasteSql(action);

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

        var sql = LiteRecommendationsReader.BuildCopyPasteSql(action);

        // Byte-identical to the shared renderer the drill-down / Dashboard / Darling reader use.
        Assert.Equal(FactRemediation.BuildModifyFileStatement("StackOverflow", "SO_data", 1024), sql);
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

    // ── #3712: the not-paged marker ─────────────────────────────────────────────────

    /// <summary>The grid marks exactly the findings the gate kept off the channels: a notify-worthy lone fact
    /// at the confidence floor reads "not paged" with the gate's reason; a chain, a below-floor finding and a
    /// mapping with no lens (the pure-mapping default every other test here takes) carry no marker.</summary>
    [Fact]
    public void MapFinding_MarksANotifyWorthyUncorroboratedSingle_AndNothingElse()
    {
        var lens = new FindingRoutingLens(NotifySeverity: 1.5, UncorroboratedRoute: FindingRoute.Digest);

        var single = Finding("ANOMALY_CPU_SPIKE", 1.8);
        single.Confidence = StoryConfidence.Floor;
        var marked = LiteRecommendationsReader.MapFinding(single, ServerName, lens);
        Assert.NotNull(marked.NotPagedReason);
        Assert.Contains("uncorroborated", marked.NotPagedReason, StringComparison.Ordinal);

        var chain = Finding("ANOMALY_CPU_SPIKE", 1.8);
        chain.FactCount = 2;
        chain.Confidence = 0.6;
        Assert.Null(LiteRecommendationsReader.MapFinding(chain, ServerName, lens).NotPagedReason);

        var belowFloor = Finding("ANOMALY_CPU_SPIKE", 1.2);
        belowFloor.Confidence = StoryConfidence.Floor;
        Assert.Null(LiteRecommendationsReader.MapFinding(belowFloor, ServerName, lens).NotPagedReason);

        Assert.Null(LiteRecommendationsReader.MapFinding(single, ServerName).NotPagedReason);

        /* The card renders it as a status line, prefixed, and hides the line when there is nothing to say. */
        var card = new LiteRecommendationCardViewModel(marked);
        Assert.True(card.ShowNotPaged);
        Assert.StartsWith("Not paged — ", card.NotPagedText, StringComparison.Ordinal);
        var quiet = new LiteRecommendationCardViewModel(LiteRecommendationsReader.MapFinding(chain, ServerName, lens));
        Assert.False(quiet.ShowNotPaged);
        Assert.Equal(string.Empty, quiet.NotPagedText);
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
        // RootFactKey is no longer surfaced on the card; verify the newer batch (CPU_SQL_PERCENT)
        // won via its mapped Title (same fallback MapFinding uses: advice headline, else the key).
        var cpuAdvice = FactAdvice.GetForFactKey("CPU_SQL_PERCENT");
        var expectedTitle = !string.IsNullOrEmpty(cpuAdvice?.Headline) ? cpuAdvice!.Headline : "CPU_SQL_PERCENT";
        Assert.Equal(expectedTitle, items[0].Title);
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
