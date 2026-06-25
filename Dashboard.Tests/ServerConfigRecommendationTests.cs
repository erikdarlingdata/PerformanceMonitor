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
/// WS3 server-level config: the FactRemediation builder (topology-based MAXDOP / flat CTFP /
/// advise-only memory), the persisted-action DTO round-trip, the Recommendations reader fan-out
/// (one card per ServerConfigTarget; MAXDOP/CTFP carry Apply, memory cards are copy-paste only), and
/// the card affordance model (Copy + Apply / Copy-only, NOT incidents).
/// </summary>
public class ServerConfigRecommendationTests
{
    // ── Builder: edition-aware capped MAXDOP, flat CTFP, advise-only memory ────────

    private static AnalysisFinding ServerConfigFinding(string rootKey, object row) => new()
    {
        ServerId = 1,
        ServerName = "SQL2022",
        Category = "config",
        StoryPath = rootKey,
        StoryPathHash = "sc000001",
        RootFactKey = rootKey,
        DrillDown = new Dictionary<string, object>
        {
            ["server_config"] = new List<object> { row }
        }
    };

    [Theory]
    // MAXDOP is topology-based: min(cores-per-socket, 8). Edition is NOT consulted.
    [InlineData(16, 8)]   // > 8 cores per socket -> capped at 8
    [InlineData(8, 8)]    // exactly 8 -> 8
    [InlineData(4, 4)]    // <= 8 -> the core count
    [InlineData(2, 2)]    // small box -> 2
    [InlineData(1, 1)]    // single core -> 1
    [InlineData(0, 8)]    // cores unknown -> the safe general cap of 8
    public void BuildServerConfig_Maxdop_TopologyBased_CappedAt8(int cores, long expectedRecommended)
    {
        // edition is supplied to mirror the production row shape but is no longer consulted.
        var finding = ServerConfigFinding("CONFIG_MAXDOP",
            new { setting = "maxdop", current_value = 0, edition = 3, cores_per_socket = cores });

        var action = FactRemediation.BuildServerConfigAction(finding);

        Assert.NotNull(action);
        Assert.Equal("SERVER_CONFIG", action!.FactKey);
        var t = Assert.Single(action.ServerConfigTargets!);
        Assert.Equal(ServerConfigSetting.Maxdop, t.Setting);
        Assert.Equal(0, t.CurrentValue);
        Assert.Equal(expectedRecommended, t.RecommendedValue);
    }

    [Fact]
    public void BuildServerConfig_Ctfp_RecommendsFifty()
    {
        var finding = ServerConfigFinding("CONFIG_CTFP",
            new { setting = "ctfp", current_value = 5, edition = 2, cores_per_socket = 8 });

        var action = FactRemediation.BuildServerConfigAction(finding);

        var t = Assert.Single(action!.ServerConfigTargets!);
        Assert.Equal(ServerConfigSetting.CostThreshold, t.Setting);
        Assert.Equal(5, t.CurrentValue);
        Assert.Equal(50, t.RecommendedValue);
    }

    [Fact]
    public void BuildServerConfig_MaxMemory_AdviseOnly_CarriesCurrent()
    {
        var finding = ServerConfigFinding("CONFIG_MAX_MEMORY_MB",
            new { setting = "max_memory", current_value = 2147483647L, edition = 2, cores_per_socket = 8 });

        var action = FactRemediation.BuildServerConfigAction(finding);

        var t = Assert.Single(action!.ServerConfigTargets!);
        Assert.Equal(ServerConfigSetting.MaxServerMemory, t.Setting);
        // Advise-only: recommended carries current (the executor refuses to apply it).
        Assert.Equal(2147483647L, t.CurrentValue);
        Assert.Equal(2147483647L, t.RecommendedValue);
    }

    [Fact]
    public void BuildServerConfig_MinMemory_AdviseOnly()
    {
        var finding = ServerConfigFinding("CONFIG_MIN_MAX_MEMORY_NARROW",
            new { setting = "min_memory", current_value = 24000L, edition = 2, cores_per_socket = 8 });

        var action = FactRemediation.BuildServerConfigAction(finding);

        var t = Assert.Single(action!.ServerConfigTargets!);
        Assert.Equal(ServerConfigSetting.MinServerMemory, t.Setting);
        Assert.Equal(24000L, t.CurrentValue);
    }

    [Fact]
    public void BuildServerConfig_NoDrillDown_ReturnsNull()
    {
        var finding = new AnalysisFinding { ServerId = 1, ServerName = "S", RootFactKey = "CONFIG_MAXDOP" };
        Assert.Null(FactRemediation.BuildServerConfigAction(finding));
    }

    [Fact]
    public void BuildSpConfigureStatement_RendersHardcodedNameAndValue()
    {
        Assert.Equal(
            "EXEC sys.sp_configure N'max degree of parallelism', 8;\nRECONFIGURE;",
            FactRemediation.BuildSpConfigureStatement(ServerConfigSetting.Maxdop, 8));
        Assert.Equal(
            "EXEC sys.sp_configure N'cost threshold for parallelism', 50;\nRECONFIGURE;",
            FactRemediation.BuildSpConfigureStatement(ServerConfigSetting.CostThreshold, 50));
    }

    // ── DTO round-trip (persisted remediation_action_json) ────────────────────────

    [Fact]
    public void ServerConfigAction_RoundTripsThroughDto()
    {
        var action = new RemediationAction(
            "SERVER_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            ServerConfigTargets: new[]
            {
                new ServerConfigTarget(ServerConfigSetting.Maxdop, 0, 8),
                new ServerConfigTarget(ServerConfigSetting.MaxServerMemory, 2147483647, 2147483647)
            });

        var json = AlertContextSerializer.SerializeAction(action);
        Assert.NotNull(json);
        var restored = AlertContextSerializer.DeserializeAction(json);

        Assert.NotNull(restored);
        Assert.Equal("SERVER_CONFIG", restored!.FactKey);
        Assert.NotNull(restored.ServerConfigTargets);
        Assert.Equal(2, restored.ServerConfigTargets!.Count);

        var maxdop = Assert.Single(restored.ServerConfigTargets, t => t.Setting == ServerConfigSetting.Maxdop);
        Assert.Equal(0, maxdop.CurrentValue);
        Assert.Equal(8, maxdop.RecommendedValue);

        var mem = Assert.Single(restored.ServerConfigTargets, t => t.Setting == ServerConfigSetting.MaxServerMemory);
        Assert.Equal(2147483647L, mem.CurrentValue);
    }

    [Fact]
    public void LegacyActionWithoutServerConfig_DeserializesNull()
    {
        // A force-plan action (no ServerConfigTargets) must round-trip with the new field null —
        // backward compatibility for legacy persisted contexts.
        var action = new RemediationAction("PLAN_REGRESSION", "force",
            new[] { new ForcePlanTarget("Db", 1, 2) });
        var restored = AlertContextSerializer.DeserializeAction(AlertContextSerializer.SerializeAction(action));
        Assert.NotNull(restored);
        Assert.Null(restored!.ServerConfigTargets);
    }

    // ── Reader fan-out: one card per target; MAXDOP/CTFP Apply, memory copy-only ───

    private static AnalysisFinding RootedFinding(string rootKey, RemediationAction action) => new()
    {
        ServerId = 1,
        ServerName = "SQL2022",
        DatabaseName = null,            // server-scoped
        Severity = 0.4,
        Category = "config",
        RootFactKey = rootKey,
        StoryPathHash = "h",
        StoryPath = "p",
        Remediation = action
    };

    [Fact]
    public void Reader_FansAllFourServerConfigCards_WithCorrectAffordances()
    {
        // One synthetic finding carrying all four targets — exercises the reader's fan loop directly
        // (each CONFIG_* roots its own finding in production; the loop turns N targets into N cards).
        var action = new RemediationAction(
            "SERVER_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            ServerConfigTargets: new[]
            {
                new ServerConfigTarget(ServerConfigSetting.Maxdop, 0, 8),
                new ServerConfigTarget(ServerConfigSetting.CostThreshold, 5, 50),
                new ServerConfigTarget(ServerConfigSetting.MaxServerMemory, 2147483647, 2147483647),
                new ServerConfigTarget(ServerConfigSetting.MinServerMemory, 24000, 24000),
            });

        var items = RecommendationsReader.MapEngineFindings(RootedFinding("CONFIG_MAXDOP", action));

        Assert.Equal(4, items.Count);
        Assert.All(items, i =>
        {
            Assert.Equal(RecommendationSource.Engine, i.Source);
            Assert.Null(i.Database);                          // server-scoped
            Assert.Equal(RecommendationSetting.None, i.Setting); // never de-dupes
            Assert.True(i.IsServerConfigAdvisory);
            Assert.False(string.IsNullOrEmpty(i.CopyPasteSql)); // every card has copy-paste
            Assert.Contains("SQL2022", i.Title);                // title names the server
        });

        // MAXDOP card: Apply present (single-target SERVER_CONFIG action), title + copy-paste.
        var maxdop = Assert.Single(items, i => i.Title.StartsWith("MAXDOP is 0"));
        Assert.NotNull(maxdop.Remediation);
        Assert.Equal("SERVER_CONFIG", maxdop.Remediation!.FactKey);
        var mt = Assert.Single(maxdop.Remediation.ServerConfigTargets!);
        Assert.Equal(ServerConfigSetting.Maxdop, mt.Setting);
        Assert.Equal(8, mt.RecommendedValue);
        Assert.Contains("max degree of parallelism", maxdop.CopyPasteSql);

        // CTFP card: Apply present.
        var ctfp = Assert.Single(items, i => i.Title.StartsWith("Cost Threshold for Parallelism is 5"));
        Assert.NotNull(ctfp.Remediation);
        Assert.Equal(ServerConfigSetting.CostThreshold, Assert.Single(ctfp.Remediation!.ServerConfigTargets!).Setting);
        Assert.Contains("cost threshold for parallelism", ctfp.CopyPasteSql);

        // max memory card: NO Apply (advise-only), copy-paste only.
        var maxMem = Assert.Single(items, i => i.Title.StartsWith("max server memory is unconfigured"));
        Assert.Null(maxMem.Remediation);
        Assert.Contains("max server memory (MB)", maxMem.CopyPasteSql);

        // min memory card: NO Apply.
        var minMem = Assert.Single(items, i => i.Title.StartsWith("min server memory is pinned near max"));
        Assert.Null(minMem.Remediation);
        Assert.Contains("min server memory (MB)", minMem.CopyPasteSql);
    }

    // ── Affordance model: Copy + Apply (MAXDOP/CTFP), Copy-only (memory), NOT incidents ──

    private static RecommendationCardViewModel Card(RecommendationItem item) => new(item, "SQL2022");

    [Fact]
    public void MaxdopCard_ShowsCopyFixAndApply_NotIncident()
    {
        var item = new RecommendationItem
        {
            CanonicalSeverity = CanonicalSeverity.Warning,
            RawSeverity = 0.4,
            Source = RecommendationSource.Engine,
            Setting = RecommendationSetting.None,
            IsServerConfigAdvisory = true,
            Title = "MAXDOP is 0 — SQL2022",
            CopyPasteSql = "EXEC sys.sp_configure N'max degree of parallelism', 8;\nRECONFIGURE;",
            Remediation = new RemediationAction("SERVER_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
                ServerConfigTargets: new[] { new ServerConfigTarget(ServerConfigSetting.Maxdop, 0, 8) }),
            StoryPathHash = "h"
        };
        var card = Card(item);

        Assert.False(card.IsIncident);
        Assert.False(card.ShowOpenInActiveQueries);
        Assert.False(card.ShowAskAi);
        Assert.True(card.ShowCopyFix);
        Assert.True(card.ShowApply);
    }

    [Fact]
    public void MaxMemoryCard_ShowsCopyFixOnly_NoApply_NotIncident()
    {
        // The advise-only memory card carries NO Remediation, but IsServerConfigAdvisory keeps it a
        // config fix (Copy fix) rather than a time-bound incident — and Apply is hidden (no action).
        var item = new RecommendationItem
        {
            CanonicalSeverity = CanonicalSeverity.Warning,
            RawSeverity = 0.4,
            Source = RecommendationSource.Engine,
            Setting = RecommendationSetting.None,
            IsServerConfigAdvisory = true,
            Title = "max server memory is unconfigured — SQL2022",
            CopyPasteSql = "EXEC sys.sp_configure N'max server memory (MB)', 28672;\nRECONFIGURE;",
            Remediation = null,
            StoryPathHash = "h"
        };
        var card = Card(item);

        Assert.False(card.IsIncident);               // NOT a time-bound incident
        Assert.False(card.ShowOpenInActiveQueries);  // no incident affordances
        Assert.False(card.ShowAskAi);
        Assert.True(card.ShowCopyFix);               // copy-paste shown
        Assert.False(card.ShowApply);                // advise-only — no Apply
    }
}
