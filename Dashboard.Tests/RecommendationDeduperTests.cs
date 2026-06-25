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
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services.Recommendations;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// WS1a coverage for the PURE Recommendations data layer (no DB): the cross-store de-dupe
/// (C3) and canonical severity mapping. Every test synthesizes <see cref="RecommendationItem"/>
/// lists (or scalars) and calls the static <see cref="RecommendationDeduper"/> / the
/// <see cref="RecommendationsReader"/> mappers directly. The actual two-store read is NOT
/// unit-tested here (it needs a live DB — see the PR's integration list).
/// </summary>
public class RecommendationDeduperTests
{
    // ---- builders ---------------------------------------------------------

    private static RecommendationItem EngineItem(
        RecommendationSetting setting,
        string? db,
        double rawSeverity = 0.3,
        string title = "engine")
    {
        var band = RecommendationDeduper.FromEngineSeverity(rawSeverity);
        return new RecommendationItem
        {
            Source = RecommendationSource.Engine,
            CanonicalSeverity = band,
            RawSeverity = rawSeverity,
            Database = db,
            Title = title,
            ProblemArea = "db_config",
            Setting = setting,
            StoryPathHash = "hash-" + title
        };
    }

    private static RecommendationItem LegacyItem(
        RecommendationSetting setting,
        string? db,
        CanonicalSeverity band = CanonicalSeverity.Warning,
        string title = "legacy")
    {
        return new RecommendationItem
        {
            Source = RecommendationSource.Legacy,
            CanonicalSeverity = band,
            RawSeverity = RecommendationDeduper.LegacyRawSeverity(band),
            Database = db,
            Title = title,
            ProblemArea = "Database Configuration",
            Setting = setting
        };
    }

    // ---- de-dupe: collisions ---------------------------------------------

    [Fact]
    public void Merge_AutoShrink_SameDb_BothStores_KeepsEngineOnly()
    {
        var engine = new[] { EngineItem(RecommendationSetting.AutoShrink, "MyDb") };
        var legacy = new[] { LegacyItem(RecommendationSetting.AutoShrink, "MyDb") };

        var result = RecommendationDeduper.Merge(engine, legacy);

        var row = Assert.Single(result);
        Assert.Equal(RecommendationSetting.AutoShrink, row.Setting);
        Assert.Equal(RecommendationSource.Engine, row.Source);
    }

    [Fact]
    public void Merge_AutoClose_SameDb_BothStores_KeepsEngineOnly()
    {
        var engine = new[] { EngineItem(RecommendationSetting.AutoClose, "MyDb") };
        var legacy = new[] { LegacyItem(RecommendationSetting.AutoClose, "MyDb") };

        var result = RecommendationDeduper.Merge(engine, legacy);

        var row = Assert.Single(result);
        Assert.Equal(RecommendationSetting.AutoClose, row.Setting);
        Assert.Equal(RecommendationSource.Engine, row.Source);
    }

    [Fact]
    public void Merge_QueryStore_SameDb_BothStores_KeepsLegacyOnly()
    {
        // Synthesize an engine QS row even though prod does not emit one today — the de-dupe
        // must still prefer the legacy row for QueryStore (the engine has no QS Apply).
        var engine = new[] { EngineItem(RecommendationSetting.QueryStore, "MyDb") };
        var legacy = new[] { LegacyItem(RecommendationSetting.QueryStore, "MyDb") };

        var result = RecommendationDeduper.Merge(engine, legacy);

        var row = Assert.Single(result);
        Assert.Equal(RecommendationSetting.QueryStore, row.Setting);
        Assert.Equal(RecommendationSource.Legacy, row.Source);
    }

    [Fact]
    public void Merge_SameSetting_DifferentDatabases_KeepsBoth()
    {
        var engine = new[] { EngineItem(RecommendationSetting.AutoShrink, "DbOne") };
        var legacy = new[] { LegacyItem(RecommendationSetting.AutoShrink, "DbTwo") };

        var result = RecommendationDeduper.Merge(engine, legacy);

        Assert.Equal(2, result.Count);
        Assert.Contains(result, r => r.Source == RecommendationSource.Engine && r.Database == "DbOne");
        Assert.Contains(result, r => r.Source == RecommendationSource.Legacy && r.Database == "DbTwo");
    }

    [Fact]
    public void Merge_DatabaseNameMatch_IsCaseAndWhitespaceInsensitive()
    {
        // " MyDb " (engine) vs "mydb" (legacy) must be treated as the SAME database.
        var engine = new[] { EngineItem(RecommendationSetting.AutoShrink, " MyDb ") };
        var legacy = new[] { LegacyItem(RecommendationSetting.AutoShrink, "mydb") };

        var result = RecommendationDeduper.Merge(engine, legacy);

        var row = Assert.Single(result);
        Assert.Equal(RecommendationSource.Engine, row.Source);
    }

    // ---- de-dupe: pass-through (Setting=None) ----------------------------

    [Fact]
    public void Merge_NoneSettingRows_NeverDedupe()
    {
        // A memory-pressure legacy row (None) + an engine CPU finding (None) both pass through.
        var legacyMemory = LegacyItem(RecommendationSetting.None, "MyDb",
            band: CanonicalSeverity.Critical, title: "Memory Pressure");
        var engineCpu = EngineItem(RecommendationSetting.None, "MyDb",
            rawSeverity: 1.6, title: "High CPU");

        var result = RecommendationDeduper.Merge(new[] { engineCpu }, new[] { legacyMemory });

        Assert.Equal(2, result.Count);
        Assert.Contains(result, r => r.Title == "Memory Pressure" && r.Source == RecommendationSource.Legacy);
        Assert.Contains(result, r => r.Title == "High CPU" && r.Source == RecommendationSource.Engine);
    }

    [Fact]
    public void Merge_TwoNoneRows_SameDb_AreNotCollapsed()
    {
        // Even with identical (db) the None setting must never key together.
        var a = LegacyItem(RecommendationSetting.None, "MyDb", title: "A");
        var b = LegacyItem(RecommendationSetting.None, "MyDb", title: "B");

        var result = RecommendationDeduper.Merge(Array.Empty<RecommendationItem>(), new[] { a, b });

        Assert.Equal(2, result.Count);
    }

    [Fact]
    public void Merge_NonOverlappingSettings_AllKept()
    {
        var engine = new[]
        {
            EngineItem(RecommendationSetting.Rcsi, "MyDb"),
            EngineItem(RecommendationSetting.PageVerify, "MyDb"),
            EngineItem(RecommendationSetting.AutoShrink, "MyDb")
        };
        var legacy = new[]
        {
            LegacyItem(RecommendationSetting.QueryStore, "MyDb")
        };

        var result = RecommendationDeduper.Merge(engine, legacy);

        Assert.Equal(4, result.Count);
        Assert.Contains(result, r => r.Setting == RecommendationSetting.Rcsi);
        Assert.Contains(result, r => r.Setting == RecommendationSetting.PageVerify);
        Assert.Contains(result, r => r.Setting == RecommendationSetting.AutoShrink && r.Source == RecommendationSource.Engine);
        Assert.Contains(result, r => r.Setting == RecommendationSetting.QueryStore && r.Source == RecommendationSource.Legacy);
    }

    // ---- de-dupe: ordering ----------------------------------------------

    [Fact]
    public void Merge_SortsBySeverityDescending()
    {
        var info = EngineItem(RecommendationSetting.None, "DbA", rawSeverity: 0.3, title: "info");
        var critical = EngineItem(RecommendationSetting.None, "DbB", rawSeverity: 1.8, title: "critical");
        var warning = LegacyItem(RecommendationSetting.None, "DbC",
            band: CanonicalSeverity.Warning, title: "warning");

        var result = RecommendationDeduper.Merge(new[] { info, critical }, new[] { warning });

        Assert.Equal(CanonicalSeverity.Critical, result[0].CanonicalSeverity);
        Assert.Equal(CanonicalSeverity.Warning, result[1].CanonicalSeverity);
        Assert.Equal(CanonicalSeverity.Info, result[2].CanonicalSeverity);
    }

    [Fact]
    public void Merge_OrderIndependent_SameWinnerRegardlessOfInputOrder()
    {
        var engine = EngineItem(RecommendationSetting.AutoShrink, "MyDb");
        var legacy = LegacyItem(RecommendationSetting.AutoShrink, "MyDb");

        var forward = RecommendationDeduper.Merge(new[] { engine }, new[] { legacy });
        var legacyOnly = RecommendationDeduper.Merge(Array.Empty<RecommendationItem>(),
            new[] { legacy }); // legacy alone -> legacy survives (no engine to prefer)

        Assert.Equal(RecommendationSource.Engine, Assert.Single(forward).Source);
        // With only the legacy row present, it must still surface (collision resolver falls back).
        Assert.Equal(RecommendationSource.Legacy, Assert.Single(legacyOnly).Source);
    }

    [Fact]
    public void Merge_EmptyInputs_ReturnsEmpty()
    {
        var result = RecommendationDeduper.Merge(
            Array.Empty<RecommendationItem>(), Array.Empty<RecommendationItem>());
        Assert.Empty(result);
    }

    // ---- canonical severity: engine boundaries ---------------------------

    [Theory]
    [InlineData(0.0, CanonicalSeverity.Info)]
    [InlineData(0.74, CanonicalSeverity.Info)]
    [InlineData(0.75, CanonicalSeverity.Warning)]
    [InlineData(1.49, CanonicalSeverity.Warning)]
    [InlineData(1.5, CanonicalSeverity.Critical)]
    [InlineData(2.0, CanonicalSeverity.Critical)]
    public void FromEngineSeverity_BoundaryCases(double severity, CanonicalSeverity expected)
    {
        Assert.Equal(expected, RecommendationDeduper.FromEngineSeverity(severity));
    }

    // ---- RCSI card severity scales with the contention it would relieve ----

    [Theory]
    [InlineData(0, 0, CanonicalSeverity.Info)]        // gated in (rw>=50) but minimal magnitude
    [InlineData(9, 0, CanonicalSeverity.Info)]        // just under the Warning blocking threshold
    [InlineData(10, 0, CanonicalSeverity.Warning)]    // notable reader/writer blocking
    [InlineData(0, 1, CanonicalSeverity.Warning)]     // a deadlock escalates off Info
    [InlineData(99, 9, CanonicalSeverity.Warning)]    // just under Critical on both axes
    [InlineData(100, 0, CanonicalSeverity.Critical)]  // extreme blocking
    [InlineData(0, 10, CanonicalSeverity.Critical)]   // many deadlocks
    public void RcsiSeverityBand_ScalesWithContention(int blocking, int deadlocks, CanonicalSeverity expected)
    {
        // rwPct doesn't affect the band (the >=50% reader/writer gate already decided the card
        // exists); the band reflects magnitude. 80 here is just a representative gated value.
        var band = RecommendationsReader.RcsiSeverityBand(new RcsiInactionFigures(blocking, deadlocks, 80));
        Assert.Equal(expected, band);
    }

    // ---- canonical severity: legacy text ---------------------------------

    [Theory]
    [InlineData("CRITICAL", CanonicalSeverity.Critical)]
    [InlineData("critical", CanonicalSeverity.Critical)]
    [InlineData(" WARNING ", CanonicalSeverity.Warning)]
    [InlineData("INFO", CanonicalSeverity.Info)]
    [InlineData("", CanonicalSeverity.Info)]
    [InlineData(null, CanonicalSeverity.Info)]
    [InlineData("something-unknown", CanonicalSeverity.Info)]
    public void FromLegacySeverity_TextMapping(string? severity, CanonicalSeverity expected)
    {
        Assert.Equal(expected, RecommendationDeduper.FromLegacySeverity(severity));
    }

    // ---- mapper-side setting derivation (feeds the de-dupe) --------------

    [Fact]
    public void SettingFromAction_DbConfigAutoShrink_MapsAutoShrink()
    {
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            new[] { new DbConfigTarget("MyDb", DbConfigSetting.AutoShrinkOff, "ON") });

        Assert.Equal(RecommendationSetting.AutoShrink, RecommendationsReader.SettingFromAction(action));
    }

    [Fact]
    public void SettingFromAction_DbConfigAutoClose_MapsAutoClose()
    {
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            new[] { new DbConfigTarget("MyDb", DbConfigSetting.AutoCloseOff, "ON") });

        Assert.Equal(RecommendationSetting.AutoClose, RecommendationsReader.SettingFromAction(action));
    }

    [Fact]
    public void SettingFromAction_RcsiFactKey_MapsRcsi()
    {
        var action = new RemediationAction(
            "RCSI", "set", Array.Empty<ForcePlanTarget>(),
            new[] { new DbConfigTarget("MyDb", DbConfigSetting.ReadCommittedSnapshotOn, "OFF") });

        Assert.Equal(RecommendationSetting.Rcsi, RecommendationsReader.SettingFromAction(action));
    }

    [Fact]
    public void SettingFromAction_ForcePlan_MapsNone()
    {
        var action = new RemediationAction(
            "PLAN_REGRESSION", "force",
            new[] { new ForcePlanTarget("MyDb", 1, 2) });

        Assert.Equal(RecommendationSetting.None, RecommendationsReader.SettingFromAction(action));
    }

    [Fact]
    public void SettingFromAction_Null_MapsNone()
    {
        Assert.Equal(RecommendationSetting.None, RecommendationsReader.SettingFromAction(null));
    }

    [Theory]
    [InlineData("Database Configuration", "ALTER DATABASE [x] SET AUTO_SHRINK OFF;", RecommendationSetting.AutoShrink)]
    [InlineData("Database Configuration", "ALTER DATABASE [x] SET AUTO_CLOSE OFF;", RecommendationSetting.AutoClose)]
    [InlineData("Query Store Configuration", "ALTER DATABASE [x] SET QUERY_STORE = ON;", RecommendationSetting.QueryStore)]
    [InlineData("Query Store Configuration", "ALTER DATABASE [x] SET QUERY_STORE (OPERATION_MODE = READ_WRITE);", RecommendationSetting.QueryStore)]
    public void SettingFromLegacy_ConfigArea_ParsesAlterKeyword(
        string problemArea, string investigateQuery, RecommendationSetting expected)
    {
        Assert.Equal(expected, RecommendationsReader.SettingFromLegacy(problemArea, investigateQuery));
    }

    [Fact]
    public void SettingFromLegacy_NonConfigArea_MapsNone()
    {
        // A memory-pressure row whose SQL incidentally mentions a keyword must NOT de-dupe:
        // only the two config problem-areas are eligible.
        Assert.Equal(RecommendationSetting.None,
            RecommendationsReader.SettingFromLegacy("Memory", "SELECT 'AUTO_SHRINK';"));
    }

    [Fact]
    public void SettingFromLegacy_ConfigArea_NoKeyword_MapsNone()
    {
        Assert.Equal(RecommendationSetting.None,
            RecommendationsReader.SettingFromLegacy("Database Configuration", "SELECT 1;"));
    }

    [Fact]
    public void SettingFromLegacy_ConfigArea_NullSql_MapsNone()
    {
        Assert.Equal(RecommendationSetting.None,
            RecommendationsReader.SettingFromLegacy("Database Configuration", null));
    }

    // ---- mapper-side copy-paste + full engine mapping --------------------

    [Fact]
    public void BuildCopyPasteFromAction_DbConfig_RendersAlterStatements()
    {
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            new[]
            {
                new DbConfigTarget("My]Db", DbConfigSetting.AutoShrinkOff, "ON"),
                new DbConfigTarget("My]Db", DbConfigSetting.AutoCloseOff, "ON")
            });

        var sql = RecommendationsReader.BuildCopyPasteFromAction(action);

        Assert.NotNull(sql);
        // Identifier bracket-doubling (QUOTENAME-equivalent) is applied.
        Assert.Contains("ALTER DATABASE [My]]Db] SET AUTO_SHRINK OFF;", sql);
        Assert.Contains("ALTER DATABASE [My]]Db] SET AUTO_CLOSE OFF;", sql);
    }

    [Fact]
    public void BuildCopyPasteFromAction_ForcePlan_ReturnsNull()
    {
        var action = new RemediationAction(
            "PLAN_REGRESSION", "force", new[] { new ForcePlanTarget("MyDb", 1, 2) });

        Assert.Null(RecommendationsReader.BuildCopyPasteFromAction(action));
    }

    [Fact]
    public void MapEngineFinding_DbConfig_CarriesSettingActionAndHash()
    {
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            new[] { new DbConfigTarget("MyDb", DbConfigSetting.AutoShrinkOff, "ON") });

        var finding = new AnalysisFinding
        {
            ServerId = 1,
            ServerName = "S",
            DatabaseName = "MyDb",
            Severity = 0.3,
            Category = "db_config",
            StoryText = "AUTO_SHRINK is on",
            RootFactKey = "DB_CONFIG",
            StoryPathHash = "abc123",
            StoryPath = "config>db_config>MyDb",
            Remediation = action
        };

        var item = RecommendationsReader.MapEngineFinding(finding);

        Assert.Equal(RecommendationSource.Engine, item.Source);
        Assert.Equal(RecommendationSetting.AutoShrink, item.Setting);
        Assert.Equal(CanonicalSeverity.Info, item.CanonicalSeverity); // 0.3 -> Info
        Assert.Same(action, item.Remediation);
        Assert.Equal("abc123", item.StoryPathHash);
        Assert.Equal("config>db_config>MyDb", item.StoryPath); // carried for the mute record
        Assert.NotNull(item.CopyPasteSql);
        Assert.Contains("AUTO_SHRINK OFF", item.CopyPasteSql);
    }

    // ---- per-(db,setting) fan-out of DB_CONFIG findings -------------------

    [Fact]
    public void MapEngineFindings_DbConfig_SingleSafeTarget_FansToPerDbCardWithSlicedApply()
    {
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            new[] { new DbConfigTarget("MyDb", DbConfigSetting.AutoShrinkOff, "ON") });

        var finding = new AnalysisFinding
        {
            ServerId = 1,
            ServerName = "S",
            DatabaseName = null,            // DB_CONFIG is SERVER-scoped
            Severity = 0.3,
            Category = "database_config",
            RootFactKey = "DB_CONFIG",
            StoryPathHash = "abc123",
            StoryPath = "config>db_config",
            Remediation = action
        };

        var item = Assert.Single(RecommendationsReader.MapEngineFindings(finding));

        Assert.Equal(RecommendationSource.Engine, item.Source);
        Assert.Equal("MyDb", item.Database);                  // from the target, NOT the null finding db
        Assert.Equal(RecommendationSetting.AutoShrink, item.Setting);
        Assert.Contains("AUTO_SHRINK", item.Title);
        Assert.Contains("MyDb", item.Title);
        Assert.NotNull(item.CopyPasteSql);
        Assert.Contains("AUTO_SHRINK OFF", item.CopyPasteSql);
        // The Apply action is sliced to exactly this one (db, setting).
        Assert.NotNull(item.Remediation);
        var target = Assert.Single(item.Remediation!.DbConfigTargets!);
        Assert.Equal("MyDb", target.Database);
        Assert.Equal(DbConfigSetting.AutoShrinkOff, target.Setting);
        Assert.Equal("abc123", item.StoryPathHash);          // mute key carried from the finding
    }

    [Fact]
    public void MapEngineFindings_DbConfig_MultipleTargets_FansOutOnePerTarget()
    {
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            new[]
            {
                new DbConfigTarget("Db1", DbConfigSetting.AutoShrinkOff, "ON"),
                new DbConfigTarget("Db2", DbConfigSetting.AutoCloseOff, "ON"),
                new DbConfigTarget("Db3", DbConfigSetting.PageVerifyChecksum, "NONE")
            });

        var finding = new AnalysisFinding
        {
            ServerId = 1,
            ServerName = "S",
            DatabaseName = null,
            Severity = 0.3,
            Category = "database_config",
            RootFactKey = "DB_CONFIG",
            StoryPathHash = "h",
            StoryPath = "p",
            Remediation = action
        };

        var items = RecommendationsReader.MapEngineFindings(finding);

        Assert.Equal(3, items.Count);
        // Each card is per-(db, setting); each Apply action is sliced to one target.
        Assert.All(items, i => Assert.Single(i.Remediation!.DbConfigTargets!));
        Assert.Contains(items, i => i.Database == "Db1" && i.Setting == RecommendationSetting.AutoShrink);
        Assert.Contains(items, i => i.Database == "Db2" && i.Setting == RecommendationSetting.AutoClose);
        Assert.Contains(items, i => i.Database == "Db3" && i.Setting == RecommendationSetting.PageVerify);
    }

    [Fact]
    public void MapEngineFindings_DbConfig_NoSafeTargets_FallsBackToSingleGenericCard()
    {
        var finding = new AnalysisFinding
        {
            ServerId = 1,
            ServerName = "S",
            DatabaseName = null,
            Severity = 0.3,
            Category = "database_config",
            StoryText = "config issues",
            RootFactKey = "DB_CONFIG",
            StoryPathHash = "h",
            StoryPath = "p",
            Remediation = null            // e.g. only RCSI-off (not a safe target) — no safe action persisted
        };

        var item = Assert.Single(RecommendationsReader.MapEngineFindings(finding));
        Assert.Equal(RecommendationSetting.None, item.Setting);   // generic advise card, never de-dupes
        Assert.Null(item.Remediation);
    }

    [Fact]
    public void MapEngineFindings_NonDbConfig_ReturnsSingleItemUnchanged()
    {
        var action = new RemediationAction(
            "FILE_AUTOGROWTH_PERCENT", "set", Array.Empty<ForcePlanTarget>(),
            FileGrowthTargets: new[] { new FileGrowthTarget("MyDb", "MyDb_data", 120000, 10, 512) });

        var finding = new AnalysisFinding
        {
            ServerId = 1,
            ServerName = "S",
            DatabaseName = "MyDb",
            Severity = 0.3,
            Category = "config",
            StoryText = "Large file(s) growing in percentage steps",
            RootFactKey = "FILE_AUTOGROWTH_PERCENT",
            StoryPathHash = "fa",
            StoryPath = "fa",
            Remediation = action
        };

        var item = Assert.Single(RecommendationsReader.MapEngineFindings(finding));
        Assert.Equal(RecommendationSetting.None, item.Setting);
        Assert.Same(action, item.Remediation);   // same single action, NOT sliced
    }

    // ---- per-db RCSI fan-out of DB_CONFIG findings (destructive, consent-gated) ----

    [Fact]
    public void MapEngineFindings_DbConfig_RcsiTargets_FanToPerDbRcsiCardsWithRcsiAction()
    {
        // A DB_CONFIG finding whose action carries TWO per-db RCSI targets fans to TWO RCSI
        // cards — each a distinct FactKey="RCSI" action (dispatches to RcsiHandler + the two-
        // sided consent gate) carrying THAT db's real inaction figures.
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            DbConfigTargets: Array.Empty<DbConfigTarget>(),
            RcsiTargets: new[]
            {
                new RcsiTarget("Sales", new RcsiInactionFigures(40, 2, 85)),
                new RcsiTarget("Orders", new RcsiInactionFigures(12, 0, 30))
            });

        var finding = new AnalysisFinding
        {
            ServerId = 1,
            ServerName = "S",
            DatabaseName = null,            // DB_CONFIG is SERVER-scoped
            Severity = 0.3,
            Category = "database_config",
            RootFactKey = "DB_CONFIG",
            StoryPathHash = "h",
            StoryPath = "p",
            Remediation = action
        };

        var items = RecommendationsReader.MapEngineFindings(finding);

        Assert.Equal(2, items.Count);
        Assert.All(items, i =>
        {
            Assert.Equal(RecommendationSource.Engine, i.Source);
            Assert.Equal(RecommendationSetting.Rcsi, i.Setting);
            Assert.NotNull(i.Remediation);
            Assert.Equal("RCSI", i.Remediation!.FactKey);                       // dispatches to RcsiHandler
            Assert.NotNull(i.Remediation.RcsiFigures);                          // real figures for the consent dialog
            Assert.Contains("READ_COMMITTED_SNAPSHOT ON", i.CopyPasteSql);
            Assert.Contains(i.Database!, i.Title);                              // Title names the db
            // The reconstructed action's single target is THIS db with the RCSI setting.
            var target = Assert.Single(i.Remediation.DbConfigTargets!);
            Assert.Equal(i.Database, target.Database);
            Assert.Equal(DbConfigSetting.ReadCommittedSnapshotOn, target.Setting);
            Assert.Equal("h", i.StoryPathHash);
        });

        var sales = Assert.Single(items, i => i.Database == "Sales");
        Assert.Equal(40, sales.Remediation!.RcsiFigures!.BlockingEvents);
        Assert.Equal(2, sales.Remediation.RcsiFigures.Deadlocks);
        Assert.Equal(85, sales.Remediation.RcsiFigures.ReaderWriterPct);

        var orders = Assert.Single(items, i => i.Database == "Orders");
        Assert.Equal(12, orders.Remediation!.RcsiFigures!.BlockingEvents);
        Assert.Equal(0, orders.Remediation.RcsiFigures.Deadlocks);
        Assert.Equal(30, orders.Remediation.RcsiFigures.ReaderWriterPct);
    }

    [Fact]
    public void MapEngineFindings_DbConfig_SafeAndRcsiTargets_ProduceBothSetsOfCards()
    {
        // One DB_CONFIG finding carrying BOTH a safe AUTO_SHRINK target and an RCSI target must
        // produce a safe-setting card AND an RCSI card (the two fan-outs are independent).
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            DbConfigTargets: new[] { new DbConfigTarget("Sales", DbConfigSetting.AutoShrinkOff, "ON") },
            RcsiTargets: new[] { new RcsiTarget("Orders", new RcsiInactionFigures(12, 3, 80)) });

        var finding = new AnalysisFinding
        {
            ServerId = 1,
            ServerName = "S",
            DatabaseName = null,
            Severity = 0.3,
            Category = "database_config",
            RootFactKey = "DB_CONFIG",
            StoryPathHash = "h",
            StoryPath = "p",
            Remediation = action
        };

        var items = RecommendationsReader.MapEngineFindings(finding);

        Assert.Equal(2, items.Count);
        // The safe AUTO_SHRINK card: DB_CONFIG action, AutoShrink setting.
        var safe = Assert.Single(items, i => i.Setting == RecommendationSetting.AutoShrink);
        Assert.Equal("Sales", safe.Database);
        Assert.Equal("DB_CONFIG", safe.Remediation!.FactKey);
        Assert.Contains("AUTO_SHRINK OFF", safe.CopyPasteSql);
        // The destructive RCSI card: distinct FactKey="RCSI" action with figures.
        var rcsi = Assert.Single(items, i => i.Setting == RecommendationSetting.Rcsi);
        Assert.Equal("Orders", rcsi.Database);
        Assert.Equal("RCSI", rcsi.Remediation!.FactKey);
        Assert.Equal(12, rcsi.Remediation.RcsiFigures!.BlockingEvents);
        Assert.Contains("READ_COMMITTED_SNAPSHOT ON", rcsi.CopyPasteSql);
    }

    [Fact]
    public void EngineDbConfigFanOut_DeDupesWithLegacyPerDbRow_EngineWinsWithApply()
    {
        // Engine: server-scoped DB_CONFIG finding carrying an AUTO_SHRINK target for MyDb.
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            new[] { new DbConfigTarget("MyDb", DbConfigSetting.AutoShrinkOff, "ON") });
        var finding = new AnalysisFinding
        {
            ServerId = 1,
            ServerName = "S",
            DatabaseName = null,
            Severity = 0.3,
            Category = "database_config",
            RootFactKey = "DB_CONFIG",
            StoryPathHash = "h",
            StoryPath = "p",
            Remediation = action
        };
        var engine = RecommendationsReader.MapEngineFindings(finding);

        // Legacy: the per-db AUTO_SHRINK row the frozen proc emits for the same database.
        var legacy = RecommendationsReader.MapLegacyIssue(new CriticalIssueItem
        {
            Severity = "WARNING",
            ProblemArea = "Database Configuration",
            AffectedDatabase = "MyDb",
            Message = "AUTO_SHRINK is enabled",
            InvestigateQuery = "ALTER DATABASE [MyDb] SET AUTO_SHRINK OFF;"
        });

        var merged = RecommendationDeduper.Merge(engine, new[] { legacy });

        // Before this fix the engine card had Database=null → keys differed → TWO cards (the
        // legacy copy-only one is what the user saw). Now they collapse to ONE engine card
        // that carries the Apply action.
        var item = Assert.Single(merged);
        Assert.Equal(RecommendationSource.Engine, item.Source);
        Assert.NotNull(item.Remediation);
    }

    // ---- WS3: percent-autogrowth advisory copy-paste flows through the reader ----

    [Fact]
    public void BuildCopyPasteFromAction_FileAutogrowth_RendersModifyFileStatements()
    {
        var action = new RemediationAction(
            "FILE_AUTOGROWTH_PERCENT", "set", System.Array.Empty<ForcePlanTarget>(),
            FileGrowthTargets: new[]
            {
                // Bracket-doubling is exercised on both the db and the logical file name.
                new FileGrowthTarget("My]Db", "data]1", 60000, 10, 512),
                new FileGrowthTarget("My]Db", "log]1", 250000, 25, 1024)
            });

        var sql = RecommendationsReader.BuildCopyPasteFromAction(action);

        Assert.NotNull(sql);
        Assert.Contains("ALTER DATABASE [My]]Db] MODIFY FILE (NAME = [data]]1], FILEGROWTH = 512MB);", sql);
        Assert.Contains("ALTER DATABASE [My]]Db] MODIFY FILE (NAME = [log]]1], FILEGROWTH = 1024MB);", sql);
    }

    [Fact]
    public void MapEngineFinding_FileAutogrowth_CarriesCopyPasteAndNoDeDupeSetting()
    {
        var action = new RemediationAction(
            "FILE_AUTOGROWTH_PERCENT", "set", System.Array.Empty<ForcePlanTarget>(),
            FileGrowthTargets: new[] { new FileGrowthTarget("MyDb", "MyDb_data", 120000, 10, 512) });

        var finding = new AnalysisFinding
        {
            ServerId = 1,
            ServerName = "S",
            DatabaseName = "MyDb",
            Severity = 0.3,
            Category = "config",
            StoryText = "Large file(s) growing in percentage steps",
            RootFactKey = "FILE_AUTOGROWTH_PERCENT",
            StoryPathHash = "fa9000",
            StoryPath = "FILE_AUTOGROWTH_PERCENT",
            Remediation = action
        };

        var item = RecommendationsReader.MapEngineFinding(finding);

        // Fix + copy-paste: the copy-paste flows, but the row never de-dupes (file-level, not a
        // per-DB config-setting collision) and the action stays attached.
        Assert.Equal(RecommendationSetting.None, item.Setting);
        Assert.Same(action, item.Remediation);
        Assert.NotNull(item.CopyPasteSql);
        Assert.Contains("MODIFY FILE (NAME = [MyDb_data], FILEGROWTH = 512MB);", item.CopyPasteSql);
        // Headline from the authored advice block.
        Assert.Equal("Large file(s) growing in percentage steps", item.Title);
    }

    [Fact]
    public void MapLegacyIssue_MemoryPressure_IsNoneAndAdviseOnly()
    {
        var issue = new CriticalIssueItem
        {
            Severity = "CRITICAL",
            ProblemArea = "Memory",
            AffectedDatabase = string.Empty,
            Message = "Memory pressure detected",
            InvestigateQuery = "SELECT * FROM sys.dm_os_memory_clerks;"
        };

        var item = RecommendationsReader.MapLegacyIssue(issue);

        Assert.Equal(RecommendationSource.Legacy, item.Source);
        Assert.Equal(RecommendationSetting.None, item.Setting);
        Assert.Equal(CanonicalSeverity.Critical, item.CanonicalSeverity);
        Assert.Null(item.Remediation);
        Assert.Null(item.StoryPathHash);
        Assert.Null(item.StoryPath); // legacy rows have no mute concept
        Assert.Null(item.Database); // empty AffectedDatabase -> null
        Assert.Equal("Memory pressure detected", item.AdviceText);
        Assert.Equal("SELECT * FROM sys.dm_os_memory_clerks;", item.CopyPasteSql);
    }

    [Fact]
    public void MapLegacyIssue_DatabaseConfiguration_AutoShrink_DerivesSetting()
    {
        var issue = new CriticalIssueItem
        {
            Severity = "WARNING",
            ProblemArea = "Database Configuration",
            AffectedDatabase = "MyDb",
            Message = "Auto shrink is enabled",
            InvestigateQuery = "ALTER DATABASE [MyDb] SET AUTO_SHRINK OFF;"
        };

        var item = RecommendationsReader.MapLegacyIssue(issue);

        Assert.Equal(RecommendationSetting.AutoShrink, item.Setting);
        Assert.Equal("MyDb", item.Database);
    }
}
