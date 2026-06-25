/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Services.Remediation;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// The DYNAMIC golden-sample half of the remediation contract — the follow-up named in
/// <see cref="RemediationContractTests"/>'s scope note. It feeds a representative detector
/// drill-down — shaped to match what the drill-down COLLECTORS actually emit
/// (<c>SqlServerDrillDownCollector</c> on Dashboard / <c>DrillDownCollector</c> on Lite, with the
/// shared <see cref="FactRemediation"/> extractors reading it) — through each builder in the EXACT
/// order <c>AnalysisService.AnalyzeAsync</c> applies them, and asserts the builder yields a
/// non-null action, carrying its target list, whose <c>FactKey</c> resolves through the production
/// handler registry.
///
/// <para>
/// This is the "autogrowth-CTE-drift" guard at the dynamic level: if a builder stops reading a
/// drill-down key the detector emits (a rename, a typo, an extractor refactor), the action comes
/// back null or target-less and the matching test fails loudly — instead of Apply silently
/// no-opping in production. <see cref="RemediationContractTests"/> locks the STATIC
/// handler&lt;-&gt;key set; this locks that real drill-down still flows through to a DISPATCHABLE
/// action, and that every registered Apply-able key has a fixture proving it.
/// </para>
///
/// <para>
/// Residual limitation (documented, not silently accepted): the fixtures are captured from the
/// collector emit at authoring time (each cites its emit site). They catch BUILDER-side drift
/// immediately; a future COLLECTOR-side key rename is caught only when these fixtures are
/// regenerated against the new emit — the fixtures ARE the contract, so keep them in sync with the
/// cited emit sites. (A drill-down key-name constant set shared by collector and builder would make
/// this automatic; that is a larger refactor, out of scope here.)
/// </para>
/// </summary>
public class RemediationGoldenSampleTests
{
    // The production dispatch point (RemediationApplyService builds the v1 registry from this).
    private static readonly RemediationHandlerRegistry Registry =
        new(RemediationApplyService.CreateDefaultHandlers());

    /// <summary>
    /// A built action is "dispatchable" when it is non-null, carries the expected fact key, and
    /// that key resolves through the production handler registry — i.e. Apply will route to a real
    /// handler instead of silently no-opping (<c>RemediationRunStatus.NoHandler</c>).
    /// </summary>
    private static void AssertDispatchable(RemediationAction? action, string expectedFactKey)
    {
        Assert.NotNull(action);
        Assert.Equal(expectedFactKey, action!.FactKey);
        Assert.NotNull(Registry.TryGet(action.FactKey));
    }

    // ── PLAN_REGRESSION → BuildAction → FactKey "PLAN_REGRESSION" (ForcePlanHandler) ──────────────
    // Drill-down: SqlServerDrillDownCollector "regressed_queries"; read by ExtractPlanRegressionTargets
    // (database / query_id / best_plan_id / *_plan_hash / *_cpu_per_exec_us / regression_factor).
    private static AnalysisFinding PlanRegression() => new()
    {
        ServerId = 1, ServerName = "GoldenServer", Category = "plan_regression",
        StoryPath = "PLAN_REGRESSION", StoryPathHash = "golden_planreg01",
        RootFactKey = "PLAN_REGRESSION",
        DrillDown = new Dictionary<string, object>
        {
            ["regressed_queries"] = new List<object>
            {
                new
                {
                    database = "AdventureWorks", query_id = 4242L, best_plan_id = 17L,
                    latest_plan_hash = "0xDEAD", best_plan_hash = "0xBEEF",
                    latest_cpu_per_exec_us = 9000.0, best_cpu_per_exec_us = 1200.0,
                    regression_factor = 7.5
                }
            }
        }
    };

    [Fact]
    public void PlanRegression_DrillDown_BuildsDispatchableForceAction()
    {
        var action = FactRemediation.BuildAction(PlanRegression());
        AssertDispatchable(action, "PLAN_REGRESSION");
        Assert.NotEmpty(action!.Targets);
    }

    // ── DB_CONFIG → BuildAction → FactKey "DB_CONFIG" (DbConfigHandler) ────────────────────────────
    // Drill-down: SqlServerDrillDownCollector "config_issues"; read by ExtractDbConfigTargets
    // (database / auto_shrink / auto_close / page_verify — the structured typed fields, never the
    // human "issues" strings).
    private static AnalysisFinding DbConfig() => new()
    {
        ServerId = 1, ServerName = "GoldenServer", Category = "config_issues",
        StoryPath = "DB_CONFIG", StoryPathHash = "golden_dbconfig1",
        RootFactKey = "DB_CONFIG",
        DrillDown = new Dictionary<string, object>
        {
            ["config_issues"] = new List<object>
            {
                new
                {
                    database = "Foo", recovery_model = "FULL", rcsi = true, query_store = true,
                    issues = new[] { "auto_shrink ON" },
                    auto_shrink = true, auto_close = false, page_verify = "CHECKSUM"
                }
            }
        }
    };

    [Fact]
    public void DbConfig_DrillDown_BuildsDispatchableSetAction()
    {
        var action = FactRemediation.BuildAction(DbConfig());
        AssertDispatchable(action, "DB_CONFIG");
        Assert.NotNull(action!.DbConfigTargets);
        Assert.NotEmpty(action.DbConfigTargets!);
    }

    // ── RCSI → BuildRcsiAction → FactKey "RCSI" (RcsiHandler, destructive) ─────────────────────────
    // Drill-down: SqlServerDrillDownCollector "config_issues" with rcsi == false + the §3.3
    // enrichment (rcsi_blocking_events / rcsi_deadlocks / rcsi_reader_writer_pct) and a
    // reader/writer share above FactRiskDisclosure.ReaderWriterMeaningfulPct; read by CollectRcsiTargets.
    private static AnalysisFinding RcsiOff() => new()
    {
        ServerId = 1, ServerName = "GoldenServer", Category = "config_issues",
        StoryPath = "DB_CONFIG", StoryPathHash = "golden_rcsioff01",
        RootFactKey = "DB_CONFIG",
        DrillDown = new Dictionary<string, object>
        {
            ["config_issues"] = new List<object>
            {
                new
                {
                    database = "Foo", recovery_model = "FULL", rcsi = false, query_store = true,
                    issues = new[] { "RCSI OFF" },
                    auto_shrink = false, auto_close = false, page_verify = "CHECKSUM",
                    rcsi_blocking_events = 12, rcsi_deadlocks = 3, rcsi_reader_writer_pct = 80
                }
            }
        }
    };

    [Fact]
    public void Rcsi_DrillDown_BuildsDispatchableRcsiAction()
    {
        var action = FactRemediation.BuildRcsiAction(RcsiOff());
        AssertDispatchable(action, "RCSI");
        Assert.NotNull(action!.DbConfigTargets);
        Assert.Equal(DbConfigSetting.ReadCommittedSnapshotOn, action.DbConfigTargets!.Single().Setting);
    }

    // ── CLEAR_PLAN → BuildClearPlanAction → FactKey "CLEAR_PLAN" (ClearPlanHandler, destructive) ────
    // Drill-down: SqlServerDrillDownCollector "abnormal_cpu_plans" on a CPU finding; read by
    // BuildClearPlanAction (query_hash 0x… / database / current+baseline_cpu_per_exec_ms /
    // anomaly_ratio / cpu_percent / *_cofired / latest_plan_handle).
    private static AnalysisFinding ClearPlan() => new()
    {
        ServerId = 1, ServerName = "GoldenServer", Category = "cpu",
        StoryPath = "CPU_SQL_PERCENT", StoryPathHash = "golden_clearpln1",
        RootFactKey = "CPU_SQL_PERCENT",
        DrillDown = new Dictionary<string, object>
        {
            ["abnormal_cpu_plans"] = new List<object>
            {
                new
                {
                    query_hash = "0x1A2B3C4D5E6F7080", database = "Foo",
                    current_cpu_per_exec_ms = 500.0, baseline_cpu_per_exec_ms = 50.0,
                    anomaly_ratio = 10.0, cpu_percent = 40,
                    latest_plan_handle = "0x06000100ABCDEF",
                    plan_regression_cofired = false, parameter_sensitivity_cofired = false
                }
            }
        }
    };

    [Fact]
    public void ClearPlan_DrillDown_BuildsDispatchableClearAction()
    {
        var action = FactRemediation.BuildClearPlanAction(ClearPlan());
        AssertDispatchable(action, "CLEAR_PLAN");
        Assert.NotNull(action!.ClearPlanTargets);
        Assert.NotEmpty(action.ClearPlanTargets!);
    }

    // ── FILE_AUTOGROWTH_PERCENT → BuildFileAutogrowthAction → FactKey "FILE_AUTOGROWTH_PERCENT"
    //    (FileAutogrowthHandler) ─────────────────────────────────────────────────────────────────
    // Drill-down: SqlServerDrillDownCollector "autogrowth_percent_files"; read by
    // ExtractFileGrowthTargets (database / logical_file_name / total_size_mb / growth_pct / file_type).
    // This is the canonical "CTE-drift" sentinel: if the collector's CTE column → drill-down key
    // mapping drifts from these names, the builder yields no targets and this fails.
    private static AnalysisFinding FileAutogrowth() => new()
    {
        ServerId = 1, ServerName = "GoldenServer", Category = "config_issues",
        StoryPath = "FILE_AUTOGROWTH_PERCENT", StoryPathHash = "golden_autogro01",
        RootFactKey = "FILE_AUTOGROWTH_PERCENT",
        DrillDown = new Dictionary<string, object>
        {
            ["autogrowth_percent_files"] = new List<object>
            {
                new
                {
                    database = "Foo", logical_file_name = "Foo_data", file_type = "ROWS",
                    total_size_mb = 51200.0, growth_pct = 10
                }
            }
        }
    };

    [Fact]
    public void FileAutogrowth_DrillDown_BuildsDispatchableSetAction()
    {
        var action = FactRemediation.BuildFileAutogrowthAction(FileAutogrowth());
        AssertDispatchable(action, "FILE_AUTOGROWTH_PERCENT");
        Assert.NotNull(action!.FileGrowthTargets);
        Assert.NotEmpty(action.FileGrowthTargets!);
    }

    // ── SERVER_CONFIG → BuildServerConfigAction → FactKey "SERVER_CONFIG" (ServerConfigHandler) ─────
    // Drill-down: SqlServerDrillDownCollector "server_config" on a CONFIG_* finding; read by
    // ExtractServerConfigTargets (setting maxdop/ctfp/max_memory/min_memory / current_value /
    // edition / cores_per_socket).
    private static AnalysisFinding ServerConfig() => new()
    {
        ServerId = 1, ServerName = "GoldenServer", Category = "config",
        StoryPath = "SERVER_CONFIG → CONFIG_MAXDOP", StoryPathHash = "golden_srvconf01",
        RootFactKey = "CONFIG_MAXDOP",
        DrillDown = new Dictionary<string, object>
        {
            ["server_config"] = new List<object>
            {
                new { setting = "maxdop", current_value = 0L, edition = 3, cores_per_socket = 16 }
            }
        }
    };

    [Fact]
    public void ServerConfig_DrillDown_BuildsDispatchableSetAction()
    {
        var action = FactRemediation.BuildServerConfigAction(ServerConfig());
        AssertDispatchable(action, "SERVER_CONFIG");
        Assert.NotNull(action!.ServerConfigTargets);
        Assert.Equal(ServerConfigSetting.Maxdop, action.ServerConfigTargets!.Single().Setting);
    }

    // ── MISSING_INDEX → BuildMissingIndexAction → FactKey "MISSING_INDEX" (WS4; COPY-ONLY) ─────────
    // The drill-down → builder drift guard for the one COPY-PASTE-ONLY advisory: a non-null action
    // carrying its CREATE-statement targets, but deliberately NOT registry-dispatchable — creating an
    // index is a judgement call, so there is no handler and no Apply (the reader renders the CREATE as
    // copy-paste and leaves the card's Remediation null). Drill-down: SqlServerDrillDownCollector /
    // Lite DrillDownCollector "missing_indexes" ({ table, impact, create_statement }).
    private static AnalysisFinding MissingIndex() => new()
    {
        ServerId = 1, ServerName = "GoldenServer", Category = "missing_index",
        StoryPath = "MISSING_INDEX", StoryPathHash = "golden_missidx01",
        RootFactKey = "MISSING_INDEX",
        DrillDown = new Dictionary<string, object>
        {
            ["missing_indexes"] = new List<object>
            {
                new
                {
                    table = "dbo.Orders", impact = 87.3,
                    create_statement = "CREATE NONCLUSTERED INDEX [ix_Orders_CustomerId] ON [dbo].[Orders] ([CustomerId]);"
                }
            }
        }
    };

    [Fact]
    public void MissingIndex_DrillDown_BuildsCopyOnlyAdvisoryAction_NotDispatchable()
    {
        var action = FactRemediation.BuildMissingIndexAction(MissingIndex());
        Assert.NotNull(action);
        Assert.Equal("MISSING_INDEX", action!.FactKey);
        Assert.NotNull(action.MissingIndexTargets);
        Assert.NotEmpty(action.MissingIndexTargets!);
        // Copy-only: NO handler resolves it — the reader surfaces copy-paste and shows no Apply.
        Assert.Null(Registry.TryGet(action.FactKey));
    }

    // ── Completeness: every registered Apply-able key has a golden fixture ─────────────────────────
    [Fact]
    public void EveryRegisteredHandlerKey_IsExercisedByAGoldenFixture()
    {
        // Every key the production registry dispatches must have a golden fixture proving real
        // drill-down still flows through to a dispatchable action. A new Apply-able handler with no
        // golden fixture here fails loudly — the dynamic complement to RemediationContractTests'
        // static handler<->key check, which forces a fixture whenever the Apply-able surface grows.
        var handlerKeys = RemediationApplyService.CreateDefaultHandlers()
            .Select(h => h.FactKey)
            .ToHashSet();

        // Each builder is invoked exactly as AnalysisService.AnalyzeAsync orders the chain.
        var produced = new[]
        {
            FactRemediation.BuildAction(PlanRegression()),
            FactRemediation.BuildAction(DbConfig()),
            FactRemediation.BuildRcsiAction(RcsiOff()),
            FactRemediation.BuildClearPlanAction(ClearPlan()),
            FactRemediation.BuildFileAutogrowthAction(FileAutogrowth()),
            FactRemediation.BuildServerConfigAction(ServerConfig()),
        }
        .Where(a => a is not null)
        .Select(a => a!.FactKey)
        .ToHashSet();

        var uncovered = handlerKeys.Except(produced).ToList();
        Assert.True(uncovered.Count == 0,
            "Registered Apply-able handler key(s) with NO golden-sample fixture producing a " +
            $"dispatchable action: {string.Join(", ", uncovered)}");
    }
}
