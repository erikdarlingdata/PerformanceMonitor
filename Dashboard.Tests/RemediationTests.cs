using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitorDashboard.Services.Remediation;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// PR-A coverage for the privileged "Apply Fix" core (no UI): the structured
/// extractor + render-stability, the self-gating handler against a faked executor
/// (fail-closed permission, audit-table hard block, freshness/skip dispositions,
/// per-target independence, gate re-derivation, applied-but-unlogged), un-apply
/// restriction, the registry, and the "no caller reaches the executor" guard.
/// The single-connection (R2-MOD-1) guarantee is an executor-level/real-server
/// concern (SPID equality) and cannot be proven against a faked executor.
/// </summary>
public class RemediationTests
{
    private static AnalysisFinding PlanRegressionFinding(List<object>? rows = null) => new()
    {
        ServerId = 1,
        ServerName = "TestServer",
        Category = "plan_regression",
        StoryPath = "PLAN_REGRESSION",
        StoryPathHash = "planreg000000001",
        RootFactKey = "PLAN_REGRESSION",
        DrillDown = new Dictionary<string, object>
        {
            ["regressed_queries"] = rows ?? new List<object>
            {
                new
                {
                    database = "AdventureWorks",
                    query_id = 4242L,
                    best_plan_id = 17L,
                    latest_plan_hash = "0xDEAD",
                    best_plan_hash = "0xBEEF",
                    latest_cpu_per_exec_us = 9000.0,
                    best_cpu_per_exec_us = 1200.0,
                    regression_factor = 7.5
                }
            }
        }
    };

    private static RemediationAction ForceAction(params ForcePlanTarget[] targets) =>
        new("PLAN_REGRESSION", "force", targets.ToList());

    private static ForcePlanTarget Target(string db = "AdventureWorks", long q = 4242, long p = 17) =>
        new(db, q, p);

    private static readonly RemediationIdentity Identity =
        new("TESTDOMAIN\\tester", "Analysis: plan_regression [abcd1234]");

    [Fact]
    public void Extract_SkipsRowsFailingGuards()
    {
        var finding = PlanRegressionFinding(new List<object>
        {
            new { database = "", query_id = 1L, best_plan_id = 1L },
            new { database = "Db", query_id = 0L, best_plan_id = 1L },
            new { database = "Db", query_id = 1L, best_plan_id = 0L },
            new { database = "Good", query_id = 5L, best_plan_id = 9L }
        });

        var targets = FactRemediation.ExtractPlanRegressionTargets(finding);

        var only = Assert.Single(targets);
        Assert.Equal("Good", only.Database);
        Assert.Equal(5, only.QueryId);
        Assert.Equal(9, only.PlanId);
    }

    [Fact]
    public void Extract_CapsAtFiveTargets()
    {
        var rows = Enumerable.Range(1, 8)
            .Select(i => (object)new { database = $"Db{i}", query_id = (long)i, best_plan_id = (long)(i + 100) })
            .ToList();

        var targets = FactRemediation.ExtractPlanRegressionTargets(PlanRegressionFinding(rows));

        Assert.Equal(5, targets.Count);
        Assert.Equal("Db1", targets[0].Database);
        Assert.Equal("Db5", targets[4].Database);
    }

    [Fact]
    public void BuildAction_PlanRegression_ProducesForceActionWithTargets()
    {
        var action = FactRemediation.BuildAction(PlanRegressionFinding());

        Assert.NotNull(action);
        Assert.Equal("PLAN_REGRESSION", action!.FactKey);
        Assert.Equal("force", action.Action);
        var t = Assert.Single(action.Targets);
        Assert.Equal("AdventureWorks", t.Database);
        Assert.Equal(4242, t.QueryId);
        Assert.Equal(17, t.PlanId);
        Assert.Equal(7.5, t.RegressionFactor);
    }

    [Fact]
    public void BuildAction_ParameterSensitivity_ReturnsNull()
    {
        var finding = PlanRegressionFinding();
        finding.RootFactKey = "PARAMETER_SENSITIVITY";
        Assert.Null(FactRemediation.BuildAction(finding));
    }

    [Fact]
    public void BuildAction_UnknownKeyOrNoTargets_ReturnsNull()
    {
        var unknown = PlanRegressionFinding();
        unknown.RootFactKey = "ZZZ_TEST";
        Assert.Null(FactRemediation.BuildAction(unknown));

        Assert.Null(FactRemediation.BuildAction(PlanRegressionFinding(new List<object>())));
    }

    [Fact]
    public void GenerateForFinding_RenderStability_ByteForByte()
    {
        const string expected =
            "-- Database: AdventureWorks\n" +
            "-- query_id = 4242, forcing plan_id = 17\n" +
            "--   latest plan hash: 0xDEAD (cpu/exec 9000 us)\n" +
            "--   best plan hash:   0xBEEF   (cpu/exec 1200 us)\n" +
            "--   regression factor: 7.5x\n" +
            "USE [AdventureWorks];\n" +
            "EXEC sys.sp_query_store_force_plan @query_id = 4242, @plan_id = 17;\n" +
            "\n" +
            "-- To back out:\n" +
            "-- EXEC sys.sp_query_store_unforce_plan @query_id = 4242, @plan_id = 17;";

        var actual = FactRemediation.GenerateForFinding(PlanRegressionFinding());

        Assert.NotNull(actual);
        Assert.Equal(expected, actual!.Replace("\r\n", "\n"));
    }

    [Fact]
    public void Registry_ResolvesForcePlanHandler_AndMissesUnknown()
    {
        var registry = new RemediationHandlerRegistry(new IRemediationHandler[] { new ForcePlanHandler() });

        Assert.IsType<ForcePlanHandler>(registry.TryGet("PLAN_REGRESSION"));
        Assert.Null(registry.TryGet("PARAMETER_SENSITIVITY"));
        Assert.Null(registry.TryGet(null));
    }

    [Fact]
    public async Task Apply_Success_ForcesAndWritesSuccessAudit()
    {
        var exec = new FakeExecutor();
        var result = await new ForcePlanHandler().ApplyAsync(ForceAction(Target()), exec, Identity, CancellationToken.None);

        var outcome = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Success, outcome.Status);
        Assert.True(outcome.AuditWritten);
        Assert.False(outcome.AppliedButUnlogged);
        Assert.Equal(1, exec.ForceCalls);

        var row = Assert.Single(exec.AuditRecords);
        Assert.Equal("force", row.Action);
        Assert.Equal("success", row.Result);
        Assert.Equal("TESTDOMAIN\\tester", row.OperatorIdentity);
        Assert.Contains("sp_query_store_force_plan", row.GeneratedSql);
    }

    [Fact]
    public async Task Apply_HasAlterZero_FailsClosed_NoElevation_AuditSkipped()
    {
        var exec = new FakeExecutor
        {
            ForceFunc = (db, q, p) => new ForcePlanOutcome
            {
                Database = db, QueryId = q, PlanId = p,
                Status = RemediationStatus.PermissionDenied, Forced = false, Message = "lacks ALTER"
            }
        };

        var result = await new ForcePlanHandler().ApplyAsync(ForceAction(Target()), exec, Identity, CancellationToken.None);

        var outcome = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.PermissionDenied, outcome.Status);
        Assert.False(outcome.AppliedButUnlogged);
        Assert.Equal("skipped", Assert.Single(exec.AuditRecords).Result);
        Assert.Equal(1, exec.ForceCalls);
    }

    [Fact]
    public async Task Apply_AuditTableAbsent_HardBlocks_NoMutation_NoAuditNoUnloggedWarning()
    {
        var exec = new FakeExecutor { AuditTableExists = false };

        var result = await new ForcePlanHandler().ApplyAsync(
            ForceAction(Target(), Target("Other", 9, 9)), exec, Identity, CancellationToken.None);

        Assert.Equal(2, result.Outcomes.Count);
        Assert.All(result.Outcomes, o =>
        {
            Assert.Equal(RemediationStatus.Blocked, o.Status);
            Assert.False(o.AuditWritten);
            Assert.False(o.AppliedButUnlogged);
            Assert.Contains("3.0.0", o.Message);
        });
        Assert.Equal(0, exec.ForceCalls);
        Assert.Empty(exec.AuditRecords);
    }

    [Fact]
    public async Task Apply_Skipped_IsNotForced_AndAuditedSkipped()
    {
        var exec = new FakeExecutor
        {
            ForceFunc = (db, q, p) => new ForcePlanOutcome
            {
                Database = db, QueryId = q, PlanId = p, Status = RemediationStatus.Skipped, Forced = false, Message = "already forced"
            }
        };

        var result = await new ForcePlanHandler().ApplyAsync(ForceAction(Target()), exec, Identity, CancellationToken.None);

        var outcome = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Skipped, outcome.Status);
        Assert.False(outcome.AppliedButUnlogged);
        Assert.Equal("skipped", Assert.Single(exec.AuditRecords).Result);
    }

    [Fact]
    public async Task Apply_GateChangesAfterPreflight_AbortsTarget()
    {
        var exec = new FakeExecutor
        {
            PreflightFunc = (db, q, p) => new TargetPreflight
            {
                Database = db, QueryId = q, PlanId = p,
                CurrentDatabase = db, HasAlter = true, QueryStoreState = "READ_WRITE",
                PlanPresent = true, IsForcedPlan = false, ForceFailureCount = 0
            },
            ForceFunc = (db, q, p) => new ForcePlanOutcome
            {
                Database = db, QueryId = q, PlanId = p, Status = RemediationStatus.Skipped, Forced = false, Message = "plan vanished"
            }
        };

        var handler = new ForcePlanHandler();
        var pre = await handler.PreflightAsync(ForceAction(Target()), exec, CancellationToken.None);
        Assert.Equal(RemediationDisposition.Ok, pre.Targets.Single().Disposition);

        var result = await handler.ApplyAsync(ForceAction(Target()), exec, Identity, CancellationToken.None);
        Assert.NotEqual(RemediationStatus.Success, result.Outcomes.Single().Status);
    }

    [Fact]
    public async Task Apply_OneTargetThrows_OthersStillRun()
    {
        var exec = new FakeExecutor
        {
            ForceFunc = (db, q, p) =>
            {
                if (q == 1) throw new InvalidOperationException("boom");
                return new ForcePlanOutcome { Database = db, QueryId = q, PlanId = p, Status = RemediationStatus.Success, Forced = true };
            }
        };

        var result = await new ForcePlanHandler().ApplyAsync(
            ForceAction(Target("A", 1, 1), Target("B", 2, 2)), exec, Identity, CancellationToken.None);

        Assert.Equal(2, result.Outcomes.Count);
        Assert.Equal(RemediationStatus.Error, result.Outcomes[0].Status);
        Assert.Equal(RemediationStatus.Success, result.Outcomes[1].Status);
    }

    [Fact]
    public async Task Apply_ForceSucceedsButAuditWriteFails_FlagsAppliedButUnlogged()
    {
        var exec = new FakeExecutor { AuditWriteResult = false };

        var result = await new ForcePlanHandler().ApplyAsync(ForceAction(Target()), exec, Identity, CancellationToken.None);

        var outcome = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Success, outcome.Status);
        Assert.False(outcome.AuditWritten);
        Assert.True(outcome.AppliedButUnlogged);
    }

    [Theory]
    [InlineData("Other", true, "READ_WRITE", true, false, 0, RemediationDisposition.BlockWrongDatabase)]
    [InlineData("AdventureWorks", false, "READ_WRITE", true, false, 0, RemediationDisposition.BlockNoAlter)]
    [InlineData("AdventureWorks", true, "READ_ONLY", true, false, 0, RemediationDisposition.BlockQueryStoreOff)]
    [InlineData("AdventureWorks", true, "READ_WRITE", false, false, 0, RemediationDisposition.BlockStale)]
    [InlineData("AdventureWorks", true, "READ_WRITE", true, true, 0, RemediationDisposition.AlreadyForced)]
    [InlineData("AdventureWorks", true, "READ_WRITE", true, false, 3, RemediationDisposition.WarnFailing)]
    [InlineData("AdventureWorks", true, "READ_WRITE", true, false, 0, RemediationDisposition.Ok)]
    public async Task Preflight_ClassifiesDisposition(
        string currentDb, bool hasAlter, string qsState, bool planPresent, bool isForced, long failCount, RemediationDisposition expected)
    {
        var exec = new FakeExecutor
        {
            PreflightFunc = (db, q, p) => new TargetPreflight
            {
                Database = db, QueryId = q, PlanId = p,
                CurrentDatabase = currentDb, HasAlter = hasAlter, QueryStoreState = qsState,
                PlanPresent = planPresent, IsForcedPlan = isForced, ForceFailureCount = failCount
            }
        };

        var pre = await new ForcePlanHandler().PreflightAsync(ForceAction(Target()), exec, CancellationToken.None);
        Assert.Equal(expected, pre.Targets.Single().Disposition);
    }

    [Fact]
    public async Task Preflight_AuditTableAbsent_OverridesPerTargetDisposition()
    {
        var exec = new FakeExecutor
        {
            AuditTableExists = false,
            PreflightFunc = (db, q, p) => new TargetPreflight
            {
                Database = db, QueryId = q, PlanId = p,
                CurrentDatabase = db, HasAlter = true, QueryStoreState = "READ_WRITE", PlanPresent = true
            }
        };

        var pre = await new ForcePlanHandler().PreflightAsync(ForceAction(Target()), exec, CancellationToken.None);
        Assert.False(pre.AuditTableExists);
        Assert.Equal(RemediationDisposition.BlockAuditTableAbsent, pre.Targets.Single().Disposition);
    }

    [Fact]
    public async Task Unapply_NoPriorForce_Skips_NoUnforce()
    {
        var exec = new FakeExecutor { PriorForce = false };

        var result = await new ForcePlanHandler().UnapplyAsync(ForceAction(Target()), exec, Identity, CancellationToken.None);

        Assert.Equal(RemediationStatus.Skipped, result.Outcomes.Single().Status);
        Assert.Equal(0, exec.UnforceCalls);
    }

    [Fact]
    public async Task Unapply_WithPriorForce_Unforces_AuditsUnforce()
    {
        var exec = new FakeExecutor { PriorForce = true };

        var result = await new ForcePlanHandler().UnapplyAsync(ForceAction(Target()), exec, Identity, CancellationToken.None);

        Assert.Equal(RemediationStatus.Success, result.Outcomes.Single().Status);
        Assert.Equal(1, exec.UnforceCalls);
        Assert.Equal("unforce", Assert.Single(exec.AuditRecords).Action);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // B3 Phase 2 — always-safe DB-config fixes (DB_CONFIG)
    // ════════════════════════════════════════════════════════════════════════════

    private static AnalysisFinding DbConfigFinding(List<object>? rows = null) => new()
    {
        ServerId = 1,
        ServerName = "TestServer",
        Category = "config_issues",
        StoryPath = "DB_CONFIG",
        StoryPathHash = "dbconfig00000001",
        RootFactKey = "DB_CONFIG",
        DrillDown = new Dictionary<string, object>
        {
            ["config_issues"] = rows ?? new List<object>
            {
                new { database = "Foo", recovery_model = "FULL", rcsi = true, query_store = true,
                      issues = new[] { "auto_shrink ON", "auto_close ON" },
                      auto_shrink = true, auto_close = true, page_verify = "CHECKSUM" }
            }
        }
    };

    private static RemediationAction DbConfigAction(params DbConfigTarget[] targets) =>
        new("DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(), targets.ToList());

    // ── Extractor: only the three safe settings, NEVER RCSI ──────────────────────

    [Fact]
    public void ExtractDbConfig_EmitsOnlySafeSettings_NeverRcsi()
    {
        var finding = DbConfigFinding(new List<object>
        {
            new { database = "Db1", rcsi = false, query_store = false,
                  auto_shrink = true, auto_close = true, page_verify = "NONE",
                  issues = new[] { "auto_shrink ON", "auto_close ON", "RCSI OFF", "page_verify=NONE" } }
        });

        var targets = FactRemediation.ExtractDbConfigTargets(finding);

        // 3 safe targets (shrink, close, page_verify) — RCSI is NEVER emitted.
        Assert.Equal(3, targets.Count);
        Assert.Contains(targets, t => t.Setting == DbConfigSetting.AutoShrinkOff && t.CurrentValue == "ON");
        Assert.Contains(targets, t => t.Setting == DbConfigSetting.AutoCloseOff && t.CurrentValue == "ON");
        Assert.Contains(targets, t => t.Setting == DbConfigSetting.PageVerifyChecksum && t.CurrentValue == "NONE");
        // No enum value for RCSI exists, but assert defensively none slipped through as page_verify.
        Assert.All(targets, t => Assert.True(Enum.IsDefined(typeof(DbConfigSetting), t.Setting)));
    }

    [Fact]
    public void ExtractDbConfig_PageVerifyChecksum_IsNotEmitted()
    {
        var finding = DbConfigFinding(new List<object>
        {
            new { database = "Db1", rcsi = true, query_store = true,
                  auto_shrink = false, auto_close = false, page_verify = "CHECKSUM",
                  issues = new string[0] }
        });

        Assert.Empty(FactRemediation.ExtractDbConfigTargets(finding));
    }

    [Fact]
    public void ExtractDbConfig_EmptyDatabase_IsSkipped()
    {
        var finding = DbConfigFinding(new List<object>
        {
            new { database = "", rcsi = true, query_store = true, auto_shrink = true, auto_close = false, page_verify = "CHECKSUM", issues = new string[0] }
        });

        Assert.Empty(FactRemediation.ExtractDbConfigTargets(finding));
    }

    [Fact]
    public void ExtractDbConfig_MultipleDatabases_MultipleTargets()
    {
        var finding = DbConfigFinding(new List<object>
        {
            new { database = "A", rcsi = true, query_store = true, auto_shrink = true,  auto_close = false, page_verify = "CHECKSUM", issues = new string[0] },
            new { database = "B", rcsi = true, query_store = true, auto_shrink = false, auto_close = false, page_verify = "TORN_PAGE_DETECTION", issues = new string[0] }
        });

        var targets = FactRemediation.ExtractDbConfigTargets(finding);
        Assert.Equal(2, targets.Count);
        Assert.Equal("A", targets[0].Database);
        Assert.Equal(DbConfigSetting.AutoShrinkOff, targets[0].Setting);
        Assert.Equal("B", targets[1].Database);
        Assert.Equal(DbConfigSetting.PageVerifyChecksum, targets[1].Setting);
        Assert.Equal("TORN_PAGE_DETECTION", targets[1].CurrentValue);
    }

    [Fact]
    public void BuildAction_DbConfig_OnlyRcsi_ReturnsNull()
    {
        // A DB whose only issue is RCSI OFF yields no safe target → no action → no Apply button.
        var finding = DbConfigFinding(new List<object>
        {
            new { database = "Db1", rcsi = false, query_store = true,
                  auto_shrink = false, auto_close = false, page_verify = "CHECKSUM",
                  issues = new[] { "RCSI OFF" } }
        });

        Assert.Null(FactRemediation.BuildAction(finding));
    }

    [Fact]
    public void BuildAction_DbConfig_ProducesSetActionWithTargets()
    {
        var action = FactRemediation.BuildAction(DbConfigFinding());

        Assert.NotNull(action);
        Assert.Equal("DB_CONFIG", action!.FactKey);
        Assert.Equal("set", action.Action);
        Assert.Empty(action.Targets);                       // force-plan list is empty for DB_CONFIG
        Assert.NotNull(action.DbConfigTargets);
        Assert.Equal(2, action.DbConfigTargets!.Count);     // shrink + close
    }

    // ── Render-stability golden (incl. "was X" + RCSI-excluded note) ─────────────

    [Fact]
    public void GenerateForFinding_DbConfig_RenderStability_ByteForByte()
    {
        var finding = DbConfigFinding(new List<object>
        {
            new { database = "Foo", rcsi = false, query_store = true,
                  auto_shrink = true, auto_close = true, page_verify = "NONE",
                  issues = new[] { "auto_shrink ON", "auto_close ON", "RCSI OFF", "page_verify=NONE" } }
        });

        const string expected =
            "-- Database: Foo\n" +
            "ALTER DATABASE [Foo] SET AUTO_SHRINK OFF;   -- was ON\n" +
            "ALTER DATABASE [Foo] SET AUTO_CLOSE OFF;   -- was ON\n" +
            "ALTER DATABASE [Foo] SET PAGE_VERIFY CHECKSUM;   -- was NONE\n" +
            "-- NOTE: [Foo] also has RCSI OFF — intentionally NOT auto-fixed (test on a copy first).";

        var actual = FactRemediation.GenerateForFinding(finding);
        Assert.NotNull(actual);
        Assert.Equal(expected, actual!.Replace("\r\n", "\n"));
    }

    // ── Handler: audit-absent hard block, fail-closed, freshness-skip, db-not-found, success ──

    [Fact]
    public async Task DbConfig_AuditTableAbsent_HardBlocks_NoMutation_NoAudit()
    {
        var exec = new FakeExecutor { AuditTableExists = false };

        var result = await new DbConfigHandler().ApplyAsync(
            DbConfigAction(new DbConfigTarget("Foo", DbConfigSetting.AutoShrinkOff, "ON"),
                           new DbConfigTarget("Bar", DbConfigSetting.AutoCloseOff, "ON")),
            exec, Identity, CancellationToken.None);

        Assert.Equal(2, result.Outcomes.Count);
        Assert.All(result.Outcomes, o =>
        {
            Assert.Equal(RemediationStatus.Blocked, o.Status);
            Assert.False(o.AuditWritten);
            Assert.Contains("3.0.0", o.Message);
        });
        Assert.Equal(0, exec.SetDbCalls);
        Assert.Empty(exec.AuditRecords);
    }

    [Fact]
    public async Task DbConfig_PermissionDenied_FailsClosed_AuditSkipped()
    {
        var exec = new FakeExecutor
        {
            SetDbFunc = (db, s) => new DbConfigOutcome
            {
                Database = db, Setting = s, Status = RemediationStatus.PermissionDenied, Applied = false,
                Message = "lacks ALTER", PriorValue = "ON"
            }
        };

        var result = await new DbConfigHandler().ApplyAsync(
            DbConfigAction(new DbConfigTarget("Foo", DbConfigSetting.AutoShrinkOff, "ON")), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.PermissionDenied, o.Status);
        Assert.False(o.AppliedButUnlogged);
        Assert.Equal("skipped", Assert.Single(exec.AuditRecords).Result);
        Assert.Equal(1, exec.SetDbCalls);
    }

    [Fact]
    public async Task DbConfig_AlreadyDesired_Skips_NoMutationFlag_AuditSkipped()
    {
        var exec = new FakeExecutor
        {
            SetDbFunc = (db, s) => new DbConfigOutcome
            {
                Database = db, Setting = s, Status = RemediationStatus.Skipped, Applied = false,
                Message = "already desired", PriorValue = "OFF"
            }
        };

        var result = await new DbConfigHandler().ApplyAsync(
            DbConfigAction(new DbConfigTarget("Foo", DbConfigSetting.AutoShrinkOff, "OFF")), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Skipped, o.Status);
        Assert.False(o.AppliedButUnlogged);
        Assert.Equal("skipped", Assert.Single(exec.AuditRecords).Result);
    }

    [Fact]
    public async Task DbConfig_DatabaseNotFound_Blocks_AuditAborted()
    {
        var exec = new FakeExecutor
        {
            SetDbFunc = (db, s) => new DbConfigOutcome
            {
                Database = db, Setting = s, Status = RemediationStatus.Blocked, Applied = false,
                Message = "not found", PriorValue = null
            }
        };

        var result = await new DbConfigHandler().ApplyAsync(
            DbConfigAction(new DbConfigTarget("Gone", DbConfigSetting.AutoShrinkOff, "ON")), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Blocked, o.Status);
        Assert.Equal("aborted", Assert.Single(exec.AuditRecords).Result);
    }

    [Fact]
    public async Task DbConfig_Success_AppliesAndAudits_NullIds_PriorValue_PreciseAction()
    {
        var exec = new FakeExecutor
        {
            SetDbFunc = (db, s) => new DbConfigOutcome
            {
                Database = db, Setting = s, Status = RemediationStatus.Success, Applied = true,
                ExecutingLogin = "sa", PriorValue = "ON",
                GeneratedSql = "ALTER DATABASE [Foo] SET AUTO_SHRINK OFF;", GateSpid = 7, ExecSpid = 7
            }
        };

        var result = await new DbConfigHandler().ApplyAsync(
            DbConfigAction(new DbConfigTarget("Foo", DbConfigSetting.AutoShrinkOff, "ON")), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Success, o.Status);
        Assert.True(o.AuditWritten);
        Assert.False(o.AppliedButUnlogged);

        var row = Assert.Single(exec.AuditRecords);
        Assert.Equal("DB_CONFIG", row.FactKey);
        Assert.Equal("set_auto_shrink_off", row.Action);     // precise taxonomy (24/19/18 chars)
        Assert.Equal("success", row.Result);
        Assert.Null(row.QueryId);                            // B-1: DB_CONFIG rows have no IDs
        Assert.Null(row.PlanId);
        Assert.Equal("ON", row.PriorValue);
        Assert.Contains("ALTER DATABASE", row.GeneratedSql);
    }

    [Fact]
    public async Task DbConfig_AppliesButAuditFails_FlagsAppliedButUnlogged()
    {
        var exec = new FakeExecutor { AuditWriteResult = false };

        var result = await new DbConfigHandler().ApplyAsync(
            DbConfigAction(new DbConfigTarget("Foo", DbConfigSetting.AutoShrinkOff, "ON")), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Success, o.Status);
        Assert.False(o.AuditWritten);
        Assert.True(o.AppliedButUnlogged);
    }

    [Fact]
    public async Task DbConfig_OneTargetThrows_OthersStillRun()
    {
        var exec = new FakeExecutor
        {
            SetDbFunc = (db, s) =>
            {
                if (db == "A") throw new InvalidOperationException("boom");
                return new DbConfigOutcome { Database = db, Setting = s, Status = RemediationStatus.Success, Applied = true, PriorValue = "ON" };
            }
        };

        var result = await new DbConfigHandler().ApplyAsync(
            DbConfigAction(new DbConfigTarget("A", DbConfigSetting.AutoShrinkOff, "ON"),
                           new DbConfigTarget("B", DbConfigSetting.AutoCloseOff, "ON")),
            exec, Identity, CancellationToken.None);

        Assert.Equal(2, result.Outcomes.Count);
        Assert.Equal(RemediationStatus.Error, result.Outcomes[0].Status);
        Assert.Equal(RemediationStatus.Success, result.Outcomes[1].Status);
    }

    [Fact]
    public async Task DbConfig_PreciseTaxonomy_AllThreeSettings()
    {
        var exec = new FakeExecutor();
        await new DbConfigHandler().ApplyAsync(
            DbConfigAction(
                new DbConfigTarget("Foo", DbConfigSetting.AutoShrinkOff, "ON"),
                new DbConfigTarget("Foo", DbConfigSetting.AutoCloseOff, "ON"),
                new DbConfigTarget("Foo", DbConfigSetting.PageVerifyChecksum, "NONE")),
            exec, Identity, CancellationToken.None);

        var actions = exec.AuditRecords.Select(r => r.Action).ToList();
        Assert.Contains("set_auto_shrink_off", actions);
        Assert.Contains("set_auto_close_off", actions);
        Assert.Contains("set_page_verify_checksum", actions);     // 24 chars — needs varchar(32) + param(32)
    }

    // ── Preflight dispositions ───────────────────────────────────────────────────

    [Theory]
    [InlineData(false, true, false, RemediationDisposition.BlockDatabaseNotFound)]
    [InlineData(true, false, false, RemediationDisposition.BlockNoAlter)]
    [InlineData(true, true, true, RemediationDisposition.AlreadyInDesiredState)]
    [InlineData(true, true, false, RemediationDisposition.Ok)]
    public async Task DbConfig_Preflight_ClassifiesDisposition(bool exists, bool hasAlter, bool alreadyDesired, RemediationDisposition expected)
    {
        var exec = new FakeExecutor
        {
            DbPreflightFunc = (db, s) => new DbConfigPreflight
            {
                Database = db, Setting = s, DatabaseExists = exists, HasAlter = hasAlter,
                AlreadyInDesiredState = alreadyDesired, CurrentValue = "ON"
            }
        };

        var pre = await new DbConfigHandler().PreflightAsync(
            DbConfigAction(new DbConfigTarget("Foo", DbConfigSetting.AutoShrinkOff, "ON")), exec, CancellationToken.None);
        Assert.Equal(expected, pre.Targets.Single().Disposition);
    }

    [Fact]
    public async Task DbConfig_Preflight_AuditTableAbsent_OverridesDisposition()
    {
        var exec = new FakeExecutor { AuditTableExists = false };

        var pre = await new DbConfigHandler().PreflightAsync(
            DbConfigAction(new DbConfigTarget("Foo", DbConfigSetting.AutoShrinkOff, "ON")), exec, CancellationToken.None);

        Assert.False(pre.AuditTableExists);
        Assert.Equal(RemediationDisposition.BlockAuditTableAbsent, pre.Targets.Single().Disposition);
    }

    // ── Apply-only enforcement (m-1) + SupportsUnapply ───────────────────────────

    [Fact]
    public void DbConfig_SupportsUnapply_IsFalse_NotDestructive()
    {
        var handler = new DbConfigHandler();
        Assert.False(handler.SupportsUnapply);
        Assert.False(handler.IsDestructive);
    }

    [Fact]
    public async Task DbConfig_UnapplyAsync_Throws()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            new DbConfigHandler().UnapplyAsync(
                DbConfigAction(new DbConfigTarget("Foo", DbConfigSetting.AutoShrinkOff, "ON")), new FakeExecutor(), Identity, CancellationToken.None));
    }

    [Fact]
    public void CoreMachineryMarkers_CoverNewPrivilegedSurface()
    {
        // M-2: the reachability guard must list the new handler + executor methods,
        // or a future UI/MCP file referencing them would silently pass the guard.
        Assert.Contains("DbConfigHandler", CoreMachineryMarkers);
        Assert.Contains("SetDatabaseOptionAsync", CoreMachineryMarkers);
        Assert.Contains("PreflightDbConfigAsync", CoreMachineryMarkers);
    }

    [Fact]
    public void Registry_ResolvesDbConfigHandler()
    {
        var registry = new RemediationHandlerRegistry(new IRemediationHandler[] { new ForcePlanHandler(), new DbConfigHandler() });
        Assert.IsType<DbConfigHandler>(registry.TryGet("DB_CONFIG"));
        Assert.IsType<ForcePlanHandler>(registry.TryGet("PLAN_REGRESSION"));
    }

    [Fact]
    public void Rcsi_Handler_IsRegistered_AndReachableThroughGate()
    {
        // PR-B makes RCSI LIVE: the PRODUCTION wiring registers RcsiHandler so the Apply
        // affordance can appear and route to the destructive handler — but ONLY through
        // the gated RemediationApplyService facade (proven by the Gate_* behavioural tests
        // and the reachability guards CoreMachinery_OnlyReferencedInRemediationCore +
        // GatedEntry_ReferencedOnlyBySanctionedUiPath).
        var productionRegistry = new RemediationHandlerRegistry(
            new IRemediationHandler[] { new ForcePlanHandler(), new DbConfigHandler(), new RcsiHandler() });
        Assert.IsType<RcsiHandler>(productionRegistry.TryGet("RCSI"));

        // Assert at the source level that the PRODUCTION wiring now constructs RcsiHandler.
        var dir = FindDashboardSourceDir();
        var serviceSrc = File.ReadAllText(Path.Combine(dir, "Services", "Remediation", "RemediationApplyService.cs"));
        Assert.Contains("new RcsiHandler()", serviceSrc);

        // The always-safe DbConfigHandler still routes ONLY DB_CONFIG — never RCSI — so
        // the two affordances can never cross at the registry/handler layer.
        Assert.IsType<DbConfigHandler>(productionRegistry.TryGet("DB_CONFIG"));
        Assert.NotEqual("RCSI", productionRegistry.TryGet("DB_CONFIG")!.FactKey);
    }

    [Fact]
    public void CoreMachineryMarkers_Include_RcsiHandler()
    {
        // m-3: the destructive handler must be in the reachability guard.
        Assert.Contains("RcsiHandler", CoreMachineryMarkers);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // B3 PHASE 3 — RCSI destructive-consent privileged core (PR-A; UNREGISTERED)
    // ════════════════════════════════════════════════════════════════════════════

    /// <summary>A DB_CONFIG finding with RCSI OFF + the §3.3 enrichment present.</summary>
    private static AnalysisFinding RcsiOffFinding(int blocking = 12, int deadlocks = 3, int? rwPct = 80, string db = "Foo") => new()
    {
        ServerId = 1,
        ServerName = "TestServer",
        Category = "config_issues",
        StoryPath = "DB_CONFIG",
        StoryPathHash = "dbconfig00000099",
        RootFactKey = "DB_CONFIG",
        DrillDown = new Dictionary<string, object>
        {
            ["config_issues"] = new List<object>
            {
                new { database = db, recovery_model = "FULL", rcsi = false, query_store = true,
                      issues = new[] { "RCSI OFF" },
                      auto_shrink = false, auto_close = false, page_verify = "CHECKSUM",
                      rcsi_blocking_events = blocking, rcsi_deadlocks = deadlocks,
                      rcsi_reader_writer_pct = rwPct }
            }
        }
    };

    private static RemediationAction RcsiAction(string db = "Foo") =>
        new("RCSI", "set", Array.Empty<ForcePlanTarget>(), new[] { new DbConfigTarget(db, DbConfigSetting.ReadCommittedSnapshotOn, "OFF") });

    // ── RcsiHandler invariants ───────────────────────────────────────────────────

    [Fact]
    public void Rcsi_Handler_IsDestructive_And_ApplyOnly()
    {
        var handler = new RcsiHandler();
        Assert.Equal("RCSI", handler.FactKey);
        Assert.True(handler.IsDestructive);     // the first (and only) true in the codebase
        Assert.False(handler.SupportsUnapply);
    }

    [Fact]
    public async Task Rcsi_UnapplyAsync_Throws()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            new RcsiHandler().UnapplyAsync(RcsiAction(), new FakeExecutor(), Identity, CancellationToken.None));
    }

    // ── Executor RCSI arm: own builder, no-injection, same-string (against the
    //    executor's OWN BuildAlterStatement — not the display renderer) ───────────

    [Fact]
    public void Rcsi_Build_SetClause_IsHardcodedLiteral_RegardlessOfName()
    {
        foreach (var name in new[] { "Db", "Db]; DROP TABLE x--", "[weird]", "Ünîçödé" })
        {
            var stmt = DatabaseService.BuildAlterStatement(name, DbConfigSetting.ReadCommittedSnapshotOn);
            Assert.EndsWith("SET READ_COMMITTED_SNAPSHOT ON;", stmt);
        }
    }

    [Theory]
    [InlineData("normal")]
    [InlineData("has]bracket")]
    [InlineData("has]]double")]
    [InlineData("ev;il-- GO DROP")]
    [InlineData("'; ALTER DATABASE master SET")]
    public void Rcsi_Build_BracketsIdentifierInert_NoInjection(string name)
    {
        var stmt = DatabaseService.BuildAlterStatement(name, DbConfigSetting.ReadCommittedSnapshotOn);
        var expectedToken = "[" + name.Replace("]", "]]") + "]";
        Assert.Equal($"ALTER DATABASE {expectedToken} SET READ_COMMITTED_SNAPSHOT ON;", stmt);
    }

    [Fact]
    public void Rcsi_Build_SameString_ValidatedNameIsBracketedExactly()
    {
        const string validated = "  Padded Name  ";
        var stmt = DatabaseService.BuildAlterStatement(validated, DbConfigSetting.ReadCommittedSnapshotOn);
        Assert.Equal("ALTER DATABASE [  Padded Name  ] SET READ_COMMITTED_SNAPSHOT ON;", stmt);
        Assert.NotEqual(stmt, DatabaseService.BuildAlterStatement(validated.TrimEnd(), DbConfigSetting.ReadCommittedSnapshotOn));
    }

    [Fact]
    public void Rcsi_SetClause_ExecutorAndService_AreReadCommittedSnapshotOn()
    {
        // m-1: all clause-builder copies render the RCSI arm (executor's own builder).
        Assert.EndsWith("SET READ_COMMITTED_SNAPSHOT ON;",
            DatabaseService.BuildAlterStatement("X", DbConfigSetting.ReadCommittedSnapshotOn));
    }

    [Fact]
    public void Rcsi_SettingTitle_IsFriendly_NotRawEnumName()
    {
        // m-2: a RCSI confirm row must not render "ReadCommittedSnapshotOn".
        Assert.Equal("Read Committed Snapshot Isolation",
            DbConfigHandler.SettingTitle(DbConfigSetting.ReadCommittedSnapshotOn));
    }

    // ── RcsiHandler apply behaviours against the faked executor ──────────────────

    [Fact]
    public async Task Rcsi_AuditTableAbsent_HardBlocks_NoMutation_NoAudit()
    {
        var exec = new FakeExecutor { AuditTableExists = false };
        var result = await new RcsiHandler().ApplyAsync(RcsiAction(), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Blocked, o.Status);
        Assert.False(o.AuditWritten);
        Assert.Equal(0, exec.SetDbCalls);
        Assert.Empty(exec.AuditRecords);
    }

    [Fact]
    public async Task Rcsi_PermissionDenied_FailsClosed_NoElevation()
    {
        var exec = new FakeExecutor
        {
            SetDbFunc = (db, s) => new DbConfigOutcome
            {
                Database = db, Setting = s, Status = RemediationStatus.PermissionDenied, Applied = false,
                Message = "lacks ALTER", PriorValue = "OFF"
            }
        };
        var result = await new RcsiHandler().ApplyAsync(RcsiAction(), exec, Identity, CancellationToken.None);

        Assert.Equal(RemediationStatus.PermissionDenied, Assert.Single(result.Outcomes).Status);
        Assert.Equal(1, exec.SetDbCalls);
    }

    [Fact]
    public async Task Rcsi_AlreadyOn_Skips_ConsentAcknowledgedStillTrue()
    {
        var exec = new FakeExecutor
        {
            SetDbFunc = (db, s) => new DbConfigOutcome
            {
                Database = db, Setting = s, Status = RemediationStatus.Skipped, Applied = false,
                Message = "already on", PriorValue = "ON", GateSpid = 7, ExecSpid = 7
            }
        };
        var result = await new RcsiHandler().ApplyAsync(RcsiAction(), exec, Identity, CancellationToken.None);

        Assert.Equal(RemediationStatus.Skipped, Assert.Single(result.Outcomes).Status);
        var rec = Assert.Single(exec.AuditRecords);
        Assert.Equal("skipped", rec.Result);
        Assert.True(rec.ConsentAcknowledged);     // gate was satisfied to reach apply
        Assert.Equal("RCSI", rec.FactKey);
        Assert.Equal("set_rcsi_on", rec.Action);
    }

    [Fact]
    public async Task Rcsi_Success_WritesAudit_ConsentAcknowledgedTrue_PriorValueOff()
    {
        var exec = new FakeExecutor
        {
            SetDbFunc = (db, s) => new DbConfigOutcome
            {
                Database = db, Setting = s, Status = RemediationStatus.Success, Applied = true,
                ExecutingLogin = "sa", PriorValue = "OFF",
                GeneratedSql = "ALTER DATABASE [Foo] SET READ_COMMITTED_SNAPSHOT ON;", GateSpid = 9, ExecSpid = 9
            }
        };
        var result = await new RcsiHandler().ApplyAsync(RcsiAction(), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Success, o.Status);
        Assert.True(o.AuditWritten);
        var rec = Assert.Single(exec.AuditRecords);
        Assert.True(rec.ConsentAcknowledged);
        Assert.Equal("OFF", rec.PriorValue);
        Assert.Equal("success", rec.Result);
    }

    [Fact]
    public async Task DbConfig_And_ForcePlan_WriteConsentAcknowledgedFalse()
    {
        // Regression-guard: the non-destructive paths must persist consent_acknowledged = 0.
        var exec = new FakeExecutor();
        await new DbConfigHandler().ApplyAsync(
            DbConfigAction(new DbConfigTarget("Foo", DbConfigSetting.AutoShrinkOff, "ON")), exec, Identity, CancellationToken.None);
        Assert.All(exec.AuditRecords, r => Assert.False(r.ConsentAcknowledged));

        var exec2 = new FakeExecutor();
        await new ForcePlanHandler().ApplyAsync(
            new RemediationAction("PLAN_REGRESSION", "force", new List<ForcePlanTarget> { Target() }),
            exec2, Identity, CancellationToken.None);
        Assert.All(exec2.AuditRecords, r => Assert.False(r.ConsentAcknowledged));
    }

    // ── BuildRcsiAction (M2-1 boolean polarity) + BuildAction regression-guard ───

    [Fact]
    public void BuildRcsiAction_EmitsOnRcsiOff_WithEnrichment()
    {
        var action = FactRemediation.BuildRcsiAction(RcsiOffFinding());
        Assert.NotNull(action);
        Assert.Equal("RCSI", action!.FactKey);
        Assert.Equal("set", action.Action);
        Assert.Empty(action.Targets);
        var t = Assert.Single(action.DbConfigTargets!);
        Assert.Equal(DbConfigSetting.ReadCommittedSnapshotOn, t.Setting);
        Assert.Equal("OFF", t.CurrentValue);
        Assert.Equal("Foo", t.Database);
    }

    [Fact]
    public void BuildRcsiAction_ReturnsNull_WhenRcsiOn()
    {
        // M2-1: rcsi == true means RCSI is ON — the affordance must NOT appear.
        var finding = DbConfigFinding(new List<object>
        {
            new { database = "Foo", rcsi = true, query_store = true,
                  auto_shrink = false, auto_close = false, page_verify = "CHECKSUM",
                  issues = new string[0],
                  rcsi_blocking_events = 99, rcsi_deadlocks = 9, rcsi_reader_writer_pct = 90 }
        });
        Assert.Null(FactRemediation.BuildRcsiAction(finding));
    }

    [Fact]
    public void BuildRcsiAction_ReturnsNull_WhenEnrichmentAbsent()
    {
        // RCSI off but no §3.3 enrichment (legacy alert) → no affordance.
        var finding = DbConfigFinding(new List<object>
        {
            new { database = "Foo", rcsi = false, query_store = true,
                  auto_shrink = false, auto_close = false, page_verify = "CHECKSUM",
                  issues = new[] { "RCSI OFF" } }
        });
        Assert.Null(FactRemediation.BuildRcsiAction(finding));
    }

    [Fact]
    public void BuildAction_NeverPutsRcsiInSafeTargets_ButCarriesRcsiTargets()
    {
        // The always-safe BuildAction must STILL never put RCSI in the EXECUTED DbConfigTargets
        // (DbConfigHandler runs those). RCSI is now CARRIED on the action's RcsiTargets purely so
        // the Recommendations reader can fan per-db RCSI cards — never executed from here. On an
        // enriched, CONTENDED RCSI-off finding the action exists (so the reader can fan), has NO
        // safe DbConfigTargets, and carries exactly the one RCSI target.
        var rcsiOnly = FactRemediation.BuildAction(RcsiOffFinding());
        Assert.NotNull(rcsiOnly);
        Assert.Equal("DB_CONFIG", rcsiOnly!.FactKey);
        Assert.True(rcsiOnly.DbConfigTargets is null || rcsiOnly.DbConfigTargets.Count == 0);
        var carried = Assert.Single(rcsiOnly.RcsiTargets!);
        Assert.Equal("Foo", carried.Database);
        Assert.Equal(12, carried.Figures.BlockingEvents);

        // And a mixed finding (RCSI off + a safe setting) yields the safe target in
        // DbConfigTargets (never RCSI) AND carries the RCSI target separately.
        var mixed = DbConfigFinding(new List<object>
        {
            new { database = "Foo", rcsi = false, query_store = true,
                  auto_shrink = true, auto_close = false, page_verify = "CHECKSUM",
                  issues = new[] { "auto_shrink ON", "RCSI OFF" },
                  rcsi_blocking_events = 1, rcsi_deadlocks = 0, rcsi_reader_writer_pct = 50 }
        });
        var safe = FactRemediation.BuildAction(mixed);
        Assert.NotNull(safe);
        Assert.Equal("DB_CONFIG", safe!.FactKey);
        Assert.NotEmpty(safe.DbConfigTargets!);
        Assert.All(safe.DbConfigTargets!, t => Assert.NotEqual(DbConfigSetting.ReadCommittedSnapshotOn, t.Setting));
        Assert.Single(safe.RcsiTargets!);   // RCSI carried for the card fan-out, not executed
    }

    [Fact]
    public void CollectRcsiTargets_OnlyReaderWriterContention_WriterWriterAndUnknownExcluded()
    {
        // RCSI only relieves reader-vs-writer blocking. The gate is the reader/writer SHARE
        // (>= FactRiskDisclosure.ReaderWriterMeaningfulPct), NOT raw blocking/deadlock counts:
        //  - reader/writer-dominant    -> recommended
        //  - writer/writer-dominant    -> NOT recommended, even with HEAVY blocking (RCSI does
        //                                 nothing for X/IX/U vs X/IX/U contention)
        //  - no blocked-process detail -> NOT recommended (pct null — can't confirm RCSI helps)
        var finding = DbConfigFinding(new List<object>
        {
            new { database = "ReaderWriter", rcsi = false, query_store = true,
                  auto_shrink = false, auto_close = false, page_verify = "CHECKSUM",
                  issues = new[] { "RCSI OFF" },
                  rcsi_blocking_events = 40, rcsi_deadlocks = 2, rcsi_reader_writer_pct = 85 },
            new { database = "WriterWriter", rcsi = false, query_store = true,
                  auto_shrink = false, auto_close = false, page_verify = "CHECKSUM",
                  issues = new[] { "RCSI OFF" },
                  // Heavy blocking + deadlocks, but almost all writer/writer — RCSI won't relieve it.
                  rcsi_blocking_events = 500, rcsi_deadlocks = 12, rcsi_reader_writer_pct = 15 },
            new { database = "Unknown", rcsi = false, query_store = true,
                  auto_shrink = false, auto_close = false, page_verify = "CHECKSUM",
                  issues = new[] { "RCSI OFF" },
                  rcsi_blocking_events = 30, rcsi_deadlocks = 0, rcsi_reader_writer_pct = (int?)null },
            new { database = "AtThreshold", rcsi = false, query_store = true,
                  auto_shrink = false, auto_close = false, page_verify = "CHECKSUM",
                  issues = new[] { "RCSI OFF" },
                  rcsi_blocking_events = 10, rcsi_deadlocks = 0, rcsi_reader_writer_pct = 50 }
        });

        var collected = FactRemediation.CollectRcsiTargets(finding);

        Assert.Equal(2, collected.Count);
        Assert.DoesNotContain(collected, t => t.Database == "WriterWriter"); // heavy blocking, but writer/writer
        Assert.DoesNotContain(collected, t => t.Database == "Unknown");      // no reader/writer detail captured
        var rw = Assert.Single(collected, t => t.Database == "ReaderWriter");
        Assert.Equal(40, rw.Figures.BlockingEvents);
        Assert.Equal(85, rw.Figures.ReaderWriterPct);
        Assert.Single(collected, t => t.Database == "AtThreshold");          // pct == threshold qualifies

        // BuildAction carries exactly the two reader/writer targets (no safe DbConfigTargets here).
        var action = FactRemediation.BuildAction(finding);
        Assert.NotNull(action);
        Assert.Equal(2, action!.RcsiTargets!.Count);
        Assert.True(action.DbConfigTargets is null || action.DbConfigTargets.Count == 0);
    }

    // ── FactRiskDisclosure: two-sided, honest-both-directions, golden prose ──────

    [Fact]
    public void RiskDisclosure_Null_ForNonDestructiveFactKey()
    {
        Assert.Null(FactRiskDisclosure.GetForAction(DbConfigAction(new DbConfigTarget("Foo", DbConfigSetting.AutoShrinkOff, "ON")), DbConfigFinding()));
        Assert.Null(FactRiskDisclosure.GetForAction(new RemediationAction("PLAN_REGRESSION", "force", new List<ForcePlanTarget> { Target() }), null));
    }

    [Fact]
    public void RiskDisclosure_Rcsi_HasAtLeastOneOfEachSide()
    {
        var d = FactRiskDisclosure.GetForAction(RcsiAction(), RcsiOffFinding());
        Assert.NotNull(d);
        Assert.NotEmpty(d!.RisksOfChanging);
        Assert.NotEmpty(d.RisksOfNotChanging);
        // The validated identifier is substituted, bracketed.
        Assert.Contains(d.RisksOfChanging, r => r.Text.Contains("[Foo]"));
    }

    [Fact]
    public void RiskDisclosure_HighReaderWriterPct_SaysRcsiEliminates()
    {
        var d = FactRiskDisclosure.GetForAction(RcsiAction(), RcsiOffFinding(blocking: 50, deadlocks: 4, rwPct: 85))!;
        Assert.Contains(d.RisksOfNotChanging, r => r.Text.Contains("85%") && r.Text.Contains("RCSI eliminates"));
        Assert.Contains(d.RisksOfNotChanging, r => r.Text.Contains("50") && r.Text.Contains("blocked-process events") && r.Text.Contains("4 deadlocks"));
    }

    [Fact]
    public void RiskDisclosure_LowReaderWriterPct_SaysRcsiWontResolveThis()
    {
        // Writer/writer-dominant: the honest-both-directions safety feature.
        var d = FactRiskDisclosure.GetForAction(RcsiAction(), RcsiOffFinding(blocking: 40, deadlocks: 2, rwPct: 10))!;
        Assert.Contains(d.RisksOfNotChanging, r => r.Text.Contains("10%") && r.Text.Contains("RCSI does NOT resolve"));
        Assert.DoesNotContain(d.RisksOfNotChanging, r => r.Text.Contains("RCSI eliminates"));
    }

    [Fact]
    public void RiskDisclosure_NoData_ShowsWeakCaseBaseline()
    {
        var d = FactRiskDisclosure.GetForAction(RcsiAction(), RcsiOffFinding(blocking: 0, deadlocks: 0, rwPct: null))!;
        Assert.Contains(d.RisksOfNotChanging, r => r.Text.Contains("Little or no reader/writer blocking"));
    }

    [Fact]
    public void RiskDisclosure_NullFinding_DegradesToWeakCaseBaseline()
    {
        var d = FactRiskDisclosure.GetForAction(RcsiAction(), null)!;
        Assert.NotEmpty(d.RisksOfChanging);
        Assert.Contains(d.RisksOfNotChanging, r => r.Text.Contains("Little or no reader/writer blocking"));
    }

    [Fact]
    public void RiskDisclosure_Rcsi_GoldenProse_ChangingSide()
    {
        var d = FactRiskDisclosure.GetForAction(RcsiAction(), RcsiOffFinding())!;
        Assert.Equal(3, d.RisksOfChanging.Count);
        Assert.StartsWith("Enabling RCSI on [Foo] takes a brief exclusive database lock", d.RisksOfChanging[0].Text);
        Assert.Contains("row-version load to tempdb", d.RisksOfChanging[1].Text);
        Assert.Contains("Reader/writer concurrency semantics change", d.RisksOfChanging[2].Text);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // CLEAR CACHED PLAN (DBCC FREEPROCCACHE) — destructive privileged core (PR-A;
    // UNREGISTERED). The two catastrophic axes: never-bare/null-handle-guard, and the
    // ALTER-SERVER-STATE fail-closed gate. Plus the §2a row-level detector exclusion.
    // ════════════════════════════════════════════════════════════════════════════

    /// <summary>A CPU finding carrying an abnormal_cpu_plans drill-down with one qualifying row.</summary>
    private static AnalysisFinding CpuFinding(List<object>? rows = null, string rootKey = "CPU_SQL_PERCENT") => new()
    {
        ServerId = 1,
        ServerName = "TestServer",
        Category = "cpu",
        StoryPath = rootKey,
        StoryPathHash = "cpuplan000000001",
        RootFactKey = rootKey,
        DrillDown = new Dictionary<string, object>
        {
            ["abnormal_cpu_plans"] = rows ?? new List<object>
            {
                new
                {
                    query_hash = "0xABCDEF0123456789",
                    database = "AdventureWorks",
                    current_cpu_per_exec_ms = 45.0,
                    baseline_cpu_per_exec_ms = 9.0,
                    anomaly_ratio = 5.0,
                    execution_count = 1200L,
                    total_cpu_ms = 54000.0,
                    latest_plan_handle = "0x06000100ABCD",
                    query_text = "SELECT * FROM dbo.BigTable WHERE x = @p",
                    cpu_percent = 62,
                    plan_regression_cofired = false,
                    parameter_sensitivity_cofired = false
                }
            }
        }
    };

    private static RemediationAction ClearAction(string hash = "0xABCDEF0123456789", string db = "AdventureWorks") =>
        new("CLEAR_PLAN", "clear", Array.Empty<ForcePlanTarget>(),
            ClearPlanTargets: new[] { new ClearPlanTarget(db, hash, 45.0, 9.0, 5.0, "0x06") },
            ClearPlanFigures: new ClearPlanFigures(45.0, 9.0, 5.0, 62, false, false));

    // ── BuildClearPlanAction (parallel builder) + BuildAction-unchanged guard ────

    [Fact]
    public void BuildClearPlanAction_EmitsOnAbnormalCpuPlans()
    {
        var action = FactRemediation.BuildClearPlanAction(CpuFinding());
        Assert.NotNull(action);
        Assert.Equal("CLEAR_PLAN", action!.FactKey);
        Assert.Equal("clear", action.Action);
        Assert.Empty(action.Targets);
        Assert.Null(action.DbConfigTargets);
        var t = Assert.Single(action.ClearPlanTargets!);
        Assert.Equal("0xABCDEF0123456789", t.QueryHash);
        Assert.Equal("AdventureWorks", t.Database);
        Assert.Equal(5.0, t.AnomalyRatio);
        // The figures are carried for the at-apply dialog (no finding in hand).
        Assert.NotNull(action.ClearPlanFigures);
        Assert.Equal(45.0, action.ClearPlanFigures!.CurrentCpuPerExecMs);
        Assert.Equal(62, action.ClearPlanFigures.CpuPercent);
    }

    [Fact]
    public void BuildClearPlanAction_CpuSpikeRootKey_AlsoEmits()
    {
        var action = FactRemediation.BuildClearPlanAction(CpuFinding(rootKey: "CPU_SPIKE"));
        Assert.NotNull(action);
        Assert.Equal("CLEAR_PLAN", action!.FactKey);
    }

    [Fact]
    public void BuildClearPlanAction_ReturnsNull_WhenNoDrillDown()
    {
        var finding = CpuFinding();
        finding.DrillDown = new Dictionary<string, object>();   // no abnormal_cpu_plans
        Assert.Null(FactRemediation.BuildClearPlanAction(finding));
    }

    [Fact]
    public void BuildClearPlanAction_ReturnsNull_WhenNotCpuFinding()
    {
        var finding = CpuFinding();
        finding.RootFactKey = "PLAN_REGRESSION";
        Assert.Null(FactRemediation.BuildClearPlanAction(finding));
    }

    [Fact]
    public void BuildClearPlanAction_RejectsRowsWithoutValidQueryHash()
    {
        // A row whose query_hash is blank / not a hex handle can never become a target —
        // it could never resolve a plan handle (defends the never-resolve-garbage edge).
        var action = FactRemediation.BuildClearPlanAction(CpuFinding(new List<object>
        {
            new { query_hash = "", database = "Db", current_cpu_per_exec_ms = 10.0, baseline_cpu_per_exec_ms = 1.0,
                  anomaly_ratio = 10.0, execution_count = 5L, total_cpu_ms = 5000.0, latest_plan_handle = "",
                  query_text = "", cpu_percent = 10, plan_regression_cofired = false, parameter_sensitivity_cofired = false },
            new { query_hash = "notahex", database = "Db", current_cpu_per_exec_ms = 10.0, baseline_cpu_per_exec_ms = 1.0,
                  anomaly_ratio = 10.0, execution_count = 5L, total_cpu_ms = 5000.0, latest_plan_handle = "",
                  query_text = "", cpu_percent = 10, plan_regression_cofired = false, parameter_sensitivity_cofired = false }
        }));
        Assert.Null(action);
    }

    [Fact]
    public void BuildAction_Unchanged_NeverEmitsClearPlan_OnCpuFinding()
    {
        // The regression guard (§9): the parallel builder does not leak into BuildAction —
        // a CPU finding carrying abnormal_cpu_plans yields NO action from BuildAction.
        Assert.Null(FactRemediation.BuildAction(CpuFinding()));
    }

    // ── FactRiskDisclosure CLEAR_PLAN arm (two-sided, real figures, both steers) ──

    [Fact]
    public void RiskDisclosure_ClearPlan_HasAtLeastOneOfEachSide_RealFigures()
    {
        var d = FactRiskDisclosure.GetForAction(ClearAction(), CpuFinding());
        Assert.NotNull(d);
        Assert.NotEmpty(d!.RisksOfChanging);
        Assert.NotEmpty(d.RisksOfNotChanging);
        // Real figures substituted from the carried ClearPlanFigures.
        Assert.Contains(d.RisksOfNotChanging, r => r.Text.Contains("5.0x") && r.Text.Contains("45.0 ms") && r.Text.Contains("9.0 ms") && r.Text.Contains("62%"));
        // The changing side discloses the recompile + not-guaranteed-better + per-handle blast radius.
        Assert.Contains(d.RisksOfChanging, r => r.Text.Contains("forces a recompile"));
        Assert.Contains(d.RisksOfChanging, r => r.Text.Contains("NOT guaranteed to produce a better plan"));
        Assert.Contains(d.RisksOfChanging, r => r.Text.Contains("EVERY currently-cached plan"));
    }

    [Fact]
    public void RiskDisclosure_ClearPlan_PlanRegressionCoFired_SteersToForce()
    {
        var action = new RemediationAction("CLEAR_PLAN", "clear", Array.Empty<ForcePlanTarget>(),
            ClearPlanTargets: new[] { new ClearPlanTarget("Db", "0xAA") },
            ClearPlanFigures: new ClearPlanFigures(40.0, 8.0, 5.0, 50, PlanRegressionCoFired: true, ParameterSensitivityCoFired: false));
        var d = FactRiskDisclosure.GetForAction(action, null)!;
        Assert.Contains(d.RisksOfNotChanging, r => r.Text.Contains("forcing it") && r.Text.Contains("more durable"));
        Assert.DoesNotContain(d.RisksOfNotChanging, r => r.Text.Contains("parameter-sensitive"));
    }

    [Fact]
    public void RiskDisclosure_ClearPlan_ParameterSensitivityCoFired_SaysWontDurablyHelp()
    {
        var action = new RemediationAction("CLEAR_PLAN", "clear", Array.Empty<ForcePlanTarget>(),
            ClearPlanTargets: new[] { new ClearPlanTarget("Db", "0xAA") },
            ClearPlanFigures: new ClearPlanFigures(40.0, 8.0, 5.0, 50, PlanRegressionCoFired: false, ParameterSensitivityCoFired: true));
        var d = FactRiskDisclosure.GetForAction(action, null)!;
        Assert.Contains(d.RisksOfNotChanging, r => r.Text.Contains("parameter-sensitive") && r.Text.Contains("WON'T durably"));
    }

    [Fact]
    public void RiskDisclosure_ClearPlan_FiguresSurvivePersistence_NoFinding()
    {
        // The RcsiInactionFigures analog: with NO finding (apply-time), the carried
        // ClearPlanFigures still render the real numbers.
        var d = FactRiskDisclosure.GetForAction(ClearAction(), null)!;
        Assert.Contains(d.RisksOfNotChanging, r => r.Text.Contains("5.0x") && r.Text.Contains("62%"));
    }

    // ── ClearPlanHandler invariants ──────────────────────────────────────────────

    [Fact]
    public void ClearPlan_Handler_IsDestructive_And_ApplyOnly()
    {
        var handler = new ClearPlanHandler();
        Assert.Equal("CLEAR_PLAN", handler.FactKey);
        Assert.True(handler.IsDestructive);          // the second true in the codebase
        Assert.False(handler.SupportsUnapply);       // you cannot un-clear a cache
    }

    [Fact]
    public async Task ClearPlan_UnapplyAsync_Throws()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            new ClearPlanHandler().UnapplyAsync(ClearAction(), new FakeExecutor(), Identity, CancellationToken.None));
    }

    [Fact]
    public void ClearPlan_Handler_IsRegistered_AndReachableThroughGate()
    {
        // PR-B makes CLEAR_PLAN LIVE: the PRODUCTION wiring registers ClearPlanHandler so the
        // "Clear cached plan (advanced)" affordance can appear and route to the destructive
        // handler — but ONLY through the gated RemediationApplyService facade (proven by the
        // Gate_* behavioural tests + the reachability guards CoreMachinery_OnlyReferencedInRemediationCore
        // and GatedEntry_ReferencedOnlyBySanctionedUiPath).
        var productionRegistry = new RemediationHandlerRegistry(
            new IRemediationHandler[] { new ForcePlanHandler(), new DbConfigHandler(), new RcsiHandler(), new ClearPlanHandler() });
        Assert.IsType<ClearPlanHandler>(productionRegistry.TryGet("CLEAR_PLAN"));

        // Assert at the source level that the PRODUCTION wiring now constructs ClearPlanHandler.
        var dir = FindDashboardSourceDir();
        var serviceSrc = File.ReadAllText(Path.Combine(dir, "Services", "Remediation", "RemediationApplyService.cs"));
        Assert.Contains("new ClearPlanHandler()", serviceSrc);

        // The destructive CLEAR_PLAN handler can NEVER be reached through the always-safe
        // force-plan / DB-config handlers, nor cross with the other destructive (RCSI) one:
        // every fact key resolves to its OWN distinct handler type.
        Assert.IsType<ForcePlanHandler>(productionRegistry.TryGet("PLAN_REGRESSION"));
        Assert.IsType<DbConfigHandler>(productionRegistry.TryGet("DB_CONFIG"));
        Assert.IsType<RcsiHandler>(productionRegistry.TryGet("RCSI"));
        Assert.NotEqual("CLEAR_PLAN", productionRegistry.TryGet("DB_CONFIG")!.FactKey);
        Assert.NotEqual("CLEAR_PLAN", productionRegistry.TryGet("PLAN_REGRESSION")!.FactKey);
        Assert.NotEqual("CLEAR_PLAN", productionRegistry.TryGet("RCSI")!.FactKey);
    }

    [Fact]
    public void CoreMachineryMarkers_Include_ClearPlanSurface()
    {
        Assert.Contains("ClearPlanHandler", CoreMachineryMarkers);
        Assert.Contains("ClearProcCacheAsync", CoreMachineryMarkers);
    }

    // ── ClearPlanHandler apply behaviours against the faked executor ─────────────

    [Fact]
    public async Task ClearPlan_AuditTableAbsent_HardBlocks_NoMutation_NoAudit()
    {
        var exec = new FakeExecutor { AuditTableExists = false };
        var result = await new ClearPlanHandler().ApplyAsync(ClearAction(), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Blocked, o.Status);
        Assert.False(o.AuditWritten);
        Assert.Equal(0, exec.ClearPlanCalls);        // no DBCC path even attempted
        Assert.Empty(exec.AuditRecords);
    }

    [Fact]
    public async Task ClearPlan_PermissionDenied_FailsClosed_NoElevation()
    {
        // The DOMINANT runtime path: the least-privilege login lacks ALTER SERVER STATE.
        var exec = new FakeExecutor
        {
            ClearPlanFunc = h => new ClearPlanOutcome
            {
                QueryHash = h, Status = RemediationStatus.PermissionDenied, Cleared = false, HandlesCleared = 0,
                ExecutingLogin = "PerfMonLogin",
                Message = "lacks ALTER SERVER STATE ... GRANT ALTER SERVER STATE TO [PerfMonLogin];",
                PriorValue = "0 plans (permission denied)"
            }
        };
        var result = await new ClearPlanHandler().ApplyAsync(ClearAction(), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.PermissionDenied, o.Status);
        Assert.Equal(1, exec.ClearPlanCalls);
        var rec = Assert.Single(exec.AuditRecords);
        Assert.Equal("skipped", rec.Result);          // PermissionDenied audits as skipped
        Assert.True(rec.ConsentAcknowledged);
        Assert.Equal("CLEAR_PLAN", rec.FactKey);
        Assert.Equal("clear_cached_plan", rec.Action);
        Assert.Null(rec.QueryId);
        Assert.Null(rec.PlanId);
        Assert.Contains("GRANT ALTER SERVER STATE", rec.ErrorMessage);
    }

    [Fact]
    public async Task ClearPlan_NoSurvivingHandle_Skips()
    {
        var exec = new FakeExecutor
        {
            ClearPlanFunc = h => new ClearPlanOutcome
            {
                QueryHash = h, Status = RemediationStatus.Skipped, Cleared = false, HandlesCleared = 0,
                ExecutingLogin = "sa", Message = "No cached plan is currently present", PriorValue = "0 plans (no surviving handle)"
            }
        };
        var result = await new ClearPlanHandler().ApplyAsync(ClearAction(), exec, Identity, CancellationToken.None);

        Assert.Equal(RemediationStatus.Skipped, Assert.Single(result.Outcomes).Status);
        var rec = Assert.Single(exec.AuditRecords);
        Assert.Equal("skipped", rec.Result);
        Assert.True(rec.ConsentAcknowledged);
    }

    [Fact]
    public async Task ClearPlan_Success_WritesAudit_ConsentTrue_NullIds_Action()
    {
        var exec = new FakeExecutor();   // default success outcome
        var result = await new ClearPlanHandler().ApplyAsync(ClearAction(), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Success, o.Status);
        Assert.True(o.AuditWritten);
        var rec = Assert.Single(exec.AuditRecords);
        Assert.Equal("success", rec.Result);
        Assert.True(rec.ConsentAcknowledged);
        Assert.Equal("clear_cached_plan", rec.Action);
        Assert.Null(rec.QueryId);
        Assert.Null(rec.PlanId);
        Assert.Contains("DBCC FREEPROCCACHE", rec.GeneratedSql);
        // The audited action string fits the VarChar(32) @action param (17 chars).
        Assert.True(rec.Action.Length <= 32);
    }

    // ── Never-bare / null-handle-guard structural assertions (M-1) ───────────────
    //
    // The single-connection SPID equality + the actual DBCC text are executor/real-server
    // concerns (cannot be exercised against a faked executor). These assert, at the SOURCE
    // level, that the executor ONLY ever builds the single-`@plan_handle` form, binds the
    // handle as a typed param, filters plan_handle IS NOT NULL, rejects null/zero-length in
    // C# before any DBCC string, and never emits the bare/whole-cache form.

    [Fact]
    public void Executor_NeverEmitsBareFreeProcCache_OnlyTypedSingleHandleForm()
    {
        var dir = FindDashboardSourceDir();
        var src = File.ReadAllText(Path.Combine(dir, "Services", "DatabaseService.Remediation.cs"));

        // The ONLY DBCC text is the single-handle form with a typed @plan_handle param.
        Assert.Contains("DBCC FREEPROCCACHE(@plan_handle)", src);
        Assert.Contains("@plan_handle", src);
        Assert.Contains("SqlDbType.VarBinary, 64", src);

        // The catastrophic bare/whole-cache form must NOT appear anywhere: no
        // no-argument FREEPROCCACHE statement and no empty-arg form.
        Assert.DoesNotContain("FREEPROCCACHE;", src);
        Assert.DoesNotContain("FREEPROCCACHE ;", src);
        Assert.DoesNotContain("FREEPROCCACHE()", src);   // no empty-arg form

        // The live resolve filters plan_handle IS NOT NULL (the BAD_ACTOR precedent),
        // and the C# guard rejects null/zero-length BEFORE any DBCC string is built.
        Assert.Contains("plan_handle IS NOT NULL", src);
        Assert.Contains("handle.Length == 0", src);
    }

    [Fact]
    public void Executor_GateIsNamedAlterServerState_FailClosed()
    {
        var dir = FindDashboardSourceDir();
        var src = File.ReadAllText(Path.Combine(dir, "Services", "DatabaseService.Remediation.cs"));

        // The NAMED server permission, fail-closed via ISNULL(...,0). NOT the generic
        // (NULL,NULL,'ALTER') token (which returns NULL even for sysadmin).
        Assert.Contains("HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER SERVER STATE')", src);
        Assert.Contains("ISNULL(HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER SERVER STATE'), 0)", src);
        Assert.DoesNotContain("HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER')", src);
        // PermissionDenied carries grant guidance.
        Assert.Contains("GRANT ALTER SERVER STATE", src);
    }

    [Fact]
    public void Executor_HexHandleParse_RejectsMalformed_NeverThrows()
    {
        // The query_hash → binary(8) bind path: a typed param, never concatenated. A
        // malformed value parses to null (→ Skip), never a DBCC string.
        Assert.Null(DatabaseService.TryParseHexHandle(null));
        Assert.Null(DatabaseService.TryParseHexHandle(""));
        Assert.Null(DatabaseService.TryParseHexHandle("0x"));
        Assert.Null(DatabaseService.TryParseHexHandle("0xABC"));     // odd length
        Assert.Null(DatabaseService.TryParseHexHandle("0xZZ"));      // not hex
        var ok = DatabaseService.TryParseHexHandle("0xABCDEF0123456789");
        Assert.NotNull(ok);
        Assert.Equal(8, ok!.Length);
        // No "0x" prefix is also accepted (raw hex).
        Assert.Equal(8, DatabaseService.TryParseHexHandle("ABCDEF0123456789")!.Length);
    }

    // ── Detector row-level exclusion (§2a / R2-MOD-A / R2-MOD-B) ─────────────────
    //
    // The production detector runs this logic as T-SQL against collect.query_stats; the
    // PlanCacheAnomalyDetector reference encodes the SAME row-level exclusion + per-exec
    // math so the catastrophic correctness rules are headlessly guarded.

    private static readonly DateTime ServerUp = new(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime CurrentStart = new(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);

    private static PlanCacheAnomalyDetector.StatRow Row(
        string hash, string plan, DateTime t, long workerUs, long execs,
        DateTime? serverStart = null, string sqlH = "S1", int so = 0, int eo = -1) =>
        new(hash, sqlH, so, eo, plan, t, serverStart ?? ServerUp, workerUs, execs);

    [Fact]
    public void Detector_LeakyFirstCollection_YieldsNoTarget()
    {
        // R2-MOD-A: a plan_handle whose GLOBAL-first collection is the FIRST in-window row,
        // carrying delta = full cumulative raw total. The leaky "≥2 in-window" gate would
        // wrongly pass it; the row-level exclusion drops it. There is no earlier collection
        // for (sql_handle, offsets, plan_handle) → not a real-delta row → excluded.
        var rows = new List<PlanCacheAnomalyDetector.StatRow>
        {
            // baseline window: a normal plan at low per-exec CPU, WITH a real prior.
            Row("0xQ1", "P0", CurrentStart.AddHours(-10), 1_000_000, 1000),   // first-collection of P0 (no prior) — excluded
            Row("0xQ1", "P0", CurrentStart.AddHours(-9),    10_000, 1000),    // real delta: 10us/exec
            // current window: a NEW plan_handle P1 whose FIRST collected row carries 6h of
            // accumulated CPU as one delta (the contamination this feature targets).
            Row("0xQ1", "P1", CurrentStart.AddHours(1), 6_000_000_000, 1000), // first-collection of P1 — excluded
        };

        var results = PlanCacheAnomalyDetector.Evaluate(rows, CurrentStart);
        // The only real-delta row is the one baseline row; current window has no real-delta
        // rows → no current execs → no anomaly. The fake 6h spike never counts.
        Assert.Empty(results);
    }

    [Fact]
    public void Detector_ServerRestartRow_YieldsNoTarget()
    {
        // R2-MOD-B: the first post-restart row (server_start_time >= prior collection_time)
        // carries delta = full cumulative raw total. The row-level predicate requires the
        // prior collection_time > this row's server_start_time, so a post-restart row is
        // excluded even though an earlier collection exists.
        var restart = CurrentStart.AddMinutes(30);   // server restarted mid-window
        var rows = new List<PlanCacheAnomalyDetector.StatRow>
        {
            Row("0xQ2", "P0", CurrentStart.AddHours(-9), 9_000, 1000),                 // first-collection — excluded
            Row("0xQ2", "P0", CurrentStart.AddHours(-8), 9_000, 1000, ServerUp),       // real delta (prior at -9 > ServerUp)
            // post-restart row: huge raw total; prior collection (-8) is BEFORE the new
            // server_start_time (restart), so it is NOT a real-delta row → excluded.
            Row("0xQ2", "P0", CurrentStart.AddHours(1), 9_000_000_000, 1000, restart),
        };

        var results = PlanCacheAnomalyDetector.Evaluate(rows, CurrentStart);
        Assert.Empty(results);
    }

    [Fact]
    public void Detector_SyntheticMassRestart_ProducesNoTargetsEnMasse()
    {
        // A mass restart: MANY plan_handles each with a first-post-restart raw-total row in
        // the current window. None has a real-delta current row → no targets en masse.
        var restart = CurrentStart.AddMinutes(15);
        var rows = new List<PlanCacheAnomalyDetector.StatRow>();
        for (int i = 0; i < 50; i++)
        {
            var hash = "0xQ" + i.ToString("X");
            var plan = "P" + i;
            // a real baseline so each query has a baseline per-exec
            rows.Add(Row(hash, plan, CurrentStart.AddHours(-9), 5_000, 500, ServerUp, sqlH: "S" + i));
            rows.Add(Row(hash, plan, CurrentStart.AddHours(-8), 5_000, 500, ServerUp, sqlH: "S" + i));
            // the mass post-restart contaminated row in the current window
            rows.Add(Row(hash, plan, CurrentStart.AddMinutes(20), 8_000_000_000, 500, restart, sqlH: "S" + i));
        }

        var results = PlanCacheAnomalyDetector.Evaluate(rows, CurrentStart);
        Assert.Empty(results);
    }

    [Fact]
    public void Detector_GenuinePerExecJump_OnRealDeltaRows_EmitsTarget()
    {
        // A genuine per-exec jump on REAL-prior-delta rows (an earlier collection exists,
        // after server_start_time) DOES emit a target. Baseline ~10us/exec; current
        // ~50us/exec = 5x, material CPU.
        var rows = new List<PlanCacheAnomalyDetector.StatRow>
        {
            Row("0xHOT", "P0", CurrentStart.AddHours(-9), 9_999_999, 1000),                 // first-collection — excluded
            Row("0xHOT", "P0", CurrentStart.AddHours(-8), 10_000_000, 1000),                // baseline real delta: 10us/exec
            Row("0xHOT", "P0", CurrentStart.AddHours(-7), 10_000_000, 1000),                // baseline real delta: 10us/exec
            Row("0xHOT", "P0", CurrentStart.AddHours(1),  50_000_000, 1000),                // current real delta: 50us/exec
        };

        var results = PlanCacheAnomalyDetector.Evaluate(rows, CurrentStart);
        var hit = Assert.Single(results);
        Assert.Equal("0xHOT", hit.QueryHash);
        Assert.True(hit.AnomalyRatio >= 4.5 && hit.AnomalyRatio <= 5.5, $"ratio was {hit.AnomalyRatio}");
        Assert.True(hit.CurrentCpuPerExecMs > hit.BaselineCpuPerExecMs);
    }

    [Fact]
    public void Detector_StableExpensiveQuery_NoTarget()
    {
        // A query that is expensive but STABLE (high but unchanging per-exec) is NOT a
        // clear-plan candidate — the anomaly gate (ratio < T) excludes it.
        var rows = new List<PlanCacheAnomalyDetector.StatRow>
        {
            Row("0xSTABLE", "P0", CurrentStart.AddHours(-9), 100_000_000, 1000),    // first-collection — excluded
            Row("0xSTABLE", "P0", CurrentStart.AddHours(-8), 100_000_000, 1000),    // baseline 100us/exec
            Row("0xSTABLE", "P0", CurrentStart.AddHours(1),  105_000_000, 1000),    // current 105us/exec — ~1.05x
        };

        Assert.Empty(PlanCacheAnomalyDetector.Evaluate(rows, CurrentStart));
    }

    [Fact]
    public void Detector_AnomalousButTrivialCpu_NoTarget()
    {
        // Abnormal per-exec ratio but below the materiality floor → no target (clearing
        // a trivial-CPU query is pointless).
        var rows = new List<PlanCacheAnomalyDetector.StatRow>
        {
            Row("0xTINY", "P0", CurrentStart.AddHours(-9), 100, 10),     // first-collection — excluded
            Row("0xTINY", "P0", CurrentStart.AddHours(-8), 100, 10),     // baseline 10us/exec
            Row("0xTINY", "P0", CurrentStart.AddHours(1),  5_000, 10),   // current 500us/exec = 50x but only 5ms total
        };

        Assert.Empty(PlanCacheAnomalyDetector.Evaluate(rows, CurrentStart));
    }

    // ── cpu_percent fix (PR-A security review LOW-1) ─────────────────────────────
    //
    // The live detector ran in T-SQL against collect.query_stats; the cpu_percent SHARE is
    // computed in that SQL (numerator/denominator over the SAME §2a real-delta rows). These
    // source-level assertions guard that PR-B replaced the hardcoded 0 with the real share.

    [Fact]
    public void Detector_CpuPercent_IsRealWindowShare_NotHardcodedZero()
    {
        var dir = FindDashboardSourceDir();
        // CollectAbnormalCpuPlans lives in a per-domain partial (SqlServerDrillDownCollector.Queries.cs)
        // after the partial-class split; read every partial so the assertions hold wherever it sits.
        var src = string.Concat(
            Directory.GetFiles(Path.Combine(dir, "Analysis"), "SqlServerDrillDownCollector*.cs")
                .OrderBy(p => p, StringComparer.Ordinal)
                .Select(File.ReadAllText));

        // The CollectAbnormalCpuPlans detector must NO LONGER hardcode cpu_percent = 0.
        Assert.DoesNotContain("cpu_percent = 0,", src);

        // It computes the real share: a window_total CTE (same §2a real-delta exclusion) as
        // the denominator, and cpu_percent = the per-query current_total_cpu_ms / that total.
        Assert.Contains("window_total", src);
        Assert.Contains("cpu_percent =", src);
        Assert.Contains("w.current_total_cpu_ms / NULLIF(wt.total_cpu_ms, 0)", src);
        Assert.Contains("CROSS JOIN window_total AS wt", src);
    }

    [Fact]
    public void ClearPlanFigures_CarryRealCpuPercent_Through_BuildClearPlanAction()
    {
        // The carried path (NOT the live finding): a finding whose abnormal_cpu_plans row
        // has a real cpu_percent flows that number onto ClearPlanFigures, and the disclosure
        // renders it — so the dialog/disclosure show the real % at apply time.
        var action = FactRemediation.BuildClearPlanAction(CpuFinding(new List<object>
        {
            new { query_hash = "0xABCDEF0123456789", database = "Db", current_cpu_per_exec_ms = 45.0,
                  baseline_cpu_per_exec_ms = 9.0, anomaly_ratio = 5.0, execution_count = 1200L,
                  total_cpu_ms = 54000.0, latest_plan_handle = "0x06", query_text = "q",
                  cpu_percent = 73, plan_regression_cofired = false, parameter_sensitivity_cofired = false }
        }));
        Assert.NotNull(action);
        Assert.Equal(73, action!.ClearPlanFigures!.CpuPercent);
        Assert.NotEqual(0, action.ClearPlanFigures.CpuPercent);

        var d = FactRiskDisclosure.GetForAction(action, null)!;
        Assert.Contains(d.RisksOfNotChanging, r => r.Text.Contains("73%"));
    }

    // ── Full lock-mode vocabulary classifier (M-1) ───────────────────────────────

    [Theory]
    [InlineData("S", RcsiLockClass.Reader)]
    [InlineData("IS", RcsiLockClass.Reader)]
    [InlineData("RangeS-S", RcsiLockClass.Reader)]
    [InlineData("RangeS-U", RcsiLockClass.Reader)]
    [InlineData("X", RcsiLockClass.Writer)]
    [InlineData("IX", RcsiLockClass.Writer)]
    [InlineData("U", RcsiLockClass.Writer)]
    [InlineData("UIX", RcsiLockClass.Writer)]
    [InlineData("RangeX-X", RcsiLockClass.Writer)]
    [InlineData("RangeI-N", RcsiLockClass.Writer)]
    [InlineData("Sch-S", RcsiLockClass.Excluded)]
    [InlineData("Sch-M", RcsiLockClass.Excluded)]
    [InlineData("BU", RcsiLockClass.Excluded)]
    [InlineData("IU", RcsiLockClass.Excluded)]
    [InlineData("SIU", RcsiLockClass.Excluded)]
    [InlineData("SIX", RcsiLockClass.Excluded)]
    [InlineData("", RcsiLockClass.Excluded)]
    [InlineData(null, RcsiLockClass.Excluded)]
    public void LockModeClassifier_FullVocabulary(string? lockMode, RcsiLockClass expected)
    {
        Assert.Equal(expected, RcsiLockModeClassifier.Classify(lockMode));
    }

    // ── Drill-down enrichment parity (types only, per M-2) ───────────────────────

    [Fact]
    public void DrillDownEnrichment_RcsiFields_TypeParity_DashboardVsLite()
    {
        // The Dashboard shape carries real ints + a nullable int; the Lite shape carries
        // 0/0/null. The shared extractor (FactRiskDisclosure) must read BOTH shapes with
        // identical field NAMES and TYPES (int / int / nullable-int). Round-trip via JSON
        // (mirrors how the collectors serialize the anonymous objects).
        var dashboardRow = new
        {
            database = "Foo", recovery_model = "FULL", rcsi = false, query_store = true,
            issues = new[] { "RCSI OFF" }, auto_shrink = false, auto_close = false, page_verify = "CHECKSUM",
            rcsi_blocking_events = 12, rcsi_deadlocks = 3, rcsi_reader_writer_pct = (int?)80
        };
        var liteRow = new
        {
            database = "Foo", recovery_model = "FULL", rcsi = false, query_store = true,
            issues = new[] { "RCSI OFF" }, auto_shrink = false, auto_close = false, page_verify = "CHECKSUM",
            rcsi_blocking_events = 0, rcsi_deadlocks = 0, rcsi_reader_writer_pct = (int?)null
        };

        var dash = System.Text.Json.JsonSerializer.SerializeToElement((object)dashboardRow);
        var lite = System.Text.Json.JsonSerializer.SerializeToElement((object)liteRow);
        foreach (var field in new[] { "rcsi_blocking_events", "rcsi_deadlocks", "rcsi_reader_writer_pct" })
        {
            Assert.True(dash.TryGetProperty(field, out var dv), $"Dashboard shape missing {field}");
            Assert.True(lite.TryGetProperty(field, out var lv), $"Lite shape missing {field}");
            // counts are Number on both; pct is Number on Dashboard and Null on Lite (nullable int)
            Assert.Equal(System.Text.Json.JsonValueKind.Number, dv.ValueKind);
            if (field == "rcsi_reader_writer_pct")
                Assert.Equal(System.Text.Json.JsonValueKind.Null, lv.ValueKind);
            else
                Assert.Equal(System.Text.Json.JsonValueKind.Number, lv.ValueKind);
        }
    }

    // ── §1 identifier safety: no-injection / QUOTENAME / same-string (M-1) ───────
    //
    // These run against the EXECUTOR's OWN statement builder (DatabaseService.
    // BuildAlterStatement), NOT the display renderer (m-B). The builder is the exact
    // composition SetDatabaseOptionAsync uses for the executed statement.

    [Theory]
    [InlineData(DbConfigSetting.AutoShrinkOff, "SET AUTO_SHRINK OFF")]
    [InlineData(DbConfigSetting.AutoCloseOff, "SET AUTO_CLOSE OFF")]
    [InlineData(DbConfigSetting.PageVerifyChecksum, "SET PAGE_VERIFY CHECKSUM")]
    public void Build_SetClause_IsHardcodedLiteral_RegardlessOfName(DbConfigSetting setting, string expectedClause)
    {
        // The SET clause is byte-identical regardless of the (pathological) db name.
        foreach (var name in new[] { "Db", "Db]; DROP TABLE x--", "[weird]", "Ünîçödé" })
        {
            var stmt = DatabaseService.BuildAlterStatement(name, setting);
            Assert.Contains(expectedClause + ";", stmt);
            Assert.EndsWith(expectedClause + ";", stmt);
        }
    }

    [Theory]
    [InlineData("normal")]
    [InlineData("has]bracket")]
    [InlineData("has]]double")]
    [InlineData("ev;il-- GO DROP")]
    [InlineData("'; ALTER DATABASE master SET")]
    [InlineData("Ünîçödé​")]   // unicode + zero-width
    public void Build_BracketsIdentifierInert_NoInjection(string name)
    {
        var stmt = DatabaseService.BuildAlterStatement(name, DbConfigSetting.AutoShrinkOff);

        // The identifier is bracketed with ]-doubling; the whole thing is one ALTER
        // with exactly one bracketed token and the hardcoded clause.
        var expectedToken = "[" + name.Replace("]", "]]") + "]";
        Assert.Equal($"ALTER DATABASE {expectedToken} SET AUTO_SHRINK OFF;", stmt);

        // No un-doubled close-bracket can prematurely end the identifier: the only
        // ']' runs in the statement are the doubled ones we produced plus the final.
        // Reconstruct: stripping the known prefix/suffix yields exactly the token.
        Assert.StartsWith("ALTER DATABASE [", stmt);
    }

    [Fact]
    public void Build_SameString_ValidatedNameIsBracketedExactly()
    {
        // M-1: the bracketed token is byte-identical to the validated name passed in
        // — an implementer cannot validate one variable and bracket a trimmed/cased one.
        const string validated = "  Padded Name  ";          // deliberate surrounding whitespace
        var stmt = DatabaseService.BuildAlterStatement(validated, DbConfigSetting.AutoCloseOff);
        Assert.Equal("ALTER DATABASE [  Padded Name  ] SET AUTO_CLOSE OFF;", stmt);

        // A name differing only by trailing whitespace produces a DIFFERENT statement,
        // proving the builder brackets exactly what it was given (no normalization).
        var stmt2 = DatabaseService.BuildAlterStatement(validated.TrimEnd(), DbConfigSetting.AutoCloseOff);
        Assert.NotEqual(stmt, stmt2);
    }

    [Fact]
    public void Build_DisplayRenderer_NotExecuted_ExecutorHasOwnBuilder()
    {
        // m-B: the executor's builder is byte-identical to the display QUOTENAME but
        // is its OWN routine in the Dashboard assembly (not the private Analysis one).
        // Spot-check parity on a ]-containing name.
        var stmt = DatabaseService.BuildAlterStatement("a]b", DbConfigSetting.PageVerifyChecksum);
        Assert.Equal("ALTER DATABASE [a]]b] SET PAGE_VERIFY CHECKSUM;", stmt);
    }

    // ── Reachability-with-gate guard (replaces PR-A's no-caller guard) ──────────
    //
    // PR-A asserted the privileged machinery had NO non-core caller. PR-B adds a
    // legitimate caller (the Apply Fix UI), so "no caller" can no longer hold. The
    // invariant becomes "reachable ONLY through the gate," proven in two parts:
    //
    //   A. The core machinery TYPES + force/unforce methods (handler, registry,
    //      executor, ForcePlanAsync/UnforcePlanAsync) are still referenced ONLY
    //      inside the remediation core — the UI never touches them directly.
    //   B. The single gated entry point (RemediationApplyService) is referenced
    //      outside the core ONLY by the sanctioned Apply Fix UI files. That facade
    //      runs the operator confirm before any handler.ApplyAsync (proven
    //      behaviourally by Gate_* in RemediationApplyServiceTests).
    //
    // Together: the UI reaches the privileged executor only via RemediationApplyService,
    // and RemediationApplyService reaches the handler only after confirm() == true.

    private static readonly string[] CoreMachineryMarkers =
    {
        "ForcePlanAsync", "UnforcePlanAsync",
        "RemediationHandlerRegistry", "DatabaseServiceRemediationExecutor",
        "ForcePlanHandler", "IRemediationExecutor", "IRemediationHandler",
        // B3 Phase 2 privileged surface (M-2): the new handler + executor methods
        // must be reachable ONLY through the gate, exactly like the force-plan ones.
        "DbConfigHandler", "SetDatabaseOptionAsync", "PreflightDbConfigAsync",
        // B3 Phase 3 (m-3): the destructive RCSI handler must be reachable ONLY through
        // the gate. It is UNREGISTERED in PR-A (dead-code-safe), but the marker still
        // guards against any UI/MCP/menu file referencing it directly.
        "RcsiHandler",
        // Clear-cached-plan (DBCC FREEPROCCACHE) destructive core: the new handler + the
        // DISTINCTIVE executor method name. UNREGISTERED in PR-A (dead-code-safe); the
        // markers still guard against any UI/MCP/menu file referencing the privileged
        // surface directly. ClearProcCacheAsync is distinctive enough not to substring-
        // false-match an unrelated reference.
        "ClearPlanHandler", "ClearProcCacheAsync",
        // WS3 percent-autogrowth always-safe core: the new handler + the DISTINCTIVE
        // executor method name. Reachable ONLY through the gate, exactly like DbConfigHandler.
        "FileAutogrowthHandler", "SetFileGrowthAsync", "PreflightFileGrowthAsync",
        // WS3 server-level config always-safe core (MAXDOP/CTFP sp_configure): the new handler + the
        // DISTINCTIVE executor method names. Reachable ONLY through the gate, exactly like DbConfigHandler.
        "ServerConfigHandler", "SetServerConfigAsync", "PreflightServerConfigAsync",
    };

    [Fact]
    public void CoreMachinery_OnlyReferencedInRemediationCore()
    {
        var dashboardDir = FindDashboardSourceDir();

        var offenders = new List<string>();
        foreach (var file in Directory.EnumerateFiles(dashboardDir, "*.cs", SearchOption.AllDirectories))
        {
            var rel = Path.GetRelativePath(dashboardDir, file).Replace('\\', '/');
            if (rel.StartsWith("bin/") || rel.StartsWith("obj/"))
                continue;

            var allowed =
                rel.StartsWith("Services/Remediation/") ||
                rel == "Services/DatabaseService.Remediation.cs";
            if (allowed)
                continue;

            var text = File.ReadAllText(file);
            if (CoreMachineryMarkers.Any(text.Contains))
                offenders.Add(rel);
        }

        Assert.True(offenders.Count == 0,
            "The privileged remediation machinery (force/unforce, handler, registry, executor) " +
            "must be reached ONLY through RemediationApplyService — no UI/MCP/menu/command file " +
            "may reference the machinery types directly. Offending files: " + string.Join(", ", offenders));
    }

    [Fact]
    public void GatedEntry_ReferencedOnlyBySanctionedUiPath()
    {
        var dashboardDir = FindDashboardSourceDir();

        // The ONLY files outside the remediation core allowed to reach the gated
        // facade. Any other file gaining a reference to RemediationApplyService is a
        // new, unreviewed path to the privileged executor and must fail the build.
        var sanctioned = new HashSet<string>(StringComparer.Ordinal)
        {
            "MainWindow.xaml.cs",                    // constructs + injects the service
            "Controls/AlertsHistoryContent.xaml.cs", // threads it into the alert detail dialog
            "AlertDetailWindow.xaml.cs",             // invokes Apply/Un-apply via the service
            "RemediationConfirmWindow.xaml.cs",      // the confirm modal (gate UI)
            "ServerTab.xaml.cs",                     // forwards it to the Recommendations sub-tab (WS1b-2)
            "Controls/RecommendationsContent.xaml.cs", // invokes Apply via the service (WS1b-2)
        };

        var offenders = new List<string>();
        foreach (var file in Directory.EnumerateFiles(dashboardDir, "*.cs", SearchOption.AllDirectories))
        {
            var rel = Path.GetRelativePath(dashboardDir, file).Replace('\\', '/');
            if (rel.StartsWith("bin/") || rel.StartsWith("obj/"))
                continue;
            if (rel.StartsWith("Services/Remediation/"))
                continue;
            if (sanctioned.Contains(rel))
                continue;

            var text = File.ReadAllText(file);
            if (text.Contains("RemediationApplyService"))
                offenders.Add(rel);
        }

        Assert.True(offenders.Count == 0,
            "Only the sanctioned Apply Fix UI path may reference RemediationApplyService. " +
            "A new reference is an unreviewed path to the privileged executor. Offending files: " +
            string.Join(", ", offenders));
    }

    private static string FindDashboardSourceDir()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "Dashboard", "Services", "Remediation", "ForcePlanHandler.cs");
            if (File.Exists(candidate))
                return Path.Combine(dir.FullName, "Dashboard");
            dir = dir.Parent;
        }
        throw new DirectoryNotFoundException("Could not locate the Dashboard source directory from " + AppContext.BaseDirectory);
    }

    private sealed class FakeExecutor : IRemediationExecutor
    {
        public bool AuditTableExists = true;
        public bool PriorForce = true;
        public bool AuditWriteResult = true;
        public Func<string, long, long, TargetPreflight>? PreflightFunc;
        public Func<string, long, long, ForcePlanOutcome>? ForceFunc;

        public int ForceCalls;
        public int UnforceCalls;
        public readonly List<RemediationAuditRecord> AuditRecords = new();

        public Task<TargetPreflight> PreflightForcePlanAsync(string database, long queryId, long planId, CancellationToken ct)
            => Task.FromResult(PreflightFunc?.Invoke(database, queryId, planId) ?? new TargetPreflight
            {
                Database = database, QueryId = queryId, PlanId = planId,
                CurrentDatabase = database, HasAlter = true, QueryStoreState = "READ_WRITE", PlanPresent = true
            });

        public Task<bool> AuditTableExistsAsync(CancellationToken ct) => Task.FromResult(AuditTableExists);

        public Task<bool> HasPriorForceAsync(string database, long queryId, long planId, CancellationToken ct)
            => Task.FromResult(PriorForce);

        public Task<ForcePlanOutcome> ForcePlanAsync(string database, long queryId, long planId, RemediationIdentity identity, CancellationToken ct)
        {
            ForceCalls++;
            return Task.FromResult(ForceFunc?.Invoke(database, queryId, planId) ?? new ForcePlanOutcome
            {
                Database = database, QueryId = queryId, PlanId = planId,
                Status = RemediationStatus.Success, Forced = true, ExecutingLogin = "sa", GateSpid = 55, ExecSpid = 55
            });
        }

        public Task<ForcePlanOutcome> UnforcePlanAsync(string database, long queryId, long planId, RemediationIdentity identity, CancellationToken ct)
        {
            UnforceCalls++;
            return Task.FromResult(new ForcePlanOutcome
            {
                Database = database, QueryId = queryId, PlanId = planId,
                Status = RemediationStatus.Success, Forced = true, ExecutingLogin = "sa", GateSpid = 55, ExecSpid = 55
            });
        }

        // ── DB-config seams (Phase 2) ────────────────────────────────────────────
        public Func<string, DbConfigSetting, DbConfigPreflight>? DbPreflightFunc;
        public Func<string, DbConfigSetting, DbConfigOutcome>? SetDbFunc;
        public int SetDbCalls;

        public Task<DbConfigPreflight> PreflightDbConfigAsync(string database, DbConfigSetting setting, CancellationToken ct)
            => Task.FromResult(DbPreflightFunc?.Invoke(database, setting) ?? new DbConfigPreflight
            {
                Database = database, Setting = setting, DatabaseExists = true, HasAlter = true,
                AlreadyInDesiredState = false, ExecutingLogin = "sa", CurrentValue = "ON"
            });

        public Task<DbConfigOutcome> SetDatabaseOptionAsync(string database, DbConfigSetting setting, RemediationIdentity identity, CancellationToken ct)
        {
            SetDbCalls++;
            return Task.FromResult(SetDbFunc?.Invoke(database, setting) ?? new DbConfigOutcome
            {
                Database = database, Setting = setting, Status = RemediationStatus.Success, Applied = true,
                ExecutingLogin = "sa", PriorValue = "ON", GeneratedSql = "ALTER DATABASE [x] SET AUTO_SHRINK OFF;",
                GateSpid = 55, ExecSpid = 55
            });
        }

        // ── Clear-cached-plan seam (DBCC FREEPROCCACHE) ──────────────────────────
        public Func<string, ClearPlanOutcome>? ClearPlanFunc;
        public int ClearPlanCalls;

        public Task<ClearPlanOutcome> ClearProcCacheAsync(string queryHash, RemediationIdentity identity, CancellationToken ct)
        {
            ClearPlanCalls++;
            return Task.FromResult(ClearPlanFunc?.Invoke(queryHash) ?? new ClearPlanOutcome
            {
                QueryHash = queryHash, Status = RemediationStatus.Success, Cleared = true, HandlesCleared = 1,
                ExecutingLogin = "sa", Message = "Cleared 1 cached plan(s).",
                GeneratedSql = "DBCC FREEPROCCACHE(0xDEADBEEF);", PriorValue = "1 plan(s) cached for this query hash",
                GateSpid = 55, ExecSpid = 55
            });
        }

        // ── Server-config seam (sp_configure MAXDOP/CTFP + RECONFIGURE) ───────────
        // Default-only here; the behavioural server-config coverage (and its own injectable
        // fake) lives in ServerConfigHandlerTests. These satisfy the interface so this shared
        // fake still compiles.
        public Task<ServerConfigPreflight> PreflightServerConfigAsync(ServerConfigSetting setting, long recommendedValue, CancellationToken ct)
            => Task.FromResult(new ServerConfigPreflight
            {
                Setting = setting, RecommendedValue = recommendedValue,
                Executable = setting is ServerConfigSetting.Maxdop or ServerConfigSetting.CostThreshold,
                HasPermission = true, AlreadyInDesiredState = false, ExecutingLogin = "sa", CurrentValue = 0
            });

        public Task<ServerConfigOutcome> SetServerConfigAsync(ServerConfigSetting setting, long value, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new ServerConfigOutcome
            {
                Setting = setting, Status = RemediationStatus.Success, Applied = true, ExecutingLogin = "sa",
                PriorValue = 0, GeneratedSql = "EXEC sys.sp_configure N'show advanced options', 1; RECONFIGURE; EXEC sys.sp_configure N'max degree of parallelism', @value; RECONFIGURE;",
                GateSpid = 55, ExecSpid = 55
            });

        // ── Percent-autogrowth seam (ALTER DATABASE … MODIFY FILE FILEGROWTH) ─────
        // Default-only here; the behavioural file-growth coverage (and its own injectable
        // fake) lives in FileAutogrowthHandlerTests. These satisfy the interface so this
        // shared force-plan/DB-config/clear-plan fake still compiles.
        public Task<FileGrowthPreflight> PreflightFileGrowthAsync(string database, string logicalFileName, int growthMb, CancellationToken ct)
            => Task.FromResult(new FileGrowthPreflight
            {
                Database = database, LogicalFileName = logicalFileName, RecommendedGrowthMb = growthMb,
                DatabaseExists = true, FileExists = true, HasAlter = true, AlreadyInDesiredState = false,
                ExecutingLogin = "sa", CurrentValue = "percent"
            });

        public Task<FileGrowthOutcome> SetFileGrowthAsync(string database, string logicalFileName, int growthMb, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new FileGrowthOutcome
            {
                Database = database, LogicalFileName = logicalFileName, Status = RemediationStatus.Success, Applied = true,
                ExecutingLogin = "sa", PriorValue = "percent",
                GeneratedSql = $"ALTER DATABASE [{database}] MODIFY FILE (NAME = [{logicalFileName}], FILEGROWTH = {growthMb}MB);",
                GateSpid = 55, ExecSpid = 55
            });

        public Task<bool> WriteAuditAsync(RemediationAuditRecord record, CancellationToken ct)
        {
            AuditRecords.Add(record);
            return Task.FromResult(AuditWriteResult);
        }
    }
}
