using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services.Remediation;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// PR-B coverage for the gated Apply Fix orchestrator: the confirm gate (the
/// privileged handler is unreachable unless confirm() returns true), M3 fail-closed
/// server resolution, applied-but-unlogged permanent-vs-transient surfacing (LOW-2),
/// and the AlertDetailWindow view-model gating (CanApply).
/// </summary>
public class RemediationApplyServiceTests
{
    private static readonly ServerConnection Server =
        new() { Id = "11111111-1111-1111-1111-111111111111", ServerName = "SQL2022", DisplayName = "Prod SQL2022" };

    private static RemediationAction ForceAction(double regression = 7.5) =>
        new("PLAN_REGRESSION", "force", new List<ForcePlanTarget> { new("AdventureWorks", 4242, 17, RegressionFactor: regression) });

    private static RemediationApplyService BuildService(
        FakeExecutor exec,
        IRemediationHandler? handler = null,
        Func<ServerConnection, CancellationToken, Task<AuditWriteFailureKind>>? classifier = null)
    {
        var handlers = handler is null ? new[] { (IRemediationHandler)new ForcePlanHandler() } : new[] { handler };
        var registry = new RemediationHandlerRegistry(handlers);
        return new RemediationApplyService(serverManager: null!, registry, _ => exec, classifier);
    }

    private static RemediationApplyService BuildServiceNoHandler(FakeExecutor exec) =>
        new(serverManager: null!, new RemediationHandlerRegistry(Array.Empty<IRemediationHandler>()), _ => exec, null);

    // ── Confirm gate ────────────────────────────────────────────────────────────

    [Fact]
    public async Task Gate_ConfirmFalse_NeverReachesHandler()
    {
        var exec = new FakeExecutor();
        var service = BuildService(exec);

        var report = await service.ApplyAsync(
            ForceAction(), Server, previewSql: "preview", operatorIdentity: "DOM\\op", sourceAlertRef: "ref",
            confirm: _ => Task.FromResult(false), CancellationToken.None);

        Assert.Equal(RemediationRunStatus.NotConfirmed, report.Status);
        Assert.Empty(report.Targets);
        Assert.Equal(0, exec.ForceCalls);          // the privileged path was never entered
        Assert.Empty(exec.AuditRecords);
    }

    [Fact]
    public async Task Gate_ConfirmTrue_ReachesHandlerExactlyOnce()
    {
        var exec = new FakeExecutor();
        var confirmCalls = 0;
        var service = BuildService(exec);

        var report = await service.ApplyAsync(
            ForceAction(), Server, "preview", "DOM\\op", "ref",
            confirm: req => { confirmCalls++; return Task.FromResult(true); }, CancellationToken.None);

        Assert.Equal(1, confirmCalls);
        Assert.Equal(RemediationRunStatus.Ran, report.Status);
        Assert.Equal(1, exec.ForceCalls);
        Assert.Equal(RemediationStatus.Success, Assert.Single(report.Targets).Status);
    }

    [Fact]
    public async Task Gate_ConfirmRequest_CarriesServerRegressionAndCaveat()
    {
        var exec = new FakeExecutor();
        var service = BuildService(exec);
        RemediationConfirmRequest? captured = null;

        await service.ApplyAsync(ForceAction(regression: 9.0), Server, "preview", "DOM\\op", "ref",
            confirm: req => { captured = req; return Task.FromResult(false); }, CancellationToken.None);

        Assert.NotNull(captured);
        Assert.Equal("Prod SQL2022", captured!.ServerDisplayName);
        Assert.Equal("preview", captured.PreviewSql);
        Assert.Equal(9.0, Assert.Single(captured.Targets).RegressionFactor);
        Assert.Contains("still the better choice", RemediationConfirmRequest.StillBetterCaveat);
    }

    [Fact]
    public async Task Apply_NoHandler_ReturnsNoHandler_NeverConfirms()
    {
        var exec = new FakeExecutor();
        var service = BuildServiceNoHandler(exec);
        var confirmed = false;

        var report = await service.ApplyAsync(ForceAction(), Server, "preview", "DOM\\op", "ref",
            confirm: _ => { confirmed = true; return Task.FromResult(true); }, CancellationToken.None);

        Assert.Equal(RemediationRunStatus.NoHandler, report.Status);
        Assert.False(confirmed);
        Assert.Equal(0, exec.ForceCalls);
    }

    [Fact]
    public async Task Unapply_ConfirmTrue_ReachesUnforce()
    {
        var exec = new FakeExecutor { PriorForce = true };
        var service = BuildService(exec);

        var report = await service.UnapplyAsync(ForceAction(), Server, "DOM\\op", "ref",
            confirm: _ => Task.FromResult(true), CancellationToken.None);

        Assert.Equal(RemediationRunStatus.Ran, report.Status);
        Assert.True(report.IsUnapply);
        Assert.Equal(1, exec.UnforceCalls);
    }

    // ── Apply-only enforcement (m-1 / m-C): un-apply for SupportsUnapply==false ──

    [Fact]
    public async Task Unapply_HandlerDoesNotSupport_ShortCircuits_NeverConfirms_NeverReachesHandler()
    {
        var exec = new FakeExecutor();
        var handler = new DbConfigHandler();        // SupportsUnapply == false
        var service = new RemediationApplyService(serverManager: null!,
            new RemediationHandlerRegistry(new IRemediationHandler[] { handler }), _ => exec, null);

        var confirmed = false;
        var dbConfigAction = new RemediationAction("DB_CONFIG", "set",
            Array.Empty<ForcePlanTarget>(),
            new List<DbConfigTarget> { new("Foo", DbConfigSetting.AutoShrinkOff, "ON") });

        // Even if a future mis-wired caller invokes UnapplyAsync, it must fail SAFE
        // (clean report, no NotSupportedException) and never confirm or mutate.
        var report = await service.UnapplyAsync(dbConfigAction, Server, "DOM\\op", "ref",
            confirm: _ => { confirmed = true; return Task.FromResult(true); }, CancellationToken.None);

        Assert.Equal(RemediationRunStatus.UnapplyNotSupported, report.Status);
        Assert.True(report.IsUnapply);
        Assert.False(confirmed);                    // the gate was never even shown
        Assert.Equal(0, exec.SetDbCalls);
        Assert.Empty(exec.AuditRecords);
    }

    [Fact]
    public async Task Apply_DbConfig_ConfirmRequest_RendersDbConfigRows_NoQueryId()
    {
        var exec = new FakeExecutor();
        var service = new RemediationApplyService(serverManager: null!,
            new RemediationHandlerRegistry(new IRemediationHandler[] { new DbConfigHandler() }), _ => exec, null);
        RemediationConfirmRequest? captured = null;

        var action = new RemediationAction("DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            new List<DbConfigTarget> { new("Foo", DbConfigSetting.AutoShrinkOff, "ON") });

        await service.ApplyAsync(action, Server, previewSql: null, "DOM\\op", "ref",
            confirm: req => { captured = req; return Task.FromResult(false); }, CancellationToken.None);

        Assert.NotNull(captured);
        Assert.Equal("DB_CONFIG", captured!.FactKey);
        var row = Assert.Single(captured.Targets);
        Assert.Contains("AUTO_SHRINK OFF", row.StatusTitle);
        Assert.True(captured.AnyActionable);        // a DB_CONFIG Ok is actionable
        // The fallback preview renders the ALTER statement, not sp_query_store_*.
        Assert.Contains("ALTER DATABASE [Foo] SET AUTO_SHRINK OFF;", captured.PreviewSql);
        Assert.DoesNotContain("sp_query_store", captured.PreviewSql);
    }

    [Fact]
    public async Task Apply_DbConfig_AllAlreadyDesired_NotActionable()
    {
        // Preflight all-AlreadyInDesiredState => AnyActionable false => Apply disabled.
        var exec = new FakeExecutor();   // PreflightDbConfigAsync returns Ok by default;
        // override via a handler driven by a preflight that's already-desired:
        var service = new RemediationApplyService(serverManager: null!,
            new RemediationHandlerRegistry(new IRemediationHandler[] { new DbConfigHandler() }),
            _ => new AlreadyDesiredExecutor(), null);
        RemediationConfirmRequest? captured = null;

        var action = new RemediationAction("DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            new List<DbConfigTarget> { new("Foo", DbConfigSetting.AutoShrinkOff, "OFF") });

        await service.ApplyAsync(action, Server, previewSql: null, "DOM\\op", "ref",
            confirm: req => { captured = req; return Task.FromResult(false); }, CancellationToken.None);

        Assert.NotNull(captured);
        Assert.False(captured!.AnyActionable);
        Assert.Equal(RemediationDisposition.AlreadyInDesiredState, Assert.Single(captured.Targets).Disposition);
    }

    // ── LOW-2: applied-but-unlogged permanent vs transient ───────────────────────

    [Fact]
    public async Task Apply_AppliedButUnlogged_PermanentClassification_Surfaced()
    {
        var exec = new FakeExecutor { AuditWriteResult = false };   // force succeeds, audit INSERT fails
        var classifierCalls = 0;
        var service = BuildService(exec, classifier: (_, _) => { classifierCalls++; return Task.FromResult(AuditWriteFailureKind.Permanent); });

        var report = await service.ApplyAsync(ForceAction(), Server, "preview", "DOM\\op", "ref",
            confirm: _ => Task.FromResult(true), CancellationToken.None);

        var t = Assert.Single(report.Targets);
        Assert.True(t.AppliedButUnlogged);
        Assert.Equal(AuditWriteFailureKind.Permanent, t.AuditFailureKind);
        Assert.Equal(1, classifierCalls);
    }

    [Fact]
    public async Task Apply_AppliedButUnlogged_TransientClassification_Surfaced()
    {
        var exec = new FakeExecutor { AuditWriteResult = false };
        var service = BuildService(exec, classifier: (_, _) => Task.FromResult(AuditWriteFailureKind.Transient));

        var report = await service.ApplyAsync(ForceAction(), Server, "preview", "DOM\\op", "ref",
            confirm: _ => Task.FromResult(true), CancellationToken.None);

        var t = Assert.Single(report.Targets);
        Assert.True(t.AppliedButUnlogged);
        Assert.Equal(AuditWriteFailureKind.Transient, t.AuditFailureKind);
    }

    [Fact]
    public async Task Apply_Success_AuditWritten_DoesNotClassify()
    {
        var exec = new FakeExecutor { AuditWriteResult = true };
        var classifierCalls = 0;
        var service = BuildService(exec, classifier: (_, _) => { classifierCalls++; return Task.FromResult(AuditWriteFailureKind.Permanent); });

        var report = await service.ApplyAsync(ForceAction(), Server, "preview", "DOM\\op", "ref",
            confirm: _ => Task.FromResult(true), CancellationToken.None);

        var t = Assert.Single(report.Targets);
        Assert.False(t.AppliedButUnlogged);
        Assert.Equal(AuditWriteFailureKind.None, t.AuditFailureKind);
        Assert.Equal(0, classifierCalls);          // classifier only runs for an unlogged target
    }

    // ── M3 fail-closed server resolution ─────────────────────────────────────────

    private static List<ServerConnection> Servers(params (string id, string name)[] defs) =>
        defs.Select(d => new ServerConnection { Id = d.id, ServerName = d.name, DisplayName = d.name }).ToList();

    [Fact]
    public void Resolve_GuidMatch_Resolves()
    {
        var servers = Servers(("guid-a", "SQL2022"), ("guid-b", "SQL2019"));
        var r = RemediationApplyService.ResolveServer("guid-a", "SQL2022", servers);

        Assert.True(r.IsResolved);
        Assert.False(r.ResolvedByName);
        Assert.Equal("guid-a", r.Server!.Id);
    }

    [Fact]
    public void Resolve_IntIdFallback_GuidMiss_ResolvesByUniqueName()
    {
        // The notify-time resolver wrote the finding's int id ("3"); GetServerById misses.
        var servers = Servers(("guid-a", "SQL2022"), ("guid-b", "SQL2019"));
        var r = RemediationApplyService.ResolveServer("3", "SQL2022", servers);

        Assert.True(r.IsResolved);
        Assert.True(r.ResolvedByName);
        Assert.Equal("guid-a", r.Server!.Id);
    }

    [Fact]
    public void Resolve_EmptyServerId_ResolvesByUniqueName()
    {
        var servers = Servers(("guid-a", "SQL2022"));
        var r = RemediationApplyService.ResolveServer("", "SQL2022", servers);

        Assert.True(r.IsResolved);
        Assert.True(r.ResolvedByName);
    }

    [Fact]
    public void Resolve_AmbiguousByName_FailsClosed()
    {
        var servers = Servers(("guid-a", "SQL2022"), ("guid-b", "SQL2022"));
        var r = RemediationApplyService.ResolveServer("3", "SQL2022", servers);

        Assert.False(r.IsResolved);
        Assert.Null(r.Server);
        Assert.Contains("unambiguously", r.Reason);
    }

    [Fact]
    public void Resolve_Unresolved_FailsClosed()
    {
        var servers = Servers(("guid-a", "SQL2022"));
        var r = RemediationApplyService.ResolveServer("3", "GhostServer", servers);

        Assert.False(r.IsResolved);
        Assert.Null(r.Server);
        Assert.False(string.IsNullOrEmpty(r.Reason));
    }

    [Fact]
    public void HasHandlerFor_KnownAndUnknown()
    {
        var service = BuildService(new FakeExecutor());
        Assert.True(service.HasHandlerFor("PLAN_REGRESSION"));
        Assert.False(service.HasHandlerFor("PARAMETER_SENSITIVITY"));
        Assert.False(service.HasHandlerFor(null));
    }

    // ── AlertDetailWindow view-model gating (CanApply) ───────────────────────────

    [Fact]
    public void CanApply_RequiresKnownFix_ResolvedServer_AndNotBusy()
    {
        // Known fix + resolved server -> enabled.
        var enabled = new AlertDetailWindow.DetailItemView { ShowApply = true };
        enabled.SetServerResolved(true);
        Assert.True(enabled.CanApply);

        // Resolved-but-unknown-fix (ShowApply false) -> never enabled.
        var noFix = new AlertDetailWindow.DetailItemView { ShowApply = false };
        noFix.SetServerResolved(true);
        Assert.False(noFix.CanApply);

        // Known fix but server unresolved (M3) -> hard-disabled.
        var unresolved = new AlertDetailWindow.DetailItemView { ShowApply = true };
        unresolved.SetServerResolved(false);
        Assert.False(unresolved.CanApply);

        // Mid-run -> disabled.
        var busy = new AlertDetailWindow.DetailItemView { ShowApply = true };
        busy.SetServerResolved(true);
        busy.BeginRun("Applying…");
        Assert.False(busy.CanApply);
    }

    // ── B3 Phase 3: informed-consent request threading (B-1) ─────────────────────
    //
    // The confirm dialog IS the trust boundary; the service trusts the callback. PR-A
    // populates the request: RequiresInformedConsent = handler.IsDestructive and, when
    // destructive, the two-sided Risks (FactRiskDisclosure). The XAML acknowledge-each-
    // risk RENDERING/gating is PR-B. These tests verify the request is populated
    // correctly so the PR-B dialog has what it needs.

    private static RemediationAction RcsiAction() =>
        new("RCSI", "set", Array.Empty<ForcePlanTarget>(),
            new List<DbConfigTarget> { new("Foo", DbConfigSetting.ReadCommittedSnapshotOn, "OFF") });

    private static AnalysisFinding RcsiFinding(int? rwPct = 80) => new()
    {
        ServerId = 1, ServerName = "SQL2022", Category = "config_issues",
        StoryPath = "DB_CONFIG", StoryPathHash = "dbconfig00000099", RootFactKey = "DB_CONFIG",
        DrillDown = new Dictionary<string, object>
        {
            ["config_issues"] = new List<object>
            {
                new { database = "Foo", rcsi = false, query_store = true, auto_shrink = false,
                      auto_close = false, page_verify = "CHECKSUM", issues = new[] { "RCSI OFF" },
                      rcsi_blocking_events = 12, rcsi_deadlocks = 3, rcsi_reader_writer_pct = rwPct }
            }
        }
    };

    [Fact]
    public async Task Apply_Destructive_Request_RequiresInformedConsent_CarriesRisks()
    {
        var exec = new FakeExecutor();
        var service = new RemediationApplyService(serverManager: null!,
            new RemediationHandlerRegistry(new IRemediationHandler[] { new RcsiHandler() }), _ => exec, null);
        RemediationConfirmRequest? captured = null;

        await service.ApplyAsync(RcsiAction(), Server, previewSql: "ALTER DATABASE [Foo] SET READ_COMMITTED_SNAPSHOT ON;",
            "DOM\\op", "ref", confirm: req => { captured = req; return Task.FromResult(false); },
            CancellationToken.None, finding: RcsiFinding());

        Assert.NotNull(captured);
        Assert.Equal("RCSI", captured!.FactKey);
        Assert.True(captured.RequiresInformedConsent);
        Assert.NotNull(captured.Risks);
        Assert.NotEmpty(captured.Risks!.RisksOfChanging);
        Assert.NotEmpty(captured.Risks.RisksOfNotChanging);
        // The RCSI confirm row title is the friendly one (m-2), not the raw enum name.
        Assert.Contains("Read Committed Snapshot Isolation", Assert.Single(captured.Targets).StatusTitle);
    }

    [Fact]
    public async Task Apply_NonDestructive_Request_NoConsent_NoRisks()
    {
        var exec = new FakeExecutor();
        var service = new RemediationApplyService(serverManager: null!,
            new RemediationHandlerRegistry(new IRemediationHandler[] { new DbConfigHandler() }), _ => exec, null);
        RemediationConfirmRequest? captured = null;

        var action = new RemediationAction("DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            new List<DbConfigTarget> { new("Foo", DbConfigSetting.AutoShrinkOff, "ON") });

        await service.ApplyAsync(action, Server, previewSql: null, "DOM\\op", "ref",
            confirm: req => { captured = req; return Task.FromResult(false); }, CancellationToken.None);

        Assert.NotNull(captured);
        Assert.False(captured!.RequiresInformedConsent);
        Assert.Null(captured.Risks);
    }

    [Fact]
    public async Task Apply_Destructive_WriterWriter_Risks_SayRcsiWontResolve()
    {
        // The honest-both-directions property survives the service threading: a low
        // reader/writer pct yields the "RCSI does NOT resolve this" inaction line.
        var exec = new FakeExecutor();
        var service = new RemediationApplyService(serverManager: null!,
            new RemediationHandlerRegistry(new IRemediationHandler[] { new RcsiHandler() }), _ => exec, null);
        RemediationConfirmRequest? captured = null;

        await service.ApplyAsync(RcsiAction(), Server, previewSql: "preview", "DOM\\op", "ref",
            confirm: req => { captured = req; return Task.FromResult(false); },
            CancellationToken.None, finding: RcsiFinding(rwPct: 8));

        Assert.Contains(captured!.Risks!.RisksOfNotChanging, r => r.Text.Contains("RCSI does NOT resolve"));
    }

    // ── Clear-cached-plan: destructive request threading + reachability (PR-B) ──────

    private static RemediationAction ClearPlanAction() =>
        new("CLEAR_PLAN", "clear", Array.Empty<ForcePlanTarget>(),
            ClearPlanTargets: new[] { new ClearPlanTarget("AdventureWorks", "0xABCDEF0123456789", 45.0, 9.0, 5.0, "0x06") },
            ClearPlanFigures: new ClearPlanFigures(45.0, 9.0, 5.0, 62, false, false));

    [Fact]
    public async Task Apply_ClearPlan_RequiresInformedConsent_CarriesRisks_AndPerQueryTarget()
    {
        var exec = new FakeExecutor();
        var service = new RemediationApplyService(serverManager: null!,
            new RemediationHandlerRegistry(new IRemediationHandler[] { new ClearPlanHandler() }), _ => exec, null);
        RemediationConfirmRequest? captured = null;

        await service.ApplyAsync(ClearPlanAction(), Server, previewSql: "DBCC FREEPROCCACHE(<resolved>);",
            "DOM\\op", "ref", confirm: req => { captured = req; return Task.FromResult(false); },
            CancellationToken.None);

        Assert.NotNull(captured);
        Assert.Equal("CLEAR_PLAN", captured!.FactKey);
        Assert.True(captured.RequiresInformedConsent);
        Assert.NotNull(captured.Risks);
        Assert.NotEmpty(captured.Risks!.RisksOfChanging);
        Assert.NotEmpty(captured.Risks.RisksOfNotChanging);
        // The confirm preview carries the per-query target (the per-HANDLE list is resolved
        // live at apply); it shows the query hash + the anomaly figures.
        var target = Assert.Single(captured.Targets);
        Assert.Contains("0xABCDEF0123456789", target.StatusTitle);
        Assert.Contains("5.0x", target.StatusTitle);
        // Gate refused (confirm == false): the privileged DBCC path was never entered.
        Assert.Equal(0, exec.ClearPlanCalls);
    }

    [Fact]
    public async Task Apply_ClearPlan_ConfirmTrue_ReachesClearProcCache_NotForceOrSetDb()
    {
        var exec = new FakeExecutor();
        var service = new RemediationApplyService(serverManager: null!,
            new RemediationHandlerRegistry(new IRemediationHandler[] { new ClearPlanHandler() }), _ => exec, null);

        var report = await service.ApplyAsync(ClearPlanAction(), Server, previewSql: "preview",
            "DOM\\op", "ref", confirm: _ => Task.FromResult(true), CancellationToken.None);

        Assert.Equal(RemediationRunStatus.Ran, report.Status);
        Assert.Equal(1, exec.ClearPlanCalls);   // routed to the DBCC executor method
        Assert.Equal(0, exec.ForceCalls);       // NOT the force-plan path
        Assert.Equal(0, exec.SetDbCalls);       // NOT the always-safe DB-config path
    }

    [Fact]
    public async Task Apply_AlwaysSafe_DbConfig_CannotExecute_ClearPlanTarget()
    {
        // Cross-routing guard: an action that (illegitimately) carries CLEAR_PLAN targets but
        // is keyed to the always-safe DB_CONFIG handler must NOT reach the DBCC executor.
        // DbConfigHandler iterates ONLY DbConfigTargets — a CLEAR_PLAN payload is inert to it,
        // and the registry would never hand a CLEAR_PLAN fact key to DbConfigHandler anyway.
        var exec = new FakeExecutor();
        var service = new RemediationApplyService(serverManager: null!,
            new RemediationHandlerRegistry(new IRemediationHandler[] { new DbConfigHandler() }), _ => exec, null);

        var crossAction = new RemediationAction("DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            DbConfigTargets: null,
            RcsiFigures: null,
            ClearPlanTargets: new[] { new ClearPlanTarget("AdventureWorks", "0xABCDEF0123456789") });

        await service.ApplyAsync(crossAction, Server, previewSql: "preview", "DOM\\op", "ref",
            confirm: _ => Task.FromResult(true), CancellationToken.None);

        Assert.Equal(0, exec.ClearPlanCalls);   // the DBCC path is unreachable via DB_CONFIG
    }

    // ── HARD gate-enforcement (B-1, MANDATORY): the acknowledge-each-risk predicate ──
    //
    // The dialog IS the trust boundary; the confirm callback returns true ONLY when the
    // gate is satisfied. The dialog's exact enablement predicate is the pure, testable
    // RemediationConfirmWindow.ComputeConfirmEnabled. These prove: a destructive request
    // keeps Apply DISABLED until ALL risk checkboxes are checked; a subset leaves it
    // disabled; un-checking any one re-disables; and the by-name ack combines (BOTH
    // required for a destructive by-name target).

    [Fact]
    public void Gate_Destructive_Disabled_UntilAllRiskBoxesChecked()
    {
        // 5 risk boxes (e.g. 3 changing + 2 not-changing). Disabled until ALL are ticked.
        for (var checkedCount = 0; checkedCount < 5; checkedCount++)
        {
            var allChecked = checkedCount == 5;   // never true in this loop (0..4)
            Assert.False(RemediationConfirmWindow.ComputeConfirmEnabled(
                baseActionable: true, requiresConsent: true, allRiskBoxesChecked: allChecked,
                resolvedByName: false, byNameAck: false, riskBoxCount: 5),
                $"Apply must stay DISABLED with a subset ({checkedCount}/5) of risk boxes checked.");
        }

        // All boxes checked -> enabled (no by-name complication).
        Assert.True(RemediationConfirmWindow.ComputeConfirmEnabled(
            baseActionable: true, requiresConsent: true, allRiskBoxesChecked: true,
            resolvedByName: false, byNameAck: false, riskBoxCount: 5));
    }

    [Fact]
    public void Gate_Destructive_UncheckingAnyBox_ReDisables()
    {
        // Enabled with all checked...
        Assert.True(RemediationConfirmWindow.ComputeConfirmEnabled(
            baseActionable: true, requiresConsent: true, allRiskBoxesChecked: true,
            resolvedByName: false, byNameAck: false, riskBoxCount: 4));
        // ...then un-checking ANY box (allRiskBoxesChecked flips false) re-disables.
        Assert.False(RemediationConfirmWindow.ComputeConfirmEnabled(
            baseActionable: true, requiresConsent: true, allRiskBoxesChecked: false,
            resolvedByName: false, byNameAck: false, riskBoxCount: 4));
    }

    [Fact]
    public void Gate_Destructive_ByName_RequiresBoth_RiskBoxesAndByNameAck()
    {
        // Risk boxes all checked but by-name NOT acked -> still disabled.
        Assert.False(RemediationConfirmWindow.ComputeConfirmEnabled(
            baseActionable: true, requiresConsent: true, allRiskBoxesChecked: true,
            resolvedByName: true, byNameAck: false, riskBoxCount: 4));
        // By-name acked but a risk box still unchecked -> still disabled.
        Assert.False(RemediationConfirmWindow.ComputeConfirmEnabled(
            baseActionable: true, requiresConsent: true, allRiskBoxesChecked: false,
            resolvedByName: true, byNameAck: true, riskBoxCount: 4));
        // BOTH satisfied -> enabled.
        Assert.True(RemediationConfirmWindow.ComputeConfirmEnabled(
            baseActionable: true, requiresConsent: true, allRiskBoxesChecked: true,
            resolvedByName: true, byNameAck: true, riskBoxCount: 4));
    }

    [Fact]
    public void Gate_NotActionable_NeverEnabled_EvenWithAllConsent()
    {
        // Audit-absent / nothing-applyable: baseActionable false hard-blocks regardless
        // of consent (the consent gate is ADDITIVE to AnyActionable, never a replacement).
        Assert.False(RemediationConfirmWindow.ComputeConfirmEnabled(
            baseActionable: false, requiresConsent: true, allRiskBoxesChecked: true,
            resolvedByName: false, byNameAck: false, riskBoxCount: 4));
    }

    [Fact]
    public void Gate_Destructive_ZeroRiskBoxes_FailsClosed()
    {
        // FAIL CLOSED (LOW-1): a destructive (requiresConsent) request with NO rendered
        // risk boxes must keep Apply DISABLED, even though allRiskBoxesChecked is vacuously
        // true (List.TrueForAll on an empty list) and the base apply-ability holds. A
        // future destructive handler whose disclosure is empty/null can never enable Apply
        // with zero acknowledged checkboxes.
        Assert.False(RemediationConfirmWindow.ComputeConfirmEnabled(
            baseActionable: true, requiresConsent: true, allRiskBoxesChecked: true,
            resolvedByName: false, byNameAck: false, riskBoxCount: 0));
        // Still fails closed even if the (irrelevant) by-name ack is satisfied.
        Assert.False(RemediationConfirmWindow.ComputeConfirmEnabled(
            baseActionable: true, requiresConsent: true, allRiskBoxesChecked: true,
            resolvedByName: true, byNameAck: true, riskBoxCount: 0));
        // One real risk box, all checked -> enabled (the guard only blocks the empty case).
        Assert.True(RemediationConfirmWindow.ComputeConfirmEnabled(
            baseActionable: true, requiresConsent: true, allRiskBoxesChecked: true,
            resolvedByName: false, byNameAck: false, riskBoxCount: 1));
    }

    [Fact]
    public void Gate_NonDestructive_Unaffected_ByConsentArm()
    {
        // A non-destructive (requiresConsent false) request ignores the risk-box arm:
        // base actionable + (by-name ack if resolved-by-name) is the whole predicate,
        // exactly as before Phase 3 — no regression for force-plan / always-safe.
        Assert.True(RemediationConfirmWindow.ComputeConfirmEnabled(
            baseActionable: true, requiresConsent: false, allRiskBoxesChecked: false,
            resolvedByName: false, byNameAck: false, riskBoxCount: 0));
        Assert.False(RemediationConfirmWindow.ComputeConfirmEnabled(
            baseActionable: true, requiresConsent: false, allRiskBoxesChecked: false,
            resolvedByName: true, byNameAck: false, riskBoxCount: 0));
    }

    // ── Real figures survive persistence to apply time (CRITICAL correctness) ─────
    //
    // The UI apply call site passes NO finding; only the persisted RemediationAction
    // survives. The RCSI figures must therefore ride the action through the AlertContext
    // serialize -> deserialize round-trip, so the dialog shows the REAL blocking numbers,
    // not the weak-case baseline.

    [Fact]
    public async Task Apply_Destructive_RealFigures_SurvivePersistence_NoFindingAtApplyTime()
    {
        // 1. Build the action the way AnalysisNotificationService does (finding in hand).
        var finding = RcsiFinding(rwPct: 80);
        var builtAction = FactRemediation.BuildRcsiAction(finding);
        Assert.NotNull(builtAction);
        Assert.NotNull(builtAction!.RcsiFigures);
        Assert.Equal(12, builtAction.RcsiFigures!.BlockingEvents);

        // 2. Round-trip it through the persisted AlertContext (the only thing that
        //    survives to the UI apply call site).
        var ctx = new AlertContext();
        ctx.Details.Add(new AlertDetailItem { Heading = "Enable RCSI (advanced)", IsCodeBlock = true, Remediation = builtAction });
        Assert.True(AlertContextSerializer.TryDeserialize(AlertContextSerializer.Serialize(ctx), out var round));
        var persistedAction = round.Details[0].Remediation!;
        Assert.NotNull(persistedAction.RcsiFigures);
        Assert.Equal(12, persistedAction.RcsiFigures!.BlockingEvents);
        Assert.Equal(3, persistedAction.RcsiFigures.Deadlocks);
        Assert.Equal(80, persistedAction.RcsiFigures.ReaderWriterPct);

        // 3. Apply with the PERSISTED action and NO finding (exactly the UI call site).
        //    The confirm request's Risks must show the REAL figures, not weak-case.
        var exec = new FakeExecutor();
        var service = new RemediationApplyService(serverManager: null!,
            new RemediationHandlerRegistry(new IRemediationHandler[] { new RcsiHandler() }), _ => exec, null);
        RemediationConfirmRequest? captured = null;

        await service.ApplyAsync(persistedAction, Server, previewSql: "preview", "DOM\\op", "ref",
            confirm: req => { captured = req; return Task.FromResult(false); },
            CancellationToken.None /* finding defaults to null — the UI apply call site */);

        Assert.NotNull(captured!.Risks);
        Assert.Contains(captured.Risks!.RisksOfNotChanging,
            r => r.Text.Contains("12") && r.Text.Contains("blocked-process events") && r.Text.Contains("3 deadlocks"));
        Assert.Contains(captured.Risks.RisksOfNotChanging, r => r.Text.Contains("80%") && r.Text.Contains("RCSI eliminates"));
        // NOT the weak-case baseline (proves the real figures reached the dialog).
        Assert.DoesNotContain(captured.Risks.RisksOfNotChanging, r => r.Text.Contains("Little or no reader/writer blocking"));
    }

    [Fact]
    public void RcsiTargets_OnDbConfigAction_SurviveAlertContextRoundTrip_WithFigures()
    {
        // The per-db RCSI targets carried on a DB_CONFIG action (for the read-time card fan-out)
        // must survive the AlertContext serialize -> deserialize round-trip with their figures
        // intact — otherwise the Recommendations reader fans no RCSI cards after persistence.
        var action = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            DbConfigTargets: new[] { new DbConfigTarget("Sales", DbConfigSetting.AutoShrinkOff, "ON") },
            RcsiTargets: new[]
            {
                new RcsiTarget("Sales", new RcsiInactionFigures(40, 2, 85)),
                new RcsiTarget("Orders", new RcsiInactionFigures(12, 0, null))
            });

        var ctx = new AlertContext();
        ctx.Details.Add(new AlertDetailItem { Heading = "DB config", IsCodeBlock = true, Remediation = action });
        Assert.True(AlertContextSerializer.TryDeserialize(AlertContextSerializer.Serialize(ctx), out var round));
        var persisted = round.Details[0].Remediation!;

        // Safe target preserved...
        Assert.Single(persisted.DbConfigTargets!);
        // ...and the two RCSI targets with their figures.
        Assert.Equal(2, persisted.RcsiTargets!.Count);
        var sales = Assert.Single(persisted.RcsiTargets, t => t.Database == "Sales");
        Assert.Equal(40, sales.Figures.BlockingEvents);
        Assert.Equal(2, sales.Figures.Deadlocks);
        Assert.Equal(85, sales.Figures.ReaderWriterPct);
        var orders = Assert.Single(persisted.RcsiTargets, t => t.Database == "Orders");
        Assert.Equal(12, orders.Figures.BlockingEvents);
        Assert.Null(orders.Figures.ReaderWriterPct);   // nullable pct round-trips as null
    }

    [Fact]
    public async Task Apply_Destructive_NoFiguresNoFinding_ShowsWeakCaseBaseline()
    {
        // Genuinely no data: an action WITHOUT figures + no finding -> weak-case baseline.
        var bareAction = RcsiAction();   // built without RcsiFigures
        Assert.Null(bareAction.RcsiFigures);
        var exec = new FakeExecutor();
        var service = new RemediationApplyService(serverManager: null!,
            new RemediationHandlerRegistry(new IRemediationHandler[] { new RcsiHandler() }), _ => exec, null);
        RemediationConfirmRequest? captured = null;

        await service.ApplyAsync(bareAction, Server, previewSql: "preview", "DOM\\op", "ref",
            confirm: req => { captured = req; return Task.FromResult(false); }, CancellationToken.None);

        Assert.Contains(captured!.Risks!.RisksOfNotChanging, r => r.Text.Contains("Little or no reader/writer blocking"));
    }

    // ── Fake executor (PR-B-local) ───────────────────────────────────────────────

    private sealed class FakeExecutor : IRemediationExecutor
    {
        public bool AuditTableExists = true;
        public bool PriorForce = true;
        public bool AuditWriteResult = true;

        public int ForceCalls;
        public int UnforceCalls;
        public readonly List<RemediationAuditRecord> AuditRecords = new();

        public Task<TargetPreflight> PreflightForcePlanAsync(string database, long queryId, long planId, CancellationToken ct)
            => Task.FromResult(new TargetPreflight
            {
                Database = database, QueryId = queryId, PlanId = planId,
                CurrentDatabase = database, HasAlter = true, QueryStoreState = "READ_WRITE",
                PlanPresent = true, ExecutingLogin = "PerfMonLogin"
            });

        public Task<bool> AuditTableExistsAsync(CancellationToken ct) => Task.FromResult(AuditTableExists);
        public Task<bool> HasPriorForceAsync(string database, long queryId, long planId, CancellationToken ct) => Task.FromResult(PriorForce);

        public Task<ForcePlanOutcome> ForcePlanAsync(string database, long queryId, long planId, RemediationIdentity identity, CancellationToken ct)
        {
            ForceCalls++;
            return Task.FromResult(new ForcePlanOutcome
            {
                Database = database, QueryId = queryId, PlanId = planId,
                Status = RemediationStatus.Success, Forced = true, ExecutingLogin = "PerfMonLogin", GateSpid = 55, ExecSpid = 55
            });
        }

        public Task<ForcePlanOutcome> UnforcePlanAsync(string database, long queryId, long planId, RemediationIdentity identity, CancellationToken ct)
        {
            UnforceCalls++;
            return Task.FromResult(new ForcePlanOutcome
            {
                Database = database, QueryId = queryId, PlanId = planId,
                Status = RemediationStatus.Success, Forced = true, ExecutingLogin = "PerfMonLogin", GateSpid = 55, ExecSpid = 55
            });
        }

        public int SetDbCalls;

        public Task<DbConfigPreflight> PreflightDbConfigAsync(string database, DbConfigSetting setting, CancellationToken ct)
            => Task.FromResult(new DbConfigPreflight
            {
                Database = database, Setting = setting, DatabaseExists = true, HasAlter = true,
                AlreadyInDesiredState = false, ExecutingLogin = "PerfMonLogin", CurrentValue = "ON"
            });

        public Task<DbConfigOutcome> SetDatabaseOptionAsync(string database, DbConfigSetting setting, RemediationIdentity identity, CancellationToken ct)
        {
            SetDbCalls++;
            return Task.FromResult(new DbConfigOutcome
            {
                Database = database, Setting = setting, Status = RemediationStatus.Success, Applied = true,
                ExecutingLogin = "PerfMonLogin", PriorValue = "ON", GeneratedSql = "ALTER DATABASE [x] SET AUTO_SHRINK OFF;",
                GateSpid = 55, ExecSpid = 55
            });
        }

        public int ClearPlanCalls;

        public Task<ClearPlanOutcome> ClearProcCacheAsync(string queryHash, RemediationIdentity identity, CancellationToken ct)
        {
            ClearPlanCalls++;
            return Task.FromResult(new ClearPlanOutcome
            {
                QueryHash = queryHash, Status = RemediationStatus.Success, Cleared = true, HandlesCleared = 1,
                ExecutingLogin = "PerfMonLogin", Message = "Cleared 1 cached plan(s).",
                GeneratedSql = "DBCC FREEPROCCACHE(0xDEADBEEF);", PriorValue = "1 plan(s) cached for this query hash",
                GateSpid = 55, ExecSpid = 55
            });
        }

        public int SetFileCalls;

        public Task<FileGrowthPreflight> PreflightFileGrowthAsync(string database, string logicalFileName, int growthMb, CancellationToken ct)
            => Task.FromResult(new FileGrowthPreflight
            {
                Database = database, LogicalFileName = logicalFileName, RecommendedGrowthMb = growthMb,
                DatabaseExists = true, FileExists = true, HasAlter = true, AlreadyInDesiredState = false,
                ExecutingLogin = "PerfMonLogin", CurrentValue = "percent"
            });

        public Task<FileGrowthOutcome> SetFileGrowthAsync(string database, string logicalFileName, int growthMb, RemediationIdentity identity, CancellationToken ct)
        {
            SetFileCalls++;
            return Task.FromResult(new FileGrowthOutcome
            {
                Database = database, LogicalFileName = logicalFileName, Status = RemediationStatus.Success, Applied = true,
                ExecutingLogin = "PerfMonLogin", PriorValue = "percent",
                GeneratedSql = $"ALTER DATABASE [{database}] MODIFY FILE (NAME = [{logicalFileName}], FILEGROWTH = {growthMb}MB);",
                GateSpid = 55, ExecSpid = 55
            });
        }

        public int SetServerConfigCalls;

        public Task<ServerConfigPreflight> PreflightServerConfigAsync(ServerConfigSetting setting, long recommendedValue, CancellationToken ct)
            => Task.FromResult(new ServerConfigPreflight
            {
                Setting = setting, RecommendedValue = recommendedValue,
                Executable = setting is ServerConfigSetting.Maxdop or ServerConfigSetting.CostThreshold,
                HasPermission = true, AlreadyInDesiredState = false, ExecutingLogin = "PerfMonLogin", CurrentValue = 0
            });

        public Task<ServerConfigOutcome> SetServerConfigAsync(ServerConfigSetting setting, long value, RemediationIdentity identity, CancellationToken ct)
        {
            SetServerConfigCalls++;
            return Task.FromResult(new ServerConfigOutcome
            {
                Setting = setting, Status = RemediationStatus.Success, Applied = true, ExecutingLogin = "PerfMonLogin",
                PriorValue = 0, GeneratedSql = "EXEC sys.sp_configure N'show advanced options', 1; RECONFIGURE; EXEC sys.sp_configure N'max degree of parallelism', @value; RECONFIGURE;",
                GateSpid = 55, ExecSpid = 55
            });
        }

        public Task<bool> WriteAuditAsync(RemediationAuditRecord record, CancellationToken ct)
        {
            AuditRecords.Add(record);
            return Task.FromResult(AuditWriteResult);
        }
    }

    /// <summary>Executor whose DB-config preflight always reports already-in-desired-state.</summary>
    private sealed class AlreadyDesiredExecutor : IRemediationExecutor
    {
        public Task<TargetPreflight> PreflightForcePlanAsync(string database, long queryId, long planId, CancellationToken ct)
            => Task.FromResult(new TargetPreflight { Database = database });
        public Task<bool> AuditTableExistsAsync(CancellationToken ct) => Task.FromResult(true);
        public Task<bool> HasPriorForceAsync(string database, long queryId, long planId, CancellationToken ct) => Task.FromResult(false);
        public Task<ForcePlanOutcome> ForcePlanAsync(string database, long queryId, long planId, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new ForcePlanOutcome { Database = database });
        public Task<ForcePlanOutcome> UnforcePlanAsync(string database, long queryId, long planId, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new ForcePlanOutcome { Database = database });
        public Task<DbConfigPreflight> PreflightDbConfigAsync(string database, DbConfigSetting setting, CancellationToken ct)
            => Task.FromResult(new DbConfigPreflight
            {
                Database = database, Setting = setting, DatabaseExists = true, HasAlter = true,
                AlreadyInDesiredState = true, ExecutingLogin = "PerfMonLogin", CurrentValue = "OFF"
            });
        public Task<DbConfigOutcome> SetDatabaseOptionAsync(string database, DbConfigSetting setting, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new DbConfigOutcome { Database = database, Setting = setting, Status = RemediationStatus.Skipped });
        public Task<ClearPlanOutcome> ClearProcCacheAsync(string queryHash, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new ClearPlanOutcome { QueryHash = queryHash, Status = RemediationStatus.Skipped });
        public Task<FileGrowthPreflight> PreflightFileGrowthAsync(string database, string logicalFileName, int growthMb, CancellationToken ct)
            => Task.FromResult(new FileGrowthPreflight
            {
                Database = database, LogicalFileName = logicalFileName, RecommendedGrowthMb = growthMb,
                DatabaseExists = true, FileExists = true, HasAlter = true, AlreadyInDesiredState = true,
                ExecutingLogin = "PerfMonLogin", CurrentValue = "percent"
            });
        public Task<FileGrowthOutcome> SetFileGrowthAsync(string database, string logicalFileName, int growthMb, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new FileGrowthOutcome { Database = database, LogicalFileName = logicalFileName, Status = RemediationStatus.Skipped });
        public Task<ServerConfigPreflight> PreflightServerConfigAsync(ServerConfigSetting setting, long recommendedValue, CancellationToken ct)
            => Task.FromResult(new ServerConfigPreflight
            {
                Setting = setting, RecommendedValue = recommendedValue,
                Executable = setting is ServerConfigSetting.Maxdop or ServerConfigSetting.CostThreshold,
                HasPermission = true, AlreadyInDesiredState = true, ExecutingLogin = "PerfMonLogin", CurrentValue = recommendedValue
            });
        public Task<ServerConfigOutcome> SetServerConfigAsync(ServerConfigSetting setting, long value, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new ServerConfigOutcome { Setting = setting, Status = RemediationStatus.Skipped });
        public Task<bool> WriteAuditAsync(RemediationAuditRecord record, CancellationToken ct) => Task.FromResult(true);
    }
}
