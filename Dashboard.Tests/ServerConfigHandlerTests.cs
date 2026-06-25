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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Services.Remediation;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Coverage for the always-safe server-config handler (SERVER_CONFIG, WS3): the self-gating
/// <see cref="ServerConfigHandler"/> against a faked executor — audit-table hard block for the
/// executable settings (no mutation), the happy path running sp_configure for MAXDOP/CTFP and
/// writing one audit row per attempt, the advise-only memory settings recording an outcome WITHOUT
/// running sp_configure (and without needing the audit table), per-target independence, the
/// preflight dispositions, and the apply-only un-apply restriction. Mirrors the DB-config /
/// file-autogrowth handler tests; the single-connection (R2-MOD-1) guarantee is an executor concern
/// (SPID equality) not provable here.
/// </summary>
public class ServerConfigHandlerTests
{
    private static readonly RemediationIdentity Identity =
        new("TESTDOMAIN\\tester", "Analysis: server_config [abcd1234]");

    private static RemediationAction ServerAction(params ServerConfigTarget[] targets) =>
        new("SERVER_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            ServerConfigTargets: targets.ToList());

    private static ServerConfigTarget MaxdopTarget(long current = 0, long recommended = 8) =>
        new(ServerConfigSetting.Maxdop, current, recommended);

    private static ServerConfigTarget CtfpTarget(long current = 5, long recommended = 50) =>
        new(ServerConfigSetting.CostThreshold, current, recommended);

    private static ServerConfigTarget MaxMemTarget() =>
        new(ServerConfigSetting.MaxServerMemory, 2147483647, 2147483647);

    private static ServerConfigTarget MinMemTarget() =>
        new(ServerConfigSetting.MinServerMemory, 24000, 24000);

    // ── Handler contract ────────────────────────────────────────────────────────

    [Fact]
    public void FactKey_IsServerConfig()
    {
        Assert.Equal("SERVER_CONFIG", new ServerConfigHandler().FactKey);
    }

    [Fact]
    public void NotDestructive_AndApplyOnly()
    {
        var handler = new ServerConfigHandler();
        Assert.False(handler.IsDestructive);     // sp_configure MAXDOP/CTFP is online metadata
        Assert.False(handler.SupportsUnapply);   // no sensible reverse for MAXDOP/CTFP
    }

    [Fact]
    public async Task UnapplyAsync_Throws()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            new ServerConfigHandler().UnapplyAsync(
                ServerAction(MaxdopTarget()), new FakeServerConfigExecutor(), Identity, CancellationToken.None));
    }

    [Fact]
    public void Registry_ResolvesServerConfigHandler()
    {
        var registry = new RemediationHandlerRegistry(
            new IRemediationHandler[] { new ForcePlanHandler(), new DbConfigHandler(), new ServerConfigHandler() });
        Assert.IsType<ServerConfigHandler>(registry.TryGet("SERVER_CONFIG"));
    }

    // ── Apply: executable settings (MAXDOP / CTFP) ────────────────────────────────

    [Fact]
    public async Task HappyPath_RunsSpConfigure_ForMaxdopAndCtfp_AndAudits()
    {
        var exec = new FakeServerConfigExecutor();   // default success

        var result = await new ServerConfigHandler().ApplyAsync(
            ServerAction(MaxdopTarget(0, 8), CtfpTarget(5, 50)), exec, Identity, CancellationToken.None);

        // One SetServerConfigAsync per executable target, each with its own recommended value.
        Assert.Equal(2, exec.SetCalls);
        Assert.Contains((ServerConfigSetting.Maxdop, 8L), exec.SetArgs);
        Assert.Contains((ServerConfigSetting.CostThreshold, 50L), exec.SetArgs);

        Assert.Equal(2, result.Outcomes.Count);
        Assert.All(result.Outcomes, o =>
        {
            Assert.Equal(RemediationStatus.Success, o.Status);
            Assert.True(o.AuditWritten);
            Assert.False(o.AppliedButUnlogged);
        });

        // Audit rows: one per attempt, the precise taxonomy, server sentinel db, non-consent.
        Assert.Equal(2, exec.AuditRecords.Count);
        Assert.All(exec.AuditRecords, r =>
        {
            Assert.Equal("SERVER_CONFIG", r.FactKey);
            Assert.Equal("success", r.Result);
            Assert.False(r.ConsentAcknowledged);
            Assert.Null(r.QueryId);
            Assert.Null(r.PlanId);
            Assert.False(string.IsNullOrEmpty(r.TargetDatabase)); // NOT NULL column sentinel
        });
        Assert.Contains(exec.AuditRecords, r => r.Action == "set_maxdop");
        Assert.Contains(exec.AuditRecords, r => r.Action == "set_cost_threshold");
    }

    [Fact]
    public async Task AuditTableAbsent_HardBlocks_ExecutableTargets_NoMutation()
    {
        var exec = new FakeServerConfigExecutor { AuditTableExists = false };

        var result = await new ServerConfigHandler().ApplyAsync(
            ServerAction(MaxdopTarget(), CtfpTarget()), exec, Identity, CancellationToken.None);

        Assert.Equal(2, result.Outcomes.Count);
        Assert.All(result.Outcomes, o =>
        {
            Assert.Equal(RemediationStatus.Blocked, o.Status);
            Assert.False(o.AuditWritten);
            Assert.Contains("3.0.0", o.Message);
        });
        Assert.Equal(0, exec.SetCalls);          // NOT mutated
        Assert.Empty(exec.AuditRecords);
    }

    [Fact]
    public async Task PermissionDenied_FailsClosed_AuditSkipped()
    {
        var exec = new FakeServerConfigExecutor
        {
            SetFunc = (setting, value) => new ServerConfigOutcome
            {
                Setting = setting, Status = RemediationStatus.PermissionDenied, Applied = false,
                Message = "lacks ALTER SETTINGS", PriorValue = 0
            }
        };

        var result = await new ServerConfigHandler().ApplyAsync(
            ServerAction(MaxdopTarget()), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.PermissionDenied, o.Status);
        Assert.False(o.AppliedButUnlogged);
        Assert.Equal("skipped", Assert.Single(exec.AuditRecords).Result);
        Assert.Equal(1, exec.SetCalls);
    }

    [Fact]
    public async Task AppliesButAuditFails_FlagsAppliedButUnlogged()
    {
        var exec = new FakeServerConfigExecutor { AuditWriteResult = false };

        var result = await new ServerConfigHandler().ApplyAsync(
            ServerAction(MaxdopTarget()), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Success, o.Status);
        Assert.False(o.AuditWritten);
        Assert.True(o.AppliedButUnlogged);
    }

    [Fact]
    public async Task OneTargetThrows_OthersStillRun()
    {
        var exec = new FakeServerConfigExecutor
        {
            SetFunc = (setting, value) =>
            {
                if (setting == ServerConfigSetting.Maxdop) throw new InvalidOperationException("boom");
                return new ServerConfigOutcome { Setting = setting, Status = RemediationStatus.Success, Applied = true, PriorValue = 5 };
            }
        };

        var result = await new ServerConfigHandler().ApplyAsync(
            ServerAction(MaxdopTarget(), CtfpTarget()), exec, Identity, CancellationToken.None);

        Assert.Equal(2, result.Outcomes.Count);
        Assert.Contains(result.Outcomes, o => o.Status == RemediationStatus.Error);
        Assert.Contains(result.Outcomes, o => o.Status == RemediationStatus.Success);
        Assert.Equal(2, exec.AuditRecords.Count); // error target still audited
    }

    // ── Advise-only memory settings: NEVER run sp_configure ───────────────────────

    [Fact]
    public async Task MemoryTargets_AreAdviseOnly_NoMutation_NoAudit()
    {
        var exec = new FakeServerConfigExecutor();

        var result = await new ServerConfigHandler().ApplyAsync(
            ServerAction(MaxMemTarget(), MinMemTarget()), exec, Identity, CancellationToken.None);

        Assert.Equal(2, result.Outcomes.Count);
        Assert.All(result.Outcomes, o =>
        {
            Assert.Equal(RemediationStatus.Skipped, o.Status);
            Assert.Contains("advise-only", o.Message);
            Assert.False(o.AuditWritten);
        });
        Assert.Equal(0, exec.SetCalls);          // executor never touched for memory
        Assert.Empty(exec.AuditRecords);
    }

    [Fact]
    public async Task MemoryTargets_AdviseOnly_EvenWhenAuditTableAbsent()
    {
        // Advise-only targets never mutate and never need the audit table, so an absent table must
        // still yield a clean advise-only result (not a spurious block).
        var exec = new FakeServerConfigExecutor { AuditTableExists = false };

        var result = await new ServerConfigHandler().ApplyAsync(
            ServerAction(MaxMemTarget()), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Skipped, o.Status);
        Assert.Contains("advise-only", o.Message);
        Assert.Equal(0, exec.SetCalls);
    }

    [Fact]
    public async Task MixedTargets_MaxdopApplied_MemoryAdviseOnly()
    {
        var exec = new FakeServerConfigExecutor();

        var result = await new ServerConfigHandler().ApplyAsync(
            ServerAction(MaxdopTarget(0, 8), MaxMemTarget()), exec, Identity, CancellationToken.None);

        Assert.Equal(2, result.Outcomes.Count);
        Assert.Equal(1, exec.SetCalls); // only MAXDOP ran sp_configure
        Assert.Contains((ServerConfigSetting.Maxdop, 8L), exec.SetArgs);
        Assert.DoesNotContain(exec.SetArgs, a => a.Item1 == ServerConfigSetting.MaxServerMemory);
        // One success audit (MAXDOP); memory advise-only writes nothing.
        Assert.Single(exec.AuditRecords);
        Assert.Equal("set_maxdop", exec.AuditRecords[0].Action);
    }

    // ── Preflight dispositions ────────────────────────────────────────────────────

    [Theory]
    [InlineData(true, false, RemediationDisposition.Ok)]            // has perm, not already -> ready
    [InlineData(true, true, RemediationDisposition.AlreadyInDesiredState)] // already at recommended
    [InlineData(false, false, RemediationDisposition.BlockNoAlter)] // no permission
    public async Task Preflight_ClassifiesExecutableDisposition(bool hasPerm, bool alreadyDesired, RemediationDisposition expected)
    {
        var exec = new FakeServerConfigExecutor
        {
            PreflightFunc = (setting, rec) => new ServerConfigPreflight
            {
                Setting = setting, RecommendedValue = rec, Executable = true,
                HasPermission = hasPerm, AlreadyInDesiredState = alreadyDesired, CurrentValue = 0
            }
        };

        var pre = await new ServerConfigHandler().PreflightAsync(
            ServerAction(MaxdopTarget()), exec, CancellationToken.None);
        Assert.Equal(expected, pre.Targets.Single().Disposition);
    }

    [Fact]
    public async Task Preflight_MemoryTarget_IsAdviseOnlyDisposition()
    {
        var exec = new FakeServerConfigExecutor();

        var pre = await new ServerConfigHandler().PreflightAsync(
            ServerAction(MaxMemTarget()), exec, CancellationToken.None);

        Assert.Equal(RemediationDisposition.AdviseOnly, pre.Targets.Single().Disposition);
    }

    [Fact]
    public async Task Preflight_AuditTableAbsent_OverridesExecutableDisposition_NotAdviseOnly()
    {
        var exec = new FakeServerConfigExecutor { AuditTableExists = false };

        var pre = await new ServerConfigHandler().PreflightAsync(
            ServerAction(MaxdopTarget(), MaxMemTarget()), exec, CancellationToken.None);

        Assert.False(pre.AuditTableExists);
        // Executable MAXDOP blocked by absent table; advise-only memory stays AdviseOnly.
        Assert.Contains(pre.Targets, t => t.Disposition == RemediationDisposition.BlockAuditTableAbsent);
        Assert.Contains(pre.Targets, t => t.Disposition == RemediationDisposition.AdviseOnly);
    }

    /// <summary>
    /// A standalone fake of <see cref="IRemediationExecutor"/> exercising only the audit + the
    /// server-config seam; the other members return harmless defaults.
    /// </summary>
    private sealed class FakeServerConfigExecutor : IRemediationExecutor
    {
        public bool AuditTableExists = true;
        public bool AuditWriteResult = true;

        public Func<ServerConfigSetting, long, ServerConfigPreflight>? PreflightFunc;
        public Func<ServerConfigSetting, long, ServerConfigOutcome>? SetFunc;
        public int SetCalls;
        public readonly List<(ServerConfigSetting Setting, long Value)> SetArgs = new();
        public readonly List<RemediationAuditRecord> AuditRecords = new();

        public Task<bool> AuditTableExistsAsync(CancellationToken ct) => Task.FromResult(AuditTableExists);

        public Task<ServerConfigPreflight> PreflightServerConfigAsync(ServerConfigSetting setting, long recommendedValue, CancellationToken ct)
            => Task.FromResult(PreflightFunc?.Invoke(setting, recommendedValue) ?? new ServerConfigPreflight
            {
                Setting = setting, RecommendedValue = recommendedValue,
                Executable = setting is ServerConfigSetting.Maxdop or ServerConfigSetting.CostThreshold,
                HasPermission = true, AlreadyInDesiredState = false, ExecutingLogin = "sa", CurrentValue = 0
            });

        public Task<ServerConfigOutcome> SetServerConfigAsync(ServerConfigSetting setting, long value, RemediationIdentity identity, CancellationToken ct)
        {
            SetCalls++;
            SetArgs.Add((setting, value));
            return Task.FromResult(SetFunc?.Invoke(setting, value) ?? new ServerConfigOutcome
            {
                Setting = setting, Status = RemediationStatus.Success, Applied = true, ExecutingLogin = "sa",
                PriorValue = 0, GeneratedSql = $"EXEC sys.sp_configure N'...', {value}; RECONFIGURE;",
                GateSpid = 55, ExecSpid = 55
            });
        }

        public Task<bool> WriteAuditAsync(RemediationAuditRecord record, CancellationToken ct)
        {
            AuditRecords.Add(record);
            return Task.FromResult(AuditWriteResult);
        }

        // ── Unused seams (harmless defaults) ──────────────────────────────────────
        public Task<TargetPreflight> PreflightForcePlanAsync(string database, long queryId, long planId, CancellationToken ct)
            => Task.FromResult(new TargetPreflight { Database = database, QueryId = queryId, PlanId = planId });
        public Task<bool> HasPriorForceAsync(string database, long queryId, long planId, CancellationToken ct)
            => Task.FromResult(false);
        public Task<ForcePlanOutcome> ForcePlanAsync(string database, long queryId, long planId, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new ForcePlanOutcome { Database = database, QueryId = queryId, PlanId = planId, Status = RemediationStatus.Success });
        public Task<ForcePlanOutcome> UnforcePlanAsync(string database, long queryId, long planId, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new ForcePlanOutcome { Database = database, QueryId = queryId, PlanId = planId, Status = RemediationStatus.Success });
        public Task<DbConfigPreflight> PreflightDbConfigAsync(string database, DbConfigSetting setting, CancellationToken ct)
            => Task.FromResult(new DbConfigPreflight { Database = database, Setting = setting });
        public Task<DbConfigOutcome> SetDatabaseOptionAsync(string database, DbConfigSetting setting, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new DbConfigOutcome { Database = database, Setting = setting, Status = RemediationStatus.Success });
        public Task<FileGrowthPreflight> PreflightFileGrowthAsync(string database, string logicalFileName, int growthMb, CancellationToken ct)
            => Task.FromResult(new FileGrowthPreflight { Database = database, LogicalFileName = logicalFileName, RecommendedGrowthMb = growthMb });
        public Task<FileGrowthOutcome> SetFileGrowthAsync(string database, string logicalFileName, int growthMb, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new FileGrowthOutcome { Database = database, LogicalFileName = logicalFileName, Status = RemediationStatus.Success });
        public Task<ClearPlanOutcome> ClearProcCacheAsync(string queryHash, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new ClearPlanOutcome { QueryHash = queryHash, Status = RemediationStatus.Success });
    }
}
