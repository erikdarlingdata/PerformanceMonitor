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
/// Coverage for the always-safe percent-autogrowth handler (FILE_AUTOGROWTH_PERCENT):
/// the self-gating <see cref="FileAutogrowthHandler"/> against a faked executor —
/// audit-table hard block (no mutation), the happy path passing each target's
/// already-computed <see cref="FileGrowthTarget.RecommendedGrowthMb"/> through to
/// <c>SetFileGrowthAsync</c> + writing one audit row per attempt, per-target
/// independence, the preflight dispositions, and the apply-only un-apply restriction.
/// Mirrors the DB-config handler tests exactly; the single-connection (R2-MOD-1)
/// guarantee is an executor/real-server concern (SPID equality) not provable here.
/// Reuses the shared <c>FakeExecutor</c> from <see cref="RemediationTests"/> via a
/// thin local fake of the same shape (the nested one is private to that class).
/// </summary>
public class FileAutogrowthHandlerTests
{
    private static readonly RemediationIdentity Identity =
        new("TESTDOMAIN\\tester", "Analysis: file_autogrowth [abcd1234]");

    private static RemediationAction FileAction(params FileGrowthTarget[] targets) =>
        new("FILE_AUTOGROWTH_PERCENT", "set", Array.Empty<ForcePlanTarget>(),
            FileGrowthTargets: targets.ToList());

    private static FileGrowthTarget DataTarget(string db = "BigDb", string logical = "BigDb_data") =>
        new(db, logical, CurrentSizeMb: 51200, CurrentGrowthPercent: 10, RecommendedGrowthMb: 1024);

    private static FileGrowthTarget LogTarget(string db = "BigDb", string logical = "BigDb_log") =>
        new(db, logical, CurrentSizeMb: 250000, CurrentGrowthPercent: 25, RecommendedGrowthMb: 64);

    // ── Handler contract ────────────────────────────────────────────────────────

    [Fact]
    public void FactKey_IsFileAutogrowthPercent()
    {
        Assert.Equal("FILE_AUTOGROWTH_PERCENT", new FileAutogrowthHandler().FactKey);
    }

    [Fact]
    public void NotDestructive_AndApplyOnly()
    {
        var handler = new FileAutogrowthHandler();
        Assert.False(handler.IsDestructive);     // metadata-only, online — same class as AUTO_SHRINK OFF
        Assert.False(handler.SupportsUnapply);   // no sensible reverse for FILEGROWTH
    }

    [Fact]
    public async Task UnapplyAsync_Throws()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            new FileAutogrowthHandler().UnapplyAsync(
                FileAction(DataTarget()), new FakeFileExecutor(), Identity, CancellationToken.None));
    }

    [Fact]
    public void Registry_ResolvesFileAutogrowthHandler()
    {
        var registry = new RemediationHandlerRegistry(
            new IRemediationHandler[] { new ForcePlanHandler(), new DbConfigHandler(), new FileAutogrowthHandler() });
        Assert.IsType<FileAutogrowthHandler>(registry.TryGet("FILE_AUTOGROWTH_PERCENT"));
    }

    // ── Apply: audit-absent hard block, happy path, per-target independence ───────

    [Fact]
    public async Task AuditTableAbsent_HardBlocks_NoMutation_NoAudit()
    {
        var exec = new FakeFileExecutor { AuditTableExists = false };

        var result = await new FileAutogrowthHandler().ApplyAsync(
            FileAction(DataTarget("BigDb", "BigDb_data"), LogTarget("BigDb", "BigDb_log")),
            exec, Identity, CancellationToken.None);

        Assert.Equal(2, result.Outcomes.Count);
        Assert.All(result.Outcomes, o =>
        {
            Assert.Equal(RemediationStatus.Blocked, o.Status);
            Assert.False(o.AuditWritten);
            Assert.Contains("3.0.0", o.Message);
        });
        Assert.Equal(0, exec.SetFileCalls);          // NOT mutated
        Assert.Empty(exec.AuditRecords);             // nowhere to write
    }

    [Fact]
    public async Task HappyPath_AppliesEachTarget_WithRecommendedGrowth_AndAudits()
    {
        var exec = new FakeFileExecutor();   // default success outcome

        var result = await new FileAutogrowthHandler().ApplyAsync(
            FileAction(DataTarget("BigDb", "BigDb_data"), LogTarget("BigDb", "BigDb_log")),
            exec, Identity, CancellationToken.None);

        // One SetFileGrowthAsync call per target, EACH with the target's own already-computed
        // RecommendedGrowthMb (1024 data / 64 log) — the handler never recomputes it.
        Assert.Equal(2, exec.SetFileCalls);
        Assert.Contains(("BigDb", "BigDb_data", 1024), exec.SetFileArgs);
        Assert.Contains(("BigDb", "BigDb_log", 64), exec.SetFileArgs);

        // Two success outcomes, both audited.
        Assert.Equal(2, result.Outcomes.Count);
        Assert.All(result.Outcomes, o =>
        {
            Assert.Equal(RemediationStatus.Success, o.Status);
            Assert.True(o.AuditWritten);
            Assert.False(o.AppliedButUnlogged);
        });

        // Audit rows: one per attempt, the precise taxonomy + per-target database, non-consent.
        Assert.Equal(2, exec.AuditRecords.Count);
        Assert.All(exec.AuditRecords, r =>
        {
            Assert.Equal("FILE_AUTOGROWTH_PERCENT", r.FactKey);
            Assert.Equal("set_file_growth", r.Action);
            Assert.Equal("success", r.Result);
            Assert.Equal("BigDb", r.TargetDatabase);
            Assert.Null(r.QueryId);                  // no query_id/plan_id for file rows
            Assert.Null(r.PlanId);
            Assert.False(r.ConsentAcknowledged);     // not destructive — explicit false
            Assert.Contains("MODIFY FILE", r.GeneratedSql);
        });
    }

    [Fact]
    public async Task AppliesButAuditFails_FlagsAppliedButUnlogged()
    {
        var exec = new FakeFileExecutor { AuditWriteResult = false };

        var result = await new FileAutogrowthHandler().ApplyAsync(
            FileAction(DataTarget()), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Success, o.Status);
        Assert.False(o.AuditWritten);
        Assert.True(o.AppliedButUnlogged);
    }

    [Fact]
    public async Task PermissionDenied_FailsClosed_AuditSkipped()
    {
        var exec = new FakeFileExecutor
        {
            SetFileFunc = (db, logical, mb) => new FileGrowthOutcome
            {
                Database = db, LogicalFileName = logical, Status = RemediationStatus.PermissionDenied,
                Applied = false, Message = "lacks ALTER", PriorValue = "percent"
            }
        };

        var result = await new FileAutogrowthHandler().ApplyAsync(
            FileAction(DataTarget()), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.PermissionDenied, o.Status);
        Assert.False(o.AppliedButUnlogged);
        Assert.Equal("skipped", Assert.Single(exec.AuditRecords).Result);
        Assert.Equal(1, exec.SetFileCalls);
    }

    [Fact]
    public async Task AlreadyDesired_Skips_AuditSkipped()
    {
        var exec = new FakeFileExecutor
        {
            SetFileFunc = (db, logical, mb) => new FileGrowthOutcome
            {
                Database = db, LogicalFileName = logical, Status = RemediationStatus.Skipped,
                Applied = false, Message = "already at desired growth", PriorValue = "1024 MB"
            }
        };

        var result = await new FileAutogrowthHandler().ApplyAsync(
            FileAction(DataTarget()), exec, Identity, CancellationToken.None);

        var o = Assert.Single(result.Outcomes);
        Assert.Equal(RemediationStatus.Skipped, o.Status);
        Assert.False(o.AppliedButUnlogged);
        Assert.Equal("skipped", Assert.Single(exec.AuditRecords).Result);
    }

    [Fact]
    public async Task OneTargetThrows_OthersStillRun()
    {
        var exec = new FakeFileExecutor
        {
            SetFileFunc = (db, logical, mb) =>
            {
                if (logical == "A") throw new InvalidOperationException("boom");
                return new FileGrowthOutcome { Database = db, LogicalFileName = logical, Status = RemediationStatus.Success, Applied = true, PriorValue = "percent" };
            }
        };

        var result = await new FileAutogrowthHandler().ApplyAsync(
            FileAction(
                new FileGrowthTarget("BigDb", "A", 51200, 10, 1024),
                new FileGrowthTarget("BigDb", "B", 51200, 10, 1024)),
            exec, Identity, CancellationToken.None);

        Assert.Equal(2, result.Outcomes.Count);
        Assert.Equal(RemediationStatus.Error, result.Outcomes[0].Status);
        Assert.Equal(RemediationStatus.Success, result.Outcomes[1].Status);
        // Error target is still audited (aborted/error result), per-target independence.
        Assert.Equal(2, exec.AuditRecords.Count);
    }

    // ── Preflight dispositions ────────────────────────────────────────────────────

    [Theory]
    [InlineData(false, true, true, false, RemediationDisposition.BlockDatabaseNotFound)]   // db missing
    [InlineData(true, false, true, false, RemediationDisposition.BlockDatabaseNotFound)]   // file missing
    [InlineData(true, true, false, false, RemediationDisposition.BlockNoAlter)]            // no ALTER
    [InlineData(true, true, true, true, RemediationDisposition.AlreadyInDesiredState)]     // already fixed-MB
    [InlineData(true, true, true, false, RemediationDisposition.Ok)]                       // ready
    public async Task Preflight_ClassifiesDisposition(bool dbExists, bool fileExists, bool hasAlter, bool alreadyDesired, RemediationDisposition expected)
    {
        var exec = new FakeFileExecutor
        {
            FilePreflightFunc = (db, logical, mb) => new FileGrowthPreflight
            {
                Database = db, LogicalFileName = logical, RecommendedGrowthMb = mb,
                DatabaseExists = dbExists, FileExists = fileExists, HasAlter = hasAlter,
                AlreadyInDesiredState = alreadyDesired, CurrentValue = "percent"
            }
        };

        var pre = await new FileAutogrowthHandler().PreflightAsync(
            FileAction(DataTarget()), exec, CancellationToken.None);
        Assert.Equal(expected, pre.Targets.Single().Disposition);
    }

    [Fact]
    public async Task Preflight_AuditTableAbsent_OverridesDisposition()
    {
        var exec = new FakeFileExecutor { AuditTableExists = false };

        var pre = await new FileAutogrowthHandler().PreflightAsync(
            FileAction(DataTarget()), exec, CancellationToken.None);

        Assert.False(pre.AuditTableExists);
        Assert.Equal(RemediationDisposition.BlockAuditTableAbsent, pre.Targets.Single().Disposition);
    }

    /// <summary>
    /// A standalone fake of <see cref="IRemediationExecutor"/> shaped like the one nested in
    /// <see cref="RemediationTests"/> (which is private to that class). Only the audit + the
    /// file-growth seam are exercised here; the force-plan / DB-config / clear-plan members
    /// return harmless defaults.
    /// </summary>
    private sealed class FakeFileExecutor : IRemediationExecutor
    {
        public bool AuditTableExists = true;
        public bool AuditWriteResult = true;

        public Func<string, string, int, FileGrowthPreflight>? FilePreflightFunc;
        public Func<string, string, int, FileGrowthOutcome>? SetFileFunc;
        public int SetFileCalls;
        public readonly List<(string Database, string Logical, int GrowthMb)> SetFileArgs = new();
        public readonly List<RemediationAuditRecord> AuditRecords = new();

        public Task<bool> AuditTableExistsAsync(CancellationToken ct) => Task.FromResult(AuditTableExists);

        public Task<FileGrowthPreflight> PreflightFileGrowthAsync(string database, string logicalFileName, int growthMb, CancellationToken ct)
            => Task.FromResult(FilePreflightFunc?.Invoke(database, logicalFileName, growthMb) ?? new FileGrowthPreflight
            {
                Database = database, LogicalFileName = logicalFileName, RecommendedGrowthMb = growthMb,
                DatabaseExists = true, FileExists = true, HasAlter = true, AlreadyInDesiredState = false,
                ExecutingLogin = "sa", CurrentValue = "percent"
            });

        public Task<FileGrowthOutcome> SetFileGrowthAsync(string database, string logicalFileName, int growthMb, RemediationIdentity identity, CancellationToken ct)
        {
            SetFileCalls++;
            SetFileArgs.Add((database, logicalFileName, growthMb));
            return Task.FromResult(SetFileFunc?.Invoke(database, logicalFileName, growthMb) ?? new FileGrowthOutcome
            {
                Database = database, LogicalFileName = logicalFileName, Status = RemediationStatus.Success, Applied = true,
                ExecutingLogin = "sa", PriorValue = "percent",
                GeneratedSql = $"ALTER DATABASE [{database}] MODIFY FILE (NAME = [{logicalFileName}], FILEGROWTH = {growthMb}MB);",
                GateSpid = 55, ExecSpid = 55
            });
        }

        public Task<bool> WriteAuditAsync(RemediationAuditRecord record, CancellationToken ct)
        {
            AuditRecords.Add(record);
            return Task.FromResult(AuditWriteResult);
        }

        // ── Unused seams (return harmless defaults) ──────────────────────────────
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

        public Task<ClearPlanOutcome> ClearProcCacheAsync(string queryHash, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new ClearPlanOutcome { QueryHash = queryHash, Status = RemediationStatus.Success });

        public Task<ServerConfigPreflight> PreflightServerConfigAsync(ServerConfigSetting setting, long recommendedValue, CancellationToken ct)
            => Task.FromResult(new ServerConfigPreflight { Setting = setting, RecommendedValue = recommendedValue, HasPermission = true });

        public Task<ServerConfigOutcome> SetServerConfigAsync(ServerConfigSetting setting, long value, RemediationIdentity identity, CancellationToken ct)
            => Task.FromResult(new ServerConfigOutcome { Setting = setting, Status = RemediationStatus.Success });
    }
}
