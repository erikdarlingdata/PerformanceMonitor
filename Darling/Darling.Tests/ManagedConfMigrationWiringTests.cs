/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Wiring pins for #4336, in the style of <see cref="StartupHostProfileLogTests"/> and
/// <see cref="DarlingStoreUpgradeTests"/>'s source-order tests: what
/// <c>DarlingManagedPostgres.EnsureRunningAsync</c>'s own source says, since none of this is otherwise
/// reachable from a unit test without a real PostgreSQL bootstrap.
/// </summary>
public sealed class ManagedConfMigrationWiringTests
{
    private static string ReadManagedSource() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedPostgres.cs");

    private static int At(string source, string needle, int from)
    {
        var index = source.IndexOf(needle, from, StringComparison.Ordinal);
        Assert.True(index >= 0, $"'{needle}' is gone from DarlingManagedPostgres.cs");
        return index;
    }

    /// <summary>The legacy appenders run ONLY on a Legacy conf: the
    /// <c>EnsureConfAppended(_dataDirectory);</c> call inside <c>EnsureRunningAsync</c> sits inside an
    /// <c>if</c> whose condition names <c>ManagedConfMigrationState.Kind.Legacy</c>.</summary>
    [Fact]
    public void EnsureConfAppended_InEnsureRunningAsync_IsGatedOnLegacyConfState()
    {
        var source = ReadManagedSource();
        var ensureRunning = At(source, "public async Task<string> EnsureRunningAsync(", 0);
        var call = At(source, "EnsureConfAppended(_dataDirectory);", ensureRunning);

        var ifIndex = source.LastIndexOf("if (confState == ManagedConfMigrationState.Kind.Legacy)", call, StringComparison.Ordinal);
        Assert.True(ifIndex >= 0 && ifIndex < call,
            "EnsureConfAppended(_dataDirectory) in EnsureRunningAsync must sit inside 'if (confState == ManagedConfMigrationState.Kind.Legacy)'.");

        /* Nothing structural (an early return, a closing brace of an unrelated scope) sits between the if and
           the call — the gap should be short and contain only the gate's own opening brace/comment. */
        var between = source[(ifIndex + "if (confState == ManagedConfMigrationState.Kind.Legacy)".Length)..call];
        Assert.DoesNotContain("EnsureDataDirectoryMajorAsync", between, StringComparison.Ordinal);
    }

    /// <summary>The upgrade path's own <c>EnsureConfAppended</c> delegate (passed into
    /// <c>UpgradeContext</c>) stays ungated — a freshly initdb'd cluster during pg_upgrade has no stamp and no
    /// managed file yet, so it must always append.</summary>
    [Fact]
    public void UpgradeContextEnsureConfAppendedDelegate_IsNotGated()
    {
        var source = ReadManagedSource();
        var upgradeCall = At(source, "UpgradeDataDirectoryAsync(", 0);
        var delegateIndex = At(source, "EnsureConfAppended,", upgradeCall);

        // Nothing named "confState" appears between the UpgradeDataDirectoryAsync( call and the delegate
        // reference — it is passed bare, not wrapped in a lambda that checks the classifier.
        var between = source[upgradeCall..delegateIndex];
        Assert.DoesNotContain("confState", between, StringComparison.Ordinal);
    }

    /// <summary><c>MigrateManagedConfAsync</c> runs after <c>ReconcileNetworkAsync</c> and before
    /// <c>return connectionString;</c>, gated on <c>_startedByThisProcess</c>.</summary>
    [Fact]
    public void MigrateManagedConfAsync_RunsAfterReconcileNetwork_BeforeReturn_GuardedByStartedByThisProcess()
    {
        var source = ReadManagedSource();
        var ensureRunning = At(source, "public async Task<string> EnsureRunningAsync(", 0);
        var reconcile = At(source, "await ReconcileNetworkAsync(binDirectory, networkPlan.Value, connectionString, cancellationToken);", ensureRunning);
        var guard = At(source, "if (_startedByThisProcess)", reconcile);
        var migrate = At(source, "MigrateManagedConfAsync(confState, connectionString, cancellationToken)", guard);
        var returnStatement = At(source, "return connectionString;", migrate);

        Assert.True(reconcile < guard, "MigrateManagedConfAsync's guard must follow ReconcileNetworkAsync.");
        Assert.True(guard < migrate, "the _startedByThisProcess guard must wrap the MigrateManagedConfAsync call.");
        Assert.True(migrate < returnStatement, "MigrateManagedConfAsync must run before EnsureRunningAsync returns.");
    }

    /// <summary>The classifier runs exactly once, before either the append gate or the migration call, so
    /// both branches see the SAME state this start read.</summary>
    [Fact]
    public void ConfStateIsClassifiedOnce_BeforeTheAppendGate()
    {
        var source = ReadManagedSource();
        var ensureRunning = At(source, "public async Task<string> EnsureRunningAsync(", 0);
        var classify = At(source, "var confState = ManagedConfMigrationState.Classify(_dataDirectory);", ensureRunning);
        var gate = At(source, "if (confState == ManagedConfMigrationState.Kind.Legacy)", ensureRunning);

        Assert.True(classify < gate, "confState must be classified before the Legacy append gate reads it.");
    }

    /// <summary>#4336: <c>MigrateManagedConfAsync</c> has a <c>Kind.Verified</c> case that calls
    /// <c>VerifyStepB</c>.</summary>
    [Fact]
    public void MigrateManagedConfAsync_HasVerifiedCase_CallingVerifyStepB()
    {
        var source = ReadManagedSource();
        var migrateMethod = At(source, "private async Task<ManagedConfMigrationOutcome?> MigrateManagedConfAsync(", 0);
        var verifiedCase = At(source, "case ManagedConfMigrationState.Kind.Verified:", migrateMethod);
        var verifyCall = At(source, "ManagedConfMigrationRunner.VerifyStepB(", verifiedCase);

        Assert.True(verifiedCase < verifyCall, "the Kind.Verified case must call VerifyStepB.");
    }

    /// <summary>#4215: the migration's <c>pg_file_settings</c> snapshot opens on a connection string with
    /// <c>Pooling = false</c> — this read must land on the server the current start just launched, never on
    /// a pooled socket left over from an earlier server lifetime in the same process (the stale-pool failure
    /// fixed for the upgrade tests in #4397).</summary>
    [Fact]
    public void MigrationSnapshotConnectionString_SetsPoolingFalse()
    {
        var source = ReadManagedSource();
        var helper = At(source, "private static string MigrationSnapshotConnectionString(string connectionString)", 0);
        var poolingFalse = At(source, "builder.Pooling = false;", helper);
        var snapshotUse = At(source, "DarlingStoreConnection.PinSessionTimeZoneUtc(MigrationSnapshotConnectionString(connectionString))", 0);

        Assert.True(helper < poolingFalse, "MigrationSnapshotConnectionString must set Pooling = false.");
        Assert.True(snapshotUse > 0, "the migration's snapshot connection must be built through MigrationSnapshotConnectionString.");
    }

    /// <summary>Pure pin on the helper's output: given any pooled connection string, the result parses with
    /// <c>Pooling=false</c>.</summary>
    [Fact]
    public void MigrationSnapshotConnectionString_OutputParsesWithPoolingFalse()
    {
        var pooled = "Host=127.0.0.1;Port=5432;Username=darling;Database=darling;Pooling=true";
        var builder = new NpgsqlConnectionStringBuilder(pooled);
        Assert.True(builder.Pooling, "the input fixture must itself be pooled to make this pin meaningful.");

        // The connection string this test's fixture would receive from PinSessionTimeZoneUtc + Pooling=false,
        // built the same way MigrationSnapshotConnectionString builds it.
        var timeZonePinned = DarlingStoreConnection.PinSessionTimeZoneUtc(pooled);
        var result = new NpgsqlConnectionStringBuilder(timeZonePinned) { Pooling = false }.ConnectionString;

        var parsed = new NpgsqlConnectionStringBuilder(result);
        Assert.False(parsed.Pooling, "the migration snapshot connection string must parse with Pooling=false.");
    }

    /// <summary>Fixes commit 9ae7410c: the <c>PendingVerify</c>-with-no-backup branch inside
    /// <c>MigrateManagedConfAsync</c> (Windows-only, not pure -- unreachable from a plain unit test without a
    /// real bootstrap) returns <c>ManagedConfVerificationStatus.Failed</c>, not <c>Unknown</c>.</summary>
    [Fact]
    public void MigrateManagedConfAsync_PendingVerifyWithNoBackup_ReturnsFailed()
    {
        var source = ReadManagedSource();
        var migrateMethod = At(source, "private async Task<ManagedConfMigrationOutcome?> MigrateManagedConfAsync(", 0);
        var pendingCase = At(source, "case ManagedConfMigrationState.Kind.PendingVerify:", migrateMethod);
        var noBackupCheck = At(source, "if (backupPath.Length == 0)", pendingCase);
        var failedReturn = At(source, "ManagedConfVerificationStatus.Failed, Array.Empty<string>(), null, ManagedConfMigrationStep.A,", noBackupCheck);

        Assert.True(pendingCase < noBackupCheck && noBackupCheck < failedReturn,
            "the PendingVerify case's no-backup branch must return ManagedConfVerificationStatus.Failed.");
    }

    /// <summary>Fixes commit 9ae7410c: the post-start SaveLastGoodManagedConf call guards on
    /// <c>confState != ManagedConfMigrationState.Kind.Verified</c> (in addition to the pre-existing
    /// File.Exists guard) -- a Verified start's fresh render has not been checked by Step B yet, so saving it
    /// here would let a render Step B goes on to reject become the fallback a future rejected render restores
    /// to. The save for a Verified start happens only once Step B verifies, in the separate branch inside
    /// MigrateManagedConfAsync (checked by MigrateManagedConfAsync_HasVerifiedCase_CallingVerifyStepB above).</summary>
    [Fact]
    public void SaveLastGoodManagedConf_PostStartCall_IsGuardedOnNonVerifiedConfState()
    {
        var source = ReadManagedSource();
        var call = At(source, "SaveLastGoodManagedConf(_dataDirectory);", 0);

        var ifIndex = source.LastIndexOf("if (confState != ManagedConfMigrationState.Kind.Verified", call, StringComparison.Ordinal);
        Assert.True(ifIndex >= 0 && ifIndex < call,
            "the post-start SaveLastGoodManagedConf call must sit inside 'if (confState != ManagedConfMigrationState.Kind.Verified && ...)'.");
    }
}
