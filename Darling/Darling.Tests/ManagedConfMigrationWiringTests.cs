/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Wiring pins for #4336 lane 5c, in the style of <see cref="StartupHostProfileLogTests"/> and
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

    /// <summary>The legacy appenders run ONLY on a Legacy conf (plan decision (c)): the
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
}
