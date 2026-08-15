/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2258: telling "never registered" from "deliberately removed" in the darling.json reconciliation.
///
/// <para><b>The ambiguity.</b> A server named in the file but absent from the registry has two indistinguishable
/// causes: it was added to the file after the first seed and never registered (the #2252 field report — the
/// operator expects monitoring and is not getting it), or it was registered once and then removed via the Viewer,
/// which is a CORRECT state. The reconciliation therefore reported at Information and worded itself for both —
/// honest, but it could neither warn about the first nor stay calm about the second.</para>
///
/// <para><b>No tombstone table was needed, which is the interesting part.</b> #2258 proposed a
/// <c>config_removed_servers</c> table or an <c>is_removed</c> flag, plus a migration rung. But the fact it wanted
/// is already recorded: <c>collect.servers</c> — the OBSERVED registry — gets a row on every successful connect,
/// the Viewer's Remove deletes only from <c>config_monitored_servers</c> (the DESIRED config), and nothing purges
/// the observed registry because it is a registry rather than a time series. So the distinction is answerable
/// today, with no schema change and no second piece of state that could disagree with the first.</para>
///
/// <para>An <c>is_removed</c> flag was worth rejecting explicitly rather than merely not choosing:
/// <c>is_enabled = FALSE</c> already means "registered but paused", so a second flag makes
/// <c>(is_enabled, is_removed)</c> a four-state space with two meaningless combinations, and every existing reader
/// of that table would have to learn the new flag or silently start including removed servers — the same seam
/// failure #2280 had to fix in the dedupe gate.</para>
/// </summary>
public sealed class FileOnlyServerCauseSplitTests
{
    private static HashSet<string> Observed(params string[] names) =>
        new(names, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// THE FIELD REPORT (#2252): in the file, never observed. That is the case where something the operator wants
    /// is not happening, so it must be the one that warns.
    /// </summary>
    [Fact]
    public void AServerNeverObservedIsReportedAsNeverRegistered()
    {
        var (never, removed) = StoreConfigProvider.SplitByEverMonitored(
            new[] { "new-server" }, Observed("some-other-server"));

        Assert.Equal(new[] { "new-server" }, never);
        Assert.Empty(removed);
    }

    /// <summary>
    /// THE CORRECT STATE: in the file, and the observed registry remembers it. The operator removed it on purpose
    /// and left the file alone — advising them to re-add it would be wrong advice, repeated at every startup
    /// forever, which is exactly what the old single message risked.
    /// </summary>
    [Fact]
    public void AServerTheStoreHasObservedIsReportedAsDeliberatelyRemoved()
    {
        var (never, removed) = StoreConfigProvider.SplitByEverMonitored(
            new[] { "retired-server" }, Observed("retired-server"));

        Assert.Empty(never);
        Assert.Equal(new[] { "retired-server" }, removed);
    }

    /// <summary>
    /// Both causes in one reconciliation, which is the normal case on a fleet that has been running a while —
    /// and the reason the two are reported as separate lines with different severities rather than one blended
    /// list. A single list would force the sharper cause to inherit the calmer wording.
    /// </summary>
    [Fact]
    public void BothCausesAreSeparatedInOnePass()
    {
        var (never, removed) = StoreConfigProvider.SplitByEverMonitored(
            new[] { "new-a", "retired-b", "new-c", "retired-d" },
            Observed("retired-b", "retired-d", "unrelated"));

        Assert.Equal(new[] { "new-a", "new-c" }, never);
        Assert.Equal(new[] { "retired-b", "retired-d" }, removed);
    }

    /// <summary>
    /// Matched case-insensitively, because the observed registry's names come from a different write path than
    /// the file's and neither normalizes case. A case difference is not a different server, and treating it as
    /// one would warn about a deliberately-removed server forever.
    /// </summary>
    [Theory]
    [InlineData("Retired-Server", "retired-server")]
    [InlineData("retired-server", "RETIRED-SERVER")]
    public void NamesMatchCaseInsensitively(string fileName, string observedName)
    {
        var (never, removed) = StoreConfigProvider.SplitByEverMonitored(
            new[] { fileName }, Observed(observedName));

        Assert.Empty(never);
        Assert.Single(removed);
    }

    /// <summary>
    /// An EMPTY observed registry sends everything to the warning arm. That is the fresh-store case — nothing has
    /// connected yet — and it is the safe direction: on a store where the file genuinely is the declared intent,
    /// a warning that these are not yet monitored is correct, whereas silence would hide it.
    /// </summary>
    [Fact]
    public void AnEmptyObservedRegistryWarnsRatherThanGoingQuiet()
    {
        var (never, removed) = StoreConfigProvider.SplitByEverMonitored(
            new[] { "a", "b" }, Observed());

        Assert.Equal(new[] { "a", "b" }, never);
        Assert.Empty(removed);
    }

    /// <summary>Nothing file-only means nothing to report, in either arm.</summary>
    [Fact]
    public void NoFileOnlyServersProducesNothing()
    {
        var (never, removed) = StoreConfigProvider.SplitByEverMonitored(
            Array.Empty<string>(), Observed("anything"));

        Assert.Empty(never);
        Assert.Empty(removed);
    }

    /// <summary>Null inputs are not a crash on a startup diagnostic path.</summary>
    [Fact]
    public void NullInputsAreTolerated()
    {
        var (never, removed) = StoreConfigProvider.SplitByEverMonitored(null!, Observed("x"));
        Assert.Empty(never);
        Assert.Empty(removed);

        var (never2, removed2) = StoreConfigProvider.SplitByEverMonitored(new[] { "a" }, null!);
        Assert.Equal(new[] { "a" }, never2);
        Assert.Empty(removed2);
    }

    /// <summary>
    /// The two arms carry the severities their causes deserve, and the observed registry is what they read —
    /// pinned at the source because the log calls need a live store to exercise.
    ///
    /// <para>The severity split IS the feature: the old single Information line could not warn about a server
    /// the operator wanted monitored, and a naive fix that warned about both would have told them to re-add a
    /// server they deliberately dropped, at every startup.</para>
    /// </summary>
    [Fact]
    public void TheWarningAndTheInformationArmsAreSplitByCause()
    {
        var source = ReadProviderSource();

        Assert.Contains("SELECT display_name, server_name FROM collect.servers", source, StringComparison.Ordinal);
        Assert.Contains("NOT monitored and never have been", source, StringComparison.Ordinal);
        Assert.Contains("LogWarning(", source, StringComparison.Ordinal);
        Assert.Contains("were monitored and have since been removed", source, StringComparison.Ordinal);
        /* The removed arm must not advise anything — it is a correct state. */
        Assert.Contains("That is expected", source, StringComparison.Ordinal);
        /* And it must still say their history is kept, since "removed" reads as "deleted" to most people. */
        Assert.Contains("collected history is", source, StringComparison.Ordinal);
    }

    private static string ReadProviderSource([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs");
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
