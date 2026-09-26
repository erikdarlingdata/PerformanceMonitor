/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4214 ruling 9: at service start, log the host facts and a verdict per sizing-relevant setting once,
/// plus one warning per <c>stale-after-hardware-change</c> setting — and never fail or delay startup doing
/// it. Three things pinned, none of them requiring a live store:
/// <list type="bullet">
/// <item><see cref="FormatStartupProfileText_PrintsHostAndSettings_NeverStore"/> — the LOG LINE shape,
/// against a fixture whose <c>Store</c> is deliberately filled with recognizable values, so the assertion
/// is "never printed", not merely "printed as blank".</item>
/// <item><see cref="GatherStartupProfileAsync_NeverCallsGatherStoreFactsAsync"/> and
/// <see cref="LogStoreHostProfileAsync_NeverThrows_AndOwnsItsBudget"/> — the FAILURE PATH and the
/// store-facts exclusion, both read from <c>DarlingWorker.cs</c>/<c>DarlingStoreHostProfile.cs</c>'s own
/// source the way <see cref="StartupCommandTimeoutTests"/> and <see cref="SerialLoopStoreSizeSourceTests"/>
/// already do, because neither is otherwise reachable from a unit test: <c>DarlingWorker</c> takes a
/// constructor's worth of collaborators no test in this file wires up.</item>
/// <item><see cref="LogStoreHostProfileAsync_RunsBeforeTheCollectionLoopBegins"/> — "off the serial loop"
/// is a source-order claim, so it is checked as one: the call site precedes the collection loop's own
/// <c>while</c>, not inside it.</item>
/// </list>
/// </summary>
public sealed class StartupHostProfileLogTests
{
    /// <summary>
    /// A profile whose <see cref="HostStoreFacts"/> carry values that would be obviously wrong to print at
    /// startup (a fake version string, a non-zero chunk count) — so a future edit that starts reading
    /// <c>profile.Store</c> inside <see cref="DarlingStoreHostProfile.FormatStartupProfileText"/> fails this
    /// test by printing the sentinel, rather than by an assertion on an empty string that a broken formatter
    /// could also satisfy by accident.
    /// </summary>
    private static HostProfile BuildFixtureProfile() => new()
    {
        Platform = "linux",
        IsContainerized = true,
        ProcessorCount = 4,
        Memory = new HostMemoryProfile(8_589_934_592, 4_294_967_296, 4_294_967_296, true, "cgroup v2 memory.max"),
        DataVolume = new HostDataVolumeProfile(107_374_182_400, 53_687_091_200, "ext4", true),
        IsManagedStore = true,
        Store = new HostStoreFacts("SHOULD-NEVER-PRINT-17.4", "SHOULD-NEVER-PRINT-2.99.0", 999_999_999_999, 42.0, 123, 999_999_999_999, 4_242),
        Settings =
        [
            new HostSettingProfile("shared_buffers", "2048MB", 2048, "v8 (#4214 managed block)", "2048MB", 2048, HostSettingVerdict.Matches),
            new HostSettingProfile("max_connections", "100", 100, "v4 (#4214 managed block)", "200", 200, HostSettingVerdict.StaleAfterHardwareChange),
        ],
        /* Same sentinel trick as Store above (#4214 part 2b): a startup profile never probes cloud identity
           (ruling 3), so a future edit that starts printing it here fails this test visibly. */
        Cloud = new CloudIdentity("SHOULD-NEVER-PRINT-aws", "SHOULD-NEVER-PRINT-instance"),
    };

    [Fact]
    public void FormatStartupProfileText_PrintsHostAndSettings_NeverStore()
    {
        var text = DarlingStoreHostProfile.FormatStartupProfileText(BuildFixtureProfile());

        Assert.Contains("Host: linux (containerized), 4 CPU(s)", text, StringComparison.Ordinal);
        Assert.Contains("RAM: 4 GB", text, StringComparison.Ordinal);
        Assert.Contains("Data volume: 100 GB total, 50 GB free (ext4)", text, StringComparison.Ordinal);

        /* The settings table: both rows, both verdicts spelled the way DescribeVerdict spells them. */
        Assert.Contains("shared_buffers", text, StringComparison.Ordinal);
        Assert.Contains("matches", text, StringComparison.Ordinal);
        Assert.Contains("max_connections", text, StringComparison.Ordinal);
        Assert.Contains("stale-after-hardware-change", text, StringComparison.Ordinal);

        /* The negative this whole fixture exists for: nothing from Store reaches the startup log, even
           though Store on this profile is populated and easy to print by accident. */
        Assert.DoesNotContain("SHOULD-NEVER-PRINT", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Store:", text, StringComparison.Ordinal);

        /* And FormatProfileText — the --check-settings sibling — still prints it, so the omission above is
           this formatter's choice and not something upstream stripped from every caller. */
        var checkSettingsText = DarlingStoreHostProfile.FormatProfileText(BuildFixtureProfile());
        Assert.Contains("Store:", checkSettingsText, StringComparison.Ordinal);
        Assert.Contains("SHOULD-NEVER-PRINT-17.4", checkSettingsText, StringComparison.Ordinal);
    }

    /// <summary>
    /// The store-facts exclusion, read from <c>GatherStartupProfileAsync</c>'s own body: it must call
    /// <c>GatherSettingProfilesAsync</c> and <c>GatherHostFacts</c> (the cheap parts) and must never call
    /// <c>GatherStoreFactsAsync</c> (the store-size/chunk-total reads <see cref="SerialLoopStoreSizeSourceTests"/>
    /// already bounds to <c>--check-settings</c> and nowhere else). A source scan rather than a live
    /// assertion on the RETURNED profile, because the return value would read the same whether the method
    /// never called the store-facts gather or called it and threw the result away — the second shape still
    /// pays <c>pg_database_size</c>'s cost every start.
    /// </summary>
    [Fact]
    public void GatherStartupProfileAsync_NeverCallsGatherStoreFactsAsync()
    {
        var body = CSharpSourceWalker.StripCommentsAndStrings(
            StartupCommandTimeoutTests.SerialLoopMemberBody("DarlingStoreHostProfile.cs", "GatherStartupProfileAsync"));

        Assert.Contains("GatherHostFacts(", body, StringComparison.Ordinal);
        Assert.Contains("GatherSettingProfilesAsync(", body, StringComparison.Ordinal);
        Assert.DoesNotContain("GatherStoreFactsAsync", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// The failure path, read from <c>DarlingWorker.LogStoreHostProfileAsync</c>'s own body: its own linked
    /// CTS at <see cref="ServiceCommandDeadlines.StartupHostProfileSeconds"/>, a quiet arm for real shutdown,
    /// a logging arm for everything else, and — the ruling's actual words, "never fails or delays startup" —
    /// no <c>throw</c> anywhere in the method, so nothing it does can propagate to
    /// <see cref="DarlingWorker"/>'s caller and stop the collection loop below it from starting.
    /// </summary>
    [Fact]
    public void LogStoreHostProfileAsync_NeverThrows_AndOwnsItsBudget()
    {
        var body = CSharpSourceWalker.StripCommentsAndStrings(
            StartupCommandTimeoutTests.SerialLoopMemberBody("DarlingWorker.cs", "LogStoreHostProfileAsync"));

        Assert.Contains("CancellationTokenSource.CreateLinkedTokenSource(stoppingToken)", body, StringComparison.Ordinal);
        Assert.Contains(
            "budget.CancelAfter(TimeSpan.FromSeconds(ServiceCommandDeadlines.StartupHostProfileSeconds))",
            body, StringComparison.Ordinal);

        Assert.Contains("GatherStartupProfileAsync(", body, StringComparison.Ordinal);
        Assert.Contains("FormatStartupProfileText(", body, StringComparison.Ordinal);
        Assert.Contains("HostSettingVerdict.StaleAfterHardwareChange", body, StringComparison.Ordinal);

        Assert.Contains("catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)", body, StringComparison.Ordinal);
        Assert.Contains("catch (Exception ex)", body, StringComparison.Ordinal);
        Assert.Contains("_logger.LogWarning(", body, StringComparison.Ordinal);

        /* The invariant itself: no arm of this method may re-raise. Both catch blocks above are the only
           two in the body (the try above them cannot itself contain a bare "throw" without a catch/finally
           of its own, and this method has neither a nested try nor a finally), so "no throw in the whole
           body" and "neither catch arm rethrows" are the same fact here. */
        Assert.DoesNotContain("throw", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// "Off the serial loop" is a source-order claim (#4214 ruling 9): the profile log must run ONCE, ahead
    /// of the collection loop's own <c>while</c>, not from inside it on every tick. Checked as a plain index
    /// comparison over <c>RunCollectionLoopAsync</c>'s own body — the same method
    /// <see cref="StartupCommandTimeoutTests"/> and <see cref="SerialLoopStoreSizeSourceTests"/> already read
    /// the ten serial-loop members out of.
    /// </summary>
    [Fact]
    public void LogStoreHostProfileAsync_RunsBeforeTheCollectionLoopBegins()
    {
        var body = CSharpSourceWalker.StripCommentsAndStrings(
            StartupCommandTimeoutTests.SerialLoopMemberBody("DarlingWorker.cs", "RunCollectionLoopAsync"));

        /* #4215's managed-conf verdicts work widened the call's own argument list (managedDataDirectory,
           managedConfWriteResult), not its position, so the locator matches on the call target alone. */
        var callSite = body.IndexOf("LogStoreHostProfileAsync(config, postgres,", StringComparison.Ordinal);
        var loopStart = body.IndexOf("while (!stoppingToken.IsCancellationRequested)", StringComparison.Ordinal);

        Assert.True(callSite >= 0, "RunCollectionLoopAsync no longer calls LogStoreHostProfileAsync");
        Assert.True(loopStart >= 0, "RunCollectionLoopAsync's own collection while-loop could not be found");
        Assert.True(
            callSite < loopStart,
            "LogStoreHostProfileAsync must run once, ahead of the collection loop's while — not from inside "
            + "it, which would pay a diagnostic's cost on every tick instead of once per start");
    }
}
