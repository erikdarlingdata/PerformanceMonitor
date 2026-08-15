/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2266: a skipped database-state maintenance cycle now says so, once per transition.
///
/// <para><b>What was silent.</b> <c>GetDatabaseStateDeviationsAsync</c> does its baseline seed, the #2189 heal,
/// the #2203 forget and the prune inside a block that opens the write connection with a 5-second lock
/// acquisition and, on <c>TimeoutException</c>, skips all of it while still running the deviation read. Skipping
/// is the right behaviour — the method's own comment makes the case that it is the only lossless option when
/// archival holds the lock — but it logged NOTHING, so a sustained window of write-lock contention meant
/// baselines quietly stopped being seeded and healed with no evidence anywhere.</para>
///
/// <para>That matters more than a typical missing log line: #2189 exists because an unhealed baseline inverts
/// the alert permanently, so the failure this maintenance prevents is itself invisible. Its absence has to be
/// visible instead.</para>
///
/// <para><b>Pinned at the source</b>, because the alternative is a live DuckDB store plus real write-lock
/// contention from another thread, and the assertion is about which lines exist on which path rather than about
/// a computed value. The one behavioural property that is easy to break — that the READ still runs after a
/// skipped maintenance block — is pinned here too, because my first attempt at this broke exactly that.</para>
/// </summary>
public sealed class SkippedMaintenanceIsReportedTests
{
    /// <summary>
    /// Warn on the way in, Info on the way out, and both keyed off a per-server latch so a standing contention
    /// window reports once rather than once per sweep.
    ///
    /// <para>Warn rather than Error is deliberate and worth pinning: one skipped cycle is the expected, benign
    /// outcome of colliding with archival and the next sweep re-runs everything. Logging it at Error would make
    /// a routine collision look like a fault, which is the fastest way to get the line filtered — and a filtered
    /// line restores the silence this closes.</para>
    /// </summary>
    [Fact]
    public void TheSkipIsWarnedOnce_AndTheRecoveryIsReported()
    {
        var source = ReadDatabaseStatesSource();

        Assert.Contains("_lastMaintenanceSkipped", source, StringComparison.Ordinal);
        Assert.Contains("AppLogger.Warn(nameof(GetDatabaseStateDeviationsAsync)", source, StringComparison.Ordinal);
        Assert.Contains("AppLogger.Info(nameof(GetDatabaseStateDeviationsAsync)", source, StringComparison.Ordinal);
        Assert.Contains("running again after one or more skipped", source, StringComparison.Ordinal);

        /* Not Error — see the summary. */
        Assert.DoesNotContain("AppLogger.Error(nameof(GetDatabaseStateDeviationsAsync)", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The message has to name the consequence, not just the event. "Skipped maintenance" means nothing to
    /// whoever reads the log; "baselines are not being seeded or healed while this persists" is what tells them
    /// whether to care, and naming #2189/#2203 is what lets them find out why it matters.
    /// </summary>
    [Fact]
    public void TheWarningNamesTheConsequenceAndNotJustTheEvent()
    {
        var source = ReadDatabaseStatesSource();

        Assert.Contains("Baselines are not being ", source, StringComparison.Ordinal);
        Assert.Contains("#2189/#2203", source, StringComparison.Ordinal);
        /* And that the read still happened, so the reader is not left wondering whether the numbers are stale. */
        Assert.Contains("deviations are still read", source, StringComparison.Ordinal);
        /* And that one occurrence is not a fault, so nobody escalates the expected case. */
        Assert.Contains("Expected ", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE BEHAVIOURAL PROPERTY: a skipped maintenance block must NOT skip the deviation read.
    ///
    /// <para>The whole design rests on it — the method's comment says the read "still runs under its read lock
    /// exactly as before", and swallowing the read instead would report every database as recovered and clear
    /// the alert memory for all of them. It is one stray <c>return</c> away at all times, and my first cut of
    /// this change added exactly that <c>return</c> in the catch. So: the catch sets a flag and falls through,
    /// and there is no <c>return</c> between it and the read.</para>
    /// </summary>
    [Fact]
    public void ASkippedMaintenanceBlockStillRunsTheDeviationRead()
    {
        var source = ReadDatabaseStatesSource().Replace("\r\n", "\n");

        var catchIndex = source.IndexOf("catch (TimeoutException)", StringComparison.Ordinal);
        var readIndex = source.IndexOf("using var connection = await OpenConnectionAsync();", StringComparison.Ordinal);

        Assert.True(catchIndex > 0, "the best-effort maintenance catch must still exist");
        Assert.True(readIndex > catchIndex, "the deviation read must follow the maintenance block");

        /* The catch records and falls through rather than returning. */
        var between = source[catchIndex..readIndex];
        Assert.Contains("maintenanceSkipped = true;", between, StringComparison.Ordinal);
        Assert.DoesNotContain("return ", between);
    }

    private static string ReadDatabaseStatesSource([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var relative = Path.Combine("Lite", "Services", "LocalDataService.DatabaseStates.cs");
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
