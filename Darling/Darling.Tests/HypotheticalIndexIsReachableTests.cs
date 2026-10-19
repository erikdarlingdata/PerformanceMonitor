/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2612 shipped a command with no caller.
///
/// <para>
/// The executor knew <c>test_hypothetical_index</c>, the worker ran it, the experiment was tested directly
/// and passed — and nothing in the product could invoke it. The only way to reach it was hand-writing a
/// row into <c>config.config_command</c>, which is how it was verified and is not a feature. A capability
/// nobody can reach is indistinguishable from one that was never built.
/// </para>
///
/// <para>
/// The caller belongs on the predicate-statistics grid specifically, because that is the shape the feature
/// was scoped to: <b>on demand only, never scheduled</b>, driven from a specific predicate row a human is
/// already looking at. Not the shared grid menu — that menu is on every grid in the Viewer, and an action
/// which reaches the monitored server does not belong on all of them.
/// </para>
/// </summary>
public sealed class HypotheticalIndexIsReachableTests
{
    private const string CommandType = "test_hypothetical_index";

    private static string Xaml => File.ReadAllText(Path.Combine(RepoRoot(),
        "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml"));

    private static string Handler => File.ReadAllText(Path.Combine(RepoRoot(),
        "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.HypotheticalIndex.cs"));

    /// <summary>
    /// The command has a caller in the product, not only in a test. This is the assertion that would have
    /// failed the day #2612 merged.
    /// </summary>
    [Fact]
    public void TheCommandIsInvokedBySomethingAUserCanReach()
    {
        Assert.Contains(CommandType, Handler, StringComparison.Ordinal);
        Assert.Contains("RunCommandAsync", Handler, StringComparison.Ordinal);
        Assert.Contains("Click=\"TestHypotheticalIndex_Click\"", Xaml, StringComparison.Ordinal);
    }

    /// <summary>
    /// It hangs off the PREDICATE grid, and only that one. The shared menu stays read-only: it is attached
    /// to every grid in the Viewer, so putting an action on it would offer the experiment against rows that
    /// name no column to test.
    /// </summary>
    [Fact]
    public void ItIsOnThePredicateGridAndNotOnTheSharedMenu()
    {
        Assert.Contains("ContextMenu=\"{StaticResource PredicateStatsContextMenu}\"", Xaml, StringComparison.Ordinal);

        var shared = Section(Xaml, "<ContextMenu x:Key=\"DataGridContextMenu\">", "</ContextMenu>");
        Assert.DoesNotContain("TestHypotheticalIndex_Click", shared, StringComparison.Ordinal);
    }

    /// <summary>
    /// The queryid crosses the wire as a STRING. It is signed 64-bit, and a JSON number would be rounded by
    /// a double-decoding parser into an id that resolves to no stored statement — which would surface as
    /// "no statement text is stored", pointing at the wrong problem entirely.
    /// </summary>
    [Fact]
    public void TheQueryIdIsSentAsAString()
        => Assert.Contains("row.QueryId.ToString(CultureInfo.InvariantCulture)", Handler, StringComparison.Ordinal);

    /// <summary>
    /// The user is told what it costs the server BEFORE it runs. Nothing executed, no index built, session
    /// discarded — those are the facts that make this safe, and a confirmation that omits them is asking
    /// for consent to something the person cannot evaluate.
    /// </summary>
    [Fact]
    public void TheConfirmationSaysWhatItDoesToTheServer()
    {
        Assert.Contains("MessageBoxButton.OKCancel", Handler, StringComparison.Ordinal);

        foreach (var promise in new[] { "Nothing is executed", "no index is built", "session is reset" })
        {
            Assert.Contains(promise, Handler, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// A high estimate error is called out BEFORE the round trip. The grid separates the two reasons a
    /// column looks interesting and only one of them is this experiment's business — an index does not fix
    /// a plan built on a wrong row count, and learning that after waiting teaches less than learning it
    /// first.
    /// </summary>
    [Fact]
    public void ABadEstimateIsFlaggedBeforeTheExperimentRuns()
    {
        Assert.Contains("WorstEstimateErrorRatio", Handler, StringComparison.Ordinal);

        var warningIndex = Handler.IndexOf("estimateWarning", StringComparison.Ordinal);
        var confirmIndex = Handler.IndexOf("MessageBoxButton.OKCancel", StringComparison.Ordinal);

        Assert.True(warningIndex >= 0 && warningIndex < confirmIndex,
            "The estimate-error warning must be composed before the confirmation, or it cannot appear in it.");
    }

    /// <summary>
    /// The verdict shown is the SERVICE's sentence. Re-deriving one in the Viewer would give the product
    /// two ways to describe one result, and they would eventually disagree about whether a "no" is an
    /// answer or a failure.
    /// </summary>
    [Fact]
    public void TheViewerReportsTheServicesOwnExplanation()
    {
        Assert.Contains("\"explanation\"", Handler, StringComparison.Ordinal);
        Assert.DoesNotContain("would use this index", Handler, StringComparison.Ordinal);
    }

    private static string Section(string source, string open, string close)
    {
        var start = source.IndexOf(open, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{open}' was not found — this pin needs re-anchoring.");

        var end = source.IndexOf(close, start, StringComparison.Ordinal);
        return end > start ? source[start..end] : source[start..];
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }
}
