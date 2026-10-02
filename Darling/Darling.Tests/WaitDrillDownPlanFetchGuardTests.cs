/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4239: <c>WaitDrillDownWindow</c>'s <c>ViewPlan_Click</c> and <c>GetActualPlan_Click</c> both call
/// <c>FetchSnapshotPlanAsync</c> — a Postgres round-trip that replaced the in-row <c>QueryPlan</c> /
/// <c>LiveQueryPlan</c> read once plan XML stopped riding along with every snapshot row. Both are
/// <c>async void</c> WPF event handlers, so an unguarded <c>await</c> there lets a store error (a dropped
/// connection, a command timeout) reach the dispatcher as an unhandled exception instead of showing the
/// user an error dialog — the same fetch-failure idiom <c>ViewerServerTab.Plans.cs</c> already uses at
/// every one of its own plan-fetch call sites.
///
/// <para><b>Why this is a SOURCE pin and not a behavioural test.</b> Exercising an <c>async void</c>
/// WPF handler needs a live <c>Window</c>, a <c>DataGrid</c> with a selected row, and a faulted
/// <see cref="PerformanceMonitor.Darling.Viewer.ViewerDataService"/> — there is no seam here to stand a
/// behavioural test on without instantiating the window itself. The property under test — "the fetch sits
/// inside a try whose catch shows an error dialog and returns" — is a shape in the shipped source, so it is
/// checked the way the repository's other guard pins check a shape: by walking the source.</para>
///
/// <para><b>Matched by shape, not by line number.</b> For each handler, this locates the
/// <c>FetchSnapshotPlanAsync(row)</c> call, finds the nearest <c>try</c> before it, confirms the call
/// actually falls inside that try's brace-balanced body (not merely somewhere after the keyword), then
/// confirms the try is followed by a <c>catch</c> whose body shows a <c>MessageBox</c> naming the failure
/// and returns — so a fix that moves the fetch back outside the try, or leaves the catch block empty, or
/// drops the <c>return</c> and falls through to code that assumes the fetch succeeded, fails here.</para>
///
/// <para>Proven red before green: reverting either handler to call <c>FetchSnapshotPlanAsync(row)</c>
/// unguarded (the pre-fix shape) and running this pin alone fails at the "preceded by a try" assertion for
/// that handler; restoring the guard turns it green again.</para>
/// </summary>
public sealed class WaitDrillDownPlanFetchGuardTests
{
    private static readonly Regex TryKeyword = new(@"\btry\b");
    private static readonly Regex CatchKeyword = new(@"\bcatch\b");

    [Theory]
    [InlineData("private async void ViewPlan_Click(")]
    [InlineData("private async void GetActualPlan_Click(")]
    public void HandlerGuardsTheSnapshotPlanFetch(string signature)
    {
        var raw = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "WaitDrillDownWindow.xaml.cs");
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(raw);

        var at = stripped.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(at > 0, $"{signature} could not be located in WaitDrillDownWindow.xaml.cs");

        var open = stripped.IndexOf('{', at);
        Assert.True(open > 0, $"{signature}: handler has no block body");

        var strippedBody = CSharpSourceWalker.BraceBalanced(stripped, open);
        var rawBody = raw[open..(open + strippedBody.Length)];

        var fetchAt = strippedBody.IndexOf("FetchSnapshotPlanAsync(row)", StringComparison.Ordinal);
        Assert.True(fetchAt > 0, $"{signature}: FetchSnapshotPlanAsync(row) call not found in the handler body");

        var tryMatches = TryKeyword.Matches(strippedBody[..fetchAt]);
        Assert.True(
            tryMatches.Count > 0,
            $"{signature}: the FetchSnapshotPlanAsync call is not preceded by a try — an unguarded fetch " +
            "inside this async void handler would reach the dispatcher as an unhandled exception (#4239)");
        var tryAt = tryMatches[^1].Index;

        var tryOpen = strippedBody.IndexOf('{', tryAt);
        Assert.True(tryOpen > 0 && tryOpen < fetchAt, $"{signature}: the nearest try has no body before the fetch call");

        var tryBody = CSharpSourceWalker.BraceBalanced(strippedBody, tryOpen);
        var tryClose = tryOpen + tryBody.Length;
        Assert.True(
            fetchAt < tryClose,
            $"{signature}: FetchSnapshotPlanAsync falls outside the try block that precedes it — the try " +
            "guards something else, not this fetch");

        var catchMatch = CatchKeyword.Match(strippedBody, tryClose);
        Assert.True(catchMatch.Success, $"{signature}: no catch block follows the try guarding FetchSnapshotPlanAsync");

        var catchOpen = strippedBody.IndexOf('{', catchMatch.Index);
        Assert.True(catchOpen > 0, $"{signature}: the catch after the guarding try has no body");

        var catchStrippedBody = CSharpSourceWalker.BraceBalanced(strippedBody, catchOpen);
        var catchRawBody = rawBody[catchOpen..(catchOpen + catchStrippedBody.Length)];

        Assert.Contains("MessageBox.Show", catchRawBody, StringComparison.Ordinal);
        Assert.Contains("Failed to retrieve plan:", catchRawBody, StringComparison.Ordinal);
        Assert.Contains("return;", catchStrippedBody, StringComparison.Ordinal);
    }
}
