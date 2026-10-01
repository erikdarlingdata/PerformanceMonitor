/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Darling Viewer.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The FinOps server picker is filled when the Viewer loads its server list. A server added afterwards (by
/// another client) was missing from the picker until restart. Activating the FinOps tab re-reads the list and
/// refills the picker before refreshing. WPF cannot run here, so this is a source pin on the FinOps case of
/// <c>LoadVisibleTabAsync</c>.
/// </summary>
public sealed class ViewerFinOpsPickerRefreshPinTests
{
    [Fact]
    public void LoadVisibleTabAsync_FinOpsCase_RereadsServersAndRefillsThePickerBeforeTheRefresh()
    {
        var src = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml.cs");

        var decl = Regex.Matches(src, @"private async Task LoadVisibleTabAsync\(");
        Assert.Single(decl);

        var from = decl[0].Index;
        var caseAt = src.IndexOf("ReferenceEquals(tab, FinOpsTab):", from, StringComparison.Ordinal);
        Assert.True(caseAt > from, "the FinOps case must exist in LoadVisibleTabAsync");

        var end = src.IndexOf("break;", caseAt, StringComparison.Ordinal);
        var body = src.Substring(caseAt, end - caseAt);

        var read = body.IndexOf("GetManagedServersAsync()", StringComparison.Ordinal);
        var set = body.IndexOf("FinOpsContent.SetServers(", StringComparison.Ordinal);
        var sorted = body.IndexOf("ApplyFavoritesAndSort(", StringComparison.Ordinal);
        var refresh = body.IndexOf("FinOpsContent.RefreshActiveSubTabAsync()", StringComparison.Ordinal);

        /* The read is the argument of the refill, so it sits after the refill call's opening text. */
        Assert.True(set >= 0, "the FinOps case refills the picker");
        Assert.True(sorted > set && read > sorted, "wrapped in the favourites ordering");
        Assert.True(read > set, "from a fresh read of the managed servers");
        Assert.True(refresh > read, "before the refresh");
    }

    [Fact]
    public void LoadVisibleTabAsync_FinOpsCase_ReadFailureIsCaughtAndTheRefreshStillRunsAfterIt()
    {
        var src = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml.cs");

        var decl = Regex.Matches(src, @"private async Task LoadVisibleTabAsync\(");
        Assert.Single(decl);

        var caseAt = src.IndexOf("ReferenceEquals(tab, FinOpsTab):", decl[0].Index, StringComparison.Ordinal);
        Assert.True(caseAt > decl[0].Index, "the FinOps case must exist in LoadVisibleTabAsync");

        var end = src.IndexOf("break;", caseAt, StringComparison.Ordinal);
        var body = src.Substring(caseAt, end - caseAt);

        var tryAt = body.IndexOf("try", StringComparison.Ordinal);
        var read = body.IndexOf("GetManagedServersAsync()", StringComparison.Ordinal);
        var catchAt = body.IndexOf("catch (", StringComparison.Ordinal);
        var refresh = body.IndexOf("FinOpsContent.RefreshActiveSubTabAsync()", StringComparison.Ordinal);

        Assert.True(tryAt >= 0, "the re-read has its own try");
        Assert.True(read > tryAt, "the re-read sits inside the try");
        Assert.True(catchAt > read, "the catch follows the re-read");
        Assert.True(refresh > catchAt, "the refresh sits after the catch, so a failed re-read cannot skip it");
        Assert.Single(Regex.Matches(body, @"\bcatch \("));
    }

    [Fact]
    public void SetServers_WhenThePreviousServerVanished_ResetsTheDrillsAndClearsTheFilters()
    {
        var src = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml.cs");

        var decl = Regex.Matches(src, @"public void SetServers\(");
        Assert.Single(decl);

        var from = decl[0].Index;
        var end = src.IndexOf("public void SelectServer(", from, StringComparison.Ordinal);
        Assert.True(end > from, "SetServers is followed by SelectServer");
        var body = src.Substring(from, end - from);

        var cond = body.IndexOf("now.ServerId != previousId", StringComparison.Ordinal);
        var storage = body.IndexOf("ShowFinOpsStorageView(", StringComparison.Ordinal);
        var locking = body.IndexOf("ShowFinOpsLockingView(", StringComparison.Ordinal);
        var filters = body.IndexOf("_filterManagers", StringComparison.Ordinal);
        var clear = body.IndexOf("ClearFilters()", StringComparison.Ordinal);

        Assert.True(cond >= 0, "the previous-server-vanished condition exists");
        Assert.True(storage > cond, "the storage drill resets inside that branch");
        Assert.True(locking > cond, "the locking drill resets inside that branch");
        Assert.True(filters > cond && clear > filters, "the column filters clear inside that branch");
    }
}
