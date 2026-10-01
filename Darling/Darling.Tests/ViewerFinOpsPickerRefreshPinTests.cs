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
        var refresh = body.IndexOf("FinOpsContent.RefreshActiveSubTabAsync()", StringComparison.Ordinal);

        /* The read is the argument of the refill, so it sits after the refill call's opening text. */
        Assert.True(set >= 0, "the FinOps case refills the picker");
        Assert.True(read > set, "from a fresh read of the managed servers");
        Assert.True(refresh > read, "before the refresh");
    }
}
