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

namespace Darling.Tests;

/// <summary>
/// A picker checkbox lives INSIDE the list its toggle handler re-sorts. Rebuilding that list's
/// ItemsSource synchronously in the toggle event tears down the container that raised the event while WPF
/// is still walking the visual tree, and WPF throws "Cannot modify the Visual children for this node
/// because a tree walk is in progress". The handlers must only ask a coalescer for a deferred refresh.
/// </summary>
public sealed class PickerToggleDeferredRefreshSourceTests
{
    private const string Viewer = "Darling/PerformanceMonitor.Darling.Viewer";

    public static TheoryData<string, string, string> Handlers => new()
    {
        { $"{Viewer}/ViewerServerTab.Waits.cs", "void WaitType_CheckChanged(", "RefreshWaitTypeListOrder" },
        { $"{Viewer}/ViewerServerTab.Memory.cs", "void MemoryClerk_CheckChanged(", "RefreshMemoryClerkListOrder" },
        { $"{Viewer}/ViewerServerTab.Perfmon.cs", "void PerfmonCounter_CheckChanged(", "RefreshPerfmonListOrder" },
        { "Lite/Controls/ServerTab.Pickers.cs", "void WaitType_CheckChanged(", "RefreshWaitTypeListOrder" },
        { "Lite/Controls/ServerTab.Pickers.cs", "void MemoryClerk_CheckChanged(", "RefreshMemoryClerkListOrder" },
        { "Lite/Controls/ServerTab.Pickers.cs", "void PerfmonCounter_CheckChanged(", "RefreshPerfmonListOrder" },
    };

    private static string Body(string relative, string signature)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile(relative.Split('/')));
        var at = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(at >= 0, $"'{signature}' not found in {relative}; this pin's anchor is stale.");
        var open = code.IndexOf('{', at);
        return CSharpSourceWalker.BraceBalanced(code, open);
    }

    [Theory]
    [MemberData(nameof(Handlers))]
    public void Toggle_handler_defers_its_refresh_to_a_coalescer(string file, string signature, string refresh)
    {
        var body = Body(file, signature);

        Assert.DoesNotContain(refresh + "(", body, StringComparison.Ordinal);
        Assert.DoesNotContain("ItemsSource", body, StringComparison.Ordinal);
        Assert.Matches(new Regex(@"\.Request\(\)"), body);
    }

    [Theory]
    [InlineData(Viewer + "/ViewerServerTab.DatabaseFilter.cs")]
    [InlineData("Lite/Controls/ServerTab.DatabaseFilter.cs")]
    public void Database_filter_toggle_does_not_rebuild_its_list(string file)
    {
        var body = Body(file, "void DatabaseFilter_CheckChanged(");

        Assert.DoesNotContain("ItemsSource", body, StringComparison.Ordinal);
        Assert.DoesNotContain("ApplyDatabaseFilterSearch(", body, StringComparison.Ordinal);
    }
}
