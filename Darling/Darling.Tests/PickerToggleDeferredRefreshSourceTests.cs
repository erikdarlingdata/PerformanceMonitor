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

        // The refresh may sit INSIDE the coalescer's deferred callback; only a direct call is the bug.
        var from = body.IndexOf("new PickerRefreshCoalescer(", StringComparison.Ordinal);
        var to = body.IndexOf(".Request()", StringComparison.Ordinal);
        var direct = from >= 0 && to > from ? body.Remove(from, to - from) : body;

        Assert.DoesNotContain(refresh + "(", direct, StringComparison.Ordinal);
        Assert.DoesNotContain("ItemsSource", direct, StringComparison.Ordinal);
        Assert.Matches(new Regex(@"\.Request\(\)"), body);
    }

    [Theory]
    [MemberData(nameof(Handlers))]
    public void Toggle_handler_posts_asynchronously_behind_a_selection_gate(string file, string signature, string refresh)
    {
        Assert.False(string.IsNullOrEmpty(refresh));
        var body = Body(file, signature);

        // A synchronous post (a => a()) would bring the tree-walk crash back.
        Assert.Contains("Dispatcher.BeginInvoke(", body, StringComparison.Ordinal);
        Assert.Contains("DispatcherPriority.Background", body, StringComparison.Ordinal);
        // The selection-signature delegate is what stops a regenerated container's first Checked from looping.
        Assert.Contains("PickerRefreshCoalescer.SignatureOf(", body, StringComparison.Ordinal);
    }

    public static TheoryData<string, string, string, string> DirectRefreshSites => new()
    {
        { $"{Viewer}/ViewerServerTab.Waits.cs", "void PopulateWaitTypePicker(", "_waitTypeRefresh", "RefreshWaitTypeListOrder(" },
        { $"{Viewer}/ViewerServerTab.Waits.cs", "void WaitTypeSelectAll_Click(", "_waitTypeRefresh", "RefreshWaitTypeListOrder(" },
        { $"{Viewer}/ViewerServerTab.Waits.cs", "void WaitTypeClearAll_Click(", "_waitTypeRefresh", "RefreshWaitTypeListOrder(" },
        { $"{Viewer}/ViewerServerTab.Memory.cs", "void PopulateMemoryClerkPicker(", "_memoryClerkRefresh", "RefreshMemoryClerkListOrder(" },
        { $"{Viewer}/ViewerServerTab.Memory.cs", "void MemoryClerkSelectTop_Click(", "_memoryClerkRefresh", "RefreshMemoryClerkListOrder(" },
        { $"{Viewer}/ViewerServerTab.Memory.cs", "void MemoryClerkClearAll_Click(", "_memoryClerkRefresh", "RefreshMemoryClerkListOrder(" },
        { $"{Viewer}/ViewerServerTab.Perfmon.cs", "void PopulatePerfmonPicker(", "_perfmonRefresh", "RefreshPerfmonListOrder(" },
        { $"{Viewer}/ViewerServerTab.Perfmon.cs", "void PerfmonPack_SelectionChanged(", "_perfmonRefresh", "RefreshPerfmonListOrder(" },
        { $"{Viewer}/ViewerServerTab.Perfmon.cs", "void PerfmonSelectAll_Click(", "_perfmonRefresh", "RefreshPerfmonListOrder(" },
        { $"{Viewer}/ViewerServerTab.Perfmon.cs", "void PerfmonClearAll_Click(", "_perfmonRefresh", "RefreshPerfmonListOrder(" },
        { "Lite/Controls/ServerTab.Pickers.cs", "void PopulateWaitTypePicker(", "_waitTypeRefresh", "RefreshWaitTypeListOrder(" },
        { "Lite/Controls/ServerTab.Pickers.cs", "void WaitTypeSelectAll_Click(", "_waitTypeRefresh", "RefreshWaitTypeListOrder(" },
        { "Lite/Controls/ServerTab.Pickers.cs", "void WaitTypeClearAll_Click(", "_waitTypeRefresh", "RefreshWaitTypeListOrder(" },
        { "Lite/Controls/ServerTab.Pickers.cs", "void PopulateMemoryClerkPicker(", "_memoryClerkRefresh", "RefreshMemoryClerkListOrder(" },
        { "Lite/Controls/ServerTab.Pickers.cs", "void MemoryClerkSelectTop_Click(", "_memoryClerkRefresh", "RefreshMemoryClerkListOrder(" },
        { "Lite/Controls/ServerTab.Pickers.cs", "void MemoryClerkClearAll_Click(", "_memoryClerkRefresh", "RefreshMemoryClerkListOrder(" },
        { "Lite/Controls/ServerTab.Pickers.cs", "void PopulatePerfmonPicker(", "_perfmonRefresh", "RefreshPerfmonListOrder(" },
        { "Lite/Controls/ServerTab.Pickers.cs", "void PerfmonPack_SelectionChanged(", "_perfmonRefresh", "RefreshPerfmonListOrder(" },
        { "Lite/Controls/ServerTab.Pickers.cs", "void PerfmonSelectAll_Click(", "_perfmonRefresh", "RefreshPerfmonListOrder(" },
        { "Lite/Controls/ServerTab.Pickers.cs", "void PerfmonClearAll_Click(", "_perfmonRefresh", "RefreshPerfmonListOrder(" },
    };

    [Theory]
    [MemberData(nameof(DirectRefreshSites))]
    public void Direct_refresh_site_invalidates_the_gate_before_it_refreshes(string file, string signature, string coalescer, string refresh)
    {
        var body = Body(file, signature);

        var invalidate = body.IndexOf(coalescer + "?.Invalidate()", StringComparison.Ordinal);
        var direct = body.IndexOf(refresh, StringComparison.Ordinal);
        Assert.True(direct >= 0, $"{signature} no longer calls {refresh}; this pin's anchor is stale.");
        Assert.True(invalidate >= 0, $"{signature} must call {coalescer}?.Invalidate().");
        Assert.True(invalidate < direct, $"{signature} must invalidate the gate BEFORE {refresh}.");
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
