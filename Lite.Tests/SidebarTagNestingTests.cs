using System.Collections.Generic;
using System.Linq;
using PerformanceMonitorLite;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5978: the sidebar's New Child Tag handler asks the shared tag tree rules, through the depth of the tag in the
/// loaded tree, whether a child fits. Lite has no separate nesting cap of its own any more.
/// </summary>
public sealed class SidebarTagNestingTests
{
    // Tag 1 is a root (depth 0); tag N sits at depth N - 1.
    private static List<ServerTag> Chain(int length)
        => Enumerable.Range(1, length)
            .Select(i => new ServerTag(Id: i, Name: "t" + i, ParentId: i == 1 ? null : i - 1, SortOrder: 0, Colour: null))
            .ToList();

    [Fact]
    public void ATagAtTheFourthLevel_IsRefusedAChild()
    {
        Assert.False(MainWindow.CanAddChildTag(Chain(4), tagId: 4));
    }

    [Fact]
    public void ATagUnderTheFourthLevel_IsAcceptedAChild()
    {
        Assert.True(MainWindow.CanAddChildTag(Chain(4), tagId: 3));
    }
}
