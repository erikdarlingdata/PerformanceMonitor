using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5978: the Manage Tags window no longer keeps its own depth cap; a node asks the shared tag tree rules whether a
/// child fits. The window's New Child button and handler both read <see cref="TagNode.CanAddChild"/>.
/// </summary>
public sealed class ManageTagsNestingLimitTests
{
    private static TagNode NodeAtDepth(int depth)
        => new(new ServerTag(Id: 1, Name: "t", ParentId: depth == 0 ? null : 0, SortOrder: 0, Colour: null), depth);

    [Fact]
    public void ATagAtTheFourthLevel_CannotTakeAChild()
    {
        // Depth is 0-based: depth 3 is the fourth level, the deepest allowed.
        Assert.False(NodeAtDepth(3).CanAddChild);
    }

    [Fact]
    public void ATagUnderTheFourthLevel_CanTakeAChild()
    {
        Assert.True(NodeAtDepth(2).CanAddChild);
    }
}
