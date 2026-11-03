using System.Collections.Generic;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using Xunit;

namespace Lite.Tests;

/// <summary>#7212: an open tab's header follows an Edit Server rename.</summary>
public sealed class ServerTabLabelsTests
{
    private static ServerConnection Server(string id, string name, bool readOnly = false) =>
        new() { Id = id, ServerName = "SQL01", DisplayName = name, ReadOnlyIntent = readOnly };

    [Fact]
    public void NeedingNewLabel_ReturnsTheServer_ForATabShowingTheOldName()
    {
        var shown = new Dictionary<string, string> { ["a"] = "Old name", ["b"] = "Other" };
        var loaded = new[] { Server("a", "New name"), Server("b", "Other") };

        var changed = ServerTabLabels.NeedingNewLabel(shown, loaded);

        var only = Assert.Single(changed);
        Assert.Equal("a", only.Id);
        Assert.Equal("New name", ServerTabLabels.HeaderLabel(only));
    }

    [Fact]
    public void NeedingNewLabel_ReturnsTheServer_WhenOnlyTheReadOnlyIntentChanged()
    {
        var shown = new Dictionary<string, string> { ["a"] = "Prod" };
        var loaded = new[] { Server("a", "Prod", readOnly: true) };

        var only = Assert.Single(ServerTabLabels.NeedingNewLabel(shown, loaded));
        Assert.Equal("Prod (RO)", ServerTabLabels.HeaderLabel(only));
    }

    [Fact]
    public void NeedingNewLabel_ReturnsNothing_ForAnUnchangedTab_ARemovedServer_AndAServerWithNoOpenTab()
    {
        var shown = new Dictionary<string, string> { ["a"] = "Same", ["gone"] = "Removed one" };
        var loaded = new[] { Server("a", "Same"), Server("c", "No tab") };

        Assert.Empty(ServerTabLabels.NeedingNewLabel(shown, loaded));
    }
}
