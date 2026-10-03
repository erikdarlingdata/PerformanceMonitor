using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>Source pins for the marked append-only regions in the FinOps web parity (#4843) lists: the MCP
/// tool registration, the read dispatch, the read catalog and Lite's allow-list of Darling-only tools. Each
/// list carries two regions, set A and set B, so two series of changes append to separate git hunks.</summary>
public sealed class FinOpsHotspotRegionTests
{
    private const string Prefix = "// FinOps web parity (#4843), set ";
    private const string AStart = Prefix + "A: append new FinOps entries below this line only.";
    private const string AEnd = Prefix + "A ends.";
    private const string BStart = Prefix + "B: append new FinOps entries below this line only.";
    private const string BEnd = Prefix + "B ends.";

    private const string Service = "PerformanceMonitor.Darling.Service";

    public static TheoryData<string, string[], string, string> Lists => new()
    {
        { "tool registration", new[] { "Darling", Service, "Mcp", "DarlingMcpHostService.cs" },
            ".WithGeminiCompatibleTools<DarlingMcpToolGuideTools>()", "_logger.LogInformation" },
        { "read dispatch", new[] { "Darling", Service, "DarlingWebEndpoints.cs" },
            "BuildReadDispatch(ILogger? logger", "if (TestOnlyExtraDispatchEntry is" },
        { "read catalog", new[] { "Darling", Service, "DarlingWebEndpoints.cs" },
            "CatalogDescriptors =", "/// <summary>" },
        { "Lite allow-list", new[] { "Lite.Tests", "CrossAppMcpToolInventoryPinTests.cs" },
            "KnownLiteMissingMcpTools = new", "[Fact]" },
    };

    private static string Read(string[] path) => RepoFile.ReadRepoFile(path).ReplaceLineEndings("\n");

    private static (string text, int start, int end) Body(string[] path, string declaration, string after)
    {
        var text = Read(path);
        var start = text.IndexOf(declaration, StringComparison.Ordinal);
        Assert.True(start >= 0, "declaration not found: " + declaration);
        var end = text.IndexOf(after, start, StringComparison.Ordinal);
        Assert.True(end > start, "end of the list not found: " + after);
        return (text, start, end);
    }

    [Theory]
    [MemberData(nameof(Lists))]
    public void EachMarkerOccursOnceInOrderInsideTheListAndTheSetsAreSeparated(
        string list, string[] path, string declaration, string after)
    {
        var (whole, start, end) = Body(path, declaration, after);
        // The read dispatch and the read catalog share a file, so each marker is counted inside its own list body.
        var text = whole[..end];

        var positions = new List<int>();
        foreach (var marker in new[] { AStart, AEnd, BStart, BEnd })
        {
            var first = text.IndexOf(marker, start, StringComparison.Ordinal);
            Assert.True(first >= 0, $"{list}: marker missing: {marker}");
            Assert.True(text.IndexOf(marker, first + 1, StringComparison.Ordinal) < 0,
                $"{list}: marker occurs more than once: {marker}");
            Assert.True(first > start, $"{list}: marker is outside the list body: {marker}");
            positions.Add(first);
        }

        Assert.True(positions[0] < positions[1] && positions[1] < positions[2] && positions[2] < positions[3],
            $"{list}: markers are out of order");

        var lineGap = text[positions[1]..positions[2]].Count(c => c == '\n');
        Assert.True(lineGap >= 5, $"{list}: set B starts only {lineGap} lines after set A ends");
    }
}
