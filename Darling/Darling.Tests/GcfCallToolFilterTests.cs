using System;
using System.Collections.Generic;
using System.Text.Json;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

[Collection("DarlingOutputFormatEnv")]
public class GcfCallToolFilterTests : IDisposable
{
    public GcfCallToolFilterTests() =>
        Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", null);

    public void Dispose() => Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", null);

    private static string RecordArrayJson()
    {
        var events = new List<object>();
        for (var i = 0; i < 20; i++)
            events.Add(new { session_id = 60 + i, wait_type = "LCK_M_X", wait_ms = 1000 + i });
        return JsonSerializer.Serialize(
            new { server = "S1", total = 20, events },
            new JsonSerializerOptions { WriteIndented = true }
        );
    }

    private static CallToolResult ResultWith(params ContentBlock[] content) =>
        new() { Content = new List<ContentBlock>(content) };

    private static string TextOf(CallToolResult r) => ((TextContentBlock)r.Content[0]).Text;

    [Fact]
    public void Transform_Enabled_Rewrites_Single_Json_Block_As_Gcf()
    {
        Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", "gcf");
        var json = RecordArrayJson();

        var outResult = GcfCallToolFilter.Transform(ResultWith(new TextContentBlock { Text = json }));

        Assert.Single(outResult.Content);
        Assert.StartsWith("GCF profile=generic", TextOf(outResult));
    }

    [Fact]
    public void Transform_Disabled_Leaves_Result_Unchanged()
    {
        var json = RecordArrayJson();

        var outResult = GcfCallToolFilter.Transform(ResultWith(new TextContentBlock { Text = json }));

        Assert.Equal(json, TextOf(outResult));
    }

    [Fact]
    public void Transform_Error_Result_Is_Left_As_Json()
    {
        Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", "gcf");
        var json = RecordArrayJson();

        var result = ResultWith(new TextContentBlock { Text = json });
        result.IsError = true;
        var outResult = GcfCallToolFilter.Transform(result);

        Assert.Equal(json, TextOf(outResult));
    }

    [Fact]
    public void Transform_Multiple_Content_Blocks_Are_Left_Unchanged()
    {
        Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", "gcf");
        var json = RecordArrayJson();

        // A JSON text block alongside another block must not be rewritten (would drop the
        // second block).
        var result = ResultWith(
            new TextContentBlock { Text = json },
            new TextContentBlock { Text = "second block" }
        );
        var outResult = GcfCallToolFilter.Transform(result);

        Assert.Equal(2, outResult.Content.Count);
        Assert.Equal(json, TextOf(outResult));
    }
}
