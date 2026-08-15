using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using BlackwellSystems.Gcf;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

// Mutates the DARLING_OUTPUT_FORMAT environment variable; the collection keeps these
// tests from racing each other (and any other env-mutating test) in parallel.
[Collection("DarlingOutputFormatEnv")]
public class GcfOutputTests : IDisposable
{
    public GcfOutputTests() => Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", null);

    public void Dispose() => Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", null);

    private static string Blocking(int rows)
    {
        var events = Enumerable
            .Range(0, rows)
            .Select(i => new
            {
                blocked_session_id = 60 + i,
                blocking_session_id = 55 + (i % 5),
                blocked_wait_type = "LCK_M_X",
                wait_duration_ms = 1000 + i * 137,
                blocked_database = "OrdersDB",
                blocked_query = "UPDATE dbo.Orders SET Status = @1 WHERE Id = @2",
                has_report_xml = true,
            })
            .ToList();

        return JsonSerializer.Serialize(
            new
            {
                server = "SQLPROD01",
                hours_back = 24,
                total_events = rows,
                events,
            },
            new JsonSerializerOptions { WriteIndented = true }
        );
    }

    [Fact]
    public void Enabled_Reflects_Environment()
    {
        Assert.False(GcfOutput.Enabled);

        foreach (var value in new[] { "gcf", "GCF", " gcf " })
        {
            Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", value);
            Assert.True(GcfOutput.Enabled);
        }

        Environment.SetEnvironmentVariable("DARLING_OUTPUT_FORMAT", "json");
        Assert.False(GcfOutput.Enabled);
    }

    [Fact]
    public void TryEncode_RecordArray_Is_Smaller_And_RoundTrips()
    {
        var json = Blocking(30);

        var wire = GcfOutput.TryEncode(json);

        Assert.NotNull(wire);
        Assert.StartsWith("GCF profile=generic", wire);
        Assert.True(wire!.Length < json.Length, "GCF wire must be smaller than the JSON");
        // Decoding then re-encoding reproduces the wire (stable, lossless round-trip).
        Assert.Equal(wire, Gcf.EncodeGeneric(Gcf.DecodeGeneric(wire)));
    }

    [Fact]
    public void TryEncode_Tiny_Payload_Falls_Back_To_Json()
    {
        var json = JsonSerializer.Serialize(new { status = "ok" });
        Assert.Null(GcfOutput.TryEncode(json)); // GCF not smaller: keep JSON
    }

    [Fact]
    public void TryEncode_Invalid_Json_Falls_Back()
    {
        Assert.Null(GcfOutput.TryEncode("{not json"));
    }

    [Fact]
    public void TryEncode_Preserves_Int64_Above_2Pow53()
    {
        // A default JSON-to-double parse would round 9007199254740993 to ...992; the
        // encoder must keep the exact integer, not render it as a float.
        var rows = Enumerable
            .Range(0, 20)
            .Select(_ => new { id = 9007199254740993L, name = "x" })
            .ToList();
        var json = JsonSerializer.Serialize(
            new { rows },
            new JsonSerializerOptions { WriteIndented = true }
        );

        var wire = GcfOutput.TryEncode(json);

        Assert.NotNull(wire);
        Assert.Contains("9007199254740993", wire);
        Assert.DoesNotContain("9.007", wire); // not a rounded float
    }
}
