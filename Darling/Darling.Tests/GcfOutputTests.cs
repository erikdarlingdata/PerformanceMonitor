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
    public void TryEncode_Decoded_Wire_Carries_Input_Values()
    {
        // The claim is that the substituted wire carries the SAME value as the JSON, not
        // merely that it re-encodes to itself. Decode the wire and assert it reproduces the
        // input's values, checked against the literals the payload was built from (not
        // re-derived from the wire), so a silent drop or garble in the round-trip fails here.
        var wire = GcfOutput.TryEncode(Blocking(30));
        Assert.NotNull(wire);

        var root = Assert.IsType<OrderedMap>(Gcf.DecodeGeneric(wire!));
        Assert.Equal("SQLPROD01", (string?)root["server"]);
        Assert.Equal(24L, (long)root["hours_back"]!);
        Assert.Equal(30L, (long)root["total_events"]!);

        var events = Assert.IsType<List<object?>>(root["events"]);
        Assert.Equal(30, events.Count);

        var first = Assert.IsType<OrderedMap>(events[0]);
        Assert.Equal(60L, (long)first["blocked_session_id"]!);
        Assert.Equal(55L, (long)first["blocking_session_id"]!);
        Assert.Equal("LCK_M_X", (string?)first["blocked_wait_type"]);
        Assert.Equal(1000L, (long)first["wait_duration_ms"]!);
        Assert.Equal("OrdersDB", (string?)first["blocked_database"]);
        Assert.True((bool)first["has_report_xml"]!);

        var last = Assert.IsType<OrderedMap>(events[29]);
        Assert.Equal(89L, (long)last["blocked_session_id"]!); // 60 + 29
        Assert.Equal(59L, (long)last["blocking_session_id"]!); // 55 + (29 % 5)
        Assert.Equal(4973L, (long)last["wait_duration_ms"]!); // 1000 + 29 * 137
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

    private static string Numbered(object value, int rows)
    {
        var arr = Enumerable
            .Range(0, rows)
            .Select(_ => new Dictionary<string, object> { ["metric"] = value, ["server"] = "SQLPROD01" })
            .ToList();
        return JsonSerializer.Serialize(
            new { rows = arr },
            new JsonSerializerOptions { WriteIndented = true }
        );
    }

    [Fact]
    public void TryEncode_Keeps_Decimal_That_Fits_Double()
    {
        // 33.5 is exactly representable as a double, so it round-trips and GCF is kept.
        var wire = GcfOutput.TryEncode(Numbered(33.5, 20));

        Assert.NotNull(wire);
        Assert.Contains("33.5", wire);
    }

    [Fact]
    public void TryEncode_Declines_High_Precision_Decimal()
    {
        // 33.333333333333333 (17 significant digits) cannot be held by a double without
        // loss. A same-shape array of integers at this size encodes to GCF (asserted by the
        // Blocking round-trip test), so a null here is the precision guard declining rather
        // than the never-grow guard: the result stays JSON instead of a silently rounded wire.
        Assert.Null(GcfOutput.TryEncode(Numbered(33.333333333333333m, 20)));
    }

    [Fact]
    public void TryEncode_Declines_UInt64_Above_Int64()
    {
        // ulong.MaxValue exceeds Int64 and is not exactly a double either; keep JSON.
        var json = Numbered(18446744073709551615UL, 20);
        Assert.Null(GcfOutput.TryEncode(json));
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
