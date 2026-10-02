/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the #2060 drill-down persistence codec: round-trip fidelity for the shapes drill-down
/// collectors actually emit (anonymous objects and lists of them), the write-time caps with their
/// EXPLICIT truncation note (the no-silent-caps rule), and the degrade-to-null discipline on
/// malformed input — a corrupt column reads as "no drill-down", never a throw mid-read.
/// </summary>
public sealed class DrillDownSerializerTests
{
    [Fact]
    public void RoundTrip_ListAndObjectSections_SurviveAsJsonElements()
    {
        var drillDown = new Dictionary<string, object>
        {
            ["parameter_sensitive_plans"] = new List<object>
            {
                new { query_hash = "0xABC", cost_ratio = 345.9, database = "sales" },
                new { query_hash = "0xDEF", cost_ratio = 12.5, database = "sales" },
            },
            ["summary"] = new { plans = 2, worst = "0xABC" },
        };

        var json = DrillDownSerializer.Serialize(drillDown);
        Assert.NotNull(json);
        var back = DrillDownSerializer.Deserialize(json);
        Assert.NotNull(back);

        var plans = (JsonElement)back!["parameter_sensitive_plans"];
        Assert.Equal(JsonValueKind.Array, plans.ValueKind);
        Assert.Equal(2, plans.GetArrayLength());
        Assert.Equal("0xABC", plans[0].GetProperty("query_hash").GetString());
        Assert.Equal(345.9, plans[0].GetProperty("cost_ratio").GetDouble());

        var summary = (JsonElement)back["summary"];
        Assert.Equal(2, summary.GetProperty("plans").GetInt32());
        Assert.False(back.ContainsKey(DrillDownSerializer.TruncationNoteKey));
    }

    [Fact]
    public void Serialize_RowCap_KeepsTheHead_AndSaysSo()
    {
        var rows = Enumerable.Range(0, 25).Select(i => (object)new { rank = i }).ToList();
        var json = DrillDownSerializer.Serialize(new Dictionary<string, object> { ["top_rows"] = rows });
        var back = DrillDownSerializer.Deserialize(json)!;

        var kept = (JsonElement)back["top_rows"];
        Assert.Equal(DrillDownSerializer.MaxRowsPerSection, kept.GetArrayLength());
        /* Worst-first ordering means the HEAD is the evidence — row 0 survives, row 24 does not. */
        Assert.Equal(0, kept[0].GetProperty("rank").GetInt32());

        var note = ((JsonElement)back[DrillDownSerializer.TruncationNoteKey]).GetString();
        Assert.Contains("top_rows", note, StringComparison.Ordinal);
        Assert.Contains("25", note, StringComparison.Ordinal);
    }

    [Fact]
    public void Serialize_ByteCap_DropsTailSections_NeverSilently()
    {
        var big = new string('x', 30_000);
        var drillDown = new Dictionary<string, object>
        {
            ["first"] = new { text = big },
            ["second"] = new { text = big },
            ["third"] = new { text = big },
        };

        var json = DrillDownSerializer.Serialize(drillDown);
        Assert.NotNull(json);
        Assert.True(json!.Length <= DrillDownSerializer.MaxSerializedBytes);

        var back = DrillDownSerializer.Deserialize(json)!;
        Assert.True(back.ContainsKey("first"), "head sections are the signal and must survive");
        Assert.False(back.ContainsKey("third"), "tail sections drop first under the byte cap");
        var note = ((JsonElement)back[DrillDownSerializer.TruncationNoteKey]).GetString();
        Assert.Contains("dropped for size", note, StringComparison.Ordinal);
    }

    [Fact]
    public void NullEmptyAndGarbage_DegradeToNull_NeverThrow()
    {
        Assert.Null(DrillDownSerializer.Serialize(null));
        Assert.Null(DrillDownSerializer.Serialize(new Dictionary<string, object>()));
        Assert.Null(DrillDownSerializer.Deserialize(null));
        Assert.Null(DrillDownSerializer.Deserialize(""));
        Assert.Null(DrillDownSerializer.Deserialize("not json at all"));
        Assert.Null(DrillDownSerializer.Deserialize("[1,2,3]"));
    }
}
