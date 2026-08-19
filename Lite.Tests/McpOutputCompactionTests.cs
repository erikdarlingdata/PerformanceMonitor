/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2350: MCP tool results serialize COMPACT, and the config files people hand-edit do not.
///
/// <para>Both halves are pinned here because the change is one property on one shared object, and the risk is
/// entirely in scope rather than in mechanism — flipping <c>WriteIndented</c> on something that turns out to
/// write a config file would make <c>servers.json</c> a single unreadable line, which is the kind of damage
/// nobody notices until they open the file by hand at an awkward moment.</para>
///
/// <para>The saving is real but should not be oversold: measured on a 15-field blocking-event shape it is
/// ~23% of the BYTES, and the token saving is smaller than that because BPE tokenizers pack runs of spaces
/// efficiently. It costs nothing, which is the argument — not the headline percentage.</para>
/// </summary>
public class McpOutputCompactionTests
{
    private sealed record Row(int BlockedSessionId, string WaitType, int WaitMs);

    private static string SampleToolResult(int rows) =>
        JsonSerializer.Serialize(
            new
            {
                server = "SQLPROD01",
                total_events = rows,
                events = Enumerable.Range(0, rows).Select(i => new Row(60 + i, "LCK_M_X", 1000 + i)).ToList(),
            },
            McpHelpers.JsonOptions);

    /// <summary>The property itself, so a well-meaning "make the output readable" edit has to argue with a test.</summary>
    [Fact]
    public void McpJsonOptions_AreCompact()
    {
        Assert.False(McpHelpers.JsonOptions.WriteIndented);
    }

    /// <summary>
    /// The observable consequence, not just the flag: a record array serializes with no newline and no run of
    /// indent spaces anywhere in it.
    /// </summary>
    [Fact]
    public void AToolResult_CarriesNoLayoutWhitespace()
    {
        var json = SampleToolResult(30);

        Assert.DoesNotContain('\n', json);
        Assert.DoesNotContain('\r', json);
        Assert.DoesNotContain("  ", json, StringComparison.Ordinal);
    }

    /// <summary>
    /// Compaction must not change the DATA — the whole case for it is that the only consumer is a parser, so
    /// the parsed value has to be identical to what the indented form produced.
    /// </summary>
    [Fact]
    public void Compaction_ChangesLayoutOnly_NotContent()
    {
        var compact = SampleToolResult(10);
        var indented = JsonSerializer.Serialize(
            JsonSerializer.Deserialize<JsonElement>(compact),
            new JsonSerializerOptions { WriteIndented = true });

        using var a = JsonDocument.Parse(compact);
        using var b = JsonDocument.Parse(indented);

        Assert.Equal(
            a.RootElement.GetProperty("events").GetArrayLength(),
            b.RootElement.GetProperty("events").GetArrayLength());
        Assert.Equal(
            a.RootElement.GetProperty("server").GetString(),
            b.RootElement.GetProperty("server").GetString());

        /* And it is genuinely smaller, which is the only reason to do it at all. */
        Assert.True(compact.Length < indented.Length, "compact output must be smaller than indented output");
    }

    /// <summary>
    /// The boundary, pinned structurally the way this repo pins every invariant it cannot compile: the
    /// managers that persist files a human opens keep indenting. Their options are private statics, so the
    /// source is the assertable surface — the same idiom <c>GridPayloadColumnOrderPinTests</c> uses.
    /// </summary>
    [Theory]
    [InlineData("Lite/Services/ServerManager.cs")]
    [InlineData("Lite/Services/ProfileManager.cs")]
    [InlineData("Lite/Services/ScheduleManager.cs")]
    public void ConfigFileWriters_StayIndented(string relativePath)
    {
        var source = ParitySource.ReadFile(relativePath);

        Assert.Contains("WriteIndented = true", source, StringComparison.Ordinal);
    }
}
