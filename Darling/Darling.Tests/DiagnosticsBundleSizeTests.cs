/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#5097: the 4 MiB cap, per-section budgets and the atomic write.</summary>
public sealed class DiagnosticsBundleSizeTests
{
    private static JsonObject Big(int rows) =>
        new() { ["status"] = "ok", ["rows"] = new JsonArray(Enumerable.Range(0, rows).Select(i => (JsonNode?)new JsonObject { ["n"] = i, ["text"] = new string('x', 500) }).ToArray()) };

    [Fact]
    public void FitToBudget_DropsRowsFromTheEnd_AndMarksTruncated()
    {
        var node = DiagnosticsBundle.FitToBudget(Big(2_000), 64 * 1024, out var truncated);
        Assert.True(truncated);
        Assert.True(Encoding.UTF8.GetByteCount(node!.ToJsonString(DiagnosticsBundle.WriteOptions)) <= 64 * 1024);
        Assert.True(node["truncated"]!.GetValue<bool>());
        Assert.Equal(0, (int)((JsonArray)node["rows"]!)[0]!["n"]!);
    }

    [Fact]
    public void Assemble_HoldsTheTotalUnderFourMiB_AndListsTruncationInTheManifest()
    {
        var sections = Enumerable.Range(0, 8).Select(i => new BundleSection("s" + i, Big(4_000), false)).ToList();
        var (text, leaks, overCap) = DiagnosticsBundle.Assemble(sections, new BundleAliaser(), DiagnosticsBundle.BuildManifest(24, "fleet", 0));
        Assert.False(overCap);
        Assert.Empty(leaks);
        Assert.True(Encoding.UTF8.GetByteCount(text!) <= DiagnosticsBundle.TotalCapBytes);
        var manifest = JsonNode.Parse(text!)!["manifest"]!["sections"]!.AsArray();
        Assert.Equal(8, manifest.Count);
        Assert.All(manifest, s => Assert.True(s!["truncated"]!.GetValue<bool>()));
    }

    [Fact]
    public void HealthFanOut_HasABudget()
    {
        Assert.True(DiagnosticsBundle.HealthFanOutBudgetBytes < DiagnosticsBundle.SectionBudgets["collection"]);
    }

    [Fact]
    public void WriteAtomically_LeavesNoTempFile_OnSuccess_AndNoPartialOnFailure()
    {
        var dir = Directory.CreateTempSubdirectory("darling-bundle-write-");
        try
        {
            var path = Path.Combine(dir.FullName, "b.json");
            Assert.Null(DiagnosticsBundle.WriteAtomically(path, "{}", force: false));
            Assert.Equal(new[] { "b.json" }, dir.GetFiles().Select(f => f.Name).ToArray());

            /* The target exists and force is off: the move refuses, and the temp file is cleaned up. */
            Assert.NotNull(DiagnosticsBundle.WriteAtomically(path, "{\"new\":1}", force: false));
            Assert.Equal(new[] { "b.json" }, dir.GetFiles().Select(f => f.Name).ToArray());
            Assert.Equal("{}", File.ReadAllText(path));
        }
        finally
        {
            dir.Delete(recursive: true);
        }
    }
}
