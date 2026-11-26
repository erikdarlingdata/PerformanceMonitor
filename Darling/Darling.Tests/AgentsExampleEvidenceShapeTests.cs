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
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9660: <c>Lite/AGENTS.example.md</c> and <c>Darling/AGENTS.example.md</c> are starter instructions an operator pastes into an agent, so the
/// evidence shape each one teaches must be one the report submit accepts. The <c>evidence</c> line of each names the keys of an entry. An entry
/// built from exactly those keys goes through the submit's own evidence check, with the basis required, as both editions call it.
/// </summary>
public sealed class AgentsExampleEvidenceShapeTests
{
    [Theory]
    [InlineData("Lite")]
    [InlineData("Darling")]
    public void AnEvidenceEntryBuiltFromTheKeysTheExampleTeaches_IsAcceptedByTheSubmitsEvidenceCheck(string edition)
    {
        var name = $"{edition}/AGENTS.example.md";
        var text = File.ReadAllText(Path.Combine(RepoRoot(), edition, "AGENTS.example.md"));
        var line = text.Split('\n').Single(l => l.TrimStart().StartsWith("- `evidence`:", StringComparison.Ordinal));
        var shape = Regex.Match(line, @"\{((?:""[a-z_]+""(?:,\s*)?)+)\}");
        Assert.True(shape.Success, $"{name}: the evidence line names no {{\"key\", ...}} shape.");
        var keys = Regex.Matches(shape.Groups[1].Value, @"""([a-z_]+)""").Select(m => m.Groups[1].Value).ToList();

        var entry = new JsonObject();
        foreach (var key in keys)
        {
            entry[key] = key switch
            {
                "arguments" => new JsonObject(),
                "basis" => InvestigationReportFields.BasisVerified,
                "tool" => "get_wait_stats",
                _ => "412 s",
            };
        }

        var problem = InvestigationToolCore.ValidateEvidence(new JsonArray(entry).ToJsonString(), out _, requireBasis: true);

        Assert.True(problem is null, $"{name} teaches the evidence keys {string.Join(", ", keys)}, and the submit refuses that entry: {problem}");
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
