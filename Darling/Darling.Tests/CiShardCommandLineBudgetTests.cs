/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5100: the sharded test steps in <c>build.yml</c> pass every class of the shard as a <c>-class</c>
/// argument. Windows CreateProcess caps the whole command line at 32,767 characters, and one Darling
/// shard's arguments alone reached ~32,660. Each step therefore splits its class list into chunks under a
/// named character budget and runs the test host once per chunk. These are source pins, because the step only
/// runs on a Windows runner. #5616: that logic is one PowerShell script per suite
/// (<c>.github/scripts/run-darling-pg-shard.ps1</c>, <c>run-lite-shard.ps1</c>) that build.yml's PR legs and
/// nightly.yml's legs both call, so the pins read the scripts; the workflow steps only pass arguments.
/// </summary>
public sealed class CiShardCommandLineBudgetTests
{
    /// <summary>The CreateProcess limit minus the ~170 characters the rest of the command takes, rounded down.</summary>
    private const int MaxBudget = 30000;

    [Theory]
    [InlineData("Run Darling PG tests")]
    [InlineData("Run Lite tests (shard)")]
    public void ShardStep_RunsTheTestHostOncePerChunk_UnderTheBudget(string stepName)
    {
        var step = ReadStep(stepName);

        var budget = Regex.Match(step, @"\$argBudget\s*=\s*(?<n>\d+)");
        Assert.True(budget.Success, $"'{stepName}' names no $argBudget, so its -class list is not chunked.");
        Assert.True(int.Parse(budget.Groups["n"].Value) <= MaxBudget,
            $"'{stepName}' budget {budget.Groups["n"].Value} leaves less than 2,767 characters under the 32,767 limit.");

        Assert.Matches(@"(?s)foreach\s*\(\$chunk\s+in\s+\$chunks\)\s*\{.*dotnet\s+run\s+--project\s+\S+.*@chunkArgs", step);
        Assert.Matches(@"-gt\s+\$argBudget", step);
    }

    [Theory]
    [InlineData("Run Darling PG tests")]
    [InlineData("Run Lite tests (shard)")]
    public void ShardStep_NeverPassesTheWholeShardListInOneInvocation(string stepName)
    {
        var step = ReadStep(stepName);

        foreach (Match run in Regex.Matches(step, @"dotnet\s+run\s+[^\r\n]*"))
        {
            if (run.Value.Contains("-list classes/json", StringComparison.Ordinal))
            {
                continue;
            }
            Assert.DoesNotContain("$mine", run.Value);
            Assert.DoesNotContain("@shardArgs", run.Value);
            Assert.Contains("@chunkArgs", run.Value);
        }
        Assert.DoesNotContain("@shardArgs", step);
    }

    [Fact]
    public void ShardSteps_RunEveryChunk_AndFailTheStepWhenAnyChunkFailed()
    {
        foreach (var stepName in new[] { "Run Darling PG tests", "Run Lite tests (shard)" })
        {
            var step = ReadStep(stepName);
            Assert.Matches(@"\$LASTEXITCODE\s+-ne\s+0\)\s*\{\s*\$failedChunks\+\+", step);
            Assert.Matches(@"\$failedChunks\s+-gt\s+0\)[^\r\n]*exit\s+1", step);
        }
    }

    [Fact]
    public void DarlingStep_GivesEachChunkItsOwnTrx()
    {
        var step = ReadStep("Run Darling PG tests");
        Assert.Contains("TestResults/$ResultPrefix-${Shard}-$chunkNumber.trx", step);

        // ...and each caller names its own prefix, so the PR legs and the nightly's legs never write the same file.
        Assert.Contains("-ResultPrefix darling-pr ", ReadWorkflowStep("build.yml", "Run Darling PG tests"));
        Assert.Contains("-ResultPrefix darling-nightly ", ReadWorkflowStep("nightly.yml", "Run Darling PG tests"));
    }

    [Fact]
    public void ShardSteps_ChunkingBlockIsIdentical_AndNativeErrorPreferenceIsOff()
    {
        var darling = ReadStep("Run Darling PG tests");
        var lite = ReadStep("Run Lite tests (shard)");
        Assert.Equal(ChunkBlock(darling), ChunkBlock(lite), StringComparer.Ordinal);

        foreach (var step in new[] { darling, lite })
        {
            var pref = step.IndexOf("$PSNativeCommandUseErrorActionPreference = $false", StringComparison.Ordinal);
            var loop = step.IndexOf("foreach ($chunk in $chunks)", StringComparison.Ordinal);
            Assert.True(pref >= 0 && pref < loop, "the step must turn the native-command error preference off before the chunk loop.");
        }
    }

    private static string ChunkBlock(string step)
    {
        var start = step.IndexOf("$argBudget =", StringComparison.Ordinal);
        const string endMarker = "if ($current.Count -gt 0) { $chunks.Add($current.ToArray()) }";
        var end = step.IndexOf(endMarker, StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "the step has no chunking block.");
        return step.Substring(start, end + endMarker.Length - start).Replace("\r\n", "\n");
    }

    /// <summary>The shard script the named step calls (#5616), linked into Fixtures by Darling.Tests.csproj.</summary>
    private static string ReadStep(string stepName)
    {
        var file = stepName == "Run Darling PG tests" ? "run-darling-pg-shard.ps1" : "run-lite-shard.ps1";
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", file);
        Assert.True(File.Exists(path), $"{file} was not copied beside the test binary (Darling.Tests.csproj links it into Fixtures\\).");
        return File.ReadAllText(path).Replace("\r\n", "\n");
    }

    private static string ReadWorkflowStep(string workflow, string stepName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", workflow);
        Assert.True(File.Exists(path), $"{workflow} was not copied beside the test binary (Darling.Tests.csproj links it into Fixtures\\).");
        var text = File.ReadAllText(path);
        var start = text.IndexOf("- name: " + stepName, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{workflow} has no step named '{stepName}'.");
        var next = Regex.Match(text[(start + 1)..], @"\r?\n      - name: |\r?\n  [a-z][\w-]*:\r?\n");
        return next.Success ? text.Substring(start, next.Index + 1) : text[start..];
    }
}
