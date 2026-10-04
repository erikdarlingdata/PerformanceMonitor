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

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5100: the sharded test steps in <c>build.yml</c> pass every class of the shard as a <c>-class</c>
/// argument. Windows CreateProcess caps the whole command line at 32,767 characters, and one Darling
/// shard's arguments alone reached ~32,660. Each step therefore splits its class list into chunks under a
/// named character budget and runs the test host once per chunk. These are source pins on the workflow
/// text, because the step only runs on a Windows runner.
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
        Assert.Contains("TestResults/darling-pr-${{ matrix.shard }}-$chunkNumber.trx", step);
    }

    private static string ReadStep(string stepName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "build.yml");
        Assert.True(File.Exists(path), "build.yml was not copied beside the test binary (Darling.Tests.csproj links it into Fixtures\\).");
        var text = File.ReadAllText(path);
        var start = text.IndexOf("- name: " + stepName, StringComparison.Ordinal);
        Assert.True(start >= 0, $"build.yml has no step named '{stepName}'.");
        var next = Regex.Match(text[(start + 1)..], @"\r?\n      - name: |\r?\n  [a-z][\w-]*:\r?\n");
        return next.Success ? text.Substring(start, next.Index + 1) : text[start..];
    }
}
