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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Darling ships no agent (#5632 Phase A, L8). The contract test's agent lives in test code only
/// (<c>Darling.Tests/AgentContract</c>); this census fails if any shipped project, meaning any source file outside
/// <c>Darling.Tests</c> and <c>Lite.Tests</c>, names the <c>AgentContract</c> namespace or <c>FakeInvestigationAgent</c>.
/// It scans source text (<c>.cs</c>, <c>.csproj</c>, <c>.xaml</c>, <c>.js</c>), relative to the repository root, so it runs
/// the same in a worktree under <c>.claude/worktrees</c>.
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Lite")]
public sealed class AgentContractNamespaceCensusTests
{
    private static readonly string[] Needles = { "AgentContract", "FakeInvestigationAgent" };

    private static readonly string[] Extensions = { ".cs", ".csproj", ".xaml", ".js" };

    private static readonly string[] ExcludedSegments = { "Darling.Tests", "Lite.Tests", "bin", "obj", ".git", ".claude", "node_modules" };

    [Fact]
    public void NoShippedProject_NamesTheFakeAgentOrItsNamespace()
    {
        string root = RepoRoot();
        var offenders = Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories)
            .Where(f => Extensions.Contains(Path.GetExtension(f), StringComparer.OrdinalIgnoreCase))
            .Select(f => Path.GetRelativePath(root, f))
            .Where(rel => !rel.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .Any(segment => ExcludedSegments.Contains(segment, StringComparer.OrdinalIgnoreCase)))
            .Where(rel => Needles.Any(n => File.ReadAllText(Path.Combine(root, rel)).Contains(n, StringComparison.Ordinal)))
            .ToList();

        Assert.True(offenders.Count == 0,
            "Darling ships no agent: these shipped files name the test-only agent contract: " + string.Join(", ", offenders));
    }

    [Fact]
    public void TheFakeAgent_IsWhereTheCensusExpectsIt()
    {
        string agent = Path.Combine(RepoRoot(), "Darling", "Darling.Tests", "AgentContract", "FakeInvestigationAgent.cs");
        Assert.True(File.Exists(agent), "The census guards a file that moved: " + agent);
        Assert.Contains("namespace Darling.Tests.AgentContract;", File.ReadAllText(agent), StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile);
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        return dir ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above " + thisFile);
    }
}
