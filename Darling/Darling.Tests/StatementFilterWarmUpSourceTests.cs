/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>#5320 N1: the statement filter's judge takes 250-900 ms to build, and the first value that reaches it pays
/// that. Each host that judges on a UI thread or on a request a user waits on starts the build on a background thread
/// at startup. This pins that each start path still does: Lite's desktop app (its connection and AG alerts run the
/// filter on the UI thread, and its MCP server shares the process) and the Darling service (its MCP tools, plan
/// analysis and alerts). The Darling viewer and the web host judge through the PostgreSQL predicate and never build
/// the .NET judge, so they have nothing to warm.</summary>
public sealed class StatementFilterWarmUpSourceTests
{
    private static readonly Regex s_warmUpCall = new(
        @"^\s*_\s*=\s*Task\.Run\(\s*SensitiveStatements\.WarmUp\s*\)\s*;", RegexOptions.CultureInvariant | RegexOptions.Multiline);

    [Theory]
    [InlineData("Lite/App.xaml.cs")]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Program.cs")]
    public void EachStartPathStartsTheJudgeWarmUpOffTheCallingThread(string relative)
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), relative.Replace('/', Path.DirectorySeparatorChar)));

        // A statement on its own line, so a call inside a comment does not count.
        Assert.True(
            s_warmUpCall.IsMatch(source),
            $"{relative} does not start SensitiveStatements.WarmUp on a background task (#5320 N1)");
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
