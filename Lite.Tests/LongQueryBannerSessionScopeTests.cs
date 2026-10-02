/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using Darling.Tests;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Lite's Long Queries banner, shown while the long-query completion trace is off. Its last sentence says where
/// turning the trace on creates the Extended Events session. On Azure SQL Database the session is per monitored
/// database (<c>LongQueryCompletionsCollector.RunsPerDatabase</c>), so the banner says so for that target and keeps
/// the server wording for every other one.
/// </summary>
public sealed class LongQueryBannerSessionScopeTests
{
    [Fact]
    public void TheLiteBanner_TakesItsTextFromTheTarget_NotFromAFixedServerSentence()
    {
        var xaml = File.ReadAllText(RepoPath("Lite", "Controls", "ServerTab.xaml"));
        var name = xaml.IndexOf("x:Name=\"LongQueriesDisabledWarning\"", StringComparison.Ordinal);
        Assert.True(name >= 0, "the Long Queries tab has its disabled-trace banner");
        var banner = xaml[name..xaml.IndexOf("/>", name, StringComparison.Ordinal)];
        Assert.DoesNotContain("session on this server", banner, StringComparison.Ordinal);

        var source = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(RepoPath("Lite", "Controls", "ServerTab.LongQueries.cs")));
        var at = source.IndexOf("private async Task RefreshLongQueriesAsync(", StringComparison.Ordinal);
        Assert.True(at >= 0, "RefreshLongQueriesAsync must exist");
        var refresh = CSharpSourceWalker.BraceBalanced(source, source.IndexOf('{', at));
        Assert.Contains("LongQueriesDisabledWarning.Text = LongQueriesDisabledText(_isAzureSqlDatabase)", refresh, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLiteBanner_OnAzureSqlDatabase_SaysEachMonitoredDatabase_NotThisServer()
    {
        var text = ServerTab.LongQueriesDisabledText(isAzureSqlDatabase: true);

        Assert.EndsWith(
            "Enabling it creates the Extended Events session in each monitored database. Disabling it drops the session from every database that has it, except databases monitored as their own servers.",
            text,
            StringComparison.Ordinal);
        Assert.DoesNotContain("session on this server", text, StringComparison.Ordinal);
        Assert.Contains("Settings → Collector Schedules → Edit (Default or per-server)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLiteBanner_OnAnyOtherTarget_KeepsTheServerSentence()
    {
        var text = ServerTab.LongQueriesDisabledText(isAzureSqlDatabase: false);

        Assert.EndsWith(
            "Enabling it creates the Extended Events session on this server. Disabling it drops the session.",
            text,
            StringComparison.Ordinal);
        Assert.DoesNotContain("monitored database", text, StringComparison.Ordinal);
    }

    private static string RepoPath(params string[] parts) => Path.Combine([RepoRoot(), .. parts]);

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
