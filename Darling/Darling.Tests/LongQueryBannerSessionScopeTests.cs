/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's Long Queries banner, shown while the long-query completion trace is off. Its last sentence says where
/// turning the trace on creates the Extended Events session. On Azure SQL Database the session is per monitored
/// database (<c>LongQueryCompletionsCollector.RunsPerDatabase</c>), so the banner says so for that target and keeps
/// the server wording for every other one. The README's sentence about the session says the same.
/// </summary>
public sealed class LongQueryBannerSessionScopeTests
{
    [Fact]
    public void TheViewerBanner_TakesItsTextFromTheTarget_NotFromAFixedServerSentence()
    {
        var banner = BannerElement(File.ReadAllText(RepoPath("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml")));
        Assert.DoesNotContain("session on this server", banner, StringComparison.Ordinal);

        var load = MethodBody(RepoPath("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.LongQueries.cs"), "private async Task LoadLongQueriesAsync(");
        Assert.Contains("LongQueriesDisabledWarning.Text = LongQueriesDisabledText(_server.EngineEdition)", load, StringComparison.Ordinal);
    }

    [Fact]
    public void TheViewerBanner_OnAzureSqlDatabase_SaysEachMonitoredDatabase_NotThisServer()
    {
        var text = ViewerServerTab.LongQueriesDisabledText(CollectorEngineCapability.AzureSqlDatabaseEngineEdition);

        Assert.EndsWith(
            "Enabling it creates the Extended Events session in each monitored database. Disabling it drops the session from each one.",
            text,
            StringComparison.Ordinal);
        Assert.DoesNotContain("session on this server", text, StringComparison.Ordinal);
        Assert.Contains("Settings → Collection Schedule → Edit Collector Schedules…", text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(CollectorEngineCapability.UnknownEngineEdition)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(8)]
    public void TheViewerBanner_OnEveryOtherEdition_KeepsTheServerSentence(int engineEdition)
    {
        var text = ViewerServerTab.LongQueriesDisabledText(engineEdition);

        Assert.EndsWith(
            "Enabling it creates the Extended Events session on this server. Disabling it drops the session.",
            text,
            StringComparison.Ordinal);
        Assert.DoesNotContain("monitored database", text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheReadme_SaysTheSessionIsPerDatabase_OnAzureSqlDatabase()
    {
        var readme = File.ReadAllText(RepoPath("Darling", "README.md"));

        Assert.Contains(
            "On Azure SQL Database the session is database-scoped, so it is created in each monitored database and dropped from each one.",
            readme,
            StringComparison.Ordinal);
    }

    /// <summary>The banner TextBlock's own markup, from its name to the end of its start tag.</summary>
    internal static string BannerElement(string xaml)
    {
        var name = xaml.IndexOf("x:Name=\"LongQueriesDisabledWarning\"", StringComparison.Ordinal);
        Assert.True(name >= 0, "the Long Queries tab has its disabled-trace banner");
        var end = xaml.IndexOf("/>", name, StringComparison.Ordinal);
        Assert.True(end > name, "the banner TextBlock is self-closing");
        return xaml[name..end];
    }

    private static string MethodBody(string path, string signature)
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));
        var at = source.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(at >= 0, $"{signature} must exist");
        return CSharpSourceWalker.BraceBalanced(source, source.IndexOf('{', at));
    }

    private static string RepoPath(params string[] parts) => Path.Combine([RepoRoot(), .. parts]);

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
