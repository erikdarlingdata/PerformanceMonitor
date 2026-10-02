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
using Darling.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins Lite's wiring of the Azure master duplicate skip. The alert sweep and the analysis provider (behind analysis,
/// the overview card, the daily summary and the MCP reads) both take the edition and the skip list from
/// <c>KnownEngineEditions</c>, which holds the edition rule, the target list and the storage-id mapping
/// (KnownEngineEditionsTests). Neither site reads the scope's edition from the live status on its own: on the live
/// edition alone, the first sweep after a start had none yet, and a master target counted its siblings' events.
/// MainWindow_Loaded seeds the stored editions before it starts anything that reads the scope.
/// </summary>
public sealed class AzureMasterLiteWiringTests
{
    [Fact]
    public void TheAlertPass_TakesTheEditionAndTheSkipList_FromKnownEngineEditions()
    {
        var body = MethodBody(StrippedSource("Lite", "MainWindow.AlertEngine.cs"), "private async void CheckPerformanceAlerts(");

        Assert.Contains("IsAzureSqlDb: badgeServer != null && _engineEditions.IsAzureSqlDatabase(badgeServer, liveEdition)", body, StringComparison.Ordinal);
        Assert.Contains("_engineEditions.SeparatelyMonitoredDatabases(badgeServer, liveEdition, _serverManager.GetAllServers())", body, StringComparison.Ordinal);
        Assert.DoesNotContain("SqlEngineEdition ==", body, StringComparison.Ordinal);
        Assert.DoesNotContain("AzureMasterScope.", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAnalysisProvider_TakesTheSkipList_FromKnownEngineEditions()
    {
        var ctor = MethodBody(StrippedSource("Lite", "MainWindow.xaml.cs"), "public MainWindow()");
        var start = ctor.IndexOf(nameof(PerformanceMonitorLite.Analysis.AnalysisService.SeparatelyMonitoredDatabasesProvider), StringComparison.Ordinal);
        Assert.True(start >= 0, "the MainWindow constructor sets the separately monitored databases provider");
        var statement = ctor[start..ctor.IndexOf(';', start)];

        Assert.Contains("_engineEditions.SeparatelyMonitoredDatabasesOrNull(", statement, StringComparison.Ordinal);
        Assert.Contains("_serverManager.GetAllServers()", statement, StringComparison.Ordinal);
        Assert.DoesNotContain("SqlEngineEdition ==", statement, StringComparison.Ordinal);
        Assert.DoesNotContain("AzureMasterScope.", ctor, StringComparison.Ordinal);
    }

    [Fact]
    public void MainWindowLoaded_SeedsTheStoredEditions_BeforeAnythingThatReadsTheScopeStarts()
    {
        /* The background service runs scheduled analysis, a sweep returns early until the alert engine is assigned,
           and the MCP server, the server list and the overview timer all read the scope. */
        var body = MethodBody(StrippedSource("Lite", "MainWindow.xaml.cs"), "private async void MainWindow_Loaded(");

        var init = body.IndexOf("_databaseInitializer.InitializeAsync(", StringComparison.Ordinal);
        var seed = body.IndexOf("await SeedKnownEngineEditionsAsync(", StringComparison.Ordinal);
        Assert.True(init >= 0, "MainWindow_Loaded initializes the database");
        Assert.True(seed > init, "the stored editions are seeded, and awaited, after the database is initialized");
        foreach (var later in new[] { "new CollectionBackgroundService(", "new AlertEngine(", "StartMcpServerAsync(", "RefreshServerList(", "_statusTimer.Start(", "RefreshOverviewAsync(" })
        {
            var at = body.IndexOf(later, StringComparison.Ordinal);
            Assert.True(at > seed, $"{later} must come after the stored editions are seeded");
        }
    }

    private static string StrippedSource(params string[] parts) =>
        CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(Path.Combine([RepoRoot(), .. parts])));

    private static string MethodBody(string strippedSource, string signature)
    {
        var at = strippedSource.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(at >= 0, $"{signature} must exist");
        return CSharpSourceWalker.BraceBalanced(strippedSource, strippedSource.IndexOf('{', at));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
