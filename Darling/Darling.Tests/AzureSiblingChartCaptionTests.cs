/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The caption under the FinOps "Allocated vs Used" chart. The chart draws one bar per database, summed over the files
/// that have a size, so the bar of a database that is another database on an Azure SQL Database server (data space
/// only, no log size reported) or a Hyperscale database (the log lives in the log service) has no log in it. The
/// caption names those databases when they have a bar and says why, in words shared with Lite
/// (<see cref="AzureSiblingDatabaseSize.ChartCaption"/>), and it stays hidden when no bar needs it. Lite.Tests'
/// <c>AzureSiblingChartCaptionTests</c> is the twin.
/// </summary>
public sealed class AzureSiblingChartCaptionTests
{
    private const string SiblingReason = "not reported for other databases on an Azure SQL Database server";
    private const string LogServiceReason = "the transaction log lives in the log service, not in storage the database holds";

    private const string SiblingSentence = "It is " + SiblingReason + ".";
    private const string LogServiceSentence = "On Azure SQL Database Hyperscale, " + LogServiceReason + ".";

    [Fact]
    public void TheCaption_NamesBothKinds_WithTheHyperscaleSentenceFirst()
    {
        var caption = AzureSiblingDatabaseSize.ChartCaption(["sales", "reports"], ["hyper"]);

        Assert.Equal(
            "Log size is left out of the bar for hyper. " + LogServiceSentence
            + " Log size is left out of the bars for sales, reports. " + SiblingSentence,
            caption);
    }

    [Fact]
    public void TheCaption_NamesOneKind_AndSaysOnlyThatReason()
    {
        var siblings = AzureSiblingDatabaseSize.ChartCaption(["sales"], []);
        Assert.Equal("Log size is left out of the bar for sales. " + SiblingSentence, siblings);
        Assert.DoesNotContain("Hyperscale", siblings, StringComparison.Ordinal);

        var logService = AzureSiblingDatabaseSize.ChartCaption([], ["hyper", "hyper2"]);
        Assert.Equal("Log size is left out of the bars for hyper, hyper2. " + LogServiceSentence, logService);
        Assert.DoesNotContain("other databases", logService, StringComparison.Ordinal);

        /* A database named twice is named once. */
        Assert.Equal(
            "Log size is left out of the bar for sales. " + SiblingSentence,
            AzureSiblingDatabaseSize.ChartCaption(["sales", "sales"], []));
    }

    [Fact]
    public void TheCaption_IsNullWhenNoDatabaseIsNamed()
    {
        Assert.Null(AzureSiblingDatabaseSize.ChartCaption([], []));
    }

    [Fact]
    public void TheCaption_SaysWhatTheLogNoteAndTheHyperscaleNoteSay()
    {
        /* The two reasons are the ones the grid note and the payload note give, so a change to one of them shows here. */
        Assert.Contains(SiblingReason, AzureSiblingDatabaseSize.LogNote, StringComparison.Ordinal);
        Assert.Contains(LogServiceReason, HyperscaleLogSize.Note, StringComparison.Ordinal);
    }

    private static DatabaseSizeRow Row(string database, int? fileId, string fileName, decimal? total) => new()
    {
        DatabaseName = database,
        FileId = fileId,
        FileName = fileName,
        TotalSizeMb = total
    };

    private static readonly DatabaseSizeRow[] Snapshot =
    [
        Row("plain", 1, "plain_data", 100m),
        Row("plain", 2, "plain_log", 50m),
        Row("hyper", 1, "hyper_data", 400m),
        Row("hyper", 2, "hyper_log", null),
        Row("sibling", null, AzureSiblingDatabaseSize.FileName, 10_240m),
        Row("sibling2", null, AzureSiblingDatabaseSize.FileName, 5_120m),
        Row("offchart", null, AzureSiblingDatabaseSize.FileName, 1m),
        Row("odd", 3, AzureSiblingDatabaseSize.FileName, 5m)
    ];

    [Fact]
    public void TheRowHelper_NamesOnlyTheDatabasesThatHaveABar_InTheOrderOfTheBars()
    {
        /* "offchart" is a sibling with no bar, "odd" has a file id (a real file that carries the sibling name), and
           "plain" counts every file: none of the three is named. */
        var caption = DatabaseSizeRow.ChartCaption(Snapshot, ["sibling2", "plain", "hyper", "odd", "sibling"]);

        Assert.Equal(AzureSiblingDatabaseSize.ChartCaption(["sibling2", "sibling"], ["hyper"]), caption);
    }

    [Fact]
    public void TheRowHelper_ReturnsNull_WhenNoBarNeedsACaption()
    {
        Assert.Null(DatabaseSizeRow.ChartCaption(Snapshot, ["plain", "odd"]));
        Assert.Null(DatabaseSizeRow.ChartCaption(Snapshot, []));
        Assert.Null(DatabaseSizeRow.ChartCaption([], ["plain", "sibling", "hyper"]));
    }

    [Fact]
    public void ThePage_KeepsTheCaptionHiddenUnderTheChart_UntilTheLoaderShowsIt()
    {
        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml");
        var chart = xaml.IndexOf("<ItemsControl x:Name=\"FinOpsDbSizeChart\"", StringComparison.Ordinal);
        Assert.True(chart > 0, "the chart is on the page");

        /* The caption sits in the chart's own box, ahead of the chart in a DockPanel, docked to the bottom. */
        var box = xaml[xaml.LastIndexOf("<GroupBox", chart, StringComparison.Ordinal)..chart];
        Assert.Contains("<DockPanel>", box, StringComparison.Ordinal);
        Assert.Contains("<TextBlock x:Name=\"FinOpsDbSizeChartCaption\" DockPanel.Dock=\"Bottom\" Visibility=\"Collapsed\"", box, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLoader_BuildsTheCaptionFromTheSharedWords_ForTheBarsItPaints()
    {
        var code = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.Loaders.cs");

        Assert.Contains("var chartCaption = DatabaseSizeRow.ChartCaption(dbSizes, dbSizeSummary.Select(b => b.DatabaseName));", code, StringComparison.Ordinal);
        Assert.Contains("FinOpsDbSizeChartCaption.Text = chartCaption ?? \"\";", code, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLoader_ShowsTheCaptionOnlyWhenThereIsText()
    {
        var code = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.Loaders.cs");

        Assert.Contains("FinOpsDbSizeChartCaption.Visibility = chartCaption is null ? Visibility.Collapsed : Visibility.Visible;", code, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLoader_HidesTheCaption_WhenThereIsNoUtilizationData()
    {
        var code = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.Loaders.cs");

        Assert.Contains("FinOpsDbSizeChartCaption.Visibility = Visibility.Collapsed;", code, StringComparison.Ordinal);
    }
}
