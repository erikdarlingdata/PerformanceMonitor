/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6861: the tempdb tab's note when tempdb_stats was refused or has not run. An empty window with a named-skip latest run shows the
/// sentence the MCP tools give their precondition answer, worded for a reader (without the closing paragraph for an MCP client).
/// A collector with no run shows the same never-ran and not-yet-due sentences the Darling viewer's note carries.
/// </summary>
public sealed class CollectorOutcomeStateTests
{
    private const string ServerName = "LiteOutcome";
    private const string Refused = "VIEW DATABASE PERFORMANCE STATE permission denied in database 'tempdb'.";
    private static readonly DateTime Ran = new(2026, 10, 28, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>A collector that has run (so it has a last run) on a server that has been collecting for days.</summary>
    private static (string Text, Visibility Visibility) State(
        bool isAzureSqlDatabase, string collector, int rowCount, string? status,
        DateTime? collectorLastRun = null, DateTime? serverFirstCollected = null) =>
        ServerTab.CollectorOutcomeState(
            ServerName, isAzureSqlDatabase, collector, rowCount, status, Refused, Ran,
            collectorLastRun ?? Ran, Ran, serverFirstCollected ?? Ran.AddDays(-3));

    [Fact]
    public void EmptyWindow_WithAPermissionsRun_ShowsTheReaderSentence_WithoutTheMcpClosingParagraph()
    {
        var mcp = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            ServerName, "tempdb_stats", CollectorRuntimePrecondition.DegradedStatus, Refused, Ran);
        Assert.NotNull(mcp);
        Assert.Contains("This is a runtime PRECONDITION", mcp, StringComparison.Ordinal);

        var (text, visibility) = State(isAzureSqlDatabase: true, "tempdb_stats", rowCount: 0, CollectorRuntimePrecondition.DegradedStatus);

        Assert.Equal(Visibility.Visible, visibility);
        Assert.Contains("is running against " + ServerName + " but cannot read its source", text, StringComparison.Ordinal);
        Assert.Contains(Refused, text, StringComparison.Ordinal);
        Assert.DoesNotContain("This is a runtime PRECONDITION", text, StringComparison.Ordinal);
        Assert.StartsWith(text, mcp, StringComparison.Ordinal);
    }

    [Fact]
    public void WindowWithRows_ShowsNothing()
    {
        var (text, visibility) = State(isAzureSqlDatabase: false, "tempdb_stats", rowCount: 3, CollectorRuntimePrecondition.DegradedStatus);

        Assert.Equal(Visibility.Collapsed, visibility);
        Assert.Equal("", text);
    }

    [Theory]
    [InlineData("SUCCESS")]
    [InlineData("ERROR")]
    [InlineData("YIELDED")]
    [InlineData(null)]
    public void LatestRunThatIsNotANamedSkip_ShowsNothing(string? status)
    {
        var (text, visibility) = State(isAzureSqlDatabase: false, "tempdb_stats", rowCount: 0, status);

        Assert.Equal(Visibility.Collapsed, visibility);
        Assert.Equal("", text);
    }

    [Fact]
    public void AzureSqlDatabaseWithNoFileIoPoints_ShowsTheSharedTempDbFileIoSentence()
    {
        var expected = CollectorEngineCapability.TempDbFileIoNotCollectedMessage(
            ServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition);
        Assert.NotNull(expected);

        var (text, visibility) = ServerTab.TempDbFileIoGapState(ServerName, isAzureSqlDatabase: true, pointCount: 0);

        Assert.Equal(Visibility.Visible, visibility);
        Assert.Equal(expected, text);
        Assert.Contains("tempdb file I/O", text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true, 4)]
    [InlineData(false, 0)]
    [InlineData(false, 4)]
    public void TempDbFileIoNote_StaysHiddenOffAzureOrWithPoints(bool isAzureSqlDatabase, int pointCount)
    {
        var (text, visibility) = ServerTab.TempDbFileIoGapState(ServerName, isAzureSqlDatabase, pointCount);

        Assert.Equal(Visibility.Collapsed, visibility);
        Assert.Equal(isAzureSqlDatabase ? CollectorEngineCapability.TempDbFileIoNotCollectedMessage(ServerName, CollectorEngineCapability.AzureSqlDatabaseEngineEdition) : "", text);
    }

    [Fact]
    public void TheEngineNoteStillWins()
    {
        var (text, visibility) = State(isAzureSqlDatabase: true, "running_jobs", rowCount: 0, CollectorRuntimePrecondition.DegradedStatus);

        Assert.Equal(Visibility.Visible, visibility);
        Assert.Equal(ServerTab.EngineGapNote(ServerName, isAzureSqlDatabase: true, "running_jobs", awsRdsKind: AwsRdsKind.None), text);
    }

    [Fact]
    public void EmptyWindow_OfACollectorThatNeverRan_SaysItNeverRan()
    {
        var (text, visibility) = ServerTab.CollectorOutcomeState(
            ServerName, isAzureSqlDatabase: false, "tempdb_stats", rowCount: 0, null, null, null,
            collectorLastRunUtc: null, serverLastCollectedUtc: Ran, serverFirstCollectedUtc: Ran.AddDays(-3));

        Assert.Equal(Visibility.Visible, visibility);
        Assert.Equal(ServerTab.NeverCollectedNote(ServerName, "tempdb_stats"), text);
    }

    [Fact]
    public void EmptyWindow_OfACollectorInsideItsFirstRunGrace_SaysItIsNotDueYet()
    {
        var expected = CollectorRuntimePrecondition.NotYetRunMessage(ServerName, "tempdb_stats", Ran, Ran);
        Assert.NotNull(expected);

        var (text, visibility) = ServerTab.CollectorOutcomeState(
            ServerName, isAzureSqlDatabase: false, "tempdb_stats", rowCount: 0, null, null, null,
            collectorLastRunUtc: null, serverLastCollectedUtc: Ran, serverFirstCollectedUtc: Ran);

        Assert.Equal(Visibility.Visible, visibility);
        Assert.Equal(expected, text);
    }

    [Fact]
    public void EmptyWindow_OnAServerThatHasCollectedNothing_MakesNoNeverRanClaim()
    {
        var (text, visibility) = ServerTab.CollectorOutcomeState(
            ServerName, isAzureSqlDatabase: false, "tempdb_stats", rowCount: 0, null, null, null,
            collectorLastRunUtc: null, serverLastCollectedUtc: null, serverFirstCollectedUtc: null);

        Assert.Equal(Visibility.Collapsed, visibility);
        Assert.Equal("", text);
    }

    [Fact]
    public void WindowWithRows_OfACollectorWithNoRunRecord_ShowsNothing()
    {
        var (text, visibility) = ServerTab.CollectorOutcomeState(
            ServerName, isAzureSqlDatabase: false, "tempdb_stats", rowCount: 4, null, null, null,
            collectorLastRunUtc: null, serverLastCollectedUtc: Ran, serverFirstCollectedUtc: Ran.AddDays(-3));

        Assert.Equal(Visibility.Collapsed, visibility);
        Assert.Equal("", text);
    }

    [Fact]
    public void ANamedSkip_DisplacesTheNeverRanSentence()
    {
        var (text, visibility) = ServerTab.CollectorOutcomeState(
            ServerName, isAzureSqlDatabase: false, "tempdb_stats", rowCount: 0, CollectorRuntimePrecondition.DegradedStatus, Refused, Ran,
            collectorLastRunUtc: null, serverLastCollectedUtc: Ran, serverFirstCollectedUtc: Ran.AddDays(-3));

        Assert.Equal(Visibility.Visible, visibility);
        Assert.Contains(Refused, text, StringComparison.Ordinal);
    }
}
