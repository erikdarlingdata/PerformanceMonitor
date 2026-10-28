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
/// #6861: the tempdb tab's note when tempdb_stats was refused. An empty window with a named-skip latest run shows the sentence the
/// MCP tools give their precondition answer.
/// </summary>
public sealed class CollectorOutcomeStateTests
{
    private const string ServerName = "LiteOutcome";
    private const string Refused = "VIEW DATABASE PERFORMANCE STATE permission denied in database 'tempdb'.";
    private static readonly DateTime Ran = new(2026, 10, 28, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void EmptyWindow_WithAPermissionsRun_ShowsTheMcpSentence()
    {
        var expected = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            ServerName, "tempdb_stats", CollectorRuntimePrecondition.DegradedStatus, Refused, Ran);
        Assert.NotNull(expected);

        var (text, visibility) = ServerTab.CollectorOutcomeState(
            ServerName, isAzureSqlDatabase: true, "tempdb_stats", rowCount: 0, CollectorRuntimePrecondition.DegradedStatus, Refused, Ran);

        Assert.Equal(Visibility.Visible, visibility);
        Assert.Equal(expected, text);
    }

    [Fact]
    public void WindowWithRows_ShowsNothing()
    {
        var (text, visibility) = ServerTab.CollectorOutcomeState(
            ServerName, isAzureSqlDatabase: false, "tempdb_stats", rowCount: 3, CollectorRuntimePrecondition.DegradedStatus, Refused, Ran);

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
        var (text, visibility) = ServerTab.CollectorOutcomeState(
            ServerName, isAzureSqlDatabase: false, "tempdb_stats", rowCount: 0, status, Refused, Ran);

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
        var (text, visibility) = ServerTab.CollectorOutcomeState(
            ServerName, isAzureSqlDatabase: true, "running_jobs", rowCount: 0, CollectorRuntimePrecondition.DegradedStatus, Refused, Ran);

        Assert.Equal(Visibility.Visible, visibility);
        Assert.Equal(ServerTab.EngineGapNote(ServerName, isAzureSqlDatabase: true, "running_jobs"), text);
    }
}
