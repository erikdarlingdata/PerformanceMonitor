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
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9099: a Lite panel whose collector needs a newer SQL Server than the monitored one says so when it is empty, instead of "no samples".
/// The panels, the Add and Edit server test and the MCP tools all take the version from the connection probe and the sentence from the
/// shared capability rule, so they agree on which servers are below the floor.
/// </summary>
public sealed class BelowFloorPanelNoteTests
{
    private const string ServerName = "LiteBelowFloor";
    private const string OwnText = "No samples in this window.";

    /// <summary>The collectors behind the panels this change wires: Active Queries, Top Queries (query_stats and procedure_stats), File I/O and Database Sizes.</summary>
    public static readonly TheoryData<string> FlooredPanelCollectors = new()
    {
        "query_snapshots", "query_stats", "procedure_stats", "file_io_stats", "database_size_stats",
    };

    private static string PanelText(string collector, int rowCount, int major, bool isAzure = false) =>
        ServerTab.EmptyPanelText(ServerName, isAzure, collector, rowCount, AwsRdsKind.None, major, OwnText);

    [Theory]
    [MemberData(nameof(FlooredPanelCollectors))]
    public void AnEmptyPanel_OnABelowFloorServer_SaysItIsNotCollectedWithTheVersionReason(string collector)
    {
        var text = PanelText(collector, rowCount: 0, major: 11);

        Assert.NotEqual(OwnText, text);
        Assert.Equal(
            CollectorEngineCapability.NotCollectedMessage(ServerName, CollectorEngineCapability.UnknownEngineEdition, engineKind: null, collector, AwsRdsKind.None, sqlMajorVersion: 11),
            text);
        Assert.Contains(CollectorEngineCapability.SupportedVersionsDescription, text, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(FlooredPanelCollectors))]
    public void AnEmptyPanel_OnASupportedServer_KeepsItsOwnText(string collector)
    {
        foreach (var major in new[] { 13, 15, 16, 17 })
        {
            Assert.Equal(OwnText, PanelText(collector, rowCount: 0, major));
        }
    }

    [Theory]
    [MemberData(nameof(FlooredPanelCollectors))]
    public void AnEmptyPanel_OnAnUnknownVersionOrAnAzureSqlDatabase_KeepsItsOwnText(string collector)
    {
        Assert.Equal(OwnText, PanelText(collector, rowCount: 0, major: 0));

        /* An Azure SQL Database reports major 12, which is not a product year there. */
        Assert.Equal(OwnText, PanelText(collector, rowCount: 0, major: 12, isAzure: true));
    }

    [Theory]
    [MemberData(nameof(FlooredPanelCollectors))]
    public void APanelWithRows_KeepsItsOwnText_OnABelowFloorServer(string collector)
    {
        Assert.Equal(OwnText, PanelText(collector, rowCount: 3, major: 11));
    }

    /* wait_stats has no floor: a panel on it keeps its ordinary sentence on the same server. */
    [Fact]
    public void ACollectorWithNoFloor_KeepsItsOwnText_OnABelowFloorServer()
    {
        Assert.Equal(OwnText, PanelText("wait_stats", rowCount: 0, major: 11));
    }

    [Fact]
    public void TheNote_OnABelowFloorServer_ShowsOverAnEmptyPanelOnly()
    {
        var empty = ServerTab.EngineGapState(ServerName, isAzureSqlDatabase: false, collectorNeverRan: false, "file_io_stats", rowCount: 0, AwsRdsKind.None, sqlMajorVersion: 11);
        var withRows = ServerTab.EngineGapState(ServerName, isAzureSqlDatabase: false, collectorNeverRan: false, "file_io_stats", rowCount: 4, AwsRdsKind.None, sqlMajorVersion: 11);
        var supported = ServerTab.EngineGapState(ServerName, isAzureSqlDatabase: false, collectorNeverRan: false, "file_io_stats", rowCount: 0, AwsRdsKind.None, sqlMajorVersion: 16);

        Assert.Equal(Visibility.Visible, empty.Visibility);
        Assert.Equal(Visibility.Collapsed, withRows.Visibility);
        Assert.Equal(Visibility.Collapsed, supported.Visibility);
        Assert.Equal(ServerTab.EngineGapNote(ServerName, isAzureSqlDatabase: false, "file_io_stats", AwsRdsKind.None, sqlMajorVersion: 11), empty.Text);
        Assert.Null(ServerTab.EngineGapNote(ServerName, isAzureSqlDatabase: false, "file_io_stats", AwsRdsKind.None, sqlMajorVersion: 16));
    }

    [Fact]
    public void TheConnectionTestNote_NamesTheVersionAndTheSupportedVersions_OnABelowFloorServer()
    {
        var note = SupportedVersionConnectionNote.For(sqlMajorVersion: 11, engineEdition: 2);

        Assert.NotNull(note);
        Assert.Contains(MonitoredEngineVersion.DescribeEngineVersion(null, 11, null, 2), note, StringComparison.Ordinal);
        Assert.Contains(CollectorEngineCapability.SupportedVersionsDescription, note, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(13, 2)]
    [InlineData(16, 3)]
    [InlineData(17, 2)]
    [InlineData(0, 2)]
    [InlineData(12, CollectorEngineCapability.AzureSqlDatabaseEngineEdition)]
    [InlineData(12, CollectorEngineCapability.AzureManagedInstanceEngineEdition)]
    public void TheConnectionTestNote_IsAbsent_OnASupportedUnknownOrAzureServer(int major, int edition)
    {
        Assert.Null(SupportedVersionConnectionNote.For(major, edition));
    }
}
