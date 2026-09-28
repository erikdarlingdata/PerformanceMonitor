/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for the Plan Insights strip's Server Context card (issue #4597): the row list built from a
/// <see cref="ServerMetadata"/>, in PerformanceStudio dev's order and text, and the quiet/empty state
/// when there is none. Mirrors PerformanceStudio dev's <c>ShowServerContext</c>
/// (erikdarlingdata/PerformanceStudio@85492a1,
/// <c>src/PlanViewer.App/Controls/PlanViewerControl.RuntimeSummary.cs:203-278</c>).
/// </summary>
public class Viewer4597Tests
{
    private static ServerMetadata FullMetadata() => new()
    {
        ServerName = "SQL01",
        Edition = "Enterprise Edition (64-bit)",
        ProductVersion = "16.0.1000.6",
        CpuCount = 8,
        PhysicalMemoryMB = 65536,
        MaxDop = 4,
        CostThresholdForParallelism = 50,
        MaxServerMemoryMB = 49152,
    };

    [Fact]
    public void Rows_NullMetadata_ReturnsEmptyList()
    {
        var rows = ServerContextCard.Rows(null);

        Assert.Empty(rows);
        Assert.False(ServerContextCard.HasContent(null));
    }

    [Fact]
    public void HasContent_TrueWhenMetadataPresent()
    {
        Assert.True(ServerContextCard.HasContent(FullMetadata()));
    }

    [Fact]
    public void Rows_FullMetadata_MatchesPerformanceStudiosOrderAndText()
    {
        var rows = ServerContextCard.Rows(FullMetadata());

        Assert.Equal(5, rows.Count);
        Assert.Equal(new ServerContextRow("Server", "SQL01 (Enterprise Edition), 16.0.1000.6"), rows[0]);
        Assert.Equal(new ServerContextRow("Hardware", "8 CPUs, 65,536 MB RAM"), rows[1]);
        Assert.Equal(new ServerContextRow("MAXDOP", "4"), rows[2]);
        Assert.Equal(new ServerContextRow("Cost threshold", "50"), rows[3]);
        Assert.Equal(new ServerContextRow("Max memory", "49,152 MB"), rows[4]);
    }

    [Fact]
    public void Rows_EditionWithout64BitSuffix_IsShownAsIs()
    {
        var metadata = FullMetadata();
        metadata.Edition = "Standard Edition";

        var rows = ServerContextCard.Rows(metadata);

        Assert.Equal("SQL01 (Standard Edition), 16.0.1000.6", rows[0].Value);
    }

    [Fact]
    public void Rows_NullServerName_ShowsUnknown()
    {
        var metadata = FullMetadata();
        metadata.ServerName = null;

        var rows = ServerContextCard.Rows(metadata);

        Assert.Equal("Unknown (Enterprise Edition), 16.0.1000.6", rows[0].Value);
    }

    [Fact]
    public void Rows_NullEdition_OmitsParenthetical()
    {
        var metadata = FullMetadata();
        metadata.Edition = null;

        var rows = ServerContextCard.Rows(metadata);

        Assert.Equal("SQL01, 16.0.1000.6", rows[0].Value);
    }

    [Fact]
    public void Rows_NullProductVersion_OmitsVersionSuffix()
    {
        var metadata = FullMetadata();
        metadata.ProductVersion = null;

        var rows = ServerContextCard.Rows(metadata);

        Assert.Equal("SQL01 (Enterprise Edition)", rows[0].Value);
    }

    [Fact]
    public void Rows_ZeroCpuCount_OmitsHardwareRow()
    {
        var metadata = FullMetadata();
        metadata.CpuCount = 0;

        var rows = ServerContextCard.Rows(metadata);

        Assert.DoesNotContain(rows, r => r.Label == "Hardware");
        Assert.Equal(4, rows.Count);
    }

    [Fact]
    public void Rows_ZeroMaxDopAndCostThreshold_StillShowTheirRows()
    {
        // A real "0" for MAXDOP or cost threshold is a fact worth showing (server default), not a
        // missing value — PerformanceStudio never drops these two rows.
        var metadata = FullMetadata();
        metadata.MaxDop = 0;
        metadata.CostThresholdForParallelism = 0;

        var rows = ServerContextCard.Rows(metadata);

        Assert.Contains(new ServerContextRow("MAXDOP", "0"), rows);
        Assert.Contains(new ServerContextRow("Cost threshold", "0"), rows);
    }
}
