/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Darling.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Top Queries by Duration grid's empty sentence, shared by Lite and the Viewer: it shows only when the strip above the grid has
/// samples to explain and the ranking returned no row. An empty strip keeps its own sentence, and a grid with rows says nothing.
/// </summary>
public sealed class TopQueriesEmptyTextTests
{
    [Fact]
    public void AnEmptyGridUnderAStripWithSamples_SaysNoQueryRan_AndWhatTheStripCounts()
    {
        var text = TopQueriesEmptyText.Text(rowCount: 0, stripHasSamples: true);

        Assert.NotNull(text);
        Assert.StartsWith("No query ran in the selected time window.", text, System.StringComparison.Ordinal);
        Assert.Contains("strip", text, System.StringComparison.Ordinal);
    }

    [Fact]
    public void AGridWithRows_OrAnEmptyStrip_AddsNoSentence()
    {
        Assert.Null(TopQueriesEmptyText.Text(rowCount: 3, stripHasSamples: true));
        Assert.Null(TopQueriesEmptyText.Text(rowCount: 0, stripHasSamples: false));
    }
}
