/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9889: the note the Query Store Regressions grid shows when its read stopped at the row cap. The rule lives on the read, so these run
/// with no window: the rendered twin (<see cref="ViewerQueryStoreRegressionsCapAndSortTests"/>) shows the same note on a real tab.
/// </summary>
public sealed class ViewerQueryStoreRegressionsCapNoteTests
{
    private static readonly int Cap = ViewerDataService.QueryStoreRegressionsRowCap;

    private static ViewerQueryStoreRegressionsRead ReadOf(int rows, long? total) =>
        new(Enumerable.Range(1, rows).Select(i => new ViewerQueryStoreRegressionRow { QueryId = i, TotalCount = total }).ToList(), null);

    [Fact]
    public void ARead_ThatStoppedAtTheCap_SaysItIsTheTopRows_AndHowManyThereAre()
    {
        var note = ReadOf(Cap, 312).CapNote;

        Assert.NotNull(note);
        Assert.StartsWith($"Showing the top {Cap} of 312", note, StringComparison.Ordinal);
        Assert.Contains("ranked by total added duration", note, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCount_ReadsWithGroupSeparators_WhenItIsLarge()
    {
        Assert.StartsWith($"Showing the top {Cap} of 12,345", ReadOf(Cap, 12_345).CapNote, StringComparison.Ordinal);
    }

    [Fact]
    public void ARead_ThatStoppedAtTheCap_WithoutACount_SaysItIsTheTopRows_AndWhatTheyAreRankedBy()
    {
        var note = ReadOf(Cap, null).CapNote;

        Assert.NotNull(note);
        Assert.StartsWith($"Showing the top {Cap}, ranked by total added duration", note, StringComparison.Ordinal);
        Assert.DoesNotContain($"top {Cap} of", note, StringComparison.Ordinal);
    }

    [Fact]
    public void ARead_UnderTheCap_HasNoNote()
    {
        Assert.Null(ReadOf(Cap - 1, Cap - 1).CapNote);
        Assert.Null(ReadOf(Cap - 1, null).CapNote);
        Assert.Null(ReadOf(1, 1).CapNote);
        Assert.Null(new ViewerQueryStoreRegressionsRead(new List<ViewerQueryStoreRegressionRow>(), null).CapNote);
    }

    [Fact]
    public void ARead_ThatHoldsExactlyTheCap_AndCountedNothingMore_HasNoNote()
    {
        Assert.Null(ReadOf(Cap, Cap).CapNote);
    }

    [Fact]
    public void ARead_OneRowOverTheCount_HasANote()
    {
        Assert.StartsWith($"Showing the top {Cap} of {Cap + 1}", ReadOf(Cap, Cap + 1).CapNote, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTotal_IsTheOneTheReadCountedBesideItsRows()
    {
        Assert.Equal(312, ReadOf(3, 312).TotalCount);
        Assert.Null(ReadOf(3, null).TotalCount);
        Assert.Null(ReadOf(0, 312).TotalCount);
    }
}
