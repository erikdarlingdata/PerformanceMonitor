/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4821: the parameter-sensitivity drill-down reads its rows before the cap, so it resolves statement text in a
/// second read, by digest, for the rows the reader keeps (#3902's rule: text for the plans that print, never for the
/// window's rows). Which digests that read is asked for is decided by <see cref="PgDrillDownCollector.DigestsToResolve"/>,
/// pure so it is pinned here without a database. The live half, which runs both statements against a real
/// store, is <see cref="ParameterSensitiveDrillDownTextLiveTests"/>.
/// </summary>
public sealed class ParameterSensitiveTextResolutionTests
{
    private static byte[] Digest(params byte[] bytes) => bytes;

    [Fact]
    public void DigestsToResolve_ReturnsOnlyTheKeptRowsWithNoInlineText()
    {
        var withInlineText = Digest(1, 1);
        var digestOnly = Digest(2, 2);
        var alsoDigestOnly = Digest(3, 3);

        var resolved = PgDrillDownCollector.DigestsToResolve(
        [
            ("SELECT 1", withInlineText),
            (null, digestOnly),
            ("SELECT 2", null),
            (null, alsoDigestOnly),
        ]);

        /* Inline text wins over the dimension's, so a row that has it never needs a dimension read. */
        Assert.Equal(2, resolved.Count);
        Assert.Equal(digestOnly, resolved[0]);
        Assert.Equal(alsoDigestOnly, resolved[1]);
    }

    [Fact]
    public void DigestsToResolve_ReturnsEachDigestOnce_ComparedByContent()
    {
        /* Two plans of one statement share a digest, and the reader materializes a new array for each row. */
        var resolved = PgDrillDownCollector.DigestsToResolve(
        [
            (null, Digest(7, 7, 7)),
            (null, Digest(9)),
            (null, Digest(7, 7, 7)),
            (null, Digest(9)),
        ]);

        Assert.Equal(2, resolved.Count);
        Assert.Equal(new byte[] { 7, 7, 7 }, resolved[0]);
        Assert.Equal(new byte[] { 9 }, resolved[1]);
    }

    [Fact]
    public void DigestsToResolve_ReturnsNothingWhenEveryKeptRowCarriesInlineText()
    {
        var resolved = PgDrillDownCollector.DigestsToResolve(
        [
            ("SELECT 1", Digest(1)),
            ("SELECT 2", Digest(2)),
            ("SELECT 3", null),
        ]);

        /* No digest to look up means no second statement at all. */
        Assert.Empty(resolved);
        Assert.Empty(PgDrillDownCollector.DigestsToResolve([]));
    }

    [Fact]
    public void DigestsToResolve_TreatsEmptyInlineTextAsText_AndSkipsRowsWithoutADigest()
    {
        var resolved = PgDrillDownCollector.DigestsToResolve(
        [
            /* The view's COALESCE let inline text win whenever it was not NULL, even when empty. */
            ("", Digest(4)),
            /* Nothing to look up, and nothing to fall back on: the row reads as empty text. */
            (null, null),
            (null, Digest(5)),
        ]);

        Assert.Single(resolved);
        Assert.Equal(Digest(5), resolved[0]);
    }
}
