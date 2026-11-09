/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8439: the families of statement shapes the live tests fill <c>pg_stat_statements</c> with share no shape. The module
/// keys an entry by database as well as by query id, and more than one family runs on one database: the chain's
/// one-free-slot fill is followed by the headroom step, and the test that lands a pass inside a batch runs the forcing
/// fill, the one-free-slot fill and its hook's headroom shapes on one scratch database. A shape already in the table for
/// that database adds no entry, and the one-free-slot fill counts its entries after each batch, so a fill that runs after
/// another on the same database must bring shapes of its own. The module's query id ignores constants, so two texts here
/// differ only where their structure does: the operator, the columns, the terms or the suffix.
/// </summary>
public sealed class PgStatementFillerFamiliesTests
{
    /// <summary>The module's default table width, and the widest table either fill covers.</summary>
    [Theory]
    [InlineData(5000)]
    [InlineData(20000)]
    public void TheOneFreeSlotFill_SharesNoShapeWithTheForcingFill_OrWithEitherHeadroomFamily(int max)
    {
        var side = (int)Math.Ceiling(Math.Sqrt((max + 1) / 8.0)) + 1;
        /* A fill that carries on past five eviction passes, each freeing five percent, runs up to a quarter more shapes than the table is wide. */
        var count = max + max / 4;
        var forcing = Enumerable.Range(0, count).Select(n => PgStatementTable.Shape(n, side)).ToArray();
        var oneFreeSlot = Enumerable.Range(0, count).Select(n => PgOwnStatementRoleLiveTests.FillerStatement(n, side)).ToArray();
        var headroom = Enumerable.Range(0, PgStatementTable.Headroom + 16).Select(PgStatementTable.HeadroomShape)
            .Concat(Enumerable.Range(0, PgOwnStatementRoleLiveTests.ChainEntries + 16).Select(PgOwnStatementRoleLiveTests.HeadroomStatement))
            .ToArray();

        /* The one-free-slot fill needs a distinct shape per slot it fills, up to the whole table. */
        Assert.Equal(count, oneFreeSlot.Distinct(StringComparer.Ordinal).Count());
        Assert.Empty(oneFreeSlot.Intersect(forcing, StringComparer.Ordinal));
        Assert.Empty(oneFreeSlot.Intersect(headroom, StringComparer.Ordinal));
        Assert.Empty(forcing.Intersect(headroom, StringComparer.Ordinal));
    }
}
