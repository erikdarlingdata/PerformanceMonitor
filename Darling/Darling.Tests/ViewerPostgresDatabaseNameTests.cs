/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7731: the Viewer's Top Query Shapes grid names the database. The name when the row has one, else <c>database &lt;id&gt;</c> with the id
/// printed plainly (no thousands separator: an oid is an identifier, not a count).
/// </summary>
public class ViewerPostgresDatabaseNameTests
{
    private static DarlingPgStatementReader.PgStatementRow Row(long databaseId, string? databaseName) => new(
        QueryId: 1, DatabaseId: databaseId, Calls: 3, TotalExecTimeMs: 4, RowsReturned: 5, MaxExecTimeMs: 6.0,
        SharedBlocksHit: 1, SharedBlocksRead: 1, StorageBlocksRead: null, OrcacheBlocksHit: null,
        TempBlocksRead: 0, TempBlocksWritten: 0, WalBytes: null, MaxPeakMemBytes: null,
        DatabaseName: databaseName);

    [Fact]
    public void ARowWithAName_ShowsTheName()
    {
        Assert.Equal("appdb", PgDisplay.Statement(Row(16384, "appdb")).DatabaseId);
    }

    [Fact]
    public void ARowWithoutAName_ShowsDatabaseAndThePlainId()
    {
        Assert.Equal("database 16384", PgDisplay.Statement(Row(16384, null)).DatabaseId);
    }
}
