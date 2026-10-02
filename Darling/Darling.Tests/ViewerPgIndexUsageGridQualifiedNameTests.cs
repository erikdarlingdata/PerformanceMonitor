/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #3576, the Postgres sibling. The viewer's Index Usage grid showed the bare table name while the five other
/// table-naming grids on the Postgres tab show Schema as a column, so <c>public.events</c> and
/// <c>archive.events</c> read as one table. The store's reader carried <c>SchemaName</c> from the start; the
/// drop happened in TWO places — the display row had no <c>SchemaName</c> member for the mapper to fill, and
/// the grid bound <c>TableName</c> — so this pins the schema through the REAL mapper (<see cref="PgDisplay.IndexUsage"/>)
/// rather than through a hand-built display row, and then pins the grid's binding from source. A pin on the
/// property alone would pass with the mapper still dropping the schema, which is exactly the shape that was
/// shipped. Darling-only: Lite has no Postgres tab and the deprecated Dashboard has no such grid.
/// </summary>
public sealed class ViewerPgIndexUsageGridQualifiedNameTests
{
    /* ---------------- through the mapper ---------------- */

    [Fact]
    public void Mapper_CarriesTheSchema_AndFullNameIsSchemaDotTable()
    {
        var projected = PgDisplay.IndexUsage(Row(schema: "archive", table: "events"));

        Assert.Equal("archive", projected.SchemaName);
        Assert.Equal("events", projected.TableName);
        Assert.Equal("archive.events", projected.FullName);
    }

    [Fact]
    public void Mapper_NullSchema_FallsBackToBareTable()
    {
        /* The reader's SchemaName is nullable; the mapper writes "" for null, and "" is the fallback input. */
        var projected = PgDisplay.IndexUsage(Row(schema: null, table: "events"));

        Assert.Equal("", projected.SchemaName);
        Assert.Equal("events", projected.FullName);
    }

    [Fact]
    public void FullName_DistinguishesTheSameTableNameAcrossSchemas()
    {
        /* The reported defect in one assertion: these two rows used to render identically. */
        var publicRow = PgDisplay.IndexUsage(Row(schema: "public", table: "events"));
        var archiveRow = PgDisplay.IndexUsage(Row(schema: "archive", table: "events"));

        Assert.Equal(publicRow.TableName, archiveRow.TableName);
        Assert.NotEqual(publicRow.FullName, archiveRow.FullName);
    }

    /* ---------------- the grid, from source ---------------- */

    [Fact]
    public void IndexUsageGrid_TableColumn_BindsFullName()
    {
        var xaml = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml"));

        var start = xaml.IndexOf("x:Name=\"PgIndexUsageGrid\"", StringComparison.Ordinal);
        Assert.True(start >= 0, "ViewerServerTab.xaml: no element named PgIndexUsageGrid — the grid was renamed or removed.");
        var end = xaml.IndexOf("</DataGrid>", start, StringComparison.Ordinal);
        Assert.True(end > start, "ViewerServerTab.xaml: PgIndexUsageGrid has no closing </DataGrid>.");
        var grid = xaml.Substring(start, end - start);

        Assert.Contains("Header=\"Table\" Binding=\"{Binding FullName}\"", grid, StringComparison.Ordinal);

        /* The regression's fingerprint. */
        Assert.DoesNotContain("Binding=\"{Binding TableName}\"", grid, StringComparison.Ordinal);
    }

    /// <summary>
    /// The reader row with only the two facts under test varied. Every other argument is inert for the
    /// mapper's schema/table handling; the droppability inputs are the shape <c>DarlingPgIndexUsageReaderTests</c>
    /// uses for a plain, valid, non-constraint index.
    /// </summary>
    private static DarlingPgIndexUsageReader.PgIndexUsageRow Row(string? schema, string table) =>
        new(
            DatabaseName: "appdb",
            SchemaName: schema,
            TableName: table,
            IndexName: "events_idx",
            MeasuredAt: DateTime.UtcNow,
            TotalScans: 0,
            ScansInWindow: 0,
            TuplesRead: 0,
            TuplesFetched: 0,
            BlocksRead: 0,
            BlocksHit: 0,
            IndexBytes: 1_000_000,
            TableBytes: 9_000_000,
            IsUnique: false,
            IsPrimaryKey: false,
            IsValid: true,
            IsReady: true,
            IsReplicaIdentity: false,
            IsPartial: false,
            IsExpression: false,
            SupportsConstraint: false,
            IndexMethod: "btree",
            ColumnCount: 1,
            IndexDefinition: "CREATE INDEX events_idx ON archive.events (a)",
            LastScan: null,
            StatsReset: null,
            FirstSeenAt: DateTime.UtcNow.AddDays(-30),
            SampleCount: 5,
            StatsWereResetInWindow: false);
}
