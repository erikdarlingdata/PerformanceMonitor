/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8787: the measure judges a table by the space its live rows take, not by pages that hold no row. After scattered deletes and a
/// VACUUM nearly every page keeps a few rows, so a count of empty pages reads the table as dense while most of the file is free.
/// These tests build that table, a freshly loaded one, a table with a fillfactor, and one with a large compressible value, against a
/// real store.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class DarlingStoreTableMeasureSparseLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<DarlingStoreTableMeasureReader.StoreTableMeasure> MeasureAsync(NpgsqlDataSource dataSource, string table, CancellationToken ct)
    {
        var result = await DarlingStoreTableMeasureReader.MeasureAsync(dataSource, table, ct);
        Assert.Equal(DarlingStoreTableMeasureReader.StoreTableMeasureOutcome.Measured, result.Outcome);
        return result.Measure!;
    }

    [Fact]
    public async Task ASparseTable_WhereEveryPageKeepsRows_ReadsRewriteNeeded_AFreshOneAndAFillfactorOneReadDense()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* About 150 MB of rows, then 80 percent deleted at random. With about 33 rows to a page, almost no page ends up with no row at all. */
        await ExecAsync(connection, @"
CREATE TABLE collect.zz_sparse (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_sparse SELECT g, repeat('x', 200) FROM generate_series(1, 650000) AS g;
CREATE TABLE collect.zz_fresh (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_fresh SELECT g, repeat('x', 200) FROM generate_series(1, 100000) AS g;
CREATE TABLE collect.zz_fill50 (id integer, payload text) WITH (autovacuum_enabled = false, fillfactor = 50);
INSERT INTO collect.zz_fill50 SELECT g, repeat('x', 200) FROM generate_series(1, 100000) AS g;
SELECT setseed(0.42);
DELETE FROM collect.zz_sparse WHERE random() < 0.8;", ct);
        await ExecAsync(connection, "VACUUM collect.zz_sparse", ct);

        var sparse = await MeasureAsync(dataSource, "collect.zz_sparse", ct);
        Assert.True(sparse.EmptyShare < 0.05, $"pages with no row {sparse.EmptyShare}: the table must keep rows on nearly every page");
        Assert.InRange(sparse.FreeShare, 0.75, 0.85);
        Assert.True(sparse.FreeBytes >= DarlingStoreTableMeasureReader.RewriteMinFreeBytes, $"free bytes {sparse.FreeBytes}");
        Assert.True(sparse.LiveBytes > 0 && sparse.LiveBytes < sparse.TotalPages * (8192 - 24) * 0.25);
        Assert.Equal(100, sparse.Fillfactor);
        Assert.Equal(DarlingStoreTableMeasureReader.VerdictRewriteNeeded, sparse.VerdictKind);
        Assert.Contains("only a rewrite", sparse.Verdict, StringComparison.Ordinal);
        Assert.Contains("Live rows fill", sparse.Verdict, StringComparison.Ordinal);
        /* A live row on the last page leaves nothing for VACUUM to cut: the sentence says so, never "only 0 pages". */
        Assert.Equal(0, sparse.TrailingEmptyPages);
        Assert.Contains("a live row sits in the last page, so VACUUM cannot shrink the file", sparse.Verdict, StringComparison.Ordinal);
        Assert.DoesNotContain("only 0", sparse.Verdict, StringComparison.Ordinal);

        var fresh = await MeasureAsync(dataSource, "collect.zz_fresh", ct);
        Assert.Equal(DarlingStoreTableMeasureReader.VerdictDense, fresh.VerdictKind);
        Assert.True(fresh.FreeShare < 0.1, $"free share {fresh.FreeShare}");

        /* A fillfactor of 50 keeps half of every page free on purpose, which is not waste. */
        var filled = await MeasureAsync(dataSource, "collect.zz_fill50", ct);
        Assert.Equal(50, filled.Fillfactor);
        Assert.Equal(DarlingStoreTableMeasureReader.VerdictDense, filled.VerdictKind);
        Assert.True(filled.FreeShare < 0.1, $"free share {filled.FreeShare}");
        Assert.Contains("fillfactor", filled.Verdict, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheMeasure_AddsUpStoredSizes_AndNeverFlattensALargeCompressedValue()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* Fifty rows of 2 MB each that compress to a few KB. Read back whole, the rows are 100 MB. As stored they are a little over 1 MB. A column
           with a name that needs quoting and a dropped column are both in the table. */
        await ExecAsync(connection, @"
CREATE TABLE collect.zz_toast (id integer, ""Mixed Case"" text, big text, gone integer) WITH (autovacuum_enabled = false);
ALTER TABLE collect.zz_toast ALTER COLUMN big SET STORAGE EXTENDED;
INSERT INTO collect.zz_toast SELECT g, 'a', repeat('a', 2000000), g FROM generate_series(1, 50) AS g;
ALTER TABLE collect.zz_toast DROP COLUMN gone;", ct);

        var measure = await MeasureAsync(dataSource, "collect.zz_toast", ct);
        Assert.Equal(50, measure.Rows);
        Assert.True(measure.LiveBytes < 5_000_000, $"live bytes {measure.LiveBytes}: a large compressed value was counted at its full size");
        Assert.True(measure.LiveBytes >= 50 * 28, $"live bytes {measure.LiveBytes}");
    }
}
