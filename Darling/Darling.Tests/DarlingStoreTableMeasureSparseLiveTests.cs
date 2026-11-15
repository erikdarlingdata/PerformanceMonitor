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

    /* Rows whose big value cannot compress, so it is stored out of line in TOAST. #8807: pg_column_size reports such a value at its size in
       the TOAST table, so the measure used to add up the whole TOAST table, read "0 B is free" and call the table dense. */
    private const string RandomPlanTableSql = @"
CREATE TABLE collect.zz_toast_random (id integer, note text, plan text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_toast_random
SELECT g, 'n' || g, (SELECT string_agg(md5(g::text || ':' || i::text), '') FROM generate_series(1, 300) AS i)
FROM generate_series(1, 2000) AS g;";

    [Fact]
    public async Task ValuesStoredOutOfLine_CountAsTheirPointer_NotAsTheirSizeInTheToastTable()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8807 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await ExecAsync(connection, RandomPlanTableSql, ct);
        await ExecAsync(connection, "VACUUM collect.zz_toast_random", ct);

        /* Each plan is 9,600 characters of md5 text. The table's TOAST file holds about 19 MB of them and its heap about 15 pages. */
        var measure = await MeasureAsync(dataSource, "collect.zz_toast_random", ct);
        Assert.Equal(2000, measure.Rows);
        var heapBytes = measure.TotalPages * 8192;
        Assert.True(heapBytes < 1_000_000, $"heap bytes {heapBytes}: the test table must keep its plans out of line");
        Assert.True(measure.LiveBytes <= heapBytes, $"live bytes {measure.LiveBytes} against a heap of {heapBytes} bytes: out-of-line values were counted at their TOAST size");
        Assert.True(measure.LiveBytes >= 2000 * (28 + 4 + 18), $"live bytes {measure.LiveBytes}");
        Assert.DoesNotContain("0 B is free", measure.Verdict, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheBeforeSeventeenForm_CountsALargeValueAsItsPointerToo()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8807 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        /* Below 17 the chunk id form falls back to the storage form, so the comparison would be the storage form against itself. */
        await using (var versionCommand = new NpgsqlCommand("SELECT current_setting('server_version_num')::int", connection))
        {
            var serverVersion = (int)(await versionCommand.ExecuteScalarAsync(ct))!;
            Assert.SkipWhen(serverVersion < 170000, "pg_column_toast_chunk_id needs PostgreSQL 17, so below that both forms are the storage form and there is nothing to compare.");
        }

        await ExecAsync(connection, RandomPlanTableSql, ct);
        await ExecAsync(connection, "VACUUM collect.zz_toast_random", ct);

        long oid;
        await using (var oidCommand = new NpgsqlCommand("SELECT 'collect.zz_toast_random'::regclass::oid::bigint", connection))
        {
            oid = (long)(await oidCommand.ExecuteScalarAsync(ct))!;
        }

        /* The same table through both forms of the column-size expression: the one that asks pg_column_toast_chunk_id (PostgreSQL 17 and
           later) and the one for older servers, which a version no server reaches selects here. Both must come to the same bytes. */
        var chunkIdForm = await DataBytesAsync(connection, oid, "collect.zz_toast_random", DarlingStoreTableMeasureReader.LayoutSqlFor(170000), ct);
        var storageForm = await DataBytesAsync(connection, oid, "collect.zz_toast_random", DarlingStoreTableMeasureReader.LayoutSqlFor(int.MaxValue), ct);
        Assert.True(chunkIdForm < 500_000, $"chunk id form {chunkIdForm} bytes");
        Assert.Equal(chunkIdForm, storageForm);
    }

    /* A lowered toast_tuple_target. The toaster runs only for a row over 2032 bytes, so a row of about 1,000 bytes keeps its value inline even
       when the table's target is 128. The before-17 form must count that value at its size and not as a pointer. */
    [Fact]
    public async Task TheBeforeSeventeenForm_CountsAnInlineValueUnderTheToasterThresholdAtItsSize_WhenTheTargetIsLowered()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8807 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        await ExecAsync(connection, @"
CREATE TABLE collect.zz_toast_lowered (id integer, v text) WITH (toast_tuple_target = 128, autovacuum_enabled = false);
INSERT INTO collect.zz_toast_lowered
SELECT g, substr((SELECT string_agg(md5(g::text || ':' || i::text), '') FROM generate_series(1, 32) AS i), 1, 1000)
FROM generate_series(1, 500) AS g;", ct);
        await ExecAsync(connection, "VACUUM collect.zz_toast_lowered", ct);

        long oid;
        await using (var oidCommand = new NpgsqlCommand("SELECT 'collect.zz_toast_lowered'::regclass::oid::bigint", connection))
        {
            oid = (long)(await oidCommand.ExecuteScalarAsync(ct))!;
        }

        var bytes = await DataBytesAsync(connection, oid, "collect.zz_toast_lowered", DarlingStoreTableMeasureReader.LayoutSqlFor(int.MaxValue), ct);
        Assert.True(bytes >= 500 * 1000, $"data bytes {bytes}: an inline value of about 1,000 bytes was counted as a pointer");
    }

    private static async Task<long> DataBytesAsync(NpgsqlConnection connection, long oid, string qualifiedName, string layoutSql, CancellationToken ct)
    {
        string expression;
        await using (var layout = new NpgsqlCommand(layoutSql, connection))
        {
            layout.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Oid, Value = (uint)oid });
            await using var reader = await layout.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            expression = reader.GetString(0);
        }

        await using var measure = new NpgsqlCommand(DarlingStoreTableMeasureReader.MeasureSqlFor(expression, qualifiedName), connection);
        measure.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Oid, Value = (uint)oid });
        await using var result = await measure.ExecuteReaderAsync(ct);
        await result.ReadAsync(ct);
        return result.GetInt64(9);
    }
}
