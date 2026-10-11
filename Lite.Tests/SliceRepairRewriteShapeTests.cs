/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5653: a repaired archive file kept its data but grew, 17.4 to 23.4 MB for 40 rows removed. Two causes, both
/// in the rewrite: the GROUP BY returned rows in hash order, so the time-ordered layout the archiver wrote (and
/// the compression that rides on it) was lost; and <c>SUM(execution_count)</c> is a HUGEINT, which parquet stores
/// as DOUBLE, so a BIGINT column came back as DOUBLE.
/// </summary>
public sealed class SliceRepairRewriteShapeTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int Queries = 300;
    private const int Intervals = 60;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _dir;

    public SliceRepairRewriteShapeTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dir = Path.Combine(Path.GetTempPath(), $"pm-5653-shape-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch (IOException)
        {
            /* A scratch directory that outlives one run is noise. */
        }
    }

    /// <summary>The rewrite keeps every column's type: execution_count came back DOUBLE.</summary>
    [Fact]
    public async Task Rewrite_KeepsColumnTypes_AndTotals()
    {
        var r = await RewriteAsync();

        Assert.Equal(r.TypesBefore, r.TypesAfter);
        Assert.Equal(r.TotalBefore, r.TotalAfter);
    }

    /// <summary>
    /// The rewrite keeps the rows in their order: a file that lost 40 of 18,040 rows must not be bigger by more
    /// than a rounding error. Hash order made it about 10 percent bigger on this data.
    /// </summary>
    [Fact]
    public async Task Rewrite_KeepsTheTimeOrder_SoTheFileDoesNotGrow()
    {
        var r = await RewriteAsync();

        Assert.True(
            r.AfterBytes <= r.BeforeBytes * 1.02,
            $"the repaired file grew from {r.BeforeBytes:N0} to {r.AfterBytes:N0} bytes after losing 40 rows");
    }

    /// <summary>
    /// The size test above is a proxy. This reads the rewritten file itself: its collection_time column, in the file's
    /// own row order, never steps backwards, and no row went missing from the order check.
    /// </summary>
    [Fact]
    public async Task Rewrite_KeepsTheRowsInTheirOriginalOrder()
    {
        var r = await RewriteAsync();

        Assert.Equal(Queries * Intervals, r.RowsAfter);
        Assert.Equal(0, r.TimeInversionsAfter);
    }

    /// <summary>
    /// The row order is explicit (ORDER BY the file's row number) when the temp allowance can hold the sort, and is
    /// left to the join when it cannot: sorting the wide rows spilled 9.3 times the file in the measurement, so a
    /// tight disk would otherwise turn a file it can rewrite into a failure.
    /// </summary>
    [Fact]
    public void Rewrite_SortsByFilePosition_OnlyWhenTheTempAllowanceCanHoldTheSort()
    {
        const long MiB = 1024 * 1024;
        var columns = new List<(string Name, string Type)>
        {
            ("collection_id", "BIGINT"), ("collection_time", "TIMESTAMP"), ("server_id", "INTEGER"),
            ("query_id", "BIGINT"), ("plan_id", "BIGINT"), ("execution_count", "BIGINT"),
        };
        var key = QueryStoreSliceRepairService.KeyColumnsFor(columns.Select(c => c.Name).ToList());

        Assert.Contains("ORDER BY t.file_row_number", QueryStoreSliceRepairService.BuildRewriteSelect(columns, key, "f.parquet", ordered: true), StringComparison.Ordinal);
        Assert.DoesNotContain("ORDER BY", QueryStoreSliceRepairService.BuildRewriteSelect(columns, key, "f.parquet", ordered: false), StringComparison.Ordinal);

        Assert.True(QueryStoreSliceRepairService.RewriteKeepsExplicitOrder(4096 * MiB, 272 * MiB));
        Assert.False(QueryStoreSliceRepairService.RewriteKeepsExplicitOrder(4096 * MiB, 400 * MiB));
        Assert.False(QueryStoreSliceRepairService.RewriteKeepsExplicitOrder(256 * MiB, 272 * MiB));
    }

    private sealed record Rewritten(
        long BeforeBytes, long AfterBytes, string TypesBefore, string TypesAfter, long TotalBefore, long TotalAfter,
        long RowsAfter, long TimeInversionsAfter);

    private async Task<Rewritten> RewriteAsync()
    {
        var file = Path.Combine(_dir, "202610_query_store_stats.parquet");
        var sql = file.Replace("\\", "/", StringComparison.Ordinal);
        using var con = _duckDb.CreateConnection();
        await con.OpenAsync(TestContext.Current.CancellationToken);

        /* A time-ordered archive like the archiver writes, built from the real table's own columns so the file has
           the real shape: 18,000 rows, plus 40 extra slices of the first intervals, which are the split rows. */
        await ExecAsync(con, $"CREATE TEMP TABLE gen AS SELECT * FROM {await GeneratedSourceAsync(con)}");
        await ExecAsync(con,
            "INSERT INTO gen SELECT * REPLACE (CAST(collection_id + 10000000 AS BIGINT) AS collection_id, " +
            "CAST(3 AS BIGINT) AS execution_count) FROM gen WHERE collection_id < 40");
        await ExecAsync(con,
            $"COPY (SELECT * FROM gen ORDER BY collection_time, collection_id) TO '{sql}' ({ParquetCompaction.ArchiveCopyOptions})");

        var beforeBytes = new FileInfo(file).Length;
        var beforeTypes = await SchemaAsync(con, sql);
        var totalBefore = await ScalarAsync(con, "SELECT CAST(SUM(execution_count) AS BIGINT) FROM gen");

        var service = new QueryStoreSliceRepairService(_duckDb, _dir);
        var result = await service.RepairAsync(cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(result.FullyRepaired, string.Join(" | ", result.Failures));
        Assert.Equal(40, result.RowsRemoved);

        return new Rewritten(
            beforeBytes,
            new FileInfo(file).Length,
            beforeTypes,
            await SchemaAsync(con, sql),
            totalBefore,
            await ScalarAsync(con, $"SELECT CAST(SUM(execution_count) AS BIGINT) FROM read_parquet('{sql}')"),
            await ScalarAsync(con, $"SELECT COUNT(*) FROM read_parquet('{sql}')"),
            await ScalarAsync(
                con,
                "SELECT COUNT(*) FROM (SELECT collection_time, lag(collection_time) OVER (ORDER BY file_row_number) AS previous " +
                $"FROM read_parquet('{sql}', file_row_number = true)) WHERE collection_time < previous"));
    }

    private static async Task ExecAsync(DuckDBConnection con, string sql)
    {
        using var cmd = con.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task<long> ScalarAsync(DuckDBConnection con, string sql)
    {
        using var cmd = con.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken), CultureInfo.InvariantCulture);
    }

    private static async Task<string> SchemaAsync(DuckDBConnection con, string sqlPath)
    {
        using var cmd = con.CreateCommand();
        cmd.CommandText =
            $"SELECT string_agg(name || ':' || type, ', ') FROM parquet_schema('{sqlPath}') WHERE name <> 'duckdb_schema'";
        return Convert.ToString(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken), CultureInfo.InvariantCulture) ?? "";
    }

    /// <summary>
    /// Rows for every column of the live <c>query_store_stats</c> table, generated by name and type so the file
    /// follows the schema as it grows. Queries repeat across intervals the way the real data does.
    /// </summary>
    private async Task<string> GeneratedSourceAsync(DuckDBConnection con)
    {
        var cols = new List<(string Name, string Type)>();
        using (var cmd = con.CreateCommand())
        {
            cmd.CommandText =
                "SELECT column_name, data_type FROM information_schema.columns " +
                "WHERE table_name = 'query_store_stats' AND table_schema = 'main' ORDER BY ordinal_position";
            using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            while (await reader.ReadAsync(TestContext.Current.CancellationToken))
            {
                cols.Add((reader.GetString(0), reader.GetString(1)));
            }
        }

        var time = $"TIMESTAMP '2026-10-01 00:00:00' + to_minutes(CAST(i // {Queries} AS INTEGER))";
        var exprs = new List<string>();
        var index = 0;
        foreach (var (name, type) in cols)
        {
            index++;
            var expr = name switch
            {
                "collection_id" => "i",
                "collection_time" or "first_execution_time" or "last_execution_time" => time,
                "server_id" => "9654",
                "database_name" => "'DB' || (q % 5)",
                "query_id" => "q",
                "plan_id" => "q * 10",
                "runtime_stats_interval_id" => $"i // {Queries}",
                "execution_type_desc" => "'Regular'",
                "query_text" => "repeat('SELECT col1, col2 FROM dbo.SomeTable WHERE x = @p AND y = ', 4) || q",
                "query_plan_text" => "repeat('<ShowPlanXML><RelOp NodeId=\"0\"/></ShowPlanXML>', 40) || q",
                "execution_count" => "1 + hash(i) % 50",
                _ when type.Contains("VARCHAR", StringComparison.OrdinalIgnoreCase) => "'v' || (q % 50)",
                _ when type.Equals("BOOLEAN", StringComparison.OrdinalIgnoreCase) => "false",
                _ when type.Contains("TIMESTAMP", StringComparison.OrdinalIgnoreCase) => "TIMESTAMP '2026-10-01 00:00:00'",
                _ when type.Equals("DOUBLE", StringComparison.OrdinalIgnoreCase) => $"(hash(i + {index}) % 100000) / 7.0",
                _ => $"hash(i + {index}) % 100000",
            };
            exprs.Add($"CAST({expr} AS {type}) AS {name}");
        }

        return $"(SELECT {string.Join(", ", exprs)} FROM (SELECT i, i % {Queries} AS q FROM range({Queries * Intervals}) t(i)))";
    }
}
