/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8806: the index and table bloat estimates look each column's statistics up in <c>pg_stats</c> by schema, table and
/// column name together. Written as a plain join, the planner could merge on the schema name alone on a store with
/// thousands of chunks in one schema and filter the other two names afterwards, which kept tens of thousands of rows
/// out of billions of pairs and ran for minutes. These tests hold the ROWS the lookups return: the statements as they
/// ship must return exactly what the plain-join shape returned, on a store with many relations in one schema, an
/// expression index, a name-typed column, a partial index and a table that was never analyzed.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class PgStatsLookupRowsLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const int SmallTables = 120;

    /* The plain-join shape the statements had before the lookups became lateral. Each test swaps the shipped lookups for it
       and compares rows. */
    private const string OldIndexLookups = @"LEFT JOIN pg_catalog.pg_stats AS sp
      ON  sp.schemaname = i.schema_name
      AND sp.tablename  = i.table_name
      AND sp.attname    = pa.attname
      AND sp.inherited  = false
    LEFT JOIN pg_catalog.pg_stats AS se
      ON  i.indkey[ia.attnum - 1] = 0
      AND se.schemaname = i.schema_name
      AND se.tablename  = i.index_name
      AND se.attname    = ia.attname
      AND se.inherited  = false
";

    private const string OldTableLookup = @"LEFT JOIN pg_catalog.pg_stats AS sts
              ON  sts.schemaname = ns.nspname
              AND sts.tablename = tbl.relname
              AND sts.inherited = false
              AND sts.attname = att.attname
";

    private static readonly Regex IndexLookups = new(
        @"LEFT JOIN LATERAL \(SELECT s\.avg_width.*?\) AS sp\s+ON true\s+LEFT JOIN LATERAL.*?\) AS se\s+ON true[ \t]*\r?\n",
        RegexOptions.Singleline | RegexOptions.CultureInvariant);

    private static readonly Regex TableLookup = new(
        @"LEFT JOIN LATERAL \(SELECT s\.attname.*?\) AS sts\s+ON true[ \t]*\r?\n",
        RegexOptions.Singleline | RegexOptions.CultureInvariant);

    private static string WithPlainIndexLookups(string sql)
    {
        Assert.Matches(IndexLookups, sql);
        return IndexLookups.Replace(sql, OldIndexLookups.Replace("\r\n", "\n", StringComparison.Ordinal));
    }

    private static string WithPlainTableLookup(string sql)
    {
        Assert.Matches(TableLookup, sql);
        return TableLookup.Replace(sql, OldTableLookup.Replace("\r\n", "\n", StringComparison.Ordinal));
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /* Every column of every row, as text, in a fixed order: two statements agree when these lists are equal. The one column
       that is a clock reading (the statistics age in hours) differs between two runs and is left out. */
    private static async Task<List<string>> ReadRowsAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cells = new string[reader.FieldCount];
            for (var i = 0; i < cells.Length; i++)
            {
                cells[i] = reader.GetName(i) == "stats_hours"
                    ? "<clock>"
                    : reader.IsDBNull(i)
                    ? "<null>"
                    : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? "<null>";
            }

            rows.Add(string.Join("|", cells));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    /* Many small relations in one schema, each with a plain, a multi-column and an expression index, plus four tables large
       enough to pass the table estimate's size floor: analyzed with a name-typed column and a partial index, never
       analyzed, and one with NULLs in a wide indexed column. */
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, $@"
DO $seed$
BEGIN
    FOR i IN 1..{SmallTables} LOOP
        EXECUTE format('CREATE TABLE collect.zz_lk_%s (id bigint PRIMARY KEY, a int, b text, c timestamptz) WITH (autovacuum_enabled = false)', i);
        EXECUTE format('CREATE INDEX ON collect.zz_lk_%s (a, c)', i);
        EXECUTE format('CREATE INDEX ON collect.zz_lk_%s (lower(b))', i);
        EXECUTE format('INSERT INTO collect.zz_lk_%s SELECT g, g %% 7, repeat(''x'', g %% 20), now() FROM generate_series(1, 50) AS g', i);
        EXECUTE format('ANALYZE collect.zz_lk_%s', i);
    END LOOP;
END
$seed$;
CREATE TABLE collect.zz_lk_big (id integer, payload text, n name) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_lk_big SELECT g, repeat('x', 100 + g % 50), 'nm' FROM generate_series(1, 40000) AS g;
CREATE INDEX zz_lk_big_id ON collect.zz_lk_big (id);
CREATE INDEX zz_lk_big_payload ON collect.zz_lk_big (payload, id);
CREATE INDEX zz_lk_big_name ON collect.zz_lk_big (n);
CREATE INDEX zz_lk_big_partial ON collect.zz_lk_big (id) WHERE id < 100;
ANALYZE collect.zz_lk_big;
CREATE TABLE collect.zz_lk_fresh (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_lk_fresh SELECT g, repeat('y', 120) FROM generate_series(1, 40000) AS g;
CREATE INDEX zz_lk_fresh_id ON collect.zz_lk_fresh (id);
CREATE TABLE collect.zz_lk_wide (id integer, a text, b text, c text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_lk_wide SELECT g, repeat('a', 40), repeat('b', 40), repeat('c', 40) FROM generate_series(1, 40000) AS g;
CREATE INDEX zz_lk_wide_abc ON collect.zz_lk_wide (a, b, c);
UPDATE collect.zz_lk_wide SET b = NULL WHERE id % 3 = 0;
ANALYZE collect.zz_lk_wide;", ct);
        await ExecAsync(connection, "SELECT pg_stat_force_next_flush()", ct);
    }

    [Fact]
    public async Task TheIndexCollectorRows_AreTheSameAsThePlainJoinShape()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8806 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedAsync(connection, ct);

        var shipped = await ReadRowsAsync(connection, PgIndexBloatCollector.QueryText, ct);
        var plain = await ReadRowsAsync(connection, WithPlainIndexLookups(PgIndexBloatCollector.QueryText), ct);

        Assert.True(shipped.Count >= SmallTables * 3, $"the seeded indexes are all read, got {shipped.Count} rows");
        Assert.Equal(plain, shipped);
        /* The comparison is not vacuous: the name-typed index is suppressed with its reason, and the wide one is estimated. */
        Assert.Contains(shipped, r => r.Contains("|zz_lk_big_name|", StringComparison.Ordinal) && r.Contains("name-typed", StringComparison.Ordinal));
        Assert.Contains(shipped, r => r.Contains("|zz_lk_wide_abc|", StringComparison.Ordinal) && !r.Contains("name-typed", StringComparison.Ordinal));
    }

    [Fact]
    public async Task TheTableCollectorRows_AreTheSameAsThePlainJoinShape()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8806 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await SeedAsync(connection, ct);

        var shipped = await ReadRowsAsync(connection, PgTableBloatStatsCollector.QueryText, ct);
        var plain = await ReadRowsAsync(connection, WithPlainTableLookup(PgTableBloatStatsCollector.QueryText), ct);

        Assert.Contains(shipped, r => r.Contains("|zz_lk_wide|", StringComparison.Ordinal));
        Assert.Contains(shipped, r => r.Contains("|zz_lk_fresh|", StringComparison.Ordinal));
        Assert.Equal(plain, shipped);
    }

    [Fact]
    public async Task TheStoreIndexAndTableReads_ReturnTheSameRowsAsThePlainJoinShape()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8806 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await SeedAsync(connection, ct);

        var indexSql = DarlingStoreTableHealthReader.BuildIndexSql(scratch.TimescaleEnabled);
        var indexShipped = await ReadRowsAsync(connection, indexSql, ct);
        var indexPlain = await ReadRowsAsync(connection, WithPlainIndexLookups(indexSql), ct);
        Assert.Equal(indexPlain, indexShipped);

        var tableSql = DarlingStoreTableHealthReader.BuildSql(scratch.TimescaleEnabled);
        var tableShipped = await ReadRowsAsync(connection, tableSql, ct);
        var tablePlain = await ReadRowsAsync(connection, WithPlainTableLookup(tableSql), ct);
        Assert.Equal(tablePlain, tableShipped);

        /* The public read returns the seeded relations: every small table's indexes roll up to a row. */
        var rows = await DarlingStoreTableHealthReader.GetIndexesAsync(dataSource, ct);
        Assert.True(rows.Count(r => r.TableName.StartsWith("zz_lk_", StringComparison.Ordinal)) >= SmallTables * 3);
        Assert.Single(rows, r => r.IndexName == "zz_lk_wide_abc");
    }
}
