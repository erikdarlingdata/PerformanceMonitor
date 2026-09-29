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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V155 (#4765): one nullable <c>timestamp</c> column, <c>interval_end_time_utc</c>, on
/// <c>collect.query_store_stats</c> and on <c>collect.query_store_interval_wide</c> (V145) - when the Query Store
/// interval a row belongs to ENDED, the counterpart of V41's <c>interval_start_time_utc</c>. A rate divides an
/// interval's totals by the interval's length, and until now the only length a read had was the time since the
/// previous STORED interval; Query Store stores no row for an interval with no executions, so an interval that
/// follows a quiet one divided by the gap plus its own length. This rung only stores the end. No DEFAULT, no
/// backfill (nothing can reconstruct it for rows already stored), no new table, no read changes.
///
/// <para>This file is the RUNG: the ladder, the DDL, the collector and compose carry, the viewer probe, and the
/// live schema-after-migrate proof, including that a store which climbs from V154 ends up with the same physical
/// column order as a fresh one (both bulk writers are positional). The collector's SQL and reader are
/// <c>Lite.Tests/QueryStoreCollectorDefinitionTests</c>; Lite's twin is <c>Lite.Tests/QueryStoreIntervalEndRungTests</c>.</para>
/// </summary>
/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres and never touches the
   shared store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class QueryStoreIntervalEndRungTests
{
    private const int RungVersion = 155;
    private const int PreviousVersion = 154;

    /// <summary>This rung's sentinel ordinal in the viewer probe. No longer the last argument - V156 (#4834) appended its
    /// own - so this is a position within the signature rather than its end.</summary>
    private const int ProbeOrdinal = 130;

    private const string Column = "interval_end_time_utc";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration V155 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /// <summary>The rung is registered in a dense ladder. The "I am the top rung" half of this claim moved to
    /// <c>CheckpointLongestSyncRungTests</c> (V156, #4834) with the top.</summary>
    [Fact]
    public void TheRungIsRegisteredInADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("query-store-interval-end", V155.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// The rung is two nullable ADD COLUMNs and the passthrough refresh, and nothing else: a DEFAULT or a NOT NULL
    /// would rewrite a compressed hypertable, and any DML would move data the census does not declare.
    /// </summary>
    [Fact]
    public void TheRungAddsOneNullableTimestampToEachOfTwoTables_AndRefreshesThePassthrough()
    {
        var sql = V155.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains($"ALTER TABLE collect.query_store_stats ADD COLUMN IF NOT EXISTS {Column} timestamp;", sql, StringComparison.Ordinal);
        Assert.Contains($"ALTER TABLE collect.query_store_interval_wide ADD COLUMN IF NOT EXISTS {Column} timestamp;", sql, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(sql, "ALTER TABLE").Count);
        Assert.Equal(2, Regex.Matches(sql, "ADD COLUMN IF NOT EXISTS").Count);

        /* V41's idiom: the bare passthrough statement, so the drift guard that scans for that form sees it. */
        Assert.Contains("CREATE OR REPLACE VIEW v_query_store_stats AS SELECT * FROM query_store_stats;", sql, StringComparison.Ordinal);
        Assert.Contains("v_query_store_stats", PgSchemaGenerator.AllPassthroughViews);

        var body = Regex.Replace(sql, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        foreach (var absent in new[] { "DEFAULT", "NOT NULL", "UPDATE ", "INSERT ", "DELETE ", "DROP " })
        {
            Assert.DoesNotContain(absent, body, StringComparison.Ordinal);
        }

        var declared = QueryStoreCollector.Instance.PayloadColumns[^1];
        Assert.Equal(Column, declared.Name);
        Assert.Equal(CollectorColumnType.Timestamp, declared.Type);
        Assert.Equal("timestamp", PgSchemaGenerator.TypeFor(declared));
    }

    /// <summary>
    /// A FRESH store builds <c>query_store_stats</c> from the generated schema, so the generated table and the
    /// binary COPY both end with the column, where the ALTER puts it on an upgraded store.
    /// </summary>
    [Fact]
    public void TheGeneratedStatsTable_AndTheCopy_EndWithTheColumn()
    {
        var generated = PgSchemaGenerator.CreateTable(QueryStoreCollector.Instance).Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains($"    interval_start_time_utc timestamp,\n    {Column} timestamp\n", generated, StringComparison.Ordinal);

        Assert.EndsWith($", interval_start_time_utc, {Column}) FROM STDIN (FORMAT BINARY)", PgCollectorRowWriter.CopyCommandFor(QueryStoreCollector.Instance), StringComparison.Ordinal);
    }

    /// <summary>
    /// The interval-wide compose carries the column wherever it carries <c>interval_start_time_utc</c>: the INSERT
    /// column list, the SELECT off the stats table, and the upsert's SET, in that order and last.
    /// </summary>
    [Fact]
    public void TheIntervalWideCompose_CarriesTheColumn_InTheInsertTheSelectAndTheUpsert()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "QueryStoreIntervalWide.cs")
            .Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains($"        interval_start_time_utc,\n        {Column}\n    )\n    SELECT DISTINCT ON", source, StringComparison.Ordinal);
        Assert.Contains($"        s.interval_start_time_utc,\n        s.{Column}\n    FROM collect.query_store_stats AS s", source, StringComparison.Ordinal);
        Assert.Contains($"        interval_start_time_utc = EXCLUDED.interval_start_time_utc,\n        {Column} = EXCLUDED.{Column}\n    WHERE", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The viewer probe's sentinel carries this rung at its own ordinal, and the map's arm for it returns 155: a
    /// sentinel present at only some of the probe's sites shifts every LATER ordinal onto the wrong column. The
    /// top-arm half of this claim moved to <c>CheckpointLongestSyncRungTests</c> (V156, #4834) with the top.
    /// </summary>
    [Fact]
    public void TheProbeMapsAStoreStoppedHereToThisRung()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains($"column_name = '{Column}'", probe, StringComparison.Ordinal);
        Assert.Contains("table_name = 'query_store_interval_wide'", probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.True(ProbeOrdinal < arity - 1);
        Assert.Equal("hasQueryStoreIntervalEnd", method.GetParameters()[ProbeOrdinal].Name);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasQueryStoreIntervalEnd)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasIntervalWideFirstExecIndex)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V155 sentinel arm - a fully-migrated store would map one rung short");
        Assert.True(thisArm < previousArm, "the V155 arm sits below V154's, so a current store maps one rung short");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    /// <summary>
    /// The LIVE schema after migrate: both tables have the column, timestamp, nullable, no DEFAULT, and LAST in
    /// physical order. Run against a pre-V155 build this is RED - the interval-wide table has no such column.
    /// </summary>
    [Fact]
    public async Task AfterMigrate_BothTablesHaveTheColumn_LastInPhysicalOrder()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V155 schema pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await AssertLastColumnAsync(connection, "query_store_stats", ct);
            await AssertLastColumnAsync(connection, "query_store_interval_wide", ct);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// A store that stopped at V154 (the column gone from both tables, the stamp gone) climbs the rung and ends up
    /// with the same physical column order, per table, as the freshly migrated store it started from: the ALTER
    /// appends, and the fresh store's tables already end with the column. Both bulk writers are positional, so a
    /// difference here would write shifted values on one of the two stores.
    /// </summary>
    [Fact]
    public async Task AStoreThatStoppedAtV154_ClimbsTheRung_ToTheSameColumnOrderAsAFreshStore()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V155 fresh-versus-upgraded pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            var freshStats = await ColumnOrderAsync(connection, "query_store_stats", ct);
            var freshWide = await ColumnOrderAsync(connection, "query_store_interval_wide", ct);
            var freshView = await ColumnOrderAsync(connection, "v_query_store_stats", ct);
            Assert.Equal($"{Column}:timestamp without time zone", freshStats[^1]);
            Assert.Equal($"{Column}:timestamp without time zone", freshWide[^1]);
            Assert.Equal($"{Column}:timestamp without time zone", freshView[^1]);

            /* Roll back to a V154 store. The passthrough view selects * from the stats table, so it depends on
               the column and goes first; it is put back over the narrower table, the state an existing store is in
               when the rung runs and re-expands it. */
            await ExecAsync(connection, "DROP VIEW IF EXISTS collect.v_query_store_stats", ct);
            await ExecAsync(connection, $"ALTER TABLE collect.query_store_stats DROP COLUMN {Column}", ct);
            await ExecAsync(connection, $"ALTER TABLE collect.query_store_interval_wide DROP COLUMN {Column}", ct);
            await ExecAsync(connection, "CREATE VIEW collect.v_query_store_stats AS SELECT * FROM collect.query_store_stats", ct);
            await ExecAsync(connection, $"DELETE FROM collect.darling_schema_version WHERE version >= {RungVersion.ToString(CultureInfo.InvariantCulture)}", ct);

            Assert.Equal(1, await PgMigrations.MigrateAsync(connection, ct));
            Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));

            Assert.Equal(freshStats, await ColumnOrderAsync(connection, "query_store_stats", ct));
            Assert.Equal(freshWide, await ColumnOrderAsync(connection, "query_store_interval_wide", ct));

            /* The refreshed passthrough lists the same columns in the same order as the fresh store's, so the new column is there and last. */
            Assert.Equal(freshView, await ColumnOrderAsync(connection, "v_query_store_stats", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private static async Task AssertLastColumnAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var describe = new NpgsqlCommand(
            "SELECT data_type, is_nullable, column_default, ordinal_position = (SELECT MAX(ordinal_position) FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = $1) " +
            "FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = $1 AND column_name = 'interval_end_time_utc'", connection);
        describe.Parameters.AddWithValue(table);
        await using var reader = await describe.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"{table} has no {Column} column after MigrateAsync");
        Assert.Equal("timestamp without time zone", reader.GetString(0));
        Assert.Equal("YES", reader.GetString(1));
        Assert.True(reader.IsDBNull(2), $"{table}.{Column} has a DEFAULT, which a compressed hypertable would have to rewrite to honour");
        Assert.True(reader.GetBoolean(3), $"{table}.{Column} is not the LAST column, so an upgraded store and a fresh one disagree on where it sits");
    }

    /// <summary>Column name and type, in physical order, so two stores' shapes compare as one list each.</summary>
    private static async Task<List<string>> ColumnOrderAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT column_name || ':' || data_type FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = $1 ORDER BY ordinal_position", connection);
        command.Parameters.AddWithValue(table);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var columns = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
