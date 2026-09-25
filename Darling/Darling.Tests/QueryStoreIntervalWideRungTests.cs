/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V145 (#3953): the wide interval table beside V143's — three more tables, all engine-plain,
/// nothing on an existing table (V143's three included). The writer and the retention are pinned where they live
/// (<see cref="QueryStoreIntervalWideWriterTests"/>); this file is the RUNG: the ladder, the DDL, and the viewer
/// probe. Mirrors <c>QueryStoreIntervalLatestRungTests</c> (V143's own rung file) column for column.
///
/// <para>This file carries the "I am the top rung" claims that moved off <c>QueryStoreIntervalLatestRungTests</c>
/// (V143) when this rung landed. When the next rung lands they move on again.</para>
/// </summary>
public sealed class QueryStoreIntervalWideRungTests
{
    private const int RungVersion = 145;
    private const int PreviousVersion = 144;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 120;

    private static PgMigrations.Migration V144 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("query-store-interval-wide", V144.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// Three idempotent CREATEs and one unique index, engine-plain, and no ALTER of anything that already exists —
    /// in particular nothing touches V143's three tables. The index is <c>NULLS NOT DISTINCT</c> in the writer's
    /// identity column order, the heap is <c>fillfactor = 50</c>, and the conflict target
    /// <see cref="QueryStoreIntervalWide.UpsertSql"/> names is exactly the index's column list.
    /// </summary>
    [Fact]
    public void TheRungCreatesThreeTablesAndOneUniqueIndex_EnginePlain_AndTouchesNothingOfV143s()
    {
        var sql = V144.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Equal(3, CountOf(sql, "CREATE TABLE IF NOT EXISTS collect."));
        Assert.Equal(1, CountOf(sql, "CREATE UNIQUE INDEX IF NOT EXISTS ux_query_store_interval_wide"));
        Assert.Equal(1, CountOf(sql, "CREATE INDEX IF NOT EXISTS ix_query_store_interval_wide_null_start"));
        Assert.Equal(5, CountOf(sql, "CREATE "));
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.query_store_interval_wide\n", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.query_store_interval_wide_coverage\n", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.query_store_interval_wide_pending\n", sql, StringComparison.Ordinal);
        Assert.Contains("WITH (fillfactor = 50);", sql, StringComparison.Ordinal);
        Assert.Contains("NULLS NOT DISTINCT;", sql, StringComparison.Ordinal);
        Assert.Contains("first_execution_time timestamp NOT NULL,", sql, StringComparison.Ordinal);

        /* No secondary index on the full table (F1, measured: a window index took HOT updates to 0%) beyond
           B4's own near-empty partial index for the slicer's legacy-row probe. */
        Assert.DoesNotContain("btree", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, CountOf(sql, "CREATE UNIQUE INDEX"));

        /* Never V143's own objects. */
        foreach (var v143Only in new[] { "query_store_interval_latest\n", "query_store_interval_latest_coverage\n", "query_store_interval_latest_pending\n" })
        {
            Assert.DoesNotContain(v143Only, sql, StringComparison.Ordinal);
        }

        var statements = System.Text.RegularExpressions.Regex.Replace(sql, @"/\*.*?\*/", " ", System.Text.RegularExpressions.RegexOptions.Singleline);
        foreach (var forbidden in new[] { "ALTER ", "DROP ", "create_hypertable", "timescaledb", "INSERT ", "UPDATE " })
        {
            Assert.DoesNotContain(forbidden, statements, StringComparison.OrdinalIgnoreCase);
        }

        var indexColumns = string.Join(", ", sql[(sql.IndexOf("ON collect.query_store_interval_wide\n(", StringComparison.Ordinal) + "ON collect.query_store_interval_wide\n(".Length)..]
            .Split(')')[0]
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries));
        Assert.Equal(QueryStoreIntervalWide.IdentityColumns, indexColumns);

        /* The identity is V143's plus execution_type_desc (D4's design, ruled). */
        Assert.Equal(QueryStoreIntervalLatest.IdentityColumns + ", execution_type_desc", QueryStoreIntervalWide.IdentityColumns);
    }

    /// <summary>
    /// The viewer probe's sentinel carries this rung, and the map treats it as the TOP arm: a missing top arm maps
    /// a fully-migrated store one rung short, permanently, because <c>RequiredStoreSchemaVersion</c> is
    /// <c>StorageVersion.SchemaVersion</c>.
    /// </summary>
    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains(
            "table_name = 'query_store_interval_wide_pending'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.Equal(ProbeOrdinal, arity - 1);
        Assert.Equal("hasQueryStoreIntervalWide", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasQueryStoreIntervalWide)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasRawChunkIntervalRungHistory)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V145 sentinel arm — a fully-migrated store would map one rung low");
        Assert.True(thisArm < previousArm, "the V145 arm sits below V144's, so a current store maps one rung low");
        Assert.Contains(
            "return " + StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the tables are named in the probe line and nowhere in the arm's prose. */
        var armProseStart = viewer.LastIndexOf("/* V145 (#3953)", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the V145 arm has no comment block saying why it exists");
        Assert.DoesNotContain("query_store_interval_wide", viewer[armProseStart..thisArm], StringComparison.Ordinal);
    }

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        for (var at = haystack.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = haystack.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
