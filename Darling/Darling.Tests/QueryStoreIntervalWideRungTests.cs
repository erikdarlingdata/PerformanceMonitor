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
/// <para>This file took over the "I am the top rung" claims that moved off <c>QueryStoreIntervalLatestRungTests</c>
/// (V143) when this rung landed, and hands them on to <c>ManagedConfVerdictsRungTests</c> (V146, #4215) when
/// that one did.</para>
/// </summary>
public sealed class QueryStoreIntervalWideRungTests
{
    private const int RungVersion = 145;
    private const int PreviousVersion = 144;

    /// <summary>This rung's sentinel ordinal in the viewer probe — a position within the signature, not its
    /// end, now that V146 has appended its own.</summary>
    private const int ProbeOrdinal = 120;

    private static PgMigrations.Migration V145 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("query-store-interval-wide", V145.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        /* Not `RungVersion == SchemaVersion` any more: that asserted this rung is the newest, which stopped
           being true when V146 landed. The invariant that outlives the handoff is that the LADDER's top and
           the declared version agree, which the two lines above already say. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
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
        var sql = V145.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Equal(3, CountOf(sql, "CREATE TABLE IF NOT EXISTS collect."));
        Assert.Equal(1, CountOf(sql, "CREATE UNIQUE INDEX IF NOT EXISTS ux_query_store_interval_wide"));
        Assert.DoesNotContain("ix_query_store_interval_wide_null_start", sql, StringComparison.Ordinal);
        Assert.Equal(4, CountOf(sql, "CREATE "));
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.query_store_interval_wide\n", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.query_store_interval_wide_coverage\n", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.query_store_interval_wide_pending\n", sql, StringComparison.Ordinal);
        Assert.Contains("WITH (fillfactor = 50);", sql, StringComparison.Ordinal);
        Assert.Contains("NULLS NOT DISTINCT;", sql, StringComparison.Ordinal);
        Assert.Contains("first_execution_time timestamp NOT NULL,", sql, StringComparison.Ordinal);

        /* No secondary index on the full table at all (F1, measured: a window index took HOT updates to 0%).
           The slicer's legacy-row probe and its near-empty partial index were removed (review-4341-r1 M1). */
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
    /// The viewer's schema map, given every sentinel up to this rung's ordinal, returns this rung's version, and with
    /// this rung's sentinel clear, the previous rung's: a sentinel at the wrong ordinal maps a store one rung off.
    /// </summary>
    [Fact]
    public void TheProbeMapsAStoreStoppedHereToThisRung()
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.True(ProbeOrdinal < arity - 1);
        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);
        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);
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
