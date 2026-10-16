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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V142 (#4196): ONE new index, <c>idx_index_object_stats_server_time</c> on
/// <c>collect.index_object_stats (server_id, collection_time DESC)</c> — the supporting index for the anomaly
/// detector's "latest two object-stats snapshots" read (<c>PgAnomalyDetector.ObjectGrowthSql</c> /
/// <c>ObjectContentionSql</c>'s <c>snaps</c> CTE), which previously had no index leading with
/// <c>collection_time</c> second and fell back to a fleet-wide <c>Custom Scan (SkipScan)</c> with a
/// <c>server_id</c> Filter — every OTHER server's rows in the newest chunk read and rejected before the two
/// this server's read wanted turned up.
///
/// <para>This file took over the "I am the top rung" claims that moved off
/// <see cref="CollectionCaveatsRungTests"/> (V141) when this rung landed. When
/// <see cref="QueryStoreIntervalLatestRungTests"/> (V143, #3953) landed, those claims moved on again.
/// What stays is everything true of this rung wherever it sits.</para>
/// </summary>
public sealed class IndexObjectStatsServerTimeIndexRungTests
{
    private const int RungVersion = 142;

    /// <summary>This rung's sentinel ordinal in the viewer probe — V143 (#3953) is now above it, so it
    /// is no longer the last argument.</summary>
    private const int ProbeOrdinal = 117;

    private const string IndexName = "idx_index_object_stats_server_time";

    private static PgMigrations.Migration V142 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("index-object-stats-server-time", V142.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// ONE additive, idempotent index — no table, no column, no GRANT, no view — the same shape as V22's own
    /// index rung on this table.
    /// </summary>
    [Fact]
    public void TheRungCreatesOneIndex_LeadingOnServerIdThenCollectionTimeDescending_AndNothingElse()
    {
        var sql = V142.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains(
            $"CREATE INDEX IF NOT EXISTS {IndexName} ON collect.index_object_stats (server_id, collection_time DESC);",
            sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, "CREATE INDEX"));

        var body = Regex.Replace(sql, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        foreach (var absent in new[] { "GRANT", "CREATE TABLE", "VIEW", "DROP ", "ALTER TABLE", "CONCURRENTLY" })
        {
            Assert.DoesNotContain(absent, body, StringComparison.Ordinal);
        }

        /* Not CONCURRENTLY: TimescaleDB refuses it on a hypertable, so this stays the plain form every other
           index rung on this table (V1, V22) uses, inside the ladder's own transaction. */
        Assert.DoesNotContain("CONCURRENTLY", sql, StringComparison.Ordinal);
    }

    /* ---- the probe (top arm) -------------------------------------------------------------------------- */
}
