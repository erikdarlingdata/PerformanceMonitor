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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling rung V141 (#3691, part a1): ONE new table, <c>collect.analysis_collection_caveats</c>, the
/// store side of <see cref="CollectionCaveatStore"/> — the current set of (server, family) pairs an analysis
/// pass could not read. No GRANT (the <c>collect</c> schema's blanket grant already covers it), no hypertable
/// (<see cref="TimescaleSupport.HypertableCount"/> stays unchanged), not in <see cref="CollectorCatalog"/> (it
/// is written by the analysis pass's own best-effort writer, not a collector definition).
///
/// <para>This file takes over the "I am the top rung" claims that moved off
/// <see cref="CheckpointsTimedRungTests"/> (V140) when this rung landed.</para>
/// </summary>
public sealed class CollectionCaveatsRungTests
{
    private const int RungVersion = 141;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the last argument, because V141 is
    /// (for now) the newest rung.</summary>
    private const int ProbeOrdinal = 116;

    private const string TableName = "analysis_collection_caveats";

    private static PgMigrations.Migration V141 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    /* ---- the rung ------------------------------------------------------------------------------------ */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("collection-caveats", V141.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);
        Assert.Same(V141, PgMigrations.Scripts[^1]);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// ONE new table, with a composite primary key on (server_id, family) and no surrogate — the identity
    /// <see cref="CollectionCaveatStore.ApplyPassAsync"/>'s upsert relies on — and nothing else: no GRANT
    /// statement (the collect schema's blanket grant covers it), no index beyond the PK, no view.
    /// </summary>
    [Fact]
    public void TheRungCreatesOneTable_WithACompositePrimaryKey_AndNothingElse()
    {
        var sql = V141.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.analysis_collection_caveats", sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, "CREATE TABLE"));
        Assert.Contains("CONSTRAINT pk_analysis_collection_caveats PRIMARY KEY (server_id, family)", sql, StringComparison.Ordinal);

        var body = Regex.Replace(sql, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        foreach (var absent in new[] { "GRANT", "CREATE INDEX", "VIEW", "DROP ", "ALTER TABLE" })
        {
            Assert.DoesNotContain(absent, body, StringComparison.Ordinal);
        }

        foreach (var column in new[] { "server_id integer NOT NULL", "family text NOT NULL", "reason text NOT NULL", "first_seen_utc timestamp NOT NULL", "last_seen_utc timestamp NOT NULL" })
        {
            Assert.Contains(column, sql, StringComparison.Ordinal);
        }

        Assert.DoesNotContain(CollectorCatalog.All, c => c.TargetTable == TableName);
        Assert.Equal(CollectionCaveatStore.TableName, "collect." + TableName);
    }

    /* ---- the probe (top arm) -------------------------------------------------------------------------- */

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel at its own ordinal, and the map's arm for it
    /// returns 141 — the top arm, since nothing has landed on top of it yet.
    /// </summary>
    [Fact]
    public void TheProbeMapsAStoreStoppedHereToThisRung()
    {
        Assert.Contains(
            $"table_name = 'analysis_collection_caveats')",
            ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasCollectionCaveats", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* This rung IS the newest sentinel for now, so its ordinal is the last parameter. */
        Assert.Equal(ProbeOrdinal, arity - 1);
        Assert.Equal("hasCollectionCaveats", method.GetParameters()[ProbeOrdinal].Name);

        var atThisRung = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(140, (int)method.Invoke(null, behind)!);

        /* In the source, this rung's arm sits ABOVE V140's and returns this build's version. */
        var thisArm = viewer.IndexOf("if (hasCollectionCaveats)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasCheckpointsTimed)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V141 sentinel arm — a fully-migrated store would map one rung low");
        Assert.True(previousArm >= 0, "the previous rung's arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "the V141 arm sits below V140's, so a current store maps one rung low");
        Assert.Contains(
            "return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* Unlike the earlier COLUMN-sentinel rungs (whose prose must avoid naming their column per the V71
           finding), this rung's own comment block does name its table — it is the TABLE the sentinel checks
           for existence, and the migration's own prose already names it in full, so there is no separate
           finding to guard here. What this pins is only that the comment block exists at all, immediately
           above the arm. */
        var armProseStart = viewer.LastIndexOf("/* V141 (#3691", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the V141 arm has no comment block saying why it exists");
    }
}
