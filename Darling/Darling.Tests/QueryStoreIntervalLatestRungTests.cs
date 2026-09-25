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
/// Pins Darling rung V143 (#3953): three new tables, all engine-plain, and nothing on an existing table. The
/// latest Query Store snapshot per interval, its per-server coverage, and the replay record for a batch whose apply
/// failed. The writer, the reads and the retention are pinned where they live
/// (<see cref="QueryStoreIntervalLatestWriterTests"/>, <see cref="PlanRegressionIntervalTableEquivalenceTests"/>);
/// this file is the RUNG: the ladder, the DDL, and the viewer probe.
///
/// <para>This file carries the "I am the top rung" claims that moved off <see cref="CollectionCaveatsRungTests"/>
/// (V141) when this rung landed: a fully-migrated store must map to EXACTLY this version, or the viewer's connect-time
/// gate refuses a store that is actually current. When the next rung lands they move on again, and what stays is
/// everything true of this rung wherever it sits.</para>
/// </summary>
public sealed class QueryStoreIntervalLatestRungTests
{
    private const int RungVersion = 143;
    private const int PreviousVersion = 141;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 117;

    private static PgMigrations.Migration V143 => PgMigrations.Scripts.Single(m => m.Version == RungVersion);

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("query-store-interval-latest", V143.Name);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    /// <summary>
    /// Three idempotent CREATEs and one unique index, engine-plain (no TimescaleDB call: the conversion is runtime
    /// work), and no ALTER of anything that already exists. The index is <c>NULLS NOT DISTINCT</c> in the writer's
    /// column order, the heap is <c>fillfactor = 50</c>, and the conflict target the writer names is exactly the
    /// index's column list.
    /// </summary>
    [Fact]
    public void TheRungCreatesThreeTablesAndOneUniqueIndex_EnginePlain_AndNothingElse()
    {
        var sql = V143.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

        Assert.Equal(3, CountOf(sql, "CREATE TABLE IF NOT EXISTS collect."));
        Assert.Equal(1, CountOf(sql, "CREATE UNIQUE INDEX IF NOT EXISTS ux_query_store_interval_latest"));
        Assert.Equal(4, CountOf(sql, "CREATE "));
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.query_store_interval_latest\n", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.query_store_interval_latest_coverage\n", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.query_store_interval_latest_pending\n", sql, StringComparison.Ordinal);
        Assert.Contains("WITH (fillfactor = 50);", sql, StringComparison.Ordinal);
        Assert.Contains("NULLS NOT DISTINCT;", sql, StringComparison.Ordinal);

        /* The statements, not the comments explaining them. */
        var statements = System.Text.RegularExpressions.Regex.Replace(sql, @"/\*.*?\*/", " ", System.Text.RegularExpressions.RegexOptions.Singleline);
        foreach (var forbidden in new[] { "ALTER ", "DROP ", "create_hypertable", "timescaledb", "INSERT ", "UPDATE " })
        {
            Assert.DoesNotContain(forbidden, statements, StringComparison.OrdinalIgnoreCase);
        }

        var indexColumns = string.Join(", ", sql[(sql.IndexOf("ON collect.query_store_interval_latest\n(", StringComparison.Ordinal) + "ON collect.query_store_interval_latest\n(".Length)..]
            .Split(')')[0]
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries));
        Assert.Equal(QueryStoreIntervalLatest.IdentityColumns, indexColumns);
    }

    /// <summary>
    /// The viewer probe's three sites carry this rung's sentinel, and the map treats it as the TOP arm: a missing top
    /// arm maps a fully-migrated store one rung short, permanently, because <c>RequiredStoreSchemaVersion</c> is
    /// <c>StorageVersion.SchemaVersion</c>. The viewer runs no analysis, so this banner is the rung's only viewer
    /// effect.
    /// </summary>
    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains(
            "table_name = 'query_store_interval_latest_pending'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.Equal(ProbeOrdinal, arity - 1);
        Assert.Equal("hasQueryStoreIntervalLatest", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasQueryStoreIntervalLatest)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasCollectionCaveats)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V143 sentinel arm — a fully-migrated store would map one rung low");
        Assert.True(thisArm < previousArm, "the V143 arm sits below the previous rung's, so a current store maps one rung low");
        Assert.Contains(
            "return " + StorageVersion.SchemaVersion.ToString(CultureInfo.InvariantCulture) + ";",
            viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the tables are named in the probe line and nowhere in the arm's prose. */
        var armProseStart = viewer.LastIndexOf("/* V143 (#3953)", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the V143 arm has no comment block saying why it exists");
        Assert.DoesNotContain("query_store_interval_latest", viewer[armProseStart..thisArm], StringComparison.Ordinal);
    }

    /// <summary>
    /// The coverage claim's first guard (design 6.1): every raw Query Store row enters the store through
    /// <c>DarlingCollectorRunner.CopyBatchOnceAsync</c>, which applies or records it in the same transaction. No
    /// product source spells a write into <c>query_store_stats</c>, and the other generic-COPY call sites (the RDS
    /// ingestors) never touch the Query Store collector. The #1912 slice repair is the one named exception: it
    /// rewrites pre-#1907 split slices, a signature no row written after #1907 can match, so it cannot reach rows
    /// the table covers.
    /// </summary>
    [Fact]
    public void RawQueryStoreRows_EnterTheStoreOnlyThroughTheWriterThatAppliesThem()
    {
        var product = System.IO.Directory
            .EnumerateFiles(RepoFile.PathTo("Darling"), "*.cs", System.IO.SearchOption.AllDirectories)
            .Where(p => !p.Contains("Darling.Tests", StringComparison.Ordinal)
                        && !p.Contains($"{System.IO.Path.DirectorySeparatorChar}obj{System.IO.Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                        && !p.Contains($"{System.IO.Path.DirectorySeparatorChar}bin{System.IO.Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .ToList();
        Assert.NotEmpty(product);

        var literalWrite = new System.Text.RegularExpressions.Regex(
            @"(INSERT\s+INTO|COPY|UPDATE)\s+(collect\.)?query_store_stats\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        foreach (var file in product)
        {
            var text = System.IO.File.ReadAllText(file);
            Assert.False(literalWrite.IsMatch(text), $"{file} writes raw query_store_stats outside the #3953 writer");

            if (text.Contains("PgCollectorRowWriter.CopyCommandFor(", StringComparison.Ordinal)
                && !file.EndsWith("DarlingCollectorRunner.cs", StringComparison.Ordinal))
            {
                Assert.DoesNotContain("QueryStoreCollector", text, StringComparison.Ordinal);
            }
        }

        var repair = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "QueryStoreSliceRepair.cs");
        Assert.Contains("INSERT INTO collect.{Table}", repair, StringComparison.Ordinal);
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
