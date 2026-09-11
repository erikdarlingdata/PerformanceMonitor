/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V114 (#3234): the statistics-estimate columns on <c>collect.pg_index_bloat</c>, and the retirement of
/// the rotation cursor the <c>pgstatindex</c> census needed.
///
/// <para>The TOP-RUNG guard has moved on to <c>PgCpuCapacityHeadroomTests</c> (V115), as this suite's own
/// note said it must: it has to live with whichever rung is actually top, because its whole point is to
/// catch a later rung that appends a sentinel without adding its own arm — which would leave the viewer
/// refusing a store that is perfectly current, permanently, because no further upgrade changes the
/// answer.</para>
/// </summary>
public class PgIndexBloatEstimateRungTests
{
    internal const int RungVersion = 114;

    /// <summary>The version a store one rung behind this one reports.</summary>
    private const int PreviousVersion = 113;

    /* The top-rung guard moved to PgCpuCapacityHeadroomTests when V115 dethroned this rung — see that
       suite's own note. What stays here is this rung's OWN arm and its ordering fact. */

    /// <summary>This rung's sentinel ordinal in the viewer probe. Its OWN ordinal, which never moves.</summary>
    internal const int ProbeOrdinal = 89;

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "pg-index-bloat-estimate-columns",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());

        /* No longer the head of the ladder — V115 landed above it. The equality that used to sit here
           asserted this rung was top, which is the clause that has to move with the head. */
        Assert.True(
            versions.Max() > RungVersion,
            "a rung above this one must exist; if this is the head again then the top-rung guard has been "
            + "lost rather than moved.");

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// The rung ADDS and never drops. The seven <c>pgstatindex</c> measurement columns stay because the
    /// store holds 90 days of exact measurements taken before #3234 and an on-request measurement needs
    /// somewhere to land — so a NULL there means the row was ESTIMATED, not that a walk found nothing.
    ///
    /// <para>All eight additions are asserted individually rather than by counting them: a count passes
    /// while naming the wrong column, and the write side indexes these positionally.</para>
    /// </summary>
    [Fact]
    public void TheRungAddsEveryEstimateColumn_AndDropsNothing()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        foreach (var column in new[]
                 {
                     "index_pages bigint",
                     "table_rows bigint",
                     "fillfactor integer",
                     "est_tuple_bytes bigint",
                     "est_leaf_pages bigint",
                     "est_bloat_pct double precision",
                     "est_reclaimable_bytes bigint",
                     "pgstattuple_available boolean",
                 })
        {
            Assert.Contains("ADD COLUMN IF NOT EXISTS " + column, sql, StringComparison.Ordinal);
        }

        /* Schema-qualified for the reason every rung here is: the migrate session's search_path puts
           collect first, so a bare name resolves wherever that points rather than where the rung meant. */
        Assert.Contains("ALTER TABLE collect.pg_index_bloat", sql, StringComparison.Ordinal);

        /* No DROP of any kind. The history is the reason, and a rung that dropped one of the measurement
           columns would destroy it silently — the read tolerates their absence by writing NULL. */
        Assert.DoesNotContain("DROP COLUMN", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("DROP TABLE", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The cursor cleanup, scoped on BOTH the collector name and the key prefix. Either alone is wrong:
    /// the prefix is generic enough that another collector could adopt it, and <c>collector_name</c> on
    /// its own would delete a future key belonging to this one. These rows need retiring here because
    /// <c>pg_index_bloat</c> now declares no <c>StateKeys</c>, so the per-database prune no longer owns
    /// the prefix and would never visit them again.
    /// </summary>
    [Fact]
    public void TheRungRetiresTheRotationCursor_ScopedOnBothNameAndPrefix()
    {
        var sql = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        Assert.Contains("DELETE FROM collect.collector_state", sql, StringComparison.Ordinal);
        Assert.Contains("collector_name = 'pg_index_bloat'", sql, StringComparison.Ordinal);
        Assert.Contains("state_key LIKE 'rotate:%'", sql, StringComparison.Ordinal);

        /* And the collector really has stopped declaring the prefix, which is what makes the cleanup
           necessary rather than merely tidy. */
        Assert.Empty(PerformanceMonitor.Collectors.PgIndexBloatCollector.Instance.StateKeys);
        Assert.Empty(PerformanceMonitor.Collectors.PgPerDatabaseCollectorState.PrunableKeys);
    }

    /// <summary>
    /// The connect-time gate. A COLUMN sentinel, because <c>collect.pg_index_bloat</c> has existed since
    /// V94 and table existence cannot separate the rungs — the same reasoning V95 records.
    /// <c>est_reclaimable_bytes</c> is the one chosen because it is what the read ranks on: if it is
    /// absent the panel has nothing to order by, which is the failure this probe exists to prevent.
    ///
    /// <para><b>The top-rung guard lives with V115 now</b> (#3281). What is asserted here is this rung's
    /// own arm, reached by switching off every sentinel above it — and the argument list is still built by
    /// reflection so the arity tracks the signature, because the literal-true form silently defaults a
    /// newly added sentinel to false and maps one version low.</para>
    /// </summary>
    [Fact]
    public void TheProbeAsksForTheEstimateColumn_AndThisRungsOwnArmAnswers()
    {
        Assert.Contains(
            "table_name = 'pg_index_bloat'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);
        Assert.Contains(
            "column_name = 'est_reclaimable_bytes'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = ParitySourceLocal.ReadFile(
            "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasPgIndexBloatEstimate", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* No longer the last sentinel — V115 appended one. This is the form the previous comment here
           predicted: the equality moved to the new top rung and what remains is the ordering fact. */
        Assert.True(
            ProbeOrdinal < arity - 1,
            "a rung above this one must own a later sentinel; if this is the last one again then the "
            + "top-rung guard has been lost rather than moved.");

        /* This rung's OWN arm, isolated by switching off every sentinel above it. The all-true case
           belongs to whichever rung is top and is asserted there; testing it here would assert that V114
           is still the head of the ladder. */
        var throughMine = Enumerable.Repeat((object)true, arity).ToArray();

        for (var i = ProbeOrdinal + 1; i < arity; i++)
        {
            throughMine[i] = false;
        }

        Assert.Equal(RungVersion, (int)method.Invoke(null, throughMine)!);

        /* One rung behind: the same store minus this rung's sentinel must report 113. Without this the arm
           above could be satisfied by an unconditional return and nothing would notice. */
        var belowMine = (object[])throughMine.Clone();
        belowMine[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, belowMine)!);
    }
}
