/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Darling service.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V46 (#1952) creates <c>collect.plan_correction</c> by hand-written literal, following the V29/V34
/// idiom so the column list stays auditable in the migration itself. The cost of that idiom is that
/// nothing structurally prevents the literal from drifting from what a FRESH store gets — V1 walks
/// the collector catalog through <see cref="PgSchemaGenerator.GenerateFullSchema"/>, so a fresh store
/// and an upgraded one would silently end up with different tables and every positional write after
/// that would land in the wrong column. V29 and V34 assert that equivalence in a doc comment; this
/// asserts it in code.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class DarlingPlanCorrectionMigrationTests
{
    private static string V46Sql =>
        PgMigrations.Scripts.Single(m => m.Version == 46).Sql;

    [Fact]
    public void V46_IsTheNewestMigration_AndTheBuildsSchemaVersion()
    {
        var migration = PgMigrations.Scripts.Single(m => m.Version == 46);

        Assert.Equal("plan-correction-collector", migration.Name);
        /* 46 is no longer the tail — #1951 landed 47 on top of it. What this test owns is that V46
           itself is intact and registered; the newest-migration pin lives in PvsStatsStoreTests. */
        Assert.Equal(46, migration.Version);
    }

    [Fact]
    public void MigrationVersions_AreUniqueAndAscending_EvenAcrossTheDeliberateV45Gap()
    {
        /* 45 belongs to a sibling lane and is deliberately absent here. The runner applies whatever sits
           above the store's current version, so a GAP is harmless; a COLLISION would mean two different
           bodies claiming one version and a store that silently skips one of them. Pin the property that
           actually matters rather than contiguity, which was never required. */
        var versions = PgMigrations.Scripts.Select(m => m.Version).ToArray();

        Assert.Equal(versions.Distinct().Count(), versions.Length);
        Assert.Equal(versions.OrderBy(v => v).ToArray(), versions);
        Assert.DoesNotContain(45, versions);

        /* 45 is permanently unused: #1951 was renumbered to 47 once #1952 took 46, because the runner
           only applies versions ABOVE the store's MAX and a store stamped 46 would never have seen it. */
        Assert.Contains(47, versions);
    }

    [Fact]
    public void V46_CreatesExactlyWhatAFreshStoreGenerates()
    {
        /* PgSchemaGenerator emits bare table names; the migration schema-qualifies them into collect.
           Normalizing that one difference away is the whole adaptation — everything else, including
           column order and every type, must match character for character. */
        var generated = PgSchemaGenerator.CreateTable(PlanCorrectionCollector.Instance)
            .Replace("EXISTS plan_correction", "EXISTS collect.plan_correction", StringComparison.Ordinal);

        Assert.Contains(generated, Normalize(V46Sql), StringComparison.Ordinal);

        var index = PgSchemaGenerator.CreateIndex(PlanCorrectionCollector.Instance)!
            .Replace(" ON plan_correction(", " ON collect.plan_correction(", StringComparison.Ordinal);

        Assert.Contains(index, Normalize(V46Sql), StringComparison.Ordinal);
    }

    [Fact]
    public void V46_IsAdditive_AndCarriesNoPassthroughView()
    {
        /* IF NOT EXISTS is what makes this a no-op on a fresh store and the real create on an upgrade.
           No v_* view: plan_correction is a post-V14 collector, and those are read from the base table. */
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.plan_correction", V46Sql, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_plan_correction_time", V46Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE OR REPLACE VIEW", V46Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_plan_correction", V46Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanCorrection_IsInTheCatalog_SoHypertableAndRetentionFlowFromIt()
    {
        /* Hypertable conversion, compression and retention are all catalog-derived at runtime — a
           collector missing from the catalog would get a plain table that nothing ever purges. */
        Assert.Contains(CollectorCatalog.All, c => c.TargetTable == "plan_correction");
        Assert.True(CollectorScheduleDefaults.All.ContainsKey("plan_correction"));
    }

    /// <summary>Line endings only — the migration literal is a verbatim string in a CRLF source file.</summary>
    private static string Normalize(string sql) =>
        sql.Replace("\r\n", "\n", StringComparison.Ordinal);
}
