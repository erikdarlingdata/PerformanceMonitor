/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Reflection;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V116 / #3285: the custom-alert core rung (config.custom_alert_rules + config.custom_alert_state). This
/// carries the "I am the top rung" claims that moved off <see cref="PgCpuCapacityHeadroomTests"/> (V115) when
/// this rung landed — a fully-migrated store must map to EXACTLY this version, or the viewer refuses a store
/// that is actually current.
/// </summary>
public sealed class CustomAlertCoreMigrationTests
{
    private const int RungVersion = 116;
    private const int PreviousVersion = 115;

    /// <summary>This rung's sentinel ordinal in the viewer probe — the newest, so the last argument.</summary>
    private const int ProbeOrdinal = 91;

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "custom-alert-core",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    [Fact]
    public void TheRungCreatesBothConfigTables_WithNoBumpTrigger()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;

        /* Schema-qualified for the reason every config rung is: the migrate session's search_path puts
           collect first, so a bare name would resolve to the wrong schema (and the wrong ACL). */
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.custom_alert_rules", rung, System.StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.custom_alert_state", rung, System.StringComparison.Ordinal);

        /* The state row cascades when its rule is deleted (teardown). */
        Assert.Contains("ON DELETE CASCADE", rung, System.StringComparison.Ordinal);

        /* Deliberately NO reload beacon, like custom_views: the evaluator reads rules itself each sweep, so a
           bump would only force a needless fleet-wide ReloadFromStoreAsync on every threshold edit. */
        Assert.DoesNotContain("config_bump_version", rung, System.StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains(
            "table_name = 'custom_alert_rules'",
            ViewerDataService.StoreSchemaProbeSql, System.StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, System.StringComparison.Ordinal);
        Assert.Contains("hasCustomAlertCore", viewer, System.StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* The top rung's sentinel IS the last argument. */
        Assert.Equal(ProbeOrdinal, arity - 1);

        /* Every sentinel true = a fully-migrated store, which must map to exactly this version. Built by
           reflection so the arity tracks the signature. */
        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        /* One rung behind: every sentinel EXCEPT this one reports 115 (the previous top rung). */
        var behind = Enumerable.Repeat((object)true, arity).ToArray();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);
    }
}
