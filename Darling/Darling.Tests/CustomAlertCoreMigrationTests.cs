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
/// V116 / #3285: the custom-alert core rung (config.custom_alert_rules + config.custom_alert_state). The
/// "I am the top rung" claims have moved on to <see cref="BuiltinAlertPersistenceRungTests"/> (V117), the
/// way this rung took them from <see cref="PgCpuCapacityHeadroomTests"/> (V115). What stays here is this
/// rung's own identity and its probe arm, which must keep mapping a store migrated to EXACTLY 116 to 116
/// rather than letting it fall through.
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
        Assert.True(RungVersion <= StorageVersion.SchemaVersion);

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
    public void TheProbeMapsAStoreAtExactlyThisRungToThisRung()
    {
        Assert.Contains(
            "table_name = 'custom_alert_rules'",
            ViewerDataService.StoreSchemaProbeSql, System.StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, System.StringComparison.Ordinal);
        Assert.Contains("hasCustomAlertCore", viewer, System.StringComparison.Ordinal);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        /* Every sentinel from this one DOWN is present and everything NEWER is absent — a store migrated
           to exactly this rung, which must map to exactly this rung rather than falling through. Built by
           reflection so the arity tracks the signature as later rungs land. */
        var atThisRung = Enumerable.Repeat((object)true, arity).ToArray();
        for (var i = ProbeOrdinal + 1; i < arity; i++)
        {
            atThisRung[i] = false;
        }

        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);

        /* One rung behind: this sentinel absent too reports 115 (the previous rung). */
        atThisRung[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, atThisRung)!);
    }
}
