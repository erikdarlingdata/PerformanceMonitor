/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the V58 Query Store backfill off switch (#2167): the migration's identity, the probe/gate rung,
/// and the config plumbing from store row to the worker's live-read seam. The switch exists because the
/// #2058 backfill previously ran unconditionally — a freshly restored catalog on a cross-region server
/// had no stop short of gutting plan capture fleet-wide.
/// </summary>
public sealed class BackfillSwitchTests
{
    [Fact]
    public void V58_MigrationIdentity_AndColumnDefaultOn()
    {
        var v58 = PgMigrations.Scripts.Single(m => m.Version == 58);

        Assert.Equal("qs-backfill-switch", v58.Name);
        /* Idempotent, on config_service, and DEFAULT TRUE — an upgraded store keeps backfilling until an
           operator turns it off; the switch must never flip as a side effect of the upgrade itself. */
        Assert.Contains(
            "ALTER TABLE config.config_service\r\n    ADD COLUMN IF NOT EXISTS query_store_backfill_enabled boolean NOT NULL DEFAULT TRUE;"
                .Replace("\r\n", "\n", StringComparison.Ordinal),
            v58.Sql.Replace("\r\n", "\n", StringComparison.Ordinal),
            StringComparison.Ordinal);
    }

    [Fact]
    public void ProbeAndGate_KnowTheV58Rung()
    {
        /* The viewer NAMES the column in ServiceConfigSelectSql/UpdateFlagsSql, so a V57 store must be
           refused (42703 otherwise) and a fully-migrated V58 store must map to exactly the required
           version — the connect-time-gate trap the V53/V56/V57 pins guard. */
        Assert.Contains("column_name = 'query_store_backfill_enabled'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);
        Assert.Contains("query_store_backfill_enabled", ViewerDataService.ServiceConfigSelectSql, StringComparison.Ordinal);
        Assert.Contains("query_store_backfill_enabled = $6", ViewerDataService.ServiceConfigUpdateFlagsSql, StringComparison.Ordinal);

        /* A store carrying the switch but NOT the later V59 knobs is exactly V58 — stating the newer flag
           false is what makes this rung's pin survive the next migration instead of silently becoming a
           test of the newest rung. */
        Assert.Equal(58, ViewerDataService.MapProbedSchemaVersion(
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, hasJobMetricsColumns: true, hasJobCadenceKnob: true,
            hasBackfillSwitch: true, hasCollectorMemoryKnobs: false));
        Assert.Equal(57, ViewerDataService.MapProbedSchemaVersion(
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            true, true, true, true, true, true, hasJobMetricsColumns: true, hasJobCadenceKnob: true,
            hasBackfillSwitch: false, hasCollectorMemoryKnobs: false));
    }

    [Fact]
    public void ApplyToConfig_CarriesTheSwitch_AndDefaultsStayOn()
    {
        /* Defaults: a fresh config and an unread view both leave backfill ON (the SKU default). */
        var config = new DarlingConfig();
        Assert.True(config.QueryStoreBackfillEnabled);
        Assert.True(new StoreConfigView().QueryStoreBackfillEnabled);

        /* The store flip reaches the held config by reference — the worker's live Func<bool> seam reads
           this field, so this IS the path a Settings-window toggle takes to the running loop. */
        StoreConfigProvider.ApplyToConfig(config, new StoreConfigView { QueryStoreBackfillEnabled = false });
        Assert.False(config.QueryStoreBackfillEnabled);

        StoreConfigProvider.ApplyToConfig(config, new StoreConfigView { QueryStoreBackfillEnabled = true });
        Assert.True(config.QueryStoreBackfillEnabled);
    }
}
