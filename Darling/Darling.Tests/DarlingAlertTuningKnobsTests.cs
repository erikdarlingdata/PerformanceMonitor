/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the V20 alert-tuning knobs the service now honors through the store instead of hardcoding them: the
/// long-running-query read shape (row cap + the five noise-filter opt-outs the shared <c>AlertEngine</c>
/// forwards to <c>GetLongRunningQueriesAsync</c>) and the connection-change notify gate. The pure tests need no
/// Postgres — <see cref="DarlingAlertSettings"/> exposes them through the by-reference config seam, so a store
/// reload (<see cref="StoreConfigProvider.ApplyToConfig"/>) is reflected on the same adapter instance — exactly
/// the V18 delivery-mode pattern. The live test (gated on DARLING_TEST_PG) round-trips the new columns through a
/// real seed/read against an ISOLATED scratch store.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It reaches DARLING_TEST_PG only to CREATE and
   DROP its own database through ScratchPostgres, then works entirely inside it — it never touches the shared
   database's tables, so it cannot race the live collection and serializing it would be pure slowdown. Leave it
   out; this comment is here so the next sweep does not "fix" it. */
public sealed class DarlingAlertTuningKnobsTests
{
    /* ---------------- #2107: the previously-hardcoded thresholds through the settings seam ---------------- */

    [Fact]
    public void SelfAlertKnobs_DefaultsAreTheConstantsTheyReplaced_AndReadsClampLikeSiblings()
    {
        var config = new DarlingConfig();
        var settings = new DarlingAlertSettings(config);

        /* Defaults mirror the V55 DDL — the compile-time constants these knobs replaced. */
        Assert.Equal(10, settings.SelfDiskFreeWarnPercent);
        Assert.Equal(30, settings.CollectionStaleMinutes);
        Assert.Equal(10, settings.CollectionFailureThreshold);
        Assert.Equal(3, settings.DiskCriticalFreePercent);
        Assert.Equal(2, settings.DiskCriticalFreeGb);
        Assert.Equal(360, settings.AnalysisNotifyCooldownMinutes);

        /* Live reload through the by-reference seam, clamped on read — a hand-edited store value
           can't drive a nonsense threshold: a 0-minute staleness window would fire every sweep, a
           0 failure threshold on the fast path would fire on any single failure, and the analysis
           cooldown keeps the shared engine's documented [30, 10080]. */
        config.Alerts.SelfDiskFreeWarnPercent = 150;
        config.Alerts.CollectionStaleMinutes = 0;
        config.Alerts.CollectionFailureThreshold = 0;
        config.Alerts.DiskCriticalFreePercent = -5;
        config.Alerts.DiskCriticalFreeGb = -1;
        config.Alerts.AnalysisNotifyCooldownMinutes = 99999;

        Assert.Equal(100, settings.SelfDiskFreeWarnPercent);
        Assert.Equal(5, settings.CollectionStaleMinutes);
        Assert.Equal(1, settings.CollectionFailureThreshold);
        Assert.Equal(0, settings.DiskCriticalFreePercent);
        Assert.Equal(0, settings.DiskCriticalFreeGb);
        Assert.Equal(10080, settings.AnalysisNotifyCooldownMinutes);
    }

    /* ---------------- pure: the long-running-query read shape through the settings seam ---------------- */

    [Fact]
    public void DarlingAlertSettings_ReadsLongRunningQueryReadShape_ThroughTheByReferenceSeam()
    {
        var config = new DarlingConfig();
        var settings = new DarlingAlertSettings(config);

        /* Defaults mirror the V20 DDL (and Lite's App.*): 5 rows, every noise filter on. */
        Assert.Equal(5, settings.LongRunningQueryMaxResults);
        Assert.True(settings.LongRunningQueryExcludeSpServerDiagnostics);
        Assert.True(settings.LongRunningQueryExcludeWaitFor);
        Assert.True(settings.LongRunningQueryExcludeBackups);
        Assert.True(settings.LongRunningQueryExcludeMiscWaits);
        Assert.True(settings.LongRunningQueryExcludeCdc);

        /* A store reload mutates the held config in place; the SAME adapter instance reflects it (no caching). */
        config.Alerts.LongRunningQueryMaxResults = 25;
        config.Alerts.LongRunningQueryExcludeSpServerDiagnostics = false;
        config.Alerts.LongRunningQueryExcludeWaitFor = false;
        config.Alerts.LongRunningQueryExcludeBackups = false;
        config.Alerts.LongRunningQueryExcludeMiscWaits = false;
        config.Alerts.LongRunningQueryExcludeCdc = false;

        Assert.Equal(25, settings.LongRunningQueryMaxResults);
        Assert.False(settings.LongRunningQueryExcludeSpServerDiagnostics);
        Assert.False(settings.LongRunningQueryExcludeWaitFor);
        Assert.False(settings.LongRunningQueryExcludeBackups);
        Assert.False(settings.LongRunningQueryExcludeMiscWaits);
        Assert.False(settings.LongRunningQueryExcludeCdc);
    }

    [Fact]
    public void ApplyToConfig_SwapsLongRunningQueryReadShape_ReflectedThroughTheSettingsSeam()
    {
        var config = new DarlingConfig();
        var settings = new DarlingAlertSettings(config);

        StoreConfigProvider.ApplyToConfig(config, new StoreConfigView
        {
            Alerts = new AlertsConfig
            {
                LongRunningQueryMaxResults = 42,
                LongRunningQueryExcludeSpServerDiagnostics = false,
                LongRunningQueryExcludeCdc = false,
            },
        });

        Assert.Equal(42, settings.LongRunningQueryMaxResults);
        Assert.False(settings.LongRunningQueryExcludeSpServerDiagnostics);
        Assert.False(settings.LongRunningQueryExcludeCdc);
        /* The unset filters keep the AlertsConfig defaults (true). */
        Assert.True(settings.LongRunningQueryExcludeWaitFor);
        Assert.True(settings.LongRunningQueryExcludeBackups);
        Assert.True(settings.LongRunningQueryExcludeMiscWaits);
    }

    /* ---------------- pure: the connection-change notify toggle through the settings seam ---------------- */

    [Fact]
    public void DarlingAlertSettings_ReadsNotifyConnectionChanges_ThroughTheByReferenceSeam()
    {
        var config = new DarlingConfig();
        var settings = new DarlingAlertSettings(config);

        /* Default mirrors the V20 DDL (and Lite's App.NotifyConnectionChanges): on. */
        Assert.True(settings.NotifyConnectionChanges);

        /* A store reload mutating the held config is reflected on the same adapter instance. */
        config.Alerts.NotifyConnectionChanges = false;
        Assert.False(settings.NotifyConnectionChanges);
    }

    [Fact]
    public void ApplyToConfig_SwapsNotifyConnectionChanges_ReflectedThroughTheSettingsSeam()
    {
        var config = new DarlingConfig();
        var settings = new DarlingAlertSettings(config);

        StoreConfigProvider.ApplyToConfig(config, new StoreConfigView
        {
            Alerts = new AlertsConfig { NotifyConnectionChanges = false },
        });

        Assert.False(settings.NotifyConnectionChanges);
    }

    /* ---------------- live (DARLING_TEST_PG): seed -> read round-trip of the V20 columns ---------------- */

    [Fact]
    public async Task SeedAndRead_RoundTripsLongRunningQueryReadShape_AgainstScratchPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the seed/read round-trip (the test mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        /* SeedIfEmptyAsync no-ops on a store any earlier test already seeded, so this test needs
           a database of its own — see ScratchPostgres. */
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct); // brings the scratch store to V20
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var provider = new StoreConfigProvider(dataSource);

        var config = new DarlingConfig();
        config.Alerts.LongRunningQueryMaxResults = 17;
        config.Alerts.LongRunningQueryExcludeSpServerDiagnostics = false;
        config.Alerts.LongRunningQueryExcludeWaitFor = false;
        config.Alerts.LongRunningQueryExcludeBackups = true;
        config.Alerts.LongRunningQueryExcludeMiscWaits = false;
        config.Alerts.LongRunningQueryExcludeCdc = true;
        config.Alerts.NotifyConnectionChanges = false;
        config.Servers.Add(new MonitoredServer { Name = "v20-lrq", Host = "v20-scratch-host", Auth = "integrated" });

        await provider.SeedIfEmptyAsync(config, ct);

        var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
        Assert.NotNull(view);

        Assert.Equal(17, view!.Alerts.LongRunningQueryMaxResults);
        Assert.False(view.Alerts.LongRunningQueryExcludeSpServerDiagnostics);
        Assert.False(view.Alerts.LongRunningQueryExcludeWaitFor);
        Assert.True(view.Alerts.LongRunningQueryExcludeBackups);
        Assert.False(view.Alerts.LongRunningQueryExcludeMiscWaits);
        Assert.True(view.Alerts.LongRunningQueryExcludeCdc);
        Assert.False(view.Alerts.NotifyConnectionChanges);
    }
}
