/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6098 (V200): the fleet-wide excluded-volumes list round-trips through the store on every surface that reads or writes it,
/// in the way the <c>excluded_databases</c> list does: seeded from darling.json (normalised), read back by the service and the
/// Viewer, written by the Viewer's Settings save and by <c>update_alert_settings</c>, and a re-run of the rung keeps the list.
/// </summary>
public sealed class ExcludedVolumesStoreTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void TheRung_IsNamedForTheColumn_AndTheLadderIsOrdered()
    {
        var rung = PgMigrations.Scripts.Single(m => m.Version == 200);

        Assert.Equal("excluded-volumes", rung.Name);
        Assert.True(StorageVersion.SchemaVersion >= 200); /* not the top any more: V202 (#6159) sits above this rung */
        Assert.Equal(PgMigrations.Scripts.Select(s => s.Version).OrderBy(v => v), PgMigrations.Scripts.Select(s => s.Version));
    }

    [Fact]
    public async Task OnALiveStore_TheListRoundTripsThroughEverySurface()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V200 round trip (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var provider = new StoreConfigProvider(dataSource);
            var config = new DarlingConfig();
            config.Alerts.ExcludedVolumes.AddRange(new[] { " S:\\ ", "s:", "", "/mnt/scratch/" });
            await provider.SeedIfEmptyAsync(config, ct);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            /* The seed holds the normalised list, and the service reads it back into the held config by the settings seam. */
            var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            Assert.Equal(new[] { "S:\\", "/mnt/scratch/" }, view.Alerts.ExcludedVolumes);
            var held = new DarlingConfig();
            StoreConfigProvider.ApplyToConfig(held, view);
            Assert.Equal(new[] { "S:\\", "/mnt/scratch/" }, new DarlingAlertSettings(held).ExcludedVolumes);

            /* The Viewer reads it, and its Settings save writes a new list (normalised) that the service reads. */
            var row = await viewer.GetAlertSettingsAsync(ct);
            Assert.NotNull(row);
            Assert.Equal(new[] { "S:\\", "/mnt/scratch/" }, row.ExcludedVolumes);
            row.ExcludedVolumes = new List<string> { "T:", "t:\\", "/data/" };
            await viewer.UpsertAlertSettingsAsync(row, ct);
            view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            Assert.Equal(new[] { "T:", "/data/" }, view.Alerts.ExcludedVolumes);

            /* update_alert_settings replaces the list in the canonical form; an empty array clears it and is honoured. */
            var updated = await DarlingMcpAlertTools.UpdateAlertSettings(dataSource, "{\"excluded_volumes\":[\" D:\\\\ \",\"d:\",\"\",\"E:\\\\\"]}");
            Assert.False(McpHelpers.IsErrorEnvelope(updated), updated);
            view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            Assert.Equal(new[] { "D:\\", "E:\\" }, view.Alerts.ExcludedVolumes);
            var reported = await DarlingMcpAlertTools.GetAlertSettings(dataSource);
            Assert.Contains("\"excluded_volumes\"", reported, StringComparison.Ordinal);
            Assert.Contains("E:", reported, StringComparison.Ordinal);

            updated = await DarlingMcpAlertTools.UpdateAlertSettings(dataSource, "{\"excluded_volumes\":[]}");
            Assert.False(McpHelpers.IsErrorEnvelope(updated), updated);
            view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            Assert.Empty(view.Alerts.ExcludedVolumes);

            /* A re-run of the rung on a store that already has the column keeps the row. */
            await using (var again = new NpgsqlConnection(scratch.ConnectionString))
            {
                await again.OpenAsync(ct);
                await using (var delete = new NpgsqlCommand("DELETE FROM darling_schema_version WHERE version >= 200", again))
                {
                    await delete.ExecuteNonQueryAsync(ct);
                }

                await PgMigrations.MigrateAsync(again, ct);
            }

            Assert.Empty((await viewer.GetAlertSettingsAsync(ct))!.ExcludedVolumes);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
