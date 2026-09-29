/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #4789 against a real store: the viewer's migration and its import from another viewer's registry refuse a
/// server whose id a DIFFERENT server already holds, count it apart, and leave the holder's row exactly as it was.
/// The same server again stays a silent skip. The pair is synthetic: two hosts whose storage names hash to one
/// <c>server_id</c> (the add tests pin that they do).
/// </summary>
public sealed class ViewerImportServerIdCollisionLiveTests
{
    private const string HolderHost = "sql9jocsv";
    private const string CollidingHost = "sqlsvvqew";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static MonitoredServerRow Holder() => new()
    {
        ServerId = ViewerDataService.ComputeServerId(HolderHost, null, false),
        Name = "Holder",
        Host = HolderHost,
    };

    private static ViewerServerEntry Entry(string host, string displayName) => new()
    {
        ServerName = host,
        DisplayName = displayName,
        AuthenticationType = AuthenticationTypes.Windows,
    };

    [Fact]
    public async Task ImportFromStore_RefusesADifferentServerAtTheId_CountsItApart_AndLeavesTheHolderAlone()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The import reads the viewer's Windows-only stores.");
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live import collision pin.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var directory = Directory.CreateTempSubdirectory("darling-import-collision-").FullName;
        var bodySucceeded = false;
        try
        {
            await MigrateStoreAsync(scratch, ct);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            var holder = Holder();
            Assert.Equal(MonitoredServerAddOutcome.Added, (await viewer.AddMonitoredServerAsync(holder, ct)).Outcome);

            /* The registry to import: the colliding server, the holder again under another name, and a free one. */
            var (source, profiles) = NewRegistry(directory, "source");
            source.AddServer(Entry(CollidingHost, "Colliding"), null, null);
            source.AddServer(Entry(HolderHost, "Holder again"), null, null);
            source.AddServer(Entry("other01", "Other"), null, null);

            var result = await ViewerServerMigration.ImportFromStoreAsync(source, profiles, viewer, ct);

            /* Only the free server landed; the colliding one is counted apart, the holder again is a silent skip. */
            Assert.Equal(new ViewerServerImportResult(Imported: 1, Collided: 1), result);

            var rows = await viewer.GetMonitoredServersAsync(ct);
            Assert.Equal(new[] { "Holder", "Other" }, rows.Select(r => r.Name).OrderBy(n => n, StringComparer.Ordinal));
            var atTheId = Assert.Single(rows, r => r.ServerId == holder.ServerId);
            Assert.Equal("Holder", atTheId.Name);
            Assert.Equal(HolderHost, atTheId.Host);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
            try { Directory.Delete(directory, recursive: true); } catch { /* best-effort */ }
        }
    }

    [Fact]
    public async Task ImportFromStore_OfAPairInOneRegistry_KeepsTheFirst_AndRefusesTheSecond()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The import reads the viewer's Windows-only stores.");
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live import collision pin.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var directory = Directory.CreateTempSubdirectory("darling-import-collision-").FullName;
        var bodySucceeded = false;
        try
        {
            await MigrateStoreAsync(scratch, ct);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            /* The registry lists its servers by display name, so "1 ..." is written first. */
            var (source, profiles) = NewRegistry(directory, "source");
            source.AddServer(Entry(HolderHost, "1 first"), null, null);
            source.AddServer(Entry(CollidingHost, "2 second"), null, null);

            var result = await ViewerServerMigration.ImportFromStoreAsync(source, profiles, viewer, ct);

            Assert.Equal(new ViewerServerImportResult(Imported: 1, Collided: 1), result);
            var only = Assert.Single(await viewer.GetMonitoredServersAsync(ct));
            Assert.Equal("1 first", only.Name);
            Assert.Equal(HolderHost, only.Host);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
            try { Directory.Delete(directory, recursive: true); } catch { /* best-effort */ }
        }
    }

    [Fact]
    public async Task Migrate_RefusesADifferentServerAtTheId_ImportsTheRest_AndStillWritesTheMarker()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The migration reads the viewer's Windows-only stores.");
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live import collision pin.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var directory = Directory.CreateTempSubdirectory("darling-import-collision-").FullName;
        var bodySucceeded = false;
        try
        {
            await MigrateStoreAsync(scratch, ct);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            var holder = Holder();
            Assert.Equal(MonitoredServerAddOutcome.Added, (await viewer.AddMonitoredServerAsync(holder, ct)).Outcome);

            /* The pre-Stage-3 registry this viewer carries: the colliding server and a free one. */
            var (registry, profiles) = NewRegistry(directory, "local");
            registry.AddServer(Entry(CollidingHost, "Colliding"), null, null);
            registry.AddServer(Entry("other01", "Other"), null, null);
            var migration = new ViewerServerMigration(registry, profiles, Path.Combine(directory, "migrated.marker"));

            var imported = await migration.MigrateAsync(viewer, ct);

            Assert.Equal(1, imported);
            Assert.True(migration.AlreadyMigrated);
            var rows = await viewer.GetMonitoredServersAsync(ct);
            Assert.Equal(new[] { "Holder", "Other" }, rows.Select(r => r.Name).OrderBy(n => n, StringComparer.Ordinal));
            Assert.Equal(HolderHost, Assert.Single(rows, r => r.ServerId == holder.ServerId).Host);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
            try { Directory.Delete(directory, recursive: true); } catch { /* best-effort */ }
        }
    }

    private static async Task MigrateStoreAsync(ScratchPostgres scratch, System.Threading.CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
    }

    private static (ViewerServerStore Servers, ViewerProfileStore Profiles) NewRegistry(string directory, string name)
    {
        var secrets = new NoSecretStore();
        var servers = new ViewerServerStore(Path.Combine(directory, name + "-servers.json"), secrets);
        var profiles = new ViewerProfileStore(servers, Path.Combine(directory, name + "-profiles.json"), secrets);
        return (servers, profiles);
    }

    /// <summary>Integrated-auth entries carry no secret, so nothing here touches the real vault.</summary>
    private sealed class NoSecretStore : IViewerServerSecretStore
    {
        public void Save(string id, string username, string password) { }
        public (string Username, string Password)? Find(string id) => null;
        public void Delete(string id) { }
    }
}
