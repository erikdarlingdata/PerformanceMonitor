/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The gated live round-trip for the #1563 custom-views store (DARLING_TEST_PG — a dev Postgres the connecting
/// role owns). Proves what the pure tests cannot: after migration <c>custom_views</c> really lives in the
/// <c>config</c> schema and resolves by its bare name, and <see cref="CustomViewStore"/>'s CRUD +
/// optimistic-concurrency + duplicate-name detection behave end-to-end. Own-scoped per the shared-store doctrine:
/// GUID-suffixed view names + a finally cleanup, so a shared CI store is never clobbered and a mid-test failure
/// leaves nothing behind.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingCustomViewsLiveTests
{
    private const string SampleDefinition = "{\"panels\":[{\"read\":\"get_wait_stats\",\"viz\":\"table\",\"span\":1}]}";
    private const string SampleDefinitionV2 = "{\"panels\":[{\"read\":\"get_cpu_utilization\",\"viz\":\"line\",\"span\":2}]}";

    /// <summary>
    /// The create's <c>updated_by</c>, and deliberately NOT the <c>web</c> constant. This fixture used to pass
    /// <c>"web"</c> and assert <c>"web"</c> came back, which a store that ignored the argument entirely and
    /// hardcoded the constant satisfied perfectly — the assertion could not discriminate the binding it was
    /// there to prove. Since #2550 that column carries an authenticated subject, so the fixture carries a
    /// subject-shaped value: a placeholder, never a real address.
    /// </summary>
    private const string CreatePrincipal = "placeholder-author@example.invalid";

    /// <summary>
    /// The update's <c>updated_by</c>, DIFFERENT from the create's so the update is shown to REPLACE the stamp
    /// rather than leave the creator's. Nothing asserted <see cref="CustomViewStore.UpdateSql"/>'s <c>$5</c>
    /// end-to-end before, so a parameter swapped with <c>description</c> would have read as a clean pass.
    /// </summary>
    private const string UpdatePrincipal = "placeholder-editor@example.invalid";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the custom-views live tests.");
        return connectionString!;
    }

    [Fact]
    public async Task V31_CustomViews_LandsInConfigSchema_NotCollect()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.Equal("config", await SchemaOfAsync(connection, "custom_views", ct));
    }

    [Fact]
    public async Task CrudRoundTrip_Create_Get_List_Update_StaleConflict_DupConflict_Delete_NotFound()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;

        await using (var migrate = new NpgsqlConnection(connectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        /* Explicit search_path so the store's bare custom_views resolves to config.custom_views regardless of the
           store's database default (mirrors the security-split test's role connection strings). */
        var dataSourceConnectionString = new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString;

        await using var dataSource = NpgsqlDataSource.Create(dataSourceConnectionString);
        var store = new CustomViewStore(dataSource);

        var name1 = "cv_live_" + Guid.NewGuid().ToString("N");
        var name2 = "cv_live_" + Guid.NewGuid().ToString("N");
        var bodySucceeded = false;
        try
        {
            /* create -> Ok, version 1, fields round-trip. */
            var created = Assert.IsType<CustomViewResult.Ok>(
                await store.CreateAsync(name1, "first", SampleDefinition, CreatePrincipal, ct));
            var view = created.View!;
            Assert.Equal(name1, view.Name);
            Assert.Equal("first", view.Description);
            Assert.Equal(1, view.Version);
            Assert.Equal(CreatePrincipal, view.UpdatedBy);
            Assert.Contains("panels", view.DefinitionJson, StringComparison.Ordinal);

            /* get -> Ok, matches. */
            var fetched = Assert.IsType<CustomViewResult.Ok>(await store.GetAsync(view.Id, ct));
            Assert.Equal(view.Id, fetched.View!.Id);
            Assert.Equal(name1, fetched.View.Name);

            /* list -> contains our summary, and the kind scalar reads through — null here, since SampleDefinition
               is a kindless dashboard (definition->>'kind' is NULL); the wire layer defaults null -> "dashboard". */
            var list = await store.ListAsync(ct);
            Assert.Contains(list, s => s.Id == view.Id && s.Name == name1 && s.Version == 1);
            Assert.Null(list.Single(s => s.Id == view.Id).Kind);

            /* update at the correct version -> Ok, version bumped to 2, and the stamp REPLACED. A different
               principal from the create's is what makes the last assertion able to fail: with one shared value
               an update that never wrote $5 at all would still read back the expected string. */
            var updated = Assert.IsType<CustomViewResult.Ok>(
                await store.UpdateAsync(view.Id, name1, "second", SampleDefinitionV2, 1, UpdatePrincipal, ct));
            Assert.Equal(2, updated.View!.Version);
            Assert.Equal("second", updated.View.Description);
            Assert.Equal(UpdatePrincipal, updated.View.UpdatedBy);

            /* stale update (still presenting version 1) -> Conflict, not a silent clobber. */
            Assert.IsType<CustomViewResult.Conflict>(
                await store.UpdateAsync(view.Id, name1, "third", SampleDefinitionV2, 1, CreatePrincipal, ct));

            /* duplicate name on CREATE -> Conflict. */
            Assert.IsType<CustomViewResult.Conflict>(
                await store.CreateAsync(name1, null, SampleDefinition, CreatePrincipal, ct));

            /* duplicate name on UPDATE: a second view renamed onto the first's name -> Conflict. */
            var second = Assert.IsType<CustomViewResult.Ok>(
                await store.CreateAsync(name2, null, SampleDefinition, CreatePrincipal, ct));
            Assert.IsType<CustomViewResult.Conflict>(
                await store.UpdateAsync(second.View!.Id, name1, null, SampleDefinition, 1, CreatePrincipal, ct));

            /* update a non-existent id -> NotFound. */
            Assert.IsType<CustomViewResult.NotFound>(
                await store.UpdateAsync(-999999, name1, null, SampleDefinition, 1, CreatePrincipal, ct));

            /* delete -> Ok; then get + delete-again -> NotFound. */
            Assert.IsType<CustomViewResult.Ok>(await store.DeleteAsync(view.Id, ct));
            Assert.IsType<CustomViewResult.NotFound>(await store.GetAsync(view.Id, ct));
            Assert.IsType<CustomViewResult.NotFound>(await store.DeleteAsync(view.Id, ct));

            bodySucceeded = true;
        }
        finally
        {
            /* This teardown already opened its own connection, so it was half-right before #1902 — what it
               did not have is the other half: it still threw straight out of the finally, and it still used
               the test's own token, which on a CANCELLED run is already signalled and would have skipped the
               delete entirely. LiveStoreCleanup supplies both, and the hand-rolled connection goes with it. */
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using var command = new NpgsqlCommand("DELETE FROM config.custom_views WHERE name = $1 OR name = $2", cleanup);
                command.Parameters.AddWithValue(name1);
                command.Parameters.AddWithValue(name2);
                await command.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task<string?> SchemaOfAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = $1", connection);
        command.Parameters.AddWithValue(table);
        return (string?)await command.ExecuteScalarAsync(ct);
    }
}
