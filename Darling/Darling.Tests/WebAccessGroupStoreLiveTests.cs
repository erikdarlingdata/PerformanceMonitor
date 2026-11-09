/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

[Collection("pg-cluster-roles")]
public sealed class WebAccessGroupStoreLiveTests
{
    [Fact]
    public async Task ProtectedLifecycleAndRoleGrantsSurviveProvisioningAndTagDeletion()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to test web access group storage and real roles.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        var ownerSettings = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var owner = NpgsqlDataSource.Create(ownerSettings.ConnectionString);
        await using (var check = owner.CreateCommand("SELECT count(*) FROM pg_roles WHERE rolname IN ('admin','viewer','web','mcp')"))
            Assert.Equal(0L, await check.ExecuteScalarAsync(ct));
        var rolesCreated = false;
        try
        {
            await using (var migration = await owner.OpenConnectionAsync(ct))
                await PgMigrations.MigrateAsync(migration, null, ct);
            var provision = DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                target: ProvisioningTarget.ComposeStore(ownerSettings.Username!, scratch.DatabaseName),
                webVerifier: ProvisioningTestSecrets.Web);
            rolesCreated = true; // Cleanup also handles a provisioning batch that only partly ran.
            await Exec(owner, provision, ct);
            await using var admin = Source("admin", ProvisioningTestSecrets.AdminPassword);
            await using var viewer = Source("viewer", ProvisioningTestSecrets.ViewerPassword);
            await using var web = Source("web", ProvisioningTestSecrets.WebPassword);
            await using var mcp = Source("mcp", ProvisioningTestSecrets.McpPassword);
            var adminStore = new WebAccessGroupStore(admin);
            var ownerStore = new WebAccessGroupStore(owner);
            var webStore = new WebAccessGroupStore(web);
            using var key = PasswordPrivateKey.Generate();
            var id = Guid.NewGuid();
            var generation = Guid.NewGuid();
            const string token = "synthetic-web-group-token-only-for-tests";
            string Seal(Guid gen) => PasswordSeal.Seal(token, key.PublicKey, PasswordBinding.ForWebAccessToken(id, gen));
            var sealedToken = Seal(generation);
            await using var tag = owner.CreateCommand("INSERT INTO config.server_tags(name) VALUES ('Finance') RETURNING id");
            var tagId = (int)(await tag.ExecuteScalarAsync(ct))!;

            Assert.Equal(1, await adminStore.CreateAsync(id, "Finance", WebAccessGroupScope.Tags,
                new[] { tagId, tagId }, generation, sealedToken, true, "test-admin", ct));
            var row = Assert.IsType<WebAccessGroup>(await webStore.ReadAsync(id, ct));
            Assert.Equal("pending", row.ActivationState);
            Assert.Equal(new[] { tagId }, row.TagIds);
            Assert.Single(await adminStore.ListAsync(ct));
            Assert.Equal(sealedToken, Assert.Single(await ownerStore.ReadCredentialsAsync(ct)).SealedToken);
            Assert.DoesNotContain(sealedToken, Assert.Single(await ownerStore.ReadCredentialsAsync(ct)).ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain(sealedToken, row.ToString(), StringComparison.Ordinal);
            Assert.Equal(token, PasswordSeal.Open(sealedToken, key, PasswordBinding.ForWebAccessToken(id, generation)));

            // The storage owner can activate only the exact pending revision and generation.
            await Denied(() => adminStore.ActivateAsync(id, 1, generation, null, ct));
            Assert.False(await ownerStore.ActivateAsync(id, 99, generation, null, ct));
            Assert.False(await ownerStore.ActivateAsync(id, 1, Guid.NewGuid(), null, ct));
            Assert.True(await ownerStore.ActivateAsync(id, 1, generation, null, ct));
            row = (await webStore.ReadAsync(id, ct))!;
            Assert.Equal("active", row.ActivationState);
            Assert.Equal(2, row.Revision);

            foreach (var source in new[] { admin, viewer, web, mcp })
            {
                await Denied(() => new WebAccessGroupStore(source).ReadCredentialsAsync(ct));
                foreach (var table in new[] { "web_access_groups", "web_access_group_tags", "web_access_group_events" })
                {
                    await Denied(() => Exec(source, "DELETE FROM config." + table, ct));
                    await Denied(() => Exec(source, "TRUNCATE config." + table, ct));
                }
            }
            foreach (var source in new[] { viewer, web, mcp })
                await Denied(() => new WebAccessGroupStore(source).DeleteAsync(id, row.Revision, "not-admin", ct));

            await State("WG409", () => adminStore.UpdateAsync(id, 1, WebAccessGroupScope.Tags, new[] { tagId }, true, "stale", ct));
            await State("WG400", () => adminStore.CreateAsync(Guid.NewGuid(), "Empty", WebAccessGroupScope.Tags,
                Array.Empty<int>(), Guid.NewGuid(), sealedToken, true, "admin", ct));
            await State("WG400", () => adminStore.CreateAsync(Guid.NewGuid(), "Plain", WebAccessGroupScope.AllServers,
                Array.Empty<int>(), Guid.NewGuid(), token, true, "admin", ct));
            await State("WG400", () => adminStore.UpdateAsync(id, 2, WebAccessGroupScope.AllServers, new[] { tagId }, true, "admin", ct));
            await State("WG400", () => adminStore.UpdateAsync(id, 2, WebAccessGroupScope.Tags, new[] { int.MaxValue }, true, "admin", ct));
            await State("WG409", () => adminStore.CreateAsync(Guid.NewGuid(), "Finance", WebAccessGroupScope.AllServers,
                Array.Empty<int>(), Guid.NewGuid(), sealedToken, true, "admin", ct));

            // A narrower tag-editor role deletes the tag; only the owner trigger touches grants.
            await Exec(owner, "GRANT DELETE ON config.server_tags TO web", ct);
            await Exec(web, "DELETE FROM config.server_tags WHERE id=" + tagId, ct);
            row = (await webStore.ReadAsync(id, ct))!;
            Assert.Equal(WebAccessGroupScope.Tags, row.Scope);
            Assert.Empty(row.TagIds);
            Assert.Equal(3, row.Revision);
            Assert.Equal(generation, row.TokenGeneration);
            Assert.Equal("active", row.ActivationState);
            await using (var recreate = owner.CreateCommand("INSERT INTO config.server_tags(name) VALUES ('Finance') RETURNING id"))
                Assert.NotEqual(tagId, (int)(await recreate.ExecuteScalarAsync(ct))!);
            Assert.Empty((await webStore.ReadAsync(id, ct))!.TagIds);

            Assert.Equal(4, await adminStore.UpdateAsync(id, 3, WebAccessGroupScope.AllServers, Array.Empty<int>(), false, "admin", ct));
            row = (await webStore.ReadAsync(id, ct))!;
            Assert.Equal(WebAccessGroupScope.AllServers, row.Scope);
            Assert.False(row.Enabled);
            Assert.Empty(row.TagIds);
            var replacement = Guid.NewGuid();
            Assert.Equal(5, await adminStore.RotateAsync(id, 4, replacement, Seal(replacement), "admin", ct));
            Assert.False(await ownerStore.ActivateAsync(id, 4, generation, null, ct));
            Assert.True(await ownerStore.ActivateAsync(id, 5, replacement, "duplicate_token", ct));
            row = (await webStore.ReadAsync(id, ct))!;
            Assert.Equal("refused", row.ActivationState);
            Assert.Equal("duplicate_token", row.RefusalCode);
            Assert.Equal(replacement, row.TokenGeneration);
            await State("WG400", () => adminStore.RotateAsync(id, 6, generation, Seal(generation), "admin", ct));
            Assert.Equal(7, await adminStore.DeleteAsync(id, 6, "admin", ct));
            await State("WG409", () => adminStore.CreateAsync(id, "Finance", WebAccessGroupScope.AllServers,
                Array.Empty<int>(), generation, sealedToken, true, "admin", ct));
            Assert.Null(await webStore.ReadAsync(id, ct));
            Assert.False(await ownerStore.ActivateAsync(id, 5, replacement, null, ct));
            await using (var events = owner.CreateCommand("SELECT string_agg(COALESCE(before_state::text,'') || COALESCE(after_state::text,''),'') FROM config.web_access_group_events"))
            {
                var text = (string)(await events.ExecuteScalarAsync(ct))!;
                Assert.DoesNotContain(token, text, StringComparison.Ordinal);
                Assert.DoesNotContain("sealed:", text, StringComparison.Ordinal);
            }

            // Recreate a name under a fresh identity: the deleted identity remains absent.
            var newId = Guid.NewGuid();
            var newGen = Guid.NewGuid();
            var newSeal = PasswordSeal.Seal(token, key.PublicKey, PasswordBinding.ForWebAccessToken(newId, newGen));
            await adminStore.CreateAsync(newId, "Finance", WebAccessGroupScope.AllServers,
                Array.Empty<int>(), newGen, newSeal, true, "admin", ct);
            Assert.Null(await webStore.ReadAsync(id, ct));
            Assert.NotEqual(id, Assert.Single(await webStore.ListAsync(ct)).GroupId);

            // Even an accidental blanket DML grant cannot bypass the owner guard.
            await Exec(owner, "GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON config.web_access_groups,config.web_access_group_tags,config.web_access_group_events TO admin,viewer,web,mcp", ct);
            foreach (var source in new[] { admin, viewer, web, mcp })
                await Denied(() => Exec(source, "DELETE FROM config.web_access_groups", ct));

            await Exec(owner, "GRANT SET ON PARAMETER session_replication_role TO viewer", ct);
            try
            {
                await using var replicaConnection = await viewer.OpenConnectionAsync(ct);
                await using var command = replicaConnection.CreateCommand();
                command.CommandText = "SET session_replication_role = replica";
                await command.ExecuteNonQueryAsync(ct);
                try
                {
                    command.CommandText = "DELETE FROM config.web_access_groups";
                    await Denied(() => command.ExecuteNonQueryAsync(ct));
                    command.CommandText = "TRUNCATE config.web_access_group_events";
                    await Denied(() => command.ExecuteNonQueryAsync(ct));
                }
                finally
                {
                    command.CommandText = "SET session_replication_role = origin";
                    await command.ExecuteNonQueryAsync(ct);
                }
            }
            finally
            {
                await Exec(owner, "REVOKE SET ON PARAMETER session_replication_role FROM viewer", CancellationToken.None);
            }
            // Re-provisioning removes secret-column/table grants, and leaves admin functions working.
            await Exec(owner, provision, ct);
            foreach (var source in new[] { admin, viewer, web, mcp })
                await Denied(() => new WebAccessGroupStore(source).ReadCredentialsAsync(ct));
            Assert.True(await ownerStore.ActivateAsync(newId, 1, newGen, null, ct));
            Assert.True(await ownerStore.ActivateAsync(newId, 2, newGen, "key_unavailable", ct));
            Assert.Equal("refused", (await webStore.ReadAsync(newId, ct))!.ActivationState);
            Assert.False(await ownerStore.ActivateAsync(newId, 3, newGen, null, ct));
            Assert.Equal(4, await adminStore.DeleteAsync(newId, 3, "admin", ct));
            // The schema is replayable without widening any grants.
            await Exec(owner, WebAccessGroupSchema.CreateSql, ct);
            await Denied(() => new WebAccessGroupStore(viewer).CreateAsync(Guid.NewGuid(), "No", WebAccessGroupScope.AllServers,
                Array.Empty<int>(), Guid.NewGuid(), newSeal, true, "viewer", ct));
        }
        finally
        {
            if (rolesCreated)
            {
                NpgsqlConnection.ClearAllPools();
                await Exec(owner, LiveCleanupBatch.DropOwnedByExistingProductRolesSql
                    + " DROP ROLE IF EXISTS admin,viewer,web,mcp", CancellationToken.None);
            }
        }

        NpgsqlDataSource Source(string role, string password) =>
            NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(ownerSettings.ConnectionString) { Username=role,Password=password }.ConnectionString);
    }


    [Fact]
    public async Task MigrationCarvesCustomDefaultGrantsOnlyOnTheNewProtectedObjects()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to test custom default grants.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var owner = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString);
        var role = "web_group_defaults_" + Guid.NewGuid().ToString("N")[..12];
        await Exec(owner, "CREATE ROLE " + role + " NOLOGIN", ct);
        try
        {
            // ScratchPostgres normally clones a fully migrated template. Rewind this rung first:
            // default grants must exist BEFORE the new protected objects are created.
            await using (var connection = await owner.OpenConnectionAsync(ct))
            {
                var previousRung = WebAccessGroupSchema.Version - 1;
                await RungRewindArtifacts.DropSentinelsAboveAsync(connection, previousRung, ct);
                await using var rewind = new NpgsqlCommand("DELETE FROM collect.darling_schema_version WHERE version > $1", connection);
                rewind.Parameters.AddWithValue(previousRung);
                await rewind.ExecuteNonQueryAsync(ct);
            }
            await Exec(owner,
                "ALTER DEFAULT PRIVILEGES IN SCHEMA config GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO " + role + "; "
                + "ALTER DEFAULT PRIVILEGES IN SCHEMA config GRANT EXECUTE ON FUNCTIONS TO " + role + "; "
                + "ALTER DEFAULT PRIVILEGES IN SCHEMA config GRANT USAGE,SELECT ON SEQUENCES TO " + role + "; "
                + "GRANT SELECT ON config.server_tags TO " + role, ct);
            await using (var connection = await owner.OpenConnectionAsync(ct))
                await PgMigrations.MigrateAsync(connection, null, ct);
            await using var check = owner.CreateCommand("""
                SELECT has_column_privilege($1,'config.web_access_groups','sealed_token','SELECT'),
                    has_table_privilege($1,'config.web_access_group_tags','INSERT'),
                    has_function_privilege($1,'config.manage_web_access_group(text,uuid,bigint,text,text,integer[],uuid,text,boolean,text)','EXECUTE'),
                    has_function_privilege($1,'config.activate_web_access_group(uuid,bigint,uuid,text)','EXECUTE'),
                    has_table_privilege($1,'config.server_tags','SELECT'),
                    has_sequence_privilege($1,pg_get_serial_sequence('config.web_access_group_events','event_id'),'USAGE')
                """);
            check.Parameters.AddWithValue(role);
            await using var reader = await check.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.False(reader.GetBoolean(0));
            Assert.False(reader.GetBoolean(1));
            Assert.False(reader.GetBoolean(2));
            Assert.False(reader.GetBoolean(3));
            Assert.True(reader.GetBoolean(4)); // Custom grants on existing tables are preserved.
            Assert.False(reader.GetBoolean(5));
        }
        finally
        {
            await Exec(owner, "DROP OWNED BY " + role + "; DROP ROLE " + role, CancellationToken.None);
        }
    }

    private static async Task Exec(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        command.CommandTimeout = 120;
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task Denied(Func<Task> action) => State("42501", action);
    private static async Task State(string code, Func<Task> action)
    {
        var error = await Assert.ThrowsAsync<PostgresException>(action);
        Assert.Equal(code, error.SqlState);
    }
}
