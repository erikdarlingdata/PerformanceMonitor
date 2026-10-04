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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and a role with a per-run
   name, and touches nothing on the shared one, so it is not serialized against the live-postgres collection. */

/// <summary>The fleet-tag writes as a non-superuser holding exactly the grants the managed <c>viewer</c> role gets
/// for them (#5085): the web dashboard's <c>/api/server-tags</c> routes run on the viewer-role pool, and every
/// other live test connects as the owner, which can write anything, so a missing grant would pass them all and
/// fail only in a real deployment with a 42501. The two grant lines are TAKEN FROM
/// <see cref="DarlingManagedRoles.BuildProvisioningSql"/>'s output with the role name swapped, so removing one from
/// the product's SQL makes this test fail.</summary>
public sealed class ServerTagViewerRoleLiveTests
{
    private static readonly string RoleName = "tag_viewer_" + Guid.NewGuid().ToString("N")[..8];
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    [Fact]
    public async Task TheViewerRoleGrants_LetServerTagStoreCreateAssignUpdateUnassignAndDelete()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live viewer-role server-tag test (it mints its own scratch database and role).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var owner = NpgsqlDataSource.Create(ownerString);
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        var provisioning = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);
        var tagGrants = provisioning
            .Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.StartsWith("GRANT ", StringComparison.Ordinal)
                && line.EndsWith(" TO " + "viewer" + ";", StringComparison.Ordinal)
                && (line.Contains("config.server_tags ", StringComparison.Ordinal) || line.Contains("config.server_tag_map ", StringComparison.Ordinal)))
            .ToList();
        Assert.Equal(2, tagGrants.Count);
        tagGrants = tagGrants
            .Select(line => line[..^(" TO " + "viewer" + ";").Length] + " TO " + RoleName + ";")
            .ToList();

        await ExecAsync(owner, $"CREATE ROLE {RoleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
        try
        {
            await ExecAsync(owner, $"GRANT USAGE ON SCHEMA config TO {RoleName}; GRANT SELECT ON ALL TABLES IN SCHEMA config TO {RoleName};", ct);
            foreach (var grant in tagGrants)
            {
                await ExecAsync(owner, grant, ct);
            }

            var asViewer = new NpgsqlConnectionStringBuilder(ownerString) { Username = RoleName, Password = RolePassword }.ConnectionString;
            await using var source = NpgsqlDataSource.Create(asViewer);
            var store = new ServerTagStore(source, 30);

            var created = Assert.IsType<ServerTagWriteResult.Ok>(await store.CreateAsync("Prod", null, null, ct));
            var tagId = created.Tag!.Id;
            Assert.Equal(new[] { 4, 5 }, await store.AssignAsync(tagId, new[] { 4, 5 }, ct));
            var updated = Assert.IsType<ServerTagWriteResult.Ok>(
                await store.UpdateAsync(tagId, new ServerTagEdit(true, "Production", false, null, false, null), ct));
            Assert.Equal("Production", updated.Tag!.Name);
            Assert.Equal(new[] { 4 }, await store.UnassignAsync(tagId, new[] { 4 }, ct));
            var deleted = Assert.IsType<ServerTagWriteResult.Ok>(await store.DeleteAsync(tagId, ct));
            Assert.Equal(1, deleted.RemovedAssignments);
            Assert.Empty(await store.ReadTagsAsync(ct));
        }
        finally
        {
            await ExecAsync(owner, $"DROP OWNED BY {RoleName}; DROP ROLE IF EXISTS {RoleName};", ct);
        }
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
