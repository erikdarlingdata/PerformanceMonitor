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

/// <summary>The fleet-tag writes as a non-superuser holding exactly the grants the managed <c>mcp</c> role gets
/// for them (#5085). Every other live test connects as the owner, which can write anything, so a missing
/// <c>mcp</c> grant would pass them all and fail only in a real deployment with a 42501.
///
/// <para>The test role is a LOGIN NOSUPERUSER role with a per-run name (the <c>DarlingMcpStoreHostToolsLiveTests</c>
/// pattern; a role belongs to the whole cluster, so the literal <c>mcp</c> would collide with a real run). It gets
/// the schema reach the real role has (USAGE and SELECT on <c>config</c>) and the two tag-table write grants
/// TAKEN FROM <see cref="DarlingManagedRoles.BuildProvisioningSql"/>'s output with the role name swapped, so
/// removing a grant from the product's SQL makes this test fail.</para></summary>
public sealed class ServerTagMcpRoleLiveTests
{
    private static readonly string RoleName = "tag_mcp_" + Guid.NewGuid().ToString("N")[..8];
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    [Fact]
    public async Task TheMcpRoleGrants_LetServerTagStoreCreateAssignUpdateAndDelete()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live mcp-role server-tag test (it mints its own scratch database and role).");

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
                && line.EndsWith(" TO mcp;", StringComparison.Ordinal)
                && (line.Contains("config.server_tags ", StringComparison.Ordinal) || line.Contains("config.server_tag_map ", StringComparison.Ordinal)))
            .Select(line => line[..^" TO mcp;".Length] + " TO " + RoleName + ";")
            .ToList();

        await ExecAsync(owner, $"CREATE ROLE {RoleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
        try
        {
            await ExecAsync(owner, $"GRANT USAGE ON SCHEMA config TO {RoleName}; GRANT SELECT ON ALL TABLES IN SCHEMA config TO {RoleName};", ct);

            /* Exactly the two tag-table write grants come out of the product's SQL; a renamed or dropped line
               would otherwise leave this test granting nothing and still passing. */
            Assert.Equal(2, tagGrants.Count);

            var asMcp = new NpgsqlConnectionStringBuilder(ownerString) { Username = RoleName, Password = RolePassword }.ConnectionString;
            await using var source = NpgsqlDataSource.Create(asMcp);
            var store = new ServerTagStore(source, 30);

            /* Before the grants the same role can read but not write: the insufficient-privilege answer proves the
               grants below are what lets the writes through. */
            var denied = await Assert.ThrowsAsync<PostgresException>(() => store.CreateAsync("Prod", null, null, ct));
            Assert.Equal("42501", denied.SqlState);

            foreach (var grant in tagGrants)
            {
                await ExecAsync(owner, grant, ct);
            }

            var created = Assert.IsType<ServerTagWriteResult.Ok>(await store.CreateAsync("Prod", null, null, ct));
            var tagId = created.Tag!.Id;
            Assert.Equal(new[] { 4, 5 }, await store.AssignAsync(tagId, new[] { 4, 5 }, ct));
            var updated = Assert.IsType<ServerTagWriteResult.Ok>(
                await store.UpdateAsync(tagId, new ServerTagEdit(true, "Production", false, null, false, null), ct));
            Assert.Equal("Production", updated.Tag!.Name);
            Assert.Equal(new[] { 4 }, await store.UnassignAsync(tagId, new[] { 4 }, ct));
            var deleted = Assert.IsType<ServerTagWriteResult.Ok>(await store.DeleteAsync(tagId, confirm: true, ct));
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
