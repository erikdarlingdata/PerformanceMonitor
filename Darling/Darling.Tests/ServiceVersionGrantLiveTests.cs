/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact here mints its own scratch database through ScratchPostgres and never touches another test's
   rows. The roles carry fixed cluster-wide names, so the class sits in the collection that serialises the role-creating
   facts, and a cluster that already has them skips (the rule the security-split facts follow). */

/// <summary>
/// The grants on <c>config.service_version</c> (V174, #5655), over a scratch store provisioned by the product's own batch:
/// the desktop viewer's two roles (<c>admin</c> and <c>viewer</c>) read the table, both when the table was there before
/// provisioning (an upgrade, where the batch runs after the migrations) and when it is created after provisioning (the
/// default privileges cover it).
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class ServiceVersionGrantLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheViewersRolesCanReadTheTable_BeforeAndAfterProvisioning()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the service version grant live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        Assert.SkipWhen(
            await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct) is true,
            "A cluster-wide admin/viewer/mcp role already exists on this cluster; the provisioning batch would adopt it.");

        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(OwnerRoleOf(owner), scratch.DatabaseName)), ct);
            await ExecAsync(owner,
                "INSERT INTO config.service_version (service_host, product_version, process_started_at) VALUES ('HOST-A', '3.11.0', now())", ct);

            /* The table was there when the batch ran: the schema-wide SELECT grant covers it. */
            await AssertReadsAsync(scratch, "admin", ProvisioningTestSecrets.AdminPassword, 1L, ct);
            await AssertReadsAsync(scratch, "viewer", ProvisioningTestSecrets.ViewerPassword, 1L, ct);

            /* The table is made after the batch ran (the store owner's migration on a later start): the default privileges cover it. */
            await ExecAsync(owner, "DROP TABLE config.service_version", ct);
            await ExecAsync(owner, ServiceVersionTable.CreateSql, ct);
            await AssertReadsAsync(scratch, "admin", ProvisioningTestSecrets.AdminPassword, 0L, ct);
            await AssertReadsAsync(scratch, "viewer", ProvisioningTestSecrets.ViewerPassword, 0L, ct);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await ExecAsync(cleanup,
                    $"{LiveCleanupBatch.DropOwnedByExistingProductRolesSql} DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanupCt));
        }
    }

    private static async Task AssertReadsAsync(ScratchPostgres scratch, string role, string password, long expectedRows, CancellationToken ct)
    {
        var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            Username = role,
            Password = password,
            SearchPath = "collect,config,public",
            Pooling = false,
        };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        var rows = await ServiceVersionTable.ReadAsync(dataSource, 15, ct);
        Assert.Equal(expectedRows, rows.Count);
    }

    private static string OwnerRoleOf(NpgsqlConnection owner) =>
        new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
