/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, L8: the Darling desktop viewer reads <c>collect.*</c> directly with its store login, not through an MCP tool or a
/// web route, so the statement filter at those reads does not reach it. This live test records what that login can read: the
/// viewer's grants are lifted from the shipped provisioning SQL (<see cref="DarlingManagedRoles.BuildProvisioningSql"/>), put on a
/// disposable role in a scratch store, and the role then SELECTs the three tables that hold statement text and plans. If any
/// is refused the viewer is a boundary of its own and the filter's scope has to be revisited, so the test fails loudly.
///
/// <para>#1776 own-store: mints its own scratch database (<see cref="ScratchPostgres"/>), never the shared
/// <c>live-postgres</c> collection. The role is cluster-wide, so it carries a per-run suffix and is dropped in cleanup.</para>
/// </summary>
public sealed class ViewerStoreLoginRawReadLiveTests
{
    private static readonly string Role = "ssf_viewer_" + Guid.NewGuid().ToString("N")[..8];
    private const string RolePassword = "SsfViewerTestPw0123456789abcdef01";

    /// <summary>The tables a plan or statement read goes to (plan section 6.6).</summary>
    private static readonly string[] Tables = { "query_text_dim", "query_plan_dim", "query_store_text" };

    [Fact]
    public async Task TheViewersShippedGrants_LetItSelectTheStatementAndPlanTables()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the viewer raw-read live test.");
        var ct = TestContext.Current.CancellationToken;

        var managed = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp, 15);

        /* The shipped statements that give viewer its read of the collect schema, lifted verbatim and retargeted. */
        var usage = Regex.Match(managed, @"GRANT USAGE ON SCHEMA [^;]*? TO admin, viewer;");
        var select = Regex.Match(managed, @"GRANT SELECT ON ALL TABLES IN SCHEMA collect TO admin, viewer;");
        Assert.True(usage.Success, "the managed provisioning SQL no longer grants viewer USAGE on the schemas");
        Assert.True(select.Success, "the managed provisioning SQL no longer grants viewer SELECT on the collect tables");

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, $"CREATE ROLE {Role} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            await ExecAsync(owner, usage.Value.Replace("TO admin, viewer;", "TO " + Role + ";", StringComparison.Ordinal), ct);
            await ExecAsync(owner, select.Value.Replace("TO admin, viewer;", "TO " + Role + ";", StringComparison.Ordinal), ct);

            var roleString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = Role,
                Password = RolePassword,
                SearchPath = "collect,config,public",
                Pooling = false,
            }.ConnectionString;
            await using var viewer = new NpgsqlConnection(roleString);
            await viewer.OpenAsync(ct);

            foreach (var table in Tables)
            {
                await using var command = new NpgsqlCommand($"SELECT count(*) FROM collect.{table}", viewer);
                var count = await command.ExecuteScalarAsync(ct);
                Assert.NotNull(count);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).DropRolesAsync(
                    $"DROP OWNED BY {Role}; DROP ROLE IF EXISTS {Role};", new[] { Role }, cleanupCt));
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
