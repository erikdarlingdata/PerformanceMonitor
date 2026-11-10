/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres; the fixed admin/viewer/web/mcp roles are why the
   class sits in the pg-cluster-roles collection. */
/// <summary>
/// The one-server <c>find_queries_by_text</c> read runs as the provisioned <c>mcp</c> role. On the managed store that role has CONNECT on
/// the database and nothing else at the database level: <c>REVOKE ALL ON DATABASE ... FROM PUBLIC</c> takes TEMPORARY away from everyone,
/// so a read that needed a temp table would fail there with 42501, and the scratch store's owner, which may create one, would never show
/// it. The test puts the scratch database in the managed store's state and runs the search through a data source connected as <c>mcp</c>.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class FindQueriesByTextMcpRoleLiveTests
{
    private static readonly string[] s_cleanupRoles = ["admin", "viewer", "web", "mcp"];

    [Fact]
    public async Task TheOneServerSearch_RunsAsTheMcpRole_WhichHasNoTemporaryPrivilege()
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the find_queries_by_text role live test.");

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            var exist = await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct);
            Assert.SkipWhen(exist == "True", "A cluster-wide admin/viewer/web/mcp role already exists on this cluster; the grants would adopt it.");

            var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
            await DarlingMcpTestData.ExecAsync(owner, ct, DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName)));

            /* The managed store's database-level state, applied here by hand because ProvisioningTarget.Managed names the fixed darling
               database: PUBLIC keeps nothing on the database, TEMPORARY included, and mcp gets CONNECT back and nothing more. */
            var database = "\"" + scratch.DatabaseName.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
            await DarlingMcpTestData.ExecAsync(owner, ct, "REVOKE ALL ON DATABASE " + database + " FROM PUBLIC");
            await DarlingMcpTestData.ExecAsync(owner, ct, "GRANT CONNECT ON DATABASE " + database + " TO mcp");
            Assert.Equal("False", await ScalarAsync(owner, "SELECT has_database_privilege('mcp', current_database(), 'TEMP')", ct));

            await DarlingMcpTestData.RegisterServerAsync(owner, 1, "ledger", ct);
            await DarlingMcpTestData.ExecAsync(owner, ct,
                @"INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
                  VALUES (sha256('ledger'::text::bytea), 'SELECT * FROM dbo.Ledger WHERE LedgerId = @p', timezone('UTC', now()))");
            var sampledAt = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow) - TimeSpan.FromHours(1);
            await DarlingMcpTestData.ExecAsync(owner, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest)
                  VALUES ($1,$2,1,'ledger','Books','0xAA01',sha256('ledger'::text::bytea))",
                CollectionIdGenerator.Next(), sampledAt);

            var mcpString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = "mcp",
                Password = ProvisioningTestSecrets.McpPassword,
                Pooling = false,
            }.ConnectionString;
            await using var mcp = NpgsqlDataSource.Create(mcpString);
            var reply = await DarlingMcpFindQueriesByTextTools.FindQueriesByText(
                mcp, "dbo.Ledger", "ledger", null, 20, McpDataDetail.Full,
                DarlingMcpFindQueriesByTextTools.SourceBudget.Default, DarlingMcpFindQueriesByTextTools.SourceBudget.Default, ct);

            var root = JsonDocument.Parse(reply).RootElement;
            Assert.True(root.TryGetProperty("sources", out var sources), reply);
            var planCache = sources.GetProperty("plan_cache");
            Assert.Equal("ok", planCache.GetProperty("status").GetString());
            Assert.Equal(1, planCache.GetProperty("returned").GetInt32());
            Assert.Equal("0xAA01", planCache.GetProperty("rows")[0].GetProperty("query_hash").GetString());
            Assert.Equal(DateTime.SpecifyKind(sampledAt, DateTimeKind.Utc).ToString("o"), planCache.GetProperty("rows")[0].GetProperty("last_collected_at").GetString());
            Assert.Equal("empty", sources.GetProperty("query_store").GetProperty("status").GetString());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(s_cleanupRoles, "fqbtmcprole", cleanupCt));
        }
    }

    private static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
    }
}
