/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres. The managed roles it provisions carry fixed,
   cluster-wide names (admin, viewer, mcp), so the class runs in the pg-cluster-roles collection with the other provisioning tests. */

/// <summary>
/// <c>update_service_flags</c> (#6393) as the provisioned, NOSUPERUSER <c>mcp</c> login, on a store provisioned by the shipped
/// <c>DarlingManagedRoles.BuildProvisioningSql</c>: the tool changes only the flags it is given, refuses an out-of-range value without
/// writing, moves the reload beacon, and the role's column grant covers the five flags and nothing else on <c>config_service</c>.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class DarlingMcpServiceFlagToolsLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonObject Parse(string json) => (JsonObject)JsonNode.Parse(json)!;

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The whole service row as one comparable string (booleans read t or f): every column the tool or the role could touch.</summary>
    private static async Task<string> RowAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT concat_ws('|', paused, capture_plans, mcp_enabled, mcp_port, web_enabled, web_port, query_store_backfill_enabled, " +
            "query_store_text_budget_mb, max_concurrent_sweeps, query_store_fetch_top_n, config_version) FROM config.config_service WHERE id = 1", owner);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<long> VersionAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT config_version FROM config.config_service WHERE id = 1", owner);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>Provisions the managed roles on a scratch store the way the service does and hands the body a data source that logs
    /// in as the real <c>mcp</c> role, plus the owner connection for seeding and for reading what landed.</summary>
    private static async Task WithMcpStoreAsync(bool seedRow, Func<NpgsqlDataSource, NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the service flag tool tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await using var existing = new NpgsqlCommand("SELECT COALESCE(string_agg(rolname, ', ' ORDER BY rolname), '') FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp')", owner);
            var names = (string)(await existing.ExecuteScalarAsync(ct))!;
            Assert.SkipWhen(names.Length > 0, $"The cluster already has the login role(s) {names}; they are cluster-wide and this test creates and drops all of admin, viewer and mcp, so it will not adopt them.");
        }

        var bodySucceeded = false;
        try
        {
            await using var ownerConnection = new NpgsqlConnection(scratch.ConnectionString);
            await ownerConnection.OpenAsync(ct);
            var ownerRole = new NpgsqlConnectionStringBuilder(ownerConnection.ConnectionString).Username ?? "darling";
            var batch = DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName));
            await ExecAsync(ownerConnection, batch, ct);
            if (seedRow)
            {
                /* Values that differ from every default and from each other, so "kept" and "written" cannot be confused. */
                await ExecAsync(ownerConnection,
                    "INSERT INTO config.config_service (id, capture_plans, mcp_enabled, mcp_port, web_enabled, web_port, query_store_backfill_enabled, " +
                    "query_store_text_budget_mb, max_concurrent_sweeps, query_store_fetch_top_n) VALUES (1, FALSE, TRUE, 5188, TRUE, 5199, FALSE, 40, 3, 250) " +
                    "ON CONFLICT (id) DO UPDATE SET capture_plans = FALSE, mcp_enabled = TRUE, mcp_port = 5188, web_enabled = TRUE, web_port = 5199, " +
                    "query_store_backfill_enabled = FALSE, query_store_text_budget_mb = 40, max_concurrent_sweeps = 3, query_store_fetch_top_n = 250", ct);
            }

            var mcpString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = "mcp",
                Password = ProvisioningTestSecrets.McpPassword,
                SearchPath = PgSchemaGenerator.SearchPath,
                Pooling = false,
            }.ConnectionString;
            await using var mcp = NpgsqlDataSource.Create(mcpString);

            await body(mcp, ownerConnection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand("DROP OWNED BY admin, viewer, web, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    [Fact]
    public async Task UpdatesOnlyTheNamedFlags_AndMovesTheBeacon_AndLeavesTheViewerOnlyFlagsAlone()
    {
        await WithMcpStoreAsync(true, async (mcp, owner, ct) =>
        {
            var versionBefore = await VersionAsync(owner, ct);

            var answer = Parse(await DarlingMcpServiceFlagTools.UpdateServiceFlags(
                mcp, capture_plans: true, query_store_text_budget_mb: 128, cancellationToken: ct));

            Assert.Equal("updated", (string)answer["status"]!);
            Assert.False((bool)answer["previous"]!["capture_plans"]!);
            Assert.Equal(40, (int)answer["previous"]!["query_store_text_budget_mb"]!);
            Assert.True((bool)answer["settings"]!["capture_plans"]!);
            Assert.Equal(128, (int)answer["settings"]!["query_store_text_budget_mb"]!);
            /* Omitted arguments keep the stored value, and the answer says so. */
            Assert.False((bool)answer["settings"]!["query_store_backfill_enabled"]!);
            Assert.Equal(3, (int)answer["settings"]!["max_concurrent_sweeps"]!);
            Assert.Equal(250, (int)answer["settings"]!["query_store_fetch_top_n"]!);

            /* paused, mcp_enabled, mcp_port, web_enabled, web_port are untouched; config_version moved by the self-bump trigger. */
            var versionAfter = await VersionAsync(owner, ct);
            Assert.True(versionAfter > versionBefore, "config_version must move so the running service reloads");
            Assert.Equal($"f|t|t|5188|t|5199|f|128|3|250|{versionAfter}", await RowAsync(owner, ct));

            var all = Parse(await DarlingMcpServiceFlagTools.UpdateServiceFlags(
                mcp, false, true, 4, 16, 0, cancellationToken: ct));
            Assert.Equal("updated", (string)all["status"]!);
            Assert.Equal($"f|f|t|5188|t|5199|t|4|16|0|{await VersionAsync(owner, ct)}", await RowAsync(owner, ct));
        });
    }

    [Theory]
    [InlineData("query_store_text_budget_mb", 3, 4, 256)]
    [InlineData("query_store_text_budget_mb", 257, 4, 256)]
    [InlineData("max_concurrent_sweeps", 0, 1, 16)]
    [InlineData("max_concurrent_sweeps", 17, 1, 16)]
    [InlineData("query_store_fetch_top_n", -1, 0, 100000)]
    [InlineData("query_store_fetch_top_n", 100001, 0, 100000)]
    public async Task AnOutOfRangeValueIsRefused_NamesTheFieldAndRange_AndWritesNothing(string field, int value, int min, int max)
    {
        await WithMcpStoreAsync(true, async (mcp, owner, ct) =>
        {
            var before = await RowAsync(owner, ct);

            /* A valid capture_plans rides along: a refusal must not write the flags that were fine. */
            var answer = Parse(await DarlingMcpServiceFlagTools.UpdateServiceFlags(
                mcp,
                capture_plans: true,
                query_store_text_budget_mb: field == "query_store_text_budget_mb" ? value : null,
                max_concurrent_sweeps: field == "max_concurrent_sweeps" ? value : null,
                query_store_fetch_top_n: field == "query_store_fetch_top_n" ? value : null,
                cancellationToken: ct));

            Assert.Equal("invalid", (string)answer["status"]!);
            var message = (string)answer["message"]!;
            Assert.Contains(field, message, StringComparison.Ordinal);
            Assert.Contains($"between {min} and {max}", message, StringComparison.Ordinal);
            Assert.Equal(before, await RowAsync(owner, ct));
        });
    }

    [Fact]
    public async Task NoArguments_IsRefused_AndWritesNothing()
    {
        await WithMcpStoreAsync(true, async (mcp, owner, ct) =>
        {
            var before = await RowAsync(owner, ct);
            var answer = Parse(await DarlingMcpServiceFlagTools.UpdateServiceFlags(mcp, cancellationToken: ct));
            Assert.Equal("invalid", (string)answer["status"]!);
            Assert.Equal(before, await RowAsync(owner, ct));
        });
    }

    [Fact]
    public async Task AnUnseededStore_AnswersUnavailable()
    {
        await WithMcpStoreAsync(false, async (mcp, owner, ct) =>
        {
            var answer = Parse(await DarlingMcpServiceFlagTools.UpdateServiceFlags(mcp, capture_plans: true, cancellationToken: ct));
            Assert.Equal("unavailable", (string)answer["status"]!);
        });
    }

    [Theory]
    [InlineData("mcp_enabled = FALSE")]
    [InlineData("mcp_port = 5555")]
    [InlineData("web_enabled = FALSE")]
    [InlineData("web_port = 5556")]
    [InlineData("paused = TRUE")]
    public async Task TheMcpRole_CannotWriteTheViewerOnlyColumns(string assignment)
    {
        await WithMcpStoreAsync(true, async (mcp, owner, ct) =>
        {
            var before = await RowAsync(owner, ct);
            var ex = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var command = mcp.CreateCommand($"UPDATE config.config_service SET {assignment} WHERE id = 1");
                await command.ExecuteNonQueryAsync(ct);
            });
            Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, ex.SqlState);
            Assert.Equal(before, await RowAsync(owner, ct));
        });
    }
}
