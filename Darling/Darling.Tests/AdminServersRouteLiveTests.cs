/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database and a role with a per-run name through
   ScratchPostgres and touches nothing on the shared one, so it is not serialized against the live-postgres
   collection. */

/// <summary>
/// <c>GET /api/admin/servers</c> through a real host over a scratch store, as a non-superuser holding exactly the
/// <c>viewer</c> role's column grant on <c>config_monitored_servers</c> (#5239): a statement that named a
/// credential column would fail here with a 42501, the way it would in a deployment, instead of passing as the
/// owner.
/// </summary>
public sealed class AdminServersRouteLiveTests
{
    private const string SecretPassword = "SECRET-BLOB-5239";
    private const string SecretLogin = "secret-login-5239";

    private static readonly string RoleName = "adm_viewer_" + Guid.NewGuid().ToString("N")[..8];
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    private static async Task ExecAsync(NpgsqlDataSource source, string sql)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task TheRoute_AnswersEveryConfiguredServer_DisabledIncluded_InOrder_WithNoCredential_AsTheViewerRole()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live admin-servers route test (it mints its own scratch database and role).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var owner = NpgsqlDataSource.Create(ownerString);
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        await ExecAsync(owner, $"CREATE ROLE {RoleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'");
        try
        {
            await ExecAsync(owner,
                $"GRANT USAGE ON SCHEMA collect, config TO {RoleName}; "
                + $"GRANT SELECT ON ALL TABLES IN SCHEMA collect TO {RoleName}; "
                + $"GRANT SELECT ON ALL TABLES IN SCHEMA config TO {RoleName}; "
                + DarlingManagedRoles.BuildViewerColumnAclSql("config", RoleName));

            /* Four configured servers. Display names are chosen so the order is neither insertion order nor id
               order: "alpha" and "Alpha" tie ignoring case and fall to the ordinal server name. */
            await ExecAsync(owner, $@"
INSERT INTO config.config_monitored_servers
    (server_id, name, host, auth, username, encrypted_password, monthly_cost_usd, is_enabled, created_at, engine)
VALUES
    (11, 'Zulu',  'zulu-b',  'sql',        '{SecretLogin}', '{SecretPassword}', 1234.5, TRUE,  '2026-01-02 03:04:05', 'sqlserver'),
    (12, 'alpha', 'alpha-02', 'integrated', NULL,            NULL,               0,      FALSE, '2026-01-03 03:04:05', 'sqlserver'),
    (13, 'Alpha', 'alpha-01', 'integrated', NULL,            NULL,               250,    TRUE,  '2026-01-04 03:04:05', 'sqlserver'),
    (14, 'Pg',    'pg-01',    'sql',        '{SecretLogin}', '{SecretPassword}', 0,      FALSE, '2026-01-05 03:04:05', 'postgres');
INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (13, 'alpha-01', 'Alpha', TRUE, 16, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC');");

            var asViewer = new NpgsqlConnectionStringBuilder(ownerString) { Username = RoleName, Password = RolePassword }.ConnectionString;
            await using var viewer = NpgsqlDataSource.Create(asViewer);

            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            await using var app = builder.Build();
            DarlingWebEndpoints.MapAdminServers(app, viewer);
            await app.StartAsync(ct);
            using var client = app.GetTestClient();

            using var response = await client.GetAsync("/api/admin/servers", ct);
            var text = await response.Content.ReadAsStringAsync(ct);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal("application/json", response.Content.Headers.ContentType?.MediaType);

            Assert.DoesNotContain(SecretPassword, text, StringComparison.Ordinal);
            Assert.DoesNotContain(SecretLogin, text, StringComparison.Ordinal);
            Assert.DoesNotContain("username", text, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("password", text, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("encrypted", text, StringComparison.OrdinalIgnoreCase);

            using var doc = JsonDocument.Parse(text);
            Assert.Equal(4, doc.RootElement.GetProperty("server_count").GetInt32());
            var servers = doc.RootElement.GetProperty("servers").EnumerateArray().ToList();
            Assert.Equal(new[] { "alpha-01", "alpha-02", "pg-01:pg", "zulu-b" },
                servers.Select(s => s.GetProperty("server_name").GetString()!).ToArray());

            var disabled = servers.Single(s => s.GetProperty("server_name").GetString() == "alpha-02");
            Assert.Equal("Disabled", disabled.GetProperty("status").GetString());
            Assert.Equal("Windows", disabled.GetProperty("auth").GetString());
            Assert.Equal(JsonValueKind.Null, disabled.GetProperty("monthly_cost").ValueKind);
            Assert.Equal("AwaitingFirstCollection", disabled.GetProperty("freshness").GetString());
            Assert.Equal("2026-01-03T03:04:05.0000000", disabled.GetProperty("added").GetString());

            var collected = servers.Single(s => s.GetProperty("server_name").GetString() == "alpha-01");
            Assert.Equal("Enabled", collected.GetProperty("status").GetString());
            Assert.Equal("$250", collected.GetProperty("monthly_cost").GetString());
            Assert.Equal("SQL Server 2022", collected.GetProperty("version").GetString());

            var zulu = servers.Single(s => s.GetProperty("server_name").GetString() == "zulu-b");
            Assert.Equal("SQL Server", zulu.GetProperty("auth").GetString());
            Assert.Equal("$1,235", zulu.GetProperty("monthly_cost").GetString());

            var postgres = servers.Single(s => s.GetProperty("server_name").GetString() == "pg-01:pg");
            Assert.Equal("Disabled", postgres.GetProperty("status").GetString());
            Assert.Equal("postgres", postgres.GetProperty("engine").GetString());

            /* Only GET is mapped: a write method on the route is refused by the router. */
            using var post = await client.PostAsync("/api/admin/servers", new StringContent("{}", System.Text.Encoding.UTF8, "application/json"), ct);
            Assert.Equal(HttpStatusCode.MethodNotAllowed, post.StatusCode);
        }
        finally
        {
            await ExecAsync(owner, $"DROP OWNED BY {RoleName}; DROP ROLE IF EXISTS {RoleName};");
        }
    }
}
