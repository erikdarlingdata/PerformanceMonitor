/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#9641: what the alert source tools keep out of a slow-read label, and what keeps an agent away from them. Pure checks.</summary>
public sealed class AlertSourceStatementLabelTests
{
    [Theory]
    [InlineData("ALTER ROLE \"darling_src_netmon\" PASSWORD 'SCRAM-SHA-256$4096:c2FsdA==$stored:server'", "ALTER ROLE \"darling_src_netmon\"")]
    [InlineData("alter role \"darling_src_netmon\" password 'SCRAM-SHA-256$4096:c2FsdA==$stored:server'", "alter role \"darling_src_netmon\"")]
    [InlineData("CREATE ROLE \"darling_src_netmon\" LOGIN NOSUPERUSER CONNECTION LIMIT 2 PassWord 'SCRAM-SHA-256$4096:c2FsdA==$stored:server';", "CREATE ROLE \"darling_src_netmon\" LOGIN NOSUPERUSER CONNECTION LIMIT 2")]
    [InlineData("-- add\nALTER ROLE x\tPASSWORD\t'SCRAM-SHA-256$1:2$3:4'", "ALTER ROLE x")]
    public void ALabel_StopsBeforeThePasswordKeyword_SoNoVerifierIsKept(string sql, string label)
    {
        Assert.Equal(label, StatementLabel.Label(sql));
    }

    [Fact]
    public void ALabel_OfAnOrdinaryStatement_IsUnchanged()
    {
        Assert.Equal("SELECT password_hash FROM app_users WHERE id = $1", StatementLabel.Label("SELECT password_hash FROM app_users WHERE id = $1"));
    }

    [Fact]
    public void EveryServerAdminTool_IsAlsoANeverAgentTool()
    {
        Assert.NotEmpty(DarlingMcpWritePolicy.ServerAdminTools);
        Assert.All(DarlingMcpWritePolicy.ServerAdminTools, tool => Assert.Contains(tool, DarlingMcpWritePolicy.NeverAgentTools));
    }
}

/// <summary>
/// #9641: rotate and remove ask for <c>confirm=true</c> before they change a login, and the statements that carry a password verifier never
/// reach a slow-read record. Each test mints its own scratch store. The roles they provision are cluster-wide, so the class sits in the
/// <c>pg-cluster-roles</c> collection.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class AlertSourceConfirmAndStatementLiveTests
{
    /* #1776 own-store: each test mints its own scratch database through ScratchPostgres and a per-run source name; the fixed
       admin/viewer/mcp roles are why the class sits in the pg-cluster-roles collection. */
    private readonly string _runSuffix = Guid.NewGuid().ToString("N")[..8];

    private static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Migrates the scratch store and runs the managed provisioning batch. False when a cluster-wide role is already there.</summary>
    private static async Task<bool> ProvisionAsync(NpgsqlConnection owner, string connectionString, string databaseName, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(owner, ct);
        if (await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct) == "True")
        {
            return false;
        }

        var ownerRole = new NpgsqlConnectionStringBuilder(connectionString).Username ?? "darling";
        await using var provision = new NpgsqlCommand(
            DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, databaseName)), owner);
        await provision.ExecuteNonQueryAsync(ct);
        return true;
    }

    [Fact]
    public async Task RotateAndRemove_WithoutConfirm_ChangeNothing_AndSayWhatTheyWouldDo()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source tool tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var source = "conf_" + _runSuffix;
        var login = ExternalAlertSourceAdmin.RoleNameFor(source);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            Assert.SkipUnless(await ProvisionAsync(owner, scratch.ConnectionString, scratch.DatabaseName, ct),
                "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var seam = new StoreOwnerSource();
            seam.Publish(dataSource);
            var postgres = new PostgresConfig();

            var added = JsonNode.Parse(await DarlingMcpAlertSourceTools.ChangeAsync(
                "add_alert_source", "--add-alert-source", dataSource, postgres, source, null, ct))!.AsObject();
            Assert.Equal("added", (string?)added["status"]);
            await ExecAsync(owner,
                "INSERT INTO config.external_alert_pending (source_id, kind, alert_key, severity, title, expires_at) "
                + $"SELECT source_id, 'fire', 'disk', 'warning', 'a title', now() + interval '15 minutes' FROM config.external_alert_source WHERE source_name = '{source}'", ct);
            var verifier = await ScalarAsync(owner, $"SELECT rolpassword FROM pg_authid WHERE rolname = '{login}'", ct);
            Assert.StartsWith("SCRAM-SHA-256$", verifier, StringComparison.Ordinal);

            /* The tool methods with confirm left at its default: rotate changes no password. */
            var rotate = JsonNode.Parse(await DarlingMcpAlertSourceTools.RotateAlertSource(seam, postgres, source, cancellationToken: ct))!.AsObject();
            Assert.Equal("confirm_required", (string?)rotate["status"]);
            Assert.Equal(login, (string?)rotate["login"]);
            Assert.Equal(source, (string?)rotate["source"]);
            Assert.NotNull(rotate["open_sessions"]);
            Assert.Equal(verifier, await ScalarAsync(owner, $"SELECT rolpassword FROM pg_authid WHERE rolname = '{login}'", ct));

            /* Remove deletes nothing: the row, the login and the queued alert are all still there, and the answer counts the undelivered alert. */
            var remove = JsonNode.Parse(await DarlingMcpAlertSourceTools.RemoveAlertSource(seam, postgres, source, cancellationToken: ct))!.AsObject();
            Assert.Equal("confirm_required", (string?)remove["status"]);
            Assert.Equal(1, (int?)remove["undelivered_alerts"]);
            Assert.Equal("1", await ScalarAsync(owner, $"SELECT count(*) FROM config.external_alert_source WHERE source_name = '{source}'", ct));
            Assert.Equal("1", await ScalarAsync(owner, $"SELECT count(*) FROM pg_roles WHERE rolname = '{login}'", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.external_alert_pending", ct));

            /* An unknown source is still refused with the usual reason. */
            var unknown = JsonNode.Parse(await DarlingMcpAlertSourceTools.RemoveAlertSource(seam, postgres, "nosuch", cancellationToken: ct))!.AsObject();
            Assert.Equal("not_registered", (string?)unknown["reason"]);

            /* With confirm=true the same calls act. */
            var rotated = JsonNode.Parse(await DarlingMcpAlertSourceTools.RotateAlertSource(seam, postgres, source, confirm: true, cancellationToken: ct))!.AsObject();
            Assert.Equal("rotated", (string?)rotated["status"]);
            Assert.NotEqual(verifier, await ScalarAsync(owner, $"SELECT rolpassword FROM pg_authid WHERE rolname = '{login}'", ct));
            var removed = JsonNode.Parse(await DarlingMcpAlertSourceTools.RemoveAlertSource(seam, postgres, source, confirm: true, cancellationToken: ct))!.AsObject();
            Assert.Equal("removed", (string?)removed["status"]);
            Assert.Equal("0", await ScalarAsync(owner, $"SELECT count(*) FROM pg_roles WHERE rolname = '{login}'", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(
                    ["admin", "viewer", "web", "mcp", login, DarlingManagedRoles.AlertSourceGroupRoleName], _runSuffix, cleanupCt));
        }
    }

    [Fact]
    public async Task ARotateThroughTheHost_LeavesNoPasswordVerifier_InAnyKeptSlowReadRecord()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source tool tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var source = "slow_" + _runSuffix;
        var login = ExternalAlertSourceAdmin.RoleNameFor(source);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            Assert.SkipUnless(await ProvisionAsync(owner, scratch.ConnectionString, scratch.DatabaseName, ct),
                "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var seam = new StoreOwnerSource();
            seam.Publish(dataSource);
            var added = JsonNode.Parse(await DarlingMcpAlertSourceTools.ChangeAsync(
                "add_alert_source", "--add-alert-source", dataSource, new PostgresConfig(), source, null, ct))!.AsObject();
            Assert.Equal("added", (string?)added["status"]);
            var verifier = await ScalarAsync(owner, $"SELECT rolpassword FROM pg_authid WHERE rolname = '{login}'", ct);

            /* The real host: its tool filter, its statement listener and a slow-read log that keeps every call (threshold 0), with the
               writes and the server admin tools served. */
            ReadStatementCapture.Register();
            var slowReads = new SlowReadLog(0);
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            DarlingMcpHostService.RegisterToolServices(
                builder.Services, dataSource, new MonitoredServerRegistryState(), new PostgresConfig(), analyzer: null, planFetcher: null,
                NullLogger.Instance, new BaselineCache(), ownerSource: seam);
            DarlingMcpHostService.ConfigureMcpServices(
                builder.Services, DarlingPeerDirectory.Snapshot.Empty, new ReadLatencyAccumulator(), NullLogger.Instance, slowReads,
                writes: new DarlingMcpWritePolicy.Effective(true, true));
            await using var app = builder.Build();
            var host = new DarlingMcpHostService(NullLogger<DarlingMcpHostService>.Instance, new McpRuntimeState(), new MonitoredServerRegistryState());
            host.ConfigurePipeline(app, networkMode: false, networkListenIp: null, allowedCidr: IPNetwork.Parse("127.0.0.1/32"), bearerToken: "alert-source-token");
            await app.StartAsync(ct);
            using var server = app.GetTestServer();

            var body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"rotate_alert_source\",\"arguments\":{\"name\":\"" + source + "\",\"confirm\":true}}}";
            var context = await server.SendAsync(c =>
            {
                c.Request.Method = "POST";
                c.Request.Path = "/";
                c.Request.Headers.Host = "localhost";
                c.Request.Headers.Accept = "application/json, text/event-stream";
                c.Request.ContentType = "application/json";
                var bytes = Encoding.UTF8.GetBytes(body);
                c.Request.Body = new System.IO.MemoryStream(bytes);
                c.Request.ContentLength = bytes.Length;
                c.Connection.RemoteIpAddress = IPAddress.Loopback;
            });
            using var reader = new System.IO.StreamReader(context.Response.Body);
            var answer = await reader.ReadToEndAsync(ct);

            Assert.Contains("rotated", answer, StringComparison.Ordinal);
            Assert.NotEqual(verifier, await ScalarAsync(owner, $"SELECT rolpassword FROM pg_authid WHERE rolname = '{login}'", ct));

            var kept = 0;
            while (slowReads.TryRead(out var record))
            {
                if (record is null || record.Route != "rotate_alert_source")
                {
                    continue;
                }

                kept++;
                Assert.DoesNotContain("SCRAM-SHA-256", record.StatementsJson, StringComparison.Ordinal);
                Assert.DoesNotMatch(@"(?i)password\s*'", record.StatementsJson);
                /* The server admin tool's statements are not captured at all, so no label is kept whatever it says. */
                Assert.Empty(JsonNode.Parse(record.StatementsJson)!.AsArray());
            }

            Assert.True(kept >= 1, "the call was kept in the slow-read log");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(
                    ["admin", "viewer", "web", "mcp", login, DarlingManagedRoles.AlertSourceGroupRoleName], _runSuffix, cleanupCt));
        }
    }
}
