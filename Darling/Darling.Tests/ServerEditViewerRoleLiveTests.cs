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
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The web edit routes (#5240) end to end against a store, on a pool connected as a role holding exactly the grants the
/// managed <c>viewer</c> role gets (taken from <see cref="DarlingManagedRoles.BuildProvisioningSql"/>, so removing the
/// column-level UPDATE makes the edit fail with 42501): an edit succeeds, a stale token is a 409 carrying
/// <c>current</c>, an edit while an add holds the shared slot is a 429, and the by-id read returns no secret. Edits here
/// change the name and cost only, which need no probe.
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class ServerEditViewerRoleLiveTests : IDisposable
{
    private readonly DarlingOwnedSet _ownedBefore = DarlingOwnedSecrets.Current;

    public ServerEditViewerRoleLiveTests() => DarlingOwnedSecrets.Set(DarlingOwnedSet.Empty);

    public void Dispose() => DarlingOwnedSecrets.Set(_ownedBefore);

    private const string StoredBlob = "stored-blob-must-never-be-returned-Q7";

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture)!;
    }

    private static async Task<(WebApplication App, HttpClient Client)> HostAsync(
        NpgsqlDataSource viewer, Func<string, Task<string>>? add, CancellationToken ct)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var app = builder.Build();
        app.Use(async (context, next) =>
        {
            context.Items[DarlingWebSeat.HttpContextItemKey] = new DarlingWebSeat("alice", true);
            await next(context);
        });
        DarlingWebEndpoints.MapServers(app, viewer, new CapturingTestLogger(), add);
        await app.StartAsync(ct);
        return (app, app.GetTestClient());
    }

    private static async Task<(HttpStatusCode Status, string Body)> PatchAsync(HttpClient client, int id, string body, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Patch, "/api/servers/" + id)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        };
        using var response = await client.SendAsync(request, ct);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(ct));
    }

    [Fact]
    public async Task AsTheViewerRole_TheWebEdit_Succeeds_KeepsTheStoredSecret_AndAStaleTokenIsA409WithTheCurrentValues()
    {
        var ct = TestContext.Current.CancellationToken;
        var roleName = "srv_web_" + Guid.NewGuid().ToString("N")[..8];
        var (scratch, owner, ownerString) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = owner;
        await using var viewer = await ServerAddViewerRoleLiveTests.ProvisionAsync(owner, ownerString, roleName, null, ct);
        var ok = false;
        try
        {
            await ExecAsync(owner, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await ExecAsync(owner, $"INSERT INTO config_monitored_servers (server_id, name, host, auth, username, encrypted_password, monthly_cost_usd) VALUES (5201, 'alpha', 'alpha.example.test', 'sql', 'monitor', '{StoredBlob}', 3)", ct);
            var (app, client) = await HostAsync(viewer, null, ct);
            await using var __app = app;
            using var ___client = client;

            /* The read: editable non-secret values plus the opaque token, and no secret anywhere in it. */
            var read = await client.GetAsync("/api/admin/servers/5201", ct);
            var readBody = await read.Content.ReadAsStringAsync(ct);
            Assert.Equal(HttpStatusCode.OK, read.StatusCode);
            Assert.DoesNotContain(StoredBlob, readBody, StringComparison.Ordinal);
            Assert.DoesNotContain("encrypted", readBody, StringComparison.OrdinalIgnoreCase);
            var token = JsonNode.Parse(readBody)!["modified_at"]!.GetValue<string>();
            Assert.Equal("alpha", JsonNode.Parse(readBody)!["display_name"]!.GetValue<string>());
            Assert.Equal((HttpStatusCode.NotFound, true), ((await client.GetAsync("/api/admin/servers/999999", ct)).StatusCode, true));

            var versionBefore = await ScalarAsync(owner, "SELECT config_version FROM config_service", ct);

            /* The edit, as viewer: the SET list names only granted columns, FOR UPDATE works, the beacon fires. */
            var (status, body) = await PatchAsync(client, 5201, "{\"display_name\":\"Orders\",\"monthly_cost_usd\":120,\"expected_modified_at\":\"" + token + "\"}", ct);
            Assert.True(status == HttpStatusCode.OK, "edit answered " + (int)status + ": " + body);
            var answer = JsonNode.Parse(body)!;
            Assert.Equal("updated", answer["status"]!.GetValue<string>());
            Assert.Equal(5201, answer["server_id"]!.GetValue<int>());
            Assert.NotEqual(token, answer["modified_at"]!.GetValue<string>());
            Assert.Equal("Orders", await ScalarAsync(owner, "SELECT name FROM config_monitored_servers WHERE server_id = 5201", ct));
            Assert.Equal("120", await ScalarAsync(owner, "SELECT monthly_cost_usd::bigint FROM config_monitored_servers WHERE server_id = 5201", ct));
            Assert.Equal(StoredBlob, await ScalarAsync(owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 5201", ct));
            Assert.Equal(long.Parse(versionBefore, System.Globalization.CultureInfo.InvariantCulture) + 1,
                long.Parse(await ScalarAsync(owner, "SELECT config_version FROM config_service", ct), System.Globalization.CultureInfo.InvariantCulture));
            Assert.DoesNotContain(StoredBlob, body, StringComparison.Ordinal);

            /* The same (now stale) token again: 409 with the current non-secret values, and nothing written. */
            var (staleStatus, staleBody) = await PatchAsync(client, 5201, "{\"display_name\":\"Other\",\"expected_modified_at\":\"" + token + "\"}", ct);
            Assert.Equal(HttpStatusCode.Conflict, staleStatus);
            var stale = JsonNode.Parse(staleBody)!;
            Assert.Equal("conflict", stale["status"]!.GetValue<string>());
            Assert.Equal("Orders", stale["current"]!["display_name"]!.GetValue<string>());
            Assert.Equal(answer["modified_at"]!.GetValue<string>(), stale["current"]!["modified_at"]!.GetValue<string>());
            Assert.DoesNotContain(StoredBlob, staleBody, StringComparison.Ordinal);
            Assert.Equal("Orders", await ScalarAsync(owner, "SELECT name FROM config_monitored_servers WHERE server_id = 5201", ct));

            /* The fresh token works, a stale id is a 404, and an unchanged request writes nothing. */
            var fresh = answer["modified_at"]!.GetValue<string>();
            Assert.Equal(HttpStatusCode.OK, (await PatchAsync(client, 5201, "{\"display_name\":\"Orders\",\"expected_modified_at\":\"" + fresh + "\"}", ct)).Status);
            Assert.Equal(HttpStatusCode.NotFound, (await PatchAsync(client, 999999, "{\"display_name\":\"x\",\"expected_modified_at\":\"" + fresh + "\"}", ct)).Status);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(ok, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    [Fact]
    public async Task AsTheViewerRole_AnEditWhileAnAddHoldsTheSlot_Is429_AndWritesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        var roleName = "srv_slot_" + Guid.NewGuid().ToString("N")[..8];
        var (scratch, owner, ownerString) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = owner;
        await using var viewer = await ServerAddViewerRoleLiveTests.ProvisionAsync(owner, ownerString, roleName, null, ct);
        var ok = false;
        try
        {
            await ExecAsync(owner, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await ExecAsync(owner, "INSERT INTO config_monitored_servers (server_id, name, host) VALUES (5202, 'beta', 'beta.example.test')", ct);
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var (app, client) = await HostAsync(viewer, _ =>
            {
                entered.TrySetResult();
                return release.Task;
            }, ct);
            await using var __app = app;
            using var ___client = client;

            var token = JsonNode.Parse(await client.GetStringAsync("/api/admin/servers/5202", ct))!["modified_at"]!.GetValue<string>();
            var held = client.PostAsync("/api/servers", new StringContent("[{\"host\":\"gamma\"}]", Encoding.UTF8, "application/json"), ct);
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);

            var (status, _) = await PatchAsync(client, 5202, "{\"display_name\":\"Blocked\",\"expected_modified_at\":\"" + token + "\"}", ct);
            Assert.Equal(HttpStatusCode.TooManyRequests, status);
            Assert.Equal("beta", await ScalarAsync(owner, "SELECT name FROM config_monitored_servers WHERE server_id = 5202", ct));

            release.SetResult("{\"requested\":1,\"added\":0,\"skipped\":0,\"collided\":0,\"failed\":0,\"results\":[]}");
            using (await held)
            {
            }

            Assert.Equal(HttpStatusCode.OK, (await PatchAsync(client, 5202, "{\"display_name\":\"Allowed\",\"expected_modified_at\":\"" + token + "\"}", ct)).Status);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(ok, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    [Fact]
    public async Task WithoutTheEditFunction_TheWebEdit_IsA500ThatSaysToRerunTheProvisionScript_AndNothingIsSaved()
    {
        var ct = TestContext.Current.CancellationToken;
        var roleName = "srv_nogr_" + Guid.NewGuid().ToString("N")[..8];
        var (scratch, owner, ownerString) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = owner;
        await using var viewer = await ServerAddViewerRoleLiveTests.ProvisionAsync(owner, ownerString, roleName, ServerAddViewerRoleLiveTests.MissingEditFunction, ct);
        var ok = false;
        try
        {
            await ExecAsync(owner, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await ExecAsync(owner, "INSERT INTO config_monitored_servers (server_id, name, host) VALUES (5203, 'delta', 'delta.example.test')", ct);
            var (app, client) = await HostAsync(viewer, null, ct);
            await using var __app = app;
            using var ___client = client;

            var token = JsonNode.Parse(await client.GetStringAsync("/api/admin/servers/5203", ct))!["modified_at"]!.GetValue<string>();
            var (status, body) = await PatchAsync(client, 5203, "{\"display_name\":\"Nope\",\"expected_modified_at\":\"" + token + "\"}", ct);
            Assert.Equal(HttpStatusCode.InternalServerError, status);
            Assert.Equal(DarlingMcpServerAdminTools.EditStoreNeedsRolesText, JsonNode.Parse(body)!["error"]!.GetValue<string>());
            Assert.Contains("provision-roles.sql", body, StringComparison.Ordinal);
            foreach (var driverText in new[] { "42883", "42501", "edit_monitored_server", "does not exist", "permission denied" })
            {
                Assert.DoesNotContain(driverText, body, StringComparison.OrdinalIgnoreCase);
            }
            Assert.Equal("delta", await ScalarAsync(owner, "SELECT name FROM config_monitored_servers WHERE server_id = 5203", ct));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(ok, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    /// <summary>
    /// The edit core's REAL write with the credential in the SET list, as the viewer role (#5240): <c>UPDATE
    /// config_monitored_servers SET encrypted_password = $3, modified_at = ... WHERE server_id = $1 AND modified_at = $2
    /// RETURNING modified_at</c>. The web edits above change the name and cost only, and the column fact in
    /// <see cref="ServerAddViewerRoleLiveTests"/> runs hand-written statements, so nothing else ran this exact statement under
    /// the viewer's grants: a write to a column viewer can set and cannot read, a predicate and a RETURNING that read only
    /// <c>modified_at</c> (which viewer may read), and the statement-level beacon trigger firing as viewer.
    /// </summary>
    [Fact]
    public async Task AsTheViewerRole_TheEditStoresRealUpdate_WithThePasswordInTheSetList_WritesUnderTheTokenPredicate_AndReturnsTheNewToken()
    {
        var ct = TestContext.Current.CancellationToken;
        var roleName = "srv_pwd_" + Guid.NewGuid().ToString("N")[..8];
        var (scratch, owner, ownerString) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var _ = scratch;
        await using var __ = owner;
        await using var viewer = await ServerAddViewerRoleLiveTests.ProvisionAsync(owner, ownerString, roleName, null, ct);
        var ok = false;
        try
        {
            await ExecAsync(owner, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await ExecAsync(owner, $"INSERT INTO config_monitored_servers (server_id, name, host, auth, username, encrypted_password) VALUES (5204, 'echo', 'echo.example.test', 'sql', 'monitor', '{StoredBlob}')", ct);

            var store = new DarlingMcpServerAdminTools.PostgresServerEditStore(viewer);
            var row = await store.ReadRowAsync(5204, ct);
            Assert.NotNull(row);
            var versionBefore = long.Parse(await ScalarAsync(owner, "SELECT config_version FROM config_service", ct), System.Globalization.CultureInfo.InvariantCulture);

            /* The write: the credential column in the SET list, under the modified_at the edit read. */
            var write = await store.WriteAsync(
                5204, row!.ModifiedAt, [new DarlingMcpServerAdminTools.EditColumnValue("password", "encrypted_password", NpgsqlDbType.Text, "blob-2")], null, null, ct);
            Assert.Equal(DarlingMcpServerAdminTools.ServerEditWriteKind.Written, write.Kind);
            Assert.True(write.ModifiedAt > row.ModifiedAt, "RETURNING modified_at did not move past the token the edit read");

            /* The new blob is stored, the new token is what the row now carries (read as owner and through the by-id read as
               viewer), and the beacon moved exactly once. */
            Assert.Equal("blob-2", await ScalarAsync(owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 5204", ct));
            await using (var stamp = owner.CreateCommand("SELECT modified_at FROM config_monitored_servers WHERE server_id = 5204"))
            {
                Assert.Equal(write.ModifiedAt, (DateTime)(await stamp.ExecuteScalarAsync(ct))!);
            }

            Assert.Equal(write.ModifiedAt, (await store.ReadRowAsync(5204, ct))!.ModifiedAt);
            Assert.Equal(versionBefore + 1, long.Parse(await ScalarAsync(owner, "SELECT config_version FROM config_service", ct), System.Globalization.CultureInfo.InvariantCulture));

            /* The token just spent is stale: the same write is a conflict and changes nothing (the blob, the token, the beacon). */
            var stale = await store.WriteAsync(
                5204, row.ModifiedAt, [new DarlingMcpServerAdminTools.EditColumnValue("password", "encrypted_password", NpgsqlDbType.Text, "blob-3")], null, null, ct);
            Assert.Equal(DarlingMcpServerAdminTools.ServerEditWriteKind.Conflict, stale.Kind);
            Assert.Equal("blob-2", await ScalarAsync(owner, "SELECT encrypted_password FROM config_monitored_servers WHERE server_id = 5204", ct));
            Assert.Equal(versionBefore + 1, long.Parse(await ScalarAsync(owner, "SELECT config_version FROM config_service", ct), System.Globalization.CultureInfo.InvariantCulture));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(ok, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }
}
