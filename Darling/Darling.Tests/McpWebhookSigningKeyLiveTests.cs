/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres. The managed roles it provisions carry fixed,
   cluster-wide names (admin, viewer, mcp), so the class runs in the pg-cluster-roles collection with the other provisioning tests. */

/// <summary>
/// The MCP webhook signing key tools (#6394), as the provisioned NOSUPERUSER <c>mcp</c> login on a managed store: a generated key is
/// sealed, opens with the service's key ring and signs, a second generate rotates the first into the previous slot with the 7 day window,
/// the answer shows the key once and never a stored one, remove clears the key, the previous key and the window, an unknown route or
/// a destination with no webhook URL is refused and writes nothing, and <c>mcp</c> has no UPDATE on a key column.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class McpWebhookSigningKeyLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    private sealed record Store(NpgsqlDataSource Mcp, NpgsqlDataSource Owner, IPasswordKeyRing Ring);

    private static async Task RunAsync(Func<Store, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the MCP signing key checks.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            /* The managed roles carry fixed names and a role is cluster-wide: never adopt or drop a set that belongs to something else. */
            await using var probe = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", owner);
            Assert.SkipWhen((bool)(await probe.ExecuteScalarAsync(ct))!, "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
        }

        var bodySucceeded = false;
        try
        {
            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);
                var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
                var batch = DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName));
                await using var provision = new NpgsqlCommand(batch, owner);
                await provision.ExecuteNonQueryAsync(ct);
            }

            var mcpString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = "mcp",
                Password = ProvisioningTestSecrets.McpPassword,
                SearchPath = PgSchemaGenerator.SearchPath,
                Pooling = false,
            }.ConnectionString;
            await using var mcp = NpgsqlDataSource.Create(mcpString);
            await using var owned = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var key = PasswordPrivateKey.Generate();
            await body(new Store(mcp, owned, DarlingPasswordKey.FromPrivateKey(key)), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand("DROP OWNED BY admin, viewer, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS mcp", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A route whose generic webhook URL is saved sealed for the route's own row, as a route write leaves it.</summary>
    private static async Task<int> SeedRouteWithUrlAsync(Store store, CancellationToken ct)
    {
        var id = Convert.ToInt32(await ScalarAsync(store.Owner, "INSERT INTO config_notification_routes (metric_match) VALUES ('Blocking') RETURNING route_id", ct),
            System.Globalization.CultureInfo.InvariantCulture);
        var sealedUrl = store.Ring.Seal("https://hooks.example.com/route", PasswordBinding.ForWebhook("generic", PasswordBinding.WebhookRouteRow(id), "", null));
        await ExecAsync(store.Owner, $"UPDATE config_notification_routes SET generic_url = '{sealedUrl}' WHERE route_id = {id}", ct);
        return id;
    }

    private static async Task SeedSettingsUrlAsync(Store store, CancellationToken ct)
    {
        var sealedUrl = store.Ring.Seal("https://hooks.example.com/default", PasswordBinding.ForWebhook("generic", PasswordBinding.WebhookSettingsRow, "", null));
        await ExecAsync(store.Owner,
            $"INSERT INTO config_notification (id, generic_url) VALUES (1, '{sealedUrl}') ON CONFLICT (id) DO UPDATE SET generic_url = EXCLUDED.generic_url", ct);
    }

    /// <summary>The route's keys as the service's webhook sender opens them (<see cref="WebhookSigningKeys.LiveKeys"/> is what it signs with).</summary>
    private static async Task<WebhookSigningKeys> OpenedRouteKeysAsync(Store store, int routeId, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(store.Owner.ConnectionString);
        await connection.OpenAsync(ct);
        var routes = await StoreConfigProvider.ReadNotificationRoutesAsync(connection, null, ct);
        return DarlingWebhookSecrets.Open(new WebhooksConfig(), routes, store.Ring).Routes.Single(r => r.RouteId == routeId).GenericSigning!;
    }

    private static async Task<WebhookSigningKeys> OpenedSettingsKeysAsync(Store store, CancellationToken ct)
    {
        await using var command = store.Owner.CreateCommand(
            "SELECT generic_url, generic_proxy, signing_key, previous_signing_key, previous_signing_key_until FROM config_notification WHERE id = 1");
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        var stored = new WebhooksConfig
        {
            GenericUrl = reader.GetString(0),
            GenericProxy = reader.GetString(1),
            GenericSigning = new WebhookSigningKeys(reader.GetString(2), reader.GetString(3), reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTime>(4).ToUniversalTime()),
        };
        return DarlingWebhookSecrets.Open(stored, Array.Empty<NotificationRoute>(), store.Ring).Webhooks.GenericSigning!;
    }

    /// <summary>The tool as the service runs it: the <c>mcp</c> data source writes, the service's own (owner) data source reads the destination, and the
    /// caller is in process (no HTTP context).</summary>
    private static Task<string> GenerateAsync(Store store, int? routeId, CancellationToken ct, bool replacePendingRotation = false) =>
        DarlingMcpAlertTools.GenerateWebhookSigningKeyAsync(store.Mcp, store.Owner, routeId, replacePendingRotation, null, store.Ring, null, ct);

    private static void AssertSigns(string key)
    {
        Assert.True(WebhookSigning.TryDecodeKey(key, out _));
        var signature = WebhookSigning.Sign(new[] { key }, "msg_1", 1_700_000_000, "{}"u8);
        Assert.StartsWith(WebhookSigning.SignatureVersion + ",", signature, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GenerateOnARoute_StoresASealedKeyTheSenderOpensAndSignsWith_AndShowsItOnce()
    {
        await RunAsync(async (store, ct) =>
        {
            var routeId = await SeedRouteWithUrlAsync(store, ct);
            var first = Parse(await GenerateAsync(store, routeId, ct));
            Assert.Equal("generated", first.GetProperty("status").GetString());
            Assert.True(first.GetProperty("shown_once").GetBoolean());
            Assert.Equal(JsonValueKind.Null, first.GetProperty("previous_key_valid_until").ValueKind);
            var firstKey = first.GetProperty("key").GetString()!;
            Assert.StartsWith(WebhookSigning.KeyPrefix, firstKey, StringComparison.Ordinal);

            var stored = (string)(await ScalarAsync(store.Owner, $"SELECT signing_key FROM config_notification_routes WHERE route_id = {routeId}", ct))!;
            Assert.StartsWith("sealed:v1:", stored, StringComparison.Ordinal);
            Assert.DoesNotContain(firstKey, stored, StringComparison.Ordinal);
            var opened = await OpenedRouteKeysAsync(store, routeId, ct);
            Assert.Equal(firstKey, opened.Key);
            AssertSigns(opened.Key);

            /* A second generate rotates: the first key is the previous one for the window, and no answer carries it. */
            var secondAnswer = await GenerateAsync(store, routeId, ct);
            var second = Parse(secondAnswer);
            var secondKey = second.GetProperty("key").GetString()!;
            Assert.NotEqual(firstKey, secondKey);
            Assert.DoesNotContain(firstKey, secondAnswer, StringComparison.Ordinal);
            var until = second.GetProperty("previous_key_valid_until").GetDateTime().ToUniversalTime();
            var expected = DateTime.UtcNow + WebhookSigning.RotationOverlap;
            Assert.InRange((until - expected).TotalMinutes, -5, 5);
            var rotated = await OpenedRouteKeysAsync(store, routeId, ct);
            Assert.Equal(secondKey, rotated.Key);
            Assert.Equal(firstKey, rotated.PreviousKey);
            Assert.Equal(new[] { secondKey, firstKey }, rotated.LiveKeys(DateTime.UtcNow));
        });
    }

    [Fact]
    public async Task GenerateOnTheDefaultDestination_RotatesTheSettingsRowsKeyTheSameWay()
    {
        await RunAsync(async (store, ct) =>
        {
            await SeedSettingsUrlAsync(store, ct);
            var first = Parse(await GenerateAsync(store, null, ct));
            Assert.Equal("generated", first.GetProperty("status").GetString());
            Assert.Equal("default", first.GetProperty("scope").GetString());
            var firstKey = first.GetProperty("key").GetString()!;
            Assert.Equal(firstKey, (await OpenedSettingsKeysAsync(store, ct)).Key);

            var second = Parse(await GenerateAsync(store, null, ct));
            var secondKey = second.GetProperty("key").GetString()!;
            var opened = await OpenedSettingsKeysAsync(store, ct);
            Assert.Equal(secondKey, opened.Key);
            Assert.Equal(firstKey, opened.PreviousKey);
            Assert.True(opened.PreviousUntilUtc > DateTime.UtcNow.AddDays(WebhookSigning.RotationOverlapDays - 1));
            AssertSigns(opened.Key);
        });
    }

    [Fact]
    public async Task Remove_ClearsTheKeyThePreviousKeyAndTheWindow_AndSaysWhetherAKeyWasThere()
    {
        await RunAsync(async (store, ct) =>
        {
            var routeId = await SeedRouteWithUrlAsync(store, ct);
            await GenerateAsync(store, routeId, ct);
            await GenerateAsync(store, routeId, ct);

            var removed = Parse(await DarlingMcpAlertTools.RemoveWebhookSigningKeyAsync(store.Mcp, routeId, null, ct));
            Assert.Equal("removed", removed.GetProperty("status").GetString());
            Assert.True(removed.GetProperty("key_was_present").GetBoolean());
            await using var command = store.Owner.CreateCommand(
                $"SELECT signing_key, previous_signing_key, previous_signing_key_until FROM config_notification_routes WHERE route_id = {routeId}");
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal("", reader.GetString(0));
                Assert.Equal("", reader.GetString(1));
                Assert.True(reader.IsDBNull(2));
            }

            var again = Parse(await DarlingMcpAlertTools.RemoveWebhookSigningKeyAsync(store.Mcp, routeId, null, ct));
            Assert.Equal("unchanged", again.GetProperty("status").GetString());
            Assert.False(again.GetProperty("key_was_present").GetBoolean());
        });
    }

    [Fact]
    public async Task AnUnknownRoute_OrADestinationWithNoWebhookUrl_IsRefused_AndWritesNothing()
    {
        await RunAsync(async (store, ct) =>
        {
            Assert.Equal("not_found", Parse(await GenerateAsync(store, 987654, ct)).GetProperty("status").GetString());
            Assert.Equal("not_found", Parse(await DarlingMcpAlertTools.RemoveWebhookSigningKeyAsync(store.Mcp, 987654, null, ct)).GetProperty("status").GetString());

            var bare = Convert.ToInt32(await ScalarAsync(store.Owner, "INSERT INTO config_notification_routes (metric_match) VALUES ('Blocking') RETURNING route_id", ct),
                System.Globalization.CultureInfo.InvariantCulture);
            await ExecAsync(store.Owner, "INSERT INTO config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            Assert.Equal("invalid", Parse(await GenerateAsync(store, bare, ct)).GetProperty("status").GetString());
            Assert.Equal("invalid", Parse(await GenerateAsync(store, null, ct)).GetProperty("status").GetString());
            Assert.Equal("invalid", Parse(await DarlingMcpAlertTools.RemoveWebhookSigningKeyAsync(store.Mcp, bare, null, ct)).GetProperty("status").GetString());
            Assert.Equal("invalid", Parse(await GenerateAsync(store, 0, ct)).GetProperty("status").GetString());

            Assert.Equal(0L, await ScalarAsync(store.Owner, "SELECT count(*) FROM config_notification_routes WHERE signing_key <> '' OR previous_signing_key <> ''", ct));
            Assert.Equal(0L, await ScalarAsync(store.Owner, "SELECT count(*) FROM config_notification WHERE signing_key <> ''", ct));
        });
    }

    [Fact]
    public async Task TheMcpLoginHasNoUpdateOnAKeyColumn_AndTheFunctionStoresOnlySealedText()
    {
        await RunAsync(async (store, ct) =>
        {
            var routeId = await SeedRouteWithUrlAsync(store, ct);
            foreach (var sql in new[]
            {
                "UPDATE config_notification SET signing_key = 'x' WHERE id = 1",
                "UPDATE config_notification SET previous_signing_key = 'x' WHERE id = 1",
                "UPDATE config_notification SET previous_signing_key_until = now() WHERE id = 1",
                $"UPDATE config_notification_routes SET signing_key = 'x' WHERE route_id = {routeId}",
                $"UPDATE config_notification_routes SET previous_signing_key_until = now() WHERE route_id = {routeId}",
                "SELECT signing_key FROM config_notification",
            })
            {
                var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(store.Mcp, sql, ct));
                Assert.Equal("42501", ex.SqlState);
            }

            /* The function takes a key only in the sealed shape: a plain key the caller made itself is refused. */
            await using var command = store.Mcp.CreateCommand("SELECT config.set_webhook_signing_key($1, $2, $3, $4)");
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = WebhookSigning.NewKey() });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "x" });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "" });
            var refused = await Assert.ThrowsAsync<PostgresException>(() => command.ExecuteScalarAsync(ct));
            Assert.Equal("DL400", refused.SqlState);
        });
    }

    [Fact]
    public async Task ASecondGenerateInsideTheChangeover_IsRefusedUnlessTheCallerAsksToReplaceIt()
    {
        await RunAsync(async (store, ct) =>
        {
            var routeId = await SeedRouteWithUrlAsync(store, ct);
            /* The first generate has no key to rotate and the second starts the 7 day changeover; neither is refused. */
            var firstKey = Parse(await GenerateAsync(store, routeId, ct)).GetProperty("key").GetString()!;
            var secondKey = Parse(await GenerateAsync(store, routeId, ct)).GetProperty("key").GetString()!;

            /* A third inside the changeover would stop the first key the receiver may still hold: refused, and nothing moved. */
            var again = Parse(await GenerateAsync(store, routeId, ct));
            Assert.Equal("refused", again.GetProperty("status").GetString());
            Assert.Contains("replace_pending_rotation: true", again.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.False(again.TryGetProperty("key", out _));
            var kept = await OpenedRouteKeysAsync(store, routeId, ct);
            Assert.Equal(secondKey, kept.Key);
            Assert.Equal(firstKey, kept.PreviousKey);

            var replaced = Parse(await GenerateAsync(store, routeId, ct, replacePendingRotation: true));
            Assert.Equal("generated", replaced.GetProperty("status").GetString());
            var rotated = await OpenedRouteKeysAsync(store, routeId, ct);
            Assert.Equal(replaced.GetProperty("key").GetString(), rotated.Key);
            Assert.Equal(secondKey, rotated.PreviousKey);

            /* The default destination's row gets the same refusal, and an expired window is no longer pending. */
            await SeedSettingsUrlAsync(store, ct);
            Assert.Equal("generated", Parse(await GenerateAsync(store, null, ct)).GetProperty("status").GetString());
            Assert.Equal("generated", Parse(await GenerateAsync(store, null, ct)).GetProperty("status").GetString());
            Assert.Equal("refused", Parse(await GenerateAsync(store, null, ct)).GetProperty("status").GetString());
            await ExecAsync(store.Owner, "UPDATE config_notification SET previous_signing_key_until = now() - interval '1 hour' WHERE id = 1", ct);
            Assert.Equal("generated", Parse(await GenerateAsync(store, null, ct)).GetProperty("status").GetString());
        });
    }

    [Fact]
    public async Task AnUnsealedSavedUrl_IsRefusedWithTheSaveItFirstSentence_AndWritesNothing()
    {
        await RunAsync(async (store, ct) =>
        {
            /* A URL saved before sealing (v3.10.0 and earlier, never re-saved) is plain text in the column; the service reads it on its own connection and refuses. */
            var routeId = Convert.ToInt32(await ScalarAsync(store.Owner,
                "INSERT INTO config_notification_routes (metric_match, generic_url) VALUES ('Blocking', 'https://hooks.example.com/plain') RETURNING route_id", ct),
                System.Globalization.CultureInfo.InvariantCulture);
            await ExecAsync(store.Owner, "INSERT INTO config_notification (id, generic_url) VALUES (1, 'https://hooks.example.com/plain') ON CONFLICT (id) DO UPDATE SET generic_url = EXCLUDED.generic_url", ct);
            foreach (int? target in new int?[] { routeId, null })
            {
                var answer = Parse(await GenerateAsync(store, target, ct));
                Assert.Equal("invalid", answer.GetProperty("status").GetString());
                Assert.Contains("Save the generic webhook URL first", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
            }

            Assert.Equal(0L, await ScalarAsync(store.Owner, "SELECT count(*) FROM config_notification_routes WHERE signing_key <> ''", ct));
            Assert.Equal(0L, await ScalarAsync(store.Owner, "SELECT count(*) FROM config_notification WHERE signing_key <> ''", ct));
        });
    }

    [Fact]
    public async Task TheMcpLoginHasNoFunctionThatReturnsTheDestination_AndAnEarlierBuildsReadFunctionIsDropped()
    {
        await RunAsync(async (store, ct) =>
        {
            /* A store that ran an earlier build of this change holds the read function; the provisioning batch's function block drops it. */
            await ExecAsync(store.Owner, "CREATE FUNCTION config.read_webhook_signing_target(p_route_id integer) RETURNS TABLE (stored_url text) LANGUAGE sql AS 'SELECT generic_url FROM config.config_notification'", ct);
            await ExecAsync(store.Owner, "GRANT EXECUTE ON FUNCTION config.read_webhook_signing_target(integer) TO mcp", ct);
            await ExecAsync(store.Owner, DarlingManagedRoles.BuildWebhookSigningKeyFunctionsSql("config"), ct);
            Assert.Equal(true, await ScalarAsync(store.Owner, "SELECT to_regprocedure('config.read_webhook_signing_target(integer)') IS NULL", ct));
            var ex = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(store.Mcp, "SELECT * FROM config.read_webhook_signing_target(1)", ct));
            Assert.Equal("42883", ex.SqlState);

            /* Generate still works on a route and on the default row without it. */
            var routeId = await SeedRouteWithUrlAsync(store, ct);
            await SeedSettingsUrlAsync(store, ct);
            Assert.Equal("generated", Parse(await GenerateAsync(store, routeId, ct)).GetProperty("status").GetString());
            Assert.Equal("generated", Parse(await GenerateAsync(store, null, ct)).GetProperty("status").GetString());
        });
    }
}
