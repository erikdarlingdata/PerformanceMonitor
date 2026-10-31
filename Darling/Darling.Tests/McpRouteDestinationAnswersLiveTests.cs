/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres. The managed roles it provisions carry fixed,
   cluster-wide names (admin, viewer, mcp), so the class runs in the pg-cluster-roles collection with the other provisioning tests. */

/// <summary>
/// What the route destination function returns and checks, and what the MCP route tools answer (#6231), as the provisioned,
/// NOSUPERUSER <c>mcp</c> login: the function hands back no stored value, it refuses a webhook value that is not exactly the sealed
/// shape and a recipient list the senders refuse, the tools show a host for Teams and Slack only and the recipient list in full, an
/// oversized value is answered as invalid, and a generic URL with a signing key cannot be changed.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class McpRouteDestinationAnswersLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    private static string Status(string json) => Parse(json).GetProperty("status").GetString()!;

    private sealed record Store(NpgsqlDataSource Mcp, NpgsqlDataSource Owner, IPasswordKeyRing Ring);

    private static readonly DarlingMcpAlertTools.RouteDestinationRequest NoDestinations = new(null, null, null, null, null);

    /// <summary>Runs <paramref name="body"/> on a scratch store with the managed roles provisioned, as the owner and as the mcp login.</summary>
    private static async Task RunAsync(Func<Store, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the MCP route destination checks.");
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
                await using var drop = new NpgsqlCommand($"{LiveCleanupBatch.DropOwnedByExistingProductRolesSql} DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task<int> SeedRouteAsync(NpgsqlDataSource owner, string columns, string values, CancellationToken ct)
    {
        await using var command = owner.CreateCommand(
            $"INSERT INTO config_notification_routes (metric_match, {columns}) VALUES ('Blocking', {values}) RETURNING route_id");
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<string> TextAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    private static string SealedFor(IPasswordKeyRing ring, string slot, int routeId, string plaintext) =>
        ring.Seal(plaintext, PasswordBinding.ForWebhook(slot, PasswordBinding.WebhookRouteRow(routeId), "", null));

    /// <summary>Calls the function as the mcp login; returns the row it answered (column values by position) or the SQLSTATE it refused with.</summary>
    private static async Task<(object?[]? Row, string? SqlState)> CallAsync(
        NpgsqlDataSource mcp, int routeId, string[] columns, string teams, string slack, string generic, string key, string recipients, CancellationToken ct)
    {
        await using var command = mcp.CreateCommand("SELECT * FROM config.set_notification_route_destinations($1, NULL, $2, $3, $4, $5, $6, $7)");
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = routeId });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = columns });
        foreach (var text in new[] { teams, slack, generic, key, recipients })
        {
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = text });
        }

        try
        {
            await using var reader = await command.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            var row = new object?[reader.FieldCount];
            for (var i = 0; i < row.Length; i++)
            {
                row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            return (row, null);
        }
        catch (PostgresException ex)
        {
            return (null, ex.SqlState);
        }
    }

    [Fact]
    public async Task TheFunction_ReturnsOnlySavedFlagsForTheColumnsNamed_NeverAStoredValue_LegacyPlaintextIncluded()
    {
        await RunAsync(async (store, ct) =>
        {
            const string LegacySlack = "https://hooks.example.test/services/legacy-plain-token";
            const string LegacyKey = "legacy-plain-routing-key";
            var legacy = await SeedRouteAsync(store.Owner, "slack_url, pagerduty_routing_key, smtp_recipients", $"'{LegacySlack}', '{LegacyKey}', 'ops@example.com'", ct);

            /* Naming only smtp_recipients: nothing about the other columns comes back, and no column is text. */
            var (row, state) = await CallAsync(store.Mcp, legacy, ["smtp_recipients"], "", "", "", "", "ops@example.com", ct);
            Assert.Null(state);
            Assert.Equal(5, row!.Length);
            Assert.Null(row[0]);
            Assert.Null(row[1]);
            Assert.Null(row[2]);
            Assert.Null(row[3]);
            Assert.Equal(true, row[4]);
            Assert.DoesNotContain(row, v => v is string);
            Assert.DoesNotContain(row, v => v?.ToString()?.Contains("legacy-plain", StringComparison.Ordinal) == true);

            /* Naming a webhook column says whether one was saved, and still never what it was. */
            var (second, secondState) = await CallAsync(store.Mcp, legacy, ["slack_url", "pagerduty_routing_key", "generic_url"], "", "", "", "", "", ct);
            Assert.Null(secondState);
            Assert.Equal(new object?[] { null, true, false, true, null }, second);
            Assert.Equal("", await TextAsync(store.Owner, $"SELECT slack_url FROM config_notification_routes WHERE route_id = {legacy}", ct));
        });
    }

    [Fact]
    public async Task AToolAnswer_ForAPreviousLegacyWebhook_SaysSaved_AndShowsNoHost()
    {
        await RunAsync(async (store, ct) =>
        {
            var legacy = await SeedRouteAsync(
                store.Owner, "slack_url, generic_url", "'https://hooks.example.test/services/legacy-plain-token', 'https://secret-endpoint.m.example.test/in'", ct);
            var answer = await DarlingMcpAlertTools.UpdateNotificationRouteAsync(
                store.Mcp, legacy, null, null, null, null, null, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(null, "https://hooks.example.test/services/new-token", "https://other-endpoint.m.example.test/in", null, null),
                store.Ring, null, ct);
            Assert.Equal("updated", Status(answer));
            Assert.DoesNotContain("legacy-plain-token", answer, StringComparison.Ordinal);
            Assert.DoesNotContain("secret-endpoint", answer, StringComparison.Ordinal);
            Assert.DoesNotContain("other-endpoint", answer, StringComparison.Ordinal);

            var destinations = Parse(answer).GetProperty("destinations");
            Assert.Equal("slack webhook saved", destinations.GetProperty("previous").GetProperty("slack").GetString());
            Assert.Equal("generic webhook saved", destinations.GetProperty("previous").GetProperty("generic").GetString());
            Assert.Equal("slack webhook at hooks.example.test", destinations.GetProperty("now").GetProperty("slack").GetString());
            Assert.Equal("generic webhook saved", destinations.GetProperty("now").GetProperty("generic").GetString());
        });
    }

    [Fact]
    public async Task AnAnswer_ShowsTheHostForTeamsAndSlackOnly_AndPagerDutyAsSaved()
    {
        await RunAsync(async (store, ct) =>
        {
            var created = await DarlingMcpAlertTools.CreateNotificationRouteAsync(
                store.Mcp, "Blocking", true, null, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(
                    "https://teams.example.test/webhook/t", "https://hooks.example.test/s", "https://abc123.m.example.test/g", "routing-key-value", null),
                store.Ring, null, ct);
            Assert.Equal("created", Status(created));
            Assert.DoesNotContain("abc123", created, StringComparison.Ordinal);
            Assert.DoesNotContain("routing-key-value", created, StringComparison.Ordinal);
            var now = Parse(created).GetProperty("destinations").GetProperty("now");
            Assert.Equal("teams webhook at teams.example.test", now.GetProperty("teams").GetString());
            Assert.Equal("slack webhook at hooks.example.test", now.GetProperty("slack").GetString());
            Assert.Equal("generic webhook saved", now.GetProperty("generic").GetString());
            Assert.Equal("routing key saved", now.GetProperty("pagerduty").GetString());
        });
    }

    [Fact]
    public async Task EveryWriteAnswer_ShowsTheFullRecipientList_SoAnOverwriteCanBeUndone()
    {
        await RunAsync(async (store, ct) =>
        {
            var created = await DarlingMcpAlertTools.CreateNotificationRouteAsync(
                store.Mcp, "Blocking", true, null, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(null, null, null, null, "ops@example.com, dba@example.org"), store.Ring, null, ct);
            var createdDoc = Parse(created);
            var routeId = createdDoc.GetProperty("route").GetProperty("route_id").GetInt32();
            Assert.Equal("ops@example.com, dba@example.org", createdDoc.GetProperty("route").GetProperty("smtp_recipients").GetString());
            Assert.Equal("ops@example.com, dba@example.org", createdDoc.GetProperty("destinations").GetProperty("now").GetProperty("email").GetString());

            var overwritten = Parse(await DarlingMcpAlertTools.UpdateNotificationRouteAsync(
                store.Mcp, routeId, null, null, null, null, null, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(null, null, null, null, "new@example.com"), store.Ring, null, ct));
            Assert.Equal("updated", overwritten.GetProperty("status").GetString());
            Assert.Equal("new@example.com", overwritten.GetProperty("route").GetProperty("smtp_recipients").GetString());
            Assert.Equal("ops@example.com, dba@example.org", overwritten.GetProperty("previous").GetProperty("smtp_recipients").GetString());
            Assert.Equal("ops@example.com, dba@example.org", overwritten.GetProperty("destinations").GetProperty("previous").GetProperty("email").GetString());
            var undo = overwritten.GetProperty("undo").GetProperty("arguments");
            Assert.Equal("ops@example.com, dba@example.org", undo.GetProperty("email_recipients").GetString());

            /* An undo that follows the answer's arguments puts the list back. */
            var restored = Parse(await DarlingMcpAlertTools.UpdateNotificationRouteAsync(
                store.Mcp, routeId, null, null, null, null, null, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(null, null, null, null, undo.GetProperty("email_recipients").GetString()), store.Ring, null, ct));
            Assert.Equal("ops@example.com, dba@example.org", restored.GetProperty("route").GetProperty("smtp_recipients").GetString());

            /* Removing the list names it in the undo, and a list that was empty is undone with the remove marker. */
            var removed = Parse(await DarlingMcpAlertTools.UpdateNotificationRouteAsync(
                store.Mcp, routeId, null, null, null, null, null, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(null, null, null, null, "-"), store.Ring, null, ct));
            Assert.Equal("none", removed.GetProperty("destinations").GetProperty("now").GetProperty("email").GetString());
            Assert.Equal("ops@example.com, dba@example.org", removed.GetProperty("undo").GetProperty("arguments").GetProperty("email_recipients").GetString());
            var fromEmpty = Parse(await DarlingMcpAlertTools.UpdateNotificationRouteAsync(
                store.Mcp, routeId, null, null, null, null, null, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(null, null, null, null, "again@example.com"), store.Ring, null, ct));
            Assert.Equal("-", fromEmpty.GetProperty("undo").GetProperty("arguments").GetProperty("email_recipients").GetString());

            /* The enable and fallback answers carry the list too. */
            var disabled = Parse(await DarlingMcpAlertTools.SetNotificationRouteEnabled(store.Mcp, routeId, false));
            Assert.Equal("updated", disabled.GetProperty("status").GetString());
            Assert.Equal("again@example.com", disabled.GetProperty("route").GetProperty("smtp_recipients").GetString());
        });
    }

    [Fact]
    public async Task AnOversizedValue_IsAnsweredInvalid_AndAFullSizeValueFitsTheFunctionsBound()
    {
        await RunAsync(async (store, ct) =>
        {
            var before = Convert.ToInt64(await TextAsync(store.Owner, "SELECT count(*)::text FROM config_notification_routes", ct), System.Globalization.CultureInfo.InvariantCulture);
            var tooLong = await DarlingMcpAlertTools.CreateNotificationRouteAsync(
                store.Mcp, "Blocking", true, null, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(null, null, null, new string('k', PasswordSeal.MaxPlaintextBytes + 1), null), store.Ring, null, ct);
            Assert.Equal("invalid", Status(tooLong));
            Assert.Contains("longer than can be stored", tooLong, StringComparison.Ordinal);
            Assert.Equal(before, Convert.ToInt64(await TextAsync(store.Owner, "SELECT count(*)::text FROM config_notification_routes", ct), System.Globalization.CultureInfo.InvariantCulture));

            var longest = await DarlingMcpAlertTools.CreateNotificationRouteAsync(
                store.Mcp, "Blocking", true, null, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(null, null, null, new string('k', PasswordSeal.MaxPlaintextBytes), null), store.Ring, null, ct);
            Assert.Equal("created", Status(longest));
            var routeId = Parse(longest).GetProperty("route").GetProperty("route_id").GetInt32();
            var length = Convert.ToInt32(await TextAsync(store.Owner, $"SELECT length(pagerduty_routing_key)::text FROM config_notification_routes WHERE route_id = {routeId}", ct), System.Globalization.CultureInfo.InvariantCulture);
            Assert.InRange(length, 1, 16384);
        });
    }

    [Fact]
    public async Task TheFunction_RefusesAChangedWebhookValueThatIsNotTheSealedShape_OrIsOverTheSizeBound()
    {
        await RunAsync(async (store, ct) =>
        {
            var routeId = await SeedRouteAsync(store.Owner, "smtp_recipients", "''", ct);
            var padded = "sealed:v1:0123456789abcdef:" + new string('A', 16400);
            foreach (var bad in new[]
            {
                "sealed:x",
                "sealed:v1:short:QUJD",
                "sealed:v1:0123456789abcdef:not base64 !",
                "sealed:v1:0123456789abcdef:",
                "sealed:v2:0123456789abcdef:QUJD",
                padded,
            })
            {
                foreach (var column in new[] { "teams_url", "slack_url", "generic_url", "pagerduty_routing_key" })
                {
                    var (_, state) = await CallAsync(
                        store.Mcp, routeId, [column],
                        column == "teams_url" ? bad : "", column == "slack_url" ? bad : "", column == "generic_url" ? bad : "", column == "pagerduty_routing_key" ? bad : "", "", ct);
                    Assert.Equal("DL400", state);
                }
            }

            Assert.Equal("", await TextAsync(store.Owner, $"SELECT slack_url || generic_url || teams_url || pagerduty_routing_key FROM config_notification_routes WHERE route_id = {routeId}", ct));

            /* A value the service sealed for the route is accepted. */
            var good = SealedFor(store.Ring, "slack", routeId, "https://hooks.example.test/services/x");
            var (row, goodState) = await CallAsync(store.Mcp, routeId, ["slack_url"], "", good, "", "", "", ct);
            Assert.Null(goodState);
            Assert.NotNull(row);
            Assert.Equal(good, await TextAsync(store.Owner, $"SELECT slack_url FROM config_notification_routes WHERE route_id = {routeId}", ct));
        });
    }

    [Fact]
    public async Task TheFunction_RefusesRecipientsThatTheSendersRefuse()
    {
        await RunAsync(async (store, ct) =>
        {
            var routeId = await SeedRouteAsync(store.Owner, "smtp_recipients", "'keep@example.com'", ct);
            var fifty = string.Join(", ", Enumerable.Range(1, 50).Select(i => $"user{i}@example.com"));
            var fiftyOne = fifty + ", extra@example.com";
            foreach (var bad in new[]
            {
                "Ops <ops@example.com>",
                "\"Ops\" ops@example.com",
                "a@example.com\r\nb@example.com",
                "a@example.com\tb@example.com",
                "<ops@example.com>",
                fiftyOne,
            })
            {
                var (_, state) = await CallAsync(store.Mcp, routeId, ["smtp_recipients"], "", "", "", "", bad, ct);
                Assert.Equal("DL400", state);
            }

            Assert.Equal("keep@example.com", await TextAsync(store.Owner, $"SELECT smtp_recipients FROM config_notification_routes WHERE route_id = {routeId}", ct));
            var (row, okState) = await CallAsync(store.Mcp, routeId, ["smtp_recipients"], "", "", "", "", fifty, ct);
            Assert.Null(okState);
            Assert.NotNull(row);
            Assert.Equal(fifty, await TextAsync(store.Owner, $"SELECT smtp_recipients FROM config_notification_routes WHERE route_id = {routeId}", ct));
        });
    }

    [Fact]
    public async Task ASigningKeyOnARoute_KeepsItsGenericUrl_AgainstTheMcpLogin_AndTheTool()
    {
        await RunAsync(async (store, ct) =>
        {
            var created = await DarlingMcpAlertTools.CreateNotificationRouteAsync(
                store.Mcp, "Blocking", true, null, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(null, null, "https://generic.example.test/in/one", null, null), store.Ring, null, ct);
            var routeId = Parse(created).GetProperty("route").GetProperty("route_id").GetInt32();
            await using (var sign = store.Owner.CreateCommand($"UPDATE config_notification_routes SET signing_key = 'sealed:v1:0123456789abcdef:QUJD' WHERE route_id = {routeId}"))
            {
                await sign.ExecuteNonQueryAsync(ct);
            }

            var before = await TextAsync(store.Owner, $"SELECT generic_url FROM config_notification_routes WHERE route_id = {routeId}", ct);
            var replacement = SealedFor(store.Ring, "generic", routeId, "https://generic.example.test/in/two");
            var (_, state) = await CallAsync(store.Mcp, routeId, ["generic_url"], "", "", replacement, "", "", ct);
            Assert.Equal("DL403", state);
            var (_, clearState) = await CallAsync(store.Mcp, routeId, ["generic_url"], "", "", "", "", "", ct);
            Assert.Equal("DL403", clearState);

            var viaTool = await DarlingMcpAlertTools.UpdateNotificationRouteAsync(
                store.Mcp, routeId, null, null, null, null, null, null, null, null,
                new DarlingMcpAlertTools.RouteDestinationRequest(null, null, "https://generic.example.test/in/two", null, null), store.Ring, null, ct);
            Assert.Equal("refused", Status(viaTool));
            Assert.Equal(before, await TextAsync(store.Owner, $"SELECT generic_url FROM config_notification_routes WHERE route_id = {routeId}", ct));
        });
    }
}
