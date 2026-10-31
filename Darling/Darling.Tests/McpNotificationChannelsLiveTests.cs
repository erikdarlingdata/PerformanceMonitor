/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
/// <c>update_notification_channels</c> (#6392) as the provisioned, NOSUPERUSER <c>mcp</c> role: the SMTP password and the webhook values
/// it sets open through the service's own reader (<see cref="StoreConfigProvider"/> and <see cref="DarlingWebhookSecrets"/>) to the text
/// that was typed, a partial call changes only what it names, a refused call writes nothing, and no answer or undo carries a secret.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class McpNotificationChannelsLiveTests
{
    private const string TeamsUrl = "https://teams.example.test/webhook/teams-secret-token";
    private const string SlackUrl = "https://hooks.example.test/services/T000/B000/slack-secret-token";
    private const string GenericUrl = "https://generic.example.test/in/generic-secret-token";
    private const string Headers = "{\"Authorization\":\"Bearer header-secret-token\"}";
    private const string SmtpPassword = "p@ss word! secret";
    private const string RoutingKey = "pd-routing-key-secret-123";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    private static string Status(string json) => Parse(json).GetProperty("status").GetString()!;

    private static void AssertNoSecret(string answer)
    {
        foreach (var secret in new[] { "secret-token", "header-secret", SmtpPassword, RoutingKey, "sealed:", TeamsUrl, SlackUrl, GenericUrl })
        {
            Assert.DoesNotContain(secret, answer, StringComparison.Ordinal);
        }
    }

    private static async Task<string> TextAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture) ?? "";
    }

    private static DarlingMcpAlertTools.NotificationChannelsRequest Request(
        string? smtpHost = null, int? smtpPort = null, bool? smtpSsl = null, string? smtpUser = null, string? smtpPassword = null, string? smtpFrom = null,
        string? smtpRecipients = null, string? teams = null, string? teamsProxy = null, string? slack = null, string? slackProxy = null,
        string? generic = null, string? genericHeaders = null, string? genericBody = null, string? genericProxy = null,
        string? pagerDuty = null, bool? pagerDutyEu = null, string? pagerDutyProxy = null, bool? pagerDutyAutoResolve = null) =>
        new(smtpHost, smtpPort, smtpSsl, smtpUser, smtpPassword, smtpFrom, smtpRecipients, teams, teamsProxy, slack, slackProxy,
            generic, genericHeaders, genericBody, genericProxy, pagerDuty, pagerDutyEu, pagerDutyProxy, pagerDutyAutoResolve);

    /// <summary>What the service reads and opens on a configuration reload: the SMTP password and each webhook value as plain text.</summary>
    private static async Task<(SmtpConfig Smtp, string? SmtpPlain, WebhooksConfig Webhooks)> ServiceReadAsync(
        StoreConfigProvider provider, IPasswordKeyRing ring, CancellationToken ct)
    {
        var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
        Assert.NotNull(view);
        var opened = DarlingWebhookSecrets.Open(view.Webhooks, Array.Empty<PerformanceMonitor.Notifications.NotificationRoute>(), ring);
        Assert.Empty(opened.Failures);
        var smtp = view.Smtp;
        var plain = string.IsNullOrEmpty(smtp.EncryptedPassword) ? null : ring.Open(smtp.EncryptedPassword, smtp.SecretBinding);
        return (smtp, plain, opened.Webhooks);
    }

    [Fact]
    public async Task TheChannelsTool_SealsEverySecretForTheServiceReader_ChangesOnlyWhatItNames_AndRefusesWithoutWriting()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the MCP notification channel checks.");
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
            var provider = new StoreConfigProvider(owned);
            await provider.SeedIfEmptyAsync(new DarlingConfig(), ct);
            using var key = PasswordPrivateKey.Generate();
            var ring = DarlingPasswordKey.FromPrivateKey(key);

            /* 1. Every channel at once: the service's own reader opens each secret to the text that was typed. */
            var first = await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(
                mcp,
                Request("smtp.example.test", 2525, false, "alerts", SmtpPassword, "alerts@example.test", "ops@example.com, dba@example.org",
                    TeamsUrl, null, SlackUrl, null, GenericUrl, Headers, null, null, RoutingKey, true, null, true),
                ring, null, ct);
            Assert.Equal("updated", Status(first));
            AssertNoSecret(first);
            var firstDoc = Parse(first);
            Assert.Equal("set", firstDoc.GetProperty("secrets").GetProperty("smtp_password").GetProperty("after").GetString());
            Assert.Equal("not set", firstDoc.GetProperty("secrets").GetProperty("smtp_password").GetProperty("before").GetString());
            Assert.Equal("smtp.example.test", firstDoc.GetProperty("settings").GetProperty("smtp_host").GetProperty("after").GetString());
            Assert.Contains("cannot be restored by undo", firstDoc.GetProperty("undo").GetProperty("note").GetString(), StringComparison.Ordinal);

            var read = await ServiceReadAsync(provider, ring, ct);
            Assert.Equal(SmtpPassword, read.SmtpPlain);
            Assert.Equal("alerts", read.Smtp.Username);
            Assert.Equal("smtp.example.test", read.Smtp.Host);
            Assert.Equal(2525, read.Smtp.Port);
            Assert.False(read.Smtp.UseSsl);
            Assert.Equal("ops@example.com, dba@example.org", read.Smtp.To);
            Assert.Equal(TeamsUrl, read.Webhooks.TeamsUrl);
            Assert.Equal(SlackUrl, read.Webhooks.SlackUrl);
            Assert.Equal(GenericUrl, read.Webhooks.GenericUrl);
            Assert.Equal(Headers, read.Webhooks.GenericHeaders);
            Assert.Equal(RoutingKey, read.Webhooks.PagerDutyRoutingKey);
            Assert.True(read.Webhooks.PagerDutyUseEuRegion);
            Assert.True(read.Webhooks.PagerDutyAutoResolve);

            /* 2. A partial call changes only the fields it names: the sealed values are the same bytes afterwards. */
            var sealedBefore = await TextAsync(owned, "SELECT concat_ws('|', smtp_encrypted_password, teams_url, slack_url, generic_url, generic_headers, pagerduty_routing_key) FROM config.config_notification", ct);
            var partial = await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(smtpFrom: "oncall@example.test", pagerDutyEu: false), ring, null, ct);
            Assert.Equal("updated", Status(partial));
            AssertNoSecret(partial);
            Assert.Empty(Parse(partial).GetProperty("secrets").EnumerateObject());
            Assert.Equal(sealedBefore, await TextAsync(owned, "SELECT concat_ws('|', smtp_encrypted_password, teams_url, slack_url, generic_url, generic_headers, pagerduty_routing_key) FROM config.config_notification", ct));
            var afterPartial = await ServiceReadAsync(provider, ring, ct);
            Assert.Equal("oncall@example.test", afterPartial.Smtp.From);
            Assert.False(afterPartial.Webhooks.PagerDutyUseEuRegion);
            Assert.Equal(SmtpPassword, afterPartial.SmtpPlain);
            Assert.Equal("unchanged", Status(await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(smtpFrom: "oncall@example.test"), ring, null, ct)));

            /* 3. A change to what a saved secret is sealed for needs the secret again, and a refusal writes nothing. */
            var everything = "SELECT concat_ws('|', smtp_host, smtp_port, smtp_encrypted_password, teams_url, teams_proxy, slack_url, generic_url, generic_headers, generic_proxy, pagerduty_routing_key, modified_at) FROM config.config_notification";
            var snapshot = await TextAsync(owned, everything, ct);
            var wrongHost = await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(smtpHost: "other.example.test"), ring, null, ct);
            Assert.Equal("refused", Status(wrongHost));
            Assert.Contains(SmtpPasswordSealing.ReenterText, wrongHost, StringComparison.Ordinal);
            Assert.Equal("refused", Status(await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(teamsProxy: "http://proxy.example.test:3128"), ring, null, ct)));
            Assert.Equal("refused", Status(await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(generic: "https://generic2.example.test/in"), ring, null, ct)));
            Assert.Equal("invalid", Status(await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(smtpPassword: "new-password"), ring, null, ct)));
            Assert.Equal(snapshot, await TextAsync(owned, everything, ct));

            /* 4. A URL that cannot be sent to, a bad port, bad recipients and headers that are not JSON are refused and write nothing. */
            foreach (var bad in new[] { "not a url", "ftp://hooks.example.test/x", "http://169.254.169.254/latest/meta-data", "https://user:pw@hooks.example.test/x" })
            {
                var refused = await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(slack: bad), ring, null, ct);
                Assert.Equal("invalid", Status(refused));
                Assert.DoesNotContain("169.254.169.254", refused, StringComparison.Ordinal);
            }

            /* #6392: a proxy written as a link-local or cloud instance address could never be connected to, so it is refused at save with its URL given again. */
            foreach (var badProxy in new[] { "http://169.254.169.254:8080", "169.254.169.254:3128", "http://[fe80::1]:3128", "http://168.63.129.16:3128" })
            {
                var refusedProxy = await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(teams: TeamsUrl, teamsProxy: badProxy), ring, null, ct);
                Assert.Equal("invalid", Status(refusedProxy));
                Assert.DoesNotContain("169.254.169.254", refusedProxy, StringComparison.Ordinal);
            }

            Assert.Equal("invalid", Status(await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(smtpPort: 70000), ring, null, ct)));
            Assert.Equal("invalid", Status(await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(smtpRecipients: "not an address"), ring, null, ct)));
            Assert.Equal("invalid", Status(await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(generic: GenericUrl, genericHeaders: "not json"), ring, null, ct)));
            Assert.Equal(snapshot, await TextAsync(owned, everything, ct));

            /* 5. A ring that cannot seal refuses with the no-key text and writes nothing; a call with no secret in it still works. */
            var noKey = DarlingPasswordKey.Refusing(NotificationRouteWriter.NoKeyText);
            var refusedNoKey = await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(slack: "https://hooks.example.test/other"), noKey, null, ct);
            Assert.Equal("invalid", Status(refusedNoKey));
            Assert.Contains(NotificationRouteWriter.NoKeyText, refusedNoKey, StringComparison.Ordinal);
            var refusedNoKeyPassword = await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(smtpUser: "alerts", smtpPassword: "x"), noKey, null, ct);
            Assert.Equal("invalid", Status(refusedNoKeyPassword));
            Assert.Equal(snapshot, await TextAsync(owned, everything, ct));

            /* 6. A secret and its proxy change together; a clear removes a value; the generic URL and headers are replaced as a pair. */
            var together = await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(
                mcp, Request(teams: TeamsUrl, teamsProxy: "http://proxy.example.test:3128", slack: "-", smtpHost: "mail2.example.test", smtpUser: "alerts2", smtpPassword: SmtpPassword,
                    generic: "https://generic2.example.test/in/generic-secret-token", genericHeaders: Headers),
                ring, null, ct);
            Assert.Equal("updated", Status(together));
            AssertNoSecret(together);
            var afterTogether = await ServiceReadAsync(provider, ring, ct);
            Assert.Equal(TeamsUrl, afterTogether.Webhooks.TeamsUrl);
            Assert.Equal("http://proxy.example.test:3128", afterTogether.Webhooks.TeamsProxy);
            Assert.Equal("", afterTogether.Webhooks.SlackUrl);
            Assert.Equal(SmtpPassword, afterTogether.SmtpPlain);
            Assert.Equal("alerts2", afterTogether.Smtp.Username);
            Assert.Equal("https://generic2.example.test/in/generic-secret-token", afterTogether.Webhooks.GenericUrl);
            Assert.Equal(Headers, afterTogether.Webhooks.GenericHeaders);
            Assert.Contains("not set", Parse(together).GetProperty("secrets").GetProperty("slack_webhook_url").GetProperty("after").GetString(), StringComparison.Ordinal);

            /* 7. The role itself cannot read or write a secret column or a column it was not granted, and the function stores only sealed text or empty. */
            foreach (var statement in new[]
            {
                "UPDATE config.config_notification SET teams_url = ''",
                "UPDATE config.config_notification SET smtp_encrypted_password = NULL",
                "UPDATE config.config_notification SET smtp_host = 'x'",
                "SELECT teams_url FROM config.config_notification",
                "SELECT smtp_encrypted_password FROM config.config_notification",
            })
            {
                await using var command = mcp.CreateCommand(statement);
                var denied = await Assert.ThrowsAsync<PostgresException>(() => command.ExecuteNonQueryAsync(ct));
                Assert.Equal("42501", denied.SqlState);
            }

            await using (var plain = mcp.CreateCommand(
                "SELECT * FROM config.set_notification_channels(ARRAY['teams_url'], NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'https://teams.example.test/plain', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"))
            {
                var refusedPlain = await Assert.ThrowsAsync<PostgresException>(() => plain.ExecuteNonQueryAsync(ct));
                Assert.Equal("DL400", refusedPlain.SqlState);
            }

            /* 8. #6392: a route's own value is sealed for its channel's proxy, so a proxy change is refused while any route holds a value for that
                  channel, and writes nothing; a route holding only another channel's value does not block it, and a proxy change with no route value works. */
            await using (var seed = owned.CreateCommand("INSERT INTO config.config_notification_routes (metric_match, teams_url) VALUES ('Blocking', 'https://teams.example.test/route-value')"))
            {
                await seed.ExecuteNonQueryAsync(ct);
            }

            var beforeRoute = await TextAsync(owned, everything, ct);
            var blocked = await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(teams: TeamsUrl, teamsProxy: "http://proxy2.example.test:3128"), ring, null, ct);
            Assert.Equal("refused", Status(blocked));
            Assert.Contains("route holds its own value", blocked, StringComparison.Ordinal);
            Assert.Equal(beforeRoute, await TextAsync(owned, everything, ct));
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(slack: SlackUrl, slackProxy: "http://proxy2.example.test:3128"), ring, null, ct)));

            await using (var clear = owned.CreateCommand("UPDATE config.config_notification_routes SET teams_url = '', slack_url = ''"))
            {
                await clear.ExecuteNonQueryAsync(ct);
            }

            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(mcp, Request(teams: TeamsUrl, teamsProxy: "http://proxy2.example.test:3128"), ring, null, ct)));

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
}
