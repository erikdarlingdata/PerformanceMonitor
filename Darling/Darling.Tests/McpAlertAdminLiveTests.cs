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
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database and is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6231 part C against a real store: a route made or edited over MCP is the row the Viewer's writer stores (same sealing, the sender
/// opens it), refusals use the Viewer's words, no answer carries a secret, and a dismissed alert leaves the history read and comes
/// back with the undo. The webhook URL is a local address that is never called.
/// </summary>
public sealed class McpAlertAdminLiveTests
{
    private const string Hook = "http://127.0.0.1:18080/hook-secret-token";
    private const string Hook2 = "http://127.0.0.1:18081/second-secret-token";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<ScratchPostgres> MintAsync(CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return scratch;
    }

    private static async Task<IReadOnlyList<NotificationRoute>> OpenedRoutesAsync(string connectionString, IPasswordKeyRing ring, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        var routes = await StoreConfigProvider.ReadNotificationRoutesAsync(connection, null, ct);
        return DarlingWebhookSecrets.Open(new WebhooksConfig(), routes, ring).Routes;
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string Status(string answer) => JsonDocument.Parse(answer).RootElement.GetProperty("status").GetString()!;

    [Fact]
    public async Task ARouteMadeOverMcp_IsTheViewersRow_TheSenderOpens_AndTheAnswerHasNoSecret()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the MCP route checks.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await MintAsync(ct);
        var bodySucceeded = false;
        try
        {
            using var key = PasswordPrivateKey.Generate();
            var ring = DarlingPasswordKey.FromPrivateKey(key);
            DarlingMcpAlertTools.RingForTests.Value = ring;
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            var answer = await DarlingMcpAlertTools.CreateNotificationRoute(source, "Blocking", generic_url: Hook, smtp_recipients: "ops@example.com", cancellationToken: ct);
            Assert.Equal("created", Status(answer));
            Assert.DoesNotContain("hook-secret-token", answer, StringComparison.Ordinal);
            Assert.DoesNotContain("127.0.0.1", answer, StringComparison.Ordinal);
            var mcpId = JsonDocument.Parse(answer).RootElement.GetProperty("route").GetProperty("route_id").GetInt32();

            /* The Viewer's own writer, same inputs. */
            var typed = new NotificationRouteRow { MetricMatch = "Blocking", GenericUrl = Hook, SmtpRecipients = "ops@example.com" };
            var viewerId = await viewer.SaveNotificationRouteSealedAsync(
                typed, null, NotificationRow.Defaults(), new ViewerPasswordSealer(key.PublicKey), null, ct);

            var rows = (await viewer.GetNotificationRoutesAsync(ct)).ToDictionary(r => r.RouteId);
            var (a, b) = (rows[mcpId], rows[viewerId]);
            Assert.True(PasswordSeal.IsSealed(a.GenericUrl) && PasswordSeal.IsSealed(b.GenericUrl));
            Assert.Equal(b.ConfiguredChannels, a.ConfiguredChannels);
            Assert.Equal((b.Enabled, b.ScopeServerId, b.ScopeTagId, b.AgentLevel, b.SendResolutions, b.SmtpRecipients),
                (a.Enabled, a.ScopeServerId, a.ScopeTagId, a.AgentLevel, a.SendResolutions, a.SmtpRecipients));

            var opened = await OpenedRoutesAsync(scratch.ConnectionString, ring, ct);
            Assert.Equal(Hook, opened.Single(r => r.RouteId == mcpId).GenericUrl);
            Assert.Equal(Hook, opened.Single(r => r.RouteId == viewerId).GenericUrl);

            /* An edit takes effect; the answer gives the previous route (masked) and the undo. */
            var edit = await DarlingMcpAlertTools.UpdateNotificationRoute(source, mcpId, metric_match: "Deadlocks", enabled: false, generic_url: Hook2, cancellationToken: ct);
            Assert.Equal("updated", Status(edit));
            foreach (var secret in new[] { "hook-secret-token", "second-secret-token", "127.0.0.1" })
            {
                Assert.DoesNotContain(secret, edit, StringComparison.Ordinal);
            }

            var doc = JsonDocument.Parse(edit).RootElement;
            Assert.Equal("Blocking", doc.GetProperty("previous").GetProperty("metric_match").GetString());
            Assert.Equal("update_notification_route", doc.GetProperty("undo").GetProperty("tool").GetString());
            var after = (await OpenedRoutesAsync(scratch.ConnectionString, ring, ct)).Single(r => r.RouteId == mcpId);
            Assert.Equal(Hook2, after.GenericUrl);
            Assert.Equal("Deadlocks", after.MetricMatch);
            Assert.False(after.Enabled);

            /* A field left out keeps the saved destination. */
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(source, mcpId, enabled: true, cancellationToken: ct)));
            Assert.Equal(Hook2, (await OpenedRoutesAsync(scratch.ConnectionString, ring, ct)).Single(r => r.RouteId == mcpId).GenericUrl);
            bodySucceeded = true;
        }
        finally
        {
            DarlingMcpAlertTools.RingForTests.Value = null;
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task BadFieldsAreRefusedInTheViewersWords_NothingIsWritten_AndAnAgentRouteIsLeftAlone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the MCP route checks.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await MintAsync(ct);
        var bodySucceeded = false;
        try
        {
            using var key = PasswordPrivateKey.Generate();
            DarlingMcpAlertTools.RingForTests.Value = DarlingPasswordKey.FromPrivateKey(key);
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);

            string Message(string answer) => JsonDocument.Parse(answer).RootElement.GetProperty("message").GetString()!;
            var wildcard = await DarlingMcpAlertTools.CreateNotificationRoute(source, "*", generic_url: Hook, cancellationToken: ct);
            Assert.Equal("invalid", Status(wildcard));
            Assert.Equal(ViewerDataService.ValidateNotificationRoute(new NotificationRouteRow { MetricMatch = "*", GenericUrl = Hook }), Message(wildcard));

            var none = await DarlingMcpAlertTools.CreateNotificationRoute(source, "Blocking", cancellationToken: ct);
            Assert.Equal(ViewerDataService.ValidateNotificationRoute(new NotificationRouteRow { MetricMatch = "Blocking" }), Message(none));

            var both = await DarlingMcpAlertTools.CreateNotificationRoute(source, "Blocking", generic_url: Hook, scope_server_id: 1, scope_tag_id: 1, cancellationToken: ct);
            Assert.Equal("invalid", Status(both));

            var fallback = await DarlingMcpAlertTools.CreateNotificationRoute(source, "Blocking", generic_url: Hook, fallback_route_id: 999, cancellationToken: ct);
            Assert.Equal("Fallback route 999 does not exist.", Message(fallback));

            /* No key: a typed secret is refused and nothing is stored. */
            DarlingMcpAlertTools.RingForTests.Value = DarlingPasswordKey.Refusing("no key yet");
            var noKey = await DarlingMcpAlertTools.CreateNotificationRoute(source, "Blocking", generic_url: Hook, cancellationToken: ct);
            Assert.Equal("refused", Status(noKey));
            Assert.DoesNotContain("hook-secret-token", noKey, StringComparison.Ordinal);
            await using (var count = source.CreateCommand("SELECT count(*) FROM config_notification_routes"))
            {
                Assert.Equal(0L, (long)(await count.ExecuteScalarAsync(ct))!);
            }

            /* An agent route is changed in the Viewer, not here. */
            await using var insert = source.CreateCommand("INSERT INTO config_notification_routes (metric_match, smtp_recipients, agent_level) VALUES ('Blocking', 'a@example.com', 1) RETURNING route_id");
            var agentId = Convert.ToInt32(await insert.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
            var agent = await DarlingMcpAlertTools.UpdateNotificationRoute(source, agentId, enabled: false, cancellationToken: ct);
            Assert.Equal("refused", Status(agent));
            bodySucceeded = true;
        }
        finally
        {
            DarlingMcpAlertTools.RingForTests.Value = null;
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task ADismissedAlert_LeavesTheHistoryRead_AndTheUndoBringsItBack()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the MCP dismiss checks.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await MintAsync(ct);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(source, @"INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, dismissed)
VALUES ((now() at time zone 'utc') - interval '1 hour', 7, 's7', 'Blocking', 9, 5, false),
       ((now() at time zone 'utc') - interval '2 hour', 7, 's7', 'Deadlocks', 9, 5, true)", ct);
            var since = DateTime.UtcNow.AddHours(-24);
            async Task<int> VisibleAsync() => (await DarlingAlertReader.GetAlertHistoryPageAsync(source, since, DateTime.UtcNow.AddMinutes(1), null, 50, false, cancellationToken: ct)).Count;
            Assert.Equal(1, await VisibleAsync());

            var all = (await DarlingAlertReader.GetAlertHistoryPageAsync(source, since, DateTime.UtcNow.AddMinutes(1), null, 50, true, cancellationToken: ct))
                .Select(r => new { alert_time = r.AlertTime.ToString("o"), server_id = r.ServerId, metric_name = r.MetricName }).ToList();
            var json = JsonSerializer.Serialize(all);

            var answer = await DarlingMcpAlertTools.DismissAlertHistory(source, json, ct);
            var doc = JsonDocument.Parse(answer).RootElement;
            Assert.Equal(1, doc.GetProperty("dismissed").GetInt32());
            Assert.Equal(1, doc.GetProperty("already_dismissed").GetInt32());
            Assert.Equal(0, await VisibleAsync());

            /* The undo names only the row this call dismissed, so the earlier dismissal stays. */
            var undo = doc.GetProperty("undo").GetProperty("arguments").GetProperty("alerts_json").GetString()!;
            Assert.Single(JsonDocument.Parse(undo).RootElement.EnumerateArray());
            var restored = await DarlingMcpAlertTools.RestoreAlertHistory(source, undo, ct);
            Assert.Equal(1, JsonDocument.Parse(restored).RootElement.GetProperty("restored").GetInt32());
            Assert.Equal(1, await VisibleAsync());

            Assert.Equal("invalid", Status(await DarlingMcpAlertTools.DismissAlertHistory(source, "not json", ct)));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Theory]
    [InlineData("create_notification_route")]
    [InlineData("update_notification_route")]
    [InlineData("dismiss_alert_history")]
    [InlineData("restore_alert_history")]
    public void TheNewWriteTools_NeedAllowWrites_AndAnInvestigatingAgentNeverGetsThem(string tool)
    {
        Assert.False(DarlingMcpWritePolicy.Effective.Off.Offers(tool));
        Assert.True(new DarlingMcpWritePolicy.Effective(true, false).Offers(tool));
        Assert.False(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true).Offers(tool));
        Assert.False(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true).Offers(tool, new HashSet<string> { tool }));
    }
}
