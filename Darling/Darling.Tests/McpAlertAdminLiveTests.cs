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
        /* The route functions are made by the owner, as the managed batch does. */
        await using (var functions = new NpgsqlCommand(DarlingManagedRoles.BuildNotificationRouteFunctionsSql("config"), connection))
        {
            await functions.ExecuteNonQueryAsync(ct);
        }

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
    public async Task ARouteMadeOverMcpWithNoDestination_HasNoneOfItsOwn_AndAnEditThatNamesNoDestinationLeavesTheViewersAlone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the MCP route checks.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await MintAsync(ct);
        var bodySucceeded = false;
        try
        {
            using var key = PasswordPrivateKey.Generate();
            var ring = DarlingPasswordKey.FromPrivateKey(key);
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            /* A route made over MCP has every destination empty: it sends through the channels configured in Settings. */
            var answer = await DarlingMcpAlertTools.CreateNotificationRoute(source, "Blocking", send_resolutions: "always", cancellationToken: ct);
            Assert.Equal("created", Status(answer));
            var created = JsonDocument.Parse(answer).RootElement;
            var mcpId = created.GetProperty("route").GetProperty("route_id").GetInt32();
            Assert.Empty(created.GetProperty("route").GetProperty("configured_channels").EnumerateArray());
            Assert.Contains("next matching route", created.GetProperty("sends_to").GetString(), StringComparison.Ordinal);
            Assert.Contains("Settings", created.GetProperty("sends_to").GetString(), StringComparison.Ordinal);
            Assert.True(created.GetProperty("route").GetProperty("send_resolutions").GetBoolean());

            /* A route the Viewer made, with a sealed destination of its own. */
            var typed = new NotificationRouteRow { MetricMatch = "Deadlocks", GenericUrl = Hook, SmtpRecipients = "ops@example.com" };
            var viewerId = await viewer.SaveNotificationRouteSealedAsync(
                typed, null, NotificationRow.Defaults(), new ViewerPasswordSealer(key.PublicKey), null, ct);

            /* What a route with a destination of its own matches can be changed over MCP: its destinations are MCP's to set too. They stay. */
            var changed = await DarlingMcpAlertTools.UpdateNotificationRoute(
                source, viewerId, metric_match: "Blocking", fleet_wide: true, cancellationToken: ct);
            Assert.Equal("updated", Status(changed));
            Assert.Equal("Blocking", (await OpenedRoutesAsync(scratch.ConnectionString, ring, ct)).Single(r => r.RouteId == viewerId).MetricMatch);
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(source, viewerId, metric_match: "Deadlocks", cancellationToken: ct)));

            /* An edit over MCP still changes the enabled flag and fallback, and never the destinations or the address list. */
            var edit = await DarlingMcpAlertTools.UpdateNotificationRoute(
                source, viewerId, enabled: false, fallback_route_id: mcpId, cancellationToken: ct);
            Assert.Equal("updated", Status(edit));
            foreach (var secret in new[] { "hook-secret-token", "127.0.0.1" })
            {
                Assert.DoesNotContain(secret, edit, StringComparison.Ordinal);
            }

            var doc = JsonDocument.Parse(edit).RootElement;
            Assert.Equal("Deadlocks", doc.GetProperty("previous").GetProperty("metric_match").GetString());
            Assert.Equal("update_notification_route", doc.GetProperty("undo").GetProperty("tool").GetString());
            var after = (await OpenedRoutesAsync(scratch.ConnectionString, ring, ct)).Single(r => r.RouteId == viewerId);
            Assert.Equal(Hook, after.GenericUrl);
            Assert.Equal("ops@example.com", after.SmtpRecipients);
            Assert.Equal("Deadlocks", after.MetricMatch);
            Assert.False(after.Enabled);
            Assert.Equal(mcpId, after.FallbackRouteId);

            /* The undo puts the fields back, and a call that changes nothing says so. */
            var undo = doc.GetProperty("undo").GetProperty("arguments");
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                source, viewerId, enabled: undo.GetProperty("enabled").GetBoolean(), clear_fallback: true, cancellationToken: ct)));
            Assert.Equal("unchanged", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(source, viewerId, enabled: true, cancellationToken: ct)));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task BadFieldsAreRefused_NothingIsWritten_AndAnAgentRouteIsLeftAlone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the MCP route checks.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await MintAsync(ct);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);

            string Message(string answer) => JsonDocument.Parse(answer).RootElement.GetProperty("message").GetString()!;
            var wildcard = await DarlingMcpAlertTools.CreateNotificationRoute(source, "*", cancellationToken: ct);
            Assert.Equal("invalid", Status(wildcard));
            Assert.Equal(ViewerDataService.ValidateNotificationRoute(new NotificationRouteRow { MetricMatch = "*", GenericUrl = Hook }), Message(wildcard));

            Assert.Equal("invalid", Status(await DarlingMcpAlertTools.CreateNotificationRoute(source, "  ", cancellationToken: ct)));
            Assert.Equal("invalid", Status(await DarlingMcpAlertTools.CreateNotificationRoute(source, "Blocking", scope_server_id: 1, scope_tag_id: 1, cancellationToken: ct)));
            Assert.Equal("invalid", Status(await DarlingMcpAlertTools.CreateNotificationRoute(source, "Blocking", scope_server_id: 404, cancellationToken: ct)));
            Assert.Equal("invalid", Status(await DarlingMcpAlertTools.CreateNotificationRoute(source, "Blocking", send_resolutions: "sometimes", cancellationToken: ct)));
            var fallback = await DarlingMcpAlertTools.CreateNotificationRoute(source, "Blocking", fallback_route_id: 999, cancellationToken: ct);
            Assert.Equal("invalid", Status(fallback));
            Assert.Contains("does not exist", Message(fallback), StringComparison.Ordinal);
            await using (var count = source.CreateCommand("SELECT count(*) FROM config_notification_routes"))
            {
                Assert.Equal(0L, (long)(await count.ExecuteScalarAsync(ct))!);
            }

            /* An agent route is changed in the Viewer, not here. */
            await using var insert = source.CreateCommand("INSERT INTO config_notification_routes (metric_match, smtp_recipients, agent_level) VALUES ('Blocking', 'a@example.com', 1) RETURNING route_id");
            var agentId = Convert.ToInt32(await insert.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
            var agent = await DarlingMcpAlertTools.UpdateNotificationRoute(source, agentId, enabled: false, cancellationToken: ct);
            Assert.Equal("refused", Status(agent));
            Assert.Equal("not_found", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(source, 9999, enabled: false, cancellationToken: ct)));
            bodySucceeded = true;
        }
        finally
        {
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

            var answer = await DarlingMcpAlertTools.DismissAlertHistory(source, json, cancellationToken: ct);
            var doc = JsonDocument.Parse(answer).RootElement;
            Assert.Equal(1, doc.GetProperty("dismissed").GetInt32());
            Assert.Equal(1, doc.GetProperty("already_dismissed").GetInt32());
            Assert.Equal(0, await VisibleAsync());

            /* The undo names only the row this call dismissed, so the earlier dismissal stays. */
            var undo = doc.GetProperty("undo").GetProperty("arguments").GetProperty("alerts_json").GetString()!;
            Assert.Single(JsonDocument.Parse(undo).RootElement.EnumerateArray());
            var restored = await DarlingMcpAlertTools.RestoreAlertHistory(source, undo, cancellationToken: ct);
            Assert.Equal(1, JsonDocument.Parse(restored).RootElement.GetProperty("restored").GetInt32());
            Assert.Equal(1, await VisibleAsync());

            Assert.Equal("invalid", Status(await DarlingMcpAlertTools.DismissAlertHistory(source, "not json", cancellationToken: ct)));

            /* #6455: keys that name no row are not_found (never a success word), and rows already in the asked state are unchanged. */
            var noSuchRow = JsonSerializer.Serialize(new[] { new { alert_time = "2001-01-01T00:00:00", server_id = 99, metric_name = "NoSuchMetric" } });
            Assert.Equal("not_found", Status(await DarlingMcpAlertTools.DismissAlertHistory(source, noSuchRow, cancellationToken: ct)));
            Assert.Equal("not_found", Status(await DarlingMcpAlertTools.RestoreAlertHistory(source, noSuchRow, cancellationToken: ct)));
            var visibleRow = JsonSerializer.Serialize(all.Where(r => r.metric_name == "Blocking").ToList());
            Assert.Equal("unchanged", Status(await DarlingMcpAlertTools.RestoreAlertHistory(source, visibleRow, cancellationToken: ct)));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    /// <summary>The mcp role's alert-log grant, lifted from the managed provisioning SQL (the REVOKE and the column GRANT) and given to a
    /// stand-in role, so the test runs the shipped statements and not a copy.</summary>
    private static string McpAlertLogGrantFor(string role)
    {
        var managed = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp, 15);
        var statements = System.Text.RegularExpressions.Regex.Matches(managed, @"(?m)^(?:REVOKE|GRANT) [^;]*? ON config\.config_alert_log (?:TO|FROM) mcp;")
            .Select(m => m.Value.Replace(" mcp;", " " + role + ";", StringComparison.Ordinal)).ToList();
        Assert.Equal(2, statements.Count);
        return string.Join("\n", statements);
    }

    [Fact]
    public async Task DismissAndRestore_RunAsTheMcpRole_SetOnlyTheDismissedColumn()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the MCP dismiss checks.");
        var ct = TestContext.Current.CancellationToken;
        var role = "mcp_dismiss_probe_" + Guid.NewGuid().ToString("N")[..8];
        /* A login role belongs to the whole cluster, so a run that cannot clean up must not leave a published password behind. */
        var rolePassword = Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(16));
        var scratch = await MintAsync(ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
            await ExecAsync(owner, $@"CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD '{rolePassword}';
GRANT USAGE ON SCHEMA config TO {role};
GRANT SELECT ON config.config_alert_log TO {role};
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, dismissed)
VALUES ('2026-10-04 10:00:00', 7, 's7', 'Blocking', 9, 5, false)", ct);

            var roleString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = rolePassword, Pooling = false }.ConnectionString;
            await using var asMcp = NpgsqlDataSource.Create(roleString);
            var rows = JsonSerializer.Serialize(new[] { new { alert_time = "2026-10-04T10:00:00Z", server_id = 7, metric_name = "Blocking" } });

            /* Before the shipped grant the role is refused, so the success below belongs to the grant. */
            Assert.Contains("permission denied", await DarlingMcpAlertTools.DismissAlertHistory(asMcp, rows, cancellationToken: ct), StringComparison.OrdinalIgnoreCase);

            await ExecAsync(owner, McpAlertLogGrantFor(role), ct);

            var dismissed = JsonDocument.Parse(await DarlingMcpAlertTools.DismissAlertHistory(asMcp, rows, cancellationToken: ct)).RootElement;
            Assert.Equal(1, dismissed.GetProperty("dismissed").GetInt32());
            var undo = dismissed.GetProperty("undo").GetProperty("arguments").GetProperty("alerts_json").GetString()!;
            var restored = JsonDocument.Parse(await DarlingMcpAlertTools.RestoreAlertHistory(asMcp, undo, cancellationToken: ct)).RootElement;
            Assert.Equal(1, restored.GetProperty("restored").GetInt32());

            /* Nothing but the dismissed flag: any other column, INSERT and DELETE are refused. */
            foreach (var statement in new[]
            {
                "UPDATE config_alert_log SET muted = TRUE",
                "UPDATE config_alert_log SET detail_text = 'x'",
                "INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value) VALUES (now(), -1, 's', 'm', 1, 1)",
                "DELETE FROM config_alert_log",
            })
            {
                var refused = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(asMcp, statement, ct));
                Assert.Equal("42501", refused.SqlState);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (owner, token) =>
            {
                await using var command = new NpgsqlCommand($"DROP OWNED BY {role}; DROP ROLE IF EXISTS {role}", owner);
                await command.ExecuteNonQueryAsync(token);
            });
            await scratch.DisposeAsync();
        }
    }

    /// <summary>A write's log line names the token's identity and the client's own name, labelled as what the client says, and no destination value.</summary>
    [Fact]
    public void ARouteWriteLogLine_NamesTheTokenIdentity_AndLabelsTheClientsOwnName()
    {
        var logger = new LineLogger();
        ReadCaller.SetAmbientMcpCaller("claude-desktop 1.2.3");
        ReadCaller.SetAmbientMcpToken(ReadCaller.OfMcpToken("a-token"));
        try
        {
            DarlingMcpAlertTools.LogRouteWrite(logger, "update_notification_route", ReadCaller.WriteIdentity(), 4, "enabled");
        }
        finally
        {
            ReadCaller.SetAmbientMcpCaller(null);
            ReadCaller.SetAmbientMcpToken(null);
        }

        var line = Assert.Single(logger.Lines);
        Assert.Contains(ReadCaller.OfMcpToken("a-token") + ", client says: claude-desktop 1.2.3", line, StringComparison.Ordinal);
        Assert.DoesNotContain("a-token", line, StringComparison.Ordinal);
    }

    private sealed class LineLogger : Microsoft.Extensions.Logging.ILogger
    {
        public List<string> Lines { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(formatter(state, exception));
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
