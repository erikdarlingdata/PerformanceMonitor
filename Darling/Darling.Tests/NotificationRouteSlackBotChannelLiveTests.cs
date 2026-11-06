/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database (and the MCP facts a role with a per-run name), so the class is deliberately
   NOT [Collection("live-postgres")]. */

/// <summary>
/// #7656: a route's Slack bot channel id against a live store. V252 on a store that already holds routes (they read blank, NULL is refused,
/// and applying the rung again changes nothing), the writer's refusal of a channel name and its round trip of an id (trimmed), the service
/// and MCP readers seeing the column, the audit row of a change to an agent route, the web route body accepting the key, and the MCP create,
/// update and get round trip AS the <c>mcp</c> role with the grants taken from the managed batch.
/// </summary>
public sealed class NotificationRouteSlackBotChannelLiveTests
{
    private const string Channel = "C0123ABCDEF";
    private const string OtherChannel = "G9ZZZ";
    private const string Sentence = SlackChannelId.RefusalText;

    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DarlingMcpServedPolicy OperatorPolicy() => new(new DarlingMcpWritePolicy.Effective(true, false, false, "ops-lead"));

    private static string Status(string answer) => JsonDocument.Parse(answer).RootElement.GetProperty("status").GetString()!;

    /// <summary>The answer's message as text. The serializer writes the sentence's apostrophe as <c>\u0027</c>, so the raw JSON never
    /// contains it.</summary>
    private static string Message(string answer) => JsonDocument.Parse(answer).RootElement.GetProperty("message").GetString()!;

    private static int RouteIdOf(string answer) =>
        JsonDocument.Parse(answer).RootElement.GetProperty("route").GetProperty("route_id").GetInt32();

    private static string StoredChannel(int routeId) => $"SELECT slack_bot_channel FROM config.config_notification_routes WHERE route_id = {routeId}";

    /// <summary>When the route got its bot channel, read through the row's JSON so a store with no such column reads as blank instead of failing the query.</summary>
    private static string StoredStamp(int routeId) => $"SELECT to_jsonb(r) ->> 'slack_bot_channel_set_at' FROM config.config_notification_routes AS r WHERE route_id = {routeId}";

    /// <summary>The service loader's copy of the stamp. By reflection, so the test names the member it needs when it is missing.</summary>
    private static DateTime? StampOf(NotificationRoute route)
    {
        var property = typeof(NotificationRoute).GetProperty("SlackBotChannelSetAt");
        Assert.True(property is not null, "NotificationRoute has no SlackBotChannelSetAt: the time the route got its bot channel, which gates the parent post (#7656).");
        return (DateTime?)property!.GetValue(route);
    }

    [Theory]
    [InlineData("")]
    [InlineData(Channel)]
    [InlineData("  " + OtherChannel + "  ")]
    public void TheWriter_AcceptsAChannelId_OrBlank(string channel) =>
        Assert.Null(NotificationRouteWriter.Validate(new NotificationRouteDraft { MetricMatch = "Blocking", SmtpRecipients = "ops@example.com", SlackBotChannel = channel }));

    [Theory]
    [InlineData("#ops")]
    [InlineData("ops")]
    [InlineData("c0123abc")]
    [InlineData("U0123ABC")]
    public void TheWriter_RefusesAChannelName_WithItsSentence(string channel) =>
        Assert.Equal(Sentence, NotificationRouteWriter.Validate(
            new NotificationRouteDraft { MetricMatch = "Blocking", SmtpRecipients = "ops@example.com", SlackBotChannel = channel }));

    [Fact]
    public async Task V252_OnAStoreWithExistingRoutes_ReadsBlank_RefusesNull_AndAppliesTwiceWithNoChange()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V252 live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            /* A store one rung behind: V252's column gone, two routes stored (one scoped, one not). */
            await ExecAsync(connection, "ALTER TABLE config.config_notification_routes DROP COLUMN slack_bot_channel", ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 252", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_notification_routes (metric_match, slack_url) VALUES ('Deadlocks Detected', 'sealed-a')", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_notification_routes (metric_match, slack_url, scope_server_id) VALUES ('High CPU', 'sealed-b', 11)", ct);

            await PgMigrations.MigrateAsync(connection, ct);

            /* Both routes read blank (none), and the column is NOT NULL with a blank default. */
            Assert.Equal("|", await ScalarAsync(connection,
                "SELECT string_agg(slack_bot_channel, '|' ORDER BY route_id) FROM config.config_notification_routes", ct));
            Assert.Equal("NO|''::text", await ScalarAsync(connection,
                "SELECT is_nullable || '|' || column_default FROM information_schema.columns WHERE table_schema = 'config' AND table_name = 'config_notification_routes' AND column_name = 'slack_bot_channel'", ct));

            var nullRefused = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
                "UPDATE config.config_notification_routes SET slack_bot_channel = NULL WHERE metric_match = 'Deadlocks Detected'", ct));
            Assert.Equal("23502", nullRefused.SqlState);

            /* The rung applied again changes nothing: the stored id survives. */
            await ExecAsync(connection, $"UPDATE config.config_notification_routes SET slack_bot_channel = '{Channel}' WHERE metric_match = 'High CPU'", ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 252", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal("|" + Channel, await ScalarAsync(connection,
                "SELECT string_agg(slack_bot_channel, '|' ORDER BY route_id) FROM config.config_notification_routes", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>The viewer and web writer: an id is stored trimmed and read back, a channel name is refused before the store is touched, and
    /// changing the channel of an agent route writes a route_agent_changed row naming it with the ids before and after. The same change to a
    /// route with no agent level writes nothing. The service's reader and the MCP store both read the column.</summary>
    [Fact]
    public async Task Writer_StoresTheChannelTrimmed_RefusesAName_JournalsAChangeToAnAgentRouteOnly_AndTheReadersSeeIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the route bot channel writer pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(connection, "INSERT INTO config.config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var proxies = new WebhookProxies(null, null, null, null);
            var agentRoute = await NotificationRouteWriter.SaveAsync(
                owner, new NotificationRouteDraft { MetricMatch = "Blocking", SmtpRecipients = "ops@example.com", AgentLevel = 2 }, null, proxies, null, null, 30, ct);
            var plainRoute = await NotificationRouteWriter.SaveAsync(
                owner, new NotificationRouteDraft { MetricMatch = "Deadlocks", SmtpRecipients = "ops@example.com", SlackBotChannel = "  " + Channel + " " }, null, proxies, null, null, 30, ct);
            const string Journal = "SELECT count(*) FROM config.investigation_journal WHERE event = 'route_agent_changed'";

            Assert.Equal("", await ScalarAsync(owner, StoredChannel(agentRoute), ct));
            Assert.Equal(Channel, await ScalarAsync(owner, StoredChannel(plainRoute), ct));
            Assert.Equal("0", await ScalarAsync(owner, Journal, ct));
            /* The stamp: a route inserted with a channel carries the time it got it, and a route with none carries no stamp. */
            Assert.False(string.IsNullOrEmpty(await ScalarAsync(owner, StoredStamp(plainRoute), ct)), "a route inserted with a bot channel carries slack_bot_channel_set_at");
            Assert.Equal("", await ScalarAsync(owner, StoredStamp(agentRoute), ct));

            /* A channel name is refused with its sentence, and nothing is stored. */
            var stored = (await NotificationRouteWriter.ReadStoredAsync(owner, agentRoute, 30, ct))!;
            Assert.Equal("", stored.SlackBotChannel);
            Assert.Equal(Channel, (await NotificationRouteWriter.ReadStoredAsync(owner, plainRoute, 30, ct))!.SlackBotChannel);
            var bad = stored.Clone();
            bad.SlackBotChannel = "#ops";
            var refusal = await Assert.ThrowsAsync<NotificationRouteRefusedException>(() =>
                NotificationRouteWriter.SaveAsync(owner, bad, stored, proxies, null, null, 30, ct, "bob"));
            Assert.Contains(Sentence, refusal.Message, StringComparison.Ordinal);
            Assert.Equal("", await ScalarAsync(owner, StoredChannel(agentRoute), ct));

            /* Blank to an id on an agent route: stored, read back, and journaled with the ids before and after. */
            var typed = stored.Clone();
            typed.SlackBotChannel = Channel;
            await NotificationRouteWriter.SaveAsync(owner, typed, stored, proxies, null, null, 30, ct, "bob");
            Assert.Equal(Channel, (await NotificationRouteWriter.ReadStoredAsync(owner, agentRoute, 30, ct))!.SlackBotChannel);
            Assert.Equal("1", await ScalarAsync(owner, Journal, ct));
            var stampedAt = await ScalarAsync(owner, StoredStamp(agentRoute), ct);
            Assert.False(string.IsNullOrEmpty(stampedAt), "setting the bot channel stamps slack_bot_channel_set_at");
            Assert.Equal("person:bob|changed slack_bot_channel||" + Channel, await ScalarAsync(owner,
                "SELECT actor || '|' || reason || '|' || coalesce(detail->'before'->>'slack_bot_channel', '<missing>') || '|' || coalesce(detail->'after'->>'slack_bot_channel', '<missing>') FROM config.investigation_journal WHERE event = 'route_agent_changed' ORDER BY journal_id DESC LIMIT 1", ct));

            /* Another id, typed with spaces around it, is stored trimmed; a route with no agent level is stored but not journaled. */
            var stored2 = (await NotificationRouteWriter.ReadStoredAsync(owner, agentRoute, 30, ct))!;
            var typed2 = stored2.Clone();
            typed2.SlackBotChannel = "  " + OtherChannel + "  ";
            await NotificationRouteWriter.SaveAsync(owner, typed2, stored2, proxies, null, null, 30, ct, "bob");
            Assert.Equal(OtherChannel, await ScalarAsync(owner, StoredChannel(agentRoute), ct));
            Assert.Equal("2", await ScalarAsync(owner, Journal, ct));
            /* A different channel moves the stamp; a save that keeps the channel keeps it. */
            var movedTo = await ScalarAsync(owner, StoredStamp(agentRoute), ct);
            Assert.False(string.IsNullOrEmpty(movedTo), "changing the bot channel stamps slack_bot_channel_set_at again");
            Assert.NotEqual(stampedAt, movedTo);
            var stored3 = (await NotificationRouteWriter.ReadStoredAsync(owner, agentRoute, 30, ct))!;
            var sameChannel = stored3.Clone();
            sameChannel.Enabled = !sameChannel.Enabled;
            await NotificationRouteWriter.SaveAsync(owner, sameChannel, stored3, proxies, null, null, 30, ct, "bob");
            Assert.Equal(movedTo, await ScalarAsync(owner, StoredStamp(agentRoute), ct));

            var plainStored = (await NotificationRouteWriter.ReadStoredAsync(owner, plainRoute, 30, ct))!;
            var plainTyped = plainStored.Clone();
            plainTyped.SlackBotChannel = "";
            await NotificationRouteWriter.SaveAsync(owner, plainTyped, plainStored, proxies, null, null, 30, ct, "bob");
            Assert.Equal("", await ScalarAsync(owner, StoredChannel(plainRoute), ct));
            Assert.Equal("2", await ScalarAsync(owner, Journal, ct));
            Assert.Equal("", await ScalarAsync(owner, StoredStamp(plainRoute), ct));

            /* The service's reader (the resolver's input) and the MCP store both read the column. */
            var view = await new StoreConfigProvider(owner).LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            Assert.Equal(OtherChannel, view!.NotificationRoutes.Single(r => r.RouteId == agentRoute).SlackBotChannel);
            Assert.Equal("", view.NotificationRoutes.Single(r => r.RouteId == plainRoute).SlackBotChannel);
            /* The loader carries the stamp the thread pass gates on: set on the route with a channel, none on the route without. */
            var loadedStamp = StampOf(view.NotificationRoutes.Single(r => r.RouteId == agentRoute));
            Assert.NotNull(loadedStamp);
            Assert.Equal(DateTimeKind.Utc, loadedStamp!.Value.Kind);
            Assert.Equal(DateTime.Parse(movedTo!, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal), loadedStamp.Value);
            Assert.Null(StampOf(view.NotificationRoutes.Single(r => r.RouteId == plainRoute)));

            var summaries = await new PgNotificationRouteStore(owner).LoadSummariesAsync(ct);
            Assert.Equal(OtherChannel, summaries.Single(r => r.RouteId == agentRoute).SlackBotChannel);
            Assert.Equal("", summaries.Single(r => r.RouteId == plainRoute).SlackBotChannel);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>The web route body: <c>slack_bot_channel</c> is a known key (not "Unknown field"), an id is stored trimmed, a channel name is
    /// a 400 with the one sentence, and a value that is not text is a 400 naming the key.</summary>
    [Fact]
    public async Task Web_SaveRoute_AcceptsTheBotChannelKey_StoresItTrimmed_AndRefusesANameOrANonTextValueWith400()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the web route bot channel pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(connection, "INSERT INTO config.config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            static JsonObject Body(string json) => JsonNode.Parse(json)!.AsObject();
            Task<(int Status, JsonNode Body)> Save(string json) =>
                DarlingWebAgentSetup.SaveRouteAsync(owner, NullLogger.Instance, null, Body(json), "ops-lead", DarlingPasswordKey.Current, ct);

            var created = await Save($$"""{"metric_match":"blocking","agent_level":1,"slack_bot_channel":"  {{Channel}} "}""");
            Assert.Equal(200, created.Status);
            Assert.Equal("created", (string)created.Body["status"]!);
            var routeId = (int)created.Body["route_id"]!;
            Assert.Equal(Channel, await ScalarAsync(owner, StoredChannel(routeId), ct));
            Assert.False(string.IsNullOrEmpty(await ScalarAsync(owner, StoredStamp(routeId), ct)), "a route the web form saves with a bot channel carries slack_bot_channel_set_at");

            var name = await Save("""{"metric_match":"deadlocks","agent_level":1,"slack_bot_channel":"#ops"}""");
            Assert.Equal(400, name.Status);
            Assert.Equal(Sentence, (string)name.Body["error"]!);

            var notText = await Save("""{"metric_match":"deadlocks","agent_level":1,"slack_bot_channel":3}""");
            Assert.Equal(400, notText.Status);
            Assert.Equal("slack_bot_channel must be text.", (string)notText.Body["error"]!);

            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.config_notification_routes", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task Mcp_CreateUpdateAndGet_RoundTripTheBotChannel_AndABadIdIsInvalid()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            /* Left out, a new route has none. */
            var plain = await DarlingMcpAlertTools.CreateNotificationRoute(asMcp, "Blocking", served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("created", Status(plain));
            Assert.Equal("", await ScalarAsync(owner, StoredChannel(RouteIdOf(plain)), ct));
            Assert.Equal("", JsonDocument.Parse(plain).RootElement.GetProperty("route").GetProperty("slack_bot_channel").GetString());

            /* An id is stored trimmed and read back. */
            var created = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Deadlocks", slack_bot_channel: "  " + Channel + " ", served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("created", Status(created));
            var routeId = RouteIdOf(created);
            Assert.Equal(Channel, await ScalarAsync(owner, StoredChannel(routeId), ct));
            Assert.Equal(Channel, await ReadBotChannelAsync(asMcp, routeId, ct));
            Assert.Equal("", await ScalarAsync(owner, StoredStamp(RouteIdOf(plain)), ct));
            var stampedAt = await ScalarAsync(owner, StoredStamp(routeId), ct);
            Assert.False(string.IsNullOrEmpty(stampedAt), "a route the MCP tool creates with a bot channel carries slack_bot_channel_set_at");

            /* An update changes it, a field left out keeps it, the undo carries it, and blank clears it. */
            var updated = await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, slack_bot_channel: OtherChannel, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("updated", Status(updated));
            Assert.Equal(Channel, JsonDocument.Parse(updated).RootElement.GetProperty("undo").GetProperty("arguments").GetProperty("slack_bot_channel").GetString());
            Assert.Equal(OtherChannel, await ReadBotChannelAsync(asMcp, routeId, ct));
            /* The stamp moves with the channel, stays through a change to another field, and goes with the channel. */
            var movedTo = await ScalarAsync(owner, StoredStamp(routeId), ct);
            Assert.False(string.IsNullOrEmpty(movedTo), "changing the bot channel through the MCP tool stamps slack_bot_channel_set_at again");
            Assert.NotEqual(stampedAt, movedTo);
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, enabled: false, served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal(OtherChannel, await ReadBotChannelAsync(asMcp, routeId, ct));
            Assert.Equal(movedTo, await ScalarAsync(owner, StoredStamp(routeId), ct));
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, slack_bot_channel: "", served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("", await ReadBotChannelAsync(asMcp, routeId, ct));
            Assert.Equal("", await ScalarAsync(owner, StoredStamp(routeId), ct));

            /* A channel name is invalid on both tools, and nothing changes or is stored. */
            var badCreate = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking Wait", slack_bot_channel: "#ops", served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("invalid", Status(badCreate));
            Assert.Contains(Sentence, Message(badCreate), StringComparison.Ordinal);
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.config_notification_routes", ct));
            var badUpdate = await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, slack_bot_channel: "c0123abc", served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("invalid", Status(badUpdate));
            Assert.Contains(Sentence, Message(badUpdate), StringComparison.Ordinal);
            Assert.Equal("", await ScalarAsync(owner, StoredChannel(routeId), ct));
        });
    }

    /// <summary>The MCP update tool: an operator who changes the bot channel of an agent route gets a route_agent_changed row (through the
    /// update function's before row), and the same change to a route with no agent level writes none.</summary>
    [Fact]
    public async Task Mcp_ChangingTheBotChannelOfAnAgentRoute_IsJournaled_AndOfARouteWithNoAgentLevelIsNot()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var agentRoute = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking", agent_level: 2, served: OperatorPolicy(), cancellationToken: ct));
            var plainRoute = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Deadlocks", served: OperatorPolicy(), cancellationToken: ct));
            const string Journal = "SELECT count(*) FROM config.investigation_journal WHERE event = 'route_agent_changed'";
            Assert.Equal("1", await ScalarAsync(owner, Journal, ct));

            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, agentRoute, slack_bot_channel: Channel, served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("2", await ScalarAsync(owner, Journal, ct));
            Assert.Equal("operator:ops-lead|changed slack_bot_channel||" + Channel, await ScalarAsync(owner,
                $"SELECT actor || '|' || reason || '|' || coalesce(detail->'before'->>'slack_bot_channel', '<missing>') || '|' || coalesce(detail->'after'->>'slack_bot_channel', '<missing>') FROM config.investigation_journal WHERE route_id = {agentRoute} AND event = 'route_agent_changed' ORDER BY journal_id DESC LIMIT 1", ct));

            /* The same id again changes nothing, and a route with no agent level writes no row. */
            await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, agentRoute, slack_bot_channel: Channel, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, plainRoute, slack_bot_channel: OtherChannel, served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal(OtherChannel, await ScalarAsync(owner, StoredChannel(plainRoute), ct));
            Assert.Equal("2", await ScalarAsync(owner, Journal, ct));
        });
    }

    private static async Task<string> ReadBotChannelAsync(NpgsqlDataSource source, int routeId, CancellationToken ct) =>
        JsonDocument.Parse(await DarlingMcpAlertTools.GetNotificationRoutes(source, ct)).RootElement
            .GetProperty("routes").EnumerateArray().Single(r => r.GetProperty("route_id").GetInt32() == routeId)
            .GetProperty("slack_bot_channel").GetString()!;

    private static async Task WithMcpRoleAsync(Func<NpgsqlDataSource, NpgsqlDataSource, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the route bot channel live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "route_bc_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            /* The functions and the journal trigger are created by the owner, as the managed batch does; the batch's own grants, retargeted. */
            await ExecAsync(owner, DarlingManagedRoles.BuildNotificationRouteFunctionsSql("config"), ct);
            await ExecAsync(owner, DarlingManagedRoles.BuildJournalEventRulesSql("config"), ct);
            await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, "mcp"))
            {
                await ExecAsync(owner, statement, ct);
            }

            var executes = ViewerGrantReplay.BatchStatements().Where(s =>
                s.StartsWith("GRANT EXECUTE ON FUNCTION config.create_notification_route", StringComparison.Ordinal)
                || s.StartsWith("GRANT EXECUTE ON FUNCTION config.update_notification_route", StringComparison.Ordinal)
                || s.StartsWith("GRANT EXECUTE ON FUNCTION config.set_notification_route_agent", StringComparison.Ordinal)).ToList();
            Assert.Equal(3, executes.Count);
            foreach (var execute in executes)
            {
                var at = execute.LastIndexOf(" TO ", StringComparison.Ordinal);
                Assert.Equal("mcp", execute[(at + 4)..].Trim());
                await ExecAsync(owner, execute[..at] + " TO " + roleName, ct);
            }

            await using var asMcp = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = RolePassword,
                Pooling = false,
            }.ConnectionString);

            await body(owner, asMcp, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<string?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }
}
