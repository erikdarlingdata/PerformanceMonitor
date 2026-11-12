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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Chat;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7659 on a live store: the rung (V254: the chat link column, the asker table, the message table and the thread reverse index), the thread
/// lookups, the request level a person's grants give, the message's natural key, the chat request open (its copied alert rows, kind, link, askers
/// and stamp) and join, the two hourly caps, <c>CountRouteSql</c> and the resolve sweep leaving chat requests out, the follow-up of a chat request
/// copying its askers and kind, the chat sweep opening one request for every waiting message, refusing a waiting message whose asker lost the
/// grant and saying the agent may be offline once, the agent's message under its claim and journal cap, and the claim answer's
/// <c>person_requests</c> carrying the granted askers' texts alone.
/// </summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
public sealed class SlackChatStoreLiveTests
{
    private static readonly Guid s_parent = Guid.Parse("76590000-0000-0000-0000-0000000000b1");

    /// <summary>The thread's first request, for the grant class's seed.</summary>
    internal static Guid Parent => s_parent;
    internal const string Channel = "C07659A1";
    internal const string ThreadTs = "1700000000.000100";
    internal const string Team = "T0TEAM";
    internal const string Admin = "person:admin";
    internal const int ServerId = 7;

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the Slack chat store live tests.");
        return connectionString!;
    }

    internal static Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct) => SlackCardStoreLiveTests.ExecAsync(connection, sql, ct);

    internal static Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct) => SlackCardStoreLiveTests.ScalarAsync(connection, sql, ct);

    /// <summary>The store's clock, read once: the one instant a fact passes everywhere it needs a now, so nothing in it compares the store's
    /// clock with the test runner's.</summary>
    internal static async Task<DateTime> StoreNowAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT now()", connection);
        return (DateTime)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>A request on the route and the server, with its alert rows. <paramref name="state"/> and <paramref name="openedAgo"/> shape the one rule.</summary>
    internal static async Task AddRequestAsync(NpgsqlConnection connection, Guid id, int routeId, string state, TimeSpan openedAgo, int level = 3, string kind = "alert",
        Guid? chatThreadOf = null, CancellationToken ct = default)
    {
        var link = chatThreadOf is null ? "NULL" : $"'{chatThreadOf}'";
        await ExecAsync(connection,
            "INSERT INTO config.investigation_requests (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until, opened_at, expires_at, state, autonomy_level, trigger_kind, chat_thread_of) "
            + $"VALUES ('{id}', {routeId}, {ServerId}, 'srv-7', 'blocking', 'CPU', now() AT TIME ZONE 'UTC' - interval '2 hours', now() AT TIME ZONE 'UTC' - interval '2 hours', now() AT TIME ZONE 'UTC' - interval '1 hour', "
            + $"now() AT TIME ZONE 'UTC' - interval '{(int)openedAgo.TotalSeconds} seconds', now() AT TIME ZONE 'UTC' - interval '{(int)openedAgo.TotalSeconds} seconds' + interval '4 hours', '{state}', {level}, '{kind}', {link})", ct);
        await ExecAsync(connection,
            $"INSERT INTO config.investigation_request_alerts (request_id, alert_time, server_id, metric_name, severity) VALUES ('{id}', now() AT TIME ZONE 'UTC' - interval '2 hours', {ServerId}, 'CPU', 'critical'), ('{id}', now() AT TIME ZONE 'UTC' - interval '90 minutes', {ServerId}, 'Blocking', 'warning')", ct);
    }

    private sealed record Seed(int RouteId, long Erik, long Dana, long Pat);

    /* The store is opened through its own data source on the scratch database, so every session carries the store's search_path. The seed: a route with
       the bot channel, the server, a reported alert request whose parent post opened the thread, and three people: Erik with ask on the fleet, Dana with
       request_level_3 (force_plan) on the server, Pat disabled with ask. */
    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, InvestigationRequestStore, Seed, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection, $"INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ({ServerId}, 'srv-7', 'host-7')", ct);
            var routeId = int.Parse((await ScalarAsync(connection,
                $"INSERT INTO config.config_notification_routes (metric_match, slack_url, slack_bot_channel, enabled, agent_level, agent_max_per_hour) VALUES ('cpu', 'sealed-cpu', '{Channel}', true, 2, 3) RETURNING route_id", ct))!,
                System.Globalization.CultureInfo.InvariantCulture);
            await AddRequestAsync(connection, s_parent, routeId, "reported", TimeSpan.FromHours(1), ct: ct);
            await ExecAsync(connection, $"INSERT INTO config.investigation_request_threads (request_id, channel_id, thread_ts) VALUES ('{s_parent}', '{Channel}', '{ThreadTs}')", ct);
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var erik = (await PersonDirectory.AddPersonAsync(source, "Erik", Admin, ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            Assert.True((await PersonDirectory.AddIdentityAsync(source, erik, PersonIdentityKinds.Slack, Team + ":U0ERIK", Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            Assert.True((await PersonDirectory.AddGrantAsync(source, erik, null, null, PersonCapabilities.Ask, null, Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            var dana = (await PersonDirectory.AddPersonAsync(source, "Dana", Admin, ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            Assert.True((await PersonDirectory.AddIdentityAsync(source, dana, PersonIdentityKinds.Slack, Team + ":U0DANA", Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            Assert.True((await PersonDirectory.AddGrantAsync(source, dana, ServerId, null, PersonCapabilities.RequestLevel3, [ServerActionNames.ForcePlan], Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            var pat = (await PersonDirectory.AddPersonAsync(source, "Pat", Admin, ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            Assert.True((await PersonDirectory.AddIdentityAsync(source, pat, PersonIdentityKinds.Slack, Team + ":U0PAT", Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            Assert.True((await PersonDirectory.AddGrantAsync(source, pat, null, null, PersonCapabilities.Ask, null, Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            Assert.True((await PersonDirectory.SetPersonEnabledAsync(source, pat, false, Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            await body(connection, source, new InvestigationRequestStore(source, 30), new Seed(routeId, erik, dana, pat), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    internal static SlackChatThread Thread(int routeId) => new(s_parent, routeId, ServerId, Channel, ThreadTs, DateTime.UtcNow.AddHours(-1));

    internal static SlackPersonMessage Message(string ts, long personId, string user, string text) =>
        new(Channel, ts, ThreadTs, s_parent, personId, "slack:" + Team + ":" + user, text);

    /* The live route reader a test controls: every metric resolves to one route at the given level. */
    internal static Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<Func<string, int, AlertSeverityLevel?, NotificationRoute?>>> RoutesAt(int? level) =>
        (_, _, _) => Task.FromResult<Func<string, int, AlertSeverityLevel?, NotificationRoute?>>((_, _, _) =>
            level is null ? null : new NotificationRoute(1, "x", "", "", "", "", "", Enabled: true) { AgentLevel = level });

    [Fact]
    public async Task TheRung_AddsTheLinkTheAskersTheMessagesAndTheThreadIndex_TheProbeSeesIt_AndARewindDropsThemForTheRungToMakeAgain()
    {
        await WithStoreAsync(async (connection, _, _, _, ct) =>
        {
            Assert.Equal(StorageVersion.SchemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture), await ScalarAsync(connection, "SELECT max(version) FROM darling_schema_version", ct));
            Assert.True(StorageVersion.SchemaVersion >= 254, "the build ships V254");
            Assert.Equal("True", await ScalarAsync(connection, "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.investigation_requests') AND attname = 'chat_thread_of' AND NOT attisdropped)", ct));
            Assert.Equal("True", await ScalarAsync(connection, "SELECT to_regclass('config.investigation_request_askers') IS NOT NULL", ct));
            Assert.Equal("True", await ScalarAsync(connection, "SELECT to_regclass('config.investigation_person_messages') IS NOT NULL", ct));
            Assert.Equal("True", await ScalarAsync(connection, "SELECT to_regclass('config.ix_investigation_request_threads_channel_ts') IS NOT NULL", ct));
            foreach (var table in new[] { "investigation_request_askers", "investigation_person_messages" })
            {
                Assert.Equal("SELECT", await ScalarAsync(connection,
                    $"SELECT string_agg(privilege_type, ',') FROM information_schema.role_table_grants WHERE table_schema = 'config' AND table_name = '{table}' AND grantee = 'admin'", ct) ?? "SELECT");
            }

            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 253, ct);
            Assert.Equal("False", await ScalarAsync(connection, "SELECT to_regclass('config.investigation_person_messages') IS NOT NULL", ct));
            Assert.Equal("False", await ScalarAsync(connection, "SELECT to_regclass('config.investigation_request_askers') IS NOT NULL", ct));
            Assert.Equal("False", await ScalarAsync(connection, "SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('config.investigation_requests') AND attname = 'chat_thread_of' AND NOT attisdropped)", ct));
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version > 253", ct);
            Assert.Equal(PgMigrations.Scripts.Count(m => m.Version > 253), await PgMigrations.MigrateAsync(connection, ct));
            Assert.Equal("True", await ScalarAsync(connection, "SELECT to_regclass('config.investigation_person_messages') IS NOT NULL", ct));
            Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));
        });
    }

    [Fact]
    public async Task TheThread_IsFoundByItsChannelAndTs_AsItsFirstRequest_AndTheNewestRequestFollowsFollowUpsAndChatLinks()
    {
        await WithStoreAsync(async (connection, _, store, seed, ct) =>
        {
            var thread = await store.FindChatThreadAsync(Channel, ThreadTs, ct);
            Assert.NotNull(thread);
            Assert.Equal((s_parent, seed.RouteId, ServerId, Channel, ThreadTs), (thread.ThreadRequestId, thread.RouteId, thread.ServerId, thread.ChannelId, thread.ThreadTs));
            /* The parent was seeded an hour before the store's clock, so its window is read against the store's clock, not the test runner's. */
            var now = await StoreNowAsync(connection, ct);
            Assert.InRange(thread.OpenedAtUtc, now.AddMinutes(-61), now.AddMinutes(-59));
            Assert.Null(await store.FindChatThreadAsync(Channel, "1700000000.000999", ct));
            Assert.Null(await store.FindChatThreadAsync("C0OTHER", ThreadTs, ct));

            /* With no other request the newest is the first itself. */
            var newest = await store.NewestThreadRequestAsync(thread, ct);
            Assert.Equal((s_parent, "reported", "alert"), (newest!.RequestId, newest.State, newest.TriggerKind));

            /* A follow-up that replied in the thread (its thread row is the same channel and ts) is newer; the thread's first request stays the oldest. */
            var followUp = Guid.NewGuid();
            await AddRequestAsync(connection, followUp, seed.RouteId, "claimed", TimeSpan.FromMinutes(20), ct: ct);
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET follow_up_of = '{s_parent}' WHERE request_id = '{followUp}'", ct);
            await ExecAsync(connection, $"INSERT INTO config.investigation_request_threads (request_id, channel_id, thread_ts) VALUES ('{followUp}', '{Channel}', '{ThreadTs}')", ct);
            Assert.Equal(s_parent, (await store.FindChatThreadAsync(Channel, ThreadTs, ct))!.ThreadRequestId);
            newest = await store.NewestThreadRequestAsync(thread, ct);
            Assert.Equal((followUp, "claimed", "alert"), (newest!.RequestId, newest.State, newest.TriggerKind));

            /* A chat request linked to the thread is newer still, before its own parent post gave it a thread row. */
            var chat = Guid.NewGuid();
            await AddRequestAsync(connection, chat, seed.RouteId, "open", TimeSpan.FromMinutes(1), kind: "chat", chatThreadOf: s_parent, ct: ct);
            newest = await store.NewestThreadRequestAsync(thread, ct);
            Assert.Equal((chat, "open", "chat"), (newest!.RequestId, newest.State, newest.TriggerKind));
        });
    }

    [Fact]
    public async Task TheRequestLevel_IsTheHighestGrantForTheServer_ZeroWithoutAsk_AndZeroForADisabledPerson()
    {
        await WithStoreAsync(async (_, source, store, seed, ct) =>
        {
            Assert.Equal(1, await store.RequestLevelAsync(seed.Erik, ServerId, ct));
            Assert.Equal(3, await store.RequestLevelAsync(seed.Dana, ServerId, ct));
            Assert.Equal(0, await store.RequestLevelAsync(seed.Dana, 8, ct));
            Assert.Equal(0, await store.RequestLevelAsync(seed.Pat, ServerId, ct));
            Assert.Equal(0, await store.RequestLevelAsync(999, ServerId, ct));
            Assert.True((await PersonDirectory.AddGrantAsync(source, seed.Erik, ServerId, null, PersonCapabilities.RequestLevel2, null, Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            Assert.Equal(2, await store.RequestLevelAsync(seed.Erik, ServerId, ct));
            Assert.Equal(1, await store.RequestLevelAsync(seed.Erik, 8, ct));
        });
    }

    [Fact]
    public async Task AChatRequest_CopiesItsParentsAlertRows_IsKindChat_LinksTheThread_KeepsItsAskers_AndIsStampedAtTheLowestOfThree()
    {
        await WithStoreAsync(async (connection, _, store, seed, ct) =>
        {
            var message = Message("1700000000.000201", seed.Dana, "U0DANA", "please force the good plan");
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(message, ct));
            Assert.Equal(SlackPersonMessageSaved.Duplicate, await store.SavePersonMessageAsync(message with { Text = "again" }, ct));
            Assert.Equal("waiting", await ScalarAsync(connection, "SELECT state FROM config.investigation_person_messages", ct));

            /* The parent is stamped 3, the routes give its metrics 2, Dana holds 3: the chat request is stamped 2. */
            var result = await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [new SlackAsker(seed.Dana, message.Identity, 3)], [message], 3, DateTime.UtcNow, RoutesAt(2), ct);
            Assert.Equal(SlackChatOpenOutcome.Opened, result.Outcome);
            var chat = result.RequestId!.Value;
            Assert.Equal("chat|" + s_parent + "|2|open|" + seed.RouteId + "|" + ServerId, await ScalarAsync(connection,
                $"SELECT trigger_kind || '|' || chat_thread_of::text || '|' || autonomy_level || '|' || state || '|' || route_id || '|' || server_id FROM config.investigation_requests WHERE request_id = '{chat}'", ct));
            Assert.Equal("2", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_request_alerts WHERE request_id = '{chat}'", ct));
            Assert.Equal("CPU,Blocking", await ScalarAsync(connection, $"SELECT string_agg(metric_name, ',' ORDER BY alert_time) FROM config.investigation_request_alerts WHERE request_id = '{chat}'", ct));
            Assert.Equal(seed.Dana + "|3|" + message.Identity, await ScalarAsync(connection, $"SELECT person_id || '|' || request_level || '|' || identity FROM config.investigation_request_askers WHERE request_id = '{chat}'", ct));
            Assert.Equal("opened|" + chat, await ScalarAsync(connection, "SELECT state || '|' || request_id::text FROM config.investigation_person_messages WHERE ts = '1700000000.000201'", ct));
            /* The opened row posts the request into the thread; the person_message row names the asker and carries no text. */
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{chat}' AND event = 'opened'", ct));
            Assert.Equal(seed.Dana.ToString(System.Globalization.CultureInfo.InvariantCulture), await ScalarAsync(connection,
                $"SELECT detail ->> 'person_id' FROM config.investigation_journal WHERE request_id = '{chat}' AND event = '{InvestigationRequestStore.AskerJournalEvent}'", ct));
            Assert.Equal("0", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{chat}' AND detail::text LIKE '%good plan%'", ct));

            /* Erik, with ask alone, joins the unclaimed chat request: a second asker row, and the stamp drops to 1. */
            var second = Message("1700000000.000202", seed.Erik, "U0ERIK", "and check blocking");
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(second, ct));
            Assert.True(await store.JoinChatRequestAsync(chat, new SlackAsker(seed.Erik, second.Identity, 1), second, ct));
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT autonomy_level::text FROM config.investigation_requests WHERE request_id = '{chat}'", ct));
            Assert.Equal("2", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_request_askers WHERE request_id = '{chat}'", ct));
            Assert.Equal("joined|" + chat, await ScalarAsync(connection, "SELECT state || '|' || request_id::text FROM config.investigation_person_messages WHERE ts = '1700000000.000202'", ct));
            Assert.Equal("2", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{chat}' AND event = '{InvestigationRequestStore.AskerJournalEvent}'", ct));

            /* Once claimed the chat request takes no join: the message stays waiting. */
            Assert.Equal(InvestigationClaimOutcome.Claimed, (await store.ClaimAsync(chat, "agent-a", ct)).Outcome);
            var third = Message("1700000000.000203", seed.Erik, "U0ERIK", "one more");
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(third, ct));
            Assert.False(await store.JoinChatRequestAsync(chat, new SlackAsker(seed.Erik, third.Identity, 1), third, ct));
            Assert.Equal("waiting", await ScalarAsync(connection, "SELECT state FROM config.investigation_person_messages WHERE ts = '1700000000.000203'", ct));

            /* The person requests the claim answer carries: each asker's name, grants for the server and text, in order. */
            var page = await store.ListPersonRequestsAsync(chat, ct);
            var requests = page.Items;
            Assert.Equal(0, page.Omitted);
            Assert.Equal(2, requests.Count);
            Assert.Equal(("Dana", "please force the good plan"), (requests[0].Name, requests[0].Text));
            Assert.Equal(new[] { "request_level_3:force_plan" }, requests[0].Grants);
            Assert.Equal(("Erik", "and check blocking"), (requests[1].Name, requests[1].Text));
            Assert.Equal(new[] { "ask" }, requests[1].Grants);
            Assert.Empty((await store.ListPersonRequestsAsync(s_parent, ct)).Items);

            /* A parent whose routes are gone stamps 1, the column's floor (reads alone), and an asker below the routes lowers it. */
            var low = await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [new SlackAsker(seed.Erik, "slack:T:U", 1)], [], 10, DateTime.UtcNow, RoutesAt(3), ct);
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT autonomy_level::text FROM config.investigation_requests WHERE request_id = '{low.RequestId}'", ct));
            var gone = await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [new SlackAsker(seed.Dana, "slack:T:U", 3)], [], 10, DateTime.UtcNow, RoutesAt(null), ct);
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT autonomy_level::text FROM config.investigation_requests WHERE request_id = '{gone.RequestId}'", ct));
            Assert.Equal(SlackChatOpenOutcome.ParentGone, (await store.OpenChatRequestAsync(Thread(seed.RouteId), Guid.NewGuid(), [], [], 10, DateTime.UtcNow, RoutesAt(2), ct)).Outcome);
        });
    }

    [Fact]
    public async Task TheCaps_HoldTheThreadToSixAnHour_AndTheRouteToItsAgentMaxPerHour_CountedApartFromAlerts()
    {
        await WithStoreAsync(async (connection, _, store, seed, ct) =>
        {
            /* One instant for the whole fact, read from the store's clock and set five seconds behind it, as a test runner whose clock lags the
               store's sees it. The parent is seeded at exactly the hour's edge from this same instant, and every open, count and window below
               takes it, so no second clock can move the parent inside the hour the counts read. */
            var now = (await StoreNowAsync(connection, ct)).AddSeconds(-5);
            await using (var edge = new NpgsqlCommand($"UPDATE config.investigation_requests SET opened_at = $1 - interval '1 hour' WHERE request_id = '{s_parent}'", connection))
            {
                edge.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz });
                Assert.Equal(1, await edge.ExecuteNonQueryAsync(ct));
            }

            var thread = new SlackChatThread(s_parent, seed.RouteId, ServerId, Channel, ThreadTs, now.AddHours(-1));
            for (var i = 0; i < 6; i++)
                Assert.Equal(SlackChatOpenOutcome.Opened, (await store.OpenChatRequestAsync(thread, s_parent, [], [], 100, now, RoutesAt(2), ct)).Outcome);
            Assert.Equal(new SlackChatCounts(6, 6), await store.ChatCountsAsync(s_parent, seed.RouteId, now, ct));
            Assert.Equal(SlackChatOpenOutcome.ThreadCap, (await store.OpenChatRequestAsync(thread, s_parent, [], [], 100, now, RoutesAt(2), ct)).Outcome);
            Assert.Equal("6", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'chat'", ct));

            /* Another thread on the same route: under its own thread cap, over the route's. */
            var other = Guid.NewGuid();
            await AddRequestAsync(connection, other, seed.RouteId, "reported", TimeSpan.FromMinutes(30), ct: ct);
            await ExecAsync(connection, $"INSERT INTO config.investigation_request_threads (request_id, channel_id, thread_ts) VALUES ('{other}', '{Channel}', '1700000000.000500')", ct);
            var otherThread = new SlackChatThread(other, seed.RouteId, ServerId, Channel, "1700000000.000500", now.AddMinutes(-30));
            Assert.Equal(SlackChatOpenOutcome.RouteCap, (await store.OpenChatRequestAsync(otherThread, other, [], [], 6, now, RoutesAt(2), ct)).Outcome);
            Assert.Equal(SlackChatOpenOutcome.Opened, (await store.OpenChatRequestAsync(otherThread, other, [], [], 7, now, RoutesAt(2), ct)).Outcome);

            /* CountRouteSql, the alert limit, leaves every chat request out; the chat count is apart. */
            await using (var count = new NpgsqlCommand(InvestigationRequestStore.CountRouteSql, connection))
            {
                count.Parameters.Add(new NpgsqlParameter<int> { TypedValue = seed.RouteId });
                count.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz });
                await using var reader = await count.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                /* The parent opened an hour ago, at the hourly window's edge; the other thread's request half an hour ago. Neither chat request counts. */
                Assert.Equal((1L, 2L), (reader.GetInt64(0), reader.GetInt64(1)));
            }

            Assert.Equal(new SlackChatCounts(1, 7), await store.ChatCountsAsync(other, seed.RouteId, now, ct));
            /* An hour on, the hour's count is empty. */
            Assert.Equal(new SlackChatCounts(0, 0), await store.ChatCountsAsync(s_parent, seed.RouteId, now.AddMinutes(61), ct));
        });
    }

    [Fact]
    public async Task TheResolveSweep_NeverResolvesAChatRequest_EvenWhenAClearIsNewerThanItsCopiedAlertRows()
    {
        await WithStoreAsync(async (connection, _, store, seed, ct) =>
        {
            var chat = (await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [], [], 10, DateTime.UtcNow, RoutesAt(2), ct)).RequestId!.Value;
            /* The parent's alert rows are two hours old and copied as they were; a clear for each metric is newer than both. */
            foreach (var metric in new[] { "CPU", "Blocking" })
            {
                await ExecAsync(connection,
                    $"INSERT INTO config.config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, muted, context_json) VALUES (now() AT TIME ZONE 'UTC' - interval '10 minutes', {ServerId}, 'srv-7', '{metric} Resolved', 0, 0, false, '{{\"Resolves\":\"{metric}\"}}')", ct);
            }

            var swept = await store.SweepAsync(ct);
            Assert.Equal(1, swept.Resolved);
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{s_parent}' AND event = 'resolved'", ct));
            Assert.Equal("0", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{chat}' AND event = 'resolved'", ct));
            Assert.Equal("open", await ScalarAsync(connection, $"SELECT state FROM config.investigation_requests WHERE request_id = '{chat}'", ct));
        });
    }

    [Fact]
    public async Task AFollowUpOfAChatRequest_CopiesItsAskersAndKind_AndAFollowUpOfAnAlertRequestDoesNot()
    {
        await WithStoreAsync(async (connection, _, store, seed, ct) =>
        {
            var message = Message("1700000000.000301", seed.Dana, "U0DANA", "force it");
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(message, ct));
            var chat = (await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [new SlackAsker(seed.Dana, message.Identity, 3)], [message], 10, DateTime.UtcNow, RoutesAt(3), ct)).RequestId!.Value;
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'reported', reported_at = now(), follow_up_at = now() - interval '1 minute' WHERE request_id IN ('{chat}', '{s_parent}')", ct);

            var swept = await store.SweepAsync(ct);
            Assert.Equal(2, swept.FollowUps);
            Assert.Equal("chat|" + s_parent + "|3", await ScalarAsync(connection,
                $"SELECT trigger_kind || '|' || chat_thread_of::text || '|' || autonomy_level FROM config.investigation_requests WHERE follow_up_of = '{chat}'", ct));
            Assert.Equal(seed.Dana + "|3", await ScalarAsync(connection,
                $"SELECT a.person_id || '|' || a.request_level FROM config.investigation_request_askers AS a JOIN config.investigation_requests AS r ON r.request_id = a.request_id WHERE r.follow_up_of = '{chat}'", ct));
            Assert.Equal("alert|", await ScalarAsync(connection,
                $"SELECT trigger_kind || '|' || COALESCE(chat_thread_of::text, '') FROM config.investigation_requests WHERE follow_up_of = '{s_parent}'", ct));
            Assert.Equal("0", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_request_askers AS a JOIN config.investigation_requests AS r ON r.request_id = a.request_id WHERE r.follow_up_of = '{s_parent}'", ct));
        });
    }

    private sealed class RecordingPosts : ISlackBotPosts
    {
        public List<(string Channel, string Text, string? ThreadTs)> Posts { get; } = [];

        public Task<SlackPostResult> PostAsync(string channel, string text, IReadOnlyList<object>? blocks, string? threadTs, CancellationToken ct)
        {
            Posts.Add((channel, text, threadTs));
            return Task.FromResult(new SlackPostResult(true, "1700000000.000900", channel, null));
        }
    }

    [Fact]
    public async Task TheChatSweep_OpensOneRequestForTwoWaitingMessages_WhenTheThreadsRequestHasEnded_AndSaysTheAgentMayBeOfflineOnce()
    {
        await WithStoreAsync(async (connection, _, store, seed, ct) =>
        {
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(Message("1700000000.000401", seed.Erik, "U0ERIK", "first"), ct));
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(Message("1700000000.000402", seed.Dana, "U0DANA", "second"), ct));
            var waiting = Assert.Single(await store.WaitingThreadsAsync(10, ct));
            Assert.Equal((s_parent, seed.RouteId, ServerId, Channel, ThreadTs, 2), (waiting.ThreadRequestId, waiting.RouteId, waiting.ServerId, waiting.ChannelId, waiting.ThreadTs, waiting.Messages));
            Assert.Equal(new[] { "first", "second" }, (await store.WaitingMessagesAsync(s_parent, ct)).Select(m => m.Text).ToArray());

            var posts = new RecordingPosts();
            var routes = new[] { new NotificationRoute(seed.RouteId, "cpu", "", "", "", "", "", Enabled: true) { SlackBotChannel = Channel, AgentMaxPerHour = 3 } };
            var now = DateTime.UtcNow;
            var sweep = new SlackChatSweep(new StoreSlackChatCommands(store, NullLogger.Instance), posts, () => routes, NullLogger.Instance, () => now);

            /* The parent is reported (ended): one chat request for both messages, the two askers, the parent's alert rows, the thread link. */
            Assert.Equal((1, 0), await sweep.RunOnceAsync(ct));
            var chat = Guid.Parse((await ScalarAsync(connection, "SELECT request_id::text FROM config.investigation_requests WHERE trigger_kind = 'chat'", ct))!);
            Assert.Equal("2", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_request_askers WHERE request_id = '{chat}'", ct));
            Assert.Equal("2", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_person_messages WHERE request_id = '{chat}' AND state = 'opened'", ct));
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT min(autonomy_level)::text FROM config.investigation_requests WHERE request_id = '{chat}'", ct));
            Assert.Empty(await store.WaitingThreadsAsync(10, ct));
            Assert.Empty(posts.Posts);

            /* Four minutes unclaimed: nothing. Five: the offline answer once, in the thread, marked under the chat request. */
            now = now.AddMinutes(4);
            Assert.Equal((0, 0), await sweep.RunOnceAsync(ct));
            Assert.Empty(posts.Posts);
            now = now.AddMinutes(1).AddSeconds(1);
            Assert.Equal((0, 1), await sweep.RunOnceAsync(ct));
            Assert.Equal((Channel, SlackChatAnswers.Offline, ThreadTs), Assert.Single(posts.Posts));
            Assert.Equal((0, 0), await sweep.RunOnceAsync(ct));
            Assert.Single(posts.Posts);
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{chat}' AND event = '{InvestigationRequestStore.SlackThreadPosted}' AND reason = '{InvestigationRequestStore.ChatAnswerKey(SlackChatAnswers.OfflineKind)}'", ct));

            /* A claimed chat request is not offline, however long it takes. */
            var later = (await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [], [], 10, now, RoutesAt(2), ct)).RequestId!.Value;
            Assert.Equal(InvestigationClaimOutcome.Claimed, (await store.ClaimAsync(later, "agent-a", ct)).Outcome);
            now = now.AddMinutes(10);
            Assert.Equal((0, 0), await sweep.RunOnceAsync(ct));
            Assert.Single(posts.Posts);
        });
    }

    [Fact]
    public async Task TheAgentsMessage_PostsUnderALiveClaimOnly_IsBounded_IsJournaledAsTheAgents_AndStopsAtTheJournalCap()
    {
        await WithStoreAsync(async (connection, _, store, seed, ct) =>
        {
            var chat = (await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [], [], 10, DateTime.UtcNow, RoutesAt(2), ct)).RequestId!.Value;
            Assert.Equal(AgentMessageOutcome.LeaseLost, (await store.PostAgentMessageAsync(chat, Guid.NewGuid(), "agent-a", "hello", ct)).Outcome);
            Assert.Equal(AgentMessageOutcome.NotFound, (await store.PostAgentMessageAsync(Guid.NewGuid(), Guid.NewGuid(), "agent-a", "hello", ct)).Outcome);

            var claim = await store.ClaimAsync(chat, "agent-a", ct);
            var posted = await store.PostAgentMessageAsync(chat, claim.ClaimId!.Value, "agent-a", "Looking now. " + new string('x', 5000), ct);
            Assert.Equal(AgentMessageOutcome.Posted, posted.Outcome);
            Assert.NotNull(posted.JournalId);
            Assert.Equal("agent_message|agent:agent-a|4000", await ScalarAsync(connection,
                $"SELECT event || '|' || actor || '|' || char_length(detail ->> 'text') FROM config.investigation_journal WHERE journal_id = {posted.JournalId}", ct));
            Assert.Equal(AgentMessageOutcome.LeaseLost, (await store.PostAgentMessageAsync(chat, Guid.NewGuid(), "agent-a", "hello", ct)).Outcome);

            /* The thread pass reads the row with the chat link, so the post lands in the thread the chat request belongs to. */
            var rows = await store.PendingSlackThreadRowsAsync(SlackThreadPass.PostedEvents, DateTime.UtcNow.AddDays(-1), DateTime.UtcNow, 100, ct);
            var row = Assert.Single(rows, r => r.Event == InvestigationRequestStore.AgentMessage);
            Assert.Equal((chat, s_parent, "agent:agent-a"), (row.RequestId, row.ChatThreadOf, row.Actor));
            Assert.Contains("Looking now.", row.DetailJson, StringComparison.Ordinal);
            Assert.Equal(s_parent, Assert.Single(rows, r => r.Event == "opened" && r.RequestId == chat).ChatThreadOf);

            /* The tool over the store: posted; blank text invalid; a lost lease by its word. */
            var answer = JsonNode.Parse(await DarlingMcpInvestigationTools.PostMessageAsync(store, chat.ToString(), claim.ClaimId.Value.ToString(), "Second note.", ct))!;
            Assert.Equal("posted", answer["status"]!.ToString());
            Assert.Equal(chat.ToString(), answer["request_id"]!.ToString());
            Assert.Equal("invalid", JsonNode.Parse(await DarlingMcpInvestigationTools.PostMessageAsync(store, chat.ToString(), claim.ClaimId.Value.ToString(), "  ", ct))!["status"]!.ToString());
            Assert.Equal("lease_lost", JsonNode.Parse(await DarlingMcpInvestigationTools.PostMessageAsync(store, chat.ToString(), Guid.NewGuid().ToString(), "x", ct))!["status"]!.ToString());
            Assert.Equal("not_found", JsonNode.Parse(await DarlingMcpInvestigationTools.PostMessageAsync(store, "nope", claim.ClaimId.Value.ToString(), "x", ct))!["status"]!.ToString());
            Assert.Equal("not_found", JsonNode.Parse(await DarlingMcpInvestigationTools.PostMessageAsync(store, Guid.NewGuid().ToString(), claim.ClaimId.Value.ToString(), "x", ct))!["status"]!.ToString());
            /* A named token that presents another token's claim is refused. */
            Assert.Equal(DarlingMcpInvestigationTools.ClaimNotToken, JsonNode.Parse(await DarlingMcpInvestigationTools.PostMessageAsync(store, chat.ToString(), claim.ClaimId.Value.ToString(), "x", ct, tokenName: "agent-b"))!["status"]!.ToString());

            /* The cap: the request's agent rows reach the limit, and the next message is refused and journaled once as capped. */
            await ExecAsync(connection,
                $"INSERT INTO config.investigation_journal (request_id, event, actor, reason) SELECT '{chat}', 'claimed', 'agent:agent-a', 'filler' FROM generate_series(1, {InvestigationRules.MaxAgentJournalRows})", ct);
            Assert.Equal(AgentMessageOutcome.JournalCapped, (await store.PostAgentMessageAsync(chat, claim.ClaimId.Value, "agent-a", "one more", ct)).Outcome);
            Assert.Equal("journal_capped", JsonNode.Parse(await DarlingMcpInvestigationTools.PostMessageAsync(store, chat.ToString(), claim.ClaimId.Value.ToString(), "x", ct))!["status"]!.ToString());
            Assert.Equal("2", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{chat}' AND event = 'agent_message'", ct));
        });
    }

    internal static ServerActionCall Call(Guid request, Guid claim, string action) =>
        new(action, action, new AgentAction(request, claim, "agent-a", "the person asked for it",
            "[{\"tool\":\"get_blocking_stats\",\"arguments\":{},\"key_numbers\":\"12\"}]"), new AgentActionTarget(ServerId, "srv-7", null, null));

    /// <summary>The gate's asker kind check (#7659): a Level 3 action on a chat request runs only when every asker holds <c>request_level_3</c> with that
    /// kind for the server; otherwise it is refused as <c>asker_grant</c>, and nothing is opened, joined or proposed.</summary>
    [Fact]
    public async Task ALevel3ActionOnAChatRequest_IsRefusedAsAskerGrant_UnlessEveryAskerHoldsTheKind()
    {
        await WithStoreAsync(async (connection, source, store, seed, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            /* Dana asks; she holds request_level_3 with force_plan alone, so the chat request is stamped 3 and her request may propose a plan force. */
            var message = Message("1700000000.000701", seed.Dana, "U0DANA", "kill the blocking session");
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(message, ct));
            var chat = (await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [new SlackAsker(seed.Dana, message.Identity, 3)], [message], 10, DateTime.UtcNow, RoutesAt(3), ct)).RequestId!.Value;
            var claim = (await store.ClaimAsync(chat, "agent-a", ct)).ClaimId!.Value;
            var requestsBefore = await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_requests", ct);
            var hooks = new ServerActionHooks
            {
                LoadRoutes = RoutesAt(3),
                ReadCredential = (_, _) => throw new InvalidOperationException("the credential is read after the asker check"),
            };

            /* A kill: Dana's grant does not name kill_session, so the gate refuses it as asker_grant before any server condition. */
            var kill = await store.CheckServerActionAsync(Call(chat, claim, ServerActionNames.KillSession), hooks, ct);
            Assert.Equal(AgentActionCodes.AskerGrant, kill?.Reason);
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{chat}' AND event = 'refused' AND reason LIKE '%{AgentActionCodes.AskerGrant}%'", ct));

            /* A plan force: every asker holds the kind, so the asker check passes and whatever answers next is another condition's. */
            var force = await store.CheckServerActionAsync(Call(chat, claim, ServerActionNames.ForcePlan), hooks, ct);
            Assert.NotEqual(AgentActionCodes.AskerGrant, force?.Reason);
            Assert.NotEqual(AgentActionCodes.NotLevel3, force?.Reason);

            /* Erik joins with request_level_3 for evict_plan alone: still Level 3, but not every asker holds force_plan, so the force is refused too. */
            Assert.True((await PersonDirectory.AddGrantAsync(source, seed.Erik, ServerId, null, PersonCapabilities.RequestLevel3, [ServerActionNames.EvictPlan], Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'open', claim_id = NULL, claimed_by = NULL, lease_until = NULL WHERE request_id = '{chat}'", ct);
            var second = Message("1700000000.000702", seed.Erik, "U0ERIK", "and evict the plan");
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(second, ct));
            Assert.True(await store.JoinChatRequestAsync(chat, new SlackAsker(seed.Erik, second.Identity, 3), second, ct));
            claim = (await store.ClaimAsync(chat, "agent-a", ct)).ClaimId!.Value;
            var forceAfter = await store.CheckServerActionAsync(Call(chat, claim, ServerActionNames.ForcePlan), hooks, ct);
            Assert.Equal(AgentActionCodes.AskerGrant, forceAfter?.Reason);

            /* Nothing opened, joined or proposed by the refusals: the same requests, two askers, no proposal. */
            Assert.Equal(requestsBefore, await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_requests", ct));
            Assert.Equal("2", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_request_askers WHERE request_id = '{chat}'", ct));
            Assert.Equal("0", await ScalarAsync(connection, $"SELECT count(*) FROM config.agent_action_proposals WHERE request_id = '{chat}'", ct));

            /* The SQL itself: true for a kind every asker holds, false for one somebody lacks, true with no askers at all. */
            await using var holds = new NpgsqlCommand(InvestigationRequestStore.AskersHoldKindSql, connection);
            holds.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = chat });
            holds.Parameters.Add(new NpgsqlParameter<string> { TypedValue = ServerActionNames.KillSession, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            holds.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            Assert.Equal(false, await holds.ExecuteScalarAsync(ct));
            holds.Parameters[1].Value = ServerActionNames.ForcePlan;
            Assert.Equal(false, await holds.ExecuteScalarAsync(ct));
            holds.Parameters[0].Value = s_parent;
            Assert.Equal(true, await holds.ExecuteScalarAsync(ct));
        });
    }

    /// <summary>The waiting caps (#7659): a person holds at most five waiting messages in a thread and a thread at most twenty, the waiting read is
    /// bounded, and past either cap the save answers the cap instead of writing.</summary>
    [Fact]
    public async Task TheWaitingCaps_HoldAPersonToFiveAndAThreadToTwenty_AndTheWaitingReadIsBounded()
    {
        await WithStoreAsync(async (connection, source, store, seed, ct) =>
        {
            for (var i = 0; i < SlackChatRules.WaitingPerPerson; i++)
                Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(Message($"1700000000.00090{i}", seed.Erik, "U0ERIK", "erik " + i), ct));
            Assert.Equal(SlackPersonMessageSaved.PersonFull, await store.SavePersonMessageAsync(Message("1700000000.000910", seed.Erik, "U0ERIK", "one too many"), ct));
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_person_messages WHERE ts = '1700000000.000910'", ct));
            /* The same ts again is still the duplicate, not the cap. */
            Assert.Equal(SlackPersonMessageSaved.Duplicate, await store.SavePersonMessageAsync(Message("1700000000.000900", seed.Erik, "U0ERIK", "again"), ct));

            /* Three more people fill the thread to twenty; a fifth person is refused by the thread cap with none of their own waiting. */
            var people = new List<long> { seed.Dana, seed.Pat };
            for (var p = 0; p < 1; p++)
                people.Add((await PersonDirectory.AddPersonAsync(source, "Extra" + p, Admin, ServerActionSurfaces.Viewer, ct)).PersonId!.Value);
            var ts = 920;
            foreach (var person in people)
                for (var i = 0; i < SlackChatRules.WaitingPerPerson; i++)
                    Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(Message($"1700000000.000{ts++}", person, "U" + person, "text"), ct));
            Assert.Equal(SlackChatRules.WaitingPerThread.ToString(System.Globalization.CultureInfo.InvariantCulture), await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_person_messages WHERE state = 'waiting'", ct));
            var fifth = (await PersonDirectory.AddPersonAsync(source, "Fifth", Admin, ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            Assert.Equal(SlackPersonMessageSaved.ThreadFull, await store.SavePersonMessageAsync(Message("1700000000.000999", fifth, "U0FIFTH", "full"), ct));

            /* Rows written past the cap by another path still come back bounded. */
            await ExecAsync(connection, $"INSERT INTO config.investigation_person_messages (channel_id, ts, thread_ts, thread_request_id, person_id, identity, message_text, state) SELECT '{Channel}', '1700000001.' || lpad(g::text, 6, '0'), '{ThreadTs}', '{s_parent}', {fifth}, 'slack:T:U', 'extra', 'waiting' FROM generate_series(1, 10) AS g", ct);
            Assert.Equal(SlackChatRules.WaitingPerThread, (await store.WaitingMessagesAsync(s_parent, ct)).Count);
            /* The thread list still counts everything waiting; the read the sweep opens with is the bounded one. */
            Assert.Equal(SlackChatRules.WaitingPerThread + 10, Assert.Single(await store.WaitingThreadsAsync(10, ct)).Messages);
        });
    }

    /// <summary>The claim answer's person_requests (#7659) is bounded by characters: the newest messages inside the ceiling, oldest first, and a count of
    /// what was left out, so one asker cannot grow the answer without bound.</summary>
    [Fact]
    public async Task PersonRequests_KeepTheNewestInsideTheCharacterCeiling_AndSayHowManyWereLeftOut()
    {
        await WithStoreAsync(async (connection, _, store, seed, ct) =>
        {
            var chat = (await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [new SlackAsker(seed.Erik, "slack:T0TEAM:U0ERIK", 1)], [], 10, DateTime.UtcNow, RoutesAt(2), ct)).RequestId!.Value;
            /* Six messages of 3,500 characters each, oldest first: four fit under the 16,000-character ceiling. */
            for (var i = 0; i < 6; i++)
                await ExecAsync(connection, $"INSERT INTO config.investigation_person_messages (channel_id, ts, thread_ts, thread_request_id, request_id, person_id, identity, message_text, state, received_at) VALUES ('{Channel}', '1700000002.00000{i}', '{ThreadTs}', '{s_parent}', '{chat}', {seed.Erik}, 'slack:T0TEAM:U0ERIK', 'm{i} ' || repeat('x', 3497), 'opened', now() AT TIME ZONE 'UTC' + interval '{i} seconds')", ct);

            var page = await store.ListPersonRequestsAsync(chat, ct);
            Assert.Equal(2, page.Omitted);
            Assert.Equal(new[] { "m2", "m3", "m4", "m5" }, page.Items.Select(r => r.Text[..2]).ToArray());
            Assert.All(page.Items, r => Assert.Equal("Erik", r.Name));
            Assert.True(page.Items.Sum(r => r.Text.Length) <= SlackChatRules.PersonRequestsCharLimit);

            var (requests, omitted) = await DarlingMcpInvestigationTools.PersonRequestsAsync(store, chat, ct);
            Assert.Equal(4, requests!.Count);
            Assert.Equal(2, omitted);
            Assert.Equal(16000, SlackChatRules.PersonRequestsCharLimit);
        });
    }

    /// <summary>The sweep applies the handler's rules on a live store (#7659): a person disabled while their mention waited behind a claimed chat
    /// request is refused once the claim lapses, with no asker row, no person_message row naming them and their text in no person_requests; the
    /// granted asker's message joins the request without touching its stamp; the sweep does not throw; and a second waiting thread still opens.</summary>
    [Fact]
    public async Task TheChatSweep_RefusesAWaitingMessageWhoseAskerWasDisabled_JoinsTheGrantedOne_AndStillOpensTheNextThread()
    {
        await WithStoreAsync(async (connection, source, store, seed, ct) =>
        {
            /* A chat request in play: Dana's, claimed by an agent. Erik and Dana mention while it is claimed, so both messages wait. */
            var chat = (await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [new SlackAsker(seed.Dana, "slack:" + Team + ":U0DANA", 3)], [], 10, DateTime.UtcNow, RoutesAt(3), ct)).RequestId!.Value;
            Assert.Equal(InvestigationClaimOutcome.Claimed, (await store.ClaimAsync(chat, "agent-a", ct)).Outcome);
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(Message("1700000000.001001", seed.Erik, "U0ERIK", "the password is hunter2"), ct));
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(Message("1700000000.001002", seed.Dana, "U0DANA", "and the blocking"), ct));

            /* A second thread on the route, with a waiting message from a person still granted. */
            var other = Guid.NewGuid();
            await AddRequestAsync(connection, other, seed.RouteId, "reported", TimeSpan.FromMinutes(30), ct: ct);
            await ExecAsync(connection, $"INSERT INTO config.investigation_request_threads (request_id, channel_id, thread_ts) VALUES ('{other}', '{Channel}', '1700000000.000500')", ct);
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(new SlackPersonMessage(Channel, "1700000000.001003", "1700000000.000500", other, seed.Dana, "slack:" + Team + ":U0DANA", "second thread"), ct));

            /* Erik is disabled while his message waits, and the claim lapses: the chat request is open and unclaimed again. */
            Assert.True((await PersonDirectory.SetPersonEnabledAsync(source, seed.Erik, false, Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'open', claim_id = NULL, claimed_by = NULL, lease_until = NULL WHERE request_id = '{chat}'", ct);

            var posts = new RecordingPosts();
            var routes = new[] { new NotificationRoute(seed.RouteId, "cpu", "", "", "", "", "", Enabled: true) { SlackBotChannel = Channel, AgentMaxPerHour = 10 } };
            var now = DateTime.UtcNow;
            var sweep = new SlackChatSweep(new StoreSlackChatCommands(store, NullLogger.Instance), posts, () => routes, NullLogger.Instance, () => now);

            /* One pass, no exception: Dana's message joins the chat request, Erik's is refused, and the second thread opens. */
            Assert.Equal((1, 0), await sweep.RunOnceAsync(ct));
            Assert.Equal("joined|" + chat, await ScalarAsync(connection, "SELECT state || '|' || request_id::text FROM config.investigation_person_messages WHERE ts = '1700000000.001002'", ct));
            Assert.Equal("refused|", await ScalarAsync(connection, "SELECT state || '|' || COALESCE(request_id::text, '') FROM config.investigation_person_messages WHERE ts = '1700000000.001001'", ct));
            Assert.Equal("3", await ScalarAsync(connection, $"SELECT autonomy_level::text FROM config.investigation_requests WHERE request_id = '{chat}'", ct));
            Assert.Equal(seed.Dana.ToString(System.Globalization.CultureInfo.InvariantCulture), await ScalarAsync(connection, $"SELECT string_agg(person_id::text, ',') FROM config.investigation_request_askers WHERE request_id = '{chat}'", ct));

            /* Erik is nowhere an asker: no person_message row names him, his text is in no journal row and in no person_requests. */
            Assert.Equal("0", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_journal WHERE event = '{InvestigationRequestStore.AskerJournalEvent}' AND detail ->> 'person_id' = '{seed.Erik}'", ct));
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE detail::text LIKE '%hunter2%'", ct));
            Assert.Equal(new[] { "and the blocking" }, (await store.ListPersonRequestsAsync(chat, ct)).Items.Select(r => r.Text).ToArray());

            /* The refusal is the handler's: one sentence to Erik, and a journal row with the identity, no text and no person_id. */
            Assert.Equal((Channel, SlackChatAnswers.NoGrant, ThreadTs), Assert.Single(posts.Posts));
            Assert.Equal("person:" + seed.Erik + "|" + SlackChatAnswers.NoGrantKind + "|slack:" + Team + ":U0ERIK|", await ScalarAsync(connection,
                $"SELECT actor || '|' || reason || '|' || (detail ->> 'identity') || '|' || COALESCE(detail ->> 'text', '') FROM config.investigation_journal WHERE event = '{InvestigationRequestStore.AskerJournalEvent}' AND detail ->> 'ts' = '1700000000.001001'", ct));

            /* The second thread opened for Dana's message, and nothing waits any more. */
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'chat' AND chat_thread_of = '{other}'", ct));
            Assert.Equal("opened", await ScalarAsync(connection, "SELECT state FROM config.investigation_person_messages WHERE ts = '1700000000.001003'", ct));
            Assert.Empty(await store.WaitingThreadsAsync(10, ct));

            /* A second pass has nothing to do and answers nothing more. */
            Assert.Equal((0, 0), await sweep.RunOnceAsync(ct));
            Assert.Single(posts.Posts);
        });
    }

    /// <summary>A message is taken for a chat request only while it still waits (#7659): the handler's open and the sweep's for the same message, in
    /// turn under the route lock, open one request, not two; a join of a message already taken joins nothing and lowers nothing; a mixed list takes
    /// the waiting message alone and stamps by the askers whose messages were taken; a list with no messages still opens; and a message whose asker
    /// is not among the askers is refused before any write.</summary>
    [Fact]
    public async Task AMessageAlreadyTaken_IsNotTakenAgain_SoASecondOpenForItOpensNothing_AndAJoinOfItJoinsNothing()
    {
        await WithStoreAsync(async (connection, _, store, seed, ct) =>
        {
            var message = Message("1700000000.001201", seed.Erik, "U0ERIK", "once");
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(message, ct));
            var erik = new SlackAsker(seed.Erik, message.Identity, 1);
            var first = await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [erik], [message], 10, DateTime.UtcNow, RoutesAt(2), ct);
            Assert.Equal(SlackChatOpenOutcome.Opened, first.Outcome);

            /* The same message again: no second request, no second opened row, the hour's count unchanged, the message still with the first. */
            var second = await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [erik], [message], 10, DateTime.UtcNow, RoutesAt(2), ct);
            Assert.Equal(SlackChatOpenOutcome.NothingWaiting, second.Outcome);
            Assert.Null(second.RequestId);
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_requests WHERE trigger_kind = 'chat'", ct));
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'opened' AND request_id <> '" + s_parent + "'", ct));
            Assert.Equal("opened|" + first.RequestId, await ScalarAsync(connection, "SELECT state || '|' || request_id::text FROM config.investigation_person_messages WHERE ts = '1700000000.001201'", ct));
            Assert.Equal(new SlackChatCounts(1, 1), await store.ChatCountsAsync(s_parent, seed.RouteId, DateTime.UtcNow, ct));

            /* A join of the taken message into another unclaimed chat request: nothing joins, the stamp and the askers are untouched. */
            var other = (await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [], [], 10, DateTime.UtcNow, RoutesAt(3), ct)).RequestId!.Value;
            Assert.False(await store.JoinChatRequestAsync(other, erik, message, ct));
            Assert.Equal("3|0", await ScalarAsync(connection, $"SELECT r.autonomy_level || '|' || (SELECT count(*) FROM config.investigation_request_askers AS a WHERE a.request_id = r.request_id) FROM config.investigation_requests AS r WHERE r.request_id = '{other}'", ct));
            Assert.Equal("opened|" + first.RequestId, await ScalarAsync(connection, "SELECT state || '|' || request_id::text FROM config.investigation_person_messages WHERE ts = '1700000000.001201'", ct));

            /* A mixed list: the taken message stays where it is, the waiting one opens, and the stamp and asker rows follow the message taken. */
            var fresh = Message("1700000000.001202", seed.Dana, "U0DANA", "fresh");
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(fresh, ct));
            var third = await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [erik, new SlackAsker(seed.Dana, fresh.Identity, 3)], [message, fresh], 10, DateTime.UtcNow, RoutesAt(3), ct);
            Assert.Equal(SlackChatOpenOutcome.Opened, third.Outcome);
            Assert.Equal("opened|" + third.RequestId, await ScalarAsync(connection, "SELECT state || '|' || request_id::text FROM config.investigation_person_messages WHERE ts = '1700000000.001202'", ct));
            Assert.Equal("opened|" + first.RequestId, await ScalarAsync(connection, "SELECT state || '|' || request_id::text FROM config.investigation_person_messages WHERE ts = '1700000000.001201'", ct));
            Assert.Equal("3|" + seed.Dana, await ScalarAsync(connection, $"SELECT r.autonomy_level || '|' || (SELECT string_agg(a.person_id::text, ',') FROM config.investigation_request_askers AS a WHERE a.request_id = r.request_id) FROM config.investigation_requests AS r WHERE r.request_id = '{third.RequestId}'", ct));
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{third.RequestId}' AND event = '{InvestigationRequestStore.AskerJournalEvent}'", ct));

            /* No message at all still opens (a follow-up's shape); a message whose asker is not among the askers is refused before any write. */
            Assert.Equal(SlackChatOpenOutcome.Opened, (await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [], [], 10, DateTime.UtcNow, RoutesAt(2), ct)).Outcome);
            var stray = Message("1700000000.001203", seed.Erik, "U0ERIK", "stray");
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(stray, ct));
            var requests = await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_requests", ct);
            await Assert.ThrowsAsync<ArgumentException>(() => store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [new SlackAsker(seed.Dana, "slack:T:U", 3)], [stray], 10, DateTime.UtcNow, RoutesAt(2), ct));
            Assert.Equal(requests, await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_requests", ct));
            Assert.Equal("waiting", await ScalarAsync(connection, "SELECT state FROM config.investigation_person_messages WHERE ts = '1700000000.001203'", ct));
        });
    }

    /// <summary>The claim answer's person_requests (#7659), through the claim tool with the owner store: exactly the granted askers' texts, with
    /// their names and grants, and the rule beside them; a message row for a disabled person or for one with no grant reaching the server is left
    /// out; an alert request nobody asked in carries neither key.</summary>
    [Fact]
    public async Task TheClaimAnswer_CarriesPersonRequestsForExactlyTheGrantedAskers_AndNoneForAnAlertRequest()
    {
        await WithStoreAsync(async (connection, source, store, seed, ct) =>
        {
            var dana = Message("1700000000.001101", seed.Dana, "U0DANA", "force the good plan");
            var erik = Message("1700000000.001102", seed.Erik, "U0ERIK", "and look at blocking");
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(dana, ct));
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(erik, ct));
            var chat = (await store.OpenChatRequestAsync(Thread(seed.RouteId), s_parent, [new SlackAsker(seed.Dana, dana.Identity, 3), new SlackAsker(seed.Erik, erik.Identity, 1)], [dana, erik], 10, DateTime.UtcNow, RoutesAt(3), ct)).RequestId!.Value;

            /* Message rows under the chat request for Pat, disabled, and for Sam, whose ask grant is for another server: never the agent's. */
            await ExecAsync(connection, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (8, 'srv-8', 'host-8')", ct);
            var sam = (await PersonDirectory.AddPersonAsync(source, "Sam", Admin, ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            Assert.True((await PersonDirectory.AddGrantAsync(source, sam, 8, null, PersonCapabilities.Ask, null, Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            await ExecAsync(connection, $"INSERT INTO config.investigation_person_messages (channel_id, ts, thread_ts, thread_request_id, request_id, person_id, identity, message_text, state, received_at) VALUES "
                + $"('{Channel}', '1700000000.001103', '{ThreadTs}', '{s_parent}', '{chat}', {seed.Pat}, 'slack:{Team}:U0PAT', 'pat says drop the table', 'opened', now() AT TIME ZONE 'UTC' + interval '1 second'), "
                + $"('{Channel}', '1700000000.001104', '{ThreadTs}', '{s_parent}', '{chat}', {sam}, 'slack:{Team}:U0SAM', 'sam says the password is hunter2', 'opened', now() AT TIME ZONE 'UTC' + interval '2 seconds')", ct);

            var composer = new InvestigationBriefComposer(new PgInvestigationBriefSourceReader(source), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);
            var answer = JsonNode.Parse(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, chat.ToString(), "agent-a", null, null, ct, ownerStore: store))!.AsObject();
            Assert.Equal("claimed", answer["status"]!.ToString());
            var requests = Assert.IsType<JsonArray>(answer["person_requests"]);
            Assert.Equal(new[] { ("Dana", "force the good plan"), ("Erik", "and look at blocking") }, requests.Select(r => (r!["name"]!.ToString(), r["text"]!.ToString())).ToArray());
            Assert.Equal(new[] { "request_level_3:force_plan" }, requests[0]!["grants"]!.AsArray().Select(g => g!.ToString()).ToArray());
            Assert.Equal(new[] { "ask" }, requests[1]!["grants"]!.AsArray().Select(g => g!.ToString()).ToArray());
            Assert.Equal(DarlingMcpInvestigationTools.PersonRequestsRule, answer["person_requests_rule"]!.ToString());
            Assert.Null(answer["person_requests_omitted"]);
            Assert.DoesNotContain("drop the table", answer.ToJsonString(), StringComparison.Ordinal);
            Assert.DoesNotContain("hunter2", answer.ToJsonString(), StringComparison.Ordinal);

            /* An alert request nobody asked in: the claim answers as before, with neither key. */
            var alert = Guid.NewGuid();
            await AddRequestAsync(connection, alert, seed.RouteId, "open", TimeSpan.FromMinutes(5), ct: ct);
            var plain = JsonNode.Parse(await DarlingMcpInvestigationTools.ClaimAsync(store, composer, alert.ToString(), "agent-a", null, null, ct, ownerStore: store))!.AsObject();
            Assert.Equal("claimed", plain["status"]!.ToString());
            Assert.Null(plain["person_requests"]);
            Assert.Null(plain["person_requests_rule"]);
        });
    }
}

/// <summary>
/// #7659 as the real logins: the asker and message tables are the owner's, with admin reading alone and the mcp login holding no SELECT on the
/// messages; and no login but the owner journals <c>agent_message</c> or <c>person_message</c>. The roles are cluster-wide, so the class is in the
/// <c>pg-cluster-roles</c> collection.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class SlackChatGrantLiveTests
{
    private static readonly string[] s_cleanupRoles = ["admin", "viewer", "web", "mcp"];
    private static readonly Guid s_request = Guid.Parse("76590000-0000-0000-0000-0000000000c1");
    private static readonly string[] s_tables = ["config.investigation_request_askers", "config.investigation_person_messages"];

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the Slack chat grant live tests.");
        return connectionString!;
    }

    private static string LoginConnectionString(ScratchPostgres scratch, string role, string password) =>
        new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = password, Pooling = false }.ConnectionString;

    private static async Task AssertDeniedAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var ex = await Assert.ThrowsAsync<PostgresException>(() => SlackChatStoreLiveTests.ExecAsync(connection, sql, ct));
        Assert.Equal("42501", ex.SqlState);
    }

    private static async Task AssertCatalogAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        foreach (var table in s_tables)
        foreach (var role in s_cleanupRoles)
        {
            var held = new List<string>();
            foreach (var verb in new[] { "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER" })
            {
                if (await SlackChatStoreLiveTests.ScalarAsync(owner, $"SELECT has_table_privilege('{role}', '{table}', '{verb}')", ct) == "True")
                    held.Add(verb);
            }

            Assert.True((role == "admin" ? "SELECT" : "") == string.Join(",", held), $"{role} holds [{string.Join(",", held)}] on {table}");
        }
    }

    [Fact]
    public async Task TheRungAndTheManagedBatch_LeaveTheAskersAndMessagesToTheOwner_AdminReads_McpCannot_AndNoLoginJournalsTheTwoKinds()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            var exist = await SlackChatStoreLiveTests.ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct) == "True";
            Assert.SkipWhen(exist, "A cluster-wide admin/viewer/web/mcp role already exists on this cluster; the grants would adopt it.");

            await PgMigrations.MigrateAsync(owner, ct);
            await SlackChatStoreLiveTests.ExecAsync(owner, DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp, 15, PasswordReassert.All,
                ProvisioningTarget.ComposeStore(new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling", scratch.DatabaseName)), ct);
            await AssertCatalogAsync(owner, ct);

            await RungRewindArtifacts.DropSentinelsAboveAsync(owner, 253, ct);
            await SlackChatStoreLiveTests.ExecAsync(owner, "DELETE FROM darling_schema_version WHERE version > 253", ct);
            Assert.Equal(PgMigrations.Scripts.Count(m => m.Version > 253), await PgMigrations.MigrateAsync(owner, ct));
            await AssertCatalogAsync(owner, ct);

            await SlackChatStoreLiveTests.ExecAsync(owner,
                "INSERT INTO config.investigation_requests (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until, expires_at, state) "
                + $"VALUES ('{s_request}', 1, 7, 'srv-7', 'blocking', 'blocking', now(), now(), now(), now() + interval '4 hours', 'claimed')", ct);
            await SlackChatStoreLiveTests.ExecAsync(owner,
                $"INSERT INTO config.investigation_person_messages (channel_id, ts, thread_ts, thread_request_id, identity, message_text, state) VALUES ('C07659A1', '1700000000.000600', '1700000000.000100', '{s_request}', 'slack:T:U', 'the password is hunter2', 'waiting')", ct);
            var journalBefore = await SlackChatStoreLiveTests.ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct);

            foreach (var (role, password) in new[] { ("admin", ProvisioningTestSecrets.AdminPassword), ("viewer", ProvisioningTestSecrets.ViewerPassword), ("mcp", ProvisioningTestSecrets.McpPassword) })
            {
                await using var login = new NpgsqlConnection(LoginConnectionString(scratch, role, password));
                await login.OpenAsync(ct);
                foreach (var table in s_tables)
                {
                    await AssertDeniedAsync(login, $"DELETE FROM {table}", ct);
                    await AssertDeniedAsync(login, $"UPDATE {table} SET request_id = NULL", ct);
                }

                await AssertDeniedAsync(login, $"INSERT INTO config.investigation_request_askers (request_id, person_id, identity, request_level) VALUES ('{s_request}', 1, 'x', 1)", ct);
                await AssertDeniedAsync(login, $"INSERT INTO config.investigation_person_messages (channel_id, ts, thread_ts, thread_request_id, identity, message_text, state) VALUES ('C', '1', '1', '{s_request}', 'x', 'y', 'waiting')", ct);
                if (role == "admin")
                    Assert.Equal("the password is hunter2", await SlackChatStoreLiveTests.ScalarAsync(login, "SELECT message_text FROM config.investigation_person_messages", ct));
                else
                    await AssertDeniedAsync(login, "SELECT count(*) FROM config.investigation_person_messages", ct);

                foreach (var kind in new[] { InvestigationRequestStore.AgentMessage, InvestigationRequestStore.AskerJournalEvent })
                {
                    var journal = await Assert.ThrowsAsync<PostgresException>(() => SlackChatStoreLiveTests.ExecAsync(login,
                        $"INSERT INTO config.investigation_journal (request_id, event, actor, reason, detail) VALUES ('{s_request}', '{kind}', 'agent:eve', NULL, '{{\"text\":\"forged\"}}'::jsonb)", ct));
                    Assert.Equal("42501", journal.SqlState);
                    if (role is "admin" or "mcp")
                        Assert.Contains("Only the Darling service journals the request event " + kind, journal.MessageText, StringComparison.Ordinal);
                }

                /* The store's own reads as the login: the owner-only reads fail for mcp and viewer, and the agent message cannot be forged through the store either. */
                await using var source = NpgsqlDataSource.Create(LoginConnectionString(scratch, role, password));
                var store = new InvestigationRequestStore(source, 30);
                if (role != "admin")
                    Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() => store.ListPersonRequestsAsync(s_request, ct))).SqlState);
            }

            Assert.Equal(journalBefore, await SlackChatStoreLiveTests.ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(s_cleanupRoles, "slackchat7659", cleanupCt));
        }
    }

    /* A provisioned scratch store (the managed roles as the real logins) with one claimed chat request: Dana, holding request_level_3 for force_plan
       alone on the server, asked in it. The body gets the owner connection and source, the chat request, its claim, and the mcp login's source. */
    private static async Task WithProvisionedChatRequestAsync(Func<NpgsqlConnection, NpgsqlDataSource, NpgsqlDataSource, Guid, Guid, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            var exist = await SlackChatStoreLiveTests.ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct) == "True";
            Assert.SkipWhen(exist, "A cluster-wide admin/viewer/web/mcp role already exists on this cluster; the grants would adopt it.");
            await PgMigrations.MigrateAsync(owner, ct);
            await SlackChatStoreLiveTests.ExecAsync(owner, DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp, 15, PasswordReassert.All,
                ProvisioningTarget.ComposeStore(new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling", scratch.DatabaseName)), ct);

            await SlackChatStoreLiveTests.ExecAsync(owner, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await SlackChatStoreLiveTests.ExecAsync(owner, $"INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ({SlackChatStoreLiveTests.ServerId}, 'srv-7', 'host-7')", ct);
            var routeId = int.Parse((await SlackChatStoreLiveTests.ScalarAsync(owner,
                $"INSERT INTO config.config_notification_routes (metric_match, slack_url, slack_bot_channel, enabled, agent_level, agent_max_per_hour) VALUES ('cpu', 'sealed-cpu', '{SlackChatStoreLiveTests.Channel}', true, 3, 3) RETURNING route_id", ct))!,
                System.Globalization.CultureInfo.InvariantCulture);
            await SlackChatStoreLiveTests.AddRequestAsync(owner, SlackChatStoreLiveTests.Parent, routeId, "reported", TimeSpan.FromHours(1), ct: ct);
            await SlackChatStoreLiveTests.ExecAsync(owner, $"INSERT INTO config.investigation_request_threads (request_id, channel_id, thread_ts) VALUES ('{SlackChatStoreLiveTests.Parent}', '{SlackChatStoreLiveTests.Channel}', '{SlackChatStoreLiveTests.ThreadTs}')", ct);
            await using var ownerSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var dana = (await PersonDirectory.AddPersonAsync(ownerSource, "Dana", SlackChatStoreLiveTests.Admin, ServerActionSurfaces.Viewer, ct)).PersonId!.Value;
            Assert.True((await PersonDirectory.AddGrantAsync(ownerSource, dana, SlackChatStoreLiveTests.ServerId, null, PersonCapabilities.RequestLevel3, [ServerActionNames.ForcePlan], SlackChatStoreLiveTests.Admin, ServerActionSurfaces.Viewer, ct)).Succeeded);
            var store = new InvestigationRequestStore(ownerSource, 30);
            var message = SlackChatStoreLiveTests.Message("1700000000.000701", dana, "U0DANA", "kill the blocking session");
            Assert.Equal(SlackPersonMessageSaved.Saved, await store.SavePersonMessageAsync(message, ct));
            var chat = (await store.OpenChatRequestAsync(SlackChatStoreLiveTests.Thread(routeId), SlackChatStoreLiveTests.Parent, [new SlackAsker(dana, message.Identity, 3)], [message], 10, DateTime.UtcNow, SlackChatStoreLiveTests.RoutesAt(3), ct)).RequestId!.Value;
            var claim = (await store.ClaimAsync(chat, "agent-a", ct)).ClaimId!.Value;

            await using var mcpSource = NpgsqlDataSource.Create(LoginConnectionString(scratch, "mcp", ProvisioningTestSecrets.McpPassword));
            await body(owner, ownerSource, mcpSource, chat, claim, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(s_cleanupRoles, "slackchat7659", cleanupCt));
        }
    }

    /// <summary>The gate's asker kind check reads the owner-only asker table, so it runs on the owner's source (#7659): a store whose own login is
    /// mcp with the owner as its agent-action source, as the Level 3 tools build theirs, refuses a kill under a chat request as <c>asker_grant</c>
    /// and journals the refusal, passes a plan force to the next condition, and the mcp login still cannot read the table.</summary>
    [Fact]
    public async Task TheAskerKindCheck_RunsOnTheOwnerSource_SoAnMcpStoreRefusesAsAskerGrantAndJournalsIt_WhileMcpCannotReadTheAskers()
    {
        await WithProvisionedChatRequestAsync(async (owner, ownerSource, mcpSource, chat, claim, ct) =>
        {
            var mcpStore = new InvestigationRequestStore(mcpSource, 30, agentActionSource: ownerSource);
            var hooks = new ServerActionHooks
            {
                LoadRoutes = SlackChatStoreLiveTests.RoutesAt(3),
                ReadCredential = (_, _) => throw new InvalidOperationException("the credential is read after the asker check"),
            };

            var kill = await mcpStore.CheckServerActionAsync(SlackChatStoreLiveTests.Call(chat, claim, ServerActionNames.KillSession), hooks, ct);
            Assert.Equal(AgentActionCodes.AskerGrant, kill?.Reason);
            Assert.Equal("1", await SlackChatStoreLiveTests.ScalarAsync(owner, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{chat}' AND event = 'refused' AND reason LIKE '%{AgentActionCodes.AskerGrant}%'", ct));

            var force = await mcpStore.CheckServerActionAsync(SlackChatStoreLiveTests.Call(chat, claim, ServerActionNames.ForcePlan), hooks, ct);
            Assert.NotEqual(AgentActionCodes.AskerGrant, force?.Reason);
            Assert.NotEqual(AgentActionCodes.NotLevel3, force?.Reason);

            await using var mcp = await mcpSource.OpenConnectionAsync(ct);
            await AssertDeniedAsync(mcp, "SELECT count(*) FROM config.investigation_request_askers", ct);
        });
    }

    /// <summary>The agent's message row is the owner's (#7659): a store whose own login is mcp posts it through the owner's source, as the Level 2
    /// tools write their rows, and the same store with no owner's source fails closed with no row written.</summary>
    [Fact]
    public async Task TheAgentsMessage_IsWrittenOnTheOwnerSource_SoAnMcpStorePostsIt_AndOneWithoutTheOwnerFailsClosed()
    {
        await WithProvisionedChatRequestAsync(async (owner, ownerSource, mcpSource, chat, claim, ct) =>
        {
            var mcpStore = new InvestigationRequestStore(mcpSource, 30, agentActionSource: ownerSource);
            var answer = JsonNode.Parse(await DarlingMcpInvestigationTools.PostMessageAsync(mcpStore, chat.ToString(), claim.ToString(), "Looking now.", ct))!;
            Assert.Equal("posted", answer["status"]?.ToString());
            Assert.Equal("agent_message|agent:agent-a", await SlackChatStoreLiveTests.ScalarAsync(owner, $"SELECT event || '|' || actor FROM config.investigation_journal WHERE journal_id = {answer["journal_id"]}", ct));

            var bare = new InvestigationRequestStore(mcpSource, 30);
            var refused = JsonNode.Parse(await DarlingMcpInvestigationTools.PostMessageAsync(bare, chat.ToString(), claim.ToString(), "Again.", ct))!;
            Assert.NotEqual("posted", refused["status"]?.ToString());
            Assert.Equal("1", await SlackChatStoreLiveTests.ScalarAsync(owner, $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{chat}' AND event = '{InvestigationRequestStore.AgentMessage}'", ct));
        });
    }
}
