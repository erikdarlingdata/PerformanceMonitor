/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>#7656: the viewer's probe arm for V252, pinned without a live store.</summary>
public sealed class SlackRequestThreadProbeTests
{
    /// <summary>
    /// The thread table is the V252 sentinel and the top arm: a store with every sentinel maps to the build's schema version, and the same
    /// store without the thread table maps to 251. Built by reflection, as the full-sentinel pin in <c>ViewerDataServiceTests</c> is, so
    /// the arity tracks the signature and the arm is found by its parameter name rather than by its ordinal.
    /// </summary>
    [Fact]
    public void TheProbe_MapsTheThreadTableTo252_AndItsAbsenceTo251()
    {
        var map = typeof(ViewerDataService).GetMethod(
            nameof(ViewerDataService.MapProbedSchemaVersion),
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var parameters = map.GetParameters();
        var threads = Array.FindIndex(parameters, p => p.Name == "hasSlackBotThreads");
        var cards = Array.FindIndex(parameters, p => p.Name == "hasSlackCards");
        Assert.True(threads >= 0, "the probe has a hasSlackBotThreads sentinel");
        Assert.True(cards >= 0, "the probe has a hasSlackCards sentinel (#7658)");
        Assert.Equal(parameters.Length - 1, cards);
        Assert.Equal(parameters.Length - 2, threads);

        var allTrue = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(253, (int)map.Invoke(null, allTrue)!);
        Assert.Equal(StorageVersion.SchemaVersion, (int)map.Invoke(null, allTrue)!);

        var withoutCards = (object[])allTrue.Clone();
        withoutCards[cards] = false;
        Assert.Equal(252, (int)map.Invoke(null, withoutCards)!);
        var withoutThreads = (object[])withoutCards.Clone();
        withoutThreads[threads] = false;
        Assert.Equal(251, (int)map.Invoke(null, withoutThreads)!);
    }
}

/// <summary>
/// #7656 on a live store: the request thread table (V252 <c>config.investigation_request_threads</c>) round-trips a request's channel and
/// parent ts, keeps the first writer's row, answers null for a request without one, marks each post once under its once key as a
/// <c>slack_thread_posted</c> journal row, and goes with its request.
/// </summary>
/* #1776 own-store: each test mints its own scratch database through ScratchPostgres. */
public sealed class SlackRequestThreadStoreLiveTests
{
    private static readonly Guid s_request = Guid.Parse("76560000-0000-0000-0000-0000000000b1");
    private static readonly Guid s_other = Guid.Parse("76560000-0000-0000-0000-0000000000b2");

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the Slack request thread store live tests.");
        return connectionString!;
    }

    private static async Task WithStoreAsync(Func<NpgsqlConnection, ISlackRequestThreadStore, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await ActionProposalStoreLiveTests.AddRequestAsync(connection, s_request, "claimed", ct);
            await ActionProposalStoreLiveTests.AddRequestAsync(connection, s_other, "claimed", ct);
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(connection, new InvestigationRequestStore(source, 30), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ASavedThread_IsFoundByItsRequest_TheFirstWriterWins_AndAnUnknownRequestHasNone()
    {
        await WithStoreAsync(async (connection, store, ct) =>
        {
            Assert.Null(await store.FindAsync(s_request, ct));

            await store.SaveAsync(new SlackRequestThread(s_request, "C0765600001", "1700000000.000100"), ct);
            var found = await store.FindAsync(s_request, ct);
            Assert.NotNull(found);
            Assert.Equal(s_request, found!.RequestId);
            Assert.Equal("C0765600001", found.ChannelId);
            Assert.Equal("1700000000.000100", found.ThreadTs);

            /* A second save for the same request, another channel and ts: the first row stays. */
            await store.SaveAsync(new SlackRequestThread(s_request, "C0765600002", "1700000000.000200"), ct);
            var again = await store.FindAsync(s_request, ct);
            Assert.Equal("C0765600001", again!.ChannelId);
            Assert.Equal("1700000000.000100", again.ThreadTs);
            Assert.Equal("1", await ActionProposalStoreLiveTests.ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_request_threads WHERE request_id = '{s_request}'", ct));

            /* Another request's thread is its own; a request with no thread, and a request that does not exist, have none. */
            await store.SaveAsync(new SlackRequestThread(s_other, "C0765600003", "1700000000.000300"), ct);
            Assert.Equal("C0765600003", (await store.FindAsync(s_other, ct))!.ChannelId);
            Assert.Equal("C0765600001", (await store.FindAsync(s_request, ct))!.ChannelId);
            Assert.Null(await store.FindAsync(Guid.Parse("76560000-0000-0000-0000-0000000000ff"), ct));
        });
    }

    [Fact]
    public async Task TheOnceMark_IsWrittenByTheFirstCallOnly_AsASlackThreadPostedJournalRow()
    {
        await WithStoreAsync(async (connection, store, ct) =>
        {
            var onceKey = $"slack_thread:{s_request:D}:opened";
            Assert.True(await store.MarkOnceAsync(s_request, 1, 7, "opened", onceKey, ct));
            Assert.False(await store.MarkOnceAsync(s_request, 1, 7, "opened", onceKey, ct));

            /* Another post of the same request has its own key, and its own first write. */
            Assert.True(await store.MarkOnceAsync(s_request, 1, 7, "evidence_ready", $"slack_thread:{s_request:D}:evidence_ready", ct));

            Assert.Equal("1", await ActionProposalStoreLiveTests.ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE event = '{InvestigationRequestStore.SlackThreadPosted}' AND once_key = '{onceKey}'", ct));
            Assert.Equal("slack_thread_posted|service|opened|1|7", await ActionProposalStoreLiveTests.ScalarAsync(connection,
                $"SELECT event || '|' || actor || '|' || reason || '|' || route_id || '|' || server_id FROM config.investigation_journal WHERE once_key = '{onceKey}'", ct));
            Assert.Equal("2", await ActionProposalStoreLiveTests.ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{s_request}' AND event = '{InvestigationRequestStore.SlackThreadPosted}'", ct));
        });
    }

    [Fact]
    public async Task ReleasingTheMark_DeletesTheServicesOwnRow_AndTheNextMarkIsWrittenAgain()
    {
        await WithStoreAsync(async (connection, store, ct) =>
        {
            var onceKey = $"slack_thread:{s_request:D}:request";
            Assert.True(await store.MarkOnceAsync(s_request, 1, 7, "request", onceKey, ct));
            Assert.False(await store.MarkOnceAsync(s_request, 1, 7, "request", onceKey, ct));

            /* The release takes the mark row away: the owner-row trigger guards inserts, and the service owns the table. */
            await store.ReleaseMarkAsync(onceKey, ct);
            Assert.Equal("0", await ActionProposalStoreLiveTests.ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE once_key = '{onceKey}'", ct));

            /* The next pass writes the mark again, so the post goes out then. */
            Assert.True(await store.MarkOnceAsync(s_request, 1, 7, "request", onceKey, ct));
            Assert.Equal("1", await ActionProposalStoreLiveTests.ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE once_key = '{onceKey}' AND event = '{InvestigationRequestStore.SlackThreadPosted}'", ct));

            /* A release names one key: another post's mark, and a row of another kind under a once key, stay. */
            var other = $"slack_thread:{s_request:D}:claimed:5";
            Assert.True(await store.MarkOnceAsync(s_request, 1, 7, "claimed:5", other, ct));
            await store.ReleaseMarkAsync(onceKey, ct);
            Assert.Equal("1", await ActionProposalStoreLiveTests.ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE once_key = '{other}'", ct));
            await ActionProposalStoreLiveTests.ExecAsync(connection,
                $"INSERT INTO config.investigation_journal (request_id, event, actor, once_key) VALUES ('{s_request}', 'report_notice_sent', 'service', 'slack_thread:{s_request:D}:other_kind')", ct);
            await store.ReleaseMarkAsync($"slack_thread:{s_request:D}:other_kind", ct);
            Assert.Equal("1", await ActionProposalStoreLiveTests.ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE once_key = 'slack_thread:{s_request:D}:other_kind'", ct));
        });
    }

    [Fact]
    public async Task DeletingTheRequest_RemovesItsThread()
    {
        await WithStoreAsync(async (connection, store, ct) =>
        {
            await store.SaveAsync(new SlackRequestThread(s_request, "C0765600001", "1700000000.000100"), ct);
            await store.SaveAsync(new SlackRequestThread(s_other, "C0765600003", "1700000000.000300"), ct);

            await ActionProposalStoreLiveTests.ExecAsync(connection, $"DELETE FROM config.investigation_requests WHERE request_id = '{s_request}'", ct);

            Assert.Null(await store.FindAsync(s_request, ct));
            Assert.Equal("0", await ActionProposalStoreLiveTests.ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_request_threads WHERE request_id = '{s_request}'", ct));
            Assert.Equal("C0765600003", (await store.FindAsync(s_other, ct))!.ChannelId);
        });
    }

    private static async Task<int> AddRouteAsync(NpgsqlConnection connection, string metric, string channel, bool enabled, CancellationToken ct) =>
        int.Parse(await ActionProposalStoreLiveTests.ScalarAsync(connection,
            $"INSERT INTO config.config_notification_routes (metric_match, slack_url, slack_bot_channel, enabled) VALUES ('{metric}', 'sealed-{metric}', '{channel}', {enabled}) RETURNING route_id", ct),
            System.Globalization.CultureInfo.InvariantCulture);

    private static Task AddRequestAsync(NpgsqlConnection connection, Guid requestId, int routeId, string openedAt, string? practiceOf, CancellationToken ct) =>
        ActionProposalStoreLiveTests.ExecAsync(connection,
            $"INSERT INTO config.investigation_requests (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until, expires_at, opened_at, max_severity, trigger_kind, practice_of) "
            + $"VALUES ('{requestId}', {routeId}, 7656, 'srv-7656', 'blocking', 'Blocking', {openedAt}, {openedAt}, now() + interval '1 hour', now() + interval '1 hour', {openedAt}, 'critical', 'alert', {(practiceOf is null ? "NULL" : $"'{practiceOf}'")})", ct);

    private static async Task<long> JournalAsync(NpgsqlConnection connection, Guid requestId, int routeId, string eventKind, string actor, string? reason, string? detailJson, string eventTime, CancellationToken ct) =>
        long.Parse(await ActionProposalStoreLiveTests.ScalarAsync(connection,
            $"INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, reason, detail) VALUES ({eventTime}, '{requestId}', {routeId}, 7656, '{eventKind}', '{actor}', {(reason is null ? "NULL" : $"'{reason}'")}, {(detailJson is null ? "NULL" : $"'{detailJson}'::jsonb")}) RETURNING journal_id", ct),
            System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>
    /// The pending read against the store: both arms (the post kinds, and an <c>action_called</c> row older than ten minutes with no
    /// result as <c>action_unknown</c>), in journal order, with the claim's number among the request's claims, the called row's detail on
    /// a change's result row, the report's verdict and root cause on the reported row, the request's opened time, and the exclusions: a
    /// kind off the post list, a row with its mark, a practice request, a disabled route's request, a route with no channel, a called
    /// row whose result came, and a row older than the window. The limit cuts the list in order.
    /// </summary>
    [Fact]
    public async Task ThePendingRead_ReturnsBothArmsInJournalOrder_WithTheClaimNumber_AndLeavesOutPracticeDisabledMarkedAndOldRows()
    {
        await WithStoreAsync(async (connection, threads, ct) =>
        {
            var rows = (ISlackThreadRowSource)threads;
            /* The fixture's two requests sit on a route id this test does not control: out of the way. */
            await ActionProposalStoreLiveTests.ExecAsync(connection, $"DELETE FROM config.investigation_requests WHERE request_id IN ('{s_request}', '{s_other}')", ct);
            var live = await AddRouteAsync(connection, "Blocking", "C0765600LIVE", enabled: true, ct);
            var disabled = await AddRouteAsync(connection, "CPU", "C0765600OFF", enabled: false, ct);
            var noChannel = await AddRouteAsync(connection, "Jobs", "", enabled: true, ct);
            var request = Guid.Parse("76560000-0000-0000-0000-0000000000c1");
            var practice = Guid.Parse("76560000-0000-0000-0000-0000000000c2");
            var offRoute = Guid.Parse("76560000-0000-0000-0000-0000000000c3");
            var noChannelRequest = Guid.Parse("76560000-0000-0000-0000-0000000000c4");
            await AddRequestAsync(connection, request, live, "now() - interval '30 minutes'", null, ct);
            await AddRequestAsync(connection, practice, live, "now() - interval '30 minutes'", request.ToString(), ct);
            await AddRequestAsync(connection, offRoute, disabled, "now() - interval '30 minutes'", null, ct);
            await AddRequestAsync(connection, noChannelRequest, noChannel, "now() - interval '30 minutes'", null, ct);

            var old = await JournalAsync(connection, request, live, "closed", "person:erik", "stale", null, "now() - interval '8 days'", ct);
            var opened = await JournalAsync(connection, request, live, "opened", "service", "Blocking", null, "now() - interval '30 minutes'", ct);
            await JournalAsync(connection, request, live, "joined", "service", "CPU", null, "now() - interval '29 minutes'", ct);
            var firstClaim = await JournalAsync(connection, request, live, "claimed", "agent:atlas", null, null, "now() - interval '28 minutes'", ct);
            var unknownCall = await JournalAsync(connection, request, live, "action_called", "agent:atlas", null, """{"tool":"kill_session","target":{"id":"session 87","database":"Sales"}}""", "now() - interval '20 minutes'", ct);
            var answeredCall = await JournalAsync(connection, request, live, "action_called", "agent:atlas", null, """{"tool":"evict_plan","target":{"id":"plan 0x1"}}""", "now() - interval '19 minutes'", ct);
            var done = await JournalAsync(connection, request, live, "action_done", "agent:atlas", null, $$$"""{"called_id":{{{answeredCall}}}}""", "now() - interval '18 minutes'", ct);
            var secondClaim = await JournalAsync(connection, request, live, "claimed", "agent:beta", null, null, "now() - interval '15 minutes'", ct);
            var youngCall = await JournalAsync(connection, request, live, "action_called", "agent:beta", null, """{"tool":"set_config"}""", "now() - interval '2 minutes'", ct);
            var reported = await JournalAsync(connection, request, live, "reported", "agent:beta", "needs_person", null, "now() - interval '5 minutes'", ct);
            await ActionProposalStoreLiveTests.ExecAsync(connection,
                $"INSERT INTO config.investigation_reports (request_id, report_format, agent_id, claim_id, verdict, root_cause, evidence, server_id) VALUES ('{request}', 1, 'agent:beta', '{Guid.NewGuid()}', 'needs_person', 'A plan regressed.', '[{{\"k\":1}}]'::jsonb, 7656)", ct);
            var resolved = await JournalAsync(connection, request, live, "resolved", "service", "Blocking", null, "now() - interval '1 minute'", ct);
            await JournalAsync(connection, practice, live, "opened", "service", "Blocking", null, "now() - interval '30 minutes'", ct);
            await JournalAsync(connection, offRoute, disabled, "opened", "service", "CPU", null, "now() - interval '30 minutes'", ct);
            await JournalAsync(connection, noChannelRequest, noChannel, "opened", "service", "Jobs", null, "now() - interval '30 minutes'", ct);
            Assert.True(await threads.MarkOnceAsync(request, live, 7656, $"claimed:{firstClaim}", $"slack_thread:{request:D}:claimed:{firstClaim}", ct));

            var now = DateTime.UtcNow;
            var pending = await rows.PendingSlackThreadRowsAsync(PerformanceMonitor.Darling.Service.Chat.SlackThreadPass.PostedEvents, now.AddDays(-7), now.AddMinutes(-10), 100, ct);

            Assert.Equal(
                [$"request|{opened}", $"action_unknown:{unknownCall}|{unknownCall}", $"action_done:{done}|{done}", $"claimed:{secondClaim}|{secondClaim}", $"reported:{reported}|{reported}", $"resolved:{resolved}|{resolved}"],
                pending.Select(r => $"{r.PostKey}|{r.JournalId}").ToArray());
            Assert.All(pending, r =>
            {
                Assert.Equal(request, r.RequestId);
                Assert.Equal(live, r.RouteId);
                Assert.Equal(7656, r.ServerId);
                Assert.Equal("srv-7656", r.ServerName);
                Assert.Equal("Blocking", r.Metric);
                Assert.Equal("critical", r.Severity);
                Assert.Equal("alert", r.Trigger);
                Assert.Null(r.FollowUpOf);
                Assert.Equal(DateTimeKind.Utc, r.OpenedAtUtc.Kind);
                Assert.InRange(r.OpenedAtUtc, now.AddMinutes(-31), now.AddMinutes(-29));
            });
            var unknown = pending[1];
            Assert.Equal("action_unknown", unknown.Event);
            Assert.Contains("session 87", unknown.DetailJson, StringComparison.Ordinal);
            Assert.Contains("evict_plan", pending[2].CalledDetailJson, StringComparison.Ordinal);
            Assert.Equal(2, pending[3].ClaimNo);
            Assert.Equal(0, pending[2].ClaimNo);
            Assert.Equal("needs_person", pending[4].Verdict);
            Assert.Equal("A plan regressed.", pending[4].RootCause);
            Assert.Null(pending[5].Verdict);
            Assert.DoesNotContain(pending, r => r.JournalId == youngCall || r.JournalId == answeredCall || r.JournalId == firstClaim || r.JournalId == old);

            /* The limit takes the first rows in order; the window reaches the old row when it is widened; a sent mark on the parent takes it out. */
            Assert.Equal([$"request|{opened}", $"action_unknown:{unknownCall}|{unknownCall}"],
                (await rows.PendingSlackThreadRowsAsync(PerformanceMonitor.Darling.Service.Chat.SlackThreadPass.PostedEvents, now.AddDays(-7), now.AddMinutes(-10), 2, ct)).Select(r => $"{r.PostKey}|{r.JournalId}").ToArray());
            Assert.Equal($"closed:{old}", (await rows.PendingSlackThreadRowsAsync(PerformanceMonitor.Darling.Service.Chat.SlackThreadPass.PostedEvents, now.AddDays(-9), now.AddMinutes(-10), 100, ct))[0].PostKey);
            Assert.True(await threads.MarkOnceAsync(request, live, 7656, "request", $"slack_thread:{request:D}:request", ct));
            Assert.DoesNotContain(await rows.PendingSlackThreadRowsAsync(PerformanceMonitor.Darling.Service.Chat.SlackThreadPass.PostedEvents, now.AddDays(-7), now.AddMinutes(-10), 100, ct), r => r.PostKey == "request");
        });
    }
}

/// <summary>
/// #7656: only the store owner writes the request thread table and admin alone reads it, in both grant sources (the managed batch's
/// section 10e5 and the bring-your-own <c>provision-roles.sql</c> step 3j5) and from the rung alone, as the real <c>admin</c>, <c>viewer</c>,
/// <c>web</c> and <c>mcp</c> logins. The journal trigger keeps the <c>slack_thread_posted</c> mark to the owner too. The fixed-name roles are
/// cluster-wide, so the class is in the <c>pg-cluster-roles</c> collection.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class SlackRequestThreadGrantLiveTests
{
    /* #1776 own-store: each test mints its own scratch database through ScratchPostgres; the fixed admin/viewer/web/mcp roles are why the
       class sits in the pg-cluster-roles collection. */
    private static readonly string[] s_cleanupRoles = ["admin", "viewer", "web", "mcp"];
    private static readonly Guid s_request = Guid.Parse("76560000-0000-0000-0000-0000000000a1");
    private const string Table = "config.investigation_request_threads";
    private const int RungBelow = 251;

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the Slack request thread grant live tests.");
        return connectionString!;
    }

    private static string LoginConnectionString(ScratchPostgres scratch, string role, string password) =>
        new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = role, Password = password, Pooling = false }.ConnectionString;

    private static async Task<NpgsqlConnection> LoginAsync(ScratchPostgres scratch, string role, string password, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(LoginConnectionString(scratch, role, password));
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task AssertDeniedAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var ex = await Assert.ThrowsAsync<PostgresException>(() => ActionProposalStoreLiveTests.ExecAsync(connection, sql, ct));
        Assert.Equal("42501", ex.SqlState);
    }

    /// <summary>The catalog's answer, role by verb: admin holds SELECT and nothing else, and the other three roles hold nothing.</summary>
    private static async Task AssertCatalogAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        foreach (var role in s_cleanupRoles)
        {
            var held = string.Join(",", new[] { "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER" }
                .Where(verb => ActionProposalStoreLiveTests.ScalarAsync(owner,
                    $"SELECT has_table_privilege('{role}', '{Table}', '{verb}')", ct).GetAwaiter().GetResult() == "True"));
            Assert.Equal(role == "admin" ? "SELECT" : "", held);
        }
    }

    /// <summary>
    /// As the real logins: admin reads the table and cannot write it, directly or through the store; each other login cannot read it;
    /// no login writes the <c>slack_thread_posted</c> mark. The owner's row and the journal are as they were afterwards.
    /// </summary>
    private static async Task AssertOwnerOnlyAsync(NpgsqlConnection owner, ScratchPostgres scratch, (string Role, string Password)[] logins, CancellationToken ct)
    {
        await AssertCatalogAsync(owner, ct);

        await ActionProposalStoreLiveTests.AddRequestAsync(owner, s_request, "claimed", ct);
        await using var ownerSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await new InvestigationRequestStore(ownerSource, 30).SaveAsync(new SlackRequestThread(s_request, "C07656A1", "1700000000.000001"), ct);
        var journalBefore = await ActionProposalStoreLiveTests.ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct);

        foreach (var (role, password) in logins)
        {
            await using var login = await LoginAsync(scratch, role, password, ct);
            await AssertDeniedAsync(login,
                $"INSERT INTO {Table} (request_id, channel_id, thread_ts) VALUES ('{s_request}', 'C07656EVE', '1700000000.000009') ON CONFLICT (request_id) DO NOTHING", ct);
            await AssertDeniedAsync(login, $"UPDATE {Table} SET channel_id = 'C07656EVE' WHERE request_id = '{s_request}'", ct);
            await AssertDeniedAsync(login, $"DELETE FROM {Table}", ct);
            if (role == "admin")
                Assert.Equal("C07656A1", await ActionProposalStoreLiveTests.ScalarAsync(login, $"SELECT channel_id FROM {Table} WHERE request_id = '{s_request}'", ct));
            else
                await AssertDeniedAsync(login, $"SELECT count(*) FROM {Table}", ct);

            /* The journal keeps the post mark to the owner: a login that wrote it first would silence the post. admin and mcp hold the
               journal writer's column INSERT, so the owner-row trigger refuses them by the event's name; viewer and web hold no INSERT on
               the journal at all, so the table refuses them before any trigger runs. */
            var journal = await Assert.ThrowsAsync<PostgresException>(() => ActionProposalStoreLiveTests.ExecAsync(login,
                $"INSERT INTO config.investigation_journal (request_id, event, actor, detail) VALUES ('{s_request}', '{InvestigationRequestStore.SlackThreadPosted}', 'person:eve', '{{}}'::jsonb)", ct));
            Assert.Equal("42501", journal.SqlState);
            Assert.Contains(role is "admin" or "mcp"
                ? "Only the Darling service journals the request event " + InvestigationRequestStore.SlackThreadPosted
                : "permission denied for table investigation_journal", journal.MessageText, StringComparison.Ordinal);

            /* The store on the login's own data source: admin finds the thread and no login saves one or marks a post. */
            await using var source = NpgsqlDataSource.Create(LoginConnectionString(scratch, role, password));
            ISlackRequestThreadStore store = new InvestigationRequestStore(source, 30);
            if (role == "admin")
                Assert.Equal("1700000000.000001", (await store.FindAsync(s_request, ct))!.ThreadTs);
            else
                Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() => store.FindAsync(s_request, ct))).SqlState);
            Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() =>
                store.SaveAsync(new SlackRequestThread(Guid.Parse("76560000-0000-0000-0000-0000000000a2"), "C07656EVE", "1700000000.000009"), ct))).SqlState);
            Assert.Equal("42501", (await Assert.ThrowsAsync<PostgresException>(() =>
                store.MarkOnceAsync(s_request, 1, 7, "opened", $"slack_thread:{s_request}:opened:{role}", ct))).SqlState);
        }

        Assert.Equal("C07656A1|1700000000.000001", await ActionProposalStoreLiveTests.ScalarAsync(owner,
            $"SELECT string_agg(channel_id || '|' || thread_ts, ',') FROM {Table}", ct));
        Assert.Equal(journalBefore, await ActionProposalStoreLiveTests.ScalarAsync(owner, "SELECT count(*) FROM config.investigation_journal", ct));
    }

    private static async Task<NpgsqlConnection> MigratedOwnerAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        var exist = await ActionProposalStoreLiveTests.ScalarAsync(owner,
            "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp'))", ct) == "True";
        Assert.SkipWhen(exist, "A cluster-wide admin/viewer/web/mcp role already exists on this cluster; the grants would adopt it.");
        return owner;
    }

    private static Task ManagedBatchAsync(NpgsqlConnection owner, ScratchPostgres scratch, CancellationToken ct) =>
        ActionProposalStoreLiveTests.ExecAsync(owner,
            DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All,
                ProvisioningTarget.ComposeStore(new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling", scratch.DatabaseName)), ct);

    /// <summary>The shipped script, whole, its database-level statements retargeted at the scratch store and its passwords filled in. The
    /// retarget matters on a shared cluster (#5618): the script's <c>REVOKE ALL ON DATABASE darling FROM PUBLIC</c> names the database by
    /// name, and database privileges live in the cluster's shared catalog, so run as written it would take CONNECT from every later test.</summary>
    private static async Task<string> ShippedScriptAsync(NpgsqlConnection owner, (string Role, string Password)[] logins, CancellationToken ct)
    {
        Assert.SkipUnless(await ActionProposalStoreLiveTests.ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'darling')", ct) == "True",
            "The script names the store owner 'darling', and this cluster has no such role.");
        var database = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Database;
        var script = ComposeStoreRolesLiveTests.WithoutPsqlMetaCommands(
            System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "provision-roles.sql")))
            .Replace("ON DATABASE darling", $"ON DATABASE \"{database}\"", StringComparison.Ordinal);
        foreach (var (role, password) in logins)
            script = script.Replace("CHANGE_ME_" + role.ToUpperInvariant() + "_PASSWORD", password, StringComparison.Ordinal);
        return script;
    }

    /// <summary>Back behind V252 and through it again: the rung recreates the table under the default privileges provisioning left.</summary>
    private static async Task RerunTheRungAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        await RungRewindArtifacts.DropSentinelsAboveAsync(owner, RungBelow, ct);
        await ActionProposalStoreLiveTests.ExecAsync(owner, "DELETE FROM darling_schema_version WHERE version > " + RungBelow, ct);
        Assert.Equal(StorageVersion.SchemaVersion - RungBelow, await PgMigrations.MigrateAsync(owner, ct));
        Assert.Equal("True", await ActionProposalStoreLiveTests.ScalarAsync(owner, $"SELECT to_regclass('{Table}') IS NOT NULL", ct));
    }

    private static async Task CleanUpAsync(ScratchPostgres scratch, bool bodySucceeded) =>
        await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            await new LiveCleanupBatch(cleanup).RemoveRolesAsync(s_cleanupRoles, "slackthread7656none", cleanupCt));

    private static (string Role, string Password)[] ScriptLogins(string suffix) =>
        [("admin", "ScriptAdmin" + suffix), ("viewer", "ScriptViewer" + suffix), ("web", "ScriptWeb" + suffix), ("mcp", "ScriptMcp" + suffix)];

    /// <summary>The rung alone, on a store whose roles and default privileges exist but whose provisioning has not run again since V252:
    /// the default privileges give admin the writes and viewer and web the read on a table made after them, and the rung takes them back.</summary>
    [Fact]
    public async Task TheRung_LeavesTheThreadsToTheOwner_AndTheReadToAdmin_BeforeProvisioningRunsAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = await MigratedOwnerAsync(scratch, ct);
            var logins = ScriptLogins("7656a");
            await ActionProposalStoreLiveTests.ExecAsync(owner, await ShippedScriptAsync(owner, logins, ct), ct);
            await RerunTheRungAsync(owner, ct);
            await AssertOwnerOnlyAsync(owner, scratch, logins, ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>The shipped script's step 3j5, run after the table exists: its blanket grants give every role the read and admin the writes
    /// first, and the step takes them back. Behind the rung the step warns, skips and the script still runs to its end.</summary>
    [Fact]
    public async Task TheBringYourOwnScript_LeavesTheThreadsToTheOwner_AndTheReadToAdmin_AndSkipsOnAStoreBehindTheRung()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = await MigratedOwnerAsync(scratch, ct);
            var logins = ScriptLogins("7656b");
            var script = await ShippedScriptAsync(owner, logins, ct);

            /* Behind the rung there is no table: the script warns, skips step 3j5 and still makes the four roles. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(owner, RungBelow, ct);
            await ActionProposalStoreLiveTests.ExecAsync(owner, "DELETE FROM darling_schema_version WHERE version > " + RungBelow, ct);
            await ActionProposalStoreLiveTests.ExecAsync(owner, script, ct);
            Assert.Equal("4", await ActionProposalStoreLiveTests.ScalarAsync(owner, "SELECT count(*) FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp')", ct));

            Assert.Equal(StorageVersion.SchemaVersion - RungBelow, await PgMigrations.MigrateAsync(owner, ct));
            await ActionProposalStoreLiveTests.ExecAsync(owner, script, ct);
            await AssertOwnerOnlyAsync(owner, scratch, logins, ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }

    /// <summary>The managed batch's section 10e5, run after the table exists. The batch makes no web password a test knows, so web is read
    /// from the catalog alone and the other three roles log in.</summary>
    [Fact]
    public async Task TheManagedBatch_LeavesTheThreadsToTheOwner_AndTheReadToAdmin()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = await MigratedOwnerAsync(scratch, ct);
            await ManagedBatchAsync(owner, scratch, ct);
            await AssertOwnerOnlyAsync(owner, scratch,
                [("admin", ProvisioningTestSecrets.AdminPassword), ("viewer", ProvisioningTestSecrets.ViewerPassword), ("mcp", ProvisioningTestSecrets.McpPassword)], ct);
            bodySucceeded = true;
        }
        finally
        {
            await CleanUpAsync(scratch, bodySucceeded);
        }
    }
}
