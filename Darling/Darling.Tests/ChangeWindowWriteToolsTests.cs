/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#9485: who is served declare_change_window and clear_change_window. Pure facts about the policy.</summary>
public sealed class ChangeWindowWriteToolsPolicyTests
{
    private static readonly string[] Tools = ["declare_change_window", "clear_change_window"];

    [Theory]
    [InlineData(false, false, false)] /* writes off, no agent token: not served */
    [InlineData(true, false, true)] /* the writes arm */
    [InlineData(false, true, true)] /* an agent token under the investigations arm */
    public void TheToolsAreServedToTheWritesArmAndToAnAgentToken_AndToNoOneElse(bool writes, bool investigations, bool served)
    {
        var policy = new DarlingMcpWritePolicy.Effective(writes, false, Investigations: investigations);

        foreach (var tool in Tools)
        {
            Assert.Equal(served, policy.Offers(tool));
            Assert.True(DarlingMcpWritePolicy.IsWrite(tool));
        }
    }
}

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>#9485 against a live store, through the real tool bodies.</summary>
public sealed class ChangeWindowWriteToolsLiveTests
{
    private static readonly DarlingAgentActionSeat AgentA = new(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true, TokenName: "agent-a"));
    private static readonly DarlingAgentActionSeat AgentB = new(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true, TokenName: "agent-b"));
    private static readonly DarlingAgentActionSeat AgentOff = new(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true, TokenName: "agent-a"), agentMuteRulesAllowed: false);
    private static readonly DarlingAgentActionSeat Operator = new(new DarlingMcpWritePolicy.Effective(true, false));

    private sealed record Ctx(NpgsqlDataSource Source, NpgsqlConnection Connection, CancellationToken Ct);

    private static async Task RunAsync(Func<Ctx, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the change window write tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        foreach (var id in new[] { 7, 8 })
        {
            await ExecAsync(connection, $@"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ({id}, 'srv-{id}', 'srv-{id}', TRUE, 15, now(), now())", ct);
        }

        await body(new Ctx(source, connection, ct));
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(Ctx c, string sql)
    {
        await using var command = new NpgsqlCommand(sql, c.Connection);
        return Convert.ToString(await command.ExecuteScalarAsync(c.Ct), CultureInfo.InvariantCulture);
    }

    private static JsonObject Parse(string answer) => JsonNode.Parse(answer)!.AsObject();

    private static string Iso(DateTime utc) => utc.ToString("o", CultureInfo.InvariantCulture);

    private static Task<string> Declare(Ctx c, DarlingAgentActionSeat? seat, string[]? servers = null, DateTime? start = null, DateTime? end = null, string? reason = "patch night")
    {
        var s = start ?? DateTime.UtcNow.AddHours(1);
        return DarlingMcpChangeWindowWriteTools.DeclareAsync(c.Source, seat, servers ?? ["srv-7"], Iso(s), Iso(end ?? s.AddHours(2)), reason, DateTime.UtcNow, c.Ct);
    }

    private static Task<string> Clear(Ctx c, DarlingAgentActionSeat? seat, long id, string? reason = "work cancelled") =>
        DarlingMcpChangeWindowWriteTools.ClearAsync(c.Source, seat, id, reason, DateTime.UtcNow, c.Ct);

    [Fact]
    public async Task ADeclaredWindow_ReadsBackAsTheRowAndItsTail_AndIsJournaledForTheCaller()
    {
        await RunAsync(async c =>
        {
            var start = DateTime.UtcNow.AddHours(1);
            var answer = Parse(await Declare(c, Operator, ["srv-7", "srv-8"], start, start.AddHours(2)));

            Assert.Equal("declared", (string?)answer["status"]);
            var id = (long)answer["window_id"]!;
            Assert.Equal(2, answer["servers"]!.AsArray().Count);
            Assert.Equal(Iso(start.AddHours(2).AddMinutes(InvestigationChangeWindows.TailMinutes)), (string?)answer["tail_ends_at_utc"]);
            Assert.Equal("{7,8}|patch night|true", await ScalarAsync(c,
                $"SELECT server_ids::text || '|' || reason || '|' || (declared_by LIKE 'operator:%') FROM config.investigation_change_windows WHERE window_id = {id}"));
            Assert.Equal("declare_change_window|change_window", await ScalarAsync(c,
                "SELECT (detail->>'tool') || '|' || (detail->>'kind') FROM config.investigation_journal WHERE event = 'action_called'"));

            var all = Parse(await Declare(c, Operator, ["all"]));
            Assert.True((bool)all["all_servers"]!);
            Assert.Equal("True", await ScalarAsync(c, $"SELECT server_ids IS NULL FROM config.investigation_change_windows WHERE window_id = {(long)all["window_id"]!}"));
        });
    }

    [Fact]
    public async Task ABadDeclare_IsRefused_AndLeavesNothing()
    {
        await RunAsync(async c =>
        {
            var now = DateTime.UtcNow;
            Assert.NotEqual("declared", (string?)Parse(await Declare(c, Operator, ["no-such-server"]))["status"]);
            Assert.Equal("invalid", (string?)Parse(await Declare(c, Operator, start: now.AddHours(3), end: now.AddHours(2)))["status"]);
            Assert.Equal("invalid", (string?)Parse(await Declare(c, Operator, start: now.AddHours(1), end: now.AddHours(1)))["status"]);
            Assert.Equal("invalid", (string?)Parse(await Declare(c, Operator, start: now.AddHours(1), end: now.AddHours(1).AddDays(7).AddSeconds(1)))["status"]);
            Assert.Equal("invalid", (string?)Parse(await Declare(c, Operator, start: now.AddHours(-3), end: now.AddHours(-1)))["status"]);
            Assert.Equal("refused", (string?)Parse(await Declare(c, Operator, reason: " "))["status"]);

            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_change_windows"));
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'action_%'"));

            Assert.Equal("declared", (string?)Parse(await Declare(c, Operator, start: now.AddHours(1), end: now.AddHours(1).AddDays(7)))["status"]);
        });
    }

    [Fact]
    public async Task AnAgentToken_DeclaresAFutureWindow_WhenAllowed_IsRefusedWhenOff_AndPastTheHourlyCap()
    {
        await RunAsync(async c =>
        {
            var answer = Parse(await Declare(c, AgentA, start: DateTime.UtcNow.AddDays(2)));
            Assert.Equal("declared", (string?)answer["status"]);
            Assert.Equal("agent:agent-a", await ScalarAsync(c, $"SELECT declared_by FROM config.investigation_change_windows WHERE window_id = {(long)answer["window_id"]!}"));

            var off = Parse(await Declare(c, AgentOff));
            Assert.Equal("refused", (string?)off["status"]);
            Assert.Equal(DarlingMcpAgentMuteDirect.OffCode, (string?)off["reason"]);

            var tooLong = Parse(await DarlingMcpChangeWindowWriteTools.DeclareAsync(c.Source, AgentA, [new string('x', 257)], Iso(DateTime.UtcNow.AddHours(1)),
                Iso(DateTime.UtcNow.AddHours(2)), "r", DateTime.UtcNow, c.Ct));
            Assert.Equal(DarlingMcpAgentMuteDirect.SizeCode, (string?)tooLong["reason"]);

            /* One declare is already counted. Each further call is another window: distinct starts keep them from reading as a repeat. */
            for (var i = 1; i < DarlingMcpAgentMuteDirect.MaxWritesPerHour; i++)
                Assert.Equal("declared", (string?)Parse(await Declare(c, AgentA, start: DateTime.UtcNow.AddHours(5 + i)))["status"]);
            var capped = Parse(await Declare(c, AgentA, start: DateTime.UtcNow.AddHours(40)));
            Assert.Equal(DarlingMcpAgentMuteDirect.RateCode, (string?)capped["reason"]);
            Assert.Equal(DarlingMcpAgentMuteDirect.MaxWritesPerHour.ToString(CultureInfo.InvariantCulture),
                await ScalarAsync(c, "SELECT count(*) FROM config.investigation_change_windows"));

            /* The cap is one per agent: another agent still has room. */
            Assert.Equal("declared", (string?)Parse(await Declare(c, AgentB, start: DateTime.UtcNow.AddHours(41)))["status"]);
        });
    }

    [Fact]
    public async Task AnAgentsStart_IsBoundedToSevenDaysAhead_AndAnOperatorsIsNot()
    {
        await RunAsync(async c =>
        {
            var now = DateTime.UtcNow;
            Task<string> At(DarlingAgentActionSeat seat, DateTime start) =>
                DarlingMcpChangeWindowWriteTools.DeclareAsync(c.Source, seat, ["srv-7"], Iso(start), Iso(start.AddHours(2)), "patch night", now, c.Ct);

            Assert.Equal("declared", (string?)Parse(await At(AgentA, now.AddDays(7)))["status"]);

            var tooFar = Parse(await At(AgentA, now.AddDays(7).AddMinutes(1)));
            Assert.Equal("refused", (string?)tooFar["status"]);
            Assert.Equal(DarlingMcpChangeWindowWriteTools.StartTooFarCode, (string?)tooFar["reason"]);
            Assert.Equal("1", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_change_windows"));

            Assert.Equal("declared", (string?)Parse(await At(Operator, now.AddDays(30)))["status"]);
            Assert.Equal("2", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_change_windows"));
        });
    }

    [Fact]
    public async Task ADeclare_DropsRepeatedNames_AndRefusesMoreThanTwoHundredBeforeResolvingAny()
    {
        await RunAsync(async c =>
        {
            var repeated = Parse(await Declare(c, Operator, ["srv-7", "SRV-7", " srv-7 "]));
            Assert.Equal("declared", (string?)repeated["status"]);
            Assert.Single(repeated["servers"]!.AsArray());

            /* 201 different names, none of them registered: a refusal that names the cap, not the first unknown name, so no name was looked up. */
            var many = Enumerable.Range(0, DarlingMcpChangeWindowWriteTools.MaxServerNames + 1).Select(i => "no-such-" + i.ToString(CultureInfo.InvariantCulture)).ToArray();
            foreach (var seat in new[] { Operator, AgentA })
            {
                var refused = Parse(await Declare(c, seat, many, DateTime.UtcNow.AddHours(3)));
                Assert.Equal("refused", (string?)refused["status"]);
                Assert.Equal(DarlingMcpChangeWindowWriteTools.TooManyNamesCode, (string?)refused["reason"]);
            }

            /* The same name repeated many times is one name: not over the cap. */
            var padded = Enumerable.Repeat("srv-8", 500).ToArray();
            Assert.Equal("declared", (string?)Parse(await Declare(c, Operator, padded, DateTime.UtcNow.AddHours(4)))["status"]);
            Assert.Equal("2", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_change_windows"));
        });
    }

    [Fact]
    public async Task AnAgentsReason_IsCappedAtTwoFiftySix_OnDeclareAndClear_AndAnOperatorsIsNot()
    {
        await RunAsync(async c =>
        {
            var atCap = new string('r', DarlingMcpAgentMuteDirect.MaxFieldChars);
            var over = new string('r', DarlingMcpAgentMuteDirect.MaxFieldChars + 1);

            var refused = Parse(await Declare(c, AgentA, reason: over));
            Assert.Equal(DarlingMcpAgentMuteDirect.SizeCode, (string?)refused["reason"]);
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_change_windows"));

            var id = (long)Parse(await Declare(c, AgentA, reason: atCap))["window_id"]!;
            Assert.Equal(DarlingMcpAgentMuteDirect.SizeCode, (string?)Parse(await Clear(c, AgentA, id, over))["reason"]);
            Assert.Equal("False", await ScalarAsync(c, $"SELECT cleared_at IS NOT NULL FROM config.investigation_change_windows WHERE window_id = {id}"));
            Assert.Equal("cleared", (string?)Parse(await Clear(c, AgentA, id, atCap))["status"]);

            var operatorWindow = Parse(await Declare(c, Operator, start: DateTime.UtcNow.AddHours(9), reason: over + over));
            Assert.Equal("declared", (string?)operatorWindow["status"]);
        });
    }

    [Fact]
    public async Task ClearingAWindow_ReleasesWhatItHeldAtOnce_WakesTheRunner_AndJournalsEachReleaseOnce()
    {
        await RunAsync(async c =>
        {
            var now = DateTime.UtcNow;
            var id = (long)Parse(await DarlingMcpChangeWindowWriteTools.DeclareAsync(c.Source, Operator, ["srv-7"], Iso(now.AddMinutes(-10)), Iso(now.AddHours(1)),
                "patch night", now, c.Ct))["window_id"]!;
            var store = new InvestigationRequestStore(c.Source, 30, () => DateTime.UtcNow);
            var held = new List<Guid>();
            foreach (var family in new[] { "blocking", "resources" })
            {
                var opened = await store.OpenOrJoinAsync(
                    new InvestigationAlert(1, 7, "srv-7", family, "Blocking", DateTime.UtcNow, [], null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240), c.Ct);
                Assert.True(opened.Held);
                held.Add(opened.RequestId!.Value);
            }

            Assert.Empty(await store.OpenedSinceAsync(now.AddHours(-1), null, 10, c.Ct));

            /* The first request also takes a join with a new metric while held, so it carries an opened and a joined row to hold back. */
            var joined = await store.OpenOrJoinAsync(
                new InvestigationAlert(1, 7, "srv-7", "blocking", "Deadlocks", DateTime.UtcNow, [], null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240), c.Ct);
            Assert.Equal((InvestigationOpenOutcome.Joined, held[0]), (joined.Outcome, joined.RequestId!.Value));
            Assert.Empty(await store.PendingDeliveriesAsync(now.AddHours(-1), 10, c.Ct));

            var signal = new InvestigationOpenSignal();
            using var waiter = signal.Register([1]);
            waiter.Rearm();
            var cleared = Parse(await DarlingMcpChangeWindowWriteTools.ClearAsync(c.Source, Operator, id, "work cancelled", DateTime.UtcNow, c.Ct, signal));
            Assert.Equal("cleared", (string?)cleared["status"]);

            Assert.True(await waiter.WaitAsync(TimeSpan.FromSeconds(5), c.Ct));
            Assert.Equal("2", await ScalarAsync(c, $"SELECT count(*) FROM config.investigation_requests WHERE held_window_id = {id} AND hold_released_at IS NOT NULL"));
            Assert.Equal("2", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_journal WHERE event = 'hold_released'"));

            /* The whole set a reader hands out: one hold_released row per request and nothing journaled while it was held (its opened and
               joined rows), in the long-poll read and in the signed delivery read. */
            var comesBack = (await store.OpenedSinceAsync(now.AddHours(-1), null, 10, c.Ct)).Select(r => (r.RequestId, r.Event)).Order().ToArray();
            Assert.Equal(held.Select(h => (h, (string?)"hold_released")).Order().ToArray(), comesBack);
            var delivered = (await store.PendingDeliveriesAsync(now.AddHours(-1), 10, c.Ct)).Select(r => (r.RequestId, (string?)r.Event)).Order().ToArray();
            Assert.Equal(held.Select(h => (h, (string?)"hold_released")).Order().ToArray(), delivered);

            /* A second clear writes nothing and releases nothing more. */
            Assert.Equal("already_cleared", (string?)Parse(await Clear(c, Operator, id))["status"]);
            Assert.Equal("2", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_journal WHERE event = 'hold_released'"));
        });
    }

    [Fact]
    public async Task AMuteRuleWrite_CountsTowardTheSameHourlyCapAsAWindowDeclare()
    {
        await RunAsync(async c =>
        {
            for (var i = 0; i < DarlingMcpAgentMuteDirect.MaxWritesPerHour - 1; i++)
            {
                var created = Parse(await DarlingMcpAlertTools.CreateMuteRule(c.Source, metric_name: "Collector Cost Regression " + i, reason: "storm",
                    expires_at: Iso(DateTime.UtcNow.AddDays(1)), action: "digest", agentSeat: AgentA));
                Assert.Equal("created", (string?)created["status"]);
            }

            Assert.Equal("declared", (string?)Parse(await Declare(c, AgentA))["status"]);
            Assert.Equal(DarlingMcpAgentMuteDirect.RateCode, (string?)Parse(await Declare(c, AgentA, start: DateTime.UtcNow.AddHours(9)))["reason"]);
        });
    }

    [Fact]
    public async Task ARepeatedDeclare_AnswersAlreadyDeclared_AndWritesNothing()
    {
        await RunAsync(async c =>
        {
            var start = DateTime.UtcNow.AddHours(1);
            var first = Parse(await Declare(c, Operator, start: start, end: start.AddHours(2)));
            var again = Parse(await Declare(c, Operator, start: start, end: start.AddHours(2)));

            Assert.Equal("already_declared", (string?)again["status"]);
            Assert.Equal((long)first["window_id"]!, (long)again["window_id"]!);
            Assert.Equal("1", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_change_windows"));
        });
    }

    [Fact]
    public async Task OperatorDeclaresStartedTogether_EndWithOneWindow()
    {
        await RunAsync(async c =>
        {
            var start = DateTime.UtcNow.AddHours(1);
            var answers = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() => Declare(c, Operator, start: start, end: start.AddHours(2)), c.Ct)));

            Assert.Equal("1", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_change_windows"));
            Assert.Equal(1, answers.Count(a => (string?)Parse(a)["status"] == "declared"));
            Assert.Equal(7, answers.Count(a => (string?)Parse(a)["status"] == "already_declared"));
        });
    }

    [Fact]
    public async Task Clear_SetsClearedAt_ASecondClearAnswersWithoutAnError_AndAClosedWindowAnswersClosed()
    {
        await RunAsync(async c =>
        {
            var id = (long)Parse(await Declare(c, Operator))["window_id"]!;
            Assert.Equal("cleared", (string?)Parse(await Clear(c, Operator, id))["status"]);
            Assert.Equal("True", await ScalarAsync(c, $"SELECT cleared_at IS NOT NULL FROM config.investigation_change_windows WHERE window_id = {id}"));

            Assert.Equal("already_cleared", (string?)Parse(await Clear(c, Operator, id))["status"]);
            Assert.Equal("2", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_journal WHERE event = 'action_called' AND detail->>'tool' IN ('declare_change_window', 'clear_change_window')"));

            await ExecAsync(c.Connection, @"INSERT INTO config.investigation_change_windows (server_ids, starts_at, ends_at, reason, declared_by)
VALUES (NULL, now() - interval '3 hours', now() - interval '2 hours', 'old work', 'operator:x')", c.Ct);
            var closedId = long.Parse((await ScalarAsync(c, "SELECT max(window_id) FROM config.investigation_change_windows"))!, CultureInfo.InvariantCulture);
            Assert.Equal("already_closed", (string?)Parse(await Clear(c, Operator, closedId))["status"]);
            Assert.Equal("not_found", (string?)Parse(await Clear(c, Operator, 99999))["status"]);
            Assert.Equal("refused", (string?)Parse(await Clear(c, Operator, id, reason: ""))["status"]);
        });
    }

    [Fact]
    public async Task AnAgent_ClearsOnlyAWindowItDeclared_AndAnOperatorClearsAny()
    {
        await RunAsync(async c =>
        {
            var id = (long)Parse(await Declare(c, AgentA))["window_id"]!;

            var other = Parse(await Clear(c, AgentB, id));
            Assert.Equal(DarlingMcpChangeWindowWriteTools.NotYoursCode, (string?)other["reason"]);
            Assert.Equal("False", await ScalarAsync(c, $"SELECT cleared_at IS NOT NULL FROM config.investigation_change_windows WHERE window_id = {id}"));

            Assert.Equal("cleared", (string?)Parse(await Clear(c, AgentA, id))["status"]);

            var second = (long)Parse(await Declare(c, AgentA, start: DateTime.UtcNow.AddHours(9)))["window_id"]!;
            Assert.Equal("cleared", (string?)Parse(await Clear(c, Operator, second))["status"]);
        });
    }

    [Fact]
    public async Task AClearedWindow_StopsHoldingNewRequests()
    {
        await RunAsync(async c =>
        {
            var start = DateTime.UtcNow.AddMinutes(-10);
            var id = (long)Parse(await Declare(c, Operator, start: start, end: start.AddHours(1)))["window_id"]!;
            var sql = InvestigationChangeWindows.HeldWindowIdSql("now()", "7");
            Assert.Equal(id.ToString(CultureInfo.InvariantCulture), await ScalarAsync(c, $"SELECT {sql}"));

            await Clear(c, Operator, id);
            Assert.Equal(string.Empty, await ScalarAsync(c, $"SELECT {sql}"));
        });
    }
}
