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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #9485, planned change windows, the hold at open on a live store. A window row is inserted directly (declaring one is the write tools' job). A request
/// whose own <c>opened_at</c> is inside (start, end + tail] opens held: not woken for the request runner, left out of the route's count, not sent a
/// no-agent notice, still listable and claimable. The store's clock is the test's, so every edge is hit to the microsecond.
/// </summary>
public sealed class InvestigationChangeWindowHoldLiveTests
{
    private static readonly DateTime T0 = new(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private DateTime _now = T0;

    private static InvestigationAlert Alert(DateTime at, int serverId = 7, string family = "blocking", int maxPerHour = 20, int maxPerDay = 100,
        string triggerKind = InvestigationTriggerKinds.Alert, int expiryMinutes = 240) =>
        new(1, serverId, "srv-" + serverId.ToString(CultureInfo.InvariantCulture), family, "Blocking", at, [], null,
            maxPerHour, maxPerDay, LeaseMinutes: 15, ExpiryMinutes: expiryMinutes)
        { TriggerKind = triggerKind };

    [Fact]
    public async Task EachEdge_IsDecidedAgainstTheRequestsOwnOpenTime_AndTheTailEndsAtThirtyMinutes()
    {
        await RunAsync(async (store, connection, _, ct) =>
        {
            var windowId = await AddWindowAsync(connection, null, T0, T0.AddMinutes(60), "resize", ct);
            var tail = TimeSpan.FromMinutes(InvestigationChangeWindows.TailMinutes);
            var cases = new (TimeSpan Offset, bool Held)[]
            {
                (TimeSpan.Zero, false),
                (TimeSpan.FromSeconds(1), true),
                (TimeSpan.FromMinutes(60) + tail, true),
                (TimeSpan.FromMinutes(60) + tail + TimeSpan.FromSeconds(1), false),
            };

            var serverId = 100;
            foreach (var (offset, expectHeld) in cases)
            {
                _now = T0 + offset;
                var opened = await store.OpenOrJoinAsync(Alert(_now, serverId++), ct);
                Assert.Equal(InvestigationOpenOutcome.Opened, opened.Outcome);
                Assert.Equal(expectHeld, opened.Held);
                var row = (await store.ReadRequestAsync(opened.RequestId!.Value, ct))!.Value.Row;
                if (expectHeld)
                {
                    var hold = Assert.IsType<InvestigationRequestHold>(row.HeldBy);
                    Assert.Equal((windowId, "resize", T0.AddMinutes(60), null), (hold.WindowId, hold.Reason, hold.EndsAtUtc, hold.ReleasedAtUtc));
                }
                else
                {
                    Assert.Null(row.HeldBy);
                }
            }
        });
    }

    [Fact]
    public async Task AClearedWindow_HoldsNothingOpenedAfterItWasCleared()
    {
        await RunAsync(async (store, connection, _, ct) =>
        {
            await AddWindowAsync(connection, null, T0, T0.AddMinutes(60), "resize", ct, clearedAt: T0.AddMinutes(10));

            _now = T0.AddMinutes(9);
            Assert.True((await store.OpenOrJoinAsync(Alert(_now, 1), ct)).Held);
            _now = T0.AddMinutes(11);
            Assert.False((await store.OpenOrJoinAsync(Alert(_now, 2), ct)).Held);
        });
    }

    [Fact]
    public async Task AWindowForOtherServersHoldsNothing_ANullServerWindowCoversEveryServer_AndAServerInAListIsHeld()
    {
        await RunAsync(async (store, connection, _, ct) =>
        {
            /* Only server 8: server 7 is not held, server 8 is. */
            await AddWindowAsync(connection, [8], T0, T0.AddMinutes(60), "other", ct);
            _now = T0.AddMinutes(5);
            Assert.False((await store.OpenOrJoinAsync(Alert(_now, 7), ct)).Held);
            Assert.True((await store.OpenOrJoinAsync(Alert(_now, 8), ct)).Held);

            /* Every server (a NULL list) a day later. */
            var day2 = T0.AddDays(1);
            await AddWindowAsync(connection, null, day2, day2.AddMinutes(60), "fleet", ct);
            _now = day2.AddMinutes(5);
            Assert.True((await store.OpenOrJoinAsync(Alert(_now, 7), ct)).Held);
            Assert.True((await store.OpenOrJoinAsync(Alert(_now, 99), ct)).Held);

            /* A list, two days later: server 7 is in it, server 4 is not. */
            var day3 = T0.AddDays(2);
            await AddWindowAsync(connection, [3, 7, 9], day3, day3.AddMinutes(60), "list", ct);
            _now = day3.AddMinutes(5);
            Assert.True((await store.OpenOrJoinAsync(Alert(_now, 7), ct)).Held);
            Assert.False((await store.OpenOrJoinAsync(Alert(_now, 4), ct)).Held);
        });
    }

    [Fact]
    public async Task AChangeWatchRequestInsideTheWindowIsHeld_AndAChatRequestInsideItIsNot()
    {
        await RunAsync(async (store, connection, _, ct) =>
        {
            /* The parent post opens before the window, so it is not held itself. */
            var parent = (await store.OpenOrJoinAsync(Alert(T0.AddMinutes(-30), 7), ct)).RequestId!.Value;
            await AddWindowAsync(connection, null, T0, T0.AddMinutes(60), "restart", ct);

            _now = T0.AddMinutes(5);
            var watch = await store.OpenOrJoinAsync(Alert(_now, 8, triggerKind: InvestigationTriggerKinds.ChangeWatch), ct);
            Assert.True(watch.Held);
            Assert.NotNull((await store.ReadRequestAsync(watch.RequestId!.Value, ct))!.Value.Row.HeldBy);

            var thread = new SlackChatThread(parent, 1, 7, "C1", "1700000000.000100", T0.AddMinutes(-30));
            var chat = await store.OpenChatRequestAsync(thread, parent, [], [], 100, _now, SlackChatStoreLiveTests.RoutesAt(2), ct);
            Assert.Equal(SlackChatOpenOutcome.Opened, chat.Outcome);
            Assert.Null((await store.ReadRequestAsync(chat.RequestId!.Value, ct))!.Value.Row.HeldBy);
        });
    }

    [Fact]
    public async Task TheWaitReadSkipsAHeldRequest_AndTheListReturnsBothWithTheHeldFilterWorkingBothWays()
    {
        await RunAsync(async (store, connection, dataSource, ct) =>
        {
            await AddWindowAsync(connection, [7], T0, T0.AddMinutes(60), "resize", ct);
            _now = T0.AddMinutes(5);
            var held = (await store.OpenOrJoinAsync(Alert(_now, 7), ct)).RequestId!.Value;
            var unheld = (await store.OpenOrJoinAsync(Alert(_now, 8), ct)).RequestId!.Value;

            /* The long-poll's catch-up read: the unheld request only, though both opened at the same instant. */
            var waited = await store.OpenedSinceAsync(T0.AddHours(-1), null, 10, ct);
            Assert.Equal([unheld], waited.Select(r => r.RequestId));

            Assert.Equal(new[] { held, unheld }.Order(), (await store.ListAsync(null, null, null, 10, ct)).Select(r => r.RequestId).Order());
            Assert.Equal([held], (await store.ListAsync(null, null, null, 10, InvestigationChangeWindows.HeldOnly, ct)).Select(r => r.RequestId));
            Assert.Equal([unheld], (await store.ListAsync(null, null, null, 10, InvestigationChangeWindows.HeldExclude, ct)).Select(r => r.RequestId));

            /* The MCP read: held_by is an object on the held row and null on the other, and a filter word it does not know is refused. */
            using var all = JsonDocument.Parse(await DarlingMcpInvestigationTools.ListInvestigationRequests(dataSource, cancellationToken: ct));
            var rows = all.RootElement.GetProperty("requests").EnumerateArray().ToDictionary(r => r.GetProperty("request_id").GetGuid());
            Assert.Equal(2, rows.Count);
            Assert.Equal(JsonValueKind.Null, rows[unheld].GetProperty("held_by").ValueKind);
            var heldBy = rows[held].GetProperty("held_by");
            Assert.Equal("resize", heldBy.GetProperty("reason").GetString());
            Assert.Equal(JsonValueKind.Number, heldBy.GetProperty("window_id").ValueKind);
            Assert.Equal(T0.AddMinutes(60), heldBy.GetProperty("ends_at").GetDateTime().ToUniversalTime());
            Assert.Equal(JsonValueKind.Null, heldBy.GetProperty("released_at").ValueKind);

            using var only = JsonDocument.Parse(await DarlingMcpInvestigationTools.ListInvestigationRequests(dataSource, held: "only", cancellationToken: ct));
            Assert.Equal([held], only.RootElement.GetProperty("requests").EnumerateArray().Select(r => r.GetProperty("request_id").GetGuid()));
            using var exclude = JsonDocument.Parse(await DarlingMcpInvestigationTools.ListInvestigationRequests(dataSource, held: "exclude", cancellationToken: ct));
            Assert.Equal([unheld], exclude.RootElement.GetProperty("requests").EnumerateArray().Select(r => r.GetProperty("request_id").GetGuid()));
            using var refused = JsonDocument.Parse(await DarlingMcpInvestigationTools.ListInvestigationRequests(dataSource, held: "sometimes", cancellationToken: ct));
            Assert.Equal("invalid", refused.RootElement.GetProperty("status").GetString());
            Assert.Equal("held", refused.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        });
    }

    [Fact]
    public async Task ReleasingAHold_ShowsTheRequestAgainInTheWaitReadAndTheExcludeFilter_WithReleasedAtSet()
    {
        await RunAsync(async (store, connection, _, ct) =>
        {
            await AddWindowAsync(connection, null, T0, T0.AddMinutes(60), "resize", ct);
            _now = T0.AddMinutes(5);
            var id = (await store.OpenOrJoinAsync(Alert(_now, 7), ct)).RequestId!.Value;
            Assert.Empty(await store.OpenedSinceAsync(T0.AddHours(-1), null, 10, ct));

            /* Same-metric joins every 25 minutes keep the 30 minute grouping window open through the tail (60 + 30 minutes) without releasing. */
            foreach (var minutes in new[] { 30, 55, 80 })
            {
                _now = T0.AddMinutes(minutes);
                Assert.Equal(InvestigationOpenOutcome.Joined, (await store.OpenOrJoinAsync(Alert(_now, 7), ct)).Outcome);
            }

            Assert.Empty(await store.OpenedSinceAsync(T0.AddHours(-1), null, 10, ct));

            /* A join after the tail releases it. */
            _now = T0.AddMinutes(95);
            Assert.Equal(InvestigationOpenOutcome.Joined, (await store.OpenOrJoinAsync(Alert(_now, 7), ct)).Outcome);

            Assert.Equal([(id, "hold_released")], (await store.OpenedSinceAsync(T0.AddHours(-1), null, 10, ct)).Select(r => (r.RequestId, r.Event)));
            Assert.Empty(await store.ListAsync(null, null, null, 10, InvestigationChangeWindows.HeldOnly, ct));
            var row = Assert.Single(await store.ListAsync(null, null, null, 10, InvestigationChangeWindows.HeldExclude, ct));
            Assert.Equal(T0.AddMinutes(95), row.HeldBy!.ReleasedAtUtc);
        });
    }

    [Fact]
    public async Task AHeldRequestCanBeClaimed()
    {
        await RunAsync(async (store, connection, _, ct) =>
        {
            await AddWindowAsync(connection, null, T0, T0.AddMinutes(60), "resize", ct);
            _now = T0.AddMinutes(5);
            var id = (await store.OpenOrJoinAsync(Alert(_now, 7), ct)).RequestId!.Value;
            Assert.NotNull((await store.ReadRequestAsync(id, ct))!.Value.Row.HeldBy);

            var claim = await store.ClaimAsync(id, "agent-a", ct);

            Assert.Equal(InvestigationClaimOutcome.Claimed, claim.Outcome);
            Assert.Equal("claimed", (await store.ReadRequestAsync(id, ct))!.Value.Row.State);
        });
    }

    [Fact]
    public async Task HeldRequestsLeaveTheRouteLimitUntouched_SoARequestAfterTheTailStillOpens()
    {
        await RunAsync(async (store, connection, _, ct) =>
        {
            await AddWindowAsync(connection, null, T0, T0.AddMinutes(10), "resize", ct);

            /* Five requests inside the window against an hourly and a daily limit of 3: none of them reads limit_reached. */
            for (var i = 0; i < 5; i++)
            {
                _now = T0.AddMinutes(1 + i);
                var held = await store.OpenOrJoinAsync(Alert(_now, 11 + i, maxPerHour: 3, maxPerDay: 3), ct);
                Assert.Equal(InvestigationOpenOutcome.Opened, held.Outcome);
                Assert.True(held.Held);
            }

            /* Just after the tail (10 + 30 minutes), inside the hour and the day the held requests opened in. */
            _now = T0.AddMinutes(41);
            var after = await store.OpenOrJoinAsync(Alert(_now, 20, maxPerHour: 3, maxPerDay: 3), ct);
            Assert.Equal(InvestigationOpenOutcome.Opened, after.Outcome);
            Assert.False(after.Held);

            var use = await store.RouteUseAsync(ct);
            Assert.Equal((1, 1), use[1]);
        });
    }

    [Fact]
    public async Task AClaimedHeldRequest_CountsTowardTheRouteLimit_SoClaimingCannotGoPastIt()
    {
        await RunAsync(async (store, connection, _, ct) =>
        {
            await AddWindowAsync(connection, null, T0, T0.AddMinutes(10), "resize", ct);
            var ids = new List<Guid>();
            for (var i = 0; i < 5; i++)
            {
                _now = T0.AddMinutes(1 + i);
                var held = await store.OpenOrJoinAsync(Alert(_now, 11 + i, maxPerHour: 3, maxPerDay: 3), ct);
                Assert.True(held.Held);
                ids.Add(held.RequestId!.Value);
            }

            /* Five held requests against a limit of 3 count for nothing. */
            Assert.False((await store.RouteUseAsync(ct)).ContainsKey(1));

            /* Each claim ignores the hold, so each one counts. */
            for (var i = 0; i < 3; i++)
            {
                Assert.Equal(InvestigationClaimOutcome.Claimed, (await store.ClaimAsync(ids[i], "agent-a", ct)).Outcome);
            }

            Assert.Equal((3, 3), (await store.RouteUseAsync(ct))[1]);
            _now = T0.AddMinutes(41);
            Assert.Equal(InvestigationOpenOutcome.LimitReached, (await store.OpenOrJoinAsync(Alert(_now, 20, maxPerHour: 3, maxPerDay: 3), ct)).Outcome);
        });
    }

    [Fact]
    public async Task ALateJoinAfterAClear_ReleasesTheHold_EvenInsideTheOriginalWindow()
    {
        await RunAsync(async (store, connection, _, ct) =>
        {
            var window = await AddWindowAsync(connection, null, T0, T0.AddMinutes(60), "resize", ct);
            _now = T0.AddMinutes(5);
            var id = (await store.OpenOrJoinAsync(Alert(_now, 7), ct)).RequestId!.Value;

            /* Inside the window and not cleared: a join does not release. */
            _now = T0.AddMinutes(8);
            Assert.Equal(InvestigationOpenOutcome.Joined, (await store.OpenOrJoinAsync(Alert(_now, 7), ct)).Outcome);
            Assert.Empty(await store.OpenedSinceAsync(T0.AddHours(-1), null, 10, ct));

            /* Cleared at +10 minutes: a join after that is outside the window, whatever the original end and tail said. */
            await ExecAsync(connection, $"UPDATE config.investigation_change_windows SET cleared_at = '{T0.AddMinutes(10):O}' WHERE window_id = {window}", ct);
            _now = T0.AddMinutes(15);
            Assert.Equal(InvestigationOpenOutcome.Joined, (await store.OpenOrJoinAsync(Alert(_now, 7), ct)).Outcome);

            /* The release delivers exactly one row for the request, its hold_released. Its opened row (journaled while it was held) and the
               joined row of this very join stay back, in the long-poll read and in the signed delivery read. */
            var waited = Assert.Single(await store.OpenedSinceAsync(T0.AddHours(-1), null, 10, ct));
            Assert.Equal((id, "hold_released"), (waited.RequestId, waited.Event));
            var delivered = Assert.Single(await store.PendingDeliveriesAsync(T0.AddHours(-1), 10, ct));
            Assert.Equal((id, "hold_released"), (delivered.RequestId, delivered.Event));
            Assert.Single(await store.ListAsync(null, null, null, 10, InvestigationChangeWindows.HeldExclude, ct));
        });
    }

    [Fact]
    public async Task AHeldRequestThatExpiresUnclaimedIsNotReadForTheNoAgentNotice_UntilItsHoldIsReleased()
    {
        await RunAsync(async (store, connection, _, ct) =>
        {
            await AddWindowAsync(connection, [7], T0, T0.AddMinutes(60), "resize", ct);
            _now = T0.AddMinutes(5);
            var held = (await store.OpenOrJoinAsync(Alert(_now, 7, expiryMinutes: 10), ct)).RequestId!.Value;
            var unheld = (await store.OpenOrJoinAsync(Alert(_now, 8, expiryMinutes: 10), ct)).RequestId!.Value;

            _now = T0.AddMinutes(30);
            await store.SweepAsync(ct);
            Assert.Equal("expired", (await store.ReadRequestAsync(held, ct))!.Value.Row.State);

            /* The read the no-agent fallback notice loop sends from. */
            Assert.Equal([unheld], (await store.UnclaimedExpiriesAsync(T0.AddHours(-1), 10, ct)).Select(e => e.RequestId));

            await ExecAsync(connection, $"UPDATE config.investigation_requests SET hold_released_at = '{_now:O}' WHERE request_id = '{held}'", ct);
            Assert.Equal(new[] { held, unheld }.Order(), (await store.UnclaimedExpiriesAsync(T0.AddHours(-1), 10, ct)).Select(e => e.RequestId).Order());
        });
    }

    private async Task RunAsync(Func<InvestigationRequestStore, NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the change window hold live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var store = new InvestigationRequestStore(dataSource, 30, () => _now);
        await body(store, connection, dataSource, ct);
    }

    private static async Task<long> AddWindowAsync(NpgsqlConnection connection, int[]? serverIds, DateTime startsAt, DateTime endsAt, string reason,
        CancellationToken ct, DateTime? clearedAt = null)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO config.investigation_change_windows (server_ids, starts_at, ends_at, reason, declared_by, cleared_at)
VALUES ($1, $2, $3, $4, 'test', $5)
RETURNING window_id", connection);
        command.Parameters.Add(new NpgsqlParameter<int[]?> { TypedValue = serverIds, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = startsAt, NpgsqlDbType = NpgsqlDbType.TimestampTz });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = endsAt, NpgsqlDbType = NpgsqlDbType.TimestampTz });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = reason, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<DateTime?> { TypedValue = clearedAt, NpgsqlDbType = NpgsqlDbType.TimestampTz });
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
