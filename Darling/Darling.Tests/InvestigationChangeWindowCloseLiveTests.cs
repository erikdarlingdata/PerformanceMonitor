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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #9485 part B on a live store: a late join releases a held request and the long-poll answers it, the close summary is written once per window
/// (at the tail end, or at <c>cleared_at</c> for a cleared window), and <c>get_change_windows</c> reads the windows, their states, their counts
/// and the held list, and says when rows were left out.
/// </summary>
public sealed class InvestigationChangeWindowCloseLiveTests
{
    private static readonly DateTime T0 = new(2026, 11, 20, 12, 0, 0, DateTimeKind.Utc);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private DateTime _now = T0;

    private NpgsqlDataSource? _source;

    private static InvestigationAlert Alert(DateTime at, string family = "blocking", int serverId = 7) =>
        new(1, serverId, "srv-" + serverId.ToString(CultureInfo.InvariantCulture), family, "Blocking", at, Array.Empty<string>(), null,
            20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);

    private static string Stamp(DateTime value) => "'" + value.ToString("O", CultureInfo.InvariantCulture) + "'::timestamptz";

    private static async Task<long> InsertWindowAsync(NpgsqlConnection connection, string servers, DateTime starts, DateTime ends, string reason,
        DateTime? cleared, CancellationToken ct)
    {
        var sql = $@"INSERT INTO config.investigation_change_windows (server_ids, starts_at, ends_at, reason, declared_by, cleared_at)
VALUES ({servers}, {Stamp(starts)}, {Stamp(ends)}, '{reason}', 'test', {(cleared is null ? "NULL" : Stamp(cleared.Value))}) RETURNING window_id";
        return Convert.ToInt64(await ScalarAsync(connection, sql, ct), CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task ALateJoinAfterTheTail_ReleasesTheHoldOnce_AndTheLongPollAnswersIt_WhileAJoinInsideTheTailChangesNothing()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            /* Ends at T0+60, so the tail runs to T0+90. */
            var windowId = await InsertWindowAsync(connection, "NULL", T0.AddMinutes(-10), T0.AddMinutes(60), "resize", null, ct);
            var opened = await store.OpenOrJoinAsync(Alert(T0), ct);
            Assert.True(opened.Held);
            var id = opened.RequestId!.Value;

            /* Joins every 25 minutes keep the request's grouping window alive. T0+75 is inside the tail: nothing changes. */
            foreach (var minutes in new[] { 25, 50, 75 })
            {
                _now = T0.AddMinutes(minutes);
                var inside = await store.OpenOrJoinAsync(Alert(_now), ct);
                Assert.Equal(InvestigationOpenOutcome.Joined, inside.Outcome);
                Assert.False(inside.Evented);
            }

            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_requests WHERE hold_released_at IS NOT NULL", ct));
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'hold_released'", ct));

            /* A cursor from just before the late join: the held request has been invisible to the wait until now. */
            _now = T0.AddMinutes(99);
            var signal = new InvestigationOpenSignal();
            var started = JsonDocument.Parse(await DarlingMcpInvestigationTools.WaitAsync(store.OpenedSinceAsync, signal, () => _now, null, null, 0, ct));
            Assert.Equal("empty", started.RootElement.GetProperty("status").GetString());
            var cursor = started.RootElement.GetProperty("cursor").GetString();

            _now = T0.AddMinutes(100);
            var late = await store.OpenOrJoinAsync(Alert(_now), ct);
            Assert.Equal(id, late.RequestId);
            Assert.True(late.Evented);

            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'hold_released'", ct));
            var detail = JsonDocument.Parse((await ScalarAsync(connection, "SELECT detail::text FROM config.investigation_journal WHERE event = 'hold_released'", ct))!).RootElement;
            Assert.Equal(windowId, detail.GetProperty("window_id").GetInt64());
            Assert.Equal("Blocking", detail.GetProperty("metric").GetString());
            Assert.Equal(_now.ToUniversalTime(), detail.GetProperty("alert_time").GetDateTime().ToUniversalTime());

            var woken = JsonDocument.Parse(await DarlingMcpInvestigationTools.WaitAsync(store.OpenedSinceAsync, signal, () => _now, null, cursor, 0, ct));
            var events = woken.RootElement.GetProperty("requests").EnumerateArray()
                .Where(r => r.GetProperty("request_id").GetGuid() == id).Select(r => r.GetProperty("event").GetString()).ToList();
            Assert.Contains("hold_released", events);

            /* A later join does not release twice or move the release time. */
            _now = T0.AddMinutes(110);
            await store.OpenOrJoinAsync(Alert(_now), ct);
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'hold_released'", ct));
            Assert.Equal("True", await ScalarAsync(connection, $"SELECT hold_released_at = {Stamp(T0.AddMinutes(100))} FROM config.investigation_requests", ct));
        });
    }

    [Fact]
    public async Task TheCloseSummary_IsWrittenOnceAtTheTailEnd_OrAtClearedAt_WithCountsByFamilyAndServer()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            /* A holds servers 7 and 8 until T0+50 (its tail end). B holds server 9 and is cleared at T0+30, long before its own end. */
            var a = await InsertWindowAsync(connection, "ARRAY[7,8]", T0.AddMinutes(-10), T0.AddMinutes(20), "script", null, ct);
            var b = await InsertWindowAsync(connection, "ARRAY[9]", T0.AddMinutes(-10), T0.AddMinutes(600), "restart", T0.AddMinutes(30), ct);
            await store.OpenOrJoinAsync(Alert(T0, "blocking", 7), ct);
            await store.OpenOrJoinAsync(Alert(T0, "resources", 7), ct);
            await store.OpenOrJoinAsync(Alert(T0, "blocking", 8), ct);
            await store.OpenOrJoinAsync(Alert(T0.AddMinutes(5), "blocking", 9), ct);

            _now = T0.AddMinutes(29);
            Assert.Equal(0, (await store.SweepAsync(ct)).ChangeWindowsClosed);

            /* B closes at cleared_at, A is still in its tail. */
            _now = T0.AddMinutes(30);
            Assert.Equal(1, (await store.SweepAsync(ct)).ChangeWindowsClosed);
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_change_windows WHERE window_id = {b} AND summarized_at IS NOT NULL", ct));
            Assert.Equal("0", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_change_windows WHERE window_id = {a} AND summarized_at IS NOT NULL", ct));

            _now = T0.AddMinutes(50);
            Assert.Equal(1, (await store.SweepAsync(ct)).ChangeWindowsClosed);
            Assert.Equal(0, (await store.SweepAsync(ct)).ChangeWindowsClosed);

            Assert.Equal("2", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'change_window_closed'", ct));
            var summary = JsonDocument.Parse((await ScalarAsync(connection,
                $"SELECT detail::text FROM config.investigation_journal WHERE event = 'change_window_closed' AND (detail ->> 'window_id')::bigint = {a}", ct))!).RootElement;
            Assert.Equal("script", summary.GetProperty("reason").GetString());
            Assert.Equal(3, summary.GetProperty("held").GetInt32());
            Assert.Equal(0, summary.GetProperty("released").GetInt32());
            Assert.Equal(2, summary.GetProperty("by_family").GetProperty("blocking").GetInt32());
            Assert.Equal(1, summary.GetProperty("by_family").GetProperty("resources").GetInt32());
            Assert.Equal(2, summary.GetProperty("by_server").GetProperty("srv-7").GetInt32());
            Assert.Equal(1, summary.GetProperty("by_server").GetProperty("srv-8").GetInt32());
            var cleared = JsonDocument.Parse((await ScalarAsync(connection,
                $"SELECT detail::text FROM config.investigation_journal WHERE event = 'change_window_closed' AND (detail ->> 'window_id')::bigint = {b}", ct))!).RootElement;
            Assert.Equal(1, cleared.GetProperty("held").GetInt32());
            Assert.Equal(1, cleared.GetProperty("by_server").GetProperty("srv-9").GetInt32());
        });
    }

    [Fact]
    public async Task GetChangeWindows_ReadsStatesCountsAndTheHeldList_AndSaysWhenRowsWereLeftOut()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            var w1 = await InsertWindowAsync(connection, "ARRAY[7]", T0, T0.AddMinutes(60), "resize", null, ct);
            var w2 = await InsertWindowAsync(connection, "NULL", T0.AddHours(10), T0.AddHours(11), "later", null, ct);
            foreach (var (minutes, family) in new[] { (1, "blocking"), (2, "resources"), (3, "availability") })
            {
                _now = T0.AddMinutes(minutes);
                Assert.True((await store.OpenOrJoinAsync(Alert(_now, family, 7), ct)).Held);
            }

            async Task<JsonElement> ReadAsync(long? windowId, int hoursBack = 168, int limit = 20, int offset = 0) =>
                JsonDocument.Parse(await DarlingMcpChangeWindowTools.GetChangeWindowsAsync(store, windowId, hoursBack, limit, offset, ct)).RootElement;

            async Task<string> StateAsync(DateTime now)
            {
                _now = now;
                var read = await ReadAsync(null);
                return read.GetProperty("windows").EnumerateArray().Single(w => w.GetProperty("window_id").GetInt64() == w1).GetProperty("state").GetString()!;
            }

            Assert.Equal("upcoming", await StateAsync(T0));
            Assert.Equal("open", await StateAsync(T0.AddMinutes(1)));
            Assert.Equal("open", await StateAsync(T0.AddMinutes(60)));
            Assert.Equal("in_tail", await StateAsync(T0.AddMinutes(61)));
            Assert.Equal("in_tail", await StateAsync(T0.AddMinutes(90)));
            Assert.Equal("closed", await StateAsync(T0.AddMinutes(91)));

            _now = T0.AddMinutes(91);
            var summary = (await ReadAsync(null)).GetProperty("windows").EnumerateArray().Single(w => w.GetProperty("window_id").GetInt64() == w1);
            Assert.Equal(3, summary.GetProperty("held").GetInt32());
            Assert.Equal(0, summary.GetProperty("released").GetInt32());
            Assert.Equal(1, summary.GetProperty("by_family").GetProperty("resources").GetInt32());
            Assert.Equal(3, summary.GetProperty("by_server").GetProperty("srv-7").GetInt32());
            Assert.Equal("resize", summary.GetProperty("reason").GetString());
            Assert.Equal(T0.AddMinutes(90), summary.GetProperty("tail_ends_at").GetDateTime().ToUniversalTime());
            var upcoming = (await ReadAsync(null)).GetProperty("windows").EnumerateArray().Single(w => w.GetProperty("window_id").GetInt64() == w2);
            Assert.Equal("all", upcoming.GetProperty("servers").GetString());
            Assert.Equal("upcoming", upcoming.GetProperty("state").GetString());

            var one = await ReadAsync(w1);
            Assert.Equal(3, one.GetProperty("held_returned").GetInt32());
            Assert.Equal(new[] { "blocking", "resources", "availability" },
                one.GetProperty("held_requests").EnumerateArray().Select(r => r.GetProperty("family").GetString()).ToArray());
            Assert.All(one.GetProperty("held_requests").EnumerateArray(), r => Assert.Equal(JsonValueKind.Null, r.GetProperty("released_at").ValueKind));

            var capped = await ReadAsync(w1, limit: 2);
            Assert.True(capped.GetProperty("truncated").GetBoolean());
            Assert.Equal(2, capped.GetProperty("next_offset").GetInt32());
            Assert.Equal(2, capped.GetProperty("held_returned").GetInt32());
            var rest = await ReadAsync(w1, limit: 2, offset: 2);
            Assert.False(rest.GetProperty("truncated").GetBoolean());
            Assert.Equal(1, rest.GetProperty("held_returned").GetInt32());

            var cappedWindows = await ReadAsync(null, limit: 1);
            Assert.True(cappedWindows.GetProperty("truncated").GetBoolean());
            Assert.Equal(1, cappedWindows.GetProperty("next_offset").GetInt32());

            Assert.Equal("empty", (await ReadAsync(w2 + 100)).GetProperty("status").GetString());
        });
    }

    [Fact]
    public async Task AScopedReader_SeesAWindowThatNamesItsServer_ButNotTheOtherServersItNames()
    {
        await RunAsync(async (store, connection, ct) =>
        {
            foreach (var id in new[] { 7, 8 })
            {
                await ScalarAsync(connection, $@"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ({id}, 'srv-{id}', 'srv-{id}', TRUE, 15, now(), now()) RETURNING server_id", ct);
            }

            var window = await InsertWindowAsync(connection, "ARRAY[7,8]", T0, T0.AddMinutes(60), "resize", null, ct);
            await InsertWindowAsync(connection, "ARRAY[8]", T0, T0.AddMinutes(60), "other", null, ct);
            _now = T0.AddMinutes(1);
            Assert.True((await store.OpenOrJoinAsync(Alert(_now, "blocking", 7), ct)).Held);
            Assert.True((await store.OpenOrJoinAsync(Alert(_now, "blocking", 8), ct)).Held);

            var scoped = new InvestigationRequestStore(_source!, 30, () => _now) { ReadServerIds = [7] };

            var all = await store.ReadChangeWindowsAsync(null, 168, 20, 0, ct);
            Assert.Equal(new[] { "srv-7", "srv-8" }, all.Windows.Single(w => w.Window.WindowId == window).Servers!.Order().ToArray());

            var read = await scoped.ReadChangeWindowsAsync(null, 168, 20, 0, ct);
            var seen = Assert.Single(read.Windows);
            Assert.Equal(window, seen.Window.WindowId);
            Assert.Equal(new[] { "srv-7" }, seen.Servers!.ToArray());
            Assert.Equal(new[] { 7 }, seen.Window.ServerIds!.ToArray());
            Assert.Equal(1, seen.Held);

            var one = await scoped.ReadChangeWindowsAsync(window, 168, 20, 0, ct);
            Assert.Equal(new[] { "srv-7" }, one.HeldRequests!.Select(r => r.ServerName).ToArray());
        });
    }

    private async Task RunAsync(Func<InvestigationRequestStore, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the change window live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        _source = dataSource;
        var store = new InvestigationRequestStore(dataSource, 30, () => _now);
        await body(store, connection, ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }
}
