/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The quiet window (#6231, part D): a mute rule with only a server scope (or none, for the fleet), an end and an optional start. These
/// tests drive the write tools over a store and judge each alert on an explicit clock, so "before the start", "inside" and "after the end"
/// are three instants rather than three sleeps.
/// </summary>
public sealed class QuietWindowBehaviourTests
{
    private static readonly DateTime Now = new(2026, 10, 20, 12, 0, 0, DateTimeKind.Utc);

    private static (int ServerId, string DisplayName) Alpha => (11, "alpha");

    private static string Iso(DateTime utc) => utc.ToString("o", System.Globalization.CultureInfo.InvariantCulture);

    private static AlertMuteContext Alert(int serverId, string serverName, string metric = "High CPU") =>
        new() { ServerId = serverId, ServerName = serverName, MetricName = metric };

    private static async Task<bool> MutedAsync(FakeMuteRuleStore store, AlertMuteContext alert, DateTime at) =>
        (await store.LoadAllAsync()).Any(r => r.MatchesAt(alert, at));

    private static JsonObject Parse(string json) => JsonNode.Parse(json)!.AsObject();

    [Fact]
    public async Task AnAlertInsideTheWindowIsMuted_OneBeforeTheStartFires_AndOneAfterTheEndFires()
    {
        var store = new FakeMuteRuleStore();
        var start = Now.AddHours(2);
        var end = Now.AddHours(4);

        var answer = Parse(await DarlingMcpQuietWindowTools.SetQuietWindowCore(store, Alpha, Iso(end), Iso(start), "patching", Now));

        Assert.Equal("created", answer["status"]!.GetValue<string>());
        Assert.Equal("upcoming", answer["state"]!.GetValue<string>());
        var alert = Alert(11, "alpha");
        Assert.False(await MutedAsync(store, alert, Now.AddHours(1)), "before the start the alert fires");
        Assert.True(await MutedAsync(store, alert, start), "the start itself is inside the window");
        Assert.True(await MutedAsync(store, alert, Now.AddHours(3)), "inside the window the alert is muted");
        Assert.False(await MutedAsync(store, alert, end), "the end is outside the window");
        Assert.False(await MutedAsync(store, alert, Now.AddHours(5)), "after the end the alert fires");
    }

    [Fact]
    public async Task AWindowWithNoStart_MutesFromNow()
    {
        var store = new FakeMuteRuleStore();
        var answer = Parse(await DarlingMcpQuietWindowTools.SetQuietWindowCore(store, Alpha, Iso(Now.AddHours(1)), null, null, Now));

        Assert.Equal("created", answer["status"]!.GetValue<string>());
        Assert.Null(answer["starts_at_utc"]);
        Assert.True(await MutedAsync(store, Alert(11, "alpha"), Now.AddMinutes(1)));
    }

    [Fact]
    public async Task AServerWindowMutesOnlyThatServer_AFleetWindowMutesEveryServer()
    {
        var serverStore = new FakeMuteRuleStore();
        await DarlingMcpQuietWindowTools.SetQuietWindowCore(serverStore, Alpha, Iso(Now.AddHours(2)), null, null, Now);
        var inside = Now.AddHours(1);

        Assert.True(await MutedAsync(serverStore, Alert(11, "alpha"), inside));
        Assert.False(await MutedAsync(serverStore, Alert(12, "beta"), inside));
        /* The window is keyed on the store id: another server that shares the display name is not covered. */
        Assert.False(await MutedAsync(serverStore, Alert(12, "alpha"), inside));

        var fleetStore = new FakeMuteRuleStore();
        var fleet = Parse(await DarlingMcpQuietWindowTools.SetQuietWindowCore(fleetStore, null, Iso(Now.AddHours(2)), null, "fleet patch", Now));
        Assert.Contains("whole fleet", fleet["scope"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.True(await MutedAsync(fleetStore, Alert(11, "alpha"), inside));
        Assert.True(await MutedAsync(fleetStore, Alert(12, "beta", "Blocking Detected"), inside));
        Assert.True(await MutedAsync(fleetStore, Alert(99, "gamma", "Deadlocks Detected"), inside));
    }

    [Fact]
    public async Task ClearingAWindowEndsItAtOnce_AndTheAnswerGivesTheUndo()
    {
        var store = new FakeMuteRuleStore();
        var set = Parse(await DarlingMcpQuietWindowTools.SetQuietWindowCore(store, Alpha, Iso(Now.AddHours(2)), null, "patching", Now));
        var id = set["rule_id"]!.GetValue<string>();
        Assert.Equal("clear_quiet_window", set["undo"]!["tool"]!.GetValue<string>());
        Assert.Equal(id, set["undo"]!["arguments"]!["rule_id"]!.GetValue<string>());
        Assert.True(await MutedAsync(store, Alert(11, "alpha"), Now.AddHours(1)));

        var cleared = Parse(await DarlingMcpQuietWindowTools.ClearQuietWindowCore(store, id, Now));

        Assert.Equal("cleared", cleared["status"]!.GetValue<string>());
        Assert.False(await MutedAsync(store, Alert(11, "alpha"), Now.AddHours(1)), "a cleared window mutes nothing");
        Assert.Equal(0, store.Count);
        Assert.Equal("active", cleared["previous"]!["state"]!.GetValue<string>());
        var undo = cleared["undo"]!;
        Assert.Equal("set_quiet_window", undo["tool"]!.GetValue<string>());
        Assert.Equal("alpha", undo["arguments"]!["server_name"]!.GetValue<string>());
        Assert.Equal("patching", undo["arguments"]!["reason"]!.GetValue<string>());
        Assert.Equal(Iso(Now.AddHours(2)), undo["arguments"]!["ends_at"]!.GetValue<string>());
    }

    [Fact]
    public async Task ClearQuietWindow_LeavesAnOrdinaryMuteAlone_AndAnUnknownIdIsNotFound()
    {
        var ordinary = new MuteRule { Id = "ordinary", MetricName = "High CPU", ServerId = 11, ServerName = "alpha", ExpiresAtUtc = Now.AddDays(1) };
        var permanent = new MuteRule { Id = "permanent", ServerId = 11, ServerName = "alpha" };
        var store = new FakeMuteRuleStore().Seed(ordinary).Seed(permanent);

        Assert.Equal("invalid", Parse(await DarlingMcpQuietWindowTools.ClearQuietWindowCore(store, "ordinary", Now))["status"]!.GetValue<string>());
        Assert.Equal("invalid", Parse(await DarlingMcpQuietWindowTools.ClearQuietWindowCore(store, "permanent", Now))["status"]!.GetValue<string>());
        Assert.Equal("not_found", Parse(await DarlingMcpQuietWindowTools.ClearQuietWindowCore(store, "nope", Now))["status"]!.GetValue<string>());
        Assert.Equal(2, store.Count);
    }

    [Fact]
    public async Task ClearQuietWindow_LeavesTheViewersSilenceRuleAlone_ButClearsOneSetWithNoReason()
    {
        /* The Viewer's Silence button writes a server-scoped rule with an end; it is not a window this tool set. */
        var silence = new MuteRule
        {
            Id = "silence", ServerId = 11, ServerName = "alpha", ExpiresAtUtc = Now.AddDays(1), Reason = "Silenced from server list",
        };
        var store = new FakeMuteRuleStore().Seed(silence);
        var refused = Parse(await DarlingMcpQuietWindowTools.ClearQuietWindowCore(store, "silence", Now));
        Assert.Equal("invalid", refused["status"]!.GetValue<string>());
        Assert.Equal(1, store.Count);

        var set = Parse(await DarlingMcpQuietWindowTools.SetQuietWindowCore(store, Alpha, Iso(Now.AddHours(2)), null, null, Now));
        var cleared = Parse(await DarlingMcpQuietWindowTools.ClearQuietWindowCore(store, set["rule_id"]!.GetValue<string>(), Now));
        Assert.Equal("cleared", cleared["status"]!.GetValue<string>());
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public async Task ASetWindowLongerThanSevenDays_IsRefused_AndExactlySevenDaysIsAccepted()
    {
        var store = new FakeMuteRuleStore();

        var tooLong = Parse(await DarlingMcpQuietWindowTools.SetQuietWindowCore(store, Alpha, Iso(Now.AddDays(7).AddMinutes(1)), null, null, Now));
        Assert.Equal("invalid", tooLong["status"]!.GetValue<string>());
        Assert.Contains("7 days", tooLong["message"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal(0, store.Count);

        /* The length is the end minus the start: a window set ahead is judged by its own length, not by how far ahead it begins. */
        var ahead = Parse(await DarlingMcpQuietWindowTools.SetQuietWindowCore(store, Alpha, Iso(Now.AddDays(11)), Iso(Now.AddDays(5)), null, Now));
        Assert.Equal("created", ahead["status"]!.GetValue<string>());

        var week = Parse(await DarlingMcpQuietWindowTools.SetQuietWindowCore(store, Alpha, Iso(Now.AddDays(7)), null, null, Now));
        Assert.Equal("created", week["status"]!.GetValue<string>());
    }

    [Fact]
    public async Task AnEndBeforeTheStart_IsRefusedAndNothingIsWritten()
    {
        var store = new FakeMuteRuleStore();
        var start = Now.AddHours(4);

        var equal = Parse(await DarlingMcpQuietWindowTools.SetQuietWindowCore(store, Alpha, Iso(start), Iso(start), null, Now));
        var before = Parse(await DarlingMcpQuietWindowTools.SetQuietWindowCore(store, Alpha, Iso(Now.AddHours(3)), Iso(start), null, Now));
        var past = Parse(await DarlingMcpQuietWindowTools.SetQuietWindowCore(store, Alpha, Iso(Now.AddHours(-1)), null, null, Now));
        var missing = Parse(await DarlingMcpQuietWindowTools.SetQuietWindowCore(store, Alpha, null, null, null, Now));
        var garbled = Parse(await DarlingMcpQuietWindowTools.SetQuietWindowCore(store, Alpha, "next tuesday-ish", null, null, Now));

        foreach (var refused in new[] { equal, before, past, missing, garbled })
        {
            Assert.Equal("invalid", refused["status"]!.GetValue<string>());
        }

        Assert.Contains("after starts_at", before["message"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task CreateMuteRule_TakesAnOptionalStart_AndRefusesAnEndBeforeIt()
    {
        var store = new FakeMuteRuleStore();
        var created = Parse(await DarlingMcpAlertTools.CreateMuteRuleOver(
            store, "alpha", "High CPU", null, null, null, null, "r", Iso(Now.AddDays(2)), null, null, null, null, Iso(Now.AddDays(1))));
        Assert.Equal("created", created["status"]!.GetValue<string>());
        Assert.Equal(Iso(Now.AddDays(1)), created["mute_rule"]!["starts_at_utc"]!.GetValue<string>());

        var refused = Parse(await DarlingMcpAlertTools.CreateMuteRuleOver(
            store, "alpha", "Deadlocks Detected", null, null, null, null, "r", Iso(Now.AddDays(1)), null, null, null, null, Iso(Now.AddDays(2))));
        Assert.Equal("invalid", refused["status"]!.GetValue<string>());
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public async Task UpdateMuteRule_CanMoveAndClearTheStart_ButNotPastTheEnd()
    {
        var rule = new MuteRule { Id = "r1", ServerName = "alpha", ExpiresAtUtc = Now.AddDays(2), StartsAtUtc = Now.AddDays(1) };
        var store = new FakeMuteRuleStore().Seed(rule);

        var past = Parse(await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"starts_at_utc\":\"" + Iso(Now.AddDays(3)) + "\"}"));
        Assert.Equal("invalid", past["status"]!.GetValue<string>());
        Assert.Equal(Now.AddDays(1), store.Row("r1")!.StartsAtUtc);

        var cleared = Parse(await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"starts_at_utc\":null}"));
        Assert.Equal("updated", cleared["status"]!.GetValue<string>());
        Assert.Null(store.Row("r1")!.StartsAtUtc);
    }

    [Fact]
    public void GetMuteRulesStates_AreActiveUpcomingExpiredAndDisabled()
    {
        var active = new MuteRule { StartsAtUtc = Now.AddHours(-1), ExpiresAtUtc = Now.AddHours(1) };
        var upcoming = new MuteRule { StartsAtUtc = Now.AddHours(1), ExpiresAtUtc = Now.AddHours(2) };
        var expired = new MuteRule { StartsAtUtc = Now.AddHours(-2), ExpiresAtUtc = Now.AddHours(-1) };
        var disabled = new MuteRule { Enabled = false, StartsAtUtc = Now.AddHours(-1) };
        var plain = new MuteRule();

        Assert.Equal("active", DarlingMcpAlertTools.BuildMuteRuleWithState(active, Now)["state"]!.GetValue<string>());
        var upcomingNode = DarlingMcpAlertTools.BuildMuteRuleWithState(upcoming, Now);
        Assert.Equal("upcoming", upcomingNode["state"]!.GetValue<string>());
        Assert.Equal(Iso(Now.AddHours(1)), upcomingNode["starts_at_utc"]!.GetValue<string>());
        Assert.Equal("expired", DarlingMcpAlertTools.BuildMuteRuleWithState(expired, Now)["state"]!.GetValue<string>());
        Assert.Equal("disabled", DarlingMcpAlertTools.BuildMuteRuleWithState(disabled, Now)["state"]!.GetValue<string>());
        Assert.Equal("active", DarlingMcpAlertTools.BuildMuteRuleWithState(plain, Now)["state"]!.GetValue<string>());
        Assert.Null(DarlingMcpAlertTools.BuildMuteRuleWithState(plain, Now)["starts_at_utc"]);
    }

    [Fact]
    public async Task TheMuteCache_HoldsAnUpcomingRuleButDoesNotApplyItUntilItsStart()
    {
        var upcoming = new MuteRule { Id = "up", ServerName = "alpha", StartsAtUtc = DateTime.UtcNow.AddHours(1), ExpiresAtUtc = DateTime.UtcNow.AddHours(2) };
        var running = new MuteRule { Id = "run", ServerName = "beta", StartsAtUtc = DateTime.UtcNow.AddHours(-1), ExpiresAtUtc = DateTime.UtcNow.AddHours(1) };
        var service = new MuteRuleService(new FakeMuteRuleStore().Seed(upcoming).Seed(running), NullLogger<MuteRuleService>.Instance);
        await service.LoadAsync();

        Assert.False(service.IsAlertMuted(new AlertMuteContext { ServerName = "alpha", MetricName = "High CPU" }));
        Assert.True(service.IsAlertMuted(new AlertMuteContext { ServerName = "beta", MetricName = "High CPU" }));
        Assert.Equal(2, service.GetRules().Count);
        Assert.Equal(new[] { "run" }, service.GetActiveRules().Select(r => r.Id).ToArray());
    }

    [Fact]
    public void TheQuietWindowTools_AreWriteTools_AndNeverOfferedToAnAgent()
    {
        foreach (var tool in new[] { "set_quiet_window", "clear_quiet_window" })
        {
            Assert.True(DarlingMcpWritePolicy.IsWrite(tool), tool + " is a write tool, served only with mcp.allowWrites");
            Assert.Contains(tool, DarlingMcpWritePolicy.NeverAgentTools);
            Assert.False(DarlingMcpWritePolicy.IsAgentAction(tool, DarlingMcpWritePolicy.AgentActionTools), tool + " is never an agent action");
            Assert.False(DarlingMcpWritePolicy.IsAgentAction(tool, new System.Collections.Generic.HashSet<string> { tool }), tool + " stays off even if listed as an action");
        }
    }

    [Fact]
    public void TheQuietWindowTools_AreServedWithWrites_AndNotWithout()
    {
        var all = typeof(DarlingMcpQuietWindowTools).Assembly.GetTypes()
            .Where(t => t.GetCustomAttributes(typeof(ModelContextProtocol.Server.McpServerToolTypeAttribute), false).Length > 0)
            .SelectMany(t => t.GetMethods())
            .Select(m => (ModelContextProtocol.Server.McpServerToolAttribute?)m.GetCustomAttributes(typeof(ModelContextProtocol.Server.McpServerToolAttribute), false).FirstOrDefault())
            .Where(a => a?.Name is not null).Select(a => a!.Name!).ToList();

        Assert.Contains("set_quiet_window", all);
        Assert.Contains("clear_quiet_window", all);
    }
}

/// <summary>The live half: the production store holds the start, and the fleet read ignores a window that has not started.</summary>
[Collection("live-postgres")]
public sealed class QuietWindowLivePostgresTests
{
    [Fact]
    public async Task PgMuteRuleStore_RoundTripsTheStart_AndTheFleetSilenceReadIgnoresAWindowNotYetStarted()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live quiet-window round trip.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var store = new PgMuteRuleStore(postgres);
        var tag = Guid.NewGuid().ToString("N");
        var early = new MuteRule { Id = "qw-early-" + tag, ServerName = "qw-" + tag, ServerId = -6231, StartsAtUtc = DateTime.UtcNow.AddHours(1), ExpiresAtUtc = DateTime.UtcNow.AddHours(2), Reason = "quiet window" };
        var running = new MuteRule { Id = "qw-run-" + tag, ServerName = "qw-" + tag, ServerId = -6232, StartsAtUtc = DateTime.UtcNow.AddHours(-1), ExpiresAtUtc = DateTime.UtcNow.AddHours(1), Reason = "quiet window" };
        var legacy = new MuteRule { Id = "qw-legacy-" + tag, ServerName = "qw-" + tag, Reason = "no start" };

        var bodySucceeded = false;
        try
        {
            await store.InsertAsync(early);
            await store.InsertAsync(running);
            await store.InsertAsync(legacy);

            var loaded = await store.LoadAllAsync();
            var loadedEarly = loaded.Single(r => r.Id == early.Id);
            Assert.NotNull(loadedEarly.StartsAtUtc);
            Assert.True((loadedEarly.StartsAtUtc!.Value - early.StartsAtUtc!.Value).Duration() < TimeSpan.FromMilliseconds(1));
            Assert.Equal(DateTimeKind.Utc, loadedEarly.StartsAtUtc.Value.Kind);
            Assert.Null(loaded.Single(r => r.Id == legacy.Id).StartsAtUtc);

            running.StartsAtUtc = null;
            await store.UpdateAsync(running);
            Assert.Null((await store.LoadAllAsync()).Single(r => r.Id == running.Id).StartsAtUtc);

            /* The fleet silence flag reads the same rows in SQL: a window set ahead of its start is not a silence yet. */
            await using (var seed = postgres.CreateCommand(
                "INSERT INTO servers (server_id, server_name, display_name, is_enabled) VALUES (-6231, 'qw-a-' || $1, 'qw-a-' || $1, TRUE), (-6232, 'qw-b-' || $1, 'qw-b-' || $1, TRUE)"))
            {
                seed.Parameters.AddWithValue(tag);
                await seed.ExecuteNonQueryAsync(ct);
            }

            var silenced = new System.Collections.Generic.Dictionary<int, bool>();
            await using (var fleet = postgres.CreateCommand(DarlingFleetReader.FleetServersSql))
            await using (var reader = await fleet.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    silenced[reader.GetInt32(0)] = reader.GetBoolean(5);
                }
            }

            Assert.False(silenced[-6231], "a window that has not started is not a silence yet");
            Assert.True(silenced[-6232], "a window in force is a silence");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                await store.DeleteAsync(early.Id);
                await store.DeleteAsync(running.Id);
                await store.DeleteAsync(legacy.Id);
                await using var cleanup = postgres.CreateCommand("DELETE FROM servers WHERE server_id IN (-6231, -6232)");
                await cleanup.ExecuteNonQueryAsync();
            });
        }
    }
}
