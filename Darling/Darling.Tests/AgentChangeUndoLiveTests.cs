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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5917: the agent changes list, the one-click undo and the switch against a live store. The tests write the journal's
/// <c>action_called</c> and <c>action_done</c> rows directly (the tools that make the changes are other slices), so they pin
/// the undo's behaviour per kind: an unchanged target is restored and the undo journaled by the person; a target changed
/// since is refused and left alone; a deleted rule comes back with its own id and a name clash refuses; an override is
/// compared per field; a call with no result row reads as unknown and has no undo; the window is 365 days; and switching
/// off makes the gate refuse <c>actions_off</c>.
/// </summary>
public sealed class AgentChangeUndoLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string Login = "erik";

    private static string Mute(string reason, bool enabled = true, string? metric = "Blocking", int? serverId = 7) =>
        new JsonObject
        {
            ["enabled"] = enabled, ["reason"] = reason, ["metric_name"] = metric, ["server_id"] = serverId,
            ["expires_at_utc"] = null, ["server_name"] = null, ["database_pattern"] = null, ["query_text_pattern"] = null,
            ["wait_type_pattern"] = null, ["job_name_pattern"] = null, ["client_app_pattern"] = null,
        }.ToJsonString();

    /// <summary>#6310: the list names the server the way the registry does (display name), and a server that left the registry keeps its id with a removed hint, in the change list, its refusals and the body the web page and get_agent_changes share.</summary>
    [Fact]
    public async Task TheList_NamesTheServerFromTheRegistry_AndMarksAServerThatLeftIt()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES (7, 'host-7', 'SQL2019', TRUE, 15, now(), now())", ct);
            await JournalChangeAsync(connection, "mute", "m1", 7, Mute("old"), Mute("agent says"), ct: ct);
            await JournalChangeAsync(connection, "mute", "m2", 99, Mute("old"), Mute("agent says"), ct: ct);
            await ExecAsync(connection, "INSERT INTO config.investigation_journal (event_time, server_id, event, actor, detail) VALUES (now(), 7, 'refused', 'agent:a', '{\"tool\":\"force_plan\"}'::jsonb), (now(), 99, 'refused', 'agent:a', '{\"tool\":\"force_plan\"}'::jsonb)", ct);

            var read = await undo.ReadAsync(50, cancellationToken: ct);
            Assert.Equal("SQL2019", read.Changes.Single(c => c.ServerId == 7).ServerLabel);
            Assert.Equal("Server 99 (removed)", read.Changes.Single(c => c.ServerId == 99).ServerLabel);
            Assert.Equal("SQL2019", read.Refusals.Single(r => r.ServerId == 7).ServerLabel);
            Assert.Equal("Server 99 (removed)", read.Refusals.Single(r => r.ServerId == 99).ServerLabel);

            var body = PerformanceMonitor.Darling.Service.DarlingWebEndpoints.AgentChangesBody(read, canDecide: false, DateTime.UtcNow);
            var changes = body["changes"]!.AsArray().Select(c => c!.AsObject()).ToList();
            var named = changes.Single(c => c["server_id"]!.GetValue<int>() == 7);
            Assert.Equal("SQL2019", named["server_name"]!.GetValue<string>());
            Assert.False(named["server_removed"]!.GetValue<bool>());
            var gone = changes.Single(c => c["server_id"]!.GetValue<int>() == 99);
            Assert.Null(gone["server_name"]);
            Assert.True(gone["server_removed"]!.GetValue<bool>());
        });
    }

    [Fact]
    public async Task MuteChangedByTheAgent_Unchanged_IsRestored_AndTheUndoIsJournaledByThePerson()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason, metric_name, server_id) VALUES ('m1', true, now() at time zone 'utc', 'agent says', 'Blocking', 7)", ct);
            var called = await JournalChangeAsync(connection, "mute", "m1", 7, Mute("old", false), Mute("agent says"), ct: ct);

            var result = await undo.UndoAsync(called, Login, ct);
            Assert.True(result.Undone, result.Words);
            Assert.Equal("old|false", await ScalarAsync(connection, "SELECT reason || '|' || enabled FROM config.config_mute_rules WHERE id = 'm1'", ct));
            Assert.Equal("person:erik", await ScalarAsync(connection,
                $"SELECT actor FROM config.investigation_journal WHERE event = 'action_undone' AND detail->>'called_id' = '{called}'", ct));

            var again = await undo.UndoAsync(called, Login, ct);
            Assert.Equal("already_undone", again.Code);
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'action_undone'", ct));
        });
    }

    [Fact]
    public async Task MuteChangedSince_IsRefused_ChangedSince_AndTheCurrentSettingsStay()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason, metric_name, server_id) VALUES ('m2', true, now() at time zone 'utc', 'a person edited this', 'Blocking', 7)", ct);
            var called = await JournalChangeAsync(connection, "mute", "m2", 7, Mute("old"), Mute("agent says"), ct: ct);

            var result = await undo.UndoAsync(called, Login, ct);
            Assert.False(result.Undone);
            Assert.Equal("changed_since", result.Code);
            Assert.Equal("a person edited this", await ScalarAsync(connection, "SELECT reason FROM config.config_mute_rules WHERE id = 'm2'", ct));
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'action_undone'", ct));
        });
    }

    /// <summary>The mute image with the four repeating-schedule fields (#7040): days as names, times as HH:mm, the IANA zone.</summary>
    private static string ScheduledMute(string reason, string[]? days, string? start, string? end, string? zone) =>
        new JsonObject
        {
            ["enabled"] = true, ["reason"] = reason, ["metric_name"] = "Blocking", ["server_id"] = 7, ["expires_at_utc"] = null,
            ["recur_days"] = days is null ? null : new JsonArray(days.Select(d => (JsonNode?)JsonValue.Create(d)).ToArray()),
            ["recur_start_time"] = start, ["recur_end_time"] = end, ["recur_time_zone"] = zone,
        }.ToJsonString();

    /// <summary>#7040: a person's repeating schedule survives an agent delete then undo. The undo writes the mask, the two times and the
    /// zone back, which needs the smallint and time columns to take the image's day names and HH:mm text.</summary>
    [Fact]
    public async Task ADeletedRepeatingMute_ComesBackWithItsSchedule()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            var called = await JournalChangeAsync(connection, "mute", "s1", 7,
                ScheduledMute("nightly", ["Mon", "Fri"], "22:00", "02:00", "America/New_York"), null, ct: ct);

            var result = await undo.UndoAsync(called, Login, ct);
            Assert.True(result.Undone, result.Words);
            Assert.Equal("34|22:00:00|02:00:00|America/New_York", await ScalarAsync(connection,
                "SELECT recur_days::int || '|' || recur_start_time::text || '|' || recur_end_time::text || '|' || recur_time_zone FROM config.config_mute_rules WHERE id = 's1'", ct));
        });
    }

    /// <summary>#7040: an update that added a schedule is undone to the rule without one while the row still reads as the agent left
    /// it (the mask compares as an integer, the times as text), and refused once a person changed the days.</summary>
    [Fact]
    public async Task AnUpdatedRepeatingMute_IsRestoredWhenUnchanged_AndRefusedWhenTheDaysChanged()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason, metric_name, server_id, recur_days, recur_start_time, recur_end_time, recur_time_zone) VALUES ('s2', true, now() at time zone 'utc', 'agent says', 'Blocking', 7, 34, '22:00', '02:00', 'America/New_York')", ct);
            var called = await JournalChangeAsync(connection, "mute", "s2", 7,
                ScheduledMute("old", null, null, null, null), ScheduledMute("agent says", ["Mon", "Fri"], "22:00", "02:00", "America/New_York"), ct: ct);

            var result = await undo.UndoAsync(called, Login, ct);
            Assert.True(result.Undone, result.Words);
            Assert.Equal("old|null|null|null|null", await ScalarAsync(connection,
                "SELECT reason || '|' || coalesce(recur_days::text, 'null') || '|' || coalesce(recur_start_time::text, 'null') || '|' || coalesce(recur_end_time::text, 'null') || '|' || coalesce(recur_time_zone, 'null') FROM config.config_mute_rules WHERE id = 's2'", ct));

            await ExecAsync(connection, "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason, metric_name, server_id, recur_days, recur_start_time, recur_end_time, recur_time_zone) VALUES ('s3', true, now() at time zone 'utc', 'agent says', 'Blocking', 7, 2, '22:00', '02:00', 'America/New_York')", ct);
            var changedSince = await JournalChangeAsync(connection, "mute", "s3", 7,
                ScheduledMute("old", null, null, null, null), ScheduledMute("agent says", ["Mon", "Fri"], "22:00", "02:00", "America/New_York"), ct: ct);
            var refused = await undo.UndoAsync(changedSince, Login, ct);
            Assert.False(refused.Undone);
            Assert.Equal("changed_since", refused.Code);
            Assert.Equal("2", await ScalarAsync(connection, "SELECT recur_days::text FROM config.config_mute_rules WHERE id = 's3'", ct));
        });
    }

    [Fact]
    public async Task MuteTheAgentCreated_IsDeleted_AndAnUpdatedAtDifferenceIsNotAChange()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason, metric_name, server_id) VALUES ('m3', true, '2020-01-01', 'agent says', 'Blocking', 7)", ct);
            var called = await JournalChangeAsync(connection, "mute", "m3", 7, null, Mute("agent says"), ct: ct);
            Assert.True((await undo.UndoAsync(called, Login, ct)).Undone);
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.config_mute_rules WHERE id = 'm3'", ct));
        });
    }

    [Fact]
    public async Task AMuteRow_LinksToTheAlertsMutedSinceTheChange()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason, metric_name, server_id) VALUES ('m4', true, now() at time zone 'utc', 'agent says', 'Blocking', 7)", ct);
            var called = await JournalChangeAsync(connection, "mute", "m4", 7, null, Mute("agent says"), ct: ct, ageHours: 5);
            await ExecAsync(connection, @"INSERT INTO config.config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, muted)
VALUES (now() at time zone 'utc' - interval '1 hour', 7, 'srv-7', 'Blocking', 9, 5, true),
       (now() at time zone 'utc' - interval '1 hour', 7, 'srv-7', 'Blocking', 9, 5, false),
       (now() at time zone 'utc' - interval '1 hour', 7, 'srv-7', 'Deadlocks', 9, 5, true),
       (now() at time zone 'utc' - interval '9 hours', 7, 'srv-7', 'Blocking', 9, 5, true)", ct);

            var row = (await undo.ReadAsync(50, cancellationToken: ct)).Changes.Single(c => c.CalledId == called);
            Assert.Equal(1, row.MutedAlerts);
            Assert.Single(await undo.ReadMutedAlertsAsync(called, 50, ct));
        });
    }

    [Fact]
    public async Task AMuteRow_CountsThePopulationOfMutedAlerts_NotThePageTheAlertsRouteReturns()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason, metric_name, server_id) VALUES ('m5', true, now() at time zone 'utc', 'agent says', 'Blocking', 7)", ct);
            var called = await JournalChangeAsync(connection, "mute", "m5", 7, null, Mute("agent says"), ct: ct, ageHours: 5);
            /* More than the old 1000-row page the list counted: the row must report all of them (#5917, #3679). */
            await ExecAsync(connection, @"INSERT INTO config.config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, muted)
SELECT now() at time zone 'utc' - interval '1 hour', 7, 'srv-7', 'Blocking', 9, 5, true FROM generate_series(1, 1005)", ct);

            var row = (await undo.ReadAsync(50, cancellationToken: ct)).Changes.Single(c => c.CalledId == called);
            Assert.Equal(1005, row.MutedAlerts);
            /* The alerts route still pages, and says how many it returned. */
            Assert.Equal(2, (await undo.ReadMutedAlertsAsync(called, 2, ct)).Count);
        });
    }

    [Fact]
    public async Task ADeletedRule_ComesBackWithItsOwnCustomId_AndANameClashRefuses()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.custom_alert_rules (name, definition, description) VALUES ('keep-me', '{\"a\":1}'::jsonb, 'd'), ('spacer', '{}'::jsonb, NULL)", ct);
            var id = await ScalarAsync(connection, "SELECT id FROM config.custom_alert_rules WHERE name = 'keep-me'", ct);
            var image = new JsonObject { ["name"] = "keep-me", ["definition"] = JsonNode.Parse("{\"a\":1}"), ["description"] = "d", ["enabled"] = true }.ToJsonString();
            await ExecAsync(connection, "DELETE FROM config.custom_alert_rules WHERE name = 'keep-me'", ct);
            var called = await JournalChangeAsync(connection, "rule", "Custom:" + id, 7, image, null, ct: ct);

            Assert.True((await undo.UndoAsync(called, Login, ct)).Undone);
            Assert.Equal("keep-me|d", await ScalarAsync(connection, $"SELECT name || '|' || description FROM config.custom_alert_rules WHERE id = {id}", ct));
            Assert.Equal("{\"a\": 1}", await ScalarAsync(connection, $"SELECT definition::text FROM config.custom_alert_rules WHERE id = {id}", ct));

            /* Delete it again, and let somebody make a rule with the name: the undo refuses and changes nothing. */
            var called2 = await JournalChangeAsync(connection, "rule", "Custom:" + id, 7, image, null, ct: ct);
            await ExecAsync(connection, "DELETE FROM config.custom_alert_rules WHERE name = 'keep-me'", ct);
            await ExecAsync(connection, "INSERT INTO config.custom_alert_rules (name, definition) VALUES ('keep-me', '{\"b\":2}'::jsonb)", ct);
            var clash = await undo.UndoAsync(called2, Login, ct);
            Assert.Equal("name_taken", clash.Code);
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.custom_alert_rules WHERE name = 'keep-me'", ct));
            Assert.Equal("0", await ScalarAsync(connection, $"SELECT count(*) FROM config.custom_alert_rules WHERE id = {id}", ct));
        });
    }

    [Fact]
    public async Task AnUpdatedRule_IsRestored_AndTheVersionMovesForwardForTheEvaluator()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.custom_alert_rules (name, definition, enabled) VALUES ('r1', '{\"t\": 9}'::jsonb, false)", ct);
            var id = await ScalarAsync(connection, "SELECT id FROM config.custom_alert_rules WHERE name = 'r1'", ct);
            string Image(int t, bool enabled) => new JsonObject { ["name"] = "r1", ["definition"] = JsonNode.Parse("{\"t\":" + t + "}"), ["description"] = null, ["enabled"] = enabled }.ToJsonString();
            var called = await JournalChangeAsync(connection, "rule", id!, 7, Image(1, true), Image(9, false), ct: ct);

            Assert.True((await undo.UndoAsync(called, Login, ct)).Undone);
            Assert.Equal("1|true|2|person:erik", await ScalarAsync(connection, $"SELECT (definition->>'t') || '|' || enabled || '|' || version || '|' || updated_by FROM config.custom_alert_rules WHERE id = {id}", ct));
        });
    }

    [Fact]
    public async Task AnOverride_IsComparedPerField_RestoredWhenUnchanged_RefusedWhenChanged_AndDeletedWhenTheAgentCreatedIt()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'h')", ct);
            await ExecAsync(connection, "INSERT INTO config.config_threshold_overrides (server_id, database_name, threshold_key, value, set_by) VALUES (7, NULL, 'blocking_sessions', 10, 'agent:a'), (7, 'db1', 'blocking_sessions', 40, 'agent:a')", ct);
            string Image(double v) => new JsonObject { ["value"] = v }.ToJsonString();

            var restored = await JournalChangeAsync(connection, "override", "blocking_sessions", 7, Image(5), Image(10), database: null, ct: ct);
            Assert.True((await undo.UndoAsync(restored, Login, ct)).Undone);
            Assert.Equal("5|person:erik", await ScalarAsync(connection, "SELECT value || '|' || set_by FROM config.config_threshold_overrides WHERE server_id = 7 AND database_name IS NULL", ct));

            /* The per-database override was changed by a person after the agent set 40: refused, kept. */
            await ExecAsync(connection, "UPDATE config.config_threshold_overrides SET value = 99 WHERE database_name = 'db1'", ct);
            var changed = await JournalChangeAsync(connection, "override", "blocking_sessions", 7, Image(20), Image(40), database: "db1", ct: ct);
            Assert.Equal("changed_since", (await undo.UndoAsync(changed, Login, ct)).Code);
            Assert.Equal("99", await ScalarAsync(connection, "SELECT value FROM config.config_threshold_overrides WHERE database_name = 'db1'", ct));

            /* The agent created it (no before image): undo removes it. */
            await ExecAsync(connection, "UPDATE config.config_threshold_overrides SET value = 40 WHERE database_name = 'db1'", ct);
            var created = await JournalChangeAsync(connection, "override", "blocking_sessions", 7, null, Image(40), database: "db1", ct: ct);
            Assert.True((await undo.UndoAsync(created, Login, ct)).Undone);
            Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.config_threshold_overrides WHERE database_name = 'db1'", ct));
        });
    }

    [Fact]
    public async Task ACallWithNoResultRow_ReadsAsUnknown_AFailedOneAsFailed_AndNeitherCanBeUndone()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason) VALUES ('m5', true, now() at time zone 'utc', 'agent says')", ct);
            var unknown = await JournalChangeAsync(connection, "mute", "m5", 7, null, Mute("agent says"), result: "none", ct: ct);
            var failed = await JournalChangeAsync(connection, "mute", "m5", 7, null, Mute("agent says"), result: "failed", ct: ct);

            var read = await undo.ReadAsync(50, cancellationToken: ct);
            Assert.Equal(AgentChangeResult.Unknown, read.Changes.Single(c => c.CalledId == unknown).Result);
            Assert.Equal(AgentChangeResult.Failed, read.Changes.Single(c => c.CalledId == failed).Result);
            Assert.All(read.Changes, c => Assert.False(c.CanTryUndo(DateTime.UtcNow)));
            Assert.Equal("not_undoable", (await undo.UndoAsync(unknown, Login, ct)).Code);
            Assert.Equal("not_undoable", (await undo.UndoAsync(failed, Login, ct)).Code);
            Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.config_mute_rules WHERE id = 'm5'", ct));
        });
    }

    [Fact]
    public async Task UndoWorksFor365Days_AndRefusesPastIt()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc, reason, metric_name, server_id) VALUES ('m6', true, now() at time zone 'utc', 'agent says', 'Blocking', 7)", ct);
            var fresh = await JournalChangeAsync(connection, "mute", "m6", 7, Mute("old"), Mute("agent says"), ct: ct, ageHours: 364 * 24);
            var old = await JournalChangeAsync(connection, "mute", "m6", 7, Mute("older"), Mute("agent says"), ct: ct, ageHours: 366 * 24);
            Assert.Equal("expired", (await undo.UndoAsync(old, Login, ct)).Code);
            Assert.True((await undo.UndoAsync(fresh, Login, ct)).Undone);
        });
    }

    [Fact]
    public async Task SwitchingOff_MakesTheGateRefuseActionsOff_IsJournaled_AndOnRestoresIt()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            /* The service creates the row when it copies darling.json; the switch only updates it (#6401). */
            await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            Assert.True(await undo.ReadEnabledAsync(ct));
            Assert.True(await undo.SetEnabledAsync(false, Login, ct));
            Assert.False(await undo.ReadEnabledAsync(ct));
            Assert.Equal("person:erik|false", await ScalarAsync(connection,
                "SELECT actor || '|' || (detail->>'enabled')::boolean FROM config.investigation_journal WHERE event = 'agent_actions_switched' ORDER BY journal_id DESC LIMIT 1", ct));

            var store = new InvestigationRequestStore(NpgsqlDataSource.Create(connection.ConnectionString), 30);
            var opened = await store.OpenOrJoinAsync(new InvestigationAlert(1, 7, "srv-7", "blocking", "Blocking", DateTime.UtcNow, Array.Empty<string>(), null,
                20, 100, LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = 2 }, ct);
            var claim = await store.ClaimAsync(opened.RequestId!.Value, "agent-a", ct);
            var writes = 0;
            var hooks = new AgentActionHooks
            {
                LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, int?>>((_, _, _) => 2),
                ReadBefore = (_, _, _) => Task.FromResult<string?>(null),
                Write = (_, _, _) => { writes++; return Task.FromResult<string?>("{}"); },
            };
            var call = new AgentActionCall("create_mute_rule", "mute",
                new AgentAction(opened.RequestId!.Value, claim.ClaimId!.Value, "agent-a", "why", "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"12\"}]"),
                new AgentActionTarget(7, "srv-7", null, null));

            Assert.Equal(AgentActionCodes.ActionsOff, (await store.RunAgentActionAsync(call, hooks, ct)).Refusal?.Reason);
            Assert.Equal(0, writes);

            Assert.True(await undo.SetEnabledAsync(true, Login, ct));
            Assert.True((await store.RunAgentActionAsync(call, hooks, ct)).Done);
            Assert.Equal(1, writes);
        });
    }

    [Fact]
    public async Task TheList_CarriesCallsAndRefusals_AndRefusesABadLogin()
    {
        await RunAsync(async (undo, connection, ct) =>
        {
            await ExecAsync(connection, "INSERT INTO config.investigation_journal (event, actor, reason, detail) VALUES ('refused', 'agent:a', 'actions_off', '{\"tool\":\"create_mute_rule\"}')", ct);
            /* A Level 1 request refusal (InvestigationRequestStore.RecordRefusalAsync: claim and submit) is an agent refusal with no tool; it refused no change (#5917). */
            await ExecAsync(connection, "INSERT INTO config.investigation_journal (event, actor, reason, detail) VALUES ('refused', 'agent:a', 'claim:not_open', NULL), ('refused', 'agent:a', 'submit:stale', '{\"note\":\"x\"}')", ct);
            var read = await undo.ReadAsync(50, cancellationToken: ct);
            var refusal = Assert.Single(read.Refusals);
            Assert.Equal("actions_off", refusal.Code);
            Assert.Equal("create_mute_rule", refusal.Tool);
            Assert.True(read.Enabled);
            await Assert.ThrowsAsync<ArgumentException>(() => undo.UndoAsync(1, "  ", ct));
        });
    }

    /// <summary>Writes the journal's called row, and its result row when asked: <c>done</c> (default), <c>failed</c> or <c>none</c>.</summary>
    private static async Task<long> JournalChangeAsync(NpgsqlConnection connection, string kind, string targetId, int serverId,
        string? before, string? after, string result = "done", string? database = null, CancellationToken ct = default, int ageHours = 1)
    {
        var request = Guid.NewGuid();
        /* The journal carries what the tools write (#5917): the shared kind names, and on the done row the tool's answer, which for a
           mute or a custom rule is a status word with the rule (a delete answers "deleted" and no rule). */
        var journaledKind = kind switch { "rule" => AgentChangeKinds.CustomRule, "override" => AgentChangeKinds.ThresholdOverride, _ => kind };
        string? answer = after;
        if (journaledKind == AgentChangeKinds.Mute)
            answer = after is null ? "{\"status\":\"deleted\",\"rule_id\":\"" + targetId + "\"}" : new JsonObject { ["status"] = "updated", ["mute_rule"] = JsonNode.Parse(after) }.ToJsonString();
        else if (journaledKind == AgentChangeKinds.CustomRule && after is null)
            answer = "{\"status\":\"deleted\",\"rule_id\":" + targetId.Replace("Custom:", "") + "}";
        after = answer;
        kind = journaledKind;
        var detail = new JsonObject
        {
            ["tool"] = kind + "_tool", ["kind"] = kind, ["claim_no"] = 1,
            ["target"] = new JsonObject { ["id"] = targetId, ["server_id"] = serverId, ["database"] = database },
            ["evidence"] = new JsonArray(), ["before"] = before is null ? null : JsonNode.Parse(before),
        }.ToJsonString();
        var called = await InsertJournalAsync(connection, "action_called", request, serverId, detail, ageHours, ct);
        if (result == "done")
            await InsertJournalAsync(connection, "action_done", request, serverId,
                new JsonObject { ["called_id"] = called, ["after"] = after is null ? null : JsonNode.Parse(after) }.ToJsonString(), ageHours, ct);
        else if (result == "failed")
            await InsertJournalAsync(connection, "action_failed", request, serverId,
                new JsonObject { ["called_id"] = called, ["error"] = "boom" }.ToJsonString(), ageHours, ct);
        return called;
    }

    private static async Task<long> InsertJournalAsync(NpgsqlConnection connection, string journalEvent, Guid request, int serverId, string detail, int ageHours, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, detail) VALUES (now() - make_interval(hours => $1), $2, $3, $4, 'agent:a', $5::jsonb) RETURNING journal_id", connection);
        command.Parameters.AddWithValue(ageHours);
        command.Parameters.AddWithValue(request);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(journalEvent);
        command.Parameters.AddWithValue(detail);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task RunAsync(Func<AgentChangeUndo, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the agent change undo live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(new AgentChangeUndo(dataSource, 30), connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }
}
