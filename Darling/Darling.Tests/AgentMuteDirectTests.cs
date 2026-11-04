/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7414: an agent token writes a mute or digest rule with no claim. Pure facts: which calls take the new path, and the expiry bound.
/// </summary>
public sealed class AgentMuteDirectTests
{
    private static readonly DarlingAgentActionSeat AgentSeat = new(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true));
    private static readonly DateTime Now = new(2026, 11, 4, 12, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData(false, false, null, false)] /* neither arm: the gate still answers not_served */
    [InlineData(true, false, null, false)] /* the writes arm: an operator's write */
    [InlineData(true, true, null, false)] /* both flags: the writes arm wins */
    [InlineData(false, false, "", false)]
    [InlineData(false, true, null, true)] /* the investigations arm, no agent_action */
    [InlineData(false, true, "  ", true)]
    [InlineData(false, true, "null", true)]
    [InlineData(false, true, "{", false)] /* a malformed argument goes to the claim path and is refused there */
    [InlineData(false, true, "{\"request_id\":\"x\"}", false)] /* any agent_action is the claim path, unchanged */
    public void OnlyTheInvestigationsArmWithNoAgentAction_TakesTheNewPath(bool writes, bool investigations, string? agentAction, bool expected)
    {
        var seat = new DarlingAgentActionSeat(new DarlingMcpWritePolicy.Effective(writes, ServerAdmin: false, Investigations: investigations));
        var applies = DarlingMcpAgentMuteDirect.Applies(seat, agentAction);

        Assert.Equal(expected, applies);
        Assert.False(DarlingMcpAgentMuteDirect.Applies(null, null));
    }

    [Theory]
    [InlineData(-1, true)] /* already past */
    [InlineData(0, true)] /* now itself is not the future */
    [InlineData(1, false)]
    [InlineData(86400, false)]
    [InlineData(7 * 86400 - 1, false)]
    [InlineData(7 * 86400, false)] /* the bound itself is allowed */
    [InlineData(7 * 86400 + 1, true)]
    [InlineData(30 * 86400, true)]
    public void TheExpiryBound_IsTheFutureUpToSevenDaysAhead(int secondsAhead, bool refused)
    {
        var refusal = DarlingMcpAgentMuteDirect.ExpiryRefusal(Now.AddSeconds(secondsAhead), Now);

        Assert.Equal(refused, refusal is not null);
        if (refusal is not null)
        {
            Assert.Equal(DarlingMcpAgentMuteDirect.ExpiryCode, refusal.Reason);
            Assert.Contains("at most 7 days ahead", refusal.Message, StringComparison.Ordinal);
            Assert.Contains(Now.AddDays(7).ToString("o", CultureInfo.InvariantCulture), refusal.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ARuleWithNoExpiry_IsRefused_NamingTheBound()
    {
        var refusal = DarlingMcpAgentMuteDirect.ExpiryRefusal(null, Now);

        Assert.NotNull(refusal);
        Assert.Contains("needs an expiry", refusal!.Message, StringComparison.Ordinal);
        Assert.Contains("at most 7 days ahead", refusal.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheRepeatScheduleAndStartsAt_AreStillRefused_OnEveryVerb_BeforeAnythingIsRead()
    {
        /* A data source that was never opened: each refusal comes before any store access. */
        await using var unused = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=none;Username=none;Timeout=1");
        var future = Now.AddYears(30).ToString("o", CultureInfo.InvariantCulture);

        var schedule = await DarlingMcpAlertTools.CreateMuteRule(unused, metric_name: "High CPU", reason: "storm", expires_at: future,
            agentSeat: AgentSeat, recur_start: "22:00", recur_end: "23:00");
        var days = await DarlingMcpAlertTools.CreateMuteRule(unused, metric_name: "High CPU", reason: "storm", expires_at: future,
            agentSeat: AgentSeat, recur_days: ["Mon"]);
        var starts = await DarlingMcpAlertTools.CreateMuteRule(unused, metric_name: "High CPU", reason: "storm", expires_at: future,
            agentSeat: AgentSeat, starts_at: "2026-11-05T00:00:00Z");
        var updateRecur = await DarlingMcpAlertTools.UpdateMuteRule(unused, "rule-1",
            "{\"reason\":\"storm\",\"expires_at_utc\":\"" + future + "\",\"recur_start\":\"22:00\"}", agentSeat: AgentSeat);
        var updateClear = await DarlingMcpAlertTools.UpdateMuteRule(unused, "rule-1",
            "{\"reason\":\"storm\",\"expires_at_utc\":\"" + future + "\",\"recur_days\":null}", agentSeat: AgentSeat);
        var updateStarts = await DarlingMcpAlertTools.UpdateMuteRule(unused, "rule-1",
            "{\"reason\":\"storm\",\"expires_at_utc\":\"" + future + "\",\"starts_at_utc\":\"2026-11-05T00:00:00Z\"}", agentSeat: AgentSeat);

        foreach (var answer in new[] { schedule, days, updateRecur, updateClear })
            Assert.Contains("repeat schedule", answer, StringComparison.Ordinal);
        foreach (var answer in new[] { starts, updateStarts })
            Assert.Contains("starts_at is not available", answer, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AWriteWithNoReason_OrNoExpiry_IsRefusedBeforeAnythingIsRead()
    {
        await using var unused = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=none;Username=none;Timeout=1");
        var soon = DateTime.UtcNow.AddDays(1).ToString("o", CultureInfo.InvariantCulture);

        var noReason = await DarlingMcpAlertTools.CreateMuteRule(unused, metric_name: "High CPU", expires_at: soon, agentSeat: AgentSeat);
        var noExpiry = await DarlingMcpAlertTools.CreateMuteRule(unused, metric_name: "High CPU", reason: "storm", agentSeat: AgentSeat);
        var tooFar = await DarlingMcpAlertTools.CreateMuteRule(unused, metric_name: "High CPU", reason: "storm",
            expires_at: DateTime.UtcNow.AddDays(8).ToString("o", CultureInfo.InvariantCulture), agentSeat: AgentSeat);
        var updateNoExpiry = await DarlingMcpAlertTools.UpdateMuteRule(unused, "rule-1", "{\"reason\":\"storm\"}", agentSeat: AgentSeat);
        var updateClears = await DarlingMcpAlertTools.UpdateMuteRule(unused, "rule-1", "{\"reason\":\"storm\",\"expires_at_utc\":null}", agentSeat: AgentSeat);
        var enableNoReason = await DarlingMcpAlertTools.SetMuteRuleEnabled(unused, "rule-1", true, agentSeat: AgentSeat);
        var deleteNoReason = await DarlingMcpAlertTools.DeleteMuteRule(unused, "rule-1", agentSeat: AgentSeat);

        foreach (var answer in new[] { noReason, enableNoReason, deleteNoReason })
            Assert.Equal(DarlingMcpAgentMuteDirect.ReasonCode, JsonNode.Parse(answer)!["reason"]!.GetValue<string>());
        foreach (var answer in new[] { noExpiry, tooFar, updateNoExpiry, updateClears })
        {
            Assert.Equal(DarlingMcpAgentMuteDirect.ExpiryCode, JsonNode.Parse(answer)!["reason"]!.GetValue<string>());
            Assert.Contains("at most 7 days ahead", answer, StringComparison.Ordinal);
        }
    }
}

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// #7414 against a live store, through the real tool methods: the bound holds on every verb, a refused write leaves nothing, the write and its
/// journal row are one transaction, and the service applies the rule an agent made.
/// </summary>
public sealed class AgentMuteDirectLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DarlingAgentActionSeat Legacy = new(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true));
    private static readonly DarlingAgentActionSeat Named = new(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true, TokenName: "agent-a"));
    private static readonly DarlingAgentActionSeat Operator = new(new DarlingMcpWritePolicy.Effective(true, false));
    private static readonly DarlingAgentActionSeat NoArm = new(DarlingMcpWritePolicy.Effective.Off);

    private sealed record Ctx(NpgsqlDataSource Source, NpgsqlConnection Connection, CancellationToken Ct);

    private static async Task RunAsync(Func<Ctx, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the agent mute live tests.");
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

    private static Task<string?> RuleCountAsync(Ctx c) => ScalarAsync(c, "SELECT count(*) FROM config_mute_rules");

    private static Task<string?> JournalCountAsync(Ctx c) => ScalarAsync(c, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'action_%'");

    /// <summary>A rule an operator wrote straight into the table: no claim, no journal, and whatever expiry the test gives it.</summary>
    private static async Task<string> SeedAsync(Ctx c, bool enabled, DateTime? expiresUtc, int? serverId = 7, string metric = "High CPU", string action = "mute", DateTime? createdUtc = null)
    {
        var id = Guid.NewGuid().ToString();
        await ExecAsync(c.Connection, $@"INSERT INTO config_mute_rules (id, enabled, created_at_utc, server_id, server_name, metric_name, reason, expires_at_utc, action)
VALUES ('{id}', {(enabled ? "TRUE" : "FALSE")}, {(createdUtc is null ? "now() AT TIME ZONE 'utc'" : $"'{Iso(createdUtc.Value)}'")}, {(serverId is null ? "NULL" : serverId.ToString())},
{(serverId is null ? "NULL" : $"'srv-{serverId}'")}, '{metric}', 'operator rule', {(expiresUtc is null ? "NULL" : $"'{Iso(expiresUtc.Value)}'")}, '{action}')", c.Ct);
        return id;
    }

    private static Task<string> Create(Ctx c, DarlingAgentActionSeat seat, string? metric = "Collector Cost Regression", int? serverId = null,
        string? reason = "alert storm after the collector change", string? expires = "", string? action = "digest")
    {
        expires = expires == "" ? Iso(DateTime.UtcNow.AddDays(1)) : expires;
        return DarlingMcpAlertTools.CreateMuteRule(c.Source, metric_name: metric, server_id: serverId, reason: reason, expires_at: expires,
            action: action, agentSeat: seat);
    }

    private static string Changes(string expires, string reason = "narrowing the storm rule") =>
        "{\"reason\":\"" + reason + "\",\"expires_at_utc\":" + (expires == "null" ? "null" : "\"" + expires + "\"") + "}";

    [Fact]
    public async Task AnAgentWithNoClaim_CreatesAnAllServersDigestRule_ForASelfAlert_TheServiceApplies_AndOneJournalRowPairExists()
    {
        await RunAsync(async c =>
        {
            var before = DateTime.UtcNow;
            var answer = Parse(await Create(c, Named));

            Assert.Equal("created", (string?)answer["status"]);
            var id = (string)answer["mute_rule"]!["id"]!;
            Assert.Equal("digest|true|", await ScalarAsync(c, $"SELECT action || '|' || (server_id IS NULL) || '|' || coalesce(server_name, '') FROM config_mute_rules WHERE id = '{id}'"));
            Assert.Equal("alert storm after the collector change [agent:agent-a]", await ScalarAsync(c, $"SELECT reason FROM config_mute_rules WHERE id = '{id}'"));
            var expiry = DateTime.SpecifyKind(DateTime.Parse(
                (await ScalarAsync(c, $"SELECT expires_at_utc FROM config_mute_rules WHERE id = '{id}'"))!, CultureInfo.InvariantCulture), DateTimeKind.Utc);
            Assert.InRange(expiry, before.AddDays(1).AddMinutes(-1), DateTime.UtcNow.AddDays(1).AddMinutes(1));

            /* One pair: action_called then action_done, no request behind them, the agent as the actor, the reason, and the rule after the write. */
            Assert.Equal("action_called|action_done", await ScalarAsync(c,
                "SELECT string_agg(event, '|' ORDER BY journal_id) FROM config.investigation_journal WHERE event LIKE 'action_%'"));
            Assert.Equal("2", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'action_%' AND request_id IS NULL AND actor = 'agent:agent-a'"));
            Assert.Equal("alert storm after the collector change|create_mute_rule|mute", await ScalarAsync(c,
                "SELECT reason || '|' || (detail->>'tool') || '|' || (detail->>'kind') FROM config.investigation_journal WHERE event = 'action_called'"));
            Assert.Equal(id, await ScalarAsync(c, "SELECT detail->'after'->'mute_rule'->>'id' FROM config.investigation_journal WHERE event = 'action_done'"));

            /* get_agent_changes lists it. */
            var changes = Parse(await DarlingMcpIncidentActionTools.GetAgentChanges(c.Source));
            Assert.Equal(1, (int)changes["count"]!);
            Assert.Contains("create_mute_rule", changes["changes"]!.ToJsonString(), StringComparison.Ordinal);
            Assert.Contains("agent:agent-a", changes["changes"]!.ToJsonString(), StringComparison.Ordinal);

            /* The service applies it: loaded from the store, the rule holds a matching fire for the digest on any server, and mutes nothing. */
            var service = new MuteRuleService(new PgMuteRuleStore(c.Source), NullLogger<MuteRuleService>.Instance);
            await service.LoadAsync();
            var fire = new AlertMuteContext { ServerName = "srv-8", ServerId = 8, MetricName = "Collector Cost Regression" };
            Assert.True(service.IsInDigestWindow(fire));
            Assert.False(service.IsAlertMuted(fire));
            Assert.False(service.IsInDigestWindow(new AlertMuteContext { ServerName = "srv-8", ServerId = 8, MetricName = "High CPU" }));
        });
    }

    [Fact]
    public async Task ALegacyTokenWrite_IsJournaledUnderTheFixedLegacyAgentId()
    {
        await RunAsync(async c =>
        {
            Assert.Equal("created", (string?)Parse(await Create(c, Legacy, action: "mute"))["status"]);
            Assert.Equal("2", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_journal WHERE actor = 'agent:legacy-token'"));
        });
    }

    [Fact]
    public async Task ACreate_WithNoExpiry_OrTooFarAhead_OrInThePast_OrNoReason_IsRefused_AndWritesNothing()
    {
        await RunAsync(async c =>
        {
            var noExpiry = await DarlingMcpAlertTools.CreateMuteRule(c.Source, metric_name: "Collector Cost Regression", reason: "storm", action: "digest", agentSeat: Named);
            var eightDays = await Create(c, Named, expires: Iso(DateTime.UtcNow.AddDays(8)));
            var past = await Create(c, Named, expires: Iso(DateTime.UtcNow.AddMinutes(-5)));
            var noReason = await Create(c, Named, reason: " ");

            Assert.Equal(DarlingMcpAgentMuteDirect.ExpiryCode, (string?)Parse(noExpiry)["reason"]);
            Assert.Contains("at most 7 days ahead", noExpiry, StringComparison.Ordinal);
            Assert.Equal(DarlingMcpAgentMuteDirect.ExpiryCode, (string?)Parse(eightDays)["reason"]);
            Assert.Equal(DarlingMcpAgentMuteDirect.ExpiryCode, (string?)Parse(past)["reason"]);
            Assert.Equal(DarlingMcpAgentMuteDirect.ReasonCode, (string?)Parse(noReason)["reason"]);
            Assert.Equal("0", await RuleCountAsync(c));
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_journal"));
        });
    }

    [Fact]
    public async Task AnExpiryWithAnOffset_IsJudgedAsTheInstantItNames_NotTheClockReadingItCarries()
    {
        await RunAsync(async c =>
        {
            /* 6 days 22 hours ahead, written as the wall clock at UTC+14: the clock reading says 7 days 12 hours, but the instant is inside the bound. */
            var inside = DateTime.UtcNow.AddDays(6).AddHours(22);
            var accepted = await Create(c, Named, expires: inside.AddHours(14).ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture) + "+14:00");
            /* 7 days 2 hours ahead, written as the wall clock at UTC-10: the clock reading says 6 days 16 hours, but the instant is past the bound. */
            var outside = DateTime.UtcNow.AddDays(7).AddHours(2);
            var refused = await Create(c, Named, metric: "High CPU", expires: outside.AddHours(-10).ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture) + "-10:00");

            Assert.Equal("created", (string?)Parse(accepted)["status"]);
            Assert.Equal(DarlingMcpAgentMuteDirect.ExpiryCode, (string?)Parse(refused)["reason"]);
            Assert.Equal("1", await RuleCountAsync(c));
        });
    }

    [Fact]
    public async Task AnUpdate_CannotLeaveTheBound_ByOmittingClearingExtendingOrAliasingTheExpiry()
    {
        await RunAsync(async c =>
        {
            var operatorRule = await SeedAsync(c, enabled: true, expiresUtc: null);
            var agentRule = Parse(await Create(c, Named, metric: "High CPU", serverId: 7, action: "mute"));
            var agentId = (string)agentRule["mute_rule"]!["id"]!;
            var journalBefore = await JournalCountAsync(c);

            foreach (var id in new[] { operatorRule, agentId })
            {
                foreach (var changes in new[]
                {
                    "{\"reason\":\"x\"}",
                    Changes("null"),
                    Changes(Iso(DateTime.UtcNow.AddDays(30))),
                    Changes(Iso(DateTime.UtcNow.AddDays(-1))),
                    "{\"reason\":\"x\",\"expires_at\":\"" + Iso(DateTime.UtcNow.AddDays(9)) + "\"}",
                })
                {
                    var answer = await DarlingMcpAlertTools.UpdateMuteRule(c.Source, id, changes, agentSeat: Named);
                    Assert.Equal(DarlingMcpAgentMuteDirect.ExpiryCode, (string?)Parse(answer)["reason"]);
                }
            }

            /* A blank expiry is not an instant: refused as invalid, nothing written. */
            Assert.Equal("invalid", (string?)Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, operatorRule, "{\"reason\":\"x\",\"expires_at_utc\":\"\"}", agentSeat: Named))["status"]);
            Assert.Equal(journalBefore, await JournalCountAsync(c));
            Assert.Equal("True", await ScalarAsync(c, $"SELECT expires_at_utc IS NULL FROM config_mute_rules WHERE id = '{operatorRule}'"));
            Assert.Equal("operator rule", await ScalarAsync(c, $"SELECT reason FROM config_mute_rules WHERE id = '{operatorRule}'"));

            /* Inside the bound it works, and the journal row carries the rule as it was and as it is. */
            var ok = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, operatorRule, Changes(Iso(DateTime.UtcNow.AddDays(2))), agentSeat: Named));
            Assert.Equal("updated", (string?)ok["status"]);
            Assert.Equal("narrowing the storm rule [agent:agent-a]", await ScalarAsync(c, $"SELECT reason FROM config_mute_rules WHERE id = '{operatorRule}'"));
            Assert.Equal("update_mute_rule|operator rule|narrowing the storm rule [agent:agent-a]", await ScalarAsync(c,
                "SELECT (j.detail->>'tool') || '|' || (j.detail->'before'->>'reason') || '|' || (d.detail->'after'->'mute_rule'->>'reason') " +
                "FROM config.investigation_journal j JOIN config.investigation_journal d ON d.detail->>'called_id' = j.journal_id::text " +
                $"WHERE j.event = 'action_called' AND j.detail->>'tool' = 'update_mute_rule' AND j.detail->'target'->>'id' = '{operatorRule}'"));
        });
    }

    [Fact]
    public async Task AnEnable_NeedsTheStoredExpiryInsideTheBound_ADisableAndADelete_AreAlwaysAllowed_AndEachIsJournaledWithItsBeforeImage()
    {
        await RunAsync(async c =>
        {
            var none = await SeedAsync(c, enabled: false, expiresUtc: null);
            var expired = await SeedAsync(c, enabled: false, expiresUtc: DateTime.UtcNow.AddDays(-1));
            var far = await SeedAsync(c, enabled: false, expiresUtc: DateTime.UtcNow.AddDays(30));
            var near = await SeedAsync(c, enabled: false, expiresUtc: DateTime.UtcNow.AddDays(3));

            foreach (var id in new[] { none, expired, far })
            {
                var refused = Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, id, true, agentSeat: Named, reason: "turn it on"));
                Assert.Equal(DarlingMcpAgentMuteDirect.ExpiryCode, (string?)refused["reason"]);
                Assert.Equal("False", await ScalarAsync(c, $"SELECT enabled FROM config_mute_rules WHERE id = '{id}'"));
            }

            Assert.Equal("0", await JournalCountAsync(c));
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, near, true, agentSeat: Named, reason: "turn it on"))["status"]);
            Assert.Equal("True", await ScalarAsync(c, $"SELECT enabled FROM config_mute_rules WHERE id = '{near}'"));
            Assert.Equal("2", await JournalCountAsync(c));

            /* Off is allowed on an operator's rule that has no expiry at all. */
            var open = await SeedAsync(c, enabled: true, expiresUtc: null);
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, open, false, agentSeat: Named, reason: "storm over"))["status"]);
            Assert.Equal("False", await ScalarAsync(c, $"SELECT enabled FROM config_mute_rules WHERE id = '{open}'"));

            var deleted = Parse(await DarlingMcpAlertTools.DeleteMuteRule(c.Source, none, agentSeat: Named, reason: "stale"));
            Assert.Equal("deleted", (string?)deleted["status"]);
            Assert.Equal("0", await ScalarAsync(c, $"SELECT count(*) FROM config_mute_rules WHERE id = '{none}'"));
            Assert.Equal("operator rule", await ScalarAsync(c,
                $"SELECT detail->'before'->>'reason' FROM config.investigation_journal WHERE event = 'action_called' AND detail->>'tool' = 'delete_mute_rule' AND detail->'target'->>'id' = '{none}'"));
            Assert.Equal("stale", await ScalarAsync(c,
                $"SELECT reason FROM config.investigation_journal WHERE event = 'action_called' AND detail->>'tool' = 'delete_mute_rule'"));
            Assert.Equal("not_found", (string?)Parse(await DarlingMcpAlertTools.DeleteMuteRule(c.Source, none, agentSeat: Named, reason: "again"))["status"]);
        });
    }

    [Fact]
    public async Task TheRepeatScheduleAndStartsAt_AreRefusedOnTheNewPath_NothingWritten()
    {
        await RunAsync(async c =>
        {
            var future = Iso(DateTime.UtcNow.AddDays(1));
            var rule = await SeedAsync(c, enabled: true, expiresUtc: DateTime.UtcNow.AddDays(1));
            var create = await DarlingMcpAlertTools.CreateMuteRule(c.Source, metric_name: "High CPU", reason: "storm", expires_at: future,
                recur_start: "22:00", recur_end: "23:00", agentSeat: Named);
            var createStarts = await DarlingMcpAlertTools.CreateMuteRule(c.Source, metric_name: "High CPU", reason: "storm", expires_at: future,
                starts_at: Iso(DateTime.UtcNow.AddHours(1)), agentSeat: Named);
            var update = await DarlingMcpAlertTools.UpdateMuteRule(c.Source, rule,
                "{\"reason\":\"storm\",\"expires_at_utc\":\"" + future + "\",\"recur_days\":[\"Mon\"],\"recur_start\":\"22:00\",\"recur_end\":\"23:00\"}", agentSeat: Named);
            var updateStarts = await DarlingMcpAlertTools.UpdateMuteRule(c.Source, rule,
                "{\"reason\":\"storm\",\"expires_at_utc\":\"" + future + "\",\"starts_at_utc\":\"" + Iso(DateTime.UtcNow.AddHours(1)) + "\"}", agentSeat: Named);

            Assert.Contains("repeat schedule", create, StringComparison.Ordinal);
            Assert.Contains("starts_at is not available", createStarts, StringComparison.Ordinal);
            Assert.Contains("repeat schedule", update, StringComparison.Ordinal);
            Assert.Contains("starts_at is not available", updateStarts, StringComparison.Ordinal);
            Assert.Equal("1", await RuleCountAsync(c));
            Assert.Equal("operator rule", await ScalarAsync(c, $"SELECT reason FROM config_mute_rules WHERE id = '{rule}'"));
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_journal"));
        });
    }

    [Fact]
    public async Task ACallThatIsNotAnAgentToken_GetsWhatItGetsToday_NoBoundNoReasonNoJournalRow()
    {
        await RunAsync(async c =>
        {
            /* The writes arm: an operator's permanent whole-fleet digest rule, no reason, no expiry. */
            var created = Parse(await DarlingMcpAlertTools.CreateMuteRule(c.Source, metric_name: "Collector Cost Regression", action: "digest", agentSeat: Operator));
            Assert.Equal("created", (string?)created["status"]);
            var id = (string)created["mute_rule"]!["id"]!;
            Assert.Equal("True", await ScalarAsync(c, $"SELECT expires_at_utc IS NULL FROM config_mute_rules WHERE id = '{id}'"));
            var update = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, id, "{\"reason\":\"permanent\"}", agentSeat: Operator));
            Assert.Equal("updated", (string?)update["status"]);
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, id, false, agentSeat: Operator))["status"]);
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, id, true, agentSeat: Operator))["status"]);
            Assert.Equal("deleted", (string?)Parse(await DarlingMcpAlertTools.DeleteMuteRule(c.Source, id, agentSeat: Operator))["status"]);
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_journal"));

            /* Neither arm: the gate still refuses it as not served, and nothing is written. */
            var refused = Parse(await DarlingMcpAlertTools.CreateMuteRule(c.Source, metric_name: "High CPU", server_id: 7, reason: "storm",
                expires_at: Iso(DateTime.UtcNow.AddDays(1)), agentSeat: NoArm));
            Assert.Equal(DarlingAgentActionGate.NotServed, (string?)refused["reason"]);
            Assert.Equal("0", await RuleCountAsync(c));

            /* An agent_action present is the claim path, so the new path's bound does not run: the gate answers its own refusal. */
            var claim = Parse(await DarlingMcpAlertTools.CreateMuteRule(c.Source, metric_name: "High CPU", server_id: 7, reason: "storm",
                agent_action: "{}", agentSeat: Legacy));
            Assert.Equal("agent_action_invalid", (string?)claim["reason"]);
            Assert.Equal("0", await RuleCountAsync(c));
        });
    }

    [Fact]
    public async Task TheServicesAgentSwitch_TurnsTheNewPathOff()
    {
        await RunAsync(async c =>
        {
            await ExecAsync(c.Connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", c.Ct);
            await ExecAsync(c.Connection, "UPDATE config.config_service SET agent_actions_enabled = false", c.Ct);
            var refused = Parse(await Create(c, Named));

            Assert.Equal("actions_off", (string?)refused["reason"]);
            Assert.Equal("0", await RuleCountAsync(c));
            Assert.Equal("0", await JournalCountAsync(c));
        });
    }

    [Fact]
    public async Task AJournalWriteThatFails_RollsBackTheRule_AndARuleWriteThatFails_LeavesNoJournalRow()
    {
        await RunAsync(async c =>
        {
            /* A rule write the store refuses (an unknown server_id) leaves no journal row. */
            var unknownServer = Parse(await Create(c, Named, metric: "High CPU", serverId: 99, action: "mute"));
            Assert.Equal("invalid", (string?)unknownServer["status"]);
            Assert.Equal("0", await RuleCountAsync(c));
            Assert.Equal("0", await JournalCountAsync(c));

            /* A journal insert that fails after the rule insert rolls the rule back. */
            await ExecAsync(c.Connection, @"CREATE FUNCTION config.fail_journal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'journal down'; END $$;
CREATE TRIGGER fail_journal BEFORE INSERT ON config.investigation_journal FOR EACH ROW EXECUTE FUNCTION config.fail_journal();", c.Ct);
            var failed = await Create(c, Named);
            Assert.Equal("error", (string?)Parse(failed)["status"]);
            Assert.Equal("0", await RuleCountAsync(c));

            var seeded = await SeedAsync(c, enabled: true, expiresUtc: DateTime.UtcNow.AddDays(2));
            var updateFailed = await DarlingMcpAlertTools.UpdateMuteRule(c.Source, seeded, Changes(Iso(DateTime.UtcNow.AddDays(1))), agentSeat: Named);
            Assert.Equal("error", (string?)Parse(updateFailed)["status"]);
            Assert.Equal("operator rule", await ScalarAsync(c, $"SELECT reason FROM config_mute_rules WHERE id = '{seeded}'"));
            var deleteFailed = await DarlingMcpAlertTools.DeleteMuteRule(c.Source, seeded, agentSeat: Named, reason: "x");
            Assert.Equal("error", (string?)Parse(deleteFailed)["status"]);
            Assert.Equal("1", await RuleCountAsync(c));
        });
    }

    private static readonly DarlingAgentActionSeat SwitchedOff = new(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true, TokenName: "agent-a"),
        agentMuteRulesAllowed: false);

    [Fact]
    public async Task ARuleWithNoMetricAndNoServer_IsRefusedOnCreateAndUpdate_NamingAnOperator_AndWritesNothing()
    {
        await RunAsync(async c =>
        {
            var create = Parse(await Create(c, Named, metric: null, serverId: null, action: "digest"));
            var created = Parse(await Create(c, Named, metric: "High CPU", serverId: 7, action: "mute"));
            var id = (string)created["mute_rule"]!["id"]!;
            var journal = await JournalCountAsync(c);
            var update = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, id,
                "{\"reason\":\"widen\",\"expires_at_utc\":\"" + Iso(DateTime.UtcNow.AddDays(1)) + "\",\"metric_name\":null,\"server_id\":null,\"server_name\":null}", agentSeat: Named));

            Assert.True(DarlingMcpAgentMuteDirect.NoMetricCode == (string?)create["reason"], create.ToJsonString());
            Assert.Contains("An operator can make a rule with no metric", (string?)create["message"], StringComparison.Ordinal);
            Assert.True(DarlingMcpAgentMuteDirect.NoMetricCode == (string?)update["reason"], update.ToJsonString());
            Assert.Equal("1", await RuleCountAsync(c));
            Assert.Equal("High CPU", await ScalarAsync(c, $"SELECT metric_name FROM config_mute_rules WHERE id = '{id}'"));
            Assert.Equal(journal, await JournalCountAsync(c));
        });
    }

    [Fact]
    public async Task AnUpdate_CannotMoveTheExpiryPastCreationPlusSevenDays_ButMayShortenIt_EvenOnAnOlderOperatorRule()
    {
        await RunAsync(async c =>
        {
            /* Made 5 days ago, expiring tomorrow: it may live until created + 7 days, which is 2 days from now. */
            var aging = await SeedAsync(c, enabled: true, expiresUtc: DateTime.UtcNow.AddDays(1), createdUtc: DateTime.UtcNow.AddDays(-5));
            var refused = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, aging, Changes(Iso(DateTime.UtcNow.AddDays(4))), agentSeat: Named));
            Assert.Equal(DarlingMcpAgentMuteDirect.LifetimeCode, (string?)refused["reason"]);
            Assert.Contains("An operator can extend it", (string?)refused["message"], StringComparison.Ordinal);
            Assert.Contains(DateTime.UtcNow.AddDays(2).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), (string?)refused["message"], StringComparison.Ordinal);
            Assert.Equal("0", await JournalCountAsync(c));

            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, aging, Changes(Iso(DateTime.UtcNow.AddDays(1.5))), agentSeat: Named))["status"]);
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, aging, Changes(Iso(DateTime.UtcNow.AddHours(6))), agentSeat: Named))["status"]);

            /* An operator's old rule with no expiry: an agent can only cut it short, and never move it out again. */
            var old = await SeedAsync(c, enabled: true, expiresUtc: null, createdUtc: DateTime.UtcNow.AddDays(-30));
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, old, Changes(Iso(DateTime.UtcNow.AddDays(1))), agentSeat: Named))["status"]);
            Assert.Equal(DarlingMcpAgentMuteDirect.LifetimeCode,
                (string?)Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, old, Changes(Iso(DateTime.UtcNow.AddDays(2))), agentSeat: Named))["reason"]);

            /* An agent's own new rule: it can renew inside creation + 7 days, and not past it. */
            /* A different metric and server from the rules above: the agent edits of those are in the journal, and the same-match check would refuse a repeat. */
            var made = (string)Parse(await Create(c, Named, metric: "Deadlocks", serverId: 8, action: "mute", expires: Iso(DateTime.UtcNow.AddDays(1))))["mute_rule"]!["id"]!;
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, made, Changes(Iso(DateTime.UtcNow.AddDays(3))), agentSeat: Named))["status"]);
            await ExecAsync(c.Connection, $"UPDATE config_mute_rules SET created_at_utc = now() AT TIME ZONE 'utc' - interval '6 days' WHERE id = '{made}'", c.Ct);
            Assert.Equal(DarlingMcpAgentMuteDirect.LifetimeCode,
                (string?)Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, made, Changes(Iso(DateTime.UtcNow.AddDays(3))), agentSeat: Named))["reason"]);
        });
    }

    [Fact]
    public async Task AMatchAnAgentMadeInTheLastFourteenDays_IsNotMadeAgain_EvenAfterItWasDeleted_ButADifferentMatchIs()
    {
        await RunAsync(async c =>
        {
            var first = Parse(await Create(c, Named));
            var id = (string)first["mute_rule"]!["id"]!;
            Assert.Equal("deleted", (string?)Parse(await DarlingMcpAlertTools.DeleteMuteRule(c.Source, id, agentSeat: Named, reason: "storm over"))["status"]);
            var journal = await JournalCountAsync(c);

            var again = Parse(await Create(c, Legacy));
            Assert.Equal(DarlingMcpAgentMuteDirect.RepeatCode, (string?)again["reason"]);
            Assert.Contains("operator", (string?)again["message"], StringComparison.Ordinal);
            Assert.Equal("0", await RuleCountAsync(c));
            Assert.Equal(journal, await JournalCountAsync(c));

            /* Not the same match: another action, another server, another metric. */
            Assert.Equal("created", (string?)Parse(await Create(c, Named, action: "mute"))["status"]);
            Assert.Equal("created", (string?)Parse(await Create(c, Named, serverId: 7))["status"]);
            Assert.Equal("created", (string?)Parse(await Create(c, Named, metric: "High CPU"))["status"]);

            /* The look-back is twice the 7 day lifetime: a match is off as long as it was on. At 8 days (the old rule's end) it is still refused,
               and the refusal says an operator can make a longer rule. Past 14 days it is allowed again. */
            await ExecAsync(c.Connection, "UPDATE config.investigation_journal SET event_time = event_time - interval '8 days'", c.Ct);
            await ExecAsync(c.Connection, "DELETE FROM config_mute_rules", c.Ct);
            var eightDays = Parse(await Create(c, Named));
            Assert.Equal(DarlingMcpAgentMuteDirect.RepeatCode, (string?)eightDays["reason"]);
            Assert.Contains("14 days", (string?)eightDays["message"], StringComparison.Ordinal);
            Assert.Contains("operator can make", (string?)eightDays["message"], StringComparison.Ordinal);
            Assert.Equal("0", await RuleCountAsync(c));

            await ExecAsync(c.Connection, "UPDATE config.investigation_journal SET event_time = event_time - interval '7 days'", c.Ct);
            Assert.Equal("created", (string?)Parse(await Create(c, Named))["status"]);
        });
    }

    private static string Move(string field, string value) =>
        "{\"reason\":\"moving the rule\",\"expires_at_utc\":\"" + Iso(DateTime.UtcNow.AddDays(1)) + "\",\"" + field + "\":" + value + "}";

    [Fact]
    public async Task AnUpdate_ThatMovesARuleOntoAMatchAnAgentMadeInTheLastFourteenDays_IsRefused_AndAMoveOntoAFreshMatchIsRememberedLikeACreate()
    {
        await RunAsync(async c =>
        {
            /* An agent made a rule on the metric, then deleted it. Its journal rows stay. */
            var storm = (string)Parse(await Create(c, Named, metric: "Collector Cost Regression"))["mute_rule"]!["id"]!;
            Assert.Equal("deleted", (string?)Parse(await DarlingMcpAlertTools.DeleteMuteRule(c.Source, storm, agentSeat: Named, reason: "storm over"))["status"]);

            var other = (string)Parse(await Create(c, Named, metric: "Long Running Query"))["mute_rule"]!["id"]!;
            var journal = await JournalCountAsync(c);

            /* The rule is not allowed onto that match, by metric, and nothing is written. */
            var metric = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, other, Move("metric_name", "\"Collector Cost Regression\""), agentSeat: Named));
            Assert.Equal(DarlingMcpAgentMuteDirect.RepeatCode, (string?)metric["reason"]);
            Assert.Equal("Long Running Query", await ScalarAsync(c, $"SELECT metric_name FROM config_mute_rules WHERE id = '{other}'"));
            Assert.Equal(journal, await JournalCountAsync(c));

            /* A rule made on one server is not allowed onto another server where the agent already made it, by server. */
            var onSeven = (string)Parse(await Create(c, Named, metric: "Blocking Chain", serverId: 7))["mute_rule"]!["id"]!;
            var onEight = (string)Parse(await Create(c, Named, metric: "Blocking Chain", serverId: 8))["mute_rule"]!["id"]!;
            Assert.Equal("deleted", (string?)Parse(await DarlingMcpAlertTools.DeleteMuteRule(c.Source, onEight, agentSeat: Named, reason: "storm over"))["status"]);
            var server = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, onSeven, Move("server_id", "8"), agentSeat: Named));
            Assert.Equal(DarlingMcpAgentMuteDirect.RepeatCode, (string?)server["reason"]);
            Assert.Equal("7", await ScalarAsync(c, $"SELECT server_id FROM config_mute_rules WHERE id = '{onSeven}'"));

            /* By action. */
            var mute = (string)Parse(await Create(c, Named, metric: "Deadlocks", action: "mute"))["mute_rule"]!["id"]!;
            Assert.Equal("deleted", (string?)Parse(await DarlingMcpAlertTools.DeleteMuteRule(c.Source, mute, agentSeat: Named, reason: "storm over"))["status"]);
            var digest = (string)Parse(await Create(c, Named, metric: "Deadlocks", action: "digest"))["mute_rule"]!["id"]!;
            var action = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, digest, Move("action", "\"mute\""), agentSeat: Named));
            Assert.Equal(DarlingMcpAgentMuteDirect.RepeatCode, (string?)action["reason"]);

            /* A move onto a match nobody made is allowed, and the move counts as a rule made on it: a create there is refused afterwards. */
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, other, Move("metric_name", "\"Tempdb Space\""), agentSeat: Named))["status"]);
            var created = Parse(await Create(c, Named, metric: "Tempdb Space"));
            Assert.Equal(DarlingMcpAgentMuteDirect.RepeatCode, (string?)created["reason"]);

            /* An update that leaves the match alone is not held to the look-back. */
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, other, Changes(Iso(DateTime.UtcNow.AddHours(5))), agentSeat: Named))["status"]);
        });
    }

    private static Task<string> CreateNamed(Ctx c, DarlingAgentActionSeat seat, string serverName, string metric) =>
        DarlingMcpAlertTools.CreateMuteRule(c.Source, server_name: serverName, metric_name: metric, reason: "alert storm after the collector change",
            expires_at: Iso(DateTime.UtcNow.AddDays(1)), action: "digest", agentSeat: seat);

    [Fact]
    public async Task OneServerNamedByIdAndByName_IsOneMatch_InBothDirections_ButAllServersIsAnother()
    {
        await RunAsync(async c =>
        {
            /* By id, then by name (the display name). */
            Assert.Equal("created", (string?)Parse(await Create(c, Named, metric: "Blocking Chain", serverId: 7))["status"]);
            Assert.Equal(DarlingMcpAgentMuteDirect.RepeatCode, (string?)Parse(await CreateNamed(c, Named, "SRV-7", "Blocking Chain"))["reason"]);

            /* By name, then by id. */
            Assert.Equal("created", (string?)Parse(await CreateNamed(c, Named, "srv-8", "Deadlocks"))["status"]);
            Assert.Equal(DarlingMcpAgentMuteDirect.RepeatCode, (string?)Parse(await Create(c, Named, metric: "Deadlocks", serverId: 8))["reason"]);

            /* A name no server has still matches itself. */
            Assert.Equal("created", (string?)Parse(await CreateNamed(c, Named, "ghost", "Tempdb Space"))["status"]);
            Assert.Equal(DarlingMcpAgentMuteDirect.RepeatCode, (string?)Parse(await CreateNamed(c, Named, "Ghost", "Tempdb Space"))["reason"]);

            /* Another server, and all servers, are other matches: a name-keyed rule is not "all servers". */
            Assert.Equal("created", (string?)Parse(await CreateNamed(c, Named, "srv-8", "Blocking Chain"))["status"]);
            Assert.Equal("created", (string?)Parse(await Create(c, Named, metric: "Deadlocks"))["status"]);
            Assert.Equal("5", await RuleCountAsync(c));
        });
    }

    /// <summary>A rule with no metric is refused even when it names something else. On one server, or with only a source, it would silence every alert it covers for 7 days.</summary>
    [Fact]
    public async Task ARuleWithNoMetric_ThatNamesAServerOrASource_IsRefusedOnCreateUpdateAndEnable_AndWritesNothing()
    {
        await RunAsync(async c =>
        {
            var oneServer = Parse(await Create(c, Named, metric: null, serverId: 7, action: "mute"));
            var sourceOnly = Parse(await DarlingMcpAlertTools.CreateMuteRule(c.Source, external_source: "nagios", reason: "alert storm",
                expires_at: Iso(DateTime.UtcNow.AddDays(1)), action: "mute", agentSeat: Named));
            Assert.True(DarlingMcpAgentMuteDirect.NoMetricCode == (string?)oneServer["reason"], oneServer.ToJsonString());
            Assert.True(DarlingMcpAgentMuteDirect.NoMetricCode == (string?)sourceOnly["reason"], sourceOnly.ToJsonString());
            Assert.Equal("0", await RuleCountAsync(c));

            var created = Parse(await Create(c, Named, metric: "High CPU", serverId: 7, action: "mute"));
            var id = (string)created["mute_rule"]!["id"]!;
            var journal = await JournalCountAsync(c);
            var update = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, id,
                "{\"reason\":\"widen\",\"expires_at_utc\":\"" + Iso(DateTime.UtcNow.AddDays(1)) + "\",\"metric_name\":null}", agentSeat: Named));
            Assert.True(DarlingMcpAgentMuteDirect.NoMetricCode == (string?)update["reason"], update.ToJsonString());
            Assert.Equal("High CPU", await ScalarAsync(c, $"SELECT metric_name FROM config_mute_rules WHERE id = '{id}'"));

            var operatorRule = Guid.NewGuid().ToString();
            await ExecAsync(c.Connection, $@"INSERT INTO config_mute_rules (id, enabled, created_at_utc, server_id, server_name, reason, expires_at_utc, action)
VALUES ('{operatorRule}', FALSE, now() AT TIME ZONE 'utc', 7, 'srv-7', 'operator rule', '{Iso(DateTime.UtcNow.AddDays(2))}', 'mute')", c.Ct);
            var enable = Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, operatorRule, true, agentSeat: Named, reason: "turn it on"));
            Assert.Equal(DarlingMcpAgentMuteDirect.NoMetricCode, (string?)enable["reason"]);
            Assert.Equal("False", await ScalarAsync(c, $"SELECT enabled FROM config_mute_rules WHERE id = '{operatorRule}'"));
            Assert.Equal(journal, await JournalCountAsync(c));
        });
    }

    [Fact]
    public async Task AnEnable_OfARuleThatMatchesEveryAlert_IsRefused_ButItsDisableIsNot()
    {
        await RunAsync(async c =>
        {
            var id = Guid.NewGuid().ToString();
            await ExecAsync(c.Connection, $@"INSERT INTO config_mute_rules (id, enabled, created_at_utc, reason, expires_at_utc, action)
VALUES ('{id}', FALSE, now() AT TIME ZONE 'utc', 'operator rule', '{Iso(DateTime.UtcNow.AddDays(2))}', 'mute')", c.Ct);

            var refused = Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, id, true, agentSeat: Named, reason: "turn it on"));
            Assert.Equal(DarlingMcpAgentMuteDirect.NoMetricCode, (string?)refused["reason"]);
            Assert.Contains("An operator can make a rule with no metric", (string?)refused["message"], StringComparison.Ordinal);
            Assert.Equal("False", await ScalarAsync(c, $"SELECT enabled FROM config_mute_rules WHERE id = '{id}'"));
            Assert.Equal("0", await JournalCountAsync(c));

            await ExecAsync(c.Connection, $"UPDATE config_mute_rules SET enabled = TRUE WHERE id = '{id}'", c.Ct);
            Assert.Equal("updated", (string?)Parse(await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, id, false, agentSeat: Named, reason: "storm over"))["status"]);
            Assert.Equal("False", await ScalarAsync(c, $"SELECT enabled FROM config_mute_rules WHERE id = '{id}'"));
        });
    }

    [Fact]
    public async Task AChangesDocumentOverTheLimit_IsRefusedForItsSize_WhateverItHolds_AndWritesNothing()
    {
        await RunAsync(async c =>
        {
            var rule = await SeedAsync(c, enabled: true, expiresUtc: DateTime.UtcNow.AddDays(1));
            /* Not JSON at all: the size is judged first, so it is a size refusal and not an invalid-document answer. */
            var garbage = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, rule, new string('x', DarlingMcpAgentMuteDirect.MaxChangesChars + 1), agentSeat: Named));
            Assert.Equal(DarlingMcpAgentMuteDirect.SizeCode, (string?)garbage["reason"]);
            Assert.Contains("8192", (string?)garbage["message"], StringComparison.Ordinal);
            Assert.Equal("0", await JournalCountAsync(c));
        });
    }

    [Fact]
    public async Task WithTheSettingOff_EveryVerbRefuses_NamingTheSetting_AndWritesNothing()
    {
        await RunAsync(async c =>
        {
            var rule = await SeedAsync(c, enabled: true, expiresUtc: DateTime.UtcNow.AddDays(1));
            var answers = new[]
            {
                await Create(c, SwitchedOff),
                await DarlingMcpAlertTools.UpdateMuteRule(c.Source, rule, Changes(Iso(DateTime.UtcNow.AddHours(5))), agentSeat: SwitchedOff),
                await DarlingMcpAlertTools.SetMuteRuleEnabled(c.Source, rule, false, agentSeat: SwitchedOff, reason: "off"),
                await DarlingMcpAlertTools.DeleteMuteRule(c.Source, rule, agentSeat: SwitchedOff, reason: "gone"),
            };

            foreach (var answer in answers)
            {
                Assert.Equal(DarlingMcpAgentMuteDirect.OffCode, (string?)Parse(answer)["reason"]);
                Assert.Contains("mcp.allowAgentMuteRules", answer, StringComparison.Ordinal);
            }

            Assert.Equal("1", await RuleCountAsync(c));
            Assert.Equal("True", await ScalarAsync(c, $"SELECT enabled FROM config_mute_rules WHERE id = '{rule}'"));
            Assert.Equal("0", await JournalCountAsync(c));

            /* The claim path is not this setting's: a call that carries agent_action still reaches the gate. */
            var claim = Parse(await DarlingMcpAlertTools.CreateMuteRule(c.Source, metric_name: "High CPU", server_id: 7, reason: "storm", agent_action: "{}", agentSeat: SwitchedOff));
            Assert.Equal("agent_action_invalid", (string?)claim["reason"]);
        });
    }

    [Fact]
    public async Task ATooLongChangeReason_IsRefusedWithTheReasonLimitInTheMessage_NotTheFieldLimit()
    {
        await RunAsync(async c =>
        {
            var rule = await SeedAsync(c, enabled: true, expiresUtc: DateTime.UtcNow.AddDays(1));
            /* #7414: the padding passes the trimmed 1 to 2000 check and fails the raw-length check, which is the reason's limit, not the 256 of other fields. */
            var padded = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, rule,
                "{\"reason\":\"x" + new string(' ', InvestigationToolCore.MaxActionReasonChars) + "\",\"expires_at_utc\":\"" + Iso(DateTime.UtcNow.AddHours(5)) + "\"}", agentSeat: Named));
            Assert.Equal(DarlingMcpAgentMuteDirect.SizeCode, (string?)padded["reason"]);
            var message = (string?)padded["message"] ?? "";
            Assert.Contains(InvestigationToolCore.MaxActionReasonChars.ToString(System.Globalization.CultureInfo.InvariantCulture) + " characters", message, StringComparison.Ordinal);
            Assert.DoesNotContain("256", message, StringComparison.Ordinal);
            Assert.Equal("0", await JournalCountAsync(c));
        });
    }

    [Fact]
    public async Task FieldsAreCapped_AndAHourlyCountOfWritesStopsARunawayAgent_NothingWrittenOnRefusal()
    {
        await RunAsync(async c =>
        {
            var long256 = new string('m', DarlingMcpAgentMuteDirect.MaxFieldChars + 1);
            var tooLongMetric = Parse(await Create(c, Named, metric: long256));
            var tooLongPattern = Parse(await DarlingMcpAlertTools.CreateMuteRule(c.Source, metric_name: "High CPU", query_text_pattern: long256, reason: "storm",
                expires_at: Iso(DateTime.UtcNow.AddDays(1)), agentSeat: Named));
            var rule = await SeedAsync(c, enabled: true, expiresUtc: DateTime.UtcNow.AddDays(1));
            var tooLongChange = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, rule,
                "{\"reason\":\"x\",\"expires_at_utc\":\"" + Iso(DateTime.UtcNow.AddHours(5)) + "\",\"database_pattern\":\"" + long256 + "\"}", agentSeat: Named));
            var tooBigBody = Parse(await DarlingMcpAlertTools.UpdateMuteRule(c.Source, rule,
                "{\"reason\":\"x\",\"pad\":\"" + new string('p', DarlingMcpAgentMuteDirect.MaxChangesChars) + "\"}", agentSeat: Named));
            foreach (var answer in new[] { tooLongMetric, tooLongPattern, tooLongChange, tooBigBody })
            {
                Assert.Equal(DarlingMcpAgentMuteDirect.SizeCode, (string?)answer["reason"]);
                Assert.Contains(answer == tooBigBody ? "8192" : "256", (string?)answer["message"] ?? "", StringComparison.Ordinal);
            }

            Assert.Equal("1", await RuleCountAsync(c));
            Assert.Equal("0", await JournalCountAsync(c));

            /* The limit is a count of direct writes in the journal in the last hour, across tokens. 19 old-enough rows leave room for one, 20 do not. */
            for (var i = 0; i < DarlingMcpAgentMuteDirect.MaxWritesPerHour - 1; i++)
                await ExecAsync(c.Connection, "INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, reason, detail) " +
                    "VALUES (now(), NULL, NULL, 'action_called', 'agent:other', 'x', '{}'::jsonb)", c.Ct);
            Assert.Equal("created", (string?)Parse(await Create(c, Named))["status"]);
            var rateRefused = Parse(await Create(c, Named, metric: "High CPU", serverId: 8, action: "mute"));
            Assert.Equal(DarlingMcpAgentMuteDirect.RateCode, (string?)rateRefused["reason"]);
            Assert.Contains("20", (string?)rateRefused["message"], StringComparison.Ordinal);
            Assert.Equal("2", await RuleCountAsync(c));

            /* Rows older than an hour do not count. */
            await ExecAsync(c.Connection, "UPDATE config.investigation_journal SET event_time = event_time - interval '2 hours'", c.Ct);
            Assert.Equal("created", (string?)Parse(await Create(c, Named, metric: "High CPU", serverId: 8, action: "mute"))["status"]);
        });
    }
}
