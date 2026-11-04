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
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>The served value of <c>mcp.allowAgentMuteRules</c> (#7414), registered by the host and read by the agent seat.</summary>
internal sealed record AgentMuteRulesSetting(bool Allowed);

/// <summary>
/// #7414: an agent token writes a mute or digest rule with no investigation claim. The rule may name one server or all servers and any
/// metric, the same scope an operator has. The price is a bound on what the rule can do: the rule left behind must expire in the future
/// and at most <see cref="MaxExpiryDays"/> days ahead, a <c>reason</c> is required, and the write and its agent change journal row (the
/// agent, the reason, the rule before and after) commit in one transaction, so a refused or failed write leaves nothing.
/// <para>
/// Only the investigations arm gets here (<see cref="Applies"/>): a call with the writes arm is an operator's write and never reaches
/// this class, and a policy with neither arm is still refused <c>not_served</c> by the gate. A call that carries an <c>agent_action</c>
/// is the claim path (<see cref="DarlingMcpAgentActions"/>), unchanged. The repeat schedule and <c>starts_at</c> refusals are the claim
/// path's own, on every verb here.
/// </para>
/// </summary>
internal static class DarlingMcpAgentMuteDirect
{
    /// <summary>The furthest ahead an agent's rule may expire, in days.</summary>
    internal const int MaxExpiryDays = 7;

    internal const string ExpiryCode = "agent_mute_expiry";
    internal const string ReasonCode = "agent_mute_reason";
    internal const string LifetimeCode = "agent_mute_lifetime";
    internal const string AllAlertsCode = "agent_mute_all_alerts";
    internal const string RepeatCode = "agent_mute_repeat";
    internal const string SizeCode = "agent_mute_size";
    internal const string RateCode = "agent_mute_rate";
    internal const string OffCode = "agent_mute_rules_off";

    /// <summary>The darling.json key that turns this path off (default on). Levels 2 and 3 are not affected by it.</summary>
    internal const string SettingName = "mcp.allowAgentMuteRules";

    /// <summary>The longest any text field an agent supplies may be (a metric, server name, pattern, source or app name). Neither the operator's path nor the
    /// store limits these, so this is a sensible bound: the longest real alert metric name and wait type are well under it.</summary>
    internal const int MaxFieldChars = 256;

    /// <summary>The longest <c>changes_json</c> an update may carry.</summary>
    internal const int MaxChangesChars = 8192;

    /// <summary>
    /// Direct-path writes per service per hour. A real storm needs a few rules: say three create calls, a couple of updates to narrow
    /// them and a delete when it ends, about ten writes, and a rule made twice because the first guess was wrong stays far under this.
    /// Twenty leaves that headroom twice over and still holds a runaway loop to 20 rows an hour, so a change an operator may want to
    /// undo stays on the 200 row Agent changes list for ten hours of the worst case instead of falling off it in minutes.
    /// </summary>
    internal const int MaxWritesPerHour = 20;

    private static readonly System.Text.RegularExpressions.Regex AgentSuffix = new(
        @"\[agent:[^\]\s]+( request [0-9a-fA-F-]{36})?\]\s*$",
        System.Text.RegularExpressions.RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    /// <summary>True when the rule's reason ends with an agent attribution suffix (either path's), so an agent made or last changed it.</summary>
    internal static bool IsAgentMade(MuteRule rule) => rule.Reason is { } r && AgentSuffix.IsMatch(r);

    internal static AgentActionRefusal MatchesEveryAlertRefusal() => new(AllAlertsCode,
        "A rule with no metric and no server would silence every alert, including the service's own health alerts and the daily report. " +
        "Name a metric (any server or one server). An operator can make a rule that matches every alert. Nothing was written.");

    /// <summary>
    /// The renewal bound (#7414): 7 days is a lifetime, counted from the rule's <c>created_at_utc</c>, not a window that slides. An agent's
    /// update may shorten the expiry, or leave it, but may not move it later than the creation time plus <see cref="MaxExpiryDays"/> days (or
    /// remove it). Shortening is judged against the stored expiry, so an operator's older or open-ended rule can be cut short but never
    /// extended by an agent. Null when the update is allowed.
    /// </summary>
    internal static AgentActionRefusal? LifetimeRefusal(MuteRule before, MuteRule after)
    {
        var created = DateTime.SpecifyKind(before.CreatedAtUtc, DateTimeKind.Utc);
        var cap = created.AddDays(MaxExpiryDays);
        var unchanged = after.ExpiresAtUtc == before.ExpiresAtUtc;
        var shortens = after.ExpiresAtUtc is { } now && (before.ExpiresAtUtc is not { } was || now <= was);
        var insideLifetime = after.ExpiresAtUtc is { } e && DateTime.SpecifyKind(e, DateTimeKind.Utc) <= cap;
        if (unchanged || shortens || insideLifetime)
            return null;
        return new AgentActionRefusal(LifetimeCode,
            $"This rule was created {created.ToString("o", CultureInfo.InvariantCulture)}. An agent's rule lives at most {MaxExpiryDays} days from then, " +
            $"so its expiry cannot be moved later than {cap.ToString("o", CultureInfo.InvariantCulture)}. An agent can shorten it. An operator can extend it. Nothing was written.");
    }

    /// <summary>True for a call this class serves: the investigations arm (not the writes arm) and no <c>agent_action</c> at all. The absent test is
    /// <see cref="InvestigationToolCore.ParseAgentAction"/>'s own, so a malformed argument still goes to the claim path and is refused there.</summary>
    internal static bool Applies(DarlingAgentActionSeat? seat, string? agentAction) =>
        seat is { Policy: { Writes: false, Investigations: true } }
        && InvestigationToolCore.ParseAgentAction(agentAction, out _)?.Reason == AgentActionCodes.Required;

    /// <summary>The agent id of a call with no <c>agent_action</c>: the named token's own name, else a fixed word for the legacy token.</summary>
    internal static string AgentIdOf(DarlingAgentActionSeat seat) => seat.Policy.AgentTokenName ?? "legacy-token";

    /// <summary>
    /// The expiry bound, judged on the instant the rule would carry after the write. UTC throughout: <paramref name="nowUtc"/> is the service
    /// clock, and an instant sent with an offset has already been converted to UTC by the parse. Null when it is inside
    /// (<paramref name="nowUtc"/>, <paramref name="nowUtc"/> + <see cref="MaxExpiryDays"/> days]. A stored expiry comes back from PostgreSQL at
    /// microsecond precision, which can only round it by half a microsecond, never past the instant it was created to be within.
    /// </summary>
    internal static AgentActionRefusal? ExpiryRefusal(DateTime? expiresAtUtc, DateTime nowUtc)
    {
        var latest = nowUtc.AddDays(MaxExpiryDays);
        var latestText = latest.ToString("o", CultureInfo.InvariantCulture);
        if (expiresAtUtc is not { } expiry)
            return new AgentActionRefusal(ExpiryCode,
                $"An agent's rule needs an expiry: expires_at must be an ISO-8601 UTC instant in the future and at most {MaxExpiryDays} days ahead (no later than {latestText}). Nothing was written.");
        return expiry <= nowUtc || expiry > latest
            ? new AgentActionRefusal(ExpiryCode,
                $"expires_at ({expiry.ToString("o", CultureInfo.InvariantCulture)}) must be in the future and at most {MaxExpiryDays} days ahead (no later than {latestText}). Nothing was written.")
            : null;
    }

    private static AgentActionRefusal SizeRefusal(string field, int limit) => new(SizeCode,
        $"{field} is longer than the {limit} characters an agent's rule may use. Nothing was written.");

    internal static AgentActionRefusal? CreateFieldsRefusal(params (string Name, string? Value)[] fields)
    {
        foreach (var (name, value) in fields)
            if (value is { } v && v.Length > MaxFieldChars)
                return SizeRefusal(name, MaxFieldChars);
        return null;
    }

    private static AgentActionRefusal? ReasonRefusal(string? reason) =>
        string.IsNullOrWhiteSpace(reason) || reason.Trim().Length > InvestigationToolCore.MaxActionReasonChars
            ? new AgentActionRefusal(ReasonCode,
                $"reason is required: 1 to {InvestigationToolCore.MaxActionReasonChars} characters saying why the rule is needed. It is kept in the agent change journal. Nothing was written.")
            : null;

    /// <summary>The reason as the rule stores it: what the agent sent, then who sent it, so a person reading the rule list sees an agent made it.</summary>
    private static string StoredReason(string reason, string agentId)
    {
        var suffix = "[agent:" + agentId + "]";
        var bare = reason.Trim();
        return bare.EndsWith(suffix, StringComparison.Ordinal) ? bare : bare + " " + suffix;
    }

    private static string? StatusOf(string answer)
    {
        try
        {
            return JsonNode.Parse(answer)?["status"]?.GetValue<string>();
        }
        catch (Exception ex) when (ex is System.Text.Json.JsonException or InvalidOperationException)
        {
            return null;
        }
    }

    private static string Refusal(AgentActionRefusal refusal) => DarlingAgentActionGate.RefusalAnswer(refusal);

    /// <summary>
    /// Opens the transaction a write and its journal row share, on the store owner's connection (the journal keeps its rows to the owner; a
    /// test host with no owner uses the tool's own source), and checks the service's agent switch in it. <paramref name="body"/> answers the
    /// tool's JSON and returns the journal row it wants (null for no row: a refusal, or a write that changed nothing); the row is written
    /// and the transaction committed here only when the answer says the rule changed. Any exception leaves the transaction uncommitted.
    /// </summary>
    private static async Task<string> InTransactionAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, string tool, string agentId,
        string reason, DateTime now, Func<NpgsqlConnection, NpgsqlTransaction, Task<(string Answer, MuteRule? Before, MuteRule? After)>> body,
        string changedStatus, CancellationToken ct)
    {
        if (!seat.AgentMuteRulesAllowed)
            return Refusal(new AgentActionRefusal(OffCode,
                $"An agent token may not write mute or digest rules without a claim on this service: {SettingName} is false in darling.json. Nothing was written."));
        try
        {
            await using var connection = await (seat.Owner?.Read() ?? postgres).OpenConnectionAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(ct);
            /* The switch read takes the service row FOR NO KEY UPDATE, so every direct write waits its turn here: the hourly count and the
               same-match check below read the journal as the write before them left it, and two calls cannot both see room. */
            if (await InvestigationRequestStore.DirectChangeSwitchRefusalAsync(connection, transaction, ct) is { } off)
                return Refusal(off);
            if (await WriteCountInLastHourAsync(connection, transaction, now, ct) >= MaxWritesPerHour)
                return Refusal(new AgentActionRefusal(RateCode,
                    $"This service allows {MaxWritesPerHour} agent mute rule writes an hour and that many were made in the last hour. Wait, or ask an operator. Nothing was written."));

            var (answer, before, after) = await body(connection, transaction);
            if (!string.Equals(StatusOf(answer), changedStatus, StringComparison.Ordinal))
                return answer;

            var rule = after ?? before!;
            await InvestigationRequestStore.JournalDirectChangeAsync(connection, transaction, now, rule.ServerId, "agent:" + agentId, reason.Trim(),
                tool, MuteRuleImage.Kind, rule.Id, DarlingMcpAgentActions.Serialize(before), answer, ct);
            await transaction.CommitAsync(ct);
            return answer;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(tool, ex);
        }
    }

    private static async Task<long> WriteCountInLastHourAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime now, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT count(*) FROM config.investigation_journal WHERE event = 'action_called' AND request_id IS NULL AND actor LIKE 'agent:%' AND event_time > $1 - interval '1 hour'",
            connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz });
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>
    /// Whether an agent made a rule with this match (server, metric, action) in the last <see cref="MaxExpiryDays"/> days, read from the
    /// agent change journal so a rule since deleted or expired still counts. Without it, delete then create again gives the same match a fresh
    /// 7 days every time. It does NOT stop a different match (another metric, a server instead of all servers, or digest instead of mute),
    /// nor an operator re-creating the rule: each of those is a different rule, still bounded, journaled and rate limited.
    /// </summary>
    private static async Task<bool> MadeRecentlyAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, MuteRule rule, DateTime now, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT EXISTS (
  SELECT 1 FROM config.investigation_journal c
  JOIN config.investigation_journal d ON d.event = 'action_done' AND d.detail->>'called_id' = c.journal_id::text
  CROSS JOIN LATERAL (SELECT COALESCE(d.detail->'after'->'mute_rule', d.detail->'after') AS r) x
  WHERE c.event = 'action_called' AND c.actor LIKE 'agent:%' AND c.detail->>'tool' = 'create_mute_rule'
    AND c.event_time > $1 - make_interval(days => $2)
    AND (x.r->>'server_id') IS NOT DISTINCT FROM $3
    AND lower(x.r->>'metric_name') IS NOT DISTINCT FROM lower($4)
    AND COALESCE(x.r->>'action', 'mute') = $5)", connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = MaxExpiryDays });
        command.Parameters.Add(new NpgsqlParameter<string?> { TypedValue = rule.ServerId?.ToString(CultureInfo.InvariantCulture), NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<string?> { TypedValue = rule.MetricName, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = MuteRuleActions.Normalize(rule.Action) });
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<(MuteRule? Rule, string? NotFound)> LockAndReadAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string ruleId, CancellationToken ct)
    {
        var store = new PgMuteRuleStore(connection, transaction);
        var notFound = DarlingMcpAlertTools.Outcome("not_found", $"No mute rule with id '{ruleId}'.");
        if (string.IsNullOrWhiteSpace(ruleId) || await store.LockServerAndMetricAsync(ruleId, ct) is null)
            return (null, notFound);
        var rule = await DarlingMcpAlertTools.FindRuleAsync(store, ruleId);
        return rule is null ? (null, notFound) : (rule, null);
    }

    internal static async Task<string> CreateAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, string? server_name, string? metric_name,
        string? database_pattern, string? query_text_pattern, string? wait_type_pattern, string? job_name_pattern, string? reason, string? expires_at,
        int? server_id, string? client_app_pattern, string? external_source, string? starts_at, bool hasRecurrence, string? action,
        CancellationToken ct = default)
    {
        const string tool = "create_mute_rule";
        if (hasRecurrence)
            return DarlingMcpAlertTools.Outcome("invalid", DarlingMcpAgentActions.RecurrenceRefusal);
        if (!string.IsNullOrWhiteSpace(starts_at))
            return DarlingMcpAlertTools.Outcome("invalid", DarlingMcpAgentActions.StartsAtRefusal);
        if (ReasonRefusal(reason) is { } noReason)
            return Refusal(noReason);
        if (CreateFieldsRefusal(("server_name", server_name), ("metric_name", metric_name), ("database_pattern", database_pattern),
                ("query_text_pattern", query_text_pattern), ("wait_type_pattern", wait_type_pattern), ("job_name_pattern", job_name_pattern),
                ("client_app_pattern", client_app_pattern), ("external_source", external_source), ("expires_at", expires_at)) is { } tooLong)
            return Refusal(tooLong);

        /* One clock, the service's UTC clock, taken once: the bound and the create's own past-expiry check read the same instant. */
        var now = DateTime.UtcNow;
        DateTime? expiry = null;
        if (!string.IsNullOrWhiteSpace(expires_at))
        {
            if (!DateTime.TryParse(expires_at, CultureInfo.InvariantCulture,
                    DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var parsed))
                return DarlingMcpAlertTools.Outcome("invalid", $"expires_at '{expires_at}' is not a valid ISO-8601 timestamp (e.g. 2027-08-01T00:00:00Z).");
            expiry = DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
        }

        if (ExpiryRefusal(expiry, now) is { } badExpiry)
            return Refusal(badExpiry);

        var agentId = AgentIdOf(seat);
        return await InTransactionAsync(seat, postgres, tool, agentId, reason!, now, async (connection, transaction) =>
        {
            var answer = await DarlingMcpAlertTools.CreateMuteRuleOver(new PgMuteRuleStore(connection, transaction), server_name, metric_name,
                database_pattern, query_text_pattern, wait_type_pattern, job_name_pattern, StoredReason(reason!, agentId), expires_at, server_id,
                id => DarlingMcpAlertTools.MonitoredServerDisplayNameAsync(postgres, id), client_app_pattern, external_source, null, now, action: action);
            MuteRule? created = null;
            if (StatusOf(answer) == "created")
            {
                var stored = JsonNode.Parse(answer)!["mute_rule"]!;
                created = await DarlingMcpAlertTools.FindRuleAsync(new PgMuteRuleStore(connection, transaction), (string)stored["id"]!);
                /* Judged on the rule as the store holds it. A refusal here is not "created", so nothing commits. */
                if (created is { MatchesEveryAlert: true })
                    return (Refusal(MatchesEveryAlertRefusal()), null, null);
                if (created is not null && await MadeRecentlyAsync(connection, transaction, created, now, ct))
                    return (Refusal(new AgentActionRefusal(RepeatCode,
                        $"An agent already made a rule with this metric, server and action in the last {MaxExpiryDays} days (it may since have been deleted or expired). " +
                        $"A rule's {MaxExpiryDays} days are not renewed by making it again. An operator can make or extend it. Nothing was written.")), null, null);
            }

            return (answer, null, created);
        }, "created", ct);
    }

    internal static async Task<string> UpdateAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, string rule_id, string changes_json,
        CancellationToken ct = default)
    {
        const string tool = "update_mute_rule";
        JsonObject? body;
        try
        {
            body = JsonNode.Parse(changes_json) as JsonObject;
        }
        catch (System.Text.Json.JsonException)
        {
            body = null;
        }

        if (body is null)
            return DarlingMcpAlertTools.Outcome("invalid", "changes_json must be a JSON object of the mute-rule fields to change.");
        if (body.ContainsKey("starts_at") || body.ContainsKey("starts_at_utc"))
            return DarlingMcpAlertTools.Outcome("invalid", DarlingMcpAgentActions.StartsAtRefusal);
        if (body.Any(p => p.Key.StartsWith("recur_", StringComparison.Ordinal)))
            return DarlingMcpAlertTools.Outcome("invalid", DarlingMcpAgentActions.RecurrenceRefusal);

        string? reason = null;
        if (body.TryGetPropertyValue("reason", out var sent) && sent is JsonValue sentValue)
            sentValue.TryGetValue(out reason);
        if (ReasonRefusal(reason) is { } noReason)
            return Refusal(noReason);
        if (changes_json.Length > MaxChangesChars)
            return Refusal(SizeRefusal("changes_json", MaxChangesChars));
        foreach (var (key, value) in body)
            if (value is JsonValue jv && jv.TryGetValue(out string? text) && text.Length > (key == "reason" ? InvestigationToolCore.MaxActionReasonChars : MaxFieldChars))
                return Refusal(SizeRefusal(key, MaxFieldChars));

        /* The write must carry the expiry itself, not lean on whatever the rule holds: an update that leaves it out or clears it is refused here,
           and the rule it would leave is judged below as well, so a value that parses to nothing (or the other spelling) cannot slip past. */
        var hasExpiry = (body["expires_at_utc"] ?? body["expires_at"]) is JsonValue;
        if (!hasExpiry)
            return Refusal(ExpiryRefusal(null, DateTime.UtcNow)!);

        var agentId = AgentIdOf(seat);
        body["reason"] = StoredReason(reason!, agentId);
        var now = DateTime.UtcNow;
        return await InTransactionAsync(seat, postgres, tool, agentId, reason!, now, async (connection, transaction) =>
        {
            var (before, notFound) = await LockAndReadAsync(connection, transaction, rule_id, ct);
            if (before is null)
                return (notFound!, null, null);
            var after = DarlingMcpAlertTools.MuteRuleAfterUpdate(before, body, out var error);
            if (after is null)
                return (DarlingMcpAlertTools.Outcome("invalid", error ?? "changes_json is invalid."), null, null);
            if (ExpiryRefusal(after.ExpiresAtUtc, now) is { } badExpiry)
                return (Refusal(badExpiry), null, null);
            if (after.MatchesEveryAlert)
                return (Refusal(MatchesEveryAlertRefusal()), null, null);
            if (LifetimeRefusal(before, after) is { } lifetime)
                return (Refusal(lifetime), null, null);

            var answer = await DarlingMcpAlertTools.UpdateMuteRuleCore(new PgMuteRuleStore(connection, transaction), rule_id, body.ToJsonString(),
                id => DarlingMcpAlertTools.MonitoredServerDisplayNameAsync(postgres, id));
            return (answer, before, after);
        }, "updated", ct);
    }

    internal static async Task<string> SetEnabledAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, string rule_id, bool enabled,
        string? reason, CancellationToken ct = default)
    {
        const string tool = "set_mute_rule_enabled";
        if (ReasonRefusal(reason) is { } noReason)
            return Refusal(noReason);

        var now = DateTime.UtcNow;
        return await InTransactionAsync(seat, postgres, tool, AgentIdOf(seat), reason!, now, async (connection, transaction) =>
        {
            var (before, notFound) = await LockAndReadAsync(connection, transaction, rule_id, ct);
            if (before is null)
                return (notFound!, null, null);
            /* Turning a rule ON is judged on the rule as stored, so an operator's rule with no expiry, one already past, or one beyond the
               bound cannot be put in force by an agent. Turning it off is always allowed: it can only make the operator hear more. */
            if (enabled && ExpiryRefusal(before.ExpiresAtUtc, now) is { } badExpiry)
                return (Refusal(badExpiry), null, null);

            var answer = await DarlingMcpAlertTools.SetMuteRuleEnabledCore(new PgMuteRuleStore(connection, transaction), rule_id, enabled);
            return (answer, before, null);
        }, "updated", ct);
    }

    internal static async Task<string> DeleteAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, string rule_id, string? reason,
        CancellationToken ct = default)
    {
        const string tool = "delete_mute_rule";
        if (ReasonRefusal(reason) is { } noReason)
            return Refusal(noReason);

        var now = DateTime.UtcNow;
        return await InTransactionAsync(seat, postgres, tool, AgentIdOf(seat), reason!, now, async (connection, transaction) =>
        {
            var (before, notFound) = await LockAndReadAsync(connection, transaction, rule_id, ct);
            if (before is null)
                return (notFound!, null, null);
            var answer = await DarlingMcpAlertTools.DeleteMuteRuleCore(new PgMuteRuleStore(connection, transaction), rule_id);
            return (answer, before, null);
        }, "deleted", ct);
    }
}
