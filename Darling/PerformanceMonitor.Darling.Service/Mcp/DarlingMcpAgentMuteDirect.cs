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
        try
        {
            await using var connection = await (seat.Owner?.Read() ?? postgres).OpenConnectionAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(ct);
            if (await InvestigationRequestStore.DirectChangeSwitchRefusalAsync(connection, transaction, ct) is { } off)
                return Refusal(off);

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
