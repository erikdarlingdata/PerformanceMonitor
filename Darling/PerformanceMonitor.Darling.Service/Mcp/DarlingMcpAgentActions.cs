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
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The policy the host started with, as a service the Level 2 tools take (#5915). A tool that is also a Level 2 action
/// tool reads <see cref="Policy"/>: under the writes arm it writes as it always has; under the investigations arm it goes
/// through <see cref="DarlingAgentActionGate"/>. Registered by the host (a DI seat, so a client can never supply it).
/// </summary>
public sealed class DarlingAgentActionSeat
{
    internal DarlingAgentActionSeat(DarlingMcpWritePolicy.Effective policy, StoreOwnerSource? owner = null,
        IReadOnlySet<string>? namedAgentNames = null)
    {
        Policy = policy;
        Owner = owner;
        NamedAgentNames = namedAgentNames ?? new HashSet<string>();
    }

    /// <summary>The names of the named agent tokens this host serves (#6542), empty when it serves none: the legacy token's agent_action
    /// may not act as one of them.</summary>
    internal IReadOnlySet<string> NamedAgentNames { get; }

    internal DarlingMcpWritePolicy.Effective Policy { get; }

    /// <summary>The service's own (store owner) data source, which the gate runs a change on (#6569): the journal keeps the agent action rows to the owner,
    /// and the tools' own data source is the <c>mcp</c> login. Null (a test host) runs the change on the tool's data source.</summary>
    internal StoreOwnerSource? Owner { get; }

    /// <summary>True when a call must go through the gate: the investigations arm, and not the writes arm.</summary>
    internal static bool IsGated(DarlingAgentActionSeat? seat) => seat is not null && !seat.Policy.Writes;
}

/// <summary>
/// A tool's own refusal or failure inside the write hook: the change rolls back with the gate's transaction, the gate journals
/// <c>action_failed</c> with <see cref="Exception.Message"/> (the same answer text), and the tool answers <see cref="Outcome"/>.
/// </summary>
internal sealed class AgentToolOutcomeException : Exception
{
    internal AgentToolOutcomeException(string outcome) : base(outcome) => Outcome = outcome;

    internal string Outcome { get; }
}

/// <summary>
/// The Level 2 mute and custom alert rule tools' agent arm (#5915): each tool's kind rule, before image and write, handed to
/// the one gate. The write is the SAME code the writes arm runs (<see cref="DarlingMcpAlertTools"/>,
/// <see cref="DarlingMcpCustomAlertTools"/>), so the two arms cannot disagree; only the checks around it differ.
/// </summary>
internal static class DarlingMcpAgentActions
{
    /// <summary>Kind rule codes (#5915). The gate's own codes are <see cref="AgentActionCodes"/>.</summary>
    internal const string MuteNeedsMetric = "mute_needs_metric";
    internal const string SelfMonitorMetric = "self_monitor_metric";
    internal const string RuleScope = "rule_scope";

    /// <summary>The self alerts an agent may still mute: the five availability group metrics, Agent Not Running, Agent Alert Gaps, the three database-protection overdue alerts (#6873), Identity Capacity (#6875) and the three report digests.</summary>
    private static readonly HashSet<string> UnprotectedConstantNames = new(StringComparer.Ordinal)
    {
        nameof(DarlingSelfAlertEvaluator.AgFailoverMetric),
        nameof(DarlingSelfAlertEvaluator.AgReplicaDisconnectedMetric),
        nameof(DarlingSelfAlertEvaluator.AgReplicaReconnectedMetric),
        nameof(DarlingSelfAlertEvaluator.AgSyncFellBehindMetric),
        nameof(DarlingSelfAlertEvaluator.AgDatabaseSuspendedMetric),
        nameof(DarlingSelfAlertEvaluator.AgentDownMetric),
        nameof(DarlingSelfAlertEvaluator.BackupOverdueMetric),
        nameof(DarlingSelfAlertEvaluator.LogBackupOverdueMetric),
        nameof(DarlingSelfAlertEvaluator.CheckDbOverdueMetric),
        nameof(DarlingSelfAlertEvaluator.AgentAlertSetupMetric),
        nameof(DarlingSelfAlertEvaluator.IdentityCapacityMetric),
        nameof(DarlingSelfAlertEvaluator.CollectorCostDigestMetric),
        nameof(DarlingSelfAlertEvaluator.FleetSweepRollupMetric),
        nameof(DarlingSelfAlertEvaluator.AnalysisSinglesDigestMetric),
    };

    /// <summary>Every <c>*Metric</c> constant of <see cref="DarlingSelfAlertEvaluator"/> an agent may not mute (read by reflection, so a new one is protected without an edit here).</summary>
    private static readonly Lazy<HashSet<string>> ProtectedConstants = new(() => new HashSet<string>(
        typeof(DarlingSelfAlertEvaluator)
            .GetFields(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name.EndsWith("Metric", StringComparison.Ordinal)
                && !UnprotectedConstantNames.Contains(f.Name))
            .Select(f => (string)f.GetRawConstantValue()!),
        StringComparer.OrdinalIgnoreCase));

    /// <summary>True when an agent may not mute <paramref name="metric"/>: its family is self-monitor, or it is a protected self alert constant.</summary>
    internal static bool IsProtectedMetric(string metric) =>
        string.Equals(AlertFamily.Of(metric), AlertFamily.SelfMonitor, StringComparison.Ordinal)
        || ProtectedConstants.Value.Contains(metric.Trim());

    private static AgentActionRefusal? MetricRefusal(string metric) => IsProtectedMetric(metric)
        ? new AgentActionRefusal(SelfMonitorMetric,
            $"'{metric}' is one of the service's own alerts. An agent does not mute those; report it instead.")
        : null;

    private static readonly AgentActionRefusal NeedsMetric = new(MuteNeedsMetric,
        "An agent mute names server_id and metric_name; agents never mute a whole server.");

    /// <summary>The kind rule for a mute after image (create, update, enable): the request's server and a metric, not a protected one.</summary>
    internal static AgentActionRefusal? MuteAfterImageRefusal(MuteRule after, int requestServerId)
    {
        if (after.ServerId is not int serverId || string.IsNullOrWhiteSpace(after.MetricName))
            return NeedsMetric;
        if (serverId != requestServerId)
            return new AgentActionRefusal(AgentActionCodes.OtherServer,
                "A mute may name only the request's server. Level 2 changes only the request's server.");
        return MetricRefusal(after.MetricName!);
    }

    /// <summary>The suffix every agent-written mute reason carries.</summary>
    internal static string ReasonSuffix(AgentAction action) =>
        string.Create(CultureInfo.InvariantCulture, $"[agent:{action.AgentId} request {action.RequestId}]");

    /// <summary>Any trailing <c>[agent:&lt;id&gt; request &lt;uuid&gt;]</c> suffixes (this agent's or another's), so a reason read back from the rule is not suffixed twice.</summary>
    private static readonly System.Text.RegularExpressions.Regex TrailingSuffix = new(
        @"(\s*\[agent:[^\]\s]+ request [0-9a-fA-F-]{36}\])+\s*$",
        System.Text.RegularExpressions.RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    /// <summary>
    /// The reason with exactly one attribution suffix, this action's: a trailing suffix already on it (the reason stored by an
    /// earlier agent change, sent back whole or left as the fallback) is dropped first, so repeated updates do not stack them.
    /// </summary>
    internal static string WithSuffix(string? reason, AgentAction action)
    {
        var bare = string.IsNullOrWhiteSpace(reason) ? "" : TrailingSuffix.Replace(reason.Trim(), "").Trim();
        return bare.Length == 0 ? ReasonSuffix(action) : bare + " " + ReasonSuffix(action);
    }

    private static string Json(string status, string message) =>
        JsonSerializer.Serialize(new { status, message }, McpHelpers.JsonOptions);

    /// <summary>Runs the gate and answers the tool's JSON: the refusal, or the write's own answer.</summary>
    private static async Task<string> RunAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, ILogger? logger, string tool,
        string kind, string? agentAction, AgentActionTarget target,
        Func<NpgsqlConnection, NpgsqlTransaction, AgentActionContext, CancellationToken, Task<AgentActionRefusal?>>? kindRule,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<string?>> readBefore,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<string?>> write, CancellationToken ct)
    {
        var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds, agentActionSource: seat.Owner?.Read());
        try
        {
            var outcome = await DarlingAgentActionGate.RunAsync(seat.Policy, postgres, store,
                logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, tool, kind, agentAction, target,
                kindRule, readBefore, write, ct, seat.NamedAgentNames);
            if (outcome.Refusal is { } refusal)
                return DarlingAgentActionGate.RefusalAnswer(refusal);
            return outcome.AfterJson ?? Json("done", tool + " done.");
        }
        catch (AgentToolOutcomeException ex)
        {
            return ex.Outcome;
        }
    }

    /// <summary>A refusal the tool makes after the argument parses but before any live check, journaled once per request, verified token (else one shared key), tool, code and hour.</summary>
    private static async Task<string> RefuseEarlyAsync(NpgsqlDataSource postgres, AgentAction action, string tool,
        AgentActionRefusal refusal, string? agentTokenName, IReadOnlySet<string>? namedAgentNames, CancellationToken ct)
    {
        var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds);
        /* #6542: a named agent token journals under its own name, never the agent_id it passed. */
        await store.RecordAgentActionRefusalAsync(action.RequestId, DarlingAgentActionGate.JournalActor(agentTokenName, namedAgentNames, action.AgentId), tool, refusal.Reason, agentTokenName, ct);
        return DarlingAgentActionGate.RefusalAnswer(refusal);
    }

    private static Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<string?>> NoBefore =>
        (_, _, _) => Task.FromResult<string?>(null);

    private static void RequireSuccess(string answer)
    {
        var status = JsonNode.Parse(answer)?["status"]?.GetValue<string>();
        if (status is not ("created" or "already_exists" or "updated" or "deleted" or "unchanged"))
            throw new AgentToolOutcomeException(answer);
    }

    private static string? Serialize(MuteRule? rule) =>
        rule is null ? null : JsonSerializer.Serialize(DarlingMcpAlertTools.BuildMuteRulePayload(rule), McpHelpers.JsonOptions);

    private static async Task<AgentActionTarget> ServerTargetAsync(NpgsqlDataSource postgres, int serverId, string? id)
    {
        var name = await DarlingMcpAlertTools.MonitoredServerDisplayNameAsync(postgres, serverId);
        return new AgentActionTarget(serverId, name ?? $"server_id {serverId}", id, null);
    }

    /* ------------------------------------------------------------------ mute rules */

    /// <summary>#7040: an agent's mute stays a one-off. A repeating schedule is a standing change to what an operator hears about, so it is left to a person.</summary>
    internal const string RecurrenceRefusal =
        "A repeat schedule (recur_days, recur_start, recur_end, recur_time_zone) is not available during an investigation: a mute made here is a one-off, and a rule that repeats is left to an operator. Nothing was written.";

    /// <summary>What an investigating agent is told when it sends a mute start (#6231): its mute applies at once.</summary>
    internal const string StartsAtRefusal =
        "starts_at is not available during an investigation: a mute made here applies at once, so a window set ahead of planned work is left to an operator. Nothing was written.";

    internal static async Task<string> CreateMuteAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, ILogger? logger,
        string? agentAction, string? server_name, string? metric_name, string? database_pattern, string? query_text_pattern,
        string? wait_type_pattern, string? job_name_pattern, string? reason, string? expires_at, int? server_id,
        string? client_app_pattern, string? external_source, string? starts_at = null, CancellationToken ct = default)
    {
        const string tool = "create_mute_rule";
        /* A start in the future would let a mute begin after the investigation's claim has ended, so an agent's mute applies at once. */
        if (!string.IsNullOrWhiteSpace(starts_at))
            return DarlingMcpAlertTools.Outcome("invalid", StartsAtRefusal);
        var parsed = InvestigationToolCore.ParseAgentAction(agentAction, out var action);
        if (parsed is null && (server_id is null || string.IsNullOrWhiteSpace(metric_name)))
            return await RefuseEarlyAsync(postgres, action!, tool, NeedsMetric, seat.Policy.AgentTokenName, seat.NamedAgentNames, ct);

        var target = await ServerTargetAsync(postgres, server_id ?? -1, null);
        return await RunAsync(seat, postgres, logger, tool, MuteRuleImage.Kind, agentAction, target,
            (_, _, _, _) => Task.FromResult(metric_name is null ? NeedsMetric : MetricRefusal(metric_name)),
            NoBefore,
            async (connection, transaction, _) =>
            {
                var answer = await DarlingMcpAlertTools.CreateMuteRuleOver(new PgMuteRuleStore(connection, transaction), server_name, metric_name,
                    database_pattern, query_text_pattern, wait_type_pattern, job_name_pattern, WithSuffix(reason, action!), expires_at,
                    server_id, id => DarlingMcpAlertTools.MonitoredServerDisplayNameAsync(postgres, id), client_app_pattern,
                    external_source, starts_at);
                RequireSuccess(answer);
                return answer;
            }, ct);
    }

    private static async Task<(MuteRule? Rule, string? NotFound)> FindMuteAsync(NpgsqlDataSource postgres, string rule_id)
    {
        var rule = string.IsNullOrWhiteSpace(rule_id) ? null : await DarlingMcpAlertTools.FindRuleAsync(new PgMuteRuleStore(postgres), rule_id);
        return rule is null ? (null, DarlingMcpAlertTools.Outcome("not_found", $"No mute rule with id '{rule_id}'.")) : (rule, null);
    }

    /// <summary>The target of a change to a stored mute: the before image's server. A rule with no server_id has none, so it is another server's.</summary>
    private static Task<AgentActionTarget> MuteTargetAsync(NpgsqlDataSource postgres, MuteRule before) =>
        before.ServerId is int id
            ? ServerTargetAsync(postgres, id, before.Id)
            : Task.FromResult(new AgentActionTarget(-1, "every server (the rule names no server_id)", before.Id, null));

    /// <summary>
    /// The write hook's recheck, in the gate's transaction: the stored rule is locked <c>FOR UPDATE</c> and is still the one the
    /// gate read, on the same server and the same metric (a rule re-pointed at a protected metric after the kind rule ran is
    /// refused here), so the recheck and the write are one transaction.
    /// </summary>
    internal static async Task StillTheSameMuteAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string ruleId,
        MuteRule before, CancellationToken ct)
    {
        var locked = await new PgMuteRuleStore(connection, transaction).LockServerAndMetricAsync(ruleId, ct);
        if (locked is null)
            throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("not_found", $"No mute rule with id '{ruleId}'."));
        var (serverId, metric) = locked.Value;

        if (serverId != before.ServerId || !string.Equals(metric, before.MetricName, StringComparison.Ordinal))
            throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("conflict",
                "The mute rule changed since it was read; read it again."));
    }

    internal static async Task<string> UpdateMuteAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, ILogger? logger,
        string? agentAction, string rule_id, string changes_json, CancellationToken ct = default)
    {
        const string tool = "update_mute_rule";
        JsonObject? body;
        try
        {
            body = JsonNode.Parse(changes_json) as JsonObject;
        }
        catch (JsonException)
        {
            body = null;
        }

        if (body is not null && (body.ContainsKey("starts_at") || body.ContainsKey("starts_at_utc")))
            return DarlingMcpAlertTools.Outcome("invalid", StartsAtRefusal);
        if (body is not null && body.Any(p => p.Key.StartsWith("recur_", StringComparison.Ordinal)))
            return DarlingMcpAlertTools.Outcome("invalid", RecurrenceRefusal);

        var (before, notFound) = await FindMuteAsync(postgres, rule_id);
        var parsed = InvestigationToolCore.ParseAgentAction(agentAction, out var action);
        MuteRule? after = null;
        if (parsed is null)
        {
            if (before is null)
                return notFound!;
            if (body is null)
                return DarlingMcpAlertTools.Outcome("invalid", "changes_json must be a JSON object of the mute-rule fields to change.");
            /* The reason carries the attribution suffix: what the agent sent, else the stored reason, then the suffix. */
            string? current = before.Reason;
            if (body.TryGetPropertyValue("reason", out var sent) && sent is JsonValue v && v.TryGetValue(out string? text))
                current = text;
            body["reason"] = WithSuffix(current, action!);
            after = DarlingMcpAlertTools.MuteRuleAfterUpdate(before, body, out var error);
            if (after is null)
                return DarlingMcpAlertTools.Outcome("invalid", error ?? "changes_json is invalid.");
        }

        var target = before is null ? new AgentActionTarget(-1, "no such target", rule_id, null) : await MuteTargetAsync(postgres, before);
        return await RunAsync(seat, postgres, logger, tool, MuteRuleImage.Kind, agentAction, target,
            (_, _, ctx, _) => Task.FromResult(after is null ? null : MuteAfterImageRefusal(after, ctx.RequestServerId)),
            (_, _, _) => Task.FromResult(Serialize(before)),
            async (connection, transaction, token) =>
            {
                await StillTheSameMuteAsync(connection, transaction, rule_id, before!, token);
                var answer = await DarlingMcpAlertTools.UpdateMuteRuleCore(new PgMuteRuleStore(connection, transaction), rule_id, body!.ToJsonString(),
                    id => DarlingMcpAlertTools.MonitoredServerDisplayNameAsync(postgres, id));
                RequireSuccess(answer);
                return answer;
            }, ct);
    }

    internal static async Task<string> DeleteMuteAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, ILogger? logger,
        string? agentAction, string rule_id, CancellationToken ct = default)
    {
        const string tool = "delete_mute_rule";
        var (before, notFound) = await FindMuteAsync(postgres, rule_id);
        if (before is null && InvestigationToolCore.ParseAgentAction(agentAction, out _) is null)
            return notFound!;
        var target = before is null ? new AgentActionTarget(-1, "no such target", rule_id, null) : await MuteTargetAsync(postgres, before);
        return await RunAsync(seat, postgres, logger, tool, MuteRuleImage.Kind, agentAction, target, null,
            (_, _, _) => Task.FromResult(Serialize(before)),
            async (connection, transaction, token) =>
            {
                await StillTheSameMuteAsync(connection, transaction, rule_id, before!, token);
                var answer = await DarlingMcpAlertTools.DeleteMuteRuleCore(new PgMuteRuleStore(connection, transaction), rule_id);
                RequireSuccess(answer);
                return answer;
            }, ct);
    }

    internal static async Task<string> SetMuteEnabledAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, ILogger? logger,
        string? agentAction, string rule_id, bool enabled, CancellationToken ct = default)
    {
        const string tool = "set_mute_rule_enabled";
        var (before, notFound) = await FindMuteAsync(postgres, rule_id);
        if (before is null && InvestigationToolCore.ParseAgentAction(agentAction, out _) is null)
            return notFound!;
        var target = before is null ? new AgentActionTarget(-1, "no such target", rule_id, null) : await MuteTargetAsync(postgres, before);
        return await RunAsync(seat, postgres, logger, tool, MuteRuleImage.Kind, agentAction, target,
            /* Turning a mute ON is an after image that mutes: the same rule as a create. Turning it off is always allowed. */
            (_, _, ctx, _) => Task.FromResult(enabled && before is not null ? MuteAfterImageRefusal(before, ctx.RequestServerId) : null),
            (_, _, _) => Task.FromResult(Serialize(before)),
            async (connection, transaction, token) =>
            {
                await StillTheSameMuteAsync(connection, transaction, rule_id, before!, token);
                var answer = await DarlingMcpAlertTools.SetMuteRuleEnabledCore(new PgMuteRuleStore(connection, transaction), rule_id, enabled);
                RequireSuccess(answer);
                return answer;
            }, ct);
    }

    /* ------------------------------------------------------------------ custom alert rules */

    /// <summary>The request's server, read before the gate so the target is the request's own (the rule's scope is what the kind rule judges).</summary>
    private static async Task<AgentActionTarget> RequestServerTargetAsync(NpgsqlDataSource postgres, string? agentAction, string? id,
        CancellationToken ct)
    {
        if (InvestigationToolCore.ParseAgentAction(agentAction, out var action) is null)
        {
            await using var command = postgres.CreateCommand("SELECT server_id, server_name FROM config.investigation_requests WHERE request_id = $1");
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Uuid, Value = action!.RequestId });
            await using var reader = await command.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
                return new AgentActionTarget(reader.GetInt32(0), reader.GetString(1), id, null);
        }

        return new AgentActionTarget(-1, "no such target", id, null);
    }

    /// <summary>The rule_scope rule: the definition's scope is exactly the request's one server (by its storage name), never all servers, a tag or a list.</summary>
    internal static async Task<AgentActionRefusal?> RuleScopeRefusalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string definitionJson, int requestServerId, string which, CancellationToken ct)
    {
        var (definition, parseError) = CustomAlertRuleDefinition.TryParse(definitionJson);
        /* A definition that does not parse has no scope to judge: refuse it as invalid, naming the field, so the agent fixes the
           JSON rather than rewriting a scope that was never the problem. (A stored rule that does not parse is rule_scope.) */
        if (definition is null && which == "after")
            return new AgentActionRefusal(AgentActionCodes.Invalid,
                $"definition is not valid: {(parseError ?? "it does not parse").TrimEnd('.', ' ')}. Fix the definition field; it was not judged for scope.");
        var storageName = await new CustomAlertRuleStore(connection, transaction, McpCommandDeadlines.ReadSeconds).StorageServerNameAsync(requestServerId, ct);

        var inScope = definition is not null && storageName is not null
            && definition.ScopeMode == CustomAlertScopeMode.Servers
            && definition.ScopeServers.Count == 1
            && string.Equals(definition.ScopeServers[0], storageName, StringComparison.OrdinalIgnoreCase);
        return inScope
            ? null
            : new AgentActionRefusal(RuleScope,
                $"The rule's scope ({which}) must be exactly the request's server: scope.mode 'servers' with that one server name, not all servers, a tag or a list.");
    }

    internal static async Task<string> CreateCustomRuleAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, ILogger? logger,
        string? agentAction, string name, string definition, string? description, bool enabled, string? expiresAtUtc = null,
        CancellationToken ct = default)
    {
        const string tool = "create_custom_alert_rule";
        InvestigationToolCore.ParseAgentAction(agentAction, out var action);
        var target = await RequestServerTargetAsync(postgres, agentAction, null, ct);
        return await RunAsync(seat, postgres, logger, tool, CustomAlertRuleImage.Kind, agentAction, target,
            (connection, transaction, ctx, token) => RuleScopeRefusalAsync(connection, transaction, definition, ctx.RequestServerId, "after", token),
            NoBefore,
            async (connection, transaction, token) =>
            {
                var (parsed, error) = CustomAlertRuleDefinition.TryParse(definition);
                if (parsed is null || error is not null)
                    throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("invalid", error ?? "definition is invalid."));
                var (expiry, expiryError) = DarlingMcpCustomAlertTools.ResolveCreateExpiry(expiresAtUtc);
                if (expiryError is not null)
                    throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("invalid", expiryError));
                var result = await new CustomAlertRuleStore(connection, transaction, McpCommandDeadlines.ReadSeconds).CreateAsync(name, description, definition, enabled, "agent:" + action!.AgentId, expiry, token);
                return CustomRuleAnswer(result);
            }, ct);
    }

    /// <summary>
    /// The custom-rule write hooks' recheck, in the gate's transaction: the stored rule is locked <c>FOR UPDATE</c> and its
    /// definition as stored now is still scoped to exactly the request's server (a rule re-scoped after the kind rule read it
    /// is refused here, not changed).
    /// </summary>
    internal static async Task StillInScopeAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, long ruleId,
        int requestServerId, CancellationToken ct)
    {
        var stored = await new CustomAlertRuleStore(connection, transaction, McpCommandDeadlines.ReadSeconds).LockDefinitionAsync(ruleId, ct);

        if (stored is null)
            throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("not_found", $"No custom alert rule with id {ruleId}."));
        if (await RuleScopeRefusalAsync(connection, transaction, stored, requestServerId, "before", ct) is { } refusal)
            throw new AgentToolOutcomeException(DarlingAgentActionGate.RefusalAnswer(refusal));
    }

    private static string CustomRuleAnswer(CustomAlertRuleResult result) => result switch
    {
        CustomAlertRuleResult.Ok ok => DarlingMcpCustomAlertTools.RuleAnswer(ok.Rule!),
        CustomAlertRuleResult.NotFound => throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("not_found", "No such custom alert rule.")),
        CustomAlertRuleResult.Conflict conflict => throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("conflict", conflict.Message)),
        CustomAlertRuleResult.Invalid invalid => throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("invalid", invalid.Message)),
        _ => throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("error", "Could not change the rule.")),
    };

    internal static async Task<string> UpdateCustomRuleAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, ILogger? logger,
        string? agentAction, long rule_id, int version, string? name, string? definition, string? description, bool? enabled,
        string? expiresAtUtc = null, CancellationToken ct = default)
    {
        const string tool = "update_custom_alert_rule";
        InvestigationToolCore.ParseAgentAction(agentAction, out var action);
        var store = new CustomAlertRuleStore(postgres, McpCommandDeadlines.ReadSeconds);
        var current = await store.GetAsync(rule_id, ct);
        var row = (current as CustomAlertRuleResult.Ok)?.Rule;
        if (row is null && InvestigationToolCore.ParseAgentAction(agentAction, out _) is null)
            return DarlingMcpAlertTools.Outcome("not_found", $"No custom alert rule with id {rule_id}.");
        var target = await RequestServerTargetAsync(postgres, agentAction, rule_id.ToString(CultureInfo.InvariantCulture), ct);
        return await RunAsync(seat, postgres, logger, tool, CustomAlertRuleImage.Kind, agentAction, target,
            async (connection, transaction, ctx, token) =>
                await RuleScopeRefusalAsync(connection, transaction, row!.DefinitionJson, ctx.RequestServerId, "before", token)
                ?? await RuleScopeRefusalAsync(connection, transaction, definition ?? row.DefinitionJson, ctx.RequestServerId, "after", token),
            (_, _, _) => Task.FromResult<string?>(DarlingWebEndpoints.BuildFullRuleNode(row!).ToJsonString(McpHelpers.JsonOptions)),
            async (connection, transaction, token) =>
            {
                await StillInScopeAsync(connection, transaction, rule_id, target.ServerId, token);
                if (definition is not null)
                {
                    var (parsed, error) = CustomAlertRuleDefinition.TryParse(definition);
                    if (parsed is null || error is not null)
                        throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("invalid", error ?? "definition is invalid."));
                }

                var (expiry, expiryError) = DarlingMcpCustomAlertTools.ResolveUpdateExpiry(expiresAtUtc, row!.ExpiresAtUtc);
                if (expiryError is not null)
                    throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("invalid", expiryError));
                var result = await new CustomAlertRuleStore(connection, transaction, McpCommandDeadlines.ReadSeconds).UpdateAsync(rule_id, name ?? row.Name,
                    DarlingMcpCustomAlertTools.ResolveOptionalText(description, row.Description), definition ?? row.DefinitionJson,
                    enabled ?? row.Enabled, version, "agent:" + action!.AgentId, expiry, token);
                return CustomRuleAnswer(result);
            }, ct);
    }

    internal static async Task<string> DeleteCustomRuleAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, ILogger? logger,
        string? agentAction, long rule_id, CancellationToken ct = default)
    {
        const string tool = "delete_custom_alert_rule";
        var current = await new CustomAlertRuleStore(postgres, McpCommandDeadlines.ReadSeconds).GetAsync(rule_id, ct);
        var row = (current as CustomAlertRuleResult.Ok)?.Rule;
        if (row is null && InvestigationToolCore.ParseAgentAction(agentAction, out _) is null)
            return DarlingMcpAlertTools.Outcome("not_found", $"No custom alert rule with id {rule_id}.");
        var target = await RequestServerTargetAsync(postgres, agentAction, rule_id.ToString(CultureInfo.InvariantCulture), ct);
        return await RunAsync(seat, postgres, logger, tool, CustomAlertRuleImage.Kind, agentAction, target,
            (connection, transaction, ctx, token) => RuleScopeRefusalAsync(connection, transaction, row!.DefinitionJson, ctx.RequestServerId, "before", token),
            (_, _, _) => Task.FromResult<string?>(DarlingWebEndpoints.BuildFullRuleNode(row!).ToJsonString(McpHelpers.JsonOptions)),
            async (connection, transaction, token) =>
            {
                await StillInScopeAsync(connection, transaction, rule_id, target.ServerId, token);
                var result = await CustomAlertEvaluator.ResolveAndDeleteRuleAsync(postgres, new CustomAlertRuleStore(connection, transaction, McpCommandDeadlines.ReadSeconds),
                    rule_id, logger, token);
                if (result is not CustomAlertRuleResult.Ok)
                    throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("not_found", $"No custom alert rule with id {rule_id}."));
                return JsonSerializer.Serialize(new { status = "deleted", rule_id }, McpHelpers.JsonOptions);
            }, ct);
    }
}
