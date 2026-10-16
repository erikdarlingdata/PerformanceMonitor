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
    internal DarlingAgentActionSeat(DarlingMcpWritePolicy.Effective policy) => Policy = policy;

    internal DarlingMcpWritePolicy.Effective Policy { get; }

    /// <summary>True when a call must go through the gate: the investigations arm, and not the writes arm.</summary>
    internal static bool IsGated(DarlingAgentActionSeat? seat) => seat is not null && !seat.Policy.Writes;
}

/// <summary>A tool's own refusal or failure inside the write hook: the gate journals <c>action_failed</c>, the tool answers <see cref="Outcome"/>.</summary>
internal sealed class AgentToolOutcomeException : Exception
{
    internal AgentToolOutcomeException(string outcome) : base("agent tool outcome") => Outcome = outcome;

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

    /// <summary>The self alerts an agent may still mute: the five availability group metrics, Agent Not Running and the three report digests.</summary>
    private static readonly HashSet<string> UnprotectedConstantNames = new(StringComparer.Ordinal)
    {
        nameof(DarlingSelfAlertEvaluator.AgFailoverMetric),
        nameof(DarlingSelfAlertEvaluator.AgReplicaDisconnectedMetric),
        nameof(DarlingSelfAlertEvaluator.AgReplicaReconnectedMetric),
        nameof(DarlingSelfAlertEvaluator.AgSyncFellBehindMetric),
        nameof(DarlingSelfAlertEvaluator.AgDatabaseSuspendedMetric),
        nameof(DarlingSelfAlertEvaluator.AgentDownMetric),
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

    internal static string WithSuffix(string? reason, AgentAction action) =>
        string.IsNullOrWhiteSpace(reason) ? ReasonSuffix(action) : reason.Trim() + " " + ReasonSuffix(action);

    private static string Json(string status, string message) =>
        JsonSerializer.Serialize(new { status, message }, McpHelpers.JsonOptions);

    /// <summary>Runs the gate and answers the tool's JSON: the refusal, or the write's own answer.</summary>
    private static async Task<string> RunAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, ILogger? logger, string tool,
        string kind, string? agentAction, AgentActionTarget target,
        Func<NpgsqlConnection, NpgsqlTransaction, AgentActionContext, CancellationToken, Task<AgentActionRefusal?>>? kindRule,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<string?>> readBefore,
        Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<string?>> write, CancellationToken ct)
    {
        var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds);
        try
        {
            var outcome = await DarlingAgentActionGate.RunAsync(seat.Policy, postgres, store,
                logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, tool, kind, agentAction, target,
                kindRule, readBefore, write, ct);
            if (outcome.Refusal is { } refusal)
                return DarlingAgentActionGate.RefusalAnswer(refusal);
            return outcome.AfterJson ?? Json("done", tool + " done.");
        }
        catch (AgentToolOutcomeException ex)
        {
            return ex.Outcome;
        }
    }

    /// <summary>A refusal the tool makes after the argument parses but before any live check, journaled once per request, tool, code and hour.</summary>
    private static async Task<string> RefuseEarlyAsync(NpgsqlDataSource postgres, AgentAction action, string tool,
        AgentActionRefusal refusal, CancellationToken ct)
    {
        var store = new InvestigationRequestStore(postgres, ServiceCommandDeadlines.SerialLoopSeconds);
        await store.RecordAgentActionRefusalAsync(action.RequestId, action.AgentId, tool, refusal.Reason, ct);
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

    internal static async Task<string> CreateMuteAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, ILogger? logger,
        string? agentAction, string? server_name, string? metric_name, string? database_pattern, string? query_text_pattern,
        string? wait_type_pattern, string? job_name_pattern, string? reason, string? expires_at, int? server_id,
        string? client_app_pattern, string? external_source, CancellationToken ct = default)
    {
        const string tool = "create_mute_rule";
        var parsed = InvestigationToolCore.ParseAgentAction(agentAction, out var action);
        if (parsed is null && (server_id is null || string.IsNullOrWhiteSpace(metric_name)))
            return await RefuseEarlyAsync(postgres, action!, tool, NeedsMetric, ct);

        var target = await ServerTargetAsync(postgres, server_id ?? -1, null);
        return await RunAsync(seat, postgres, logger, tool, "mute", agentAction, target,
            (_, _, _, _) => Task.FromResult(metric_name is null ? NeedsMetric : MetricRefusal(metric_name)),
            NoBefore,
            async (_, _, _) =>
            {
                var answer = await DarlingMcpAlertTools.CreateMuteRuleOver(new PgMuteRuleStore(postgres), server_name, metric_name,
                    database_pattern, query_text_pattern, wait_type_pattern, job_name_pattern, WithSuffix(reason, action!), expires_at,
                    server_id, id => DarlingMcpAlertTools.MonitoredServerDisplayNameAsync(postgres, id), client_app_pattern,
                    external_source);
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

    /// <summary>The write hook's recheck: the stored rule is still the one the gate read, on the request's server.</summary>
    private static async Task StillOnServerAsync(NpgsqlDataSource postgres, string ruleId, int? serverId)
    {
        var now = await DarlingMcpAlertTools.FindRuleAsync(new PgMuteRuleStore(postgres), ruleId);
        if (now is null)
            throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("not_found", $"No mute rule with id '{ruleId}'."));
        if (now.ServerId != serverId)
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
        return await RunAsync(seat, postgres, logger, tool, "mute", agentAction, target,
            (_, _, ctx, _) => Task.FromResult(after is null ? null : MuteAfterImageRefusal(after, ctx.RequestServerId)),
            (_, _, _) => Task.FromResult(Serialize(before)),
            async (_, _, _) =>
            {
                await StillOnServerAsync(postgres, rule_id, before!.ServerId);
                var answer = await DarlingMcpAlertTools.UpdateMuteRuleCore(new PgMuteRuleStore(postgres), rule_id, body!.ToJsonString(),
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
        return await RunAsync(seat, postgres, logger, tool, "mute", agentAction, target, null,
            (_, _, _) => Task.FromResult(Serialize(before)),
            async (_, _, _) =>
            {
                await StillOnServerAsync(postgres, rule_id, before!.ServerId);
                var answer = await DarlingMcpAlertTools.DeleteMuteRuleCore(new PgMuteRuleStore(postgres), rule_id);
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
        return await RunAsync(seat, postgres, logger, tool, "mute", agentAction, target,
            /* Turning a mute ON is an after image that mutes: the same rule as a create. Turning it off is always allowed. */
            (_, _, ctx, _) => Task.FromResult(enabled && before is not null ? MuteAfterImageRefusal(before, ctx.RequestServerId) : null),
            (_, _, _) => Task.FromResult(Serialize(before)),
            async (_, _, _) =>
            {
                await StillOnServerAsync(postgres, rule_id, before!.ServerId);
                var answer = await DarlingMcpAlertTools.SetMuteRuleEnabledCore(new PgMuteRuleStore(postgres), rule_id, enabled);
                RequireSuccess(answer);
                return answer;
            }, ct);
    }

    /* ------------------------------------------------------------------ custom alert rules */

    /// <summary>The request's server, read before the gate so the target is the request's own (the rule's scope is what the kind rule judges).</summary>
    private static async Task<AgentActionTarget> RequestServerTargetAsync(NpgsqlDataSource postgres, string? agentAction, string? id)
    {
        if (InvestigationToolCore.ParseAgentAction(agentAction, out var action) is null)
        {
            await using var command = postgres.CreateCommand("SELECT server_id, server_name FROM config.investigation_requests WHERE request_id = $1");
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Uuid, Value = action!.RequestId });
            await using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
                return new AgentActionTarget(reader.GetInt32(0), reader.GetString(1), id, null);
        }

        return new AgentActionTarget(-1, "no such target", id, null);
    }

    /// <summary>The rule_scope rule: the definition's scope is exactly the request's one server (by its storage name), never all servers, a tag or a list.</summary>
    internal static async Task<AgentActionRefusal?> RuleScopeRefusalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string definitionJson, int requestServerId, string which, CancellationToken ct)
    {
        var (definition, _) = CustomAlertRuleDefinition.TryParse(definitionJson);
        string? storageName;
        await using (var command = new NpgsqlCommand("SELECT server_name FROM servers WHERE server_id = $1", connection, transaction))
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = requestServerId });
            storageName = await command.ExecuteScalarAsync(ct) as string;
        }

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
        string? agentAction, string name, string definition, string? description, bool enabled, CancellationToken ct = default)
    {
        const string tool = "create_custom_alert_rule";
        InvestigationToolCore.ParseAgentAction(agentAction, out var action);
        var target = await RequestServerTargetAsync(postgres, agentAction, null);
        return await RunAsync(seat, postgres, logger, tool, "custom_rule", agentAction, target,
            (connection, transaction, ctx, token) => RuleScopeRefusalAsync(connection, transaction, definition, ctx.RequestServerId, "after", token),
            NoBefore,
            async (_, _, token) =>
            {
                var (parsed, error) = CustomAlertRuleDefinition.TryParse(definition);
                if (parsed is null || error is not null)
                    throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("invalid", error ?? "definition is invalid."));
                var result = await new CustomAlertRuleStore(postgres).CreateAsync(name, description, definition, enabled, "agent:" + action!.AgentId, token);
                return CustomRuleAnswer(result);
            }, ct);
    }

    private static string CustomRuleAnswer(CustomAlertRuleResult result) => result switch
    {
        CustomAlertRuleResult.Ok ok => DarlingWebEndpoints.BuildFullRuleNode(ok.Rule!).ToJsonString(McpHelpers.JsonOptions),
        CustomAlertRuleResult.NotFound => throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("not_found", "No such custom alert rule.")),
        CustomAlertRuleResult.Conflict conflict => throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("conflict", conflict.Message)),
        CustomAlertRuleResult.Invalid invalid => throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("invalid", invalid.Message)),
        _ => throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("error", "Could not change the rule.")),
    };

    internal static async Task<string> UpdateCustomRuleAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, ILogger? logger,
        string? agentAction, long rule_id, int version, string? name, string? definition, string? description, bool? enabled,
        CancellationToken ct = default)
    {
        const string tool = "update_custom_alert_rule";
        InvestigationToolCore.ParseAgentAction(agentAction, out var action);
        var store = new CustomAlertRuleStore(postgres);
        var current = await store.GetAsync(rule_id, ct);
        var row = (current as CustomAlertRuleResult.Ok)?.Rule;
        if (row is null && InvestigationToolCore.ParseAgentAction(agentAction, out _) is null)
            return DarlingMcpAlertTools.Outcome("not_found", $"No custom alert rule with id {rule_id}.");
        var target = await RequestServerTargetAsync(postgres, agentAction, rule_id.ToString(CultureInfo.InvariantCulture));
        return await RunAsync(seat, postgres, logger, tool, "custom_rule", agentAction, target,
            async (connection, transaction, ctx, token) =>
                await RuleScopeRefusalAsync(connection, transaction, row!.DefinitionJson, ctx.RequestServerId, "before", token)
                ?? await RuleScopeRefusalAsync(connection, transaction, definition ?? row.DefinitionJson, ctx.RequestServerId, "after", token),
            (_, _, _) => Task.FromResult<string?>(DarlingWebEndpoints.BuildFullRuleNode(row!).ToJsonString(McpHelpers.JsonOptions)),
            async (_, _, token) =>
            {
                if (definition is not null)
                {
                    var (parsed, error) = CustomAlertRuleDefinition.TryParse(definition);
                    if (parsed is null || error is not null)
                        throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("invalid", error ?? "definition is invalid."));
                }

                var result = await store.UpdateAsync(rule_id, name ?? row!.Name,
                    DarlingMcpCustomAlertTools.ResolveOptionalText(description, row!.Description), definition ?? row.DefinitionJson,
                    enabled ?? row.Enabled, version, "agent:" + action!.AgentId, token);
                return CustomRuleAnswer(result);
            }, ct);
    }

    internal static async Task<string> DeleteCustomRuleAsync(DarlingAgentActionSeat seat, NpgsqlDataSource postgres, ILogger? logger,
        string? agentAction, long rule_id, CancellationToken ct = default)
    {
        const string tool = "delete_custom_alert_rule";
        var current = await new CustomAlertRuleStore(postgres).GetAsync(rule_id, ct);
        var row = (current as CustomAlertRuleResult.Ok)?.Rule;
        if (row is null && InvestigationToolCore.ParseAgentAction(agentAction, out _) is null)
            return DarlingMcpAlertTools.Outcome("not_found", $"No custom alert rule with id {rule_id}.");
        var target = await RequestServerTargetAsync(postgres, agentAction, rule_id.ToString(CultureInfo.InvariantCulture));
        return await RunAsync(seat, postgres, logger, tool, "custom_rule", agentAction, target,
            (connection, transaction, ctx, token) => RuleScopeRefusalAsync(connection, transaction, row!.DefinitionJson, ctx.RequestServerId, "before", token),
            (_, _, _) => Task.FromResult<string?>(DarlingWebEndpoints.BuildFullRuleNode(row!).ToJsonString(McpHelpers.JsonOptions)),
            async (_, _, token) =>
            {
                var result = await CustomAlertEvaluator.ResolveAndDeleteRuleAsync(postgres, rule_id, logger, token);
                if (result is not CustomAlertRuleResult.Ok)
                    throw new AgentToolOutcomeException(DarlingMcpAlertTools.Outcome("not_found", $"No custom alert rule with id {rule_id}."));
                return JsonSerializer.Serialize(new { status = "deleted", rule_id }, McpHelpers.JsonOptions);
            }, ct);
    }
}
