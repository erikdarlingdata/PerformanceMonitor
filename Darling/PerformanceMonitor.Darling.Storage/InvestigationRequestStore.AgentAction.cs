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
using NpgsqlTypes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One Level 2 change, as a tool hands it to the gate (#5912): the tool, the kind of change (the journal's word,
/// e.g. <c>mute</c>), the validated <c>agent_action</c> and the target.</summary>
public sealed record AgentActionCall(string Tool, string Kind, AgentAction Action, AgentActionTarget Target);

/// <summary>What the gate knows about the request when it asks a tool's kind rule (#5912).</summary>
public sealed record AgentActionContext(AgentActionCall Call, int RequestServerId, string RequestServerName, int ClaimNo);

/// <summary>
/// The tool's part of a Level 2 change (#5912). <see cref="ResolveLevel"/> re-resolves a metric over the live routes
/// (<c>NotificationRouter.ResolveAgent</c>'s level, null when no route sends it to an agent). <see cref="KindRule"/> is
/// check 7, run inside the gate's transaction; null has none. <see cref="ReadBefore"/> reads the before image (JSON, or
/// null when the object does not exist yet) inside the gate's transaction. <see cref="Write"/> makes the change inside
/// the write's own transaction and answers the after image.
/// </summary>
public sealed class AgentActionHooks
{
    /// <summary>The level the live routes send <c>metric</c> on server <c>serverId</c> at; null when none does.</summary>
    public required Func<string, int, CancellationToken, Task<int?>> ResolveLevel { get; init; }

    /// <summary>Check 7, the kind rule; null when the kind has none.</summary>
    public Func<NpgsqlConnection, NpgsqlTransaction, AgentActionContext, CancellationToken, Task<AgentActionRefusal?>>? KindRule { get; init; }

    /// <summary>The before image, read inside the gate's transaction.</summary>
    public required Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<string?>> ReadBefore { get; init; }

    /// <summary>The change, made inside the write's own transaction; answers the after image.</summary>
    public required Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<string?>> Write { get; init; }
}

/// <summary>What the gate did (#5912): a refusal, or the change with the <c>action_called</c> row's id and the after image.</summary>
public sealed record AgentActionResult(AgentActionRefusal? Refusal, long? CalledId, string? AfterJson)
{
    /// <summary>True when the change was made and journaled <c>action_done</c>.</summary>
    public bool Done => Refusal is null && CalledId is not null;
}

public sealed partial class InvestigationRequestStore
{
    /// <summary>The service-wide agent switch (V191). A store with no row reads as on, its default.</summary>
    internal const string AgentActionsEnabledSql =
        "SELECT agent_actions_enabled FROM config.config_service WHERE id = 1";

    /// <summary>The request row the gate checks, held <c>FOR SHARE</c> for the gate's transaction (#5912), so a submit,
    /// release or claim (each takes the row <c>FOR UPDATE</c> or updates it) waits for the gate, and the gate for them.</summary>
    internal const string AgentActionRequestSql = @"
SELECT state, claim_id, claimed_by, lease_until, expires_at, server_id, server_name, practice_of, autonomy_level,
       claim_count, first_metric
FROM config.investigation_requests
WHERE request_id = $1
FOR SHARE";

    /// <summary>Every metric that opened or joined the request, for the re-resolve: the lowest level wins.</summary>
    internal const string AgentActionMetricsSql = @"
SELECT DISTINCT metric_name
FROM config.investigation_request_alerts
WHERE request_id = $1";

    internal const string AgentActionJournalSql = @"
INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, reason, detail)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
RETURNING journal_id";

    /// <summary>The journal rows a change takes: <c>action_called</c> and <c>action_done</c> or <c>action_failed</c>.</summary>
    internal const int AgentActionJournalRows = 2;

    /// <summary>
    /// The one gate every Level 2 change goes through (#5912). Checks, in order, in one transaction with the request row
    /// taken <c>FOR SHARE</c>: 2 <c>actions_off</c>, 3 <c>claim_not_live</c> (one message for every cause), 4 <c>practice</c>,
    /// 5 <c>not_level_2</c> (the stamped level, and every metric of the request re-resolved over the live routes; the lowest
    /// wins), 6 <c>other_server</c>, 7 the kind rule, 8 <c>journal_capped</c>. Check 1 (the argument) is the caller's, through
    /// <c>InvestigationToolCore.ParseAgentAction</c>, before the gate. The first failing check answers its refusal, changes
    /// nothing, and is journaled <c>refused</c> at most once per request, tool, code and hour. On a pass the gate reads the
    /// before image and commits <c>action_called</c> first, which drops the lock; the change then runs in its own
    /// transaction, which takes the row <c>FOR SHARE</c> again and re-checks 3, 5 and 6 before it writes, and journals
    /// <c>action_done</c> with it, or <c>action_failed</c>. No row holds the claim id: the journal names the claim by its
    /// number (<c>claim_no</c>).
    /// </summary>
    public async Task<AgentActionResult> RunAgentActionAsync(AgentActionCall call, AgentActionHooks hooks,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(call);
        ArgumentNullException.ThrowIfNull(hooks);
        var action = call.Action;
        var actor = "agent:" + action.AgentId;
        var now = Now();

        long calledId;
        await using (var connection = await _dataSource.OpenConnectionAsync(cancellationToken))
        await using (var transaction = await connection.BeginTransactionAsync(cancellationToken))
        {
            bool enabled;
            await using (var command = Command(connection, transaction, AgentActionsEnabledSql))
            {
                enabled = await command.ExecuteScalarAsync(cancellationToken) is not bool on || on;
            }

            var request = await ReadAgentActionRequestAsync(connection, transaction, action.RequestId, cancellationToken);
            var refusal = !enabled
                ? new AgentActionRefusal(AgentActionCodes.ActionsOff,
                    "Agent changes are turned off for this service (config_service.agent_actions_enabled). Report what you would change instead.")
                : await LiveChecksAsync(request, call, hooks, now, cancellationToken);

            AgentActionContext? context = null;
            if (refusal is null)
            {
                context = new AgentActionContext(call, request!.ServerId, request.ServerName, request.ClaimCount);
                if (hooks.KindRule is not null)
                    refusal = await hooks.KindRule(connection, transaction, context, cancellationToken);
            }

            if (refusal is null)
            {
                long rows;
                await using (var count = Command(connection, transaction, AgentJournalCountSql, P(action.RequestId)))
                {
                    rows = (long)(await count.ExecuteScalarAsync(cancellationToken))!;
                }

                if (rows + AgentActionJournalRows > InvestigationRules.MaxAgentJournalRows)
                {
                    /* The cap's own row, once per request, as claim and submit write it: a refused row here would grow the
                       journal the cap bounds. */
                    await JournalAsync(connection, transaction, now, action.RequestId, null, request!.ServerId, "journal_capped",
                        actor, call.Tool, null, "journal_capped:" + action.RequestId.ToString(), cancellationToken);
                    await transaction.CommitAsync(cancellationToken);
                    return new AgentActionResult(new AgentActionRefusal(AgentActionCodes.JournalCapped,
                        $"This request's agent journal is full ({InvestigationRules.MaxAgentJournalRows} rows). No more changes are allowed under it."),
                        null, null);
                }
            }

            if (refusal is not null)
            {
                await JournalRefusedAsync(connection, transaction, now, request is null ? null : action.RequestId,
                    request?.ServerId, actor, call.Tool, refusal.Reason, cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return new AgentActionResult(refusal, null, null);
            }

            var before = await hooks.ReadBefore(connection, transaction, cancellationToken);
            var detail = new JsonObject
            {
                ["tool"] = call.Tool,
                ["kind"] = call.Kind,
                ["target"] = new JsonObject
                {
                    ["id"] = call.Target.Id,
                    ["server_id"] = call.Target.ServerId,
                    ["database"] = call.Target.Database,
                },
                ["claim_no"] = context!.ClaimNo,
                ["evidence"] = JsonNode.Parse(action.EvidenceJson),
                ["before"] = before is null ? null : JsonNode.Parse(before),
            };
            calledId = await JournalReturningAsync(connection, transaction, now, action.RequestId, request!.ServerId,
                "action_called", actor, action.Reason, detail.ToJsonString(), cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }

        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            var writeNow = Now();
            var request = await ReadAgentActionRequestAsync(connection, transaction, action.RequestId, cancellationToken);
            var recheck = await LiveChecksAsync(request, call, hooks, writeNow, cancellationToken, practice: false);
            if (recheck is not null)
            {
                await JournalReturningAsync(connection, transaction, writeNow, action.RequestId, request?.ServerId,
                    "action_failed", actor, recheck.Reason,
                    new JsonObject { ["called_id"] = calledId, ["error"] = recheck.Reason }.ToJsonString(), cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return new AgentActionResult(recheck, calledId, null);
            }

            var after = await hooks.Write(connection, transaction, cancellationToken);
            await JournalReturningAsync(connection, transaction, writeNow, action.RequestId, request!.ServerId, "action_done",
                actor, null,
                new JsonObject { ["called_id"] = calledId, ["after"] = after is null ? null : JsonNode.Parse(after) }.ToJsonString(),
                cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return new AgentActionResult(null, calledId, after);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The change rolled back with its transaction; the called row stands, so journal the failure on its own. A
               failure to write this row leaves the called row with neither result, which reads as "result unknown". */
            await using var connection = await _dataSource.OpenConnectionAsync(CancellationToken.None);
            await using var transaction = await connection.BeginTransactionAsync(CancellationToken.None);
            await JournalReturningAsync(connection, transaction, Now(), action.RequestId, call.Target.ServerId, "action_failed",
                actor, "error", new JsonObject { ["called_id"] = calledId, ["error"] = ex.Message }.ToJsonString(),
                CancellationToken.None);
            await transaction.CommitAsync(CancellationToken.None);
            throw;
        }
    }

    /// <summary>
    /// Journals a refusal the gate's caller made before the gate (#5912, check 1, or the tool's own refusal): <c>refused</c>,
    /// at most once per request, tool, code and hour. An unparsed request id journals with no request, once per tool, code
    /// and hour. An actor the agent id rule refuses is journaled as <see cref="InvestigationRules.InvalidAgentId"/>.
    /// </summary>
    public async Task RecordAgentActionRefusalAsync(Guid? requestId, string? agentId, string tool, string code,
        CancellationToken cancellationToken = default)
    {
        var actor = "agent:" + (InvestigationRules.IsValidAgentId(agentId) ? agentId : InvestigationRules.InvalidAgentId);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        int? serverId = null;
        Guid? known = null;
        if (requestId is Guid id)
        {
            var request = await ReadAgentActionRequestAsync(connection, transaction, id, cancellationToken);
            if (request is not null)
            {
                known = id;
                serverId = request.ServerId;
            }
        }

        await JournalRefusedAsync(connection, transaction, Now(), known, serverId, actor, tool, code, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    /// <summary>Checks 3 to 6 (4 only in the gate's own transaction: practice is fixed at open).</summary>
    private static async Task<AgentActionRefusal?> LiveChecksAsync(AgentActionRequest? request, AgentActionCall call,
        AgentActionHooks hooks, DateTime now, CancellationToken cancellationToken, bool practice = true)
    {
        var action = call.Action;
        if (request is null
            || request.State != "claimed"
            || request.ClaimId != action.ClaimId
            || !string.Equals(request.ClaimedBy, action.AgentId, StringComparison.Ordinal)
            || request.LeaseUntil is not DateTime lease || lease <= now
            || request.ExpiresAt <= now)
        {
            return new AgentActionRefusal(AgentActionCodes.ClaimNotLive, AgentActionCodes.ClaimNotLiveMessage);
        }

        if (practice && request.PracticeOf is not null)
            return new AgentActionRefusal(AgentActionCodes.Practice,
                "This is a practice request: it changes nothing. Report what you would change instead.");

        var level = request.AutonomyLevel;
        foreach (var metric in request.Metrics)
        {
            if (level < 2)
                break;
            var resolved = await hooks.ResolveLevel(metric, request.ServerId, cancellationToken);
            level = Math.Min(level, resolved ?? 0);
        }

        if (level != 2)
            return new AgentActionRefusal(AgentActionCodes.NotLevel2,
                "This request is not Level 2: its routes let an agent investigate, not change settings. Report what you would change instead.");

        if (call.Target.ServerId != request.ServerId)
            return new AgentActionRefusal(AgentActionCodes.OtherServer,
                $"This change targets {call.Target.ServerName}; the request is for {request.ServerName}. Level 2 changes only the request's server.");

        return null;
    }

    private sealed record AgentActionRequest(string State, Guid? ClaimId, string? ClaimedBy, DateTime? LeaseUntil,
        DateTime ExpiresAt, int ServerId, string ServerName, Guid? PracticeOf, int AutonomyLevel, int ClaimCount,
        IReadOnlyList<string> Metrics);

    private async Task<AgentActionRequest?> ReadAgentActionRequestAsync(NpgsqlConnection connection,
        NpgsqlTransaction transaction, Guid requestId, CancellationToken cancellationToken)
    {
        AgentActionRequest? row = null;
        string firstMetric;
        await using (var command = Command(connection, transaction, AgentActionRequestSql, P(requestId)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
                return null;
            firstMetric = reader.GetString(10);
            row = new AgentActionRequest(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetGuid(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Utc),
                DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc),
                reader.GetInt32(5),
                reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetGuid(7),
                reader.GetInt16(8),
                reader.GetInt32(9),
                Array.Empty<string>());
        }

        var metrics = new List<string> { firstMetric };
        await using (var command = Command(connection, transaction, AgentActionMetricsSql, P(requestId)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var metric = reader.GetString(0);
                if (!metrics.Contains(metric, StringComparer.Ordinal))
                    metrics.Add(metric);
            }
        }

        return row with { Metrics = metrics };
    }

    private Task<int> JournalRefusedAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime now,
        Guid? requestId, int? serverId, string actor, string tool, string code, CancellationToken cancellationToken)
    {
        var onceKey = string.Create(CultureInfo.InvariantCulture,
            $"agent_refused:{(requestId is Guid id ? id.ToString() : "unknown")}:{tool}:{code}:{now:yyyy-MM-ddTHH}");
        return JournalAsync(connection, transaction, now, requestId, null, serverId, "refused", actor, code,
            new JsonObject { ["tool"] = tool }.ToJsonString(), onceKey, cancellationToken);
    }

    private async Task<long> JournalReturningAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime now,
        Guid requestId, int? serverId, string journalEvent, string actor, string? reason, string detailJson,
        CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, AgentActionJournalSql,
            P(now), P(requestId), new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer },
            P(journalEvent), P(actor), PNullable(reason), PNullable(detailJson));
        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }
}
