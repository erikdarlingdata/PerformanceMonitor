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
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One Level 2 change, as a tool hands it to the gate (#5912): the tool, the kind of change (the journal's word,
/// e.g. <c>mute</c>), the validated <c>agent_action</c> and the target.</summary>
public sealed record AgentActionCall(string Tool, string Kind, AgentAction Action, AgentActionTarget Target, string? TokenName = null);

/// <summary>The journal events one agent action writes (#6569): the call, then its result. Only the store owner journals them, as
/// <c>server_action_switched</c> is (#6269): <see cref="InvestigationRequestStore.RunAgentActionAsync"/>, a person's undo through the
/// owner, and nothing else. The journal's trigger refuses them from the <c>mcp</c> and <c>admin</c> logins (<c>refused</c> and
/// <c>journal_capped</c> stay writable by both, because the claim and submit tools journal them too).</summary>
public static class AgentActionEvents
{
    public const string Called = "action_called";
    public const string Done = "action_done";
    public const string Failed = "action_failed";

    /// <summary>The events above, for the journal trigger's owner-only list.</summary>
    public static IReadOnlyList<string> OwnerOnly { get; } = [Called, Done, Failed];
}

/// <summary>What the gate knows about the request when it asks a tool's kind rule (#5912).</summary>
public sealed record AgentActionContext(AgentActionCall Call, int RequestServerId, string RequestServerName, int ClaimNo);

/// <summary>
/// The tool's part of a Level 2 change (#5912). <see cref="LoadLevels"/> reads the live routes ONCE, on the gate's own
/// connection and transaction, and answers a resolver over that one list (<c>NotificationRouter.ResolveAgent</c>'s level
/// for a metric, null when no route sends it to an agent). <see cref="KindRule"/> is
/// check 7, run inside the gate's transaction; null has none. <see cref="ReadBefore"/> reads the before image (JSON, or
/// null when the object does not exist yet) inside the gate's transaction. <see cref="Write"/> makes the change inside
/// the write's own transaction and answers the after image.
/// </summary>
public sealed class AgentActionHooks
{
    /// <summary>
    /// Set by a tool whose <see cref="Write"/> changes something the store's transaction cannot undo (a monitored SQL Server:
    /// #5632, step 13). Null for a store-only write (mute, signature merge, threshold override), whose rollback really does undo it.
    /// </summary>
    public ServerChangeSignal? ServerChange { get; init; }

    /// <summary>
    /// Reads the live routes once, inside the gate's own transaction (check 5), and answers the resolver every metric of the
    /// request is read against: the level the routes send <c>metric</c> on server <c>serverId</c> at, null when none does. Called
    /// at most once per gate pass, so a request with several metrics does not read the route table (and its scope inputs) again
    /// for each one.
    /// </summary>
    public required Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<Func<string, int, AlertSeverityLevel?, int?>>> LoadLevels { get; init; }

    /// <summary>Check 7, the kind rule; null when the kind has none.</summary>
    public Func<NpgsqlConnection, NpgsqlTransaction, AgentActionContext, CancellationToken, Task<AgentActionRefusal?>>? KindRule { get; init; }

    /// <summary>The before image, read inside the gate's transaction.</summary>
    public required Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<string?>> ReadBefore { get; init; }

    /// <summary>The change, made inside the write's own transaction; answers the after image.</summary>
    public required Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<string?>> Write { get; init; }
}

/// <summary>
/// The moment a tool's <see cref="AgentActionHooks.Write"/> knows the monitored server accepted the change (#5632, step 13). The tool
/// calls <see cref="MarkServerChanged"/> right after its executor says it wrote, before any later read. From then on the change happened
/// whatever the store does, so the gate never journals <c>action_failed</c> for the call. One signal per call.
/// </summary>
public sealed class ServerChangeSignal
{
    private volatile bool _changed;

    /// <summary>True once <see cref="MarkServerChanged"/> was called.</summary>
    public bool Changed => _changed;

    /// <summary>The server accepted the change.</summary>
    public void MarkServerChanged() => _changed = true;

    private volatile bool _unknown;

    /// <summary>True once <see cref="MarkOutcomeUnknown"/> was called.</summary>
    public bool OutcomeUnknown => _unknown;

    /// <summary>
    /// The write was sent to the server and its reply was lost, so nobody knows whether the server did it (#5632). The gate then writes no
    /// result row: the call keeps its <c>action_called</c> alone, which the agent changes list reads as "result unknown". Never
    /// <c>action_failed</c>, which would say nothing changed, and never <c>action_done</c>, which would say it did.
    /// </summary>
    public void MarkOutcomeUnknown() => _unknown = true;
}

/// <summary>What the gate did (#5912): a refusal, or the change with the <c>action_called</c> row's id and the after image.</summary>
public sealed record AgentActionResult(AgentActionRefusal? Refusal, long? CalledId, string? AfterJson)
{
    /// <summary>True when the change was made and journaled <c>action_done</c>.</summary>
    public bool Done => Refusal is null && CalledId is not null && !OutcomeUnknown;

    /// <summary>True when the server accepted the change but the store's own transaction failed, so <c>action_done</c> was written
    /// afterwards on a fresh connection with <c>recorded_late</c> (#5632, step 13).</summary>
    public bool RecordedLate { get; init; }

    /// <summary>True when the server accepted the change and the store could not record it at all. The call has <c>action_called</c>
    /// and no result row, so the agent changes list reads "result unknown" (#5632, step 13).</summary>
    public bool JournalMissing { get; init; }

    /// <summary>True when the tool sent the write and lost its reply (<see cref="ServerChangeSignal.MarkOutcomeUnknown"/>): the call has
    /// <c>action_called</c> and no result row, so the agent changes list reads "result unknown" (#5632).</summary>
    public bool OutcomeUnknown { get; init; }
}

public sealed partial class InvestigationRequestStore
{
    /// <summary>The service-wide agent switch (V191). A store with no row reads as on, its default.</summary>
    internal const string AgentActionsEnabledSql =
        "SELECT agent_actions_enabled FROM config.config_service WHERE id = 1";

    /// <summary>The same read for the write's own transaction: the row is held <c>FOR SHARE</c> to the end of the write, so turning agent
    /// changes off waits for the writes in flight, and a write that starts after it commits reads it off (#5632). The <c>mcp</c> role may
    /// lock the row: it holds <c>UPDATE</c> on two columns of it.</summary>
    internal const string AgentActionsEnabledLockedSql = AgentActionsEnabledSql + " FOR SHARE";

    /// <summary>The same read for a Level 2 store write (a mute, a threshold override). That write runs in this transaction, and its
    /// <c>config_bump_version</c> trigger UPDATEs this row, so a share lock would make two such writes wait on each other (40P01). The
    /// <c>FOR NO KEY UPDATE</c> lock lets them queue, and a person's switch write still waits for it (#5632).</summary>
    internal const string AgentActionsEnabledStoreWriteLockedSql = AgentActionsEnabledSql + " FOR NO KEY UPDATE";

    /// <summary>The longest the gated remote work of one agent change may take: one deadline over the connect, the read and the write (#5632).
    /// The gate holds the service row's lock and the server's switch lock until the work ends, so this is also the longest a person's switch
    /// waits behind it. Its expiry is an unknown outcome: the called row stands with no result.</summary>
    public const int AgentActionRemoteWorkSeconds = 30;

    /// <summary>The deadline a gated write's remote work gets; <see cref="AgentActionRemoteWorkSeconds"/> unless a test shortens it.</summary>
    internal TimeSpan RemoteWorkDeadline { get; init; } = TimeSpan.FromSeconds(AgentActionRemoteWorkSeconds);

    /// <summary>How long a person's off switch (and a pause) waits for an agent change in progress to finish: longer than
    /// <see cref="AgentActionRemoteWorkSeconds"/>, so the off waits and then succeeds instead of giving up first (#5632).</summary>
    public const int AgentOffSwitchWaitSeconds = 60;

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
SELECT DISTINCT metric_name, severity
FROM config.investigation_request_alerts
WHERE request_id = $1";

    internal const string AgentActionJournalSql = @"
INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, reason, detail)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
RETURNING journal_id";

    /// <summary>The late <c>action_done</c> of a change the server accepted (#5632, step 13). Written only when the call has no
    /// <c>action_done</c> yet, so a commit that succeeded before its connection failed is not recorded twice.</summary>
    internal const string AgentActionLateDoneSql = @"
INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, reason, detail)
SELECT $1, $2, $3, 'action_done', $4, NULL, $5::jsonb
WHERE NOT EXISTS (
    SELECT 1 FROM config.investigation_journal AS x
    WHERE x.request_id = $2 AND x.event = 'action_done' AND x.detail->>'called_id' = $6)
RETURNING journal_id";

    /// <summary>How many times the late <c>action_done</c> is tried (#5632, step 13).</summary>
    internal const int AgentActionLateAttempts = 2;

    /// <summary>The journal rows a change takes: <c>action_called</c> and <c>action_done</c> or <c>action_failed</c>.</summary>
    internal const int AgentActionJournalRows = 2;

    /// <summary>
    /// The one gate every Level 2 change goes through (#5912). Checks, in order, in one transaction with the request row
    /// taken <c>FOR SHARE</c>: 2 <c>actions_off</c>, 3 <c>claim_not_live</c> (one message for every cause), 4 <c>practice</c>,
    /// 5 <c>not_level_2</c> (the stamped level, and every metric of the request re-resolved over the live routes; the lowest
    /// wins), 6 <c>other_server</c>, 7 the kind rule, 8 <c>journal_capped</c>. Check 1 (the argument) is the caller's, through
    /// <c>InvestigationToolCore.ParseAgentAction</c>, before the gate. The first failing check answers its refusal, changes
    /// nothing, and is journaled <c>refused</c> at most once per request, verified token, tool, code and hour. On a pass the gate reads the
    /// before image and commits <c>action_called</c> first, which drops the lock; the change then runs in its own
    /// transaction, which takes the row <c>FOR SHARE</c> again and re-checks 3, 5 and 6 before it writes, and journals
    /// <c>action_done</c> with it, or <c>action_failed</c>. No row holds the claim id: the journal names the claim by its
    /// number (<c>claim_no</c>). Every transaction here, and the late <c>action_done</c>, runs on the store owner's data source
    /// when the store was given one (#6569): the journal keeps <see cref="AgentActionEvents"/> to the owner, so the checks, the
    /// store change a tool's write hook makes and the rows stay in one transaction on the login that may write them. The agent's
    /// own identity checks (token, claim, request) are the same; only the login that writes changed.
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
        await using (var connection = await AgentActionSource.OpenConnectionAsync(cancellationToken))
        await using (var transaction = await connection.BeginTransactionAsync(cancellationToken))
        {
            var request = await ReadAgentActionRequestAsync(connection, transaction, action.RequestId, cancellationToken);
            var refusal = await ActionsOffRefusalAsync(connection, transaction, lockRow: false, cancellationToken)
                ?? await LiveChecksAsync(connection, transaction, request, call, hooks, now, cancellationToken);

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
                    request?.ServerId, actor, call.TokenName, call.Tool, refusal.Reason, cancellationToken);
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

        var signal = hooks.ServerChange;
        string? after = null;
        var writeNow = Now();
        var requestServerId = call.Target.ServerId;
        try
        {
            await using var connection = await AgentActionSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            var request = await ReadAgentActionRequestAsync(connection, transaction, action.RequestId, cancellationToken);
            /* Everything the gate checked is checked again here (#5632): a person can turn agent changes off, turn this server's switch off or
               lower the route between the committed action_called row and the write, and a kill cannot be undone. */
            var recheck = await WriteTimeRefusalAsync(connection, transaction, request, call, hooks, writeNow, cancellationToken);
            if (recheck is not null)
            {
                await JournalReturningAsync(connection, transaction, writeNow, action.RequestId, request?.ServerId,
                    "action_failed", actor, recheck.Reason,
                    new JsonObject { ["called_id"] = calledId, ["error"] = recheck.Reason }.ToJsonString(), cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return new AgentActionResult(recheck, calledId, null);
            }

            requestServerId = request!.ServerId;
            /* One deadline over the whole of the remote work (connect, read, write), because the locks above are held until it ends and a
               person's off switch waits behind them (#5632). Its expiry reaches the tool as a cancelled token: a write that was sent reports
               an unknown outcome, as for any cancel. Only a tool with a monitored server behind it has remote work to bound. */
            using var remoteWork = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (signal is not null)
                remoteWork.CancelAfter(RemoteWorkDeadline);
            try
            {
                after = await hooks.Write(connection, transaction, remoteWork.Token);
            }
            catch (OperationCanceledException) when (signal is not null && remoteWork.IsCancellationRequested
                && !cancellationToken.IsCancellationRequested && !signal.Changed)
            {
                /* The deadline ran out and the tool did not say where: it may have been in the connect or the read, or after the write was sent
                   (the PostgreSQL executor lets a cancel go by). So the outcome is unknown, exactly as for a write whose reply was lost: the
                   called row stands with no result, and the caller gets an answer instead of a bare cancellation. */
                signal.MarkOutcomeUnknown();
                throw new TimeoutException($"The monitored server did not finish within {AgentActionRemoteWorkSeconds} seconds. "
                    + "Whether the change was made is unknown: check the server before trying again.");
            }

            if (signal?.OutcomeUnknown == true && !signal.Changed)
            {
                /* The write was sent and its reply was lost (#5632): no result row, so the called row reads "result unknown". The
                   transaction holds only the re-check's reads, so rolling it back undoes nothing on the server. The same network fault
                   that lost the reply can fail this rollback, or the caller can cancel; neither may turn the answer into an error or
                   journal action_failed, so the rollback takes no token and its failure is dropped (the connection's close ends the
                   transaction anyway). */
                try
                {
                    await transaction.RollbackAsync(CancellationToken.None);
                }
                catch (Exception)
                {
                }

                return new AgentActionResult(null, calledId, null) { OutcomeUnknown = true };
            }

            await JournalReturningAsync(connection, transaction, writeNow, action.RequestId, request.ServerId, "action_done",
                actor, null,
                new JsonObject { ["called_id"] = calledId, ["after"] = after is null ? null : JsonNode.Parse(after) }.ToJsonString(),
                cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return new AgentActionResult(null, calledId, after);
        }
        catch (Exception ex) when (signal?.Changed == true)
        {
            /* The monitored server accepted the change, and that does not roll back with the store's transaction (#5632, step 13). So
               the journal never says action_failed for this call: record action_done on a fresh connection, then let a cancellation
               go on (the change happened whatever the caller does). When the store cannot be written at all the called row stands
               with no result, which reads as "result unknown". */
            var late = await RecordLateDoneAsync(action.RequestId, requestServerId, actor, calledId, after, ex.Message, writeNow);
            if (ex is OperationCanceledException)
                throw;
            return new AgentActionResult(null, calledId, after) { RecordedLate = late, JournalMissing = !late };
        }
        catch (Exception ex) when (ex is not OperationCanceledException && signal?.OutcomeUnknown != true)
        {
            /* Only a store-only write, or a failure before the server accepted anything, gets here (a write whose outcome is unknown never
               does: its called row stays with no result, which reads as "result unknown"): the change rolled back with
               its transaction (a store change) or never happened (a refused write), so journal the failure on its own. A
               failure to write this row leaves the called row with neither result, which reads as "result unknown". */
            await using var connection = await AgentActionSource.OpenConnectionAsync(CancellationToken.None);
            await using var transaction = await connection.BeginTransactionAsync(CancellationToken.None);
            await JournalReturningAsync(connection, transaction, Now(), action.RequestId, call.Target.ServerId, "action_failed",
                actor, "error", new JsonObject { ["called_id"] = calledId, ["error"] = ex.Message }.ToJsonString(),
                CancellationToken.None);
            await transaction.CommitAsync(CancellationToken.None);
            throw;
        }
    }

    /// <summary>
    /// Records <c>action_done</c> with <c>recorded_late</c> for a change the server accepted, on a fresh connection with no
    /// cancellation, at most <see cref="AgentActionLateAttempts"/> times (#5632, step 13). True when the call has its
    /// <c>action_done</c> afterwards; false when both attempts failed and the call is left with no result row.
    /// </summary>
    private async Task<bool> RecordLateDoneAsync(Guid requestId, int serverId, string actor, long calledId, string? after,
        string storeError, DateTime at)
    {
        var detail = new JsonObject
        {
            ["called_id"] = calledId,
            ["after"] = after is null ? null : JsonNode.Parse(after),
            ["recorded_late"] = true,
            ["error"] = storeError,
        }.ToJsonString();
        for (var attempt = 0; attempt < AgentActionLateAttempts; attempt++)
        {
            try
            {
                await using var connection = await AgentActionSource.OpenConnectionAsync(CancellationToken.None);
                await using var command = Command(connection, null, AgentActionLateDoneSql,
                    P(at), P(requestId), new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer },
                    P(actor), P(detail), P(calledId.ToString(CultureInfo.InvariantCulture)));
                /* No row back means the call already had its action_done (the commit went through and the failure came after it),
                   so there is nothing to add and the call is recorded either way. */
                await command.ExecuteScalarAsync(CancellationToken.None);
                return true;
            }
            catch (Exception)
            {
                /* Try once more; the second failure leaves the call with no result row, which reads as "result unknown". */
            }
        }

        return false;
    }

    /// <summary>
    /// Journals a refusal the gate's caller made before the gate (#5912, check 1, or the tool's own refusal): <c>refused</c>,
    /// at most once per request, verified token, tool, code and hour. An unparsed request id journals with no request, once per verified
    /// token, tool, code and hour. The key is the named agent token's name (<paramref name="tokenName"/>), else <see cref="RefusalUnboundKey"/>, never the
    /// agent id the caller typed: with no named token a new agent id is free text, and a key built on it would write a new row on every call.
    /// The row's actor is the agent id (the token's name when there is one). An actor the agent id rule refuses is journaled as
    /// <see cref="InvestigationRules.InvalidAgentId"/>.
    /// </summary>
    public async Task RecordAgentActionRefusalAsync(Guid? requestId, string? agentId, string tool, string code, string? tokenName,
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

        await JournalRefusedAsync(connection, transaction, Now(), known, serverId, actor, tokenName, tool, code, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    /// <summary>
    /// The re-check in the write's own transaction (#5632): every condition the gate checked that a person can change in between, with the
    /// service row held <c>FOR SHARE</c> (<c>FOR NO KEY UPDATE</c> for a store write) and the advisory lock for the server's switch, until the write ends. A server action also needs Level 3
    /// and this server's switch for its action, through the same code the gate runs.
    /// </summary>
    private async Task<AgentActionRefusal?> WriteTimeRefusalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        AgentActionRequest? request, AgentActionCall call, AgentActionHooks hooks, DateTime now, CancellationToken cancellationToken)
    {
        var serverAction = ServerActionForKind(call.Kind);
        if (serverAction is not null)
            await ServerActionSwitchLock.AcquireSharedAsync(connection, transaction, call.Target.ServerId, serverAction,
                _commandTimeoutSeconds, cancellationToken);
        /* The routes and the tags are not locked here: every writer of those rows (the route store, the tag store, the Viewer's edits, the role
           provisioning script) fires the statement-level config_bump_version trigger, which UPDATEs the service row read below, so a change to
           them waits for this write through that row, and the next write reads it. A change another transaction has made but not committed is
           not seen here, and its trigger then waits for this write to end. */
        if (await ActionsOffRefusalAsync(connection, transaction, lockRow: true, cancellationToken, storeWrite: serverAction is null) is { } off)
            return off;

        return serverAction is null
            ? await LiveChecksAsync(connection, transaction, request, call, hooks, now, cancellationToken, practice: false)
            : await ServerActionConditionsRefusalAsync(connection, transaction, request, serverAction, call.Action, call.Target,
                hooks.LoadLevels, now, practice: false, cancellationToken);
    }

    /// <summary>Check 2: the service's agent switch (<c>config_service.agent_actions_enabled</c>). Null when it is on. Read in the gate and again before the write
    /// (<paramref name="lockRow"/>: the write's read holds the row <c>FOR SHARE</c>).</summary>
    private async Task<AgentActionRefusal?> ActionsOffRefusalAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, bool lockRow,
        CancellationToken cancellationToken, bool storeWrite = false)
    {
        await using var command = Command(connection, transaction,
            !lockRow ? AgentActionsEnabledSql : storeWrite ? AgentActionsEnabledStoreWriteLockedSql : AgentActionsEnabledLockedSql);
        var enabled = await command.ExecuteScalarAsync(cancellationToken) is not bool on || on;
        return enabled ? null
            : new AgentActionRefusal(AgentActionCodes.ActionsOff,
                "Agent changes are turned off for this service (config_service.agent_actions_enabled). Report what you would change instead.");
    }

    /// <summary>Checks 3 to 6 (4 only in the gate's own transaction: practice is fixed at open).</summary>
    private static async Task<AgentActionRefusal?> LiveChecksAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        AgentActionRequest? request, AgentActionCall call, AgentActionHooks hooks, DateTime now, CancellationToken cancellationToken,
        bool practice = true)
    {
        var action = call.Action;
        if (ClaimOrPracticeRefusal(request, action, now, practice) is { } claimRefusal)
            return claimRefusal;
        var live = request!;

        var level = await ResolveRequestLevelAsync(connection, transaction, live, hooks.LoadLevels, 2, cancellationToken);

        /* Level 3 (#5632, step 5) includes everything Level 2 allows, so a Level 3 request passes this check. The routes' lowest level
           still wins above, and a stamped level above 3 is not a level. */
        if (level < 2)
            return new AgentActionRefusal(AgentActionCodes.NotLevel2,
                "This request is not Level 2: its routes let an agent investigate, not change settings. Report what you would change instead.");

        if (call.Target.ServerId != live.ServerId)
            return new AgentActionRefusal(AgentActionCodes.OtherServer,
                $"This change targets {call.Target.ServerName}; the request is for {live.ServerName}. Level 2 changes only the request's server.");

        return null;
    }

    /// <summary>
    /// The claim and practice checks both gates share: the claim is live and this agent's, and the request is not a practice
    /// request. One message for every claim cause, so it is not an oracle. Null when both pass.
    /// </summary>
    private static AgentActionRefusal? ClaimOrPracticeRefusal(AgentActionRequest? request, AgentAction action, DateTime now, bool practice)
    {
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

        return null;
    }

    /// <summary>
    /// The level the request holds now: its stamp, lowered to the level the live routes send each of its metrics on. The routes
    /// are read once per pass, and only when the stamp is at least <paramref name="floor"/>. A metric whose route was deleted
    /// after the request opened resolves to null and lowers the request to 0: fail closed (pinned live).
    /// </summary>
    private static async Task<int> ResolveRequestLevelAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        AgentActionRequest request, Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<Func<string, int, AlertSeverityLevel?, int?>>> loadLevels,
        int floor, CancellationToken cancellationToken)
    {
        var level = request.AutonomyLevel;
        if (level >= floor)
        {
            var resolve = await loadLevels(connection, transaction, cancellationToken);
            foreach (var (metric, severity) in request.Metrics)
            {
                if (level < floor)
                    break;
                level = Math.Min(level, resolve(metric, request.ServerId, severity) ?? 0);
            }
        }

        return level;
    }

    private sealed record AgentActionRequest(string State, Guid? ClaimId, string? ClaimedBy, DateTime? LeaseUntil,
        DateTime ExpiresAt, int ServerId, string ServerName, Guid? PracticeOf, int AutonomyLevel, int ClaimCount,
        IReadOnlyList<(string Metric, AlertSeverityLevel? Severity)> Metrics);

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
                Array.Empty<(string, AlertSeverityLevel?)>());
        }

        /* #6928: each alert's metric at the tier it arrived at, so the live routes are read at the tier dispatch used: a Critical alert
           dispatched under one route is not re-resolved under a Warning-only route that dispatch skipped. The first metric stands
           alone only when no alert row names it (an old request), and resolves with no tier. */
        var metrics = new List<(string Metric, AlertSeverityLevel? Severity)>();
        await using (var command = Command(connection, transaction, AgentActionMetricsSql, P(requestId)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var metric = reader.GetString(0);
                var severity = reader.IsDBNull(1) ? null
                    : string.Equals(reader.GetString(1), InvestigationEvents.Critical, StringComparison.Ordinal) ? AlertSeverityLevel.Critical
                    : (AlertSeverityLevel?)AlertSeverityLevel.Warning;
                if (!metrics.Contains((metric, severity)))
                    metrics.Add((metric, severity));
            }
        }

        if (!metrics.Any(m => string.Equals(m.Metric, firstMetric, StringComparison.Ordinal)))
            metrics.Insert(0, (firstMetric, null));

        return row with { Metrics = metrics };
    }

    /// <summary>The once-key segment of a refusal that no named agent token made (the legacy token, or a named operator token).</summary>
    internal const string RefusalUnboundKey = "unbound";

    private Task<int> JournalRefusedAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime now,
        Guid? requestId, int? serverId, string actor, string? tokenName, string tool, string code, CancellationToken cancellationToken)
    {
        var onceKey = string.Create(CultureInfo.InvariantCulture,
            $"agent_refused:{(requestId is Guid id ? id.ToString() : "unknown")}:{tokenName ?? RefusalUnboundKey}:{tool}:{code}:{now:yyyy-MM-ddTHH}");
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
