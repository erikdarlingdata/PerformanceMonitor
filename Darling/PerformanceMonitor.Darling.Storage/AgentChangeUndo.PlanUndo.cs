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
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>What a plan force or unforce did, read from its journal rows (#5632, step 9): the query and plan, the state the agent read before
/// it wrote (<see cref="BeforeForcedPlanId"/> is the plan that was forced then, how it was forced is <see cref="BeforeForcingType"/>),
/// and how the call ended.</summary>
public sealed record PlanChangeFacts(
    long CalledId,
    Guid RequestId,
    int ServerId,
    string ServerName,
    string Kind,
    string Database,
    long QueryId,
    long PlanId,
    long? BeforeForcedPlanId,
    string? BeforeForcingType,
    AgentChangeResult Result,
    JsonNode? Target,
    JsonNode? Guide = null)
{
    /// <summary>True for <c>plan_force</c>: the undo puts back what was forced before, or removes the force.</summary>
    public bool IsForce => Kind == AgentChangeKinds.PlanForce;

    /// <summary>True for <c>plan_guide_create</c> (#6569): the undo drops the guide <see cref="GuideName"/>. <see cref="QueryId"/> and <see cref="PlanId"/> are 0 and mean nothing.</summary>
    public bool IsGuide => Kind is AgentChangeKinds.PlanGuideCreate or AgentChangeKinds.PlanGuideDrop;

    /// <summary>True for <c>plan_guide_drop</c> (#6569, slice 2b): the undo re-creates the guide from the definition the drop journaled (<see cref="Guide"/>).</summary>
    public bool IsGuideDrop => Kind == AgentChangeKinds.PlanGuideDrop;

    /// <summary>The guide a create made: its name, from the journaled target's id.</summary>
    public string? GuideName => Guide is JsonObject g && g["name"] is JsonValue v && v.TryGetValue(out string? s) ? s : null;
}

/// <summary>
/// A person's undo of a plan force or unforce, as one answer (#5632, step 9). <see cref="Status"/> is <c>confirm</c> (the statement and the state
/// read, nothing written), <c>undone</c>, <c>refused</c> (<see cref="Code"/> says why) or <c>failed</c> (the server did not take the write).
/// </summary>
public sealed record PlanUndoAnswer(
    string Status,
    string? Code,
    string Message,
    string? Statement = null,
    JsonNode? State = null,
    JsonNode? After = null,
    string? RestoredAs = null,
    long? CalledId = null,
    string? UndoneBy = null)
{
    public const string Confirm = "confirm";
    public const string Undone = "undone";
    public const string RefusedStatus = "refused";
    public const string Failed = "failed";

    /// <summary>The write was sent and its reply was lost (or the call was cancelled while it ran): nobody knows whether the plan changed (#5632). Nothing is journaled as undone or as refused.</summary>
    public const string OutcomeUnknown = "outcome_unknown";

    /// <summary>True when the request was answered without a refusal or a failure: a confirm step or an undo.</summary>
    public bool Succeeded => Status is Confirm or Undone;

    public static PlanUndoAnswer Refused(string code, string message) => new(RefusedStatus, code, message);

    /// <summary>The answer as the web route and the Viewer's command carry it.</summary>
    public JsonObject ToJson()
    {
        var node = new JsonObject { ["status"] = Status };
        if (Code is not null)
            node["code"] = Code;
        node["message"] = Message;
        if (Statement is not null)
            node["statement"] = Statement;
        if (State is not null)
            node["state"] = State.DeepClone();
        if (After is not null)
            node["after"] = After.DeepClone();
        if (RestoredAs is not null)
            node["restored_as"] = RestoredAs;
        if (CalledId is not null)
            node["called_id"] = CalledId;
        if (UndoneBy is not null)
            node["undone_by"] = UndoneBy;
        return node;
    }

    /// <summary>Reads an answer back from <see cref="ToJson"/>'s text (the Viewer reads the command's <c>result_json</c> with it). Null when the text is not an answer.</summary>
    public static PlanUndoAnswer? TryRead(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return null;
        try
        {
            if (JsonNode.Parse(json) is not JsonObject o || o["status"] is not JsonValue sv || !sv.TryGetValue(out string? status) || status is null)
                return null;
            string? Text(string key) => o[key] is JsonValue v && v.TryGetValue(out string? s) ? s : null;
            return new PlanUndoAnswer(status, Text("code"), Text("message") ?? Text("error") ?? "", Text("statement"), o["state"]?.DeepClone(), o["after"]?.DeepClone(),
                Text("restored_as"), o["called_id"] is JsonValue c && c.TryGetValue(out long id) ? id : null, Text("undone_by"));
        }
        catch (System.Text.Json.JsonException)
        {
            return null;
        }
    }
}

/// <summary>The start of a plan undo: the session when the change can be undone from here, else the refusal.</summary>
public sealed record PlanUndoBegin(AgentChangeUndo.PlanUndoSession? Session, AgentUndoResult? Refusal);

public sealed partial class AgentChangeUndo
{
    /// <summary>The tool name a person's plan undo journals its refusals under.</summary>
    public const string PlanUndoTool = "undo_plan_change";

    /// <summary>The late <c>action_undone</c> of a plan undo the server accepted (#5632, step 13): written only while the call has no <c>action_undone</c> yet.</summary>
    private const string LateUndoneSql = @"
INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, reason, detail)
SELECT $1, $2, $3, 'action_undone', $4, NULL, $5::jsonb
WHERE NOT EXISTS (
    SELECT 1 FROM config.investigation_journal AS x
    WHERE x.request_id IS NOT DISTINCT FROM $2 AND x.event = 'action_undone' AND x.detail->>'called_id' = $6)
RETURNING journal_id";

    private const string PlanCalledSql = @"
SELECT c.detail::text, c.request_id, c.server_id, c.event_time, r.server_name, c.actor, r.server_id, r.autonomy_level
FROM config.investigation_journal AS c
LEFT JOIN config.investigation_requests AS r ON r.request_id = c.request_id
WHERE c.journal_id = $1 AND c.event = 'action_called'";

    /// <summary>
    /// Opens a person's undo of the plan force or unforce <paramref name="calledId"/> (#5632, step 9). One store transaction that takes the same
    /// advisory lock on the call's id as <see cref="UndoAsync"/>, so two clicks cannot both write: the second waits, then reads
    /// <c>already_undone</c>. The caller (the service's plan force code) reads the server, decides, writes there, then journals through the session.
    /// Throws <see cref="ArgumentException"/> for a login that cannot be recorded. Never throws for a refusal.
    /// </summary>
    public async Task<PlanUndoBegin> BeginPlanUndoAsync(long calledId, string login, CancellationToken cancellationToken = default)
    {
        var actor = PersonActor(login);
        var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        NpgsqlTransaction? transaction = null;
        try
        {
            transaction = await connection.BeginTransactionAsync(cancellationToken);
            await using (var lockCommand = Command(connection, transaction, "SELECT pg_advisory_xact_lock($1)", P(calledId)))
            {
                await lockCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            var (facts, refusal) = await ReadPlanFactsAsync(connection, transaction, calledId, cancellationToken);
            if (facts is null)
            {
                await transaction.DisposeAsync();
                await connection.DisposeAsync();
                return new PlanUndoBegin(null, refusal);
            }

            return new PlanUndoBegin(new PlanUndoSession(this, connection, transaction, facts, actor), null);
        }
        catch
        {
            if (transaction is not null)
                await transaction.DisposeAsync();
            await connection.DisposeAsync();
            throw;
        }
    }

    private async Task<(PlanChangeFacts? Facts, AgentUndoResult? Refusal)> ReadPlanFactsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        long calledId, CancellationToken cancellationToken)
    {
        string? calledDetail;
        Guid? requestId;
        int? serverId;
        DateTime calledAt;
        string? serverName;
        string? calledActor;
        int? requestServerId;
        int? requestLevel;
        await using (var command = Command(connection, transaction, PlanCalledSql, P(calledId)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
                return (null, AgentUndoResult.Refused("not_found", "That change is not in the journal."));
            calledDetail = reader.IsDBNull(0) ? null : reader.GetString(0);
            requestId = reader.IsDBNull(1) ? null : reader.GetGuid(1);
            serverId = reader.IsDBNull(2) ? null : reader.GetInt32(2);
            calledAt = Utc(reader.GetDateTime(3));
            serverName = reader.IsDBNull(4) ? null : reader.GetString(4);
            calledActor = reader.IsDBNull(5) ? null : reader.GetString(5);
            requestServerId = reader.IsDBNull(6) ? null : reader.GetInt32(6);
            requestLevel = reader.IsDBNull(7) ? null : Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture);
        }

        if (Now() - calledAt > Window)
            return (null, AgentUndoResult.Refused("expired", "Undo works for 365 days from the change, and this one is older."));
        if (JsonNode.Parse(calledDetail ?? "{}") is not JsonObject called)
            return (null, AgentUndoResult.Refused("not_undoable", "This change has no recorded before image."));
        var kind = called["kind"] is JsonValue kv && kv.TryGetValue(out string? k) ? k : null;
        if (kind is not (AgentChangeKinds.PlanForce or AgentChangeKinds.PlanUnforce or AgentChangeKinds.PlanGuideCreate or AgentChangeKinds.PlanGuideDrop))
            return (null, AgentUndoResult.Refused("not_undoable", "This kind of change is not a plan force, an unforce or a plan guide create or drop."));
        if (await FirstDetailAsync(connection, transaction, "action_undone", calledId, requestId, cancellationToken) is not null)
            return (null, AgentUndoResult.Refused("already_undone", "This change was already undone."));

        var result = await FirstDetailAsync(connection, transaction, "action_done", calledId, requestId, cancellationToken) is not null ? AgentChangeResult.Done
            : await FirstDetailAsync(connection, transaction, "action_failed", calledId, requestId, cancellationToken) is not null ? AgentChangeResult.Failed
            : AgentChangeResult.Unknown;

        var target = called["target"] as JsonObject;
        var before = called["before"] as JsonObject;
        string? Text(JsonNode? node) => node is JsonValue v && v.TryGetValue(out string? s) ? s : node?.ToString();
        long? Number(JsonNode? node) => node is JsonValue v && v.TryGetValue(out long n) ? n : (long.TryParse(Text(node), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ? parsed : null);
        var database = Text(target?["database"]) ?? Text(before?["database"]);
        if (kind is AgentChangeKinds.PlanGuideCreate or AgentChangeKinds.PlanGuideDrop)
        {
            /* #6569: a plan guide create is undone by dropping the guide by name, a drop by re-creating it from the journaled definition, so the facts are the target's id and the definition the call journaled. */
            var guide = before?["guide"] as JsonObject;
            var guideName = Text(target?["id"]);
            if (requestId is not Guid guideRequest || serverId is not int guideServer || string.IsNullOrWhiteSpace(database) || string.IsNullOrWhiteSpace(guideName) || guide is null)
                return (null, AgentUndoResult.Refused("not_undoable", "This change's guide or database was not recorded, so it cannot be undone from here."));
            /* #6569 round 2: the Undo acts only on a row the create and drop tools wrote. They record an agent actor ("agent:" + the agent id) under a request of the same server that was
               Level 3 when the call was made, so a journal row without those is not one of theirs. */
            if (calledActor is null || !calledActor.StartsWith("agent:", StringComparison.Ordinal) || requestServerId != guideServer || requestLevel is not >= 3)
                return (null, AgentUndoResult.Refused("not_undoable", "This row was not written by a plan guide create or drop made by an agent under a Level 3 request on this server, so it cannot be undone from here."));
            var guideFacts = guide.DeepClone().AsObject();
            guideFacts["name"] = guideName;
            return (new PlanChangeFacts(calledId, guideRequest, guideServer, serverName ?? ("server " + guideServer.ToString(CultureInfo.InvariantCulture)), kind, database, 0, 0, null, null,
                result, target?.DeepClone(), guideFacts), null);
        }

        var queryId = Number(before?["query_id"]);
        var planId = Number(target?["id"]) ?? Number(before?["plan_id"]);
        var forcedBefore = before?["forced_plan"] as JsonObject;
        if (requestId is not Guid request || serverId is not int server || string.IsNullOrWhiteSpace(database) || queryId is not long query || planId is not long plan)
            return (null, AgentUndoResult.Refused("not_undoable", "This change's query, plan or database was not recorded, so it cannot be undone from here."));

        return (new PlanChangeFacts(calledId, request, server, serverName ?? ("server " + server.ToString(CultureInfo.InvariantCulture)), kind, database, query, plan,
            Number(forcedBefore?["plan_id"]), Text(forcedBefore?["forcing_type"]), result, target?.DeepClone()), null);
    }

    /// <summary>One person's undo in progress: the advisory lock is held until <see cref="CommitAsync"/> or disposal.</summary>
    public sealed class PlanUndoSession : IAsyncDisposable
    {
        private readonly AgentChangeUndo _owner;
        private readonly NpgsqlConnection _connection;
        private readonly NpgsqlTransaction _transaction;
        private bool _done;
        private bool _serverChanged;
        private JsonObject? _detail;

        internal PlanUndoSession(AgentChangeUndo owner, NpgsqlConnection connection, NpgsqlTransaction transaction, PlanChangeFacts facts, string actor)
        {
            _owner = owner;
            _connection = connection;
            _transaction = transaction;
            Facts = facts;
            Actor = actor;
        }

        public PlanChangeFacts Facts { get; }

        /// <summary><c>person:&lt;login&gt;</c>.</summary>
        public string Actor { get; }

        /// <summary>
        /// The permit for one write or read on the server, after the two server-side conditions only (#5632, step 9, rule 2): the server's
        /// <c>force_plan</c> switch and an <c>env:</c> or <c>file:</c> remediation login. The agent-only conditions (the service's agent switch, a claim,
        /// a Level 3 request) are not asked. A refusal is journaled with the person as the actor, once per call, code and hour.
        /// <paramref name="planId"/> is the plan the permit is for: the executor refuses a write on any other.
        /// </summary>
        public Task<ServerActionDecision> AuthorizeAsync(long planId, NpgsqlDataSource ownerSource, CancellationToken cancellationToken = default) =>
            AuthorizeAsync(planId.ToString(CultureInfo.InvariantCulture), ownerSource, cancellationToken);

        /// <summary>The same two server-side conditions for the object the permit is for: a plan id (as text) for a force or unforce, the guide's name for a plan guide create's or drop's undo, which that server's <c>create_plan_guide</c> or <c>drop_plan_guide</c> switch gates (#6569).</summary>
        public async Task<ServerActionDecision> AuthorizeAsync(string permitId, NpgsqlDataSource ownerSource, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(ownerSource);
            var serverAction = Facts.IsGuideDrop ? ServerActionNames.DropPlanGuide : Facts.IsGuide ? ServerActionNames.CreatePlanGuide : ServerActionNames.ForcePlan;
            var store = new InvestigationRequestStore(_owner._dataSource, _owner._commandTimeoutSeconds);
            /* The shared form of the server's switch lock, in the session's transaction and before the switch is read, as an agent's write takes it:
               a switch writer that takes the exclusive form then waits for this undo, and an undo that starts after it reads the committed switch. */
            await ServerActionSwitchLock.AcquireSharedAsync(_connection, _transaction, Facts.ServerId, serverAction,
                _owner._commandTimeoutSeconds, cancellationToken);
            var refusal = await store.SwitchAndCredentialRefusalAsync(_connection, _transaction, Facts.ServerId, Facts.ServerName, serverAction,
                (serverId, token) => store.ReadServerCredentialStateAsync(ownerSource, serverId, token), person: true, cancellationToken);
            if (refusal is not null)
            {
                await RecordRefusalAsync(refusal.Reason, cancellationToken);
                return new ServerActionDecision(null, refusal);
            }

            return new ServerActionDecision(new ServerActionPermit(serverAction, Facts.ServerId, Facts.ServerName,
                permitId, Facts.Database, Facts.RequestId, Actor), null);
        }

        /// <summary>Journals a refusal of this undo with the person as the actor: <c>refused</c>, once per call, code and hour.</summary>
        public async Task RecordRefusalAsync(string code, CancellationToken cancellationToken = default)
        {
            var now = _owner.Now();
            var onceKey = string.Create(CultureInfo.InvariantCulture, $"person_refused:{Facts.RequestId}:{Facts.CalledId}:{code}:{now:yyyy-MM-ddTHH}");
            await using var command = _owner.Command(_connection, _transaction, InvestigationRequestStore.JournalSql,
                new NpgsqlParameter<DateTime> { TypedValue = now, NpgsqlDbType = NpgsqlDbType.TimestampTz },
                new NpgsqlParameter<Guid?> { TypedValue = Facts.RequestId, NpgsqlDbType = NpgsqlDbType.Uuid },
                new NpgsqlParameter<int?> { TypedValue = null, NpgsqlDbType = NpgsqlDbType.Integer },
                new NpgsqlParameter<int?> { TypedValue = Facts.ServerId, NpgsqlDbType = NpgsqlDbType.Integer },
                PText("refused"), PText(Actor), PText(code),
                PText(new JsonObject { ["tool"] = PlanUndoTool, ["called_id"] = Facts.CalledId }.ToJsonString()), PText(onceKey));
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        /// <summary>
        /// The undo's write was sent and its reply was lost, or the call was cancelled while it ran (#5632): the plan may have changed. Journals the
        /// attempt as an <c>action_called</c> row of kind <see cref="AgentChangeKinds.PlanUndo"/> with the person as the actor and no result row,
        /// the way a kill whose reply was lost is journaled, so the Agent changes list reads it as "result unknown" next to the change it tried to
        /// undo. It is written and committed here with no cancellation (the caller's token may be the thing that fired), on a fresh connection when
        /// the session's own cannot take it. The change itself stays offered: its next undo reads the server first. Best effort: a store that
        /// cannot be written leaves only the service log.
        /// </summary>
        public async Task RecordOutcomeUnknownAsync(string statement)
        {
            var detail = new JsonObject
            {
                ["kind"] = AgentChangeKinds.PlanUndo,
                ["tool"] = PlanUndoTool,
                ["called_id"] = Facts.CalledId,
                ["target"] = Facts.Target?.DeepClone(),
                ["statement"] = statement,
            }.ToJsonString();
            try
            {
                await _owner.JournalAsync(_connection, _transaction, "action_called", Actor, Facts.RequestId, Facts.ServerId, detail, CancellationToken.None);
                await _transaction.CommitAsync(CancellationToken.None);
                _done = true;
                return;
            }
            catch (Exception)
            {
                /* The session's transaction cannot take it: write it on its own. */
            }

            try
            {
                await using var connection = await _owner._dataSource.OpenConnectionAsync(CancellationToken.None);
                await _owner.JournalAsync(connection, null, "action_called", Actor, Facts.RequestId, Facts.ServerId, detail, CancellationToken.None);
            }
            catch (Exception)
            {
            }
        }

        /// <summary>
        /// The monitored server accepted the undo's write (#5632, step 13). From here the undo has happened whatever the store does, so a failed
        /// <c>action_undone</c> insert or commit is recorded late on a fresh connection instead of being lost with the rolled-back transaction.
        /// </summary>
        public async Task MarkServerChangedAsync(string statement, JsonNode? replaced, string? restoredAs)
        {
            _serverChanged = true;
            _detail = BuildDetail(statement, replaced, null, restoredAs);
            /* The call's advisory lock is a transaction lock, so a failed commit would drop it before the late row exists and a second undo
               could read the call as not yet undone. Hold it for the session as well; DisposeAsync lets it go after the late row. */
            try
            {
                await using var command = _owner.Command(_connection, _transaction, "SELECT pg_advisory_lock($1)", P(Facts.CalledId));
                await command.ExecuteNonQueryAsync(CancellationToken.None);
                _sessionLocked = true;
            }
            catch (Exception)
            {
                /* The connection is already failing: the late write then takes the lock on a fresh connection. */
            }
        }

        private bool _sessionLocked;
        private bool _lateHandled;

        /// <summary>True when the <c>action_undone</c> row was written late, after the store's own write or commit failed. False when the commit had gone through, so the row was already there.</summary>
        public bool RecordedLate { get; private set; }

        /// <summary>True when the server accepted the undo and the store could not record it at all, not even late.</summary>
        public bool JournalMissing { get; private set; }

        private JsonObject BuildDetail(string statement, JsonNode? replaced, JsonNode? restored, string? restoredAs)
        {
            var detail = new JsonObject
            {
                ["called_id"] = Facts.CalledId,
                ["kind"] = Facts.Kind,
                ["target"] = Facts.Target?.DeepClone(),
                ["replaced"] = replaced?.DeepClone(),
                ["restored"] = restored?.DeepClone(),
                ["statement"] = statement,
            };
            if (restoredAs is not null)
                detail["restored_as"] = restoredAs;
            return detail;
        }

        /// <summary>
        /// Journals <c>action_undone</c> (#5632, step 9, rule 7) with the call's own <c>request_id</c> and <c>server_id</c> and <c>detail.called_id</c> as
        /// a JSON number, the shape step 8's readers and the list match on. <paramref name="replaced"/> is the state read before the write,
        /// <paramref name="restored"/> the state read after it. After <see cref="MarkServerChangedAsync"/> a failure of the insert is recorded late and
        /// not thrown (a cancellation is thrown after the late row).
        /// </summary>
        public async Task JournalUndoneAsync(string statement, JsonNode? replaced, JsonNode? restored, string? restoredAs, CancellationToken cancellationToken = default)
        {
            _detail = BuildDetail(statement, replaced, restored, restoredAs);
            try
            {
                await _owner.JournalAsync(_connection, _transaction, "action_undone", Actor, Facts.RequestId, Facts.ServerId, _detail.ToJsonString(), cancellationToken);
            }
            catch (Exception ex) when (_serverChanged)
            {
                await RecordLateAsync(ex.Message);
                if (ex is OperationCanceledException)
                    throw;
            }
        }

        /// <summary>
        /// Commits what the session journaled and drops the lock. After <see cref="MarkServerChangedAsync"/> a failed commit records
        /// <c>action_undone</c> late and does not throw, so the person is told the undo happened (a cancellation is thrown after the late row).
        /// </summary>
        public async Task CommitAsync(CancellationToken cancellationToken = default)
        {
            if (_done)
                return;
            _done = true;
            if (_lateHandled)
                return;
            try
            {
                await _transaction.CommitAsync(cancellationToken);
            }
            catch (Exception ex) when (_serverChanged)
            {
                await RecordLateAsync(ex.Message);
                if (ex is OperationCanceledException)
                    throw;
            }
        }

        /// <summary>
        /// Writes <c>action_undone</c> with <c>recorded_late</c>, at most <see cref="InvestigationRequestStore.AgentActionLateAttempts"/> times, under the
        /// call's advisory lock, as one statement that skips a call that already has its row (#5632). The first attempt runs on this session's own
        /// connection, which still holds the lock (<see cref="MarkServerChangedAsync"/>), so a second undo waiting on the call stays waiting until the
        /// row exists and then reads <c>already_undone</c>. If that connection is gone the next attempt opens a fresh one and takes the lock there.
        /// </summary>
        private async Task RecordLateAsync(string storeError)
        {
            var detail = (_detail ?? new JsonObject { ["called_id"] = Facts.CalledId, ["kind"] = Facts.Kind, ["target"] = Facts.Target?.DeepClone() }).DeepClone().AsObject();
            detail["recorded_late"] = true;
            detail["error"] = storeError;
            var text = detail.ToJsonString();
            for (var attempt = 0; attempt < InvestigationRequestStore.AgentActionLateAttempts; attempt++)
            {
                try
                {
                    /* While this session's connection is open every attempt runs on it: a statement timeout or a deadlock leaves it usable and it holds
                       the call's lock, so a fresh connection would wait on a lock its own session holds. A fresh one is for a broken connection only. */
                    RecordedLate = _connection.State == System.Data.ConnectionState.Open
                        ? await InsertLateAsync(_connection, text, takeLock: !_sessionLocked, endFirst: true)
                        : await InsertLateOnFreshConnectionAsync(text);
                    _lateHandled = true;
                    return;
                }
                catch (Exception)
                {
                    /* Try once more; a second failure leaves the change with no row, which the person is told. */
                }
            }

            _lateHandled = true;
            JournalMissing = true;
        }

        private async Task<bool> InsertLateOnFreshConnectionAsync(string text)
        {
            await using var connection = await _owner._dataSource.OpenConnectionAsync(CancellationToken.None);
            return await InsertLateAsync(connection, text, takeLock: true, endFirst: false);
        }

        /// <summary>One transaction: the call's lock when it is not already held, then the insert that skips a call with a row. True when a row was written.</summary>
        private async Task<bool> InsertLateAsync(NpgsqlConnection connection, string text, bool takeLock, bool endFirst)
        {
            if (endFirst)
            {
                /* The session's own transaction failed (an insert error leaves it aborted; a failed commit has ended it): end it first. */
                try
                {
                    await _transaction.RollbackAsync(CancellationToken.None);
                }
                catch (Exception)
                {
                }
            }

            await using var transaction = await connection.BeginTransactionAsync(CancellationToken.None);
            if (takeLock)
            {
                /* A lock still held by a connection the server has not noticed is dead must not hang the late write. */
                await using (var timeout = _owner.Command(connection, transaction, "SET LOCAL lock_timeout = '5s'"))
                    await timeout.ExecuteNonQueryAsync(CancellationToken.None);
                await using var lockCommand = _owner.Command(connection, transaction, "SELECT pg_advisory_xact_lock($1)", P(Facts.CalledId));
                await lockCommand.ExecuteNonQueryAsync(CancellationToken.None);
            }

            bool inserted;
            await using (var command = _owner.Command(connection, transaction, LateUndoneSql,
                new NpgsqlParameter<DateTime> { TypedValue = _owner.Now(), NpgsqlDbType = NpgsqlDbType.TimestampTz },
                new NpgsqlParameter<Guid?> { TypedValue = Facts.RequestId, NpgsqlDbType = NpgsqlDbType.Uuid },
                new NpgsqlParameter<int?> { TypedValue = Facts.ServerId, NpgsqlDbType = NpgsqlDbType.Integer },
                PText(Actor), PText(text), PText(Facts.CalledId.ToString(CultureInfo.InvariantCulture))))
            {
                /* No row back means the call already has its action_undone (the commit went through and the failure came after it). */
                inserted = await command.ExecuteScalarAsync(CancellationToken.None) is not null;
            }

            await transaction.CommitAsync(CancellationToken.None);
            return inserted;
        }

        public async ValueTask DisposeAsync()
        {
            _done = true;
            try
            {
                try
                {
                    await _transaction.DisposeAsync();
                }
                catch (Exception)
                {
                    /* A broken connection: the unlock below fails too and the pool is cleared. */
                }

                if (_sessionLocked)
                {
                    try
                    {
                        await using var unlock = _owner.Command(_connection, null, "SELECT pg_advisory_unlock($1)", P(Facts.CalledId));
                        await unlock.ExecuteNonQueryAsync(CancellationToken.None);
                    }
                    catch (Exception)
                    {
                        /* A pooled connection keeps a session lock until it is reused: have the data source close its connections for good instead.
                           (NpgsqlConnection.ClearPool clears only the pools of connection strings, not a data source's.) */
                        try
                        {
                            _owner._dataSource.Clear();
                        }
                        catch (Exception)
                        {
                        }
                    }
                }
            }
            finally
            {
                await _connection.DisposeAsync();
            }
        }
    }
}
