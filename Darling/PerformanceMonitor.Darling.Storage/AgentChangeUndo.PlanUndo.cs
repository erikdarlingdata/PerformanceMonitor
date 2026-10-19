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
    JsonNode? Target)
{
    /// <summary>True for <c>plan_force</c>: the undo puts back what was forced before, or removes the force.</summary>
    public bool IsForce => Kind == AgentChangeKinds.PlanForce;
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

    private const string PlanCalledSql = @"
SELECT c.detail::text, c.request_id, c.server_id, c.event_time, r.server_name
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
        }

        if (Now() - calledAt > Window)
            return (null, AgentUndoResult.Refused("expired", "Undo works for 365 days from the change, and this one is older."));
        if (JsonNode.Parse(calledDetail ?? "{}") is not JsonObject called)
            return (null, AgentUndoResult.Refused("not_undoable", "This change has no recorded before image."));
        var kind = called["kind"] is JsonValue kv && kv.TryGetValue(out string? k) ? k : null;
        if (kind is not (AgentChangeKinds.PlanForce or AgentChangeKinds.PlanUnforce))
            return (null, AgentUndoResult.Refused("not_undoable", "This kind of change is not a plan force or unforce."));
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
        public async Task<ServerActionDecision> AuthorizeAsync(long planId, NpgsqlDataSource ownerSource, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(ownerSource);
            var store = new InvestigationRequestStore(_owner._dataSource, _owner._commandTimeoutSeconds);
            var refusal = await store.SwitchAndCredentialRefusalAsync(_connection, _transaction, Facts.ServerId, Facts.ServerName, ServerActionNames.ForcePlan,
                (serverId, token) => store.ReadServerCredentialStateAsync(ownerSource, serverId, token), person: true, cancellationToken);
            if (refusal is not null)
            {
                await RecordRefusalAsync(refusal.Reason, cancellationToken);
                return new ServerActionDecision(null, refusal);
            }

            return new ServerActionDecision(new ServerActionPermit(ServerActionNames.ForcePlan, Facts.ServerId, Facts.ServerName,
                planId.ToString(CultureInfo.InvariantCulture), Facts.Database, Facts.RequestId, Actor), null);
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
        /// Journals <c>action_undone</c> (#5632, step 9, rule 7) with the call's own <c>request_id</c> and <c>server_id</c> and <c>detail.called_id</c> as
        /// a JSON number, the shape step 8's readers and the list match on. <paramref name="replaced"/> is the state read before the write,
        /// <paramref name="restored"/> the state read after it.
        /// </summary>
        public async Task JournalUndoneAsync(string statement, JsonNode? replaced, JsonNode? restored, string? restoredAs, CancellationToken cancellationToken = default)
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
            await _owner.JournalAsync(_connection, _transaction, "action_undone", Actor, Facts.RequestId, Facts.ServerId, detail.ToJsonString(), cancellationToken);
        }

        /// <summary>Commits what the session journaled and drops the lock.</summary>
        public async Task CommitAsync(CancellationToken cancellationToken = default)
        {
            if (_done)
                return;
            _done = true;
            await _transaction.CommitAsync(cancellationToken);
        }

        public async ValueTask DisposeAsync()
        {
            _done = true;
            await _transaction.DisposeAsync();
            await _connection.DisposeAsync();
        }
    }
}
