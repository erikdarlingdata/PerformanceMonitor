/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #9485: <c>declare_change_window</c> and <c>clear_change_window</c>, the two writes of planned change windows. A window names the servers
/// a planned change touches (one, a list or all) and a start and an end. An investigation request that opens inside it, or inside the
/// <see cref="InvestigationChangeWindows.TailMinutes"/> minute tail after it, is created held (<see cref="InvestigationChangeWindows"/>).
/// Pages and alert history do not change, which is what sets a window apart from <c>set_quiet_window</c>.
/// <para>
/// Who may call them: the writes arm (an operator, <c>mcp.allowWrites</c>), and an agent token under the same switch as agent mute rules,
/// <c>mcp.allowAgentMuteRules</c> (#7414), with the same limits (<see cref="DarlingMcpAgentMuteDirect"/>): each text field at most
/// <see cref="DarlingMcpAgentMuteDirect.MaxFieldChars"/> characters, and the agent's hourly cap, which these two tools share with the mute rule
/// verbs. Unlike a mute rule under an investigation, an agent MAY set a future <c>starts_at</c>: declaring ahead of planned work is the
/// point. An agent clears only a window it declared.
/// </para>
/// <para>
/// Both write the window and its agent change journal row (<see cref="AgentChangeKinds.ChangeWindow"/>) in one transaction on the store
/// owner's connection, as the mute rule verbs do, so a refused or failed write leaves nothing and the change is listed in Agent changes.
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpChangeWindowWriteTools
{
    internal const string DeclareToolName = "declare_change_window";
    internal const string ClearToolName = "clear_change_window";

    /// <summary>The longest window, in days: the same bound as a quiet window and an agent's mute rule.</summary>
    internal const int MaxWindowDays = 7;

    internal const string InvalidStatus = "invalid";

    /// <summary>
    /// How far ahead of now an agent's window may start, in days. With the 7 day window length, nothing an agent writes holds past 14 days, and
    /// a token cannot chain windows that outlive it. An operator is not bounded.
    /// </summary>
    internal const int MaxAgentLeadDays = 7;

    /// <summary>The refusal code for an agent whose <c>starts_at</c> is more than <see cref="MaxAgentLeadDays"/> days ahead.</summary>
    internal const string StartTooFarCode = "change_window_start_too_far";

    /// <summary>The most server names one declare may carry, after the duplicates are dropped; each name costs a registry read.</summary>
    internal const int MaxServerNames = 200;

    /// <summary>The refusal code for a declare that names more than <see cref="MaxServerNames"/> servers.</summary>
    internal const string TooManyNamesCode = "too_many_server_names";

    /// <summary>The refusal code for an agent that clears a window another caller declared.</summary>
    internal const string NotYoursCode = "change_window_not_yours";

    private const string AllWord = "all";

    private static readonly string OffMessage =
        $"An agent token may not declare or clear change windows on this service: {DarlingMcpAgentMuteDirect.SettingName} is false in darling.json. Nothing was written.";

    private const string NoOwnerMessage =
        "The service has not published its store owner connection yet, so the window and its journal row cannot be written. Try again shortly. Nothing was written.";

    private static readonly string RateMessage =
        $"This service allows each agent {DarlingMcpAgentMuteDirect.MaxWritesPerHour} direct writes an hour (mute rules and change windows together), and this agent made that many in the last hour. Wait, or ask an operator. Nothing was written.";

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "declare_change_window"), Description(
        "Declares a CHANGE WINDOW: investigation requests that open for the named servers between starts_at and ends_at plus 30 minutes are created held, not handed to the request runner. Pages and alert history do not change. Changes shared investigation configuration. " +
        "<<GUIDE>>" +
        "Use it ahead of planned work that touches servers (a maintenance script, a resize, a baseline reset, a restart that re-captures configuration), so the requests that work causes do not each open as a separate incident. " +
        "server_names is one name, a list of names, or the single word all (every server). Each name is resolved against the registry and an unknown name is refused. A name given twice counts once, and a list of more than 200 different names is refused. " +
        "starts_at and ends_at are ISO-8601 UTC instants (a start in the future is fine: declare ahead of the work; an agent's start may be at most 7 days from now, an operator's is not bounded). ends_at must be after starts_at and in the future, and a window lasts at most 7 days. reason is required. " +
        "A request is held when it opens after starts_at and up to 30 minutes after ends_at (the tail; fixed, so a real problem that follows the work is never hidden for longer). A held request is still listed " +
        "(list_investigation_requests) and can still be claimed, but it does not wake wait_for_investigation_requests and it counts against no route limit. " +
        "Alert-triggered and change-watch requests are held. A chat request, a force check and a practice request are asked for on purpose and are never held. " +
        "It is not a quiet window: a quiet window (set_quiet_window) also silences pages, and a change window holds only the investigation hand-off and records what it held. " +
        "Repeating the same call (same servers, start and end, window still open) answers already_declared with the window id and writes nothing. " +
        "Answer: {status:\"declared\", window_id, all_servers, servers:[{server_id, server}], starts_at_utc, ends_at_utc, tail_ends_at_utc, tail_minutes, reason, declared_by, undo:{tool:\"clear_change_window\", arguments:{window_id}}}. " +
        "Needs mcp.allowWrites, or an agent token while mcp.allowAgentMuteRules is on (each text field at most 256 characters; an agent shares the 20 writes an hour of the mute rule tools).")]
    public static Task<string> DeclareChangeWindow(
        NpgsqlDataSource postgres,
        [Description("The servers the planned change touches: one server name, several, or the single word all. Each name is the server's display name or the start of it.")] string[] server_names,
        [Description("ISO-8601 UTC start of the planned work (e.g. 2027-08-01T02:00:00Z). May be in the future: at most 7 days ahead for an agent token, any distance for an operator.")] string starts_at,
        [Description("ISO-8601 UTC end of the planned work. After starts_at, in the future, and at most 7 days after starts_at.")] string ends_at,
        [Description("Why the window is declared (1 to 2000 characters, at most 256 for an agent token, for example 'patch night, change 4471').")] string reason,
        DarlingAgentActionSeat? agentSeat = null,
        CancellationToken cancellationToken = default) =>
        DeclareAsync(postgres, agentSeat, server_names, starts_at, ends_at, reason, DateTime.UtcNow, cancellationToken);

    [DarlingToolAccess(ToolAccess.Write)]
    [McpServerTool(Name = "clear_change_window"), Description(
        "Clears a CHANGE WINDOW now: it holds nothing that opens after this moment. Every request the window still holds is released at once and reaches the request runner and wait_for_investigation_requests as a hold_released event. Changes shared investigation configuration. " +
        "<<GUIDE>>" +
        "window_id is the id declare_change_window answered. Clearing sets the window's cleared_at to now and releases the requests the window held; a request that opens later is not held, and the close summary still covers what the window held before. " +
        "A window that is already cleared, or whose tail has ended, answers already_cleared or already_closed and writes nothing (not an error). An unknown id answers not_found. reason is required. " +
        "An agent token can clear only a window it declared; an operator can clear any. " +
        "Answer: {status:\"cleared\", window_id, cleared_at_utc, previous:{cleared_at_utc:null}, undo:null, undo_note}. A cleared window is brought back by declaring a new one. " +
        "Needs mcp.allowWrites, or an agent token while mcp.allowAgentMuteRules is on.")]
    public static Task<string> ClearChangeWindow(
        NpgsqlDataSource postgres,
        [Description("The window id declare_change_window answered.")] long window_id,
        [Description("Why the window is cleared (1 to 2000 characters, at most 256 for an agent token, for example 'the work was cancelled').")] string reason,
        DarlingAgentActionSeat? agentSeat = null,
        InvestigationOpenSignal? signal = null,
        CancellationToken cancellationToken = default) =>
        ClearAsync(postgres, agentSeat, window_id, reason, DateTime.UtcNow, cancellationToken, signal);

    private static bool IsAgent(DarlingAgentActionSeat? seat) => seat is { Policy: { Writes: false, Investigations: true } };

    /// <summary>The journal actor and the window's <c>declared_by</c>: <c>agent:&lt;id&gt;</c> for an agent, else the operator's own identity, or <c>mcp</c> for a host with none.</summary>
    private static string ActorOf(DarlingAgentActionSeat? seat) => IsAgent(seat)
        ? "agent:" + DarlingMcpAgentMuteDirect.AgentIdOf(seat!)
        : seat?.Policy.OperatorActor ?? "mcp";

    private static string Refusal(string reason, string message) =>
        DarlingAgentActionGate.RefusalAnswer(new AgentActionRefusal(reason, message));

    private static string Iso(DateTime utc) => utc.ToString("o", CultureInfo.InvariantCulture);

    private static bool TryParseInstant(string? text, out DateTime utc)
    {
        if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var parsed))
        {
            utc = DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
            return true;
        }

        utc = default;
        return false;
    }

    /// <summary>Reason required, and at most <see cref="InvestigationToolCore.MaxActionReasonChars"/> characters.</summary>
    private static string? ReasonProblem(string? reason) =>
        string.IsNullOrWhiteSpace(reason) || reason.Trim().Length > InvestigationToolCore.MaxActionReasonChars
            ? Refusal(DarlingMcpAgentMuteDirect.ReasonCode,
                $"reason is required: 1 to {InvestigationToolCore.MaxActionReasonChars} characters saying why. It is kept in the agent change journal. Nothing was written.")
            : null;

    /// <summary>An agent's text fields are bounded like an agent mute rule's (<see cref="DarlingMcpAgentMuteDirect.MaxFieldChars"/>).</summary>
    private static string? FieldProblem(params (string Name, string? Value)[] fields)
    {
        foreach (var (name, value) in fields)
        {
            if (value is { } v && v.Length > DarlingMcpAgentMuteDirect.MaxFieldChars)
                return Refusal(DarlingMcpAgentMuteDirect.SizeCode,
                    $"{name} is longer than the {DarlingMcpAgentMuteDirect.MaxFieldChars} characters an agent may use here. Nothing was written.");
        }

        return null;
    }

    /// <summary>The tool body, so a test drives either arm. A null <paramref name="seat"/> is the writes arm on a host with no store owner source.</summary>
    internal static async Task<string> DeclareAsync(NpgsqlDataSource postgres, DarlingAgentActionSeat? seat, string[]? serverNames, string? startsAt,
        string? endsAt, string? reason, DateTime now, CancellationToken ct)
    {
        var agent = IsAgent(seat);
        /* A name given twice is one name, and a long list is refused before any name is resolved: each one is a registry read, and a refused call
           is not counted against the hourly cap. */
        var names = (serverNames ?? []).Select(n => (n ?? "").Trim()).Where(n => n.Length > 0).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        if (agent)
        {
            if (DarlingMcpAgentMuteDirect.SeatRefusal(seat!, OffMessage, NoOwnerMessage) is { } seatRefusal)
                return DarlingAgentActionGate.RefusalAnswer(seatRefusal);
            if (FieldProblem([.. names.Select((n, i) => ($"server_names[{i}]", (string?)n)), ("starts_at", startsAt), ("ends_at", endsAt), ("reason", reason?.Trim())]) is { } tooLong)
                return tooLong;
        }

        if (names.Count > MaxServerNames)
            return Refusal(TooManyNamesCode,
                $"server_names holds {names.Count} different names, and one change window may name at most {MaxServerNames}. Use the single word all for every server, or declare more than one window. Nothing was written.");

        if (ReasonProblem(reason) is { } noReason)
            return noReason;
        if (names.Count == 0)
            return DarlingMcpAlertTools.Outcome(InvalidStatus, "server_names is required: one server name, a list of names, or the single word all.");
        if (names.Count > 1 && names.Any(n => string.Equals(n, AllWord, StringComparison.OrdinalIgnoreCase)))
            return DarlingMcpAlertTools.Outcome(InvalidStatus, "Name servers, or give the single word all, not both.");
        if (!TryParseInstant(startsAt, out var start))
            return DarlingMcpAlertTools.Outcome(InvalidStatus, $"starts_at '{startsAt}' is not a valid ISO-8601 timestamp (e.g. 2027-08-01T02:00:00Z).");
        if (!TryParseInstant(endsAt, out var end))
            return DarlingMcpAlertTools.Outcome(InvalidStatus, $"ends_at '{endsAt}' is not a valid ISO-8601 timestamp (e.g. 2027-08-01T06:00:00Z).");
        if (agent && start - now > TimeSpan.FromDays(MaxAgentLeadDays))
            return Refusal(StartTooFarCode,
                $"An agent may declare a window that starts at most {MaxAgentLeadDays} days from now, and starts_at ({Iso(start)}) is later than that. Declare it closer to the work, or ask an operator. Nothing was written.");
        if (end <= start)
            return DarlingMcpAlertTools.Outcome(InvalidStatus, "ends_at must be after starts_at. Nothing was written.");
        if (end <= now)
            return DarlingMcpAlertTools.Outcome(InvalidStatus, $"ends_at ({Iso(end)}) is already past, so the window would hold nothing. Nothing was written.");
        if (end - start > TimeSpan.FromDays(MaxWindowDays))
            return DarlingMcpAlertTools.Outcome(InvalidStatus,
                $"A change window lasts at most {MaxWindowDays} days (ends_at minus starts_at). Declare a shorter window, or one window for each stretch of work. Nothing was written.");

        try
        {
            var all = names.Count == 1 && string.Equals(names[0], AllWord, StringComparison.OrdinalIgnoreCase);
            var servers = new List<(int ServerId, string Display)>();
            if (!all)
            {
                foreach (var name in names)
                {
                    var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, name, ct, NameClash.Refuse);
                    if (error is not null)
                        return error;
                    if (servers.All(s => s.ServerId != resolved.ServerId))
                        servers.Add((resolved.ServerId, resolved.DisplayName));
                }
            }

            var ids = servers.Select(s => s.ServerId).Order().ToArray();
            var actor = ActorOf(seat);
            await using var connection = await OpenAsync(postgres, seat, ct);
            await using var transaction = await connection.BeginTransactionAsync(ct);
            if (agent && await DarlingMcpAgentMuteDirect.GuardAsync(connection, transaction, now, DarlingMcpAgentMuteDirect.AgentIdOf(seat!), RateMessage, ct) is { } guard)
                return DarlingAgentActionGate.RefusalAnswer(guard);

            /* The twin check and the insert are one step: two declares started together (an operator has no hourly guard to queue behind)
               would both find no twin and both insert, and the second twin would keep holding requests after the first was cleared. */
            await using (var lockCommand = new NpgsqlCommand(DeclareLockSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds })
                await lockCommand.ExecuteNonQueryAsync(ct);

            if (await FindOpenTwinAsync(connection, transaction, all ? null : ids, start, end, now, ct) is { } twin)
                return AlreadyDeclared(twin);

            long id;
            await using (var insert = new NpgsqlCommand(InsertSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds })
            {
                insert.Parameters.Add(new NpgsqlParameter<int[]?> { TypedValue = all ? null : ids, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer });
                insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = start, NpgsqlDbType = NpgsqlDbType.TimestampTz });
                insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end, NpgsqlDbType = NpgsqlDbType.TimestampTz });
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = reason!.Trim(), NpgsqlDbType = NpgsqlDbType.Text });
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = actor, NpgsqlDbType = NpgsqlDbType.Text });
                insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now, NpgsqlDbType = NpgsqlDbType.TimestampTz });
                id = (long)(await insert.ExecuteScalarAsync(ct))!;
            }

            var answer = new JsonObject
            {
                ["status"] = "declared",
                ["window_id"] = id,
                ["all_servers"] = all,
                ["servers"] = new JsonArray([.. servers.Select(s => (JsonNode)new JsonObject { ["server_id"] = s.ServerId, ["server"] = s.Display })]),
                ["starts_at_utc"] = Iso(start),
                ["ends_at_utc"] = Iso(end),
                ["tail_ends_at_utc"] = Iso(end.AddMinutes(InvestigationChangeWindows.TailMinutes)),
                ["tail_minutes"] = InvestigationChangeWindows.TailMinutes,
                ["reason"] = reason.Trim(),
                ["declared_by"] = actor,
                ["undo"] = new JsonObject { ["tool"] = ClearToolName, ["arguments"] = new JsonObject { ["window_id"] = id } },
            };
            var json = answer.ToJsonString(McpHelpers.JsonOptions);
            await InvestigationRequestStore.JournalDirectChangeAsync(connection, transaction, now, ids.Length == 1 ? ids[0] : null, actor, reason.Trim(),
                DeclareToolName, AgentChangeKinds.ChangeWindow, id.ToString(CultureInfo.InvariantCulture), json, ct);
            await transaction.CommitAsync(ct);
            return json;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return McpHelpers.Status("precondition", PreconditionMessage);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(DeclareToolName, ex);
        }
    }

    private const string PreconditionMessage =
        "The service's store role may not write config.investigation_change_windows. A bring-your-own store needs " +
        DarlingManagedRoles.ProvisionRolesScriptPath + " re-run (it is idempotent) as a superuser. Managed and compose stores grant it themselves on start.";

    private const string InsertSql = @"
INSERT INTO config.investigation_change_windows (server_ids, starts_at, ends_at, reason, declared_by, declared_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING window_id";

    /* Serializes the twin check with the insert, for every caller. The key pair is this feature's own. */
    private const string DeclareLockSql = "SELECT pg_advisory_xact_lock(9485, 1)";

    /* A repeated declare (a client retry after a slow answer) must not stack a second identical window: the same server list, start and end,
       not cleared, whose tail has not ended. The list compares as a set because the insert stores it sorted. */
    private const string TwinSql = @"
SELECT window_id FROM config.investigation_change_windows
WHERE server_ids IS NOT DISTINCT FROM $1 AND starts_at = $2 AND ends_at = $3 AND cleared_at IS NULL
  AND ends_at + make_interval(mins => $4) > $5
ORDER BY window_id LIMIT 1";

    private static async Task<long?> FindOpenTwinAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int[]? ids, DateTime start, DateTime end,
        DateTime now, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(TwinSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
        command.Parameters.Add(new NpgsqlParameter<int[]?> { TypedValue = ids, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = start, NpgsqlDbType = NpgsqlDbType.TimestampTz });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = end, NpgsqlDbType = NpgsqlDbType.TimestampTz });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = InvestigationChangeWindows.TailMinutes, NpgsqlDbType = NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now, NpgsqlDbType = NpgsqlDbType.TimestampTz });
        return await command.ExecuteScalarAsync(ct) is long id ? id : null;
    }

    private static string AlreadyDeclared(long id) => JsonSerializer.Serialize(new
    {
        status = "already_declared",
        window_id = id,
        message = "An open window with the same servers, start and end exists. Nothing was written.",
    }, McpHelpers.JsonOptions);

    /// <summary>The owner's connection, which alone may write the table and the journal; a host with no owner source (a test host) uses the tool's own.</summary>
    private static async Task<NpgsqlConnection> OpenAsync(NpgsqlDataSource postgres, DarlingAgentActionSeat? seat, CancellationToken ct)
    {
        if (seat?.Owner?.Read() is { } owner)
            return await owner.OpenConnectionAsync(ct);
        return await postgres.OpenConnectionAsync(ct);
    }

    private const string ReadWindowSql = @"
SELECT server_ids, starts_at, ends_at, declared_by, cleared_at
FROM config.investigation_change_windows WHERE window_id = $1 FOR UPDATE";

    /* Releases every request the window holds, and journals one hold_released event for each (the once key keeps it to one), the event that
       brings a request into wait_for_investigation_requests and the route's delivery. $1 window, $2 now. Answers the route of each request released. */
    private static readonly string ReleaseOnClearSql = @"
WITH released AS (
    UPDATE config.investigation_requests
    SET hold_released_at = $2
    WHERE held_window_id = $1 AND hold_released_at IS NULL
    RETURNING request_id, route_id, server_id, first_metric
),
journaled AS (
    INSERT INTO config.investigation_journal (event_time, request_id, route_id, server_id, event, actor, reason, detail, once_key)
    SELECT $2, r.request_id, r.route_id, r.server_id, '" + InvestigationEvents.HoldReleased + @"', 'service', r.first_metric,
           jsonb_build_object('window_id', $1::bigint, 'metric', r.first_metric),
           'hold_released:' || r.request_id::text
    FROM released AS r
    ON CONFLICT (once_key) WHERE once_key IS NOT NULL DO NOTHING
)
SELECT route_id FROM released";

    private const string ClearSql = "UPDATE config.investigation_change_windows SET cleared_at = $2 WHERE window_id = $1";

    /// <summary>The tool body, so a test drives either arm.</summary>
    internal static async Task<string> ClearAsync(NpgsqlDataSource postgres, DarlingAgentActionSeat? seat, long windowId, string? reason, DateTime now,
        CancellationToken ct, InvestigationOpenSignal? signal = null)
    {
        var agent = IsAgent(seat);
        if (agent && DarlingMcpAgentMuteDirect.SeatRefusal(seat!, OffMessage, NoOwnerMessage) is { } seatRefusal)
            return DarlingAgentActionGate.RefusalAnswer(seatRefusal);
        if (agent && FieldProblem(("reason", reason?.Trim())) is { } tooLong)
            return tooLong;
        if (ReasonProblem(reason) is { } noReason)
            return noReason;

        try
        {
            var actor = ActorOf(seat);
            await using var connection = await OpenAsync(postgres, seat, ct);
            await using var transaction = await connection.BeginTransactionAsync(ct);
            if (agent && await DarlingMcpAgentMuteDirect.GuardAsync(connection, transaction, now, DarlingMcpAgentMuteDirect.AgentIdOf(seat!), RateMessage, ct) is { } guard)
                return DarlingAgentActionGate.RefusalAnswer(guard);

            int[]? serverIds;
            DateTime end;
            string declaredBy;
            DateTime? clearedAt;
            await using (var read = new NpgsqlCommand(ReadWindowSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds })
            {
                read.Parameters.Add(new NpgsqlParameter<long> { TypedValue = windowId, NpgsqlDbType = NpgsqlDbType.Bigint });
                await using var reader = await read.ExecuteReaderAsync(ct);
                if (!await reader.ReadAsync(ct))
                    return DarlingMcpAlertTools.Outcome("not_found", $"No change window with window_id {windowId}.");
                serverIds = reader.IsDBNull(0) ? null : reader.GetFieldValue<int[]>(0);
                end = reader.GetFieldValue<DateTime>(2);
                declaredBy = reader.GetString(3);
                clearedAt = reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTime>(4);
            }

            if (agent && !string.Equals(declaredBy, actor, StringComparison.Ordinal))
                return Refusal(NotYoursCode, "An agent clears only a change window it declared. Ask the operator, or the agent that declared it. Nothing was written.");
            if (clearedAt is { } already)
                return Settled("already_cleared", windowId, $"The window was cleared at {Iso(already)}. Nothing was written.");
            if (end.AddMinutes(InvestigationChangeWindows.TailMinutes) <= now)
                return Settled("already_closed", windowId, $"The window's tail ended at {Iso(end.AddMinutes(InvestigationChangeWindows.TailMinutes))}, so it holds nothing more. Nothing was written.");

            await using (var update = new NpgsqlCommand(ClearSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds })
            {
                update.Parameters.Add(new NpgsqlParameter<long> { TypedValue = windowId, NpgsqlDbType = NpgsqlDbType.Bigint });
                update.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now, NpgsqlDbType = NpgsqlDbType.TimestampTz });
                await update.ExecuteNonQueryAsync(ct);
            }

            /* The window now holds nothing, so what it still holds goes to the runner at once (the same release a late join makes). The routes
               woken are collected here and woken after the commit, so a waiter never reads a row that is not yet visible. */
            var releasedRoutes = new List<int>();
            await using (var release = new NpgsqlCommand(ReleaseOnClearSql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds })
            {
                release.Parameters.Add(new NpgsqlParameter<long> { TypedValue = windowId, NpgsqlDbType = NpgsqlDbType.Bigint });
                release.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now, NpgsqlDbType = NpgsqlDbType.TimestampTz });
                await using var released = await release.ExecuteReaderAsync(ct);
                while (await released.ReadAsync(ct))
                    releasedRoutes.Add(released.GetInt32(0));
            }

            var answer = new JsonObject
            {
                ["status"] = "cleared",
                ["window_id"] = windowId,
                ["cleared_at_utc"] = Iso(now),
                ["previous"] = new JsonObject { ["cleared_at_utc"] = null },
                ["undo"] = null,
                ["undo_note"] = "A cleared window is not restored by a call. Declare a new window with declare_change_window to hold requests again.",
            };
            var json = answer.ToJsonString(McpHelpers.JsonOptions);
            var before = new JsonObject { ["window_id"] = windowId, ["cleared_at_utc"] = null }.ToJsonString();
            await InvestigationRequestStore.JournalDirectChangeAsync(connection, transaction, now, serverIds is { Length: 1 } one ? one[0] : null, actor, reason!.Trim(),
                ClearToolName, AgentChangeKinds.ChangeWindow, windowId.ToString(CultureInfo.InvariantCulture), before, json, ct);
            await transaction.CommitAsync(ct);
            foreach (var route in releasedRoutes.Distinct())
                signal?.Opened(route);
            return json;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return McpHelpers.Status("precondition", PreconditionMessage);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(ClearToolName, ex);
        }
    }

    private static string Settled(string status, long windowId, string message) =>
        JsonSerializer.Serialize(new { status, window_id = windowId, message }, McpHelpers.JsonOptions);
}
