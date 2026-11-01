/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The investigation admin writes over MCP (#6400): <c>write_investigation_note</c>, <c>approve_investigation_note</c>,
/// <c>reject_investigation_note</c> and <c>close_investigation_request</c>. Each calls the store method the web route and the Viewer call
/// (<see cref="InvestigationRequestStore.WriteNoteAsync"/>, <see cref="InvestigationRequestStore.ApproveNoteAsync"/>,
/// <see cref="InvestigationRequestStore.RejectNoteAsync"/>, <see cref="InvestigationRequestStore.CloseRequestAsync"/>) with the
/// same validation (<see cref="InvestigationPersonWrites"/>), so the states, the refusals and the journal rows are the same.
/// Served only with <c>mcp.allowWrites</c> and a token, and on <see cref="DarlingMcpWritePolicy.NeverAgentTools"/>: an agent that
/// investigates an alert proposes a note through <c>submit_investigation_report</c>, and can never approve its own note, write one
/// as approved or close a request. They run as the <c>mcp</c> role, which holds only the columns these statements set.
/// <para>The decision and the close are recorded as every person write is (<c>person:&lt;name&gt;</c>), the name being the MCP caller:
/// <c>person:mcp</c> and the token by name (<c>named token NAME</c>) or as <c>legacy token</c> (<see cref="ReadCaller.StoreTokenIdentity"/>), which an
/// operator can trace to the token in the log line each write also leaves with the client's own name
/// (<see cref="ReadCaller.WriteIdentity"/>).</para>
/// <para>None of the four has an undo: the web and the Viewer have no way to reopen a request, return a decided note to pending or
/// delete a note, so the answers say so rather than offer a call that does not exist.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpInvestigationAdminTools
{
    internal const string PreconditionMessage =
        "The service's mcp store role may not write the investigation tables this tool changes. A bring-your-own store needs " +
        DarlingManagedRoles.ProvisionRolesScriptPath + " re-run (it is idempotent) as a superuser. Managed and compose stores grant it themselves on start.";

    private const string NotPendingSentence = "That note is not pending: it was already decided, or it is gone.";
    private const string NotOpenSentence = "That request is not open: it is already closed or expired, or it is gone.";

    [McpServerTool(Name = "write_investigation_note"), Description(
        "Writes an investigation note as a person: approved as written, so it reaches the briefs of the next investigations at once (the web's Write a note). Changes the monitoring store only. " +
        "<<GUIDE>>" +
        "note_text is 1 to 2000 characters. server_name (optional) scopes the note to one server; leave it out for a fleet note. metric_name (optional, up to 128 characters) ties it to one metric. " +
        "days is how long it is kept: 1 to 365, default 90. An agent's own proposed note is not this: it waits for approve_investigation_note or reject_investigation_note. " +
        "Answer: {status:\"written\", note_id, author, server, metric_name, days, note: {before: null, after: {...}}, undo: null}. There is no undo: a written note cannot be deleted from the web or the Viewer; " +
        "reject_investigation_note applies only to a pending note. Needs mcp.allowWrites; an agent investigation can never call it.")]
    public static async Task<string> WriteInvestigationNote(
        NpgsqlDataSource postgres,
        [Description("The note, 1 to 2000 characters.")] string note_text,
        [Description("Server name (the name list_servers shows), or a unique part of it. Leave out for a note that applies to the whole fleet.")] string? server_name = null,
        [Description("Optional metric the note is about (up to 128 characters).")] string? metric_name = null,
        [Description("Days the note is kept, 1 to 365. Default 90.")] int days = InvestigationPersonWrites.NoteDefaultDays,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (InvestigationPersonWrites.ValidateNote(note_text, days, metric_name) is { } noteRefusal)
            {
                return McpHelpers.Refusal(NoteParameter(note_text, days), noteRefusal + " Nothing was changed.");
            }

            var actor = Actor();
            if (InvestigationPersonWrites.ValidateActor(actor) is { } actorRefusal)
            {
                return McpHelpers.Refusal("note_text", actorRefusal + " Nothing was changed.");
            }

            int? serverId = null;
            string? serverDisplay = null;
            if (!string.IsNullOrWhiteSpace(server_name))
            {
                var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
                if (error is not null)
                {
                    return error;
                }

                serverId = resolved.ServerId;
                serverDisplay = resolved.DisplayName;
            }

            var store = Store(postgres);
            var noteId = await store.WriteNoteAsync(serverId, metric_name, note_text, actor, days, cancellationToken);
            var after = await store.ReadNoteStateAsync(noteId, cancellationToken);
            logger?.LogInformation("MCP write write_investigation_note by {Identity}: note {NoteId} {Server}", ReadCaller.WriteIdentity(), noteId, serverDisplay ?? "(fleet)");
            return JsonSerializer.Serialize(new
            {
                status = "written",
                note_id = noteId,
                author = actor,
                server = serverDisplay,
                metric_name = string.IsNullOrWhiteSpace(metric_name) ? null : metric_name.Trim(),
                days,
                note = new { before = (object?)null, after = NoteImage(after) },
                undo = (object?)null,
                undo_note = "There is no undo: a written note cannot be deleted from the web or the Viewer.",
            }, McpHelpers.JsonOptions);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return McpHelpers.Status("precondition", PreconditionMessage);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("write_investigation_note", ex);
        }
    }

    [McpServerTool(Name = "approve_investigation_note"), Description(
        "Approves a pending agent note so it reaches the briefs of the next investigations (the web's Approve on the pending notes screen). Changes the monitoring store only. " +
        "<<GUIDE>>" +
        "note_id is a pending note's id from the pending agent notes screen or the submit_investigation_report answer. Only a pending note can be approved: a note that is already approved or rejected, or not there, is refused and nothing is written. " +
        "Answer: {status:\"approved\", note: {before, after}, undo: null}; before and after are the note's state, decided_by and decided_at. There is no undo: a decided note cannot return to pending. " +
        "Needs mcp.allowWrites; an agent investigation can never call it.")]
    public static Task<string> ApproveInvestigationNote(
        NpgsqlDataSource postgres,
        [Description("The note's id.")] long note_id,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        DecideAsync(postgres, note_id, approve: true, logger, cancellationToken);

    [McpServerTool(Name = "reject_investigation_note"), Description(
        "Rejects a pending agent note so it never reaches a brief (the web's Reject on the pending notes screen). Changes the monitoring store only. " +
        "<<GUIDE>>" +
        "note_id is a pending note's id. Only a pending note can be rejected: a note that is already approved or rejected, or not there, is refused and nothing is written. " +
        "Answer: {status:\"rejected\", note: {before, after}, undo: null}; before and after are the note's state, decided_by and decided_at. There is no undo: a decided note cannot return to pending. " +
        "Needs mcp.allowWrites; an agent investigation can never call it.")]
    public static Task<string> RejectInvestigationNote(
        NpgsqlDataSource postgres,
        [Description("The note's id.")] long note_id,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        DecideAsync(postgres, note_id, approve: false, logger, cancellationToken);

    [McpServerTool(Name = "close_investigation_request"), Description(
        "Closes an open, claimed or reported investigation request with a reason, so no agent works it further (the web's Close). Changes the monitoring store only. " +
        "<<GUIDE>>" +
        "request_id is the request's id from list_investigation_requests. reason is 1 to 500 characters. A request that is already closed or expired, or not there, is refused and nothing is written. " +
        "Answer: {status:\"closed\", request: {before, after}, undo: null}; before and after are the request's state, closed_by, closed_at and closed_reason. There is no undo: a closed request cannot be reopened. " +
        "Needs mcp.allowWrites; an agent investigation can never call it.")]
    public static async Task<string> CloseInvestigationRequest(
        NpgsqlDataSource postgres,
        [Description("The request's id (a GUID), from list_investigation_requests.")] string request_id,
        [Description("Why it is closed, 1 to 500 characters.")] string reason,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            if (!Guid.TryParse(request_id, out var request))
            {
                return McpHelpers.Refusal("request_id", "The request id is not a request id. Nothing was changed.");
            }

            if (InvestigationPersonWrites.ValidateCloseReason(reason) is { } reasonRefusal)
            {
                return McpHelpers.Refusal("reason", reasonRefusal + " Nothing was changed.");
            }

            var actor = Actor();
            if (InvestigationPersonWrites.ValidateActor(actor) is { } actorRefusal)
            {
                return McpHelpers.Refusal("reason", actorRefusal + " Nothing was changed.");
            }

            var store = Store(postgres);
            var before = await store.ReadRequestCloseStateAsync(request, cancellationToken);
            if (!await store.CloseRequestAsync(request, actor, reason, cancellationToken))
            {
                return McpHelpers.Refusal("request_id", NotOpenSentence + " Nothing was changed.");
            }

            var after = await store.ReadRequestCloseStateAsync(request, cancellationToken);
            logger?.LogInformation("MCP write close_investigation_request by {Identity}: request {RequestId}", ReadCaller.WriteIdentity(), request);
            return JsonSerializer.Serialize(new
            {
                status = "closed",
                request_id = request.ToString(),
                closed_by = actor,
                request = new { before = RequestImage(before), after = RequestImage(after) },
                undo = (object?)null,
                undo_note = "There is no undo: a closed request cannot be reopened.",
            }, McpHelpers.JsonOptions);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return McpHelpers.Status("precondition", PreconditionMessage);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("close_investigation_request", ex);
        }
    }

    private static async Task<string> DecideAsync(NpgsqlDataSource postgres, long noteId, bool approve, ILogger? logger, CancellationToken ct)
    {
        var tool = approve ? "approve_investigation_note" : "reject_investigation_note";
        try
        {
            var actor = Actor();
            if (InvestigationPersonWrites.ValidateActor(actor) is { } actorRefusal)
            {
                return McpHelpers.Refusal("note_id", actorRefusal + " Nothing was changed.");
            }

            var store = Store(postgres);
            var before = await store.ReadNoteStateAsync(noteId, ct);
            if (!await store.DecideNoteAsync(noteId, approve, actor, ct))
            {
                return McpHelpers.Refusal("note_id", NotPendingSentence + " Nothing was changed.");
            }

            var after = await store.ReadNoteStateAsync(noteId, ct);
            logger?.LogInformation("MCP write {Tool} by {Identity}: note {NoteId}", tool, ReadCaller.WriteIdentity(), noteId);
            return JsonSerializer.Serialize(new
            {
                status = approve ? "approved" : "rejected",
                note_id = noteId,
                decided_by = actor,
                note = new { before = NoteImage(before), after = NoteImage(after) },
                undo = (object?)null,
                undo_note = "There is no undo: a decided note cannot return to pending.",
            }, McpHelpers.JsonOptions);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return McpHelpers.Status("precondition", PreconditionMessage);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(tool, ex);
        }
    }

    private static InvestigationRequestStore Store(NpgsqlDataSource postgres) =>
        new(postgres, McpCommandDeadlines.ReadSeconds, () => DateTime.UtcNow);

    /// <summary>The person actor the store records: <c>person:mcp</c> and the token by name or as <c>legacy token</c>, or <c>person:mcp</c>
    /// alone when that identity is not a name the person rule accepts. Never the client's own claimed name, which any client can send.</summary>
    internal static string Actor()
    {
        var named = InvestigationPersonWrites.PersonActor("mcp " + ReadCaller.StoreTokenIdentity());
        return InvestigationPersonWrites.ValidateActor(named) is null ? named : InvestigationPersonWrites.PersonActor("mcp");
    }

    private static string NoteParameter(string? noteText, int days) =>
        days is < 1 or > InvestigationPersonWrites.NoteMaxDays && !string.IsNullOrWhiteSpace(noteText) ? "days" : "note_text";

    private static object? NoteImage(InvestigationNoteState? note) =>
        note is null ? null : new
        {
            state = note.State,
            author = note.Author,
            decided_by = note.DecidedBy,
            decided_at = Utc(note.DecidedAt),
            expires_at = Utc(note.ExpiresAt),
        };

    private static object? RequestImage(InvestigationRequestCloseState? request) =>
        request is null ? null : new
        {
            state = request.State,
            closed_by = request.ClosedBy,
            closed_at = Utc(request.ClosedAt),
            closed_reason = request.ClosedReason,
        };

    private static string? Utc(DateTime? value) =>
        value?.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture);
}
