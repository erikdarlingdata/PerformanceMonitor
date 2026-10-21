/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The rules of the two writes a person makes on the investigation tables (#6265, #5632): closing a request and writing a note.
/// One home for the desktop Viewer and the web routes, so the bounds and the words are the same on both.
/// </summary>
public static class InvestigationPersonWrites
{
    /// <summary>The longest close reason.</summary>
    public const int CloseReasonMaxLength = 500;

    /// <summary>The longest note a person may write; the V184 CHECK's bound.</summary>
    public const int NoteMaxLength = 2000;

    /// <summary>A written note's default lifetime in days.</summary>
    public const int NoteDefaultDays = 90;

    /// <summary>A written note's longest lifetime in days.</summary>
    public const int NoteMaxDays = 365;

    /// <summary>The longest journal actor: the prefix and the longest name the undo path's person shape holds.</summary>
    public const int ActorMaxLength = 7 + AgentChangeUndo.LoginMaxLength;

    /// <summary>The longest metric name a note may carry.</summary>
    public const int MetricNameMaxLength = 128;

    /// <summary>The prefix every person's journal actor starts with.</summary>
    public const string PersonActorPrefix = "person:";

    /// <summary>The journal actor for a person: <c>person:&lt;login&gt;</c>, as the V184 journal records people. Shapes the name only;
    /// <see cref="ValidateActor"/> says whether the result may be written.</summary>
    public static string PersonActor(string login) => PersonActorPrefix + AgentChangeUndo.NormalizeLogin(login);

    /// <summary>The login the actor names: not blank, and the same shape the undo path holds a person's name to (<see cref="AgentChangeUndo.IsValidLogin"/>),
    /// which refuses control, format, bidi and zero-width characters and odd whitespace in a name.</summary>
    public static string? ValidateLogin(string? login)
    {
        if (string.IsNullOrWhiteSpace(login))
        {
            return "The viewer could not tell who you are, so it cannot record the change.";
        }

        return AgentChangeUndo.IsValidLogin(AgentChangeUndo.NormalizeLogin(login)) ? null : AgentChangeUndo.LoginRefusal;
    }

    /// <summary>The storage guard: a <c>person:&lt;login&gt;</c> actor built by <see cref="PersonActor"/> from a valid login, and nothing
    /// else, so no caller can write an approved note or a close as an agent or the service.</summary>
    public static string? ValidateActor(string? actor)
    {
        const string notAPerson = "Only a person can make this change: the actor must be person:<login>.";
        if (actor is null || !actor.StartsWith(PersonActorPrefix, StringComparison.Ordinal))
        {
            return notAPerson;
        }

        var login = actor[PersonActorPrefix.Length..];
        return ValidateLogin(login) ?? (PersonActor(login) == actor ? null : notAPerson);
    }

    /// <summary>A close reason: not blank, at most 500 characters.</summary>
    public static string? ValidateCloseReason(string? reason) =>
        string.IsNullOrWhiteSpace(reason) ? "Enter a reason for closing the request."
        : reason.Trim().Length > CloseReasonMaxLength
            ? string.Create(CultureInfo.InvariantCulture, $"The reason must be {CloseReasonMaxLength} characters or fewer.")
        : null;

    /// <summary>A note: not blank, at most 2,000 characters, kept 1 to 365 days (the V184 CHECK's twin, said in words), and a metric
    /// name (when given) of at most 128 characters with no control characters.</summary>
    public static string? ValidateNote(string? noteText, int days, string? metricName = null) =>
        string.IsNullOrWhiteSpace(noteText) ? "Enter the note's text."
        : noteText.Trim().Length > NoteMaxLength
            ? string.Create(CultureInfo.InvariantCulture, $"A note must be {NoteMaxLength:N0} characters or fewer.")
        : days < 1 || days > NoteMaxDays
            ? string.Create(CultureInfo.InvariantCulture, $"A note is kept from 1 to {NoteMaxDays} days.")
        : metricName is not null && metricName.Trim().Length > MetricNameMaxLength
            ? string.Create(CultureInfo.InvariantCulture, $"The metric name must be {MetricNameMaxLength} characters or fewer.")
        : metricName is not null && metricName.Any(char.IsControl) ? "The metric name cannot hold control characters."
        : null;
}

public sealed partial class InvestigationRequestStore
{
    /// <summary>Closes a request still in play or reported. $1 request, $2 actor, $3 reason. The store's own clock (now()), as the Viewer's close always
    /// used, never the caller's. RETURNING the server and the time for the journal row; no row when the request is gone, already closed or expired.</summary>
    internal const string PersonCloseSql = @"
UPDATE config.investigation_requests
SET state = 'closed', closed_at = now(), closed_by = $2, closed_reason = $3
WHERE request_id = $1 AND state IN ('open', 'claimed', 'reported')
RETURNING server_id, closed_at";

    /// <summary>A note a person writes: approved as written, by its author. $1 server (or NULL for the fleet), $2 metric (or NULL),
    /// $3 text, $4 author, $5 lifetime in days. Times are the store's clock (now()).</summary>
    internal const string PersonNoteInsertSql = @"
INSERT INTO config.investigation_notes
    (server_id, metric_name, note_text, author, state, created_at, expires_at, decided_at, decided_by)
VALUES ($1, $2, $3, $4, 'approved', now(), now() + make_interval(days => $5), now(), $4)
RETURNING note_id, created_at";

    /// <summary>True when the registry holds the server. A note written for a server that is not registered would never reach a brief.</summary>
    public async Task<bool> ServerRegisteredAsync(int serverId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null, "SELECT 1 FROM collect.servers WHERE server_id = $1", P(serverId));
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    /// <summary>
    /// Closes a request with a reason and journals <c>closed</c> against <paramref name="actor"/> (<c>person:&lt;login&gt;</c>), in one
    /// transaction. False when the request is gone, already closed or expired (nothing is written then). Throws
    /// <see cref="ArgumentException"/> for a blank or over-long actor or reason. A login that may not write throws
    /// <see cref="PostgresException"/> (42501).
    /// </summary>
    public async Task<bool> CloseRequestAsync(Guid requestId, string actor, string reason, CancellationToken cancellationToken = default)
    {
        if (InvestigationPersonWrites.ValidateActor(actor) is { } actorError)
        {
            throw new ArgumentException(actorError, nameof(actor));
        }

        if (InvestigationPersonWrites.ValidateCloseReason(reason) is { } reasonError)
        {
            throw new ArgumentException(reasonError, nameof(reason));
        }

        reason = reason.Trim();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        int serverId;
        DateTime now;
        await using (var close = Command(connection, transaction, PersonCloseSql, P(requestId), P(actor), P(reason)))
        await using (var reader = await close.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
            {
                return false;
            }

            serverId = reader.GetInt32(0);
            now = reader.GetFieldValue<DateTime>(1);
        }

        await JournalAsync(connection, transaction, now, requestId, null, serverId,
            "closed", actor, reason, null, null, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    /// <summary>
    /// Writes a note as a person: approved as written, author <paramref name="actor"/> (<c>person:&lt;login&gt;</c>), expiring after
    /// <paramref name="days"/> (90 by default, 365 at most). Journals <c>note_written</c>. Returns the new note's id. Throws
    /// <see cref="ArgumentException"/> for a value the rules refuse. A login that may not write throws <see cref="PostgresException"/> (42501).
    /// </summary>
    public async Task<long> WriteNoteAsync(
        int? serverId, string? metricName, string noteText, string actor, int days = InvestigationPersonWrites.NoteDefaultDays,
        CancellationToken cancellationToken = default)
    {
        var error = InvestigationPersonWrites.ValidateActor(actor) ?? InvestigationPersonWrites.ValidateNote(noteText, days, metricName);
        if (error is not null)
        {
            throw new ArgumentException(error, nameof(noteText));
        }

        var metric = string.IsNullOrWhiteSpace(metricName) ? null : metricName.Trim();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        long noteId;
        DateTime now;
        await using (var insert = Command(connection, transaction, PersonNoteInsertSql,
            new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer },
            PNullable(metric), P(noteText.Trim()), P(actor), P(days)))
        await using (var reader = await insert.ExecuteReaderAsync(cancellationToken))
        {
            await reader.ReadAsync(cancellationToken);
            noteId = reader.GetInt64(0);
            now = reader.GetFieldValue<DateTime>(1);
        }

        await JournalAsync(connection, transaction, now, null, null, serverId, "note_written", actor,
            "note " + noteId.ToString(CultureInfo.InvariantCulture), null, null, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return noteId;
    }
}
