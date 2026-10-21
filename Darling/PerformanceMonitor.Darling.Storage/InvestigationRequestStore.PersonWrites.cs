/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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

    /// <summary>The longest journal actor (the same bound the note decision holds).</summary>
    public const int ActorMaxLength = 128;

    /// <summary>The journal actor for a person: <c>person:&lt;login&gt;</c>, as the V184 journal records people.</summary>
    public static string PersonActor(string login) => "person:" + login.Trim();

    /// <summary>The login the actor names: not blank, at most 128 characters.</summary>
    public static string? ValidateLogin(string? login) =>
        string.IsNullOrWhiteSpace(login) ? "The viewer could not tell who you are, so it cannot record the change."
        : login.Trim().Length > ActorMaxLength ? "The login is longer than 128 characters."
        : null;

    /// <summary>A close reason: not blank, at most 500 characters.</summary>
    public static string? ValidateCloseReason(string? reason) =>
        string.IsNullOrWhiteSpace(reason) ? "Enter a reason for closing the request."
        : reason.Trim().Length > CloseReasonMaxLength
            ? string.Create(CultureInfo.InvariantCulture, $"The reason must be {CloseReasonMaxLength} characters or fewer.")
        : null;

    /// <summary>A note: not blank, at most 2,000 characters, kept 1 to 365 days (the V184 CHECK's twin, said in words).</summary>
    public static string? ValidateNote(string? noteText, int days) =>
        string.IsNullOrWhiteSpace(noteText) ? "Enter the note's text."
        : noteText.Trim().Length > NoteMaxLength
            ? string.Create(CultureInfo.InvariantCulture, $"A note must be {NoteMaxLength:N0} characters or fewer.")
        : days < 1 || days > NoteMaxDays
            ? string.Create(CultureInfo.InvariantCulture, $"A note is kept from 1 to {NoteMaxDays} days.")
        : null;
}

public sealed partial class InvestigationRequestStore
{
    /// <summary>Closes a request still in play or reported. $1 request, $2 actor, $3 reason, $4 now. RETURNING the server for the
    /// journal row; no row when the request is gone, already closed or expired.</summary>
    internal const string PersonCloseSql = @"
UPDATE config.investigation_requests
SET state = 'closed', closed_at = $4, closed_by = $2, closed_reason = $3
WHERE request_id = $1 AND state IN ('open', 'claimed', 'reported')
RETURNING server_id";

    /// <summary>A note a person writes: approved as written, by its author. $1 server (or NULL for the fleet), $2 metric (or NULL),
    /// $3 text, $4 author, $5 lifetime in days, $6 now.</summary>
    internal const string PersonNoteInsertSql = @"
INSERT INTO config.investigation_notes
    (server_id, metric_name, note_text, author, state, created_at, expires_at, decided_at, decided_by)
VALUES ($1, $2, $3, $4, 'approved', $6, $6 + make_interval(days => $5), $6, $4)
RETURNING note_id";

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
        if (InvestigationPersonWrites.ValidateLogin(actor) is { } actorError)
        {
            throw new ArgumentException(actorError, nameof(actor));
        }

        if (InvestigationPersonWrites.ValidateCloseReason(reason) is { } reasonError)
        {
            throw new ArgumentException(reasonError, nameof(reason));
        }

        actor = actor.Trim();
        reason = reason.Trim();
        var now = Now();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        object? serverId;
        await using (var close = Command(connection, transaction, PersonCloseSql, P(requestId), P(actor), P(reason), P(now)))
        {
            serverId = await close.ExecuteScalarAsync(cancellationToken);
        }

        if (serverId is null or DBNull)
        {
            return false;
        }

        await JournalAsync(connection, transaction, now, requestId, null, Convert.ToInt32(serverId, CultureInfo.InvariantCulture),
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
        var error = InvestigationPersonWrites.ValidateLogin(actor) ?? InvestigationPersonWrites.ValidateNote(noteText, days);
        if (error is not null)
        {
            throw new ArgumentException(error, nameof(noteText));
        }

        actor = actor.Trim();
        var metric = string.IsNullOrWhiteSpace(metricName) ? null : metricName.Trim();
        var now = Now();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        long noteId;
        await using (var insert = Command(connection, transaction, PersonNoteInsertSql,
            new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer },
            PNullable(metric), P(noteText.Trim()), P(actor), P(days), P(now)))
        {
            noteId = Convert.ToInt64(await insert.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        }

        await JournalAsync(connection, transaction, now, null, null, serverId, "note_written", actor,
            "note " + noteId.ToString(CultureInfo.InvariantCulture), null, null, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return noteId;
    }
}
