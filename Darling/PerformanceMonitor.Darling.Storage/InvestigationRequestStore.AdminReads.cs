/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>A request's state and close columns, as the investigation admin tools (#6400) report the row before and after a close.</summary>
public sealed record InvestigationRequestCloseState(Guid RequestId, string State, DateTime? ClosedAt, string? ClosedBy, string? ClosedReason);

/// <summary>A note's decision columns, as the investigation admin tools (#6400) report the row before and after a decision.</summary>
public sealed record InvestigationNoteState(
    long NoteId, string State, string Author, DateTime? DecidedAt, string? DecidedBy, DateTime? ExpiresAt, int? ServerId, string? MetricName);

public sealed partial class InvestigationRequestStore
{
    /// <summary>One request's state and close columns, or null when there is no such request.</summary>
    public async Task<InvestigationRequestCloseState?> ReadRequestCloseStateAsync(Guid requestId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null,
            "SELECT state, closed_at, closed_by, closed_reason FROM config.investigation_requests WHERE request_id = $1", P(requestId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new InvestigationRequestCloseState(
            requestId,
            reader.GetString(0),
            reader.IsDBNull(1) ? null : DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetString(3));
    }

    /// <summary>One note's decision columns, or null when there is no such note.</summary>
    public async Task<InvestigationNoteState?> ReadNoteStateAsync(long noteId, CancellationToken cancellationToken = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = Command(connection, null,
            "SELECT state, author, decided_at, decided_by, expires_at, server_id, metric_name FROM config.investigation_notes WHERE note_id = $1", P(noteId));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new InvestigationNoteState(
            noteId,
            reader.GetString(0),
            reader.GetString(1),
            reader.IsDBNull(2) ? null : DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            reader.IsDBNull(4) ? null : DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc),
            reader.IsDBNull(5) ? null : reader.GetInt32(5),
            reader.IsDBNull(6) ? null : reader.GetString(6));
    }
}
