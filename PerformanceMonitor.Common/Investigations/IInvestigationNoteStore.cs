/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Common;

/// <summary>
/// The note decisions of Level 1 investigations (#5903): an agent proposes a note with its report and it waits as pending
/// until a person approves or rejects it. Kept apart from <see cref="IInvestigationRequestStore"/> so each product's store
/// takes it on when its screen does: Darling's PostgreSQL <c>InvestigationRequestStore</c> now and Lite's DuckDB store later.
/// The brief composer reads approved notes only; a rejected note stays stored with who rejected it and when.
/// </summary>
public interface IInvestigationNoteStore
{
    /// <summary>The pending notes, oldest proposal first, at most <paramref name="limit"/>.</summary>
    Task<IReadOnlyList<PendingInvestigationNote>> ListPendingNotesAsync(int limit = 200, CancellationToken cancellationToken = default);

    /// <summary>Approves a pending note and stamps <paramref name="decidedBy"/> and the time. False when the note is gone or
    /// already decided, in which case nothing changes.</summary>
    Task<bool> ApproveNoteAsync(long noteId, string decidedBy, CancellationToken cancellationToken = default);

    /// <summary>Rejects a pending note and stamps <paramref name="decidedBy"/> and the time. The note stays stored and never
    /// reaches a brief. False when the note is gone or already decided, in which case nothing changes.</summary>
    Task<bool> RejectNoteAsync(long noteId, string decidedBy, CancellationToken cancellationToken = default);
}

/// <summary>One pending note for the approve screen. <see cref="ServerName"/>, <see cref="Family"/> and
/// <see cref="SourceRequestId"/> are null for a note with no request behind it.</summary>
public sealed record PendingInvestigationNote(
    long NoteId,
    int? ServerId,
    string? ServerName,
    string? MetricName,
    string? Family,
    string? StoryPathHash,
    string NoteText,
    string Author,
    Guid? SourceRequestId,
    DateTime ProposedAtUtc);
