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
/// takes it on when its screen does: Darling's PostgreSQL <c>InvestigationRequestStore</c> and Lite's DuckDB store.
/// The brief composer reads approved notes only; a rejected note stays stored with who rejected it and when.
/// </summary>
public interface IInvestigationNoteStore
{
    /// <summary>The pending notes that have not expired, oldest proposal first, at most <paramref name="limit"/>. A note is born
    /// with a 90-day <c>expires_at</c> and the brief composer drops one past it, so approving an expired note would do nothing:
    /// an expired pending note is not listed (#5903).</summary>
    Task<IReadOnlyList<PendingInvestigationNote>> ListPendingNotesAsync(int limit = 200, CancellationToken cancellationToken = default);

    /// <summary>Approves a pending note and stamps <paramref name="decidedBy"/> and the time. False when the note is gone or
    /// already decided, in which case nothing changes. Throws <see cref="ArgumentException"/> when <paramref name="decidedBy"/>
    /// fails <see cref="InvestigationNoteRules.ValidateDecidedBy"/>.</summary>
    Task<bool> ApproveNoteAsync(long noteId, string decidedBy, CancellationToken cancellationToken = default);

    /// <summary>Rejects a pending note and stamps <paramref name="decidedBy"/> and the time. The note stays stored and never
    /// reaches a brief. False when the note is gone or already decided, in which case nothing changes.</summary>
    Task<bool> RejectNoteAsync(long noteId, string decidedBy, CancellationToken cancellationToken = default);
}

/// <summary>The one rule on who a note decision names, shared by the Darling store (and so the desktop Viewer and the web
/// routes) and Lite's store, so no surface can stamp a <c>decided_by</c> another refuses (#5903).</summary>
public static class InvestigationNoteRules
{
    /// <summary>The longest <c>decided_by</c> a decision may carry.</summary>
    public const int MaxDecidedByLength = 128;

    /// <summary>Null when <paramref name="decidedBy"/> is 1 to <see cref="MaxDecidedByLength"/> characters once trimmed,
    /// else a plain sentence that says why a decision naming it is refused.</summary>
    public static string? ValidateDecidedBy(string? decidedBy) =>
        string.IsNullOrWhiteSpace(decidedBy) ? "A note decision must say who made it."
        : decidedBy.Trim().Length > MaxDecidedByLength
            ? "The name on a note decision is longer than " + MaxDecidedByLength.ToString(System.Globalization.CultureInfo.InvariantCulture) + " characters."
        : null;
}

/// <summary>One pending note for the approve screen. <see cref="ServerName"/>, <see cref="Family"/> and
/// <see cref="SourceRequestId"/> are null for a note with no request behind it. <see cref="ExpiresAtUtc"/> is when the note
/// stops reaching a brief.</summary>
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
    DateTime ProposedAtUtc,
    DateTime ExpiresAtUtc);
