/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// The RDS/Aurora stderr deadlock carry (#4735 item 4): a deadlock report that a chunk of the log ends inside is held
/// until the next chunk, so it is stored once, whole, instead of as the fragment the first chunk held.
///
/// <para><b>Why it is needed here and not on the self-hosted route.</b> <c>DownloadDBLogFilePortion</c> is
/// consume-once: the rest of a report cut at a chunk's end is never offered again. The pieces of it that a chunk
/// held used to be stored as a smaller deadlock (the DETAIL block takes however many continuation lines arrived), and
/// the rest was lost. The self-hosted <c>pg_read_file</c> route re-reads the end of the previous read, so it just skips the
/// unfinished report and reads it whole next time.</para>
///
/// <para><b>What is unfinished.</b> <see cref="PgDeadlockLogParser.UnfinishedTailStart"/>: the text ends inside the
/// last report, before the line that follows its DETAIL block (PostgreSQL writes a HINT there). A report that other
/// log lines follow is complete as it is, because no HINT will come for it.</para>
///
/// <para><b>It cannot be held forever.</b> A report still unfinished after one full extra chunk is stored as it is,
/// so is one that cannot be finished (the last chunk of a rotated file), and so is one larger than
/// <see cref="MaxCarryLength"/>. The pieces of the report that the chunks held are then stored as before.</para>
/// </summary>
internal static class RdsDeadlockCarry
{
    /// <summary>The most a held report may be, so an unfinished one cannot grow the carry without bound. The same bound
    /// the csvlog carry keeps for a partial record.</summary>
    internal const int MaxCarryLength = RdsCsvlogCarry.MaxCarryLength;

    /// <summary>
    /// What to store from a chunk and what to hold for the next.
    /// </summary>
    /// <param name="Text">The text to extract deadlocks from now.</param>
    /// <param name="Next">The report to hold for the next chunk, or empty.</param>
    /// <param name="StoredUnfinished">True when <paramref name="Text"/> ends in a report that was stored as it is
    /// because it could not be held any longer.</param>
    internal readonly record struct Portion(string Text, string Next, bool StoredUnfinished);

    /// <summary>
    /// One chunk through the carry. A pure step, so the rules are testable without a store or an RDS client.
    /// </summary>
    /// <param name="held">The report the previous chunk ended inside, or empty.</param>
    /// <param name="chunkText">The new chunk's text.</param>
    /// <param name="moreCanArrive">False when this chunk is the last of its file, so a report it ends inside can
    /// never be finished.</param>
    internal static Portion Step(string held, string? chunkText, bool moreCanArrive)
    {
        var fresh = chunkText ?? string.Empty;

        if (fresh.Length == 0)
        {
            /* Nothing new arrived. While the file can still grow, a held report has not been given its extra chunk and
               stays held. On the last chunk of a rotated file nothing will ever finish it, so it is stored as it is,
               exactly as it would have been had that chunk carried text (#4735). */
            return moreCanArrive || held.Length == 0
                ? new Portion(string.Empty, held, false)
                : new Portion(held, string.Empty, true);
        }

        var text = held + fresh;
        var start = PgDeadlockLogParser.UnfinishedTailStart(text);

        if (start < 0)
        {
            return new Portion(text, string.Empty, false);
        }

        /* Still unfinished when it started the text (it is the held one, and a full extra chunk did not finish it),
           when nothing more can arrive, or when it is too large to hold: stored as it is. */
        if ((held.Length > 0 && start == 0) || !moreCanArrive || text.Length - start > MaxCarryLength)
        {
            return new Portion(text, string.Empty, true);
        }

        return new Portion(text[..start], text[start..], false);
    }
}

/// <summary>
/// The per-instance bookkeeping for <see cref="RdsDeadlockCarry"/>, modelled on <see cref="RdsCsvlogCarryBook"/>: keyed
/// by the instance half of <see cref="RdsLogSource.ResumeMarker.Key"/> with the file name carried inside, so a
/// rotation to a new file starts fresh instead of gluing the new file's first bytes onto the old file's tail. The held
/// head stays in memory, and the saved resume marker (#4708) is not moved past a chunk while a report is held, so a
/// restart reads that chunk again rather than starting in the middle of the report. The carry only moves alongside the
/// marker's own commit. One book per ingestor, never shared.
/// </summary>
internal sealed class RdsDeadlockCarryBook
{
    private readonly Dictionary<string, (string Report, string? FileName)> _held = new(StringComparer.Ordinal);

    /// <summary>The report held for this chunk's file (empty for none), the key and file name the eventual
    /// <see cref="Commit"/> needs, and the report held for ANOTHER file of the instance (empty for none).
    ///
    /// <para>That last one can never be finished: the read has moved on to a different file without the held file's own
    /// last chunk, because RDS stopped listing the file. The caller stores it as it is, where it would otherwise be
    /// dropped without a count when <see cref="Commit"/> replaces the entry (#4735).</para></summary>
    public (string Held, string? CarryKey, string? FileName, string Abandoned) CarryFor(string? resumeKey)
    {
        var carryKey = RdsCsvlogCarryBook.InstanceKey(resumeKey);
        var fileName = RdsCsvlogCarryBook.ResumeFileName(resumeKey);

        if (!string.IsNullOrEmpty(carryKey) && _held.TryGetValue(carryKey, out var held))
        {
            return string.Equals(held.FileName, fileName, StringComparison.Ordinal)
                ? (held.Report, carryKey, fileName, string.Empty)
                : (string.Empty, carryKey, fileName, held.Report);
        }

        return (string.Empty, carryKey, fileName, string.Empty);
    }

    /// <summary>Holds <paramref name="next"/> for the next chunk of the same file, or clears the entry when it is empty.
    /// Called once, at the marker's own commit and only when it advanced.</summary>
    public void Commit(string? carryKey, string? fileName, string next)
    {
        if (string.IsNullOrEmpty(carryKey))
        {
            return;
        }

        if (next.Length == 0)
        {
            _held.Remove(carryKey);
        }
        else
        {
            _held[carryKey] = (next, fileName);
        }
    }
}
