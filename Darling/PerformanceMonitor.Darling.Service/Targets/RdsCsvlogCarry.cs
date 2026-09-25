/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// The RDS/Aurora csvlog partial-record carry (#4053 part c1, review rounds 1 and 2), extracted from
/// <see cref="RdsLogEventIngestor"/> so <see cref="RdsDeadlockIngestor"/> can read the same <c>.csv</c> file
/// through it (#4053 part c2). Moved, not changed: every rule below — forward-only parity, the never-trust-
/// a-file's-end read, the 1 MiB bound with parity-preserving skip, and the RDS truncation-notice downgrade —
/// went through two security review rounds against untrusted field content while it lived on the log-events
/// ingestor, and this file is that design at its original behaviour, given a name two callers can share.
/// </summary>
internal static class RdsCsvlogCarry
{
    /// <summary>
    /// The bound on a carried partial record (#4053 part c1): a single record straddling more than this
    /// many chars across a portion boundary is dropped rather than grown without limit across cycles — one
    /// oversized record must not turn into unbounded memory growth if the file never gives it a closing
    /// boundary.
    /// </summary>
    internal const int MaxCarryLength = 1_048_576;

    /// <summary>
    /// The notice AWS's own <c>DownloadDBLogFilePortion</c> appends when a 1 MB portion cap cuts a line
    /// mid-write (#4053 review round 2, item 1; public reports aws/aws-sdk#20, aws/aws-cli#2268): bytes are
    /// missing after this point, so forward mode's byte-for-byte parity assumption no longer holds for the
    /// rest of this portion.
    /// </summary>
    internal const string RdsTruncationNotice = "[Your log message was truncated]";

    /// <summary>A carried partial csvlog record and what's known about resuming it (#4053 part c1, review round 1).
    /// <see cref="StartKnown"/> is true ONLY when the partial starts on a record boundary this route actually
    /// walked forward from — never inferred from a file's end, which the multi-write race can lie about.
    /// <see cref="Skipping"/> is the parity-bound state (#4053 review round 1): an oversized record was
    /// dropped, but instead of losing <see cref="StartKnown"/> outright the route remembers whether the drop
    /// left an open quote (<see cref="SkipInQuotes"/>) and keeps scanning forward for the first newline
    /// outside quotes — the record's own true end — before resuming with a known start again.</summary>
    internal readonly record struct CsvCarry(string Partial, bool StartKnown, bool Skipping = false, bool SkipInQuotes = false, string? FileName = null)
    {
        public static CsvCarry Empty => new(string.Empty, false);
    }

    /// <summary>What one csvlog portion parses to, and the carry for the next portion (#4053 part c1).</summary>
    internal readonly record struct CsvPortion(List<PgLogEntry> Entries, int RecordsDiscarded, CsvCarry Next);

    /// <summary>
    /// One csvlog portion through the parser, with the carry (#4053 part c1; forward-only after review round
    /// 1). A pure step, so the carry rules are testable without a store.
    ///
    /// <para><b>The only known start is offset 0 of a file, and a file's end is never trusted.</b> The old
    /// design let a portion that reached the file's current end hand the NEXT portion a known start — but
    /// the syslogger can write one record across several writes, so a read can land between two of them at a
    /// newline that is still inside an open quoted field. Trusting that as a boundary inverts the parity of
    /// every later forward walk, and the old resync check (kept &lt; discarded, then re-parse) was gameable: a
    /// client that plants enough look-alike lines inside a quoted field can make the inverted walk keep more
    /// than it discards. So this step never states <see cref="PgServerLogCsvParser.CsvBodyEdges.EndsOnRecordBoundary"/>
    /// and there is no resync trigger to game.</para>
    ///
    /// <para><b>Forward mode</b> (<see cref="CsvCarry.StartKnown"/> or <see cref="CsvCarry.Skipping"/>): every
    /// portion of the file is parsed with <see cref="PgServerLogCsvParser.CsvBodyEdges.StartsOnRecordBoundary"/>
    /// ONLY, whether or not more data is pending. The text after the last true boundary found is ALWAYS
    /// carried — a half-written last record simply waits for the portion that completes it — and the next
    /// carry's start is known again, because <c>consumedLength</c> under a forward walk is a true boundary by
    /// construction.</para>
    ///
    /// <para><b>Unknown-start mode</b> (first contact, via the tail read): parsed with
    /// <see cref="PgServerLogCsvParser.CsvBodyEdges.None"/>, or <c>EndsOnRecordBoundary</c> when no more data
    /// is pending AND the body ends in <c>'\n'</c> — never inferred from a body that merely stops pending with
    /// no trailing newline, which is not the same fact. The next carry's start is ALWAYS unknown in this mode;
    /// it can never become known until the next file (see <see cref="RdsLogSource.LogChunk.StartsAtFileStart"/>).</para>
    ///
    /// <para><b>The forward-mode bound</b> (#4053 review round 1): a carry over <see cref="MaxCarryLength"/>
    /// keeps the PARITY instead of dropping <see cref="CsvCarry.StartKnown"/> outright — the text is dropped,
    /// but <see cref="CsvCarry.Skipping"/> and <see cref="CsvCarry.SkipInQuotes"/> (whether the dropped text
    /// left an open quote) are kept. The NEXT portion scans forward from that quote state for the first
    /// newline outside quotes — the true end of the record being skipped — drops up to it, counts exactly one
    /// discard, and resumes with a known start from there; a portion with no such newline in it keeps
    /// skipping. The unknown-start mode's own bound is unchanged: drop, count one, carry empty.</para>
    ///
    /// <para><b>An RDS truncation notice</b> (#4053 review round 2, item 1): checked only at this text's own
    /// end, after trimming trailing '\r'/'\n' — never in the middle, where a client's own field content
    /// could plant the notice text to force a downgrade it doesn't own. When it is found, the portion is
    /// parsed as usual first (parity is exact up to the cut, in either mode), then the carry is dropped —
    /// not kept — because the missing bytes past the cut mean the next portion's start is no longer known in
    /// either mode; <see cref="CsvCarry.Empty"/> is handed on and the drop counts one discard.</para>
    /// </summary>
    internal static CsvPortion ParseCsvPortion(CsvCarry carry, string? text, bool additionalDataPending)
    {
        if (IsTruncatedByRds(text))
        {
            var parsed = carry.Skipping
                ? StepSkipping(carry, text ?? string.Empty)
                : carry.StartKnown
                    ? StepForward(carry.Partial, text)
                    : StepUnknownStart(carry.Partial, text, additionalDataPending);

            return new CsvPortion(parsed.Entries, parsed.RecordsDiscarded + 1, CsvCarry.Empty);
        }

        if (carry.Skipping)
        {
            return StepSkipping(carry, text ?? string.Empty);
        }

        if (carry.StartKnown)
        {
            /* additionalDataPending is deliberately never read here: EndsOnRecordBoundary is never stated in
               forward mode, whether or not more data is pending, which is exactly what makes this immune to
               the multi-write race the round-1 review found. */
            return StepForward(carry.Partial, text);
        }

        return StepUnknownStart(carry.Partial, text, additionalDataPending);
    }

    /// <summary>Forward mode: parity is exact from offset 0, so only <c>StartsOnRecordBoundary</c> is ever
    /// stated — never <c>EndsOnRecordBoundary</c>.</summary>
    private static CsvPortion StepForward(string? carriedPartial, string? text)
    {
        var body = (carriedPartial ?? string.Empty) + (text ?? string.Empty);
        if (body.Length == 0)
        {
            return new CsvPortion(new List<PgLogEntry>(), 0, new CsvCarry(string.Empty, true));
        }

        var entries = PgServerLogCsvParser.Parse(
            body, PgServerLogCsvParser.CsvBodyEdges.StartsOnRecordBoundary, out var discarded, out var consumedLength);

        var partial = body[consumedLength..];

        if (partial.Length > MaxCarryLength)
        {
            var skipInQuotes = IsQuoteCountOdd(partial);
            return new CsvPortion(entries, discarded, new CsvCarry(string.Empty, false, Skipping: true, SkipInQuotes: skipInQuotes));
        }

        return new CsvPortion(entries, discarded, new CsvCarry(partial, true));
    }

    /// <summary>Skipping mode: scans forward from the carried quote state for the record's true end (the
    /// first newline outside quotes), one portion of new text at a time. Nothing before that newline was
    /// ever a candidate boundary, so nothing here is scored or trusted except the newline itself.</summary>
    private static CsvPortion StepSkipping(CsvCarry carry, string text)
    {
        var inQuotes = carry.SkipInQuotes;
        var boundary = -1;

        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];
            if (c == '"')
            {
                inQuotes = !inQuotes;
            }
            else if (c == '\n' && !inQuotes)
            {
                boundary = i;
                break;
            }
        }

        if (boundary < 0)
        {
            /* No true end found yet in this portion either — keep skipping, carrying only the quote state,
               never the text itself (the whole point of the bound: an oversized record must not regrow the
               carry it was just dropped for). */
            return new CsvPortion(new List<PgLogEntry>(), 0, new CsvCarry(string.Empty, false, Skipping: true, SkipInQuotes: inQuotes));
        }

        /* The record's true end was found: everything up to and including it is the rest of the skipped
           record, counted here as its one discard. None was counted when it was dropped over the bound
           (StepForward), so the total for the record is exactly one. Text after it resumes forward parsing
           with a known start. */
        var rest = text[(boundary + 1)..];
        var resumed = StepForward(null, rest);
        return new CsvPortion(resumed.Entries, resumed.RecordsDiscarded + 1, resumed.Next);
    }

    /// <summary>Unknown-start mode (first contact via the tail read, or right after a resync): scoring
    /// decides the boundaries, and the next carry's start is NEVER known — it stays this way until the next
    /// file gives a true offset-0 start (#4053 review round 1). <c>EndsOnRecordBoundary</c> is stated only
    /// when the body actually ends in <c>'\n'</c>, never merely because no more data is pending.</summary>
    private static CsvPortion StepUnknownStart(string? carriedPartial, string? text, bool additionalDataPending)
    {
        var body = (carriedPartial ?? string.Empty) + (text ?? string.Empty);
        if (body.Length == 0)
        {
            return new CsvPortion(new List<PgLogEntry>(), 0, CsvCarry.Empty);
        }

        var endsInNewline = body.Length > 0 && body[^1] == '\n';
        var edges = (!additionalDataPending && endsInNewline)
            ? PgServerLogCsvParser.CsvBodyEdges.EndsOnRecordBoundary
            : PgServerLogCsvParser.CsvBodyEdges.None;

        var entries = PgServerLogCsvParser.Parse(body, edges, out var discarded, out var consumedLength);

        if (!additionalDataPending)
        {
            /* The file's read ended here, but the start is still unknown — the carry (if any) is whatever
               follows the winning hypothesis's last mark, still guessed, still not a known start. */
            if (consumedLength == 0)
            {
                discarded = 0;
            }

            var tail = body[consumedLength..];
            if (tail.Length > MaxCarryLength)
            {
                return new CsvPortion(entries, discarded + 1, CsvCarry.Empty);
            }

            return new CsvPortion(entries, discarded, new CsvCarry(tail, false));
        }

        if (consumedLength == 0)
        {
            discarded = 0;
        }

        var partial = body[consumedLength..];
        if (partial.Length > MaxCarryLength)
        {
            return new CsvPortion(entries, discarded + 1, CsvCarry.Empty);
        }

        return new CsvPortion(entries, discarded, new CsvCarry(partial, false));
    }

    /// <summary>Whether <paramref name="text"/> ends with the RDS truncation notice (#4053 review round 2,
    /// item 1), trailing '\r'/'\n' trimmed first. Checked only at the end — a client's own field content
    /// planting this text in the middle of a portion must not be able to force the downgrade.</summary>
    private static bool IsTruncatedByRds(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        return text.AsSpan().TrimEnd("\r\n".AsSpan()).EndsWith(RdsTruncationNotice, StringComparison.Ordinal);
    }

    /// <summary>Whether <paramref name="text"/> leaves an open quote by its end — used only to seed
    /// <see cref="CsvCarry.SkipInQuotes"/> when a forward-mode carry is dropped over the bound.</summary>
    private static bool IsQuoteCountOdd(string text)
    {
        var count = 0;
        foreach (var c in text)
        {
            if (c == '"')
            {
                count++;
            }
        }

        return (count & 1) == 1;
    }
}

/// <summary>
/// The per-instance csvlog carry bookkeeping every RDS csvlog reader shares (#4053 part c2), extracted from
/// <see cref="RdsLogEventIngestor.IngestAsync"/> at its original behaviour: keyed by instance alone with the
/// file name carried inside (a rotation to a new file name resets the carry, counting exactly one discard on
/// commit, rather than leaking the old file's entry forever); a read at a known file start
/// (<see cref="RdsLogSource.LogChunk.StartsAtFileStart"/>) seeds a known-start carry with nothing carried;
/// and the carry only ever advances alongside a genuinely-advanced resume marker, never on a replay.
///
/// <para><b>One book per ingestor, never shared</b> — the same reason <see cref="RdsLogSource"/> itself is
/// never shared between the plan, deadlock and log-event ingestors (see the marker remark on
/// <see cref="RdsLogSource"/> ~36-50): each keeps its own in-memory resume marker, consumed once per read, so
/// two ingestors sharing one book would starve whichever runs second in a cycle.</para>
/// </summary>
internal sealed class RdsCsvlogCarryBook
{
    /// <summary>
    /// The csvlog partial-record carry (#4053 part c1), keyed exactly like <see cref="RdsLogSource.ResumeMarker.Key"/>'s
    /// instance half so a rotation to a new file starts fresh rather than resuming a new file's bytes as a
    /// continuation of the old one's tail. In memory, for the same reason the resume marker is: RDS's
    /// <c>DownloadDBLogFilePortion</c> is consume-once, so the carry can only be updated alongside the
    /// marker's own commit.
    /// </summary>
    private readonly Dictionary<string, RdsCsvlogCarry.CsvCarry> _carry = new(StringComparer.Ordinal);

    /// <summary>
    /// The carry to parse this chunk with, and the bookkeeping the eventual commit needs: the carry key
    /// (instance half of <paramref name="resumeKey"/>, or null when it is itself null/empty — the same case
    /// a non-RDS host or a first-ever read of an instance produces), whether this rotation drops an old
    /// file's carry (only counted as a discard once the commit below actually fires), and the current file
    /// name for the carry the commit writes back.
    /// </summary>
    /// <param name="resumeKey">The chunk's <c>ResumeMarker.Key</c> ("instance|file").</param>
    /// <param name="startsAtFileStart">#4053 review round 1 (item 2's tail): true when this read was
    /// requested from offset 0 — the only source of a known start this forward-only route has besides a
    /// forward walk actually proving one.</param>
    public (RdsCsvlogCarry.CsvCarry Carry, string? CarryKey, int DroppedByRotation, string? CurrentFileName) CarryFor(
        string? resumeKey, bool startsAtFileStart)
    {
        var currentFileName = ResumeFileName(resumeKey);
        var carryKey = InstanceKey(resumeKey);
        var carry = RdsCsvlogCarry.CsvCarry.Empty;
        var droppedByRotation = 0;

        if (!string.IsNullOrEmpty(carryKey) && _carry.TryGetValue(carryKey, out var held))
        {
            if (string.Equals(held.FileName, currentFileName, StringComparison.Ordinal))
            {
                carry = held;
            }
            else if (held.Skipping || !string.IsNullOrEmpty(held.Partial))
            {
                /* #4053 review round 2 (item 4): a rotation drops the old file's unfinished record, a
                   carried partial or a record being skipped over the bound. Count it as the one discard
                   it is, and only once the carry update below commits. */
                droppedByRotation = 1;
            }
        }

        if (startsAtFileStart)
        {
            /* #4053 review round 1 (item 2's tail): a rotation's first read of the NEW file starts at
               offset 0, so the carry gets a known start with nothing carried — the only source of a known
               start this forward-only route has besides a forward walk actually proving one. */
            carry = new RdsCsvlogCarry.CsvCarry(string.Empty, true, FileName: currentFileName);
        }

        return (carry, carryKey, droppedByRotation, currentFileName);
    }

    /// <summary>
    /// The carry update after <see cref="RdsLogSource.CommitResume"/> — called exactly once, at the same
    /// point the marker's own commit happens, and only when the marker actually advanced. #4053 review
    /// round 1 (item 3): <c>CommitResume</c> is a no-op on an empty/null marker (a replay), and gluing this
    /// portion's carry onto itself in that case would apply the same bytes' tail twice, so the caller must
    /// not call this on a replay.
    /// </summary>
    /// <param name="carryKey">The key <see cref="CarryFor"/> returned for this chunk.</param>
    /// <param name="currentFileName">The file name <see cref="CarryFor"/> returned for this chunk.</param>
    /// <param name="nextCarry">The carry <see cref="RdsCsvlogCarry.ParseCsvPortion"/> handed back.</param>
    /// <param name="droppedByRotation">The rotation discard <see cref="CarryFor"/> returned for this chunk.</param>
    /// <returns>The total csvlog records discarded this cycle: the parser's own count plus the rotation
    /// discard, now that the commit this discard was waiting on has happened.</returns>
    public int Commit(string? carryKey, string? currentFileName, RdsCsvlogCarry.CsvCarry nextCarry, int droppedByRotation)
    {
        if (carryKey is null)
        {
            return 0;
        }

        var nextCarryWithFile = nextCarry with { FileName = currentFileName };

        if (nextCarryWithFile.Partial.Length == 0 && !nextCarryWithFile.StartKnown && !nextCarryWithFile.Skipping)
        {
            _carry.Remove(carryKey);
        }
        else
        {
            _carry[carryKey] = nextCarryWithFile;
        }

        return droppedByRotation;
    }

    /// <summary>The instance half of a <c>ResumeMarker.Key</c> ("instance|file"), or null when the key itself
    /// is null/empty — #4053 review round 1's carry key.</summary>
    private static string? InstanceKey(string? resumeKey)
    {
        if (string.IsNullOrEmpty(resumeKey))
        {
            return null;
        }

        var separator = resumeKey.IndexOf('|');
        return separator < 0 ? resumeKey : resumeKey[..separator];
    }

    /// <summary>The file half of a <c>ResumeMarker.Key</c>, or null when the key itself is null/empty.</summary>
    private static string? ResumeFileName(string? resumeKey)
    {
        if (string.IsNullOrEmpty(resumeKey))
        {
            return null;
        }

        var separator = resumeKey.IndexOf('|');
        return separator < 0 ? null : resumeKey[(separator + 1)..];
    }
}
