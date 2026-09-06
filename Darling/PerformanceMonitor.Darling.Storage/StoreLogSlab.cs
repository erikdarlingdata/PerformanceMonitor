/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The two pure decisions the store-log read (#3021) makes about BYTES, split out from
/// <see cref="StoreLogSweep"/> so both are testable without a store: where to resume in a file, and how much
/// of a read is safe to classify.
/// </summary>
public static class StoreLogSlab
{
    /// <summary>
    /// How much of one file is read per capture. Same 4 MB as the two <c>pg_read_file</c> log routes
    /// (<c>PgDeadlocksCollector</c>, <c>PgPlanCaptureCollector</c>) — but for a different reason. Theirs is
    /// an OVERLAP window and the size is what makes truncation recoverable; this route resumes from a
    /// marker, so the size is only a per-capture memory bound and anything left over is reported as
    /// <c>bytes_pending</c> and read by the next capture rather than lost.
    /// </summary>
    public const int MaxBytesPerRead = 4 * 1024 * 1024;

    /// <summary>Where to start reading a file, and whether the ring came round underneath the marker.</summary>
    /// <param name="Offset">The byte offset to read from.</param>
    /// <param name="OffsetReset">True when the stored marker was discarded because the file shrank.</param>
    /// <param name="HasWork">False when there is nothing unread in this file.</param>
    public readonly record struct ResumePoint(long Offset, bool OffsetReset, bool HasWork);

    /// <summary>
    /// Decides where to resume in one log file.
    ///
    /// <para><b>Why <paramref name="storedLastSize"/> exists rather than comparing the offset alone.</b> The
    /// managed store's log is a SELF-CAPPING weekday ring — <c>log_filename = 'postgresql-%a.log'</c> with
    /// <c>log_truncate_on_rotation = on</c> — so <c>postgresql-Fri.log</c> is TRUNCATED next Friday rather
    /// than rolled aside. A marker held over that truncation points past the end of a file that is now
    /// small, and reading from it would return nothing forever. Comparing the current size against the size
    /// at the last read detects that exactly, including the corner where the regrown file happens to reach
    /// precisely the old offset — which comparing size against the OFFSET does not, and which would then
    /// skip a whole week's file silently. The reset is REPORTED
    /// (<c>collect.store_log_captures.offset_reset</c>) rather than absorbed, because it means this capture's
    /// counts cover an interval the previous ones did not.</para>
    ///
    /// <para>The one residual gap, stated rather than papered over: if the service is down long enough for a
    /// weekday file to truncate AND regrow past the stored offset, that interval is under-read and nothing
    /// here can tell. The tell is the capture DENOMINATOR — a window missing captures — which is why
    /// <c>collect.store_log_captures</c> is a table and not a log line.</para>
    /// </summary>
    public static ResumePoint ResolveResume(long? storedOffset, long? storedLastSize, long currentSize)
    {
        if (currentSize < 0)
        {
            return new ResumePoint(0, false, false);
        }

        if (storedOffset is not { } offset || storedLastSize is not { } lastSize)
        {
            /* Never read. Start at the beginning; this is not a reset, it is a first read. */
            return new ResumePoint(0, false, currentSize > 0);
        }

        if (currentSize < lastSize)
        {
            return new ResumePoint(0, true, currentSize > 0);
        }

        return new ResumePoint(offset, false, currentSize > offset);
    }

    /// <summary>The text of a read that is safe to classify, and how many bytes it accounts for.</summary>
    /// <param name="Text">Complete lines only, decoded UTF-8.</param>
    /// <param name="BytesConsumed">Exactly the bytes <paramref name="Text"/> came from, which is what the
    /// marker advances by.</param>
    public readonly record struct Slab(string Text, int BytesConsumed);

    /// <summary>
    /// Trims a raw read to its last complete line and decodes it.
    ///
    /// <para><b>Why this is done on bytes.</b> A file being appended to while it is read ends mid-line, and
    /// so does a read that hit <see cref="MaxBytesPerRead"/>. Cutting at the last <c>0x0A</c> and advancing
    /// the marker by exactly those bytes means the NEXT read always begins at a line start — so the issue's
    /// "a pg_read_file tail can land mid-line, treat the first partial line as noise" case is unreachable by
    /// construction rather than handled by convention. Doing it on bytes rather than on decoded text is
    /// load-bearing: <c>0x0A</c> can never be a UTF-8 continuation byte (those are all <c>0x80</c>-<c>0xBF</c>),
    /// so a cut at a newline byte cannot split a multi-byte character, which a cut at a decoded character
    /// index computed from a byte length could.</para>
    ///
    /// <para>Decoding uses the REPLACEMENT fallback, not the throwing one. This reads the store's own log,
    /// where a message can carry an identifier in any encoding the operator's client sent; a decode
    /// exception would abandon the whole capture over one byte, and a census that refuses to count is worse
    /// than one that counts a replacement character.</para>
    ///
    /// <para>A read with no newline in it at all consumes NOTHING and returns empty: the marker stays put and
    /// the line is read whole once the server finishes writing it.</para>
    /// </summary>
    public static Slab TrimToLastNewline(ReadOnlySpan<byte> bytes)
    {
        var last = bytes.LastIndexOf((byte)0x0A);
        if (last < 0)
        {
            return new Slab(string.Empty, 0);
        }

        var complete = bytes[..(last + 1)];
        return new Slab(Utf8Replacing.GetString(complete), complete.Length);
    }

    /// <summary>UTF-8 with the replacement fallback — see <see cref="TrimToLastNewline"/> for why a decode
    /// fault must not abandon a capture.</summary>
    private static readonly UTF8Encoding Utf8Replacing = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: false);
}
