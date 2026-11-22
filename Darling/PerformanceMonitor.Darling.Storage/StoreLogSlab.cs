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
    /// <param name="BytesConsumed">What the marker advances by. It is exactly the bytes <paramref name="Text"/>
    /// came from, except for a line longer than one read (#9314), where <paramref name="Text"/> is only the line's
    /// first bytes and this is the whole line through its newline.</param>
    /// <param name="Cut">True when <paramref name="Text"/>'s last line is only the start of a longer line (#9314), so
    /// the classifier treats that line as cut text. False for every slab that ends on a line end.</param>
    public readonly record struct Slab(string Text, int BytesConsumed, bool Cut = false);

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

    /// <summary>
    /// A finished line longer than one read (#9314): <paramref name="slab"/> is the first
    /// <see cref="MaxBytesPerRead"/> bytes of it, with no newline inside, and <paramref name="lineBytes"/> is the line's
    /// whole length through its newline. The text is the slab cut to whole UTF-8 characters, so the classifier counts
    /// and classifies the line by its start, and the marker advances by the whole line.
    ///
    /// <para>A slab that ends inside a multi-byte character loses that character's leading bytes rather than decoding
    /// to a replacement character, so the cut never splits one. The classifier caps a retained sample at
    /// <see cref="StoreLogClassifier.MaxSampleLength"/> and already treats a sample the cap cut as cut, so a cut line is
    /// marked the way every long line is.</para>
    /// </summary>
    public static Slab TrimLongLine(ReadOnlySpan<byte> slab, int lineBytes)
    {
        var whole = slab[..Utf8WholePrefixLength(slab)];
        return new Slab(Utf8Replacing.GetString(whole), lineBytes, Cut: true);
    }

    /// <summary>The length of the longest prefix of <paramref name="bytes"/> that ends on a UTF-8 character boundary:
    /// a trailing lead byte whose continuation bytes are not all there is left off.</summary>
    internal static int Utf8WholePrefixLength(ReadOnlySpan<byte> bytes)
    {
        /* A character is at most 4 bytes, so the lead byte of an unfinished one is within the last 3. */
        for (var back = 1; back <= 3 && back <= bytes.Length; back++)
        {
            var b = bytes[^back];
            if ((b & 0xC0) == 0x80)
            {
                continue;
            }

            var needed = b >= 0xF0 ? 4 : b >= 0xE0 ? 3 : b >= 0xC0 ? 2 : 1;
            return needed > back ? bytes.Length - back : bytes.Length;
        }

        return bytes.Length;
    }

    /// <summary>UTF-8 with the replacement fallback — see <see cref="TrimToLastNewline"/> for why a decode
    /// fault must not abandon a capture.</summary>
    private static readonly UTF8Encoding Utf8Replacing = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: false);
}
