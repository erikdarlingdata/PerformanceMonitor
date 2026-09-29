/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Common;

/// <summary>
/// The collection-log note a per-database cycle writes when SOME of its databases failed and the rest
/// succeeded (#2623), and the reader that turns it back into counts (#4748).
///
/// <para>
/// The writer (<c>EnumeratedCollectorDriver.BuildPartialFailureNote</c>) and the reader
/// (<see cref="TryParse"/>) sit in one class, and share the words the reader looks for, so that rewording
/// the note cannot leave the health band reading a sentence nobody writes any more. That cycle still
/// records SUCCESS - tolerating one unreachable database must not cost the other twenty-nine - so the
/// note is the only place the loss is recorded, and <see cref="CollectorHealthClassifier.Classify"/>
/// bands on it.
/// </para>
/// </summary>
public static class PartialDatabaseFailureNote
{
    /// <summary>The words that follow the counts. The reader searches for them, so they are the one part of
    /// the sentence the writer and the reader must agree on.</summary>
    private const string Marker = " database(s) failed and were skipped";

    private const string OfSeparator = " of ";

    /// <summary>
    /// The composite format of the note. <c>{0}</c> = how many databases failed, <c>{1}</c> = how many were
    /// attempted, <c>{2}</c> = up to a few of the failed names, <c>{3}</c> = the first error's message.
    /// </summary>
    public const string Format =
        "{0} of {1}" + Marker + " ({2}) - any rows this cycle are from the "
        + "survivors ONLY, so a low or zero row count here is not evidence the server is quiet; "
        + "first error: {3}";

    /// <summary>
    /// Reads the failed and attempted database counts out of a collection-log note. The note may be one part
    /// of a longer text (a cycle can also carry a probe-failure note or a measurement note), so the sentence
    /// is searched for rather than expected at the start. Returns false, with both counts zero, for a null or
    /// blank note and for any note that does not carry the sentence.
    /// </summary>
    public static bool TryParse(string? note, out int failed, out int total)
    {
        failed = 0;
        total = 0;

        if (string.IsNullOrEmpty(note))
        {
            return false;
        }

        var at = note.IndexOf(Marker, StringComparison.Ordinal);
        while (at >= 0)
        {
            if (TryReadCountsBefore(note, at, out failed, out total))
            {
                return true;
            }

            at = note.IndexOf(Marker, at + Marker.Length, StringComparison.Ordinal);
        }

        failed = 0;
        total = 0;
        return false;
    }

    /// <summary>Reads "&lt;failed&gt; of &lt;total&gt;" ending at <paramref name="end"/> (the index where the
    /// marker starts).</summary>
    private static bool TryReadCountsBefore(string note, int end, out int failed, out int total)
    {
        failed = 0;
        total = 0;

        var totalStart = DigitsStartBefore(note, end);
        if (totalStart == end)
        {
            return false;
        }

        var ofStart = totalStart - OfSeparator.Length;
        if (ofStart < 0 || string.CompareOrdinal(note, ofStart, OfSeparator, 0, OfSeparator.Length) != 0)
        {
            return false;
        }

        var failedStart = DigitsStartBefore(note, ofStart);
        if (failedStart == ofStart)
        {
            return false;
        }

        return int.TryParse(note.AsSpan(failedStart, ofStart - failedStart), NumberStyles.None, CultureInfo.InvariantCulture, out failed)
            && int.TryParse(note.AsSpan(totalStart, end - totalStart), NumberStyles.None, CultureInfo.InvariantCulture, out total);
    }

    /// <summary>The index of the first digit of the run of ASCII digits that ends just before
    /// <paramref name="end"/>; <paramref name="end"/> itself when no digit precedes it.</summary>
    private static int DigitsStartBefore(string text, int end)
    {
        var start = end;
        while (start > 0 && char.IsAsciiDigit(text[start - 1]))
        {
            start--;
        }

        return start;
    }
}
