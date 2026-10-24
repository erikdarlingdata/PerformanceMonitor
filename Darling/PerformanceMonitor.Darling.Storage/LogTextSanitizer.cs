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

/// <summary>#6390 S2c: moved from the service's <c>DarlingHttpRefusalLog.Sanitize</c> (which forwards here) so the composed-panel run
/// in Storage can name a server in its unknown-server answer the way the service does.</summary>
internal static class LogTextSanitizer
{
    /// <summary>
    /// Makes attacker-supplied text safe to put in a log line: control characters become '.', and the
    /// result is truncated.
    ///
    /// <para>CR and LF are the ones that matter. A log file is a text file and a reader — human or
    /// otherwise — splits it on newlines, so a Host header carrying <c>\r\n</c> could forge whole log
    /// entries. Truncation bounds the other half of the problem: a megabyte header should not become a
    /// megabyte of log.</para>
    /// </summary>
    internal static string Sanitize(string? value, int maxLength)
    {
        if (string.IsNullOrEmpty(value))
        {
            return "(none)";
        }

        var take = Math.Min(value.Length, maxLength);

        /* #4281 review, finding 1 (Medium): a cut at exactly `take` can land between a surrogate pair's high
           and low half, leaving a lone high surrogate at the end. File.AppendAllText's UTF-8 encoder throws
           on a lone surrogate and drops the WHOLE log batch, not just this line -- so the cut must never
           split a pair. #4286 review note: `take` can only be 0 when maxLength is 0, which no caller passes
           today, but value[take - 1] would throw IndexOutOfRangeException rather than degrade if one ever did. */
        if (take > 0 && take < value.Length && char.IsHighSurrogate(value[take - 1]))
        {
            take--;
        }

        var builder = new StringBuilder(take + 1);
        for (var i = 0; i < take; i++)
        {
            var c = value[i];
            /* #4286 review, Low 4: char.IsControl covers C0, DEL and the C1 range (U+0080-U+009F, including
               NEL/U+0085) that `c < ' ' || c == 0x7F` let through -- Kestrel decodes those from a
               percent-encoded path (%C2%85). U+2028/U+2029 (Unicode LINE/PARAGRAPH SEPARATOR) are not
               char.IsControl but split a line for readers like Python's str.splitlines() that ReadLine and
               grep do not, so a forged entry could hide from ReadLine-based tooling while a splitlines()-based
               one saw it as real. All are sanitized the same as CR/LF so a reader can never disagree with
               another about where one log entry ends. */
            if (char.IsControl(c) || c == (char)0x2028 || c == (char)0x2029
                || System.Globalization.CharUnicodeInfo.GetUnicodeCategory(c) == System.Globalization.UnicodeCategory.Format)
            {
                /* Unicode format characters (category Cf: zero-width marks, the bidi controls U+202A-U+202E and
                   U+2066-U+2069) change how a line DISPLAYS, so a request text could make a log line read
                   reordered; they become '.' the same way. */
                builder.Append('.');
            }
            else if (char.IsHighSurrogate(c) && i + 1 < take && char.IsLowSurrogate(value[i + 1]))
            {
                /* A well-formed pair inside the kept range, copied whole. */
                builder.Append(c);
                builder.Append(value[i + 1]);
                i++;
            }
            else if (char.IsSurrogate(c))
            {
                /* A lone surrogate that is not a truncation artifact -- the input already carried one.
                   Sanitized the same way a control character is. */
                builder.Append('.');
            }
            else
            {
                builder.Append(c);
            }
        }

        if (value.Length > maxLength)
        {
            builder.Append('…');
        }

        return builder.ToString();
    }
}
