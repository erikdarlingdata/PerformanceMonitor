/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text;

namespace PerformanceMonitor.Common;

/// <summary>
/// The one rule both products' file logs apply to a line before it is written (#4281, #4286, #9768): Darling's
/// <c>DarlingFileLoggerProvider</c> and Lite's <c>AppLogger</c> call this at their one sink, so a log reader sees the same
/// shape from either: an entry's first line starts with its timestamp in column 0, and every other line of that entry starts
/// with <see cref="ContinuationIndent"/>.
/// </summary>
public static class LogLineText
{
    /// <summary>The prefix a continuation line gets after a kept line feed (#4286 review, Low 5): long enough that a
    /// continuation line can never be mistaken, at a glance or by a naive "does this line start with a timestamp" reader, for
    /// the start of a new entry.</summary>
    public const string ContinuationIndent = "    ";

    /// <summary>
    /// #4281 review, finding 5 and its #4286 follow-up (Low 5): exception.Message (or ANY text a call site pastes into the
    /// message, most commonly a logged <c>ex.Message</c>) can repeat text a server or a request supplied. A SqlException joins
    /// every error the server returned with a line feed, a PostgreSQL cast error echoes the bad value, and a KeyNotFoundException
    /// echoes the key. A CR or LF in it could forge a second entry. Cleaning the WHOLE assembled line at the one sink every log
    /// call funnels through covers text baked into a message as well as an exception passed alongside it.
    ///
    /// <para>Control characters (<see cref="char.IsControl(char)"/>, plus U+2028 and U+2029) map to '.', with no length cap:
    /// an operator needs the exception text in full. Line feed and tab are the exception: a message may embed '\n' on purpose
    /// to print a readable multi-line block, so it stays a real multi-line block rather than turning into a run of dots. A CR
    /// immediately before a kept LF counts as the SAME line feed (a Windows-formatted embedded string must not print a blank
    /// line), and a bare CR with no following LF is a plain control character. Every kept line feed is followed by
    /// <see cref="ContinuationIndent"/>, so a continuation line can never start in column 0, where a reader looks for the next
    /// entry's timestamp, which is what would let embedded text pass for a second entry.</para>
    /// </summary>
    public static string Clean(string line)
    {
        var builder = new StringBuilder(line.Length);
        for (var i = 0; i < line.Length; i++)
        {
            var c = line[i];
            if (c == '\r' && i + 1 < line.Length && line[i + 1] == '\n')
            {
                /* CRLF is ONE line feed -- drop the CR, let the LF branch below fire on the next char. */
                continue;
            }

            if (c == '\n')
            {
                builder.Append('\n').Append(ContinuationIndent);
            }
            else if (c == '\t' || !(char.IsControl(c) || c == (char)0x2028 || c == (char)0x2029))
            {
                builder.Append(c);
            }
            else
            {
                builder.Append('.');
            }
        }

        return builder.ToString();
    }
}
