/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// #9107: the count the default_trace_events collector leaves on its run when the monitored server's default trace is
/// turned off (<c>default trace enabled</c> is 0 in <c>sys.configurations</c>), and the sentence a reader turns it back into.
/// The label lives here, in the assembly every reader already references, so the writer and each reader move together (the
/// same reason as <see cref="RingBufferTruncationNote"/>).
///
/// <para>The collector writes a COUNT, never a verdict: the read derives "needs setup" fresh from the newest run, so once
/// somebody turns the trace on, the next run carries no count and the answer changes by itself. The run stays a SUCCESS: a
/// trace somebody chose to switch off is not a monitoring fault and not a refused grant.</para>
/// </summary>
public static class DefaultTraceOffNote
{
    /// <summary>The measurement label, a legal one (a snake_case count name).</summary>
    public const string Label = "default_trace_off";

    /// <summary>The server setting that decides this state, named once so every sentence and the collector's query agree.</summary>
    public const string SettingName = "default trace enabled";

    /// <summary>The statement that turns the default trace on. It takes effect at once and needs no restart.</summary>
    public const string EnableStatement = "EXEC sp_configure 'default trace enabled', 1; RECONFIGURE;";

    /// <summary>
    /// True when the note carries <c>default_trace_off=N</c> with N above zero. Reads label=value pairs anywhere in the note,
    /// so a host note in front of the counts does not hide it.
    /// </summary>
    public static bool TryParse(string? note)
    {
        if (string.IsNullOrWhiteSpace(note))
        {
            return false;
        }

        var prefix = Label + "=";
        foreach (var token in note.Split(new[] { ' ', ';' }, StringSplitOptions.RemoveEmptyEntries))
        {
            if (!token.StartsWith(prefix, StringComparison.Ordinal))
            {
                continue;
            }

            var digits = token[prefix.Length..];
            if (digits.Length > 0
                && digits.All(char.IsAsciiDigit)
                && long.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out var count)
                && count > 0)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The short sentence for a health row's note cell and the Collection Log.</summary>
    public static string Sentence =>
        "the default trace is turned off on this server (" + SettingName + " is 0), so there is nothing to read; this needs setup, not a grant. "
        + "To turn it on, run: " + EnableStatement;
}
