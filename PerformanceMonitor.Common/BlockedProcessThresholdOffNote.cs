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
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Common;

/// <summary>
/// #9518: the count the blocked_process_report collector leaves on its run when the monitored server's
/// <c>blocked process threshold (s)</c> is 0 in use, and the sentence a blocking read turns it back into. With the threshold
/// at 0 SQL Server never raises <c>blocked_process_report</c>, so the collector's session can hold no event and the run reads
/// the setting and nothing else. The label lives here, in the assembly every reader already references, so the writer and
/// each reader move together (the same reason as <see cref="DefaultTraceOffNote"/>).
///
/// <para>A threshold of 0 can be deliberate: an estate may leave reports off and rely on the DMV blocking snapshots. So the
/// run stays a SUCCESS, the count is not a fault and not a precondition, and <see cref="Hint"/> states what is off, what
/// still answers, and how to turn reports on without asking anyone to. The collector writes a COUNT, never a verdict: a read
/// derives the answer from the newest run, so once the threshold is set the next run carries no count and the hint goes away
/// by itself.</para>
/// </summary>
public static class BlockedProcessThresholdOffNote
{
    /// <summary>The measurement label, a legal one (a snake_case count name).</summary>
    public const string Label = "blocked_process_threshold_off";

    /// <summary>The server setting that decides this state, named once so the sentence and the collector's query agree.</summary>
    public const string SettingName = "blocked process threshold (s)";

    /// <summary>The key the sentence rides under in an empty blocking answer's <c>hints</c>.</summary>
    public const string HintKey = "blocked_process_threshold_note";

    /// <summary>What an empty blocking answer adds when the reports are off and the DMV blocking snapshots covered the window. One text for Lite and Darling.</summary>
    public const string Hint =
        "Blocked process reports are off on this server because '" + SettingName + "' is 0. "
        + "Blocking is still answered from DMV snapshots. "
        + "To turn reports on, set the threshold with sp_configure, or in the DB parameter group on RDS.";

    /// <summary>
    /// What the count means, for the collection health note of the two desktop apps (<c>CollectorHealthClassifier.CountMeaning</c>).
    /// The web page words it the same way in plain-text.js COUNT_MEANINGS.
    /// </summary>
    public const string CountMeaning =
        "blocked process reports are off because '" + SettingName + "' is 0, so the session was not read";

    /// <summary>
    /// True when the note carries <c>blocked_process_threshold_off=N</c> with N above zero. Reads label=value pairs anywhere in
    /// the note, so a host note in front of the counts does not hide it.
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

    /// <summary>
    /// The hints of an empty blocking answer with <see cref="Hint"/> added under <see cref="HintKey"/>, beside whatever the
    /// answer already carries there (the window keys, the ring-buffer loss). <paramref name="hints"/> may be null (a failed
    /// window probe); the sentence stands on its own then. Returns <paramref name="hints"/> untouched when
    /// <paramref name="reportsOff"/> is false, so an answer on a server whose reports are on is byte for byte what it was.
    /// </summary>
    public static object? WithHint(object? hints, bool reportsOff)
    {
        if (!reportsOff)
        {
            return hints;
        }

        var node = hints is null
            ? new JsonObject()
            : JsonSerializer.SerializeToNode(hints, McpHelpers.JsonOptions)!.AsObject();
        node[HintKey] = Hint;
        return node;
    }
}
