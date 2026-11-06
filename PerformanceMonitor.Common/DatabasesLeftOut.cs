/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// #5633 review (finding D): what a ranking read tells its reader when its database narrowing left databases out. With no
/// <c>database_name</c>, <c>get_top_procedures_by_cpu</c> leaves out the databases the server dropped since an earlier
/// <c>database_states</c> snapshot (#5633 item 4). That is right for the ranking, but an empty page then must not read as
/// "the collector recorded nothing": the omitted databases may be the only place a procedure ran. The names and the one note
/// below travel in the envelope of every answer, empty or not, in the same words on both products.
/// </summary>
public static class DatabasesLeftOut
{
    /// <summary>The most names the envelope lists; the note carries the full count, so a server with hundreds of dropped test
    /// databases does not bloat every page.</summary>
    public const int MaxNamed = 20;

    /// <summary>The envelope key of the omitted names.</summary>
    public const string NamesKey = "databases_left_out";

    /// <summary>#7538: the envelope key of the true number of databases left out, <see cref="Count"/>. The names list stops at
    /// <see cref="MaxNamed"/>, so a page that says "and N more" reads N from this field, not from the words of the note.</summary>
    public const string CountKey = "databases_left_out_count";

    /// <summary>The envelope key of <see cref="Note"/>.</summary>
    public const string NoteKey = "databases_left_out_note";

    /// <summary>The omitted names, ordinal-sorted and distinct, or null when none were left out. Not capped: use
    /// <see cref="Named"/> for the envelope.</summary>
    public static IReadOnlyList<string>? Distinct(IEnumerable<string?>? names)
    {
        var sorted = (names ?? Array.Empty<string?>())
            .Where(name => !string.IsNullOrWhiteSpace(name))
            .Select(name => name!)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToList();
        return sorted.Count == 0 ? null : sorted;
    }

    /// <summary>The first <see cref="MaxNamed"/> of <paramref name="all"/> for the envelope, or null when none.</summary>
    public static IReadOnlyList<string>? Named(IReadOnlyList<string>? all)
        => all is null || all.Count == 0 ? null : all.Count <= MaxNamed ? all : all.Take(MaxNamed).ToList();

    /// <summary>#7538: how many databases the read left out (every one, not only the <see cref="MaxNamed"/> listed), or null when none.</summary>
    public static int? Count(IReadOnlyList<string>? all) => all is null || all.Count == 0 ? null : all.Count;

    /// <summary>#7726 item 4: "was" for one database, "were" for any other count, for the sentences that say how many databases were left out.</summary>
    public static string WasOrWere(int count) => count == 1 ? "was" : "were";

    /// <summary>The one note beside the names, or null when none were left out.</summary>
    public static string? Note(IReadOnlyList<string>? all)
    {
        if (all is null || all.Count == 0)
        {
            return null;
        }

        var more = all.Count > MaxNamed
            ? string.Create(CultureInfo.InvariantCulture, $" ({all.Count} in all; the first {MaxNamed} are listed)")
            : "";
        return "These databases were left out of this read because the server's newest database snapshot no longer lists them "
            + "(dropped or detached since an earlier snapshot did)" + more + ". Name one in database_name to read its history.";
    }

    /// <summary>
    /// #7538, #7651: the sentence a person's window shows for a read that left databases out, the same words as the web page
    /// (util.js leftOutNoteText): the count once, the reason, and the names, at most <see cref="MaxNamed"/> with the rest counted. It does
    /// not tell the reader to name a database in <c>database_name</c>, an argument a window does not have. Null when none were left out.
    /// </summary>
    public static string? PageSentence(IReadOnlyList<string>? all)
    {
        if (all is null || all.Count == 0)
        {
            return null;
        }

        var named = Named(all)!;
        var one = all.Count == 1;
        var rest = all.Count - named.Count;
        var was = WasOrWere(all.Count);
        return string.Format(CultureInfo.InvariantCulture, "{0:N0} {1} {2} left out because the server no longer lists {3}. {4} {2} dropped or detached: {5}{6}.",
            all.Count, one ? "database" : "databases", was, one ? "it" : "them", one ? "It" : "They", string.Join(", ", named),
            rest > 0 ? string.Format(CultureInfo.InvariantCulture, ", and {0:N0} more", rest) : "");
    }

    /// <summary>
    /// #6651: the banner text for a Top Procedures read that left databases out, or null when it left none. It states the count once, with the
    /// names (<see cref="PageSentence"/>). On an empty page the shared empty-page sentence
    /// (<see cref="CollectorRunFacts.ProcedureStatsEmpty"/>, so the wording matches the web page and the MCP tool) comes first, so the banner
    /// says why nothing ranked as well. Shared by the Darling Viewer and Lite (#6667), so the two cannot drift.
    /// </summary>
    public static string? ProcedureBanner(IReadOnlyList<string>? leftOut, int rowCount, CollectorRunFacts? facts, int hoursBack)
    {
        var sentence = PageSentence(leftOut);
        if (sentence is null)
        {
            return null;
        }

        if (rowCount == 0 && facts is not null)
        {
            var (_, message) = facts.ProcedureStatsEmpty(hoursBack, leftOut!.Count);
            return message + " " + sentence;
        }

        return sentence;
    }
}
