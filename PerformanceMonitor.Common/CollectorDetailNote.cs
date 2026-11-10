/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// get_collection_health's <c>collector_detail_note</c>, shared by both products so the words cannot drift. It says how the
/// default cut shaped the rows: how many of the collectors that run on the server are healthy and compacted, how many need a
/// look and carry the leaner partial shape, and, when the reply also lists collectors the server does not run (an engine that
/// never runs them, Always On off, a Query Store that is a readable secondary), how many of those sit below as not collected,
/// so the note's count and the reply's <c>collectors_total</c> describe the same list. Every verb agrees with its count.
/// </summary>
public static class CollectorDetailNote
{
    /// <summary>The note when <c>full_detail=true</c>: nothing was cut.</summary>
    public const string FullDetail = "full_detail=true: every field is served for every collector below.";

    /// <summary>
    /// The default cut's note. <paramref name="compactCount"/> healthy rows of the <paramref name="shownCount"/> collectors that run on the
    /// server are compacted, <paramref name="partialCount"/> need a look, and <paramref name="notCollectedCount"/> more rows below are
    /// collectors the server does not run, listed as not collected and counted in <c>collectors_total</c> but not here.
    /// </summary>
    public static string Build(int compactCount, int shownCount, int partialCount, int notCollectedCount)
    {
        var note =
            $"{compactCount} of {CountText.Plural(shownCount, "collector")} below {(compactCount == 1 ? "is" : "are")} HEALTHY with nothing to report (no errors, denials, session/extension-missing runs or abandoned cycles this window) and {(compactCount == 1 ? "is" : "are")} compacted to collector/status/compact/total_runs/rows_stored/avg_duration_ms/last_success. "
            + $"{partialCount} {(partialCount == 1 ? "needs" : "need")} a look (failing, stale, stopped, erroring, denied, regressed, or an unexplained zero) and {(partialCount == 1 ? "carries" : "carry")} a leaner partial_detail shape instead of full detail: in a partial row a missing key means null, false or 0, and only collector, status, partial_detail, total_runs and errors are always present.";
        if (notCollectedCount > 0)
        {
            note += $" {CountText.Plural(notCollectedCount, "more row")} below {(notCollectedCount == 1 ? "is" : "are")} not collected on this server ({(notCollectedCount == 1 ? "the row says why" : "each row says why")}) and {(notCollectedCount == 1 ? "is" : "are")} counted in collectors_total but not above.";
        }

        return note + " Pass full_detail=true for every field on every collector.";
    }
}
