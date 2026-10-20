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
/// What <c>collection_log</c> says about one collector on one server: the evidence an empty MCP read needs to tell
/// "the collector ran and found nothing" from "the collector has barely run" (#5633 items 3 and 19). Authored once
/// here, the way <see cref="ConfigChangeDiff"/> is, because Darling reads it from PostgreSQL and Lite from DuckDB and
/// the words they say must not drift. Each store supplies only the numbers; the sentences live here.
///
/// <para>Only SUCCESS rows count. A PERMISSIONS or ERROR row is a collector that did not look, so it is never
/// evidence that there was nothing to find.</para>
/// </summary>
/// <param name="SuccessfulRuns">The collector's successful runs in all retained history, capped at
/// <see cref="RunsCap"/> (callers only ask whether there are two).</param>
/// <param name="SuccessfulRunsInWindow">Successful runs inside the read's window, or null when the caller asked no
/// window question.</param>
/// <param name="LastSuccessUtc">The newest successful run, or null when there is none.</param>
/// <param name="LastSuccessRows"><c>rows_collected</c> of that newest successful run, or null when it was not
/// recorded (an older log row).</param>
public sealed record CollectorRunFacts(
    long SuccessfulRuns,
    long? SuccessfulRunsInWindow,
    DateTime? LastSuccessUtc,
    long? LastSuccessRows)
{
    /// <summary>The count <see cref="SuccessfulRuns"/> stops at: the reads only need "none, one, or two or more".</summary>
    public const int RunsCap = 2;

    /// <summary>A delta collector (procedure stats, perfmon counters) needs two runs before it can show a delta.</summary>
    public const int RunsForDeltaBaseline = 2;

    /// <summary>No run known: a store that cannot say makes no claim, so every sentence falls back to its old text.</summary>
    public static CollectorRunFacts None { get; } = new(0, null, null, null);

    /// <summary>True when the collector has run successfully at least <see cref="RunsForDeltaBaseline"/> times, so a delta
    /// collector has had a baseline and an empty read is not explained by newness.</summary>
    public bool HasDeltaBaseline => SuccessfulRuns >= RunsForDeltaBaseline;

    /// <summary>True when the collector's newest successful run recorded zero rows: it looked, and found nothing there to
    /// store. A NULL row count is "not recorded", not zero.</summary>
    public bool LastRunFoundNothing => LastSuccessUtc is not null && LastSuccessRows == 0;

    /// <summary>
    /// The sentence for an empty <c>get_top_procedures_by_cpu</c> page, with its status word.
    /// <list type="bullet">
    /// <item>Fewer than two successful runs: <c>unavailable</c>, the two-cycles explanation. That text is true only for a
    /// collector this new.</item>
    /// <item>Two or more runs, and at least one inside the window: <c>empty</c>. The collector looked and no procedure ran
    /// (or none matched the request).</item>
    /// <item>Two or more runs, none inside the window: <c>unavailable</c>. The collector is not new, but it did not look at
    /// this window, so "nothing ran" would be a claim nobody measured.</item>
    /// </list>
    /// <paramref name="databasesLeftOut"/> (#5633 review, finding D) is how many dropped databases the read left out; in the
    /// <c>empty</c> case a positive count changes the words so they claim only the databases the server still has.
    /// </summary>
    public (string Status, string Message) ProcedureStatsEmpty(int hoursBack, int databasesLeftOut = 0)
    {
        if (!HasDeltaBaseline)
        {
            return ("unavailable",
                "No procedure stats available. Delta-based collection requires at least two collection cycles (~30 minutes) to produce non-zero values, and the procedure_stats collector has not yet completed two successful runs for this server.");
        }

        if (SuccessfulRunsInWindow is > 0 && databasesLeftOut > 0)
        {
            /* #5633 review (finding D): the read left out databases the server no longer lists, so "recorded nothing for it" would
               deny rows the collector did record there. Say what was measured: nothing in the databases it still has. */
            return ("empty",
                string.Create(CultureInfo.InvariantCulture,
                    $"No procedure ran in the last {hoursBack}h in a database this server still has: the procedure_stats collector ran successfully {SuccessfulRunsInWindow} time(s) in the window and recorded nothing for them. {databasesLeftOut} database(s) the server no longer lists were left out of this read (see databases_left_out)."));
        }

        if (SuccessfulRunsInWindow is > 0)
        {
            return ("empty",
                string.Create(CultureInfo.InvariantCulture,
                    $"No procedure executed in the last {hoursBack}h that matches this request: the procedure_stats collector ran successfully {SuccessfulRunsInWindow} time(s) in the window and recorded nothing for it."));
        }

        var last = LastSuccessUtc is { } at ? string.Create(CultureInfo.InvariantCulture, $" (its last successful run was {at:yyyy-MM-dd HH:mm:ss} UTC)") : "";
        return ("unavailable",
            $"The procedure_stats collector has no successful run inside the last {hoursBack}h{last}, so this window was not measured. This is a collection gap, not an all-clear: check collection health.");
    }

    /// <summary>
    /// #6226: the sentence for a ranking that came back empty over a window the collector DID run in, or null when it has no
    /// successful run inside the window (the caller then keeps its <c>unavailable</c>, "could have it, does not right now").
    /// A collector that stores only what changed or ran since its watermark (query_stats, Query Store, pg_stat_statements) logs
    /// a SUCCESS with zero rows when nothing did, so successful runs and no rows is a measurement: we looked, and nothing ran.
    /// <paramref name="found"/> is the noun phrase for what it looked for ("executed statements"). <paramref name="scope"/>
    /// names a database the caller filtered to: the runs may have stored rows for other databases, so the sentence then claims
    /// only the chosen one. <paramref name="requireDeltaBaseline"/> is for a collector whose FIRST run stores nothing by design
    /// (it has nothing to difference against), so one run in the window is not yet a measurement.
    /// </summary>
    public string? RanFoundNothing(string collectorName, int hoursBack, string found, string? scope = null, bool requireDeltaBaseline = false)
    {
        if (SuccessfulRunsInWindow is not > 0 || (requireDeltaBaseline && !HasDeltaBaseline))
        {
            return null;
        }

        var verdict = scope is null ? $"found no {found} to store" : $"stored no {found} for {scope}";
        return string.Create(CultureInfo.InvariantCulture,
            $"{collectorName} ran {SuccessfulRunsInWindow} time(s) in the last {hoursBack} hour(s) and {verdict}. This is not a collection gap: collection health showing successful runs that stored no rows is consistent with it.");
    }

    /// <summary>
    /// The sentence for a trace-flag read that came back with no flags, or null when the collector's newest run does not
    /// show it found none (no run, or a run that stored rows). <c>DBCC TRACESTATUS(-1)</c> lists only flags that are on,
    /// so a successful run that stored zero rows is the collector saying no trace flag is enabled, which is a measurement
    /// and not a missing snapshot.
    /// </summary>
    public string? NoTraceFlagsEnabled()
    {
        if (!LastRunFoundNothing)
        {
            return null;
        }

        return string.Create(CultureInfo.InvariantCulture, $"No trace flags are enabled as of the trace_flags collector's latest successful run ({LastSuccessUtc:yyyy-MM-dd HH:mm:ss} UTC).");
    }
}
