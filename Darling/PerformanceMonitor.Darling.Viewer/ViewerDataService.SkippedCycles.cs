/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

public sealed partial class ViewerDataService
{
    /// <summary>
    /// #5735: this server's skipped-slot rows in the trailing 24 hours, from the Storage read the MCP
    /// <c>skipped_cycles</c> block and the web panel share (<see cref="SkippedCycleReader"/>). Read on every refresh: it
    /// is one index seek and is never memoised, so a row the loop just wrote shows at once.
    /// </summary>
    public Task<SkippedCycleSummary> GetSkippedCycleSummaryAsync(int serverId, CancellationToken cancellationToken = default) =>
        SkippedCycleReader.GetSkippedCycleSummaryAsync(
            _dataSource, serverId, commandTimeoutSeconds: ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken: cancellationToken);
}

/// <summary>
/// #5735: the one line the Collection Health tab prints about skipped slots; Lite's <c>SkippedCycleSummary.SummaryLine</c> twin
/// (#5643). The all-clear when there is none, else the rows, the slots and the newest row's time and cause. On Darling a slot is
/// one scheduled run of one collector, so the line says "slot(s)" and not cycles; a row is one late stretch (at most one is
/// written per server per five minutes).
/// </summary>
internal static class SkippedCycleLine
{
    /// <summary>The line for <paramref name="summary"/>; the newest row's time is worded in the tab's current display mode.</summary>
    internal static string Format(SkippedCycleSummary summary)
    {
        ArgumentNullException.ThrowIfNull(summary);
        if (summary.Count == 0 || summary.LastAtUtc is not { } lastAtUtc)
        {
            return "No skipped collection slots in 24 h";
        }

        var step = summary.LastSlowestStep is null
            ? "cause not recorded"
            : summary.LastSlowestStepMs is { } ms
                ? $"cause {summary.LastSlowestStep}, {FormatSeconds(ms)}"
                : $"cause {summary.LastSlowestStep}";

        var when = ViewerTimeHelper.FormatForDisplay(lastAtUtc, "yyyy-MM-dd HH:mm") + " "
            + ViewerTimeHelper.GetTimezoneLabel(ViewerTimeHelper.CurrentDisplayMode, ViewerTimeHelper.ActiveServerClock, lastAtUtc);

        return string.Create(CultureInfo.InvariantCulture,
            $"Skipped collection slots in 24 h: {summary.SlotsSkipped:N0} slot(s) in {summary.Count:N0} late stretch(es). Last {when}: {step}.");
    }

    /// <summary>The line when the read failed: nothing is known, which is not the all-clear.</summary>
    internal const string Unreadable = "Skipped collection slots in 24 h: could not be read";

    internal static string FormatSeconds(long ms) =>
        (ms / 1000.0).ToString("0.#", CultureInfo.InvariantCulture) + " s";
}
