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
using System.Text;

namespace PerformanceMonitor.Common;

/// <summary>
/// The newest tempdb collection in a chart window, as far as the tempdb log is concerned (#8881): who holds the oldest open
/// tempdb transaction and why the log cannot clear. Lite, the Darling viewer and the web page's read each fill one from their own
/// store, so all three screens say the same thing through <see cref="TempDbLogHolderLine.Build"/>. <see cref="SessionId"/> is null
/// when the collector captured no holder, which it does below <see cref="MinLogUsedPercent"/> percent of the log used and on an engine that cannot report one.
/// <see cref="LogTotalMb"/> and <see cref="LogReuseWaitDesc"/> are read only by <c>get_tempdb_trend</c>, which states them once for the window instead of on every point.
/// </summary>
internal sealed record TempDbLogHolder(
    DateTime AsOfUtc,
    int? SessionId,
    string? LoginName,
    string? ProgramName,
    DateTime? BeginTimeUtc,
    long? LogBytesUsed,
    string? HoldupReason,
    double? LogTotalMb = null,
    string? LogReuseWaitDesc = null);

/// <summary>The one plain line the three tempdb screens show beside the chart when the newest point has a holder (#8881).</summary>
internal static class TempDbLogHolderLine
{
    /// <summary>The log used percent at which the tempdb collector starts reading the holder (#8881). Below it the holder columns are
    /// stored NULL because nothing was read, not because nothing was found. The collector's SQL and every line that explains a missing
    /// holder take the figure from here.</summary>
    public const int MinLogUsedPercent = 50;

    /// <summary>What to say when the log is under <see cref="MinLogUsedPercent"/> used and no holder is stored, so the reader does not
    /// take the empty holder for a look that found nothing. An alert bar set below the percent fires in this range.</summary>
    public static string NotReadBelowBarText => string.Create(CultureInfo.InvariantCulture, $"the holder is read at {MinLogUsedPercent}% log used and above");

    /// <summary>
    /// "Oldest tempdb log transaction: session 87, login app_user, program MyApp, open 14 minutes, 3.2 GB of log used." followed by the
    /// holdup reason when it is anything but NOTHING. Null when there is no holder, so the screen shows no line. The transaction's age
    /// is measured to the collection's own time, not the clock now, so a window ending in the past reads the same as it did then.
    /// </summary>
    public static string? Build(TempDbLogHolder? holder)
    {
        if (holder is null)
        {
            return null;
        }

        var sb = new StringBuilder();
        if (holder.SessionId is { } session)
        {
            sb.Append("Oldest tempdb log transaction: ");
            sb.Append(CultureInfo.InvariantCulture, $"session {session}");
            sb.Append(", login ").Append(string.IsNullOrWhiteSpace(holder.LoginName) ? "unknown" : holder.LoginName);
            sb.Append(", program ").Append(string.IsNullOrWhiteSpace(holder.ProgramName) ? "unknown" : holder.ProgramName);
        }
        else if (holder.BeginTimeUtc is not null || holder.LogBytesUsed is not null)
        {
            /* The collector keeps the oldest open tempdb transaction even when no session owns it: a system or orphaned transaction. */
            sb.Append("Oldest open tempdb transaction has no session (a system or orphaned transaction)");
        }
        else
        {
            return null;
        }

        if (holder.BeginTimeUtc is { } begin)
        {
            var age = holder.AsOfUtc > begin ? holder.AsOfUtc - begin : TimeSpan.Zero;
            sb.Append(", open ").Append(Age(age));
        }

        if (holder.LogBytesUsed is { } bytes)
        {
            sb.Append(", ").Append(Size(bytes)).Append(" of log used");
        }

        sb.Append('.');
        var reason = holder.HoldupReason?.Trim();
        if (!string.IsNullOrEmpty(reason) && !string.Equals(reason, "NOTHING", StringComparison.OrdinalIgnoreCase))
        {
            sb.Append(" Log truncation is held up by ").Append(reason).Append('.');
        }

        return sb.ToString();
    }

    private static string Age(TimeSpan age)
    {
        if (age.TotalDays >= 1)
        {
            return Plural((int)age.TotalDays, "day") + (age.Hours > 0 ? " " + Plural(age.Hours, "hour") : "");
        }

        if (age.TotalHours >= 1)
        {
            return Plural((int)age.TotalHours, "hour") + (age.Minutes > 0 ? " " + Plural(age.Minutes, "minute") : "");
        }

        return age.TotalMinutes >= 1 ? Plural((int)age.TotalMinutes, "minute") : Plural((int)age.TotalSeconds, "second");
    }

    private static string Plural(int count, string unit) =>
        string.Create(CultureInfo.InvariantCulture, $"{count} {unit}{(count == 1 ? "" : "s")}");

    private static string Size(long bytes)
    {
        const double MiB = 1024.0 * 1024.0;
        if (bytes >= 1024L * 1024 * 1024)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{bytes / (MiB * 1024.0):0.#} GB");
        }

        return bytes >= 1024L * 1024
            ? string.Create(CultureInfo.InvariantCulture, $"{bytes / MiB:0} MB")
            : "under 1 MB";
    }
}

/// <summary>The tempdb chart's "Log Used" series (#8881): what the Lite and viewer charts plot from the log reading each bucket carries.</summary>
internal static class TempDbLogSeries
{
    /// <summary>
    /// The values to plot, one per chart point, or null when no point has a reading so the chart draws no "Log Used" series and no empty
    /// legend entry. A point with no reading is NaN, which the plot leaves as a gap. It is never 0: a zero would read as an empty log on a
    /// server whose collector could not read the log, or whose rows were stored before the log columns.
    /// </summary>
    public static double[]? Values(IReadOnlyList<double?> logUsedMb)
    {
        if (!logUsedMb.Any(v => v.HasValue))
        {
            return null;
        }

        return logUsedMb.Select(v => v ?? double.NaN).ToArray();
    }

    /// <summary>The largest real value in a series from <see cref="Values"/>, ignoring the gaps, so a NaN does not poison the chart's Y limit.</summary>
    public static double Max(double[] values) => values.Where(v => !double.IsNaN(v)).DefaultIfEmpty(0).Max();
}
