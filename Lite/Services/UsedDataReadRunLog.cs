/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitorLite.Services;

/// <summary>Why a read of the data in the store gave nothing (#9250).</summary>
internal enum UsedDataReadFailure
{
    None,

    /// <summary>The read lock was not free within the read's short wait, for example behind a checkpoint or an archival pass.</summary>
    LockBusy,

    /// <summary>The lock was held but the query threw or returned no value.</summary>
    QueryFailed,
}

/// <summary>One read of the data in the store: the megabytes, or null with the reason it was not read.</summary>
internal readonly record struct UsedDataRead(double? Mb, UsedDataReadFailure Failure);

/// <summary>
/// Tracks a run of failed data reads at the per-tick size check (#9250). Once the store file reaches the hot store reset point, each
/// tick reads how much data the file holds, and a failed read falls back to the file ceiling with no trace. This keeps the run and
/// writes three kinds of line: a Warning on the first failure (file size, reset point and its source, file ceiling, and the
/// reason), a Warning with the running count every <see cref="ReportEvery"/> failed reads while the run lasts, and one Information
/// line with the count when a read works again.
///
/// <para>Only ticks that attempted the read are recorded. A tick with the file under the reset point attempts none, so it neither
/// starts nor ends a run. The maintenance loop is the only caller and runs one tick at a time, so the state needs no lock.</para>
/// </summary>
internal sealed class UsedDataReadRunLog(ILogger? logger)
{
    /// <summary>
    /// Failed reads between the count lines. The maintenance tick is <see cref="CollectionBackgroundService.HousekeepingInterval"/>,
    /// one minute, so 60 reads is about one line an hour: enough to show the data-based reset is still not firing, without a line a
    /// minute.
    /// </summary>
    internal const int ReportEvery = 60;

    private int _failedReads;

    /// <summary>The failed reads in the run in progress, 0 when there is none.</summary>
    internal int FailedReads => _failedReads;

    /// <summary>Records one attempted read. Call it only on a tick where the file was at or over the reset point.</summary>
    internal void Record(UsedDataRead read, double fileMb, HotStoreResetLimit limit)
    {
        if (read.Mb is { } usedMb)
        {
            if (_failedReads > 0)
            {
                logger?.LogInformation("Store data read works again after {FailedReads} failed read(s) in a row. Data is {UsedMb} MB, file {FileMb:F0} MB, reset point {Threshold:F0} MB ({Source})",
                    _failedReads, usedMb.ToString("F0", CultureInfo.InvariantCulture), fileMb, limit.Mb, limit.Source);
                _failedReads = 0;
            }

            return;
        }

        _failedReads++;
        var reason = read.Failure == UsedDataReadFailure.LockBusy
            ? "the read lock was busy"
            : "the query failed";
        if (_failedReads == 1)
        {
            logger?.LogWarning("Could not read the data in the store file ({FileMb:F0} MB, reset point {Threshold:F0} MB from {Source}, file ceiling {CeilingMb:F0} MB): {Reason}. The data-based reset cannot fire while reads fail, so only the file ceiling can reset the store. Logging again every {ReportEvery} failed reads",
                fileMb, limit.Mb, limit.Source, limit.FileCeilingMb, reason, ReportEvery);
        }
        else if (_failedReads % ReportEvery == 0)
        {
            logger?.LogWarning("Store data reads are still failing: {FailedReads} in a row, latest because {Reason}. File {FileMb:F0} MB, reset point {Threshold:F0} MB from {Source}, file ceiling {CeilingMb:F0} MB",
                _failedReads, reason, fileMb, limit.Mb, limit.Source, limit.FileCeilingMb);
        }
    }

    /// <summary>
    /// Ends the run without a line, after the store was reset: the file is small again, so the next failure is a new run and gets
    /// its first-failure line instead of continuing the old count.
    /// </summary>
    internal void EndRunAfterReset() => _failedReads = 0;
}
