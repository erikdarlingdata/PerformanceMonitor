/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// When the Running Jobs surfaces (the Lite and Darling Viewer tabs, the MCP tool and the web page they feed) may still call a
/// stored snapshot "running now". The alert read already decides this for the long-running-job alert (#1812): a snapshot older than
/// <see cref="AnomalousJobsResult.MaxSnapshotAge"/> at the server's effective running_jobs cadence is no evidence of anything
/// current. The display reads use the same bound, because the snapshot is written only while a job runs: after a server goes
/// offline, the msdb login is lost or the collector is switched off, the newest snapshot stays the newest forever and a job read
/// as running for days (31 hours on the reported store) that ended long before. Without the bound only the latest SUCCESSFUL
/// run decided, and a run that stored rows followed by nothing but failed runs (or no runs) never changed that answer.
/// </summary>
public static class RunningJobsCurrency
{
    /// <summary>
    /// The oldest collection time still counted as current at <paramref name="nowUtc"/>: the same
    /// <see cref="AnomalousJobsResult.MaxSnapshotAge"/> rule the alert read applies (three missed cycles at
    /// <paramref name="cadenceMinutes"/>, floored at 10 minutes), a snapshot AT the cutoff still current.
    /// </summary>
    public static DateTime Cutoff(DateTime nowUtc, int cadenceMinutes)
        => nowUtc - AnomalousJobsResult.MaxSnapshotAge(cadenceMinutes);

    /// <summary>
    /// The last good collection when it is older than <paramref name="cutoffUtc"/> (collection is not current), otherwise null.
    /// A store that holds no collection at all (<paramref name="lastGoodCollectionUtc"/> null) has nothing to call stale:
    /// the other empty-state notes (not collected, no access) say why it is empty.
    /// </summary>
    public static DateTime? NotCurrentSince(DateTime? lastGoodCollectionUtc, DateTime cutoffUtc)
        => lastGoodCollectionUtc is DateTime last && last < cutoffUtc ? last : null;

    /// <summary>The note shown where the jobs would be when collection is not current.</summary>
    public static string NotCurrentNote(DateTime lastGoodCollectionUtc)
        => string.Create(CultureInfo.InvariantCulture,
            $"Collection is not current. The last good collection was at {lastGoodCollectionUtc:yyyy-MM-dd HH:mm} UTC, so no running jobs are shown.");
}
