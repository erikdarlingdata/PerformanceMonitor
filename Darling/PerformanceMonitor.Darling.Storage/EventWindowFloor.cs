/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The partition-column floor a read windowed on an EVENT's own timestamp carries beside that window (#3895).
///
/// <para><b>Why a second predicate at all.</b> <c>blocked_process_reports</c>, <c>dmv_blocking_snapshots</c>
/// and <c>deadlocks</c> are hypertables partitioned on <c>collection_time</c>, and a count bounded only on
/// <c>event_time</c> / <c>deadlock_time</c> gives TimescaleDB nothing to exclude a chunk on: it plans and opens
/// every retained chunk to count the last hour. Measured on DARLING01 (18 daily chunks): the fleet's deadlock
/// count took 10.4 ms to plan and 30.2 ms to run against 4,124 rows, and 0.2 ms / 0.02 ms with this floor
/// beside the window.</para>
///
/// <para><b>Why it cannot drop a qualifying row.</b> A row is collected after its event happened, so its
/// <c>collection_time</c> is at or after its event stamp except by the skew between the monitored server's
/// clock, which stamps the event, and the collector host's, which stamps the collection. Measured on the same
/// store: blocked-process reports land 4.8 s to 62 s after their <c>event_time</c>, a DMV blocking snapshot's
/// <c>event_time</c> IS its <c>collection_time</c>, and the worst deadlock sat 27 ms AFTER its collection.
/// <see cref="SkewAllowance"/> is one whole chunk — a day — so a monitored clock would have to run a day
/// ahead of the collector's before a row fell out, and the floor costs at most one chunk more than the window
/// itself while still excluding every chunk older than that.</para>
///
/// <para><b>Deliberately no upper bound on <c>collection_time</c>.</b> An event collected late — the catch-up
/// after an outage — is still an event inside the window, and it is exactly the row an upper bound would
/// lose.</para>
/// </summary>
public static class EventWindowFloor
{
    /// <summary>How far before an event's own timestamp its <c>collection_time</c> may fall and still be read:
    /// the store's chunk width, derived from it so "at most one chunk beyond the window" stays true if the
    /// width ever moves.</summary>
    public static readonly TimeSpan SkewAllowance = TimeSpan.FromDays(TimescaleSupport.ChunkIntervalDays);

    /// <summary>The <c>collection_time</c> floor for an event window starting at
    /// <paramref name="eventWindowStartUtc"/>, as naive UTC — the store's timestamp convention, so the bind
    /// compares like with like instead of Npgsql inferring <c>timestamptz</c> from a Kind=Utc value.</summary>
    public static DateTime For(DateTime eventWindowStartUtc) =>
        DateTime.SpecifyKind(eventWindowStartUtc - SkewAllowance, DateTimeKind.Unspecified);
}
