/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// #7896: how the CPU spike drill-down's <c>queries_at_spike</c> read tells one request from the next on the same session, shared by the
/// Lite (DuckDB) and Darling (PostgreSQL) readers so the two stores cannot drift.
///
/// <para>(session_id, request_id) does not name a request: <c>sys.dm_exec_requests.request_id</c> is 0 for every request outside MARS and is
/// unique only among a session's CONCURRENT requests, so one pooled connection that runs several requests in the four minutes around the
/// peak would collapse to one entry and hide the consumers the read exists to show. The snapshot stores no request start, but
/// <c>collection_time - total_elapsed_time_ms</c> is one: the same instant for every snapshot of one request, and a later instant for the
/// session's next request.</para>
///
/// <para>That computed start carries jitter. <c>collection_time</c> is stamped on the monitoring host when the collector run begins, before
/// the store watermark read, the connection and the snapshot batch; <c>total_elapsed_time_ms</c> is the DMV's whole-millisecond reading
/// when the batch reaches <c>sys.dm_exec_requests</c>. The computed start therefore sits earlier than the true start by that run's
/// stamp-to-DMV latency, which differs from one snapshot to the next: milliseconds normally, and more on a server under the load the
/// drill-down describes. Truncating or rounding the start to a fixed bucket of any width splits a request whose computed starts fall
/// either side of a bucket edge, which jitter of a few milliseconds does whenever the true start sits near one. So the key is a gap rule
/// with no edges: within (session_id, request_id), snapshots ordered by computed start belong to one request while each start is within
/// <see cref="StartGapSeconds"/> of the one before it. A gap wider than that begins the session's next request.</para>
///
/// <para>The only requests the rule folds together are ones the same session sampled under <see cref="StartGapSeconds"/> into their run
/// before its next request began, and the heavier sample of the pair is the one kept. Rows with no computed start (an older row whose
/// <c>collection_time</c> or <c>total_elapsed_time_ms</c> is NULL) group as one key per (session_id, request_id), as a NULL
/// <c>request_id</c> does.</para>
/// </summary>
public static class QueriesAtSpikeRequestKey
{
    /// <summary>The largest difference, in whole seconds, between two snapshots' computed starts that still reads as one request. A
    /// string so both readers can splice it into their <c>const</c> SQL.</summary>
    public const string StartGapSeconds = "2";
}
