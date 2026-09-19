/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetBaselineProvider
{
    /// <summary>
    /// <c>pg_wal_bytes_per_sec</c>: the reset-aware WAL counter difference over the collection's own gap, from
    /// <c>pg_write_stats</c> — the <c>PgTps</c> arm's shape on the write table. /* filled by lane 15 — #3691 step 15,
    /// design §3.11. */
    ///
    /// <para><b>One differencing, three readers.</b> <c>sampled</c> is <c>PgTargetFactCollector.PgTargetWalVolumeSql</c>'s
    /// differencing with the half-open upper bound (<c>&lt; $3</c>, every arm's shape): per-sample <c>LAG</c> on
    /// <c>wal_bytes</c>, <c>GREATEST(raw, 0)</c> so a rewound counter (a <c>wal_stats_reset</c> mid-series) adds
    /// nothing, the gap the series' own second-truncated <c>LAG(collection_time)</c>. The reset is CLAMPED here,
    /// not counted: a bucket has no metadata to carry a reset count on, and the clamp alone is what keeps the
    /// distribution honest — the collector's read records the count for the fact, which does. The fact's mean
    /// and peak, the detector's window peak and this bucket are therefore one unit — bytes of WAL per second per
    /// collection — and a rate the fact states is a rate the bucket can be asked about.</para>
    ///
    /// <para><b>Untracked rows are not samples.</b> <c>wal_bytes IS NOT NULL</c> in <c>sampled</c>, so an Aurora
    /// series (typed NULL — <c>pg_stat_wal</c> is not implemented there) or a pre-14 one yields an EMPTY bucket
    /// (<c>SampleCount</c> 0, the "no baseline" answer) rather than a thirty-day distribution of zeros with a
    /// zero MAD against which any first byte would be infinitely deviant. The detector gates on the window's
    /// trackedness BEFORE it asks for this bucket, so on Aurora this query does not run at all; the filter here
    /// is the belt to that brace. A tracked-but-idle server (real zeros, records still counting) is a legitimate
    /// series of small values and stays in. No restart-signature exclusion, for the reason the <c>pg_tps</c> arm
    /// gives: the reset is explicit and the rewind clamped, so a restart is one small interval a median/MAD
    /// frame absorbs.</para>
    /// </summary>
    private static partial string? WalBytesPerSecBaselineQuery() => @"
WITH sampled AS (
    SELECT collection_time,
           wal_bytes - LAG(wal_bytes) OVER series AS raw_wal_bytes,
           extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER series))) AS interval_sec
    FROM pg_write_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   wal_bytes IS NOT NULL
    WINDOW series AS (ORDER BY collection_time)
),
clean AS (
    SELECT collection_time, GREATEST(raw_wal_bytes, 0)::DOUBLE PRECISION / interval_sec AS v
    FROM sampled
    WHERE raw_wal_bytes IS NOT NULL
    AND   interval_sec > 0
)," + PgBaselineProvider.RobustTierScaffold;
}
