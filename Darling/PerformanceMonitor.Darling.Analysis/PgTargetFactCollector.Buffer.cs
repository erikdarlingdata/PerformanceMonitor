/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// The three components of the buffer-cache composite, each differenced over the window in its own CTE
    /// and joined into ONE row: block hits and reads from <c>pg_database_stats</c> (every flavour, every
    /// major), <c>shared_buffers</c> evictions from <c>pg_io_stats</c> (PG 16+), and the bgwriter counters
    /// from <c>pg_write_stats</c> (PG 14+). <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window.
    ///
    /// <para><b>The differencing is <c>DarlingPgDatabaseReader.PgDatabaseSql</c>'s, verbatim for the
    /// <c>pg_database_stats</c> arm and applied unchanged to the other two tables' reset stamps</b>: per-series
    /// <c>LAG</c>, <c>GREATEST(raw, 0)</c>, and the reset as <c>ROW_NUMBER() OVER series &gt; 1 AND stamp IS
    /// DISTINCT FROM LAG(stamp)</c>. The comment on that read records the NULL→timestamp first-reset trap
    /// that a <c>LAG(stamp) IS NOT NULL</c> guard falls into; it is not re-derived here. The database arm
    /// partitions by <c>database_name</c> (NULL included — the shared-relation row is a series of its own),
    /// the I/O arm by <c>(backend_type, object_type, context)</c> as <c>DarlingPgIoReader.PgIoSql</c> does,
    /// and the bgwriter arm is one series guarded by <c>bgwriter_stats_reset</c>.</para>
    ///
    /// <para><b>Which evictions count.</b> <c>object_type = 'relation' AND context = 'normal'</c>: an eviction
    /// in the <c>normal</c> context is a page pushed out of <c>shared_buffers</c> to make room, which is the
    /// cache-too-small signal. The <c>vacuum</c>, <c>bulkread</c> and <c>bulkwrite</c> contexts run through
    /// ring buffers whose evictions are the ring recycling by design, and counting them would make every
    /// <c>VACUUM</c> and every <c>COPY</c> look like cache pressure.</para>
    ///
    /// <para><b>Structural absence is reported as absence.</b> <c>evictions_tracked</c> is true only when a
    /// <c>pg_io_stats</c> row in the window carried a non-NULL <c>evictions</c>; below PG 16 the collector
    /// does not apply and the CTE returns one row of zeros with <c>false</c>. <c>bgwriter_tracked</c> likewise
    /// for <c>maxwritten_clean</c> (PG 14+). An empty CTE still yields a row (aggregates over nothing), so the
    /// <c>CROSS JOIN</c> never loses the arms that DO have data.</para>
    /// </summary>
    public const string PgTargetBufferCompositeSql = @"
WITH db_sampled AS (
    SELECT
        database_name,
        collection_time,
        blks_read - LAG(blks_read) OVER series AS raw_blks_read,
        blks_hit  - LAG(blks_hit)  OVER series AS raw_blks_hit,
        (ROW_NUMBER() OVER series > 1
         AND stats_reset IS DISTINCT FROM LAG(stats_reset) OVER series) AS reset_here
    FROM pg_database_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    WINDOW series AS (
        PARTITION BY database_name
        ORDER BY collection_time
    )
),
db AS (
    SELECT
        CAST(coalesce(SUM(GREATEST(raw_blks_read, 0)), 0) AS bigint) AS blks_read,
        CAST(coalesce(SUM(GREATEST(raw_blks_hit, 0)), 0) AS bigint)  AS blks_hit,
        CAST(count(*) FILTER (WHERE reset_here) AS integer)          AS db_reset_count,
        CAST(count(*) AS integer)                                    AS db_sample_count
    FROM db_sampled
),
io_sampled AS (
    SELECT
        collection_time,
        evictions - LAG(evictions) OVER series AS raw_evictions,
        (ROW_NUMBER() OVER series > 1
         AND stats_reset IS DISTINCT FROM LAG(stats_reset) OVER series) AS reset_here,
        (evictions IS NOT NULL) AS evictions_tracked
    FROM pg_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   object_type = 'relation'
    AND   context = 'normal'
    WINDOW series AS (
        PARTITION BY backend_type, object_type, context
        ORDER BY collection_time
    )
),
io AS (
    SELECT
        CAST(coalesce(SUM(GREATEST(raw_evictions, 0)), 0) AS bigint) AS evictions,
        coalesce(bool_or(evictions_tracked), false)                  AS evictions_tracked,
        CAST(count(*) FILTER (WHERE reset_here) AS integer)          AS io_reset_count,
        CAST(count(*) AS integer)                                    AS io_sample_count
    FROM io_sampled
),
bg_sampled AS (
    SELECT
        collection_time,
        buffers_clean    - LAG(buffers_clean)    OVER series AS raw_buffers_clean,
        maxwritten_clean - LAG(maxwritten_clean) OVER series AS raw_maxwritten_clean,
        buffers_alloc    - LAG(buffers_alloc)    OVER series AS raw_buffers_alloc,
        (ROW_NUMBER() OVER series > 1
         AND bgwriter_stats_reset IS DISTINCT FROM LAG(bgwriter_stats_reset) OVER series) AS reset_here,
        (maxwritten_clean IS NOT NULL) AS bgwriter_tracked
    FROM pg_write_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    WINDOW series AS (ORDER BY collection_time)
),
bg AS (
    SELECT
        CAST(coalesce(SUM(GREATEST(raw_buffers_clean, 0)), 0) AS bigint)    AS buffers_clean,
        CAST(coalesce(SUM(GREATEST(raw_maxwritten_clean, 0)), 0) AS bigint) AS maxwritten_clean,
        CAST(coalesce(SUM(GREATEST(raw_buffers_alloc, 0)), 0) AS bigint)    AS buffers_alloc,
        coalesce(bool_or(bgwriter_tracked), false)                          AS bgwriter_tracked,
        CAST(count(*) FILTER (WHERE reset_here) AS integer)                 AS bg_reset_count,
        CAST(count(*) AS integer)                                           AS bg_sample_count
    FROM bg_sampled
)
SELECT
    db.blks_read, db.blks_hit, db.db_reset_count, db.db_sample_count,
    io.evictions, io.evictions_tracked, io.io_reset_count, io.io_sample_count,
    bg.buffers_clean, bg.maxwritten_clean, bg.buffers_alloc, bg.bgwriter_tracked, bg.bg_reset_count, bg.bg_sample_count
FROM db
CROSS JOIN io
CROSS JOIN bg";

    /// <summary>
    /// <c>PG_BUFFER_CACHE_PRESSURE</c> (filled by lane 2 — #3542 step 2, design §3.8): deliberately ONE fact
    /// over three components rather than three facts, because the components are correlated measurements
    /// of one thing — a working set larger than <c>shared_buffers</c> — and three facts would count that one
    /// thing three times in severity and in the story list (D2; OtterTune's PostgreSQL metric clusters formed
    /// around exactly these counters). Each component rides as metadata so the scorer grades them separately
    /// and the advice states each; <see cref="Fact.Value"/> is the MISS SHARE, Δblks_read / (Δblks_hit +
    /// Δblks_read) over the window — the one figure every flavour and major reports — in [0, 1].
    ///
    /// <para><b>Per-component honesty flags, all read by the scorer.</b> <c>hit_ratio_suppressed = 1</c> on
    /// Aurora (off the registry fact's <c>is_aurora</c>): Aurora serves a <c>blks_read</c> from either its
    /// storage volume or the local NVMe tier, and <c>pg_stat_database</c> cannot tell which, so the community
    /// hit-ratio arithmetic is misleading there (the V64 note on <c>storage_blks_read</c> /
    /// <c>orcache_blks_hit</c>) — the number is stated, never graded. <c>evictions_tracked = 0</c> below PG 16,
    /// where <c>pg_stat_io</c> does not exist: the eviction arm is structurally absent and the composite rests
    /// on hit ratio and bgwriter alone, and says so. <c>bgwriter_tracked = 0</c> below PG 14. A composite
    /// with every arm suppressed or untracked scores 0 and the advice explains which arms were unavailable
    /// on this flavour and major (D6).</para>
    ///
    /// <para><b>Rates are over OBSERVED time (#3538 A7)</b>, and two of them are dimensionless by design so
    /// they mean the same on every host. <c>cache_turnovers_per_hour</c> = evictions × block size /
    /// <c>shared_buffers</c> bytes per observed hour — how many times the whole cache was replaced —
    /// computable only when the config collector emitted <c>shared_buffers</c> a moment before this method
    /// ran. <c>bgwriter_halt_share</c> = <c>maxwritten_clean</c> / the rounds the bgwriter could have run
    /// (observed seconds / <c>bgwriter_delay</c>): the share of its wake-ups on which it hit
    /// <c>bgwriter_lru_maxpages</c> and stopped with dirty buffers still ahead of the clock sweep, leaving
    /// backends to write their own. Both denominators are engine-defined; the bars over the ratios are the
    /// scorer's.</para>
    ///
    /// <para><b>What is NOT emitted.</b> No fact when the window has no observed time, or when nothing moved —
    /// no block requests, no evictions, no buffer allocations — because a composite over zero traffic is a
    /// claim about an idle server, not about its cache. The plumbing e2e plants flat counters and expects
    /// exactly this silence.</para>
    /// </summary>
    private async partial Task CollectBufferFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        if (context.ObservedDurationMs <= 0) return;

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetBufferCompositeSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var blksRead = ToInt64(reader.GetValue(0));
            var blksHit = ToInt64(reader.GetValue(1));
            var dbResets = Convert.ToInt32(reader.GetValue(2));
            var dbSamples = Convert.ToInt32(reader.GetValue(3));
            var evictions = ToInt64(reader.GetValue(4));
            var evictionsTracked = !reader.IsDBNull(5) && reader.GetBoolean(5);
            var ioResets = Convert.ToInt32(reader.GetValue(6));
            var ioSamples = Convert.ToInt32(reader.GetValue(7));
            var buffersClean = ToInt64(reader.GetValue(8));
            var maxwrittenClean = ToInt64(reader.GetValue(9));
            var buffersAlloc = ToInt64(reader.GetValue(10));
            var bgwriterTracked = !reader.IsDBNull(11) && reader.GetBoolean(11);
            var bgResets = Convert.ToInt32(reader.GetValue(12));
            var bgSamples = Convert.ToInt32(reader.GetValue(13));

            var blocksTotal = blksRead + blksHit;
            if (blocksTotal <= 0 && evictions <= 0 && buffersAlloc <= 0) return;

            var observedSeconds = context.ObservedDurationMs / 1000.0;
            var observedHours = observedSeconds / 3600.0;

            var registry = facts.Find(f => f.Key == PgTargetFactKeys.ServerMajorVersion);
            var isAurora = registry is not null && registry.Metadata.GetValueOrDefault("is_aurora") > 0;

            var fact = new Fact
            {
                Source = PgTargetSources.BufferSource,
                Key = PgTargetFactKeys.BufferCachePressure,
                Value = blocksTotal > 0 ? blksRead / (double)blocksTotal : 0.0,
                ServerId = context.ServerId,
                Metadata =
                {
                    /* hit-ratio arm */
                    ["blks_hit"] = blksHit,
                    ["blks_read"] = blksRead,
                    ["blocks_total"] = blocksTotal,
                    ["block_requests_per_sec"] = blocksTotal / observedSeconds,
                    ["miss_share"] = blocksTotal > 0 ? blksRead / (double)blocksTotal : 0.0,
                    ["hit_ratio"] = blocksTotal > 0 ? blksHit / (double)blocksTotal : 0.0,
                    ["hit_ratio_suppressed"] = isAurora ? 1 : 0,
                    ["db_reset_count"] = dbResets,
                    ["db_sample_count"] = dbSamples,
                    /* eviction arm */
                    ["evictions"] = evictions,
                    ["evictions_tracked"] = evictionsTracked ? 1 : 0,
                    ["evictions_per_sec"] = evictions / observedSeconds,
                    ["io_reset_count"] = ioResets,
                    ["io_sample_count"] = ioSamples,
                    /* bgwriter arm */
                    ["buffers_clean"] = buffersClean,
                    ["maxwritten_clean"] = maxwrittenClean,
                    ["buffers_alloc"] = buffersAlloc,
                    ["buffers_alloc_per_sec"] = buffersAlloc / observedSeconds,
                    ["bgwriter_tracked"] = bgwriterTracked ? 1 : 0,
                    ["bg_reset_count"] = bgResets,
                    ["bg_sample_count"] = bgSamples,
                    ["observed_ms"] = context.ObservedDurationMs,
                },
            };

            /* The knob and the bgwriter rhythm, off the shared_buffers fact emitted a moment ago (emission
               order: Config before Buffer). Absent when the config snapshot has not been collected — the
               dimensionless ratios are then not stamped and their arms are not graded. */
            var sharedBuffers = facts.Find(f => f.Key == PgTargetFactKeys.ConfigSharedBuffers);
            if (sharedBuffers is not null)
            {
                if (sharedBuffers.Metadata.TryGetValue("bytes", out var sharedBuffersBytes) && sharedBuffersBytes > 0)
                {
                    fact.Metadata["shared_buffers_bytes"] = sharedBuffersBytes;
                    if (evictionsTracked)
                        fact.Metadata["cache_turnovers_per_hour"] = evictions * (double)PgSettingValue.DefaultBlockBytes / sharedBuffersBytes / observedHours;
                }

                if (sharedBuffers.Metadata.TryGetValue("bgwriter_delay_ms", out var bgwriterDelayMs) && bgwriterDelayMs > 0)
                {
                    fact.Metadata["bgwriter_delay_ms"] = bgwriterDelayMs;
                    var rounds = observedSeconds * 1000.0 / bgwriterDelayMs;
                    fact.Metadata["bgwriter_rounds"] = rounds;
                    if (bgwriterTracked && rounds > 0)
                        fact.Metadata["bgwriter_halt_share"] = Math.Min(1.0, maxwrittenClean / rounds);
                }

                if (sharedBuffers.Metadata.TryGetValue("bgwriter_lru_maxpages", out var lruMaxPages))
                    fact.Metadata["bgwriter_lru_maxpages"] = lruMaxPages;
            }

            facts.Add(fact);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Three tables from three rungs (V83, V88, and the pg_io_stats rung): a store behind any one of
               them raises 42P01 here, which the reporter classifies quiet. An abandonment is NOT swallowed
               (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }
}
