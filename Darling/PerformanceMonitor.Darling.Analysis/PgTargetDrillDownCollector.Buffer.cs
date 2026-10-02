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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetDrillDownCollector
{
    /// <summary>The drill-down section a buffer-pressure finding carries: one object, never a bare list.</summary>
    internal const string BufferCompositionSection = "pg_buffer_composition";

    /// <summary>
    /// How many resident relations the drill-down carries, largest first. Ten: the question the drill-down
    /// answers is "is the cache one relation, or a working set" and ten rows settle it; the capture holds
    /// every relation over the collector's 8-buffer floor and <c>relations_in_capture</c> says how many were
    /// not shown, with <c>get_pg_buffer_usage</c> one call away for the rest. A bound, not a bar — nothing is
    /// graded on it.
    /// </summary>
    internal const int BufferTopRelationCap = 10;

    /// <summary>
    /// The character bound applied IN THE READ to a relation name (<c>LEFT(relation_name, $n)</c>), with the
    /// untruncated length beside it for the flag — lane 7's shape for statement text. PostgreSQL's own
    /// <c>NAMEDATALEN</c> makes an identifier at most 63 bytes, so a name the collector wrote is never cut;
    /// the bound and its flag exist so a store written by anything else cannot ship an unbounded string into
    /// a finding row, and so the payload's contract is the same as every other drill-down's.
    /// </summary>
    internal const int RelationNameCap = 63;

    /// <summary>
    /// The usage count at or below which a resident buffer is "cold". Engine-defined: the clock sweep
    /// decrements <c>usagecount</c> on every pass and evicts a buffer when it reaches 0, so a relation whose
    /// buffers average at most 1 is the next pass's victim; <c>BM_MAX_USAGE_COUNT</c> is 5, so a hot relation
    /// sits near it. The stored figure is an AVERAGE per relation (the collector groups by relation), so the
    /// share this yields is "buffers in relations that are cold on average", and the prose says so.
    /// </summary>
    internal const double ColdUsageCountCeiling = 1.0;

    /// <summary>
    /// The latest <c>pg_buffer_usage</c> capture in the window, as the top <c>$4</c> relations by buffers
    /// with the capture-wide totals on every row. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC),
    /// <c>$4</c> the relation cap, <c>$5</c> the relation-name cap.
    ///
    /// <para><b>Latest, taken whole.</b> Residency is a level, not a counter (<c>DarlingPgBufferUsageReader</c>'s
    /// reasoning, copied): the pool totals are consistent within one capture and not across captures, so the
    /// <c>newest</c> CTE picks one <c>collection_time</c> and every row below is from it. A window with several
    /// captures shows the last; the pressure counters the card was graded on are the window's, and the
    /// sentence says the capture is a level beside them.</para>
    ///
    /// <para><b>The totals are window functions over the capture, not a second read</b>, for the same reason
    /// the collector repeats <c>pool_buffers_total</c> on every row: a share computed against a pool that has
    /// since moved is wrong. <c>buffers_listed</c> is the sum over the rows the collector kept (it drops
    /// relations under 8 buffers), so it is at most <c>pool_buffers_used</c> and the payload states both
    /// rather than pretending the tail does not exist. The kind shares split <c>buffers_listed</c> by
    /// <c>relkind</c>: <c>r</c>/<c>m</c> heap (tables and materialised views have heap storage), <c>i</c>
    /// index, <c>t</c> TOAST, and everything else — including the NULL kind of a buffer from another database
    /// or a shared catalog, which the collector keeps because dropping it would understate the fill — as
    /// other. Partitioned parents (<c>p</c>/<c>I</c>) have no storage and never appear.</para>
    ///
    /// <para><c>cold_buffers</c> is the sum over relations whose <c>avg_usage_count</c> is at or below
    /// <c>$6</c> (<see cref="ColdUsageCountCeiling"/>); a NULL average (no buffer carried a count) is not cold.
    /// Ordered by buffers, then name, so equal counts rank deterministically.</para>
    /// </summary>
    public const string PgTargetBufferCompositionSql = @"
WITH newest AS (
    SELECT max(collection_time) AS at
    FROM pg_buffer_usage
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
),
capture AS (
    SELECT
        b.collection_time,
        b.database_name,
        b.relation_name,
        b.relation_kind,
        b.buffers,
        b.dirty_buffers,
        b.avg_usage_count,
        b.pool_buffers_total,
        b.pool_buffers_used
    FROM pg_buffer_usage AS b
    JOIN newest AS n ON b.collection_time = n.at
    WHERE b.server_id = $1
),
totals AS (
    SELECT
        max(collection_time)                                                                   AS captured_at,
        CAST(coalesce(max(pool_buffers_total), 0) AS bigint)                                   AS pool_buffers_total,
        CAST(coalesce(max(pool_buffers_used), 0) AS bigint)                                    AS pool_buffers_used,
        CAST(count(*) AS integer)                                                              AS relations_in_capture,
        CAST(coalesce(SUM(buffers), 0) AS bigint)                                              AS buffers_listed,
        CAST(coalesce(SUM(dirty_buffers), 0) AS bigint)                                        AS dirty_buffers_listed,
        CAST(coalesce(SUM(buffers) FILTER (WHERE relation_kind IN ('r', 'm')), 0) AS bigint)   AS heap_buffers,
        CAST(coalesce(SUM(buffers) FILTER (WHERE relation_kind = 'i'), 0) AS bigint)           AS index_buffers,
        CAST(coalesce(SUM(buffers) FILTER (WHERE relation_kind = 't'), 0) AS bigint)           AS toast_buffers,
        CAST(coalesce(SUM(buffers) FILTER (WHERE avg_usage_count <= $6), 0) AS bigint)         AS cold_buffers
    FROM capture
)
SELECT
    c.database_name,
    LEFT(c.relation_name, $5)                                AS relation_name,
    length(c.relation_name)                                  AS relation_name_length,
    c.relation_kind,
    c.buffers,
    c.dirty_buffers,
    c.avg_usage_count,
    t.captured_at,
    t.pool_buffers_total,
    t.pool_buffers_used,
    t.relations_in_capture,
    t.buffers_listed,
    t.dirty_buffers_listed,
    t.heap_buffers,
    t.index_buffers,
    t.toast_buffers,
    t.cold_buffers
FROM capture AS c
CROSS JOIN totals AS t
ORDER BY c.buffers DESC, c.relation_name NULLS LAST, c.database_name NULLS LAST
LIMIT $4";

    /// <summary>
    /// When the window holds no capture: the server's latest capture anywhere, so the sentence can say how
    /// long before the window it was (or that there has never been one). <c>$1</c> server_id. One
    /// <c>max()</c> over the <c>(server_id, collection_time)</c> index — a bounded read by shape.
    /// </summary>
    public const string PgTargetBufferLastCaptureSql = @"
SELECT max(collection_time) AS last_capture
FROM pg_buffer_usage
WHERE server_id = $1";

    /// <summary>
    /// <c>pg_extension_availability</c>'s latest word on <c>pg_buffercache</c> at or before the window's end:
    /// one row per distinct <c>state</c> in the newest snapshot, because the collector writes one row per
    /// database and the extension may be installed in one and merely available in another. <c>$1</c>
    /// server_id, <c>$2</c> the window's end. Read ONLY when the window has no capture — a capture is proof
    /// the extension worked, and the question is why there is nothing to show.
    /// </summary>
    public const string PgTargetBufferCacheExtensionSql = @"
WITH newest AS (
    SELECT max(collection_time) AS at
    FROM pg_extension_availability
    WHERE server_id = $1
    AND   collection_time <= $2
    AND   extension_name = 'pg_buffercache'
)
SELECT DISTINCT e.state
FROM pg_extension_availability AS e
JOIN newest AS n ON e.collection_time = n.at
WHERE e.server_id = $1
AND   e.extension_name = 'pg_buffercache'";

    /// <summary>
    /// The buffer-composition drill-down (lane 29 of #3691), for a finding whose path carries
    /// <see cref="PgTargetFactKeys.BufferCachePressure"/>: what the cache HOLDS beside the counters that said
    /// it is short. The composite grades hit ratio, evictions and bgwriter halts — every one a rate that says
    /// the working set does not fit — and none of them says what the working set IS. The latest
    /// <c>pg_buffer_usage</c> capture does: pool fill, the ten largest resident relations with their share of
    /// the pool and their dirty share, the split between indexes, heaps and TOAST, the dirty share of the
    /// pool, and the share sitting in relations the clock sweep will evict first.
    ///
    /// <para><b>The pressure figures are REUSED from the root fact's metadata, never recomputed.</b>
    /// <c>PgTargetFactCollector.Buffer.cs</c> stamps <c>miss_share</c>, <c>hit_ratio_suppressed</c>, <c>evictions</c>,
    /// <c>buffers_alloc</c>, <c>observed_ms</c> and <c>cache_turnovers_per_hour</c>;
    /// <see cref="AnalysisFinding.RootFactMetadata"/> carries them here when the composite is the root (the
    /// memory chain roots on it), and they ride in the payload so a reader has the rate and the level side by
    /// side without opening three tables. When the composite is on the path but not the root the figures are
    /// null, stated as such — never a second read of <c>pg_database_stats</c>.</para>
    ///
    /// <para><b>Inside the 0.5 gate, by construction.</b> The composite scores 0 below its concerning line and
    /// at least 0.5 at it (<c>PgTargetScorer.GradeArm</c>), so "on the path" already means "fired at the
    /// incident line or above" and the gate adds nothing here; it is kept because the walk is shared. The
    /// <c>CONFIG_PG_SHARED_BUFFERS</c> advisory that roots ALONE on a quiet server (0.4) is not enriched — the
    /// composition of a cache nothing is short of is <c>get_pg_buffer_usage</c>'s to browse, not a finding's
    /// to carry.</para>
    ///
    /// <para><b>Nothing to show is a sentence, never an empty array</b>, and WHICH sentence is read, not
    /// guessed. <c>pg_buffer_usage</c> collects hourly and needs <c>pg_buffercache</c>; a window with no
    /// capture is either a cadence gap (the extension is installed; the last capture was N h before the
    /// window — one <c>max()</c> says which N) or an extension gap, and <c>pg_extension_availability</c>'s
    /// latest state says whether the extension is <c>available</c> (one <c>CREATE EXTENSION</c> away) or
    /// <c>absent</c> (not on this server's menu at all). A missing availability row is reported as the
    /// cadence arm: the pass did not read the extension's state and does not claim one.</para>
    ///
    /// <para>The same summary is folded into the finding's frozen advice: <see cref="FactAdvice.PopulateStoryText"/>
    /// composed the card's prose from facts alone (step 3.5 of the pass), before any drill-down ran, so the
    /// composition sentence is appended HERE — <see cref="PgTargetAdvice.WithBufferComposition"/> writes the
    /// words, this method re-freezes them — and the persisted <c>StoryText</c> and the <c>advice</c> in
    /// <c>analyze_server</c>'s payload both carry it. A finding whose <c>StoryText</c> is empty or legacy is
    /// left as it was: the payload section still carries the numbers.</para>
    /// </summary>
    private async partial Task CollectBufferCompositionAsync(AnalysisFinding finding, AnalysisContext context)
    {
        var top = new List<PgTargetBufferResident>(BufferTopRelationCap);
        DateTime? capturedAt = null;
        long poolTotal = 0, poolUsed = 0, listed = 0, dirtyListed = 0, heap = 0, index = 0, toast = 0, cold = 0;
        var relationsInCapture = 0;

        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
        using (var cmd = new NpgsqlCommand(PgTargetBufferCompositionSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds })
        {
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            cmd.Parameters.AddWithValue(BufferTopRelationCap);
            cmd.Parameters.AddWithValue(RelationNameCap);
            cmd.Parameters.AddWithValue(ColdUsageCountCeiling);

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                capturedAt = reader.IsDBNull(7) ? null : reader.GetDateTime(7);
                poolTotal = reader.IsDBNull(8) ? 0 : Convert.ToInt64(reader.GetValue(8));
                poolUsed = reader.IsDBNull(9) ? 0 : Convert.ToInt64(reader.GetValue(9));
                relationsInCapture = reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetValue(10));
                listed = reader.IsDBNull(11) ? 0 : Convert.ToInt64(reader.GetValue(11));
                dirtyListed = reader.IsDBNull(12) ? 0 : Convert.ToInt64(reader.GetValue(12));
                heap = reader.IsDBNull(13) ? 0 : Convert.ToInt64(reader.GetValue(13));
                index = reader.IsDBNull(14) ? 0 : Convert.ToInt64(reader.GetValue(14));
                toast = reader.IsDBNull(15) ? 0 : Convert.ToInt64(reader.GetValue(15));
                cold = reader.IsDBNull(16) ? 0 : Convert.ToInt64(reader.GetValue(16));

                var relationName = reader.IsDBNull(1) ? null : reader.GetString(1);
                var relationNameLength = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2));
                var relationKind = reader.IsDBNull(3) ? null : reader.GetString(3);
                var buffers = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));
                var dirty = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5));

                top.Add(new PgTargetBufferResident(
                    Rank: top.Count + 1,
                    DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                    RelationName: relationName,
                    RelationNameTruncated: IsTruncated(relationName, relationNameLength),
                    RelationKind: relationKind,
                    Kind: KindLabel(relationKind),
                    Buffers: buffers,
                    ShareOfPool: Share(buffers, poolTotal) ?? 0,
                    DirtyBuffers: dirty,
                    DirtyShare: Share(dirty, buffers) ?? 0,
                    AvgUsageCount: reader.IsDBNull(6) ? null : Convert.ToDouble(reader.GetValue(6))));
            }
        }

        /* The pressure figures, reused: the composite's own metadata when it is the root. */
        var metadata = finding.RootFactMetadata;
        double? Meta(string key) => metadata is not null && metadata.TryGetValue(key, out var v) ? v : null;

        var status = PgTargetBufferCompositionStatus.Captured;
        DateTime? lastCapture = null;
        double? hoursBefore = null;
        if (capturedAt is null)
        {
            /* Nothing in the window: why. The extension's state first — an absent extension makes the cadence
               question moot — then the last capture the server ever had. */
            status = await ReadBufferCacheExtensionStatusAsync(connection, context);
            if (status == PgTargetBufferCompositionStatus.NoCapture)
            {
                lastCapture = await ReadLastBufferCaptureAsync(connection, context);
                if (lastCapture is { } last)
                    hoursBefore = (AsNaive(context.TimeRangeStart) - last).TotalHours;
            }
        }

        var summary = new PgTargetBufferCompositionSummary(
            Status: status,
            CapturedAt: capturedAt,
            LastCaptureAt: lastCapture,
            LastCaptureHoursBeforeWindow: hoursBefore,
            PoolBuffersTotal: poolTotal,
            PoolBuffersUsed: poolUsed,
            PoolBytes: poolTotal * PgSettingValue.DefaultBlockBytes,
            PoolFill: Share(poolUsed, poolTotal),
            BuffersListed: listed,
            DirtyBuffersListed: dirtyListed,
            DirtyShareOfPool: Share(dirtyListed, poolTotal),
            HeapShare: Share(heap, listed),
            IndexShare: Share(index, listed),
            ToastShare: Share(toast, listed),
            OtherShare: Share(listed - heap - index - toast, listed),
            ColdBuffers: cold,
            ColdShare: Share(cold, listed),
            RelationsInCapture: relationsInCapture,
            TopRelations: top,
            MissShare: Meta("miss_share"),
            HitRatioSuppressed: (Meta("hit_ratio_suppressed") ?? 0) > 0,
            Evictions: Meta("evictions"),
            BuffersAlloc: Meta("buffers_alloc"),
            ObservedMs: Meta("observed_ms"),
            CacheTurnoversPerHour: Meta("cache_turnovers_per_hour"));

        finding.DrillDown![BufferCompositionSection] = new
        {
            status = status switch
            {
                PgTargetBufferCompositionStatus.Captured => "captured",
                PgTargetBufferCompositionStatus.NoCapture => "no_capture_in_window",
                PgTargetBufferCompositionStatus.ExtensionAvailable => "pg_buffercache_available",
                _ => "pg_buffercache_absent",
            },
            captured_at = summary.CapturedAt?.ToString("o", CultureInfo.InvariantCulture),
            last_capture_at = summary.LastCaptureAt?.ToString("o", CultureInfo.InvariantCulture),
            last_capture_hours_before_window = summary.LastCaptureHoursBeforeWindow is { } h ? Math.Round(h, 2) : (double?)null,
            /* A level at captured_at, not a window counter: pg_buffer_usage collects hourly. */
            pool_buffers_total = summary.PoolBuffersTotal,
            pool_buffers_used = summary.PoolBuffersUsed,
            pool_bytes = summary.PoolBytes,
            pool_fill = Round(summary.PoolFill),
            /* The rows the collector kept (relations of 8+ buffers); at most pool_buffers_used. */
            relations_in_capture = summary.RelationsInCapture,
            buffers_listed = summary.BuffersListed,
            dirty_buffers_listed = summary.DirtyBuffersListed,
            dirty_share_of_pool = Round(summary.DirtyShareOfPool),
            /* Of buffers_listed; the four sum to 1 when the capture has rows. */
            kind_shares = new
            {
                heap = Round(summary.HeapShare),
                index = Round(summary.IndexShare),
                toast = Round(summary.ToastShare),
                other = Round(summary.OtherShare),
            },
            cold_buffers = summary.ColdBuffers,
            cold_share = Round(summary.ColdShare),
            cold_note = "Buffers in relations whose average usage count is at or below 1 — the clock sweep's next victims; a per-relation average, not a per-buffer count.",
            top_relations = top.Select(r => new
            {
                rank = r.Rank,
                database_name = r.DatabaseName,
                /* Null for a buffer of another database or a shared catalog — the collector cannot name it from here; never "". */
                relation_name = r.RelationName,
                relation_name_truncated = r.RelationNameTruncated,
                relation_kind = r.RelationKind,
                kind = r.Kind,
                buffers = r.Buffers,
                bytes = r.Buffers * PgSettingValue.DefaultBlockBytes,
                share_of_pool = Math.Round(r.ShareOfPool, 4),
                dirty_buffers = r.DirtyBuffers,
                dirty_share = Math.Round(r.DirtyShare, 4),
                avg_usage_count = r.AvgUsageCount is { } u ? Math.Round(u, 2) : (double?)null,
            }).ToList(),
            /* The root fact's figures, reused so the rate and the level sit side by side. The counts and the
               observed span ride rather than the fact's per-second quotients: the rates are the fact's to publish
               (get_analysis_facts source=pg_buffer stamps evictions_per_sec and buffers_alloc_per_sec, computed
               ONCE in PgTargetFactCollector.Buffer.cs over observed time) and the advice prose states them; a
               copy here under a per-second name would be a second publication of one quotient. */
            pressure = new
            {
                miss_share = Round(summary.MissShare),
                hit_ratio_suppressed = summary.HitRatioSuppressed,
                evictions = summary.Evictions,
                buffers_alloc = summary.BuffersAlloc,
                observed_ms = summary.ObservedMs,
                cache_turnovers_per_hour = Round(summary.CacheTurnoversPerHour),
            },
            note = PgTargetAdvice.BufferCompositionSentence(summary),
        };

        /* Re-freeze the card's prose with the composition sentence — see the summary. */
        var frozen = FactAdvice.TryReadStoryText(finding.StoryText);
        if (frozen is not null)
            finding.StoryText = FactAdvice.SerializeForStoryText(PgTargetAdvice.WithBufferComposition(frozen, summary));
    }

    private async Task<PgTargetBufferCompositionStatus> ReadBufferCacheExtensionStatusAsync(NpgsqlConnection connection, AnalysisContext context)
    {
        var states = new List<string>(4);
        using var cmd = new NpgsqlCommand(PgTargetBufferCacheExtensionSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            if (!reader.IsDBNull(0))
                states.Add(reader.GetString(0));
        }

        return ClassifyExtensionStates(states);
    }

    private async Task<DateTime?> ReadLastBufferCaptureAsync(NpgsqlConnection connection, AnalysisContext context)
    {
        using var cmd = new NpgsqlCommand(PgTargetBufferLastCaptureSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        var value = await cmd.ExecuteScalarAsync(context.CancellationToken);
        return value is DateTime at ? at : null;
    }

    /// <summary>
    /// The extension's state across the databases the collector saw, folded to the drill-down's arm: installed
    /// (or outdated — installed, older files) in ANY database means the collector can run and the gap is
    /// cadence; else <c>available</c> anywhere means one <c>CREATE EXTENSION</c> away; else <c>absent</c>;
    /// and no row at all is the cadence arm, because the pass did not read a state and does not claim one.
    /// </summary>
    internal static PgTargetBufferCompositionStatus ClassifyExtensionStates(IReadOnlyCollection<string> states)
    {
        ArgumentNullException.ThrowIfNull(states);
        if (states.Count == 0) return PgTargetBufferCompositionStatus.NoCapture;
        if (states.Any(s => string.Equals(s, "installed", StringComparison.OrdinalIgnoreCase) || string.Equals(s, "outdated", StringComparison.OrdinalIgnoreCase)))
            return PgTargetBufferCompositionStatus.NoCapture;
        if (states.Any(s => string.Equals(s, "available", StringComparison.OrdinalIgnoreCase)))
            return PgTargetBufferCompositionStatus.ExtensionAvailable;
        if (states.Any(s => string.Equals(s, "absent", StringComparison.OrdinalIgnoreCase)))
            return PgTargetBufferCompositionStatus.ExtensionAbsent;
        return PgTargetBufferCompositionStatus.NoCapture;
    }

    /// <summary>
    /// <c>pg_class.relkind</c> to the word the prose uses: <c>r</c>/<c>m</c> heap, <c>i</c> index, <c>t</c>
    /// TOAST, and anything else — including the NULL kind of a buffer the collector could not name — other.
    /// The same partition the SQL's kind totals use, so a row's word and the shares agree.
    /// </summary>
    internal static string KindLabel(string? relationKind) => relationKind switch
    {
        "r" or "m" => "heap",
        "i" => "index",
        "t" => "toast",
        _ => "other",
    };

    /// <summary>The read cut the name when the untruncated length exceeds what arrived; null and empty names are never "truncated".</summary>
    internal static bool IsTruncated(string? text, int untruncatedLength) =>
        !string.IsNullOrEmpty(text) && untruncatedLength > text.Length;

    /// <summary>A fraction, or null over an empty denominator — never a divide-by-zero and never a fabricated 0.</summary>
    internal static double? Share(long numerator, long denominator) =>
        denominator > 0 ? numerator / (double)denominator : null;

    /// <summary>Four places, or null through — the per-relation shares are rounded inline; this is for the nullable capture-wide ones.</summary>
    private static double? Round(double? value)
    {
        if (!value.HasValue)
            return null;
        return Math.Round(value.Value, 4);
    }
}
