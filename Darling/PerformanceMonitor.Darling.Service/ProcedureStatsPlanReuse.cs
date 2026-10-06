/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The host's decisions about <c>procedure_stats</c> plans after the main read (#5158): which module plans the
/// store already holds, which still need rendering, and, in shadow mode, whether the plan identity
/// (<see cref="ProcedureStatsPlanKey"/>) would have been right about the plans the run just rendered inline.
///
/// <para>Separate from the runner so the decisions run without a monitored server: the plan fetch is a delegate.
/// The runner owns the connection, the budget and the counters; this owns the rows and the cache.</para>
///
/// <para><b>Nothing is trusted before a commit.</b> Every plan this pass renders is added to the cache as pending;
/// the runner confirms the returned keys after the batch commits, or discards them.</para>
/// </summary>
internal static class ProcedureStatsPlanReuse
{
    /// <summary>
    /// The most module plans one run renders in the second query, taken in row order. Today's inline capture renders at
    /// most as many. This is a guard only: the main query is already <c>TOP (150)</c>, so at most 150 rows can miss and
    /// <c>deferred_over_cap = 0</c> is what to expect. A zero there is not evidence that anything was tested.
    /// </summary>
    internal const int MaxMissesPerRun = 150;

    /// <summary>
    /// How many capture cycles an identity may go without being rendered again before a hit is refused. The
    /// fingerprint is the control that notices a recompile; this bounds what it cannot see.
    /// </summary>
    internal const int TtlCaptureCycles = 4;

    /// <summary>The second query: renders the plans for these handles, keyed by the handle's position in the list.</summary>
    internal delegate Task<Dictionary<int, (string? PlanXml, long? Bytes)>> PlanFetch(
        IReadOnlyList<byte[]> planHandles, CancellationToken cancellationToken);

    /// <summary>What one pass did. Counts are per run.</summary>
    internal sealed class Outcome
    {
        /// <summary>Keys this pass added to the cache as pending: confirm after the commit, discard if the write failed.</summary>
        public List<ProcedureStatsPlanKey> Pending { get; } = new();

        /// <summary>On: rows that carried a cached digest instead of a plan.</summary>
        public int Hit { get; set; }

        /// <summary>Shadow: rows whose identity was in the cache.</summary>
        public int WouldHit { get; set; }

        /// <summary>Shadow: would-hits whose cached plan differs from the plan the run just rendered.</summary>
        public int FalseHit { get; set; }

        /// <summary>Rows the cache did not hold, or held past its age limit, or could not key.</summary>
        public int Miss { get; set; }

        /// <summary>Plans rendered: inline in shadow, in the second query in on.</summary>
        public int Rendered { get; set; }

        /// <summary>The measured size of the rendered plans, over-cap plans included.</summary>
        public long RenderedBytes { get; set; }

        /// <summary>On: misses beyond <see cref="MaxMissesPerRun"/>; they ship no plan this run.</summary>
        public int OverCap { get; set; }

        /// <summary>The second query failed; the rows it covered ship no plan and nothing from it is cached.</summary>
        public Exception? FetchFailure { get; set; }
    }

    /// <summary>True when the entry was rendered more than <see cref="TtlCaptureCycles"/> capture cycles ago.</summary>
    internal static bool IsExpired(PlanDigestEntry entry, long captureOrdinal) =>
        captureOrdinal - entry.RenderedOrdinal > TtlCaptureCycles;

    /// <summary>The store's digest of a plan: what the writer will compute from the same text.</summary>
    internal static string DigestOf(string planXml) =>
        Convert.ToHexString(PayloadDimensions.Digest(PgCollectorRowWriter.StripEmbeddedNuls(planXml)));

    /// <summary>
    /// Shadow: the rows already carry their inline plans. Looks each identity up, compares a would-hit with the
    /// plan just rendered, and warms the cache from the inline render. Changes no row.
    /// </summary>
    /// <param name="onFalseHit">Called with the identity of each would-hit whose cached plan differs.</param>
    internal static Outcome ApplyShadow(
        int serverId,
        PlanDigestCache<ProcedureStatsPlanKey> cache,
        IReadOnlyList<ProcedureStatsCollector.Row> rows,
        long captureOrdinal,
        DateTime nowUtc,
        Action<ProcedureStatsPlanKey>? onFalseHit)
    {
        var outcome = new Outcome();
        foreach (var row in rows)
        {
            var rendered = row.QueryPlanXml is not null || row.QueryPlanXmlBytes is not null;
            if (rendered)
            {
                outcome.Rendered++;
                outcome.RenderedBytes += row.QueryPlanXmlBytes ?? 0;
            }

            if (ProcedureStatsPlanKey.TryCreate(serverId, row) is not { } key)
            {
                outcome.Miss++;
                continue;
            }

            var inlineDigest = row.QueryPlanXml is null ? null : DigestOf(row.QueryPlanXml);
            if (cache.TryGet(key, nowUtc, out var entry) && !IsExpired(entry, captureOrdinal))
            {
                outcome.WouldHit++;
                if (!rendered)
                {
                    continue; /* nothing was rendered to compare with */
                }

                var same = string.Equals(inlineDigest, entry.Digest, StringComparison.OrdinalIgnoreCase)
                    && (inlineDigest is not null || entry.Bytes == row.QueryPlanXmlBytes);
                if (same)
                {
                    continue;
                }

                outcome.FalseHit++;
                onFalseHit?.Invoke(key);
            }
            else
            {
                outcome.Miss++;
                if (!rendered)
                {
                    continue;
                }
            }

            cache.AddPending(key, inlineDigest, row.QueryPlanXmlBytes, nowUtc, captureOrdinal);
            outcome.Pending.Add(key);
        }

        return outcome;
    }

    /// <summary>
    /// On: the rows carry no plan. A recognized identity gets its cached digest and size; the rest are rendered by
    /// <paramref name="fetch"/> when this is a capture cycle, at most <see cref="MaxMissesPerRun"/> in row order,
    /// and on a cycle the cadence gate skips they ship no plan, as before. A row that cannot be keyed is rendered
    /// like a miss but never cached.
    /// </summary>
    internal static async Task<Outcome> ApplyOnAsync(
        int serverId,
        PlanDigestCache<ProcedureStatsPlanKey> cache,
        List<ProcedureStatsCollector.Row> rows,
        bool captureCycle,
        long captureOrdinal,
        DateTime nowUtc,
        PlanFetch fetch,
        CancellationToken cancellationToken)
    {
        var outcome = new Outcome();
        var candidates = new List<(int RowIndex, ProcedureStatsPlanKey? Key, byte[] Handle)>();

        for (var i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            var key = ProcedureStatsPlanKey.TryCreate(serverId, row);
            if (key is { } k && cache.TryGet(k, nowUtc, out var entry) && !IsExpired(entry, captureOrdinal))
            {
                /* The cached size rides along with the digest. A null digest is an over-cap plan: the size and no plan. */
                rows[i] = row with { KnownPlanDigest = entry.Digest, QueryPlanXmlBytes = entry.Bytes };
                outcome.Hit++;
                continue;
            }

            outcome.Miss++;
            if (captureCycle && ProcedureStatsCollector.TryParsePlanHandle(row.PlanHandle, out var handle))
            {
                candidates.Add((i, key, handle));
            }
        }

        if (candidates.Count > MaxMissesPerRun)
        {
            outcome.OverCap = candidates.Count - MaxMissesPerRun;
            candidates.RemoveRange(MaxMissesPerRun, candidates.Count - MaxMissesPerRun);
        }

        if (candidates.Count == 0)
        {
            return outcome;
        }

        try
        {
            var fetched = await fetch(candidates.ConvertAll(c => c.Handle), cancellationToken);
            foreach (var (ord, result) in fetched)
            {
                if ((uint)ord >= (uint)candidates.Count)
                {
                    continue;
                }

                var (rowIndex, key, _) = candidates[ord];
                if (result.PlanXml is null && result.Bytes is null)
                {
                    continue; /* aged out between the two queries: no plan and no size, nothing worth caching */
                }

                outcome.Rendered++;
                outcome.RenderedBytes += result.Bytes ?? 0;
                rows[rowIndex] = rows[rowIndex] with { QueryPlanXml = result.PlanXml, QueryPlanXmlBytes = result.Bytes };

                /* #4348: a plan the filter withheld whole (its budget ran out) is stored as the marker for this cycle but
                   never cached, so the next cycle fetches and filters it again. */
                if (key is { } cacheKey && !QueryStatsCollector.IsWithheldWhole(result.PlanXml))
                {
                    cache.AddPending(
                        cacheKey, result.PlanXml is null ? null : DigestOf(result.PlanXml), result.Bytes, nowUtc, captureOrdinal);
                    outcome.Pending.Add(cacheKey);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException and not OutOfMemoryException
                                   && !cancellationToken.IsCancellationRequested)
        {
            outcome.FetchFailure = ex;
        }

        return outcome;
    }
}
