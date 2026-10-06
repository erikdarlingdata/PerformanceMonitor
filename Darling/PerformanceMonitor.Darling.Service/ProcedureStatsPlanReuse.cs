/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
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

        /// <summary>
        /// Shadow: would-hits whose cached plan's SHAPE differs from the plan the run just rendered (#5158): a statement
        /// recompiled, or one came or went. A render whose shape held is <see cref="SameShape"/>, not this.
        /// </summary>
        public int FalseHit { get; set; }

        /// <summary>
        /// Shadow: would-hits whose rendered plan differs in bytes from the cached one but whose shape held. In the field
        /// only the memory grant moved: grant feedback adjusted the cached plan in place. The hash key proves the shape
        /// held, not that nothing else did. That is a minor mutation of a plan the store already holds, so it is not
        /// a new plan and not a false hit (#5158).
        /// </summary>
        public int SameShape { get; set; }

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
    /// The shape of a plan (#5158): every <c>Stmt*</c> element in document order (<c>StmtSimple</c>, <c>StmtCond</c>,
    /// <c>StmtCursor</c> and the rest), each as its element name and its <c>QueryPlanHash</c> (a dash when it has none:
    /// <c>SET</c>, <c>RETURN</c>, <c>ASSIGN</c> and <c>COND</c> wrappers carry no plan), hashed to one key. Two renders of
    /// the same cached plan have the same shape even when the engine's memory-grant feedback rewrote
    /// <c>MemoryGrantInfo</c> and the operators' <c>MemoryFractions</c> in place. A statement with no hash has no shape to
    /// compare, so it does not send the plan to the fallback. The fallback, a hash of the whole XML with the attributes of
    /// those two elements left out, runs only when a <c>QueryPlan</c> opens under a statement that has no hash (a
    /// shape the key cannot see) or when the plan has no statement. Returns null for XML that does not parse; the caller
    /// then cannot prove the shape unchanged. Forward-only: no DOM is built.
    /// </summary>
    internal static string? ShapeOf(string planXml)
    {
        ArgumentNullException.ThrowIfNull(planXml);

        var text = PgCollectorRowWriter.StripEmbeddedNuls(planXml);
        try
        {
            var statements = new StringBuilder();
            var count = 0;
            var lastHadHash = true;
            var unseenPlan = false;
            using (var reader = OpenReader(text))
            {
                while (reader.Read())
                {
                    if (reader.NodeType != XmlNodeType.Element)
                    {
                        continue;
                    }

                    var name = reader.LocalName;
                    if (name.StartsWith("Stmt", StringComparison.Ordinal))
                    {
                        count++;
                        var hash = reader.GetAttribute("QueryPlanHash");
                        lastHadHash = !string.IsNullOrEmpty(hash);
                        statements.Append(name).Append('=').Append(lastHadHash ? hash : "-").Append(',');
                    }
                    else if (name == "QueryPlan" && !lastHadHash)
                    {
                        unseenPlan = true;
                        break;
                    }
                }
            }

            if (!unseenPlan && count > 0)
            {
                return "h:" + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(statements.ToString())));
            }

            return "x:" + GrantlessDigestOf(text);
        }
        catch (XmlException)
        {
            return null;
        }
    }

    private static XmlReader OpenReader(string text) =>
        XmlReader.Create(
            new StringReader(text),
            new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit, XmlResolver = null, CheckCharacters = false });

    /// <summary>A hash of the plan's XML, element by element, leaving out the attributes grant feedback rewrites.</summary>
    private static string GrantlessDigestOf(string text)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        using var reader = OpenReader(text);
        while (reader.Read())
        {
            switch (reader.NodeType)
            {
                case XmlNodeType.Element:
                    AppendToken(hash, "<" + reader.LocalName);
                    if (reader.LocalName is not ("MemoryGrantInfo" or "MemoryFractions") && reader.MoveToFirstAttribute())
                    {
                        do
                        {
                            AppendToken(hash, " " + reader.Name + "=" + reader.Value);
                        }
                        while (reader.MoveToNextAttribute());
                        reader.MoveToElement();
                    }

                    break;
                case XmlNodeType.EndElement:
                    AppendToken(hash, ">");
                    break;
                case XmlNodeType.Text:
                case XmlNodeType.CDATA:
                    AppendToken(hash, "#" + reader.Value);
                    break;
                default:
                    break;
            }
        }

        return Convert.ToHexString(hash.GetHashAndReset());
    }

    private static readonly byte[] s_tokenSeparator = { 0 };

    private static void AppendToken(IncrementalHash hash, string token)
    {
        hash.AppendData(Encoding.UTF8.GetBytes(token));
        hash.AppendData(s_tokenSeparator);
    }

    /// <summary>Why shadow counted a would-hit as a false hit. Only <see cref="ShapeChanged"/> compared two shapes (#5158).</summary>
    internal enum FalseHitCause
    {
        /// <summary>Both shapes were known and they differ.</summary>
        ShapeChanged,
        /// <summary>The cached entry has no shape yet (cached by <c>on</c>, or after a parse failure), so nothing was compared.</summary>
        EntryHasNoShape,
        /// <summary>The render was over the size cap and has no text, so it has no shape to compare.</summary>
        OverCapRender,
        /// <summary>The render's XML did not parse, so it has no shape to compare.</summary>
        RenderUnparseable,
    }

    /// <summary>The words the shadow false-hit warning uses for each <see cref="FalseHitCause"/>.</summary>
    internal static string DescribeFalseHit(FalseHitCause cause) => cause switch
    {
        FalseHitCause.ShapeChanged => "the plan's shape changed",
        FalseHitCause.EntryHasNoShape => "the cached entry has no shape yet, so the shapes were not compared",
        FalseHitCause.OverCapRender => "the rendered plan is over the size cap, so the shapes were not compared",
        _ => "the rendered plan's XML did not parse, so the shapes were not compared",
    };

    /// <summary>
    /// Shadow: the rows already carry their inline plans. Looks each identity up, compares a would-hit with the
    /// plan just rendered, and warms the cache from the inline render. Changes no row.
    /// </summary>
    /// <param name="onFalseHit">Called with the identity of each would-hit counted as a false hit.</param>
    /// <param name="onFalseHitCause">Called beside <paramref name="onFalseHit"/> with the identity and which case it was (#5158):
    /// a shape that changed, or a case where no two shapes were compared.</param>
    internal static Outcome ApplyShadow(
        int serverId,
        PlanDigestCache<ProcedureStatsPlanKey> cache,
        IReadOnlyList<ProcedureStatsCollector.Row> rows,
        long captureOrdinal,
        DateTime nowUtc,
        Action<ProcedureStatsPlanKey>? onFalseHit,
        Action<ProcedureStatsPlanKey, FalseHitCause>? onFalseHitCause = null)
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
            string? shape = null;
            var shapeKnown = false;
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

                /* The bytes differ. It is a false hit only when the shape does (#5158): grant feedback adjusts a cached
                   plan in place, and that is not a new plan. A render with no text (over the cap) has no shape to
                   compare, and neither has an entry cached without one; those stay false hits, as before. */
                shape = row.QueryPlanXml is null ? null : ShapeOf(row.QueryPlanXml);
                shapeKnown = true;
                if (shape is not null && string.Equals(shape, entry.Shape, StringComparison.Ordinal))
                {
                    outcome.SameShape++;
                }
                else
                {
                    outcome.FalseHit++;
                    onFalseHit?.Invoke(key);
                    onFalseHitCause?.Invoke(key, row.QueryPlanXml is null ? FalseHitCause.OverCapRender
                        : shape is null ? FalseHitCause.RenderUnparseable
                        : entry.Shape is null ? FalseHitCause.EntryHasNoShape
                        : FalseHitCause.ShapeChanged);
                }
            }
            else
            {
                outcome.Miss++;
                if (!rendered)
                {
                    continue;
                }
            }

            /* The shape rides in the cache beside the digest, so the next render compares with it. */
            if (!shapeKnown && row.QueryPlanXml is not null)
            {
                shape = ShapeOf(row.QueryPlanXml);
            }

            cache.AddPending(key, inlineDigest, row.QueryPlanXmlBytes, nowUtc, captureOrdinal, shape);
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

                if (key is { } cacheKey)
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
