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

namespace PerformanceMonitor.Collectors;

/// <summary>The result of advancing a text watermark: see <see cref="QueryStoreTextState.AdvanceWatermark"/>.</summary>
public readonly record struct TextWatermarkAdvance(long Watermark, bool ArrivedInQueryIdOrder);

/// <summary>
/// Per-database watermark for the query-text fetch (#2150), the sibling of
/// <see cref="QueryStorePlanXmlState"/> — same encoding, same conservative-zero rules, same
/// never-backward advance.
///
/// <para><b>Why this exists.</b> The runtime-stats payload carried <c>query_sql_text</c>
/// (<c>nvarchar(max)</c>) inside a <c>TOP ... WITH TIES ... ORDER BY last_execution_time</c> projection.
/// A Top-N Sort carries every output column through the sort and reads ALL of its input before emitting
/// a row, so choosing the rows to ship materialized the text for the entire qualifying set. Measured on
/// a purpose-built Azure SQL DB store with the plan XML already removed by #2210 — the only difference
/// being that one column — time-to-first-row was <b>4.67s vs 0.45s</b> at 1,505 rows / 12.8 MB of text
/// and <b>5.02s vs 0.57s</b> at 4,037 rows / 34 MB, with full drain <b>8.06s vs 0.50s</b> and
/// <b>16.95s vs 1.45s</b>. Neither the row cap nor the client byte budget can bound that: TOP (500)
/// measured identical to TOP (50000), and wall time was flat across a 4 MB → 256 MB budget sweep,
/// because the server finishes before the client sees a byte.</para>
///
/// <para><b>Why a watermarked fetch rather than a per-pass dedupe.</b> That path was already tried on the
/// plan side and abandoned: #1556's <c>ROW_NUMBER</c> gate shipped each plan once per PASS, and #2164
/// replaced it precisely because "the ROW_NUMBER gate ships each plan once per pass but re-ships it every
/// pass forever, and since drain is 94-97% of a pass and is per-row LOB cost, NOT fetching is worth far
/// more than fetching less." #2210 then took the column out of the stream entirely.
/// <c>query_id</c> is an identity, monotonic within a database, so the same shape applies: fetch a
/// statement's text ONCE, ever.</para>
///
/// <para><b>Keyed on <c>query_id</c>, not <c>query_text_id</c>, and that is what keeps this cheap.</b>
/// <c>query_id</c> is ALREADY a stored payload column on the runtime row, so readers get the join key for
/// free and the fact table needs no new column and no migration. Keying on <c>query_text_id</c> would have
/// required adding it to the payload — a schema change — to buy de-duplication across the handful of
/// <c>query_id</c>s that share one text (a <c>query_id</c> is per text PLUS context settings, so the two
/// are close to 1:1 in practice). Storing a rare duplicate is the cheaper side of that trade.</para>
///
/// <para><b>What this deliberately does NOT mirror, and why.</b> The plan side carries a whole candidate-
/// window estimator (<c>FirstContactAvgPlanBytes</c>, min/max clamps, an observed-average learning loop)
/// because <c>SUM(DATALENGTH(query_plan)) OVER (ORDER BY plan_id)</c> forces the server to DECOMPRESS
/// every plan in the window — <c>sys.query_store_plan.query_plan</c> is decompressed by the view on
/// access. <c>sys.query_store_query_text.query_sql_text</c> is not, so its <c>DATALENGTH</c> is cheap and
/// the window needs no estimate at all: a flat coarse bound plus the exact running-byte total is enough.
/// The plan side also re-verifies content hashes because plan XML can be rewritten in place; a
/// <c>query_text_id</c> maps to fixed text forever — a changed statement is a new id — so there is
/// nothing to re-verify and no content digest to track.</para>
/// </summary>
public static class QueryStoreTextState
{
    /// <summary>
    /// The collector name the watermark is stored under. Separate from the plan fetch's own state so the
    /// two advance independently: they walk different catalogs at different rates, and sharing a key would
    /// let a plan-side reset drop the text watermark (and vice versa) for no reason.
    /// </summary>
    public const string StateCollectorName = "query_store_text";

    /// <summary>Prefix for the per-database state key.</summary>
    public const string WatermarkKeyPrefix = "textwm:";

    /// <summary>
    /// How long a watermark stands before a full re-walk. Matched to the plan side's one day rather than
    /// tuned separately, so an operator reasoning about one fetch reasons about both — and the term that
    /// made the plan side's choice tight does not apply here: expiry means a budgeted catalog walk, and
    /// text is roughly an order of magnitude smaller per row than plan XML (8.5 KB against 195 KB on the
    /// measured store), so the walk this horizon triggers is correspondingly cheaper.
    ///
    /// <para>The re-walk is not decoration. <c>query_id</c> is monotonic in FIRST-SEEN order, not in "we
    /// have stored it", so two things arrive below a standing watermark: a statement first seen before
    /// monitoring began and only executed later, and — the one that matters — a Query Store reset, which
    /// renumbers ids from the start. Without a bounded horizon a reset would suppress every text forever.</para>
    /// </summary>
    public static readonly TimeSpan RefreshAfter = TimeSpan.FromDays(1);

    /// <summary>
    /// How many texts one pass may CONSIDER. A flat bound, not an estimate: the running byte total is the
    /// exact constraint and <c>DATALENGTH(query_sql_text)</c> is cheap to evaluate, so this only has to be
    /// large enough that the budget binds first and small enough that a pass never windows an entire
    /// catalog. At the 12 MB default ship budget this covers texts averaging under ~2.5 KB, which is
    /// comfortably below what a fragmenting literal-heavy statement produces.
    /// </summary>
    public const int CandidateTexts = 5_000;

    /// <summary>The state key for one database.</summary>
    public static string KeyFor(string databaseName) => WatermarkKeyPrefix + databaseName;

    /// <summary>
    /// The highest <c>query_id</c> landed, or the standing watermark when a pass lands nothing.
    ///
    /// <para>Reports whether the ids arrived in <c>query_id</c> order, because that ordering is what
    /// makes a budget cut a SUFFIX — everything up to the cut is stored, so the highest stored id is a
    /// safe resume point. Out of order, that argument collapses and the caller must hold the watermark
    /// rather than advance past statements whose text it never stored.</para>
    ///
    /// <para>Never moves backward. A pass landing nothing, or only ids at or below the standing watermark,
    /// is an ordinary quiet pass — not a reset — and lowering the watermark would refetch the catalog.</para>
    /// </summary>
    public static TextWatermarkAdvance AdvanceWatermark(long standing, IReadOnlyList<long> landedQueryIdsInOrder)
    {
        if (landedQueryIdsInOrder is null || landedQueryIdsInOrder.Count == 0)
        {
            return new TextWatermarkAdvance(standing, true);
        }

        var advanced = standing;
        var previous = long.MinValue;

        foreach (var queryId in landedQueryIdsInOrder)
        {
            if (queryId < previous)
            {
                return new TextWatermarkAdvance(standing, false);
            }

            previous = queryId;

            if (queryId > advanced)
            {
                advanced = queryId;
            }
        }

        return new TextWatermarkAdvance(advanced, true);
    }

    /// <summary>
    /// The watermark to apply for one database, or 0 — meaning "fetch every text" — for an absent,
    /// malformed, EXPIRED or future-stamped one. Zero is the conservative path, and all three of a first
    /// run, a restarted host and a broken store look identical from here: every one of them must refetch
    /// rather than skip. A future stamp means the clock moved backwards, which would otherwise pin the
    /// watermark for as long as the skew lasts.
    /// </summary>
    public static long Resolve(IReadOnlyDictionary<string, string> state, string databaseName, DateTime utcNow)
    {
        if (!TryParse(state, databaseName, out var textId, out var stamped))
        {
            return 0;
        }

        if (stamped > utcNow || utcNow - stamped >= RefreshAfter)
        {
            return 0;
        }

        return textId;
    }

    /// <summary>
    /// The stored stamp — when this database last did a FULL text fetch — with no expiry applied, so a
    /// write-back can carry it forward across an advance instead of renewing the refresh horizon. Null
    /// when there is nothing parseable to carry, which the caller treats as "stamp now".
    /// </summary>
    public static DateTime? ResolveStamp(IReadOnlyDictionary<string, string> state, string databaseName) =>
        TryParse(state, databaseName, out _, out var stamped) ? stamped : null;

    /// <summary>
    /// Formats a watermark for storage: highest stored <c>query_id</c> plus the stamp dating the last
    /// FULL fetch. The stamp is a parameter rather than "now" precisely because it must survive advances —
    /// re-stamping on every advance would push the horizon out forever on any database that keeps seeing
    /// new statements, which is exactly where a reset would hurt most, and the bounded re-walk would never
    /// fire.
    /// </summary>
    public static string Format(long textId, DateTime fullFetchAtUtc) =>
        textId.ToString(CultureInfo.InvariantCulture) + ":" +
        new DateTimeOffset(DateTime.SpecifyKind(fullFetchAtUtc, DateTimeKind.Utc)).ToUnixTimeSeconds()
            .ToString(CultureInfo.InvariantCulture);

    private static bool TryParse(
        IReadOnlyDictionary<string, string> state, string databaseName, out long textId, out DateTime stamped)
    {
        textId = 0;
        stamped = default;

        if (state is null || !state.TryGetValue(KeyFor(databaseName), out var raw) || string.IsNullOrWhiteSpace(raw))
        {
            return false;
        }

        var split = raw.IndexOf(':');
        if (split <= 0 || split == raw.Length - 1)
        {
            return false;
        }

        if (!long.TryParse(raw.AsSpan(0, split), NumberStyles.Integer, CultureInfo.InvariantCulture, out textId)
            || textId < 0)
        {
            textId = 0;
            return false;
        }

        if (!long.TryParse(raw.AsSpan(split + 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out var unix))
        {
            textId = 0;
            return false;
        }

        try
        {
            stamped = DateTimeOffset.FromUnixTimeSeconds(unix).UtcDateTime;
        }
        catch (ArgumentOutOfRangeException)
        {
            textId = 0;
            return false;
        }

        return true;
    }
}
