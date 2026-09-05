/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>Which physical tier answers a window: the raw hypertable, the hourly CAGG, or the daily CAGG.</summary>
public enum RetentionTier
{
    /// <summary>The raw hypertable — full per-sweep rows, including query text and plan XML.</summary>
    Raw,

    /// <summary>The hourly continuous aggregate — pre-summed per hour, no per-row text.</summary>
    Hourly,

    /// <summary>The daily continuous aggregate — pre-summed per day, kept indefinitely, no per-row text.</summary>
    Daily,
}

/// <summary>
/// Picks the retention tier that can actually answer a window, by the AGE of the window's oldest point.
///
/// <para>This is the single source of truth for that decision. It exists in Storage because BOTH readers need it
/// and neither can see the other: the composer lives in the service (<c>ComposeSourceRouter</c>, which delegates
/// its age decision here) and the viewer's built-in tabs live in the viewer, which does not reference the service.
/// Before #1661 only the composer routed, so the built-in tabs read the raw table exclusively and silently
/// returned ~4 days for any longer window once <see cref="TimescaleSupport.EnsureRetentionPoliciesAsync"/> began
/// dropping chunks.</para>
///
/// <para>Age is measured from a caller-supplied <c>nowUtc</c> rather than the window's end, because retention drops
/// chunks by actual wall-clock now: a purely historical window ("30 to 25 days ago") has to reach a tier that still
/// retains it or it comes back empty. The thresholds sit deliberately inside the matching retention horizon so a
/// route never lands on an about-to-drop chunk.</para>
///
/// <para><b>A rollup cannot serve per-row text.</b> The CAGGs group by identity columns and sum deltas — they carry
/// no <c>query_text</c> and no <c>query_plan</c>. A reader that projects either is limited to the raw horizon no
/// matter how wide the requested window is, which is what <see cref="RawTextHorizon"/> is for: clamp the window and
/// tell the user, rather than silently returning a short slice of what they asked for.</para>
///
/// <para>#1767 moved that text out of the fact row and into the <c>query_text_dim</c> / <c>query_plan_dim</c>
/// dimension tables, which changes nothing here: the CAGGs still carry no payload, and the dimension GC is pinned
/// to the FACT tables' horizon so a dim row never outlives the facts referencing it. The horizon therefore still
/// tracks the fact tables, not the dims.</para>
/// </summary>
public static class RetentionTierRouter
{
    /// <summary>
    /// How far inside a tier's retention horizon its route threshold sits. One day, applied identically to raw
    /// and hourly, so a window is never routed at a tier whose oldest chunk is about to be dropped underneath
    /// it. Small relative to every horizon it applies to, which is what lets one constant serve both.
    ///
    /// <para>Declared FIRST deliberately: static field initializers run in textual order, so a margin declared
    /// below the fields that subtract it would silently read <c>TimeSpan.Zero</c> and every route threshold
    /// would land exactly ON its horizon instead of inside it.</para>
    /// </summary>
    private static readonly TimeSpan RouteMargin = TimeSpan.FromDays(1);

    /// <summary>
    /// Raw answers windows whose oldest point is within this age — a day inside
    /// <see cref="TimescaleSupport.RawRetentionSpan"/>, so raw never routes to an about-to-drop chunk.
    /// </summary>
    public static readonly TimeSpan RawMaxAge = TimescaleSupport.RawRetentionSpan - RouteMargin;

    /// <summary>
    /// The hourly CAGG answers up to this age — a day inside
    /// <see cref="TimescaleSupport.HourlyRetentionSpan"/>, so a read never routes to an about-to-drop chunk;
    /// older windows fall to the daily CAGG, which has no retention policy and is kept indefinitely.
    ///
    /// <para><b>DERIVED, since #1937, and that was the whole bug.</b> This was a hardcoded 20 days sitting a
    /// day under a hardcoded 21-day horizon — two independent literals that agreed only because someone kept
    /// them agreeing. Raising retention to 90 while this stayed at 20 would have changed nothing an operator
    /// could see: a 30-day window would still have routed straight past the hourly tier to daily, which is
    /// EXACTLY the complaint (#1937) that moved the horizon in the first place. The margin is now the only
    /// number written down; the horizon it hangs off is read.</para>
    /// </summary>
    public static readonly TimeSpan HourlyMaxAge = TimescaleSupport.HourlyRetentionSpan - RouteMargin;

    /// <summary>
    /// How far back per-row text (<c>query_text</c>, <c>query_plan</c>) actually exists. Identical to
    /// <see cref="RawMaxAge"/> — text is reachable only from raw, whether stored inline or resolved through
    /// the #1767 dimension tables — but named separately because it means something different to a caller:
    /// not "which relation do I read" but "how much of the user's requested window can I honestly answer at
    /// all".
    /// </summary>
    public static TimeSpan RawTextHorizon => RawMaxAge;

    /// <summary>
    /// The tier that can answer a window whose oldest point is <paramref name="windowStartUtc"/>, as of
    /// <paramref name="nowUtc"/>. A window starting in the future (or now) is Raw.
    /// </summary>
    public static RetentionTier Resolve(DateTime nowUtc, DateTime windowStartUtc)
    {
        var age = nowUtc - windowStartUtc;

        if (age <= RawMaxAge)
        {
            return RetentionTier.Raw;
        }

        return age <= HourlyMaxAge ? RetentionTier.Hourly : RetentionTier.Daily;
    }

    /// <summary>
    /// The availability-gated form (#1664): the age decision of <see cref="Resolve(DateTime, DateTime)"/>,
    /// degraded to what the store actually HAS. Age alone is only half the answer — the rollups are runtime
    /// TimescaleDB setup, so a plain-PostgreSQL store never has them (and never drops raw either, making raw
    /// both available AND complete there), and a partially-built TimescaleDB store may have one tier but not
    /// the other. Degrade mirrors <c>ComposeSourceRouter</c>: a daily-age window with no daily view falls to
    /// the hourly view (capped at its horizon); a window whose resolved tier's view is missing falls to raw.
    /// Callers get the flags from <see cref="TimescaleSupport.DetectRollupsAsync"/>, picking the grain pair
    /// their query reads.
    /// </summary>
    public static RetentionTier Resolve(DateTime nowUtc, DateTime windowStartUtc, bool hourlyAvailable, bool dailyAvailable)
    {
        var tier = Resolve(nowUtc, windowStartUtc);

        if (tier == RetentionTier.Daily && !dailyAvailable)
        {
            tier = RetentionTier.Hourly;
        }

        if (tier == RetentionTier.Hourly && !hourlyAvailable)
        {
            tier = RetentionTier.Raw;
        }

        return tier;
    }

    /// <summary>
    /// The COVERAGE-gated form (#1759): the age + availability decision above, degraded again to what each
    /// tier has actually MATERIALIZED. A rollup created <c>WITH NO DATA</c> over pre-existing history serves
    /// only its materialized span — real-time aggregation cannot rescue the rest, because the watermark is a
    /// hard partition (materialized below <c>UNION ALL</c> raw at-or-above) rather than a fallback, so raw
    /// older than the watermark is excluded by construction. Age alone therefore routed windows onto rollups
    /// that could not answer them and returned EMPTY while raw still held every row.
    ///
    /// <para>The rule is comparative, never absolute: a tier is abandoned only when a LOWER tier is measured
    /// to reach further back, so the fallback fires on the held-purge shape #1759 describes and stays silent
    /// on a healthy store, where raw keeps ~4 days against the rollups' weeks and dropping to raw would
    /// return LESS than staying put. See <see cref="TierCoverage.ReachesFurtherBack"/>.</para>
    ///
    /// <para>Daily degrades to hourly before either degrades to raw, mirroring the availability ladder. That
    /// step matters for exactly one live shape: a daily rollup still empty while its hourly has materialized
    /// (the transient state after a rollup pair is created, before the daily policy's first run). Without it
    /// a 30-day window there would skip a populated hourly tier to reach a 4-day raw one.</para>
    ///
    /// <para>Unknown coverage is INERT by construction — see <see cref="TierCoverage"/>. A failed probe, a
    /// plain-PostgreSQL store and a partially-built one all produce nulls, and nulls never move a window.</para>
    ///
    /// <para><b>THE FLOOR IS A DEPTH MEASURE, NOT A COMPLETENESS GUARANTEE.</b> A routed tier is the best
    /// available answer for a window; it is not a promise that the window is fully materialized. Two shapes
    /// slip through, both known and both accepted:</para>
    ///
    /// <para><i>A mid-window HOLE is invisible.</i> Coverage is one number — the oldest materialized bucket —
    /// so it cannot see a gap ABOVE itself. A service down longer than the refresh policy's
    /// <c>start_offset</c> (<see cref="TimescaleSupport.HourlyRefreshStartOffset"/> since #3012 narrowed it,
    /// so this exposure is WIDER than it was) resumes at now minus that offset and never backfills the
    /// skipped interval, while
    /// <c>min(bucket)</c> goes on reporting the original deep floor. The window then routes to the tier and is
    /// served as complete with the gap silently inside it.</para>
    ///
    /// <para><i>A window STRADDLING the floor is served partially, with no signal to the caller.</i> When the
    /// window starts below the floor but no lower tier reaches further back, this returns the tier — which is
    /// the correct CHOICE (raw would return less on a healthy store), but the part of the window below the
    /// floor is missing and nothing here tells the caller so.</para>
    ///
    /// <para>Both are accepted because both are strictly better than the age-only routing they replace, which
    /// served the ENTIRE window as empty in exactly these cases. What must NOT be inferred is the belief this
    /// paragraph exists to block: <b>"the coverage gate passed, therefore the result is complete."</b> That
    /// belief-shape is precisely how #1759 survived as long as it did — the previous comment here asserted the
    /// view was "correct to query for any window immediately", it read as reasonable, and nobody re-derived it
    /// for months. Do not re-derive it. Closing the two gaps properly means a two-dimensional coverage probe
    /// (floor plus gap detection) and a partial-coverage notice for the straddle case; both are tracked
    /// separately rather than assumed away here.</para>
    /// </summary>
    public static RetentionTier Resolve(
        DateTime nowUtc,
        DateTime windowStartUtc,
        bool hourlyAvailable,
        bool dailyAvailable,
        TierCoverage coverage)
    {
        var tier = Resolve(nowUtc, windowStartUtc, hourlyAvailable, dailyAvailable);

        /* Raw is the floor of the ladder: it is the fallback, so it is never itself degraded. */
        if (tier == RetentionTier.Raw)
        {
            return tier;
        }

        var floor = tier == RetentionTier.Daily ? coverage.DailyFloorUtc : coverage.HourlyFloorUtc;
        if (TierCoverage.Covers(floor, windowStartUtc))
        {
            return tier;
        }

        /* The chosen rollup cannot answer the oldest part of the window. Prefer the deepest tier that can. */
        if (tier == RetentionTier.Daily
            && hourlyAvailable
            && TierCoverage.ReachesFurtherBack(coverage.HourlyFloorUtc, floor))
        {
            tier = RetentionTier.Hourly;
            floor = coverage.HourlyFloorUtc;

            if (TierCoverage.Covers(floor, windowStartUtc))
            {
                return tier;
            }
        }

        /* #1939: everything above escalates toward FINER grain, so a window past the hourly floor used to be
           served PARTIALLY at hourly — silently missing its oldest part — even when the daily tier held the
           COMPLETE window. Complete beats partial: a month view missing its older weeks is the #1937 complaint
           wearing routing clothes, and the field case is every store upgraded across a horizon raise, whose
           hourly tier spends weeks regrowing while HourlyMaxAge already points far past its floor. The arm
           self-limits on the degraded path: when HOURLY was reached by falling from an age-picked DAILY, the
           daily floor has already failed Covers above, so this cannot bounce back and forth. */
        if (tier == RetentionTier.Hourly
            && dailyAvailable
            && TierCoverage.Covers(coverage.DailyFloorUtc, windowStartUtc))
        {
            return RetentionTier.Daily;
        }

        return TierCoverage.ReachesFurtherBack(coverage.RawOldestUtc, floor) ? RetentionTier.Raw : tier;
    }

    /// <summary>
    /// The earliest instant a reader that projects per-row text can honestly cover, as of
    /// <paramref name="nowUtc"/>. Callers clamp their window start to this and surface the clamp.
    /// </summary>
    public static DateTime OldestTextInstant(DateTime nowUtc) => nowUtc - RawTextHorizon;

    /// <summary>
    /// Clamps <paramref name="windowStartUtc"/> to the text horizon. <c>clamped</c> is true when the caller asked
    /// for more history than per-row text exists for, which is the signal to tell the user rather than quietly
    /// narrowing what they requested.
    /// </summary>
    public static (DateTime EffectiveStartUtc, bool Clamped) ClampToTextHorizon(DateTime nowUtc, DateTime windowStartUtc)
    {
        var oldest = OldestTextInstant(nowUtc);
        return windowStartUtc < oldest ? (oldest, true) : (windowStartUtc, false);
    }
}
