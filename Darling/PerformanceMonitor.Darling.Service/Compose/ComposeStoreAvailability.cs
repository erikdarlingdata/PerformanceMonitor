/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The compose runner's cached answer to "which retention rollups does this store have?" (#1665 — the
/// composer's arm of the #1664 availability guard), plus the "partial window, and says so" notice for the
/// routes that availability (or the value-remap/dimension-coverage gates) forced below what the window's age
/// wanted. Caching mirrors the viewer's <c>GetRollupAvailabilityAsync</c> semantics exactly: once every
/// rollup exists the answer is cached for the store's lifetime (a created CAGG is never dropped outside the
/// service's own reshape sweep); while the store reports none/partial, re-probe at most every
/// <see cref="ReprobeInterval"/> so an ensure sweep finishing mid-session converges without a restart; a
/// failed probe answers <see cref="RollupAvailability.None"/> — raw always exists, so "route everything to
/// raw" is the never-wrong fallback. Keyed per data source (not a bare static) so gated-live tests spinning
/// several stores in one process can never bleed one store's shape into another's.
/// </summary>
internal static class ComposeStoreAvailability
{
    private sealed class Entry
    {
        public RollupAvailability Rollups;
        public RollupCoverage Coverage = RollupCoverage.Unknown;
        public bool Probed;
        public DateTime ProbedAtUtc;
    }

    private static readonly ConditionalWeakTable<NpgsqlDataSource, Entry> s_entries = new();

    /// <summary>While the store reports no (or partial) rollups, re-probe at most this often — the same
    /// convergence interval the viewer uses. Since #1759 this ALSO bounds how stale a coverage floor can
    /// get, which is what lets a backfill be picked up without restarting the service.</summary>
    internal static readonly TimeSpan ReprobeInterval = TimeSpan.FromMinutes(5);

    /// <summary>
    /// The store's rollup availability AND each rollup's materialized-coverage floor, probed lazily and
    /// cached per data source. Benignly racy: concurrent panel runs may probe twice; the probes are two
    /// small catalog/aggregate lookups and last-write-wins caches the same answer.
    ///
    /// <para><b>Coverage expires even when availability does not (#1759).</b> Availability is permanent once
    /// complete — a created aggregate is never dropped outside the service's own reshape sweep — but a
    /// coverage floor MOVES: a <c>--backfill-rollups</c> run pushes it backwards, a retention drop pushes it
    /// forwards. Caching the pair on availability's permanent terms would pin a pre-backfill floor for the
    /// life of the process, so an operator who just backfilled would keep getting raw fallbacks until a
    /// restart. The <see cref="ReprobeInterval"/> TTL therefore applies unconditionally, and the
    /// <c>AllPresent</c> shortcut is deliberately gone.</para>
    /// </summary>
    internal static async ValueTask<(RollupAvailability Rollups, RollupCoverage Coverage)> GetRollupsAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var entry = s_entries.GetOrCreateValue(postgres);

        lock (entry)
        {
            if (entry.Probed && DateTime.UtcNow - entry.ProbedAtUtc < ReprobeInterval)
            {
                return (entry.Rollups, entry.Coverage);
            }
        }

        RollupAvailability rollups;
        RollupCoverage coverage;
        try
        {
            rollups = await TimescaleSupport.DetectRollupsAsync(postgres, cancellationToken);
            coverage = await TimescaleSupport.DetectRollupCoverageAsync(postgres, rollups, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A store hiccup mid-probe must not fail the panel — raw is the safe answer for availability,
               and "no coverage evidence" leaves the age ladder in charge. The re-probe interval retries soon. */
            rollups = RollupAvailability.None;
            coverage = RollupCoverage.Unknown;
            _ = ex;
        }

        lock (entry)
        {
            entry.Rollups = rollups;
            entry.Coverage = coverage;
            entry.Probed = true;
            entry.ProbedAtUtc = DateTime.UtcNow;
        }

        return (rollups, coverage);
    }

    /// <summary>
    /// The caller-facing "partial window" notice for a compiled panel, or null when the route can honestly
    /// serve the whole window. Fires when the route landed on a tier whose RETENTION cannot reach the
    /// window's start on a retention-active store: raw keeps ~<see cref="TimescaleSupport.RawRetentionInterval"/>,
    /// the hourly CAGG ~<see cref="TimescaleSupport.HourlyRetentionInterval"/>, the daily CAGG everything.
    /// Deliberately states the FACT (older points are missing) without guessing the cause — the same raw
    /// route is reached by a missing rollup, a value expression the CAGG can't remap, or an uncovered
    /// dimension, and all of them truncate identically. On a store with no rollups at all
    /// (<see cref="RollupAvailability.None"/> — plain PostgreSQL, or a failed probe) there is no notice:
    /// without the extension no retention policy ever drops raw, so raw holds the complete answer (#1665).
    /// Scoped to the TIERED tables (<see cref="ComposeCaggCatalog"/>): every other source reaches Raw via
    /// the no-CAGG early return and lives on the 30-day collector purge, not the 4-day tier — a 7-day
    /// wait_stats panel is complete on raw, and a notice there would be a false alarm.
    ///
    /// <para><b>MEASURED beats assumed (#1759).</b> The retention SPAN is only a proxy for how far a tier
    /// reaches, and on the stores #1759 is about it is the wrong proxy in the dangerous direction: their raw
    /// purges are HELD PAUSED by the #1680 arming gate, so raw holds months rather than the ~4 days its
    /// policy nominally keeps. Left assuming, this would stamp "older points are not included" on precisely
    /// the coverage-fallback panels that are in fact COMPLETE — a false alarm introduced by the fix. So when
    /// <paramref name="coverage"/> carries a real floor for the routed tier, that floor decides; the span is
    /// the fallback for an unmeasured store, which reproduces the pre-#1759 text exactly.</para>
    /// </summary>
    internal static string? BuildRetentionNotice(
        string sourceTable, ComposeRoute route, DateTime windowStartUtc, DateTime nowUtc,
        RollupAvailability rollups, RollupCoverage coverage)
    {
        var cagg = ComposeCaggCatalog.For(sourceTable);
        if (rollups == RollupAvailability.None || cagg is null)
        {
            return null;
        }

        ArgumentNullException.ThrowIfNull(coverage);

        TimeSpan retained;
        string tierName;
        DateTime? measuredOldest;
        switch (route.Tier)
        {
            case ComposeSourceTier.Raw:
                retained = TimescaleSupport.RawRetentionSpan;
                tierName = "raw";
                measuredOldest = RollupCoverage.RawTableFor(cagg.HourlyView) is string rawTable
                    ? coverage.RawOldestOf(rawTable)
                    : null;
                break;
            case ComposeSourceTier.Hourly:
                retained = TimescaleSupport.HourlyRetentionSpan;
                tierName = "hourly rollup";
                measuredOldest = coverage.FloorOf(cagg.HourlyView);
                break;
            default:
                /* Daily is kept indefinitely, so its RETENTION always fits — but its COVERAGE need not, and
                   a daily rollup only reached by the router because nothing below it was measurably deeper
                   can still start after the window does (#1759). Measured only: with no floor there is
                   nothing to report, which is the pre-#1759 behaviour. */
                retained = TimeSpan.MaxValue;
                tierName = "daily rollup";
                measuredOldest = cagg.DailyView is null ? null : coverage.FloorOf(cagg.DailyView);
                if (measuredOldest is null)
                {
                    return null;
                }

                break;
        }

        var oldestHeld = measuredOldest ?? nowUtc - retained;
        if (windowStartUtc >= oldestHeld)
        {
            return null;
        }

        var windowDays = (nowUtc - windowStartUtc).TotalDays;
        var heldDays = (nowUtc - oldestHeld).TotalDays;
        /* Pluralise on the RENDERED number, not the raw double: heldDays formats as a whole number and windowDays
           to one decimal, so "1" is singular ("1 day") while "1.5" and "30" are plural — a one-day store no
           longer reads "reaches back about 1 days". */
        var heldText = heldDays.ToString("0", CultureInfo.InvariantCulture);
        var windowText = windowDays.ToString("0.#", CultureInfo.InvariantCulture);
        return string.Create(
            CultureInfo.InvariantCulture,
            $"partial window: this panel read the {tierName} tier, which on this store reaches back about {heldText} day{(heldText == "1" ? "" : "s")}, but the requested window starts {windowText} day{(windowText == "1" ? "" : "s")} back — older points are not included.");
    }

    /// <summary>
    /// The row-cap sibling of <see cref="BuildRetentionNotice"/> (#1687): a time-series panel whose
    /// (buckets × groups) product exceeds <see cref="ComposeLimits.HardRowCap"/> is truncated by the
    /// compiler's LIMIT, and the caller must be told — the same silent-truncation class as the retention
    /// gaps, where the numbers are right and the WINDOW is wrong.
    ///
    /// <para>Detected by <paramref name="rowCount"/> landing EXACTLY on the cap, which is the only signal
    /// available: the compiler cannot know the product ahead of time (it depends on the data's group
    /// cardinality, not the spec). That means a panel producing exactly 10,000 rows on its own is
    /// reported as capped — a false positive, deliberately accepted. Erring toward an unnecessary notice
    /// beats erring toward a truncated chart that looks complete, and the reverse mistake is the bug this
    /// exists to fix.</para>
    ///
    /// <para>TIME-SERIES SHAPES ONLY — plain and the #2734 rank-then-bucket alike, whose row count is
    /// series × buckets (the bound topN LIMIT lives in its rank CTE, not on the rows). A Ranked panel's
    /// LIMIT is the user's own topN — hitting it is the request being honored, not truncation — and
    /// Scalar is a single row.</para>
    /// </summary>
    internal static string? BuildRowCapNotice(PanelMode mode, int rowCount)
    {
        if (mode is not (PanelMode.TimeSeries or PanelMode.RankedTimeSeries) || rowCount < ComposeLimits.HardRowCap)
        {
            return null;
        }

        return string.Create(
            CultureInfo.InvariantCulture,
            $"row cap reached — showing the most recent {ComposeLimits.HardRowCap:N0} buckets of the window, not the whole of it. Coarsen the time bucket or narrow the group-by to fit the window into fewer rows.");
    }

    /// <summary>
    /// Joins the notices that apply into the single <c>notice</c> string the payload carries, or null when
    /// none do. Kept as its own function so the ALL-of-them behavior is testable: retention and the row cap
    /// truncate a panel from opposite ends and a panel can hit both at once, so showing one and dropping
    /// the other would under-report precisely the worst-off panel — the same silent-truncation failure
    /// these notices exist to end.
    /// </summary>
    internal static string? CombineNotices(params string?[] notices)
    {
        var present = notices.Where(n => !string.IsNullOrWhiteSpace(n)).ToArray();
        return present.Length == 0 ? null : string.Join(" ", present);
    }
}
