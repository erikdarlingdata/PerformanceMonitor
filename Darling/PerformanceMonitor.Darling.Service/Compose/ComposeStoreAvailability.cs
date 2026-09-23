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
/// The service's cached answer to "which retention rollups does this store have, and how far back has each
/// materialized?" (#1665 — the composer's arm of the #1664 availability guard; since #3905 also the gate the
/// daily-summary reader routes on), plus the "partial window, and says so" notice for the routes that
/// availability (or the value-remap/dimension-coverage gates) forced below what the window's age wanted.
/// Caching follows the viewer's <c>GetRollupAvailabilityAsync</c>: the answer is re-probed at most every
/// <see cref="ReprobeInterval"/>, unconditionally since #1759 (see <see cref="GetRollupsAsync(NpgsqlDataSource, CancellationToken)"/>),
/// so an ensure sweep or a backfill finishing mid-session converges without a restart; a failed probe answers
/// <see cref="RollupAvailability.None"/> — raw always exists, so "route everything to raw" is the never-wrong
/// fallback. Keyed per data source (not a bare static) so gated-live tests spinning several stores in one
/// process can never bleed one store's shape into another's.
/// </summary>
internal static class ComposeStoreAvailability
{
    /// <summary>One probe's answer, and whether it is a measurement or the fallback a failed probe left
    /// (#3905): the compose panels read both alike, the daily summary may not.</summary>
    private readonly record struct ProbeAnswer(RollupAvailability Rollups, RollupCoverage Coverage, bool Failed);

    private sealed class Entry
    {
        public ProbeAnswer Answer = new(RollupAvailability.None, RollupCoverage.Unknown, Failed: false);
        public bool Probed;
        public DateTime ProbedAtUtc;

        /// <summary>The probe the current callers share (#3905): non-null and incomplete while one is running.
        /// A completed task here is history, and the next caller who finds the cache stale starts a new one.</summary>
        public Task<ProbeAnswer>? InFlight;

        /// <summary>Probes started for this data source over its whole life. Diagnostic; see
        /// <see cref="ProbesStartedFor"/>.</summary>
        public int ProbesStarted;
    }

    private static readonly ConditionalWeakTable<NpgsqlDataSource, Entry> s_entries = new();

    /// <summary>While the store reports no (or partial) rollups, re-probe at most this often — the same
    /// convergence interval the viewer uses. Since #1759 this ALSO bounds how stale a coverage floor can
    /// get, which is what lets a backfill be picked up without restarting the service.</summary>
    internal static readonly TimeSpan ReprobeInterval = TimeSpan.FromMinutes(5);

    /// <summary>
    /// The store's rollup availability AND each rollup's materialized-coverage floor, probed lazily and
    /// cached per data source.
    ///
    /// <para><b>Coverage expires even when availability does not (#1759).</b> Availability is permanent once
    /// complete — a created aggregate is never dropped outside the service's own reshape sweep — but a
    /// coverage floor MOVES: a <c>--backfill-rollups</c> run pushes it backwards, a retention drop pushes it
    /// forwards. Caching the pair on availability's permanent terms would pin a pre-backfill floor for the
    /// life of the process, so an operator who just backfilled would keep getting raw fallbacks until a
    /// restart. The <see cref="ReprobeInterval"/> TTL therefore applies unconditionally, and the
    /// <c>AllPresent</c> shortcut is deliberately gone.</para>
    ///
    /// <para><b>One probe in flight per data source (#3905).</b> This used to be "benignly racy": concurrent
    /// callers that found the cache stale each ran the probe, on the argument that it was two small lookups.
    /// Neither half held. The daily-summary reader now reads coverage through here too, agents race their
    /// daily reads, the compose panels, trend tools and custom alerts read the same gate, and on the largest
    /// production store the coverage probe is >= 1.7 s on a quiet minute, not a small lookup. So a stale cache
    /// is refreshed by exactly one probe and every caller that arrives while it runs awaits that one: the
    /// single-flight shape of <c>DarlingFleetReader.CollectionHealthMemo</c> (#3735), for its reasons. The
    /// shared probe runs on <see cref="CancellationToken.None"/>, so a caller's token releases only that
    /// caller's wait, and the probe cannot fault the shared task: a failure is the cached raw fallback below,
    /// as before. A reader that must not route on that fallback reads
    /// <see cref="GetMeasuredRollupsAsync(NpgsqlDataSource, CancellationToken)"/> instead.</para>
    /// </summary>
    internal static ValueTask<(RollupAvailability Rollups, RollupCoverage Coverage)> GetRollupsAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken)
        => GetRollupsAsync(postgres, ProbeAsync, cancellationToken);

    /// <summary>
    /// <see cref="GetRollupsAsync(NpgsqlDataSource, CancellationToken)"/> with the probe substitutable, so a
    /// test can count and pace executions without a store. <paramref name="probe"/> is called with
    /// <see cref="CancellationToken.None"/>: the cache, not the caller, decides the probe's lifetime.
    /// </summary>
    internal static async ValueTask<(RollupAvailability Rollups, RollupCoverage Coverage)> GetRollupsAsync(
        NpgsqlDataSource postgres,
        Func<NpgsqlDataSource, CancellationToken, Task<(RollupAvailability Rollups, RollupCoverage Coverage)>> probe,
        CancellationToken cancellationToken)
    {
        var answer = await GetAnswerAsync(postgres, probe, cancellationToken);
        return (answer.Rollups, answer.Coverage);
    }

    /// <summary>
    /// The same cached gate, for a reader whose numbers the failure fallback would falsify (#3905: the daily
    /// summary).
    ///
    /// <para>The fallback is <see cref="RollupAvailability.None"/>, and <see cref="RetentionTierRouter"/> sends
    /// None to raw for any window. The compose panels accept that: raw is complete on a plain-PostgreSQL store,
    /// and on a TimescaleDB one a panel without its rollup reads as a partial window. The daily summary cannot.
    /// On a TimescaleDB store whose raw purges are armed, raw <c>query_stats</c> holds four days, so a month
    /// routed to raw prints <c>unique_queries = 0</c> for every older day. That is not NULL and not in
    /// <c>days_missing</c>: it is the #3653 A6 lie, told for as long as the failure stays cached. Before #3905
    /// a failed probe failed this reader's call instead, because it probed per call and let the exception
    /// through. So a cached failure is not an answer here. This reader probes again itself, on its own token,
    /// exactly as it did per call before, and a probe that fails again surfaces as the caller's error. A
    /// success is used and not written back: the failure fallback and its interval are the compose panels'
    /// contract. Every measured answer, which is every answer on a healthy store, is the cached one.</para>
    /// </summary>
    internal static ValueTask<(RollupAvailability Rollups, RollupCoverage Coverage)> GetMeasuredRollupsAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken)
        => GetMeasuredRollupsAsync(postgres, ProbeAsync, cancellationToken);

    /// <summary><see cref="GetMeasuredRollupsAsync(NpgsqlDataSource, CancellationToken)"/> with the probe
    /// substitutable. The shared probe gets <see cref="CancellationToken.None"/>; the direct re-probe after a
    /// cached failure gets <paramref name="cancellationToken"/>, since it is this caller's alone.</summary>
    internal static async ValueTask<(RollupAvailability Rollups, RollupCoverage Coverage)> GetMeasuredRollupsAsync(
        NpgsqlDataSource postgres,
        Func<NpgsqlDataSource, CancellationToken, Task<(RollupAvailability Rollups, RollupCoverage Coverage)>> probe,
        CancellationToken cancellationToken)
    {
        var answer = await GetAnswerAsync(postgres, probe, cancellationToken);
        return answer.Failed
            ? await probe(postgres, cancellationToken)
            : (answer.Rollups, answer.Coverage);
    }

    private static async ValueTask<ProbeAnswer> GetAnswerAsync(
        NpgsqlDataSource postgres,
        Func<NpgsqlDataSource, CancellationToken, Task<(RollupAvailability Rollups, RollupCoverage Coverage)>> probe,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(probe);

        var entry = s_entries.GetOrCreateValue(postgres);

        Task<ProbeAnswer> shared;
        TaskCompletionSource<ProbeAnswer>? lead = null;
        lock (entry)
        {
            if (entry.Probed && DateTime.UtcNow - entry.ProbedAtUtc < ReprobeInterval)
            {
                return entry.Answer;
            }

            if (entry.InFlight is not { IsCompleted: false })
            {
                /* RunContinuationsAsynchronously: the waiters' continuations (the rest of each panel or daily
                   read) must not run inline on whichever thread completes the probe. */
                lead = new TaskCompletionSource<ProbeAnswer>(TaskCreationOptions.RunContinuationsAsynchronously);
                entry.InFlight = lead.Task;
                entry.ProbesStarted++;
            }

            shared = entry.InFlight;
        }

        if (lead is not null)
        {
            /* Started outside the lock, and not awaited here: this caller is one waiter among any number, and
               its own cancellation below must not be the probe's. RunProbeAsync completes the source on every
               path and never throws, so the discarded task cannot fault. */
            _ = RunProbeAsync(entry, lead, postgres, probe);
        }

        return await shared.WaitAsync(cancellationToken);
    }

    /// <summary>How many probes <see cref="GetRollupsAsync(NpgsqlDataSource, CancellationToken)"/> has started
    /// for <paramref name="postgres"/> — the figure a live test compares against the number of reads it raced
    /// (#3905). Diagnostic; nothing reads it in production.</summary>
    internal static int ProbesStartedFor(NpgsqlDataSource postgres)
    {
        var entry = s_entries.GetOrCreateValue(postgres);
        lock (entry)
        {
            return entry.ProbesStarted;
        }
    }

    private static async Task<(RollupAvailability Rollups, RollupCoverage Coverage)> ProbeAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var rollups = await TimescaleSupport.DetectRollupsAsync(postgres, cancellationToken);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(postgres, rollups, cancellationToken);
        return (rollups, coverage);
    }

    private static async Task RunProbeAsync(
        Entry entry,
        TaskCompletionSource<ProbeAnswer> lead,
        NpgsqlDataSource postgres,
        Func<NpgsqlDataSource, CancellationToken, Task<(RollupAvailability Rollups, RollupCoverage Coverage)>> probe)
    {
        ProbeAnswer answer;
        try
        {
            var (rollups, coverage) = await probe(postgres, CancellationToken.None);
            answer = new ProbeAnswer(rollups, coverage, Failed: false);
        }
        catch (Exception ex)
        {
            /* A store hiccup mid-probe must not fail the panel — raw is the safe answer for availability,
               and "no coverage evidence" leaves the age ladder in charge. The re-probe interval retries soon.
               Every exception, cancellation included: the probe runs on no caller's token, so nothing here
               is a caller asking to stop, and the shared task must complete for every waiter. Marked Failed
               so GetMeasuredRollupsAsync can refuse to route on it. */
            answer = new ProbeAnswer(RollupAvailability.None, RollupCoverage.Unknown, Failed: true);
            _ = ex;
        }

        lock (entry)
        {
            entry.Answer = answer;
            entry.Probed = true;
            entry.ProbedAtUtc = DateTime.UtcNow;
        }

        lead.SetResult(answer);
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
