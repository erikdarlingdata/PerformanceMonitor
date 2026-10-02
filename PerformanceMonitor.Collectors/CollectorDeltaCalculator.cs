/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Calculates delta values for cumulative metrics between collection intervals, caching previous
/// values in memory. The shared implementation of <see cref="ICollectorDeltaCalculator"/> both
/// SKUs run (extracted verbatim from Lite's DeltaCalculator, which now derives from this), so the
/// baseline / counter-reset / gap-policy semantics can never drift between portable Lite and the
/// Darling service. Hosts that survive restarts by re-seeding baselines from their own store call
/// the protected <see cref="Seed"/> for every key and <see cref="SeedPass"/> for every delta group
/// (Lite: DuckDB; Darling: Postgres).
///
/// <para><b>The restart contract (#3540 A4).</b> A host seeds EVERY family in
/// <see cref="DeltaFamilyCollectors"/> that it monitors, keys and pass window both, not the four it
/// happened to seed first. Before #3540 the two seeders covered wait_stats, file_io_stats, perfmon_stats
/// and memory_grant_stats; latch_stats, spinlock_stats, query_stats, procedure_stats and the PostgreSQL
/// pair took the first-sighting path after every restart or deploy, so each fabricated one full interval
/// of quiet per restart — and the pass window was never seeded at all, which left the #2235 series-age
/// rescue inert on exactly the cycle it exists for. query_stats was the one family a host could not
/// key-seed until Darling V128 / Lite v61: it keys its deltas on
/// <c>sql_handle:statement_start_offset:statement_end_offset:plan_handle</c> and the store persisted
/// neither offset, so no store row could reproduce the key and only its PASS WINDOW was seeded. The
/// offsets are stored now and both hosts key-seed it from rows that carry them; a pre-V128 row (NULL
/// offsets) still contributes its collection time to the pass window and seeds no key, because a key
/// built from a fabricated offset is one nothing will ever present. Lite.Tests'
/// <c>DeltaFamilySeedingCensusTests</c> enumerates the family against both hosts' seeders so an eleventh
/// family cannot ship unseeded.</para>
/// </summary>
public class CollectorDeltaCalculator : ICollectorDeltaCalculator
{
    /// <summary>
    /// The gap past which a cached baseline is treated as too stale to subtract from, shared by every
    /// delta call site in this assembly so the policy cannot drift collector by collector.
    ///
    /// <para>One hour, chosen from measurement rather than intuition. The previous value — 300 s,
    /// hard-coded at all 41 call sites — sat almost exactly on the fleet's median sweep gap, so it
    /// fired during ordinary operation instead of after the restarts it was written for. Measured over
    /// 99,717 consecutive perfmon gaps across 52 production servers and 7 days: p50 <b>299 s</b>,
    /// p90 580 s, p99 830 s, p99.9 1,190 s, max 2,514 s. The share of ordinary gaps each candidate
    /// rejects: <b>300 s → 50.0%</b>, 600 s → 8.3%, 900 s → 0.6%, 1,800 s → 0.0%, 3,600 s → 0.0%.
    /// Half of every delta collector's output was a fabricated zero (#2233, #2234).</para>
    ///
    /// <para>An hour clears the observed maximum with room to spare while still catching what the
    /// guard is actually for: a server unreachable for hours, or a baseline restored from a store row
    /// old enough that attributing its whole accrual to one interval would read as a spike. Note the
    /// direction of the harm this replaces — a rejected gap returns 0, and a 0 is indistinguishable
    /// from a genuinely idle interval, so the guard did not merely lose data, it invented quiet.</para>
    /// </summary>
    public const int DefaultMaxGapSeconds = 3600;

    /// <summary>
    /// The slowest cadence (minutes) a schedule may give a delta-family collector — half of
    /// <see cref="DefaultMaxGapSeconds"/> (#3532).
    ///
    /// <para>The gap between consecutive collections is never less than the cadence, and a gap past the
    /// policy makes <see cref="CalculateDelta"/> discard the baseline and return (0, 0) — so a cadence AT
    /// the policy (60 minutes) fabricates permanent quiet: every cycle's gap is the cadence plus scheduling
    /// latency, always past 3600s, so every cycle re-baselines, every delta is zero, the charts flatline,
    /// and the product reads green precisely because it stopped measuring. A cadence between half the
    /// policy and the policy is wrong less deterministically: one sweep overrun longer than the leftover
    /// headroom zeros that interval, and the fleet measurement above saw ~15 minutes of overrun at p99.9.
    /// Half the policy is the cadence at which even an entirely missed cycle (a gap of two cadences)
    /// still yields a real delta.</para>
    /// </summary>
    public const int MaxDeltaFrequencyMinutes = DefaultMaxGapSeconds / 60 / 2;

    /// <summary>
    /// The collectors whose stored values are deltas of cumulative counters — every schedule surface caps
    /// their cadence at <see cref="MaxDeltaFrequencyMinutes"/> (#3532). Membership means "calls
    /// <see cref="ICollectorDeltaCalculator"/> under <see cref="DefaultMaxGapSeconds"/>"; the
    /// DeltaFamilyScheduleBoundTests census asserts this set equals the calculator's caller set, so a new
    /// delta call site that is not listed here (or a listed collector that stopped calling) fails tests.
    /// Snapshot collectors are deliberately absent — a long cadence loses them nothing.
    /// </summary>
    public static readonly IReadOnlySet<string> DeltaFamilyCollectors = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "wait_stats",
        "latch_stats",
        "spinlock_stats",
        "query_stats",
        "procedure_stats",
        "file_io_stats",
        "memory_grant_stats",
        "perfmon_stats",
        "pg_wait_stats",
        "pg_statement_stats",
    };

    /// <summary>True when the named collector stores deltas of cumulative counters and so must stay
    /// inside <see cref="MaxDeltaFrequencyMinutes"/>.</summary>
    public static bool IsDeltaFamily(string collectorName) =>
        collectorName is not null && DeltaFamilyCollectors.Contains(collectorName);

    /// <summary>
    /// The refusal for a delta-family collector scheduled past <see cref="MaxDeltaFrequencyMinutes"/>, or
    /// null when the cadence is fine (any cadence on a non-delta collector is). Shared by both SKUs'
    /// schedule editors and Lite's ScheduleManager so the bound and its explanation live exactly once.
    /// </summary>
    public static string? DeltaFrequencyError(string collectorName, int frequencyMinutes)
    {
        if (frequencyMinutes <= MaxDeltaFrequencyMinutes || !IsDeltaFamily(collectorName))
        {
            return null;
        }

        return $"'{collectorName}': frequency (minutes) can't exceed {MaxDeltaFrequencyMinutes} for this collector. " +
            $"It reads cumulative counters and stores the change between consecutive runs; past the " +
            $"{DefaultMaxGapSeconds / 60}-minute delta gap policy every reading is discarded as too stale to subtract from, " +
            "so the collector would record zero activity forever. The cap is half the policy so a slow or missed cycle still lands inside it.";
    }

    /// <summary>
    /// How far back a restart re-seed reads when restoring baselines from a host's own store.
    ///
    /// <para>Fifteen minutes, and since <see cref="DefaultMaxGapSeconds"/> became an hour this window
    /// — not the gap policy — is what bounds restart recovery. It used to be the other way round: at a
    /// 300 s policy every seed row older than five minutes was rejected on arrival, so most of this
    /// window was work whose result was thrown away. Now every row it returns can produce a real
    /// delta, which is the point.</para>
    ///
    /// <para>Left at fifteen minutes deliberately. It sits well inside one store chunk, which is the
    /// property that matters: it lets TimescaleDB exclude the rest of a multi-hundred-GB hypertable
    /// rather than scan every chunk on a 30-second command timeout — the field failure in #1772.
    /// Widening it to chase the hour-long policy would trade that back.</para>
    /// </summary>
    public static readonly TimeSpan SeedLookback = TimeSpan.FromMinutes(15);

    /// <summary>
    /// The cutoff a seed read binds to its <c>collection_time &gt;= $1</c> bound, defined once so the
    /// two hosts cannot drift. Naive UTC by the product-wide storage convention — Kind is stripped
    /// deliberately, because Npgsql 6+ rejects a <c>Kind=Utc</c> value against a <c>timestamp</c>
    /// column, and DuckDB stores the same naive-UTC values.
    /// </summary>
    public static DateTime SeedCutoff()
        => DateTime.SpecifyKind(DateTime.UtcNow - SeedLookback, DateTimeKind.Unspecified);

    /// <summary>
    /// Cache structure: serverId -> collectorName -> key -> (previousValue, timestamp)
    /// </summary>
    private readonly ConcurrentDictionary<int, ConcurrentDictionary<string, ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>>> _cache = new();

    /// <summary>
    /// When this (server, collector) pair was last looked at, and the look before that:
    /// serverId -> collectorName -> (current pass, previous pass).
    ///
    /// <para>Needed by <see cref="CalculateDeltaWithSeriesAge"/> to answer "did this counter series begin
    /// since we last looked?" for a key that has no history of its own. The PREVIOUS pass is the useful
    /// one, and it is tracked separately from the per-key timestamps because a brand-new key has none.</para>
    ///
    /// <para>Advanced only when the collection time actually CHANGES, which is what makes it stable
    /// across the many rows of one pass: a collector calls in per row, and if the first row rolled the
    /// window forward every later row in the same pass would compare against its own pass and see a zero
    /// gap — quietly disabling the credit for every row but the first.</para>
    /// </summary>
    private readonly ConcurrentDictionary<int, ConcurrentDictionary<string, (DateTime Current, DateTime? Previous)>> _passes = new();

    /// <summary>
    /// Rolls the (server, collector) pass window forward when <paramref name="collectionTime"/> is a new
    /// pass, and returns the previous pass — the boundary a series age is measured against.
    /// </summary>
    private DateTime? PreviousPass(int serverId, string collectorName, DateTime? collectionTime)
    {
        if (!collectionTime.HasValue)
        {
            return null;
        }

        var byCollector = _passes.GetOrAdd(serverId, _ => new ConcurrentDictionary<string, (DateTime, DateTime?)>());
        var updated = byCollector.AddOrUpdate(
            collectorName,
            _ => (collectionTime.Value, null),
            (_, existing) => existing.Current == collectionTime.Value
                ? existing
                : (collectionTime.Value, existing.Current));

        return updated.Previous;
    }

    /// <inheritdoc />
    ///
    /// <para>#4428: the same window-rolling machinery <see cref="PreviousPass(int, string, DateTime?)"/>
    /// (private, above) already keeps for a collector-clock caller, exposed under the interface's own
    /// name for a caller that tracks a DIFFERENT clock. <paramref name="group"/> shares the private
    /// method's dictionary rather than a second one, on the contract stated on the interface member: a
    /// caller's group name never collides with any <c>collectorName</c> an ordinary delta call passes, so
    /// one dictionary safely serves both without either clock's window disturbing the other's.</para>
    public DateTime? PreviousPass(int serverId, string group, DateTime observedTime)
        => PreviousPass(serverId, group, (DateTime?)observedTime);

    /// <summary>
    /// The discontinuity accounts (#3653 A5) a definition handed to <see cref="ClearServer"/> or
    /// <see cref="ClearGroups"/> and no host has logged yet: serverId -> the lines, in the order they were
    /// given. The calculator is the one object a definition and its host both hold, so it is where the
    /// account waits between the read that saw the epoch and the host's log line after the run; see
    /// <see cref="DrainDiscontinuities"/>.
    /// </summary>
    private readonly ConcurrentDictionary<int, ConcurrentQueue<string>> _discontinuities = new();

    /// <summary>
    /// Removes all cached entries for a server (e.g., when the server tab is closed).
    /// Next collection will re-seed from database if needed.
    ///
    /// <para>Since #3653 A5 this is also what a definition calls when it sees that the counters behind
    /// <paramref name="serverId"/> are a different instance's than the baselines were read from — a new
    /// <c>sqlserver_start_time</c> or <c>@@SERVERNAME</c> against the pair persisted in
    /// <c>collector_state</c> (<see cref="ServerEpoch"/>) — and what Darling's reconcile calls when a
    /// registration reconnects under the same id with a changed connection. <paramref name="discontinuity"/>
    /// is the definition's one-line account of the change, queued for the host to log; null from the
    /// remove paths, which log their own reason.</para>
    /// </summary>
    public void ClearServer(int serverId, string? discontinuity = null)
    {
        _cache.TryRemove(serverId, out _);
        /* The pass window goes with the baselines it is interpreted against. Left behind, a re-added
           server's first pass would measure a series age against a look from before it was removed and
           credit a full counter to an interval that never happened. */
        _passes.TryRemove(serverId, out _);
        NoteDiscontinuity(serverId, discontinuity);
    }

    /// <summary>
    /// Forgets the baselines AND the pass window of the named delta groups for one server, leaving the
    /// server's other groups untouched (#3653 A5). For an epoch that is one family's alone —
    /// <c>pg_stat_statements_info.stats_reset</c> moving says the statements counters restarted and says
    /// nothing about any other counter on the instance. Both halves go together for the reason
    /// <see cref="ClearServer"/> gives: a group's pass window left behind would credit a series age against
    /// a look that belongs to the counters being forgotten.
    /// </summary>
    public void ClearGroups(int serverId, string? discontinuity, params string[] groups)
    {
        if (groups is null || groups.Length == 0)
        {
            return;
        }

        if (_cache.TryGetValue(serverId, out var serverCache))
        {
            foreach (var group in groups)
            {
                serverCache.TryRemove(group, out _);
            }
        }

        if (_passes.TryGetValue(serverId, out var serverPasses))
        {
            foreach (var group in groups)
            {
                serverPasses.TryRemove(group, out _);
            }
        }

        NoteDiscontinuity(serverId, discontinuity);
    }

    /// <summary>
    /// Hands back — and forgets — every discontinuity account queued for <paramref name="serverId"/> since
    /// the last drain, oldest first; empty when there is nothing to say, which is every ordinary run. A host
    /// calls this once per collector run, after the run, and writes each line to its log at Information:
    /// the definition that saw the epoch composed the sentence (old value, new value, what was forgotten)
    /// and the host owns the logger and the server's display name. Read-once so a line is logged by exactly
    /// one run and never re-logged by the next.
    /// </summary>
    public IReadOnlyList<string> DrainDiscontinuities(int serverId)
    {
        if (!_discontinuities.TryRemove(serverId, out var queue) || queue.IsEmpty)
        {
            return System.Array.Empty<string>();
        }

        var lines = new List<string>(queue.Count);
        while (queue.TryDequeue(out var line))
        {
            lines.Add(line);
        }

        return lines;
    }

    private void NoteDiscontinuity(int serverId, string? discontinuity)
    {
        if (string.IsNullOrWhiteSpace(discontinuity))
        {
            return;
        }

        _discontinuities.GetOrAdd(serverId, _ => new ConcurrentQueue<string>()).Enqueue(discontinuity);
    }

    /// <summary>
    /// #4428: the calendar-UTC-day a server's wait-stats-clear warning last queued, so
    /// <see cref="NoteWaitStatsClear"/> can throttle to once per server per day even across many clears an
    /// hour. In memory, like every other per-server throttle on this type: a restart simply warns once more.
    /// </summary>
    private readonly ConcurrentDictionary<int, DateOnly> _waitStatsClearWarnedDate = new();

    /// <summary>The queued, ready-to-log wait-stats-clear sentences — see <see cref="NoteWaitStatsClear"/>
    /// and <see cref="DrainWaitStatsClearWarnings"/>.</summary>
    private readonly ConcurrentDictionary<int, ConcurrentQueue<string>> _waitStatsClearWarnings = new();

    /// <inheritdoc />
    public IReadOnlyDictionary<string, long> PeekBaselines(int serverId, string collectorName)
    {
        if (!_cache.TryGetValue(serverId, out var serverCache)
            || !serverCache.TryGetValue(collectorName, out var collectorCache))
        {
            return new Dictionary<string, long>(0);
        }

        var snapshot = new Dictionary<string, long>(collectorCache.Count);
        foreach (var entry in collectorCache)
        {
            snapshot[entry.Key] = entry.Value.Value;
        }

        return snapshot;
    }

    /// <inheritdoc />
    ///
    /// <para>#4428: rebases the VALUE half of every cached (Value, Timestamp) pair to zero for the named
    /// groups, on THIS server only, leaving the Timestamp untouched — the ordinary delta path
    /// (<see cref="Core"/>'s Update branch) then measures the real interval against that kept timestamp and
    /// reports "current value minus zero" as the delta, instead of "current value minus the pre-clear
    /// baseline" going negative and being read as an ordinary counter reset (the (0, 0) unknowable pair).</para>
    public void RebaseFamiliesToZero(int serverId, IEnumerable<string> collectorNames)
    {
        if (collectorNames is null || !_cache.TryGetValue(serverId, out var serverCache))
        {
            return;
        }

        foreach (var collectorName in collectorNames)
        {
            if (!serverCache.TryGetValue(collectorName, out var collectorCache))
            {
                continue;
            }

            foreach (var key in collectorCache.Keys)
            {
                collectorCache.AddOrUpdate(
                    key,
                    static _ => (0L, (DateTime?)null),
                    static (_, existing) => (0L, existing.Timestamp));
            }
        }
    }

    /// <inheritdoc />
    public void NoteWaitStatsClear(int serverId, string serverName, DateTime nowUtc)
    {
        var today = DateOnly.FromDateTime(nowUtc);

        var alreadyWarnedToday = _waitStatsClearWarnedDate.TryGetValue(serverId, out var last) && last == today;

        if (alreadyWarnedToday)
        {
            return;
        }

        _waitStatsClearWarnedDate[serverId] = today;

        var line = $"Wait statistics on {serverName} were cleared between collections, as a DBCC " +
            "SQLPERF(..., CLEAR) job does. This collection's wait figures cover only the time since the " +
            "clear. Frequent clears also reset the wait history any other tool reads from this server.";

        _waitStatsClearWarnings.GetOrAdd(serverId, _ => new ConcurrentQueue<string>()).Enqueue(line);
    }

    /// <inheritdoc />
    public IReadOnlyList<string> DrainWaitStatsClearWarnings(int serverId)
    {
        if (!_waitStatsClearWarnings.TryRemove(serverId, out var queue) || queue.IsEmpty)
        {
            return Array.Empty<string>();
        }

        var lines = new List<string>(queue.Count);
        while (queue.TryDequeue(out var line))
        {
            lines.Add(line);
        }

        return lines;
    }

    /// <inheritdoc />
    ///
    /// <para>#4428: peeks every family's cached (Value, Timestamp) for <paramref name="key"/> WITHOUT
    /// updating anything — no baseline write, no pass-window roll — so it costs nothing beyond the calls a
    /// caller was already going to make. A family with no cached entry yet cannot have restarted (its own
    /// per-family call takes the ordinary first-sighting path), so it is skipped rather than counted as a
    /// reset.</para>
    public RowResetDecision DecideRow(int serverId, IReadOnlyList<(string Family, long Current)> counters, string key,
        int? seriesAgeSeconds, DateTime? collectionTime, int maxGapSeconds)
    {
        if (counters is null || counters.Count == 0)
        {
            return default;
        }

        if (!_cache.TryGetValue(serverId, out var serverCache))
        {
            return default;
        }

        var anyReset = false;

        foreach (var (family, current) in counters)
        {
            if (!serverCache.TryGetValue(family, out var collectorCache)
                || !collectorCache.TryGetValue(key, out var previous))
            {
                /* No cached baseline for this family under this key yet — a genuine first sighting, which
                   is not a restart and takes its own per-family path (including the #2235 rescue). */
                continue;
            }

            if (current < previous.Value)
            {
                anyReset = true;
                break;
            }
        }

        if (!anyReset)
        {
            return default;
        }

        /* Same #2235 test a per-family reset already uses to place a restart inside the gap: read the
           PREVIOUS pass without rolling it forward (PreviousPass only rolls when collectionTime is a NEW
           pass for this (server, collector), and this call passes the family name of the first counter
           purely as the collector key that pass window is stored under — every family sharing this row
           already shares one pass window because they are called in the same WritePayload for the same
           collectorName-per-family scheme, so any one of them reads the same previous pass). */
        if (seriesAgeSeconds.HasValue && seriesAgeSeconds.Value >= 0 && collectionTime.HasValue)
        {
            var previousPass = PreviousPass(serverId, counters[0].Family, collectionTime);

            if (previousPass.HasValue)
            {
                var gap = (collectionTime.Value - previousPass.Value).TotalSeconds;

                if (gap > 0 && (maxGapSeconds <= 0 || gap <= maxGapSeconds) && seriesAgeSeconds.Value <= gap)
                {
                    return new RowResetDecision(AnyReset: true, CreditedInGap: true, IntervalSeconds: (int)gap);
                }
            }
        }

        return new RowResetDecision(AnyReset: true, CreditedInGap: false, IntervalSeconds: 0);
    }

    /// <summary>
    /// Calculates the delta between the current value and the previous cached value.
    /// First-ever sighting (no baseline): returns 0 and stores the value as the new baseline.
    /// Counter reset (value decreased): returns 0 to avoid inflated deltas from plan cache churn.
    /// Gap detection: if collectionTime and maxGapSeconds are provided and the gap since the
    /// last cached value exceeds maxGapSeconds, returns 0 to avoid inflated deltas after restarts.
    /// Thread-safe via atomic AddOrUpdate.
    /// <para>All three of those zeros mean "no delta is knowable here", which a <c>long</c> cannot say
    /// any other way — and none of them is the same claim as "this interval was idle". Use
    /// <see cref="CalculateDeltaWithInterval"/> when a caller has to tell them apart: the reported
    /// interval is 0 in exactly these cases and non-zero whenever the delta is real, so a stored
    /// (delta, interval) pair of (0, 0) reads as unknown while (0, n) reads as genuinely idle. That
    /// pairing is what makes a zero interpretable downstream (#2234).</para>
    /// </summary>
    public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
        DateTime? collectionTime = null, int maxGapSeconds = 0)
        => CalculateDeltaWithInterval(serverId, collectorName, key, currentValue, out _, collectionTime, maxGapSeconds);

    /// <summary>
    /// Same as <see cref="CalculateDelta"/>, but also reports the number of seconds between the
    /// previous cached collection and the current one via <paramref name="intervalSeconds"/>.
    /// The interval is 0 on the first sighting of a key or after a gap reset (no prior baseline to
    /// measure against), mirroring the delta's own 0 in those cases. Callers can divide a delta by
    /// this interval to derive a per-second rate (e.g. CPU-ms per wall-clock second).
    /// Thread-safe via atomic AddOrUpdate.
    /// </summary>
    public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
        out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        => Core(serverId, collectorName, key, currentValue, seriesAgeSeconds: null, out intervalSeconds,
            collectionTime, maxGapSeconds);

    /// <inheritdoc />
    public long CalculateDeltaWithSeriesAge(int serverId, string collectorName, string key, long currentValue,
        int? seriesAgeSeconds, out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        => Core(serverId, collectorName, key, currentValue, seriesAgeSeconds, out intervalSeconds,
            collectionTime, maxGapSeconds);

    private long Core(int serverId, string collectorName, string key, long currentValue,
        int? seriesAgeSeconds, out int intervalSeconds, DateTime? collectionTime, int maxGapSeconds)
    {
        /* Read (and roll) the pass window BEFORE touching the key cache: the Add path below needs the
           previous pass, and a key that is new has no timestamp of its own to supply it. Always called,
           even when no series age was passed, so the window advances on every pass rather than only on
           the passes that happen to use the hint. */
        var previousPass = PreviousPass(serverId, collectorName, collectionTime);

        var serverCache = _cache.GetOrAdd(serverId, _ => new ConcurrentDictionary<string, ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>>());
        var collectorCache = serverCache.GetOrAdd(collectorName, _ => new ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>());

        long delta = 0;
        int interval = 0;

        collectorCache.AddOrUpdate(
            key,
            /* Add: first time seeing this key — store the baseline only and return 0.
               All callers track cumulative counters (perfmon, wait stats, file IO, etc.).

               #2235 exception, and the ONLY case where a first sighting can report real work: when the
               caller supplies a series age younger than the gap since our previous pass, the counter
               demonstrably STARTED inside that gap, so its whole value accrued there and its baseline
               was 0 rather than currentValue. Without this a recompiling plan reports 0 forever — it
               presents a new plan_handle, hence a new key, on nearly every sighting. A real interval is
               reported alongside it because this delta IS knowable; the (0, 0) pairing stays reserved
               for the cases that genuinely are not. */
            _ =>
            {
                delta = 0;
                interval = 0;

                if (seriesAgeSeconds.HasValue && seriesAgeSeconds.Value >= 0
                    && collectionTime.HasValue && previousPass.HasValue)
                {
                    var gap = (collectionTime.Value - previousPass.Value).TotalSeconds;

                    /* Bounded by the same gap policy as the reset branch: past it, attributing a whole
                       cumulative counter to one interval is the inflated spike that guard exists to
                       prevent — a plan compiled during an hour-long outage is not an hour of work in
                       the next minute. */
                    if (gap > 0 && (maxGapSeconds <= 0 || gap <= maxGapSeconds) && seriesAgeSeconds.Value <= gap)
                    {
                        delta = currentValue;
                        interval = (int)gap;
                    }
                }

                return (currentValue, collectionTime);
            },
            /* Update: compute delta atomically */
            (_, previous) =>
            {
                /* Gap detection: if too much time has passed since the last cached value,
                   treat this as a new baseline to avoid inflated deltas after app restarts */
                if (maxGapSeconds > 0 && collectionTime.HasValue && previous.Timestamp.HasValue
                    && (collectionTime.Value - previous.Timestamp.Value).TotalSeconds > maxGapSeconds)
                {
                    delta = 0;
                    interval = 0;
                    return (currentValue, collectionTime);
                }

                /* Seconds between the previous and current collection, when both timestamps exist —
                   the wall-clock span this delta accrued over. */
                interval = (collectionTime.HasValue && previous.Timestamp.HasValue)
                    ? (int)(collectionTime.Value - previous.Timestamp.Value).TotalSeconds
                    : 0;

                if (currentValue < previous.Value)
                {
                    /* Counter reset (plan cache eviction/re-entry): the work between the two readings
                       is unknowable, not zero. Report no interval either, so the pair stays honest —
                       a 0 delta over a REAL interval is a claim that nothing happened for that long,
                       and this is the one case where that claim would be false. That invariant
                       (interval 0 <=> no delta knowable) is what lets a reader tell a fabricated zero
                       from an idle one — and since Darling V128 / Lite v61 the interval REACHES the store
                       for EVERY delta family: all ten members of DeltaFamilyCollectors persist a
                       sample_interval_seconds column beside their deltas (perfmon_stats and query_stats
                       from the start; wait_stats, file_io_stats,
                       latch_stats and spinlock_stats since Darling V127 / Lite v60, #3540;
                       procedure_stats, memory_grant_stats, pg_wait_stats and pg_statement_stats since
                       Darling V128 / Lite v61, #3540), and every per-second reader maps the 0 to NULL
                       via NULLIF(sample_interval_seconds, 0) or filters it out of an aggregate with
                       sample_interval_seconds IS DISTINCT FROM 0. Before #3540 this comment
                       claimed "every consumer" while four of six SQL Server families discarded the
                       interval at the write, so the fabricated zero survived as a measured one and their
                       readers LAG-divided it into a confident 0.00 at exactly the moments it was
                       unknowable; two more took the bare long and the PostgreSQL pair asked for the
                       interval only to skip idle rows. The claim is pinned rather than trusted:
                       Lite.Tests' DeltaFamilyIntervalColumnTests is the census that asserts every member
                       of DeltaFamilyCollectors carries the column, so an eleventh family cannot ship
                       naked and this sentence cannot silently go false again. */
                    delta = 0;
                    interval = 0;
                }
                else
                {
                    delta = currentValue - previous.Value;
                }

                return (currentValue, collectionTime);
            });

        intervalSeconds = interval;
        return delta;
    }

    /// <summary>
    /// Seeds a single value into the cache without computing a delta — the restart-survival hook
    /// hosts use to restore baselines from their own store.
    /// </summary>
    protected void Seed(int serverId, string collectorName, string key, long value, DateTime? timestamp = null)
    {
        var serverCache = _cache.GetOrAdd(serverId, _ => new ConcurrentDictionary<string, ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>>());
        var collectorCache = serverCache.GetOrAdd(collectorName, _ => new ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>());
        collectorCache[key] = (value, timestamp);
    }

    /// <summary>
    /// Seeds the (server, delta group) pass window — when this group was last looked at, and optionally the
    /// look before that — the restart-survival hook for the #2235 series-age rescue.
    ///
    /// <para><b>Why a second hook.</b> <see cref="Seed"/> restores per-KEY baselines, and the rescue does not
    /// read those: it asks <see cref="PreviousPass"/> for the pass BEFORE the current one, and until #3540 no
    /// host ever wrote that window from the store, so the first post-restart pass always saw
    /// <c>previousPass == null</c> and baselined every new key without credit. That is the exact cycle the
    /// rescue was written for — a service restart is when the most plans have recompiled since we last
    /// looked — and it was the one cycle the rescue could never fire on.</para>
    ///
    /// <para><b>What to pass.</b> <paramref name="current"/> is the LATEST collection_time the store holds for
    /// the server in the seed window — the last time this process's predecessor looked. On the first
    /// post-restart pass <see cref="PreviousPass"/> sees a new collection time, rolls <c>current</c> into the
    /// Previous slot and measures the gap against it, so the seeded Current alone is what makes the rescue
    /// fire. <paramref name="previous"/> is accepted for completeness — a seeder whose read happens to return
    /// more than one collection time per server can pass the second-latest — but it is read only if the
    /// first post-restart pass carried a collection time EQUAL to the last pre-restart one, which a restart
    /// makes impossible; hosts whose seed read returns one collection time per server pass null and lose
    /// nothing. Seeding this from the family table rather than <c>collection_log</c> is deliberate: the
    /// family row's collection_time is the value the delta calls were made with, the log row's is the run's
    /// start clock, and a run that failed at the target logs a row while having made no delta call.</para>
    ///
    /// <para>Keyed by the delta GROUP name (<c>query_stats_worker</c>, <c>wait_stats_time</c>, …), the same
    /// name the collector's <c>CalculateDelta*</c> call passes as <c>collectorName</c>, because that is how
    /// <see cref="_passes"/> is keyed — a seeder must seed every group of a family, not the family name.</para>
    /// </summary>
    protected void SeedPass(int serverId, string collectorName, DateTime current, DateTime? previous = null)
    {
        var byCollector = _passes.GetOrAdd(serverId, _ => new ConcurrentDictionary<string, (DateTime, DateTime?)>());
        byCollector[collectorName] = (current, previous);
    }

    /// <summary>
    /// <see cref="SeedPass"/> for every delta group of one family, from the collection times a seed read
    /// observed. The pass window is derived from the SAME rows the key seed streams — no second read per
    /// family — which is why the tracker is fed row by row rather than queried.
    /// </summary>
    protected void SeedPasses(SeedPassTracker passes, params string[] groups)
    {
        foreach (var (serverId, latest, before) in passes.Servers)
        {
            foreach (var group in groups)
            {
                SeedPass(serverId, group, latest, before);
            }
        }
    }

    /// <summary>
    /// Per server, the latest and second-latest DISTINCT collection times a seed read has streamed past so
    /// far. A latest-collection-per-server read (the wait_stats shape) observes one time per server and
    /// yields <c>Before = null</c>; a latest-row-per-key read (the procedure_stats shape) can observe several
    /// and yields the two most recent. Either is enough for <see cref="SeedPass"/> — see its remarks on why
    /// Current alone arms the rescue.
    /// </summary>
    protected sealed class SeedPassTracker
    {
        private readonly Dictionary<int, (DateTime Latest, DateTime? Before)> _byServer = new();

        /// <summary>Records one row's collection time. A null time (a row that never recorded one) is
        /// ignored rather than treated as "now", because a pass window built from a guess is exactly the
        /// fabricated evidence the rescue's gap bound exists to refuse.</summary>
        public void Observe(int serverId, DateTime? collectionTime)
        {
            if (!collectionTime.HasValue)
            {
                return;
            }

            var t = collectionTime.Value;
            if (!_byServer.TryGetValue(serverId, out var window))
            {
                _byServer[serverId] = (t, null);
            }
            else if (t > window.Latest)
            {
                _byServer[serverId] = (t, window.Latest);
            }
            else if (t < window.Latest && (!window.Before.HasValue || t > window.Before.Value))
            {
                _byServer[serverId] = (window.Latest, t);
            }
        }

        /// <summary>Every server observed, with its window.</summary>
        public IEnumerable<(int ServerId, DateTime Latest, DateTime? Before)> Servers
        {
            get
            {
                foreach (var entry in _byServer)
                {
                    yield return (entry.Key, entry.Value.Latest, entry.Value.Before);
                }
            }
        }

        /// <summary>How many servers have been observed — for the seeders' debug line.</summary>
        public int Count => _byServer.Count;
    }
}
