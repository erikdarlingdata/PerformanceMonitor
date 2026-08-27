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

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// The collection-freshness bands the headless surfaces derive a server's connection status from — the
    /// viewer has no live ping to a monitored server, so "is this server reporting" is answered by how old the
    /// newest collection is (see <see cref="ServerHealthClassifier.ClassifyFreshness"/>).
    /// </summary>
    public enum ServerFreshness
    {
        /// <summary>The newest collection is within twice the fastest collector's cadence — Online (green).</summary>
        Fresh,

        /// <summary>Collection has lagged past twice the cadence but the server isn't long-dead — Warning (amber).</summary>
        Stale,

        /// <summary>The newest collection is long-dead — the Offline overlay (red).</summary>
        Offline,

        /// <summary>
        /// No collection has EVER landed for this server (this run or any prior) — the service has not reached it
        /// yet. Distinct from <see cref="Offline"/> (which means data STOPPED): during a slow fleet bootstrap a
        /// red "Offline" on a server that was merely still queued sent a 24-server field report chasing a phantom
        /// scheduler bug. Rendered as an amber "Awaiting first collection", never the red overlay.
        /// </summary>
        NeverCollected,
    }

    /// <summary>
    /// One server's COLLECTION status, as a value rather than a rendered string — the discriminant every
    /// Darling surface that reports "is this server reporting" renders. The Overview card's word and colour,
    /// the sidebar row's dot, the fleet roll-up's label and the <c>list_servers</c> MCP status are all
    /// renderings OF THIS.
    ///
    /// <para><b>Why it lives here and not in the viewer.</b> It used to be a viewer-local enum, and three
    /// other places wrote their own copy of the same ladder anyway — one of them (the sidebar dot) had only
    /// four of the five states, so a registered-but-never-collected server got a grey "Unknown" dot beside an
    /// amber "Awaiting first collection" card, on the same screen, from the same
    /// <see cref="ServerHealthClassifier.ClassifyFreshness"/> call (#2473). Two of the copies live in the
    /// headless service, which cannot reference WPF, so the only place all four can render one ladder is
    /// this assembly.</para>
    ///
    /// <para><b>Why the name is not "card status".</b> Lite has a <c>ServerCardStatus</c> of its own and it
    /// answers a DIFFERENT question: Lite's word comes from a live connection check, this one from how old
    /// the newest collection is. #2457 turned down folding freshness into Lite's word precisely so the two
    /// axes stay apart, and the same distinction already has a type here —
    /// <see cref="Models.ServerConnectionStatus"/> is the connection answer, this is the collection one.
    /// Sharing a name across the two would have invited exactly the conflation both issues were about.</para>
    /// </summary>
    public enum ServerCollectionStatus
    {
        /// <summary>Collection is current.</summary>
        Online,

        /// <summary>Online, but the newest collection has lagged — the amber "Warning".</summary>
        Stale,

        /// <summary>Nothing has landed for long enough to call the server dark.</summary>
        Offline,

        /// <summary>Registered but never collected — the service has not reached it yet, not a dead server.</summary>
        AwaitingFirstCollection,

        /// <summary>Freshness was never classified. <see cref="ServerCollectionStatusRules.FlagsFor"/> cannot
        /// produce this; a hand-built card can, which is exactly why it is a named state rather than a
        /// fall-through.</summary>
        Unknown,
    }

    /// <summary>
    /// The three status flags a freshness band explodes into, as ONE value. Every surface that shows a server
    /// carries these three as separate settable properties (WPF binds them individually — the offline overlay
    /// reads <c>IsOnline</c>, the card border reads all three), so the flags cannot simply be replaced by the
    /// discriminant. What they CAN be is derived in one place: the sidebar dot's own
    /// <c>ApplyFreshness</c> set two of the three and silently dropped the third, which is the whole of #2473.
    /// Returning them together is what makes dropping one a visible edit rather than an omission.
    /// </summary>
    /// <param name="IsOnline">Reachability: true = fresh or stale, false = offline, null = not reached yet.</param>
    /// <param name="HasCollectorErrors">The amber warning flag — in Darling, a stale collection.</param>
    /// <param name="AwaitingFirstCollection">No collection has EVER landed (a bootstrap state, not an outage).</param>
    public readonly record struct ServerCollectionFlags(
        bool? IsOnline,
        bool HasCollectorErrors,
        bool AwaitingFirstCollection);

    /// <summary>
    /// The collection-status ladder, in ONE function, plus the three renderings of its result. Nothing else
    /// in the product may turn a freshness band into a status word.
    ///
    /// <para><b>The failure this exists to make impossible.</b> Four places derived this ladder independently:
    /// the WPF Overview card, the WPF sidebar dot, the web/MCP fleet roll-up, and <c>list_servers</c>. Three
    /// agreed; the sidebar dot had no arm for a never-collected server and fell through to grey "Unknown"
    /// while the card one panel over said amber "Awaiting first collection" (#2473). That is the same defect
    /// #2429 spent two review rounds on, and its argument applies unchanged: with a single discriminant there
    /// is no combination left for the renderings to disagree about, because they no longer each decide.</para>
    ///
    /// <para><b>Three renderings, not one, and that is deliberate.</b> <see cref="Word"/> is what a human
    /// reads on a card, a dot or a roll-up. <see cref="McpToken"/> is what an MCP client keys on — a
    /// consumer API whose values were published as machine tokens and cannot be re-spelled without breaking
    /// downstream automation. <see cref="Headline"/> is the sentence a tooltip opens on. They are three
    /// renderings of one decision; only the decision is shared, and only the decision needed to be.</para>
    /// </summary>
    public static class ServerCollectionStatusRules
    {
        /// <summary>
        /// The (<c>IsOnline</c>, <c>HasCollectorErrors</c>, <c>AwaitingFirstCollection</c>) triple, resolved.
        /// The order matters and is the #2429 reading: an online server's flags win over an awaiting marker,
        /// so a stale card cannot also claim to be awaiting its first collection.
        /// </summary>
        public static ServerCollectionStatus Classify(bool? isOnline, bool hasCollectorErrors, bool awaitingFirstCollection) =>
            isOnline switch
            {
                true when hasCollectorErrors => ServerCollectionStatus.Stale,
                true => ServerCollectionStatus.Online,
                false => ServerCollectionStatus.Offline,
                _ => awaitingFirstCollection ? ServerCollectionStatus.AwaitingFirstCollection : ServerCollectionStatus.Unknown,
            };

        /// <summary>
        /// A freshness band exploded into the three flags every surface binds. <see cref="ServerFreshness.NeverCollected"/>
        /// leaves <c>IsOnline</c> null on purpose: the truth is "unknown, not reached yet", not "was up and died",
        /// and a red Offline on a merely-queued server is what sent a 24-server field report chasing a phantom
        /// scheduler bug.
        /// </summary>
        public static ServerCollectionFlags FlagsFor(ServerFreshness freshness) => freshness switch
        {
            ServerFreshness.NeverCollected => new ServerCollectionFlags(null, false, true),
            ServerFreshness.Offline => new ServerCollectionFlags(false, false, false),
            ServerFreshness.Stale => new ServerCollectionFlags(true, true, false),
            _ => new ServerCollectionFlags(true, false, false),
        };

        /// <summary>
        /// Freshness straight to the discriminant, for the surfaces that carry no flags of their own. Composed
        /// out of <see cref="FlagsFor"/> and <see cref="Classify"/> rather than switching on the band again —
        /// a second switch is a second ladder even when it returns the same type, which is the correction
        /// #2470 had to make once already.
        /// </summary>
        public static ServerCollectionStatus FromFreshness(ServerFreshness freshness)
        {
            var flags = FlagsFor(freshness);
            return Classify(flags.IsOnline, flags.HasCollectorErrors, flags.AwaitingFirstCollection);
        }

        /// <summary>The words a human reads. They are also the <c>DataTrigger</c> values the WPF sidebar keys
        /// its dot colour off, so a word that stopped matching would silently fall through to the muted default
        /// dot rather than fail anything — which is why the viewer pins the trigger set against this enum.</summary>
        public static string Word(this ServerCollectionStatus status) => status switch
        {
            ServerCollectionStatus.Stale => "Warning",
            ServerCollectionStatus.Online => "Online",
            ServerCollectionStatus.Offline => "Offline",
            ServerCollectionStatus.AwaitingFirstCollection => "Awaiting first collection",
            _ => "Unknown",
        };

        /// <summary>
        /// The token the MCP <c>list_servers</c> / <c>get_server_status</c> surface publishes. It differs from
        /// <see cref="Word"/> in exactly one arm, and the difference is load-bearing rather than sloppy:
        /// <c>AwaitingFirstCollection</c> shipped as a machine token beside the pre-existing values, and MCP
        /// status values are a consumer API — clients key on them, so re-spelling one is a breaking change.
        /// Keeping the two vocabularies next to each other is what stops the next reader "fixing" the
        /// inconsistency.
        /// </summary>
        public static string McpToken(this ServerCollectionStatus status) => status switch
        {
            ServerCollectionStatus.Stale => "Warning",
            ServerCollectionStatus.Online => "Online",
            ServerCollectionStatus.Offline => "Offline",
            ServerCollectionStatus.AwaitingFirstCollection => "AwaitingFirstCollection",
            _ => "Unknown",
        };

        /// <summary>What the word MEANS, in words — the first line of whichever tooltip renders it. Every arm
        /// names collection explicitly, because the complaint in #2422 was precisely that a word and a colour
        /// left the reader guessing which axis they were about. In Darling that axis is always collection
        /// freshness: there is no live ping to a monitored server.</summary>
        public static string Headline(this ServerCollectionStatus status) => status switch
        {
            ServerCollectionStatus.Stale => "Warning — collection has lagged on this server",
            ServerCollectionStatus.Online => "Online — collection is current",
            ServerCollectionStatus.Offline => "Offline — nothing has been collected for long enough to call the server dark",
            ServerCollectionStatus.AwaitingFirstCollection =>
                "Awaiting first collection — registered, but the service has not reached it yet",
            _ => "Unknown — this server's collection freshness has not been classified",
        };
    }

    /// <summary>
    /// Per-metric health bands for an Overview card's severity dots — a verbatim mirror of the Dashboard's
    /// <c>HealthSeverity</c>. <see cref="Unknown"/> is a metric with no collected data (e.g. Threads on Azure SQL
    /// DB) — it never escalates the card's overall band.
    /// </summary>
    public enum HealthSeverity
    {
        Unknown,
        Healthy,
        Warning,
        Critical,
    }

    /// <summary>
    /// A server's fleet-health band — the SAME banding an Overview card computes, collapsed to one label per
    /// server (offline / critical → red, warning / stale → amber, else calm).
    /// </summary>
    public enum FleetHealthBand
    {
        Healthy,
        Warning,
        Critical,

        /// <summary>The server's collection is long-dead / never happened (the card's red offline overlay).</summary>
        Offline,
    }

    /// <summary>
    /// The one, documented place for the per-server health thresholds shared by the Darling web dashboard, the
    /// get_fleet_overview MCP tool, and the WPF viewer's Overview cards. Freshness thresholds and the per-metric
    /// severity cutoffs used to live twice (WPF <c>ServerSummaryItem</c> vs the service-side reads) at numerically
    /// equal but independently-editable values — a drift risk. They now live here once; each host maps a band to
    /// its own brush/color, but the numbers are read from this single source (#1562).
    /// </summary>
    public static class ServerHealthThresholds
    {
        /// <summary>
        /// The fastest scheduled collector's cadence (wait_stats / cpu_utilization / memory_stats etc. all run
        /// every minute), so MAX(collection_time) tracks a one-minute rhythm on a healthy server. Freshness bands
        /// are multiples of this.
        /// </summary>
        public static readonly TimeSpan CollectorCadence = TimeSpan.FromMinutes(1);

        /// <summary>Older than twice the cadence = the collection has visibly lagged (Warning).</summary>
        public static readonly TimeSpan StaleThreshold = TimeSpan.FromTicks(CollectorCadence.Ticks * 2);

        /// <summary>Older than this (or no collection at all) = the server is treated as Offline.</summary>
        public static readonly TimeSpan OfflineThreshold = TimeSpan.FromMinutes(15);
    }

    /// <summary>
    /// The raw per-metric inputs a server card bands on — no brushes, no store, no display strings. Every field
    /// is a value the collectors already produced; <see cref="ServerHealthClassifier"/> reduces them to the six
    /// per-metric bands, the card's overall band, and the fleet score. Both the service-side cross-server reader
    /// and the WPF viewer's <c>ServerSummaryItem</c> build this and hand it to the classifier, so the thresholds
    /// live in exactly one place.
    /// </summary>
    public readonly record struct ServerHealthMetrics
    {
        /// <summary>Total non-idle CPU the CPU band evaluates (SQL + other-process), or null with no snapshot.</summary>
        public double? CpuPercentForAlert { get; init; }

        /// <summary>True when the resource semaphore shows grant waiters, timeouts, or forced grants.</summary>
        public bool HasMemoryPressure { get; init; }

        /// <summary>Blocking events in the window.</summary>
        public int BlockingCount { get; init; }

        /// <summary>The worst blocking wait in the window, in seconds.</summary>
        public double MaxBlockedSeconds { get; init; }

        /// <summary>Deadlocks in the window.</summary>
        public int DeadlockCount { get; init; }

        /// <summary>Worker-thread ceiling (max_workers_count), or null with no scheduler snapshot (e.g. Azure SQL DB).</summary>
        public int? TotalThreads { get; init; }

        /// <summary>Available worker threads = ceiling - in-use, or null with no scheduler snapshot.</summary>
        public int? AvailableThreads { get; init; }

        /// <summary>Runnable tasks waiting for a CPU (total_runnable_tasks_count).</summary>
        public int ThreadsWaitingForCpu { get; init; }

        /// <summary>Requests starved of a worker thread (total_work_queue_count).</summary>
        public long RequestsWaitingForThreads { get; init; }

        /// <summary>Collectors whose 7-day band is FAILING (no success in over 24h).</summary>
        public int FailedCollectorCount { get; init; }
    }

    /// <summary>
    /// The single, app-agnostic source of truth for a server's per-metric health bands, its overall card band,
    /// the collection-freshness band, and the fleet-ranking score. Reproduces the Dashboard's <c>ServerHealthStatus</c>
    /// CASE logic exactly. Pure + static so every host (web, MCP tool, WPF viewer) bands identically and the whole
    /// decision table is unit-testable without a store (#1562).
    /// </summary>
    public static class ServerHealthClassifier
    {
        /// <summary>
        /// Classify how fresh the newest collection is. Pure over (last-collection, now). Both instants are UTC
        /// (the store is naive UTC; <paramref name="nowUtc"/> is <see cref="DateTime.UtcNow"/>), so the
        /// subtraction is a true elapsed-time regardless of Kind.
        /// </summary>
        public static ServerFreshness ClassifyFreshness(DateTime? lastCollectionUtc, DateTime nowUtc)
        {
            if (!lastCollectionUtc.HasValue)
            {
                return ServerFreshness.NeverCollected;
            }

            var age = nowUtc - lastCollectionUtc.Value;
            if (age > ServerHealthThresholds.OfflineThreshold)
            {
                return ServerFreshness.Offline;
            }

            if (age > ServerHealthThresholds.StaleThreshold)
            {
                return ServerFreshness.Stale;
            }

            return ServerFreshness.Fresh;
        }

        /// <summary>CPU band on total non-idle CPU: >= 95% Critical, >= 80% Warning; no snapshot Unknown.</summary>
        public static HealthSeverity CpuSeverity(double? cpuPercentForAlert)
        {
            if (!cpuPercentForAlert.HasValue)
            {
                return HealthSeverity.Unknown;
            }

            if (cpuPercentForAlert >= 95)
            {
                return HealthSeverity.Critical;
            }

            if (cpuPercentForAlert >= 80)
            {
                return HealthSeverity.Warning;
            }

            return HealthSeverity.Healthy;
        }

        /// <summary>Memory band — Critical on any resource-semaphore pressure, else Healthy.</summary>
        public static HealthSeverity MemorySeverity(bool hasMemoryPressure) =>
            hasMemoryPressure ? HealthSeverity.Critical : HealthSeverity.Healthy;

        /// <summary>Blocking band: >= 60s max wait or >= 5 events Critical; >= 10s max wait, >= 2 events, or any blocking Warning.</summary>
        public static HealthSeverity BlockingSeverity(int blockingCount, double maxBlockedSeconds)
        {
            if (maxBlockedSeconds >= 60)
            {
                return HealthSeverity.Critical;
            }

            if (blockingCount >= 5)
            {
                return HealthSeverity.Critical;
            }

            if (maxBlockedSeconds >= 10)
            {
                return HealthSeverity.Warning;
            }

            if (blockingCount >= 2)
            {
                return HealthSeverity.Warning;
            }

            if (blockingCount > 0)
            {
                return HealthSeverity.Warning;
            }

            return HealthSeverity.Healthy;
        }

        /// <summary>Deadlock band — any deadlock in the window is Critical.</summary>
        public static HealthSeverity DeadlockSeverity(int deadlockCount) =>
            deadlockCount > 0 ? HealthSeverity.Critical : HealthSeverity.Healthy;

        /// <summary>
        /// Threads band: work-queue starvation Critical; >= 20 runnable-waiting or under 10% workers available
        /// Warning. Unknown when there is no scheduler snapshot.
        /// </summary>
        public static HealthSeverity ThreadsSeverity(int? totalThreads, int? availableThreads, int threadsWaitingForCpu, long requestsWaitingForThreads)
        {
            if (!totalThreads.HasValue)
            {
                return HealthSeverity.Unknown;
            }

            if (requestsWaitingForThreads > 0)
            {
                return HealthSeverity.Critical;
            }

            if (threadsWaitingForCpu >= 20)
            {
                return HealthSeverity.Warning;
            }

            if (totalThreads.Value > 0 && availableThreads < totalThreads.Value * 0.10)
            {
                return HealthSeverity.Warning;
            }

            return HealthSeverity.Healthy;
        }

        /// <summary>Collectors band — any FAILING collector is Warning.</summary>
        public static HealthSeverity CollectorSeverity(int failedCollectorCount) =>
            failedCollectorCount > 0 ? HealthSeverity.Warning : HealthSeverity.Healthy;

        /// <summary>The six per-metric card severities, in card row order — the reuse surface for scoring / reasons.</summary>
        public static IEnumerable<HealthSeverity> MetricSeverities(ServerHealthMetrics m)
        {
            yield return CpuSeverity(m.CpuPercentForAlert);
            yield return ThreadsSeverity(m.TotalThreads, m.AvailableThreads, m.ThreadsWaitingForCpu, m.RequestsWaitingForThreads);
            yield return MemorySeverity(m.HasMemoryPressure);
            yield return BlockingSeverity(m.BlockingCount, m.MaxBlockedSeconds);
            yield return DeadlockSeverity(m.DeadlockCount);
            yield return CollectorSeverity(m.FailedCollectorCount);
        }

        /// <summary>
        /// The card's worst metric band (offline handled separately by the border / overlay). Unknown and Healthy
        /// never escalate — matching <c>ServerHealthStatus.OverallSeverity</c>'s reduce.
        /// </summary>
        public static HealthSeverity OverallMetricSeverity(in ServerHealthMetrics m)
        {
            var worst = HealthSeverity.Healthy;
            foreach (var s in MetricSeverities(m))
            {
                if (s == HealthSeverity.Critical)
                {
                    return HealthSeverity.Critical;
                }

                if (s == HealthSeverity.Warning)
                {
                    worst = HealthSeverity.Warning;
                }
            }

            return worst;
        }

        /// <summary>
        /// Collapses a server's health to one fleet band, mirroring the card border: offline collection -> Offline;
        /// a never-collected (queued-during-bootstrap) server -> Warning (attention-worthy but not the red overlay);
        /// else the card's worst metric band, with a stale collection also Warning.
        /// </summary>
        public static FleetHealthBand ClassifyBand(bool? isOnline, bool awaitingFirstCollection, bool hasCollectorErrors, HealthSeverity overallMetricSeverity)
        {
            if (isOnline == false)
            {
                return FleetHealthBand.Offline;
            }

            if (awaitingFirstCollection)
            {
                return FleetHealthBand.Warning;
            }

            return overallMetricSeverity switch
            {
                HealthSeverity.Critical => FleetHealthBand.Critical,
                HealthSeverity.Warning => FleetHealthBand.Warning,
                _ => hasCollectorErrors ? FleetHealthBand.Warning : FleetHealthBand.Healthy,
            };
        }

        /// <summary>
        /// The worst-first ordering score. Band rank dominates (Offline &gt; Critical &gt; Warning &gt; Healthy) in
        /// steps of 1000; within a band, servers are ranked by how many of the six card metrics are Critical (x100)
        /// or Warning (x10), with the blocking + deadlock counts (capped at 99) as a final tiebreak. The within-band
        /// terms are bounded well under 1000, so they never reorder bands.
        /// </summary>
        public static long FleetHealthScore(FleetHealthBand band, in ServerHealthMetrics m)
        {
            long bandRank = band switch
            {
                FleetHealthBand.Offline => 4000,
                FleetHealthBand.Critical => 3000,
                FleetHealthBand.Warning => 2000,
                _ => 0,
            };

            var criticals = 0;
            var warnings = 0;
            foreach (var sev in MetricSeverities(m))
            {
                if (sev == HealthSeverity.Critical)
                {
                    criticals++;
                }
                else if (sev == HealthSeverity.Warning)
                {
                    warnings++;
                }
            }

            long magnitude = (criticals * 100L) + (warnings * 10L);
            long incidents = Math.Min(m.BlockingCount + m.DeadlockCount, 99);
            return bandRank + magnitude + incidents;
        }

        /// <summary>A short human label for a fleet band ("Healthy" / "Warning" / "Critical" / "Offline").</summary>
        public static string BandLabel(FleetHealthBand band) => band switch
        {
            FleetHealthBand.Critical => "Critical",
            FleetHealthBand.Warning => "Warning",
            FleetHealthBand.Offline => "Offline",
            _ => "Healthy",
        };
    }

    /// <summary>
    /// The one, app-agnostic source of truth for the collector-health ROW banding — the per-collector
    /// NEVER_RUN / NO_PERMISSIONS / FAILING / STALE / WARNING / HEALTHY status shown on every Collection
    /// Health surface (Lite's grid, the Darling WPF viewer's grid + Overview "collectors failing" count,
    /// and the service's <c>get_collection_health</c> MCP tool + web fleet failing-count reader). It was
    /// three byte-identical copies (Lite / viewer / service) with FLAT 4h-STALE / 24h-FAILING thresholds;
    /// nothing pinned them together, so they could drift (#1573), and the flat numbers assumed a frequent
    /// (~1-min) collector — a healthy DAILY collector that succeeds every run still read as STALE past 4h
    /// and FAILING past 24h (index_object_stats at a 1440-min cadence, a real field false-positive).
    /// <para>
    /// The fix makes the staleness thresholds RELATIVE to each collector's own cadence, with floors set to
    /// the original flat values so every FREQUENT collector bands byte-for-byte identically — only a slow
    /// collector (cadence past ~2.7h for STALE / 12h for FAILING) relaxes. Pure + static so all three
    /// surfaces band identically and the whole decision table is unit-testable without a store. Each host
    /// keeps its own SQL, row model, and brush/display mapping; only the band DECISION lives here.
    /// </para>
    /// </summary>
    public static class CollectorHealthClassifier
    {
        /* The band strings every surface's brush / display mapping already switches on — unchanged values. */
        public const string NeverRun = "NEVER_RUN";
        public const string NoPermissions = "NO_PERMISSIONS";
        public const string Failing = "FAILING";
        public const string Stale = "STALE";
        public const string Warning = "WARNING";
        public const string Healthy = "HEALTHY";

        /// <summary>A collector with runs whose error rate exceeds this percent bands WARNING (when not STALE/FAILING).</summary>
        public const double WarningFailureRatePercent = 20.0;

        /* Staleness cutoffs are max(floor, multiplier x the collector's own cadence in hours). The floors are
           the original flat thresholds, so a collector with a cadence at/under the floor is unchanged; only a
           slow collector relaxes. Chosen defaults (#1573): FAILING = max(24, 2 x freqHours) — a 1-min
           collector still fails at 24h, a daily (1440-min) collector fails at 48h; STALE = max(4, 1.5 x
           freqHours) — a 1-min collector still goes stale at 4h, a daily collector goes stale at 36h. So
           index_object_stats (1440-min) at 27h since last success reads HEALTHY, not FAILING — the bug. */

        /// <summary>Hours-since-last-success floor for FAILING — the original flat threshold.</summary>
        public const double FailingFloorHours = 24.0;

        /// <summary>FAILING when hours-since-success exceeds this multiple of the collector's cadence (or the floor, whichever is larger).</summary>
        public const double FailingCadenceMultiplier = 2.0;

        /// <summary>Hours-since-last-success floor for STALE — the original flat threshold.</summary>
        public const double StaleFloorHours = 4.0;

        /// <summary>STALE when hours-since-success exceeds this multiple of the collector's cadence (or the floor, whichever is larger).</summary>
        public const double StaleCadenceMultiplier = 1.5;

        /// <summary>
        /// On-load collectors run once per server connect / tab open, NOT on the scheduled loop, so the
        /// staleness thresholds never apply to them — they are banded by failure rate only (a 100-hour-old
        /// last success is fine). Centralized here so the three surfaces cannot disagree on the set. These
        /// are exactly the <c>FrequencyMinutes == 0</c> entries in <c>CollectorScheduleDefaults</c>, kept as
        /// an explicit name set so this classifier stays free of a dependency on the collector catalog.
        /// </summary>
        private static readonly HashSet<string> OnLoadCollectorNames = new(StringComparer.OrdinalIgnoreCase)
        {
            "server_config",
            "database_config",
            "database_scoped_config",
            "trace_flags",
            "server_properties",
        };

        /// <summary>True for a collector that runs on connect rather than on the scheduled loop (staleness-exempt).</summary>
        public static bool IsOnLoadCollector(string? collectorName) =>
            collectorName is not null && OnLoadCollectorNames.Contains(collectorName);

        /// <summary>
        /// The collectors whose enumeration draws its item list from the target's USER DATABASES — exactly
        /// the collectors that override <c>BuildEnumerationQuery</c> today. For these, and only these,
        /// "the enumeration yielded 0 items" is worth qualifying against whether the target has any user
        /// databases at all (#1852): zero items on a server that has none is the ordinary, legitimate case
        /// and must stay quiet, while zero items on a server that HAS them is the interesting one — a login
        /// that cannot enter any of them, an exclusion filter that swallowed everything, or a feature no
        /// database has turned on.
        ///
        /// <para>Kept as an explicit name set for the same reason <see cref="OnLoadCollectorNames"/> is: so
        /// this classifier stays free of a dependency on the collector catalog. Both apps' suites pin the
        /// set against the catalog's actual enumerators, so a collector cannot start enumerating without a
        /// decision landing here.</para>
        /// </summary>
        private static readonly HashSet<string> UserDatabaseEnumeratorNames = new(StringComparer.OrdinalIgnoreCase)
        {
            "query_store",
            "database_scoped_config",
            "index_object_stats",
            "plan_correction",
            "query_store_health",
        };

        /// <summary>
        /// True when this collector enumerates the target's user databases, so a persistently empty
        /// enumeration can be qualified against whether the target actually has any (#1852). False for every
        /// other collector, which is what keeps an unmapped collector's note unqualified.
        /// </summary>
        public static bool ExpectsUserDatabases(string? collectorName) =>
            collectorName is not null && UserDatabaseEnumeratorNames.Contains(collectorName);

        /// <summary>
        /// The leading text of the shared empty-enumeration note
        /// (<c>EnumeratedCollectorDriver.EmptyEnumerationMessage</c>), matched to tell that note apart from
        /// the probe-failure summary — which names its own cause and needs no inventory qualifier.
        ///
        /// <para>Duplicated here rather than referenced because PerformanceMonitor.Common deliberately does
        /// not depend on PerformanceMonitor.Collectors — the same boundary <see cref="OnLoadCollectorNames"/>
        /// keeps, and inverting it would drag the collector layer into every consumer of Common. Both apps'
        /// suites pin the two strings against each other, so editing one alone fails a build.</para>
        /// </summary>
        public const string EmptyEnumerationMarker = "enumeration yielded 0 items";

        /// <summary>
        /// What the qualifier adds inside the "(all N runs)" parentheses when the inventory says a
        /// persistently empty enumeration is surprising (#1852). Deliberately a statement about the TARGET
        /// rather than a verdict: the row stays HEALTHY and the operator gets the one fact that separates
        /// "nothing to collect" from "collecting nothing".
        /// </summary>
        public const string HasUserDatabasesQualifier = "target has user databases";

        /// <summary>The FAILING cutoff (hours since last success) for a collector of the given cadence.</summary>
        public static double FailingThresholdHours(int frequencyMinutes) =>
            Math.Max(FailingFloorHours, FailingCadenceMultiplier * (frequencyMinutes / 60.0));

        /// <summary>The STALE cutoff (hours since last success) for a collector of the given cadence.</summary>
        public static double StaleThresholdHours(int frequencyMinutes) =>
            Math.Max(StaleFloorHours, StaleCadenceMultiplier * (frequencyMinutes / 60.0));

        /// <summary>
        /// Band one collector's trailing-window roll-up. Order is fixed: NEVER_RUN (no runs at all) ->
        /// NO_PERMISSIONS (only permission denials) -> on-load (failure-rate only, never STALE/FAILING) ->
        /// FAILING -> STALE -> WARNING (failure rate over the threshold) -> HEALTHY.
        /// <paramref name="hoursSinceLastSuccess"/> is the caller's elapsed-hours value — its 999 sentinel
        /// for "ran but never a success" flows straight through to FAILING, exactly as before.
        /// <paramref name="frequencyMinutes"/> is the collector's cadence (callers resolve it from
        /// <c>CollectorScheduleDefaults</c>; 0 for on-load or an unknown collector, which yields the floor
        /// thresholds = the old flat behavior). <paramref name="isOnLoad"/> is <see cref="IsOnLoadCollector"/>.
        /// </summary>
        public static string Classify(
            long totalRuns,
            long successCount,
            long errorCount,
            long permissionDeniedCount,
            double hoursSinceLastSuccess,
            int frequencyMinutes,
            bool isOnLoad)
        {
            if (totalRuns == 0)
            {
                return NeverRun;
            }

            if (permissionDeniedCount > 0 && errorCount == 0 && successCount == 0)
            {
                return NoPermissions;
            }

            var failureRatePercent = totalRuns > 0 ? (double)errorCount / totalRuns * 100 : 0;

            if (isOnLoad)
            {
                return failureRatePercent > WarningFailureRatePercent ? Warning : Healthy;
            }

            if (hoursSinceLastSuccess > FailingThresholdHours(frequencyMinutes))
            {
                return Failing;
            }

            if (hoursSinceLastSuccess > StaleThresholdHours(frequencyMinutes))
            {
                return Stale;
            }

            if (failureRatePercent > WarningFailureRatePercent)
            {
                return Warning;
            }

            return Healthy;
        }

        /// <summary>
        /// Renders the informational note a collector's NON-failing runs left behind (#1837) — an
        /// enumeration that yielded 0 items, items whose enumeration probe failed — qualified by how much
        /// of the window carried it. Empty string when there is nothing to say, which is the overwhelmingly
        /// common case and keeps the column blank for a plainly healthy collector.
        ///
        /// <para>
        /// Deliberately NOT a band and deliberately not an input to <see cref="Classify"/>. A target with
        /// no user databases, no AGs, or nothing matching a collector's filter is legitimately empty and
        /// must keep reading HEALTHY; making "empty" a band would cry wolf on exactly those installs. What
        /// an operator actually needs is the DISTINCTION — "this collector has been coming back with
        /// nothing" as a fact next to the green band, so a zero-row week is a thing you can see instead of
        /// something you have to already suspect. The qualifier carries that: <c>all N runs</c> means every
        /// run in the window came back empty, which is the persistently-empty signal; a fraction means it
        /// happens sometimes, which is normal for a collector whose databases go quiet.
        /// </para>
        ///
        /// <para>
        /// Both counts come from the collection_log aggregate the health grid already reads, so the
        /// qualifier itself adds no signal and no query. #1852 adds ONE more input —
        /// <paramref name="targetHasUserDatabases"/> — and only to the per-server reads, which is what
        /// turns "this collector has been coming back with nothing" into "…on a target that HAS user
        /// databases". Still not a band: <see cref="Classify"/> reads none of this.
        /// </para>
        /// </summary>
        /// <param name="lastNote">
        /// The note text (health SQL's <c>last_note</c>); null/blank = nothing to render. Since #1855 it
        /// is the message from the NEWEST run in the window that left one, so a probe note whose count
        /// moves cycle to cycle shows the latest number rather than the greatest string.
        /// </param>
        /// <param name="noteCount">Runs in the window that carried a note (<c>note_count</c>).</param>
        /// <param name="totalRuns">Runs in the window (<c>total_runs</c>).</param>
        /// <param name="collectorName">
        /// The collector the row describes, used only to decide whether an empty enumeration is worth
        /// qualifying (<see cref="ExpectsUserDatabases"/>). Omitted = unmapped = never qualified.
        /// </param>
        /// <param name="targetHasUserDatabases">
        /// Whether the store observed user databases on this target inside the health read's own window
        /// (health SQL's <c>has_user_databases</c>). Defaults to FALSE, and false means silence: an
        /// inventory that is absent, stale past the window, or simply not collected says nothing about
        /// whether the emptiness is surprising, and a false alarm on a legitimately empty install is
        /// worse than a missing hint. The fleet rollup passes nothing at all, by design.
        /// </param>
        public static string FormatCollectionNote(
            string? lastNote,
            long noteCount,
            long totalRuns,
            string? collectorName = null,
            bool targetHasUserDatabases = false)
        {
            if (string.IsNullOrWhiteSpace(lastNote) || noteCount <= 0)
            {
                return string.Empty;
            }

            /* >= rather than ==: the counts come from one GROUP BY over the same window, so they cannot
               disagree, but "all" must never be the branch that a future off-by-one turns into "97 of 96". */
            if (noteCount < totalRuns || totalRuns <= 0)
            {
                return string.Format(CultureInfo.InvariantCulture, "{0} ({1} of {2} runs)", lastNote, noteCount, totalRuns);
            }

            /* #1852, all three conditions load-bearing:
               - PERSISTENCE is the "all N runs" branch itself. A sometimes-empty collector is normal and
                 gets no qualifier, however much inventory the target has.
               - The note must be the EMPTY-ENUMERATION one. A probe-failure note already says why the
                 enumeration came back short, and appending "target has user databases" to it would restate
                 what the operator just read.
               - The collector must actually enumerate user databases. Everything else — a note from a
                 collector with no inventory to compare against — stays exactly as it read before. */
            var qualified =
                targetHasUserDatabases
                && ExpectsUserDatabases(collectorName)
                && lastNote.Contains(EmptyEnumerationMarker, StringComparison.Ordinal);

            return qualified
                ? string.Format(CultureInfo.InvariantCulture, "{0} (all {1} runs, {2})", lastNote, totalRuns, HasUserDatabasesQualifier)
                : string.Format(CultureInfo.InvariantCulture, "{0} (all {1} runs)", lastNote, totalRuns);
        }
    }

    /// <summary>
    /// Sweep-pressure verdict for get_collection_health (#2296): does the collection body's own execution
    /// demand fit inside its fastest cadence?
    ///
    /// <para><b>Why this number and not delivered-gap statistics:</b> at fleet scale the delivered cadence
    /// stretches for a benign reason — bounded sweep concurrency queues bodies, and the fleet-wide median
    /// gap runs a multiple of the configured minute — so measuring gaps flags every server and drowns the
    /// two that matter. What isolates a SATURATED server is the arithmetic behind its own watchdog line
    /// ("collection body has not completed after Ns of EXECUTION — skipping relaunch"): the collectors'
    /// summed average durations, amortized by cadence, exceed the sweep budget itself. Queueing cannot
    /// inflate this number, because it is built from the collectors' own execution times.</para>
    ///
    /// <para><b>The consequence it names:</b> a body that cannot fit its cadence finishes after the next
    /// due time, every relaunch is skipped, and the server collects at a multiple of its configured
    /// interval — while every collector, from its own point of view, is HEALTHY. That is precisely why
    /// this signal exists: before it, half-rate collection was only visible by reading service-log
    /// warnings (#2296 measured two servers at ~50 skip-warnings/hour with 40 of 40 collectors green).</para>
    ///
    /// <para><b>The second dimension (#2446), and why it is not the verdict:</b> amortizing answers "can
    /// this server's total demand fit inside its cadence on average", and an operator reading a skipped
    /// relaunch is asking "did THIS sweep overrun". Those diverge exactly when one collector's single run
    /// approaches the budget while its amortized cost is negligible — the signature of an infrequent heavy
    /// collector. prod-sql-use2-multi-49 is the measured case: index_object_stats averages 37,207 ms of a
    /// 60,000 ms body, and at a 1440-minute cadence contributes 26 ms/min to a 12,250 ms/min total that
    /// reads OK at 20.4%. So <see cref="SweepPressure.PeakCycleMs"/> adds the collectors' single-run costs
    /// WITHOUT amortizing: the body's cost on the cycle where every cadence comes due together. That cycle
    /// is not a hypothetical worst case. Any set of positive integer cadences coincides on their LCM, so
    /// the aligned body is a periodic certainty for ANY schedule, including one hand-edited in Lite's
    /// schedule editor — the shipped defaults (1 | 5 | 60 | 1440) merely make it frequent, every 1440
    /// minutes, rather than rare.</para>
    ///
    /// <para><b>Which single-run cost, and why not the mean (#2460):</b> #2446 built that cycle out of each
    /// collector's AVERAGE duration, which is a contradiction whenever a collector's run cost is bimodal —
    /// and on this fleet one of them plainly is. <c>query_store</c> on prod-sql-use2-multi-49 reported an
    /// average of 13,834 ms over 1,155 runs, but 958 of those runs carried the empty-enumeration note and
    /// cost about 36 ms each (measured on prod-sql-use2-alpha-01, which yields nothing on all 1,551 of its
    /// runs and pays 36 ms for it). Back that out and the 197 PRODUCTIVE runs cost ~80,900 ms EACH — more
    /// than the entire 60,000 ms budget, on their own, once each. 13,834 ms describes neither population;
    /// it is an 83/17 blend that happens to land in a range that reads like a plausible single number, and
    /// a "worst scheduled cycle" built from it understated that server's worst body by ~67,000 ms.
    ///
    /// So the caller now measures each collector's TAIL run cost as well as its mean — p95 of the same
    /// window's <c>duration_ms</c> values, a statistic the store has always held per run and nothing has
    /// ever read — and the peak cycle is built from <see cref="PeakRunMs"/>. p95 rather than the maximum
    /// because a max is one run: a single pathological cycle would make a collector look permanently
    /// terrible and every server on the fleet read BODY_OVERRUN, which is exactly how a second signal
    /// teaches operators to skip it. p95 also degrades gracefully with sample size — over 3,500 runs it
    /// discards the one bad cycle, and over the six runs a daily collector gets in a week there is no
    /// outlier anyone can afford to discard, so it lands on the max by construction. The maximum is served
    /// beside it as a fact rather than fed into a decision, because comparing a p95 to a max is what tells
    /// a routine cost from a one-off.</para>
    ///
    /// <para>It is reported as its own field with its own vocabulary rather than folded into the verdict,
    /// because a once-daily 37-second collector is not saturation and calling it SATURATED would spend the
    /// word on a case where the capacity lever it recommends is the wrong lever. The verdict keeps meaning
    /// sustained demand; BODY_OVERRUN means one scheduled body does not fit. A fleet scan can filter on
    /// either, which a prose-only footnote would not allow — measured across the dogfood fleet, the two
    /// servers that logged skipped relaunches read OK/BODY_OVERRUN (122% and 109% of budget) while a quiet
    /// one read OK/FITS at 19%, so the dimensions are genuinely orthogonal and the new one discriminates.</para>
    ///
    /// <para>Pure and static like <see cref="CollectorHealthClassifier"/>: the caller resolves each
    /// collector's cadence (from the shared schedule defaults, matching the banding's parity choice) and
    /// only the DECISION lives here, pinned by the same table in both suites.</para>
    /// </summary>
    public static class SweepPressureClassifier
    {
        /* The verdict strings, same switch-friendly shape as the banding's. */
        public const string Ok = "OK";
        public const string AtRisk = "AT_RISK";
        public const string Saturated = "SATURATED";

        /* #2446, the peak-cycle risk strings. Deliberately a SEPARATE vocabulary from the verdict's,
           because the two answer different questions and a reader who meets BODY_OVERRUN sitting beside
           OK must not be able to read it as a fourth saturation band. BODY_OVERRUN is the watchdog's own
           wording ("collection body has not completed after Ns of execution - skipping relaunch"), so the
           field an operator filters on and the service-log line they grep for say the same thing. */
        public const string PeakCycleFits = "FITS";
        public const string PeakCycleBodyOverrun = "BODY_OVERRUN";

        /// <summary>
        /// AT_RISK at 75% of budget: the amortized average leaves no headroom for variance — the slow
        /// collectors on a busy hour are what push a 75% body over its cadence intermittently, which is
        /// how saturation looks before it is constant. Chosen against the #2296 measurements: the two
        /// saturated servers computed ~101%, the in-region fleet sits far below.
        /// </summary>
        public const double AtRiskBusyPercent = 75.0;

        /// <summary>SATURATED at 100%: the body mathematically cannot fit its cadence, so every cycle skips.</summary>
        public const double SaturatedBusyPercent = 100.0;

        /// <summary>
        /// The sweep budget one body has to finish in: the 60,000 ms the fastest shipped cadence holds.
        /// Both dimensions are measured against this same minute, which is the point — the amortized
        /// verdict asks whether the AVERAGE minute's demand fits inside it, and the peak cycle asks
        /// whether the WORST scheduled minute's does.
        /// </summary>
        public const double SweepBudgetMs = 60_000.0;

        /// <summary>
        /// BODY_OVERRUN at 100% of the budget, with no warning band beneath it. The amortized bands need
        /// one because an average smooths spikes and 75% is where variance starts pushing a body over; the
        /// peak cycle is already the worst scheduled case, and a headroom band on top of a worst case
        /// would be a band on a band.
        /// </summary>
        public const double BodyOverrunPercent = 100.0;

        /// <summary>
        /// #2460: how far a collector's tail run has to stand above its mean before the peak-cycle note
        /// says so out loud — twice the mean. For a two-mode population (an empty run costing <i>a</i>, a
        /// productive one costing <i>b</i>, in an 83/17 mix) that fires once <i>b</i> is about 2.5x
        /// <i>a</i>, which is comfortably past anything ordinary run-to-run variance produces and well
        /// short of the 2,000x this was found on. A ratio rather than a millisecond gap so it means the
        /// same thing for a 30 ms collector and a 30-second one.
        /// </summary>
        public const double BimodalTailRatio = 2.0;

        /// <summary>
        /// The single-run cost one scheduled body is charged for a collector: its p95, floored at its mean.
        ///
        /// <para>The floor is load-bearing, not defensive. p95 is not guaranteed to sit above the mean —
        /// a collector with 99 runs at 10 ms and one at 1,000,000 ms has a mean of 10,009 ms and a p95 of
        /// 10 ms — so taking the p95 unconditionally could compute a SMALLER aligned cycle than the
        /// mean-based one #2446 shipped and make a BODY_OVERRUN it already caught disappear. Flooring
        /// makes this change monotonic: the aligned cycle can only ever go up, so #2460 refines #2446's
        /// answer and can never retract it.</para>
        ///
        /// <para>Lives here, public, because both SKUs' get_collection_health also render a per-collector
        /// "% of the sweep budget per run" from the same rule, and a hand-copied floor in two tools is the
        /// drift <see cref="FormatPeakCycleNote"/> exists to avoid.</para>
        /// </summary>
        public static double PeakRunMs(double avgDurationMs, double p95DurationMs) =>
            Math.Max(avgDurationMs, p95DurationMs);

        /// <summary>
        /// Both answers in one pass, over one population of collectors.
        ///
        /// <para><b>Amortized execution demand and its verdict.</b> Each scheduled collector contributes
        /// its average duration divided by its cadence in minutes — milliseconds of work demanded per
        /// minute of wall time for a body that runs collectors serially. Percent is against the 60,000 ms
        /// one minute holds; the fastest shipped cadence is one minute, which is what makes the minute the
        /// budget. This half deliberately keeps using the MEAN even though #2460 hands the method a tail
        /// statistic as well: sustained demand over a window IS the mean, and amortizing a p95 would claim
        /// a rate of work the server never sustains.</para>
        ///
        /// <para><b>The peak cycle and its risk (#2446, #2460).</b> The same collectors' single-run costs
        /// added WITHOUT being divided: what the body costs when every cadence comes due together, which
        /// the nested shipped cadences make a periodic certainty rather than a hypothetical. Each
        /// collector is charged <see cref="PeakRunMs"/> — its p95 floored at its mean — rather than its
        /// mean, because a mean over a bimodal collector describes neither of its populations. Reported
        /// separately from the verdict, never folded into it — see the type's remarks for why.</para>
        ///
        /// <para>A non-recurring collector (<paramref name="collectors"/> entry with frequency &lt;= 0:
        /// on-load, unknown) contributes to NEITHER — it runs on connect, not in the recurring body, so it
        /// does not compete for the sweep and is not part of any scheduled cycle. Nor can it become the
        /// peak collector, which would otherwise name a collector that never shares a body with the ones
        /// it is being compared against.</para>
        /// </summary>
        public static SweepPressure Compute(
            IEnumerable<(string CollectorName, double AvgDurationMs, double P95DurationMs, int FrequencyMinutes)> collectors)
        {
            double busyMsPerMinute = 0;
            double peakCycleMs = 0;
            string? peakCollectorName = null;
            double peakCollectorPeakRunMs = 0;
            double peakCollectorAvgDurationMs = 0;
            int peakCollectorFrequencyMinutes = 0;

            foreach (var (collectorName, avgDurationMs, p95DurationMs, frequencyMinutes) in collectors)
            {
                /* "Nothing measured" is BOTH statistics being empty, not the mean alone: a collector whose
                   mean rounds to nothing is still allowed to contribute a tail, which is the whole shape
                   #2460 is about. */
                if (frequencyMinutes <= 0 || (avgDurationMs <= 0 && p95DurationMs <= 0))
                {
                    continue;
                }

                busyMsPerMinute += avgDurationMs / frequencyMinutes;

                /* The same population as the amortized sum, added WITHOUT being divided: what the body
                   costs on the cycle where every cadence comes due together. Excluding the on-load
                   collectors here is the same choice for the same reason — they run on connect, not in
                   the recurring body, so they are not part of any scheduled cycle. */
                var peakRunMs = PeakRunMs(avgDurationMs, p95DurationMs);
                peakCycleMs += peakRunMs;

                /* Ranked on the SINGLE-RUN cost, which is the question this field answers — and on the
                   tail rather than the mean, so the collector named is the one that actually owns the
                   body. On multi-49 that changes the answer: index_object_stats has the larger mean
                   (37,207 ms against query_store's 13,834) but query_store's heavy run is ~80,900 ms, so
                   it, not the daily collector, is what puts that body over the budget.

                   Strict >, so an exact tie keeps the first collector the caller enumerated rather than
                   letting the answer wobble with the row order of whatever query produced it. */
                if (peakRunMs > peakCollectorPeakRunMs)
                {
                    peakCollectorName = collectorName;
                    peakCollectorPeakRunMs = peakRunMs;
                    peakCollectorAvgDurationMs = avgDurationMs;
                    peakCollectorFrequencyMinutes = frequencyMinutes;
                }
            }

            var busyPercent = busyMsPerMinute / SweepBudgetMs * 100.0;
            var verdict = busyPercent >= SaturatedBusyPercent ? Saturated
                : busyPercent >= AtRiskBusyPercent ? AtRisk
                : Ok;

            var peakCyclePercent = peakCycleMs / SweepBudgetMs * 100.0;
            var peakCycleRisk = peakCyclePercent >= BodyOverrunPercent ? PeakCycleBodyOverrun : PeakCycleFits;

            return new SweepPressure(
                busyMsPerMinute,
                busyPercent,
                verdict,
                peakCycleMs,
                peakCyclePercent,
                peakCycleRisk,
                peakCollectorName,
                peakCollectorPeakRunMs,
                peakCollectorAvgDurationMs,
                peakCollectorFrequencyMinutes);
        }

        /// <summary>
        /// The sentence a BODY_OVERRUN needs, composed HERE rather than at each SKU's tool the way
        /// <see cref="CollectorHealthClassifier.FormatCollectionNote"/> is, because it interpolates the
        /// numbers: two hand-copied format strings would drift the moment one of them was tuned, and the
        /// whole point of this note is that the operator is reading it instead of the amortized figure.
        /// Empty string when the peak cycle fits — a note that fires on the healthy case is how a signal
        /// teaches people to ignore it.
        /// </summary>
        public static string FormatPeakCycleNote(SweepPressure pressure)
        {
            if (pressure is null
                || !string.Equals(pressure.PeakCycleRisk, PeakCycleBodyOverrun, StringComparison.Ordinal)
                || string.IsNullOrWhiteSpace(pressure.PeakCollectorName))
            {
                return string.Empty;
            }

            /* #2460: the bimodal clause, appended only when this collector's tail really does stand above
               its mean. On a collector whose runs all cost about the same the two numbers are the same
               number, and "its mean run is 37,207 ms, so the mean understates one body by 0 ms" is the
               kind of sentence that trains a reader to stop reading the note. */
            var bimodalClause = pressure.PeakCollectorAvgDurationMs > 0
                && pressure.PeakCollectorPeakRunMs >= pressure.PeakCollectorAvgDurationMs * BimodalTailRatio
                ? string.Format(
                    CultureInfo.InvariantCulture,
                    " Its MEAN run is only {0:N0} ms, so its cost is bimodal and any figure built from that mean — the sustained verdict, the heaviest_collectors ranking, and this cycle before #2460 — understates one body by {1:N0} ms.",
                    Math.Round(pressure.PeakCollectorAvgDurationMs),
                    Math.Round(pressure.PeakCollectorPeakRunMs - pressure.PeakCollectorAvgDurationMs))
                : string.Empty;

            return string.Format(
                CultureInfo.InvariantCulture,
                "On the cycle where every scheduled cadence comes due together this body costs {0:N0} ms — {1:N1}% of the {2:N0} ms sweep budget — so that body cannot finish inside its cadence and its relaunch is skipped, however much headroom the sustained {3:N1}% reports. Its largest single contributor is {4}, {5:N0} ms on a heavy run and {6:N1}% of the budget on its own, every {7} minutes. Amortized over that cadence it is worth {8:N0} ms per minute, so the sustained figure and the heaviest_collectors ranked by it both understate what this collector does to one body.{9} The lever here is the schedule's shape — moving or splitting that collector so it stops sharing a cycle — not the capacity answer a SATURATED verdict calls for.",
                Math.Round(pressure.PeakCycleMs),
                pressure.PeakCyclePercent,
                SweepBudgetMs,
                pressure.BusyPercent,
                pressure.PeakCollectorName,
                Math.Round(pressure.PeakCollectorPeakRunMs),
                pressure.PeakCollectorPeakRunMs / SweepBudgetMs * 100.0,
                pressure.PeakCollectorFrequencyMinutes,
                Math.Round(pressure.PeakCollectorAvgDurationMs / pressure.PeakCollectorFrequencyMinutes),
                bimodalClause);
        }
    }

    /// <summary>
    /// One server's sweep-pressure answer, as a single value so a caller cannot drop the verdict from its
    /// numbers — or, since #2446, drop the second dimension from the verdict.
    /// </summary>
    /// <param name="BusyMsPerMinute">Amortized execution demand: milliseconds of collector work per minute of wall time.</param>
    /// <param name="BusyPercent"><paramref name="BusyMsPerMinute"/> against <see cref="SweepPressureClassifier.SweepBudgetMs"/>.</param>
    /// <param name="Verdict">OK / AT_RISK / SATURATED — the SUSTAINED answer, and only that.</param>
    /// <param name="PeakCycleMs">What the body costs on the cycle where every scheduled cadence coincides.</param>
    /// <param name="PeakCyclePercent"><paramref name="PeakCycleMs"/> against the same budget.</param>
    /// <param name="PeakCycleRisk">FITS / BODY_OVERRUN — the SINGLE-SWEEP answer. Never a verdict value.</param>
    /// <param name="PeakCollectorName">The largest single contributor to that cycle, or null when nothing is scheduled.</param>
    /// <param name="PeakCollectorPeakRunMs">
    /// That collector's HEAVY single-run cost — <see cref="SweepPressureClassifier.PeakRunMs"/>, its p95
    /// floored at its mean (#2460). What one aligned body is actually charged for it.
    /// </param>
    /// <param name="PeakCollectorAvgDurationMs">
    /// That same collector's MEAN single-run cost. Carried beside the tail rather than replaced by it for
    /// two reasons: the amortized line in the note has to be computed from the mean (that is what
    /// amortization means), and the gap between the two IS the finding whenever the collector is bimodal.
    /// </param>
    /// <param name="PeakCollectorFrequencyMinutes">That collector's cadence — the number that makes its amortized share small.</param>
    public sealed record SweepPressure(
        double BusyMsPerMinute,
        double BusyPercent,
        string Verdict,
        double PeakCycleMs,
        double PeakCyclePercent,
        string PeakCycleRisk,
        string? PeakCollectorName,
        double PeakCollectorPeakRunMs,
        double PeakCollectorAvgDurationMs,
        int PeakCollectorFrequencyMinutes);
}
