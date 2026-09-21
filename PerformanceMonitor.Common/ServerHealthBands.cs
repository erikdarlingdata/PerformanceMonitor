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
    /// <param name="CollectionStale">The amber warning flag: the newest collection has lagged past
    /// <see cref="ServerHealthThresholds.StaleThreshold"/> but is not old enough to call the server dark. It is
    /// <see cref="ServerFreshness.Stale"/> and nothing else — no error count, no <c>collection_log</c> read.
    ///
    /// <para><b>It is named for freshness because that is its whole population, and the name is load-bearing.</b>
    /// Lite carries a flag of the same shape on its own card (<c>ServerCardStatusRules.Classify</c>) fed from
    /// <c>ErroringCollectors &gt; 0</c> — collectors that are actually failing. Two agents made four wrong
    /// inferences from this one in a day (#3098), every one of them a reasonable reading of a name that said
    /// errors, and the reading each time was falsified by the store: cards flagged with every collector
    /// <c>HEALTHY</c>, and cards clear with <c>ERROR</c> rows inside their own published window. Failure is
    /// reported on the axis that measures it — <c>failed_collector_count</c> and
    /// <see cref="ServerHealthClassifier.CollectorSeverity"/>.</para></param>
    /// <param name="AwaitingFirstCollection">No collection has EVER landed (a bootstrap state, not an outage).</param>
    public readonly record struct ServerCollectionFlags(
        bool? IsOnline,
        bool CollectionStale,
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
        /// The (<c>IsOnline</c>, <c>CollectionStale</c>, <c>AwaitingFirstCollection</c>) triple, resolved.
        /// The order matters and is the #2429 reading: an online server's flags win over an awaiting marker,
        /// so a stale card cannot also claim to be awaiting its first collection.
        /// </summary>
        public static ServerCollectionStatus Classify(bool? isOnline, bool collectionStale, bool awaitingFirstCollection) =>
            isOnline switch
            {
                true when collectionStale => ServerCollectionStatus.Stale,
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
            return Classify(flags.IsOnline, flags.CollectionStale, flags.AwaitingFirstCollection);
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

        /// <summary>
        /// The ONE default for "collection has stopped", shared by the display's Offline band and the alert
        /// engine's Collection Stopped window (#2794). They used to disagree — display called a server dark at a
        /// bare 15 minutes while <c>DarlingSelfAlertEvaluator.StaleWindow</c> deliberately waited 30 — so one
        /// condition had two definitions, and the tighter one false-alarmed: a long <c>query_store</c> cycle
        /// holds the whole sweep body (the sweep skips relaunch while a body runs), so a healthy server
        /// legitimately goes quiet for 12–19 minutes with nothing failed anywhere. Measured on the production
        /// fleet: the worst legitimate inter-collection gap in 24h was 12m12s across 42 servers (issue-day load
        /// reached 19m18s), while genuine dark events run HOURS — so 30 minutes separates the two populations
        /// with real margin on both sides, and it is the number the alert engine already committed to.
        /// <c>AlertsConfig.CollectionStaleMinutes</c> can still widen the ALERT window per deployment; the
        /// display band stays at this shared default (it has no live settings on every surface), which is the
        /// conservative direction — the band can only be tighter than the alert, never looser.
        /// </summary>
        public const int CollectionStoppedMinutesDefault = 30;

        /// <summary>
        /// Older than this (or no collection at all) = the server is treated as Offline. Derived from
        /// <see cref="CollectionStoppedMinutesDefault"/> so the display's "dark" and the alert engine's
        /// "stopped" are the same claim (#2794); a server merely between stretched sweeps bands
        /// <see cref="ServerFreshness.Stale"/>, which is the honest reading.
        /// </summary>
        public static readonly TimeSpan OfflineThreshold = TimeSpan.FromMinutes(CollectionStoppedMinutesDefault);

        /* ── the CPU band's two cutoffs ── */

        /// <summary>
        /// The CPU band's Warning bar, in percent of a FIXED capacity (see
        /// <see cref="ServerHealthClassifier.CpuSeverity"/> for which percentage qualifies). Named here
        /// rather than left as a literal in the classifier because a SECOND surface bands on it: the
        /// Performance Calendar's daily aggregate counts "high-CPU samples" as samples whose total host CPU
        /// is at or above this bar, in SQL, in both SKUs (<c>DailySummarySql.RangeSql</c> and Lite's
        /// <c>LocalDataService.DailySummary</c>), and both suites pin that literal against this constant so
        /// the day cell and the card cannot drift apart on what "high CPU" means (#3539 A2).
        ///
        /// <para><b>Deliberately NOT the alert engine's configurable CPU threshold.</b> That knob governs
        /// when a page is DELIVERED and an operator may retune it; the calendar is a historical record
        /// whose SQL re-counts samples at read time, so binding the day cell to the knob would recolour
        /// every past day the moment the knob moved — the coupling #3539 objects to on the alert-count
        /// signal, and the one signal here where it would actually happen. The card band is a compile-time
        /// constant for the same reason, and it is the card the calendar is the day-scale analogue of.</para>
        /// </summary>
        public const double CpuWarningPercent = 80.0;

        /// <summary>The CPU band's Critical bar, percent of a fixed capacity — see
        /// <see cref="CpuWarningPercent"/> for why the pair is named.</summary>
        public const double CpuCriticalPercent = 95.0;

        /* ── the blocking RATE band (#3539 A3) ── */

        /// <summary>
        /// The shortest window a blocking rate is computed over — the SAME hour the deadlock band uses
        /// (<see cref="DeadlockRateMinimumWindow"/>): every surface that windows one count windows the
        /// other identically (the fleet reader's <c>hours_back</c>, <c>/api/fleet</c>, the viewer card's
        /// fixed hour), so a single minimum keeps the two bands rateable on exactly the same windows.
        ///
        /// <para><b>Written as its own literal, NOT as <c>= DeadlockRateMinimumWindow</c>, on purpose.</b>
        /// Static readonly fields initialise in textual order, and this one is declared above the deadlock
        /// constant; an alias here would read <see cref="TimeSpan.Zero"/> at initialisation and silently
        /// make every window rateable — a 15-minute sweep span multiplied into an hourly rate, and an
        /// undeclared window divided by zero. The tests pin the two equal instead.</para>
        /// </summary>
        public static readonly TimeSpan BlockingRateMinimumWindow = TimeSpan.FromHours(1);

        /// <summary>
        /// WARNING tier of the blocking COUNT arm: blocked-process reports per hour, normalised over the
        /// window.
        ///
        /// <para><b>The top of the measured quiet mode.</b> 14 days of <c>blocked_process_reports</c> on
        /// the 43-server dogfood fleet — 2,398 reports over 14,448 server-hours, bucketed one server-hour
        /// at a time. 99.39% of server-hours hold no report at all; of the 88 that hold any, the
        /// distribution is bimodal: 51 hours hold 1–4 reports (35 hold exactly one), and a storm mode of 23
        /// hours sits at 20 or more (up to 232). The 5–10 band between them holds 5 hours in 14 days. So a
        /// tier at 5 per hour bands the whole quiet mode Healthy — the requirement, for the reason the
        /// deadlock tiers state it: the band exists to find the server in trouble — and fires on 37 of
        /// 14,448 server-hours (0.26%).</para>
        ///
        /// <para>The report count is a fair incident proxy on this population: the storm hours are MANY
        /// distinct victim/blocker pairs (the worst hour's 232 reports were 232 distinct pairs), not one
        /// long chain re-reporting; one pair re-firing up to 15 times exists but is the tail, not the
        /// mode.</para>
        /// </summary>
        public const double BlockingWarnPerHour = 5.0;

        /// <summary>
        /// CRITICAL tier of the blocking COUNT arm: reports per hour.
        ///
        /// <para><b>Placed inside the measured trough, the #3368 method.</b> On the same 14-day distribution
        /// the interval [5, 19] holds 14 server-hours (5 at 5–10, 9 at 11–19) against 23 at 20 or more, and
        /// the quiet mode ends at 4. Twenty is the lower edge of the storm mode — 4x the warning tier, above
        /// every hour of the quiet mode — so it catches a storm as it builds and nothing routine can reach
        /// it. 23 of 14,448 server-hours (0.16%) band Critical by count.</para>
        ///
        /// <para>Compare the rule this replaces: <c>count &gt;= 5 &rarr; Critical</c> fired on 37 of the 88
        /// active hours at a one-hour window, and on five reports per WEEK at the 168-hour one — the same
        /// server, the same code, Critical or Healthy on the window alone (#3539 A3).</para>
        /// </summary>
        public const double BlockingCriticalPerHour = 20.0;

        /// <summary>
        /// The WAIT arm's Critical bar, seconds: the longest single block in the window. Rate-independent
        /// on purpose — a 60-second block is a 60-second block whatever the window, and it is a claim about
        /// one event's magnitude rather than about frequency. Measured: 20 of 2,398 reports (0.83%) reached
        /// it, in 3 server-hours out of 14 days; the per-report distribution is p50 14.2 s, p90 18.9 s,
        /// p99 47.4 s, max 208 s. Unchanged from the pre-#3539 ladder.
        /// </summary>
        public const double BlockingCriticalWaitSeconds = 60.0;

        /// <summary>
        /// The WAIT arm's Warning bar, seconds — unchanged from the pre-#3539 ladder, and deliberately NOT
        /// re-derived here: the measurement that licensed the count tiers was a frequency distribution,
        /// and this arm bands on magnitude. On the measured population the median report already waits
        /// 14 s, so most hours with any blocking reach this bar; what the count tiers change is that
        /// CRITICAL is no longer reachable by a handful of reports in a quiet hour or a quiet week.
        /// </summary>
        public const double BlockingWarnWaitSeconds = 10.0;

        /* ── the deadlock RATE band (#3368) ── */

        /// <summary>
        /// The shortest window a deadlock rate is computed over. A window below this is not rateable and
        /// <see cref="ServerHealthClassifier.DeadlockSeverity"/> declines to band on one.
        ///
        /// <para><b>It costs nothing, which is why it is set here.</b> Every surface that windows this band
        /// takes its span as an integral number of hours with a floor of one:
        /// <c>McpHelpers.ValidateHoursBack</c> refuses anything below 1 and above
        /// <c>McpHelpers.MaxHoursBack</c>, <c>/api/fleet</c> clamps to the same pair, and the viewer's
        /// Overview card reads a fixed one hour. So no window this product can produce is excluded by the
        /// minimum, and the arithmetic below never divides by a fraction.</para>
        ///
        /// <para><b>What it buys is that one sample cannot be multiplied into a rate.</b> A single deadlock
        /// in a 60-second window is 60 per hour, which is true and says nothing: the whole hour was not
        /// observed. Declining to band is the honest reading, and it is the reading
        /// <see cref="HealthSeverity.Unknown"/> exists for on every other metric here.</para>
        /// </summary>
        public static readonly TimeSpan DeadlockRateMinimumWindow = TimeSpan.FromHours(1);

        /// <summary>
        /// WARNING tier, shipped default: deadlocks per hour, normalised over the window.
        ///
        /// <para><b>The 99.94th percentile of a measured production distribution.</b> 14 days of
        /// <c>collect.deadlocks</c> on a 43-server OLTP fleet — 2,722 deadlocks over 14,448 server-hours —
        /// bucketed one server-hour at a time: 86.5% of server-hours hold none, 97.4% hold at most one,
        /// 99.1% at most two, and 99.94% at most five. So a typical server-hour (0, 1 or 2) bands Healthy,
        /// which is the requirement — the band exists to find the server in trouble, and a fleet whose
        /// normal state is 11-20 deadlocks per hour across 43 servers has no such server most hours.</para>
        ///
        /// <para>Twenty of those 14,448 server-hours reach this tier or above, spread over 13 distinct
        /// servers — 18 band Warning and 2 band Critical. That is about one banded server-hour every 37
        /// hours fleet-wide, which is a rate an operator can read.</para>
        /// </summary>
        public const double DeadlockWarnPerHourDefault = 5.0;

        /// <summary>
        /// CRITICAL tier, shipped default: deadlocks per hour.
        ///
        /// <para><b>Placed inside a measured empty interval, not chosen for roundness.</b> The same 14-day
        /// distribution is bimodal with nothing between the two modes: the routine mode tops out at 15
        /// deadlocks in an hour (one server-hour in 14 days), and the next observation at all is <b>90</b>,
        /// then 102. The interval [16, 89] is empty across 43 servers and 14 days. 20 sits near its lower
        /// edge — 4x the warning tier, above every routine hour measured, and 4.5x below the smallest storm
        /// — so it catches a storm while it is building rather than only at full height, and nothing routine
        /// can reach it. Both storm hours band Critical; 0.014% of server-hours do.</para>
        /// </summary>
        public const double DeadlockCriticalPerHourDefault = 20.0;

        /// <summary>
        /// The FLOOR both deadlock-rate knobs clamp to.
        ///
        /// <para><b>It is what keeps the knob a rate.</b> One per hour is the tightest threshold that still
        /// describes a rate rather than an occurrence: on any window longer than an hour it means "more than
        /// one deadlock for every hour observed", so no setting reachable through this knob can restore the
        /// "any deadlock in the window is Critical" reading this band refuses (#3368). That reading is not a tighter
        /// threshold, it is a different claim, and it is unexpressible here by construction.</para>
        ///
        /// <para>A fleet where one deadlock an hour genuinely is the alarm can still say so; a fleet with a
        /// deliberate retry-on-deadlock design raises instead, which is the direction the field asked
        /// for.</para>
        /// </summary>
        public const double DeadlockRatePerHourFloor = 1.0;

        /// <summary>
        /// The CEILING both deadlock-rate knobs clamp to, so the knob stays a threshold rather than becoming
        /// an undisclosed off switch — <c>create_mute_rule</c> silences a signal visibly, is scoped, expires
        /// and is listed; a threshold parked out of reach reports nothing about what it suppressed.
        ///
        /// <para>1,000 per hour is an order of magnitude above the worst hour in the measured distribution
        /// (102). A server deadlocking faster than that is doing little else, and the CPU and blocking bands
        /// reach it first regardless.</para>
        /// </summary>
        public const double DeadlockRatePerHourCeiling = 1000.0;
    }

    /// <summary>
    /// The two settable tiers of the deadlock RATE band (#3368) — deadlocks per hour, normalised over the
    /// window the count was taken from, so one pair of numbers means the same thing on a one-hour read and a
    /// 24-hour one.
    ///
    /// <para><b>Raw in, clamped out.</b> The store holds what the operator set and
    /// <c>get_alert_settings</c> reports it back; these accessors clamp to
    /// <see cref="ServerHealthThresholds.DeadlockRatePerHourFloor"/> ..
    /// <see cref="ServerHealthThresholds.DeadlockRatePerHourCeiling"/> on read, the split
    /// <c>DarlingAlertSettings</c> uses for its own knobs. A hand-edited row cannot drive a nonsense
    /// threshold, and a <c>default</c> struct — every field zero — clamps to the floor rather than to zero,
    /// which would band every measured deadlock Critical.</para>
    ///
    /// <para><b>The critical tier is not floored at the warning tier.</b> Set below it, every fire is
    /// Critical and the Warning tier is empty, which is a coherent reading of what an operator who set it
    /// there asked for — where a <c>Math.Max</c> would silently band on a number no surface reports.</para>
    /// </summary>
    /// <param name="WarnPerHourRaw">The stored warning tier, before clamping.</param>
    /// <param name="CriticalPerHourRaw">The stored critical tier, before clamping.</param>
    public readonly record struct DeadlockRateThresholds(double WarnPerHourRaw, double CriticalPerHourRaw)
    {
        /// <summary>The warning tier the band evaluates, clamped.</summary>
        public double WarnPerHour => Clamp(WarnPerHourRaw);

        /// <summary>The critical tier the band evaluates, clamped independently of the warning tier.</summary>
        public double CriticalPerHour => Clamp(CriticalPerHourRaw);

        /// <summary>The shipped pair — what a store at its V120 column defaults hands the band, and what a
        /// bundle that declares no thresholds is banded on.</summary>
        public static DeadlockRateThresholds Default => new(
            ServerHealthThresholds.DeadlockWarnPerHourDefault,
            ServerHealthThresholds.DeadlockCriticalPerHourDefault);

        private static double Clamp(double value) =>
            double.IsNaN(value)
                ? ServerHealthThresholds.DeadlockRatePerHourFloor
                : Math.Clamp(
                    value,
                    ServerHealthThresholds.DeadlockRatePerHourFloor,
                    ServerHealthThresholds.DeadlockRatePerHourCeiling);
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
        /// <summary>Total non-idle CPU (SQL + other-process, or the Performance Insights instance total),
        /// or null with no snapshot. It is what the CPU band evaluates only where its denominator is FIXED
        /// — see <see cref="CapacityUtilizationPercent"/>.</summary>
        public double? CpuPercentForAlert { get; init; }

        /// <summary>Percent of the CONFIGURED capacity ceiling in use (#3281) — Aurora Serverless v2's
        /// <c>os.general.acuUtilization.avg</c>. The figure the CPU band evaluates when
        /// <see cref="CpuPercentForAlert"/>'s denominator is an allocation that MOVES, which is every
        /// instance in the measured fleet. Null where none was recorded, and a null bands Unknown rather
        /// than Healthy: nothing measured the headroom for that minute.</summary>
        public double? CapacityUtilizationPercent { get; init; }

        /// <summary>Which collector produced <see cref="CpuPercentForAlert"/> — the fact that decides
        /// whether its denominator moves, and so which of the two percentages above the band may read
        /// (#3281). Its default arm is <see cref="FleetCpuSource.NotCollected"/>, so a bundle built by a
        /// path that sets no reading cannot land on an arm meaning "measured".</summary>
        public FleetCpuSource CpuSource { get; init; }

        /// <summary>True when the resource semaphore shows grant waiters, timeouts, or forced grants;
        /// <c>null</c> when this target has no resource-semaphore source at all (#3272 — every PostgreSQL
        /// target). Nullable for the reason <see cref="TotalThreads"/> is: <c>false</c> is a MEASUREMENT
        /// meaning the semaphore is calm, and a target with nothing to read must not be able to make
        /// it.</summary>
        public bool? HasMemoryPressure { get; init; }

        /// <summary>Blocking events in the window, or <c>null</c> when this target has no blocking source
        /// the card reads (#3272). Same reasoning as <see cref="HasMemoryPressure"/>: <c>0</c> is a
        /// measured quiet window.</summary>
        public int? BlockingCount { get; init; }

        /// <summary>The worst blocking wait in the window, in seconds.</summary>
        public double MaxBlockedSeconds { get; init; }

        /// <summary>
        /// How long the window <see cref="BlockingCount"/> was counted over (#3539 A3) — the denominator of
        /// the blocking rate the count arm evaluates. The <see cref="DeadlockWindow"/> discipline, verbatim:
        /// <see cref="TimeSpan.Zero"/> means no window was declared and
        /// <see cref="ServerHealthClassifier.BlockingSeverity"/> refuses to compute a rate from it.
        ///
        /// <para>A property of its own rather than a reuse of the deadlock window, even though every
        /// production bundle fills both from the one variable that windowed both reads: each count carries
        /// its own denominator on its own terms, so a future surface that windows the two differently
        /// cannot silently band one count over the other's span. The rung census pins that every
        /// production bundle declares both.</para>
        /// </summary>
        public TimeSpan BlockingWindow { get; init; }

        /// <summary>Deadlocks in the window, or <c>null</c> when this target has no deadlock source the
        /// card reads (#3272). Same reasoning as <see cref="HasMemoryPressure"/>.</summary>
        public int? DeadlockCount { get; init; }

        /// <summary>
        /// How long the window <see cref="DeadlockCount"/> was counted over (#3368) — the denominator of the
        /// deadlock rate the band evaluates.
        ///
        /// <para><b>A measurement property, so its <c>default</c> has to be unusable.</b>
        /// <see cref="TimeSpan.Zero"/> means no window was declared, and
        /// <see cref="ServerHealthClassifier.DeadlockSeverity"/> refuses to compute a rate from it rather
        /// than dividing by zero or reading it as "none per hour" — which would paint a green dot on a
        /// server that deadlocked. It is the <see cref="FleetCpuSource.NotCollected"/> discipline: a bundle
        /// built by a path that declares no window cannot land on an arm meaning "measured".</para>
        ///
        /// <para>Populated on the count's own terms: a bare deadlock count means different things over an
        /// hour and over a week. <see cref="MaxBlockedSeconds"/> needs no window because a wait duration
        /// carries its own scale; the blocking COUNT does need one, and carries it as
        /// <see cref="BlockingWindow"/> (#3539 A3).</para>
        /// </summary>
        public TimeSpan DeadlockWindow { get; init; }

        /// <summary>
        /// The store's deadlock-rate tiers (#3368), or <c>null</c> to band on the shipped pair.
        ///
        /// <para>Null is the unsupplied-seam fallback its #3297 sibling uses, so a path built before the
        /// knobs existed behaves like a store at its V120 defaults rather than banding on zeros.
        /// <c>DeadlockRateBandRungTests.EveryProductionMetricBundleDeclaresTheWindowAndTheTiers</c> is what
        /// stops a PRODUCTION path taking that fallback: on a store whose tiers were raised, a bundle that
        /// left this null would band on numbers <c>get_alert_settings</c> does not report — the "the setting
        /// did not stick" reading.</para>
        /// </summary>
        public DeadlockRateThresholds? DeadlockRateThresholds { get; init; }

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

        /// <summary>
        /// How many collectors were banded at all for this server in the same health window (#3539 A8d) —
        /// every row of the per-collector aggregate, whatever band it landed on — the denominator that
        /// turns <see cref="FailedCollectorCount"/> into a share. Zero means no denominator was declared;
        /// <see cref="ServerHealthClassifier.CollectorSeverity"/> then bands a non-zero failing count
        /// Warning and never Critical, the fail-away-from-Healthy reading every undeclared denominator
        /// here takes — and a ZERO failing count Unknown rather than Healthy (#3539 A6), because with no
        /// collector banded there was nothing that could have failed.
        /// </summary>
        public int CollectorCount { get; init; }
    }

    /// <summary>
    /// Whether a card's SQL-Server-DMV-sourced metric readings are measurements at all, for this target's
    /// engine (#3272) — the ONE place that decision is made, so the service's fleet card and the viewer's
    /// Overview card cannot disagree about whether a zero means anything.
    ///
    /// <para><b>Why these travel together.</b> The memory-pressure and blocking rows on a card come from
    /// <c>v_memory_grant_stats</c> and <c>v_blocked_process_reports</c> / <c>v_dmv_blocking_snapshots</c> —
    /// SQL Server captures, neither of which a PostgreSQL target has a single row in. The per-metric reads
    /// therefore hand the card zeros, and a zero is indistinguishable from a genuinely calm SQL Server.
    /// Threads already escaped this because its ceiling is nullable and CPU escaped it in #3267; these had
    /// no way to say "not measured" at all. Deadlocks were the third member until #3539 gave the PostgreSQL
    /// card its own count (the <c>pg_stat_database.deadlocks</c> counter, differenced over the window), at
    /// which point the reading has a source on both engines and no longer passes through here. That arm
    /// carries its own measured/not-measured test instead — whether at least one difference was taken in
    /// the window — because a difference of fewer than two samples is not a zero, where a <c>COUNT(*)</c>
    /// over an event table is; the two fleet readers hold that decision beside the read.</para>
    ///
    /// <para><b>Blocking stays here on purpose, and the reason is the shape of the evidence, not its
    /// absence.</b> A PostgreSQL target's blocking IS collected (<c>pg_blocking</c>, <c>pg_lock_stats</c>),
    /// but as per-minute SAMPLES of <c>pg_stat_activity</c>: a waiter seen in three consecutive captures is
    /// one wait observed three times, where SQL Server's blocked-process report is one engine-recorded event
    /// per threshold crossing. <see cref="ServerHealthClassifier.BlockingSeverity"/>'s count tiers were
    /// measured in reports per server-hour on that engine-recorded shape (#3596), and a sampled sighting
    /// count fed through them would band on a denominator the tiers were never measured against — three
    /// sightings of one 3-minute wait is not three reports. An honest PostgreSQL band needs its own
    /// sampled-shape tiers (share of captures holding a waiter, longest observed wait) measured on that
    /// fleet, which is a distribution nobody has taken yet; until then Unknown is the reading, and the
    /// alert (<c>DarlingWorker.EvaluatePgBlockingAsync</c>, root blockers per rolling window) is the
    /// surface that speaks for PostgreSQL blocking. Tiering pending: #3601's <c>lock_wait</c> log-event
    /// family (<c>log_lock_waits</c> "still waiting" lines) is the EVENT-grain evidence a report-rate band
    /// would read — one line per wait past <c>deadlock_timeout</c>, the shape the SQL Server tiers were
    /// measured on — and is the join point for a future PostgreSQL arm here; nothing reads it yet. The
    /// Darling README's engine-coverage table states this beside the band.</para>
    ///
    /// <para><b>It names the ENGINE, not the collector state.</b> A SQL Server whose deadlock collector is
    /// permission-denied also reads zero, and that stays Healthy here on purpose: #3017 routed that case to
    /// <c>failed_collector_count</c> / <see cref="ServerHealthClassifier.CollectorSeverity"/> and the fleet
    /// coverage block, which is where a fixable gap belongs. This distinguishes only the structural case,
    /// where no grant, collector run or upgrade of the monitored server produces the number.</para>
    /// </summary>
    public static class ServerMetricSources
    {
        /// <summary>
        /// The reading as measured, or <c>null</c> when this target's engine has no source behind it.
        /// Generic over the reading's own type because the metrics are a <c>bool</c> and an <c>int</c>,
        /// and the DECISION is the same for both — one function rather than two that could drift.
        /// </summary>
        /// <param name="reading">What the SQL Server metric read produced (a zero, for a target with no rows).</param>
        /// <param name="isPostgres">Whether the store SAYS this target is PostgreSQL. Absence of an engine
        /// token is false, matching <c>MonitoredEngineKind.IsPostgres</c>'s asymmetry: a row no connect has
        /// stamped keeps the SQL Server reading rather than being told its metrics do not exist.</param>
        public static T? DmvSourced<T>(T reading, bool isPostgres)
            where T : struct =>
            isPostgres ? null : reading;
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

        /// <summary>
        /// CPU band: &gt;= 95% Critical, &gt;= 80% Warning; nothing bandable Unknown. One ladder, applied to
        /// whichever percentage is a fraction of a capacity that does not move.
        ///
        /// <para><b>The cutoffs are stated against a QUANTITY, not against a source</b> (#3267), and
        /// #3281 is the finding that the quantity was wrong on one of the two arms. SQL Server's arm is
        /// <c>100 - SystemIdle</c> from the <c>SCHEDULER_MONITOR</c> ring buffer — percent of a FIXED host,
        /// averaged over one minute, so 80 there means "this host is approaching saturation" and the ladder
        /// reads it directly.</para>
        ///
        /// <para><b>The Performance Insights arm bands on CAPACITY HEADROOM instead.</b> PI's
        /// <c>os.cpuUtilization.total.avg</c> is percent of the capacity CURRENTLY ALLOCATED, and all 153
        /// Aurora PostgreSQL instances in the measured fleet are <c>db.serverless</c>, where the allocation
        /// is re-sized continuously — so a one-vCPU instance reads exactly <c>100.0</c> with <c>idle</c>
        /// exactly <c>0.0</c> whenever one core stays busy for a minute, which is the routine trigger for
        /// scaling up. Measured at one such minute: <b>4 of 12 configured ACUs, 33% of the ceiling</b>. The
        /// ladder therefore reads <c>os.general.acuUtilization.avg</c> on this arm, where 100% means the
        /// configured ceiling really is reached. Nothing about the raw reading is miscollected and it stays
        /// collected and shown — it answers "was a core pinned" — it simply is not the saturation
        /// signal.</para>
        ///
        /// <para><b>Where no capacity reading exists the band is Unknown, never Healthy.</b> That is the
        /// rule #3271 set for this same card, and the fallback-to-raw-CPU shape would silently band
        /// percent-of-allocated as saturation. It does mean a hypothetical PROVISIONED instance — whose
        /// raw reading IS a fraction of fixed capacity — bands Unknown rather than on that reading, because
        /// an absent ACU sample cannot be told apart from a serverless instance PI had no capacity sample
        /// for; the measured fleet has no such population, and Unknown is the honest reading of "we do not
        /// know what this percentage is a fraction of".</para>
        ///
        /// <para>Which percentage is bandable is <see cref="FleetCpuProvenance.CpuBandInputPercent"/>'s
        /// decision rather than a branch here, because both cards also report the figure that decided.
        /// CloudWatch's <c>CPUUtilization</c> is still not the source: measured on one instance over one
        /// window at 6.8% against PI's 16.8% (see <c>PgCpuUtilizationCollector</c>).</para>
        /// </summary>
        /// <param name="cpuPercentForAlert">Total non-idle CPU, from whichever collector has it.</param>
        /// <param name="capacityUtilizationPercent">Percent of the configured capacity ceiling in use, or
        /// null where none was recorded.</param>
        /// <param name="cpuSource">Which collector produced the reading. Required rather than defaulted:
        /// a caller that kept the old single-argument call would compile and silently band a serverless
        /// instance's percent-of-allocated as saturation again, which is the entire defect.</param>
        public static HealthSeverity CpuSeverity(
            double? cpuPercentForAlert,
            double? capacityUtilizationPercent,
            FleetCpuSource cpuSource)
        {
            var banded = FleetCpuProvenance.CpuBandInputPercent(
                cpuPercentForAlert, capacityUtilizationPercent, cpuSource);

            if (!banded.HasValue)
            {
                return HealthSeverity.Unknown;
            }

            if (banded >= ServerHealthThresholds.CpuCriticalPercent)
            {
                return HealthSeverity.Critical;
            }

            if (banded >= ServerHealthThresholds.CpuWarningPercent)
            {
                return HealthSeverity.Warning;
            }

            return HealthSeverity.Healthy;
        }

        /// <summary>Memory band — Critical on any resource-semaphore pressure, else Healthy; no source
        /// Unknown (#3272).
        ///
        /// <para><b>The Unknown arm is not cosmetic.</b> This band is read off
        /// <c>v_memory_grant_stats</c>, a SQL Server DMV capture a PostgreSQL target has no row in, so the
        /// zero counters such a card carried argued <c>false</c> and this returned <b>Healthy</b> — a
        /// positive claim of health about a metric nothing measured, rendered as a green dot. That is worse
        /// than the null it sat beside, and it is the same failure <see cref="CpuSeverity"/> and
        /// <see cref="ThreadsSeverity"/> already avoid by taking a nullable input.</para></summary>
        public static HealthSeverity MemorySeverity(bool? hasMemoryPressure)
        {
            if (!hasMemoryPressure.HasValue)
            {
                return HealthSeverity.Unknown;
            }

            return hasMemoryPressure.Value ? HealthSeverity.Critical : HealthSeverity.Healthy;
        }

        /// <summary>
        /// Blocked-process reports per hour over <paramref name="window"/>, or <c>null</c> when the window
        /// is too short to normalise honestly (below <see cref="ServerHealthThresholds.BlockingRateMinimumWindow"/>,
        /// which includes a zero, negative or undeclared one) — <see cref="DeadlockRatePerHour"/>'s arithmetic
        /// over the blocking count (#3539 A3). Public for the same reason: both cards and the day cell REPORT
        /// the rate beside the count, because a band that reads a figure it does not show leaves "Blocking 7"
        /// against a Critical dot with no way to see which tier was crossed.
        /// </summary>
        public static double? BlockingRatePerHour(long blockingCount, TimeSpan window) =>
            RatePerHour(blockingCount, window, ServerHealthThresholds.BlockingRateMinimumWindow);

        /// <summary>
        /// Blocking band (#3539 A3): three arms, in this order — the longest single block's magnitude,
        /// then blocked-process reports per HOUR over the window against
        /// <see cref="ServerHealthThresholds.BlockingCriticalPerHour"/> / <see cref="ServerHealthThresholds.BlockingWarnPerHour"/>,
        /// then the wait arm's Warning bar; no source Unknown (#3272).
        ///
        /// <para><b>A rate on the count, because the count was window-scoped and the band was not.</b> The
        /// ladder this replaces read a raw count over a caller-chosen 1–168 hour window — <c>count &gt;= 5
        /// → Critical</c> — so five reports were Critical at a one-week read and Healthy at a one-hour read
        /// of the same server. Normalising removes the window from the answer, the way
        /// <see cref="DeadlockSeverity"/> did for deadlocks: one pair of tiers means the same condition on
        /// the MCP tool's <c>hours_back</c>, <c>/api/fleet</c>'s window, the viewer card's fixed hour, and —
        /// through the daily classifier — a calendar day and a fleet-sweep span. The tiers come off the
        /// 14-day distribution the tier constants cite: the quiet mode (1–4 reports an hour) bands Healthy
        /// by count; Critical sits at the lower edge of the storm mode.</para>
        ///
        /// <para><b>The wait arms are NOT normalised, and that is the point of having two kinds of arm.</b>
        /// The longest block in the window is a claim about one event's magnitude — 60 seconds blocked is
        /// 60 seconds blocked over any window — where a report count is a claim about frequency, which
        /// means nothing without its denominator. So the 60 s Critical arm fires whatever the rate (a
        /// rate-independent Critical, measured at 3 server-hours in 14 days), the 10 s Warning arm fires
        /// whatever the rate, and only the count is divided by the hours. On the measured fleet the median
        /// report waits 14 s, so Warning-by-blocking stays close to "any blocking" there; what changes is
        /// that Critical stops being reachable by a handful of reports.</para>
        ///
        /// <para><b>The unrateable arm fails away from Healthy, never into Critical</b> — #3368's rule. On a
        /// sub-hour or undeclared window the wait arms still apply (they need no denominator), and past them
        /// a non-zero count reads Warning: blocking demonstrably happened and no rate supports a Critical
        /// claim. A zero count on such a window reads Unknown, not Healthy — a window of no length measured
        /// nothing.</para>
        ///
        /// <para><b>What the count is, on each source.</b> The tiers were measured on
        /// <c>blocked_process_reports</c>; a card that falls back to the DMV blocking snapshots counts
        /// blocked-session snapshots instead (one per blocked session per collection cycle), a coarser unit
        /// on which the same numbers are conservative rather than calibrated — and there the wait arms,
        /// which read the same duration either way, carry the band.</para>
        ///
        /// <para>Compile-time constants, not store-backed knobs: the deadlock tiers' V120 knob is the
        /// precedent for making a threshold settable, and it is the shape this band would take if the
        /// field asks for it — a migration rung of its own, deliberately not ridden in on a re-banding.</para>
        /// </summary>
        /// <param name="blockingCountOrNullWhenUnmeasured">Blocking events counted in the window, or null where
        /// the engine has no blocking source behind the reading. The COUNT carries the measured/not-measured
        /// distinction on its own because a max wait means nothing without a population to have waited (see
        /// <see cref="MemorySeverity"/> for why the arm exists). A <c>long</c> for the daily classifier's
        /// day-scale roll-ups; every <c>int</c> caller widens implicitly.</param>
        /// <param name="maxBlockedSeconds">The longest single block in the window.</param>
        /// <param name="window">How long the count covers. Required rather than defaulted, for the reason
        /// <see cref="DeadlockSeverity"/>'s is: a caller that kept the old two-argument call would compile
        /// and silently band a bare count again, which is the entire defect.</param>
        public static HealthSeverity BlockingSeverity(
            long? blockingCountOrNullWhenUnmeasured,
            double maxBlockedSeconds,
            TimeSpan window)
        {
            if (!blockingCountOrNullWhenUnmeasured.HasValue)
            {
                return HealthSeverity.Unknown;
            }

            var blockingCount = blockingCountOrNullWhenUnmeasured.Value;

            if (maxBlockedSeconds >= ServerHealthThresholds.BlockingCriticalWaitSeconds)
            {
                return HealthSeverity.Critical;
            }

            var ratePerHour = BlockingRatePerHour(blockingCount, window);
            if (ratePerHour.HasValue && ratePerHour.Value >= ServerHealthThresholds.BlockingCriticalPerHour)
            {
                return HealthSeverity.Critical;
            }

            if (maxBlockedSeconds >= ServerHealthThresholds.BlockingWarnWaitSeconds)
            {
                return HealthSeverity.Warning;
            }

            if (!ratePerHour.HasValue)
            {
                return blockingCount > 0 ? HealthSeverity.Warning : HealthSeverity.Unknown;
            }

            return ratePerHour.Value >= ServerHealthThresholds.BlockingWarnPerHour
                ? HealthSeverity.Warning
                : HealthSeverity.Healthy;
        }

        /// <summary>The one division behind both rate bands: events per hour, or null below the band's own
        /// minimum window (which a zero, negative or undeclared window is). One function rather than two so
        /// the two bands cannot disagree about what "too short to rate" means. The explicit positive-window
        /// guard is belt to the minimum's braces: no minimum this class declares is zero, and if one ever
        /// were, a division by a zero window must still be a null rather than an infinite rate.</summary>
        private static double? RatePerHour(long count, TimeSpan window, TimeSpan minimumWindow) =>
            window > TimeSpan.Zero && window >= minimumWindow ? count / window.TotalHours : null;

        /// <summary>
        /// Deadlocks per hour over <paramref name="window"/>, or <c>null</c> when the window is too short to
        /// normalise honestly (below <see cref="ServerHealthThresholds.DeadlockRateMinimumWindow"/>, which
        /// includes a zero, negative or undeclared one).
        ///
        /// <para>Public because both cards REPORT the rate beside the count: a card that bands on a figure
        /// it does not show leaves an operator reading "Deadlocks 3" against a Critical dot with no way to
        /// see which number crossed which tier.</para>
        ///
        /// <para>The count is a <c>long</c> because the daily classifier's signals carry day-scale
        /// roll-ups as longs (#3525); every <c>int</c> caller widens implicitly.</para>
        /// </summary>
        public static double? DeadlockRatePerHour(long deadlockCount, TimeSpan window) =>
            RatePerHour(deadlockCount, window, ServerHealthThresholds.DeadlockRateMinimumWindow);

        /// <summary>
        /// Deadlock band (#3368): deadlocks per HOUR over the window, Critical at
        /// <see cref="DeadlockRateThresholds.CriticalPerHour"/> and Warning at
        /// <see cref="DeadlockRateThresholds.WarnPerHour"/>; no source Unknown (#3272).
        ///
        /// <para><b>A rate, because the count is window-scoped and the band is not.</b> Measured on a
        /// 43-server production OLTP fleet, the count band <c>&gt; 0</c> read 13.4% of one-hour windows
        /// Critical and 87.9% of 24-hour windows Critical — the same servers, the same code, a 6.5x swing
        /// from the window alone. Normalising removes the window from the answer: one pair of tiers means
        /// the same condition on a one-hour read and a 24-hour read, which is what makes the band
        /// comparable across the surfaces that window differently (the MCP tool's <c>hours_back</c>,
        /// <c>/api/fleet</c>'s, and the viewer card's fixed hour).</para>
        ///
        /// <para><b>A resolved deadlock is not a present-tense state, and the tiers are set accordingly.</b>
        /// By the time the engine wrote the graph it had already chosen a victim and rolled something back;
        /// what the band can honestly claim is that deadlocking is frequent enough to be the server's
        /// problem. On the measured distribution a typical server-hour is 0, 1 or 2 deadlocks (99.1% of
        /// them) and bands Healthy — see the tier constants for the percentiles and the empty interval the
        /// Critical tier sits in. An unthresholded band here makes Critical's most common cause the one
        /// condition that has already resolved itself, at which point the label stops discriminating and an
        /// operator scanning for the server in trouble cannot use it (#3368).
        /// <see cref="MemorySeverity"/> and <see cref="ThreadsSeverity"/> keep their unthresholded booleans
        /// because both read a condition holding NOW.</para>
        ///
        /// <para><b>An unrateable window fails away from Healthy, never into Critical.</b> With no honest
        /// denominator the count is all there is, and banding it is the count band this one exists instead of. A
        /// count above zero reads <see cref="HealthSeverity.Warning"/> — deadlocks demonstrably happened,
        /// which #3368 calls a real finding, and no rate supports a Critical claim. A count of zero reads
        /// <see cref="HealthSeverity.Unknown"/>, not Healthy: a window of no length measured nothing, and a
        /// green dot for an unmeasured metric is the failure <see cref="MemorySeverity"/>'s Unknown arm and
        /// <see cref="CpuSeverity"/>'s exist to avoid.</para>
        ///
        /// <para><b>The null arm is for a path with no source, and since #3539 neither engine's card is
        /// one.</b> #3017 established that a PostgreSQL target's <c>v_deadlocks</c> zero was structural and
        /// gave the CARD <see cref="FleetDeadlockSource"/> plus the fleet total a coverage denominator to
        /// say so; the card then passed null here and banded Unknown. #3539 feeds that card the engine's own
        /// count instead — <c>pg_stat_database.deadlocks</c>, a server-maintained counter differenced per
        /// database over the window and summed — through THIS band and THESE tiers, because a deadlock per
        /// hour is the same quantity whichever engine recorded it and the tiers were set on the
        /// rate, not on the instrument. The null arm remains for the PostgreSQL card whose window held no
        /// two samples to difference (a difference of nothing is not a zero), for any caller that genuinely
        /// reads no source, and for the daily classifier's day cells before a count exists. #3017's own rule
        /// is untouched: no band was added to the FLEET ROLLUP, and a quiet, fully-covered fleet keeps
        /// reading healthy.</para></summary>
        /// <param name="deadlockCount">Deadlocks counted in the window, or null where the caller has no
        /// source behind the reading. A <c>long</c> for <see cref="DeadlockRatePerHour"/>'s reason (#3525):
        /// the shared daily classifier routes its day-scale <c>Deadlocks</c> roll-up through this same band,
        /// so the calendar, <c>get_daily_summary</c>, the fleet sweep and the Overview card cannot disagree
        /// about what the same count over the same window means.</param>
        /// <param name="window">How long that count covers. Required rather than defaulted, for the reason
        /// <see cref="CpuSeverity"/>'s source is: a caller that kept the old single-argument call would
        /// compile and silently band a bare count again, which is the entire defect.</param>
        /// <param name="thresholds">The store's tiers. Required for the same reason — a defaulted parameter
        /// would let a surface band on the shipped pair while <c>get_alert_settings</c> reported the
        /// store's.</param>
        public static HealthSeverity DeadlockSeverity(
            long? deadlockCount,
            TimeSpan window,
            DeadlockRateThresholds thresholds)
        {
            if (!deadlockCount.HasValue)
            {
                return HealthSeverity.Unknown;
            }

            var ratePerHour = DeadlockRatePerHour(deadlockCount.Value, window);
            if (!ratePerHour.HasValue)
            {
                return deadlockCount.Value > 0 ? HealthSeverity.Warning : HealthSeverity.Unknown;
            }

            if (ratePerHour.Value >= thresholds.CriticalPerHour)
            {
                return HealthSeverity.Critical;
            }

            if (ratePerHour.Value >= thresholds.WarnPerHour)
            {
                return HealthSeverity.Warning;
            }

            return HealthSeverity.Healthy;
        }

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

        /// <summary>
        /// The share of a server's banded collectors that are FAILING, in percent, or <c>null</c> when no
        /// denominator was declared (<paramref name="collectorCount"/> at or below zero). Public because the
        /// card reasons name it (#3539 A8d): "3 of 40 collectors failing" is the fact the band read.
        /// </summary>
        public static double? FailingCollectorSharePercent(int failedCollectorCount, int collectorCount) =>
            collectorCount > 0 ? failedCollectorCount * 100.0 / collectorCount : null;

        /// <summary>
        /// Collectors band (#3539 A8d): Unknown when NO collector has been banded for this server at all;
        /// Healthy with collectors banded and nothing FAILING; otherwise Warning, escalating to Critical
        /// when the FAILING share of this server's banded collectors exceeds
        /// <see cref="CollectorHealthClassifier.WarningFailureRatePercent"/>.
        ///
        /// <para><b>Graded on a share, because a count of failing collectors was presence-flat.</b> The arm
        /// this replaces was <c>failing &gt; 0 &rarr; Warning</c>, so one FAILING collector of forty and
        /// forty of forty banded identically — a card whose collection had entirely stopped producing data
        /// read the same amber dot as one with a single permission gap. A FAILING collector is one with no
        /// success in over 24 hours (<see cref="CollectorHealthClassifier"/>'s ladder), so the share of them
        /// is the share of this server's collection that has been dark for a day.</para>
        ///
        /// <para><b>The boundary is the one rate bar the product has already committed to for this
        /// evidence, not a new measurement.</b> The collector-health classifier bands a single collector
        /// WARNING when more than 20% of its runs error; the same fifth, applied to the collectors of a
        /// server rather than the runs of a collector, is where Warning becomes Critical here. Stated
        /// plainly: no fleet distribution of failing-collector shares was measured for this tier, and it is
        /// borrowed by analogy from a bar that WAS chosen for the same "how much of the collection is
        /// failing" question one level down. A measured share distribution would be the evidence to move
        /// it, the way the blocking tiers next door were moved.</para>
        ///
        /// <para><b>Any FAILING collector still reads Warning.</b> One collector dark for a day is a real
        /// gap in what this server's other bands can see, and the count is disclosed beside the band, so
        /// the Healthy arm stays reserved for nothing failing. With no denominator declared the share cannot
        /// be formed and a non-zero count fails away from Healthy into Warning, never into Critical — the
        /// unrateable-window discipline the rate bands above follow.</para>
        ///
        /// <para><b>Zero banded collectors is Unknown, not Healthy (#3539, A6's sibling).</b> The arm this
        /// replaces read <c>failing == 0 → Healthy</c> whatever the denominator, on the argument that the
        /// counts come off a seven-day aggregate that only lacks rows for a server that has collected nothing
        /// in a week, which the freshness axis already paints. That argument covers OFFLINE. It does not
        /// cover a server registered and reachable whose first collection has not landed in the aggregate
        /// yet, or a store whose collector-health read returned no rows for it: both handed this band
        /// <c>(0, 0)</c> and got a green dot for a collection nobody had banded — the same positive claim of
        /// health for an unmeasured metric that <see cref="MemorySeverity"/>'s null arm exists to refuse,
        /// one row down the card. "Nothing failing" is only a health claim when there was something that
        /// could have failed; with no collector banded there was not, and Unknown is the honest reading.
        /// The viewer's offline arm and the web's <c>is_online === false</c> chip stay where they are: they
        /// paint a KNOWN-dark server, and this arm paints one nothing has banded yet, which are different
        /// facts on different axes.</para>
        ///
        /// <para>Unknown here is rank-neutral like every other Unknown on the card: the worst-of fold and the
        /// fleet score skip it, and <see cref="MeasuredMetricCounts"/> stops counting the collectors row as
        /// measured, so a card whose ONLY reading used to be this green dot now reads "0 of 6 measured" and
        /// bands through <see cref="OverallMetricSeverity"/>'s nothing-measured arm rather than as Healthy.
        /// A card with any collector banded is unchanged.</para>
        /// </summary>
        /// <param name="failedCollectorCount">Collectors whose seven-day band is FAILING.</param>
        /// <param name="collectorCount">Collectors banded at all in the same window (every band). Required
        /// rather than defaulted, for the reason the rate bands' windows are: a caller that kept the old
        /// one-argument call would compile and silently band presence-flat again.</param>
        public static HealthSeverity CollectorSeverity(int failedCollectorCount, int collectorCount)
        {
            if (failedCollectorCount <= 0)
            {
                /* Nothing failing is Healthy only when something was banded; (0, 0) is a collection nobody
                   has classified, not a clean one. A non-zero failing count with no denominator still falls
                   through to the Warning arm below: a failure was observed even if the population was not. */
                return collectorCount > 0 ? HealthSeverity.Healthy : HealthSeverity.Unknown;
            }

            var share = FailingCollectorSharePercent(failedCollectorCount, collectorCount);
            return share.HasValue && share.Value > CollectorHealthClassifier.WarningFailureRatePercent
                ? HealthSeverity.Critical
                : HealthSeverity.Warning;
        }

        /// <summary>The six per-metric card severities, in card row order — the reuse surface for scoring / reasons.</summary>
        public static IEnumerable<HealthSeverity> MetricSeverities(ServerHealthMetrics m)
        {
            yield return CpuSeverity(m.CpuPercentForAlert, m.CapacityUtilizationPercent, m.CpuSource);
            yield return ThreadsSeverity(m.TotalThreads, m.AvailableThreads, m.ThreadsWaitingForCpu, m.RequestsWaitingForThreads);
            yield return MemorySeverity(m.HasMemoryPressure);
            yield return BlockingSeverity(m.BlockingCount, m.MaxBlockedSeconds, m.BlockingWindow);
            yield return DeadlockSeverity(
                m.DeadlockCount,
                m.DeadlockWindow,
                m.DeadlockRateThresholds ?? DeadlockRateThresholds.Default);
            yield return CollectorSeverity(m.FailedCollectorCount, m.CollectorCount);
        }

        /// <summary>
        /// The card's worst metric band (offline handled separately by the border / overlay). Unknown and Healthy
        /// never escalate — matching <c>ServerHealthStatus.OverallSeverity</c>'s reduce — and a card on which
        /// NO metric was measured folds to <see cref="HealthSeverity.Unknown"/>, not Healthy.
        ///
        /// <para><b>The nothing-measured arm (#3539 A6).</b> The fold skips Unknown so that an unmeasured
        /// metric can never escalate a card, and that is still right: a partially-measured card bands on
        /// what WAS read and <see cref="MeasuredMetricCounts"/> lets every label say how much that was
        /// ("Healthy — 1 of 6 measured", #3528). But a fold over six Unknowns has nothing to fold, and
        /// answering Healthy from it was a positive claim about a server on which not one reading had been
        /// taken — an online server whose collectors had not yet been banded counted in the fleet's healthy
        /// mass and drew a green card, with only a "0 of 6 measured" qualifier to say the green was empty.
        /// Unknown is the reading the fold actually has, and <see cref="ClassifyBand"/> gives it the band
        /// the product already gives a server nothing has measured yet: the awaiting-first-collection
        /// Warning.</para>
        ///
        /// <para>This does not move a partially-measured card. One measured Healthy metric among five
        /// Unknowns still folds to Healthy, exactly as before, so the #3528 rank-neutrality of Unknown holds
        /// wherever there is anything measured to be neutral against; the only cards that move are the ones
        /// on which there was nothing.</para>
        /// </summary>
        public static HealthSeverity OverallMetricSeverity(in ServerHealthMetrics m)
        {
            var worst = HealthSeverity.Unknown;
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
                else if (s == HealthSeverity.Healthy && worst == HealthSeverity.Unknown)
                {
                    /* The first measured reading lifts the fold off Unknown; a Warning already found keeps
                       its place, because Healthy never de-escalates. */
                    worst = HealthSeverity.Healthy;
                }
            }

            return worst;
        }

        /// <summary>
        /// How many of the six card metrics carry a real reading (#3528): measured = severities that are
        /// not <see cref="HealthSeverity.Unknown"/>, out of the per-metric total. The worst-of fold above
        /// SKIPS Unknown — deliberately, so an unmeasured metric can never escalate — which means an online
        /// server with five of six metrics structurally Unknown still folds to Healthy. These counts let a
        /// consumer say so ("Healthy — 1 of 6 measured") instead of rendering that fold as an unqualified
        /// green. Rank-neutrality is untouched: nothing here feeds <see cref="FleetHealthScore"/>.
        /// </summary>
        public static (int Measured, int Total) MeasuredMetricCounts(in ServerHealthMetrics m)
        {
            var measured = 0;
            var total = 0;
            foreach (var s in MetricSeverities(m))
            {
                total++;
                if (s != HealthSeverity.Unknown)
                {
                    measured++;
                }
            }

            return (measured, total);
        }

        /// <summary>
        /// Collapses a server's health to one fleet band, mirroring the card border: offline collection -> Offline;
        /// a never-collected (queued-during-bootstrap) server -> Warning (attention-worthy but not the red overlay);
        /// else the card's worst metric band, with a stale collection also Warning, and a card on which nothing
        /// was measured (<see cref="HealthSeverity.Unknown"/> overall) Warning for the same reason the
        /// never-collected server is.
        ///
        /// <para><b>Why Unknown overall is Warning and not a band of its own (#3539 A6).</b> An online server
        /// with no metric measured is in the same epistemic state as one awaiting its first collection — the
        /// product knows nothing about its health — and that state already has a band here: Warning,
        /// "attention-worthy but not the red overlay", chosen after a 24-server bootstrap incident put a
        /// never-collected fleet under the red Offline overlay. A fifth <see cref="FleetHealthBand"/> member
        /// would have been the alternative, and was rejected: it is a wire-visible enum on <c>/api/fleet</c>
        /// and <c>get_fleet_overview</c>, every band consumer (the web tiles, the viewer's brushes, the
        /// worst-first score's rank steps, the sweep's counts) would need an arm, and the reading it would
        /// give — "this needs a look" — is the one Warning already gives. What this arm changes is that such
        /// a server leaves the healthy mass: <c>healthy_count</c> no longer counts it, and it appears in the
        /// worst-first ranking with a reason that says nothing was measured.</para>
        /// </summary>
        public static FleetHealthBand ClassifyBand(bool? isOnline, bool awaitingFirstCollection, bool collectionStale, HealthSeverity overallMetricSeverity)
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
                HealthSeverity.Unknown => FleetHealthBand.Warning,
                _ => collectionStale ? FleetHealthBand.Warning : FleetHealthBand.Healthy,
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
            /* An unmeasured count contributes nothing, which is what keeps the whole Unknown arm
               rank-neutral: the magnitude terms above already skip Unknown exactly as they skip Healthy,
               so a card that gained an Unknown where it used to claim Healthy scores identically and
               cannot move in the worst-first ranking. Pinned by
               UnmeasuredMetricsAreNotHealthyTests. */
            long incidents = Math.Min((m.BlockingCount ?? 0) + (m.DeadlockCount ?? 0), 99);
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

        /// <summary>
        /// Every attempt in the window was refused because a PostgreSQL extension the collector DECLARES
        /// (<c>ICollectorSchemaInfo.RequiredPgExtensions</c>) is not installed on the target (#3240). Split
        /// out of <see cref="NoPermissions"/> because the two bands demand opposite actions: NO_PERMISSIONS
        /// sends an operator after a grant, and for these rows a grant fixes nothing — the stored message
        /// names the extension and <c>CREATE EXTENSION</c> is the remedy, or leaving it uninstalled is a
        /// legitimate resting state for an optional module. Matches the <c>EXTENSION_MISSING</c>
        /// collection_log status the PostgreSQL fault mapper writes for exactly these runs.
        /// </summary>
        public const string ExtensionMissing = "EXTENSION_MISSING";
        public const string Stopped = "STOPPED";
        public const string Failing = "FAILING";
        public const string Stale = "STALE";
        public const string Warning = "WARNING";
        public const string Healthy = "HEALTHY";

        /// <summary>
        /// The bands in which the collector read NOTHING over the whole window — so a surface reporting a
        /// total assembled from that collector's rows covers none of the window for a server sitting in one
        /// of them. Named as a set rather than compared band-by-band at each call site so a band added later
        /// gets one decision here instead of N independent omissions, each of which fails silently by
        /// counting an unread server as read.
        ///
        /// <para><b>NO_PERMISSIONS, EXTENSION_MISSING and STOPPED are the ones the field produces.</b>
        /// NO_PERMISSIONS is every attempt refused by a grant; EXTENSION_MISSING is every attempt skipped
        /// because the extension the collector declares is not installed (#3240) — a different remedy, the
        /// same "nothing of this collector's is in any total". STOPPED is this classifier's own "attempted
        /// nothing at all — no success, no error, nothing" past the FAILING cutoff, which an extended outage
        /// or a stalled loop reaches while the server is still enabled.</para>
        ///
        /// <para><b>NEVER_RUN is in the set on MEANING, not on reachability.</b> It is <c>totalRuns == 0</c>,
        /// which a <c>GROUP BY</c> over a run log cannot currently produce — no rows, no group — so today a
        /// caller aggregating that way never sees it. That is a property of the QUERY, not of the band: a
        /// later outer join against the collector catalog or the server registry (the natural way to make a
        /// never-invoked collector visible at all) makes it reachable, and leaving it out would then count
        /// the most completely unread server of all as covered. It cannot mean anything but "nothing was
        /// read", so it belongs here whether or not a caller can reach it.</para>
        ///
        /// <para><b>FAILING, STALE and WARNING are deliberately NOT in the set.</b> Those collectors did
        /// read on some cycles and their rows ARE in the total; excluding them would shrink the denominator
        /// and read as a smaller fleet, which is a new wrong number in place of the old one rather than a
        /// fix. HEALTHY is obviously not in it.</para>
        /// </summary>
        public static readonly IReadOnlySet<string> NothingReadBands =
            new HashSet<string>(StringComparer.Ordinal) { NeverRun, NoPermissions, ExtensionMissing, Stopped };

        /// <summary>
        /// True when <paramref name="band"/> is one of <see cref="NothingReadBands"/> — the collector read
        /// nothing in the window, so nothing of its is in any total built from its rows. A null or unknown
        /// band answers FALSE: absence of a band is not a claim that nothing was read, and a caller with no
        /// band at all has to decide that case for itself rather than have this predicate decide it silently.
        /// </summary>
        public static bool ReadNothing(string? band) =>
            band is not null && NothingReadBands.Contains(band);

        /// <summary>A collector with runs whose error rate exceeds this percent bands WARNING (when not STALE/FAILING).</summary>
        public const double WarningFailureRatePercent = 20.0;

        /// <summary>
        /// A collector whose ABANDONED rate exceeds this percent bands WARNING (#2804). An abandoned cycle is
        /// the #2673 whole-server wall-clock budget giving up: it stores nothing and advances no watermark, so
        /// it is guaranteed data loss rather than a retryable fault.
        ///
        /// <para><b>Why abandonment needed its own threshold rather than joining the error rate.</b> An
        /// ABANDONED run increments <c>total_runs</c> and NOTHING else — it is not a success, an error, a
        /// permission denial or a yield. So it grew the failure-rate DENOMINATOR while contributing nothing to
        /// the numerator, and never advanced <c>last_success_time</c>. Total abandonment does eventually trip
        /// STALE then FAILING through the staleness path, because no success lands at all. The gap this closes
        /// is the PARTIAL case: a collector abandoning some cycles while its other cycles still succeed keeps a
        /// fresh last-success, so staleness never fires, its error rate is exactly 0, and it reads HEALTHY
        /// indefinitely while losing cycles.</para>
        ///
        /// <para><b>Why 0.5, from the fleet rather than from taste.</b> Measured across a production store over
        /// 24 hours — 1,639 (server, collector) pairs, 520,455 runs — abandonment is not a continuous rate
        /// phenomenon. Only FOUR pairs abandoned anything at all, 28 runs in total (0.005% fleet-wide), and the
        /// per-pair rate distribution is p50 = p75 = p90 = p95 = <b>p99 = 0.000%</b> with a maximum of 2.157%.
        /// The real population is therefore an empty body and a four-point tail spanning 0.60%–2.16%. 0.5 sits
        /// strictly BELOW that observed floor, so it catches every genuinely-degraded collector on the fleet,
        /// and strictly ABOVE the 99th percentile, so it fires on nobody who is not abandoning. It also keeps a
        /// single isolated abandonment quiet in any window of fewer than ~400 runs, which is the "one in a
        /// thousand is noise, twenty-four is a finding" line. Deliberately far below
        /// <see cref="WarningFailureRatePercent"/>: an error may be transient and is retried, where the 120 s
        /// budget is itself generous (#2673 chose it after measuring a 176 s tail), so reaching it at all means
        /// exceeding a bound already set well above normal.</para>
        ///
        /// <para><b>Why WARNING rather than a new band.</b> WARNING is already the rate-based "degrading but
        /// still running" verdict, which is exactly what partial abandonment is — and a guard firing is not an
        /// ERROR. A new band string would have to be learned by four independent display mappings, and the two
        /// that do not switch on it fail in opposite directions: the web's <c>statusToSev</c> defaults to
        /// "Unknown", while the deprecated Dashboard's brush converter defaults to <c>Transparent</c> — the
        /// same brush it gives HEALTHY. Attribution is not lost by sharing the band, because the ABANDONED
        /// count now sits in the same row as the error count, so a reader can always tell which cause produced
        /// the WARNING.</para>
        /// </summary>
        public const double WarningAbandonRatePercent = 0.5;

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
        /// The collectors for which "zero rows over the whole window" is the DOCUMENTED resting state
        /// (#3754): a row exists only when the monitored engine recorded an event or is inside a condition -
        /// an XE / trace / ring-buffer / server-log capture (a deadlock, a blocked-process report, a long
        /// completion, a system_health or default-trace event, a memory-pressure notification, a logged
        /// PostgreSQL deadlock, log event or captured plan), or a chain / held horizon that exists only while
        /// something is wrong (the SQL Server and PostgreSQL blocking captures, the xmin horizon). These are
        /// the collectors <see cref="FormatOutputFinding"/>'s event-collector sentence - "zero is the
        /// correct resting state on a well-behaved target and needs no action" - is TRUE of. It used to be
        /// offered to every collector that stored nothing and left no note, and on the issue's Azure server
        /// that put it on <c>database_scoped_config</c>, a configuration snapshot that returns a row per
        /// setting per database and for which zero is never a resting state.
        ///
        /// <para><b>Why a name list here, when #3160 refused one for this exact sentence.</b> #3160's
        /// objection was to a list of the PERIODIC collectors - the ones to withhold the sentence from -
        /// because such a list goes stale in the direction that makes it pass: the next snapshot collector
        /// to break gets the reassuring sentence until somebody remembers to add it. This list has the
        /// opposite polarity. It names the collectors to OFFER the sentence to, so an omission fails loud:
        /// a new event capture left off it gets the non-event sentence, which is not reassuring and says
        /// the zero needs a look, and the operator who reads it is the one who adds the name. Kept as an
        /// explicit set for the same reason <see cref="OnLoadCollectorNames"/> and
        /// <see cref="UserDatabaseEnumeratorNames"/> are - this classifier does not depend on the collector
        /// catalog - and pinned by both suites against the catalog's real names so a typo or a rename
        /// cannot silently drop a collector from it.</para>
        ///
        /// <para><b>Deliberately NOT in it:</b> the polled snapshots of current activity (waiting_tasks,
        /// query_snapshots, memory_grant_stats, running_jobs, job_history) whose zero on an idle
        /// target is legitimate too. They are not event captures, and the non-event sentence is worded to
        /// be honest for them without alarm - it says the source returned nothing on every run and that
        /// this needs a look "on a target that has anything for it to report", which an operator reading
        /// an idle development box can answer for themselves. Wrongly OMITTING a collector costs a
        /// non-reassuring sentence; wrongly INCLUDING one costs the reassuring sentence over a broken zero,
        /// which is the defect. So the list stays short.</para>
        /// </summary>
        private static readonly HashSet<string> EventCollectorNames = new(StringComparer.OrdinalIgnoreCase)
        {
            "deadlocks",
            "blocked_process_report",
            "long_query_completions",
            "system_health_events",
            "default_trace_events",
            "memory_pressure_events",
            "dmv_blocking_snapshot",
            "pg_deadlocks",
            "pg_log_events",
            "pg_plan_capture",
            "pg_blocking",
            "pg_xmin_horizon",
        };

        /// <summary>
        /// True when this collector stores a row only when an event occurred or a condition held, so a
        /// zero-row window is its documented resting state (<see cref="EventCollectorNames"/>, #3754). False
        /// for every other collector, and for a null name - absence of a name is not a claim about category.
        /// </summary>
        public static bool IsEventCollector(string? collectorName) =>
            collectorName is not null && EventCollectorNames.Contains(collectorName);

        /// <summary>
        /// The names in <see cref="EventCollectorNames"/>, exposed read-only so both suites can pin the set
        /// against the catalog's real collector names (every entry must be a collector that exists) and
        /// against the four the <c>get_collection_health</c> description has always named as its examples.
        /// </summary>
        public static IReadOnlyCollection<string> EventCollectorNamesForPinning => EventCollectorNames;

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

        /// <summary>
        /// Whether a collector's newest DENIAL postdates its newest SUCCESS — the answer to the only
        /// question a reader of <c>last_error</c> actually has: is this the collector's current state, or a
        /// fault from a code path it no longer takes (#3010)?
        ///
        /// <para><b>The field this exists for cannot be read correctly without it.</b> <c>last_error</c> is
        /// a single retained slot holding the newest ERROR/PERMISSIONS message in a seven-day window, and
        /// both MCP <c>get_collection_health</c> tools served it with no timestamp at all. So a message
        /// from six days ago, recorded on a route the collector has since stopped taking, read exactly like
        /// one from the last cycle — and nothing on the surface could contradict the assumption.</para>
        ///
        /// <para><b>Measured, and it produced a filed issue.</b> <c>pg_deadlocks</c> moved from the
        /// in-database <c>pg_read_file</c> route to the RDS log API. Its 15,885 PERMISSIONS runs stop dead
        /// at the cutover; all 50 targets have returned SUCCESS every day since. Six days later
        /// <c>get_collection_health</c> still showed HEALTHY, <c>errors 0</c>, a reassuring note, AND the
        /// stale 42501 — every element individually true, together describing a server being refused right
        /// now, which was false. #2994 was filed on that reading and closed as not-a-defect.</para>
        ///
        /// <para><b>Why a comparison of instants and not a rate.</b> A denial RATE would read 15.9% on
        /// that collector today and call it denied — sending an operator to issue a grant for a route the
        /// collector does not use. Two stored instants out of one aggregate over one window answer the
        /// currency question directly, and the answer flips the moment a success lands.</para>
        ///
        /// <para><b>Deliberately NOT an input to <see cref="Classify"/>.</b> This reports; it does not
        /// band. The banding chain is untouched by #3010, and widening a band on this predicate is a
        /// separate question with its own evidence bar — one nothing measured here clears.</para>
        /// </summary>
        /// <param name="permissionDeniedCount">PERMISSIONS runs in the window (<c>permission_denied_count</c>).</param>
        /// <param name="errorCount">
        /// ERROR runs in the window; any at all makes this false. The precondition lives WITH the
        /// derivation rather than at each caller so relaxing one cannot silently widen the other: with an
        /// ERROR present, "denied since the last success" is no longer the whole story of what went wrong.
        /// </param>
        /// <param name="lastSuccessTimeUtc">
        /// The newest SUCCESS/SKIPPED instant (<c>last_success_time</c>), or null when the window holds no
        /// success — in which case a denial is trivially the newest outcome.
        /// </param>
        /// <param name="lastDeniedTimeUtc">
        /// The newest PERMISSIONS instant (<c>last_denied_time</c>). Null says nothing was denied inside
        /// the window, whatever the count claims, so this returns false.
        /// </param>
        public static bool DeniedSinceLastSuccess(
            long permissionDeniedCount,
            long errorCount,
            DateTime? lastSuccessTimeUtc,
            DateTime? lastDeniedTimeUtc)
        {
            if (permissionDeniedCount <= 0 || errorCount > 0 || lastDeniedTimeUtc is null)
            {
                return false;
            }

            /* Both instants come from ONE aggregate over ONE window, so this compares two stored values
               rather than two clock reads — which is why the decision takes timestamps instead of two
               independently-computed elapsed-hours doubles. Two DateTime.UtcNow subtractions taken
               microseconds apart can order equal instants either way, and this answer has to be stable.

               Strictly greater: equal instants are NOT "denied since". A tie is a window whose newest
               success and newest denial landed in the same cycle, where "which came last" is not a fact
               the store holds, and claiming currency on a coin flip is the defect this reports on. */
            return lastSuccessTimeUtc is null || lastDeniedTimeUtc.Value > lastSuccessTimeUtc.Value;
        }

        /// <summary>
        /// A collector that WAS producing rows and now reports a named skip every cycle has REGRESSED, and
        /// this is the predicate that says so (#3819). Three stored instants out of the one aggregate the
        /// health reads already take, so a caller can never compose it from instants describing different
        /// windows.
        ///
        /// <para><b>The reading this exists to end.</b> The <c>.453</c> install took
        /// <c>pg_statement_stats</c> from 85% productive to <c>EXTENSION_MISSING</c> every cycle on 23 of
        /// 50 PostgreSQL clusters. For the first day after it, the row read HEALTHY — the ladder saw a
        /// recent success (the last productive cycle, hours old), no errors, and rates of zero — and the
        /// install countersign, which reads this surface, accepted that for 24 hours. Only when the success
        /// clock ran past the FAILING cutoff did any band move. A named skip on a collector that had been
        /// producing is a different fact from the same status on one that never has, and nothing here could
        /// tell them apart.</para>
        ///
        /// <para><b>Why the band arms cannot catch it.</b>
        /// <see cref="ExtensionMissing"/> and <see cref="NoPermissions"/> are both gated on
        /// <c>successCount == 0</c> — the window's story has to be nothing but the skip. A regressed
        /// collector's window holds its productive days, so it never reaches either arm; it falls through
        /// to the staleness ladder and reads HEALTHY until the success ages out. The two conditions are
        /// therefore disjoint by construction: the benign band and this predicate cannot both describe one
        /// row, which is what keeps the Aurora optional-extension class honest.</para>
        ///
        /// <para><b>Why it takes instants rather than a status word.</b> The named-skip vocabulary lives in
        /// <c>CollectorRuntimePrecondition</c>, in the collector assembly, and this one deliberately
        /// depends on nothing (the boundary <see cref="OnLoadCollectorNames"/> keeps). The reads resolve
        /// the vocabulary at the STORE — where the question is asked of every row anyway — and hand back
        /// two instants: when this collector last did something that was NOT a named skip, and when it
        /// last stored a row. Both are plain aggregates over the window the read already takes, so the
        /// fleet rollup and the per-server grid compute the identical predicate without the fleet read
        /// growing a window function.</para>
        ///
        /// <para><b>What it is bounded by, stated rather than implied.</b> Both reads window seven days,
        /// so this can only see productivity that is still inside that window. A skip older than the
        /// window leaves no productive row to find and the collector reverts to its ordinary band — which
        /// is the honest answer, because at that point the read holds no evidence of a regression, and a
        /// WARNING that never expires is a mute nobody tracks. The install countersign this serves reads
        /// the surface within hours of an install, which is well inside that bound.</para>
        /// </summary>
        /// <param name="lastRunTimeUtc">The newest run of ANY status (<c>last_run_time</c>). Null means the
        /// collector left no row in the window at all, which is NEVER_RUN's question, not this one.</param>
        /// <param name="lastNonSkipTimeUtc">
        /// The newest run whose status was NOT a named skip (<c>last_non_skip_time</c>) — the instant the
        /// current skip streak began after. Null means every run in the window was a skip, which is the
        /// never-produced-here case the benign band already describes correctly.
        /// </param>
        /// <param name="lastProductiveTimeUtc">The newest run that stored rows
        /// (<c>last_productive_time</c>). Null means the window holds no productivity to have regressed
        /// from.</param>
        public static bool RegressedFromProductive(
            DateTime? lastRunTimeUtc,
            DateTime? lastNonSkipTimeUtc,
            DateTime? lastProductiveTimeUtc)
        {
            if (lastRunTimeUtc is null || lastNonSkipTimeUtc is null || lastProductiveTimeUtc is null)
            {
                return false;
            }

            /* Currently skipping: the newest run postdates the newest non-skip, so every run since that
               instant was a named skip. Strictly greater, for the reason DeniedSinceLastSuccess is strict —
               equal instants mean the newest run IS the non-skip one, and a tie decided the other way
               would claim a streak that has not started. */
            if (lastNonSkipTimeUtc.Value >= lastRunTimeUtc.Value)
            {
                return false;
            }

            /* And the productivity has to sit BEFORE the streak rather than inside it. A named skip stores
               nothing, so this holds on every row the store can currently produce; asserted anyway, because
               what makes this a regression is the ORDER of the two facts, and a predicate that assumed the
               order would be reporting its own assumption. */
            return lastProductiveTimeUtc.Value <= lastNonSkipTimeUtc.Value;
        }

        /// <summary>
        /// The band a row carries once <see cref="RegressedFromProductive"/> is known: WARNING where the
        /// ladder said HEALTHY, and the ladder's own answer everywhere else (#3819).
        ///
        /// <para><b>It can only ever make a row louder.</b> HEALTHY is the only band a regressed collector
        /// reaches that is quieter than WARNING — the two benign skip bands are unreachable for it (see
        /// <see cref="RegressedFromProductive"/>), and STALE, FAILING and STOPPED all already say more than
        /// WARNING would. So this is a floor, not a re-banding: nothing that was alarming becomes less so,
        /// and the attribution a louder band carries is not traded away for a flag. A row that keeps its
        /// louder band still reports <c>regressed_from_productive</c> and still counts in the fleet's
        /// regressed total, because the flag and the band answer different questions.</para>
        ///
        /// <para><b>WARNING rather than a new band string</b>, for the reason
        /// <see cref="WarningAbandonRatePercent"/> gives: WARNING is already the "degrading but still
        /// running" verdict, and a new string would have to be learned by four independent display
        /// mappings, two of which default in opposite directions — one to "Unknown" and one to the brush it
        /// gives HEALTHY. Attribution is not lost by sharing the band, because the regression flag and its
        /// finding sit on the same row.</para>
        /// </summary>
        public static string BandWithRegression(string band, bool regressedFromProductive) =>
            regressedFromProductive && string.Equals(band, Healthy, StringComparison.Ordinal)
                ? Warning
                : band;

        /// <summary>
        /// The sentence a regressed collector carries (#3819), or null when the row is not one. States the
        /// three facts an operator needs in the order they happened: how much it was producing, when it
        /// stopped, and what it has said since.
        ///
        /// <para>The closing clause names a restart or an upgrade because that is what the measured case
        /// was, and because it is the one cause an operator reading a collection-health surface would not
        /// otherwise consider: nothing about the monitored server changed, so the natural reading is that
        /// the target lost something, when in fact the monitoring build did.</para>
        ///
        /// <para><paramref name="rowsInPriorWindow"/> is the row's <c>rows_stored</c> over the read's own
        /// seven-day window. On a regressed row that is entirely pre-regression, because a named skip
        /// stores nothing — which is why the count can be taken from the window total instead of needing a
        /// second, differently-bounded aggregate that could describe different runs.</para>
        /// </summary>
        /// <param name="rowsInPriorWindow">Rows stored over the window (<c>rows_stored</c>).</param>
        /// <param name="lastNonSkipTimeUtc">The instant the skip streak began after
        /// (<c>last_non_skip_time</c>) — the newest run that was not a named skip.</param>
        /// <param name="currentStatus">The status the collector has been reporting since
        /// (<c>current_status</c>). Null on a surface that does not read it, which answers null rather than
        /// composing a sentence with a hole in it.</param>
        public static string? FormatRegressedFromProductiveFinding(
            long rowsInPriorWindow,
            DateTime? lastNonSkipTimeUtc,
            string? currentStatus)
        {
            if (lastNonSkipTimeUtc is null || string.IsNullOrWhiteSpace(currentStatus))
            {
                return null;
            }

            return string.Format(
                CultureInfo.InvariantCulture,
                "produced {0:N0} rows in the seven days before {1}; has reported {2} since — a restart or "
                + "upgrade changed what this collector can read",
                rowsInPriorWindow,
                DateTime.SpecifyKind(lastNonSkipTimeUtc.Value, DateTimeKind.Utc)
                    .ToString("u", CultureInfo.InvariantCulture),
                currentStatus);
        }

        /// <summary>The FAILING cutoff (hours since last success) for a collector of the given cadence.</summary>
        public static double FailingThresholdHours(int frequencyMinutes) =>
            Math.Max(FailingFloorHours, FailingCadenceMultiplier * (frequencyMinutes / 60.0));

        /// <summary>The STALE cutoff (hours since last success) for a collector of the given cadence.</summary>
        public static double StaleThresholdHours(int frequencyMinutes) =>
            Math.Max(StaleFloorHours, StaleCadenceMultiplier * (frequencyMinutes / 60.0));

        /// <summary>
        /// Band one collector's trailing-window roll-up. Order is fixed: NEVER_RUN (no runs at all) ->
        /// EXTENSION_MISSING (a declared extension absent, #3240) -> NO_PERMISSIONS (only permission
        /// denials) -> on-load (failure-rate only, never STOPPED/STALE/FAILING) -> STOPPED (no attempt of
        /// ANY kind recently, despite a history of runs) -> FAILING -> STALE -> WARNING (failure rate OR
        /// abandon rate over its own threshold) -> HEALTHY.
        /// <paramref name="extensionMissingCount"/> is runs recorded <c>EXTENSION_MISSING</c> — the
        /// PostgreSQL fault mapper's named skip for a source whose DECLARED extension is not installed
        /// (#3240); like the permission count, any success or error makes the window's story bigger than
        /// the skip and the row falls through to the ordinary ladder.
        /// <paramref name="abandonedCount"/> is runs the #2673 wall-clock budget gave up on; see
        /// <see cref="WarningAbandonRatePercent"/> for why it bands WARNING on its own much lower rate and
        /// why it needed a band at all when a partially-abandoning collector reaches neither STALE nor FAILING.
        /// <paramref name="hoursSinceLastSuccess"/> is the caller's elapsed-hours value — its 999 sentinel
        /// for "ran but never a success" flows straight through to FAILING, exactly as before.
        /// <paramref name="hoursSinceLastRun"/> is hours since the newest run of ANY status (success,
        /// error, or permissions) — see <see cref="Stopped"/> below for why this is a separate input from
        /// <paramref name="hoursSinceLastSuccess"/>. <paramref name="frequencyMinutes"/> is the collector's
        /// cadence (callers resolve it from <c>CollectorScheduleDefaults</c>; 0 for on-load or an unknown
        /// collector, which yields the floor thresholds = the old flat behavior). <paramref name="isOnLoad"/>
        /// is <see cref="IsOnLoadCollector"/>.
        /// </summary>
        public static string Classify(
            long totalRuns,
            long successCount,
            long errorCount,
            long permissionDeniedCount,
            long extensionMissingCount,
            long abandonedCount,
            double hoursSinceLastSuccess,
            double hoursSinceLastRun,
            int frequencyMinutes,
            bool isOnLoad)
        {
            if (totalRuns == 0)
            {
                return NeverRun;
            }

            /* #3240, and BEFORE the permission arm on purpose. The only population that carries both
               counts with no success and no error is one condition recorded under two vocabularies: a
               window straddling the upgrade that split this status out of PERMISSIONS holds the same
               absent-extension fault under both names, and banding it NO_PERMISSIONS for the seven days
               the old rows take to age out would keep the wrong hint alive for exactly the deployments
               the split is for. The two SQLSTATEs describe the same read, so one collector cannot be
               grant-refused and extension-absent in the same cycle; a target that moved between the two
               states lands successes or newer rows that resolve the tie as the window slides. */
            if (extensionMissingCount > 0 && errorCount == 0 && successCount == 0)
            {
                return ExtensionMissing;
            }

            if (permissionDeniedCount > 0 && errorCount == 0 && successCount == 0)
            {
                return NoPermissions;
            }

            var failureRatePercent = totalRuns > 0 ? (double)errorCount / totalRuns * 100 : 0;

            /* #2804. Kept as its own rate rather than folded into failureRatePercent: the two have very
               different thresholds (0.5 against 20) precisely because they mean different things, and adding
               abandonment to the error numerator would have made a 2%-abandoning collector read as a 2%-erroring
               one — a number no run actually produced. Both counts reach the surface, so the reader can always
               attribute the band. */
            var abandonRatePercent = totalRuns > 0 ? (double)abandonedCount / totalRuns * 100 : 0;

            if (isOnLoad)
            {
                /* On-load collectors are staleness-exempt and banded by rate only, so abandonment has to be
                   asked here too — otherwise the one class of collector that CANNOT reach the staleness safety
                   net below would be the one class where abandonment stays invisible. */
                return failureRatePercent > WarningFailureRatePercent || abandonRatePercent > WarningAbandonRatePercent
                    ? Warning
                    : Healthy;
            }

            /* STOPPED: a collector whose LAST ATTEMPT OF ANY KIND — success, error, or
               permissions, not just success — is older than the FAILING cutoff has not been invoked at
               all, which is a different fact from "it runs and keeps failing". A collector that is still
               being invoked and erroring every cycle has a RECENT hoursSinceLastRun (the failure itself
               is a run), so it falls through to FAILING below exactly as before; this branch only catches
               genuine silence. hoursSinceLastRun can never exceed hoursSinceLastSuccess (every success is
               a run), so this is strictly a subset of what would otherwise read FAILING — it recategorizes
               rather than suppresses. The house case: a collector whose AppliesTo gate flipped off for a
               target (an RDS instance where SQL Agent job/status collection is not applicable) stops being
               invoked entirely; its last historical success sits inside the health window and ages past
               the FAILING cutoff, reading as an alarming "this keeps failing" when nothing has been
               attempted in either direction. Reusing FailingThresholdHours rather than a new constant: the
               question here is the same one FAILING already asks ("has this collector gone dark for too
               long"), just asked of attempts instead of successes. */
            if (hoursSinceLastRun > FailingThresholdHours(frequencyMinutes))
            {
                return Stopped;
            }

            if (hoursSinceLastSuccess > FailingThresholdHours(frequencyMinutes))
            {
                return Failing;
            }

            if (hoursSinceLastSuccess > StaleThresholdHours(frequencyMinutes))
            {
                return Stale;
            }

            /* Below STALE/FAILING deliberately. A collector that has not succeeded in far too long is the
               louder fact and keeps its band; abandonment is the one that would otherwise have NO band at all,
               because a partially-abandoning collector still lands successes and so never ages into either. */
            if (failureRatePercent > WarningFailureRatePercent || abandonRatePercent > WarningAbandonRatePercent)
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

        /// <summary>
        /// What <c>get_collection_health</c>'s output figures are measured over, and — the load-bearing
        /// half — what they are NOT (#3017).
        ///
        /// <para><b>Both windows named, because only one of them was read here.</b> <c>rows_stored</c> and
        /// <c>runs_with_rows</c> come out of the SAME aggregate over the SAME fixed trailing seven days as
        /// <c>total_runs</c> and the duration statistics beside them, so cost and output on one row always
        /// describe one set of runs. <c>get_collector_cost</c>'s <c>total_rows</c> is a different
        /// measurement entirely — a separate hourly series, summed over the caller's own <c>days_back</c>,
        /// across every server at once — and this note disclaims it outright rather than letting a reader
        /// assume the two reconcile. That is #3027's discipline one level down: a surface must not assert a
        /// scope it did not measure.</para>
        ///
        /// <para><b>The disclaimed tool is attributed to Darling on purpose.</b> One note serves both SKUs
        /// (the whole reason it lives here), but <c>get_collector_cost</c> is Darling-ONLY by architecture —
        /// it reads the central store's own hourly self-metric, which a single-instance Lite install has no
        /// twin of. Unattributed, the sentence would point a Lite caller at a tool that SKU does not expose.
        /// Naming the SKU keeps one string honest on both rather than splitting it per SKU, which is the
        /// drift this class lives in Common to prevent.</para>
        ///
        /// <para><b>And the third thing it is not.</b> <c>rows_stored</c> counts what a run STORED. Nothing on
        /// this surface reads what the monitored engine COUNTED, so a zero here cannot separate a
        /// genuinely quiet source from a reader that is capturing nothing off a busy one. Engine-counter
        /// against rows-stored is a YIELD instrument and a different piece of work; saying so is what
        /// stops this figure being read as one.</para>
        /// </summary>
        public const string OutputWindowNote =
            "rows_stored and runs_with_rows are counted over the SAME fixed trailing seven days as total_runs "
            + "and the duration statistics beside them - one aggregate over one window, so cost and output on "
            + "a collector row always describe the same runs. They are NOT the hourly per-collector series "
            + "Darling's get_collector_cost reports as total_rows, which is summed over that caller's own "
            + "days_back and across every server at once; these figures make no claim about it. rows_stored "
            + "is also what a run STORED, never what the monitored engine counted - so a zero cannot tell a "
            + "genuinely quiet source apart from a reader capturing nothing off a busy one, and nothing on "
            + "this surface measures that.";

        /// <summary>
        /// The closing sentence every ZERO-OUTPUT reading ends on. One copy, because the branches differ in
        /// what they claim about the row and agree only here — and a sentence that exists twice is the one
        /// that gets reworded once, which is why <see cref="OutputWindowNote"/> is a constant too.
        /// </summary>
        private const string ZeroOutputCaveat =
            "What this cannot tell you is whether the source really was empty: it counts rows stored, never "
            + "what the monitored engine counted.";

        /// <summary>
        /// The sentence a collector that SPENT and STORED NOTHING gets, and the readings it has to keep
        /// apart (#3017, #3160).
        ///
        /// <para><b>Why this is a sentence and not a band.</b> <c>pg_deadlocks</c> was the dearest collector
        /// on a managed store — 49,258,335 ms over 79,333 runs in seven days — and stored zero rows. That
        /// zero was CORRECT: the reader was working on all 50 targets and there were no deadlocks to find.
        /// A verdict keyed on cost-plus-zero-rows fires on the healthy quiet install, which is the
        /// cry-wolf failure <see cref="HasUserDatabasesQualifier"/> (#1852) exists to prevent. So this puts
        /// the fact beside the cost and names what would tell the two apart, exactly as #1852 states a fact
        /// about the TARGET beside a persistently empty enumeration instead of banding it.</para>
        ///
        /// <para><b>The third term, and the one thing it must not become.</b> Zero output WITH a current
        /// denial is a collector that could not read; zero output alone is one that read and found nothing.
        /// <see cref="DeniedSinceLastSuccess"/> is what separates them, and it is READ here in exactly the
        /// way its own doc comment permits — reported, never banded. This method returns display text and
        /// <see cref="Classify"/> never calls it, so consuming the predicate here cannot widen a band.</para>
        ///
        /// <para><b>Empty on the productive case, deliberately.</b> Like
        /// <see cref="SweepPressureClassifier.FormatPeakCycleNote"/>: a note that fires when nothing is
        /// wrong is how a signal teaches people to ignore it. The numbers on the row already say
        /// "expensive and productive" when <paramref name="rowsStored"/> is positive.</para>
        ///
        /// <para><b>Scoped to zero output, also deliberately.</b> A collector that stored rows earlier in
        /// the window and is being denied right now gets no finding from here — that is
        /// <c>denied_since_last_success</c>'s own job on the same row, and firing a second time for it
        /// would make this a duplicate denial alarm rather than a cost/output instrument.</para>
        ///
        /// <para><b>The fourth term, and why it is a COUNT rather than a collector name (#3160).</b> The
        /// event-collector reading is the right one for <c>deadlocks</c> and <c>blocked_process_report</c>
        /// and wrong for <c>query_store</c>, which is not an event collector and stored zero rows on 11,728
        /// consecutive runs on a read-replica fleet — every one of them carrying an empty-enumeration note.
        /// Asserting the category there reaches the right conclusion by a rationale that does not hold, and
        /// the identical sentence over a <c>query_store</c> that had genuinely stopped would read as
        /// reassurance. <paramref name="noteCount"/> answers it from the row: runs that recorded what they
        /// found get a finding that DEFERS to the note, and runs that recorded nothing keep the category
        /// reading with its precondition stated out loud.</para>
        ///
        /// <para>A name list was the other option and it is the one #2511 exists to refuse — it would go
        /// stale in the direction that makes it pass, because the next periodic collector to break gets the
        /// event-collector sentence until somebody remembers to add it. The property being kept is that a
        /// deliberate zero and a broken zero stay DISTINGUISHABLE, and neither branch is quieter than the
        /// text it replaces: the "SUCCESS with zero rows" sweep that surfaced #3030, #3109 and #3154 reads
        /// <c>rows_stored</c>, which nothing here touches.</para>
        ///
        /// <para><b>The fifth and sixth terms (#3754), and the case that showed the fourth was not
        /// enough.</b> On an Azure SQL DB target two collectors failed on every monitored database every
        /// sweep, the runners swallowed the per-item failures into <c>SUCCESS</c> rows with no note (the
        /// runner-side half of #3754), and this method - seeing zero rows, no denial and no note - told the
        /// reader they had "read and found nothing rather than being unable to read", that this "needs no
        /// action", and that the reading rested on "no run recorded a note". Every clause was false, and
        /// the last one named the bug as its own evidence. The runners now record those failures - as
        /// <c>ERROR</c> / <c>PERMISSIONS</c> when every item fails, as a note when some do, as
        /// <c>SESSION_MISSING</c> when an XE session could not be created - and this method has to READ
        /// them, or the honest run record would sit beside the same false sentence.</para>
        ///
        /// <para><paramref name="faultedRuns"/> is the fifth term: runs in the window that recorded a
        /// fault rather than a result - <c>ERROR</c> and <c>SESSION_MISSING</c>, the two statuses that mean
        /// "could not read" and are not already a term here. PERMISSIONS is deliberately excluded because
        /// its currency is the THIRD term's job (#3010): a denial that predates a later success is history,
        /// and a collector that has read fine since must keep its resting-state reading (pg_deadlocks with
        /// 15,885 old denials off a retired route is the measured case). EXTENSION_MISSING is excluded
        /// because a Darling row in that band already suppresses this finding outright (#3240). ABANDONED
        /// and YIELDED are excluded because each is a guard doing its job with its own count and, for
        /// abandonment, its own WARNING band. Any fault at all withholds the resting-state reading: "read
        /// and found nothing" is a claim about every run in the window, and one run that could not read
        /// falsifies it - the sentence says how many, and what the rest did.</para>
        ///
        /// <para><paramref name="isEventCollector"/> is the sixth: whether the category sentence is TRUE of
        /// this collector at all (<see cref="IsEventCollector"/>). Zero rows, no denial, no fault and no
        /// note from a collector that stores a row only when an event occurs is that collector at rest;
        /// the same four facts from a configuration or snapshot collector mean its source returned nothing
        /// on every run, and the sentence says that instead. A bool computed by the caller from the name,
        /// on <see cref="Classify"/>'s <c>isOnLoad</c> pattern, so this method still takes no string and
        /// the note-text property above is preserved.</para>
        /// </summary>
        /// <param name="faultedRuns">
        /// <c>error_count + session_missing_count</c> for the same row - the fifth term. Runs that recorded
        /// a fault rather than a result; see the remarks for which statuses count and why the rest do not.
        /// </param>
        /// <param name="isEventCollector">
        /// <see cref="IsEventCollector"/> for the row's collector - the sixth term, deciding whether the
        /// resting-state sentence may be offered when nothing else on the row explains the zero.
        /// </param>
        /// <param name="rowsStored">Rows the window's runs stored (<c>rows_stored</c>). Positive = silent.</param>
        /// <param name="totalRuns">Runs in the window (<c>total_runs</c>) — the spend this qualifies.</param>
        /// <param name="deniedSinceLastSuccess">
        /// <see cref="DeniedSinceLastSuccess"/> for the same row. The third term: it is what turns "stored
        /// nothing" from an ambiguity into a named reading.
        /// </param>
        /// <param name="noteCount">
        /// <c>note_count</c> for the same row — how many of these runs recorded a note about what the run
        /// itself found. The FOURTH term, and the one that stops the event-collector reading being asserted
        /// over a collector that already said why (#3160).
        /// </param>
        public static string FormatOutputFinding(
            long rowsStored,
            long totalRuns,
            bool deniedSinceLastSuccess,
            long noteCount,
            long faultedRuns,
            bool isEventCollector)
        {
            if (rowsStored > 0 || totalRuns <= 0)
            {
                return string.Empty;
            }

            if (deniedSinceLastSuccess)
            {
                return string.Format(
                    CultureInfo.InvariantCulture,
                    "Stored 0 rows across {0:N0} runs, and denied_since_last_success is true - the newest "
                    + "denial postdates the newest success, so this collector is being refused NOW and the "
                    + "spend bought nothing because nothing could be read. That is a grant, not a collector "
                    + "repair.",
                    totalRuns);
            }

            /* #3754: runs that recorded a FAULT. Ahead of the note branch because a fault is the louder
               fact - a window can hold both (some cycles lost every database and classified ERROR, others
               lost some and noted it), and the sentence then names both. Like the note branch it points at
               the fields rather than restating them: errors and session_missing count the runs, last_error
               carries the newest ERROR message, last_note the newest note. It does NOT restate the message,
               for the reason the note branch does not - the copy that drifts is never the one being read.
               Two shapes: every run faulted (nothing was read, action needed) and some did (the survivors
               read and found nothing, but the resting-state reading is not offered over a window in which
               any run could not read, because that reading is a claim about every run). */
            if (faultedRuns > 0)
            {
                var allFaulted = faultedRuns >= totalRuns;
                var noted = noteCount > 0
                    ? string.Format(
                        CultureInfo.InvariantCulture,
                        " {0:N0} of the runs also recorded a note about what they found - last_note carries it.",
                        noteCount)
                    : string.Empty;

                return string.Format(
                    CultureInfo.InvariantCulture,
                    "Stored 0 rows across {0:N0} runs with no current denial (denied_since_last_success is "
                    + "false), but {1:N0} of those runs recorded a fault - an ERROR or SESSION_MISSING status; "
                    + "errors and session_missing on this row count them and last_error carries the newest "
                    + "ERROR message - so on those runs this collector was UNABLE to read rather than idle. "
                    + "{2}{3} The event-collector resting-state reading is not offered over a window in which "
                    + "any run could not read. "
                    + ZeroOutputCaveat,
                    totalRuns,
                    faultedRuns,
                    allFaulted
                        ? "Every run in the window faulted: nothing was read at all, and this needs action - "
                          + "read last_error (or the collection_log rows for a SESSION_MISSING run) for what "
                          + "refused it."
                        : string.Format(
                            CultureInfo.InvariantCulture,
                            "The other {0:N0} runs completed and stored nothing.",
                            totalRuns - faultedRuns),
                    noted);
            }

            /* #3160: the runs accounted for themselves, so this defers instead of categorising. It reports
               the COUNT and names where the reason is; it does not restate the note, which
               FormatCollectionNote already renders and which would then exist twice. */
            if (noteCount > 0)
            {
                return string.Format(
                    CultureInfo.InvariantCulture,
                    "Stored 0 rows across {0:N0} runs with no current denial (denied_since_last_success is "
                    + "false), so this collector read rather than being unable to read. {1:N0} of those runs "
                    + "recorded a note about what the run itself found - last_note carries what they said, "
                    + "and note_count against total_runs says how many. Read that note for why this zero "
                    + "happened. It is deliberately NOT claimed here that this is a collector storing a row "
                    + "only when an event occurs, which is the reading that applies only when no run "
                    + "recorded anything. "
                    + ZeroOutputCaveat,
                    totalRuns,
                    noteCount);
            }

            /* #3754: the category sentence, offered ONLY to a collector it is true of. Same tokens as before
               ("read and found nothing", "correct resting state", "needs no action", "No run recorded a
               note") so the reading an operator has learned keeps its shape; the precondition it states
               now includes the fault term, because that is what it rests on too. */
            if (isEventCollector)
            {
                return string.Format(
                    CultureInfo.InvariantCulture,
                    "Stored 0 rows across {0:N0} runs with no current denial (denied_since_last_success is "
                    + "false), so this collector read and found nothing rather than being unable to read. "
                    + "It stores a row only when an event occurs - a deadlock, a blocked-process report, a "
                    + "long completion, a blocking chain, a held xmin - so zero is the correct resting state "
                    + "on a well-behaved target and needs no action. No run recorded a note or a fault, "
                    + "which is what that reading rests on. "
                    + ZeroOutputCaveat,
                    totalRuns);
            }

            /* #3754: NOT an event collector, and nothing on the row explains the zero. A configuration or
               snapshot read returns rows whenever its source has any, so every run returning nothing is
               not a resting state - it is the source coming back empty, and the sentence says so. Worded
               without alarm on purpose: the polled activity snapshots (waiting_tasks, query_snapshots and
               their kin) legitimately read zero on an idle target, and "on a target that has anything for
               it to report" is the clause that lets the operator who knows the box is idle move on. What
               it must not do is offer the reassurance it replaced: this branch exists because
               database_scoped_config, a per-database configuration snapshot, was told its zero was the
               correct resting state of an event capture. */
            return string.Format(
                CultureInfo.InvariantCulture,
                "Stored 0 rows across {0:N0} runs with no current denial (denied_since_last_success is "
                + "false), no recorded fault and no note, so every run completed and its source returned no "
                + "rows. This is not an event-triggered collector - it is a snapshot or configuration read "
                + "that returns rows whenever its source has any - so a persistent zero here is not a resting "
                + "state: the source came back empty on every run, which on a target that has anything for "
                + "it to report needs a look. "
                + ZeroOutputCaveat,
                totalRuns);
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
