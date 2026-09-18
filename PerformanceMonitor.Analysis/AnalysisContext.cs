using System;
using System.Collections.Generic;
using System.Threading;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Context for an analysis run — what server, what time range.
/// </summary>
public class AnalysisContext
{
    public int ServerId { get; set; }
    public string ServerName { get; set; } = string.Empty;
    public DateTime TimeRangeStart { get; set; }
    public DateTime TimeRangeEnd { get; set; }

    /// <summary>
    /// The token the pass's store reads observe. Default <see cref="CancellationToken.None"/> — a
    /// caller that does not plumb one (the fact-inspection paths) keeps the prior behavior exactly,
    /// because every abandon classification requires this token to be SIGNALLED. Carried on the context
    /// rather than on thirty method signatures because the context already reaches every pipeline stage.
    ///
    /// <para>Originally this WAS the host's stopping token (#2299). Since #2430 it is the pass's
    /// EFFECTIVE token, which the Darling worker links from the stopping token and arms with the
    /// per-pass budget — so it now fires on an ordinary timeout against a perfectly healthy service,
    /// not only at shutdown. That is why <see cref="ShutdownToken"/> exists: something has to still know
    /// which of the two happened, and this token can no longer answer it.</para>
    /// </summary>
    public CancellationToken CancellationToken { get; set; }

    /// <summary>
    /// The host's stopping token, and ONLY that (#2430). Default <see cref="CancellationToken.None"/>,
    /// which reads as "this pass has no shutdown to distinguish" — correct for the on-demand callers,
    /// whose cancellation is never a service stop.
    ///
    /// <para>Kept separate from <see cref="CancellationToken"/> because a classifier that asks the
    /// armed token "are we stopping?" gets a yes on every timeout, and would log an ordinary overrun on
    /// a running service as "abandoned at shutdown" at Information — a wrong answer that reads as a
    /// calm one, which is the worst kind.</para>
    /// </summary>
    public CancellationToken ShutdownToken { get; set; }

    /// <summary>
    /// The monitored SERVER's UTC offset (SYSDATETIME − SYSUTCDATETIME), captured once at
    /// analysis start. <see cref="TimeRangeStart"/>/<see cref="TimeRangeEnd"/> are in the
    /// server's LOCAL clock so every windowed read matches the collectors (which stamp rows
    /// with SYSDATETIME, server-local); this offset converts that window back to UTC for
    /// persistence/display. <see cref="TimeSpan.Zero"/> when the clock probe was unavailable
    /// (the window is then host-UTC — the prior behavior).
    /// </summary>
    public TimeSpan ServerUtcOffset { get; set; }

    /// <summary>
    /// The explicit UTC instant this pass's window was anchored at (#2506's <c>as_of</c>), or null when
    /// the window simply ends at "now". Null is the ONLY shape the scheduled worker, the WPF viewers and
    /// the alerting path ever produce, so every one of them keeps its behaviour untouched.
    ///
    /// <para>It is carried rather than inferred because <see cref="TimeRangeEnd"/> cannot answer the
    /// question: a scheduled pass's end is "now" and an anchored pass's end can be a second ago, and the
    /// two are indistinguishable by value. <see cref="PersistFindings"/> is the decision that needs the
    /// answer, and getting it from a comparison against the clock would make persistence depend on how
    /// long the pass took to start.</para>
    /// </summary>
    public DateTime? AsOfUtc { get; set; }

    /// <summary>
    /// Whether this pass's findings are WRITTEN to the store. False for exactly one reason: the window was
    /// anchored at a past instant, which makes the pass exploratory by definition.
    ///
    /// <para><b>Why the engine refuses instead of the caller remembering to ask.</b> A finding row's
    /// identity, for every consumer we have, is its <c>analysis_time</c> — the moment the pass ran, not the
    /// window it looked at. The viewers' Recommendations tab reads <c>MAX(analysis_time)</c> and calls the
    /// result the server's CURRENT state; the findings read filters on <c>analysis_time</c> and then
    /// collapses on <c>(story_path_hash, incident_id)</c> to produce occurrences / first_seen / last_seen /
    /// peak_severity. So a backdated pass stamped now would (a) become "what is wrong with this server" for
    /// every human looking at the viewer and (b) inflate the very occurrence stats an operator uses to
    /// decide whether a live incident is getting worse — caused, invisibly, by somebody else's exploratory
    /// read. Recording the window on the row does not fix either: <c>time_range_start</c>/
    /// <c>time_range_end</c> are ALREADY persisted and already returned, and no consumer filters on them.
    ///
    /// <para>Making it a derived rule rather than a settable flag is the point. There is no legitimate
    /// caller for "anchored AND persist", so there must be no way to express it — including for the next
    /// caller, who will not have read this comment.</para></para>
    /// </summary>
    public bool PersistFindings => AsOfUtc is null;

    /// <summary>
    /// The NOMINAL length of the examined window in milliseconds — <see cref="TimeRangeEnd"/> minus
    /// <see cref="TimeRangeStart"/>, whether or not the collector was running for any of it.
    ///
    /// <para>#3538 (A2): this is NOT the denominator for a rate or a fraction fact any more. It was, for
    /// every one of them, and that made collector downtime read as improvement: a wait that truly held
    /// 25% of the time the collector was up reads as 6% when the collector was down for three of the
    /// window's four hours, because the numerator lost three quarters of its rows and the denominator
    /// lost nothing. Rates divide by <see cref="ObservedDurationMs"/>. This property remains for the
    /// things that are legitimately about the window itself: the coverage fraction's own denominator,
    /// the <c>period_duration_ms</c> / <c>period_hours</c> metadata that states what window was asked
    /// for, and the caveat prose that says how much of it was seen.</para>
    /// </summary>
    public double PeriodDurationMs => (TimeRangeEnd - TimeRangeStart).TotalMilliseconds;

    /// <summary>
    /// How much of the window the collector actually observed, stamped by the fact collector at the
    /// start of every pass from the wait-stats collection series (see <see cref="WindowCoverage"/> for
    /// the measurement). Null until that stamp happens — a context that has not been through a collector
    /// has no observed time, and every rate-fact site treats that as "nothing to divide by" rather than
    /// falling back to the nominal window, because the silent fallback IS the defect this replaces.
    /// </summary>
    public WindowCoverage? Coverage { get; set; }

    /// <summary>
    /// The denominator for every rate and fraction fact: wall-clock milliseconds inside the window over
    /// which collected deltas actually accrued (<see cref="WindowCoverage.ObservedMs"/>), or 0 when the
    /// coverage has not been stamped or the window was not observed at all. A site dividing by this must
    /// check for 0 first and emit NO fact — an unobserved window has no rate, and a fabricated 0 would
    /// read as "idle", which is the calm-sounding wrong answer the whole change exists to stop.
    /// </summary>
    public double ObservedDurationMs => Coverage?.ObservedMs ?? 0;
}

/// <summary>
/// The share of an analysis window the collector was actually up for, measured from the collection
/// series itself (#3538 A2) — the stamp every rate and fraction fact divides by, and the evidence the
/// coverage caveat states.
///
/// <para><b>What it measures.</b> Every delta-family row (wait stats first among them) carries the
/// change since the PREVIOUS collection, so the row at time T accounts for the interval
/// (LAG(T), T]. The observed time in a window is therefore the sum of those intervals for the rows
/// inside it — the exact pattern the anomaly path already uses for its ms/sec rates ("interval via
/// LAG — never an assumed cadence", <c>PgAnomalyDetector.WaitRateWindowSql</c>), applied to the
/// denominator instead of to a per-sample rate. Three rules make the sum honest:</para>
/// <list type="bullet">
/// <item><description>An interval longer than the delta gap policy
/// (<c>CollectorDeltaCalculator.DefaultMaxGapSeconds</c>, an hour) counts as ZERO observed time, not as
/// an hour. The calculator DISCARDS a delta whose gap exceeds the policy — the row after a three-hour
/// outage is stored with delta 0 and no interval — so the numerator has already lost that stretch, and
/// a denominator that credited any of it would re-create the deflation in miniature.</description></item>
/// <item><description>The first in-window row's interval is clipped at the window start (its
/// predecessor is found by scanning one gap-policy back), so observed time never exceeds the nominal
/// window and the fraction needs no clamp. That row's delta straddles the boundary and is counted whole
/// by the numerator, as it always has been — a bias of at most one collection cadence per window, the
/// same one the nominal division carried, and not one this change is about.</description></item>
/// <item><description>A row with no predecessor inside the lookback — the first collection ever, or the
/// first after an outage longer than the policy — contributes nothing. Its delta was unknowable, and
/// so is the time it stands for.</description></item>
/// </list>
///
/// <para><b>Why wait_stats is the witness, on both SKUs.</b> It is the series the wait fractions
/// themselves are summed from, so numerator and denominator come from ONE set of rows and cannot
/// disagree about when the collector was up. It is also already the product's canary for "is this server
/// being collected": the analysis data-span gate (<c>TotalDataSpanSql</c>) and the anomaly baseline gate
/// (<c>HasBaselineDataSql</c>) both read it for exactly that reason. The blocking and deadlock rates
/// divide by the same observed time even though their rows come from event tables — those collectors
/// run in the same service on the same schedule, and when it was down neither table got rows either.
/// The collection log was the alternative witness; it was rejected because it records when a collector
/// RAN, not what interval its rows account for, and the gap policy is defined on the latter.</para>
///
/// <para><b>What it does not do.</b> It says nothing about the monitored server's own uptime — a
/// server that was down while the collector kept failing to reach it shows as unobserved here, which is
/// the right reading for a rate but not a claim about the server. And it does not correct the wait
/// numerator's other known scale-dependence: <c>delta_wait_time_ms</c> sums CONCURRENT waiting tasks,
/// so the same fraction of observed time means something different on 4 cores than on 64 (a 25%
/// CXPACKET fraction on a 64-core box is a handful of parallel queries; on a 4-core box it is most of
/// the machine). That is documented on the wait facts and left to the threshold work (#3538 A5).</para>
/// </summary>
public sealed class WindowCoverage
{
    /// <summary>
    /// Below this observed fraction the pass emits the <see cref="FactKey"/> context fact and every
    /// tool payload carries a caveat. Not fleet-measured — a judgment about materiality set by the
    /// #3538 engine review: at 70% the old nominal division was understating every rate by 1/0.7 ≈
    /// 1.43×, which is enough to move a fact across a severity band on the wait ladder (most
    /// concerning→critical pairs there are within a factor of two), while above it the understatement
    /// stays inside the ordinary sample-to-sample movement of a four-hour window. The rates themselves
    /// are corrected at EVERY coverage; this bar only decides when the correction is large enough that
    /// the reader must be told the window had a hole in it. Revise it against the fleet distribution of
    /// observed coverage once that has been measured.
    /// </summary>
    public const double PartialThreshold = 0.70;

    /// <summary>
    /// The context fact's source — its own, scored 0 by <c>FactScorer.ScoreAll</c>'s default arm
    /// (unknown sources score nothing), so it can never root a story or reach a notification. It exists
    /// to be READ: in <c>get_analysis_facts</c> next to the facts it qualifies, and in
    /// <c>compare_analysis</c> side by side across the two windows.
    /// </summary>
    public const string FactSource = "coverage";

    /// <summary>The context fact emitted when coverage is partial: Value = the observed fraction.</summary>
    public const string FactKey = "COLLECTION_GAP";

    /// <summary>Nominal window length in milliseconds (<see cref="AnalysisContext.PeriodDurationMs"/>).</summary>
    public double NominalMs { get; init; }

    /// <summary>Wall-clock milliseconds inside the window over which collected deltas accrued — the
    /// rate denominator. 0 when the window was not observed at all.</summary>
    public double ObservedMs { get; init; }

    /// <summary>Distinct collections whose rows landed inside the window (the orphan first row included).</summary>
    public int SampleCount { get; init; }

    /// <summary>
    /// The longest single unobserved stretch inside the window, in milliseconds: the largest interval
    /// that exceeded the gap policy (clipped to the window), the lead-in before a first row that had no
    /// predecessor, or the tail after the last row — whichever is longest. Equals <see cref="NominalMs"/>
    /// when nothing was collected.
    /// </summary>
    public double LargestGapMs { get; init; }

    /// <summary>
    /// Observed share of the nominal window, 0..1. Clipping keeps <see cref="ObservedMs"/> at or under
    /// <see cref="NominalMs"/> by construction; the bound here is a guard against sub-millisecond
    /// rounding in the epoch arithmetic, not a correction of the measurement.
    /// </summary>
    public double Fraction => NominalMs > 0 ? Math.Clamp(ObservedMs / NominalMs, 0.0, 1.0) : 0.0;

    /// <summary>True when any observed time exists to divide by.</summary>
    public bool IsObserved => NominalMs > 0 && ObservedMs > 0;

    /// <summary>True when the window was observed, but less of it than <see cref="PartialThreshold"/>.</summary>
    public bool IsPartial => IsObserved && Fraction < PartialThreshold;

    /// <summary>The stamp for a window the collector never observed (no rows, a reversed or zero-length
    /// window, or only an orphan first row): nothing to divide by, and the whole window is the gap.</summary>
    public static WindowCoverage Unobserved(double nominalMs) => new()
    {
        NominalMs = Math.Max(0, nominalMs),
        ObservedMs = 0,
        SampleCount = 0,
        LargestGapMs = Math.Max(0, nominalMs)
    };

    /// <summary>
    /// The <see cref="FactKey"/> context fact: Value is the observed fraction; the metadata carries the
    /// numbers a reader needs to reason about the hole without re-deriving them. Built here so both
    /// SKUs emit exactly the same shape.
    /// </summary>
    public Fact ToGapFact(int serverId) => new()
    {
        Source = FactSource,
        Key = FactKey,
        Value = Fraction,
        ServerId = serverId,
        Metadata = new Dictionary<string, double>
        {
            ["covered_fraction"] = Fraction,
            ["observed_ms"] = ObservedMs,
            ["nominal_ms"] = NominalMs,
            ["unobserved_ms"] = Math.Max(0, NominalMs - ObservedMs),
            ["largest_gap_ms"] = LargestGapMs,
            ["sample_count"] = SampleCount
        }
    };

    /// <summary>
    /// The structured block every analysis tool payload carries (<c>coverage</c>), identical across
    /// SKUs because it is built once here. <c>partial</c> is the caller's branch point.
    /// </summary>
    public object ToPayload() => new
    {
        observed_fraction = Math.Round(Fraction, 4),
        observed_hours = Math.Round(ObservedMs / 3_600_000.0, 2),
        nominal_hours = Math.Round(NominalMs / 3_600_000.0, 2),
        largest_gap_hours = Math.Round(LargestGapMs / 3_600_000.0, 2),
        sample_count = SampleCount,
        partial = IsPartial,
        unobserved = !IsObserved
    };

    /// <summary>
    /// One sentence of prose for the caveats: what share of the window was observed and how long the
    /// largest hole was. Written once so <c>analyze_server</c>, <c>get_analysis_facts</c> and
    /// <c>compare_analysis</c> say the same thing on both SKUs.
    /// </summary>
    public string Describe()
    {
        var nominalHours = NominalMs / 3_600_000.0;
        var observedHours = ObservedMs / 3_600_000.0;
        var gapHours = LargestGapMs / 3_600_000.0;
        return !IsObserved
            ? $"the collector observed NONE of this {nominalHours:0.#}h window — no collection interval landed inside it"
            : $"the collector observed {Fraction:P0} of this {nominalHours:0.#}h window ({observedHours:0.##}h of collected time; " +
              $"largest unobserved stretch {gapHours:0.##}h)";
    }
}
