using System;
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
    /// Duration of the examined period in milliseconds.
    /// </summary>
    public double PeriodDurationMs => (TimeRangeEnd - TimeRangeStart).TotalMilliseconds;
}
