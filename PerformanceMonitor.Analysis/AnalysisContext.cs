using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
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
    /// The shortest lookback a "latest value" read takes from <see cref="TimeRangeEnd"/> for each series'
    /// newest sample (#3896): the database files summed into <c>DATABASE_TOTAL_SIZE_MB</c> and counted by
    /// <c>FILE_AUTOGROWTH_PERCENT</c>, the volumes behind <c>DISK_SPACE</c>, the memory clerks, the plan-cache
    /// snapshot and the memory_stats row.
    ///
    /// <para>Those reads used to take each series' newest row with only an upper bound, so they numbered
    /// the server's ENTIRE retained history to keep a few dozen rows (1.2 s for the database-size read alone
    /// on a 17-day DARLING01 store, most of it decompressing chunks) and, worse, answered "latest EVER": a
    /// dropped database's files stayed in the size total until retention aged them out — 31% over on that
    /// server.
    /// With the bound the answer is "present within the lookback", which is the right meaning. A series
    /// with no sample in the lookback is either gone or its collector is down, and in the second case the
    /// fact is better absent than stale; the coverage machinery (#3524/#3551) already reports a dead
    /// collector.</para>
    ///
    /// <para>A day at least, so a dropped database leaves the answer the next day; longer only for a
    /// collector scheduled slower than twice a day (<see cref="LatestValueLookbackFor"/>). The on-load config
    /// snapshots (server_config, database_config, trace_flags, server_properties) are NOT bounded this way:
    /// they are written once per connect, so their newest capture can be weeks old on a healthy
    /// server.</para>
    /// </summary>
    public static readonly TimeSpan LatestValueLookback = TimeSpan.FromHours(24);

    /// <summary>
    /// The lookback for a latest-value read fed by a collector that runs every
    /// <paramref name="frequencyMinutes"/>: a day, or twice the collector's interval if that is longer
    /// (#3896). Collector cadences are operator-editable and a steady-state collection advances by exactly
    /// the interval (#1553), so a flat day would drop a daily collector's fact on every pass that landed
    /// between the day and the next sample — and resolve, then re-fire, the findings built on it. Twice the
    /// interval covers the gap between runs with a whole interval to spare for a late or failed one. Null
    /// for an on-load collector (0), whose reads anchor on its newest capture instead, however old.
    /// </summary>
    public static TimeSpan? LatestValueLookbackFor(int frequencyMinutes) =>
        frequencyMinutes <= 0
            ? null
            : TimeSpan.FromMinutes(Math.Max(LatestValueLookback.TotalMinutes, 2.0 * frequencyMinutes));

    /// <summary>
    /// Each latest-value read's lower bound for this pass, keyed by the collector that feeds it (#3896) —
    /// <see cref="TimeRangeEnd"/> less that collector's <see cref="LatestValueLookbackFor"/>, or its newest
    /// capture when it runs on load. Stamped once per pass by the SKU's fact collector from each collector's
    /// EFFECTIVE schedule (the operator's override where one is set), and read by the drill-down that lists
    /// the same rows, so a fact and its drill-down cannot disagree on which rows are current. Null until
    /// stamped.
    /// </summary>
    public IReadOnlyDictionary<string, DateTime>? LatestValueStarts { get; set; }

    /// <summary>
    /// The lower bound a latest-value read over <paramref name="collectorName"/>'s table binds: the stamped
    /// <see cref="LatestValueStarts"/> entry, else <see cref="TimeRangeEnd"/> less
    /// <see cref="LatestValueLookback"/> — the stamp's own answer for every shipped default cadence. Anchored
    /// on the window's END, not "now", so an anchored or historical window reads the state as it stood then.
    /// </summary>
    public DateTime LatestValueStartFor(string collectorName) =>
        LatestValueStarts is not null && LatestValueStarts.TryGetValue(collectorName, out var start)
            ? start
            : TimeRangeEnd - LatestValueLookback;

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

    /// <summary>
    /// The fact families whose read FAILED this pass and contributed nothing (#3691) — one entry per
    /// degrade site that fired, recorded by <see cref="RecordCollectionFailure"/> beside the log line every
    /// collector's <c>ReportCollectionFailure</c> already writes. Empty on a clean pass, and empty on a
    /// context that has not been through a collector.
    ///
    /// <para><b>Why the context and not only the log.</b> Every family read in all three collectors
    /// degrades to "no facts" on failure, which is right — one unavailable table must not cost a server
    /// its other thirty facts — and since #2826 the catch LOGS. But the pass learned nothing from it: a pass
    /// in which every family failed returned the same empty fact list as a quiet server, scored nothing,
    /// and both <c>analyze_server</c> tools rendered it as the <c>empty</c> all-clear. An operator (or the
    /// agent reading for them) trusted a clean card that was blind. The log line is written for whoever
    /// reads the service log; this list is for the pass itself, so the payload the caller actually sees can
    /// say which families were not read.</para>
    /// </summary>
    public List<CollectionFailure> CollectionFailures { get; } = [];

    /// <summary>
    /// How many fact families the collector RUNS in a pass — the <c>families_total</c> a caveat is stated
    /// against, stamped by the collector at the start of <c>CollectFactsAsync</c> from
    /// <see cref="CollectionCaveats.CountFamilies"/> over its own type, never hard-coded. 0 until a
    /// collector stamps it, which the caveat prose treats as "unknown" rather than as a denominator.
    /// </summary>
    public int CollectionFamilyCount { get; set; }

    /// <summary>
    /// The queries the PLAN_REGRESSION fact reported this pass (#3902): one entry per (database, query_id)
    /// among its at most twenty offenders, stamped by the fact collector after the read. Null until then,
    /// and null when that read failed.
    ///
    /// <para>The regressed-queries drill-down re-runs the same detection to list the top five with their
    /// text and plan ids, and before this it re-deduplicated the server's whole Query Store slice to do it:
    /// the most expensive read in the pass, twice. Its top five are the head of the fact's own ranking, so
    /// it now computes its rows for these queries only, and reads unrestricted when this is null or empty
    /// (a drill-down run without a fact pass, after a failed fact read, or with no fact to follow),
    /// exactly as it always did.</para>
    /// </summary>
    public IReadOnlyList<PlanRegressionOffender>? PlanRegressionOffenders { get; set; }

    /// <summary>
    /// Which source the PLAN_REGRESSION fact read this pass on Darling (#3953): true for the latest-snapshot interval
    /// table, false for the raw Query Store slice, null when the fact did not decide (it did not run, or this is
    /// Lite, which has no such table). The regressed-queries drill-down reads the SAME source, so it can always
    /// reproduce what the fact reported: a best plan the table still holds and raw has purged would otherwise vanish
    /// from the drill-down.
    /// </summary>
    public bool? PlanRegressionReadsIntervalTable { get; set; }

    /// <summary>
    /// Records one failed read. <paramref name="family"/> is the family label the caveat counts by
    /// (<see cref="CollectionFailure.FamilyOf"/> from the collect method's name on the SQL Server collectors,
    /// <see cref="CollectionFailure.FamilyOfFile"/> from the partial file on the PostgreSQL-target one, whose
    /// families split their reads across helper methods); <paramref name="read"/> is the reporting method's
    /// own name (the reporter's <see cref="CallerMemberNameAttribute"/> value),
    /// kept beside it so the entry says WHICH read of the family failed.
    ///
    /// <para>#4316 round 1 B1: <paramref name="exception"/> rather than a caller-built string, so the record's
    /// <see cref="CollectionFailure.Message"/> is always <see cref="CollectionFailure.Describe"/>'s output and
    /// a fact collector cannot put the exception's own message — which can name a role, a host or a relation —
    /// into a record this project serves to MCP clients. There is no string parameter left to do that with.</para>
    /// </summary>
    public void RecordCollectionFailure(string family, string read, CollectionFailureOutcome outcome, Exception exception) =>
        CollectionFailures.Add(new CollectionFailure(family, read, outcome, CollectionFailure.Describe(exception, outcome)));
}

/// <summary>One query the PLAN_REGRESSION fact reported (#3902) — see <see cref="AnalysisContext.PlanRegressionOffenders"/>.</summary>
public readonly record struct PlanRegressionOffender(string DatabaseName, long QueryId);

/// <summary>
/// How a family read failed (#3691) — the three-outcome degrade the collectors already classify for their log
/// level, plus the cancellation the Lite filter lets through, named so a payload reader can tell a growth
/// signal (<see cref="Timeout"/>) from a deploy-window skew (<see cref="MissingSchema"/>) from a fault
/// (<see cref="Error"/>) without reading the message text.
/// </summary>
public enum CollectionFailureOutcome
{
    /// <summary>The read outgrew its command deadline (57014 or a wrapped <see cref="TimeoutException"/>,
    /// classified structurally by <c>PgBaselineProvider.IsCommandTimeout</c>). A growth signal.</summary>
    Timeout,

    /// <summary>An <see cref="OperationCanceledException"/> that was NOT the pass's own abandonment (the
    /// abandon filter on every catch excludes that): a cancellation from somewhere else.</summary>
    Cancelled,

    /// <summary>The store does not have a table or column the read asked for (42P01 / 42703) — the
    /// pre-migration / rolling-deploy skew the collectors log at Debug. Still a family that was not read.</summary>
    MissingSchema,

    /// <summary>Anything else. A fault until someone says otherwise.</summary>
    Error
}

/// <summary>One failed read: which family, which read of it, how it failed, and <see cref="Describe"/>'s
/// non-message text for it — never the exception's own message (#4316).</summary>
public sealed record CollectionFailure(string Family, string Read, CollectionFailureOutcome Outcome, string Message)
{
    /// <summary>#4316: the only text a collection-failure record carries: the exception's type, plus its SQLSTATE
    /// when it is a database error, and where the full error is. Never the message: a PostgreSQL or DuckDB message
    /// can name roles, hosts and relations, and this record is served to MCP clients.</summary>
    public static string Describe(Exception exception, CollectionFailureOutcome outcome)
    {
        var what = exception is DbException { SqlState: { Length: > 0 } sqlState }
            ? $"{exception.GetType().Name}, SQLSTATE {sqlState}"
            : exception.GetType().Name;

        /* #4316 round 1 (L1): the missing-schema arm logs at Debug (the expected pre-migration case), so at the
           default level the log holds nothing for it; say so rather than point at an empty log. */
        return outcome == CollectionFailureOutcome.MissingSchema
            ? $"{what}; a table or column the read needs is missing, logged only at Debug level"
            : $"{what}; the log has the full error";
    }

    /// <summary>
    /// The family label for a collector partial file: the dotted segment before <c>.cs</c>, lower-cased —
    /// <c>PgTargetFactCollector.Vacuum.cs</c> → <c>vacuum</c>, <c>PgTargetFactCollector.Write.cs</c> → <c>write</c>.
    /// For the collector whose families read through helper methods (the PostgreSQL target's vacuum family is
    /// three reads in three methods), the FILE is the family and the method is the read; the reporter takes the
    /// path from <see cref="CallerFilePathAttribute"/>, which is a compile-time
    /// constant on the call site in the partial file. Either directory separator is honoured, because the
    /// path is the build machine's.
    /// </summary>
    public static string FamilyOfFile(string callerFilePath)
    {
        var path = callerFilePath ?? string.Empty;
        var cut = Math.Max(path.LastIndexOf('/'), path.LastIndexOf('\\'));
        var file = cut >= 0 ? path[(cut + 1)..] : path;
        if (file.EndsWith(".cs", StringComparison.OrdinalIgnoreCase)) file = file[..^3];
        var dot = file.LastIndexOf('.');
        var segment = dot >= 0 ? file[(dot + 1)..] : file;
        return segment.Length == 0 ? "unknown" : segment.ToLowerInvariant();
    }

    /// <summary>
    /// The family label for a collect method name, derived rather than tabled: <c>CollectWriteFactsAsync</c>
    /// → <c>write</c>, <c>CollectPlanRegressionFactsAsync</c> → <c>plan_regression</c>,
    /// <c>CollectDatabaseSizeFactAsync</c> → <c>database_size</c>, <c>CollectObservedCoverageAsync</c> →
    /// <c>observed_coverage</c>. The prefix and the <c>Fact(s)Async</c> suffix are the collectors' naming
    /// convention (every family method in all three matches <c>Collect*Async(AnalysisContext, List&lt;Fact&gt;)</c>,
    /// which <see cref="CollectionCaveats.CountFamilies"/> also rests on); a name outside it is returned
    /// snake-cased whole rather than rejected, so a renamed method still reports under SOME name.
    /// </summary>
    public static string FamilyOf(string collectMethod)
    {
        var name = collectMethod ?? string.Empty;
        if (name.StartsWith("Collect", StringComparison.Ordinal)) name = name["Collect".Length..];
        foreach (var suffix in new[] { "FactsAsync", "FactAsync", "Async" })
        {
            if (name.EndsWith(suffix, StringComparison.Ordinal) && name.Length > suffix.Length)
            {
                name = name[..^suffix.Length];
                break;
            }
        }

        var sb = new StringBuilder(name.Length + 4);
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c) && i > 0 && !char.IsUpper(name[i - 1])) sb.Append('_');
            sb.Append(char.ToLowerInvariant(c));
        }
        return sb.Length == 0 ? "unknown" : sb.ToString();
    }

    /// <summary>The payload spelling of the outcome: <c>timeout</c>, <c>cancelled</c>, <c>missing_schema</c>, <c>error</c>.</summary>
    public static string Label(CollectionFailureOutcome outcome) => outcome switch
    {
        CollectionFailureOutcome.Timeout => "timeout",
        CollectionFailureOutcome.Cancelled => "cancelled",
        CollectionFailureOutcome.MissingSchema => "missing_schema",
        _ => "error"
    };
}

/// <summary>
/// What a facts read hands its caller about collection (#3691): the failures and the family total off the
/// context the read ran on, as ONE value so the read's tuple grows by one element rather than two. Returned
/// rather than parked on a service property for the reason <c>CollectAndScoreFactsAsync</c>'s coverage is:
/// that path has no <c>IsAnalyzing</c> guard, two on-demand callers can overlap, and a shared property would
/// let one read the other's failures.
/// </summary>
public sealed record CollectionCaveatState(IReadOnlyList<CollectionFailure> Failures, int FamiliesTotal)
{
    /// <summary>The state the context carries after its collector ran (or threw before recording anything).</summary>
    public static CollectionCaveatState From(AnalysisContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return new(context.CollectionFailures, context.CollectionFamilyCount);
    }

    /// <summary>True when at least one family failed — the only case in which anything is emitted.</summary>
    public bool Any => Failures.Count > 0;

    /// <summary>The caveat sentence, or null on a clean read (<see cref="CollectionCaveats.Describe"/>).</summary>
    public string? Describe() => CollectionCaveats.Describe(Failures, FamiliesTotal);

    /// <summary>Adds <c>collection_caveats</c> only when owed (<see cref="CollectionCaveats.Attach"/>).</summary>
    public object Attach(object payload, JsonSerializerOptions options) => CollectionCaveats.Attach(payload, Failures, FamiliesTotal, options);
}

/// <summary>
/// The <c>collection_caveats</c> block and its sentence (#3691), built once here so <c>analyze_server</c> and
/// <c>get_analysis_facts</c> say the same thing on both SKUs — the <see cref="WindowCoverage"/> pattern.
///
/// <para><b>Emitted only when a family failed.</b> The SQL Server exit checks pin a clean pass's payload
/// byte-for-byte, and the serializer the tools use writes nulls, so a <c>collection_caveats = null</c>
/// property would be a byte change on every clean pass for a caveat that was not owed. <see cref="Attach"/>
/// therefore returns the caller's payload object UNTOUCHED when there is nothing to say — the same object
/// through the same serializer call — and only a failing pass takes the path that adds the block.</para>
/// </summary>
public static class CollectionCaveats
{
    /// <summary>
    /// How many family reads a collector type runs: its non-public instance methods of the shape
    /// <c>Collect*Async(AnalysisContext, List&lt;Fact&gt;)</c>, which is every family method in all three
    /// collectors and nothing else (the public <c>CollectFactsAsync</c> entry point is excluded by
    /// visibility; a local function compiles to a mangled name that does not start with <c>Collect</c>).
    /// Derived from the type so a new family cannot leave the denominator stale.
    /// </summary>
    public static int CountFamilies(Type collectorType)
    {
        ArgumentNullException.ThrowIfNull(collectorType);
        return collectorType
            .GetMethods(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Count(m =>
                m.Name.StartsWith("Collect", StringComparison.Ordinal)
                && m.Name.EndsWith("Async", StringComparison.Ordinal)
                && m.GetParameters() is { Length: 2 } p
                && p[0].ParameterType == typeof(AnalysisContext)
                && p[1].ParameterType == typeof(List<Fact>));
    }

    /// <summary>
    /// The caveat sentence, or null when no family failed. Names the count of DISTINCT families against the
    /// total, each family with its outcome(s), and says what the reader must not infer.
    /// </summary>
    public static string? Describe(IReadOnlyList<CollectionFailure> failures, int familiesTotal)
    {
        if (failures is null || failures.Count == 0) return null;
        var byFamily = failures
            .GroupBy(f => f.Family, StringComparer.Ordinal)
            .Select(g => $"{g.Key} ({string.Join("/", g.Select(f => CollectionFailure.Label(f.Outcome)).Distinct())})")
            .ToList();
        var list = string.Join(", ", byFamily);
        var total = familiesTotal > 0 ? familiesTotal.ToString(System.Globalization.CultureInfo.InvariantCulture) : "an unknown number of";
        return $"{byFamily.Count} of {total} fact families could not be read ({list}) — the absence of findings is not evidence: nothing from those families was scored this pass, and a finding they would have rooted or corroborated is simply missing. The service log carries each failure; check get_collection_health for the store.";
    }

    /// <summary>The structured block: <c>families_failed</c> (DISTINCT families — a family whose two reads both
    /// failed is one family missing), <c>families_total</c>, <c>entries[{family, read, outcome, message}]</c>, one
    /// entry per failed read. Null when no family failed.</summary>
    public static object? ToPayload(IReadOnlyList<CollectionFailure> failures, int familiesTotal)
    {
        if (failures is null || failures.Count == 0) return null;
        return new
        {
            families_failed = failures.Select(f => f.Family).Distinct(StringComparer.Ordinal).Count(),
            families_total = familiesTotal,
            entries = failures.Select(f => new
            {
                family = f.Family,
                read = f.Read,
                outcome = CollectionFailure.Label(f.Outcome),
                message = f.Message
            }).ToList()
        };
    }

    /// <summary>
    /// Adds <c>collection_caveats</c> to a payload object ONLY when a family failed. With no failures the
    /// very same <paramref name="payload"/> reference is returned, so the caller's serializer call is the
    /// one it always made and a clean pass's bytes cannot move. With failures the payload is serialized to
    /// a <see cref="JsonObject"/> with the caller's options, the block is appended as the LAST property,
    /// and the node is returned for the caller to serialize in place of the object.
    /// </summary>
    public static object Attach(object payload, IReadOnlyList<CollectionFailure> failures, int familiesTotal, JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var block = ToPayload(failures, familiesTotal);
        if (block is null) return payload;

        var node = JsonSerializer.SerializeToNode(payload, options)?.AsObject()
            ?? throw new InvalidOperationException("the payload did not serialize to a JSON object");
        node["collection_caveats"] = JsonSerializer.SerializeToNode(block, options);
        return node;
    }

    /// <summary>Joins an existing caveat (the coverage one, null at full coverage) with the collection one
    /// (null on a clean pass); null when both are, so a clean full-coverage pass keeps its null.</summary>
    public static string? Compose(string? existingCaveat, string? collectionCaveat) =>
        existingCaveat is null ? collectionCaveat
        : collectionCaveat is null ? existingCaveat
        : existingCaveat + " " + collectionCaveat;
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
