/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>Per-run outcome of the enumeration driver: rows written and the summed SQL/storage slice times.</summary>
public readonly record struct EnumeratedRunResult(int Rows, long SqlMs, long StorageMs);

/// <summary>
/// What a per-database fan-out cost, rolled up to the one thing a blended <c>collection_log</c> row cannot
/// say: how many items it covered, which one was dearest, and what that one cost (#2472).
///
/// <para>The point is the RATIO, not the parts. <c>SlowestItemMs * ItemCount / duration_ms</c> is 1.0 for a
/// perfectly even fan-out and rises with concentration, so 8 databases at 10.1s each reads 1.0 and one at
/// 62s beside seven at 2.7s reads 6.1 — two runs that are both 80,900 ms and want opposite fixes. Neither
/// <c>max_duration_ms</c> nor <c>p95_duration_ms</c> can tell them apart, because both aggregate over RUNS
/// and each of those runs is one row.</para>
///
/// <para>Deliberately a rollup and not a distribution. The full per-item series would need its own retained
/// hypertable; this rides three nullable columns on a row that is written anyway, and answers the question
/// the remedies in #2468 actually turn on.</para>
/// </summary>
/// <param name="ItemCount">Items whose cost was counted — every item that completed a read, empty batch or not,
/// because their SQL slices are in the blended total too and the ratio has to be against the same denominator.</param>
/// <param name="SlowestItem">The dearest item's name (a database, for every fan-out that exists today).</param>
/// <param name="SlowestItemMs">That item's SQL plus storage milliseconds.</param>
public readonly record struct FanoutCost(int ItemCount, string SlowestItem, int SlowestItemMs);

/// <summary>
/// Accumulates a fan-out's per-item costs into one <see cref="FanoutCost"/>. Shared by both SKUs and by both
/// fan-out shapes — the enumeration driver's <c>onItemComplete</c> hook and the Azure per-database connection
/// loop — because two hand-rolled copies of "keep the biggest" is how the two paths would come to disagree
/// about what a slow database is.
/// </summary>
public sealed class FanoutCostAccumulator
{
    private int _itemCount;
    private string? _slowestItem;

    /* -1, not 0: an item that genuinely cost 0 ms still has to become the slowest one when it is the only
       item, and 0 as the floor would leave SlowestItem null on a fan-out that really did run. */
    private long _slowestMs = -1;

    /// <summary>Records one item's total cost. Ties keep the FIRST item seen, so a run's answer does not
    /// wobble between equally-priced databases from cycle to cycle.</summary>
    public void Observe(string item, long itemMs)
    {
        _itemCount++;
        if (itemMs > _slowestMs)
        {
            _slowestMs = itemMs;
            _slowestItem = item;
        }
    }

    /// <summary>The rollup, or null when nothing fanned out — a plain single-query collector, or an
    /// enumeration that yielded no items. Null is the honest answer there: the columns say "this run had no
    /// fan-out", which is not the same claim as "its fan-out was free".</summary>
    public FanoutCost? Result =>
        _itemCount > 0 && _slowestItem is not null
            ? new FanoutCost(_itemCount, _slowestItem, (int)Math.Min(_slowestMs, int.MaxValue))
            : null;
}

/// <summary>
/// One item the enumeration query could not PROBE (#1837): the enumeration reached the item but the
/// per-item eligibility check failed — a database mid-restore, a login that cannot enter it, a
/// cross-database reference the target rejected. Distinct from a per-item COLLECTION failure, which the
/// driver's own <c>onItemError</c> already reports: this one happens before the item ever reaches the
/// driver, so without this contract it can only vanish.
/// </summary>
public readonly record struct EnumerationProbeFailure(string Item, string Error);

/// <summary>
/// What the shared enumeration read produced: the item list both runners iterate, the items whose probe
/// failed, and the collection-log note (null on the ordinary path) the host attaches to the run's row.
/// </summary>
public sealed record EnumerationOutcome(
    IReadOnlyList<string> Items,
    IReadOnlyList<EnumerationProbeFailure> ProbeFailures,
    string? Note);

/// <summary>
/// What the shared PAYLOAD-path probe-failure read produced (#1851): the items the collector's own
/// internal enumeration could not probe, and the collection-log note (null on the ordinary path) the host
/// attaches to the run's row. The payload twin of <see cref="EnumerationOutcome"/>, minus the item list —
/// on this path the result set the host already consumed IS the payload, so there is no item list to
/// return.
/// </summary>
public sealed record ProbeFailureOutcome(
    IReadOnlyList<EnumerationProbeFailure> ProbeFailures,
    string? Note);

/// <summary>
/// Accumulates payload probe failures across the databases of ONE per-database cycle (#1875), so that
/// cycle reports them the way every other path does: one note on one collection_log row, and one capped
/// burst of app-log lines.
///
/// <para>
/// The plain path reads a collector's trailing failure set once, so it can assign
/// <see cref="ProbeFailureOutcome.Note"/> straight onto the run's telemetry. The per-database path reads
/// it N times — once per monitored database — and neither half of that generalizes: N single-shot note
/// assignments leave only the LAST database's, and N calls to the host's capped logger give a
/// 200-database Azure server 200 five-line bursts instead of one. Both failures are of the same kind, a
/// per-READ decision applied to a per-CYCLE fact, which is why the accumulation lives here beside the
/// read rather than being re-derived in each runner's loop body — the reason #1556 moved that loop into
/// this class to begin with.
/// </para>
///
/// <para>
/// <see cref="Add"/> deliberately takes the whole <see cref="ProbeFailureOutcome"/> rather than its
/// failure list: the per-read <see cref="ProbeFailureOutcome.Note"/> is wrong on this path by
/// construction, and taking the outcome is what makes discarding it a documented step at the one place
/// it happens instead of an omission at each call site.
/// </para>
/// </summary>
public sealed class CycleProbeFailures
{
    private readonly List<EnumerationProbeFailure> _failures = new();

    /// <summary>Every failure this cycle's databases reported, in the order they were read.</summary>
    public IReadOnlyList<EnumerationProbeFailure> Failures => _failures;

    /// <summary>
    /// The ONE note for the whole cycle, composed through the same <see cref="EnumeratedCollectorDriver.BuildNote"/>
    /// both other channels use, so its wording cannot drift from theirs. Null when nothing failed —
    /// identical to the pre-#1875 behavior, where this path never set a note at all.
    /// </summary>
    public string? Note => EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: false, _failures.Count);

    /// <summary>
    /// Folds one database's read into the cycle. The outcome's own note is discarded on purpose (see the
    /// type remarks); its failures are what carry forward.
    /// </summary>
    public void Add(ProbeFailureOutcome outcome)
    {
        ArgumentNullException.ThrowIfNull(outcome);
        _failures.AddRange(outcome.ProbeFailures);
    }
}

/// <summary>
/// The shared control-flow driver for the enumeration collectors' per-item loop (#1556). Both hosts
/// (Lite → DuckDB, Darling → Postgres) ran a byte-identical per-item loop that accumulated EVERY
/// database's rows into one list before a single write — the shape that let one 24-server query_store
/// cycle balloon to 13GB. Extracting the loop here does two things at once: it FLUSHES each item's
/// batch before reading the next (so peak memory is one database's rows, not the fleet's), and it
/// removes the duplicate that let the same defect live in two runners.
///
/// <para>
/// The driver owns only the control flow — iteration, cancellation, the per-item catch SHAPE, the
/// per-item flush, and the interleaved SQL/storage timing. Everything app-specific stays in the
/// caller's delegates: the SQL connection and per-item query (readItem), the storage engine
/// (writeBatch), the host store's per-database watermark read and its catch-up clamp (perItemWatermark),
/// and the log text / display name (onItemComplete / onItemError). This is the seam the plan required:
/// no app or collector semantics leak into the shared loop.
/// </para>
/// </summary>
public static class EnumeratedCollectorDriver
{
    /// <summary>
    /// What both hosts put on the collection_log row when an enumerated collector's enumeration query
    /// returned NO items — so the driver never even runs. That cycle records SUCCESS with 0 rows, which
    /// on its own is indistinguishable from a healthy collector whose databases were simply quiet; it is
    /// equally the shape of query_store enumerating zero Query-Store-enabled databases, or
    /// index_object_stats being filtered down to nothing. The status deliberately stays SUCCESS (this is
    /// not a failure, and #1837's health-banding design is the larger fix); this message is the
    /// fixed, greppable breadcrumb that says WHY the row is empty. Shared so the two runners cannot
    /// drift on the wording the operator greps for.
    /// </summary>
    public const string EmptyEnumerationMessage = "enumeration yielded 0 items - nothing to collect this cycle";

    /// <summary>
    /// The collection-log note for an enumeration that reported PROBE failures (#1837), <c>{0}</c> = how
    /// many items failed. Deliberately a count and a pointer rather than the errors themselves: the note
    /// column is a one-line summary read at a glance in Collection Health, and one unlucky server can fail
    /// to probe hundreds of databases. The per-item text goes to the app log (capped at
    /// <see cref="MaxLoggedProbeFailures"/> lines), which is what "see the app log" means.
    /// </summary>
    public const string ProbeFailureNoteFormat = "{0} item(s) failed their enumeration probe - see the app log for the per-item errors";

    /// <summary>
    /// How many per-item probe failures each host writes to the app log before collapsing the rest into the
    /// count. A server whose login cannot enter ANY database fails every database's probe, and neither host
    /// should turn that into hundreds of log lines per cycle.
    /// </summary>
    public const int MaxLoggedProbeFailures = 5;

    /// <summary>
    /// The per-item app-log line for one probe failure, written by both hosts (at most
    /// <see cref="MaxLoggedProbeFailures"/> of them per cycle). Shared as a const so the wording an
    /// operator greps for is identical in Lite and Darling, and so each host's logging call keeps a
    /// constant message template.
    /// </summary>
    public const string ProbeFailureLogTemplate =
        "{Collector} on '{Server}': enumeration could not probe [{Item}] - {Error}";

    /// <summary>
    /// The one line that closes out a capped probe-failure burst, so the suppressed remainder is never
    /// silent — the failure mode this whole contract exists to end.
    /// </summary>
    public const string ProbeFailureOverflowLogTemplate =
        "{Collector} on '{Server}': {Total} item(s) failed their enumeration probe; {Suppressed} beyond the first {Shown} not logged.";

    /// <summary>
    /// The item name reported when the probe-failure result set itself could not be read as
    /// (item_name, error_text) — the wrong SHAPE (see <see cref="ReadEnumerationAsync"/>) or a reader
    /// that faulted while advancing to it (see <see cref="ReadPayloadProbeFailuresAsync"/>). Not a
    /// database name, so it cannot collide with one.
    /// </summary>
    public const string ContractViolationItem = "(enumeration)";

    /// <summary>
    /// What a probe-failure result set with the wrong shape reports. It is turned into a probe failure
    /// rather than thrown so that a bad enumeration surfaces through the very mechanism this contract
    /// exists for — a note in Collection Health plus an app-log line — instead of failing an otherwise
    /// working collection cycle.
    ///
    /// <para>Names the set by its ROLE rather than its position, because the two channels place it
    /// differently: it is an enumeration's SECOND result set (#1837) and a payload collector's TRAILING
    /// one (#1851). One wording covers both; naming a position would be wrong on one of them.</para>
    /// </summary>
    public const string ContractViolationError =
        "the probe-failure result set must be (item_name, error_text); probe failures were not read";

    /// <summary>
    /// What an UNREADABLE trailing probe-failure set reports on the payload path (#1851), <c>{0}</c> = the
    /// reader's own message. Reached when advancing past the payload throws — a batch that raised an error
    /// after emitting its rows is the realistic case. Reported AS a probe failure for the same reason a
    /// malformed set is: the payload rows already read are good and about to be written, so failing the
    /// whole cycle to announce a diagnostics fault would trade a quiet problem for a loud unrelated one.
    /// </summary>
    public const string UnreadableFailureSetErrorFormat =
        "the trailing probe-failure result set could not be read - {0}";

    /// <summary>Stand-in for a probe failure whose error column came back NULL — the failure still counts.</summary>
    public const string NoErrorText = "(no error text)";

    /// <summary>
    /// Stand-in for a GENUINE probe failure whose item-name column came back NULL. Deliberately distinct
    /// from <see cref="ContractViolationItem"/>: sharing one sentinel would render a real failure the
    /// enumeration merely failed to name identically to a malformed-result-set diagnostic, and send an
    /// operator hunting a SQL defect that is not there. No shipped enumeration can produce it (query_store
    /// takes the name from sys.databases), so this is for the enumerations that adopt the contract next.
    /// </summary>
    public const string UnnamedItem = "(unnamed item)";

    /// <summary>
    /// What an item abandoned at its wall-clock budget reports (#2150), <c>{0}</c> = the budget, already
    /// rendered by <see cref="DescribeBudget"/>. Shared so the enumerated loop and each host's per-database
    /// (Azure SQL DB) loop say the same thing, and so the wording an operator greps for is one string.
    /// </summary>
    public const string WallClockBudgetErrorFormat =
        "abandoned after exceeding its {0} per-database wall-clock budget; the range was not "
        + "collected and will be re-read next cycle (the watermark did not advance)";

    /// <summary>
    /// The collection_log status for a cycle the #2673 whole-server wall-clock budget abandoned, shared so
    /// both hosts write the same value.
    ///
    /// <para>Its own status rather than any existing one, because each of the alternatives says something
    /// false. <c>SUCCESS</c> is what it used to be and is the bug: the run shipped nothing and advanced no
    /// watermark, yet counted as the newest success in <c>ReadCollectionSignalsAsync</c>'s
    /// <c>status IN ('SUCCESS', 'SKIPPED')</c>, so a collector abandoning every cycle read as perpetually
    /// fresh, and it landed in the #1837 note channel whose whole claim is that the run SUCCEEDED.
    /// <c>ERROR</c> would page on a guard doing exactly its job. <c>YIELDED</c> is documented as the 1s
    /// LOCK_TIMEOUT guard and is read as evidence of lock contention on the TARGET — reusing it would send
    /// an operator hunting contention that is not there. <c>SKIPPED</c> is a healthy no-op that counts as
    /// success; this is the opposite, work attempted and paid for that shipped nothing.</para>
    ///
    /// <para>Safe to add because every read buckets by explicit list — <c>IN ('ERROR', 'PERMISSIONS')</c>,
    /// <c>= 'YIELDED'</c>, <c>IN ('SUCCESS', 'SKIPPED')</c> — never by complement, so a new value joins no
    /// bucket rather than silently joining the wrong one, and <c>collection_log.status</c> carries no CHECK
    /// constraint, so no migration rung is needed. The self-alert's consecutive-failure fast path is
    /// server-scoped across every collector, so one collector abandoning among ~40 healthy ones cannot
    /// empty its success window.</para>
    /// </summary>
    public const string AbandonedStatus = "ABANDONED";

    /// <summary>
    /// What a whole-cycle #2673 abandonment writes to <c>collection_log.error_message</c>, <c>{0}</c> = the
    /// budget in seconds. Shared for the same reason <see cref="WallClockBudgetErrorFormat"/> is: both hosts
    /// had their own copy of this literal, so the wording an operator greps for could drift between them.
    /// </summary>
    public const string WholeCycleBudgetNoteFormat =
        "wall-clock budget ({0}s) reached; cycle abandoned";

    /// <summary>
    /// The statuses the freshness reads count as a collection having happened —
    /// <c>DarlingSelfAlertEvaluator.ReadCollectionSignalsAsync</c>'s <c>last_success</c> and
    /// <c>recent_success</c>, and the health reads' <c>last_success_time</c>. Named here so the invariant
    /// that <see cref="AbandonedStatus"/> is NOT one of them is assertable rather than a property of four
    /// separately-maintained SQL strings.
    /// </summary>
    public static readonly IReadOnlyList<string> FreshnessSuccessStatuses = new[] { "SUCCESS", "SKIPPED" };

    /// <summary>
    /// How a run that RETURNED (rather than threw) becomes a collection_log status, shared by both hosts so
    /// the two cannot drift on it — they previously held one hardcoded <c>"SUCCESS"</c> literal each, which
    /// is precisely how the whole-cycle abandonment inherited a success status in both.
    /// </summary>
    /// <param name="abandoned">
    /// <c>CollectorRunResult.Abandoned</c> / <c>RunTelemetry.Abandoned</c> — set only where the #2673
    /// whole-server wall-clock budget gave up, having stored nothing and advanced no watermark.
    /// </param>
    public static string ClassifyReturnedRun(bool abandoned) =>
        abandoned ? AbandonedStatus : "SUCCESS";

    /// <summary>
    /// The collection-log note for a per-database cycle where SOME databases failed and the rest
    /// succeeded (#2623). <c>{0}</c> = how many failed, <c>{1}</c> = how many were attempted,
    /// <c>{2}</c> = up to <see cref="MaxNamedFailedDatabases"/> of their names, <c>{3}</c> = the first
    /// error's message.
    ///
    /// <para>
    /// Tolerating the failure is right - one unreachable database must not cost the other twenty-nine -
    /// but before this note the cycle recorded SUCCESS, whatever the survivors produced, and NOTHING
    /// else. When the failing database is the only one with data, that is SUCCESS with zero rows, which
    /// is exactly the shape of a target that genuinely has nothing to report. Three collectors were
    /// broken that way for three schema versions (#2622); the one that surfaced as an ERROR did so only
    /// because it happened to fail in every database, tripping the all-failed escalation.
    /// </para>
    ///
    /// <para>
    /// Names, not just a count, unlike <see cref="ProbeFailureNoteFormat"/>: a probe failure is usually
    /// one login problem repeated across every database, where the names add nothing, while THIS is
    /// usually a few specific databases and the name is the whole lead. Capped for the case where it
    /// is not.
    /// </para>
    /// </summary>
    public const string PartialDatabaseFailureNoteFormat =
        "{0} of {1} database(s) failed and were skipped ({2}) - any rows this cycle are from the "
        + "survivors ONLY, so a low or zero row count here is not evidence the server is quiet; "
        + "first error: {3}";

    /// <summary>
    /// How many failed database names <see cref="BuildPartialFailureNote"/> spells out before collapsing
    /// the rest into "and N more". The note column is a one-line summary read at a glance.
    /// </summary>
    public const int MaxNamedFailedDatabases = 3;

    /// <summary><see cref="PartialDatabaseFailureNoteFormat"/> parsed once (CA1863).</summary>
    private static readonly CompositeFormat s_partialFailureNote = CompositeFormat.Parse(PartialDatabaseFailureNoteFormat);

    /// <summary><see cref="ProbeFailureNoteFormat"/> parsed once (CA1863) — the const stays the greppable, pinnable text.</summary>
    private static readonly CompositeFormat s_probeFailureNote = CompositeFormat.Parse(ProbeFailureNoteFormat);

    /// <summary><see cref="WallClockBudgetErrorFormat"/> parsed once (CA1863).</summary>
    private static readonly CompositeFormat s_wallClockBudget = CompositeFormat.Parse(WallClockBudgetErrorFormat);

    /// <summary><see cref="UnreadableFailureSetErrorFormat"/> parsed once (CA1863).</summary>
    private static readonly CompositeFormat s_unreadableFailureSet = CompositeFormat.Parse(UnreadableFailureSetErrorFormat);

    /// <summary>
    /// Reads an enumeration query's result: the item list, then the OPTIONAL SECOND RESULT SET of
    /// (item_name, error_text) rows describing items the enumeration could not probe (#1837).
    ///
    /// <para>
    /// The second result set exists because probe failures cannot ride the first one: the first result set
    /// IS the item list both runners consume as database names, so anything added to it would be collected
    /// from. Before this contract, the on-prem query_store enumeration swallowed every per-database probe
    /// failure in an empty CATCH and reported the survivors — a login that could not enter a single
    /// database produced zero items, one SUCCESS row, and no evidence anywhere.
    /// </para>
    ///
    /// <para>
    /// An enumeration that returns ONE result set behaves exactly as before: no second set means no probe
    /// failures, no note, nothing logged. Shared between the hosts so the item read, the failure read, and
    /// the note WORDING cannot drift — the reason #1556 moved the per-item loop here in the first place.
    /// </para>
    /// </summary>
    /// <param name="reader">An open reader positioned on the enumeration's first result set.</param>
    public static async Task<EnumerationOutcome> ReadEnumerationAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var items = new List<string>();
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(reader.GetString(0));
        }

        var probeFailures = await ReadProbeFailuresAsync(reader, cancellationToken);
        return new EnumerationOutcome(items, probeFailures, BuildNote(items.Count == 0, probeFailures.Count));
    }

    /// <summary>
    /// The PAYLOAD path's half of the probe-failure contract (#1851). #1837 gave the ENUMERATING
    /// collectors a channel for items they could not probe; the collectors whose one result set IS the
    /// payload had none, so <c>database_size_stats</c>'s server-side cursor discarded every
    /// inaccessible database in an empty CATCH and reported SUCCESS with that database's rows simply
    /// missing. Such a collector may now return an OPTIONAL result set of (item_name, error_text)
    /// AFTER its payload, which the host reads here — through the same reader, composer, templates and
    /// log cap as the enumeration path, so the two channels cannot drift into two wordings.
    ///
    /// <para>
    /// Call this AFTER <see cref="ICollectorDefinition{TRow}.ReadAsync"/> has drained the payload and
    /// only when the definition declares <see cref="ICollectorDefinition{TRow}.EmitsProbeFailures"/>.
    /// The declaration is what makes the read safe: unlike an enumeration — whose first result set is a
    /// bare item list, so anything after it can only be the failure set — a payload collector's reader
    /// may legitimately hold result sets its own <c>ReadAsync</c> chose to consume or ignore
    /// (<c>tempdb_stats</c> reads two), and this read must never reinterpret one of those as failures.
    /// </para>
    ///
    /// <para>
    /// A declaring collector that returns NO trailing set is the healthy case, not a fault: it reads as
    /// zero failures and no note, exactly like the empty set. That tolerance is what lets one definition
    /// declare the contract for a shape that only some targets produce — <c>database_size_stats</c>
    /// emits the set from its on-prem cursor and has no cursor at all on Azure SQL DB.
    /// </para>
    /// </summary>
    /// <param name="reader">The payload reader, already drained by the definition's own read.</param>
    public static async Task<ProbeFailureOutcome> ReadPayloadProbeFailuresAsync(
        DbDataReader reader,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);

        IReadOnlyList<EnumerationProbeFailure> failures;
        try
        {
            failures = await ReadProbeFailuresAsync(reader, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Advancing past the payload can throw where an enumeration's read cannot: the batch may
               have raised an error AFTER emitting its rows, and the provider surfaces that here. The
               payload rows are already materialized and are about to be written, so this reports itself
               through the contract instead of discarding a good collection to announce it. */
            failures = new[]
            {
                new EnumerationProbeFailure(
                    ContractViolationItem,
                    string.Format(CultureInfo.InvariantCulture, s_unreadableFailureSet, ex.Message)),
            };
        }

        /* enumerationWasEmpty: false — a payload collector has no enumeration whose emptiness could be
           reported, and its own empty-payload case is already visible as rows_collected = 0. */
        return new ProbeFailureOutcome(failures, BuildNote(enumerationWasEmpty: false, failures.Count));
    }

    /// <summary>
    /// Composes the collection-log note for one enumeration: the empty-enumeration breadcrumb, the
    /// probe-failure summary, both (all probes failed, so nothing was enumerable), or null for the
    /// ordinary path. The single place either note text is built, so the two hosts write the same string
    /// and an operator can grep for one wording.
    /// </summary>
    public static string? BuildNote(bool enumerationWasEmpty, int probeFailureCount)
    {
        var probeNote = probeFailureCount > 0
            ? string.Format(CultureInfo.InvariantCulture, s_probeFailureNote, probeFailureCount)
            : null;

        return (enumerationWasEmpty, probeNote) switch
        {
            (true, null) => EmptyEnumerationMessage,
            (true, not null) => $"{EmptyEnumerationMessage}; {probeNote}",
            (false, _) => probeNote,
        };
    }

    /// <summary>
    /// Composes <see cref="PartialDatabaseFailureNoteFormat"/> for a per-database cycle that lost SOME
    /// databases but not all. Returns null when nothing failed, and null when EVERYTHING failed - the
    /// all-failed case rethrows the first failure so the run is classified as an error, and a note on a
    /// row about to be marked ERROR would only compete with the error message.
    /// </summary>
    public static string? BuildPartialFailureNote(
        int failed, int attempted, IReadOnlyList<string> failedDatabases, string? firstError)
    {
        if (failed <= 0 || attempted <= 0 || failed >= attempted)
        {
            return null;
        }

        var named = failedDatabases.Count <= MaxNamedFailedDatabases
            ? string.Join(", ", failedDatabases)
            : string.Join(", ", failedDatabases.Take(MaxNamedFailedDatabases))
              + $", and {failedDatabases.Count - MaxNamedFailedDatabases} more";

        if (string.IsNullOrWhiteSpace(named))
        {
            named = UnnamedItem;
        }

        return string.Format(
            CultureInfo.InvariantCulture,
            s_partialFailureNote,
            failed,
            attempted,
            named,
            string.IsNullOrWhiteSpace(firstError) ? NoErrorText : firstError);
    }

    /// <summary>
    /// Joins two collection-log notes, either of which may be null. Shared because both hosts now compose
    /// a cycle note from two independent sources - probe failures and skipped databases - and a cycle can
    /// legitimately have both; whichever host wrote its own join would be the one that silently dropped
    /// one of them.
    /// </summary>
    public static string? MergeNotes(string? first, string? second)
    {
        if (string.IsNullOrWhiteSpace(first))
        {
            return string.IsNullOrWhiteSpace(second) ? null : second;
        }

        return string.IsNullOrWhiteSpace(second) ? first : $"{first}; {second}";
    }

    /// <summary>
    /// Advances to the optional trailing result set and reads its (item_name, error_text) rows. No such
    /// result set — the shape every enumeration had before #1837, and every payload collector before
    /// #1851 — returns an empty list. The single implementation behind BOTH channels: the enumeration
    /// read above and <see cref="ReadPayloadProbeFailuresAsync"/>.
    /// </summary>
    private static async Task<IReadOnlyList<EnumerationProbeFailure>> ReadProbeFailuresAsync(
        DbDataReader reader,
        CancellationToken cancellationToken)
    {
        if (!await reader.NextResultAsync(cancellationToken))
        {
            return Array.Empty<EnumerationProbeFailure>();
        }

        /* A second result set that is not (item_name, error_text) is a first-party SQL defect. Report it
           AS a probe failure instead of throwing: this contract exists so silent enumeration problems
           become visible, and killing the collection cycle to say so would trade one invisible defect for
           a loud unrelated one. */
        if (reader.FieldCount < 2)
        {
            return new[] { new EnumerationProbeFailure(ContractViolationItem, ContractViolationError) };
        }

        var failures = new List<EnumerationProbeFailure>();
        while (await reader.ReadAsync(cancellationToken))
        {
            /* GetValue().ToString() rather than GetString(): a second result set with the right column
               COUNT but a non-string column would throw InvalidCastException out of here and fail the
               whole collection cycle — the outcome the arity check above deliberately avoids. Read
               loosely and the malformed set still reports itself as a probe failure. */
            var item = reader.IsDBNull(0) ? UnnamedItem : reader.GetValue(0).ToString() ?? UnnamedItem;
            var error = reader.IsDBNull(1) ? NoErrorText : reader.GetValue(1).ToString() ?? NoErrorText;
            failures.Add(new EnumerationProbeFailure(item, error));
        }

        return failures;
    }

    /// <summary>
    /// Runs the per-item loop: for each item, (optionally) refresh its per-database watermark, read its
    /// rows, surface the cap/byte-budget WARNING, then flush that batch before moving on.
    /// </summary>
    /// <param name="items">The enumerated items (database names), already listed by the caller.</param>
    /// <param name="perItemWatermark">
    /// Refreshes <see cref="CollectorContext.Watermark"/> for this item before its query is built — the
    /// per-database watermark read plus its clamp. Null when the definition has no per-database watermark
    /// (the single server-wide watermark already sits on the context).
    /// </param>
    /// <param name="readItem">
    /// The SQL phase: builds the per-item query, runs it, and materializes the batch. Returns a non-null
    /// (possibly empty) list. Its wall time is summed into <see cref="EnumeratedRunResult.SqlMs"/>.
    /// </param>
    /// <param name="writeBatch">
    /// The storage phase: writes ONE item's batch to the host store. Skipped for an empty batch. Its wall
    /// time is summed into <see cref="EnumeratedRunResult.StorageMs"/>. A flush failure PROPAGATES —
    /// storage failure is systemic, and batches already flushed stay committed (commit-1..N-1 on abort).
    /// </param>
    /// <param name="onItemComplete">Per-item completion hook (item, batch count, SQL ms, storage ms),
    /// invoked after a successful read AND its flush (#1565: the hosts log a per-database line from this,
    /// so a burst on one database is visible instead of blending into the per-server total; they also
    /// surface the row-cap / byte-budget warning here — the context truncation signal persists until the
    /// next item's read resets it, so reading it post-flush is equivalent).</param>
    /// <param name="onItemError">Per-item skip log, invoked when one item fails (offline DB, timeout, permissions).</param>
    /// <param name="perItemBudget">
    /// Wall-clock ceiling for one item's watermark refresh plus its read (#2150), from
    /// <c>ICollectorDefinition.PerItemWallClockBudget</c>. Null (every collector but <c>query_store</c>) leaves
    /// the loop exactly as it was. Exceeding it abandons THAT item as a per-item failure and continues;
    /// the WRITE is deliberately outside the budget, because abandoning a flush that is already underway
    /// would trade a slow cycle for a partially-written one.
    /// </param>
    public static async Task<EnumeratedRunResult> RunAsync<TRow>(
        IReadOnlyList<string> items,
        Func<string, CancellationToken, Task>? perItemWatermark,
        Func<string, CancellationToken, Task<List<TRow>>> readItem,
        Func<List<TRow>, CancellationToken, Task> writeBatch,
        Action<string, int, long, long> onItemComplete,
        Action<string, Exception> onItemError,
        CancellationToken cancellationToken,
        TimeSpan? perItemBudget = null)
    {
        var totalRows = 0;
        long sqlMs = 0;
        long storageMs = 0;

        foreach (var item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();

            List<TRow>? batch = null;
            long itemSqlMs = 0;
            var sqlSlice = Stopwatch.StartNew();

            /* #2150: the item's wall-clock budget. Null for every collector that declares none, in which
               case itemToken IS cancellationToken and this loop is what it always was. */
            using var itemBudget = StartItemBudget(perItemBudget, cancellationToken);
            var itemToken = itemBudget?.Token ?? cancellationToken;
            try
            {
                /* Per-database watermark refresh (query_store): its cutoff — including the catch-up
                   clamp — is computed HERE, inside the loop, so each database's commit advances only its
                   own watermark and an abort loses no other database's intervals. Inside the budget on
                   purpose: it is a store read, and a store that has stopped answering is exactly the kind
                   of stall the budget exists to bound. */
                if (perItemWatermark is not null)
                {
                    await perItemWatermark(item, itemToken);
                }

                batch = await readItem(item, itemToken);
            }
            catch (OutOfMemoryException)
            {
                /* OOM is filtered OUT of the per-item skip below and rethrown: it is fatal to this run,
                   not a routine one-database skip. There is no cross-item accumulator to clear — the
                   per-item batch is a local that unwinds with this frame — so filter+rethrow is the whole
                   handler; the host classifies the run ERROR. */
                throw;
            }
            catch (Exception ex) when (ItemBudgetExpired(itemBudget, cancellationToken))
            {
                /* #2150: THIS item ran out of wall clock. Reported as a per-item failure so the sweep
                   continues, which is the entire point — one database must not be able to starve the rest.
                   Ahead of the generic catch because a cancelled command does not reliably arrive as an
                   OperationCanceledException, so the generic filter cannot be trusted to claim it; and the
                   token check is what keeps a real shutdown out of this arm. `ex` is deliberately dropped
                   in favour of the budget message: whatever the provider raised on cancellation is an
                   artifact of HOW it was cancelled, not why. */
                _ = ex;
                onItemError(item, ItemBudgetException(perItemBudget!.Value));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* One item failing is routine (an offline/mid-restore database, a permissions oddity, a
                   timeout) — skip it and keep collecting the rest, matching the original per-item loop.
                   OCE and OOM deliberately propagate (they are not per-item faults). */
                onItemError(item, ex);
            }
            finally
            {
                itemSqlMs = sqlSlice.ElapsedMilliseconds;
                sqlMs += itemSqlMs;
            }

            /* A null batch means the read faulted and was skipped above; a successful read is a non-null
               (possibly empty) list. */
            if (batch is null)
            {
                continue;
            }

            /* Empty batch: no COPY/appender opened (rows_collected = Σ non-empty batch counts). */
            long itemStorageMs = 0;
            if (batch.Count > 0)
            {
                var storageSlice = Stopwatch.StartNew();
                await writeBatch(batch, cancellationToken);
                itemStorageMs = storageSlice.ElapsedMilliseconds;
                storageMs += itemStorageMs;
                totalRows += batch.Count;
            }

            /* Completion hook AFTER the flush so it carries both per-item slices (#1565). The context
               truncation signal is still this item's — the next read resets it. */
            onItemComplete(item, batch.Count, itemSqlMs, itemStorageMs);
        }

        return new EnumeratedRunResult(totalRows, sqlMs, storageMs);
    }

    /// <summary>
    /// Starts one item's wall-clock budget (#2150), or returns null when the definition declares none —
    /// which is every collector but <c>query_store</c>, so the unbounded path stays byte-identical.
    ///
    /// <para>A LINKED source, so host shutdown still cancels the item promptly; the timer only adds a
    /// second reason to stop. Callers must pass <see cref="CancellationTokenSource.Token"/> to the work
    /// and dispose the source when the item ends.</para>
    /// </summary>
    public static CancellationTokenSource? StartItemBudget(TimeSpan? budget, CancellationToken outer)
    {
        if (budget is not TimeSpan span || span <= TimeSpan.Zero)
        {
            return null;
        }

        var source = CancellationTokenSource.CreateLinkedTokenSource(outer);
        source.CancelAfter(span);
        return source;
    }

    /// <summary>
    /// Did THIS item's budget fire, as opposed to the host shutting down?
    ///
    /// <para>The distinction is the whole point: a budget expiry is a per-item fault to be reported and
    /// skipped, while shutdown must propagate and stop the sweep. Shutdown deliberately WINS the ambiguous
    /// case — if both are cancelled, this returns false and the exception propagates — because misreading a
    /// shutdown as a per-item skip would have the loop keep collecting through it.</para>
    ///
    /// <para>Classifying on the TOKENS rather than the exception type is deliberate too. Cancelling a
    /// SqlClient command mid-execute does not reliably surface as <see cref="OperationCanceledException"/>:
    /// it commonly arrives as a provider exception ("Operation cancelled by user"), and which one depends on
    /// whether the cancellation landed during the open or during the drain. The tokens know; the exception
    /// type does not.</para>
    /// </summary>
    public static bool ItemBudgetExpired(CancellationTokenSource? itemBudget, CancellationToken outer) =>
        itemBudget is not null
        && itemBudget.IsCancellationRequested
        && !outer.IsCancellationRequested;

    /// <summary>The exception handed to the per-item error hook for an abandoned item, so both loops report
    /// it identically. A <see cref="TimeoutException"/> because that is what it is — and because the hosts'
    /// hooks log <c>ex.Message</c>, which carries the whole explanation.</summary>
    public static TimeoutException ItemBudgetException(TimeSpan budget) =>
        new(string.Format(CultureInfo.InvariantCulture, s_wallClockBudget, DescribeBudget(budget)));

    /// <summary>
    /// Renders a budget the way the operator set it, choosing the unit rather than fixing one.
    ///
    /// <para>Fixing it at minutes was the first cut, and a scratch harness caught it reporting a
    /// sub-minute budget as "0.0-minute" — a message that names no number at all, on the one line an
    /// operator has to work from. The shipped value is 10 minutes so it would never have shown in the
    /// field; a test asserting the message merely CONTAINS "wall-clock budget" would not have shown it
    /// either. Small values are the ones a person types while diagnosing, which is exactly when the
    /// message matters most.</para>
    /// </summary>
    public static string DescribeBudget(TimeSpan budget) =>
        budget < TimeSpan.FromMinutes(1)
            ? string.Format(CultureInfo.InvariantCulture, "{0:0.###}-second", budget.TotalSeconds)
            : string.Format(CultureInfo.InvariantCulture, "{0:0.#}-minute", budget.TotalMinutes);
}
