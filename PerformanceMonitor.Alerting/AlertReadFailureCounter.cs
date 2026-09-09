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
using System.Globalization;
using System.Linq;
using System.Threading;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// Counts the store reads the alerting layer performed, failed, and SWALLOWED (#3013).
///
/// <para><b>The blind spot this closes.</b> Every condition check in the alert pass wraps its read in
/// log-and-skip: on a failure it writes one <c>[ERROR</c> line and returns, because firing on absent
/// evidence would fabricate an alert and resolving on it would fabricate a recovery. That posture is
/// correct and stays. What was missing is that a swallowed read reaches NO surface a person reads —
/// it is not a collector run, so it writes no <c>collection_log</c> row, so <c>get_collection_health</c>
/// and every other health read stayed green while the alert pass was going blind one condition at a
/// time. Only a grep of the service log found it.</para>
///
/// <para><b>Why it matters more than the raw count suggests.</b> The alert pass runs on
/// <c>DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds</c> while the collection sweep runs on
/// budgets an order of magnitude longer. As store latency rises the SHORT-deadline consumers cross
/// their limit first, so the failure ordering under store contention is: alerting first, collection
/// last. During one measured episode of store-side lock contention the service log's <c>[ERROR</c>
/// rate rose 41 → 61 per hour, every line an alerting-side store read, while collector failures in
/// <c>collection_log</c> FELL over the same hours (23, 7, 5, 2). Two populations moving in opposite
/// directions, and the rising one was the invisible one.</para>
///
/// <para><b>Every recorded failure carries how long it waited.</b> The count says a condition went blind;
/// the elapsed says whose deadline ended it. A read that gives up AT its own command timeout was cut off
/// by this process while the statement was still running on the store; one that fails far below the bound
/// carries a fault the store returned. The exception text cannot separate them — a client-side deadline
/// renders as a torn stream with no SQLSTATE, identically to a dropped connection — so the duration is the
/// only discriminator, and a population whose elapsed clusters at a bound and never below it is
/// client-side expiry, because a client deadline fires at its bound while a server fault lands anywhere.
/// That argument was already available for the COLLECTOR population from <c>collection_log.duration_ms</c>;
/// this class's population writes no <c>collection_log</c> row by design, so until the elapsed was recorded
/// here and in the call site's log line it recorded no elapsed time anywhere and could not be classified at
/// all.</para>
///
/// <para><b>One awaited operation per figure, which the call sites maintain rather than the counter.</b>
/// The argument above needs the elapsed to be ONE deadline's worth. A counted block often performs several
/// awaited operations — the PostgreSQL predictor group reads four store tables, the store background-job
/// group five reads interleaved with three applies — so a single clock started at the top of the block
/// would report the sum of everything that ran. An ordinary client-side cutoff of the last read would then
/// read well ABOVE the per-read bound, and an apply summed with a read can push the total BELOW it, which
/// misreads a client cutoff as a store fault. Every site therefore restarts its clock between every pair of
/// consecutive awaits, so the figure is the elapsed of the operation that faulted. Two residuals, both
/// narrow: the shared-engine-sweep entry's awaited operation IS a whole alert pass, so its figure is coarse
/// by nature and its read name says so; and an <c>await using</c> scope's disposal at block exit has no
/// boundary of its own, so a fault there reports the last operation's elapsed plus the disposal's.</para>
///
/// <para><b>And one entry the client-versus-server reading does not apply to at all.</b> The store
/// background-job health entry names five reads that each swallow their own faults one level down and run
/// on a 30-second budget rather than the alert pass's ten, so a timeout on any of them never reaches the
/// block this measurement is scoped to and the ten-second bound would be the wrong comparison if it did.
/// Its figure is a real duration for whatever actually faulted — the connection open, most likely — and
/// nothing more. The site says so where it records.</para>
///
/// <para><b>The discriminating reading is Darling's, and the gap is named rather than papered over.</b>
/// Everything above needs the read to HAVE a deadline for the elapsed to say who ended it. Darling's alert
/// pass sets one on every store read; Lite's <c>LiteAlertReadAdapter</c> reads the local store through
/// <c>LocalDataService</c> with no command deadline. So Lite records the same measurement for the same
/// payload shape, and on Lite it means only that a read became slow.</para>
///
/// <para><b>Deliberately in memory, and deliberately not persisted.</b> The thing being counted is a
/// failure to read the store, so a counter that had to WRITE the store to be readable would be
/// unavailable exactly when it has something to say. Every consumer of this count lives in the same
/// process as the alert pass that produces it (both SKUs host their MCP surface in-process), so there
/// is nothing to persist it for. The cost is stated rather than hidden: the count covers this process
/// only, from <see cref="CountingSince"/>, and a restart takes it to zero — see
/// <see cref="WindowNote"/>, which says so on the surface rather than leaving a reader to assume it
/// shares the seven-day window the collector rows carry.</para>
///
/// <para><b>Not a band.</b> This reports counts, a currency stamp and the name of the read that failed
/// most recently. It feeds no verdict and no health status, for #3017's reason one level down: any
/// threshold over it would have to guess how many failed reads make alerting "unhealthy", and a wrong
/// guess on a surface like this one either cries wolf or — worse, and the failure mode #3013 is about —
/// says nothing is wrong.</para>
///
/// <para><b>Keying.</b> Per-server counts are held under the alert pass's own server key, verbatim and
/// ordinal, so a reader must derive the key the same way its own SKU's alert pass does (Darling:
/// <c>serverId.ToString(CultureInfo.InvariantCulture)</c>; Lite: <c>serverId.ToString()</c>). Both are
/// same-process reads of the same rendering, so they agree by construction — and
/// <c>AlertReadFailureSurfaceTests</c> pins the agreement from source rather than trusting it. Failures
/// belonging to no server (the fleet-scoped conditions <see cref="FleetScopedReads"/> names) are
/// recorded with a null key: they land in the instance total and
/// in no server's count, which is why the surface reports BOTH numbers. A per-server-only figure would
/// have given those conditions no home at all, reproducing #3013's own defect one level down.</para>
///
/// <para><b>Thread-safety.</b> Many alert passes run concurrently across servers. Counters are
/// <see cref="Interlocked"/> longs; the per-server map is a <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// of boxed holders so an increment never replaces an entry. The newest failure's stamp, name and elapsed
/// are one immutable value exchanged as a reference, so those three ARE atomic together — that one is
/// load-bearing, because the elapsed is only meaningful as the elapsed of the read the other two name, and
/// a blend of two failures would be a wrong classification rather than a stale one. The COUNTS carry no
/// such guarantee and need none: a total and a per-server count sampled a microsecond apart is not a defect
/// this surface can be misread on.</para>
/// </summary>
public sealed class AlertReadFailureCounter
{
    /// <summary>
    /// The process's counter — the one the alert pass writes and the MCP surfaces read.
    ///
    /// <para>A static well-known instance rather than a container registration because the value is
    /// process-global by nature and the two readers (each SKU's <c>get_collection_health</c>) are
    /// static tool methods in a DI container built separately from the one the alert pass is
    /// constructed in. The WRITE side is still injected — every producer takes a nullable
    /// <see cref="AlertReadFailureCounter"/> and production passes this instance explicitly — so a
    /// test constructs its own and cannot pollute this one.</para>
    /// </summary>
    public static AlertReadFailureCounter Shared { get; } = new AlertReadFailureCounter();

    /// <summary>
    /// The newest failure's three facts, held as ONE immutable value so a reader cannot see a mixture.
    ///
    /// <para><b>Why not three fields.</b> Three independent writes — even three interlocked ones — can be
    /// observed part-applied: two failures landing in the same bucket concurrently can leave a reader with
    /// one failure's elapsed beside another's read name and stamp. That is not academic here. Several call
    /// sites record with a null key, so they all share the fleet bucket, and the surface's whole claim is
    /// that the stamp, the name and the elapsed describe ONE event — the elapsed says whose deadline ended
    /// <em>that</em> read. A torn trio would make the classification wrong rather than merely stale, and it
    /// would satisfy a single-threaded test perfectly; <c>TheNewestFailuresFactsAreNeverABlendOfTwo</c>
    /// is the one that fails on it, and it does fail when the trio is published in two steps.</para>
    ///
    /// <para>Exchanged as a reference, so the trio moves atomically and every reader sees some failure's
    /// consistent facts, never a blend of two.</para>
    /// </summary>
    private sealed record LastFailure(long Ticks, string Read, long ElapsedMs);

    private sealed class ServerCounts
    {
        public long ReadFailures;
        public long Passes;
        public LastFailure? Newest;
    }

    private readonly ConcurrentDictionary<string, ServerCounts> _byServer =
        new ConcurrentDictionary<string, ServerCounts>(StringComparer.Ordinal);

    /* Failures that belong to no server: the fleet-scoped store self-alerts. Held apart from the
       per-server map rather than under a sentinel key, so no reader can accidentally resolve a server
       named after the sentinel and be handed the fleet bucket. */
    private readonly ServerCounts _fleet = new ServerCounts();

    private long _instanceReadFailures;

    /* Same trio, same reason, one level up: the instance-wide stamp and name are exchanged together so a
       reader cannot pair one failure's time with another's name. */
    private LastFailure? _instanceNewest;

    private readonly Func<DateTime> _utcNow;

    /// <summary>When this counter first counted. For <see cref="Shared"/> that is the first touch of the
    /// static — early in the host's startup, not the process's first instruction, which is why the
    /// surface reports the value rather than describing it.</summary>
    public DateTime CountingSince { get; }

    public AlertReadFailureCounter(Func<DateTime>? utcNow = null)
    {
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        CountingSince = _utcNow();
    }

    /// <summary>
    /// Records one alerting-side store read that failed and was swallowed.
    /// </summary>
    /// <param name="serverKey">
    /// The alert pass's server key, or null for a condition that belongs to no server (the fleet-scoped
    /// store self-alerts). A null or blank key lands in the fleet bucket and the instance total.
    /// </param>
    /// <param name="readName">
    /// A short, CONSTANT name for the read that failed — "deadlocks", "forced-plan failures",
    /// "collection-health self-alert". It is the actionable half of the count: which condition went
    /// blind, rather than merely that something did.
    ///
    /// <para>Deliberately not the exception message. Npgsql renders both a deadline and an unreachable
    /// backend as the same seven words, so the message adds no information the count does not already
    /// carry — and an exception message can carry host and database names, which must not reach an MCP
    /// response.</para>
    /// </param>
    /// <param name="elapsedMilliseconds">
    /// Wall clock from the start of the read attempt to the fault, in milliseconds.
    ///
    /// <para><b>Why a duration is on this call at all, given the read name already says which condition
    /// went blind.</b> The name says WHICH read failed; the elapsed says WHOSE deadline ended it. A read
    /// that gives up at its own command timeout was cut off by this process — the statement was still
    /// running on the store when the client stopped waiting — whereas one that fails far below the bound
    /// carries a fault the store returned. Those two have different remedies and the exception text
    /// cannot tell them apart: Npgsql renders a client-side deadline as a torn stream with no SQLSTATE,
    /// which is the same rendering as a dropped connection.</para>
    ///
    /// <para><b>The argument this makes available.</b> A population of failures whose elapsed clusters AT
    /// a bound and never below it is client-side deadline expiry, because a client deadline fires at its
    /// bound while an external cancel or a server fault lands anywhere. That argument already settles the
    /// collector population from <c>collection_log.duration_ms</c>; the alerting population writes no
    /// <c>collection_log</c> row by design, so without this parameter and the matching log line it
    /// recorded no elapsed time anywhere and was unclassifiable.</para>
    ///
    /// <para>Not compared to a threshold here, and deliberately: the bound belongs to whichever adapter
    /// issued the read, and the two SKUs do not even have the same KIND of bound. Darling's alert pass sets
    /// an explicit <c>CommandTimeout</c> on every store read; Lite's <c>LiteAlertReadAdapter</c> reads the
    /// local store with no command deadline at all. So a limit baked into this shared counter would be
    /// wrong for one of them and meaningless for the other. This records the measurement; the reader
    /// compares it to whatever deadline their own SKU actually sets.</para>
    ///
    /// <para>A negative value is clamped to zero. A negative duration on a health surface reads as a
    /// broken instrument rather than as a fast failure, and there is no reading it usefully.</para>
    /// </param>
    public void RecordReadFailure(string? serverKey, string readName, long elapsedMilliseconds)
    {
        var name = string.IsNullOrWhiteSpace(readName) ? "unnamed read" : readName;
        var nowTicks = _utcNow().Ticks;
        var elapsed = elapsedMilliseconds < 0 ? 0 : elapsedMilliseconds;

        /* ONE value carrying all three facts, so a concurrent second failure in the same bucket cannot
           leave a reader with this failure's elapsed beside that one's name. Published in a single
           exchange for the same reason. */
        var newest = new LastFailure(nowTicks, name, elapsed);

        var bucket = string.IsNullOrWhiteSpace(serverKey) ? _fleet : Bucket(serverKey!);
        Interlocked.Increment(ref bucket.ReadFailures);
        Interlocked.Exchange(ref bucket.Newest, newest);

        /* The instance side carries the same trio. Its ELAPSED reaches no surface today — nothing renders
           ReadInstance — so it is not on the tuple that method returns; the value is shared rather than
           recomputed because a second construction is a second chance to disagree. */
        Interlocked.Increment(ref _instanceReadFailures);
        Interlocked.Exchange(ref _instanceNewest, newest);
    }

    /// <summary>
    /// Records one alert evaluation pass for a server — the denominator the failure count is read
    /// against. Deliberately a raw count and not a rate: a Darling sweep of a connected server runs two
    /// passes (the shared engine's conditions and the service's own store-polled self-alerts) where a
    /// Lite sweep runs one, and each pass issues many reads, so no quotient of these two numbers names
    /// anything. What the denominator is FOR is telling three failures over two hundred passes apart
    /// from three over four.
    /// </summary>
    /// <remarks>
    /// <para>Three callers today, all per-server: the shared engine's <c>EvaluateCoreAsync</c>, Darling's
    /// <c>DarlingSelfAlertEvaluator.EvaluateStoreAlertsAsync</c>, and Darling's PostgreSQL predictor group
    /// <c>DarlingWorker.EvaluatePostgresAlertsAsync</c>. Each is one PASS containing many independently
    /// failure-isolated checks — isolation granularity is not pass granularity, which is why the engine's
    /// fourteen checks and the predictor group's six are one pass each rather than twenty.</para>
    /// <para>Nullable for symmetry with <see cref="RecordReadFailure"/>, but no caller passes null today:
    /// every pass is per-server, and the fleet-scoped store self-alerts are polled on their own cadences
    /// rather than as a pass over a server.</para>
    /// </remarks>
    public void RecordPass(string? serverKey)
    {
        var bucket = string.IsNullOrWhiteSpace(serverKey) ? _fleet : Bucket(serverKey!);
        Interlocked.Increment(ref bucket.Passes);
    }

    private ServerCounts Bucket(string serverKey) =>
        _byServer.GetOrAdd(serverKey, _ => new ServerCounts());

    /// <summary>
    /// One server's alerting-read health plus the instance totals it sits inside.
    /// </summary>
    /// <param name="ServerReadFailures">Swallowed alerting-side store reads for THIS server.</param>
    /// <param name="ServerAlertPasses">Alert evaluation passes for this server — the denominator.</param>
    /// <param name="InstanceReadFailures">
    /// Swallowed alerting-side store reads across every server AND the fleet-scoped store self-alerts,
    /// which belong to no server and would otherwise appear nowhere.
    /// </param>
    /// <param name="LastFailureAtUtc">
    /// When the newest failure for this server happened, or null if it has none. The currency term: a
    /// nonzero count with a stamp from days ago is a healed episode, and a count with no stamp beside it
    /// is the mistake <c>last_error</c> already taught this surface (#3010).
    /// </param>
    /// <param name="LastFailureRead">Which read failed most recently for this server.</param>
    /// <param name="LastFailureElapsedMs">
    /// How long the newest failing read for this server ran before it faulted, in milliseconds, or null if
    /// it has none.
    ///
    /// <para>The classification term, and the reason it sits beside the stamp rather than replacing it: an
    /// elapsed figure at the read's own command deadline says THIS PROCESS stopped waiting while the
    /// statement was still running on the store, and one well below the bound says the store returned a
    /// fault. Read it against the deadline the SKU documents for its alert pass — this record carries the
    /// measurement and no threshold, because the two SKUs do not share a bound — and Lite's alerting
    /// reads set no command deadline at all, so on Lite this is a plain duration rather than a
    /// client-versus-server test.</para>
    ///
    /// <para>Null exactly when <paramref name="LastFailureAtUtc"/> is null, and from the same value rather
    /// than from a matching test, so the pair cannot disagree even under concurrent failures: a reading
    /// either has a newest failure with a stamp, a name and an elapsed that all describe it, or has none of
    /// the three. One measurement beside a null stamp would be a duration belonging to no event, and one
    /// beside ANOTHER failure's name would be worse — a confident classification of the wrong read.</para>
    /// </param>
    /// <param name="CountingSinceUtc">When counting began — see <see cref="CountingSince"/>.</param>
    public sealed record Reading(
        long ServerReadFailures,
        long ServerAlertPasses,
        long InstanceReadFailures,
        DateTime? LastFailureAtUtc,
        string? LastFailureRead,
        long? LastFailureElapsedMs,
        DateTime CountingSinceUtc);

    /// <summary>
    /// Reads one server's figures. An unseen key reads as zeroes, not as an absence — the surface
    /// serializes this straight into JSON, and a null-shaped reading for a server that simply has not
    /// failed would render as a block of nulls for a reader to interpret.
    ///
    /// <para>A null or blank key is deliberately NOT a route to the fleet bucket, even though
    /// <see cref="RecordReadFailure"/> writes there for one: this is the PER-SERVER read, and a caller
    /// with no server in hand wants <see cref="ReadInstance"/>. The fleet bucket still reaches every
    /// reader through <c>InstanceReadFailures</c>, which is the field that exists for it.</para>
    /// </summary>
    public Reading ReadFor(string? serverKey)
    {
        var bucket = string.IsNullOrWhiteSpace(serverKey) ? null : Lookup(serverKey!);
        var serverFailures = bucket is null ? 0L : Interlocked.Read(ref bucket.ReadFailures);
        var serverPasses = bucket is null ? 0L : Interlocked.Read(ref bucket.Passes);

        /* ONE read of the trio, so the stamp, the name and the elapsed on the returned reading always
           describe the same failure. Taking them from three fields could pair one failure's elapsed with
           another's name, which would make the client-versus-server classification wrong rather than
           merely stale — and would pass every single-threaded test. */
        var newest = bucket is null ? null : Volatile.Read(ref bucket.Newest);

        return new Reading(
            serverFailures,
            serverPasses,
            Interlocked.Read(ref _instanceReadFailures),
            newest is null ? null : new DateTime(newest.Ticks, DateTimeKind.Utc),
            newest?.Read,
            /* Null exactly when the stamp above is null, from the same value rather than from a matching
               test, so the two cannot disagree even in principle: an elapsed with no stamp beside it would
               be a duration belonging to no event. A zero elapsed is a real reading — a read that faulted
               immediately — rather than an absence, which is why the absence is carried by the null. */
            newest?.ElapsedMs,
            CountingSince);
    }

    private ServerCounts? Lookup(string serverKey) =>
        _byServer.TryGetValue(serverKey, out var counts) ? counts : null;

    /// <summary>The instance-wide figures, for a caller with no server in hand.</summary>
    public (long ReadFailures, DateTime? LastFailureAtUtc, string? LastFailureRead) ReadInstance()
    {
        var newest = Volatile.Read(ref _instanceNewest);
        return (
            Interlocked.Read(ref _instanceReadFailures),
            newest is null ? null : new DateTime(newest.Ticks, DateTimeKind.Utc),
            newest?.Read);
    }

    /// <summary>Every server key that has recorded a pass or a failure — for a fleet-level reader.</summary>
    public IReadOnlyList<string> ServerKeys() =>
        _byServer.Keys.OrderBy(k => k, StringComparer.Ordinal).ToList();

    /// <summary>
    /// The sentence that names WHICH read went blind and when, or null when nothing has failed.
    ///
    /// <para>Display text, exactly like #3017's <c>output_finding</c>: it states a fact and recommends
    /// nothing. Composed here so the two SKUs' tools and the web panel cannot render it three ways.</para>
    /// </summary>
    public static string? FormatFinding(Reading reading)
    {
        if (reading is null)
        {
            throw new ArgumentNullException(nameof(reading));
        }

        if (reading.ServerReadFailures == 0 && reading.InstanceReadFailures == 0)
        {
            return null;
        }

        if (reading.ServerReadFailures == 0)
        {
            return string.Format(
                CultureInfo.InvariantCulture,
                "No alerting-side store read has failed for this server, but {0} failed elsewhere in this "
                + "service since {1:yyyy-MM-dd HH:mm}Z — on another server, or on a store self-alert that "
                + "belongs to no server. This server's alerting is reading fine; the service's is not "
                + "entirely.",
                reading.InstanceReadFailures,
                reading.CountingSinceUtc);
        }

        var stamp = reading.LastFailureAtUtc.HasValue
            ? string.Format(CultureInfo.InvariantCulture, ", newest at {0:yyyy-MM-dd HH:mm:ss}Z", reading.LastFailureAtUtc.Value)
            : string.Empty;

        /* The elapsed rides on the same sentence as the name because the two answer one question together:
           which read went blind, and whose deadline ended it. Stated as a measurement against "its own
           command deadline" rather than against a number, because the bound is the calling SKU's constant
           and this shared formatter does not know which SKU it is rendering for.

           THREE readings, not two, and the third is here rather than only in the window note. Some entries'
           awaited operation is not one bounded read — the shared engine sweep's is a whole alert pass — so a
           sentence offering only "at the bound" and "well below it" would hand an operator a binary for a
           figure that can legitimately be neither. Stated GENERICALLY rather than keyed on the read names it
           applies to today: a list of names in a formatter is the shape this change spent its whole review
           removing, and it would be wrong for the twenty-eighth site. */
        var which = string.IsNullOrWhiteSpace(reading.LastFailureRead)
            ? string.Empty
            : string.Format(
                CultureInfo.InvariantCulture,
                " The newest was the {0} read{1}.",
                reading.LastFailureRead,
                reading.LastFailureElapsedMs.HasValue
                    ? string.Format(
                        CultureInfo.InvariantCulture,
                        ", which ran {0} ms before it failed. Where the alert pass sets a command "
                        + "deadline on its store reads, an elapsed at or about that bound means this "
                        + "process stopped waiting while the statement was still running on the store, "
                        + "and one well below it means the store returned a fault. A figure well ABOVE "
                        + "that bound means the failure was not a single bounded read at all — some "
                        + "entries cover a whole alert pass rather than one command, so read the elapsed "
                        + "against what the named read actually is",
                        reading.LastFailureElapsedMs.Value)
                    : string.Empty);

        return string.Format(
            CultureInfo.InvariantCulture,
            "{0} alerting-side store read(s) for this server failed and were logged but reached no health "
            + "surface{1}, over {2} alert pass(es) since {3:yyyy-MM-dd HH:mm}Z. Each one is a condition this "
            + "server was not judged on for that pass — not a fired alert that was lost, and not a collector "
            + "failure ({4} across this whole service).{5}",
            reading.ServerReadFailures,
            stamp,
            reading.ServerAlertPasses,
            reading.CountingSinceUtc,
            reading.InstanceReadFailures,
            which);
    }

    /// <summary>
    /// The conditions whose swallowed reads belong to NO server, named once so the note, the class
    /// remarks and both SKUs' tool descriptions cannot disagree about the set.
    ///
    /// <para>Referenced rather than repeated: both <c>get_collection_health</c> descriptions
    /// concatenate this constant into their <c>Description</c> attribute, which is legal because a
    /// const-string concatenation is a compile-time constant — so the set cannot grow in one place and
    /// go stale in three. The first draft hand-maintained it and was wrong in both directions at once:
    /// it listed store DISK PRESSURE, whose two feed reads are both exempt (a local filesystem read,
    /// and a recorded-store-size lookup that is context for the alert text rather than the evidence
    /// it is judged on), so that condition can never contribute a failure here; and it omitted the
    /// collector-cost regression self-alert, which does. An operator reading the phantom list would
    /// have hunted a disk-pressure read that cannot fail into this number, and would not have thought
    /// to check the one that can.</para>
    /// </summary>
    public const string FleetScopedReads =
        "the collector-cost regression self-alert, and the store background-job health reads behind "
        + "compression-job health, store-job cadence and retention holds";

    /// <summary>
    /// The window these figures cover, and the window they do NOT.
    ///
    /// <para>Said out loud because this block is the one part of <c>get_collection_health</c> that is not
    /// on the response's seven-day window, and a reader who assumed it was would read a zero as "no
    /// alerting failures in seven days" when a restart minutes ago is all it means. #3017's
    /// <c>output_note</c> established the discipline; this is the same claim about a different window.</para>
    /// </summary>
    public const string WindowNote =
        "alert_read_health is the ONLY block on this response that is not measured over the trailing seven "
        + "days. It is an in-memory count kept by the running service or app, from counting_since — which is "
        + "when this process began counting, early in its own startup — to now, and a restart takes it to "
        + "zero. So a zero here means \"none since counting_since\" and NOT \"none in seven days\": check "
        + "counting_since before reading "
        + "the zero as reassurance, because a process that started a minute ago can only report on a minute. "
        + "It is deliberately not persisted: what it counts is a failure to READ the store, so a counter that "
        + "had to write the store would be unavailable exactly when it has something to report. It counts "
        + "alerting-side store reads that failed and were swallowed by design (the alert pass logs and skips "
        + "rather than firing or resolving on absent evidence), which is why they appear on no other health "
        + "surface: they are not collector runs and write no collection_log row. last_failure_elapsed_ms is "
        + "how long that newest failing read ran before it faulted. Where the alert pass sets a command "
        + "deadline on its store reads it is the term that says WHOSE deadline ended the read - at or about "
        + "that bound means this process stopped waiting while the statement was still running on the store, "
        + "and well below it means the store returned a fault - and those need different answers, because "
        + "the exception text cannot tell them apart: a client-side deadline renders as a torn stream with "
        + "no SQLSTATE, exactly like a dropped connection. A figure well ABOVE that bound is a third reading: "
        + "the failure was not a single bounded read. Each site restarts its clock between every pair of "
        + "consecutive awaits so that is rare, and the one entry where it is expected is the shared engine "
        + "sweep, whose awaited operation is a whole alert pass rather than one command. One further "
        + "entry the client-versus-server reading does not apply to at all: the store background-job "
        + "health reads each swallow their own faults one level down and run on a 30-second budget "
        + "rather than the alert pass's ten, so that entry's figure is a real duration for whatever "
        + "faulted and not evidence about who ended it. Read the rest that way on the Darling service, "
        + "whose "
        + "alert pass sets an explicit command deadline. On Lite the alerting reads hit the local store with "
        + "no command deadline of their own, so the figure there is a plain duration - it says a read became "
        + "slow, and nothing about who ended it. Either way it is null exactly when last_failure_at is null, "
        + "so an elapsed never describes an event with no stamp. It does NOT count fired "
        + "alerts that failed to DELIVER, and it makes no claim about them — that is the alert-history read's "
        + "question, not this one. instance_read_failures spans every server on this service plus the "
        + "fleet-scoped conditions that belong to no server and so appear in no per-server count: "
        + FleetScopedReads + ". "
        + "server_alert_passes is a denominator for judging whether the failure count is large, not a rate: a "
        + "pass issues many reads, and the number of passes per sweep differs by host and by target engine: a "
        + "Darling sweep of a SQL Server target runs two (the shared engine's conditions and the service's "
        + "own store-polled self-alerts), a PostgreSQL target runs three (those two plus the PostgreSQL "
        + "predictor group), and a Lite sweep runs one. So this denominator is comparable between servers "
        + "on the same host and engine, and NOT across engines or across SKUs.";
}
