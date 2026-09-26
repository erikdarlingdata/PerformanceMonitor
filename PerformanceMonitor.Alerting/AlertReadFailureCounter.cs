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
/// <para><b>Every count is reported with the stamp and the name that identify it.</b> A count on this
/// surface never ages out of a window, so a bare number cannot separate a healed startup artefact from a
/// live episode — <c>last_error</c>'s #3010 lesson, and it applies once per population rather than once
/// per surface. Each of the three counts a reading carries therefore has its own newest-failure trio
/// taken from its own bucket: this server's, the fleet bucket's, and the instance-wide newest whatever its
/// scope. The fleet one is the load-bearing addition, because a failure recorded under a null key belongs
/// to no server, so no <see cref="ReadFor"/> key can reach the per-server trio that would name it — a
/// fleet-scoped failure was countable and not identifiable, on exactly the conditions the null key exists
/// to give a home to. The fourth population, the failures on OTHER servers, is the subtraction of the two
/// parts from the total: it gets no count and no trio here, deliberately, because this class holds no
/// newest failure for it and a count with nothing to date it is the defect rather than the fix.</para>
///
/// <para><b>Thread-safety.</b> Many alert passes run concurrently across servers. Counters are
/// <see cref="Interlocked"/> longs; the per-server map is a <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// of boxed holders so an increment never replaces an entry. The newest failure's stamp, name and elapsed
/// are one immutable value exchanged as a reference, so those three ARE atomic together — that one is
/// load-bearing, because the elapsed is only meaningful as the elapsed of the read the other two name, and
/// a blend of two failures would be a wrong classification rather than a stale one.</para>
///
/// <para>The COUNTS are not atomic together and cannot be, but their relative ORDER is load-bearing and is
/// stated at both ends. Two of the block's readings are cross-scope — a count read against the trio that
/// identifies it, and a total read against the two parts subtracted from it — so a pair sampled a
/// microsecond apart is a defect this surface can be misread on, in two specific directions. Both are
/// closed by ordering rather than by locking: <see cref="RecordReadFailure"/> publishes a failure's
/// identity ahead of the count it identifies and the instance total ahead of either bucket, and
/// <see cref="ReadFor"/> mirrors it, sampling every count before its trio and the total last of the three.
/// One half of that was wrong when the cross-scope subtraction was first written, and the assertion that
/// found it reported about six negative readings per thousand — so neither half is a stylistic
/// preference.</para>
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
        public long RetriedReads;
        public long Passes;
        public LastFailure? Newest;
    }

    private readonly ConcurrentDictionary<string, ServerCounts> _byServer =
        new ConcurrentDictionary<string, ServerCounts>(StringComparer.Ordinal);

    /* Failures that belong to no server: the fleet-scoped store self-alerts. Held apart from the
       per-server map rather than under a sentinel key, so no reader can accidentally resolve a server
       named after the sentinel and be handed the fleet bucket.

       Its count and its newest-failure trio are both READ, by every per-server reading: a failure here
       belongs to no server, so no per-server bucket holds it and no ReadFor key can reach it, which left
       the instance total as the only trace it made and made the condition that went blind unnameable. Its
       Passes field is deliberately NOT read — no caller records a fleet-scoped pass, so a denominator for
       this bucket would be a permanent zero, which is the confident-zero shape this class exists to
       remove rather than add. */
    private readonly ServerCounts _fleet = new ServerCounts();

    private long _instanceReadFailures;

    /* #3848: retried reads, a SECOND population held beside the failures rather than folded into them.
       It carries no newest-failure trio and deliberately: what it counts is a read that CROSSED the
       alert pass's command deadline once and then answered on the single retry two seconds later, so
       there is no failure to date or name — the condition was judged, from evidence that arrived late.
       The three-stamps discipline above applies to counts of things that went BLIND; this one counts
       things that did not, and a stamp on it would invite the reading that a retried read is a soft
       failure. Its currency term is the same counting_since every other count on the block is read
       against.

       Incremented at most ONCE per read attempt-pair, whatever the outcome: a read that timed out and
       then succeeded lands here only, and one that timed out twice lands here AND in the failure count
       — the retry happened either way, and a retry counted only when it worked would understate the
       write band's cost by exactly the episodes where the band was worst. See
       DarlingAlertReadAdapter.ExecuteWithOneRetryAsync, which is the only producer. */
    private long _instanceRetriedReads;

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

        /* PUBLICATION ORDER, and both halves of it are load-bearing. The instance side carries the same
           trio as the bucket — every reading renders the instance stamp, read name and elapsed beside the
           instance count, so that count is dated and named exactly the way the per-server one is — and the
           value is shared with the bucket rather than recomputed, because a second construction is a second
           chance to disagree.

           IDENTITY BEFORE COUNT, per scope. A count published first is observable for as long as the next
           instruction takes with nothing beside it to date or name it, which is precisely the reading this
           block exists to make impossible. Published this way round, a reader that has seen a nonzero count
           has already had the trio available, so "every count carries its trio" holds at every instant and
           not merely once the writer has finished. The reverse pairing — a trio published with the count
           still at zero — is the harmless one: it claims no count it cannot identify.

           INSTANCE BEFORE BUCKET. The instance total is what the two parts are subtracted from to name the
           failures on OTHER servers, so it must never trail them; if a bucket were incremented first, a
           reader could see a bucket that the total had not counted yet and the subtraction would come out
           NEGATIVE, which on a health surface reads as a broken instrument rather than a small number.
           Incremented this way round the total leads by however many writes are in flight, so the
           subtraction can read transiently high and never below zero. High is a number; negative is not.
           ReadFor's own sampling order is the other half of both guarantees and says so. */
        Interlocked.Exchange(ref _instanceNewest, newest);
        Interlocked.Exchange(ref bucket.Newest, newest);
        Interlocked.Increment(ref _instanceReadFailures);
        Interlocked.Increment(ref bucket.ReadFailures);
    }

    /// <summary>
    /// Records one alerting-side store read that crossed its command deadline and was RETRIED once
    /// (#3848) — the write bands' cost, counted rather than blinding an alert.
    ///
    /// <para><b>Why this is a count and not a failure.</b> Every episode this counts is one the previous
    /// design recorded through <see cref="RecordReadFailure"/>: the read crossed the ten-second deadline,
    /// the condition was skipped for that pass, and the next pass thirty seconds later ran into the same
    /// write band. The retry converts most of that population into answered reads. The cost must stay
    /// VISIBLE anyway, because it is the store's write bands showing through — a store whose compression,
    /// aggregate-materialization and checkpoint bands stall reads for twelve seconds at a time is a fact
    /// about the store, and a fix that made it silent would have traded one blind spot for another. So
    /// this is the number that rises where <c>ReadFailures</c> used to, and an operator reads the pair:
    /// retries alone is a store under write pressure with alerting intact, retries plus failures is a
    /// store where twelve seconds was not enough.</para>
    ///
    /// <para><b>Counted per attempt-pair, not per success.</b> A read that timed out and then answered
    /// increments this only; a read that timed out TWICE increments this and
    /// <see cref="RecordReadFailure"/>. The retry happened in both cases, and a counter that only
    /// recorded the working ones would understate the cost exactly during the episodes where the band was
    /// worst — the confident-zero shape this whole class exists to remove.</para>
    ///
    /// <para><b>No stamp, no elapsed, and no newest-retry trio</b>, unlike every count above. Those exist
    /// because a count of blind reads with nothing to date it cannot separate a healed episode from a live
    /// one (#3010's lesson). A retried read is not blind: the condition WAS judged on real evidence that
    /// arrived late, so there is no episode to attribute, and a trio here would invite exactly the reading
    /// this paragraph refuses — that a retried read is a soft failure. <see cref="CountingSince"/> is its
    /// currency term, the same one the block's other counts are read against.</para>
    /// </summary>
    /// <param name="serverKey">
    /// The alert pass's server key, or null for a read that belongs to no server — the same keying
    /// <see cref="RecordReadFailure"/> documents, so a retry and a failure on the same read land in the
    /// same bucket.
    /// </param>
    /// <param name="readName">
    /// The same short CONSTANT name the failure call site uses for that read. Not currently rendered on
    /// any surface — the block publishes the retry COUNT and no newest-retry identity, for the reason
    /// above — and taken anyway so the producer cannot record a retry it could not name, which is how a
    /// future per-read breakdown becomes possible without re-instrumenting fourteen sites.
    /// </param>
    public void RecordRetriedRead(string? serverKey, string readName)
    {
        _ = readName;

        var bucket = string.IsNullOrWhiteSpace(serverKey) ? _fleet : Bucket(serverKey!);

        /* INSTANCE BEFORE BUCKET, the same way RecordReadFailure orders its two counts and for the same
           reason: the instance total is what a reader subtracts the parts from, so it must never trail
           them. There is no trio to publish ahead of either count here, which is why this is two
           statements rather than four. */
        Interlocked.Increment(ref _instanceRetriedReads);
        Interlocked.Increment(ref bucket.RetriedReads);
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
    /// which belong to no server and appear in no per-server count.
    ///
    /// <para>The three populations this total spans are readable from the block rather than blended into
    /// it: <paramref name="ServerReadFailures"/> is this server's, <paramref name="FleetReadFailures"/> is
    /// the part belonging to no server, and the remainder — this total less those two — is what failed on
    /// OTHER servers. That remainder is deliberately not a member here: this counter holds no newest
    /// failure for it, and a count with nothing to date or name it is the gap the three stamps on this
    /// record exist to close.</para>
    ///
    /// <para>This total LEADS its two parts by however many failures are in flight while the reading is
    /// taken, by construction — see the publication and sampling orders in
    /// <see cref="RecordReadFailure"/> and <see cref="ReadFor"/>. So the remainder can read a failure or
    /// two high on a service that is failing reads concurrently, and can never read below zero. That
    /// direction is the deliberate one: a count slightly high is a number, and a negative count on a
    /// health surface is a broken instrument.</para>
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
    /// <param name="FleetReadFailures">
    /// Swallowed alerting-side store reads that belong to NO server — the fleet-scoped conditions named by
    /// <see cref="FleetScopedReads"/>.
    ///
    /// <para>The figure that makes a nonzero <paramref name="InstanceReadFailures"/> readable from a server
    /// whose own count is zero. Without it that total spans two populations an operator would act on
    /// differently and cannot tell apart: a blind read on another server, which the same block on that
    /// server answers, and a blind read on a condition that belongs to no server, which no per-server block
    /// answers at all. Two of the conditions in that set are the store background-job health reads, whose
    /// alerts are what would say the store itself is in trouble — so this count is most likely to be
    /// nonzero exactly when the population it isolates is the one that matters.</para>
    /// </param>
    /// <param name="FleetLastFailureAtUtc">
    /// When the newest fleet-scoped failure happened, or null if there has been none. The currency term for
    /// <paramref name="FleetReadFailures"/>: that count never ages out of a window, so without a stamp a
    /// healed episode from hours ago and one still in progress read identically.
    /// </param>
    /// <param name="FleetLastFailureRead">
    /// Which fleet-scoped read failed most recently — the one field that says WHICH of those conditions
    /// went blind, rather than that one did.
    /// </param>
    /// <param name="FleetLastFailureElapsedMs">
    /// How long that newest fleet-scoped read ran before it faulted, in milliseconds, or null if there has
    /// been none.
    ///
    /// <para>Read it against what the named read actually is, not against the alert pass's deadline: the
    /// store background-job health reads swallow their own faults one level down and run on their own
    /// budget, so for that entry this is a plain duration for whatever faulted rather than evidence about
    /// who ended it.</para>
    /// </param>
    /// <param name="InstanceLastFailureAtUtc">
    /// When the newest failure ANYWHERE in this service happened, whatever its scope, or null if there has
    /// been none. The currency term for <paramref name="InstanceReadFailures"/>, and the only one that
    /// covers the other-server population — which has no count of its own on this record.
    ///
    /// <para>It makes no claim about scope: it is the newest of every failure this counter has seen, so it
    /// may name a per-server read, a fleet-scoped one, or one on a server the caller did not ask about.
    /// <paramref name="FleetLastFailureAtUtc"/> is the one that is attributable by construction.</para>
    /// </param>
    /// <param name="InstanceLastFailureRead">Which read failed most recently anywhere in this service.</param>
    /// <param name="InstanceLastFailureElapsedMs">
    /// How long that newest failing read anywhere ran before it faulted, in milliseconds, or null if there
    /// has been none.
    /// </param>
    /// <param name="ServerRetriedReads">
    /// Reads for THIS server that crossed the alert pass's ten-second deadline once and were retried two
    /// seconds later (#3848) — see <see cref="RecordRetriedRead"/>.
    ///
    /// <para>Read it beside <paramref name="ServerReadFailures"/>, because the two together are what the
    /// retry made legible and neither says it alone. Retries rising with failures at zero is a store under
    /// write pressure whose alerting is intact — the population that used to BE the failure count. Both
    /// rising is a store where a second attempt twelve seconds later still found the band on, which is the
    /// reading that wants the store's write schedule looked at rather than the reader's.</para>
    ///
    /// <para>Carries no newest-retry stamp, name or elapsed, unlike each count above, and that asymmetry
    /// is deliberate rather than an omission: those three exist to date and attribute a condition that went
    /// BLIND, and a retried read did not — it was judged on real evidence that arrived late. A trio here
    /// would invite the reading that a retry is a soft failure.</para>
    /// </param>
    /// <param name="InstanceRetriedReads">
    /// The same count across every server on this service plus any read belonging to none, the way
    /// <paramref name="InstanceReadFailures"/> spans its own three populations.
    ///
    /// <para>No fleet-scoped part is reported beside it, unlike the failure total, because nothing
    /// records a fleet-scoped retry today, and #3854 did not change that while widening what the seam
    /// covers: the retried population is now every store read the alert pass issues PER SERVER — the
    /// twelve on the alert-read adapter, the six the Darling store's self-alert evaluator issues for one
    /// server (collection signals, missing capture sessions, the two agent-status reads, and the two
    /// Availability-Group grains), and the worker's latest-CPU read — so every one of them keys a server
    /// bucket. The conditions <see cref="FleetScopedReads"/> names still execute their own commands
    /// outside the seam, so a fleet part would be a structural zero, which is the confident-zero shape
    /// this class exists to remove — the same reason the fleet bucket's pass denominator is written and
    /// never read. Should a fleet-scoped read ever join the seam, its retries land in this total and a
    /// fleet part becomes worth publishing.</para>
    /// </param>
    public sealed record Reading(
        long ServerReadFailures,
        long ServerAlertPasses,
        long InstanceReadFailures,
        DateTime? LastFailureAtUtc,
        string? LastFailureRead,
        long? LastFailureElapsedMs,
        DateTime CountingSinceUtc,
        long FleetReadFailures,
        DateTime? FleetLastFailureAtUtc,
        string? FleetLastFailureRead,
        long? FleetLastFailureElapsedMs,
        DateTime? InstanceLastFailureAtUtc,
        string? InstanceLastFailureRead,
        long? InstanceLastFailureElapsedMs,
        long ServerRetriedReads,
        long InstanceRetriedReads);

    /// <summary>
    /// Reads one server's figures. An unseen key reads as zeroes, not as an absence — the surface
    /// serializes this straight into JSON, and a null-shaped reading for a server that simply has not
    /// failed would render as a block of nulls for a reader to interpret.
    ///
    /// <para>A null or blank key is deliberately NOT a route to the fleet bucket, even though
    /// <see cref="RecordReadFailure"/> writes there for one: this is the PER-SERVER read, and a caller
    /// with no server in hand wants <see cref="ReadInstance"/>. The fleet bucket reaches every reader
    /// through <c>FleetReadFailures</c> and its own newest-failure trio, which are on every reading
    /// whatever key was asked for — those fields exist for it, and are the reason a failure belonging to
    /// no server is nameable from a surface that requires one.</para>
    /// </summary>
    public Reading ReadFor(string? serverKey)
    {
        var bucket = string.IsNullOrWhiteSpace(serverKey) ? null : Lookup(serverKey!);

        /* SAMPLING ORDER — the mirror of RecordReadFailure's publication order, and the other half of both
           guarantees it states. Every count first, the instance total LAST of the three, and only then the
           trios.

           COUNTS BEFORE TRIOS, because identity is published before the count it identifies: a reader that
           has already seen a nonzero count is guaranteed a trio, since the trio was in place before that
           count became visible and is never cleared. Sampling a trio FIRST would allow a null trio beside a
           count read afterwards — a count with nothing to date it, which is the defect and not the fix.

           THE TOTAL LAST, because the writer increments it before the bucket: the total therefore leads the
           buckets, and sampling it after them keeps it at least their sum, so the subtraction that names the
           failures on OTHER servers cannot come out negative. Both orders are needed — reading the total
           last while the writer incremented the bucket first still reads a bucket the total has not counted.
           Not a theoretical pairing: the assertion that discriminates them found the write order wrong the
           first time it ran, at about six negative readings per thousand. */
        var serverFailures = bucket is null ? 0L : Interlocked.Read(ref bucket.ReadFailures);
        var serverPasses = bucket is null ? 0L : Interlocked.Read(ref bucket.Passes);

        /* The fleet bucket is read on EVERY reading, not only when the caller has no server: these failures
           belong to no server, so a reader who asked about one still needs them to make sense of the
           instance total standing beside their own zero. */
        var fleetFailures = Interlocked.Read(ref _fleet.ReadFailures);

        /* #3848's pair, sampled BUCKET-THEN-TOTAL like the failures beside them and for the identical
           reason: no reading subtracts these two today, but the total is the one a future fleet part would
           be taken out of, and an order that reads correctly only while nothing subtracts it is an order
           that breaks silently the day something does. Sampled before the trios below, so the
           counts-before-identity rule holds for every count on the reading rather than for most of them. */
        var serverRetries = bucket is null ? 0L : Interlocked.Read(ref bucket.RetriedReads);
        var instanceRetries = Interlocked.Read(ref _instanceRetriedReads);

        var instanceFailures = Interlocked.Read(ref _instanceReadFailures);

        /* ONE read of each trio, so the stamp, the name and the elapsed on the returned reading always
           describe the same failure. Taking them from three fields could pair one failure's elapsed with
           another's name, which would make the client-versus-server classification wrong rather than
           merely stale — and would pass every single-threaded test. */
        var newest = bucket is null ? null : Volatile.Read(ref bucket.Newest);
        var fleetNewest = Volatile.Read(ref _fleet.Newest);
        var instanceNewest = Volatile.Read(ref _instanceNewest);

        /* Named arguments, not positional. Fourteen of them repeat four shapes across three scopes, so a
           reordering compiles while pairing one scope's stamp with another scope's read name — a confident
           wrong attribution, which is the defect class this block exists to remove rather than produce. */
        return new Reading(
            ServerReadFailures: serverFailures,
            ServerAlertPasses: serverPasses,
            InstanceReadFailures: instanceFailures,
            LastFailureAtUtc: Stamp(newest),
            LastFailureRead: newest?.Read,
            /* Null exactly when the stamp above is null, from the same value rather than from a matching
               test, so the two cannot disagree even in principle: an elapsed with no stamp beside it would
               be a duration belonging to no event. A zero elapsed is a real reading — a read that faulted
               immediately — rather than an absence, which is why the absence is carried by the null. */
            LastFailureElapsedMs: newest?.ElapsedMs,
            CountingSinceUtc: CountingSince,
            /* Each of the other two counts carries the same trio, from its own single value, for the same
               reason: a count on this surface with no stamp to date it and no name to attribute it is the
               #3010 mistake one level down, and the fleet count is the one no per-server bucket can ever
               supply. */
            FleetReadFailures: fleetFailures,
            FleetLastFailureAtUtc: Stamp(fleetNewest),
            FleetLastFailureRead: fleetNewest?.Read,
            FleetLastFailureElapsedMs: fleetNewest?.ElapsedMs,
            InstanceLastFailureAtUtc: Stamp(instanceNewest),
            InstanceLastFailureRead: instanceNewest?.Read,
            InstanceLastFailureElapsedMs: instanceNewest?.ElapsedMs,
            /* #3848: the retry counts last on the record, appended rather than placed beside the failure
               counts they are read against. Both surfaces render this block field-by-field from named
               members, so position carries no meaning for a reader — while the record's ORDER is what a
               positional construction elsewhere would bind to, and the two SKUs' surface pins reflect over
               these members by name. Appended keeps every existing ordinal where it was. */
            ServerRetriedReads: serverRetries,
            InstanceRetriedReads: instanceRetries);
    }

    /// <summary>
    /// A failure's tick count as a UTC <see cref="DateTime"/>, or null for no failure.
    ///
    /// <para>One helper for all three scopes rather than the expression written out three times: the three
    /// stamps must render identically, and three copies of a <c>DateTimeKind</c> argument is three chances
    /// for one of them to say <c>Unspecified</c> and serialize without its offset.</para>
    /// </summary>
    private static DateTime? Stamp(LastFailure? failure) =>
        failure is null ? null : new DateTime(failure.Ticks, DateTimeKind.Utc);

    private ServerCounts? Lookup(string serverKey) =>
        _byServer.TryGetValue(serverKey, out var counts) ? counts : null;

    /// <summary>
    /// The instance-wide figures, for a caller with no server in hand.
    ///
    /// <para>Carries the whole trio, including the elapsed, so this route and the instance fields on
    /// <see cref="Reading"/> cannot differ about which facts about the newest failure exist. A tuple
    /// missing one of them would be a second definition of the same event.</para>
    /// </summary>
    public (long ReadFailures, DateTime? LastFailureAtUtc, string? LastFailureRead, long? LastFailureElapsedMs) ReadInstance()
    {
        var newest = Volatile.Read(ref _instanceNewest);
        return (
            Interlocked.Read(ref _instanceReadFailures),
            Stamp(newest),
            newest?.Read,
            newest?.ElapsedMs);
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

        /* The failures on OTHER servers, by subtraction: the instance total, less this server's, less the
           fleet-scoped ones that belong to no server. Stated as arithmetic over three figures the block
           also carries, so a reader can check it, and NOT rendered as a field of its own — this counter
           holds no newest failure for that population, and a fourth count with nothing to date or name it
           would be the same defect the three stamps here exist to close. The writer increments the total
           before either part and the reader samples it after both, so the total leads and this cannot come
           out negative; it can read a failure or two high while failures are landing concurrently, which
           is the direction chosen deliberately. */
        var otherServers =
            reading.InstanceReadFailures - reading.ServerReadFailures - reading.FleetReadFailures;

        if (reading.ServerReadFailures == 0)
        {
            return string.Format(
                CultureInfo.InvariantCulture,
                "No alerting-side store read has failed for this server, but {0} failed elsewhere in this "
                + "service since {1:yyyy-MM-dd HH:mm}Z: {2} on store self-alerts that belong to no server, "
                + "and {3} on other servers.{4} This server's alerting is reading fine; the service's is "
                + "not entirely.",
                reading.InstanceReadFailures,
                reading.CountingSinceUtc,
                reading.FleetReadFailures,
                otherServers,
                FleetSentence(reading));
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
            + "failure.{4} Across this whole service {5} alerting-side read(s) failed: {0} here, {6} on "
            + "store self-alerts that belong to no server, and {7} on other servers.{8}",
            reading.ServerReadFailures,
            stamp,
            reading.ServerAlertPasses,
            reading.CountingSinceUtc,
            which,
            reading.InstanceReadFailures,
            reading.FleetReadFailures,
            otherServers,
            FleetSentence(reading));
    }

    /// <summary>
    /// Names the newest fleet-scoped failure, or renders nothing when there has been none.
    ///
    /// <para>Its own sentence rather than part of the count, because the count and the name answer
    /// different halves of one question: how much of the service total belongs to no server, and WHICH of
    /// those conditions went blind. The read name is the only attributable one on the block — the newest
    /// failure anywhere may be on a server the caller did not ask about, so it is reported as scope-free,
    /// while this one belongs to the fleet bucket by construction.</para>
    /// </summary>
    private static string FleetSentence(Reading reading)
    {
        if (reading.FleetReadFailures == 0)
        {
            return string.Empty;
        }

        var stamp = reading.FleetLastFailureAtUtc.HasValue
            ? string.Format(
                CultureInfo.InvariantCulture,
                " at {0:yyyy-MM-dd HH:mm:ss}Z",
                reading.FleetLastFailureAtUtc.Value)
            : string.Empty;

        /* The elapsed rides along for the same reason it does on the per-server sentence, and without the
           client-versus-server framing: the store background-job health reads in this set swallow their own
           faults one level down and run on their own budget rather than the alert pass's, so a bound to
           compare against is not something this formatter can name for the fleet scope. */
        var ran = reading.FleetLastFailureElapsedMs.HasValue
            ? string.Format(
                CultureInfo.InvariantCulture,
                ", which ran {0} ms before it failed",
                reading.FleetLastFailureElapsedMs.Value)
            : string.Empty;

        return string.Format(
            CultureInfo.InvariantCulture,
            " The newest of the ones belonging to no server was the {0} read{1}{2}.",
            string.IsNullOrWhiteSpace(reading.FleetLastFailureRead)
                ? "unnamed"
                : reading.FleetLastFailureRead,
            stamp,
            ran);
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
    ///
    /// <para>The mute-rule reload is the member of this set whose failure leaves a cache that is correct
    /// and STALE rather than absent — the rules already in force stay in force, so nothing is un-muted, but
    /// a rule written or deleted during the outage is not honoured yet. Nothing else can tell that apart
    /// from a cache that is correct and current: the rule list, every mute surface and the stale-mute
    /// condition all read what the last successful load put there. A nonzero instance total naming this
    /// read is the artefact, which is why it is counted rather than exempt.</para>
    ///
    /// <para>#3443 grew the collector-cost condition from one read to three, and they are named apart
    /// rather than folded into one entry because they blind DIFFERENT things: the regression read is the
    /// predicate's own evidence, the census read is the fan-out denominator that decides which channel a
    /// finding reaches, and the digest read is the daily report's movers. A failure of any one of them
    /// skips the rest of that tick, so an operator chasing a nonzero count needs to know which stage went
    /// quiet — a blind denominator and a blind report are not the same outage.</para>
    ///
    /// <para>It is also the case that only ONE member of this set — the mute-rule reload — can be produced
    /// by BOTH SKUs, and that is load-bearing for a constant both descriptions concatenate. The others are
    /// Darling store self-alerts that have no Lite equivalent at all, so naming them there describes a
    /// shared inventory rather than promising a Lite reading. The mute-rule reload is different: Lite
    /// performs that read, Lite swallows its failure, and Lite's call site therefore records it here too.
    /// A read this constant names on a SKU that cannot increment it would be this class's own defect — a
    /// confident zero — reproduced in its documentation.</para>
    ///
    /// <para>#3580 added one more Darling-only member: the daily documents' DELIVERY-STAMP read, the gate
    /// that decides whether the digest or the rollup was already delivered today. It is one read site
    /// serving both documents (so one name here), and it is counted rather than exempt because its
    /// swallowed failure is the gate falling back to process memory — which re-announces the document once
    /// per restart until the store answers, the very behaviour #3580 retired. A nonzero count naming it
    /// says the store could not be asked "was one delivered today", not that a document was lost.</para>
    ///
    /// <para>#3712 added the analysis singles digest read — the third daily document's span read over the
    /// digest-routed alert-history rows, Darling-only like the other two documents. Counted on the rollup's
    /// reasoning: its swallowed failure skips the day's tick without consuming the interval, and a fault
    /// folded into "no singles today" would make an unreadable store read as a quiet one.</para>
    /// </summary>
    public const string FleetScopedReads =
        "the collector-cost regression self-alert and its two #3443 companions (the collector-cost census "
        + "read that decides paging-versus-digest routing, and the collector-cost digest read behind the "
        + "daily report), the mute-rule reload, the fleet-sweep rollup read behind the daily sweep report "
        + "(#3466), the analysis singles digest read behind the daily copy of the findings the corroboration "
        + "gate kept off the paging channels (#3712), the daily documents' delivery-stamp read that gates the "
        + "digest, the rollup and the singles digest on delivered-today (#3580), the store background-job "
        + "health reads behind compression-job health, store-job cadence and retention holds, and the two "
        + "informational store self-alerts #3826 added — the plan dimension's TOAST slack read and the "
        + "store checkpointer pressure read behind the WAL levers, and the store settings self-alert's "
        + "managed-conf verdicts read (#4215)";

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
        + "question, not this one. retried_reads is the SECOND population, and on the Darling service it is "
        + "where most of what this block used to count now lands: reads that crossed the 10 s deadline once "
        + "and succeeded on the single retry two seconds later - the write bands' cost, counted rather than "
        + "blinding an alert. Every one of those was a swallowed failure before #3848, so read the two "
        + "together: retries rising with failures at zero is a store under write pressure whose alerting is "
        + "intact, and both rising is a store where a second attempt twelve seconds later still found the "
        + "band on - which wants the store's write schedule looked at rather than the reader's. A read that "
        + "failed twice counts in BOTH, because the retry happened. It carries no stamp and no read name, "
        + "deliberately: a retried read did not go blind, so there is no episode to date or attribute, and a "
        + "stamp on it would invite reading a retry as a soft failure. counting_since is its currency term "
        + "like every other count here. instance_retried_reads is the same figure across this whole service; "
        + "there is no fleet part for it because nothing records a fleet-scoped retry today - the retry seam "
        + "covers the alert pass's PER-SERVER store reads (the alert-read adapter's, the Darling store's "
        + "self-alert reads for one server, and the latest-CPU read), and the conditions listed below run "
        + "their own commands outside it, so a fleet part would be a structural zero. On Lite both figures "
        + "stay at zero, and "
        + "that is a property of the SKU rather than a quiet store: Lite's alerting reads hit the local store "
        + "with no command deadline, so there is no deadline for a read to cross and nothing to retry on. "
        + "instance_read_failures spans every server on this service plus the "
        + "fleet-scoped conditions that belong to no server and so appear in no per-server count: "
        + FleetScopedReads + ". "
        + "fleet_read_failures is how many of that total belong to no server, and it is what makes a "
        + "nonzero service count readable from a server whose own count is zero: those two populations "
        + "take opposite actions, since a blind fleet-scoped read means the store's own self-alerts went "
        + "quiet and two of them are the reads whose alerts would say the store is in trouble, while one on "
        + "another server is answered by reading this same block there. Every count here carries its own "
        + "newest-failure stamp, read name and elapsed, so none of them sits undated: last_failure_* are "
        + "THIS server's, fleet_last_failure_* are the fleet-scoped conditions' and are attributable by "
        + "construction, and instance_last_failure_* are the newest failure anywhere in this service "
        + "whatever its scope - so that last trio may name a read on a server you did not ask about and "
        + "makes no claim about which. The third population is a subtraction: instance_read_failures minus "
        + "server_read_failures minus fleet_read_failures is how many failed on OTHER servers. It is "
        + "deliberately not a field, because nothing here holds a newest failure for it and a count with "
        + "nothing to date or name it is the gap the three stamps close; read this block on those servers "
        + "to attribute it. "
        + "server_alert_passes is a denominator for judging whether the failure count is large, not a rate: a "
        + "pass issues many reads, and the number of passes per sweep differs by host and by target engine: a "
        + "Darling sweep of a SQL Server target runs two (the shared engine's conditions and the service's "
        + "own store-polled self-alerts), a PostgreSQL target runs three (those two plus the PostgreSQL "
        + "predictor group), and a Lite sweep runs one. So this denominator is comparable between servers "
        + "on the same host and engine, and NOT across engines or across SKUs.";
}
