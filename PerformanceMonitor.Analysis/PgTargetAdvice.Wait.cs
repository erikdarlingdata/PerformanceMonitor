/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for the wait profile (filled by lane 5 of #3542): the four type rollups and the four named
/// standouts, value-stated from the fact the collector emitted — the wait time, the observed time it is a
/// fraction of, the share of all waiting, and (for a standout) the share of its type — followed by what that
/// wait IS in PostgreSQL and the levers that move it, each with its counter-objective.
///
/// <para><b>Two grades, two evidence sentences, one vocabulary.</b> An Aurora fact's sentence says the engine
/// MEASURED the wait. A sampled fact's sentence says the figure is "estimated from sampling" at the stated
/// period, that the estimate cannot see a wait shorter than that period, and that it is backend-sample time
/// summed over tasks — and no wait block, of either grade, says the server "spent" time waiting, because the
/// fraction is time-over-tasks per wall second and can exceed one; a reader who hears "spent 120 % of the
/// window" has been told something false. Both pins (<c>estimated from sampling</c> present on a sampled
/// fact, <c>spent</c> absent on every wait block) are held by <c>PgTargetWaitTests</c>.</para>
///
/// <para><b>The SQL Server lock advice does not transfer.</b> PostgreSQL has MVCC: readers never block writers
/// and writers never block readers, so a relation-lock queue is DDL, an explicit <c>LOCK TABLE</c>, a
/// <c>TRUNCATE</c>, or a conflicting mode held by a long or parked transaction — never a reader/writer
/// isolation problem. The remedies named are PostgreSQL's: find the holder, <c>lock_timeout</c>,
/// <c>idle_in_transaction_session_timeout</c>, <c>CONCURRENTLY</c> where the DDL has it. The SQL Server
/// lock advice's isolation-level remedy is named nowhere in this file; a pin holds that too.</para>
///
/// <para><b>Durability settings are not a lever here (D6).</b> The WAL waits' remedies are the WAL device,
/// <c>wal_buffers</c>, commit batching and the checkpoint ceiling — never <c>synchronous_commit</c>,
/// <c>fsync</c> or <c>full_page_writes</c>, which are posture and live only in the posture family.</para>
/// </summary>
public static partial class PgTargetAdvice
{
    /// <summary>The display name for each v1 wait key — a constant, as the collector stamps it in
    /// <see cref="Fact.ObjectName"/>, so the static (no-fact) block reads the same as the composed one.</summary>
    private static readonly Dictionary<string, string> s_waitDisplayNames = new(StringComparer.Ordinal)
    {
        [PgTargetFactKeys.WaitKey("Lock", null)] = "Lock",
        [PgTargetFactKeys.WaitKey("LWLock", null)] = "LWLock",
        [PgTargetFactKeys.WaitKey("IO", null)] = "IO",
        [PgTargetFactKeys.WaitKey("IPC", null)] = "IPC",
        [PgTargetFactKeys.WaitKey("Lock", "relation")] = "Lock:relation",
        [PgTargetFactKeys.WaitKey("LWLock", "WALWrite")] = "LWLock:WALWrite",
        [PgTargetFactKeys.WaitKey("IO", "DataFileRead")] = "IO:DataFileRead",
        [PgTargetFactKeys.WaitKey("IO", "WALSync")] = "IO:WALSync",
    };

    private static partial AdviceBlock? ComposeWait(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        if (!s_waitDisplayNames.TryGetValue(key, out var display))
            return null;

        var fact = KnobFact(factsByKey, key);
        var sampled = fact is not null && (KnobMeta(fact, PgTargetScorer.WaitIsSampledKey) ?? 0) > 0;

        var headline = fact is null
            ? $"{display} waits are a large share of this server's waiting"
            : sampled
                ? $"{display} waits — about {WaitBackendEquivalents(fact.Value)} of a backend, estimated from sampling ({KnobPct(KnobMeta(fact, PgTargetScorer.WaitShareOfWaitTimeKey) ?? 0)} of all waiting)"
                : $"{display} waits — {WaitBackendEquivalents(fact.Value)} of a backend continuously, {KnobPct(KnobMeta(fact, PgTargetScorer.WaitShareOfWaitTimeKey) ?? 0)} of all waiting";

        var investigation = new StringBuilder(960);
        AppendWaitEvidence(investigation, fact, display, sampled);
        investigation.Append(WaitMechanism(key, factsByKey));

        return new AdviceBlock(
            Headline: headline,
            Investigation: investigation.ToString().TrimEnd(),
            Remediation: WaitRemediation(key, fact, factsByKey));
    }

    /// <summary>
    /// The value-stated evidence, in the grade's own words. Nothing in either branch says the server
    /// "spent" time: the fraction is backend-time summed over tasks per second of wall clock.
    /// </summary>
    private static void AppendWaitEvidence(StringBuilder sb, Fact? fact, string display, bool sampled)
    {
        if (fact is null)
        {
            sb.Append("The measured wait time, the observed time it is a fraction of and its share of all waiting were not " +
                      "frozen into this finding; get_pg_wait_stats (Aurora) or get_pg_wait_sampling (stock, an estimate from " +
                      "sampling) over the same window carries them. ");
            return;
        }

        var waitMs = KnobMeta(fact, PgTargetScorer.WaitMsKey) ?? 0;
        var observedMs = KnobMeta(fact, PgTargetScorer.WaitSourceObservedMsKey) ?? 0;
        var collections = KnobMeta(fact, PgTargetScorer.WaitSampleCountKey) ?? 0;
        var share = KnobMeta(fact, PgTargetScorer.WaitShareOfWaitTimeKey) ?? 0;
        var typeShare = KnobMeta(fact, PgTargetScorer.WaitShareOfTypeKey);
        var witnessMs = KnobMeta(fact, PgTargetScorer.WaitObservedMsKey) ?? 0;

        if (sampled)
        {
            var samples = KnobMeta(fact, PgTargetScorer.WaitDeltaSamplesKey) ?? 0;
            var period = KnobMeta(fact, PgTargetScorer.WaitEstimateResolutionMsKey) ?? 0;
            var peakBackends = KnobMeta(fact, PgTargetScorer.WaitPeakBackendsKey) ?? 0;
            var intervalMs = KnobMeta(fact, PgTargetScorer.WaitSourceIntervalMsKey) ?? observedMs;
            var sampledMsKnown = (KnobMeta(fact, PgTargetScorer.WaitSampledMsKnownKey) ?? 0) > 0;
            sb.Append("Over ").Append(KnobSeconds(observedMs / 1000.0)).Append(" the sampler was watching (")
              .Append(KnobNum(collections)).Append(" collection intervals");
            /* The duty cycle, stated when the watched time and the wall time differ: the #3604 service sampler
               watches 30 s of each 300 s cycle and V133 stores what it watched, so the fraction is per second WATCHED
               — the honest rate — and a reader must not divide the same seconds by the wall interval again. */
            if (intervalMs > 0 && Math.Abs(intervalMs - observedMs) / intervalMs > 0.05)
                sb.Append(" spanning ").Append(KnobSeconds(intervalMs / 1000.0)).Append(" of wall clock — the sampler watches part of each cycle and the fraction is per second watched");
            sb.Append("), ").Append(display)
              .Append(" was seen in ").Append(KnobNum(samples)).Append(" backend-samples at a ").Append(KnobNum(period))
              .Append(" ms sampling period — about ").Append(KnobSeconds(waitMs / 1000.0))
              .Append(" of waiting, estimated from sampling, which is ").Append(WaitBackendEquivalents(fact.Value))
              .Append(" of one backend waiting on this continuously. ");
            if (!sampledMsKnown)
                sb.Append("Some or all of these collections did not record how long the sampler watched (rows written before V133, or the " +
                          "pg_wait_sampling extension, which watches the whole interval), so they are read over their whole interval; on " +
                          "the service-side sampler that reads about a tenth of the true rate. ");
            sb.Append("The estimate cannot see a wait shorter than ").Append(KnobNum(period))
              .Append(" ms and rounds a continuous wait to the nearest period per backend-sample, and the count is per BACKEND-sample — " +
                      "ten backends waiting one second is ten seconds of waiting — so this is time summed over tasks, not a share " +
                      "of the server's clock, and it can exceed one. ");
            if (peakBackends > 1)
                sb.Append("At most ").Append(KnobNum(peakBackends)).Append(" backends were seen waiting on it in one collection. ");
            var resets = KnobMeta(fact, PgTargetScorer.WaitCounterResetsKey) ?? 0;
            if (resets > 0)
                sb.Append("The sampling profile was reset ").Append(KnobNum(resets)).Append(" time(s) inside the window, so the count is a floor. ");
        }
        else
        {
            var count = KnobMeta(fact, PgTargetScorer.WaitCountKey) ?? 0;
            sb.Append("Over ").Append(KnobSeconds(observedMs / 1000.0)).Append(" of observed wait-counter time (")
              .Append(KnobNum(collections)).Append(" one-minute collections), the engine measured ")
              .Append(KnobSeconds(waitMs / 1000.0)).Append(" of ").Append(display).Append(" waiting across ")
              .Append(KnobNum(count)).Append(" completed waits — ").Append(WaitBackendEquivalents(fact.Value))
              .Append(" of one backend waiting on this continuously (time summed over tasks per second of wall clock; it can exceed one). ");
            var restarts = KnobMeta(fact, PgTargetScorer.WaitRestartCollectionsKey) ?? 0;
            if (restarts > 0)
                sb.Append(KnobNum(restarts)).Append(" collection(s) in the window were a restart or first sighting and contributed no sample. ");
        }

        sb.Append("That is ").Append(KnobPct(share)).Append(" of all waiting the profile holds for the window");
        if (typeShare is not null)
            sb.Append(" and ").Append(KnobPct(typeShare.Value)).Append(" of its own wait type");
        sb.Append(". ");

        /* On a sampled fact the wall time between collections is the figure to hold against the witness — the
           watched time is a fraction of it by design and has already been stated above. */
        var sourceWallMs = sampled ? (KnobMeta(fact, PgTargetScorer.WaitSourceIntervalMsKey) ?? observedMs) : observedMs;
        if (witnessMs > 0 && Math.Abs(witnessMs - sourceWallMs) / witnessMs > 0.05)
        {
            sb.Append("The pass's coverage witness (pg_database_stats) observed ").Append(KnobSeconds(witnessMs / 1000.0))
              .Append("; the fraction above is over the wait source's own ").Append(KnobSeconds(observedMs / 1000.0))
              .Append(" because that is the time the wait figure was summed across. ");
        }
    }

    /* ── lane 24 (#3691): the stock SAMPLED wait-profile anomaly ── */

    private static readonly AdviceBlock s_sampledWaitProfileStatic = new(
        Headline: "The server's sampled wait profile shifted well above its normal for this time of week (estimated from sampling)",
        Investigation:
            "The window's peak all-types wait rate — milliseconds of sampled waiting per second the sampler was watching, " +
            "estimated from sampling (pg_wait_sampling: Δ backend-samples × the sampling period, over each collection's " +
            "sampled_ms), CPU/Running excluded — judged against this server's hour-of-week baseline of the same estimate. " +
            "A profile shift says the server was seen waiting far more than it usually is at this hour; the named wait " +
            "types say on what. The estimate cannot see a wait shorter than the sampling period and counts per " +
            "backend-sample, so it is time summed over tasks, not a share of the clock." + s_anomalyHedge,
        Remediation:
            "get_pg_wait_sampling over this window shows which wait types and which statements drove the shift; chase the " +
            "dominant one with its own playbook (Lock — the holder via get_pg_blocking, lock_timeout, " +
            "idle_in_transaction_session_timeout; IO — the buffer-cache and checkpoint findings; LWLock:WALWrite — the WAL " +
            "device and commit batching). If the elevated profile persists across windows the threshold-based sampled wait " +
            "finding for the leading wait will fire and the standard advice applies.");

    /// <summary>
    /// The stock sampled wait-profile anomaly's block (<c>ANOMALY_PG_SAMPLED_WAIT_PROFILE</c>, lane 24): the Aurora
    /// composer's shape in the SAMPLED grade's own words — peak sampled ms per second watched, the multiple or the
    /// modified z, the window mean beside the peak (the #3653 peak-and-mean gate), the leading <c>contrib_Type:event</c>
    /// contributors, the duty cycle when the watched time and the wall time differ, and the pre-V133 caveat when
    /// <c>sampled_ms_known = 0</c> — or the first-occurrence rendering when <c>is_new</c>. Says "estimated from
    /// sampling"; never "the engine measured", never "spent". Static when the fact set does not carry the key.
    /// </summary>
    private static AdviceBlock ComposeSampledWaitAnomaly(IReadOnlyDictionary<string, Fact> factsByKey)
    {
        if (!factsByKey.TryGetValue(PgTargetFactKeys.AnomalySampledWaitProfile, out var fact)
            || !fact.Metadata.TryGetValue("current_ms_per_sec", out var current))
            return s_sampledWaitProfileStatic;

        var contributors = new List<(string Name, double Ms)>();
        foreach (var (metaKey, value) in fact.Metadata)
        {
            if (metaKey.StartsWith(PgTargetFactKeys.WaitContributorMetadataPrefix, StringComparison.Ordinal))
                contributors.Add((metaKey.Substring(PgTargetFactKeys.WaitContributorMetadataPrefix.Length), value));
        }
        contributors.Sort((a, b) => b.Ms != a.Ms ? b.Ms.CompareTo(a.Ms) : string.CompareOrdinal(a.Name, b.Name));
        var led = contributors.Count == 0 ? "the sampled wait types" : string.Join(", ", contributors.Take(3).Select(c => c.Name));
        var currentText = current.ToString("0.#", System.Globalization.CultureInfo.InvariantCulture);
        var meanText = (KnobMeta(fact, "mean_ms_per_sec") ?? 0).ToString("0.#", System.Globalization.CultureInfo.InvariantCulture);

        var instrument = new StringBuilder(320);
        var observedMs = KnobMeta(fact, PgTargetScorer.WaitSourceObservedMsKey) ?? 0;
        var intervalMs = KnobMeta(fact, PgTargetScorer.WaitSourceIntervalMsKey) ?? observedMs;
        if (observedMs > 0 && intervalMs > 0 && Math.Abs(intervalMs - observedMs) / intervalMs > 0.05)
        {
            instrument.Append(" The sampler watched ").Append(KnobSeconds(observedMs / 1000.0)).Append(" of the ")
                      .Append(KnobSeconds(intervalMs / 1000.0)).Append(" between collections, and the rate is per second watched.");
        }
        if ((KnobMeta(fact, PgTargetScorer.WaitSampledMsKnownKey) ?? 0) <= 0)
        {
            instrument.Append(" Some collections did not record how long the sampler watched (rows written before V133, or the " +
                              "pg_wait_sampling extension) and are read over their whole interval, which on the service-side sampler " +
                              "understates the rate about tenfold; the baseline was built the same way.");
        }

        if (fact.Metadata.GetValueOrDefault("is_new") >= 1.0)
        {
            return s_sampledWaitProfileStatic with
            {
                Headline = "The server's sampled wait profile is heavy, with no baseline yet for this time of week (estimated from sampling)",
                Investigation =
                    $"The all-types wait rate estimated from sampling peaked at about {currentText} ms of sampled waiting per second the " +
                    $"sampler was watching this window (mean {meanText} ms/sec; CPU/Running excluded), led by {led}. This server's " +
                    "hour-of-week sampled-wait baseline is too thin to trust a deviation against yet, so this fired on its absolute level " +
                    "— a first look at where the server is seen waiting, not a proven shift." + instrument + s_anomalyHedge,
            };
        }

        var modifiedZ = fact.Metadata.GetValueOrDefault("modified_z");
        var ratio = fact.Metadata.GetValueOrDefault("ratio");
        var mean = fact.Metadata.GetValueOrDefault("baseline_mean");
        var meanBaselineText = mean.ToString("0.#", System.Globalization.CultureInfo.InvariantCulture);
        var deviation = modifiedZ > 0
            ? $"{modifiedZ.ToString("0.#", System.Globalization.CultureInfo.InvariantCulture)} robust sigmas above its {meanBaselineText} ms/sec baseline"
            : $"about {ratio.ToString("0.#", System.Globalization.CultureInfo.InvariantCulture)}× its {meanBaselineText} ms/sec baseline";
        return s_sampledWaitProfileStatic with
        {
            Headline = modifiedZ > 0
                ? $"The server's sampled wait profile shifted to {currentText} ms/sec — {modifiedZ.ToString("0.#", System.Globalization.CultureInfo.InvariantCulture)}σ above its baseline for this time of week (estimated from sampling)"
                : $"The server's sampled wait profile shifted to about {ratio.ToString("0.#", System.Globalization.CultureInfo.InvariantCulture)}× its baseline for this time of week (estimated from sampling)",
            Investigation =
                $"The all-types wait rate estimated from sampling peaked at about {currentText} ms of sampled waiting per second the " +
                $"sampler was watching this window (mean {meanText} ms/sec, also above the bar; CPU/Running excluded) — {deviation} for " +
                $"this hour-of-week — led by {led}. This is a shift in the overall sampled wait profile, and the named contributors are " +
                "where to look." + instrument + s_anomalyHedge,
        };
    }

    /// <summary>"0.35 of a backend" reads better than "35 %" for a figure that can exceed one.</summary>
    private static string WaitBackendEquivalents(double fraction) =>
        fraction.ToString("0.##", System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>What the wait IS, per key, and the co-fire sentence when the mechanism the design names for it fired.</summary>
    private static string WaitMechanism(string key, IReadOnlyDictionary<string, Fact> facts)
    {
        var sb = new StringBuilder(640);

        if (key == PgTargetFactKeys.WaitKey("Lock", null))
        {
            sb.Append("Lock is the heavyweight-lock class: relation, transactionid, tuple, extend, advisory and the rest. PostgreSQL " +
                      "grants them in arrival order behind whatever conflicting mode is already held, and by default a waiter waits " +
                      "forever (lock_timeout = 0) — so a Lock fraction is a queue, and the queue's head is one session holding something " +
                      "the others need. Because of MVCC, plain readers and writers never queue on each other here; what does is DDL, " +
                      "explicit LOCK TABLE, TRUNCATE, and row-level conflicts between writers on the same rows. ");
            AppendCoFire(sb, facts, PgTargetFactKeys.ConnectionSaturation,
                "PG_CONNECTION_SATURATION co-fired: sessions are at the connection ceiling, and a saturated pool is where parked or " +
                "queued sessions hold locks the rest are waiting for. ");
        }
        else if (key == PgTargetFactKeys.WaitKey("Lock", "relation"))
        {
            sb.Append("Lock:relation is a wait for a lock on a whole table or index. Under MVCC a SELECT never blocks an UPDATE and " +
                      "an UPDATE never blocks a SELECT, so a relation-lock queue is not readers versus writers: it is a session " +
                      "holding a mode the waiters conflict with — ALTER TABLE, an index build without CONCURRENTLY, REINDEX, VACUUM FULL, " +
                      "CLUSTER, TRUNCATE, LOCK TABLE, or an autovacuum that is truncating — or an ACCESS EXCLUSIVE request queued " +
                      "behind ordinary traffic and stacking every later statement behind itself. The holder is very often a " +
                      "transaction that finished its work and stayed open (idle in transaction). ");
            AppendCoFire(sb, facts, PgTargetFactKeys.ConnectionSaturation,
                "PG_CONNECTION_SATURATION co-fired: the connection ceiling is in reach, and the parked sessions consuming slots are " +
                "the usual relation-lock holders. ");
        }
        else if (key == PgTargetFactKeys.WaitKey("LWLock", null))
        {
            sb.Append("LWLock is the lightweight-lock class: latches on shared-memory structures (WAL insert and write, the buffer " +
                      "mapping table, buffer content, the lock manager's partitions). Each is held for microseconds by design, so a " +
                      "sustained LWLock fraction is many backends contending for the same structure at once — the WAL, one hot page, " +
                      "or the lock manager when a transaction touches more relations than its sixteen fast-path slots (partitioned " +
                      "tables, wide joins). The named standout in this window, if one fired, says which. ");
        }
        else if (key == PgTargetFactKeys.WaitKey("LWLock", "WALWrite"))
        {
            sb.Append("LWLock:WALWrite is a backend waiting for the WAL to be written to the operating system: every commit, and every " +
                      "backend whose WAL buffers filled, queues here behind whichever process holds the write lock while it pushes " +
                      "WAL to the device. It rises with commit rate, with WAL volume per transaction (full-page images after a " +
                      "checkpoint, unlogged-to-logged conversions, bulk loads) and with the latency of the WAL volume. ");
            AppendCoFire(sb, facts, PgTargetFactKeys.CheckpointPressure,
                "PG_CHECKPOINT_PRESSURE co-fired: WAL volume is forcing checkpoints ahead of checkpoint_timeout, and each forced " +
                "checkpoint re-arms full-page images that inflate the WAL every commit is queued behind. ");
        }
        else if (key == PgTargetFactKeys.WaitKey("IO", null))
        {
            sb.Append("IO is the class of waits on the file system: data file reads and writes, WAL writes and syncs, temp file I/O, " +
                      "SLRU pages. Some IO waiting is what a database does; the fraction is a finding when it is a sustained multiple " +
                      "of a backend, and the named standout in this window (data file reads, or WAL sync) says which side of the " +
                      "storage the time is on. ");
        }
        else if (key == PgTargetFactKeys.WaitKey("IO", "DataFileRead"))
        {
            sb.Append("IO:DataFileRead is a backend waiting for a heap or index page to arrive from the file system — a shared_buffers " +
                      "miss, served either from the operating system's page cache (fast, but still a system call and a copy) or from " +
                      "the device. It is the cost of a working set larger than the buffer cache, of a sequential scan over a table " +
                      "the cache does not hold, or of an index that is not there. ");
            AppendCoFire(sb, facts, PgTargetFactKeys.BufferCachePressure,
                "PG_BUFFER_CACHE_PRESSURE co-fired: the buffer cache is measurably short for this working set, so these reads are " +
                "misses the cache would otherwise have absorbed. ");
        }
        else if (key == PgTargetFactKeys.WaitKey("IO", "WALSync"))
        {
            sb.Append("IO:WALSync is a backend waiting for fsync on a WAL segment — the durability cost paid at commit, by the committing " +
                      "backend or by the WAL writer on its behalf. It is bounded below by the WAL device's sync latency and scales " +
                      "with commits per second and with WAL bytes per commit. ");
            AppendCoFire(sb, facts, PgTargetFactKeys.CheckpointPressure,
                "PG_CHECKPOINT_PRESSURE co-fired: WAL is reaching max_wal_size before checkpoint_timeout, and the full-page images " +
                "each forced checkpoint re-arms are more bytes to sync per commit. ");
        }
        else if (key == PgTargetFactKeys.WaitKey("IPC", null))
        {
            sb.Append("IPC is backends waiting on other backends: parallel workers and their leader exchanging tuples, a leader waiting " +
                      "for workers to start or finish, synchronous replication waiting for a standby's acknowledgement, logical " +
                      "replication and NOTIFY hand-offs, and the checkpointer signalling. A sustained IPC fraction is coordination " +
                      "cost — parallelism that does not pay for itself, or a standby that is slow to confirm. ");
        }

        return sb.ToString();
    }

    private static void AppendCoFire(StringBuilder sb, IReadOnlyDictionary<string, Fact> facts, string key, string sentence)
    {
        if (facts.TryGetValue(key, out var other) && other.BaseSeverity > 0)
            sb.Append(sentence);
    }

    /// <summary>The levers, per key, each with its counter-objective; sampled facts are told what the estimate cannot show.</summary>
    private static string WaitRemediation(string key, Fact? fact, IReadOnlyDictionary<string, Fact> facts)
    {
        var sb = new StringBuilder(960);
        var sampled = fact is not null && (KnobMeta(fact, PgTargetScorer.WaitIsSampledKey) ?? 0) > 0;

        if (key == PgTargetFactKeys.WaitKey("Lock", null) || key == PgTargetFactKeys.WaitKey("Lock", "relation"))
        {
            sb.Append("Find the holder first: get_pg_blocking shows the blocked/blocking pairs the collector caught with both sides' " +
                      "statements, and get_pg_lock_stats shows which relation and which mode the queue is on. If the holder is idle in " +
                      "transaction, set idle_in_transaction_session_timeout so a parked transaction is terminated and releases what it " +
                      "holds (counter-objective: an application that legitimately pauses mid-transaction loses that transaction and must " +
                      "retry). Set lock_timeout for the application role, or per statement for DDL, so a request that cannot get its " +
                      "lock fails fast instead of stacking every later statement behind itself (counter-objective: the statement errors " +
                      "and the caller owns the retry). ");
            if (key == PgTargetFactKeys.WaitKey("Lock", "relation"))
            {
                sb.Append("Run schema changes with the CONCURRENTLY form where it exists (index builds, index drops, REINDEX) and the " +
                          "rest inside a lock_timeout-guarded retry loop in a quiet window; never VACUUM FULL or CLUSTER a live table " +
                          "as a routine (counter-objective: CONCURRENTLY builds take longer and cannot run inside a transaction block). " +
                          "Do not reach for a reader/writer isolation change — PostgreSQL already has MVCC; this queue is a held lock, " +
                          "not a read blocking a write. ");
            }
            AppendCoFire(sb, facts, PgTargetFactKeys.ConnectionSaturation,
                "Resolve PG_CONNECTION_SATURATION alongside this: a pool at its ceiling keeps the parked holders alive. ");
        }
        else if (key == PgTargetFactKeys.WaitKey("LWLock", null))
        {
            sb.Append("Read the named LWLock standouts for this window (get_pg_wait_stats or get_pg_wait_sampling, by event): WALWrite " +
                      "and WALInsert point at the WAL path; BufferMapping and BufferContent at one hot page or a buffer cache too small " +
                      "for the working set (size shared_buffers to the working set — counter-objective: memory taken from the page cache " +
                      "and from work_mem); LockManager at transactions touching more than sixteen relations, where the fix is fewer " +
                      "partitions touched per statement or fewer relations per transaction (counter-objective: a schema change, not a " +
                      "setting). No setting lowers LWLock waits directly. ");
        }
        else if (key == PgTargetFactKeys.WaitKey("LWLock", "WALWrite") || key == PgTargetFactKeys.WaitKey("IO", "WALSync"))
        {
            sb.Append("Measure the WAL volume's write and sync latency first — this wait is bounded below by the device. Then reduce WAL " +
                      "bytes per commit: commit in batches where the application allows (counter-objective: a failure loses the batch), " +
                      "avoid full-page-image storms by raising the checkpoint ceiling so checkpoints run on the clock (max_wal_size — " +
                      "counter-objective: pg_wal footprint and crash-recovery replay length), and enable wal_compression when the WAL is " +
                      "full-page-image heavy (counter-objective: CPU per WAL record on the backends writing it). wal_buffers above its " +
                      "auto-tuned default helps only when many backends fill it between writes. Durability settings are not a lever " +
                      "here: a commit that waits for its WAL to reach the device is doing what it promised. ");
            AppendCoFire(sb, facts, PgTargetFactKeys.CheckpointPressure,
                "Resolve PG_CHECKPOINT_PRESSURE first — the requested checkpoints are the WAL inflation this wait is paying for. ");
        }
        else if (key == PgTargetFactKeys.WaitKey("IO", null))
        {
            sb.Append("Split the class by event (get_pg_wait_stats or get_pg_wait_sampling): DataFileRead is the buffer cache and the " +
                      "working set; WALSync and WALWrite are the WAL device; BufferFileRead/Write and temp events are work_mem spills. " +
                      "Each has its own lever and none is a general I/O setting. Confirm device latency with get_pg_io_stats (PG 16+) " +
                      "before buying storage. ");
        }
        else if (key == PgTargetFactKeys.WaitKey("IO", "DataFileRead"))
        {
            sb.Append("Check whether the reads are misses the cache should have absorbed: get_pg_database_stats for the hit ratio, " +
                      "get_pg_buffer_usage for what the cache holds. If shared_buffers is at the initdb default, size it to the working " +
                      "set (counter-objective: memory taken from the operating system's page cache, which serves the misses today, and " +
                      "from work_mem). Then look at the statements doing the reading — get_pg_top_queries by shared blocks read — for a " +
                      "sequential scan a predicate could avoid; this advice states the evidence and does not write index DDL. ");
            AppendCoFire(sb, facts, PgTargetFactKeys.BufferCachePressure,
                "Resolve PG_BUFFER_CACHE_PRESSURE alongside this: the same cache shortage is behind both facts. ");
        }
        else if (key == PgTargetFactKeys.WaitKey("IPC", null))
        {
            sb.Append("Name the event (get_pg_wait_stats or get_pg_wait_sampling): ParallelFinish / ExecuteGather / MessageQueue* are " +
                      "parallel query — lower max_parallel_workers_per_gather or raise parallel_setup_cost for the statements that do " +
                      "not gain from workers (counter-objective: the statements that did gain run longer); SyncRep is synchronous " +
                      "replication waiting on the standby — a standby or network latency problem, not a primary setting (changing the " +
                      "replication level is a durability posture, not a lever here). ");
        }

        if (sampled)
        {
            sb.Append("This figure is estimated from sampling: it cannot see waits shorter than the sampling period and is a floor, " +
                      "not a measurement. Confirm the picture with the query attribution get_pg_wait_sampling carries before acting on " +
                      "one event's share.");
        }
        else
        {
            sb.Append("Confirm the wait's trend over a longer window with get_pg_wait_stats before treating one window's fraction as " +
                      "the server's steady state.");
        }

        return sb.ToString();
    }
}
