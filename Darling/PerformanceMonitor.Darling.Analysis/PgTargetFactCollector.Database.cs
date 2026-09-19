/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// The per-database counters of the window, differenced per <c>database_name</c> series and summed —
    /// one row per database, spilled bytes first. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC),
    /// <c>$4</c> the window's midpoint (naive UTC) — the transactions and intervals of the SECOND half ride as two
    /// extra columns so <c>PG_TPS</c> can carry a trend (second-half rate minus first-half rate) for lane 3's
    /// offered-vs-delivered co-fire, without a second scan.
    ///
    /// <para><b>The differencing is <c>DarlingPgDatabaseReader.PgDatabaseSql</c>'s, verbatim</b>: per-series
    /// <c>LAG</c>, <c>GREATEST(raw, 0)</c>, and the explicit reset as <c>ROW_NUMBER() OVER series &gt; 1 AND
    /// stats_reset IS DISTINCT FROM LAG(stats_reset)</c>. The comment on that read records why the first
    /// sample is excluded by its ROW_NUMBER and not by <c>LAG(stats_reset) IS NOT NULL</c> — <c>stats_reset</c>
    /// is NULL until the first reset ever, so a database's FIRST reset moves it NULL → timestamp and a guard
    /// on the LAG evaluates false. That trap is not re-derived here; this is the fifth site of the shape
    /// (the reader, the fleet card's <c>FleetPgDeadlockSql</c>, the buffer composite's database arm, the
    /// write arm) and the five must agree on a server or the analysis contradicts the tool it sends a
    /// reader to. The implicit reset (<c>counter_rewind_count</c>, any selected counter below its
    /// predecessor) rides beside it for the crash-restart case the timestamp cannot see.</para>
    ///
    /// <para><b>The columns this family owns.</b> Transactions (<c>xact_commit</c> + <c>xact_rollback</c>),
    /// temp files and bytes, deadlocks. <c>blks_hit</c> / <c>blks_read</c> are NOT read here: the buffer
    /// composite (<c>PgTargetFactCollector.Buffer.cs</c>) differences them for its hit-ratio arm, and a second
    /// hit-ratio fact from this read would be the same condition counted twice (D2). <c>intervals</c> is the
    /// count of differences actually taken (<c>count(raw_deadlocks)</c> — NULL on the first sample of every
    /// series), and it is what makes a zero a MEASUREMENT: a series with one sample has had no difference
    /// taken, and its 0 is the arithmetic of an empty set, not an observation that nothing happened.</para>
    ///
    /// <para>No <c>HAVING</c> and no <c>LIMIT</c>, unlike the reader: the pass wants the server TOTAL, so every
    /// database's row is summed in the C#, and an idle database's zeros cost nothing. The NULL-named
    /// shared-relation row PostgreSQL emits is its own series under <c>PARTITION BY</c> (grouping semantics)
    /// and sums in.</para>
    /// </summary>
    public const string PgTargetDatabaseCountersSql = @"
WITH sampled AS (
    SELECT
        database_name,
        collection_time,
        xact_commit   - LAG(xact_commit)   OVER series AS raw_xact_commit,
        xact_rollback - LAG(xact_rollback) OVER series AS raw_xact_rollback,
        temp_files    - LAG(temp_files)    OVER series AS raw_temp_files,
        temp_bytes    - LAG(temp_bytes)    OVER series AS raw_temp_bytes,
        deadlocks     - LAG(deadlocks)     OVER series AS raw_deadlocks,
        (ROW_NUMBER() OVER series > 1
         AND stats_reset IS DISTINCT FROM LAG(stats_reset) OVER series) AS reset_here
    FROM pg_database_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    WINDOW series AS (
        PARTITION BY database_name
        ORDER BY collection_time
    )
)
SELECT
    database_name,
    CAST(coalesce(SUM(GREATEST(raw_xact_commit, 0)), 0) AS bigint)   AS xact_commit,
    CAST(coalesce(SUM(GREATEST(raw_xact_rollback, 0)), 0) AS bigint) AS xact_rollback,
    CAST(coalesce(SUM(GREATEST(raw_temp_files, 0)), 0) AS bigint)    AS temp_files,
    CAST(coalesce(SUM(GREATEST(raw_temp_bytes, 0)), 0) AS bigint)    AS temp_bytes,
    CAST(coalesce(SUM(GREATEST(raw_deadlocks, 0)), 0) AS bigint)     AS deadlocks,
    CAST(coalesce(SUM(GREATEST(raw_xact_commit, 0) + GREATEST(raw_xact_rollback, 0)) FILTER (WHERE collection_time > $4), 0) AS bigint) AS xact_second_half,
    CAST(count(raw_deadlocks) FILTER (WHERE collection_time > $4) AS integer) AS intervals_second_half,
    CAST(count(*) FILTER (WHERE reset_here) AS integer)              AS stats_reset_count,
    CAST(count(*) FILTER (WHERE LEAST(raw_xact_commit, raw_xact_rollback, raw_temp_files, raw_temp_bytes, raw_deadlocks) < 0) AS integer) AS counter_rewind_count,
    CAST(count(*) AS integer)                                        AS sample_count,
    CAST(count(raw_deadlocks) AS integer)                            AS intervals
FROM sampled
GROUP BY database_name
ORDER BY coalesce(SUM(GREATEST(raw_temp_bytes, 0)), 0) DESC";

    /// <summary>
    /// How many deadlock reports the LOG capture holds for the window — the exemplar count that rides beside
    /// the counter on <c>PG_DEADLOCK_RATE</c>, never the rate itself. <c>$1</c> server_id, <c>$2</c>/<c>$3</c>
    /// window (naive UTC). Bounded on <c>collection_time</c> (the indexed, chunk-partitioning column; the log
    /// tail runs every five minutes, so a report is "captured in the window" up to one cadence after it
    /// happened), and a plain <c>count(*)</c> because a captured graph IS an observation — the collector
    /// looked at the log and found this many — where the counter above is a difference.
    /// </summary>
    public const string PgTargetDeadlockExemplarCountSql = @"
SELECT CAST(count(*) AS integer) AS exemplar_count
FROM pg_deadlocks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

    /// <summary>
    /// The ONE <c>pg_database_stats</c> read of the pass, emitting three facts (filled by lane 6 — #3542 step 6,
    /// design §3.9; the plan first placed this read with lane 2, whose buffer partial took only the block
    /// counters): <c>PG_TPS</c>, <c>PG_DEADLOCK_RATE</c> and <c>PG_TEMP_SPILL</c>.
    /// <c>PG_HIT_RATIO</c> is declared in the vocabulary but NOT emitted — the hit ratio is one arm of
    /// <c>PG_BUFFER_CACHE_PRESSURE</c> (<c>PgTargetFactCollector.Buffer.cs</c>), and a second fact over the
    /// same two counters would double-count the condition the composite exists to collapse (D2).
    ///
    /// <para><b>Every rate is over OBSERVED time (#3538 A7).</b> Transactions per second, deadlocks per hour and
    /// spilled bytes per second all divide by <see cref="AnalysisContext.ObservedDurationMs"/>, never the
    /// nominal window, so a 24-hour read and a 4-hour read of the same server say the same thing. Nothing is
    /// emitted when the window has no observed time or no difference was taken (<c>intervals = 0</c> across
    /// every series — one sample per database, or none): a difference needs two rows, and a zero over none is
    /// not an observation.</para>
    ///
    /// <para><b>Flat counters are silence, per fact.</b> Each fact is emitted only when ITS counter moved in the
    /// window — a transaction committed or rolled back, a deadlock counted, a temp file written. This is the
    /// convention every family reading this series keeps (the buffer composite is "emitted whenever blocks
    /// moved"), and it is load-bearing: the plumbing e2e plants a day of flat counters and pins that the pass
    /// emits exactly the registry fact, and three sibling lanes' e2es pin their <c>total_facts</c> over the same
    /// flat series. A zero rate beside a fired deadlock alert is <c>get_pg_database_stats</c>' to state, with its
    /// interval count; here a fact that says "0 per hour" would be a claim the census forbids and a card no
    /// reader asked for.</para>
    ///
    /// <para><b><c>PG_TPS</c></b> is context (base 0): <c>xact_commit + xact_rollback</c> per observed second,
    /// with the rollback share stamped so the advice for a busier fact can say what kind of busy, and
    /// <c>tps_trend</c> — the second half of the window's rate minus the first half's, each over its own
    /// observed time — for lane 3's offered-vs-delivered co-fire (sessions climbing while throughput is flat or
    /// falling is queueing at the cliff, design §3.6 / D7). On any server doing work it is also this family's
    /// witness that the read ran: a pass with a <c>PG_TPS</c> and no <c>PG_TEMP_SPILL</c> means "nothing
    /// spilled", not "not collected".</para>
    ///
    /// <para><b><c>PG_DEADLOCK_RATE</c></b> is the COUNTER's deadlocks per observed hour — never a
    /// <c>pg_deadlocks</c> row count. The counter is complete (the engine counts every deadlock it detected,
    /// in every database, including ones no session the log parser sees was party to); the log tail is lossy
    /// on exactly the busiest servers (<c>CollectorScheduleDefaults</c>' ~14 KB/s note). The two disagree in
    /// public unless the fact says why, so <c>counter_count</c> and <c>exemplar_count</c> ride together and
    /// the advice says "the engine counted N; M were captured from the log". The exemplar read is a
    /// separate command behind its own degrade, run only when the counter moved: a store without the
    /// log-capture table (or one whose tail transport is not configured) loses the exemplar figure, never the
    /// rate.</para>
    ///
    /// <para><b><c>PG_TEMP_SPILL</c></b> is spilled bytes per observed second, emitted only when something
    /// spilled (<c>temp_files &gt; 0</c>) — "no spill" is honest absence and <c>PG_TPS</c> is the proof the read
    /// ran. <see cref="Fact.DatabaseName"/> names the database that spilled the most with its share stamped,
    /// so the advice can say where. <c>work_mem</c> and <c>max_connections</c> are read off the config facts
    /// the collector emitted a moment ago (emission order: Config before Database) and stamped as
    /// <c>work_mem_bytes</c> / <c>max_connections</c>, so the advice can do the overcommit arithmetic with the
    /// host's own numbers; absent when the config snapshot has not been collected, and the advice then says
    /// so rather than assuming 4 MB.</para>
    ///
    /// <para><b>The D5 co-fire is stamped, not inferred.</b> <c>CONFIG_PG_WORK_MEM</c> is emitted by the config
    /// read with source <c>pg_config</c>, so its BASE severity is <c>PgTargetScorer.ScoreConfigFact</c>'s,
    /// which sees one fact at a time; and <c>FactScorer.ScoreAll</c> never runs an amplifier over a base of 0.
    /// For the knob to reach the incident line only beside a spill, the spill evidence has to be ON the knob
    /// fact when its base is graded — so this method stamps <c>temp_spill_bytes_per_sec</c> onto the
    /// <c>CONFIG_PG_WORK_MEM</c> fact it finds in the list, and <c>PgTargetScorer.Temp.cs</c> grades the knob
    /// from that stamp against the same floor the spill fact is graded on. No stamp (no spill, or no config
    /// snapshot) means the knob's base stays 0 and it roots nothing, which is D5's "no spill evidence, no
    /// finding" made structural.</para>
    /// </summary>
    private async partial Task CollectDatabaseFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        if (context.ObservedDurationMs <= 0) return;

        long xactCommit = 0, xactRollback = 0, tempFiles = 0, tempBytes = 0, deadlocks = 0, xactSecondHalf = 0;
        int statsResetCount = 0, counterRewindCount = 0, sampleCount = 0, intervals = 0, intervalsSecondHalf = 0, databases = 0;
        string? topSpillDatabase = null;
        long topSpillBytes = 0;
        string? topDeadlockDatabase = null;
        long topDeadlocks = 0;

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetDatabaseCountersSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart + (context.TimeRangeEnd - context.TimeRangeStart) / 2));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                /* The shared-relation row has a NULL name; it is a real series and sums in, but it cannot be
                   "the database that spilled the most" by name. */
                var databaseName = reader.IsDBNull(0) ? null : reader.GetString(0);
                var rowCommit = ToInt64(reader.GetValue(1));
                var rowRollback = ToInt64(reader.GetValue(2));
                var rowTempFiles = ToInt64(reader.GetValue(3));
                var rowTempBytes = ToInt64(reader.GetValue(4));
                var rowDeadlocks = ToInt64(reader.GetValue(5));

                xactCommit += rowCommit;
                xactRollback += rowRollback;
                tempFiles += rowTempFiles;
                tempBytes += rowTempBytes;
                deadlocks += rowDeadlocks;
                xactSecondHalf += ToInt64(reader.GetValue(6));
                intervalsSecondHalf += Convert.ToInt32(reader.GetValue(7));
                statsResetCount += Convert.ToInt32(reader.GetValue(8));
                counterRewindCount += Convert.ToInt32(reader.GetValue(9));
                sampleCount += Convert.ToInt32(reader.GetValue(10));
                intervals += Convert.ToInt32(reader.GetValue(11));
                databases++;

                if (databaseName is not null && rowTempBytes > topSpillBytes)
                {
                    topSpillBytes = rowTempBytes;
                    topSpillDatabase = databaseName;
                }

                if (databaseName is not null && rowDeadlocks > topDeadlocks)
                {
                    topDeadlocks = rowDeadlocks;
                    topDeadlockDatabase = databaseName;
                }
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_database_stats is the coverage witness's own table, so a missing table has already failed the
               pass loudly upstream; what reaches here is a timeout or an unclassified fault. An abandonment is
               NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
            return;
        }

        /* A difference needs two samples; none taken means nothing measured (see the SQL's `intervals`). */
        if (intervals <= 0) return;

        var observedSeconds = context.ObservedDurationMs / 1000.0;
        var observedHours = observedSeconds / 3600.0;
        var transactions = xactCommit + xactRollback;

        if (transactions > 0)
        {
            var tps = new Fact
            {
                Source = PgTargetSources.DatabaseSource,
                Key = PgTargetFactKeys.Tps,
                Value = transactions / observedSeconds,
                ServerId = context.ServerId,
                Metadata =
                {
                    [PgTargetScorer.TpsCommitsKey] = xactCommit,
                    [PgTargetScorer.TpsRollbacksKey] = xactRollback,
                    [PgTargetScorer.TpsRollbackShareKey] = xactRollback / (double)transactions,
                    [PgTargetScorer.CounterDatabasesKey] = databases,
                    [PgTargetScorer.CounterSampleCountKey] = sampleCount,
                    [PgTargetScorer.CounterIntervalsKey] = intervals,
                    [PgTargetScorer.CounterStatsResetCountKey] = statsResetCount,
                    [PgTargetScorer.CounterRewindCountKey] = counterRewindCount,
                    [PgTargetScorer.CounterObservedMsKey] = context.ObservedDurationMs,
                },
            };

            /* The trend, for lane 3's offered-vs-delivered co-fire (sessions climbing while TPS is flat or
               falling): each half's rate over ITS OWN observed seconds, apportioned by the intervals that fell in
               it (one-minute cadence, so intervals ARE the observed minutes), so a half the collector missed is
               not read as a quiet half. Stamped only when both halves were observed — a trend over one half is
               not a trend. */
            var intervalsFirstHalf = intervals - intervalsSecondHalf;
            if (intervalsFirstHalf > 0 && intervalsSecondHalf > 0)
            {
                var tpsFirstHalf = (transactions - xactSecondHalf) / (observedSeconds * intervalsFirstHalf / intervals);
                var tpsSecondHalf = xactSecondHalf / (observedSeconds * intervalsSecondHalf / intervals);
                tps.Metadata[PgTargetScorer.TpsFirstHalfKey] = tpsFirstHalf;
                tps.Metadata[PgTargetScorer.TpsSecondHalfKey] = tpsSecondHalf;
                tps.Metadata[PgTargetScorer.TpsTrendKey] = tpsSecondHalf - tpsFirstHalf;
            }

            facts.Add(tps);
        }

        if (deadlocks > 0)
            await AddDeadlockRateFactAsync(context, facts, deadlocks, observedHours, topDeadlockDatabase, topDeadlocks,
                databases, sampleCount, intervals, statsResetCount, counterRewindCount);

        if (tempFiles > 0)
            AddTempSpillFact(context, facts, tempBytes, tempFiles, observedSeconds, topSpillDatabase, topSpillBytes,
                databases, sampleCount, intervals, statsResetCount, counterRewindCount);
    }

    private async Task AddDeadlockRateFactAsync(
        AnalysisContext context, List<Fact> facts, long deadlocks, double observedHours, string? topDeadlockDatabase, long topDeadlocks,
        int databases, int sampleCount, int intervals, int statsResetCount, int counterRewindCount)
    {
        var deadlockFact = new Fact
        {
            Source = PgTargetSources.DatabaseSource,
            Key = PgTargetFactKeys.DeadlockRate,
            Value = deadlocks / observedHours,
            ServerId = context.ServerId,
            DatabaseName = topDeadlockDatabase,
            Metadata =
            {
                [PgTargetScorer.DeadlockCounterCountKey] = deadlocks,
                [PgTargetScorer.DeadlocksPerHourKey] = deadlocks / observedHours,
                [PgTargetScorer.DeadlockObservedHoursKey] = observedHours,
                [PgTargetScorer.DeadlockTopDatabaseCountKey] = topDeadlocks,
                [PgTargetScorer.CounterDatabasesKey] = databases,
                [PgTargetScorer.CounterSampleCountKey] = sampleCount,
                [PgTargetScorer.CounterIntervalsKey] = intervals,
                [PgTargetScorer.CounterStatsResetCountKey] = statsResetCount,
                [PgTargetScorer.CounterRewindCountKey] = counterRewindCount,
                [PgTargetScorer.CounterObservedMsKey] = context.ObservedDurationMs,
            },
        };

        /* The exemplar count, behind its own degrade: the rate fact is emitted whether or not this read
           succeeds, and the advice says "the log capture was not read" when the key is absent. */
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(PgTargetDeadlockExemplarCountSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            var exemplars = await cmd.ExecuteScalarAsync(context.CancellationToken);
            if (exemplars is not null && exemplars is not DBNull)
                deadlockFact.Metadata[PgTargetScorer.DeadlockExemplarCountKey] = Convert.ToInt32(exemplars);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_deadlocks arrived with the log-capture rung; a pre-migration store raises 42P01 here, which
               the reporter classifies quiet. An abandonment is NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }

        facts.Add(deadlockFact);
    }

    private static void AddTempSpillFact(
        AnalysisContext context, List<Fact> facts, long tempBytes, long tempFiles, double observedSeconds, string? topSpillDatabase, long topSpillBytes,
        int databases, int sampleCount, int intervals, int statsResetCount, int counterRewindCount)
    {
        var bytesPerSec = tempBytes / observedSeconds;
        var spill = new Fact
        {
            Source = PgTargetSources.TempSource,
            Key = PgTargetFactKeys.TempSpill,
            Value = bytesPerSec,
            ServerId = context.ServerId,
            DatabaseName = topSpillDatabase,
            Metadata =
            {
                [PgTargetScorer.TempSpillBytesKey] = tempBytes,
                [PgTargetScorer.TempSpillFilesKey] = tempFiles,
                [PgTargetScorer.TempSpillBytesPerSecKey] = bytesPerSec,
                [PgTargetScorer.TempSpillFilesPerSecKey] = tempFiles / observedSeconds,
                [PgTargetScorer.TempSpillTopDatabaseShareKey] = tempBytes > 0 ? topSpillBytes / (double)tempBytes : 0,
                [PgTargetScorer.CounterDatabasesKey] = databases,
                [PgTargetScorer.CounterSampleCountKey] = sampleCount,
                [PgTargetScorer.CounterIntervalsKey] = intervals,
                [PgTargetScorer.CounterStatsResetCountKey] = statsResetCount,
                [PgTargetScorer.CounterRewindCountKey] = counterRewindCount,
                [PgTargetScorer.CounterObservedMsKey] = context.ObservedDurationMs,
            },
        };

        /* The knob and the ceiling, off the config facts emitted a moment ago (emission order: Config before
           Database). Absent when the snapshot has not been collected — the advice then says so. */
        var workMem = facts.Find(f => f.Key == PgTargetFactKeys.ConfigWorkMem);
        if (workMem is not null)
        {
            if (workMem.Metadata.TryGetValue("bytes", out var workMemBytes))
                spill.Metadata[PgTargetScorer.TempSpillWorkMemBytesKey] = workMemBytes;

            /* The D5 stamp — see the method summary: the knob's base is graded from this, one fact at a time. */
            workMem.Metadata[PgTargetScorer.WorkMemSpillBytesPerSecKey] = bytesPerSec;
        }

        var maxConnections = facts.Find(f => f.Key == PgTargetFactKeys.ConfigMaxConnections);
        if (maxConnections is not null)
            spill.Metadata[PgTargetScorer.TempSpillMaxConnectionsKey] = maxConnections.Value;

        facts.Add(spill);
    }
}
