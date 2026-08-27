// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The first PostgreSQL time series (#2663). The service shipped fourteen trend reads and none worked on a
/// PostgreSQL target, so every PostgreSQL answer described one window and none answered "is this getting
/// worse".
/// </summary>
public sealed class PgTrendReaderTests
{
    /// <summary>
    /// A trend differences CONSECUTIVE snapshots. That is the whole difference from the window reads next
    /// door, which want one number and take newest-minus-oldest — taking the window's ends here would
    /// produce a single point and call it a shape.
    /// </summary>
    [Fact]
    public void TheWaitTrendDifferencesConsecutiveSnapshots()
    {
        var sql = DarlingPgTrendReader.WaitTrendSql;

        Assert.Contains("LAG(samples) OVER (ORDER BY collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE prev_samples IS NOT NULL", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// A counter going backwards is a RESET, not negative work.
    /// <c>pg_wait_sampling_reset_profile()</c> and a server restart both zero the profile, and the second
    /// happens without anyone deciding to. <c>GREATEST(delta, 0)</c> would report a QUIET interval across a
    /// restart — the one reading that is definitely wrong, because the server was not idle. The interval
    /// takes the new value whole, which is everything since the reset.
    /// </summary>
    [Fact]
    public void AWaitCounterGoingBackwardsIsAResetRatherThanClampedToZero()
    {
        var sql = DarlingPgTrendReader.WaitTrendSql;

        Assert.Contains("CASE WHEN samples < prev_samples THEN samples ELSE samples - prev_samples END", sql, StringComparison.Ordinal);
        Assert.Contains("(samples < prev_samples)            AS counter_reset", sql, StringComparison.Ordinal);

        /* The clamp this read must NOT use on the sample counter. It is right for a per-interval difference
           that cannot legitimately go negative and wrong here, where the negative IS the signal. */
        Assert.DoesNotContain("GREATEST(samples", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Per SECOND, because collection intervals are not uniform: a restart or a slow cycle stretches one,
    /// and a per-interval total renders that as a spike in the data rather than in the server.
    /// </summary>
    [Fact]
    public void TheWaitTrendNormalisesByTheIntervalLength()
    {
        var sql = DarlingPgTrendReader.WaitTrendSql;

        Assert.Contains("interval_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("/ interval_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("estimated_wait_ms_per_second", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The query trend reads the <c>delta_*</c> columns the collector already wrote rather than
    /// differencing the cumulative ones again.
    ///
    /// <para>They are not equivalent. The collector's delta spans the interval it actually OBSERVED, while a
    /// <c>LAG</c> here spans the gap in the STORED data — so whenever a snapshot is missing the two give
    /// different answers, and only the collector's is about the server.</para>
    /// </summary>
    [Fact]
    public void TheQueryTrendUsesTheCollectorsDeltas_NotItsOwn()
    {
        var sql = DarlingPgTrendReader.QueryDurationTrendSql;

        Assert.Contains("SUM(delta_calls)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_total_exec_time_ms)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LAG(total_exec_time_ms)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// A mean over no calls is ABSENT, not fast. Returning 0 would draw the line through the floor of the
    /// chart at exactly the intervals the statement was idle, which reads as the query getting faster.
    /// </summary>
    [Fact]
    public void AnIntervalWithNoCallsHasANullMeanRatherThanZero()
    {
        var sql = DarlingPgTrendReader.QueryDurationTrendSql;

        Assert.Contains("ELSE NULL", sql, StringComparison.Ordinal);
        Assert.Contains("WHEN coalesce(calls, 0) > 0", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The automatic choice must skip the CPU class. <c>pg_wait_sampling</c>'s <c>Running</c> means the
    /// backend was NOT waiting and dominates any healthy server's profile — measured on the rig it grew by
    /// 1,534 samples against 194 for the next event — so defaulting to it answers the opposite of the
    /// question a wait trend asks. It stays askable by name, because CPU time is a real signal; it is just
    /// never the automatic answer.
    /// </summary>
    [Fact]
    public void TheAutomaticWaitChoiceExcludesTheNotWaitingClass()
    {
        var sql = DarlingPgTrendReader.DominantWaitEventSql;

        Assert.Contains("coalesce(event_type, '') <> 'CPU'", sql, StringComparison.Ordinal);

        /* Ranked on the DIFFERENCE across the window. The profile is cumulative, so ranking it raw returns
           whichever event has accumulated longest since startup — a fact about uptime, not about now. */
        Assert.Contains("ORDER BY (hi - lo) DESC", sql, StringComparison.Ordinal);

        /* The CPU exclusion belongs to the CHOICE only: naming the event explicitly must still follow it. */
        Assert.DoesNotContain("'CPU'", DarlingPgTrendReader.WaitTrendSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The automatic query choice ranks on time actually spent in the window, and skips statements that
    /// recorded none — a queryid present in every snapshot with zero calls is not "the busiest statement".
    /// </summary>
    [Fact]
    public void TheAutomaticQueryChoiceRanksOnTimeSpentInTheWindow()
    {
        var sql = DarlingPgTrendReader.TopQueryIdSql;

        Assert.Contains("HAVING SUM(delta_total_exec_time_ms) > 0", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(delta_total_exec_time_ms) DESC", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The I/O and database trends difference with <c>LAG</c> and NOT with collector-written deltas — the
    /// opposite call from the query trend above, and correct for the opposite reason: neither
    /// <c>pg_io_stats</c> nor <c>pg_database_stats</c> stores a <c>delta_*</c> column. Checked against the
    /// stored schema rather than assumed, and pinned here so "make these consistent with the query trend"
    /// cannot be done by reflex against columns that do not exist.
    /// </summary>
    [Fact]
    public void TheIoAndDatabaseTrendsDifferenceWithLag_BecauseNeitherTableStoresDeltas()
    {
        var io = Code(DarlingPgTrendReader.IoTrendSql);
        var db = Code(DarlingPgTrendReader.DatabaseTrendSql);

        Assert.Contains("LAG(reads)", io, StringComparison.Ordinal);
        Assert.Contains("LAG(blks_hit)", db, StringComparison.Ordinal);

        Assert.DoesNotContain("delta_", io, StringComparison.Ordinal);
        Assert.DoesNotContain("delta_", db, StringComparison.Ordinal);
    }

    /// <summary>
    /// An I/O counter below its predecessor is a RESET, and the interval takes the new value whole.
    ///
    /// <para><c>GREATEST(delta, 0)</c> — which the single-window I/O read uses, correctly, because it wants
    /// one total for the window — would report a QUIET interval across a
    /// <c>pg_stat_reset_shared('io')</c> or a restart. That is the one reading that is definitely wrong,
    /// because the server was not idle. Proven against controlled rows: a series rewinding to 30 reports
    /// 30, not the 0 clamping gives and not a negative.</para>
    /// </summary>
    [Fact]
    public void AnIoCounterGoingBackwardsIsAResetRatherThanClampedToZero()
    {
        var sql = Code(DarlingPgTrendReader.IoTrendSql);

        Assert.Contains("CASE WHEN raw_reads         < 0 THEN reads         ELSE raw_reads         END", sql, StringComparison.Ordinal);
        Assert.Contains("AS counter_reset", sql, StringComparison.Ordinal);

        /* The clamp belongs to the single-window read and to the subject CHOICE, never to a reported
           interval. If this ever appears here, a restart has become a quiet minute. */
        Assert.DoesNotContain("GREATEST(reads", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GREATEST(writes", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The same rule on the database trend, with BOTH reset signals, because neither one sees every reset.
    ///
    /// <para>The explicit <c>stats_reset</c> move is the only thing that can see a reset followed by enough
    /// activity to climb back PAST the old value inside one interval — every difference is positive there
    /// and the arithmetic sees an ordinary busy minute. The implicit rewind is the only thing that catches a
    /// crash restart, where <c>stats_reset</c> can stay NULL through a genuine loss. And the first-sample
    /// guard is <c>ROW_NUMBER() OVER series &gt; 1</c>, not a test on <c>LAG(stats_reset)</c>: LAG is NULL
    /// both when there is no previous row and when the previous row's own <c>stats_reset</c> was NULL, and
    /// the second is the COMMON state, so a LAG guard misses a server's first reset entirely.</para>
    /// </summary>
    [Fact]
    public void TheDatabaseTrendCarriesBothResetSignals_AndGuardsOnRowNumber()
    {
        var sql = Code(DarlingPgTrendReader.DatabaseTrendSql);

        Assert.Contains("CASE WHEN raw_xact_commit   < 0 THEN xact_commit   ELSE raw_xact_commit   END", sql, StringComparison.Ordinal);
        Assert.Contains("ROW_NUMBER() OVER series > 1", sql, StringComparison.Ordinal);
        Assert.Contains("stats_reset IS DISTINCT FROM LAG(stats_reset) OVER series", sql, StringComparison.Ordinal);
        Assert.Contains("LEAST(raw_xact_commit", sql, StringComparison.Ordinal);

        Assert.DoesNotContain("GREATEST(raw_", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both new trends normalise per SECOND by the interval's own length. Collection cadence is not
    /// uniform — measured at 60 s and 75 s within one hour on the verification rig — so a per-interval
    /// total renders a slow sweep as a spike in the data rather than in the server.
    /// </summary>
    [Fact]
    public void BothNewTrendsNormaliseByTheIntervalLength()
    {
        Assert.Contains("/ interval_seconds", Code(DarlingPgTrendReader.IoTrendSql), StringComparison.Ordinal);
        Assert.Contains("reads_per_second", Code(DarlingPgTrendReader.IoTrendSql), StringComparison.Ordinal);

        Assert.Contains("/ interval_seconds", Code(DarlingPgTrendReader.DatabaseTrendSql), StringComparison.Ordinal);
        Assert.Contains("transactions_per_second", Code(DarlingPgTrendReader.DatabaseTrendSql), StringComparison.Ordinal);
    }

    /// <summary>
    /// The automatic I/O subject ranks on OPERATIONS, never on read time.
    ///
    /// <para>This is the pin that matters most here, because ranking on read time is the reasonable-looking
    /// change: it is what the single-window read orders by, and it is the better ranking when it is
    /// populated. It usually is not. <c>track_io_timing</c> is <b>off by default</b> in PostgreSQL and was
    /// off on both verification-rig targets, so every <c>read_time_ms</c> in the store is 0.0 and a ranking
    /// over it resolves to the tiebreak — which for a subject CHOICE means picking a name alphabetically
    /// and calling it the busiest thing on the server.</para>
    ///
    /// <para>Unlike the wait choice, nothing is EXCLUDED: <c>pg_stat_io</c> has no counterpart to
    /// <c>pg_wait_sampling</c>'s not-waiting class, so every combination in it is real device work and a
    /// checkpointer flushing hard is a finding rather than noise.</para>
    /// </summary>
    [Fact]
    public void TheAutomaticIoChoiceRanksOnOperations_NotOnUnmeasuredReadTime()
    {
        var sql = Code(DarlingPgTrendReader.DominantIoSubjectSql);

        Assert.Contains("SUM(d_reads), 0) + coalesce(SUM(d_writes), 0) + coalesce(SUM(d_extends), 0) DESC", sql, StringComparison.Ordinal);

        /* Any appearance of the timing columns in the ranking is the regression this exists to catch. */
        Assert.DoesNotContain("read_time_ms", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("write_time_ms", sql, StringComparison.Ordinal);

        /* Buffer hits QUALIFY a pair and break the tie ahead of the name, but never outrank operations.
           Found by running a quieter window: a fully cached workload has hits and no physical I/O at all,
           and filtering on operations alone answered "nothing to follow" for a healthy server. Without the
           hits tiebreak the same window falls through to alphabetical order, which is the very defect
           ranking on an unmeasured read time would have caused. */
        Assert.Contains("+ coalesce(SUM(d_hits), 0) > 0", sql, StringComparison.Ordinal);
        Assert.Contains("coalesce(SUM(d_hits), 0) DESC,", sql, StringComparison.Ordinal);

        /* The clamp is legitimate HERE and only here: this ranks a window rather than reporting an
           interval, so bounding a reset's contribution beats letting one restart hand over the default. */
        Assert.Contains("GREATEST(reads", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The I/O subject is a PAIR, and either half can be named alone.
    ///
    /// <para>A pair because a hit ratio summed across contexts is meaningless: <c>bulkread</c> is a
    /// sequential scan deliberately using a ring buffer so it cannot evict the pool, and averaging its
    /// misses with the normal context's understates both when the two have opposite remedies. Either half
    /// alone constrains the choice rather than being dropped — accepting a backend type and quietly
    /// answering about a different one is the failure this shape prevents.</para>
    /// </summary>
    [Fact]
    public void TheIoSubjectIsAPair_AndEitherHalfCanConstrainTheChoice()
    {
        Assert.Contains("AND   backend_type = $2", Code(DarlingPgTrendReader.IoTrendSql), StringComparison.Ordinal);
        Assert.Contains("AND   context = $3", Code(DarlingPgTrendReader.IoTrendSql), StringComparison.Ordinal);

        var choice = Code(DarlingPgTrendReader.DominantIoSubjectSql);

        /* Spelled inline with explicit casts rather than composed into the string: a read whose text only
           becomes valid after a substitution cannot pass the parse-analysis pin, and the null needs a type
           a prepared statement can infer. */
        Assert.Contains("($4::text IS NULL OR backend_type = $4)", choice, StringComparison.Ordinal);
        Assert.Contains("($5::text IS NULL OR context = $5)", choice, StringComparison.Ordinal);
    }

    /// <summary>
    /// The database trend can follow PostgreSQL's NULL-<c>datname</c> row.
    ///
    /// <para>That row is shared-relation activity — the cluster-wide catalog, which belongs to no database —
    /// and its NULL is a real value rather than missing data. An equality test would make it the one series
    /// this read could never follow, while the single-window read next door reports it.</para>
    /// </summary>
    [Fact]
    public void TheDatabaseTrendCanFollowTheSharedRelationsRow()
    {
        var sql = Code(DarlingPgTrendReader.DatabaseTrendSql);

        Assert.Contains("database_name IS NOT DISTINCT FROM $2", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("database_name = $2", sql, StringComparison.Ordinal);

        /* Excluded from the automatic CHOICE only: the catalog is never the database in trouble. */
        Assert.Contains("database_name IS NOT NULL", Code(DarlingPgTrendReader.TopDatabaseSql), StringComparison.Ordinal);
    }

    /// <summary>
    /// The pre-18 and 18+ byte answers are never mixed, and a pre-18 zero is never reported as a
    /// measurement.
    ///
    /// <para>18 removed <c>op_bytes</c> and replaced it with measured totals. They are a different quantity,
    /// not a rename: <c>op_bytes</c> was the per-operation BLOCK SIZE, and 18 moves several blocks per
    /// operation, so deriving bytes there would undercount. The read takes the measured columns where they
    /// exist, derives from the block size where they do not, and reports which — never a silent swap.</para>
    /// </summary>
    [Fact]
    public void TheIoTrendKeepsTheMeasuredAndDerivedByteAnswersApart()
    {
        var sql = Code(DarlingPgTrendReader.IoTrendSql);

        Assert.Contains("(read_bytes IS NOT NULL) AS bytes_measured", sql, StringComparison.Ordinal);
        Assert.Contains("(op_bytes IS NOT NULL)   AS bytes_estimable", sql, StringComparison.Ordinal);
        Assert.Contains("WHEN read_bytes IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("WHEN op_bytes IS NOT NULL", sql, StringComparison.Ordinal);

        /* extend_bytes must never be the probe: a WAL row legitimately reports none, so it would answer
           "not measured" for a row that simply does not extend. */
        Assert.DoesNotContain("(extend_bytes IS NOT NULL) AS bytes_measured", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The I/O trend asks the target's OWN <c>track_io_timing</c> before reporting a latency, bounded by
    /// the window's end.
    ///
    /// <para>A zero <c>read_time_ms</c> permits two readings — the disk is instant, and nobody is timing it
    /// — and only one of them is ever true. <c>read_time_ms / reads</c> over an untimed server is 0.000 ms,
    /// which reads as an impossibly fast disk. Bounded by the window end rather than taking the newest value
    /// outright so an <c>as_of</c> read of last week is told what the setting was THEN.</para>
    /// </summary>
    [Fact]
    public void TheIoTrendReadsTheTargetsOwnTrackIoTimingSetting()
    {
        var sql = DarlingPgTrendReader.IoTimingSettingSql;

        Assert.Contains("FROM pg_server_config", sql, StringComparison.Ordinal);
        Assert.Contains("name = 'track_io_timing'", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $2", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// An interval with NO activity is still a point on both new trends.
    ///
    /// <para>The single-window reads carry a <c>HAVING</c> that drops anything which did not move, and that
    /// is right there: an all-zero row is noise in a ranked grid. In a time series it is a reading — the
    /// quiet stretch either side of a spike is what makes the spike legible — so a <c>HAVING</c> copied
    /// across from the window read would delete exactly the shape a trend exists to show.</para>
    /// </summary>
    [Fact]
    public void AQuietIntervalIsStillAPoint()
    {
        Assert.DoesNotContain("HAVING", Code(DarlingPgTrendReader.IoTrendSql), StringComparison.Ordinal);
        Assert.DoesNotContain("HAVING", Code(DarlingPgTrendReader.DatabaseTrendSql), StringComparison.Ordinal);

        /* Non-vacuous: the same check must FIRE on a read that legitimately has one, or it is a check that
           would pass against anything. */
        Assert.Contains("HAVING", Code(DarlingPgTrendReader.DominantIoSubjectSql), StringComparison.Ordinal);
    }

    /// <summary>
    /// The query with its block comments stripped.
    ///
    /// <para><b>Every "must not contain" assertion above runs against this rather than the raw text</b>,
    /// and the reason is a defect this change made twice before catching it: a pin went red against
    /// CORRECT SQL because the comment explaining a decision quoted the very string the pin forbade — the
    /// one saying why there is no <c>HAVING</c>, and the one saying why the ranking is not on
    /// <c>read_time_ms</c>. Both comments are load-bearing and neither should be reworded to appease a
    /// test. A pin that fires on prose is a pin that gets deleted rather than believed, so the pin reads
    /// the code and the comments stay as written.</para>
    /// </summary>
    private static string Code(string sql) => BlockComment.Replace(sql, " ");

    private static readonly Regex BlockComment = new(@"/\*.*?\*/", RegexOptions.Singleline);
}
