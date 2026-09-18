/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Common
{
    /// <summary>
    /// Whether a daily-summary row's counts are a MEASUREMENT of that day or the shape retention left behind
    /// (#3541 A9).
    ///
    /// <para><b>The defect this names.</b> The daily aggregate's day spine is a UNION over nine sources, and
    /// each source is <c>LEFT JOIN</c>ed and <c>COALESCE</c>d to zero. The sources age out at DIFFERENT
    /// horizons: the per-signal collector tables at their 30-day default, the collection log at twice that,
    /// the alert log at ninety days. So for every day between the shortest horizon and the longest, the spine
    /// still has the day (the collection log or an alert still holds a row for it) while every signal the
    /// band reads — deadlocks, blocking, CPU, memory — has been purged, and the COALESCE renders each of them
    /// as a measured zero. Measured zeros band Healthy. A 366-day range therefore painted the purged months
    /// green, under a description that said a returned day meant collection happened and a quiet day was a
    /// healthy one.</para>
    ///
    /// <para>Shared by both SKUs so the two products, and the MCP tools and the calendar within each, make
    /// the SAME decision from the same two inputs: the day, and the store's retention horizon.</para>
    /// </summary>
    public enum DailySummaryDataState
    {
        /// <summary>The day is inside every signal's retention and the collection log recorded runs for
        /// it: the counts are measurements and the band is a verdict.</summary>
        Collected = 0,

        /// <summary>The day is BEFORE the store's retention horizon — the oldest day the shortest-lived
        /// signal source can still answer for — and NO signal source holds a row for it: the spine has the
        /// day from a longer-lived source alone (a run record, an alert). The signals the band reads are
        /// gone, so its zeros are absences and its band is <see cref="DailyHealthBand.NoData"/>.</summary>
        Purged = 1,

        /// <summary>The day is inside retention but has NO collector run recorded for it: the spine has the
        /// day from a signal row or an alert alone. Inside retention every zero is a measurement (the tables
        /// hold whatever the day had), so the band STANDS — an alert-only day is Warning, as it always was —
        /// and the state is a disclosure: the collection-error share has no denominator (the band's own arm
        /// then fails away from Healthy on a non-zero error count), and nothing records that the day was
        /// fully collected. Named for the fact, not for a claim: a deadlock row on such a day IS evidence of
        /// collection — what is missing is the record.</summary>
        NoRunRecord = 2,

        /// <summary>The day is before the retention horizon yet SOME signal source still holds rows for it —
        /// the purge has not reached it (held by the rollup-coverage gate, paused, or the boundary day), or
        /// the sources' retentions differ so one aged out ahead of the others. The rows that are there are
        /// real; the zeros beside them may be absences; the band cannot tell which, so it is
        /// <see cref="DailyHealthBand.NoData"/> and the row says how many sources still answer.</summary>
        PastHorizon = 3,
    }

    /// <summary>
    /// The one decision both SKUs' daily-summary readers make about a returned day (#3541 A9), plus the
    /// payload vocabulary for it.
    /// </summary>
    public static class DailySummaryRetention
    {
        /// <summary>
        /// The number of per-signal sources the daily aggregate reads: waits, queries, deadlocks, blocked-process
        /// reports, DMV blocking snapshots, CPU samples, memory-pressure events. The collection log and the alert
        /// log are the spine's other two members and are NOT signals — they are the longer-lived sources whose
        /// survival is what lets a day outlive its signals.
        /// </summary>
        public const int SignalSourceCount = 7;

        /// <summary>
        /// The state of one returned day from the three facts that decide it. Only the two past-horizon states
        /// withhold the band: inside retention a zero is a measurement, and a day the alert log alone names is
        /// still a day an alert fired on.
        ///
        /// <para>The horizon test comes FIRST, so a purged day with a surviving run record (the collection
        /// log outlives the signals by design — its horizon is twice theirs so a failure's evidence outlives
        /// the metric rows it explains) reads as purged rather than as collected-and-quiet. That ordering IS
        /// the fix: <c>runs &gt; 0</c> alone would have called every day in the second month collected.</para>
        ///
        /// <para>Presence is what keeps the horizon honest. The horizon is ARITHMETIC — now minus the shortest
        /// retention — while a purge is an EVENT that may not have happened yet: the sweep runs daily, the
        /// TimescaleDB path drops whole chunks, and the rollup-coverage gate can hold a table's purge for
        /// weeks. A day before the horizon that still has signal rows is therefore not called purged, because
        /// "the tables no longer hold it" would be false; it is <see cref="DailySummaryDataState.PastHorizon"/>,
        /// which keeps the rows and withholds the verdict.</para>
        /// </summary>
        /// <param name="summaryDate">The day the row describes (UTC date).</param>
        /// <param name="collectionRuns">Collector runs of every status the collection log holds for the day.</param>
        /// <param name="signalSourcesPresent">How many of the <see cref="SignalSourceCount"/> signal sources hold
        /// at least one row for the day — the aggregate's <c>signal_sources_present</c> column.</param>
        /// <param name="retentionHorizon">The oldest UTC day the shortest-lived signal source still holds —
        /// see <see cref="HorizonFor"/>.</param>
        public static DailySummaryDataState StateFor(DateTime summaryDate, long collectionRuns, int signalSourcesPresent, DateTime retentionHorizon)
        {
            if (summaryDate.Date < retentionHorizon.Date)
                return signalSourcesPresent > 0 ? DailySummaryDataState.PastHorizon : DailySummaryDataState.Purged;

            return collectionRuns > 0 ? DailySummaryDataState.Collected : DailySummaryDataState.NoRunRecord;
        }

        /// <summary>
        /// The retention horizon: the oldest UTC day on which EVERY signal the band reads is still present,
        /// from the SHORTEST retention among the sources.
        ///
        /// <para>The purge cutoff is <c>now − retention</c>, an instant; the day containing it is the first
        /// day the shortest-lived source still holds rows for. On the TimescaleDB path that whole day is
        /// present until its chunk's END passes the cutoff (drop_chunks removes only chunks entirely older
        /// than the cutoff, and raw chunks are one day wide), so the cutoff's own day is complete there; on
        /// the DELETE path the cutoff's day may hold only the hours after the cutoff. The horizon is the
        /// cutoff's DATE, which is exact on the production path and at most one partial day generous on the
        /// other — the direction that never calls a day with real rows purged.</para>
        ///
        /// <para>The clock is the READER's wall clock, never the caller's <c>as_of</c>: a purge is a
        /// wall-clock event, and a backdated anchor cannot un-purge a table. Anchoring the horizon to
        /// <c>as_of</c> would let "as_of 25 days ago, days_back 30" paint the purged stretch green again —
        /// the exact defect. The tools' own no-<c>DateTime.UtcNow</c> rule is about the WINDOW (which must
        /// follow the anchor); the horizon is a property of the store, and it belongs in the reader.</para>
        /// </summary>
        public static DateTime HorizonFor(DateTime utcNow, int shortestRetentionDays)
        {
            if (shortestRetentionDays < 1)
                throw new ArgumentOutOfRangeException(nameof(shortestRetentionDays), shortestRetentionDays, "A retention horizon needs at least one day.");

            return utcNow.AddDays(-shortestRetentionDays).Date;
        }

        /// <summary>The payload spelling of a state — one vocabulary on both SKUs.</summary>
        public static string Label(DailySummaryDataState state) => state switch
        {
            DailySummaryDataState.Collected => "collected",
            DailySummaryDataState.Purged => "purged",
            DailySummaryDataState.PastHorizon => "past_horizon",
            _ => "no_run_record",
        };

        /// <summary>
        /// The one-sentence reason a non-collected day carries on its row; <c>null</c> for a collected day so
        /// the common case pays nothing. Says what the zeros ARE, because a reader that sees
        /// <c>deadlock_count: 0</c> beside <c>health_band: NoData</c> otherwise has to guess which of the two
        /// to believe.
        /// </summary>
        public static string? Note(DailySummaryDataState state, DateTime retentionHorizon, int signalSourcesPresent = 0) => state switch
        {
            DailySummaryDataState.Purged =>
                "PURGED: this day is before the store's retention_horizon (" + retentionHorizon.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
                + ") and none of the per-signal tables the band reads (waits, queries, deadlocks, blocking, CPU, memory) holds a row for it; their zeros are absences, not measurements. "
                + "The day is listed because a longer-lived source (the collection log or the alert log) still records it — collection_runs and alert_count are real where non-zero. No health verdict is possible.",
            DailySummaryDataState.PastHorizon =>
                "PAST HORIZON: this day is before the store's retention_horizon (" + retentionHorizon.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
                + ") but " + signalSourcesPresent.ToString(CultureInfo.InvariantCulture) + " of " + SignalSourceCount.ToString(CultureInfo.InvariantCulture)
                + " signal sources still hold rows for it — the purge has not reached it, or the sources' retentions differ. Non-zero counts are real; a zero may be a measurement or an absence, and the band cannot tell which, so no health verdict is given.",
            DailySummaryDataState.NoRunRecord =>
                "NO RUN RECORD: no collector run is recorded for this day, so the collection-error share has no denominator and nothing records that the day was fully collected; the day appears because a signal table or the alert log holds rows for it. Inside retention its zeros are measurements, so the band stands on the counts as read.",
            _ => null,
        };
    }
}
