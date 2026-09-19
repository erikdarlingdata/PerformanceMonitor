/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// <c>pg_database</c> — the per-database counter facts from the one <c>pg_database_stats</c> read (lane 6 of
/// #3542, which owns that read): <c>PG_TPS</c> is context (0); <c>PG_DEADLOCK_RATE</c> grades deadlocks per
/// observed hour. <c>PG_HIT_RATIO</c> is declared and never emitted — the hit ratio is an arm of
/// <c>PG_BUFFER_CACHE_PRESSURE</c> — and scores 0 here should a row ever carry the key.
///
/// <para><b>The deadlock tiers are the alert band's, and the lineage is MEASURED.</b> The two bars below are
/// <c>ServerHealthThresholds.DeadlockWarnPerHourDefault</c> / <c>DeadlockCriticalPerHourDefault</c>
/// (<c>PerformanceMonitor.Common/ServerHealthBands.cs</c>, #3368): the 99.94th percentile of deadlocks per
/// server-hour over 14 days × 43 servers of the dogfood fleet (2,722 deadlocks over 14,448 server-hours), and
/// a critical line placed inside the measured empty interval [16, 89] between the routine mode and the storms.
/// The fleet card already bands a PostgreSQL server's <c>FleetPgDeadlockSql</c> difference on exactly these
/// tiers (<c>DarlingFleetReader</c>), so the analysis rate and the card a reader is sent to agree by
/// construction — the D9 argument for the vacuum family, applied to the one rate the alerting layer measured.
/// The values are repeated rather than bound because this assembly references nothing (see
/// <c>PostgresOutagePredictorThresholds</c> for why); <c>PgTargetTempTests</c> pins the two pairs equal, the
/// way <c>FactScorerTests</c> pins the SQL Server <c>DEADLOCKS</c> arm to the same constants. This is not a
/// SQL Server constant reused by value: it is the alerting layer's measured tier, which PostgreSQL targets
/// are already banded on. The #3691 fleet calibration (2026-09-19) then read the PostgreSQL side directly —
/// 14 days × 50 Aurora PostgreSQL clusters of the dogfood fleet: 40 of 50 had zero deadlocks in 14 days, the
/// other 10 had 1 – 4 in total with at most 2 in any hour, so both tiers sit in that population's measured
/// empty interval and the first-occurrence anomaly (<c>ANOMALY_PG_DEADLOCK_RATE</c>, floor 1/h) is the
/// detector that fires there. Measured on Aurora; the stock-PostgreSQL population is not yet measured.</para>
///
/// <para><b>What the alert's <c>pg_count_threshold</c> is NOT.</b> The PostgreSQL deadlock ALERT counts
/// <c>pg_deadlocks</c> rows with a threshold of 1 — a different instrument on a different source (the log
/// capture), and the evaluator's own comment argues the surfaces differ. It is deliberately not shared here:
/// the rate is the counter's, and the advice says how the two can disagree.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /* ── metadata keys the collector stamps and the advice reads ── */

    public const string TpsCommitsKey = "xact_commit";
    public const string TpsRollbacksKey = "xact_rollback";
    public const string TpsRollbackShareKey = "rollback_share";
    /// <summary>Transactions per observed second over the first / second half of the window, and their difference
    /// (second minus first): the trend lane 3's offered-vs-delivered co-fire reads (<c>PgTargetScorer.Sessions.cs</c>,
    /// the v2 hook — sessions climbing while <c>tps_trend ≤ 0</c> is queueing at the cliff). Absent when only one
    /// half of the window was observed.</summary>
    public const string TpsFirstHalfKey = "tps_first_half";
    public const string TpsSecondHalfKey = "tps_second_half";
    public const string TpsTrendKey = "tps_trend";

    /// <summary>The COUNTER's deadlocks in the window — the number the rate is derived from.</summary>
    public const string DeadlockCounterCountKey = "counter_count";
    /// <summary>How many deadlock reports the LOG capture holds for the window; absent when that read failed
    /// (the store has no <c>pg_deadlocks</c>, or the read timed out).</summary>
    public const string DeadlockExemplarCountKey = "exemplar_count";
    /// <summary>The rate again, under the <c>_per_hour</c> key the #3538 A7 pin recognises as a rate.</summary>
    public const string DeadlocksPerHourKey = "deadlocks_per_hour";
    public const string DeadlockObservedHoursKey = "observed_hours";
    /// <summary>The counter's deadlocks in the database that had the most (named as <see cref="Fact.DatabaseName"/>).</summary>
    public const string DeadlockTopDatabaseCountKey = "top_database_count";

    /* measured: p99.94 of deadlocks per server-hour over 14 days × 43 servers of the dogfood fleet (#3368; 2,722
       deadlocks over 14,448 server-hours) — ServerHealthThresholds.DeadlockWarnPerHourDefault, repeated here because
       this assembly references nothing; PgTargetTempTests pins the pair equal. Re-read on the PostgreSQL side over
       14 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19: above the fleet maximum of 2
       deadlocks in any hour (40 clusters had none; 10 had 1 – 4 in 14 days) — the measured empty interval. */
    public const double DeadlockWarnPerHour = 5.0;

    /* measured: the same 14-day distribution — bimodal, the routine mode topping out at 15 and the next observation
       at 90 — with the critical line placed inside the empty interval [16, 89]; ServerHealthThresholds.
       DeadlockCriticalPerHourDefault, repeated and pinned equal for the same reason. On the 50 Aurora PostgreSQL
       clusters read 2026-09-19 the line was never approached (fleet maximum 2 in any hour). */
    public const double DeadlockCriticalPerHour = 20.0;

    /// <summary>
    /// <c>PG_DEADLOCK_RATE</c> through the shared formula on the alert band's two tiers; <c>PG_TPS</c> and any
    /// other <c>pg_database</c> key are context (0). Stamps <c>threshold_lineage = 1</c>: both bars are measured
    /// (the alert band's #3368 read and the 2026-09-19 PostgreSQL-side re-read). Since #3691 the stamp is a verdict
    /// wherever a family states lineage — 0 means at least one chosen bar decided, 1 means every bar that decided is
    /// measured or engine-defined — so a reader of <c>get_analysis_facts</c> sees the verdict on this fact instead of
    /// inferring it from an absent key. Context facts (<c>PG_TPS</c>) are not graded and carry no stamp.
    /// </summary>
    private static partial double ScoreDatabaseFact(Fact fact)
    {
        if (fact.Key != PgTargetFactKeys.DeadlockRate) return 0.0;

        fact.Metadata["threshold_lineage"] = 1;
        /* measured: DeadlockWarnPerHour / DeadlockCriticalPerHour (see the constants). Value is per observed hour. */
        return FactScorer.ApplyThresholdFormula(fact.Value, DeadlockWarnPerHour, DeadlockCriticalPerHour);
    }
}
