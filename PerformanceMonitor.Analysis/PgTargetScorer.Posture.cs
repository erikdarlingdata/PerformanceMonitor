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
/// <c>pg_posture</c> — durability posture (filled by lane 8, #3542 step 8). <c>fsync</c> / <c>full_page_writes</c>
/// off score the CRITICAL band (1.5 — <c>FactScorer</c> reads ≥ 1.5 as CRITICAL and as the notify floor),
/// <c>synchronous_commit</c> off the 0.4 advisory band. There is deliberately NO amplifier partial for this
/// source (D6): a posture fact is a durability statement, never an argument about how the server runs, and
/// <c>PgTargetPostureIsolationTests</c> pins that no amplifier or edge anywhere references a posture key.
///
/// <para><b>Why the bands are categorical and not a ramp.</b> Every other family grades a MEASUREMENT through
/// <c>ApplyThresholdFormula</c>; a durability setting is a switch. The SQL Server scorer's precedent for a
/// categorically-wrong setting is <c>CONFIG_PRIORITY_BOOST</c> / <c>CONFIG_LIGHTWEIGHT_POOLING</c> — a flat band
/// when the value is the wrong one, 0 otherwise — and that is the METHOD inherited here, not the number: the
/// SQL Server pair sit at the WARNING band because their harm is to scheduling; <c>fsync = off</c> is a
/// documented path to an unrecoverable cluster after a crash, which is the outage class the CRITICAL band
/// and the notify floor exist for. The lineage of each bar is engine-defined in the strict sense: the
/// condition is PostgreSQL's own documented default (<c>on</c> for all three) being turned off, and the
/// documented consequence of that specific setting decides which band.</para>
///
/// <para><b>Aurora.</b> <c>fsync</c> and <c>full_page_writes</c> are owned by Aurora's storage layer; the
/// parameter group does not expose them and the value the catalog reports is the platform's, not the
/// operator's. The collector stamps <c>managed_by_platform = 1</c> and the fact grades 0 — present, so the
/// posture is visible in <c>get_analysis_facts</c>, and never a card, because there is no policy for the
/// operator to own. <c>synchronous_commit</c> IS exposed on Aurora and keeps its grade there.</para>
///
/// <para><b>What this scorer never does.</b> It reads no other fact, so no workload can raise or lower a
/// posture grade; it reads no rate, so nothing here divides by observed time; and a posture key is not in
/// <c>FactScorer.IsTuningClassKey</c>'s set, so the CRITICAL grade is not capped to WARNING on the way out —
/// a server running with <c>fsync = off</c> must be able to notify.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /// <summary>
    /// CRITICAL band for <c>fsync = off</c> and <c>full_page_writes = off</c>. Engine-defined: PostgreSQL's
    /// default for both is <c>on</c>, and its documentation states the consequence of <c>off</c> as
    /// unrecoverable data corruption after an operating-system crash or power loss (<c>fsync</c>) or a torn
    /// page the WAL cannot repair (<c>full_page_writes</c>). 1.5 is the house scale's CRITICAL floor and
    /// notify floor (<c>FactScorer</c>), not a measured bar — there is nothing to measure about a switch.
    /// </summary>
    private const double PostureCriticalBand = 1.5;

    /// <summary>
    /// Advisory band for <c>synchronous_commit = off</c>. Engine-defined: the default is <c>on</c>, and the
    /// documented consequence of <c>off</c> is a bounded window of committed-then-lost transactions on a
    /// crash — at most three times <c>wal_writer_delay</c> (600 ms at the shipped 200 ms) — with NO corruption
    /// and NO inconsistency, which is why it is a posture statement at the 0.4 convention band
    /// (<c>ConfigAdvisoryRoots</c> lets it root a card at any positive grade) and not an incident.
    /// </summary>
    private const double PostureAdvisoryBand = 0.4;

    private static partial double ScorePostureFact(Fact fact)
    {
        /* Platform-owned on Aurora: the value is not the operator's policy, so it is not graded (D6). */
        if (fact.Metadata.GetValueOrDefault("managed_by_platform") == 1)
            return 0.0;

        /* The finding condition is the setting being OFF (Value = 1, the collector's categorical encoding);
           on / local / remote_* keep local durability and score 0. */
        if (fact.Value != 1)
            return 0.0;

        return fact.Key switch
        {
            /* engine-defined: PostgreSQL's documented default (on) turned off; the documented consequence is
               unrecoverable corruption — see PostureCriticalBand. */
            PgTargetFactKeys.PostureFsync => PostureCriticalBand,
            PgTargetFactKeys.PostureFullPageWrites => PostureCriticalBand,
            /* engine-defined: default on; documented consequence is a bounded loss window, no corruption —
               see PostureAdvisoryBand. */
            PgTargetFactKeys.PostureSynchronousCommit => PostureAdvisoryBand,
            _ => 0.0,
        };
    }
}
