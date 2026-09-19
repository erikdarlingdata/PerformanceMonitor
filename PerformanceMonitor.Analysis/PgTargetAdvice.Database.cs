/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for the per-database counter facts (lane 6 of #3542, which owns the one <c>pg_database_stats</c>
/// read): the deadlock rate states "the engine counted N; M were captured from the log", because the counter
/// is complete and the log tail is lossy, and a card saying "12 deadlocks per hour" beside three exemplars
/// looks like a bug to a reader unless it says why (design §6 C). <c>PG_TPS</c> is context and composes a
/// context block; <c>PG_HIT_RATIO</c> is never emitted and its block points at the buffer composite.
/// </summary>
public static partial class PgTargetAdvice
{
    private static partial AdviceBlock? ComposeDatabase(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        return key switch
        {
            PgTargetFactKeys.DeadlockRate => ComposeDeadlockRate(factsByKey),
            PgTargetFactKeys.Tps => ComposeTps(factsByKey),
            PgTargetFactKeys.HitRatio => s_hitRatioPointer,
            _ => null,
        };
    }

    private static readonly AdviceBlock s_hitRatioPointer = new(
        Headline: "Buffer-cache hit ratio — read from PG_BUFFER_CACHE_PRESSURE, not from a fact of its own",
        Investigation:
            "The hit ratio (blks_hit against blks_hit + blks_read from pg_stat_database) is one arm of the buffer-cache " +
            "composite, PG_BUFFER_CACHE_PRESSURE, so that the one condition it measures — a working set larger than " +
            "shared_buffers — is counted once. No PG_HIT_RATIO fact is emitted; this key is declared and inert.",
        Remediation: "See PG_BUFFER_CACHE_PRESSURE and CONFIG_PG_SHARED_BUFFERS.");

    private static AdviceBlock ComposeDeadlockRate(IReadOnlyDictionary<string, Fact> facts)
    {
        var rate = KnobFact(facts, PgTargetFactKeys.DeadlockRate);

        if (rate is null)
        {
            return new AdviceBlock(
                Headline: "Deadlock rate — the engine's own per-database counter, per observed hour",
                Investigation:
                    "pg_stat_database.deadlocks counts every deadlock the engine detected and broke, in every database, " +
                    "whether or not any log capture saw it. This finding is that counter's consecutive-sample differences " +
                    "over the window, per observed hour, with a pg_stat_reset() mid-window clamped and reported. It is graded " +
                    "on the same two tiers the fleet card bands a PostgreSQL server's deadlocks on. The captured deadlock " +
                    "graphs (pg_deadlocks) are the exemplars, never the count: the log tail is lossy on exactly the busiest " +
                    "servers, so the engine counted N and the log captured M, and the two can differ.",
                Remediation: DeadlockRemediation());
        }

        var counter = rate.Metadata.GetValueOrDefault(PgTargetScorer.DeadlockCounterCountKey);
        var observedHours = rate.Metadata.GetValueOrDefault(PgTargetScorer.DeadlockObservedHoursKey);
        var hasExemplars = rate.Metadata.TryGetValue(PgTargetScorer.DeadlockExemplarCountKey, out var exemplars);

        var sb = new StringBuilder(1000);
        sb.Append("pg_stat_database.deadlocks is the engine's own count: every deadlock it detected and broke, in every " +
                  "database, including ones no session the log parser sees was party to. Over ")
          .Append(DbHours(observedHours)).Append(" of observed time the engine counted ").Append(DbCount(counter))
          .Append(counter == 1 ? " deadlock" : " deadlocks").Append(" — ")
          .Append(rate.Value.ToString("0.##", CultureInfo.InvariantCulture)).Append(" per hour");
        if (!string.IsNullOrEmpty(rate.DatabaseName))
        {
            sb.Append(", ").Append(DbCount(rate.Metadata.GetValueOrDefault(PgTargetScorer.DeadlockTopDatabaseCountKey)))
              .Append(" of them in ").Append(rate.DatabaseName);
        }

        sb.Append(". ");
        sb.Append(hasExemplars
            ? $"The log capture holds {DbCount(exemplars)} deadlock report{(exemplars == 1 ? string.Empty : "s")} for the same window: the engine counted {DbCount(counter)}; {DbCount(exemplars)} {(exemplars == 1 ? "was" : "were")} captured from the log. "
            : $"The log capture was not read this pass (the engine counted {DbCount(counter)}; how many were captured from the log is not known here). ");
        sb.Append("The two differ by design, not by defect: the counter is per database and complete; the captured graphs " +
                  "come from a log tail that silently loses whole reports past a few kilobytes a second of log volume, and " +
                  "the parser only sees deadlocks the log carried. ");

        var resets = rate.Metadata.GetValueOrDefault(PgTargetScorer.CounterStatsResetCountKey);
        var rewinds = rate.Metadata.GetValueOrDefault(PgTargetScorer.CounterRewindCountKey);
        sb.Append("The count is consecutive-sample differences over ")
          .Append(DbCount(rate.Metadata.GetValueOrDefault(PgTargetScorer.CounterIntervalsKey))).Append(" intervals");
        if (resets > 0 || rewinds > 0)
        {
            sb.Append("; the counters were reset ").Append(DbCount(resets)).Append(" time(s) in the window (")
              .Append(DbCount(rewinds)).Append(" rewind(s) seen), each clamped to zero rather than read as a negative rate");
        }

        sb.Append(". The tiers are the alerting layer's measured deadlock band (")
          .Append(PgTargetScorer.DeadlockWarnPerHour.ToString("0.#", CultureInfo.InvariantCulture)).Append(" and ")
          .Append(PgTargetScorer.DeadlockCriticalPerHour.ToString("0.#", CultureInfo.InvariantCulture))
          .Append(" per hour), the same ones the fleet card bands this server on.");

        return new AdviceBlock(
            Headline: $"{rate.Value.ToString("0.##", CultureInfo.InvariantCulture)} deadlocks per hour over {DbHours(observedHours)} — the engine counted {DbCount(counter)}" +
                      (hasExemplars ? $"; {DbCount(exemplars)} {(exemplars == 1 ? "was" : "were")} captured from the log" : string.Empty),
            Investigation: sb.ToString(),
            Remediation: DeadlockRemediation());
    }

    private static string DeadlockRemediation() =>
        "Read the captured graphs (get_pg_deadlocks, then get_pg_deadlock_detail for one) for the lock modes and the " +
        "statements on each side; the victim_statement and the participants' relations name the access pattern. Fix the " +
        "ordering in the application — every transaction touching the same set of rows or tables in the same order — before " +
        "any setting. deadlock_timeout only decides how long a waiter sits before the detector runs; lowering it finds the " +
        "same deadlocks sooner and costs detector runs on every long wait. Counter-objective of retry-on-40P01 in the " +
        "application: it hides the rate this fact measures, so keep the counter in view after adding it.";

    private static AdviceBlock ComposeTps(IReadOnlyDictionary<string, Fact> facts)
    {
        var tps = KnobFact(facts, PgTargetFactKeys.Tps);
        if (tps is null)
        {
            return new AdviceBlock(
                Headline: "Transaction throughput — context, not a finding",
                Investigation:
                    "xact_commit + xact_rollback per second of observed time, differenced from the one-minute pg_database_stats " +
                    "series across every database. Context (severity 0): throughput is what the other facts are read against, " +
                    "and its deviation from this server's own hour-of-week baseline is the anomaly family's, not this fact's.",
                Remediation: "Nothing to change; read the facts beside it.");
        }

        var commits = tps.Metadata.GetValueOrDefault(PgTargetScorer.TpsCommitsKey);
        var rollbacks = tps.Metadata.GetValueOrDefault(PgTargetScorer.TpsRollbacksKey);
        var share = tps.Metadata.GetValueOrDefault(PgTargetScorer.TpsRollbackShareKey);
        return new AdviceBlock(
            Headline: $"{tps.Value.ToString("0.##", CultureInfo.InvariantCulture)} transactions per second of observed time ({(share * 100).ToString("0.#", CultureInfo.InvariantCulture)}% rollbacks)",
            Investigation:
                $"{DbCount(commits)} commits and {DbCount(rollbacks)} rollbacks over the window, across " +
                $"{DbCount(tps.Metadata.GetValueOrDefault(PgTargetScorer.CounterDatabasesKey))} database series and " +
                $"{DbCount(tps.Metadata.GetValueOrDefault(PgTargetScorer.CounterIntervalsKey))} consecutive-sample intervals. " +
                "Context (severity 0): this is the denominator the other facts are read against, and the deviation from this " +
                "server's own hour-of-week baseline is the anomaly family's. A rollback share in the tens of percent is worth a " +
                "look on its own — serialization failures, deadlock victims (PG_DEADLOCK_RATE) and application-side aborts all land here.",
            Remediation: "Nothing to change on throughput alone; read the facts beside it.");
    }

    private static string DbCount(double value) => value.ToString("N0", CultureInfo.InvariantCulture);

    private static string DbHours(double hours) => hours switch
    {
        < 1 => $"{hours * 60:0} minutes",
        < 48 => $"{hours:0.#} hours",
        _ => $"{hours / 24:0.#} days",
    };
}
