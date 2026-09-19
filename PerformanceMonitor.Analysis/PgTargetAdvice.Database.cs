/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
///
/// <para>Lane 16 of #3691 adds the exemplar sentence: the deadlock card's frozen prose is composed from facts
/// alone, before any drill-down has run, so <see cref="WithDeadlockExemplars"/> is the second composer — the
/// Darling drill-down reads <c>pg_deadlocks</c>, builds a <see cref="PgTargetDeadlockExemplarSummary"/>, and
/// re-freezes the card through it. The words live here with the rest of the deadlock prose; the read lives
/// with the other drill-downs. Every number in the sentence is the summary's, never assumed.</para>
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

    /// <summary>
    /// The deadlock card with the exemplar drill-down folded into its prose (lane 16 of #3691): the
    /// investigation gains <see cref="DeadlockExemplarSentence"/>, and the remediation gains the lever the
    /// most-recurrent shape points at — access ORDER first, <c>deadlock_timeout</c>'s counter-objective second
    /// — stated against that shape's lock modes and resources rather than in the abstract. Applied to whichever
    /// block the finding froze (the rate's or the anomaly's); a block that already carries the sentence is
    /// returned unchanged, so a re-run over the same finding cannot stack it.
    /// </summary>
    public static AdviceBlock WithDeadlockExemplars(AdviceBlock advice, PgTargetDeadlockExemplarSummary summary)
    {
        ArgumentNullException.ThrowIfNull(advice);
        ArgumentNullException.ThrowIfNull(summary);

        if (advice.Investigation.Contains(ExemplarSentenceMarker, StringComparison.Ordinal))
            return advice;

        var investigation = string.Concat(advice.Investigation.TrimEnd(), " ", DeadlockExemplarSentence(summary));
        var remediation = advice.Remediation;

        var top = summary.Exemplars.Count > 0 ? summary.Exemplars[0] : null;
        if (top is not null)
        {
            var sb = new StringBuilder(advice.Remediation.Length + 400);
            sb.Append(advice.Remediation.TrimEnd()).Append(" Here: the shape to fix first is the one that recurred most — ")
              .Append(DescribeShape(top)).Append(". ");
            sb.Append(top.ParticipantCount switch
            {
                2 => "Two participants is the ordering deadlock: two transactions that take the same two locks in opposite " +
                     "order, and the fix is to make every code path that touches those resources take them in ONE order (the " +
                     "measured population's every captured graph has this shape). ",
                > 2 => $"{top.ParticipantCount.Value.ToString("N0", CultureInfo.InvariantCulture)} participants is a chain, not a pair: several transactions each waiting on the next — " +
                       "typically a hot row or a lock-escalating batch under concurrency — and re-ordering two code paths will not " +
                       "break it; shorten the transactions that hold the contended lock, or serialise the batch. ",
                _ => string.Empty,
            });
            sb.Append("deadlock_timeout decides only how long a waiter sits before the detector runs: longer means fewer detector " +
                      "runs on ordinary lock waits and a longer stall for every genuine deadlock; shorter finds the same cycles sooner " +
                      "and pays the detector on every long wait. It changes how fast a deadlock is broken, never whether it happens.");
            remediation = sb.ToString();
        }

        return advice with { Investigation = investigation, Remediation = remediation };
    }

    /// <summary>The phrase every exemplar sentence starts with; <see cref="WithDeadlockExemplars"/> keys its idempotence on it.</summary>
    internal const string ExemplarSentenceMarker = "Exemplars: ";

    /// <summary>
    /// The value-stated exemplar sentence, the same words in the drill-down payload's <c>note</c> and in the
    /// re-frozen card. Three arms: no capture at all (the posture sentence — settings NAMED, values not stated,
    /// because the pass did not read them); one shape; several. Counts are the summary's, never recomputed.
    /// </summary>
    public static string DeadlockExemplarSentence(PgTargetDeadlockExemplarSummary summary)
    {
        ArgumentNullException.ThrowIfNull(summary);

        var counted = summary.EngineCounted is { } c ? DbCount(c) : "a non-zero number of";
        if (summary.LogCaptured == 0 || summary.Exemplars.Count == 0)
        {
            return ExemplarSentenceMarker +
                   $"the engine counted {counted} deadlock{(summary.EngineCounted == 1 ? string.Empty : "s")} in the window and the log capture holds none of them. " +
                   "That is a logging posture, not a contradiction: PostgreSQL writes a deadlock report only when log_lock_waits is on " +
                   "and log_min_messages admits it, the report lands deadlock_timeout after the wait began, and the log tail this " +
                   "store reads drops whole reports when log volume out-runs it. Read log_lock_waits, log_min_messages and " +
                   "deadlock_timeout with get_pg_server_config; there is no graph to read until a report is captured.";
        }

        var top = summary.Exemplars[0];
        var sb = new StringBuilder(600);
        sb.Append(ExemplarSentenceMarker)
          .Append(DbCount(summary.LogCaptured)).Append(summary.LogCaptured == 1 ? " report" : " reports").Append(" captured");
        if (summary.ReportsCaptured != summary.LogCaptured)
            sb.Append(" (").Append(DbCount(summary.ReportsCaptured)).Append(" distinct once the overlapping log tail is de-duplicated)");
        sb.Append(" against ").Append(counted).Append(" the engine counted, in ")
          .Append(DbCount(summary.DistinctShapes)).Append(summary.DistinctShapes == 1 ? " shape" : " distinct shapes");
        /* The reading of the shape count: one shape over several reports is a recurring pattern; as many shapes
           as reports is no recurrence yet; one report is neither and gets no gloss. */
        sb.Append(summary.DistinctShapes == 1 && summary.ReportsCaptured > 1
            ? " — one access pattern, recurring"
            : summary.DistinctShapes > 1 && summary.DistinctShapes == summary.ReportsCaptured
                ? " — every report its own pattern, none recurring yet"
                : string.Empty);
        sb.Append(". The most frequent ").Append(DescribeShape(top)).Append(", seen ")
          .Append(DbCount(top.Reports)).Append(top.Reports == 1 ? " time" : " times");
        if (top.LastSeen is { } last)
            sb.Append(", last at ").Append(last.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)).Append(" UTC");
        sb.Append('.');
        if (summary.DistinctShapes > summary.Exemplars.Count)
            sb.Append(" The drill-down carries the top ").Append(DbCount(summary.Exemplars.Count)).Append("; get_pg_deadlocks lists the rest.");
        return sb.ToString();
    }

    /// <summary>"involves &lt;modes&gt; on &lt;resources&gt; with the victim &lt;fingerprint&gt;", each arm dropped when the parser did not recover it.</summary>
    private static string DescribeShape(PgTargetDeadlockExemplar shape)
    {
        var sb = new StringBuilder(200);
        sb.Append(shape.ParticipantCount is { } p ? $"{p.ToString("N0", CultureInfo.InvariantCulture)}-participant shape" : "shape");
        if (!string.IsNullOrEmpty(shape.LockModes))
            sb.Append(" involves ").Append(shape.LockModes);
        if (!string.IsNullOrEmpty(shape.Resources))
            sb.Append(string.IsNullOrEmpty(shape.LockModes) ? " involves " : " on ").Append(shape.Resources);
        sb.Append(string.IsNullOrEmpty(shape.VictimStatementFingerprint)
            ? " (the victim's statement text was not recovered)"
            : $" with the victim `{shape.VictimStatementFingerprint}`");
        return sb.ToString();
    }

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

/// <summary>
/// What the deadlock exemplar drill-down found (lane 16 of #3691), handed from the Darling read to the advice
/// composer so the words and the numbers come from one object. <paramref name="EngineCounted"/> is the ROOT
/// FACT's counter delta (reused, never recomputed; null when the finding carried no metadata);
/// <paramref name="LogCaptured"/> the rows the log capture holds for the window and
/// <paramref name="LogCapturedSource"/> which read said so; <paramref name="ReportsCaptured"/> the distinct
/// <c>deadlock_hash</c> count (the overlapping-tail re-read de-duplicated); <paramref name="DistinctShapes"/>
/// the number of <c>(participant_count, lock_modes, resources)</c> groups, of which
/// <paramref name="Exemplars"/> carries the most-recurrent few.
/// </summary>
public sealed record PgTargetDeadlockExemplarSummary(
    int? EngineCounted,
    int LogCaptured,
    string LogCapturedSource,
    int ReportsCaptured,
    int DistinctShapes,
    IReadOnlyList<PgTargetDeadlockExemplar> Exemplars);

/// <summary>
/// One deadlock SHAPE and its latest report. <paramref name="Reports"/> is the recurrence (distinct hashes in
/// the shape); <paramref name="RowsCaptured"/> the raw rows, equal off the <c>pg_read_file</c> route. The
/// statement and graph are the latest report's, bounded, with the flags saying whether a bound cut them;
/// <paramref name="VictimStatementFingerprint"/> is the prose-sized form the advice names.
/// </summary>
public sealed record PgTargetDeadlockExemplar(
    int Rank,
    int? ParticipantCount,
    string? LockModes,
    string? Resources,
    int Reports,
    int RowsCaptured,
    DateTime? FirstSeen,
    DateTime? LastSeen,
    string? DeadlockHash,
    int? VictimPid,
    string? VictimStatement,
    bool VictimStatementMayBeTruncated,
    string? VictimStatementFingerprint,
    string? GraphText,
    int GraphTextLinesTotal,
    bool GraphTextTruncated);
