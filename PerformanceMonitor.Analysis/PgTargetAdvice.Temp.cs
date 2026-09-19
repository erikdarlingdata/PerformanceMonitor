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
/// Advice for temp-file spill and its <c>work_mem</c> co-fire (lane 6 of #3542): spilled bytes and files per
/// second of observed time, the database that spilled the most, <c>work_mem</c> as the host has it, and the
/// overcommit arithmetic with the host's own <c>max_connections</c>.
///
/// <para><b>Value-stated, with the counter-objective in the same breath (OtterTune doctrine).</b> <c>work_mem</c>
/// is the classic directionally-dangerous knob: it is a per-sort-node, per-backend allocation, so raising it
/// globally multiplies by <c>max_connections</c> (and again by the number of sort / hash nodes in a plan), and
/// the overcommit that follows fails only at peak. Every block here states that arithmetic with the numbers
/// read from the facts — "<c>work_mem</c> is 4 MB and <c>max_connections</c> is 200, so 800 MB at a full house
/// of one-sort backends" — never a formula, and leads with the statements: the drill-down's
/// <c>pg_temp_spill_statements</c> (top <c>temp_blks_written</c> from <c>pg_statement_stats</c>) is where a
/// spill is fixed, a session-scoped <c>SET work_mem</c> is the targeted lever, and the global knob is last.
/// No DDL (D8). <c>effective_cache_size</c> is never named as a memory figure (design §6 D).</para>
///
/// <para><b>Two roots, one evidence paragraph.</b> <c>PG_TEMP_SPILL</c> roots the story (the knob is its leaf);
/// <c>CONFIG_PG_WORK_MEM</c> composes only as that leaf, and its block says so — a <c>work_mem</c> value alone
/// is not a finding (D5), so the block for the key without a spill fact beside it describes the gate rather
/// than pretending to a verdict.</para>
/// </summary>
public static partial class PgTargetAdvice
{
    private static partial AdviceBlock? ComposeTemp(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        return key switch
        {
            PgTargetFactKeys.TempSpill => ComposeTempSpill(factsByKey),
            PgTargetFactKeys.ConfigWorkMem => ComposeWorkMem(factsByKey),
            _ => null,
        };
    }

    private static AdviceBlock ComposeTempSpill(IReadOnlyDictionary<string, Fact> facts)
    {
        var spill = KnobFact(facts, PgTargetFactKeys.TempSpill);
        var knob = KnobFact(facts, PgTargetFactKeys.ConfigWorkMem);

        if (spill is null)
        {
            return new AdviceBlock(
                Headline: "Temp-file spill — sorts and hashes are exceeding work_mem and writing to disk",
                Investigation:
                    "When a sort, hash or materialize node's working set exceeds work_mem, PostgreSQL writes the overflow to " +
                    "temp files under pgsql_tmp, and pg_stat_database counts every such file and byte per database " +
                    "(temp_files, temp_bytes). This finding is the spilled bytes per second of OBSERVED time over the window, " +
                    "differenced from the one-minute pg_database_stats series with a pg_stat_reset() mid-window clamped and " +
                    "reported rather than read as a negative rate. The measured figures were not frozen into this finding; " +
                    "get_pg_database_stats carries them per database and get_pg_top_queries lists the statements writing the " +
                    "most temp blocks.",
                Remediation: WorkMemRemediation(knob, spill: null));
        }

        return new AdviceBlock(
            Headline: TempSpillHeadline(spill),
            Investigation: TempSpillEvidence(spill, knob).ToString().TrimEnd(),
            Remediation: WorkMemRemediation(knob, spill));
    }

    private static AdviceBlock ComposeWorkMem(IReadOnlyDictionary<string, Fact> facts)
    {
        var knob = KnobFact(facts, PgTargetFactKeys.ConfigWorkMem);
        var spill = KnobFact(facts, PgTargetFactKeys.TempSpill);

        var stated = knob is null ? "not known here (the config snapshot has not been collected)" : KnobMb(knob.Value);

        if (spill is null || spill.BaseSeverity <= 0)
        {
            return new AdviceBlock(
                Headline: $"work_mem is {stated} — a value alone is not a finding",
                Investigation:
                    "`work_mem` is evidence-gated (D5): there is no right value without the workload, and raising it on a " +
                    "convention is the classic overcommit path, because it is allocated per sort or hash node per backend. " +
                    "This card exists only as the leaf of a PG_TEMP_SPILL story — temp files written at or above the floor " +
                    "over the observed window — and roots nothing on its own. " + (knob is null
                        ? "The setting was not read this pass."
                        : $"On this host it is {stated}" + (knob.Value <= PgTargetScorer.WorkMemBootValMb ? ", the shipped default." : ".")),
                Remediation:
                    "Nothing to change on this value alone. If a spill finding appears, start from the statements it names " +
                    "(the drill-down's pg_temp_spill_statements, or get_pg_top_queries) before the global knob.");
        }

        return new AdviceBlock(
            Headline: $"work_mem is {stated} on this host, and the window spilled {RatePerSec(spill.Value)} to temp files",
            Investigation: TempSpillEvidence(spill, knob).ToString().TrimEnd(),
            Remediation: WorkMemRemediation(knob, spill));
    }

    private static string TempSpillHeadline(Fact spill)
    {
        var bytes = spill.Metadata.GetValueOrDefault(PgTargetScorer.TempSpillBytesKey);
        var files = spill.Metadata.GetValueOrDefault(PgTargetScorer.TempSpillFilesKey);
        var sb = new StringBuilder(160);
        sb.Append("Temp-file spill: ").Append(RatePerSec(spill.Value)).Append(" of observed time — ")
          .Append(KnobBytes(bytes)).Append(" across ").Append(TempCount(files)).Append(" temp files in the window");
        if (!string.IsNullOrEmpty(spill.DatabaseName))
        {
            var share = spill.Metadata.GetValueOrDefault(PgTargetScorer.TempSpillTopDatabaseShareKey);
            sb.Append(", ").Append(TempPercent(share)).Append(" of it in ").Append(spill.DatabaseName);
        }

        return sb.ToString();
    }

    private static StringBuilder TempSpillEvidence(Fact spill, Fact? knob)
    {
        var sb = new StringBuilder(1200);
        var observedHours = spill.Metadata.GetValueOrDefault(PgTargetScorer.CounterObservedMsKey) / 3_600_000.0;
        var files = spill.Metadata.GetValueOrDefault(PgTargetScorer.TempSpillFilesKey);
        var bytes = spill.Metadata.GetValueOrDefault(PgTargetScorer.TempSpillBytesKey);

        sb.Append("When a sort, hash or materialize node's working set exceeds `work_mem`, PostgreSQL writes the overflow to temp " +
                  "files under pgsql_tmp; pg_stat_database counts every such file and byte per database. Over ")
          .Append(TempHours(observedHours)).Append(" of observed time this server wrote ")
          .Append(KnobBytes(bytes)).Append(" in ").Append(TempCount(files)).Append(" temp files — ")
          .Append(RatePerSec(spill.Value)).Append(", ")
          .Append(spill.Metadata.GetValueOrDefault(PgTargetScorer.TempSpillFilesPerSecKey).ToString("0.###", CultureInfo.InvariantCulture))
          .Append(" files/s, an average of ")
          .Append(files > 0 ? KnobBytes(bytes / files) : "0 B").Append(" per file. ");

        if (!string.IsNullOrEmpty(spill.DatabaseName))
        {
            sb.Append(spill.DatabaseName).Append(" accounts for ")
              .Append(TempPercent(spill.Metadata.GetValueOrDefault(PgTargetScorer.TempSpillTopDatabaseShareKey)))
              .Append(" of the bytes across ")
              .Append(TempCount(spill.Metadata.GetValueOrDefault(PgTargetScorer.CounterDatabasesKey)))
              .Append(" database series. ");
        }

        var resets = spill.Metadata.GetValueOrDefault(PgTargetScorer.CounterStatsResetCountKey);
        var rewinds = spill.Metadata.GetValueOrDefault(PgTargetScorer.CounterRewindCountKey);
        sb.Append("The figures are consecutive-sample differences over ")
          .Append(TempCount(spill.Metadata.GetValueOrDefault(PgTargetScorer.CounterIntervalsKey)))
          .Append(" intervals");
        if (resets > 0 || rewinds > 0)
        {
            sb.Append("; the counters were reset ").Append(TempCount(resets)).Append(" time(s) in the window (")
              .Append(TempCount(rewinds)).Append(" rewind(s) seen), each clamped to zero rather than read as a negative rate, so the total under-counts by the clamped intervals");
        }

        sb.Append(". ");

        sb.Append("The floor this fact fired at (")
          .Append(RatePerSec(PgTargetScorer.TempSpillConcerningBytesPerSec))
          .Append(") is fleet-measured — about the 99.7th percentile of the measured fleet's five-minute spill rates (threshold_lineage = 1); the baseline-relative comparison " +
                  "against this database's own hour-of-week temp rate is the later slice. ");

        sb.Append(WorkMemStatement(knob, spill));

        sb.Append(" The statements that wrote the most temp blocks in the window are in this finding's drill-down " +
                  "(pg_temp_spill_statements, from pg_statement_stats.temp_blks_written); get_pg_top_queries lists them live.");

        return sb;
    }

    /// <summary>The knob as the host has it, from the knob fact when present and the stamp on the spill fact
    /// otherwise; "not known" when neither was collected — never an assumed 4 MB.</summary>
    private static string WorkMemStatement(Fact? knob, Fact? spill)
    {
        var workMemBytes = knob is not null
            ? knob.Value * 1024 * 1024
            : KnobMeta(spill, PgTargetScorer.TempSpillWorkMemBytesKey);
        if (workMemBytes is null)
            return "work_mem on this host is not known here — the config snapshot has not been collected — so the per-sort budget these files exceeded cannot be stated.";

        var sb = new StringBuilder(240);
        sb.Append("work_mem on this host is ").Append(KnobBytes(workMemBytes.Value));
        if (knob is not null && knob.Value <= PgTargetScorer.WorkMemBootValMb)
            sb.Append(", the shipped default");
        sb.Append(": every sort or hash whose working set exceeded that spilled.");
        return sb.ToString();
    }

    private static string WorkMemRemediation(Fact? knob, Fact? spill)
    {
        var sb = new StringBuilder(900);
        sb.Append("Start from the statements, not the knob: the drill-down names the statements that wrote the most temp " +
                  "blocks, and a sort that can be avoided (a different join order, a narrower sort key, an existing index " +
                  "that returns rows in the order asked for) or a hash whose estimate was wrong is fixed at the statement. " +
                  "For a statement that legitimately needs more, set work_mem for that session, role or function (SET LOCAL " +
                  "work_mem inside the transaction) — targeted, and without the global multiplier. ");

        var workMemBytes = knob is not null
            ? knob.Value * 1024 * 1024
            : KnobMeta(spill, PgTargetScorer.TempSpillWorkMemBytesKey);
        var maxConnections = KnobMeta(spill, PgTargetScorer.TempSpillMaxConnectionsKey);

        sb.Append("Counter-objective of raising it globally: work_mem is allocated per sort or hash node per backend, so the " +
                  "ceiling is work_mem × max_connections × the sort and hash nodes in a plan, and the overcommit it produces " +
                  "fails only at peak. ");
        if (workMemBytes is not null && maxConnections is > 0)
        {
            sb.Append("Here: ").Append(KnobBytes(workMemBytes.Value)).Append(" × ")
              .Append(TempCount(maxConnections.Value)).Append(" connections = ")
              .Append(KnobBytes(workMemBytes.Value * maxConnections.Value))
              .Append(" at a full house of one-sort backends, before shared_buffers; doubling work_mem doubles that figure. ");
        }
        else if (workMemBytes is not null)
        {
            sb.Append("Here work_mem is ").Append(KnobBytes(workMemBytes.Value))
              .Append("; multiply by max_connections (not collected this pass) for the full-house figure. ");
        }

        sb.Append("log_temp_files = 0 records every temp file with its size and the statement that wrote it; " +
                  "temp_file_limit caps a runaway sort before it fills the volume.");
        return sb.ToString();
    }

    /* ── formatting (this family; the byte formatters are the knob family's KnobBytes / KnobMb) ── */

    private static string RatePerSec(double bytesPerSec) => KnobBytes(bytesPerSec) + "/s";

    private static string TempCount(double value) => value.ToString("N0", CultureInfo.InvariantCulture);

    private static string TempPercent(double share) => (share * 100).ToString("0", CultureInfo.InvariantCulture) + "%";

    private static string TempHours(double hours) => hours switch
    {
        < 1 => $"{hours * 60:0} minutes",
        < 48 => $"{hours:0.#} hours",
        _ => $"{hours / 24:0.#} days",
    };
}
