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
using System.Text.Json;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Builds the two-sided <see cref="RiskDisclosure"/> for a DESTRUCTIVE remediation
/// action (B3 Phase 3). Returns null for non-destructive fact keys — the consent
/// gate is never requested for them.
///
/// <para>
/// The "risks of NOT changing" side is quantified from monitoring data the analysis
/// ALREADY holds: the per-database <c>config_issues</c> drill-down is enriched at
/// analysis time (in both collectors) with <c>rcsi_blocking_events</c>,
/// <c>rcsi_deadlocks</c>, and <c>rcsi_reader_writer_pct</c>. This reads those typed
/// fields — it issues NO SQL of its own and performs NO live probe of the target
/// (the same single-parse pattern as <see cref="FactRemediation"/>).
/// </para>
///
/// <para>
/// All prose is FIXED/reviewed; the only substituted tokens are the validated
/// database identifier and the numeric figures. The honest-both-directions property:
/// when the reader/writer share is low (writer/writer contention), the inaction side
/// says RCSI will NOT resolve it rather than overstate the benefit; when little or no
/// blocking was captured, it shows the weak-case baseline.
/// </para>
/// </summary>
public static class FactRiskDisclosure
{
    /// <summary>
    /// The reader/writer-share threshold below which the "RCSI won't resolve this"
    /// (writer/writer-dominant) inaction line is shown instead of the "RCSI eliminates
    /// this" line. A share at or above this is treated as a meaningful reader/writer case.
    /// </summary>
    public const int ReaderWriterMeaningfulPct = 50;

    /// <summary>
    /// Returns the two-sided risk disclosure for a destructive action, or null for a
    /// non-destructive fact key (the gate is never requested there). The finding may
    /// be null — the inaction side then degrades to the weak-case baseline line.
    /// </summary>
    public static RiskDisclosure? GetForAction(RemediationAction action, AnalysisFinding? finding)
    {
        if (action is null || string.IsNullOrEmpty(action.FactKey))
            return null;

        return action.FactKey switch
        {
            "RCSI" => BuildRcsiDisclosure(action, finding),
            _ => null
        };
    }

    private static RiskDisclosure BuildRcsiDisclosure(RemediationAction action, AnalysisFinding? finding)
    {
        // The target database (validated non-empty by the extractor; re-validated
        // against sys.databases at apply time). Bracketed for display only.
        var database = FirstTargetDatabase(action);
        var db = QuoteName(database);

        var changing = new List<RiskItem>
        {
            new($"Enabling RCSI on {db} takes a brief exclusive database lock and will " +
                "block until all in-flight transactions in that database complete — run it at " +
                "a quiet moment; under load it can wait or be chosen as a deadlock victim."),
            new($"RCSI adds row-version load to tempdb; a long-running reader transaction on " +
                $"{db} can grow the version store and pressure tempdb."),
            new("Reader/writer concurrency semantics change — readers stop taking shared locks " +
                "and read the last committed row version instead. Application code that relies on " +
                "default locking behavior (or uses NOLOCK to work around blocking today) should be " +
                "tested on a copy first."),
        };

        var figures = ReadInactionFigures(action, finding, database);
        var notChanging = BuildInactionRisks(db, figures);

        return new RiskDisclosure(changing, notChanging);
    }

    /// <summary>
    /// The inaction-side risk lines, filled from the finding's monitoring figures.
    /// Always returns at least one item (the honest-both-directions framing). When no
    /// reader/writer share was captured, shows the weak-case baseline.
    /// </summary>
    private static List<RiskItem> BuildInactionRisks(string db, InactionFigures f)
    {
        var notChanging = new List<RiskItem>
        {
            new($"Over the analysis window ({f.HoursBack}h), {db} recorded " +
                $"{f.BlockingEvents.ToString(CultureInfo.InvariantCulture)} blocked-process events and " +
                $"{f.Deadlocks.ToString(CultureInfo.InvariantCulture)} deadlocks."),
        };

        if (f.ReaderWriterPct is int pct && (f.BlockingEvents > 0 || f.Deadlocks > 0 || pct > 0))
        {
            if (pct >= ReaderWriterMeaningfulPct)
            {
                notChanging.Add(new(
                    $"Roughly {pct.ToString(CultureInfo.InvariantCulture)}% of the captured lock " +
                    "contention was readers blocked by writers (shared / range-shared locks) — the " +
                    "exact pattern RCSI eliminates."));
            }
            else
            {
                notChanging.Add(new(
                    $"Only {pct.ToString(CultureInfo.InvariantCulture)}% of the captured contention " +
                    "was reader-vs-writer; the majority was writer/writer (X/IX/U), which RCSI does " +
                    "NOT resolve — shorter transactions and better write-path indexing are the levers. " +
                    "Enabling RCSI may add tempdb overhead without relieving this contention."));
            }
        }
        else
        {
            notChanging.Add(new(
                $"Little or no reader/writer blocking was captured for {db} in this window — the case " +
                "for enabling RCSI here is weak; consider whether the finding is driven by another database."));
        }

        return notChanging;
    }

    private static string FirstTargetDatabase(RemediationAction action)
    {
        if (action.DbConfigTargets is { Count: > 0 } dbTargets)
            return dbTargets[0].Database;
        if (action.Targets is { Count: > 0 } targets)
            return targets[0].Database;
        return string.Empty;
    }

    /// <summary>
    /// Resolves the three inaction figures, REAL-figures-first: the figures captured on
    /// the persisted <see cref="RemediationAction"/> at BuildRcsiAction time take
    /// precedence (this is what survives to apply time — the UI apply call site has no
    /// finding). Only when the action carries none do we fall back to the finding's
    /// <c>config_issues</c> drill-down (the analysis-time path: email/webhook/MCP build
    /// the disclosure while the finding is still in hand). Degrades to the weak-case
    /// baseline (null/zero) when neither source has the data.
    /// </summary>
    private static InactionFigures ReadInactionFigures(RemediationAction action, AnalysisFinding? finding, string database)
    {
        // Real-figures path: the action carries the figures captured when the finding
        // WAS available, so the dialog shows the true blocking/deadlock/reader-writer
        // numbers even though the apply call site passes no finding.
        if (action.RcsiFigures is { } carried)
        {
            return new InactionFigures
            {
                HoursBack = HoursBack(finding),
                BlockingEvents = carried.BlockingEvents,
                Deadlocks = carried.Deadlocks,
                ReaderWriterPct = carried.ReaderWriterPct
            };
        }

        var figures = new InactionFigures { HoursBack = HoursBack(finding) };

        if (finding?.DrillDown is null ||
            string.IsNullOrEmpty(database) ||
            !finding.DrillDown.TryGetValue("config_issues", out var raw) ||
            raw is null)
            return figures;

        JsonElement element;
        try
        {
            element = JsonSerializer.SerializeToElement(raw);
        }
        catch
        {
            return figures;
        }

        if (element.ValueKind != JsonValueKind.Array)
            return figures;

        foreach (var row in element.EnumerateArray())
        {
            if (row.ValueKind != JsonValueKind.Object) continue;
            if (!string.Equals(GetString(row, "database"), database, StringComparison.Ordinal)) continue;

            figures.BlockingEvents = GetInt(row, "rcsi_blocking_events");
            figures.Deadlocks = GetInt(row, "rcsi_deadlocks");
            figures.ReaderWriterPct = GetNullableInt(row, "rcsi_reader_writer_pct");
            return figures;
        }

        return figures;
    }

    private static int HoursBack(AnalysisFinding? finding)
    {
        // The disclosure window length. The finding does not carry the raw hours, so
        // fall back to the canonical default (24h) used for the analysis window.
        // Substituted into fixed prose only — never an execution input.
        return 24;
    }

    private sealed class InactionFigures
    {
        public int HoursBack { get; set; } = 24;
        public int BlockingEvents { get; set; }
        public int Deadlocks { get; set; }
        public int? ReaderWriterPct { get; set; }
    }

    /// <summary>
    /// QUOTENAME-equivalent for display (mirrors <see cref="FactRemediation"/>'s
    /// private routine; the database name is from sys.databases via the collector).
    /// </summary>
    private static string QuoteName(string identifier) =>
        string.IsNullOrEmpty(identifier) ? "[unknown]" : "[" + identifier.Replace("]", "]]") + "]";

    private static string GetString(JsonElement row, string property)
    {
        if (!row.TryGetProperty(property, out var v)) return string.Empty;
        return v.ValueKind == JsonValueKind.String ? (v.GetString() ?? string.Empty) : string.Empty;
    }

    private static int GetInt(JsonElement row, string property)
    {
        if (!row.TryGetProperty(property, out var v)) return 0;
        return v.ValueKind switch
        {
            JsonValueKind.Number when v.TryGetInt32(out var i) => i,
            JsonValueKind.Number => (int)v.GetDouble(),
            _ => 0
        };
    }

    private static int? GetNullableInt(JsonElement row, string property)
    {
        if (!row.TryGetProperty(property, out var v)) return null;
        return v.ValueKind switch
        {
            JsonValueKind.Number when v.TryGetInt32(out var i) => i,
            JsonValueKind.Number => (int)v.GetDouble(),
            _ => null
        };
    }
}
