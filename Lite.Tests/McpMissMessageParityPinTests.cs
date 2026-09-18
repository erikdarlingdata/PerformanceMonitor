/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The miss SENTENCES the two SKUs share, pinned against both source trees.
///
/// <para>#2485 had two halves. One was reads that could not say which kind of nothing they had found; the
/// other was three tools that answered the same question differently depending on which SKU the client was
/// pointed at. The first half is fixable in one place per tool. The second is not: every shared sentence
/// lives twice, once per SKU, and nothing stops one copy being reworded on its own — which is how the
/// divergence this issue exists to close got there in the first place.</para>
///
/// <para>So the sentences are pinned as SOURCE, in both trees at once. A fragment listed here must appear in
/// <c>Lite/Mcp</c> AND in <c>Darling/PerformanceMonitor.Darling.Service/Mcp</c>; reword one copy and this
/// fails naming the tree that no longer has it. Fragments are chosen to sit BETWEEN interpolation holes, so
/// they are the literal bytes both SKUs emit rather than an approximation of them.</para>
///
/// <para>This is a per-change pin, not a survey: it holds the sentences this repo has deliberately made
/// shared, and each change that adds one is expected to add it here. It does not claim to enumerate every
/// message either server can produce, and a naive extension that tried to would pass vacuously the day
/// somebody added an unshared one.</para>
/// </summary>
public sealed class McpMissMessageParityPinTests
{
    /// <summary>
    /// #2559 wiring, per SKU. The shared builder guarantees the SENTENCES match; what it cannot guarantee is
    /// that both tool bodies actually CALL it, which is the drift the review caught on the first draft of
    /// that change — Darling grew the arm and Lite did not, so a Lite login with no msdb access kept getting
    /// the affirmative "no jobs running" claim the arm exists to remove.
    ///
    /// <para>Order matters as much as presence: the gate inference must sit after the collector's own
    /// recorded denial (specific evidence beats an inference from an absence) and before the empty miss.
    /// Anchored on CODE rather than on the sentences, because both files contain comments that quote them.</para>
    /// </summary>
    [Theory]
    [InlineData("Lite/Mcp/McpJobTools.cs", "McpRuntimePrecondition")]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpJobTools.cs", "DarlingRuntimePrecondition")]
    public void BothSkus_AskAboutTheGate_AfterTheRecordedOutcome_AndBeforeTheEmptyMiss(string relativePath, string helper)
    {
        var source = File.ReadAllText(Path.Combine(ParitySource.RepoRoot(), relativePath));

        var recorded = source.IndexOf($"{helper}.StatusAsync", StringComparison.Ordinal);
        var gate = source.IndexOf($"{helper}.GatedOffStatusAsync", StringComparison.Ordinal);
        var empty = source.IndexOf("McpHelpers.Status(\"empty\"", StringComparison.Ordinal);

        Assert.True(gate > 0, $"{relativePath} never asks whether the collector is gated off (#2559)");
        Assert.True(recorded > 0 && empty > 0, $"{relativePath} no longer has the arms this pin describes");
        Assert.True(gate > recorded, $"{relativePath} lets the gate inference pre-empt a recorded denial");
        Assert.True(empty > gate, $"{relativePath} no longer keeps the empty miss as the last resort");
    }

    private const string LiteMcpDir = "Lite/Mcp";
    private const string DarlingMcpDir = "Darling/PerformanceMonitor.Darling.Service/Mcp";

    /// <summary>
    /// Sentence fragments that must read identically on both SKUs. Each sits between interpolation holes, so
    /// what is compared is the literal text a caller receives.
    /// </summary>
    public static TheoryData<string> SharedMissFragments() => new()
    {
        /* get_wait_types */
        "This server HAS collected wait stats before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.",
        "Delta wait stats need a SECOND collection cycle before the first row exists, so on a newly added server this clears itself; otherwise check that collection is running and that the server is enabled.",

        /* get_collection_log. Byte-identical in DarlingMcpDataTools and Lite McpHealthTools, and it was
           the only shared miss sentence this list did not name. Darling's sentinel population gets its
           OWN sentences ahead of these three, so what is compared here stays the monitored-server text. */
        "This server HAS collected before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent runs.",

        /* get_memory_clerks */
        "This read returns the LATEST snapshot rather than a window, so an empty result is never a quiet period — a live SQL Server always has memory clerks.",

        /* get_mute_rules */
        "No mute rules are configured for this store, so no alert is being suppressed anywhere — a quiet alert history is genuine rather than muted.",
        "is disabled or expired, so nothing is being suppressed. Pass enabled_only=false to list them — this is a lapsed mute, not an absent one.",

        /* compare_analysis */
        "in EITHER window, so there is nothing to compare — this is NOT a report that nothing changed.",
        "The BASELINE window produced no facts at all, so every fact below counts as a new issue only because there was nothing to compare it against.",
        "The COMPARISON window produced no facts at all, so every fact below counts as a resolved issue only because there is nothing in the recent window to compare against.",

        /* The analysis family's coverage caveats (#3538 A2). The NUMBERS in them come from
           WindowCoverage.Describe(), shared by construction; what lives twice is the sentence each tool
           body wraps around them, and that is what would drift — one SKU telling a caller its rates are
           per observed time while the other said nothing. analyze_server (findings and the scoped
           all-clear), get_analysis_facts (the partial caveat and the unobserved miss), compare_analysis
           (either side partly collected, and the closing warning). */
        "Rates and fractions below are per observed time, so they are not deflated by the gap — but the unobserved stretch could have held anything, and nothing here speaks for it. Check get_collection_health for why collection stopped.",
        "of this window the collector observed — a PARTIAL reading, not a full all-clear. ",
        ". The unobserved stretch could have held anything, and nothing here speaks for it; check get_collection_health for why collection stopped.",
        "Every fraction-of-period and per-hour value below is per OBSERVED time (period_duration_ms × coverage_fraction, or observed_hours), not per nominal window; the COLLECTION_GAP fact carries the hole.",
        ", so no windowed fact (wait fractions, blocking or deadlock rates) exists to show.",
        "point-in-time fact(s) — configuration and current state — could still be read; audit_config reports those.",
        "The BASELINE window was only partly collected: ",
        "The COMPARISON window was only partly collected: ",
        ". Its rates are per observed time, and its windowed facts are absent where nothing was observed.",
        "A side that was not fully observed cannot be read as the whole period: a wait that is absent because the collector was down is not a wait that resolved. Confirm coverage (get_collection_log, get_collection_health) before reading worse/better/resolved_issues as change.",

        /* get_query_heatmap (#2484) — the three empty branches, one of which (a collected but IDLE
           window) no other read has. */
        "so this is NOT a report of a quiet server — there is nothing to draw. query_stats is a PERIODIC table rather than an edge table: the collector writes rows every cycle for whatever is in the plan cache, so an empty history means nobody looked. Check get_collection_health for this server.",
        " hour(s), so the grid has no columns rather than no hot cells. Widen hours_back, or check get_collection_health — a collector that stopped looks exactly like this.",
        " hour(s), but no capture recorded an execution: every row carried a zero execution delta, so nothing lands on the grid. A server that is up and idle looks exactly like this, and so does a database_name filter matching nothing collected. Delta-based collection also needs a SECOND cycle before the first non-zero row exists.",

        /* get_lock_wait_trend (#2484). The all-clear sentence is get_wait_types' own, reused deliberately:
           both reads are looking at the same PERIODIC table and the advice is identical, so two spellings
           of it would be a divergence for nothing. Only the never-collected half needs its own words,
           because "no lock contention" is the wrong thing to hear about a server nothing was stored for. */
        "No lock waits recorded for ",
        ", so this is NOT a report of a server without lock contention — nothing has been stored for it at all. ",

        /* get_daily_summary_range (#2484) — the two empty branches. The first is the one that is easy to
           get wrong: a day with ANY collection appears even when quiet, so no days at all cannot mean a
           quiet stretch. */
        ". A day with ANY collection appears here even when every signal was quiet, so this range is outside what the store holds for this server rather than a stretch of quiet days — widen days_back, or move as_of.",
        ", so the calendar is empty because nothing has been collected — not because those days were quiet. Check that the service is running and that the server is enabled for collection.",

        /* The instructions' miss-vocabulary paragraph (#2511). The engine-gap MESSAGE itself is built by
           CollectorEngineCapability and is byte-identical by construction rather than by pinning; what lives
           twice, and therefore belongs here, is the paragraph that teaches a caller how to read it. */
        "`not_collected` means this server does not collect that at all — and when the reason is the ENGINE, the gap is PERMANENT",

        /* The PostgreSQL half of the same paragraph (#2532). An agent over MCP has no tabs: it asks a
           read by name, so the instructions are the only place it can be told which family answers on
           this engine. */
        "a PostgreSQL target collects none of the SQL Server signals at all, and the `get_pg_*` reads are the ones that answer there",

        /* The FOURTH word (#2546). The precondition MESSAGES themselves are built by
           CollectorRuntimePrecondition and are byte-identical by construction; what lives twice, and
           therefore belongs here, is the paragraph teaching a caller that this one is the one they can act
           on — and specifically that it is re-derived per read, so doing what it asks is enough. An agent
           told to restart the monitoring service instead would be back at the defect the word exists to
           close. */
        "`precondition` is the one that IS worth acting on: this server could have that data, the collector is running, and a setup step on the monitored server is in the way",
        "It is re-derived on EVERY read rather than decided when the connection was made, so once somebody does the thing it asked for the next call answers with data",

        /* #2559. The paragraph above USED to end "there is nothing to restart on the monitoring side" as a
           flat promise, and for connect-scoped preconditions that is false — the fact that gates them is read
           once at connect and cached for the connection's life, so an agent following the general case sends
           the user round a loop that never terminates. The correction has to be identical on both SKUs or one
           of them keeps teaching the wrong rule, which is exactly what this pin is for. */
        "A few preconditions are the exception and SAY SO IN THEIR OWN MESSAGE: the fact that gates them is read once when the service connects to that server and cached for the connection's life",
        "telling somebody to retry a connect-scoped one without reconnecting sends them round a loop that never terminates",

        /* The gated-off arm's GATE CANDIDATES (#2559). The message body itself comes from the shared
           CollectorRuntimePrecondition and is byte-identical by construction, so it does not belong here —
           but this sentence is supplied by each tool body at its own call site, lives twice, and is exactly
           what drifts. A tree missing it is a tree whose get_running_jobs never grew the arm at all. */
        "tables are not reachable to a monitoring login at all and no grant changes that.",

        /* The confidence DEFINITION (#3538 A6). The confidence_basis STRING itself is built by the shared
           StoryConfidence.DescribeBasis and is byte-identical by construction, so it does not belong here;
           what lives twice is the tool-description sentence teaching a caller what the number is — and
           that is exactly the text that would drift into one SKU saying "evidence score" while the other
           still implied a probability. analyze_server, get_analysis_findings (the legacy-row warning) and
           get_analysis_facts (the baseline_confidence disambiguation), plus the instructions rows. */
        "confidence is an EVIDENCE score, not a probability: 0.20 for the fired symptom alone, plus up to 0.48 for the share of the root fact's amplifier checks (its expected companions) that matched and up to 0.32 for the depth of the evidence chain",
        "Rank by severity for impact and by confidence for how much of the engine's own corroboration showed up; do not multiply them.",
        "Rows persisted before this definition carried a PATH-LENGTH statistic under the same name, with a lone symptom at 1.0 — confidence_basis labels those rows path-shape (pre-#3538) and they must not be read as corroborated.",
        "For ANOMALY_* facts the metadata carries baseline_confidence — the baseline's own trustworthiness (tier x sample density), which the scorer multiplies into that fact's severity; it is a different quantity from a finding's confidence in analyze_server.",
        "Each finding's `confidence` is an EVIDENCE score (0.20 for the symptom alone, more as the root fact's amplifier checks match and the chain deepens — a lone uncorroborated symptom is 0.20, never 1.0) and `confidence_basis` says in words what it rests on",
        "(an ANOMALY_* fact's `baseline_confidence` is the baseline's trustworthiness, not a finding's `confidence`)",
    };

    [Theory]
    [MemberData(nameof(SharedMissFragments))]
    public void EverySharedMissSentence_ReadsIdenticallyOnBothSkus(string fragment)
    {
        Assert.True(
            AppearsIn(LiteMcpDir, fragment),
            $"Lite/Mcp no longer contains the shared sentence: \"{fragment}\"");

        Assert.True(
            AppearsIn(DarlingMcpDir, fragment),
            $"Darling's MCP tools no longer contain the shared sentence: \"{fragment}\"");
    }

    /// <summary>
    /// A non-vacuous floor. Without it, a fragment list that drifted into naming text neither tree contains
    /// would still be a green suite the day somebody emptied it — the guard-that-stopped-guarding shape.
    /// </summary>
    [Fact]
    public void ThePinCoversTheSentencesThisChangeMadeShared()
    {
        Assert.True(SharedMissFragments().Count() >= 11,
            "the shared-sentence pin lost entries; a sentence that stops being pinned can drift between the SKUs unnoticed");
    }

    private static bool AppearsIn(string relativeDir, string fragment) =>
        ParitySource.EnumerateCsFiles(relativeDir)
            .Any(f => File.ReadAllText(f).Contains(fragment, StringComparison.Ordinal));
}
