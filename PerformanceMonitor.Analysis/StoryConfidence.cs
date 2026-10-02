/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Story confidence as an EVIDENCE statistic (#3538 A6), and the read-time description of what a
/// given confidence value rests on.
///
/// <para><b>The defect this replaces.</b> Confidence used to be a path-shape statistic:
/// <c>path.Count == 1 ? 1.0 : (path.Count - 1) / path.Count</c>. A lone symptom with NO corroboration
/// carried the HIGHEST confidence the engine could express (1.0), a two-node chain 0.5, a ten-node chain
/// 0.9 — inverted at exactly the point that matters, and exported to LLM consumers under an evidence
/// name beside <c>severity</c>. An agent ranking findings by severity × confidence preferred the least
/// evidenced one. Meanwhile the engine already computed real corroboration for every root and threw
/// it away at this step: the FactScorer evaluates a per-key AMPLIFIER catalogue ("what else should be
/// true if this symptom is real?") and records each match on <see cref="Fact.AmplifierResults"/>.</para>
///
/// <para><b>The formula.</b> Two corroboration terms, each in [0, 1], over a floor for the symptom
/// itself:</para>
/// <list type="bullet">
/// <item><description><c>amplifier share</c> = matched ÷ defined amplifiers for the root fact — the
/// share of the engine's own "if this is real, X should also be true" checks that came back true.</description></item>
/// <item><description><c>path depth</c> = (n − 1) ÷ n for an n-node path — each traversed edge is a
/// fired predicate onto a fact that itself scored, so a deeper chain is more corroborated, with
/// diminishing weight per extra node (0 for a lone symptom, 0.5 at two nodes, 0.9 at ten).</description></item>
/// </list>
/// <para><c>confidence = 0.20 + 0.48 × amplifier share + 0.32 × path depth</c> when the root has an
/// amplifier catalogue; <c>0.20 + 0.80 × path depth</c> when it has none. The 0.20 floor is the fired
/// symptom itself: a threshold was crossed by a measured value, which is evidence of something, and a
/// finding the engine chose to root should never read as 0. The 60/40 split between the two terms
/// (0.48/0.32 of the 0.80 that corroboration can earn) puts the larger weight on the amplifier
/// catalogue because those checks are hand-written per key against the things that DISTINGUISH a
/// real instance of the symptom, while a graph edge is the next question to ask and fires on
/// presence more often than on distinguishing evidence. When the root has no catalogue the path
/// term carries the whole 0.80 rather than capping the story at 0.52: no catalogue is the ABSENCE of
/// evidence and is scored neutrally, whereas a catalogue whose checks all came back false is evidence
/// AGAINST — the engine looked for this symptom's usual companions and found none — and is scored down.
/// These weights are design constants, not fleet measurements; the property they were chosen to hold
/// is the ordering below, which the tests pin.</para>
///
/// <para><b>Worked examples.</b> A lone SCH_M (no catalogue, one node): 0.20. A lone PAGEIOLATCH_SH
/// with 0 of 3 amplifiers matched: 0.20. A lone PAGEIOLATCH_SH with 3 of 3 matched: 0.68.
/// A three-node chain such as PAGEIOLATCH_SH → RESOURCE_SEMAPHORE → MEMORY_GRANT_PENDING with 2 of 3
/// matched: 0.20 + 0.32 + 0.21 = 0.73. SCH_M → RUNNING_JOBS (no catalogue, two nodes): 0.60. A ten-node
/// chain with no catalogue: 0.92; with a fully matched catalogue: 0.968. The deepest path the traversal
/// builds is eleven nodes (<c>InferenceEngine.MaxPathDepth</c> = 10 hops plus the root), where the two
/// ceilings are 0.927 and 0.971 — under 1.0, because path depth never reaches 1. Nothing built by this
/// formula is ever exactly 1.0,
/// and it is monotonic by construction: one more matched amplifier or one more path node never lowers
/// it.</para>
///
/// <para><b>Legacy rows without a schema change.</b> The persisted <c>confidence</c> column keeps its
/// name and pre-change rows keep their values; a migration rung to stamp a version marker was ruled
/// out (one un-landed rung at a time, repo-wide). Instead the basis is RE-DERIVED at read time from the
/// value and the path length, which is sound because the two formulas' ranges are disjoint at every
/// path length: the legacy value for n nodes is exactly 1.0 (n = 1) or (n − 1)/n, and this formula
/// never lands on that number for the same n with any small-integer amplifier catalogue (pinned
/// exhaustively over every path length the traversal can build — n ≤ <see cref="MaxPathNodes"/> — and
/// catalogue ≤ 12). <see cref="DescribeBasis"/> therefore labels a
/// value that equals its path-shape number as <c>path-shape (pre-#3538)</c> and everything else as
/// corroboration-derived, and the two built-by-construction 1.0s (absolution, the same-statement
/// pileup detector) are named by their root key so they are never mislabelled as legacy.</para>
/// </summary>
public static class StoryConfidence
{
    /// <summary>
    /// The longest story path <see cref="InferenceEngine.BuildStories"/> can produce: the root plus
    /// <see cref="InferenceEngine.MaxPathDepth"/> hops. The exhaustive pins enumerate to this bound so the
    /// no-collision guarantee covers every path the engine can actually build, not a round number.
    /// </summary>
    public const int MaxPathNodes = InferenceEngine.MaxPathDepth + 1;

    /// <summary>The fired symptom itself — a measured value crossed a threshold. Never 0.</summary>
    public const double Floor = 0.20;

    /// <summary>Weight of the matched-amplifier share when the root has a catalogue.</summary>
    public const double AmplifierWeight = 0.48;

    /// <summary>Weight of the path-depth term when the root has a catalogue.</summary>
    public const double PathWeight = 0.32;

    /// <summary>Weight of the path-depth term when the root has NO amplifier catalogue (the amplifier
    /// term's share is folded in rather than forfeited — see the class remarks).</summary>
    public const double UncataloguedPathWeight = AmplifierWeight + PathWeight;

    /// <summary>The same-statement pileup detector's root key — its stories carry 1.0 by construction
    /// (a directly observed convoy against its own baseline), not by this formula and not by the
    /// legacy one. Named here so <see cref="DescribeBasis"/> can say so instead of calling it legacy.</summary>
    public const string PileupRootKey = "SAME_STATEMENT_PILEUP";

    /// <summary>The absolution story's root key (see <see cref="InferenceEngine.BuildStories"/>).</summary>
    public const string AbsolutionRootKey = "server_health";

    /// <summary>
    /// Computes a story's confidence from its root fact's amplifier results and its path length. The
    /// FactScorer must have run first — <see cref="Fact.AmplifierResults"/> is populated there and is
    /// empty for a key with no catalogue (which is the uncatalogued arm, deliberately). A null root
    /// (a path whose first key is not in the working set, which BuildStories never produces) is scored
    /// as uncatalogued.
    /// </summary>
    public static double Compute(Fact? rootFact, int pathLength)
    {
        var results = rootFact?.AmplifierResults;
        var defined = results?.Count ?? 0;
        var matched = results?.Count(r => r.Matched) ?? 0;
        return Compute(matched, defined, pathLength);
    }

    /// <summary>
    /// The pure arithmetic, exposed so the pins can enumerate it. <paramref name="pathLength"/> below
    /// 1 is treated as 1 (a story has at least its root).
    /// </summary>
    public static double Compute(int matchedAmplifiers, int definedAmplifiers, int pathLength)
    {
        var n = Math.Max(1, pathLength);
        var pathDepth = (n - 1.0) / n;
        if (definedAmplifiers <= 0)
            return Floor + UncataloguedPathWeight * pathDepth;

        var share = Math.Clamp((double)matchedAmplifiers / definedAmplifiers, 0.0, 1.0);
        return Floor + AmplifierWeight * share + PathWeight * pathDepth;
    }

    /// <summary>
    /// The value the PRE-#3538 formula produced for a path of <paramref name="factCount"/> nodes:
    /// 1.0 for a lone symptom, (n − 1)/n otherwise. Kept only so a persisted row can be recognised as
    /// legacy at read time; never used to score anything.
    /// </summary>
    public static double LegacyPathShape(int factCount)
    {
        var n = Math.Max(1, factCount);
        return n == 1 ? 1.0 : (n - 1.0) / n;
    }

    /// <summary>
    /// True when <paramref name="confidence"/> is exactly the legacy path-shape number for a path of
    /// <paramref name="factCount"/> nodes — a row written before confidence measured corroboration.
    /// Exact to 1e-9: the two formulas' ranges are disjoint (class remarks), so equality IS the test.
    /// </summary>
    public static bool IsLegacyPathShape(double confidence, int factCount) =>
        Math.Abs(confidence - LegacyPathShape(factCount)) < 1e-9;

    /// <summary>
    /// The <c>confidence_basis</c> string both MCP SKUs publish beside <c>confidence</c> — what the
    /// number rests on, in words an agent can act on, derived from the finding's own shape so that a
    /// row persisted before this change is labelled as such rather than read as a corroboration score.
    /// One sentence per arm; the arms are: the two by-construction 1.0s (named by root key), a legacy
    /// path-shape row, and a corroboration-derived value.
    /// </summary>
    public static string DescribeBasis(string? rootFactKey, double confidence, int factCount)
    {
        if (string.Equals(rootFactKey, AbsolutionRootKey, StringComparison.Ordinal))
            return "absolution: every fact was scored and none rooted a finding; 1.0 by construction, not an evidence score.";

        if (string.Equals(rootFactKey, PileupRootKey, StringComparison.Ordinal))
            return "detector-measured: the same-statement pileup is observed directly (concurrent sessions on one statement against that statement's own duration baseline), so it carries 1.0 by construction rather than by the corroboration formula.";

        if (IsLegacyPathShape(confidence, factCount))
            return "path-shape (pre-#3538): this row was persisted when confidence was (n-1)/n over the story path with a lone symptom at 1.0 — a path-length statistic, NOT an evidence score; it is not comparable to corroboration-derived values and a lone-symptom 1.0 here means UNCORROBORATED.";

        var n = Math.Max(1, factCount);
        return $"corroboration (#3538): 0.20 for the fired symptom + up to 0.48 for the share of the root fact's amplifier checks that matched + up to 0.32 for path depth ((n-1)/n over the {n}-node story path; a root with no amplifier catalogue earns the full 0.80 from path depth). A lone uncorroborated symptom scores 0.20; a fully corroborated deep chain approaches but never reaches 1.0.";
    }
}
