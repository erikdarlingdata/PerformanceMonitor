using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Greedy traversal engine that builds analysis stories from scored facts
/// and the relationship graph.
///
/// Algorithm:
/// 1. Start at the highest-severity fact as entry point
/// 2. Evaluate all edge predicates from current node
/// 3. Follow edge to highest-severity destination (that hasn't been visited)
/// 4. Repeat until leaf (no active edges or all destinations visited)
/// 5. The path IS the story
/// 6. Mark traversed facts as consumed, repeat from next highest-severity
/// 7. Stop when remaining facts are below 0.5 severity
/// </summary>
public class InferenceEngine
{
    private const double MinimumSeverityThreshold = 0.5;
    private const int MaxPathDepth = 10; // Safety limit

    /// <summary>
    /// Config-advisory fact keys that root a finding at ANY positive severity, bypassing the
    /// MinimumSeverityThreshold. A standing misconfiguration (RCSI off, auto-shrink on, MAXDOP at
    /// a silly default) is an advisory the operator should see regardless of current load — unlike
    /// an incident fact, which must clear 0.5 to be worth surfacing. The existing severity-ordered
    /// <c>consumed</c> traversal still suppresses duplicates: a higher-severity incident story that
    /// consumes the config fact (e.g. CXPACKET → CONFIG_MAXDOP, or LCK_M_S → DB_CONFIG) wins, and
    /// only an UN-consumed config fact roots a standalone recommendation.
    /// </summary>
    private static readonly HashSet<string> ConfigAdvisoryRootKeys = new(StringComparer.Ordinal)
    {
        "DB_CONFIG",
        "SERVER_CONFIG",
        "FILE_AUTOGROWTH_PERCENT",
        // WS3: bad server-level config. Each per-setting CONFIG_* fact scores 0.4 ONLY when
        // bad (FactScorer) and roots its own standalone advisory card here, bypassing the 0.5
        // incident threshold — a standing misconfig should surface on a quiet, healthy server.
        "CONFIG_MAXDOP",
        "CONFIG_CTFP",
        "CONFIG_MAX_MEMORY_MB",
        "CONFIG_MIN_MAX_MEMORY_NARROW",
        // batch-2b: priority boost / lightweight pooling enabled — Dashboard WARNINGs. Score the
        // WARNING band (0.9, FactScorer) and root a standalone card here so a rare, clearly-wrong
        // scheduling setting surfaces on a quiet, healthy server (redundant with the 0.5 incident
        // threshold at 0.9, but keeps every CONFIG_* fact in one rooting set).
        "CONFIG_PRIORITY_BOOST",
        "CONFIG_LIGHTWEIGHT_POOLING",
        // WS5: server-health advisories (advise-only). Each scores its 0.4 advisory base only when
        // bad (FactScorer) and roots its own standalone card here, bypassing the 0.5 incident
        // threshold — a standing server-health gap should surface on a quiet, healthy server.
        "CONFIG_IFI_DISABLED",
        "CONFIG_LPIM_DISABLED",
        "SERVER_MEMORY_DUMPS",
        // WS4: plan-XML advisories (advise-only) parsed from the top collected query plans —
        // missing indexes and actionable plan warnings each root their own standalone advisory card.
        "MISSING_INDEX",
        "PLAN_WARNING",
        // Tier-2: plan-cache single-use bloat is a STANDING cache-composition state (not a momentary
        // incident), so it roots its own standalone advisory card at any positive severity — it should
        // surface on a quiet, healthy server the same way a standing misconfig does. The FactScorer only
        // scores it at >= 0.5 (MEDIUM+) behind a size guard, so this is belt-and-suspenders with the 0.5
        // incident threshold, and keeps the standing-state key in the same rooting set as the others.
        "PLAN_CACHE_BLOAT",
    };

    private readonly RelationshipGraph _graph;

    public InferenceEngine(RelationshipGraph graph)
    {
        _graph = graph;
    }

    /// <summary>
    /// Builds analysis stories by traversing the relationship graph
    /// starting from the highest-severity facts.
    /// </summary>
    public List<AnalysisStory> BuildStories(List<Fact> facts)
    {
        var stories = new List<AnalysisStory>();
        var factsByKey = facts
            .Where(f => f.Severity > 0)
            .ToFactLookup();
        var consumed = new HashSet<string>();

        // Process facts in severity order. Incident facts must clear the 0.5 threshold to root;
        // config-advisory facts (DB_CONFIG/SERVER_CONFIG) root at any positive severity so a
        // standing misconfig surfaces on a quiet, healthy server (it would otherwise never reach
        // 0.5 without contention — e.g. RCSI-off is base 0.3). Severity ordering + `consumed`
        // (below) keep an incident story from being shadowed by, or duplicating, its config leaf.
        var entryPoints = facts
            .Where(f => f.Severity >= MinimumSeverityThreshold
                     || (ConfigAdvisoryRootKeys.Contains(f.Key) && f.Severity > 0))
            .OrderByDescending(f => f.Severity)
            .ToList();

        foreach (var entryFact in entryPoints)
        {
            if (consumed.Contains(entryFact.Key))
                continue;

            var path = Traverse(entryFact.Key, factsByKey, consumed);

            // Mark all facts in this path as consumed
            foreach (var node in path)
                consumed.Add(node);

            var story = BuildStory(path, factsByKey);
            stories.Add(story);
        }

        // Check for absolution — if no stories were generated at all
        if (stories.Count == 0 && facts.Count > 0)
        {
            stories.Add(new AnalysisStory
            {
                RootFactKey = "server_health",
                RootFactValue = 0,
                Severity = 0,
                // 1.0 by construction, not by evidence arithmetic: an absolution is the statement that
                // every fact was scored and none rooted, which is exactly as certain as the scoring
                // pass itself. StoryConfidence.DescribeBasis names this arm so a reader never mistakes
                // it for the legacy path-shape 1.0 (#3538 A6).
                Confidence = 1.0,
                Category = "absolution",
                Path = ["server_health"],
                StoryPath = "server_health",
                StoryPathHash = ComputeHash("server_health"),
                StoryText = string.Empty,
                IsAbsolution = true
            });
        }

        return stories;
    }

    /// <summary>
    /// Greedy traversal from an entry point through the relationship graph.
    /// Returns the path as a list of fact keys.
    /// </summary>
    private List<string> Traverse(string startKey,
        Dictionary<string, Fact> factsByKey,
        HashSet<string> consumed)
    {
        var path = new List<string> { startKey };
        var visited = new HashSet<string> { startKey };
        var current = startKey;

        for (var depth = 0; depth < MaxPathDepth; depth++)
        {
            var activeEdges = _graph.GetActiveEdges(current, factsByKey);

            // Filter to destinations not already in this path and not consumed by prior stories
            var candidates = activeEdges
                .Where(e => !visited.Contains(e.Destination) && !consumed.Contains(e.Destination))
                .Where(e => factsByKey.ContainsKey(e.Destination))
                .OrderByDescending(e => factsByKey[e.Destination].Severity)
                .ToList();

            if (candidates.Count == 0)
                break; // Leaf node — no more edges to follow

            var best = candidates[0];
            path.Add(best.Destination);
            visited.Add(best.Destination);
            current = best.Destination;
        }

        return path;
    }

    /// <summary>
    /// Builds an AnalysisStory from a traversal path.
    /// </summary>
    private static AnalysisStory BuildStory(List<string> path, Dictionary<string, Fact> factsByKey)
    {
        var rootFact = factsByKey.GetValueOrDefault(path[0]);
        var leafKey = path.Count > 1 ? path[^1] : null;
        var leafFact = leafKey != null ? factsByKey.GetValueOrDefault(leafKey) : null;

        // Attribute THREADPOOL (thread exhaustion) by its co-elevated cause, so the finding's ROOT
        // KEY — the thing that persists and that FactAdvice is keyed on — carries parallel-vs-blocking.
        // Only parallel-driven exhaustion is a MAXDOP/CTFP problem; blocking-driven exhaustion pins
        // workers on blocked (not running) sessions and is fixed by clearing the blocking. The fact
        // object keeps its "THREADPOOL" key (relationship graph + amplifiers untouched); only the
        // finding's root key and story path are relabeled to the attribution variant.
        var rootKey = path[0] == "THREADPOOL" ? ClassifyThreadpool(factsByKey) : path[0];
        var effectivePath = rootKey == path[0]
            ? path
            : path.Select((k, i) => i == 0 ? rootKey : k).ToList();

        var storyPath = string.Join(" → ", effectivePath);
        var category = rootFact?.Source ?? "unknown";

        // Confidence is an EVIDENCE statistic (#3538 A6): how much of the corroboration the engine knows
        // to look for around this root actually showed up — the root fact's matched amplifier share plus
        // the depth of the graph path it traversed. See StoryConfidence for the formula, the worked
        // examples, and why a lone uncorroborated symptom now scores LOW rather than 1.0.
        var confidence = StoryConfidence.Compute(rootFact, path.Count);

        return new AnalysisStory
        {
            RootFactKey = rootKey,
            // RootFactValue/LeafFactValue carry the fact's RAW collected value (the setting/metric —
            // MAXDOP 0, a wait's fraction-of-period, etc.), NOT its severity. Severity has its own
            // field below. These feed the MCP root_fact.value / leaf_fact.value contract and the
            // notification headline; conflating them with severity made an MCP payload report
            // "MAXDOP is 0" next to value 0.4 (the severity).
            RootFactValue = rootFact?.Value ?? 0,
            Severity = rootFact?.Severity ?? 0,
            Confidence = confidence,
            Category = category,
            Path = effectivePath,
            StoryPath = storyPath,
            StoryPathHash = ComputeHash(storyPath),
            StoryText = string.Empty,
            LeafFactKey = leafKey,
            LeafFactValue = leafFact?.Value,
            FactCount = path.Count,
            IsAbsolution = false,
            RootFactMetadata = rootFact?.Metadata,
            // Carry the root fact's database through so findings/recommendation cards can show it.
            DatabaseName = rootFact?.DatabaseName
        };
    }

    /// <summary>
    /// Classifies a THREADPOOL (thread-exhaustion) root by its co-elevated cause so the persisted,
    /// read-back root key carries the attribution that FactAdvice routes on. Parallel-driven
    /// (CXPACKET / high-DOP co-fired) is the only flavor MAXDOP/CTFP fixes; blocking-driven
    /// (blocked sessions pinning workers) is fixed by clearing the blocking. Mirrors the THREADPOOL
    /// amplifier peers (CXPACKET, blocking/LCK). Returns the generic "THREADPOOL" when neither
    /// co-fired (unattributed — the advice then tells the operator how to tell which it is).
    /// factsByKey here contains only facts that scored above zero, so presence == fired.
    /// </summary>
    private static string ClassifyThreadpool(Dictionary<string, Fact> factsByKey)
    {
        var parallel = factsByKey.ContainsKey("CXPACKET")
                    || factsByKey.ContainsKey("QUERY_HIGH_DOP");
        var blocking = factsByKey.ContainsKey("BLOCKING_EVENTS")
                    || factsByKey.ContainsKey("BLOCKING_CHAIN")
                    || factsByKey.ContainsKey("LCK");
        return (parallel, blocking) switch
        {
            (true, true) => "THREADPOOL_MIXED",
            (true, false) => "THREADPOOL_PARALLEL",
            (false, true) => "THREADPOOL_BLOCKING",
            _ => "THREADPOOL"
        };
    }

    /// <summary>
    /// Groups a run's stories into INCIDENTS — sets of stories that are causally related — via
    /// connected components over the relationship graph's ACTIVE edges (correlate-and-focus). The
    /// greedy traversal in <see cref="BuildStories"/> only follows the single highest-severity edge
    /// from each node, so it splits one root cause across several stories (e.g. a PLAN_REGRESSION that
    /// is really a facet of the CPU incident); this re-merges them. Two graph families merge only when
    /// a bridge edge actually fires (THREADPOOL→LCK when both are elevated = one blocking-driven
    /// thread-exhaustion incident), and a genuinely-independent finding (a standalone disk advisory
    /// with no active edge to anything present) stays its own incident.
    ///
    /// <para>Absolution stories are excluded. Returns one list per incident; a lone story is a
    /// one-member incident. The fact a story OWNS is its <see cref="AnalysisStory.Path"/> keys,
    /// normalized for the THREADPOOL relabel (the story path carries THREADPOOL_PARALLEL etc. while
    /// the graph + facts use the base THREADPOOL key — see <see cref="ClassifyThreadpool"/>).</para>
    /// </summary>
    public List<List<AnalysisStory>> ClusterIntoIncidents(IReadOnlyList<AnalysisStory> stories, List<Fact> facts)
    {
        var incidentStories = (stories ?? [])
            .Where(s => s is not null && !s.IsAbsolution)
            .ToList();
        if (incidentStories.Count <= 1)
            return incidentStories.Select(s => new List<AnalysisStory> { s }).ToList();

        // Same >0 working set BuildStories used, so the edge predicates evaluate identically.
        var factsByKey = (facts ?? []).Where(f => f.Severity > 0).ToFactLookup();

        // Each fact (normalized) is consumed by exactly one story; map it to that story's index.
        var owner = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < incidentStories.Count; i++)
            foreach (var key in incidentStories[i].Path)
                owner[NormalizeKey(key)] = i;

        // Union-find over story indices.
        var parent = Enumerable.Range(0, incidentStories.Count).ToArray();
        int Find(int x) { while (parent[x] != x) { parent[x] = parent[parent[x]]; x = parent[x]; } return x; }
        void Union(int a, int b) { var ra = Find(a); var rb = Find(b); if (ra != rb) parent[ra] = rb; }

        for (var i = 0; i < incidentStories.Count; i++)
            foreach (var key in incidentStories[i].Path)
                foreach (var edge in _graph.GetActiveEdges(NormalizeKey(key), factsByKey))
                    if (owner.TryGetValue(NormalizeKey(edge.Destination), out var j) && j != i
                        && CanUnionAcrossDatabase(incidentStories[i], incidentStories[j]))
                        Union(i, j);

        var components = new Dictionary<int, List<AnalysisStory>>();
        for (var i = 0; i < incidentStories.Count; i++)
        {
            var root = Find(i);
            if (!components.TryGetValue(root, out var list))
                components[root] = list = new List<AnalysisStory>();
            list.Add(incidentStories[i]);
        }
        return components.Values.ToList();
    }

    /// <summary>
    /// Guards the incident union against merging two DB-scoped findings from DIFFERENT databases
    /// (correlate-and-focus DB-awareness — vetted option i). A server-scoped story (no DatabaseName)
    /// bridges freely, so a server-wide symptom (CPU, waits) still correlates with a db-scoped cause;
    /// only two stories that EACH name a database, and name DIFFERENT ones, are held apart. This is
    /// the structural fix that stops a db1 finding and a db2 finding from being fingerprinted into one
    /// cross-database incident (the anomaly-fold reconciler is DB-aware for the same reason). It does
    /// not over-fragment same-database incidents: same-DB and server-scoped pairs still union.
    /// </summary>
    private static bool CanUnionAcrossDatabase(AnalysisStory a, AnalysisStory b)
    {
        if (string.IsNullOrEmpty(a.DatabaseName) || string.IsNullOrEmpty(b.DatabaseName))
            return true;
        return string.Equals(a.DatabaseName, b.DatabaseName, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Maps a relabeled THREADPOOL story root (THREADPOOL_PARALLEL / _BLOCKING / _MIXED) back to the
    /// base THREADPOOL key the relationship graph and fact set use, so clustering sees its edges.
    /// Every other key passes through unchanged.
    /// </summary>
    private static string NormalizeKey(string key) =>
        key.StartsWith("THREADPOOL_", StringComparison.Ordinal) ? "THREADPOOL" : key;

    /// <summary>
    /// Stable hash for story path deduplication and muting.
    /// </summary>
    private static string ComputeHash(string storyPath)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(storyPath));
        return Convert.ToHexString(bytes).ToLowerInvariant()[..16];
    }
}

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
/// chain (the traversal depth cap) with no catalogue: 0.92; with a fully matched catalogue: 0.968 — the
/// two ceilings, because path depth never reaches 1. Nothing built by this formula is ever exactly 1.0,
/// and it is monotonic by construction: one more matched amplifier or one more path node never lowers
/// it.</para>
///
/// <para><b>Legacy rows without a schema change.</b> The persisted <c>confidence</c> column keeps its
/// name and pre-change rows keep their values; a migration rung to stamp a version marker was ruled
/// out (one un-landed rung at a time, repo-wide). Instead the basis is RE-DERIVED at read time from the
/// value and the path length, which is sound because the two formulas' ranges are disjoint at every
/// path length: the legacy value for n nodes is exactly 1.0 (n = 1) or (n − 1)/n, and this formula
/// never lands on that number for the same n with any small-integer amplifier catalogue (pinned
/// exhaustively over n ≤ 10, catalogue ≤ 12). <see cref="DescribeBasis"/> therefore labels a
/// value that equals its path-shape number as <c>path-shape (pre-#3538)</c> and everything else as
/// corroboration-derived, and the two built-by-construction 1.0s (absolution, the same-statement
/// pileup detector) are named by their root key so they are never mislabelled as legacy.</para>
/// </summary>
public static class StoryConfidence
{
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
