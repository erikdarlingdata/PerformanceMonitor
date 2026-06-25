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

        // Confidence = what fraction of edge destinations had matching facts
        // For single-node paths, confidence is 1.0 (we found the symptom, just no deeper cause)
        var confidence = path.Count == 1 ? 1.0 : (path.Count - 1.0) / path.Count;

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
                    if (owner.TryGetValue(NormalizeKey(edge.Destination), out var j) && j != i)
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
