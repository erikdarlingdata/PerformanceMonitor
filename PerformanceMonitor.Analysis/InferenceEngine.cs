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
    /// <summary>Hops the greedy traversal will follow from a root before it stops (a safety limit); the
    /// longest story path is therefore this plus the root — <see cref="StoryConfidence.MaxPathNodes"/>.</summary>
    internal const int MaxPathDepth = 10;

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
        // #3653 A10 (Q2): a server configuration change observed inside the pass window, with the ±4 h
        // before/after compare frozen on the fact. Rooted at ConfigChangeAttribution.InformationSeverity
        // (0.25) — INFO by the readers' band table, below every standing-misconfiguration advisory — so
        // it can NEVER reach the 0.5 incident threshold and must be listed here to become a story at all.
        // Explicitly, not by a prefix rule: the fact is built after ScoreAll (the scorer would zero an
        // unknown "config" key) with its severity preset, and this line is the one that says "and it
        // roots". It has no graph edges, so it is always a one-node story and its own incident; the
        // finding attributes, it does not accuse, and nothing folds onto it.
        ConfigChangeAttribution.FactKey,
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
        /* #3542: the PostgreSQL-target advisory roots live in PgTargetFactKeys.ConfigAdvisoryRoots (the
           CONFIG_PG_* convention checks and the pg_posture keys, D5/D6) and are consulted through this one
           delegating check, so the content lanes register a root in the shared keys file and never edit
           this set again. */
        var entryPoints = facts
            .Where(f => f.Severity >= MinimumSeverityThreshold
                     || (IsConfigAdvisoryRoot(f.Key) && f.Severity > 0))
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

            /* #3691: the config levers hanging off this path that the single-edge walk could not reach. Swept
               AFTER the path is consumed (so the "not on the path" test is the same `consumed` lookup) and
               BEFORE BuildStory, which carries them on the story beside the path. */
            var sideLeafKeys = SweepSideLeaves(path, factsByKey, consumed);

            var story = BuildStory(path, factsByKey, sideLeafKeys);
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
    /// Whether a fact key roots a standalone advisory card at ANY positive severity — the union of this engine's
    /// <see cref="ConfigAdvisoryRootKeys"/> and the PostgreSQL-target arm
    /// (<see cref="PgTargetFactKeys.IsConfigAdvisoryRoot"/>). This is the ENTRY-POINT question only: may this fact
    /// root below the 0.5 incident line? <see cref="IsConfigAdvisoryFact"/> is the different, wider question the
    /// side-leaf sweep asks.
    /// </summary>
    private static bool IsConfigAdvisoryRoot(string key) =>
        ConfigAdvisoryRootKeys.Contains(key) || PgTargetFactKeys.IsConfigAdvisoryRoot(key);

    /// <summary>
    /// Whether a fact is a CONFIG ADVISORY — a standing setting whose card recommends changing a knob — as opposed
    /// to an incident fact, which reports something that happened. This is the class
    /// <see cref="SweepSideLeaves"/> may consume, and it is deliberately WIDER than
    /// <see cref="IsConfigAdvisoryRoot"/>.
    ///
    /// <para><b>Why the two differ, which is the subtle part of this change.</b> The root set answers "may this
    /// fact root BELOW 0.5?" and its membership follows D5: a CONVENTION check (a knob at its shipped default) may
    /// root a card on a quiet server; an EVIDENCE-gated check (<c>work_mem</c>, <c>maintenance_work_mem</c>) may
    /// not, because it scores above zero only when its workload co-fire exists. But an evidence-gated knob that
    /// DID get its evidence is exactly the orphan this lane exists to fix:
    /// <c>CONFIG_PG_MAINT_WORK_MEM</c> takes the 0.4 advisory base from the stamped backlog ratio and the backlog
    /// co-fire amplifier lifts it to 0.6 — past the ORDINARY 0.5 threshold — so it roots its own one-node card
    /// without ever consulting the advisory-root set, which is why the defect in the face shows that key and not a
    /// convention one. Testing membership of the root set here would have missed the reported scenario entirely.</para>
    ///
    /// <para><b>The test used instead, and why it is safe.</b> An advisory fact is one whose SOURCE is a
    /// settings source — <see cref="PgTargetSources.ConfigSource"/> (<c>pg_config</c>),
    /// <see cref="PgTargetSources.PostureSource"/> (<c>pg_posture</c>), or the SQL Server <c>config</c> /
    /// <c>database_config</c> collectors — UNION the advisory-root set, which catches the keys whose card is an
    /// advisory but whose fact rides a measurement source (<c>MISSING_INDEX</c> and <c>PLAN_WARNING</c> on
    /// <c>queries</c>, <c>PLAN_CACHE_BLOAT</c> on <c>memory</c>, <c>CONFIG_PG_AUTOVACUUM_DISABLED</c> on
    /// <c>pg_vacuum</c>). A source is the collector's own statement about what kind of thing it read, so this
    /// cannot drift the way a hand-maintained key list does — and it cannot capture an incident by accident: no
    /// measured symptom is emitted on a settings source (verified over every emission site).</para>
    /// </summary>
    private static bool IsConfigAdvisoryFact(Fact fact) =>
        fact.Source is "config" or "database_config"
        || fact.Source == PgTargetSources.ConfigSource
        || fact.Source == PgTargetSources.PostureSource
        || IsConfigAdvisoryRoot(fact.Key);

    /// <summary>
    /// Collects the CONFIG-ADVISORY facts that hang off any node on <paramref name="path"/> by an ACTIVE edge the
    /// greedy walk did not follow, and marks them consumed so they no longer root a card of their own (#3691).
    ///
    /// <para><b>The defect this closes.</b> <see cref="Traverse"/> follows the SINGLE highest-severity active edge
    /// from each node. A node mid-path can have two: the vacuum backlog reaches the wraparound trend (higher, and
    /// the rest of the chain behind it) and also <c>CONFIG_PG_MAINT_WORK_MEM</c> — the dead-tuple memory that bounds
    /// how much of the backlog one pass can clear, i.e. the lever that would relieve the very thing the story is
    /// about. The walk never visits the lever, so nothing consumes it, and because a config-advisory key roots at
    /// any positive severity (<see cref="IsConfigAdvisoryRoot"/>) it then rooted its own one-node story at 0.6 NEXT
    /// TO the incident. The reader saw two cards where the truth is one story with a lever.</para>
    ///
    /// <para><b>Why only config-advisory destinations</b> (<see cref="IsConfigAdvisoryFact"/>, whose doc explains
    /// why that test is by SOURCE and not by advisory-root membership). An incident-class side destination is a
    /// DIFFERENT story that must keep its own root — it has its own symptom, its own advice and its own occurrence
    /// history, and <see cref="ClusterIntoIncidents"/> is the mechanism that re-merges it into one incident without
    /// taking its card away. A config advisory has no incident of its own: it is a standing setting whose whole
    /// claim to a card was that nobody else had said it. Widening this to any destination would silently delete
    /// findings, which is why the class test is the one thing in this method that must not be loosened casually.</para>
    ///
    /// <para><b>What it does not do.</b> It does not extend the path (the story's
    /// <see cref="AnalysisStory.StoryPath"/> and its hash are untouched, so incident identity is stable and every
    /// existing pin holds), it does not lift severity or confidence (context, not corroboration), and it follows no
    /// edges OUT of a side leaf — a lever is a leaf by construction, and a walk from it would be the second path
    /// this method exists to avoid inventing. Edge activity is read through
    /// <see cref="RelationshipGraph.GetActiveEdges"/>, the same call the traversal makes, so the bad-actor alias
    /// resolution and every predicate behave identically here.</para>
    ///
    /// <para>Returns highest severity first, ties by ordinal key, so a pass is deterministic. Empty — the common
    /// case — leaves the story byte-identical to what it was.</para>
    /// </summary>
    private List<string> SweepSideLeaves(List<string> path,
        Dictionary<string, Fact> factsByKey,
        HashSet<string> consumed)
    {
        List<string>? sideLeaves = null;

        foreach (var node in path)
        {
            foreach (var edge in _graph.GetActiveEdges(node, factsByKey))
            {
                var destination = edge.Destination;

                /* `consumed` already holds this path (the caller marked it before calling) and every prior
                   story's facts, so this one lookup is both "not on the path" and "not consumed" — and, as the
                   loop adds each leaf below, "not already swept from an earlier node on this same path". */
                if (consumed.Contains(destination))
                    continue;
                if (!factsByKey.TryGetValue(destination, out var fact) || fact.Severity <= 0)
                    continue;
                if (!IsConfigAdvisoryFact(fact))
                    continue;

                (sideLeaves ??= []).Add(destination);
                consumed.Add(destination);
            }
        }

        if (sideLeaves is null)
            return [];

        return sideLeaves
            .OrderByDescending(k => factsByKey[k].Severity)
            .ThenBy(k => k, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>
    /// Builds an AnalysisStory from a traversal path and the config levers swept off it
    /// (<paramref name="sideLeafKeys"/>, see <see cref="SweepSideLeaves"/>).
    /// </summary>
    private static AnalysisStory BuildStory(List<string> path, Dictionary<string, Fact> factsByKey, List<string> sideLeafKeys)
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

        /* The RENDERED path — a display string, and (through its hash) the mute/occurrence identity. #3859: it is
           no longer anybody's SOURCE for the fact keys. The typed effectivePath rides to the finding as PathKeys,
           so the drill-down collectors and next_tools read keys instead of splitting this string on the arrow at
           six sites; a key containing the arrow, or a change to this separator, used to corrupt all six silently.
           Change this join and the payload's story_path changes — which the SQL Server exit checks pin — but no
           consumer's key matching moves with it. */
        var storyPath = string.Join(" → ", effectivePath);
        var category = rootFact?.Source ?? "unknown";

        // Confidence is an EVIDENCE statistic (#3538 A6): how much of the corroboration the engine knows
        // to look for around this root actually showed up — the root fact's matched amplifier share plus
        // the depth of the graph path it traversed. See StoryConfidence for the formula, the worked
        // examples, and why a lone uncorroborated symptom now scores LOW rather than 1.0.
        var confidence = StoryConfidence.Compute(rootFact, path.Count);

        /* #3712: the two corroboration COMPONENTS the confidence just folded together, kept apart on the
           story so the notification layer can route on them by name. Read here, at the one place the root
           fact is in hand, rather than re-derived from the scalar downstream: the inverse of the formula
           is exact today (a lone symptom above the 0.20 floor has a matched check) and stops being exact
           the day a weight moves, which is the silent-bar-shift design point 1 of #3712 forbids. */
        var amplifierResults = rootFact?.AmplifierResults;
        var definedAmplifiers = amplifierResults?.Count ?? 0;
        var matchedAmplifiers = amplifierResults?.Count(r => r.Matched) ?? 0;

        /* #3691: the hops this traversal CONSUMED whose fact names a thing, so the root's card can say who was
           behind it. Consumption is the whole reason: a PG_IDLE_IN_TRANSACTION at 1.2 walked under a saturation
           root at 1.25 roots no card of its own, and the holder it named (application, role, database) reached no
           surface at all — the exit check's grep over the payload found nothing. Recorded HERE, off the raw path
           (the THREADPOOL relabel touches only the root, and the root is excluded — its own advice names it), from
           the same >0 working set the traversal walked; FactAdvice.PopulateStoryText renders the sentences.
           Which keys name something is FactIdentity's roster, per engine, so this line is engine-neutral. */
        var namedHops = FactIdentity.NamedHopsOf(path, factsByKey);

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
            MatchedAmplifiers = matchedAmplifiers,
            DefinedAmplifiers = definedAmplifiers,
            IsAbsolution = false,
            RootFactMetadata = rootFact?.Metadata,
            /* #3691 lane 43: the root fact's ranked objects, beside its metadata and for the same consumer
               shape — the payload names them; nothing here re-sorts or trims the collector's rank. */
            RootFactRanked = rootFact?.Ranked ?? [],
            // Carry the root fact's database through so findings/recommendation cards can show it.
            DatabaseName = rootFact?.DatabaseName,
            NamedHops = namedHops,
            /* #3691: beside the path, not in it — FactCount above still counts the path, and the hash above is
               computed from the path alone, so this story's identity is what it was. */
            SideLeafKeys = sideLeafKeys
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
