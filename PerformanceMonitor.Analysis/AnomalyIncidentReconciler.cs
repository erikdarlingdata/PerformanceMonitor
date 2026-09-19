/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Reconciles each surviving <c>ANOMALY_*</c> story's <see cref="AnalysisStory.IncidentId"/> onto the
/// REGULAR (non-anomaly) finding that describes the SAME symptom in the SAME run, so the anomaly folds
/// into that incident instead of rendering as its own card (viewer) and its own e-mail
/// (<c>AnalysisNotificationService</c>). This runs AFTER
/// <see cref="InferenceEngine.ClusterIntoIncidents"/> + <see cref="IncidentId.StampClusters"/> and
/// BEFORE mute-filtering, at the identical wiring site in all three analysis services.
///
/// <para><b>Fold, don't suppress, don't add graph edges</b> (Round-2 vetted design). Suppression is
/// rejected — the anomaly carries the vs-baseline delta the regular fact does not, catches
/// sub-threshold regressions, and covers no-regular-path waits (PAGELATCH_*/ASYNC_NETWORK_IO/…).
/// Broad <see cref="RelationshipGraph"/> edges are rejected — union-find over the existing THREADPOOL
/// bridge over-merges unrelated incidents. Reconciling the id keeps every finding while collapsing the
/// duplicate presentation: nothing is dropped, only the incident tag is rewritten.</para>
///
/// <para><b>Symptom-family map</b> mirrors the collector's grouping
/// (<see cref="FactCollectorHelpers.WaitFamilyKey"/> / <see cref="FactCollectorHelpers.IsGeneralLockWait"/>)
/// exactly. <c>ANOMALY_WAIT_PROFILE</c> resolves to the family of its DOMINANT <c>contrib_&lt;TYPE&gt;</c>
/// driver (Stage A made the whole wait profile one fact, so this is a clean single lookup).
/// <c>ANOMALY_CPU_SPIKE</c> accepts EITHER CPU family target — the sustained <c>CPU_SQL_PERCENT</c> or
/// the burst <c>CPU_SPIKE</c> — since a run may key its CPU story on either. Anomalies with no regular
/// counterpart (batch / session / query-duration, and the db-scoped <c>ANOMALY_OBJECT_*</c> pair) are
/// unmapped and always stay solo.</para>
///
/// <para><b>Fold targets are indexed by EVERY fact key a regular story consumed</b> — its whole
/// <see cref="AnalysisStory.Path"/>, not just its <see cref="AnalysisStory.RootFactKey"/>. A mapped
/// family key (<c>CPU_SQL_PERCENT</c>, <c>RESOURCE_SEMAPHORE</c>, <c>BLOCKING_EVENTS</c>, …) is
/// frequently a MID/LEAF member of a larger story when a higher-severity neighbour roots it
/// (<c>SOS_SCHEDULER_YIELD → CPU_SQL_PERCENT</c>, <c>PAGEIOLATCH_* → RESOURCE_SEMAPHORE</c>,
/// <c>THREADPOOL/LCK → BLOCKING_EVENTS</c>); in those common correlated runs NO story is ROOTED on the
/// family key, so root-only indexing would miss the fold exactly where dedup was most wanted.
/// <see cref="InferenceEngine.BuildStories"/> consumes each fact into exactly ONE story (its
/// <c>consumed</c> set), so a given family key belongs to at most one story's path — the mapping is
/// unambiguous and cannot over-fold onto an unrelated story.</para>
///
/// <para><b>Database-aware.</b> The fold only happens when a regular parent with the matching family
/// key exists in the SAME database (server-scoped findings match on the empty-string database). A
/// db-scoped <c>ANOMALY_OBJECT_*</c> can therefore never fold into a finding in another database, and
/// two different databases never share a folded incident.</para>
///
/// <para><b>The maintenance arm (#3704).</b> #3538 A9 (PR #3632) gave the graph three edges —
/// <c>SCH_M</c>, <c>IO_WRITE_LATENCY_MS</c> and <c>WRITELOG</c> onto <c>RUNNING_JOBS</c>, each gated on
/// the job having FIRED — so a maintenance window's REGULAR cards cluster into one incident. Those
/// edges are keyed on the regular symptom facts, and an anomaly is exactly the case where the regular
/// symptom did NOT fire: write latency or log flushes up against the server's own baseline while the
/// absolute threshold held. So on a server running its own index-maintenance job for two hours, the
/// engine produced four one-fact <c>ANOMALY_*</c> stories, none with a same-family parent to fold into,
/// and paged four times without naming the job. The fix is a conditional entry in the FOLD-TARGET set,
/// not a graph edge (the Round-2 rationale above stands): when an anomaly's resolved family is one of
/// the three maintenance symptoms (<see cref="MaintenanceFamilies"/>), <c>RUNNING_JOBS</c> joins its
/// candidate list LAST, so it folds onto the regular story that consumed the job — the job-rooted story,
/// or a symptom story that reached it through #3632's edge. The fired-gate is structural rather than
/// re-evaluated: a story carries <c>RUNNING_JOBS</c> in its <see cref="AnalysisStory.Path"/> only when
/// the fact scored above zero (<see cref="InferenceEngine.BuildStories"/> roots at 0.5 or better, and
/// the edges' predicate is <c>BaseSeverity &gt; 0</c>), and <c>FactScorer.ScoreJobFact</c> scores the
/// running-long count on a (1, 3) ramp, so ONE job running long is base 0.5 — which is both "fired" and
/// "roots a story". A present-but-quiet job (running, not running long) scores 0, drops out of the
/// working set, roots nothing and is consumed by nothing, so no story carries it and the anomalies stay
/// exactly as they were. There is therefore no "the job fired but no story carries it" case for the
/// reconciler to mint an incident for. Anomalies outside the three families (CPU, memory, blocking,
/// deadlocks, read latency, batch/session/query-duration, the db-scoped object pair) never gain the
/// job as a target: #3632 declined those edges deliberately — a CPU spike during a rebuild is not the
/// rebuild's card — and this arm honours the same line.</para>
///
/// <para><b>The folded anomaly names the job.</b> The incident's e-mail is led by its highest-severity
/// member and the viewer/MCP cards render each finding's own frozen <see cref="AnalysisStory.StoryText"/>;
/// the job story scores 0.5 for one long job while a fired anomaly scores 0.5–1.0 and amplifies from
/// there, so the anomaly ordinarily leads and the job card may sit under the notify threshold and never
/// reach the e-mail at all. The name lives on the <c>RUNNING_JOBS</c> fact's <see cref="Fact.ObjectName"/> (#3653,
/// PR #3693) and reaches the operator only through composed advice frozen into StoryText, so when the
/// caller passes the run's facts and a maintenance-family anomaly lands on the job's incident, one
/// sentence naming the job is appended to the anomaly's frozen Investigation (read back with
/// <see cref="FactAdvice.TryReadStoryText"/>, re-frozen with <see cref="FactAdvice.SerializeForStoryText"/>
/// — the same <c>{h,i,r}</c> blob every card deserializes, so no renderer changes). The headline is
/// left alone: it is <c>FactAdvice</c>'s arm and the anomaly's own measurement, and the e-mail's
/// "Co-fired in this incident" line names the job card whenever it clears the threshold.</para>
///
/// <para>Stateless — mutates only <see cref="AnalysisStory.IncidentId"/> and, for the maintenance fold
/// with facts supplied, <see cref="AnalysisStory.StoryText"/> on the passed stories, so it is directly
/// unit-testable without any collector or store. The one-argument <c>Reconcile</c> is the pre-#3704
/// signature, kept for the frozen <c>deprecated/Dashboard</c> twin: it folds by the same rule and
/// writes no sentence, because the sentence needs the fact.</para>
/// </summary>
public static class AnomalyIncidentReconciler
{
    /// <summary>
    /// Maps a server-scoped <c>ANOMALY_*</c> root key to the REGULAR fact key(s) — symptom family —
    /// that describe the same event, in priority order (the anomaly folds into the FIRST family that
    /// has a same-database parent this run). Most anomalies have a single family; <c>ANOMALY_CPU_SPIKE</c>
    /// accepts either the sustained <c>CPU_SQL_PERCENT</c> or the burst <c>CPU_SPIKE</c>, since a run may
    /// key its CPU story on either detector. <c>ANOMALY_WAIT_PROFILE</c> is resolved separately from its
    /// dominant contributor (<see cref="ResolveFamilies"/>). Anomaly keys absent from this map —
    /// batch-request, session, query-duration spikes, and the db-scoped <c>ANOMALY_OBJECT_GROWTH</c> /
    /// <c>ANOMALY_OBJECT_CONTENTION</c> — have no regular counterpart and always stay solo.
    /// </summary>
    private static readonly Dictionary<string, string[]> AnomalyToFamilies = new(StringComparer.Ordinal)
    {
        ["ANOMALY_CPU_SPIKE"] = ["CPU_SQL_PERCENT", "CPU_SPIKE"],
        ["ANOMALY_READ_LATENCY"] = ["IO_READ_LATENCY_MS"],
        ["ANOMALY_WRITE_LATENCY"] = ["IO_WRITE_LATENCY_MS"],
        ["ANOMALY_MEMORY_PRESSURE"] = ["RESOURCE_SEMAPHORE"],
        ["ANOMALY_BLOCKING_SPIKE"] = ["BLOCKING_EVENTS"],
        ["ANOMALY_DEADLOCK_SPIKE"] = ["DEADLOCKS"],
    };

    /// <summary>
    /// #3704: the three REGULAR symptom keys whose graph edge points at <c>RUNNING_JOBS</c>
    /// (<c>RelationshipGraph.BuildMaintenanceEdges</c>, #3538 A9 / PR #3632) — the schema-lock, write and
    /// log-flush pressure a long index rebuild, CHECKDB or reload produces. An anomaly whose resolved
    /// family is one of these gains <see cref="JobKey"/> as its LAST fold-target candidate. A literal list
    /// rather than a read off the graph because the reconciler is static and graph-less by design; the
    /// test pins that every member has a "maintenance" edge onto the job and that no other mapped family
    /// does, so the two cannot drift apart silently.
    /// </summary>
    internal static readonly IReadOnlySet<string> MaintenanceFamilies =
        new HashSet<string>(StringComparer.Ordinal) { "SCH_M", "IO_WRITE_LATENCY_MS", "WRITELOG" };

    /// <summary>The long-running-job fact key — the maintenance edges' destination and this arm's extra fold target.</summary>
    internal const string JobKey = "RUNNING_JOBS";

    /// <summary>
    /// A marker every appended job sentence starts with, so the append is idempotent (a second pass over
    /// the same stories writes nothing) and a test can find the sentence without pinning its whole prose.
    /// </summary>
    internal const string JobSentenceMarker = "RUNNING_JOBS fired in the same window";

    /// <summary>
    /// Rewrites the incident id of each <c>ANOMALY_*</c> story that has a same-run, same-database
    /// regular parent onto that parent's incident id. No-op when there are fewer than two
    /// non-absolution stories or no regular stories to fold into. The pre-#3704 signature: folds by every
    /// rule the two-argument form does (including the maintenance arm, whose gate is structural) but
    /// cannot name the job, because the name is on the fact and this form is not given the facts. The
    /// live SKUs call the two-argument form; this one remains for the frozen <c>deprecated/Dashboard</c>.
    /// </summary>
    public static void Reconcile(IReadOnlyList<AnalysisStory> stories) =>
        Reconcile(stories, facts: null);

    /// <summary>
    /// Rewrites the incident id of each <c>ANOMALY_*</c> story that has a same-run, same-database
    /// regular parent onto that parent's incident id, and — for a maintenance-family anomaly that lands
    /// on the incident carrying the fired <c>RUNNING_JOBS</c> — appends one sentence naming the job to the
    /// anomaly's frozen StoryText (#3704). <paramref name="facts"/> is the run's FULL scored fact list,
    /// the same list <see cref="InferenceEngine.ClusterIntoIncidents"/> and <c>FactAdvice.PopulateStoryText</c>
    /// were given; only the <c>RUNNING_JOBS</c> fact is read from it, for its fired bit and its
    /// <see cref="Fact.ObjectName"/>. Null skips the sentence and changes nothing else. No-op when there
    /// are fewer than two non-absolution stories or no regular stories to fold into.
    /// </summary>
    public static void Reconcile(IReadOnlyList<AnalysisStory> stories, IReadOnlyList<Fact>? facts)
    {
        if (stories is null)
            return;

        var incidentStories = stories.Where(s => s is not null && !s.IsAbsolution).ToList();
        if (incidentStories.Count < 2)
            return;

        // Index the REGULAR (non-anomaly) stories by (fact key, database) over EVERY key the story
        // consumed — its whole Path (root + mid/leaf members), not just its root. A mapped family key
        // is frequently a non-root member of a larger story (SOS_SCHEDULER_YIELD -> CPU_SQL_PERCENT,
        // THREADPOOL/LCK -> BLOCKING_EVENTS); indexing the whole path is what lets the anomaly fold
        // there. BuildStories consumes each fact into exactly one story, so a given key belongs to at
        // most one story's path — this cannot over-fold. The database is normalized (null/empty -> "")
        // so a server-scoped anomaly matches its server-scoped counterpart on "", while a db-scoped
        // anomaly only ever matches a regular finding in its OWN database. On the (shouldn't-happen,
        // consumed-set-violating) chance two stories share a (key, db), keep the highest-severity one.
        var regularByKey = new Dictionary<(string Key, string Db), AnalysisStory>();
        foreach (var s in incidentStories)
        {
            if (IsAnomaly(s.RootFactKey))
                continue; // an anomaly-rooted story is never a fold target — anomalies fold into REGULAR findings

            var db = s.DatabaseName ?? string.Empty;
            foreach (var key in FoldKeys(s))
            {
                var mapKey = (key, db);
                if (!regularByKey.TryGetValue(mapKey, out var existing) || s.Severity > existing.Severity)
                    regularByKey[mapKey] = s;
            }
        }

        if (regularByKey.Count == 0)
            return;

        // #3704: the incident ids that CARRY the job — every regular story with RUNNING_JOBS on its path
        // (the job-rooted story, or the symptom story that consumed it through a maintenance edge) and,
        // through the union ClusterIntoIncidents already performed, every story stamped with the same id
        // (a lone SCH_M story unions onto the job across its own edge without carrying the key itself).
        // A maintenance-family anomaly that ends up on one of these ids gets the job sentence below. The
        // set is empty whenever the job did not fire — a quiet RUNNING_JOBS scores 0, roots nothing and
        // is consumed by nothing — which is the whole of the fired-gate on this path.
        var jobIncidentIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (var s in incidentStories)
        {
            if (!IsAnomaly(s.RootFactKey) && !string.IsNullOrEmpty(s.IncidentId) && FoldKeys(s).Contains(JobKey))
                jobIncidentIds.Add(s.IncidentId);
        }

        // The job fact, when the caller passed the run's facts and it FIRED (the #3632 predicate, base
        // severity above zero — one job running past its own history). Read once; it names the job for
        // every folded anomaly. Null when the facts were not passed, the fact is absent, or it is quiet —
        // in the last two cases jobIncidentIds is empty as well, by construction, so nothing is written.
        var jobFact = facts?.FirstOrDefault(f => f is not null && f.Key == JobKey && f.BaseSeverity > 0);

        foreach (var anomaly in incidentStories)
        {
            if (!IsAnomaly(anomaly.RootFactKey))
                continue;

            var db = anomaly.DatabaseName ?? string.Empty;
            var families = ResolveFamilies(anomaly);
            var isMaintenance = families.Any(MaintenanceFamilies.Contains);

            // #3704: a maintenance-family anomaly tries its own family FIRST (a fired regular symptom is
            // the closer parent, and when the job fired that story consumed the job through #3632's edge,
            // so both routes land on one incident) and the job LAST — the case the live page hit, where
            // no regular symptom fired and the job story was the only thing in the run that named the
            // event. Every other anomaly keeps exactly its pre-#3704 candidates.
            var candidates = isMaintenance ? families.Append(JobKey) : families;

            foreach (var family in candidates)
            {
                // Fold into the FIRST candidate family that has a real parent in the SAME database
                // carrying an id. Never overwrite a solo anomaly's id with an empty one (StampClusters
                // always assigns a non-empty id, so that guard is belt-and-suspenders).
                if (regularByKey.TryGetValue((family, db), out var parent) && !string.IsNullOrEmpty(parent.IncidentId))
                {
                    anomaly.IncidentId = parent.IncidentId;
                    break;
                }
            }

            // #3704: say the job's name on the anomaly that now shares its incident. Only a maintenance-
            // family anomaly (the CPU spike that happens to union into a wide incident is not the job's
            // card), only an incident that carries the job, and only when the fact is in hand to name it.
            if (isMaintenance && jobFact is not null && jobIncidentIds.Contains(anomaly.IncidentId))
                AppendJobSentence(anomaly, jobFact);
        }
    }

    /// <summary>
    /// Appends the one sentence tying a folded maintenance-family anomaly to the fired job, into the
    /// anomaly's frozen <c>{h,i,r}</c> StoryText Investigation. Named when the fact carries the job's
    /// name on <see cref="Fact.ObjectName"/> (#3653: the job furthest past its own history, chosen among
    /// the running-long rows only), unnamed — pointing at the Running Jobs view — when it does not (a
    /// store whose <c>job_name</c> was NULL). The register is <c>FactAdvice</c>'s <c>LinkedJobClause</c>,
    /// which the SCH_M / IO_WRITE_LATENCY_MS / WRITELOG cards carry for the same link; this is the
    /// anomaly-side counterpart, written here because the anomaly composers ran before the fold existed
    /// and the fold is the only place that knows it happened. Idempotent on the marker. A StoryText that
    /// is empty or not the frozen blob (a legacy shape no engine-built story has) is left untouched —
    /// the reconciler does not own that format and will not guess at it.
    /// </summary>
    private static void AppendJobSentence(AnalysisStory anomaly, Fact jobFact)
    {
        var advice = FactAdvice.TryReadStoryText(anomaly.StoryText);
        if (advice is null || advice.Investigation.Contains(JobSentenceMarker, StringComparison.Ordinal))
            return;

        var name = string.IsNullOrEmpty(jobFact.ObjectName) ? null : jobFact.ObjectName;
        var sentence = name is null
            ? $" {JobSentenceMarker} — an Agent job was running well past its normal duration (the Running Jobs view names it) — so this anomaly and that job are ONE incident, not two: a long index rebuild, CHECKDB or reload drives exactly this write, log-flush and schema-lock pressure, and moving or shortening the job is the fix for both cards."
            : $" {JobSentenceMarker} — Agent job `{name}` was running well past its normal duration — so this anomaly and that job are ONE incident, not two: a long index rebuild, CHECKDB or reload drives exactly this write, log-flush and schema-lock pressure, and moving or shortening the job is the fix for both cards.";

        anomaly.StoryText = FactAdvice.SerializeForStoryText(advice with { Investigation = advice.Investigation + sentence });
    }

    /// <summary>
    /// Every fact key a regular story consumed — its full <see cref="AnalysisStory.Path"/> (root plus
    /// each mid/leaf member) so a family key that is a NON-ROOT member is still indexable as a fold
    /// target. Falls back to the root key alone when a story carries no path (defensive; real stories
    /// from <see cref="InferenceEngine.BuildStory"/> always have a populated path).
    /// </summary>
    private static IEnumerable<string> FoldKeys(AnalysisStory story)
    {
        if (story.Path is { Count: > 0 })
            return story.Path;
        return string.IsNullOrEmpty(story.RootFactKey)
            ? Array.Empty<string>()
            : new[] { story.RootFactKey };
    }

    private static bool IsAnomaly(string? key) =>
        key is not null && key.StartsWith("ANOMALY_", StringComparison.Ordinal);

    /// <summary>
    /// The regular-finding family key(s) an anomaly story may fold into, in priority order, or an empty
    /// list when the anomaly has no regular counterpart (it then stays solo). <c>ANOMALY_WAIT_PROFILE</c>
    /// resolves to the family of its dominant <c>contrib_&lt;TYPE&gt;</c> driver; <c>ANOMALY_CPU_SPIKE</c>
    /// resolves to both CPU families (sustained then burst); every other mapped anomaly resolves to one.
    /// </summary>
    private static string[] ResolveFamilies(AnalysisStory anomaly)
    {
        if (string.Equals(anomaly.RootFactKey, "ANOMALY_WAIT_PROFILE", StringComparison.Ordinal))
        {
            var family = DominantWaitFamily(anomaly.RootFactMetadata);
            return family is null ? Array.Empty<string>() : new[] { family };
        }

        /* #3691 (v1 residue): the PostgreSQL wait profile gets the SQL Server profile's treatment above —
           resolved per story from its dominant contributor, not from a static map — with the resolution
           living beside the PostgreSQL vocabulary (WaitKey is the only thing that knows how a contributor
           name becomes a PG_WAIT_* key). One arm here, so the wait family's shape stays in its own file. */
        if (string.Equals(anomaly.RootFactKey, PgTargetFactKeys.AnomalyWaitProfile, StringComparison.Ordinal))
            return PgTargetFactKeys.WaitProfileFamilies(anomaly.RootFactMetadata);

        if (AnomalyToFamilies.TryGetValue(anomaly.RootFactKey, out var families))
            return families;

        /* #3542: the PostgreSQL-target anomalies register their parents in PgTargetFactKeys.AnomalyToFamilies
           (ANOMALY_PG_DEADLOCK_RATE -> PG_DEADLOCK_RATE and so on) — one lookup here, so the content lanes
           add a parent in the shared keys file and never edit this map. A key in neither map stays solo. */
        return PgTargetFactKeys.AnomalyToFamilies.TryGetValue(anomaly.RootFactKey, out var pgFamilies)
            ? pgFamilies
            : Array.Empty<string>();
    }

    /// <summary>
    /// The wait FAMILY of the <c>ANOMALY_WAIT_PROFILE</c>'s dominant (largest-ms) <c>contrib_&lt;TYPE&gt;</c>
    /// metadata entry, mapped through <see cref="FactCollectorHelpers.WaitFamilyKey"/> so it matches the
    /// grouped regular wait fact (CX* → CXPACKET, general lock modes → LCK, everything else → itself).
    /// Ties are broken by ordinal type name so the result is deterministic across dictionary orderings.
    /// Null when there is no <c>contrib_</c> metadata to resolve.
    /// </summary>
    private static string? DominantWaitFamily(Dictionary<string, double>? metadata)
    {
        if (metadata is null || metadata.Count == 0)
            return null;

        const string prefix = "contrib_";
        string? dominantType = null;
        var dominantValue = double.NegativeInfinity;

        foreach (var kv in metadata)
        {
            if (!kv.Key.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            var type = kv.Key.Substring(prefix.Length);
            if (dominantType is null
                || kv.Value > dominantValue
                || (kv.Value == dominantValue && string.CompareOrdinal(type, dominantType) < 0))
            {
                dominantValue = kv.Value;
                dominantType = type;
            }
        }

        return dominantType is null ? null : FactCollectorHelpers.WaitFamilyKey(dominantType);
    }
}
