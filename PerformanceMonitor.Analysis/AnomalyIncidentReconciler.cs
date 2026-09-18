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
/// <para>Pure and stateless — mutates only <see cref="AnalysisStory.IncidentId"/> on the passed
/// stories, so it is directly unit-testable without any collector or store.</para>
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
    /// Rewrites the incident id of each <c>ANOMALY_*</c> story that has a same-run, same-database
    /// regular parent onto that parent's incident id. No-op when there are fewer than two
    /// non-absolution stories or no regular stories to fold into.
    /// </summary>
    public static void Reconcile(IReadOnlyList<AnalysisStory> stories)
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

        foreach (var anomaly in incidentStories)
        {
            if (!IsAnomaly(anomaly.RootFactKey))
                continue;

            var db = anomaly.DatabaseName ?? string.Empty;
            foreach (var family in ResolveFamilies(anomaly))
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
        }
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
