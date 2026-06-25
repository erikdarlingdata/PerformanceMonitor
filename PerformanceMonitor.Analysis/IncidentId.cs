using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Computes the stable, trackable incident id for an analysis run (correlate-and-focus slice 2,
/// review §1d / §1). v1 treats one analysis run — one time window — as one incident, so every
/// non-absolution finding in the run shares the id (this is what lets the surface stop sprawling
/// into a sea of warnings, and what "what else fired" in slice 1 means). The id is a fingerprint of
/// the run's PRIMARY (highest-severity) finding — its root key + database, scoped to the server — so
/// the SAME recurring incident keeps the SAME id across runs and users can track it over time
/// ("the CPU incident on Sales recurred"), rather than a per-run GUID that changes every run.
///
/// <para>Finer time-clustering within a run (multiple incidents per run) and cross-database focus are
/// later slices; this is the deliberately small substrate (write-only — nothing reads it for
/// behaviour yet).</para>
/// </summary>
public static class IncidentId
{
    /// <summary>
    /// Stamps each incident component with its OWN id — the primary fingerprint of that component
    /// (see <see cref="InferenceEngine.ClusterIntoIncidents"/>). This is the per-component refinement
    /// of <see cref="StampStories"/>: instead of one id for the whole run, each causally-related
    /// group of findings gets its own trackable id, so the surface can render one report per real
    /// incident rather than lumping a run's unrelated problems together.
    /// </summary>
    public static void StampClusters(
        string serverName, IReadOnlyList<IReadOnlyList<AnalysisStory>> components)
    {
        if (components is null)
            return;
        foreach (var component in components)
            StampStories(serverName, component);
    }

    /// <summary>
    /// Stamps a single incident's id onto every non-absolution story in the group. No-op for an
    /// empty / absolution-only group. Used per-component by <see cref="StampClusters"/>.
    /// </summary>
    public static void StampStories(string serverName, IReadOnlyList<AnalysisStory> stories)
    {
        if (stories is null)
            return;

        var incidentStories = stories.Where(s => s is not null && !s.IsAbsolution).ToList();
        if (incidentStories.Count == 0)
            return;

        var id = Compute(serverName, incidentStories);
        foreach (var s in incidentStories)
            s.IncidentId = id;
    }

    /// <summary>
    /// The fingerprint of a run's incident: server + the primary (highest-severity, root-key-tiebroken)
    /// story's root key + database. Stable across runs for the same recurring primary problem.
    /// </summary>
    public static string Compute(string serverName, IReadOnlyList<AnalysisStory> incidentStories)
    {
        var primary = incidentStories
            .OrderByDescending(s => s.Severity)
            .ThenBy(s => s.RootFactKey, StringComparer.Ordinal) // deterministic on ties
            .First();

        var seed = (serverName ?? string.Empty) + "|" + primary.RootFactKey + "|" + (primary.DatabaseName ?? string.Empty);
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(seed));
        return Convert.ToHexString(bytes).ToLowerInvariant()[..16];
    }
}
