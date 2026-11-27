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
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #6074: the fleet form of <c>get_collection_health</c> (<c>server_name</c> "*" with a <c>collector_name</c>): for ONE collector, every
/// enabled server's newest successful run and newest failure in a window, and the servers the collector does not apply to. Both SKUs
/// run the one <see cref="LivenessSql"/> over their own <c>collection_log</c> and hand the rows here, so the answer cannot differ in shape.
/// A zero-event read has to prove the collector still ran; before this, that was one <c>get_collection_health</c> call per server.
/// </summary>
public static class CollectorFleetLiveness
{
    /// <summary>The statuses that count as a failed run: a run that RAN and could not read or finish. SKIPPED, YIELDED and WARNING are not failures.</summary>
    public const string FailureStatusSqlList = "'ERROR', 'PERMISSIONS', 'EXTENSION_MISSING', 'SESSION_MISSING', 'ABANDONED'";

    /// <summary>The longest failure text one row carries; the rest is cut and says so.</summary>
    public const int MaxMessageChars = 300;

    /// <summary>
    /// One row per server that has a run of the collector in the window: newest SUCCESS, newest failure, the run and failure counts, and the
    /// status and text of the newest failure. $1 collector, $2 window start, $3 window end (inclusive both), $4 the start of the health window
    /// (<see cref="BandWindowStart"/>). <paramref name="logRelation"/> is
    /// <c>collection_log</c> (Darling) or <c>v_collection_log</c> (Lite, hot plus archive); the text is otherwise identical on PostgreSQL and DuckDB.
    /// The first seven counts and instants are over $2 to $3 (the row's runs, failures and newest success and failure). The last seven (#9202) are what
    /// <see cref="CollectorHealthClassifier"/> bands a collector from, and they are over the one-server read's seven-day window ($4 to $3), whatever
    /// the window is, so a collector that produced 30 hours ago and has only been skipping since cannot read as a setup state on a 24-hour read.
    /// Only a server whose window could change the band reads the longer window: one that is currently skipping, or that has no error and an
    /// EXTENSION_MISSING run, or that has neither an error nor a success (#9213: its band can still turn on EXTENSION_MISSING runs from earlier in
    /// the seven days). Every other server's seven values are the window's own, so the fleet scan stays on the window it was asked for. The last
    /// value, <c>total_runs</c>, is the run count of that same longer window: the band's empty-successes rung compares it with the success and
    /// EXTENSION_MISSING counts, so it has to come from the window they do, while the row's <c>runs</c> stays the <c>hours_back</c> count.
    /// </summary>
    public static string LivenessSql(string logRelation) => $@"
WITH w AS (
    SELECT server_id, status, error_message, collection_time, rows_collected
    FROM {logRelation}
    WHERE collector_name = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
),
agg AS (
    SELECT server_id,
           MAX(collection_time) FILTER (WHERE status = 'SUCCESS') AS last_success,
           MAX(collection_time) FILTER (WHERE status IN ({FailureStatusSqlList})) AS last_failure,
           COUNT(*) AS runs,
           COUNT(*) FILTER (WHERE status IN ({FailureStatusSqlList})) AS failures,
           COUNT(*) FILTER (WHERE status = 'SUCCESS' AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql}) AS success_runs,
           COUNT(*) FILTER (WHERE status = 'ERROR') AS error_runs,
           COUNT(*) FILTER (WHERE status = 'EXTENSION_MISSING') AS extension_missing_runs,
           MAX(collection_time) AS last_run,
           MAX(collection_time) FILTER (WHERE status IS NULL OR status NOT IN ({CollectorRuntimePrecondition.NamedSkipStatusSqlList})) AS last_non_skip,
           MAX(collection_time) FILTER (WHERE rows_collected > 0) AS last_productive
    FROM w
    GROUP BY server_id
),
cand AS (
    SELECT server_id
    FROM agg
    WHERE (error_runs = 0 AND extension_missing_runs > 0)
    OR    (error_runs = 0 AND success_runs = 0)
    OR    last_non_skip IS NULL
    OR    last_non_skip < last_run
),
band AS (
    SELECT l.server_id,
           COUNT(*) AS total_runs,
           COUNT(*) FILTER (WHERE l.status = 'SUCCESS' AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql}) AS success_runs,
           COUNT(*) FILTER (WHERE l.status = 'ERROR') AS error_runs,
           COUNT(*) FILTER (WHERE l.status = 'EXTENSION_MISSING') AS extension_missing_runs,
           MAX(l.collection_time) AS last_run,
           MAX(l.collection_time) FILTER (WHERE l.status IS NULL OR l.status NOT IN ({CollectorRuntimePrecondition.NamedSkipStatusSqlList})) AS last_non_skip,
           MAX(l.collection_time) FILTER (WHERE l.rows_collected > 0) AS last_productive
    FROM {logRelation} l
    WHERE l.collector_name = $1
    AND   l.collection_time >= $4
    AND   l.collection_time <= $3
    AND   l.server_id IN (SELECT server_id FROM cand)
    GROUP BY l.server_id
)
SELECT a.server_id,
       a.last_success,
       a.last_failure,
       a.runs,
       a.failures,
       f.status AS failure_status,
       f.error_message AS failure_message,
       COALESCE(b.success_runs, a.success_runs) AS success_runs,
       COALESCE(b.error_runs, a.error_runs) AS error_runs,
       COALESCE(b.extension_missing_runs, a.extension_missing_runs) AS extension_missing_runs,
       COALESCE(b.last_run, a.last_run) AS last_run,
       COALESCE(b.last_non_skip, a.last_non_skip) AS last_non_skip,
       COALESCE(b.last_productive, a.last_productive) AS last_productive,
       COALESCE(b.total_runs, a.runs) AS total_runs
FROM agg a
LEFT JOIN band b ON b.server_id = a.server_id
LEFT JOIN LATERAL (
    SELECT x.status, x.error_message
    FROM {logRelation} x
    WHERE x.server_id = a.server_id
    AND   x.collector_name = $1
    AND   x.collection_time = a.last_failure
    AND   x.status IN ({FailureStatusSqlList})
    LIMIT 1
) f ON true
ORDER BY a.server_id";

    /// <summary>The start of the seven-day health window the one-server read bands over, ending at <paramref name="windowEndUtc"/>: the $4 of <see cref="LivenessSql"/>.</summary>
    public static DateTime BandWindowStart(DateTime windowEndUtc) => windowEndUtc.AddMinutes(-CollectorHealthClassifier.HealthWindowMinutes);

    /// <summary>One enabled server and what the registry knows of its engine (the inputs of <see cref="CollectorEngineCapability.NotCollectedMessage"/>; the Amazon RDS kind is required, and is AwsRdsKind.None when the host does not know it, #6894 and #6893).</summary>
    public sealed record ServerFacts(int ServerId, string ServerName, string? DisplayName, int EngineEdition, string? EngineKind, AwsRdsKind AwsRdsKind, int SqlMajorVersion = 0);

    /// <summary>One row of <see cref="LivenessSql"/>. The last six fields (#9202) are what <see cref="CollectorHealthClassifier"/> bands a collector from,
    /// the same counts and instants the one-server read selects, over its seven-day window, and <paramref name="BandRuns"/> (#9213) is that window's run count.
    /// A caller that leaves them at their defaults gets the plain outcomes; a null <paramref name="BandRuns"/> reads as <see cref="Runs"/>.</summary>
    public sealed record RunFacts(
        int ServerId, DateTime? LastSuccessUtc, DateTime? LastFailureUtc, long Runs, long Failures, string? FailureStatus, string? FailureMessage,
        long SuccessRuns = 0, long ErrorRuns = 0, long ExtensionMissingRuns = 0,
        DateTime? LastRunUtc = null, DateTime? LastNonSkipUtc = null, DateTime? LastProductiveUtc = null, long? BandRuns = null);

    /// <summary>The refusal for a collector name this build does not know, or null when it is known. A typo would otherwise read as "no run on any server".</summary>
    public static string? ValidateCollector(string? collectorName)
    {
        if (string.IsNullOrWhiteSpace(collectorName))
        {
            return Refusal(
                "collector_name",
                "collector_name is required with server_name \"*\": the one collector to report on every server (for example query_store or wait_stats; get_collection_health on one server lists the names).");
        }

        return CollectorCatalog.Find(collectorName.Trim()) is null
            ? Refusal("collector_name", $"'{collectorName.Trim()}' is not a collector this build knows. Collector names are the ones get_collection_health lists for a server (for example query_store, wait_stats, deadlocks).")
            : null;
    }

    /// <summary>
    /// The answer: applicable servers (one row each, newest success and newest failure, or no run at all) and the servers where the
    /// collector does not apply. <paramref name="noRunReasons"/> (#8361) names, by server id, a reason a server with no run in the window
    /// is not collected on this host, such as a Query Store collector on a readable secondary; such a server is listed under
    /// <c>not_applicable</c> with that reason, the way an engine gap is, instead of as a <c>no_run_in_window</c> row.
    /// </summary>
    public static string Build(
        string collectorName, int hoursBack, DateTime windowStartUtc, DateTime windowEndUtc,
        IReadOnlyList<ServerFacts> servers, IReadOnlyList<RunFacts> runs,
        IReadOnlyDictionary<int, string>? noRunReasons = null)
    {
        var byServer = runs.ToDictionary(r => r.ServerId);
        var applicable = new List<object>();
        var notApplicable = new List<object>();
        var ranCount = 0;
        var succeededCount = 0;
        var noRunCount = 0;
        var needsSetupCount = 0;
        var failingCount = 0;

        foreach (var server in servers.OrderBy(s => s.ServerName, StringComparer.OrdinalIgnoreCase))
        {
            var gap = CollectorEngineCapability.NotCollectedMessage(server.ServerName, server.EngineEdition, server.EngineKind, collectorName, server.AwsRdsKind, sqlMajorVersion: server.SqlMajorVersion);
            if (gap is not null)
            {
                notApplicable.Add(new { server_name = server.ServerName, display_name = Name(server.DisplayName, server.ServerName), reason = gap });
                continue;
            }

            if (!byServer.TryGetValue(server.ServerId, out var run))
            {
                if (noRunReasons is not null && noRunReasons.TryGetValue(server.ServerId, out var reason))
                {
                    notApplicable.Add(new { server_name = server.ServerName, display_name = Name(server.DisplayName, server.ServerName), reason });
                    continue;
                }

                noRunCount++;
                applicable.Add(new
                {
                    server_name = server.ServerName,
                    display_name = Name(server.DisplayName, server.ServerName),
                    outcome = "no_run_in_window",
                    regressed_from_productive = false,
                    last_success_at = (string?)null,
                    last_failure_at = (string?)null,
                    runs = 0L,
                    failures = 0L,
                    last_failure_status = (string?)null,
                    last_failure_message = (string?)null,
                });
                continue;
            }

            ranCount++;
            if (run.LastSuccessUtc is not null)
            {
                succeededCount++;
            }

            /* #9202: the one-server read's band decides whether this is a setup state, so the fleet form cannot answer "is it failing" differently.
               The inputs are over that read's seven-day window (LivenessSql), not hours_back. The ladder returns EXTENSION_MISSING before any rung that
               reads a clock, so the time inputs here are only the ones SuccessCountForLadder reads. */
            var bandRuns = run.BandRuns ?? run.Runs;
            var needsSetup = CollectorHealthClassifier.Classify(
                bandRuns,
                CollectorHealthClassifier.SuccessCountForLadder(
                    bandRuns, run.SuccessRuns, run.ExtensionMissingRuns, run.LastRunUtc, run.LastNonSkipUtc, run.LastProductiveUtc),
                run.ErrorRuns, 0, run.ExtensionMissingRuns, 0, 0, 0, 0, null, 0) == CollectorHealthClassifier.ExtensionMissing;

            /* The outcome is the newer of the two: a failure after the last success is the collector's current state; a success after the last failure means it recovered.
               A setup state (#9202) is neither: it has a remedy, and it reads the same as the one-server band and the fleet overview. */
            var outcome = needsSetup ? "needs_setup"
                : run.LastSuccessUtc is null ? "failing"
                : run.LastFailureUtc is { } failure && failure > run.LastSuccessUtc ? "failing_since_last_success"
                : "succeeding";
            if (needsSetup)
            {
                needsSetupCount++;
            }
            else if (outcome != "succeeding")
            {
                failingCount++;
            }

            applicable.Add(new
            {
                server_name = server.ServerName,
                display_name = Name(server.DisplayName, server.ServerName),
                outcome,
                /* #9202: the one-server read's regressed_from_productive: rows were stored earlier in the window and every run since is a named skip. */
                regressed_from_productive = CollectorHealthClassifier.RegressedFromProductive(run.LastRunUtc, run.LastNonSkipUtc, run.LastProductiveUtc),
                last_success_at = Stamp(run.LastSuccessUtc),
                last_failure_at = Stamp(run.LastFailureUtc),
                runs = run.Runs,
                failures = run.Failures,
                last_failure_status = run.FailureStatus,
                last_failure_message = Cut(run.FailureMessage),
            });
        }

        /* #9202: the reply is held to the response budget, whole rows only. The applicable rows come first and the not-applicable rows after
           them, so a cut leaves off the end of the second list before the first. The counts at the top are over every server, cut or not. */
        var rowTotal = applicable.Count + notApplicable.Count;
        string Reply(int kept)
        {
            var keptApplicable = Math.Min(kept, applicable.Count);
            var cut = kept < rowTotal;
            return JsonSerializer.Serialize(new
            {
                scope = "fleet",
                collector_name = collectorName,
                hours_back = hoursBack,
                window_start = Stamp(windowStartUtc),
                window_end = Stamp(windowEndUtc),
                servers_with_a_run = ranCount,
                servers_with_a_success = succeededCount,
                servers_with_no_run = noRunCount,
                servers_needing_setup = needsSetupCount,
                servers_failing = failingCount,
                servers_not_applicable = notApplicable.Count,
                note = Note,
                budget_truncated = cut,
                budget_note = cut
                    ? McpResponseBudget.PageCutNote(rowTotal, kept, "applicable servers first, each group in server-name order", "server rows",
                        "The counts at the top cover every server. Call get_collection_health with one server_name for a server whose row was left out.")
                    : null,
                servers = applicable.Take(keptApplicable),
                not_applicable = notApplicable.Take(Math.Max(0, kept - applicable.Count)),
            }, s_json);
        }

        return McpResponseBudget.FitPrefix(rowTotal, Reply).Json;
    }

    private const string Note =
        "One row per enabled server. outcome: succeeding = the newest run in the window succeeded or no failure came after it; "
        + "failing_since_last_success = a failure is newer than the last success; failing = no success in the window; "
        + "needs_setup = the one-server collection health band is EXTENSION_MISSING, which that read decides over the last seven days whatever hours_back is: every run found an optional extension absent, or its successes stored nothing and every run after them did (a setup state with a remedy, not a fault); "
        + "no_run_in_window = the collector did not run on this server in the window (a stopped service, a gate that is off, or a collector on a longer cadence than the window). "
        + "regressed_from_productive is true when the collector stored rows earlier in the last seven days, whatever hours_back is, and every run since is a named skip such as EXTENSION_MISSING: that is a regression, not rest, and it reads failing_since_last_success, or failing when its last success is older than hours_back. "
        + "servers_needing_setup counts the needs_setup rows apart from servers_failing, which counts failing and failing_since_last_success. "
        + "last_failure_* and failures count every ERROR, PERMISSIONS, EXTENSION_MISSING, SESSION_MISSING or ABANDONED run, so a needs_setup row still shows its EXTENSION_MISSING runs there; SKIPPED, YIELDED and WARNING are not failures. "
        + "not_applicable lists the servers whose engine never runs this collector.";

    private static readonly JsonSerializerOptions s_json = new() { WriteIndented = false };

    /// <summary>The <c>invalid</c> envelope <c>McpHelpers.Refusal</c> writes (that helper is internal to the Common assembly): status, message, hints.parameter.</summary>
    private static string Refusal(string parameter, string sentence) =>
        JsonSerializer.Serialize(new { status = "invalid", message = sentence, hints = new { parameter } }, s_json);

    private static string? Name(string? displayName, string serverName) =>
        string.IsNullOrWhiteSpace(displayName) || string.Equals(displayName, serverName, StringComparison.Ordinal) ? null : displayName;

    private static string? Stamp(DateTime? utc) =>
        utc is { } value ? DateTime.SpecifyKind(value, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture) : null;

    private static string? Cut(string? text) =>
        text is null ? null : text.Length <= MaxMessageChars ? text : text[..MaxMessageChars] + "... (cut at " + MaxMessageChars + " characters)";
}
