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
    /// status and text of the newest failure. $1 collector, $2 window start, $3 window end (inclusive both). <paramref name="logRelation"/> is
    /// <c>collection_log</c> (Darling) or <c>v_collection_log</c> (Lite, hot plus archive); the text is otherwise identical on PostgreSQL and DuckDB.
    /// </summary>
    public static string LivenessSql(string logRelation) => $@"
WITH w AS (
    SELECT server_id, status, error_message, collection_time
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
           COUNT(*) FILTER (WHERE status IN ({FailureStatusSqlList})) AS failures
    FROM w
    GROUP BY server_id
)
SELECT a.server_id,
       a.last_success,
       a.last_failure,
       a.runs,
       a.failures,
       (SELECT x.status FROM w x WHERE x.server_id = a.server_id AND x.collection_time = a.last_failure AND x.status IN ({FailureStatusSqlList}) LIMIT 1) AS failure_status,
       (SELECT x.error_message FROM w x WHERE x.server_id = a.server_id AND x.collection_time = a.last_failure AND x.status IN ({FailureStatusSqlList}) LIMIT 1) AS failure_message
FROM agg a
ORDER BY a.server_id";

    /// <summary>One enabled server and what the registry knows of its engine (the inputs of <see cref="CollectorEngineCapability.NotCollectedMessage"/>; the Amazon RDS fact is false when the host does not know it, #6894).</summary>
    public sealed record ServerFacts(int ServerId, string ServerName, string? DisplayName, int EngineEdition, string? EngineKind, bool IsAwsRds = false);

    /// <summary>One row of <see cref="LivenessSql"/>.</summary>
    public sealed record RunFacts(int ServerId, DateTime? LastSuccessUtc, DateTime? LastFailureUtc, long Runs, long Failures, string? FailureStatus, string? FailureMessage);

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

    /// <summary>The answer: applicable servers (one row each, newest success and newest failure, or no run at all) and the servers where the collector does not apply.</summary>
    public static string Build(
        string collectorName, int hoursBack, DateTime windowStartUtc, DateTime windowEndUtc,
        IReadOnlyList<ServerFacts> servers, IReadOnlyList<RunFacts> runs)
    {
        var byServer = runs.ToDictionary(r => r.ServerId);
        var applicable = new List<object>();
        var notApplicable = new List<object>();
        var ranCount = 0;
        var succeededCount = 0;
        var noRunCount = 0;

        foreach (var server in servers.OrderBy(s => s.ServerName, StringComparer.OrdinalIgnoreCase))
        {
            var gap = CollectorEngineCapability.NotCollectedMessage(server.ServerName, server.EngineEdition, server.EngineKind, collectorName, server.IsAwsRds);
            if (gap is not null)
            {
                notApplicable.Add(new { server_name = server.ServerName, display_name = Name(server.DisplayName, server.ServerName), reason = gap });
                continue;
            }

            if (!byServer.TryGetValue(server.ServerId, out var run))
            {
                noRunCount++;
                applicable.Add(new
                {
                    server_name = server.ServerName,
                    display_name = Name(server.DisplayName, server.ServerName),
                    outcome = "no_run_in_window",
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

            /* The outcome is the newer of the two: a failure after the last success is the collector's current state; a success after the last failure means it recovered. */
            var outcome = run.LastSuccessUtc is null ? "failing"
                : run.LastFailureUtc is { } failure && failure > run.LastSuccessUtc ? "failing_since_last_success"
                : "succeeding";
            applicable.Add(new
            {
                server_name = server.ServerName,
                display_name = Name(server.DisplayName, server.ServerName),
                outcome,
                last_success_at = Stamp(run.LastSuccessUtc),
                last_failure_at = Stamp(run.LastFailureUtc),
                runs = run.Runs,
                failures = run.Failures,
                last_failure_status = run.FailureStatus,
                last_failure_message = Cut(run.FailureMessage),
            });
        }

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
            servers_not_applicable = notApplicable.Count,
            note = "One row per enabled server. outcome: succeeding = the newest run in the window succeeded or no failure came after it; failing_since_last_success = a failure is newer than the last success; failing = no success in the window; no_run_in_window = the collector did not run on this server in the window (a stopped service, a gate that is off, or a collector on a longer cadence than the window). A failure is ERROR, PERMISSIONS, EXTENSION_MISSING, SESSION_MISSING or ABANDONED; SKIPPED, YIELDED and WARNING are not failures. not_applicable lists the servers whose engine never runs this collector.",
            servers = applicable,
            not_applicable = notApplicable,
        }, s_json);
    }

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
