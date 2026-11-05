/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #6130: the first sentence of an <c>unavailable</c> answer from a server that has had no recent collection at all.
///
/// <para>An <c>unavailable</c> read tells the caller to check the collector, to confirm pg_stat_statements is installed, to look at
/// get_collection_health. All of that is advice for a server that IS being collected. A server whose last collection was days
/// ago has a simpler explanation that outranks every one of them: nothing is reaching the store. So the answer says that first,
/// with the last collection time.</para>
///
/// <para><b>The rule is Lite's, and it is not <c>list_servers</c>'s.</b> A server counts as offline when
/// <see cref="DarlingMcpDataTools.FreshnessStatus"/> says <c>Offline</c> for the instant of its newest SUCCESS collector run
/// (#7167): only a run that reached the server is "heard from". A server the service cannot reach still writes ERROR, PERMISSIONS or
/// SKIPPED rows every cycle, which keep <c>list_servers</c>'s newest-row probe (<see cref="FreshnessSql"/>, any non-cycle row) fresh
/// while nothing is being read, so the two can disagree on such a server: <c>list_servers</c> bands it by its failing rows, and the
/// read says nothing is arriving. That is deliberate, and Lite makes the same split (<c>GetLastSuccessfulCollectionAsync</c> against
/// its Overview band), so a read answers the same way in both products.</para>
/// </summary>
internal static class DarlingServerOfflineNote
{
    /// <summary>The <c>list_servers</c> probe for one server: its registration instant and its newest non-cycle collection-log row, whatever its status.</summary>
    internal static readonly string FreshnessSql = BuildFreshnessSql(statusPredicate: "");

    /// <summary>
    /// #7167: <see cref="FreshnessSql"/> narrowed to the newest SUCCESS run (<c>status = 'SUCCESS'</c>, the status Collection Health counts
    /// as a run that reached the server), so a server that logs only failed runs still reads as silent. Lite's rule.
    /// </summary>
    internal static readonly string SuccessFreshnessSql = BuildFreshnessSql(statusPredicate: "AND   cl.status = 'SUCCESS'");

    /// <summary>The one probe both rules read, with <paramref name="statusPredicate"/> (empty for any row) closing the inner filter.</summary>
    private static string BuildFreshnessSql(string statusPredicate) => $"""
        SELECT
            s.created_date,
            latest.collection_time AS last_collection
        FROM servers s
        LEFT JOIN LATERAL
        (
            SELECT cl.collection_time
            FROM v_collection_log cl
            WHERE cl.server_id = s.server_id
            AND   {CollectionCycleRow.NotCycleRowSql}
            {statusPredicate}
            ORDER BY cl.collection_time DESC
            LIMIT 1
        ) AS latest ON TRUE
        WHERE s.server_id = $1
        """;

    /// <summary>
    /// The sentence that opens an <c>unavailable</c> answer for a server that is offline by the SUCCESS-run rule, or null
    /// when the server is not offline or when the read fails: a diagnostic on a path that already found nothing must not turn
    /// that into an error.
    /// </summary>
    internal static async Task<string?> LeadAsync(
        NpgsqlDataSource postgres, int serverId, string displayName, DateTime nowUtc, CancellationToken cancellationToken,
        DateTime? windowEndUtc = null) =>
        (await LeadWithNewestAsync(postgres, serverId, displayName, nowUtc, windowEndUtc, cancellationToken)).Lead;

    /// <summary><see cref="LeadAsync"/>, with the newest SUCCESS collection it judged by (#7724), so a caller can say what was collected.</summary>
    private static async Task<(string? Lead, DateTime? Newest)> LeadWithNewestAsync(
        NpgsqlDataSource postgres, int serverId, string displayName, DateTime nowUtc, DateTime? windowEndUtc,
        CancellationToken cancellationToken)
    {
        if (await ReadFreshnessAsync(postgres, serverId, cancellationToken, successOnly: true) is not var (registered, last))
        {
            return (null, null);
        }

        /* #6155: a window that ends at or before the newest collection lies inside collected history, so an empty read there is a true
           negative even though the server has gone quiet since. Only a window that reaches into the silence is missing data. */
        if (windowEndUtc is { } windowEnd && last is { } newest && windowEnd <= newest)
        {
            return (null, last);
        }

        /* #7590: a stored failed connect is Offline whatever the collection age, and the note says so instead of the age. */
        var (connectDownSince, lastConnectError) = await ReadConnectDownAsync(postgres, serverId, cancellationToken);
        return (LeadFor(displayName, last, registered, nowUtc, connectDownSince, lastConnectError), last);
    }

    /// <summary>The registry's failed-connect state for one server (#7590): since when and the last error, or nulls while the last connect succeeded
    /// or when the read fails (a diagnostic never turns a finding into an error).</summary>
    internal static async Task<(DateTime? Since, string? Error)> ReadConnectDownAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = postgres.CreateCommand("SELECT connect_down_since, last_connect_error FROM servers WHERE server_id = $1");
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            return await reader.ReadAsync(cancellationToken) && !reader.IsDBNull(0)
                ? (reader.GetDateTime(0), reader.IsDBNull(1) ? null : reader.GetString(1))
                : (null, null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return (null, null);
        }
    }

    /// <summary>
    /// The <see cref="FreshnessSql"/> probe for one server: its registration instant and its newest collection. Null when the server is
    /// not registered or the read fails, so a caller's diagnostic never turns a finding into an error.
    /// </summary>
    internal static async Task<(DateTime? Registered, DateTime? Last)?> ReadFreshnessAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken, bool successOnly = false)
    {
        try
        {
            await using var command = postgres.CreateCommand(successOnly ? SuccessFreshnessSql : FreshnessSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return (reader.IsDBNull(0) ? null : reader.GetDateTime(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>The pure half of <see cref="LeadAsync"/>, over an already-read freshness probe.</summary>
    internal static string? LeadFor(
        string displayName, DateTime? lastCollectionUtc, DateTime? registeredAtUtc, DateTime nowUtc,
        DateTime? connectDownSinceUtc = null, string? lastConnectError = null)
    {
        connectDownSinceUtc = ServerHealthClassifier.EffectiveConnectDownSince(lastCollectionUtc, connectDownSinceUtc);
        if (DarlingMcpDataTools.FreshnessStatus(lastCollectionUtc, registeredAtUtc, nowUtc, connectDownSinceUtc) != "Offline")
        {
            return null;
        }

        if (connectDownSinceUtc is { } downSince)
        {
            return $"{displayName} is offline: the service cannot connect to it. "
                + ServerHealthClassifier.UnreachableReason(downSince, lastConnectError).TrimEnd('.')
                + ". Nothing is arriving to read, whatever the collectors would otherwise report.";
        }

        return lastCollectionUtc is { } last
            ? $"{displayName} is offline: the store has had no successful collection from it since "
                + $"{last.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture)} UTC, "
                + "so nothing is arriving to read, whatever the collectors would otherwise report."
            : $"{displayName} is offline: the store holds no successful collection from it within its retained history, "
                + "so nothing is arriving to read, whatever the collectors would otherwise report.";
    }

    /// <summary>
    /// <c>McpHelpers.Status("unavailable", message, hints)</c>, with <see cref="LeadAsync"/>'s sentence in front of the message when
    /// the server is offline.
    /// </summary>
    internal static Task<string> UnavailableAsync(
        NpgsqlDataSource postgres, int serverId, string displayName, string message, object? hints, CancellationToken cancellationToken) =>
        WithLeadAsync(McpHelpers.Status("unavailable", message, hints), postgres, serverId, displayName, cancellationToken);

    /// <summary><see cref="UnavailableAsync(NpgsqlDataSource, int, string, string, object, CancellationToken)"/> for an answer with no hints.</summary>
    internal static Task<string> UnavailableAsync(
        NpgsqlDataSource postgres, int serverId, string displayName, string message, CancellationToken cancellationToken) =>
        UnavailableAsync(postgres, serverId, displayName, message, null, cancellationToken);

    /// <summary>
    /// #6155: <paramref name="answer"/>, when it says <c>empty</c> (the read looked and found nothing) for a server that is offline by
    /// the SUCCESS-run rule, as an <c>unavailable</c> answer that opens with <see cref="LeadAsync"/>'s sentence. An offline server's
    /// empty read is not a true negative: nothing has been arriving, so "no dead tuples", "no replication slots" or "nothing held the
    /// xmin horizon" would be a healthy all-clear built from missing data. The original answer's own wording is dropped with its word.
    /// With <paramref name="windowStartUtc"/> and a collection inside the window (#7724) the <c>unavailable</c> answer is <see cref="McpHelpers.CollectedThroughStatus"/> instead (the offline lead, then the collected-through sentence).
    /// Any other answer, any answer for a server that is not offline, and any answer whose window ends at or before the server's newest
    /// collection (it is inside collected history, so its emptiness is a real finding), comes back unchanged.
    /// </summary>
    internal static async Task<string> EmptyAsync(
        NpgsqlDataSource postgres, int serverId, string displayName, DateTime windowEndUtc, string answer, CancellationToken cancellationToken,
        DateTime? windowStartUtc = null)
    {
        if (!IsEmptyAnswer(answer))
        {
            return answer;
        }

        /* The caller's window end is the read's anchor (as_of, or now for a live read), so offline is judged as of it, not as of the
           clock: an as_of read of a past window must not answer as the server is today. */
        var (lead, newest) = await LeadWithNewestAsync(postgres, serverId, displayName, windowEndUtc, windowEndUtc, cancellationToken);
        if (lead is null)
        {
            return answer;
        }

        /* #7724: the window holds a collection (the server went quiet partway through it), so say what was collected through, not that nothing is known.
           A caller that does not pass its window start keeps the offline sentence. */
        return McpHelpers.OfflineEmptyAnswer(lead, newest, windowEndUtc, windowStartUtc, OfflineInsteadOfEmpty(lead)) ?? answer;
    }

    /// <summary>
    /// #7459: <see cref="EmptyAsync"/> for a read whose own answer to a window nobody looked at is already <c>unavailable</c>. That answer
    /// reports a gap in collection, and on an offline server the reason is known, so the offline answer replaces it whichever of the two
    /// words the read chose. Judged as of the window end, as <see cref="EmptyAsync"/> is, so a window inside collected history keeps the
    /// read's own answer.
    /// </summary>
    internal static async Task<string> UnavailableOrEmptyAsync(
        NpgsqlDataSource postgres, int serverId, string displayName, DateTime windowEndUtc, string answer, CancellationToken cancellationToken)
    {
        var lead = await LeadAsync(postgres, serverId, displayName, windowEndUtc, cancellationToken, windowEndUtc);
        return lead is null ? answer : OfflineInsteadOfEmpty(lead);
    }

    /// <summary>The <c>unavailable</c> answer that replaces an offline server's empty read.</summary>
    internal static string OfflineInsteadOfEmpty(string lead) =>
        McpHelpers.Status("unavailable", lead + " An empty result here is missing data, not a finding.", null);

    /// <summary>True when <paramref name="answer"/> is a JSON object whose <c>status</c> is <c>empty</c>.</summary>
    internal static bool IsEmptyAnswer(string answer)
    {
        try
        {
            return JsonNode.Parse(answer) is JsonObject node
                && node["status"] is JsonValue status && status.TryGetValue<string>(out var word) && word == "empty";
        }
        catch (System.Text.Json.JsonException)
        {
            return false;
        }
    }

    /// <summary>
    /// <paramref name="answer"/>, when it is an <c>unavailable</c> envelope for an offline server, with <see cref="LeadAsync"/>'s
    /// sentence put in front of its <c>message</c>. Any other answer, and any answer for a server that is not offline, comes back unchanged.
    /// </summary>
    internal static async Task<string> WithLeadAsync(
        string answer, NpgsqlDataSource postgres, int serverId, string displayName, CancellationToken cancellationToken)
    {
        JsonObject? node;
        try
        {
            node = JsonNode.Parse(answer) as JsonObject;
        }
        catch (System.Text.Json.JsonException)
        {
            return answer;
        }

        if (node is null
            || node["status"] is not JsonValue status || !status.TryGetValue<string>(out var word) || word != "unavailable"
            || node["message"] is not JsonValue message || !message.TryGetValue<string>(out var text))
        {
            return answer;
        }

        var lead = await LeadAsync(postgres, serverId, displayName, DateTime.UtcNow, cancellationToken);
        if (lead is null)
        {
            return answer;
        }

        node["message"] = lead + " " + text;
        return node.ToJsonString(McpHelpers.JsonOptions);
    }
}
