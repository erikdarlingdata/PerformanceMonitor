/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store half of the #2546 runtime-precondition answer: reads what collection already recorded about a
/// collector's most recent run — or about the target's Query Store configuration — and, when it names a
/// precondition somebody can satisfy, returns the <c>precondition</c> envelope both SKUs emit. The DECISION
/// and the WORDS come from <see cref="CollectorRuntimePrecondition"/>; nothing about what counts as a
/// precondition, and nothing about how it is described, lives in this file or in its Lite twin
/// (<c>McpRuntimePrecondition</c>).
///
/// <para><b>Why the store and not a probe.</b> The MCP surface never touches a monitored server — every read
/// is served from collected rows — so "is the session running right now" has to be answered from what the
/// last collection cycle found. That is not a compromise: the runners already classify these outcomes
/// (SESSION_MISSING for an absent capture session, the non-fatal bucket for a denied grant, a missing source
/// object or a disabled feature) and already write the actionable sentence into
/// <c>collection_log.error_message</c>. The store has held the answer all along; no read reported it.</para>
///
/// <para><b>Called after the capability answer, and only on the miss path.</b> A permanent engine gap wins:
/// a precondition message on an engine that can never have the surface would re-introduce the defect #2511
/// closed. And like its capability sibling, this is asked AFTER the read came back empty, never before, so a
/// server whose recorded state disagrees with its collected rows still gets its DATA.</para>
///
/// <para><b>A registry or log read that FAILS answers null</b>, deliberately and for the same reason the
/// capability probe does: this runs on a path that has already found no data, and turning a diagnostic into
/// a read error would replace one honest miss with a worse one.</para>
/// </summary>
internal static class DarlingRuntimePrecondition
{
    /// <summary>
    /// Whether one collector has EVER run for one server, beside when that server last collected anything at
    /// all. Both halves in one round trip, because the inference needs both and they must describe the same
    /// instant: a collector with no rows on a server that is collecting normally is gated off, while a
    /// collector with no rows on a server that has collected nothing is just a collection outage.
    ///
    /// <para><c>EXISTS</c> rather than a count or a MAX on the collector side: the question is presence, and
    /// on a hypertable holding months of rows for dozens of collectors an existence probe stops at the first
    /// hit while an aggregate reads the partition. $1 server_id, $2 collector.</para>
    /// </summary>
    public const string CollectorEverRanSql = @"
SELECT EXISTS (
           SELECT 1
           FROM collection_log
           WHERE server_id = $1
           AND   collector_name = $2
       ) AS collector_ever_ran,
       (
           SELECT MAX(collection_time)
           FROM collection_log
           WHERE server_id = $1
       ) AS server_last_collected";

    /// <summary>
    /// The most recent run of one collector for one server. Ordered by <c>log_id</c> rather than
    /// <c>collection_time</c> because the id is monotonic per insert while two runs inside the same cycle can
    /// share a timestamp — and "the latest run" is the whole claim this makes. $1 server_id, $2 collector.
    /// </summary>
    public const string LatestCollectorOutcomeSql = @"
SELECT status, error_message, collection_time
FROM collection_log
WHERE server_id = $1
AND   collector_name = $2
ORDER BY log_id DESC
LIMIT 1";

    /// <summary>
    /// The latest per-database Query Store configuration snapshot, optionally narrowed to the one database a
    /// read was scoped to. Reads <c>v_query_store_health</c> — the same relation
    /// <c>get_query_store_health</c> answers from, so this can never claim a state that tool would not
    /// confirm. $1 server_id, $2 database name or NULL for every database.
    ///
    /// <para>The database filter is part of the EVIDENCE, not a convenience: a caller who asked
    /// <c>get_query_store_top</c> for one database and got nothing needs to know about THAT database's Query
    /// Store, and a server-wide answer would hide an off database behind a healthy sibling.</para>
    /// </summary>
    public const string LatestQueryStoreStatesSql = @"
SELECT database_name, actual_state, capture_time
FROM v_query_store_health
WHERE server_id = $1
AND   capture_time = (SELECT MAX(capture_time) FROM v_query_store_health WHERE server_id = $1)
AND   ($2::text IS NULL OR database_name = $2)
ORDER BY database_name";

    /// <summary>
    /// The <c>precondition</c> envelope when the collector serving this read recorded one on its most recent
    /// run, or <c>null</c> when it did not — in which case the caller falls through to its own
    /// <c>empty</c>/<c>unavailable</c> miss, unchanged.
    /// </summary>
    public static async Task<string?> StatusAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string serverName,
        string collectorName,
        CancellationToken cancellationToken = default)
    {
        string? status;
        string? errorMessage;
        DateTime? observedUtc;

        try
        {
            (status, errorMessage, observedUtc) =
                await ReadLatestOutcomeAsync(postgres, serverId, collectorName, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }

        var message = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            serverName, collectorName, status, errorMessage, observedUtc);

        return message is null ? null : McpHelpers.Status(CollectorRuntimePrecondition.StatusWord, message);
    }

    /// <summary>
    /// The <c>precondition</c> envelope when the collector serving this read has never run against this
    /// server while the server is collecting normally — i.e. its <c>AppliesTo</c> gate is off — or
    /// <c>null</c> otherwise.
    ///
    /// <para>Call this AFTER <see cref="StatusAsync"/>, never instead of it: a collector that ran and
    /// recorded a denial has a specific sentence from the monitored server itself, which is strictly better
    /// evidence than this inference from an absence.</para>
    /// </summary>
    /// <param name="gateCandidates">Operator-facing text naming what could switch this collector off. The
    /// deciding facts are not persisted, so the caller supplies the candidates rather than this method
    /// guessing which one applies.</param>
    public static async Task<string?> GatedOffStatusAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string serverName,
        string collectorName,
        string gateCandidates,
        CancellationToken cancellationToken = default)
    {
        bool everRan;
        DateTime? serverLastCollectedUtc;

        try
        {
            (everRan, serverLastCollectedUtc) =
                await ReadCollectorEverRanAsync(postgres, serverId, collectorName, cancellationToken);
        }
        catch (Exception)
        {
            /* Same rule as the rest of this class: a diagnostic that throws must not turn an honest miss
               into a read error. */
            return null;
        }

        var message = CollectorRuntimePrecondition.GatedOffMessage(
            serverName, collectorName, gateCandidates, everRan, serverLastCollectedUtc);

        return message is null ? null : McpHelpers.Status(CollectorRuntimePrecondition.StatusWord, message);
    }

    /// <summary>
    /// The <c>precondition</c> envelope when this server's recorded Query Store configuration says the
    /// databases the read covered are not recording runtime statistics, or <c>null</c> when it says nothing
    /// of the kind (no snapshot yet, or at least one database in scope is READ_WRITE).
    /// </summary>
    public static async Task<string?> QueryStoreStatusAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string serverName,
        string? databaseName,
        CancellationToken cancellationToken = default)
    {
        List<CollectorRuntimePrecondition.QueryStoreDatabaseState> states;
        DateTime? observedUtc;

        try
        {
            (states, observedUtc) =
                await ReadQueryStoreStatesAsync(postgres, serverId, databaseName, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }

        var message = CollectorRuntimePrecondition.QueryStoreDisabledMessage(serverName, states, observedUtc);
        return message is null ? null : McpHelpers.Status(CollectorRuntimePrecondition.StatusWord, message);
    }

    private static async Task<(string? Status, string? ErrorMessage, DateTime? ObservedUtc)> ReadLatestOutcomeAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string collectorName,
        CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(LatestCollectorOutcomeSql);
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, collectorName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            return (null, null, null);
        }

        return (
            reader.IsDBNull(0) ? null : reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? null : DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc));
    }

    private static async Task<(bool EverRan, DateTime? ServerLastCollectedUtc)> ReadCollectorEverRanAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string collectorName,
        CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(CollectorEverRanSql);
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, collectorName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            /* No row is impossible for this shape (both halves are scalar subqueries), but treating it as
               "we cannot tell" keeps the caller on its existing miss rather than asserting a gate. */
            return (true, null);
        }

        return (
            !reader.IsDBNull(0) && reader.GetBoolean(0),
            reader.IsDBNull(1) ? null : DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc));
    }

    private static async Task<(List<CollectorRuntimePrecondition.QueryStoreDatabaseState> States, DateTime? ObservedUtc)>
        ReadQueryStoreStatesAsync(
            NpgsqlDataSource postgres,
            int serverId,
            string? databaseName,
            CancellationToken cancellationToken)
    {
        var states = new List<CollectorRuntimePrecondition.QueryStoreDatabaseState>();
        DateTime? observedUtc = null;

        await using var command = postgres.CreateCommand(LatestQueryStoreStatesSql);
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddNullableText(command, string.IsNullOrWhiteSpace(databaseName) ? null : databaseName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            states.Add(new CollectorRuntimePrecondition.QueryStoreDatabaseState(
                reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1)));

            observedUtc ??= reader.IsDBNull(2)
                ? null
                : DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc);
        }

        return (states, observedUtc);
    }
}
