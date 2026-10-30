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
using PerformanceMonitor.Darling.Storage;

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
    /// When one collector last ran for one server, beside when that server last and first collected anything
    /// at all. All three in one round trip, because the inference needs them together and they must describe
    /// the same instant: a collector that has gone dark while the server keeps collecting is gated off, while a
    /// collector with no recent rows on a server that has collected nothing is just a collection outage. The
    /// first collection feeds the first-run grace. It is read as the oldest row in time order,
    /// <c>ORDER BY collection_time ASC LIMIT 1</c>, not as a <c>MIN</c>: <c>collection_time</c> is the
    /// hypertable's time dimension, so the ordered read can stop in the oldest chunk, which is likely compressed
    /// and out of reach of <c>idx_collection_log_time</c>. A read that fails or times out makes no claim:
    /// <see cref="GatedOffStatusAsync"/> answers null, and the tool keeps its own miss.
    ///
    /// <para>The collector half reads the latest run's <c>collection_time</c> rather than probing for
    /// PRESENCE, because presence is the wrong question — see
    /// <see cref="CollectorRuntimePrecondition.GatedOffMessage"/> for why a <c>collection_log</c> row is not
    /// proof of a run. It orders by <c>collection_time DESC</c>, the hypertable's time dimension, so the newest chunk
    /// answers first and a collector that ran recently is found there at once. Ordering by <c>log_id</c> instead would read
    /// the server's whole retained history on every call, because <c>collection_log</c> has no primary key and no index on
    /// <c>log_id</c>. Only the timestamp is returned, so two runs that share one give the same answer whichever comes first;
    /// that is why this read does not need the <c>log_id</c> tiebreak that <see cref="LatestCollectorOutcomeSql"/> keeps, where the row's status
    /// is the answer. The server half uses <c>idx_collection_log_time (server_id, collection_time)</c>. The collector half
    /// filters with <c>idx_collection_log_watermark (server_id, collector_name, collection_time DESC)</c>. A collector that
    /// has never run has no row to find, so for it the read still goes through the server's whole retained history.
    /// $1 server_id, $2 collector.</para>
    /// </summary>
    public const string CollectorLastRunSql = $@"
SELECT (
           SELECT collection_time
           FROM collection_log
           WHERE server_id = $1
           AND   collector_name = $2
           ORDER BY collection_time DESC
           LIMIT 1
       ) AS collector_last_run,
       (
           SELECT MAX(collection_time)
           FROM collection_log
           WHERE server_id = $1
           AND   {CollectionCycleRow.NotCycleRowSql}
       ) AS server_last_collected,
       (
           SELECT collection_time
           FROM collection_log
           WHERE server_id = $1
           AND   {CollectionCycleRow.NotCycleRowSql}
           ORDER BY collection_time ASC
           LIMIT 1
       ) AS server_first_collected";

    /// <summary>
    /// The most recent run of one collector for one server. Ordered by <c>collection_time DESC</c> first (#4974):
    /// <c>idx_collection_log_watermark (server_id, collector_name, collection_time DESC)</c> serves that first key,
    /// so ChunkAppend walks the chunks newest-first and stops at the first row the walk returns, where a
    /// <c>log_id</c>-first order (no index on <c>log_id</c>) read every retained row of the pair. The trade: after
    /// the wall clock steps back, this read can return a run from before the step, for as long as the step lasts.
    ///
    /// <para><c>, log_id DESC</c> only makes the order deterministic; two runs of one server and collector do not
    /// share a microsecond in practice. No index carries <c>log_id</c>, so the tiebreak is a sort. In the plans a
    /// test store gets, it sits inside each chunk the walk visits: a Sort of the pair's rows in that chunk, or an
    /// Incremental Sort presorted on <c>collection_time</c> that reads the rows at the newest timestamp plus one
    /// more to close that group. An Incremental Sort above the walk would instead take that extra row from the
    /// next older chunk whenever the newest chunk holds a single row for the pair. The newest chunk answers a
    /// collector that ran recently. A collector with no row in the newest chunk (a daily collector, or one that
    /// never ran) walks back through older chunks, which are compressed.</para>
    ///
    /// <para><see cref="DarlingSelfAlertEvaluator.MissingCaptureSessionsSql"/>, a one-collector read of the same
    /// table, leaves this tiebreak out: there it would decide nothing, and its <c>(server_id, collection_time)</c>
    /// index cannot serve a second key, so the sort would cover the server's whole newest chunk (1,158 buffers
    /// against 5). $1 server_id, $2 collector.</para>
    /// </summary>
    public const string LatestCollectorOutcomeSql = CollectorOutcomeStatements.LatestCollectorOutcomeSql;

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
SELECT database_name, actual_state, capture_time, interval_length_minutes
FROM v_query_store_health
WHERE server_id = $1
AND   capture_time = (SELECT MAX(capture_time) FROM v_query_store_health WHERE server_id = $1)
AND   ($2::text[] IS NULL OR database_name = ANY($2))
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
    /// #6453: the sentence for a read that found no stored rows while the collector serving it is HEALTHY: its newest
    /// <c>collection_log</c> run is <c>SUCCESS</c> with no note, so the monitored server answered and had nothing for it. Null
    /// when there is no run, the newest run is anything else (a fault, a denial, a missing session, a note that explains the
    /// zero), or the log read fails; the caller then keeps its own <c>unavailable</c> text. The caller answers it with
    /// <c>empty</c>: <c>get_collection_health</c> calls the same collector HEALTHY ("every run completed and its source returned
    /// no rows"), and a read that said <c>unavailable</c> beside it sent the reader in a circle.
    /// <para>Call this AFTER <see cref="StatusAsync"/> and the engine gate, never before: a recorded precondition or a
    /// wrong-engine target is a stronger answer than a clean run.</para>
    /// </summary>
    public static async Task<string?> HealthyEmptyMessageAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string serverName,
        string collectorName,
        string what,
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
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }

        return CollectorRuntimePrecondition.HealthyEmptyMessage(serverName, collectorName, what, status, errorMessage, observedUtc);
    }

    /// <summary>
    /// The <c>precondition</c> envelope when the collector serving this read is not being invoked against
    /// this server while the server is collecting normally — i.e. its <c>AppliesTo</c> gate is off — or
    /// <c>null</c> otherwise. A collector that has never run and is not due yet gets the <c>unavailable</c>
    /// envelope instead (see <see cref="CollectorRuntimePrecondition.GatedOffStatusWord"/>).
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
        DateTime? collectorLastRunUtc;
        DateTime? serverLastCollectedUtc;
        DateTime? serverFirstCollectedUtc;
        DateTime? serviceStartedUtc;

        try
        {
            (collectorLastRunUtc, serverLastCollectedUtc, serverFirstCollectedUtc) =
                await ReadCollectorLastRunAsync(postgres, serverId, collectorName, cancellationToken);
            serviceStartedUtc = collectorLastRunUtc is null
                ? await ReadServiceStartedAsync(postgres, cancellationToken)
                : null;
        }
        catch (Exception)
        {
            /* Same rule as the rest of this class: a diagnostic that throws must not turn an honest miss
               into a read error. */
            return null;
        }

        var message = CollectorRuntimePrecondition.GatedOffMessage(
            serverName, collectorName, gateCandidates, collectorLastRunUtc, serverLastCollectedUtc, serverFirstCollectedUtc, serviceStartedUtc);

        return message is null ? null : McpHelpers.Status(
            CollectorRuntimePrecondition.GatedOffStatusWord(collectorName, collectorLastRunUtc, serverLastCollectedUtc, serverFirstCollectedUtc, serviceStartedUtc),
            message);
    }

    /// <summary>
    /// The <c>precondition</c> envelope when this server's recorded Query Store configuration says the
    /// databases the read covered are not recording runtime statistics, or <c>null</c> when it says nothing
    /// of the kind (no snapshot yet, or at least one database in scope is READ_WRITE).
    /// </summary>
    public static Task<string?> QueryStoreStatusAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string serverName,
        string? databaseName,
        CancellationToken cancellationToken = default) =>
        QueryStoreStatusAsync(postgres, serverId, serverName, DatabaseFilter.One(databaseName), cancellationToken);

    /// <summary>
    /// #5245: <see cref="QueryStoreStatusAsync(NpgsqlDataSource,int,string,string,CancellationToken)"/> over a SET of
    /// databases. The existing rule is kept as it was, not turned into a per-database verdict: the answer is silent
    /// (<c>null</c>) when at least one database in scope is READ_WRITE (Query Store is collecting somewhere the read could
    /// have looked, so the emptiness has another cause), and the precondition envelope when every database in scope
    /// reports not collecting. <see cref="DatabaseFilter.All"/> reads every database's state, as a null name did.
    /// </summary>
    public static async Task<string?> QueryStoreStatusAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string serverName,
        DatabaseFilter databases,
        CancellationToken cancellationToken = default)
    {
        List<CollectorRuntimePrecondition.QueryStoreDatabaseState> states;
        DateTime? observedUtc;

        try
        {
            (states, observedUtc) =
                await ReadQueryStoreStatesAsync(postgres, serverId, databases, cancellationToken);
        }
        catch (Exception)
        {
            return null;
        }

        var message = CollectorRuntimePrecondition.QueryStoreDisabledMessage(serverName, states, observedUtc);
        return message is null ? null : McpHelpers.Status(CollectorRuntimePrecondition.StatusWord, message);
    }

    /// <summary>
    /// #6554: the three facts <see cref="CollectorRuntimePrecondition.QueryStoreReadableSecondaryMessage"/> judges, in one round trip:
    /// the newest collected <c>ag_replica_role</c>, whether the server names a <c>query_store_replica</c>, and how many databases
    /// in the newest <c>query_store_health</c> capture carry the readable-secondary bit (of how many, leaving out tempdb and databases whose
    /// Query Store is OFF: the on-prem capture always holds tempdb, and neither has Query Store rows to read). $1 server_id.
    /// </summary>
    public const string ReadableSecondaryFactsSql = @"
SELECT
    (SELECT sp.ag_replica_role FROM server_properties AS sp WHERE sp.server_id = $1 ORDER BY sp.collection_time DESC LIMIT 1),
    (SELECT COALESCE(NULLIF(btrim(c.query_store_replica), ''), NULL) FROM config.config_monitored_servers AS c WHERE c.server_id = $1),
    (SELECT COUNT(*) FROM v_query_store_health AS h
      WHERE h.server_id = $1 AND h.capture_time = (SELECT MAX(h2.capture_time) FROM v_query_store_health AS h2 WHERE h2.server_id = $1)
        AND h.database_name <> 'tempdb' AND UPPER(COALESCE(h.actual_state, '')) <> 'OFF'),
    (SELECT COUNT(*) FROM v_query_store_health AS h
      WHERE h.server_id = $1 AND (h.readonly_reason & 8) <> 0
        AND h.capture_time = (SELECT MAX(h2.capture_time) FROM v_query_store_health AS h2 WHERE h2.server_id = $1)
        AND h.database_name <> 'tempdb' AND UPPER(COALESCE(h.actual_state, '')) <> 'OFF')";

    /// <summary>
    /// #6554: the <c>not_collected</c> envelope for a Query Store read on a server that is a readable secondary with
    /// <c>query_store_replica</c> off, or <c>null</c> (the caller keeps its own answer) when the evidence does not say so or the
    /// read fails: this runs on a path that already found nothing.
    /// </summary>
    public static async Task<string?> QueryStoreReadableSecondaryStatusAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, CancellationToken cancellationToken = default)
    {
        var message = await QueryStoreReadableSecondaryMessageAsync(postgres, serverId, serverName, cancellationToken);
        return message is null ? null : McpHelpers.Status(CollectorRuntimePrecondition.NotCollectedStatusWord, message);
    }

    /// <summary>
    /// #6554: the one-line reason a Query Store read on this server is empty because it is a readable secondary with
    /// <c>query_store_replica</c> off, or <c>null</c> when it is not or the read fails. The one source of the wording: the
    /// Query Store reads' <c>not_collected</c> answer and a Change Watch request's brief both use it.
    /// </summary>
    public static async Task<string?> QueryStoreReadableSecondaryMessageAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = postgres.CreateCommand(ReadableSecondaryFactsSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return CollectorRuntimePrecondition.QueryStoreReadableSecondaryMessage(
                serverName,
                reader.IsDBNull(0) ? null : reader.GetString(0),
                (int)reader.GetInt64(2),
                (int)reader.GetInt64(3),
                replicaConfigured: !reader.IsDBNull(1),
                "Darling collects Query Store from a registered server only when that server is not a readable secondary, and query_store_replica is off for it. " +
                "Register the primary, or set query_store_replica on the primary to read Query Store from a readable secondary (edit_server).");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>
    /// #6181: the message for a Query Store read that found no sample EVER stored, when the latest health snapshot says Query
    /// Store is on and the query_store collector's latest run succeeded (see
    /// <see cref="CollectorRuntimePrecondition.QueryStoreFirstIntervalMessage"/>), or null when it does not (state unknown,
    /// off, read-only, in error, or a collector run that failed or is missing) and the caller keeps its "may be OFF" text.
    /// Null as well when either read fails: this runs on a path that already found nothing.
    /// </summary>
    public static async Task<string?> QueryStoreFirstIntervalMessageAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, DatabaseFilter databases, CancellationToken cancellationToken = default)
    {
        try
        {
            var (states, observedUtc) = await ReadQueryStoreStatesAsync(postgres, serverId, databases, cancellationToken);
            var (status, _, _) = await ReadLatestOutcomeAsync(postgres, serverId, "query_store", cancellationToken);
            return CollectorRuntimePrecondition.QueryStoreFirstIntervalMessage(serverName, states, status, observedUtc);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>
    /// #6074: the sentence a Query Store read that found nothing appends when a database it covers is not READ_WRITE
    /// (see <see cref="CollectorRuntimePrecondition.QueryStoreNotWritingHint"/>), read from the same
    /// <c>v_query_store_health</c> row <c>get_query_store_health</c> answers from. Null when every database in scope is collecting,
    /// when there is no health row, and when the read fails: this runs on a path that already found nothing, and a diagnostic
    /// must not turn that into an error.
    /// </summary>
    public static async Task<string?> QueryStoreNotWritingHintAsync(
        NpgsqlDataSource postgres, int serverId, DatabaseFilter databases, CancellationToken cancellationToken = default)
    {
        try
        {
            var (states, observedUtc) = await ReadQueryStoreStatesAsync(postgres, serverId, databases, cancellationToken);
            return CollectorRuntimePrecondition.QueryStoreNotWritingHint(states, observedUtc);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>
    /// <paramref name="answer"/> with <see cref="QueryStoreNotWritingHintAsync"/>'s sentence appended to its message when the
    /// answer is an empty or unavailable status (#6074). Other statuses (a precondition, a not-collected answer, an error) already say their own cause.
    /// </summary>
    public static async Task<string> WithQueryStoreNotWritingHintAsync(
        string answer, NpgsqlDataSource postgres, int serverId, string serverName, DatabaseFilter databases, CancellationToken cancellationToken = default)
    {
        if (!McpHelpers.IsEmptyOrUnavailableStatus(answer))
        {
            return answer;
        }

        /* #6554: on a readable secondary READ_ONLY is the normal state, so say it is not collected instead of hinting at a fault. */
        if (await QueryStoreReadableSecondaryStatusAsync(postgres, serverId, serverName, cancellationToken) is { } notCollected)
        {
            return notCollected;
        }

        return McpHelpers.AppendToMessage(answer, await QueryStoreNotWritingHintAsync(postgres, serverId, databases, cancellationToken));
    }

    private static async Task<(string? Status, string? ErrorMessage, DateTime? ObservedUtc)> ReadLatestOutcomeAsync(
        NpgsqlDataSource postgres,
        int serverId,
        string collectorName,
        CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(LatestCollectorOutcomeSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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

    /// <summary>
    /// #6932: when the newest collecting service process started, from the version row each service host writes at startup
    /// (<c>config.service_version</c>), or null when no host has recorded one. Read only for a collector with no run at all, which is
    /// the one case that needs it: a collector an upgrade adds waits one cadence after this instant for its first slot, and the
    /// server's first collection (long ago) cannot say so.
    /// </summary>
    private static async Task<DateTime?> ReadServiceStartedAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand("SELECT MAX(process_started_at) FROM config.service_version");
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is DateTime started ? DateTime.SpecifyKind(started, DateTimeKind.Utc) : null;
    }

    private static async Task<(DateTime? CollectorLastRunUtc, DateTime? ServerLastCollectedUtc, DateTime? ServerFirstCollectedUtc)>
        ReadCollectorLastRunAsync(
            NpgsqlDataSource postgres,
            int serverId,
            string collectorName,
            CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(CollectorLastRunSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, collectorName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            /* No row is impossible for this shape (all three are scalar subqueries), but answering "the
               server has collected nothing" keeps the caller on its existing miss rather than asserting a
               gate from a read that told us nothing. */
            return (null, null, null);
        }

        return CollectorRuntimePrecondition.CollectorLastRunFrom(reader);
    }

    private static async Task<(List<CollectorRuntimePrecondition.QueryStoreDatabaseState> States, DateTime? ObservedUtc)>
        ReadQueryStoreStatesAsync(
            NpgsqlDataSource postgres,
            int serverId,
            DatabaseFilter databases,
            CancellationToken cancellationToken)
    {
        var states = new List<CollectorRuntimePrecondition.QueryStoreDatabaseState>();
        DateTime? observedUtc = null;

        await using var command = postgres.CreateCommand(LatestQueryStoreStatesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        command.Parameters.Add(databases.Parameter());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            states.Add(new CollectorRuntimePrecondition.QueryStoreDatabaseState(
                reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(3) ? null : reader.GetInt64(3)));

            observedUtc ??= reader.IsDBNull(2)
                ? null
                : DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc);
        }

        return (states, observedUtc);
    }
}
