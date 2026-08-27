/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The service's writes into the V2 observability store: the servers registry (upserted on every
/// successful connect) and the per-run collection_log (one row per collector cycle, Lite's status
/// vocabulary: SUCCESS / PERMISSIONS / ERROR). Column names mirror Lite's DuckDB tables so
/// viewer/analysis SQL can twin — duckdb_duration_ms records the Postgres storage phase here.
/// Both writes are failure-isolated: they log at Debug and never throw, because an observability
/// write must never break the collection loop.
/// </summary>
public static class DarlingObservability
{
    /* is_enabled is written ONLY on first insert (a server reaching a connect is enabled by definition —
       only enabled servers are in the loop). The ON CONFLICT re-connect deliberately does NOT touch
       is_enabled, so a control-plane disable (config_monitored_servers.is_enabled = FALSE, mirrored onto
       this observed row by SyncServerEnabledStatesAsync) is never clobbered back to TRUE on the next
       connect. Before Stage 2 this forced is_enabled = TRUE on every connect and nothing ever read it.

       engine_kind (V82, #2530) is written on BOTH arms, unlike is_enabled: it is a probed fact about the
       target rather than an operator decision, so the re-connect arm is exactly where a re-pointed
       registration (same storage name, different engine) has to correct it. Internal so a pure test can pin
       the shape without a live store. */
    internal const string UpsertServerSql = @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, sql_major_version, engine_kind, created_date, modified_date, monthly_cost_usd, postgres_major_version)
VALUES ($1, $2, $3, TRUE, $4, $5, $6, $7, $7, $8, $9)
ON CONFLICT (server_id) DO UPDATE SET
    server_name = EXCLUDED.server_name,
    display_name = EXCLUDED.display_name,
    sql_engine_edition = EXCLUDED.sql_engine_edition,
    sql_major_version = EXCLUDED.sql_major_version,
    engine_kind = EXCLUDED.engine_kind,
    modified_date = EXCLUDED.modified_date,
    monthly_cost_usd = EXCLUDED.monthly_cost_usd,
    postgres_major_version = EXCLUDED.postgres_major_version;";

    /* Mirror the DESIRED config (config.config_monitored_servers) onto the OBSERVED registry
       (collect.servers) for the two fields the viewer/FinOps read straight off collect.servers: is_enabled
       and monthly_cost_usd. A disabled server drops out of the collection loop and stops upserting, so without
       this its observed row would keep its last (TRUE) value forever; likewise a cost-only edit reaches the
       FinOps display (which reads collect.servers) through THIS sync, so a cost change needs no
       disconnect+reconnect (see DarlingWorker.ServerDefinitionEquals, which deliberately no longer compares
       cost). Fires when EITHER field drifts. Internal so a pure test can pin the SHAPE. Runs on every reload. */
    internal const string SyncEnabledStatesSql = @"
UPDATE collect.servers s
SET is_enabled = c.is_enabled,
    monthly_cost_usd = c.monthly_cost_usd,
    modified_date = (now() AT TIME ZONE 'UTC')
FROM config.config_monitored_servers c
WHERE s.server_id = c.server_id
  AND (s.is_enabled IS DISTINCT FROM c.is_enabled
       OR s.monthly_cost_usd IS DISTINCT FROM c.monthly_cost_usd);";

    /* The DELETED-server half of the mirror (#2030): the sync above is an inner join on the config row, so a
       server REMOVED from config_monitored_servers (not merely disabled) kept its last observed is_enabled —
       almost always TRUE — forever, and every reader that scopes to `WHERE is_enabled` (the web dashboard's
       /api/fleet card list, get_fleet_overview, the fleet Collection-Health aggregate, FinOps) showed the
       ghost indefinitely. An observed row with NO desired row flips to disabled here. Deliberately an UPDATE,
       never a DELETE: the row anchors whatever collected history retention has not yet aged out, and a
       re-added server (same storage name ⇒ same server_id) gets flipped back by the sync above on the next
       reload, resuming under its old identity. The fleet sentinel (server_id 0) is never registered in
       collect.servers, so it cannot match. Internal so a pure test can pin the SHAPE. */
    internal const string DisableOrphanedServersSql = @"
UPDATE collect.servers s
SET is_enabled = FALSE,
    modified_date = (now() AT TIME ZONE 'UTC')
WHERE s.is_enabled
  AND NOT EXISTS
      (SELECT 1 FROM config.config_monitored_servers c WHERE c.server_id = s.server_id);";

    private const string InsertCollectionLogSql = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms, fanout_item_count, slowest_item, slowest_item_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);";

    /* The fleet-sentinel server_id the daily retention purge writes its run-record under. collection_log's
       server_id is NOT NULL and Collection Health reads per real server_id, but the purge is fleet-wide (per
       shared table, not per server), so it has no real server to attribute to. This reserved id is never a
       monitored server (server_ids are FNV-1a hashes of a storage name; it is also never registered in
       config_monitored_servers / collect.servers), so the sentinel row is: (a) EXCLUDED from the fleet
       Collection-Health aggregate — that read is scoped to server_id IN config_monitored_servers WHERE
       is_enabled; (b) never matched by any per-server read (all WHERE server_id = $realId) or the self-alert
       collection-signal reads; yet (c) fully auditable via v_collection_log (WHERE collector_name =
       'data_retention'). Deliberately NOT a new fleet UI surface — just an auditable log row. */
    internal const int FleetServerId = 0;
    private const string FleetServerName = "(fleet)";

    /* Matches the Dashboard's config.data_retention collector_name so the audit vocabulary twins across SKUs. */
    private const string RetentionCollectorName = "data_retention";

    /* The per-server analysis-state marker (V19): the analysis pass's insufficient-data determination,
       persisted so the Viewer's Recommendations tab can tell "still collecting" (a young deployment under
       the 24h data-span gate) apart from a genuine all-clear. One row per server, upserted after each
       completed pass on the natural key (server_id). The bare name resolves through the collect/config
       search path to collect.analysis_state — the observed-output schema, like servers/collection_log.
       Internal so a pure test can pin the UPSERT shape. */
    internal const string WriteAnalysisStateSql = @"
INSERT INTO analysis_state (server_id, insufficient_data, message, analysis_time)
VALUES ($1, $2, $3, $4)
ON CONFLICT (server_id) DO UPDATE SET
    insufficient_data = EXCLUDED.insufficient_data,
    message = EXCLUDED.message,
    analysis_time = EXCLUDED.analysis_time;";

    /// <summary>
    /// Registers (or refreshes) a connected server in the registry: created_date is written only on first
    /// insert, modified_date on every connect. is_enabled is set TRUE on first insert only and left
    /// UNTOUCHED on re-connect (Stage 2) so a control-plane disable is not resurrected — the desired enable
    /// state is mirrored onto this observed row by <see cref="SyncServerEnabledStatesAsync"/> on each reload.
    /// </summary>
    public static async Task UpsertServerAsync(NpgsqlDataSource postgres, ServerRuntime server, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(UpsertServerSql, connection);
            command.Parameters.AddWithValue(server.ServerId);
            command.Parameters.AddWithValue(server.StorageName);
            command.Parameters.AddWithValue(server.Config.DisplayName);
            /* The raw probed SERVERPROPERTY('EngineEdition') — real box editions (2/3/4...), not
               just the 5/8 Azure classifications. */
            command.Parameters.AddWithValue(server.EngineEdition);
            command.Parameters.AddWithValue(server.Target.SqlMajorVersion);
            /* The engine KIND (#2530), derived from the target the connector probed rather than from the
               configured engine string: Aurora-ness is not configurable — it comes from aurora_version being
               present in pg_proc — and it is half of what this column exists to carry. */
            command.Parameters.AddWithValue(MonitoredEngineKind.For(server.Target));
            /* Naive-UTC storage: Npgsql 6+ rejects Kind=Utc against `timestamp` — see PgCollectorRowWriter. */
            command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
            /* Per-server FinOps budget from darling.json (0 = hide cost in the viewer, like Lite). */
            command.Parameters.AddWithValue(server.Config.MonthlyCostUsd);
            /* The probed PostgreSQL major (V100, #2653), so a READ can explain a column the target's version
               does not have. DBNull rather than 0 off a PostgreSQL target: the reads treat NULL as "no claim",
               and a 0 would be a version nobody runs asserted as fact. Guarded on Engine as well as on the
               value because 0 is also what a PostgreSQL probe that failed before reading server_version_num
               leaves behind. */
            command.Parameters.AddWithValue(
                server.Target.Engine == CollectorTargetEngine.PostgreSql && server.Target.PostgresMajorVersion > 0
                    ? server.Target.PostgresMajorVersion
                    : (object)DBNull.Value);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            /* Failure-isolated by design — an observability write must never break the collection loop. */
            logger?.LogDebug("Observability: servers upsert for '{Server}' failed: {Message}",
                server.Config.DisplayName, ex.Message);
        }
    }

    /// <summary>
    /// Mirrors the DESIRED config (<c>config.config_monitored_servers</c>) onto the OBSERVED registry
    /// (<c>collect.servers</c>) for the two fields the viewer/FinOps read straight off <c>collect.servers</c>:
    /// <c>is_enabled</c> and <c>monthly_cost_usd</c>. So a control-plane <c>disable_server</c> flips the
    /// observed row to FALSE even though the disabled server stops upserting (a later <c>enable_server</c>
    /// flips it back), and a cost-only edit reaches the FinOps display through this reload sync with NO
    /// disconnect+reconnect. A server DELETED from the desired config is handled by the second statement
    /// (#2030): its orphaned observed row flips to disabled, so it leaves the web dashboard and every other
    /// <c>WHERE is_enabled</c> reader instead of lingering as a ghost. Runs on each control-plane reload.
    /// Failure-isolated (Debug + no-op) like the other observability writes.
    /// </summary>
    public static async Task SyncServerEnabledStatesAsync(NpgsqlDataSource postgres, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            using (var command = new NpgsqlCommand(SyncEnabledStatesSql, connection))
            {
                var changed = await command.ExecuteNonQueryAsync(cancellationToken);
                if (changed > 0)
                {
                    logger?.LogInformation("Synced observed enable-state/cost for {Count} server(s) from the desired config", changed);
                }
            }

            using (var command = new NpgsqlCommand(DisableOrphanedServersSql, connection))
            {
                var orphaned = await command.ExecuteNonQueryAsync(cancellationToken);
                if (orphaned > 0)
                {
                    logger?.LogInformation("Disabled {Count} observed server(s) no longer present in the desired config (removed from monitoring; collected history stays until retention ages it out)", orphaned);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Failure-isolated by design — an observability write must never break the collection loop. */
            logger?.LogDebug("Observability: server enable-state sync failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Records one collector run's outcome in collection_log. duration_ms is the SQL fetch plus
    /// the storage phase; duckdb_duration_ms carries the storage (Postgres) milliseconds under
    /// Lite's column name so analysis SQL can twin.
    /// </summary>
    /// <param name="fanout">
    /// The per-database rollup for a run that fanned out, null for one that did not (#2472). REQUIRED rather
    /// than defaulted on purpose: five collectors fan out and the rest do not, so every call site has to say
    /// which it is. A default would have let the five failure sites below — which genuinely have no fan-out —
    /// stand in for a success site that forgot, and the whole point of the columns is that a blended number
    /// stops being the only thing recorded.
    /// </param>
    public static async Task LogCollectionAsync(
        NpgsqlDataSource postgres,
        ServerRuntime server,
        string collectorName,
        string status,
        int rowsCollected,
        long sqlMs,
        long storageMs,
        string? errorMessage,
        FanoutCost? fanout,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        try
        {
            var message = errorMessage;
            if (message is not null && message.Length > 4000)
            {
                message = message.Substring(0, 4000);
            }

            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(InsertCollectionLogSql, connection);
            command.Parameters.AddWithValue(CollectionIdGenerator.Next());
            command.Parameters.AddWithValue(server.ServerId);
            command.Parameters.AddWithValue(server.StorageName);
            command.Parameters.AddWithValue(collectorName);
            /* Naive-UTC storage: Npgsql 6+ rejects Kind=Utc against `timestamp` — see PgCollectorRowWriter. */
            command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue((int)(sqlMs + storageMs));
            command.Parameters.AddWithValue(status);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)message ?? DBNull.Value });
            command.Parameters.AddWithValue(rowsCollected);
            command.Parameters.AddWithValue((int)sqlMs);
            command.Parameters.AddWithValue((int)storageMs);

            /* All three NULL together or all three set: a slowest item with no count cannot be turned into
               the dominance ratio the columns exist for, so half an answer is worse than none. */
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = fanout.HasValue ? fanout.Value.ItemCount : (object)DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = fanout.HasValue ? fanout.Value.SlowestItem : (object)DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = fanout.HasValue ? fanout.Value.SlowestItemMs : (object)DBNull.Value });
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            /* Failure-isolated by design — an observability write must never break the collection loop. */
            logger?.LogDebug("Observability: collection_log write for '{Server}' / {Collector} failed: {Message}",
                server.Config.DisplayName, collectorName, ex.Message);
        }
    }

    /// <summary>
    /// Records one daily retention-purge sweep in collection_log as an auditable <c>data_retention</c>
    /// run-record (the Darling twin of the Dashboard's config.data_retention log rows). Fleet-wide, so it is
    /// written under the reserved <see cref="FleetServerId"/> sentinel — never a real monitored server, and
    /// excluded from every per-server and fleet Collection-Health read (see the constant's remarks). <paramref
    /// name="status"/> is SUCCESS / WARNING (some tables failed) / ERROR; <paramref name="rowsPurged"/> is the
    /// coarse rows-deleted-plus-chunks-dropped activity count; <paramref name="durationMs"/> is the whole-sweep
    /// elapsed (recorded as both the total and the storage phase — a purge is all store-side work, no SQL-target
    /// phase). Failure-isolated (Debug + no-op) like the other observability writes — an audit write must never
    /// break the collection loop.
    /// </summary>
    public static async Task LogRetentionRunAsync(
        NpgsqlDataSource postgres,
        string status,
        int rowsPurged,
        long durationMs,
        string? message,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        try
        {
            var text = message;
            if (text is not null && text.Length > 4000)
            {
                text = text.Substring(0, 4000);
            }

            var elapsed = durationMs > int.MaxValue ? int.MaxValue : (int)durationMs;

            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(InsertCollectionLogSql, connection);
            command.Parameters.AddWithValue(CollectionIdGenerator.Next());                                    // log_id
            command.Parameters.AddWithValue(FleetServerId);                                                   // server_id (sentinel)
            command.Parameters.AddWithValue(FleetServerName);                                                 // server_name
            command.Parameters.AddWithValue(RetentionCollectorName);                                          // collector_name
            /* Naive-UTC storage: Npgsql 6+ rejects Kind=Utc against `timestamp` — see PgCollectorRowWriter. */
            command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified)); // collection_time
            command.Parameters.AddWithValue(elapsed);                                                         // duration_ms
            command.Parameters.AddWithValue(status);                                                          // status
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)text ?? DBNull.Value }); // error_message
            command.Parameters.AddWithValue(rowsPurged);                                                      // rows_collected
            command.Parameters.AddWithValue(0);                                                               // sql_duration_ms (no SQL-target phase)
            command.Parameters.AddWithValue(elapsed);                                                         // duckdb_duration_ms (storage phase = whole sweep)

            /* The fan-out rollup columns (#2472). The retention sweep is fleet-wide over shared tables, not
               a per-database fan-out, so it has nothing to attribute and says so with NULL rather than a
               zero that would read as "fanned out over nothing". These three exist because this INSERT is
               shared with the collector writer above; the shape is one statement on purpose. */
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DBNull.Value }); // fanout_item_count
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });    // slowest_item
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DBNull.Value }); // slowest_item_ms
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            /* Failure-isolated by design — an observability write must never break the collection loop. */
            logger?.LogDebug("Observability: data_retention run-record write failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Upserts the per-server analysis-state marker (V19) after an analysis pass: <c>insufficient_data =
    /// true</c> plus the engine's message when the pass hit the 24h data-span gate, or <c>false</c> + null
    /// when a real pass completed on enough data. The engine ALREADY makes this determination
    /// (<c>DarlingAnalysisService.InsufficientDataMessage</c>); this persists it so the Viewer's
    /// Recommendations tab — which never calls the engine — shows "still collecting" instead of a false
    /// all-clear on a young deployment's zero-finding read. One row per server, upserted on
    /// <c>server_id</c>. Failure-isolated (Debug + no-op) like the other observability writes — an
    /// analysis-state write must never break the collection loop.
    /// </summary>
    public static async Task WriteAnalysisStateAsync(
        NpgsqlDataSource postgres,
        int serverId,
        bool insufficientData,
        string? message,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(WriteAnalysisStateSql, connection);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(insufficientData);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)message ?? DBNull.Value });
            /* Naive-UTC storage: Npgsql 6+ rejects Kind=Utc against `timestamp` — see PgCollectorRowWriter. */
            command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Failure-isolated by design — an observability write must never break the collection loop. */
            logger?.LogDebug("Observability: analysis-state write for server {ServerId} failed: {Message}",
                serverId, ex.Message);
        }
    }
}
