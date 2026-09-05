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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The retained <c>(server_name, sql_handle) → module</c> attribution table. query_stats' <c>object_name</c> is not
/// a query_stats column — it is stitched from procedure_stats via <c>sql_handle</c> at read time (#1568). That join
/// works against RAW procedure_stats, but raw is dropped at 4 days, so an OLD-window query_stats panel (served by
/// the query_stats CAGG) has no live procedure_stats to join to. This table is that join's retained source: it
/// ACCUMULATES the sql_handle → object_name mapping indefinitely (tiny — one row per distinct handle), upserted from
/// the last couple of days of procedure_stats, so the composer can attribute CAGG rows to their module for any age.
///
/// <para>Idempotent RUNTIME setup (a plain table created in the worker's setup, NOT a versioned migration — the
/// same posture as the CAGGs / <see cref="PgTableTuning"/>, so it never bumps the schema version or gates the
/// viewer). Deliberately NOT a collector table and NOT a hypertable: it is a small mutable dimension, not
/// time-series, and NOT subject to the tiered retention.</para>
/// </summary>
public static class DarlingModuleMap
{
    private const int SetupTimeoutSeconds = 300;

    /// <summary>The attribution table. <c>(server_name, sql_handle)</c> is the identity (a handle reused across
    /// servers attributes per server, matching the #1568 join's partitioning).</summary>
    public const string CreateTableSql = @"CREATE TABLE IF NOT EXISTS collect.module_map (
    server_name text NOT NULL,
    sql_handle text NOT NULL,
    database_name text,
    schema_name text,
    object_name text,
    last_seen timestamp,
    PRIMARY KEY (server_name, sql_handle)
)";

    /// <summary>
    /// Upserts the latest <c>(server_name, sql_handle) → (database_name, schema_name, object_name)</c> from the last
    /// two days of procedure_stats — well inside its 4-day raw retention, so no handle is missed between runs.
    /// ACCUMULATES: a handle that stops appearing keeps its last-known attribution forever (no delete). DISTINCT ON
    /// keeps the most-recent row per handle; the ON CONFLICT only advances a row (never regresses last_seen), so a
    /// stale run can't overwrite a fresher attribution.
    ///
    /// <para><c>now()</c> IS LEFT BARE HERE, alone in the store's SQL, and it survives on margin rather than on
    /// being harmless. <c>collection_time</c> is naive UTC and <c>now()</c> is <c>timestamptz</c>, so the store
    /// session's TimeZone shifts this bound like any other: across the real offset range (UTC-12..UTC+14) the
    /// 48-hour window delivers between 34 and 60 hours. Both ends are safe for THIS query and only because of
    /// the two numbers either side of it — 34h still covers the refresh cadence (startup, then riding the 24h
    /// purge) so no handle is missed between runs, and 60h is still inside
    /// <c>TimescaleSupport.RawRetentionInterval</c> (4 days) so the widened end scans nothing that has been
    /// dropped. The map also ACCUMULATES, so a wider window only re-reads rows it already has. Shorten the
    /// cadence past 34h or the raw retention past 60h and this has to become a bound parameter like every other
    /// windowed read; it is not a licence for the next one.</para>
    ///
    /// <para>The <c>ORDER BY</c> its DISTINCT ON requires also happens to be a deterministic ascending order
    /// on the conflict key, so concurrent refreshes take these row locks in the same relative order and the
    /// unordered-batch-upsert deadlock (#1801) cannot form here. Checked, not assumed -- do not drop or
    /// reorder it on the belief that it is only about picking the latest row.</para>
    /// </summary>
    public const string RefreshSql = @"INSERT INTO collect.module_map (server_name, sql_handle, database_name, schema_name, object_name, last_seen)
SELECT DISTINCT ON (server_name, sql_handle)
    server_name, sql_handle, database_name, schema_name, object_name, collection_time
FROM collect.procedure_stats
WHERE collection_time >= now() - interval '2 days'
  AND sql_handle IS NOT NULL
  AND sql_handle <> ''
ORDER BY server_name, sql_handle, collection_time DESC
ON CONFLICT (server_name, sql_handle) DO UPDATE SET
    database_name = EXCLUDED.database_name,
    schema_name = EXCLUDED.schema_name,
    object_name = EXCLUDED.object_name,
    last_seen = EXCLUDED.last_seen
WHERE module_map.last_seen IS NULL OR EXCLUDED.last_seen >= module_map.last_seen";

    /// <summary>Creates the table if absent (idempotent). Returns true on success; a failure warns and leaves
    /// object_name-on-query_stats routing to fall back to raw (never kills startup).</summary>
    public static async Task<bool> EnsureTableAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        try
        {
            using var command = new NpgsqlCommand(CreateTableSql, connection) { CommandTimeout = SetupTimeoutSeconds };
            await command.ExecuteNonQueryAsync(cancellationToken);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("module_map table setup failed — object_name-on-query_stats routing falls back to raw: {Message}", ex.Message);
            return false;
        }
    }

    /// <summary>Upserts the map from recent procedure_stats. Returns the number of rows upserted; a failure warns
    /// and the existing map keeps serving (never throws). Called from the periodic maintenance sweep.</summary>
    public static async Task<int> RefreshAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        try
        {
            using var command = new NpgsqlCommand(RefreshSql, connection) { CommandTimeout = SetupTimeoutSeconds };
            var rows = await command.ExecuteNonQueryAsync(cancellationToken);
            logger?.LogInformation("module_map: upserted {Rows} sql_handle→module attribution(s) from recent procedure_stats.", rows);
            return rows;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("module_map refresh failed — the existing map keeps serving: {Message}", ex.Message);
            return 0;
        }
    }
}
