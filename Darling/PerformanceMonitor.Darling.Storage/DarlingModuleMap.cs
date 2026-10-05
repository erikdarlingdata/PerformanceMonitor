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

    /// <summary>The refresh watermark: one row (<c>id = 1</c>) holding the largest procedure_stats
    /// <c>collection_time</c> the last refresh read. It is written in the same statement, and so the same snapshot,
    /// as the map upsert, so a reader that trusts the watermark knows the map holds every attribution up to it.
    /// Naive UTC like every timestamp in the store. A runtime table created beside <see cref="CreateTableSql"/>,
    /// not a migration.</summary>
    public const string CreateStateTableSql = @"CREATE TABLE IF NOT EXISTS collect.module_map_state (
    id integer PRIMARY KEY CHECK (id = 1),
    refreshed_through timestamp,
    refreshed_at timestamp
)";

    /// <summary>The late-commit slack the hourly refresh re-reads behind the watermark: a procedure_stats row
    /// committed up to this long after its <c>collection_time</c> is still picked up by the next refresh. A row
    /// committed later than that waits for the daily refresh.</summary>
    public static readonly TimeSpan WatermarkSlack = TimeSpan.FromMinutes(10);

    /// <summary>The farthest back an hourly refresh ever reads, inside procedure_stats' 4-day raw retention.
    /// It bounds the first refresh on a store with no watermark, and a refresh after a long stall.</summary>
    public static readonly TimeSpan MaxLookback = TimeSpan.FromDays(2);

    /// <summary>
    /// The statement tail both refreshes share, after their own <c>WITH src AS (...)</c> head: upsert the latest
    /// attribution per handle from <c>src</c>, then advance the watermark in the same statement.
    /// ACCUMULATES: a handle that stops appearing keeps its last-known attribution forever (no delete). DISTINCT ON
    /// keeps the most-recent row per handle; the ON CONFLICT only advances a row (never regresses last_seen), so a
    /// stale run can't overwrite a fresher attribution. The watermark only moves forward too (GREATEST), never
    /// runs ahead of the store's clock (LEAST: one row stamped in the future would otherwise pin it there for good,
    /// since GREATEST never lets it back down), and an empty <c>src</c> writes nothing, so a refresh that read no
    /// rows leaves it where it was.
    ///
    /// <para>The <c>ORDER BY</c> its DISTINCT ON requires also happens to be a deterministic ascending order
    /// on the conflict key, so concurrent refreshes take these row locks in the same relative order and the
    /// unordered-batch-upsert deadlock (#1801) cannot form here. Checked, not assumed -- do not drop or
    /// reorder it on the belief that it is only about picking the latest row.</para>
    ///
    /// <para>The statement returns one row: the number of map rows upserted and the watermark after the
    /// statement (null when <c>src</c> was empty).</para>
    /// </summary>
    private const string UpsertAndAdvanceTail = @"up AS (
    INSERT INTO collect.module_map (server_name, sql_handle, database_name, schema_name, object_name, last_seen)
    SELECT DISTINCT ON (server_name, sql_handle)
        server_name, sql_handle, database_name, schema_name, object_name, collection_time
    FROM src
    ORDER BY server_name, sql_handle, collection_time DESC
    ON CONFLICT (server_name, sql_handle) DO UPDATE SET
        database_name = EXCLUDED.database_name,
        schema_name = EXCLUDED.schema_name,
        object_name = EXCLUDED.object_name,
        last_seen = EXCLUDED.last_seen
    WHERE module_map.last_seen IS NULL OR EXCLUDED.last_seen >= module_map.last_seen
    RETURNING 1
),
st AS (
    INSERT INTO collect.module_map_state (id, refreshed_through, refreshed_at)
    SELECT 1, LEAST(max(collection_time), (now() AT TIME ZONE 'UTC')::timestamp), (now() AT TIME ZONE 'UTC')::timestamp
    FROM src
    HAVING max(collection_time) IS NOT NULL
    ON CONFLICT (id) DO UPDATE SET
        refreshed_through = GREATEST(module_map_state.refreshed_through, EXCLUDED.refreshed_through),
        refreshed_at = EXCLUDED.refreshed_at
    RETURNING refreshed_through
)
SELECT (SELECT count(*) FROM up)::integer, (SELECT refreshed_through FROM st)";

    /// <summary>
    /// The daily refresh: upserts the latest <c>(server_name, sql_handle) → (database_name, schema_name,
    /// object_name)</c> from the last two days of procedure_stats — well inside its 4-day raw retention, so no
    /// handle is missed between runs — and advances the watermark (see <see cref="UpsertAndAdvanceTail"/>).
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
    /// </summary>
    public const string RefreshSql = @"WITH src AS (
    SELECT server_name, sql_handle, database_name, schema_name, object_name, collection_time
    FROM collect.procedure_stats
    WHERE collection_time >= now() - interval '2 days'
      AND sql_handle IS NOT NULL
      AND sql_handle <> ''
),
" + UpsertAndAdvanceTail;

    /// <summary>
    /// The hourly refresh: the same upsert and watermark advance as <see cref="RefreshSql"/>, over the rows at or
    /// after the bound <c>$1</c> (a naive UTC timestamp, Kind Unspecified). The caller computes it from the
    /// watermark (see <see cref="SinceFor"/>), so each run reads about the last hour and a little behind it
    /// instead of two days. No clock inside the statement: the bound is a parameter like every other windowed
    /// read.
    /// </summary>
    public const string RefreshSinceSql = @"WITH src AS (
    SELECT server_name, sql_handle, database_name, schema_name, object_name, collection_time
    FROM collect.procedure_stats
    WHERE collection_time >= $1
      AND sql_handle IS NOT NULL
      AND sql_handle <> ''
),
" + UpsertAndAdvanceTail;

    /// <summary>Reads the watermark; no row yet reads NULL.</summary>
    public const string ReadWatermarkSql = "SELECT refreshed_through FROM collect.module_map_state WHERE id = 1";

    private const int WatermarkReadTimeoutSeconds = 30;

    /// <summary>
    /// The lower bound of the hourly refresh: the watermark (never later than <paramref name="utcNow"/>) less
    /// <see cref="WatermarkSlack"/>, but never older than <see cref="MaxLookback"/> before <paramref name="utcNow"/>;
    /// with no watermark, exactly that floor. The result is a naive (Kind Unspecified) timestamp ready to bind.
    /// </summary>
    public static DateTime SinceFor(DateTime? watermark, DateTime utcNow)
    {
        var floor = utcNow - MaxLookback;
        var since = watermark is { } w && Min(w, utcNow) - WatermarkSlack > floor ? Min(w, utcNow) - WatermarkSlack : floor;
        return DateTime.SpecifyKind(since, DateTimeKind.Unspecified);
    }

    /// <summary>True when <see cref="SinceFor"/> had to stop at the <see cref="MaxLookback"/> floor although a
    /// watermark exists: the rows between the watermark and that floor are not re-read.</summary>
    public static bool IsCappedByLookback(DateTime? watermark, DateTime utcNow)
    {
        return watermark is { } w && Min(w, utcNow) - WatermarkSlack < utcNow - MaxLookback;
    }

    private static DateTime Min(DateTime a, DateTime b) => a < b ? a : b;

    /// <summary>Creates the map and its watermark table if absent (idempotent). Returns true on success; a failure warns and leaves
    /// object_name-on-query_stats routing to fall back to raw (never kills startup).</summary>
    public static async Task<bool> EnsureTableAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        try
        {
            using var command = new NpgsqlCommand(CreateTableSql, connection) { CommandTimeout = SetupTimeoutSeconds };
            await command.ExecuteNonQueryAsync(cancellationToken);
            using var stateCommand = new NpgsqlCommand(CreateStateTableSql, connection) { CommandTimeout = SetupTimeoutSeconds };
            await stateCommand.ExecuteNonQueryAsync(cancellationToken);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("module_map table setup failed — object_name-on-query_stats routing falls back to raw: {Message}", ex.Message);
            return false;
        }
    }

    /// <summary>Upserts the map from the last two days of procedure_stats and advances the watermark. Returns the
    /// number of rows upserted; a failure warns and the existing map keeps serving (never throws). Called from the
    /// daily maintenance sweep.</summary>
    public static async Task<int> RefreshAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        try
        {
            using var command = new NpgsqlCommand(RefreshSql, connection) { CommandTimeout = SetupTimeoutSeconds };
            var (rows, _) = await ReadRefreshResultAsync(command, cancellationToken);
            logger?.LogInformation("module_map: upserted {Rows} sql_handle→module attribution(s) from recent procedure_stats.", rows);
            return rows;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("module_map refresh failed — the existing map keeps serving: {Message}", ex.Message);
            return 0;
        }
    }

    /// <summary>
    /// The hourly incremental refresh: upserts the map from procedure_stats at or after <see cref="SinceFor"/>
    /// and advances the watermark, in one statement. Returns the number of rows upserted; a failure warns and the
    /// existing map keeps serving (never throws), and the next hour reads from the same watermark, so nothing is
    /// skipped.
    /// </summary>
    public static Task<int> RefreshRecentAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default) =>
        RefreshRecentAsync(connection, logger, DateTime.UtcNow, cancellationToken);

    /// <summary>The hourly refresh against an explicit clock, which only the lookback floor reads; the tests pass
    /// a fixed one.</summary>
    public static async Task<int> RefreshRecentAsync(NpgsqlConnection connection, ILogger? logger, DateTime utcNow, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        try
        {
            var watermarkBefore = await ReadWatermarkAsync(connection, cancellationToken);
            var since = SinceFor(watermarkBefore, utcNow);
            if (IsCappedByLookback(watermarkBefore, utcNow))
            {
                /* The map shares this property with the rollup route and the daily refresh: a stall longer than the
                   lookback leaves the span between the old watermark and the floor unread, and the map keeps the
                   names it already had for it. Say so once per run instead of hiding it. */
                logger?.LogWarning(
                    "module_map hourly refresh: the watermark {Watermark:yyyy-MM-dd HH:mm:ss} is older than the {Days}-day lookback, so procedure_stats rows between it and {Since:yyyy-MM-dd HH:mm:ss} were not re-read; module names for that span come from the map as it was.",
                    watermarkBefore, MaxLookback.TotalDays, since);
            }

            using var command = new NpgsqlCommand(RefreshSinceSql, connection) { CommandTimeout = SetupTimeoutSeconds };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = since });
            var (rows, watermark) = await ReadRefreshResultAsync(command, cancellationToken);
            logger?.LogInformation(
                "module_map: upserted {Rows} sql_handle→module attribution(s) from procedure_stats since {Since:yyyy-MM-dd HH:mm:ss}; watermark now {Watermark}.",
                rows, since, watermark?.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture) ?? "unchanged (nothing read)");
            return rows;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("module_map hourly refresh failed — the existing map keeps serving and the next run reads from the same watermark: {Message}", ex.Message);
            return 0;
        }
    }

    /// <summary>The watermark: the largest procedure_stats <c>collection_time</c> the last refresh read, or null
    /// when there is none yet or the read fails for any reason (the table missing, a dropped connection). Callers
    /// treat null as "no watermark" and fall back.</summary>
    public static async Task<DateTime?> ReadWatermarkAsync(NpgsqlConnection connection, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        try
        {
            using var command = new NpgsqlCommand(ReadWatermarkSql, connection) { CommandTimeout = WatermarkReadTimeoutSeconds };
            var value = await command.ExecuteScalarAsync(cancellationToken);
            return value is DateTime watermark ? DateTime.SpecifyKind(watermark, DateTimeKind.Unspecified) : null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    private static async Task<(int Rows, DateTime? Watermark)> ReadRefreshResultAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return (0, null);
        }

        return (reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1));
    }
}
