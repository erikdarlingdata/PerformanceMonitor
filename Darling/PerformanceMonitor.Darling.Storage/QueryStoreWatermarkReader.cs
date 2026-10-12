/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The READ half of the per-batch Query Store watermark side table (#5684, V178; the write half is
/// <see cref="QueryStoreWatermarks"/>). It answers the per-database watermark seed read: the highest
/// <c>last_execution_time</c> of a (server, database) pair's <c>collect.query_store_stats</c> rows after a floor, with its
/// witness, the newest <c>collection_time</c> among the rows at that value. The raw answer visits every row of the pair in the
/// last three hours (22,000 to 47,000 rows); this read touches the side table's ~36 rows and one raw index probe.
///
/// <para><b>A value that is too HIGH is the dangerous direction</b> (it skips Query Store rows for good), so the result is built
/// to be, in every case, either equal to the raw read or lower, and never above it:</para>
/// <list type="number">
/// <item><b>Side arm.</b> The side table's best row after the floor (<c>batch_max DESC, collection_time DESC</c>), plus the
/// coverage start, in one statement. Every arm uses a strict <c>&gt;</c> against the floor, like the raw read.</item>
/// <item><b>Verification.</b> The side value is returned only after one raw row is found with that row's server, database,
/// query_id, plan_id, <c>collection_time</c> and <c>last_execution_time</c> = the side value, through
/// <c>idx_query_store_stats_server_db_query_plan_time</c>. A side row whose raw row is gone (a restore, a hand DELETE, a future
/// delete path) fails it, and the read is then the full raw read, whose answer belongs to a stored raw row by construction.</item>
/// <item><b>Raw arm</b>, only while the floor is below <c>covered_since</c> (the first hours after the upgrade): the raw read over
/// <c>(floor, covered_since)</c>. Once the floor passes the coverage start it never runs. The two arms combine by the larger value,
/// then the larger <c>collection_time</c>; an overlap is harmless because both come from stored rows.</item>
/// </list>
/// A missing side row (a savepoint fault, a build that did not write one) only lowers the answer. A store with no side tables
/// (SQLSTATE 42P01) reads raw.
/// </summary>
public sealed class QueryStoreWatermarkReader
{
    /// <summary>The leading comment of the stitched read's first statement (the side arm). The seed-read counting tests match it.</summary>
    public const string SeedMarker = "/* pm:qs-watermark-seed */";

    /// <summary>The leading comment of the verification probe.</summary>
    public const string VerifyMarker = "/* pm:qs-watermark-verify */";

    /// <summary>The leading comment of the raw arm that covers the window before <c>covered_since</c>.</summary>
    public const string PreCoverageMarker = "/* pm:qs-watermark-precoverage */";

    /// <summary>The leading comment of the full raw read the verification falls back to.</summary>
    public const string FallbackMarker = "/* pm:qs-watermark-fallback */";

    /// <summary>How often a pair's fallback is logged: once in this window per (server, database).</summary>
    public static readonly TimeSpan FallbackLogInterval = TimeSpan.FromHours(1);

    /// <summary>
    /// The side arm plus the coverage start. <c>$1</c> server, <c>$2</c> database, <c>$3</c> the floor (strict). One row always (the
    /// coverage row is one row); the side columns are NULL when no side row survives the floor. A store whose coverage table is empty
    /// returns no row and reads raw.
    /// </summary>
    public const string SideSql =
        SeedMarker + " SELECT c.covered_since, w.batch_max, w.collection_time, w.max_query_id, w.max_plan_id "
        + "FROM collect.query_store_watermarks_coverage AS c "
        + "LEFT JOIN LATERAL (SELECT batch_max, collection_time, max_query_id, max_plan_id FROM collect.query_store_watermarks "
        + "WHERE server_id = $1 AND database_name = $2 AND collection_time > $3 AND batch_max IS NOT NULL "
        + "ORDER BY batch_max DESC, collection_time DESC LIMIT 1) AS w ON true";

    /// <summary>
    /// The verification probe: <c>$1</c> server, <c>$2</c> database, <c>$3</c> query_id, <c>$4</c> plan_id, <c>$5</c>
    /// <c>collection_time</c>, <c>$6</c> the side value. A point look-up on <c>idx_query_store_stats_server_db_query_plan_time</c>.
    /// </summary>
    public const string VerifySql =
        VerifyMarker + " SELECT 1 FROM collect.query_store_stats WHERE server_id = $1 AND database_name = $2 "
        + "AND query_id = $3 AND plan_id = $4 AND collection_time = $5 AND last_execution_time = $6 LIMIT 1";

    /// <summary>
    /// The raw read over <c>(floor, covered_since)</c>: <c>$3</c> the floor, <c>$4</c> the coverage start, both strict. The same
    /// ordered single pass as the full raw read, with the coverage bound added.
    /// </summary>
    public const string PreCoverageSql =
        PreCoverageMarker + " SELECT last_execution_time, collection_time FROM collect.query_store_stats "
        + "WHERE server_id = $1 AND database_name = $2 AND collection_time > $3 AND collection_time < $4 "
        + "AND last_execution_time IS NOT NULL ORDER BY last_execution_time DESC, collection_time DESC LIMIT 1";

    private readonly ILogger? _logger;
    private readonly Func<DateTime> _utcNow;
    private readonly ConcurrentDictionary<(int ServerId, string Database), DateTime> _fallbackLoggedAt = new();
    private long _fallbacks;
    private long _preCoverageReads;

    /// <summary>Creates the reader. <paramref name="utcNow"/> is the clock the fallback log throttle uses (tests pass their own).</summary>
    public QueryStoreWatermarkReader(ILogger? logger, Func<DateTime>? utcNow = null)
    {
        _logger = logger;
        _utcNow = utcNow ?? (static () => DateTime.UtcNow);
    }

    /// <summary>How many reads fell back to the full raw read because verification failed or the side tables were absent.</summary>
    public long Fallbacks => Interlocked.Read(ref _fallbacks);

    /// <summary>How many times the raw arm over the window before <c>covered_since</c> ran.</summary>
    public long PreCoverageReads => Interlocked.Read(ref _preCoverageReads);

    /// <summary>
    /// Reads the (value, witness) pair. <paramref name="fullRawSql"/> is the full raw read (<c>$1</c> server, <c>$2</c> database,
    /// <c>$3</c> floor), the statement the fallback and the no-side-table path run. A null value has a null witness.
    /// </summary>
    public async Task<(DateTime? Value, DateTime? Witness)> ReadAsync(
        NpgsqlConnection connection, int serverId, string database, DateTime floor, string fullRawSql,
        int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(fullRawSql);

        var naiveFloor = DateTime.SpecifyKind(floor, DateTimeKind.Unspecified);
        DateTime? coveredSince = null;
        var hasCoverageRow = false;
        DateTime? sideValue = null;
        DateTime sideTime = default;
        long? queryId = null;
        long? planId = null;
        try
        {
            await using var side = new NpgsqlCommand(SideSql, connection) { CommandTimeout = commandTimeoutSeconds };
            side.Parameters.AddWithValue(serverId);
            side.Parameters.AddWithValue(database);
            side.Parameters.AddWithValue(naiveFloor);
            await using var reader = await side.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                hasCoverageRow = true;
                coveredSince = reader.GetValue(0) as DateTime?;
                if (reader.GetValue(1) is DateTime batchMax)
                {
                    sideValue = batchMax;
                    sideTime = (DateTime)reader.GetValue(2);
                    queryId = reader.GetValue(3) as long?;
                    planId = reader.GetValue(4) as long?;
                }
            }
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            /* A store below V178 has neither table. */
            return await FallbackAsync(connection, serverId, database, naiveFloor, fullRawSql, commandTimeoutSeconds, "side tables absent", cancellationToken);
        }

        if (!hasCoverageRow)
        {
            /* No coverage row: nothing proves what the side table covers, so read raw. */
            return await FallbackAsync(connection, serverId, database, naiveFloor, fullRawSql, commandTimeoutSeconds, "no coverage row", cancellationToken);
        }

        if (coveredSince is null)
        {
            return await FallbackAsync(connection, serverId, database, naiveFloor, fullRawSql, commandTimeoutSeconds, "coverage start is NULL", cancellationToken);
        }

        if (sideValue is DateTime verifiedValue)
        {
            if (!await IsVerifiedAsync(connection, serverId, database, queryId, planId, sideTime, verifiedValue, commandTimeoutSeconds, cancellationToken))
            {
                return await FallbackAsync(connection, serverId, database, naiveFloor, fullRawSql, commandTimeoutSeconds, "the side row's raw row is missing", cancellationToken);
            }
        }

        (DateTime? Value, DateTime? Witness) best = sideValue is DateTime v ? (v, sideTime) : (null, null);
        if (naiveFloor < coveredSince.Value)
        {
            Interlocked.Increment(ref _preCoverageReads);
            await using var pre = new NpgsqlCommand(PreCoverageSql, connection) { CommandTimeout = commandTimeoutSeconds };
            pre.Parameters.AddWithValue(serverId);
            pre.Parameters.AddWithValue(database);
            pre.Parameters.AddWithValue(naiveFloor);
            pre.Parameters.AddWithValue(DateTime.SpecifyKind(coveredSince.Value, DateTimeKind.Unspecified));
            await using var preReader = await pre.ExecuteReaderAsync(cancellationToken);
            if (await preReader.ReadAsync(cancellationToken) && preReader.GetValue(0) is DateTime rawValue)
            {
                var rawTime = (DateTime)preReader.GetValue(1);
                if (best.Value is not DateTime current || rawValue > current || (rawValue == current && rawTime > best.Witness))
                {
                    best = (rawValue, rawTime);
                }
            }
        }

        return best;
    }

    private static async Task<bool> IsVerifiedAsync(
        NpgsqlConnection connection, int serverId, string database, long? queryId, long? planId, DateTime collectionTime,
        DateTime value, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        if (queryId is null || planId is null)
        {
            return false;
        }

        await using var command = new NpgsqlCommand(VerifySql, connection) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(queryId.Value);
        command.Parameters.AddWithValue(planId.Value);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(value, DateTimeKind.Unspecified));
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    private async Task<(DateTime? Value, DateTime? Witness)> FallbackAsync(
        NpgsqlConnection connection, int serverId, string database, DateTime floor, string fullRawSql,
        int commandTimeoutSeconds, string reason, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _fallbacks);
        var now = _utcNow();
        var key = (serverId, database);
        var due = !_fallbackLoggedAt.TryGetValue(key, out var last) || now - last >= FallbackLogInterval;
        if (due)
        {
            _fallbackLoggedAt[key] = now;
            _logger?.LogWarning(
                "Query Store watermark side table (#5684): the read for server {ServerId} database {Database} fell back to the full raw read ({Reason}); logged once per pair per hour",
                serverId, database, reason);
        }

        /* The fallback carries its own marker so a seed read counts once. */
        var sql = fullRawSql.Replace(SeedMarker, FallbackMarker, StringComparison.Ordinal);
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(floor);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken) && reader.GetValue(0) is DateTime value)
        {
            return (value, reader.GetValue(1) as DateTime?);
        }

        return (null, null);
    }
}
