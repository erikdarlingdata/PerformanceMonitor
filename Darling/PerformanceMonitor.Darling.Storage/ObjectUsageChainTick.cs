/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The hourly tick that keeps the persisted object usage chain (#9349) up to date: it plans which (server, database) pairs have raw
/// <c>collect.index_object_stats</c> snapshots the chain has not folded yet and runs <see cref="ObjectUsageChainBuilder.BuildPairAsync(NpgsqlDataSource, int, string, int, CancellationToken)"/>
/// on each. It runs on the housekeeping tick only, never on a request path and never from the MCP role, and it never writes
/// <c>index_object_stats</c>.
///
/// <para><b>The plan is three cheap reads, none of which reads index rows.</b> (1) Each server's newest raw snapshot time: a skip scan over
/// <c>idx_index_object_stats_server_time (server_id, collection_time DESC)</c>, one index probe per server. (2) The server's pair watermarks:
/// <c>max(collection_time)</c> per database in <c>object_usage_snapshots</c>, an index-only read of that table's primary key. (3) Only for a
/// server that needs it, the database names: a skip scan over <c>idx_index_object_stats_object</c>, one probe per name. A pair is built when it
/// has no watermark (a backfill) or when its watermark is older than the server's newest raw snapshot; the builder says whether anything was
/// there (0 steps = nothing to fold).</para>
///
/// <para><b>A server is skipped</b> when its newest raw snapshot is not newer than any of its pair watermarks and is older than
/// <see cref="QuietWindow"/>. The window exists because the collector commits a run one database at a time, so a tick that lands mid-run sees a
/// server whose newest snapshot is built for the databases already committed and not yet written for the rest. Until the run is surely over
/// the server is listed again each tick. Each database's snapshot is written by one binary COPY (one commit), so a pair's snapshot is never
/// visible half written and the builder never folds a partial one.</para>
///
/// <para><b>Bounds.</b> At most <see cref="MaxStepsPerTick"/> snapshots are folded a tick and no longer than <see cref="MaxTickDuration"/>;
/// the next tick resumes, because the builder restores from what is committed. A pair or a server whose work throws is logged and skipped, and
/// nothing in the tick fails the caller.</para>
/// </summary>
public static class ObjectUsageChainTick
{
    /// <summary>The most snapshots one tick folds. High enough that <see cref="MaxTickDuration"/> is the bound that usually holds.</summary>
    public const int MaxStepsPerTick = 5000;

    /// <summary>The longest one tick runs. A snapshot being folded when it passes is rolled back and built again by the next tick.</summary>
    public static readonly TimeSpan MaxTickDuration = TimeSpan.FromMinutes(5);

    /// <summary>How long after a server's newest raw snapshot its databases are still listed every tick (see the type's remarks).</summary>
    public static readonly TimeSpan QuietWindow = TimeSpan.FromHours(3);

    private const int CommandTimeoutSeconds = 120;

    /// <summary>
    /// Each server with raw snapshots and the time of its newest one. A recursive skip scan: the first row is the lowest server's newest
    /// entry on <c>idx_index_object_stats_server_time</c>, and each next row is the first entry past the previous server id.
    /// </summary>
    internal const string ServersSql = """
        WITH RECURSIVE servers AS
        (
            (
                SELECT
                    s.server_id,
                    s.collection_time
                FROM collect.index_object_stats AS s
                ORDER BY
                    s.server_id,
                    s.collection_time DESC
                LIMIT 1
            )
            UNION ALL
            SELECT
                n.server_id,
                n.collection_time
            FROM servers AS p
            CROSS JOIN LATERAL
            (
                SELECT
                    s.server_id,
                    s.collection_time
                FROM collect.index_object_stats AS s
                WHERE s.server_id > p.server_id
                ORDER BY
                    s.server_id,
                    s.collection_time DESC
                LIMIT 1
            ) AS n
        )
        SELECT
            servers.server_id,
            servers.collection_time
        FROM servers
        """;

    /// <summary>One server's pair watermarks. $1 server_id.</summary>
    internal const string WatermarksSql = """
        SELECT
            u.database_name,
            max(u.collection_time)
        FROM collect.object_usage_snapshots AS u
        WHERE u.server_id = $1
        GROUP BY
            u.database_name
        """;

    /// <summary>One server's database names, one index probe per name. $1 server_id.</summary>
    internal const string DatabasesSql = """
        WITH RECURSIVE databases AS
        (
            (
                SELECT
                    s.database_name
                FROM collect.index_object_stats AS s
                WHERE s.server_id = $1
                AND   s.database_name IS NOT NULL
                ORDER BY
                    s.database_name
                LIMIT 1
            )
            UNION ALL
            SELECT
            (
                SELECT
                    s.database_name
                FROM collect.index_object_stats AS s
                WHERE s.server_id = $1
                AND   s.database_name > d.database_name
                ORDER BY
                    s.database_name
                LIMIT 1
            )
            FROM databases AS d
            WHERE d.database_name IS NOT NULL
        )
        SELECT
            databases.database_name
        FROM databases
        WHERE databases.database_name IS NOT NULL
        """;

    /// <summary>What one tick did: pairs that folded at least one snapshot, snapshots folded, pairs or servers that failed, and whether a bound stopped the tick early.</summary>
    public readonly record struct TickResult(int PairsBuilt, int Steps, int Failed, bool Deferred);

    /// <summary>The builder call a tick makes for one pair. The default is <see cref="ObjectUsageChainBuilder.BuildPairAsync(NpgsqlDataSource, int, string, int, CancellationToken)"/>.</summary>
    internal delegate Task<int> BuildPair(NpgsqlDataSource dataSource, int serverId, string databaseName, int maxSteps, CancellationToken cancellationToken);

    /// <summary>One tick with the production bounds and the real builder.</summary>
    public static Task<TickResult> RunTickAsync(NpgsqlDataSource dataSource, ILogger logger, CancellationToken cancellationToken) =>
        RunTickAsync(dataSource, DateTime.UtcNow, MaxStepsPerTick, MaxTickDuration, ObjectUsageChainBuilder.BuildPairAsync, logger, cancellationToken);

    /// <summary>One tick with the bounds, the clock and the builder call set, so a test can cap the steps or make one pair fail.</summary>
    internal static async Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, int maxSteps, TimeSpan maxDuration, BuildPair build, ILogger logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(build);
        ArgumentNullException.ThrowIfNull(logger);

        var clock = Stopwatch.StartNew();
        int pairsBuilt = 0, steps = 0, failed = 0;
        var deferred = false;

        List<(int ServerId, DateTime Newest)> servers;
        try
        {
            servers = await ReadServersAsync(dataSource, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning("Object usage chain: the server list could not be read; the next tick retries: {Message}", ex.Message);
            return new TickResult(0, 0, 1, false);
        }

        foreach (var (serverId, newest) in servers)
        {
            if (steps >= maxSteps || clock.Elapsed >= maxDuration)
            {
                deferred = true;
                break;
            }

            List<string> candidates;
            try
            {
                candidates = await PlanServerAsync(dataSource, serverId, newest, nowUtc, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed++;
                logger.LogWarning(
                    "Object usage chain: planning server {ServerId} failed (SqlState {SqlState}); the next tick retries: {Message}",
                    serverId, (ex as PostgresException)?.SqlState ?? "n/a", ex.Message);
                continue;
            }

            foreach (var database in candidates)
            {
                var remaining = maxDuration - clock.Elapsed;
                if (steps >= maxSteps || remaining <= TimeSpan.Zero)
                {
                    deferred = true;
                    break;
                }

                /* The duration bound reaches into the builder: it checks its token before each snapshot, so a pair with a long backlog stops
                   at the bound instead of running past it. A stop by the bound is not a failure, and the next tick resumes. */
                using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                bound.CancelAfter(remaining);
                try
                {
                    var built = await build(dataSource, serverId, database, maxSteps - steps, bound.Token);
                    if (built > 0)
                    {
                        pairsBuilt++;
                        steps += built;
                    }
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    deferred = true;
                    break;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    failed++;
                    logger.LogWarning(
                        "Object usage chain: building server {ServerId} database [{Database}] failed (SqlState {SqlState}); the next tick retries: {Message}",
                        serverId, database, (ex as PostgresException)?.SqlState ?? "n/a", ex.Message);
                }
            }

            if (deferred)
            {
                break;
            }
        }

        return new TickResult(pairsBuilt, steps, failed, deferred);
    }

    /// <summary>The pairs of one server worth a builder call, per the type's remarks. Empty when the server is skipped.</summary>
    private static async Task<List<string>> PlanServerAsync(
        NpgsqlDataSource dataSource, int serverId, DateTime newest, DateTime nowUtc, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);

        var watermarks = new Dictionary<string, DateTime>(StringComparer.Ordinal);
        await using (var command = new NpgsqlCommand(WatermarksSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                watermarks[reader.GetString(0)] = reader.GetDateTime(1);
            }
        }

        var behind = watermarks.Count == 0;
        foreach (var watermark in watermarks.Values)
        {
            behind |= watermark < newest;
        }

        if (!behind && nowUtc - newest >= QuietWindow)
        {
            return [];
        }

        var candidates = new List<string>();
        await using (var command = new NpgsqlCommand(DatabasesSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var database = reader.GetString(0);
                if (!watermarks.TryGetValue(database, out var watermark) || watermark < newest)
                {
                    candidates.Add(database);
                }
            }
        }

        return candidates;
    }

    private static async Task<List<(int ServerId, DateTime Newest)>> ReadServersAsync(NpgsqlDataSource dataSource, CancellationToken cancellationToken)
    {
        var servers = new List<(int ServerId, DateTime Newest)>();
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(ServersSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            servers.Add((reader.GetInt32(0), reader.GetDateTime(1)));
        }

        return servers;
    }
}
