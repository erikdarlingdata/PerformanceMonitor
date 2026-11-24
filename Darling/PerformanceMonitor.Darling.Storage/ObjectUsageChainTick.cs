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
using System.Linq;
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
/// <para><b>The plan is cheap reads, none of which reads index rows, and the two listings are bounded to the newest
/// <see cref="ListingWindowDays"/> days.</b> (1) Each server's newest raw snapshot time: a skip scan over
/// <c>idx_index_object_stats_server_time (server_id, collection_time DESC)</c>, one index probe per server. (2) The server's pair watermarks:
/// <c>max(collection_time)</c> per database in <c>object_usage_snapshots</c>, an index-only read of that table's primary key. (3) The server's
/// database names: a skip scan over <c>idx_index_object_stats_object</c>, one probe per name. Both raw listings carry a plain
/// <c>collection_time</c> bound so chunk exclusion drops every chunk older than the window; a listing over all history is slow on a store
/// with years of compressed chunks, and a pair with no raw row in the window is dead (its server or database is no longer collected), so
/// there is nothing new to fold for it. A pair is built when it has no watermark (a backfill, or a build that failed before its first
/// snapshot) or when its watermark is older than the server's newest raw snapshot; the builder says whether anything was there (0 steps =
/// nothing to fold).</para>
///
/// <para><b>The backfill reads all history, once per server.</b> A server with no watermark at all (never built: the upgrade, or a new server)
/// lists its databases over all history, and a server whose raw rows are all older than the window is still found, through the server
/// registry (<c>collect.servers</c>) for servers with no snapshots row that have at least one raw index row, so a dead database's history is
/// folded once. A backfill that a bound or a failure stops half way keeps that server on the all-history listing, and in the server list even
/// when none of its raw rows are in the window (a dead server has a snapshots row after its first step, so the registry listing no longer finds
/// it), in the process's memory, until a tick finishes it. After a service restart in the middle of a backfill the server already has
/// watermarks and the memory is gone, so only servers and databases with a raw row in the window are listed from then on.</para>
///
/// <para><b>Every server's databases are listed every tick.</b> A cheaper skip (the newest raw snapshot is not newer than any pair watermark)
/// would be wrong twice: the collector commits a run one database at a time, so a tick that lands mid-run sees the databases already committed
/// at the newest time and misses the rest until the next run, and a pair that has no watermark yet cannot be seen in the watermarks at all.
/// Each database's snapshot is written by one binary COPY (one commit), so a pair's snapshot is never visible half written and the builder
/// never folds a partial one.</para>
///
/// <para><b>Bounds and fairness.</b> At most <see cref="MaxStepsPerTick"/> snapshots are folded a tick and no longer than
/// <see cref="MaxTickDuration"/>; the next tick resumes, because the builder restores from what is committed. A tick that a bound stops
/// leaves a cursor at the last pair it tried, and the next tick starts with the pair after it (servers and databases in order, wrapping at
/// the end), so one pair with a long backlog cannot keep the pairs behind it from ever being built. The cursor is in the service's memory:
/// after a restart the first tick starts at the first pair again, which costs one pass of fairness and nothing else. A pair or a server
/// whose work throws is logged and skipped, and nothing in the tick fails the caller.</para>
///
/// <para><b>One builder per store.</b> The tick holds a session advisory lock on its own connection for its whole run, and a tick that
/// cannot take it skips the hour. Two services on one store therefore never build the same pair at once. Late rows for a
/// <c>collection_time</c> a pair has already folded cannot happen on one host: the daily run holds the per-(server, collector)
/// <c>DetachedCollectorGate</c> slot, the at-connect run and snapshot_now take that same slot first (<c>TryTakeInlineDailySlot</c>), and the
/// timestamps come from <c>CollectionTimeClock.NextStrictlyAfter</c> per (server, collector).</para>
/// </summary>
public static class ObjectUsageChainTick
{
    /// <summary>The most snapshots one tick folds. High enough that <see cref="MaxTickDuration"/> is the bound that usually holds.</summary>
    public const int MaxStepsPerTick = 5000;

    /// <summary>The longest one tick runs. A snapshot being folded when it passes is rolled back and built again by the next tick.</summary>
    public static readonly TimeSpan MaxTickDuration = TimeSpan.FromMinutes(5);

    private const int CommandTimeoutSeconds = 120;

    /// <summary>How many days of raw snapshots the server and database listings read (#9350): a pair with no raw row in this window is dead.</summary>
    public const int ListingWindowDays = 2;

    /// <summary>The listing window's start: <see cref="ListingWindowDays"/> days before now, as the naive UTC timestamp the raw tables' <c>collection_time</c> holds.</summary>
    internal static DateTime ListingWindowStart() => DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-ListingWindowDays), DateTimeKind.Unspecified);

    /// <summary>The store-level claim: one tick at a time per store.</summary>
    internal const string ClaimLockSql = "SELECT pg_try_advisory_lock(hashtextextended('object_usage_chain_tick', 0))";

    internal const string ClaimUnlockSql = "SELECT pg_advisory_unlock(hashtextextended('object_usage_chain_tick', 0))";

    /// <summary>
    /// Servers with no snapshots row at all that have at least one raw index row: the backfill's candidates whose raw rows may all be older
    /// than the listing window. The server registry stands in for a listing of all history, and the <c>EXISTS</c> probe (one index entry on
    /// the server's leading index column) keeps a server that was never collected, or a PostgreSQL target, off the list for good.
    /// </summary>
    internal const string UnbuiltServersSql = """
        SELECT
            sv.server_id
        FROM collect.servers AS sv
        WHERE NOT EXISTS
        (
            SELECT
                1
            FROM collect.object_usage_snapshots AS u
            WHERE u.server_id = sv.server_id
        )
        AND   EXISTS
        (
            SELECT
                1
            FROM collect.index_object_stats AS s
            WHERE s.server_id = sv.server_id
        )
        ORDER BY
            sv.server_id
        """;

    /// <summary>
    /// Each server with a raw snapshot in the newest <see cref="ListingWindowDays"/> days and the time of its newest one. A recursive skip scan:
    /// the first row is the lowest server's newest entry on <c>idx_index_object_stats_server_time</c>, and each next row is the first entry past
    /// the previous server id. The bound is a plain predicate on the chunk column, so chunks older than the window are excluded. $1 the window
    /// start, <see cref="ListingWindowStart"/> (a naive timestamp, bound like the other windowed reads).
    /// </summary>
    internal const string ServersSql = """
        WITH RECURSIVE servers AS
        (
            (
                SELECT
                    s.server_id,
                    s.collection_time
                FROM collect.index_object_stats AS s
                WHERE s.collection_time >= $1
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
                AND   s.collection_time >= $1
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

    /// <summary>One server's database names with a raw row in the newest <see cref="ListingWindowDays"/> days, one index probe per name. $1 server_id, $2 the window start (<see cref="ListingWindowStart"/>).</summary>
    internal const string DatabasesSql = """
        WITH RECURSIVE databases AS
        (
            (
                SELECT
                    s.database_name
                FROM collect.index_object_stats AS s
                WHERE s.server_id = $1
                AND   s.database_name IS NOT NULL
                AND   s.collection_time >= $2
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
                AND   s.collection_time >= $2
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

    /// <summary>One server's database names over all history, one index probe per name: the backfill of a server with no watermark. $1 server_id.</summary>
    internal const string DatabasesAllHistorySql = """
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

    /// <summary>
    /// What a tick remembers for the next one: the cursor (the last pair it tried when a bound stopped it) and the servers whose
    /// all-history backfill listing is not finished. It is read and written only while the tick holds the store claim.
    /// </summary>
    internal sealed class TickState
    {
        internal (int ServerId, string Database)? Cursor { get; set; }

        internal HashSet<int> PendingBackfill { get; } = [];
    }

    private static readonly TickState SharedState = new();

    private enum Segment
    {
        All,
        AfterCursor,
        UpToCursor,
    }

    private sealed record Plan(List<string> Candidates, bool AllHistory);

    /// <summary>One tick with the production bounds and the real builder.</summary>
    public static Task<TickResult> RunTickAsync(NpgsqlDataSource dataSource, ILogger logger, CancellationToken cancellationToken) =>
        RunTickAsync(dataSource, MaxStepsPerTick, MaxTickDuration, ObjectUsageChainBuilder.BuildPairAsync, logger, cancellationToken, SharedState);

    /// <summary>
    /// One tick with the bounds and the builder call set, so a test can cap the steps or make one pair fail. <paramref name="state"/> is what
    /// the ticks of one service share; a test passes its own so ticks of different stores never share a cursor.
    /// </summary>
    internal static async Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, int maxSteps, TimeSpan maxDuration, BuildPair build, ILogger logger, CancellationToken cancellationToken,
        TickState? state = null)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(build);
        ArgumentNullException.ThrowIfNull(logger);

        state ??= new TickState();
        var clock = Stopwatch.StartNew();
        int pairsBuilt = 0, steps = 0, failed = 0;
        var deferred = false;

        /* The store-level claim: one chain builder per store. Two services on one store, or an overlapping tick, would fold the same pair
           twice. A tick that cannot take it skips the hour; the holder is building the same pairs. Late rows for a collection_time already
           folded cannot happen on one host (see the type's remarks), so a claim per tick is all the exclusion the chain needs. */
        StoreClaim? claim;
        try
        {
            claim = await StoreClaim.TryTakeAsync(dataSource, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning("Object usage chain: the store claim could not be taken; the next tick retries: {Message}", ex.Message);
            return new TickResult(0, 0, 1, false);
        }

        if (claim is null)
        {
            logger.LogDebug("Object usage chain: another builder holds the store claim; this hour is skipped");
            return new TickResult(0, 0, 0, false);
        }

        await using var held = claim;

        List<(int ServerId, DateTime Newest)> servers;
        try
        {
            servers = await ReadServersAsync(dataSource, state.PendingBackfill, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning("Object usage chain: the server list could not be read; the next tick retries: {Message}", ex.Message);
            return new TickResult(0, 0, 1, false);
        }

        /* The visit order starts after the pair the last stopped tick ended at and wraps: the rest of the cursor's server, the servers after
           it, the servers before it, then the cursor server's pairs up to the cursor. */
        var cursor = state.Cursor;
        var visits = new List<(int ServerId, DateTime Newest, Segment Segment)>();
        if (cursor is { } at)
        {
            var cursorServer = servers.FindIndex(s => s.ServerId == at.ServerId);
            if (cursorServer >= 0)
            {
                visits.Add((at.ServerId, servers[cursorServer].Newest, Segment.AfterCursor));
            }

            visits.AddRange(servers.Where(s => s.ServerId > at.ServerId).Select(s => (s.ServerId, s.Newest, Segment.All)));
            visits.AddRange(servers.Where(s => s.ServerId < at.ServerId).Select(s => (s.ServerId, s.Newest, Segment.All)));
            if (cursorServer >= 0)
            {
                visits.Add((at.ServerId, servers[cursorServer].Newest, Segment.UpToCursor));
            }
        }
        else
        {
            visits.AddRange(servers.Select(s => (s.ServerId, s.Newest, Segment.All)));
        }

        var plans = new Dictionary<int, Plan>();
        var incomplete = new HashSet<int>();
        var visitsDone = new Dictionary<int, int>();
        (int ServerId, string Database)? last = null;

        foreach (var (serverId, newest, segment) in visits)
        {
            if (steps >= maxSteps || clock.Elapsed >= maxDuration)
            {
                deferred = true;
                break;
            }

            if (!plans.TryGetValue(serverId, out var plan))
            {
                try
                {
                    plan = await PlanServerAsync(dataSource, serverId, newest, state.PendingBackfill.Contains(serverId), cancellationToken);
                    plans[serverId] = plan;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    failed++;
                    logger.LogWarning(
                        "Object usage chain: planning server {ServerId} failed (SqlState {SqlState}); the next tick retries: {Message}",
                        serverId, (ex as PostgresException)?.SqlState ?? "n/a", ex.Message);
                    continue;
                }
            }

            var serverDeferred = false;
            foreach (var database in plan.Candidates)
            {
                if (cursor is { } stop && serverId == stop.ServerId)
                {
                    var after = string.CompareOrdinal(database, stop.Database) > 0;
                    if (after != (segment == Segment.AfterCursor))
                    {
                        continue;
                    }
                }

                var remaining = maxDuration - clock.Elapsed;
                if (steps >= maxSteps || remaining <= TimeSpan.Zero)
                {
                    deferred = true;
                    serverDeferred = true;
                    break;
                }

                last = (serverId, database);

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

                    /* The builder stops at the step cap it was given, so a pair that used the whole cap may have snapshots left, and no later
                       pair in this tick is there to notice (the cap check above runs before a pair, not after the last). The server's
                       backfill is then not finished: it stays pending, and a dead server (no raw row in the window) is listed by that. */
                    if (steps >= maxSteps)
                    {
                        incomplete.Add(serverId);
                    }
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    deferred = true;
                    serverDeferred = true;
                    break;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    failed++;
                    incomplete.Add(serverId);
                    logger.LogWarning(
                        "Object usage chain: building server {ServerId} database [{Database}] failed (SqlState {SqlState}); the next tick retries: {Message}",
                        serverId, database, (ex as PostgresException)?.SqlState ?? "n/a", ex.Message);
                }
            }

            if (serverDeferred)
            {
                incomplete.Add(serverId);
                break;
            }

            visitsDone[serverId] = visitsDone.GetValueOrDefault(serverId) + 1;
        }

        /* A server listed over all history stays on that listing until a tick has gone through all of its pairs without a bound or a
           failure stopping it, so a backfill that takes several ticks does not lose the databases that have no recent rows. */
        foreach (var (serverId, plan) in plans)
        {
            if (!plan.AllHistory)
            {
                continue;
            }

            var expected = visits.Count(v => v.ServerId == serverId);
            if (incomplete.Contains(serverId) || visitsDone.GetValueOrDefault(serverId) < expected)
            {
                state.PendingBackfill.Add(serverId);
            }
            else
            {
                state.PendingBackfill.Remove(serverId);
            }
        }

        if (!deferred)
        {
            state.Cursor = null;
        }
        else if (last is { } stoppedAt)
        {
            state.Cursor = stoppedAt;
        }

        return new TickResult(pairsBuilt, steps, failed, deferred);
    }

    /// <summary>The pairs of one server worth a builder call, per the type's remarks, in ordinal name order.</summary>
    private static async Task<Plan> PlanServerAsync(
        NpgsqlDataSource dataSource, int serverId, DateTime newest, bool backfillPending, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(StoreSessionNames.ObjectUsageChain, cancellationToken);

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

        var allHistory = backfillPending || watermarks.Count == 0;
        var candidates = new List<string>();
        await using (var command = new NpgsqlCommand(allHistory ? DatabasesAllHistorySql : DatabasesSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            if (!allHistory)
            {
                command.Parameters.Add(new NpgsqlParameter<DateTime> { NpgsqlDbType = NpgsqlDbType.Timestamp, TypedValue = ListingWindowStart() });
            }

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

        candidates.Sort(StringComparer.Ordinal);
        return new Plan(candidates, allHistory);
    }

    /// <summary>
    /// The servers to visit, by id: each server with a raw snapshot in the listing window and its newest time, and each server with no
    /// snapshots row (the backfill) and each server whose backfill is unfinished (<paramref name="pendingBackfill"/>) with a time that never
    /// makes a pair look built.
    /// </summary>
    private static async Task<List<(int ServerId, DateTime Newest)>> ReadServersAsync(NpgsqlDataSource dataSource, HashSet<int> pendingBackfill, CancellationToken cancellationToken)
    {
        var servers = new SortedDictionary<int, DateTime>();
        await using var connection = await dataSource.OpenConnectionAsync(StoreSessionNames.ObjectUsageChain, cancellationToken);
        await using (var command = new NpgsqlCommand(ServersSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            command.Parameters.Add(new NpgsqlParameter<DateTime> { NpgsqlDbType = NpgsqlDbType.Timestamp, TypedValue = ListingWindowStart() });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                servers[reader.GetInt32(0)] = reader.GetDateTime(1);
            }
        }

        await using (var command = new NpgsqlCommand(UnbuiltServersSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                servers.TryAdd(reader.GetInt32(0), DateTime.MaxValue);
            }
        }

        /* A backfill that a bound or a failure stopped after some steps leaves the server with a snapshots row, so it is on neither list
           above when its raw rows are all older than the listing window (a dead server). The server stays listed until a tick finishes it. */
        foreach (var serverId in pendingBackfill)
        {
            servers.TryAdd(serverId, DateTime.MaxValue);
        }

        return servers.Select(s => (s.Key, s.Value)).ToList();
    }

    /// <summary>The store claim: a session advisory lock on a connection of its own, held until disposal.</summary>
    private sealed class StoreClaim : IAsyncDisposable
    {
        private readonly NpgsqlConnection _connection;

        private StoreClaim(NpgsqlConnection connection) => _connection = connection;

        /// <summary>The claim, or null when another session holds it.</summary>
        internal static async Task<StoreClaim?> TryTakeAsync(NpgsqlDataSource dataSource, CancellationToken cancellationToken)
        {
            var connection = await dataSource.OpenConnectionAsync(StoreSessionNames.ObjectUsageChain, cancellationToken);
            try
            {
                await using var command = new NpgsqlCommand(ClaimLockSql, connection) { CommandTimeout = CommandTimeoutSeconds };
                if ((bool)(await command.ExecuteScalarAsync(cancellationToken))!)
                {
                    return new StoreClaim(connection);
                }
            }
            catch
            {
                await connection.DisposeAsync();
                throw;
            }

            await connection.DisposeAsync();
            return null;
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await using var command = new NpgsqlCommand(ClaimUnlockSql, _connection) { CommandTimeout = CommandTimeoutSeconds };
                await command.ExecuteNonQueryAsync(CancellationToken.None);
            }
            catch (Exception)
            {
                // A broken connection ends its session, and the session's locks end with it.
            }

            await _connection.DisposeAsync();
        }
    }
}
