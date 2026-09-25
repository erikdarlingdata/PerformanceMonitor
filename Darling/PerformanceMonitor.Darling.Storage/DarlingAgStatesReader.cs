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
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The Availability Group topology reads (#991) behind the WPF viewer's AG tab, the web dashboard's
/// <c>/api/ag</c> and <c>/api/ag/count</c>, and the <c>get_ag_health</c> MCP tool — moved here (#4228), like
/// <see cref="DarlingQueryStoreClutterReader"/> before it, because the Viewer has no route to the Service
/// assembly and three surfaces were carrying independent copies of the same statement text.
///
/// <para><b>The cost problem this replaces (#4228).</b> Every shipped statement found "each server's newest
/// AG snapshot" by joining to <c>SELECT server_id, MAX(collection_time) ... GROUP BY server_id</c> with no
/// bound on <c>collection_time</c>. That inner aggregate reads every retained row for every server —
/// 2,803 ms and 441 k buffers for 398 rows on a production store — because the instant it finds is not known
/// until the whole table has been read, so TimescaleDB cannot exclude a single chunk. A literal lower bound
/// on the partition column is what makes the read cheap (2.1 ms + 1.0 ms measured for the same 398 rows), so
/// every read below is TWO statements: <see cref="ReplicaNewestInstantSql"/> /
/// <see cref="DatabaseNewestInstantSql"/> find each server's newest instant by an ORDERED DESCENT per server
/// (index range scan, one chunk), and <see cref="ReplicaStatesSql"/> / <see cref="DatabaseReplicaStatesSql"/>
/// / <see cref="ReplicaGroupCountSql"/> re-run the original shape with that instant as a literal floor on
/// BOTH the inner aggregate and the outer scan.</para>
///
/// <para><b>Why the floor is a shared MINIMUM, not one server_id at a time.</b> The two-step measurement
/// that hit 1.0 ms ran ONE statement for the whole fleet with one floor, not a per-server LATERAL join back
/// (that form was measured too: 2,090 ms, because probing every chunk for a found instant is still probing
/// every chunk). So step two stays the fleet-wide statement it always was; only the floor changes.</para>
///
/// <para><b>Stale servers do not drag the shared floor back (#4228).</b> A server whose AG collection
/// stopped days ago would otherwise pull the shared floor back to its own ancient instant, reintroducing the
/// full scan for every OTHER server too. <see cref="StalenessHorizon"/> — one chunk width — splits step
/// one's per-server instants into CURRENT (within the horizon, feed the shared floor, read together in one
/// statement) and STALE (older, read on its OWN with its OWN instant as the floor — a single-chunk point
/// read, same statement text, just scoped to one server_id). Results are identical to the unbounded shape
/// either way; only the read plan differs. One chunk width keeps the shared floor inside "today plus
/// yesterday" whenever every server is current, which is what bounds the outer scan to at most two chunks —
/// a wider horizon would let the shared floor span three.</para>
///
/// <para><b>Step one is NOT scoped to <c>servers.is_enabled</c>, on purpose, even though the issue's own
/// sketch shows that filter.</b> #4236 (the same night) was a by-server read scoped to an enabled/registered
/// table, which dropped a disabled server's and an unregistered server's own rows when a caller looked THAT
/// server up directly. Step one plays the same role here — it decides what feeds the floor and what gets its
/// own point read, not what is VISIBLE. Every statement below keeps the SAME <c>JOIN servers AS s ON
/// s.server_id = ... AND s.is_enabled</c> the shipped statements always had, unchanged, so a disabled
/// server's rows are excluded from the OUTPUT exactly as they are today, and a server missing from
/// <c>servers</c> entirely (no FK ties <c>ag_replica_states.server_id</c> to it) is excluded the same way
/// today's INNER JOIN already excludes it. Scoping step one to enabled servers would not change either of
/// those outcomes — the output-side join already decides them — so the only effect of adding it would be to
/// risk repeating #4236's mistake for no benefit.</para>
///
/// <para><b>Bare table names, matching <see cref="DarlingQueryStoreClutterReader"/>.</b> The store session's
/// <c>search_path</c> is <c>collect, config, public</c> (<see cref="PgSchemaGenerator.SearchPath"/>), so
/// <c>ag_replica_states</c> and <c>ag_database_replica_states</c> (both <c>collect</c>-schema) and
/// <c>servers</c> (public-schema) all resolve unqualified. The Viewer's copy of these statements was already
/// bare; the Service's copy was <c>collect.</c>-qualified. This picks the Storage-layer convention.</para>
///
/// <para><b>The database grain selects two columns the Viewer does not currently show</b>
/// (<c>last_hardened_lsn</c>, <c>last_commit_lsn</c>) because the Service's shipped statement already
/// selected them and one shared statement has one column list. The Viewer's mapping simply does not read
/// them, exactly as it does not today.</para>
/// </summary>
public static class DarlingAgStatesReader
{
    /// <summary>How far a server's newest AG snapshot may fall behind <c>nowUtc</c> and still feed the
    /// shared floor. One chunk width (<see cref="TimescaleSupport.ChunkIntervalDays"/>): with every server
    /// current, the shared floor is at most one chunk width old, so the bounded outer scan touches at most
    /// two calendar-day chunks (today's, plus yesterday's when the floor falls before UTC midnight) — the
    /// live EXPLAIN pin's bound (#4228).</summary>
    public static readonly TimeSpan StalenessHorizon = TimeSpan.FromDays(TimescaleSupport.ChunkIntervalDays);

    /* ─────────────────────────── SQL: step one, per-server newest instant ─────────────────────────── */

    /// <summary>Step one for the replica grain: every server's newest <c>ag_replica_states</c> instant, via
    /// an ordered descent per server (the index on <c>(server_id, collection_time)</c> answers each LATERAL
    /// with a single-chunk range scan) rather than the unbounded <c>MAX(...) GROUP BY</c> this replaces.
    /// Driven from <c>servers</c> so the driving set is fleet-sized, not retention-sized; deliberately NOT
    /// scoped to <c>is_enabled</c> (see the type doc). A server with no replica rows contributes no row (CROSS
    /// JOIN LATERAL). $1 an optional server_id filter — NULL means the whole fleet.</summary>
    public const string ReplicaNewestInstantSql = """
        SELECT s.server_id, l.collection_time
        FROM servers AS s
        CROSS JOIN LATERAL
        (
            SELECT r.collection_time
            FROM ag_replica_states AS r
            WHERE r.server_id = s.server_id
            ORDER BY r.collection_time DESC
            LIMIT 1
        ) AS l
        WHERE ($1::integer IS NULL OR s.server_id = $1)
        """;

    /// <summary>Step one for the database grain — same shape as <see cref="ReplicaNewestInstantSql"/>, over
    /// <c>ag_database_replica_states</c>. Windowed independently: the two collectors sweep separately, so
    /// their newest instants need not coincide, exactly as the shipped statements always assumed. $1 an
    /// optional server_id filter — NULL means the whole fleet.</summary>
    public const string DatabaseNewestInstantSql = """
        SELECT s.server_id, l.collection_time
        FROM servers AS s
        CROSS JOIN LATERAL
        (
            SELECT d.collection_time
            FROM ag_database_replica_states AS d
            WHERE d.server_id = s.server_id
            ORDER BY d.collection_time DESC
            LIMIT 1
        ) AS l
        WHERE ($1::integer IS NULL OR s.server_id = $1)
        """;

    /* ─────────────────────────── SQL: step two, floor-bounded snapshot ─────────────────────────── */

    /// <summary>Step two for the replica grain: the shipped statement's exact shape (the inner aggregate,
    /// the join back on the found instant, the enabled-registry join — all unchanged), with
    /// <c>collection_time &gt;= $1</c> added to BOTH the inner aggregate and the outer scan. Called once for
    /// the whole "current" set with the shared floor, and once per "stale" server with that server's own
    /// instant as $1 and its own server_id as $2. $1 the floor (naive UTC); $2 an optional server_id filter —
    /// NULL means every current server.</summary>
    public const string ReplicaStatesSql = """
        SELECT
            r.server_id,
            r.server_name,
            r.collection_time,
            r.ag_name,
            r.replica_server_name,
            r.role_desc,
            r.is_local,
            r.operational_state_desc,
            r.connected_state_desc,
            r.recovery_health_desc,
            r.synchronization_health_desc,
            r.availability_mode_desc,
            r.failover_mode_desc,
            r.endpoint_url
        FROM ag_replica_states AS r
        JOIN
        (
            SELECT server_id, MAX(collection_time) AS max_collection_time
            FROM ag_replica_states
            WHERE collection_time >= $1
              AND ($2::integer IS NULL OR server_id = $2)
            GROUP BY server_id
        ) AS latest
            ON r.server_id = latest.server_id
            AND r.collection_time = latest.max_collection_time
        JOIN servers AS s
            ON s.server_id = r.server_id
            AND s.is_enabled
        WHERE r.collection_time >= $1
          AND ($2::integer IS NULL OR r.server_id = $2)
        ORDER BY r.server_name, r.ag_name, r.replica_server_name
        """;

    /// <summary>Step two for the database grain — same shape as <see cref="ReplicaStatesSql"/>, over
    /// <c>ag_database_replica_states</c>, selecting the Service's superset column list (includes
    /// <c>last_hardened_lsn</c> / <c>last_commit_lsn</c>; see the type doc). $1 the floor (naive UTC); $2 an
    /// optional server_id filter — NULL means every current server.</summary>
    public const string DatabaseReplicaStatesSql = """
        SELECT
            d.server_id,
            d.server_name,
            d.collection_time,
            d.ag_name,
            d.database_name,
            d.replica_server_name,
            d.is_local,
            d.synchronization_state_desc,
            d.last_hardened_lsn,
            d.last_commit_lsn,
            d.log_send_queue_size,
            d.redo_queue_size,
            d.log_send_rate,
            d.redo_rate,
            d.is_suspended,
            d.suspend_reason_desc,
            d.availability_mode_desc,
            d.secondary_lag_seconds
        FROM ag_database_replica_states AS d
        JOIN
        (
            SELECT server_id, MAX(collection_time) AS max_collection_time
            FROM ag_database_replica_states
            WHERE collection_time >= $1
              AND ($2::integer IS NULL OR server_id = $2)
            GROUP BY server_id
        ) AS latest
            ON d.server_id = latest.server_id
            AND d.collection_time = latest.max_collection_time
        JOIN servers AS s
            ON s.server_id = d.server_id
            AND s.is_enabled
        WHERE d.collection_time >= $1
          AND ($2::integer IS NULL OR d.server_id = $2)
        ORDER BY d.server_name, d.ag_name, d.database_name, d.replica_server_name
        """;

    /// <summary>The nav-gate probe's aggregate (#4189): the same (server, ag) groups
    /// <see cref="ReplicaStatesSql"/> would build, counted server-side instead of assembled into rows. Same
    /// floor-bounded shape as the row reads. $1 the floor (naive UTC); $2 an optional server_id filter — NULL
    /// means every current server.</summary>
    public const string ReplicaGroupCountSql = """
        SELECT COUNT(DISTINCT (r.server_id, UPPER(COALESCE(r.ag_name, ''))))
        FROM ag_replica_states AS r
        JOIN
        (
            SELECT server_id, MAX(collection_time) AS max_collection_time
            FROM ag_replica_states
            WHERE collection_time >= $1
              AND ($2::integer IS NULL OR server_id = $2)
            GROUP BY server_id
        ) AS latest
            ON r.server_id = latest.server_id
            AND r.collection_time = latest.max_collection_time
        JOIN servers AS s
            ON s.server_id = r.server_id
            AND s.is_enabled
        WHERE r.collection_time >= $1
          AND ($2::integer IS NULL OR r.server_id = $2)
        """;

    /* ─────────────────────────── rows ─────────────────────────── */

    /// <summary>One replica-grain row, exactly as <c>ag_replica_states</c> stores it.</summary>
    public readonly record struct ReplicaStateRow(
        int ServerId,
        string ServerName,
        DateTime CollectionTime,
        string? AgName,
        string? ReplicaServerName,
        string? RoleDesc,
        bool? IsLocal,
        string? OperationalStateDesc,
        string? ConnectedStateDesc,
        string? RecoveryHealthDesc,
        string? SynchronizationHealthDesc,
        string? AvailabilityModeDesc,
        string? FailoverModeDesc,
        string? EndpointUrl);

    /// <summary>One database-grain row, exactly as <c>ag_database_replica_states</c> stores it. Queue sizes
    /// are KB and rates KB/s (the DMV's units), both instantaneous gauges rather than counters.</summary>
    public readonly record struct DatabaseReplicaStateRow(
        int ServerId,
        string ServerName,
        DateTime CollectionTime,
        string? AgName,
        string? DatabaseName,
        string? ReplicaServerName,
        bool? IsLocal,
        string? SynchronizationStateDesc,
        string? LastHardenedLsn,
        string? LastCommitLsn,
        long? LogSendQueueSize,
        long? RedoQueueSize,
        long? LogSendRate,
        long? RedoRate,
        bool? IsSuspended,
        string? SuspendReasonDesc,
        string? AvailabilityModeDesc,
        long? SecondaryLagSeconds);

    /* ─────────────────────────── reads ─────────────────────────── */

    /// <summary>Every replica row from each server's newest <c>ag_replica_states</c> snapshot — the two-step,
    /// floor-bounded read (#4228). <paramref name="nowUtc"/> anchors the staleness split; pass the same
    /// instant a caller would have stamped the unbounded read with.</summary>
    public static async Task<List<ReplicaStateRow>> GetReplicaStatesAsync(
        NpgsqlDataSource postgres, int? serverIdFilter, DateTime nowUtc, CancellationToken cancellationToken = default)
    {
        var instants = await GetNewestInstantsAsync(postgres, ReplicaNewestInstantSql, serverIdFilter, cancellationToken);
        var (floor, stale) = SplitCurrentAndStale(instants, nowUtc);

        var rows = new List<ReplicaStateRow>();
        if (floor is not null)
        {
            rows.AddRange(await ReadReplicaStatesAsync(postgres, floor.Value, serverIdFilter, cancellationToken));
        }

        foreach (var server in stale)
        {
            rows.AddRange(await ReadReplicaStatesAsync(postgres, server.Instant, server.ServerId, cancellationToken));
        }

        rows.Sort(CompareReplicaRows);
        return rows;
    }

    /// <summary>Every database-grain row from each server's newest <c>ag_database_replica_states</c> snapshot
    /// — same two-step shape as <see cref="GetReplicaStatesAsync"/>, windowed independently.</summary>
    public static async Task<List<DatabaseReplicaStateRow>> GetDatabaseReplicaStatesAsync(
        NpgsqlDataSource postgres, int? serverIdFilter, DateTime nowUtc, CancellationToken cancellationToken = default)
    {
        var instants = await GetNewestInstantsAsync(postgres, DatabaseNewestInstantSql, serverIdFilter, cancellationToken);
        var (floor, stale) = SplitCurrentAndStale(instants, nowUtc);

        var rows = new List<DatabaseReplicaStateRow>();
        if (floor is not null)
        {
            rows.AddRange(await ReadDatabaseReplicaStatesAsync(postgres, floor.Value, serverIdFilter, cancellationToken));
        }

        foreach (var server in stale)
        {
            rows.AddRange(await ReadDatabaseReplicaStatesAsync(postgres, server.Instant, server.ServerId, cancellationToken));
        }

        rows.Sort(CompareDatabaseRows);
        return rows;
    }

    /// <summary>The nav-gate probe's count (#4189): how many distinct (reporting server, AG) groups the
    /// replica grain would build, without assembling a single row. Same two-step split as
    /// <see cref="GetReplicaStatesAsync"/>; the current-set count and every stale server's own count are
    /// disjoint by server_id, so they sum without double-counting.</summary>
    public static async Task<int> GetReplicaGroupCountAsync(
        NpgsqlDataSource postgres, int? serverIdFilter, DateTime nowUtc, CancellationToken cancellationToken = default)
    {
        var instants = await GetNewestInstantsAsync(postgres, ReplicaNewestInstantSql, serverIdFilter, cancellationToken);
        var (floor, stale) = SplitCurrentAndStale(instants, nowUtc);

        var total = 0;
        if (floor is not null)
        {
            total += await ReadReplicaGroupCountAsync(postgres, floor.Value, serverIdFilter, cancellationToken);
        }

        foreach (var server in stale)
        {
            total += await ReadReplicaGroupCountAsync(postgres, server.Instant, server.ServerId, cancellationToken);
        }

        return total;
    }

    /* ─────────────────────────── step one + the staleness split ─────────────────────────── */

    private static async Task<List<(int ServerId, DateTime Instant)>> GetNewestInstantsAsync(
        NpgsqlDataSource postgres, string sql, int? serverIdFilter, CancellationToken cancellationToken)
    {
        var rows = new List<(int, DateTime)>();
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        AddServerFilter(command, serverIdFilter);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add((reader.GetInt32(0), reader.GetDateTime(1)));
        }

        return rows;
    }

    /// <summary>Splits step one's per-server instants into the shared floor (the minimum instant among
    /// servers within <see cref="StalenessHorizon"/> of <paramref name="nowUtc"/>, or null when none are
    /// current) and the stale servers' own (server_id, instant) pairs — internal so this pure split tests
    /// without a store.</summary>
    internal static (DateTime? Floor, List<(int ServerId, DateTime Instant)> Stale) SplitCurrentAndStale(
        IReadOnlyList<(int ServerId, DateTime Instant)> instants, DateTime nowUtc)
    {
        DateTime? floor = null;
        var stale = new List<(int, DateTime)>();
        var horizonStart = nowUtc - StalenessHorizon;

        foreach (var (serverId, instant) in instants)
        {
            if (instant < horizonStart)
            {
                stale.Add((serverId, instant));
            }
            else if (floor is null || instant < floor.Value)
            {
                floor = instant;
            }
        }

        return (floor, stale);
    }

    /* ─────────────────────────── step two ─────────────────────────── */

    private static async Task<List<ReplicaStateRow>> ReadReplicaStatesAsync(
        NpgsqlDataSource postgres, DateTime floor, int? serverIdFilter, CancellationToken cancellationToken)
    {
        var rows = new List<ReplicaStateRow>();
        await using var command = postgres.CreateCommand(ReplicaStatesSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        AddTimestamp(command, floor);
        AddServerFilter(command, serverIdFilter);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ReplicaStateRow(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.GetDateTime(2),
                Text(reader, 3),
                Text(reader, 4),
                Text(reader, 5),
                Flag(reader, 6),
                Text(reader, 7),
                Text(reader, 8),
                Text(reader, 9),
                Text(reader, 10),
                Text(reader, 11),
                Text(reader, 12),
                Text(reader, 13)));
        }

        return rows;
    }

    private static async Task<List<DatabaseReplicaStateRow>> ReadDatabaseReplicaStatesAsync(
        NpgsqlDataSource postgres, DateTime floor, int? serverIdFilter, CancellationToken cancellationToken)
    {
        var rows = new List<DatabaseReplicaStateRow>();
        await using var command = postgres.CreateCommand(DatabaseReplicaStatesSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        AddTimestamp(command, floor);
        AddServerFilter(command, serverIdFilter);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new DatabaseReplicaStateRow(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.GetDateTime(2),
                Text(reader, 3),
                Text(reader, 4),
                Text(reader, 5),
                Flag(reader, 6),
                Text(reader, 7),
                Text(reader, 8),
                Text(reader, 9),
                Count(reader, 10),
                Count(reader, 11),
                Count(reader, 12),
                Count(reader, 13),
                Flag(reader, 14),
                Text(reader, 15),
                Text(reader, 16),
                Count(reader, 17)));
        }

        return rows;
    }

    private static async Task<int> ReadReplicaGroupCountAsync(
        NpgsqlDataSource postgres, DateTime floor, int? serverIdFilter, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(ReplicaGroupCountSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        AddTimestamp(command, floor);
        AddServerFilter(command, serverIdFilter);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt32(result ?? 0L);
    }

    /* ─────────────────────────── merge ordering ─────────────────────────── */

    /// <summary>Mirrors <see cref="ReplicaStatesSql"/>'s <c>ORDER BY server_name, ag_name,
    /// replica_server_name</c> in C#, needed only because a stale server's own read is a separate round trip
    /// from the shared-floor read and the two lists have to interleave back into one order.</summary>
    private static int CompareReplicaRows(ReplicaStateRow a, ReplicaStateRow b)
    {
        var byServer = string.CompareOrdinal(a.ServerName, b.ServerName);
        if (byServer != 0)
        {
            return byServer;
        }

        var byAg = string.CompareOrdinal(a.AgName ?? "", b.AgName ?? "");
        return byAg != 0 ? byAg : string.CompareOrdinal(a.ReplicaServerName ?? "", b.ReplicaServerName ?? "");
    }

    /// <summary>Mirrors <see cref="DatabaseReplicaStatesSql"/>'s <c>ORDER BY server_name, ag_name,
    /// database_name, replica_server_name</c>, for the same reason as <see cref="CompareReplicaRows"/>.</summary>
    private static int CompareDatabaseRows(DatabaseReplicaStateRow a, DatabaseReplicaStateRow b)
    {
        var byServer = string.CompareOrdinal(a.ServerName, b.ServerName);
        if (byServer != 0)
        {
            return byServer;
        }

        var byAg = string.CompareOrdinal(a.AgName ?? "", b.AgName ?? "");
        if (byAg != 0)
        {
            return byAg;
        }

        var byDatabase = string.CompareOrdinal(a.DatabaseName ?? "", b.DatabaseName ?? "");
        return byDatabase != 0 ? byDatabase : string.CompareOrdinal(a.ReplicaServerName ?? "", b.ReplicaServerName ?? "");
    }

    /* ─────────────────────────── binding + column helpers ─────────────────────────── */

    /// <summary>Binds the optional server filter as a nullable integer — NULL means the whole fleet (step
    /// one) or every current server (step two).</summary>
    private static void AddServerFilter(NpgsqlCommand command, int? serverIdFilter) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Integer,
            Value = (object?)serverIdFilter ?? DBNull.Value,
        });

    /// <summary>Binds a floor as <c>Kind=Unspecified</c> — the naive-UTC discipline the whole
    /// <c>DarlingPg*Reader</c> family binds by (<see cref="EventWindowFloor"/>'s doc). A <c>Kind=Utc</c> value
    /// makes Npgsql infer <c>timestamptz</c>, and PostgreSQL then resolves the comparison against these naive
    /// <c>timestamp</c> columns by converting the COLUMNS at the store session's TimeZone.</summary>
    private static void AddTimestamp(NpgsqlCommand command, DateTime value) =>
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) });

    private static string? Text(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static bool? Flag(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetBoolean(ordinal);

    private static long? Count(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetInt64(ordinal);
}
