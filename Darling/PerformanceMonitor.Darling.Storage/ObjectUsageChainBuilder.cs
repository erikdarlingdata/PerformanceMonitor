/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Builds the persisted object usage chain (#9349) for one (server, database): it folds the pair's <c>collect.index_object_stats</c> snapshots
/// forward with the shared stepper (<c>ObjectUsageStepper</c>, the same rules <c>get_object_usage</c>'s full fold runs) and stores what each step
/// decided in <see cref="ObjectUsageChain"/>'s three tables. It never writes <c>index_object_stats</c> and never runs on a request path.
///
/// <para><b>One step is one snapshot and one transaction.</b> The step's <c>object_usage_snapshots</c> row, its delta rows and its run changes
/// commit together, so a reader never sees half a step. A failed step stops the build; the next call restores the chain's state from what was
/// committed and goes on.</para>
///
/// <para><b>Where a call starts.</b> A pair with no snapshots row, or with a row built under an older <see cref="ObjectUsageChainVersion.Current"/>,
/// has all three tables' rows deleted and is built from its oldest retained snapshot (a backfill). Otherwise the stepper's state is restored from
/// the pair's snapshots rows (each node's last snapshot and kept start, the previous node and the previous carried server name) and the raw rows
/// of each node's last snapshot, and the call goes on with the raw snapshots after the watermark. A backfill and a build one snapshot per call
/// store the same rows, apart from <c>built_at</c>.</para>
///
/// <para><b>Two runs of one server's <c>index_object_stats</c> never overlap on one host, so the raw snapshots of a pair arrive in
/// <c>collection_time</c> order and a snapshot once there is complete.</b> The daily run holds the per-(server, collector) <c>DetachedCollectorGate</c> slot,
/// and the at-connect run and <c>snapshot_now</c> take that same slot before they run (<c>DarlingWorker.TryTakeInlineDailySlot</c>, called from
/// <c>RunOnLoadAsync</c> and from the snapshot path), so an inline run never starts beside a scheduled one. A run with a non-daily schedule override runs
/// under the per-server <c>CollectionGate</c>, which <c>snapshot_now</c> also takes. A snapshot's <c>collection_time</c> comes from
/// <c>CollectionTimeClock.NextStrictlyAfter</c> per (server, collector), so no two snapshots of one server share a time and a later run never lands before
/// an earlier one. The builder therefore never scans for a snapshot that is missing from the middle of what it has built, and a call that finds
/// the watermark behind the newest snapshot just goes on from it. The invariant is per host: a second host writing the same store is not covered by
/// it.</para>
///
/// <para><b>Raw rows are read in batches</b> of whole snapshots, so a pair with years of rows is never held in memory or in one long read.</para>
/// </summary>
public static class ObjectUsageChainBuilder
{
    /// <summary>Raw rows a read asks for. A batch that holds one snapshot only is read again twice as large.</summary>
    public const int DefaultRawBatchRows = 20000;

    private const int CommandTimeoutSeconds = 300;

    /// <summary>The raw rows after a time, oldest first, a fixed order inside a snapshot. $1 server_id, $2 database_name, $3 the first time read (inclusive), $4 the row limit.</summary>
    private const string RawAfterSql = "SELECT " + ObjectUsageWindowRead.Columns + """

        FROM collect.v_index_object_stats AS s
        WHERE s.server_id = $1
        AND   s.database_name = $2
        AND   s.collection_time >= $3
        ORDER BY
            s.collection_time,
            s.collection_id
        LIMIT $4
        """;

    /// <summary>The raw rows of named snapshots. $1 server_id, $2 database_name, $3 the collection times.</summary>
    private const string RawAtSql = "SELECT " + ObjectUsageWindowRead.Columns + """

        FROM collect.v_index_object_stats AS s
        WHERE s.server_id = $1
        AND   s.database_name = $2
        AND   s.collection_time = ANY($3)
        ORDER BY
            s.collection_time,
            s.collection_id
        """;

    private const string ChainRowsSql = """
        SELECT
            collection_time,
            step_version,
            answering_server_name,
            node_key,
            kept_start
        FROM collect.object_usage_snapshots
        WHERE server_id = $1
        AND   database_name = $2
        ORDER BY
            collection_time
        """;

    /// <summary>The pair's watermark and the oldest step version in its snapshots rows (null, null when it has none). $1 server_id, $2 database_name. Reads the primary key's range of one pair.</summary>
    private const string LastSnapshotSql = """
        SELECT
            max(collection_time),
            min(step_version)
        FROM collect.object_usage_snapshots
        WHERE server_id = $1
        AND   database_name = $2
        """;

    /// <summary>One raw row after the watermark, if there is any. $1 server_id, $2 database_name, $3 the watermark.</summary>
    private const string RawAfterProbeSql = """
        SELECT
            1
        FROM collect.v_index_object_stats AS s
        WHERE s.server_id = $1
        AND   s.database_name = $2
        AND   s.collection_time > $3
        LIMIT 1
        """;

    /// <summary>
    /// A test's count of the chain-row reads the builder made in its own async flow (a one-element array it sets before the call), so a test
    /// can tell a call that stopped at the probe from one that read the chain. Null in production, which counts nothing.
    /// </summary>
    internal static readonly System.Threading.AsyncLocal<int[]?> ChainReadCount = new();

    private const string OpenRunsSql = """
        SELECT
            object_id,
            index_id,
            schema_name,
            object_name,
            is_indexed_view,
            is_memory_optimized
        FROM collect.object_usage_runs
        WHERE server_id = $1
        AND   database_name = $2
        AND   last_time IS NULL
        """;

    /// <summary>The pair's rows in each table, one statement each (a parameterised statement is a single command). $1 server_id, $2 database_name.</summary>
    private static readonly string[] DeletePairSql =
    [
        "DELETE FROM collect.object_usage_deltas WHERE server_id = $1 AND database_name = $2",
        "DELETE FROM collect.object_usage_runs WHERE server_id = $1 AND database_name = $2",
        "DELETE FROM collect.object_usage_snapshots WHERE server_id = $1 AND database_name = $2",
    ];

    private const string InsertSnapshotSql = """
        INSERT INTO collect.object_usage_snapshots
            (server_id, database_name, collection_time, step_version, answering_server_name, sqlserver_start_time, node_key, compared_with,
             kept_start, reasons, counted, index_resets, blind_from, built_at)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        """;

    private const string InsertDeltasSql = """
        INSERT INTO collect.object_usage_deltas
            (server_id, database_name, collection_time, object_id, index_id, reads, updates, last_read, last_update)
        SELECT $1, $2, $3, u.object_id, u.index_id, u.reads, u.updates, u.last_read, u.last_update
        FROM unnest($4::integer[], $5::integer[], $6::bigint[], $7::bigint[], $8::timestamp[], $9::timestamp[])
            AS u(object_id, index_id, reads, updates, last_read, last_update)
        """;

    private const string CloseRunsSql = """
        UPDATE collect.object_usage_runs AS r
        SET last_time = $3
        FROM unnest($4::integer[], $5::integer[]) AS u(object_id, index_id)
        WHERE r.server_id = $1
        AND   r.database_name = $2
        AND   r.object_id = u.object_id
        AND   r.index_id = u.index_id
        AND   r.last_time IS NULL
        """;

    private const string InsertRunsSql = """
        INSERT INTO collect.object_usage_runs
            (server_id, database_name, object_id, index_id, first_time, last_time, schema_name, object_name, is_indexed_view, is_memory_optimized)
        SELECT $1, $2, u.object_id, u.index_id, $3, NULL, u.schema_name, u.object_name, u.is_indexed_view, u.is_memory_optimized
        FROM unnest($4::integer[], $5::integer[], $6::text[], $7::text[], $8::boolean[], $9::boolean[])
            AS u(object_id, index_id, schema_name, object_name, is_indexed_view, is_memory_optimized)
        """;

    /// <summary>What a run's identity is: the columns whose change starts a new run.</summary>
    private readonly record struct RunIdentity(string SchemaName, string ObjectName, bool IsIndexedView, bool? IsMemoryOptimized);

    private readonly record struct ChainRow(DateTime Time, string? Server, string NodeKey, DateTime? KeptStart);

    /// <summary>
    /// Builds up to <paramref name="maxSteps"/> snapshots of one pair's chain and returns how many it built. Zero means the chain was already
    /// up to date. See the type's remarks for where it starts.
    /// </summary>
    /// <param name="dataSource">The store, opened as a role that writes the <c>collect</c> schema.</param>
    /// <param name="serverId">The server's id.</param>
    /// <param name="databaseName">The monitored database's name, exactly as <c>index_object_stats</c> holds it.</param>
    /// <param name="maxSteps">The most snapshots to build in this call.</param>
    /// <param name="cancellationToken">Checked before each step and by every statement.</param>
    public static Task<int> BuildPairAsync(
        NpgsqlDataSource dataSource, int serverId, string databaseName, int maxSteps, CancellationToken cancellationToken) =>
        BuildPairAsync(dataSource, serverId, databaseName, maxSteps, DefaultRawBatchRows, cancellationToken);

    /// <summary><see cref="BuildPairAsync(NpgsqlDataSource, int, string, int, CancellationToken)"/> with the raw batch size set, so a test can put a batch boundary inside a pair's history.</summary>
    internal static async Task<int> BuildPairAsync(
        NpgsqlDataSource dataSource, int serverId, string databaseName, int maxSteps, int rawBatchRows, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentException.ThrowIfNullOrEmpty(databaseName);
        ArgumentOutOfRangeException.ThrowIfLessThan(rawBatchRows, 1);
        if (maxSteps < 1)
        {
            return 0;
        }

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);

        /* #9350: a pair that has raw rows after its watermark is the only pair with anything to fold, and the tick lists pairs whose raw rows may
           have all been folded already (a database that stopped being collected on a server that did not). One probe of the watermark and
           one LIMIT 1 over the raw rows answer that before the chain rows, the last snapshot's raw rows or the open runs are read. A pair
           with no watermark is a backfill, and a pair built under an older version is rebuilt, so neither takes the probe. */
        if (await ReadLastSnapshotAsync(connection, serverId, databaseName, cancellationToken) is { } built
            && built.Version >= ObjectUsageChainVersion.Current
            && !await HasRawAfterAsync(connection, serverId, databaseName, built.Time, cancellationToken))
        {
            return 0;
        }

        var chain = await ReadChainRowsAsync(connection, serverId, databaseName, cancellationToken);
        var rebuild = chain.Rows.Count == 0 || chain.Versions.Any(v => v < ObjectUsageChainVersion.Current);
        var stepper = new ObjectUsageStepper();
        var open = new Dictionary<(int ObjectId, int IndexId), RunIdentity>();
        DateTime? watermark = null;

        if (rebuild)
        {
            await using var delete = await connection.BeginTransactionAsync(cancellationToken);
            foreach (var statement in DeletePairSql)
            {
                await ExecuteAsync(connection, delete, statement, [serverId, databaseName], cancellationToken);
            }

            await delete.CommitAsync(cancellationToken);
        }
        else
        {
            watermark = chain.Rows[^1].Time;
            await RestoreAsync(connection, serverId, databaseName, chain.Rows, stepper, cancellationToken);
            await using var runs = new NpgsqlCommand(OpenRunsSql, connection) { CommandTimeout = CommandTimeoutSeconds };
            runs.Parameters.Add(Param(NpgsqlDbType.Integer, serverId));
            runs.Parameters.Add(Param(NpgsqlDbType.Text, databaseName));
            await using var reader = await runs.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                open[(reader.GetInt32(0), reader.GetInt32(1))] = new RunIdentity(
                    reader.GetString(2), reader.GetString(3), reader.GetBoolean(4), reader.IsDBNull(5) ? null : reader.GetBoolean(5));
            }
        }

        var steps = 0;
        var from = watermark is { } mark ? mark.AddTicks(10) : DateTime.MinValue;
        var limit = rawBatchRows;
        while (steps < maxSteps)
        {
            var batch = await ReadRawAsync(connection, RawAfterSql, serverId, databaseName, [from, limit], cancellationToken);
            if (batch.Count == 0)
            {
                break;
            }

            var full = batch.Count == limit;
            var lastTime = batch[^1].CollectionTime;
            var complete = full ? batch.FindLastIndex(r => r.CollectionTime < lastTime) + 1 : batch.Count;
            if (complete == 0)
            {
                limit *= 2;
                continue;
            }

            var index = 0;
            while (index < complete && steps < maxSteps)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var time = batch[index].CollectionTime;
                var rows = new List<ObjectUsageSample>();
                while (index < complete && batch[index].CollectionTime == time)
                {
                    rows.Add(batch[index++]);
                }

                await StepAsync(connection, serverId, databaseName, stepper, open, time, rows, cancellationToken);
                steps++;
            }

            if (!full || index < complete)
            {
                break;
            }

            from = lastTime;
        }

        return steps;
    }

    /// <summary>One snapshot: step the shared stepper, then write the snapshots row, the delta rows and the run changes in one transaction.</summary>
    private static async Task StepAsync(
        NpgsqlConnection connection, int serverId, string databaseName, ObjectUsageStepper stepper,
        Dictionary<(int ObjectId, int IndexId), RunIdentity> open, DateTime time, List<ObjectUsageSample> rows, CancellationToken cancellationToken)
    {
        var previousTime = stepper.Nodes.PreviousTime;
        var step = stepper.Step(time, rows);
        var node = step.Node;

        /* Identity per index as the window reads it: the last row of the snapshot names the index, and a null memory-optimized flag does not
           override one an earlier row of the snapshot gave. */
        var present = new Dictionary<(int ObjectId, int IndexId), RunIdentity>();
        foreach (var row in rows)
        {
            var key = (row.ObjectId, row.IndexId);
            var memory = row.IsMemoryOptimized ?? (present.TryGetValue(key, out var earlier) ? earlier.IsMemoryOptimized : null);
            present[key] = new RunIdentity(row.SchemaName, row.ObjectName, row.IsIndexedView, memory);
        }

        var closing = new List<(int ObjectId, int IndexId)>();
        var opening = new List<((int ObjectId, int IndexId) Key, RunIdentity Identity)>();
        foreach (var key in open.Keys)
        {
            if (!present.ContainsKey(key))
            {
                closing.Add(key);
            }
        }

        foreach (var (key, identity) in present)
        {
            if (!open.TryGetValue(key, out var current))
            {
                opening.Add((key, identity));
            }
            else if (current != identity)
            {
                closing.Add(key);
                opening.Add((key, identity));
            }
        }

        /* A step whose node is new to the chain writes no delta rows: every window holding it recomputes it from the raw rows. */
        var deltas = node.ComparedWith is null ? new List<ObjectUsageIndexStep>() : step.Indexes.FindAll(i => i.Moved);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await ExecuteAsync(
            connection, transaction, InsertSnapshotSql,
            [
                serverId, databaseName, Naive(time), (short)ObjectUsageChainVersion.Current, (object?)node.Server ?? DBNull.Value,
                Db(node.Start), node.NodeKey, Db(node.ComparedWith), Db(node.KeptStart), (int)step.Reasons, (short)step.Counted,
                step.IndexResets, Db(step.BlindFrom), Naive(DateTime.UtcNow),
            ],
            cancellationToken);

        if (deltas.Count > 0)
        {
            await using var insert = new NpgsqlCommand(InsertDeltasSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            insert.Parameters.Add(Param(NpgsqlDbType.Integer, serverId));
            insert.Parameters.Add(Param(NpgsqlDbType.Text, databaseName));
            insert.Parameters.Add(Param(NpgsqlDbType.Timestamp, Naive(time)));
            insert.Parameters.Add(Param(NpgsqlDbType.Array | NpgsqlDbType.Integer, deltas.Select(d => d.ObjectId).ToArray()));
            insert.Parameters.Add(Param(NpgsqlDbType.Array | NpgsqlDbType.Integer, deltas.Select(d => d.IndexId).ToArray()));
            insert.Parameters.Add(Param(NpgsqlDbType.Array | NpgsqlDbType.Bigint, deltas.Select(d => d.Reads).ToArray()));
            insert.Parameters.Add(Param(NpgsqlDbType.Array | NpgsqlDbType.Bigint, deltas.Select(d => d.Updates).ToArray()));
            insert.Parameters.Add(Param(NpgsqlDbType.Array | NpgsqlDbType.Timestamp, deltas.Select(d => d.Now.LastRead is { } t ? Naive(t) : (DateTime?)null).ToArray()));
            insert.Parameters.Add(Param(NpgsqlDbType.Array | NpgsqlDbType.Timestamp, deltas.Select(d => d.Now.LastUpdate is { } t ? Naive(t) : (DateTime?)null).ToArray()));
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        if (closing.Count > 0)
        {
            /* An open run was present through the snapshot before this one. */
            await using var close = new NpgsqlCommand(CloseRunsSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            close.Parameters.Add(Param(NpgsqlDbType.Integer, serverId));
            close.Parameters.Add(Param(NpgsqlDbType.Text, databaseName));
            close.Parameters.Add(Param(NpgsqlDbType.Timestamp, Naive(previousTime ?? time)));
            close.Parameters.Add(Param(NpgsqlDbType.Array | NpgsqlDbType.Integer, closing.Select(k => k.ObjectId).ToArray()));
            close.Parameters.Add(Param(NpgsqlDbType.Array | NpgsqlDbType.Integer, closing.Select(k => k.IndexId).ToArray()));
            await close.ExecuteNonQueryAsync(cancellationToken);
        }

        if (opening.Count > 0)
        {
            await using var start = new NpgsqlCommand(InsertRunsSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            start.Parameters.Add(Param(NpgsqlDbType.Integer, serverId));
            start.Parameters.Add(Param(NpgsqlDbType.Text, databaseName));
            start.Parameters.Add(Param(NpgsqlDbType.Timestamp, Naive(time)));
            start.Parameters.Add(Param(NpgsqlDbType.Array | NpgsqlDbType.Integer, opening.Select(o => o.Key.ObjectId).ToArray()));
            start.Parameters.Add(Param(NpgsqlDbType.Array | NpgsqlDbType.Integer, opening.Select(o => o.Key.IndexId).ToArray()));
            start.Parameters.Add(Param(NpgsqlDbType.Array | NpgsqlDbType.Text, opening.Select(o => o.Identity.SchemaName).ToArray()));
            start.Parameters.Add(Param(NpgsqlDbType.Array | NpgsqlDbType.Text, opening.Select(o => o.Identity.ObjectName).ToArray()));
            start.Parameters.Add(Param(NpgsqlDbType.Array | NpgsqlDbType.Boolean, opening.Select(o => o.Identity.IsIndexedView).ToArray()));
            start.Parameters.Add(Param(NpgsqlDbType.Array | NpgsqlDbType.Boolean, opening.Select(o => o.Identity.IsMemoryOptimized).ToArray()));
            await start.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);

        foreach (var key in closing)
        {
            open.Remove(key);
        }

        foreach (var (key, identity) in opening)
        {
            open[key] = identity;
        }
    }

    private static async Task<(DateTime Time, short Version)?> ReadLastSnapshotAsync(
        NpgsqlConnection connection, int serverId, string databaseName, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(LastSnapshotSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(Param(NpgsqlDbType.Integer, serverId));
        command.Parameters.Add(Param(NpgsqlDbType.Text, databaseName));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return reader.IsDBNull(0) ? null : (reader.GetDateTime(0), reader.GetInt16(1));
    }

    private static async Task<bool> HasRawAfterAsync(
        NpgsqlConnection connection, int serverId, string databaseName, DateTime watermark, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(RawAfterProbeSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(Param(NpgsqlDbType.Integer, serverId));
        command.Parameters.Add(Param(NpgsqlDbType.Text, databaseName));
        command.Parameters.Add(Param(NpgsqlDbType.Timestamp, watermark));
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    private static async Task<(List<ChainRow> Rows, List<short> Versions)> ReadChainRowsAsync(
        NpgsqlConnection connection, int serverId, string databaseName, CancellationToken cancellationToken)
    {
        if (ChainReadCount.Value is { } counter)
        {
            counter[0]++;
        }

        var rows = new List<ChainRow>();
        var versions = new List<short>();
        await using var command = new NpgsqlCommand(ChainRowsSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(Param(NpgsqlDbType.Integer, serverId));
        command.Parameters.Add(Param(NpgsqlDbType.Text, databaseName));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            versions.Add(reader.GetInt16(1));
            rows.Add(new ChainRow(
                reader.GetDateTime(0), reader.IsDBNull(2) ? null : reader.GetString(2), reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetDateTime(4)));
        }

        return (rows, versions);
    }

    /// <summary>
    /// Puts the stepper where the chain's last snapshot left it. The fold's state is each node's last snapshot, its kept start and that
    /// snapshot's deduped counters, the node and the carried server name of the snapshot before, and the first snapshot (for the node baseline's
    /// start rule). All but the counters come from the chain rows, and the counters from the raw rows of each node's last snapshot. An unnamed
    /// node exists in the fold only until a name takes it over, so it is restored only while no snapshot has had a name.
    /// </summary>
    private static async Task RestoreAsync(
        NpgsqlConnection connection, int serverId, string databaseName, List<ChainRow> chain, ObjectUsageStepper stepper,
        CancellationToken cancellationToken)
    {
        var last = new Dictionary<string, ChainRow>(StringComparer.OrdinalIgnoreCase);
        string? server = null;
        var named = false;
        foreach (var row in chain)
        {
            last[row.NodeKey] = row;
            server = row.Server ?? server;
            named |= row.NodeKey.Length > 0;
        }

        if (named)
        {
            last.Remove("");
        }

        var times = last.Values.Select(r => Naive(r.Time)).Distinct().ToArray();
        var raw = await ReadRawAsync(connection, RawAtSql, serverId, databaseName, [times], cancellationToken);
        var byTime = raw.GroupBy(r => r.CollectionTime).ToDictionary(g => g.Key, g => ObjectUsageCounterStep.Dedupe(g.ToList()));

        var nodes = last.Select(kv => (
            kv.Key,
            kv.Value.Time,
            kv.Value.KeptStart,
            byTime.TryGetValue(kv.Value.Time, out var counters) ? counters : new Dictionary<(int ObjectId, int IndexId), ObjectUsageCounters>()));
        stepper.Nodes.Restore(nodes, chain[^1].NodeKey, server, chain[0].Time, chain[^1].Time);
    }

    private static async Task<List<ObjectUsageSample>> ReadRawAsync(
        NpgsqlConnection connection, string sql, int serverId, string databaseName, object[] args, CancellationToken cancellationToken)
    {
        var rows = new List<ObjectUsageSample>();
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(Param(NpgsqlDbType.Integer, serverId));
        command.Parameters.Add(Param(NpgsqlDbType.Text, databaseName));
        foreach (var arg in args)
        {
            command.Parameters.Add(arg switch
            {
                DateTime t => Param(NpgsqlDbType.Timestamp, Naive(t)),
                int n => Param(NpgsqlDbType.Integer, n),
                DateTime[] ts => Param(NpgsqlDbType.Array | NpgsqlDbType.Timestamp, ts),
                _ => throw new ArgumentException("Unsupported raw read argument.", nameof(args)),
            });
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ObjectUsageWindowRead.Map(reader));
        }

        return rows;
    }

    private static async Task ExecuteAsync(
        NpgsqlConnection connection, NpgsqlTransaction? transaction, string sql, object[] args, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
        foreach (var arg in args)
        {
            command.Parameters.Add(arg switch
            {
                int n => Param(NpgsqlDbType.Integer, n),
                short n => Param(NpgsqlDbType.Smallint, n),
                string s => Param(NpgsqlDbType.Text, s),
                DateTime t => Param(NpgsqlDbType.Timestamp, t),
                DBNull => new NpgsqlParameter { Value = DBNull.Value },
                _ => throw new ArgumentException("Unsupported statement argument.", nameof(args)),
            });
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static NpgsqlParameter Param(NpgsqlDbType type, object value) => new() { NpgsqlDbType = type, Value = value };

    private static object Db(DateTime? value) => value is { } t ? Naive(t) : DBNull.Value;

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);
}
