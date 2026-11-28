/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Builds Lite's persisted object usage chain (#9596, twinning Darling's <c>ObjectUsageChainBuilder</c> of #9349): for each database whose
/// <c>index_object_stats</c> snapshots are ahead of its chain, it folds the snapshots forward with the shared stepper
/// (<see cref="ObjectUsageStepper"/>, the same rules <c>get_object_usage</c>'s full fold runs) and stores what each step decided in
/// <c>object_usage_snapshots</c>, <c>object_usage_deltas</c> and <c>object_usage_runs</c>. It never writes <c>index_object_stats</c>.
///
/// <para><b>Where a pair starts.</b> A database with no chain row, or with a row built under an older <see cref="ObjectUsageChainVersion.Current"/>,
/// has its chain rows deleted and is built from its oldest stored snapshot (the backfill that fills a store from an earlier version, and the rebuild
/// after a change of the stepper's rules), or from the trim floor when the cap has trimmed the chain since the app started (<see cref="_trimFloor"/>).
/// Otherwise the stepper is restored from the chain rows and the raw rows of each answering instance's last snapshot, and carries on with the
/// snapshots after the watermark.</para>
///
/// <para><b>Short lock holds.</b> One snapshot is one read (the store's read lock, released before the step) and one write transaction under the
/// store's READ lock. This class is the only writer of the three chain tables and its gate keeps it to one pass at a time, so no other writer
/// can collide with its UPDATEs and DELETEs, and the read lock keeps out what must not run beside it: archival, CHECKPOINT and a size reset.
/// The write lock would shut every collector append and every read out for the length of a step. The runs a step ends are closed by one
/// statement over the whole set, never one statement per index. A pass that is stopped or fails part way leaves a chain that ends at the
/// last whole snapshot, and the next pass carries on.</para>
///
/// <para><b>Who runs it.</b> The housekeeping loop of <see cref="CollectionBackgroundService"/>, never a request path. A database is listed
/// when its newest snapshot in the hot <c>index_object_stats</c> table is newer than its chain. The newest snapshot of a database is always hot,
/// because archival moves only rows older than a week, so the hot table answers the listing without reading the archive. A database whose
/// snapshots are all archived is not listed, and <c>get_object_usage</c> folds it as before.</para>
/// </summary>
internal sealed class ObjectUsageChainBuilder
{
    /// <summary>How long one pass runs before it stops and leaves the rest to the next tick.</summary>
    internal static readonly TimeSpan MaxPassDuration = TimeSpan.FromMinutes(5);

    /// <summary>The most snapshots one pass builds.</summary>
    internal const int MaxStepsPerPass = 5000;

    /// <summary>
    /// The longest a chain row is kept: the largest window <c>get_object_usage</c> takes, the baseline's reach before it and a week of slack,
    /// so a derived row is never purged before the raw row it derives from. The store's own retention removes archived raw rows at the
    /// <see cref="RetentionService.ArchiveRetentionMonths"/> horizon.
    /// </summary>
    internal static readonly int RetentionDays = RetentionService.ArchiveRetentionMonths * 31 + ObjectUsageWindowRead.BaselineReachDays + 7;

    /// <summary>
    /// The share of the hot store's reset point (<see cref="HotStoreResetLimit.Current"/>, the <c>hot_store_reset_mb</c> setting) the three chain
    /// tables may use together. They live in the hot store and count toward the reset, and a collector that runs hourly writes about 24 times the
    /// delta rows a daily one does.
    /// </summary>
    internal const double CapShareOfResetPoint = 0.10;

    /// <summary>
    /// The stored size of one chain row, for the size estimate: the row count of the three tables times this. A store of 4,950,000 delta rows and
    /// 50,000 runs measured 11.3 bytes a row in <c>pragma_database_size()</c> on counters that grow evenly, which compress well, so the estimate
    /// uses twice that. A row count is one metadata read per table, so the check is cheap enough to run after every step.
    /// </summary>
    internal const int EstimatedBytesPerRow = 24;

    /// <summary>A trim goes down to this share of the cap, so the pass that follows has room to build before it is over again.</summary>
    internal const double TrimTargetShare = 0.8;

    /// <summary>The window the minimum span serves, in days: a <c>get_object_usage</c> read of the newest week.</summary>
    internal const int ServedWindowDays = 7;

    /// <summary>
    /// The shortest span of newest snapshots worth keeping, in calendar days with the newest day counted as the first: a chain that cannot hold this
    /// much under the cap is dropped and the fold answers. It is the span a window of <see cref="ServedWindowDays"/> days needs in the chain. A window
    /// read at or after the newest snapshot starts on the 8th day at the earliest, and its baseline is the snapshot at or before its start: on the 8th
    /// day, or on the 9th when the 8th day's first snapshot is later than the start. A trim cuts at a midnight, so the span holds both days whole. A
    /// window whose baseline is older still, after more than a day with no snapshot, folds.
    /// </summary>
    internal const int MinKeptDays = ServedWindowDays + 2;

    /// <summary>The databases whose newest hot snapshot is ahead of their chain, or whose chain was built under older rules. $1 the current step version.</summary>
    internal const string PendingPairsSql = """
        SELECT
            h.server_id,
            h.database_name
        FROM
        (
            SELECT
                server_id,
                database_name,
                max(collection_time) AS newest
            FROM index_object_stats
            GROUP BY
                server_id,
                database_name
        ) AS h
        LEFT JOIN
        (
            SELECT
                server_id,
                database_name,
                max(collection_time) AS built,
                min(step_version) AS version
            FROM object_usage_snapshots
            GROUP BY
                server_id,
                database_name
        ) AS c
          ON  c.server_id = h.server_id
          AND c.database_name = h.database_name
        WHERE c.built IS NULL
        OR    c.built < h.newest
        OR    c.version < $1
        ORDER BY
            h.server_id,
            h.database_name
        """;

    private const string ChainRowsSql = """
        SELECT
            collection_time,
            step_version,
            answering_server_name,
            node_key,
            kept_start
        FROM object_usage_snapshots
        WHERE server_id = $1
        AND   database_name = $2
        ORDER BY
            collection_time
        """;

    private const string OpenRunsSql = """
        SELECT
            object_id,
            index_id,
            schema_name,
            object_name,
            is_indexed_view,
            is_memory_optimized
        FROM object_usage_runs
        WHERE server_id = $1
        AND   database_name = $2
        AND   last_time IS NULL
        """;

    /// <summary>The distinct snapshot times of one database after a watermark. $1 server_id, $2 database_name, $3 the watermark.</summary>
    private const string TimesAfterSql = """
        SELECT DISTINCT
            s.collection_time
        FROM v_index_object_stats AS s
        WHERE s.server_id = $1
        AND   s.database_name = $2
        AND   s.collection_time > $3
        ORDER BY
            s.collection_time
        """;

    /// <summary>The distinct snapshot times of one database from a floor on, the floor included. $1 server_id, $2 database_name, $3 the floor.</summary>
    private const string TimesFromSql = """
        SELECT DISTINCT
            s.collection_time
        FROM v_index_object_stats AS s
        WHERE s.server_id = $1
        AND   s.database_name = $2
        AND   s.collection_time >= $3
        ORDER BY
            s.collection_time
        """;

    /// <summary>Every snapshot time of one database. $1 server_id, $2 database_name.</summary>
    private const string TimesAllSql = """
        SELECT DISTINCT
            s.collection_time
        FROM v_index_object_stats AS s
        WHERE s.server_id = $1
        AND   s.database_name = $2
        ORDER BY
            s.collection_time
        """;

    private static readonly string[] DeletePairSql =
    [
        "DELETE FROM object_usage_deltas WHERE server_id = $1 AND database_name = $2",
        "DELETE FROM object_usage_runs WHERE server_id = $1 AND database_name = $2",
        "DELETE FROM object_usage_snapshots WHERE server_id = $1 AND database_name = $2",
    ];

    private const string InsertSnapshotSql = """
        INSERT INTO object_usage_snapshots
            (server_id, database_name, collection_time, step_version, answering_server_name, sqlserver_start_time, node_key, compared_with,
             kept_start, reasons, counted, index_resets, blind_from, built_at)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        """;

    /// <summary>The connection's own scratch table for the keys of the runs one step ends.</summary>
    private const string ClosingTable = "object_usage_closing";

    private const string StageClosingSql = "CREATE TEMP TABLE " + ClosingTable + " (object_id INTEGER NOT NULL, index_id INTEGER NOT NULL)";

    /// <summary>Ends every open run whose key is staged. $1 server_id, $2 database_name, $3 the last snapshot the runs were present in.</summary>
    private const string CloseRunsSql = """
        UPDATE object_usage_runs
        SET last_time = $3
        FROM object_usage_closing AS c
        WHERE object_usage_runs.server_id = $1
        AND   object_usage_runs.database_name = $2
        AND   object_usage_runs.object_id = c.object_id
        AND   object_usage_runs.index_id = c.index_id
        AND   object_usage_runs.last_time IS NULL
        """;

    private readonly DuckDbInitializer _duckDb;

    /// <summary>One pass at a time: the housekeeping loop awaits each pass, and a caller outside it (a test, a future verb) takes the same gate.</summary>
    private readonly SemaphoreSlim _gate = new(1, 1);

    /// <summary>The clock the purge horizon is measured from. The wall clock, except in a test that builds a history around a fixed day.</summary>
    private readonly Func<DateTime> _utcNow;

    /// <summary>The hot store's reset point in MB, read at each check. The setting the reset itself uses, except in a test that needs a small one.</summary>
    private readonly Func<double> _resetPointMb;

    /// <summary>
    /// Databases whose chain was dropped because the newest <see cref="MinKeptDays"/> days did not fit under the cap, each with the rows those days
    /// held when it was dropped (the most of every drop). A pass skips one until the chain's rows plus that many fit under <see cref="TrimTargetShare"/>
    /// of the cap, which is the condition the drop broke, so it comes back once the cap is raised or a trim, a purge or the size reset frees room, and
    /// is not built and dropped again on every pass while that condition holds.
    /// </summary>
    private readonly Dictionary<(int ServerId, string DatabaseName), long> _stopped = [];

    /// <summary>The database being extended when the chain reached the cap, for the pass to deal with at its end.</summary>
    private (int ServerId, string DatabaseName)? _capHit;

    /// <summary>
    /// The cutoff of the latest cap trim (<see cref="EnforceCapAsync"/>), null until one runs. A trim removes the rows older than its cutoff from every
    /// database, so a database that starts a chain from nothing starts at this floor and not at its oldest stored snapshot. Built from its oldest
    /// snapshot, a database part way through its backfill would hold only days older than the cutoff the other databases' newest days set: the trim
    /// would remove all of them and every pass would build the same days again, so the database would never keep a chain. Built from the floor, its
    /// days are days the trim keeps. Its windows that start before the floor fold, as every database's do after a trim. The floor lives in memory, so
    /// after a restart one pass may build such a database from its oldest snapshot and lose it to one trim, which sets the floor again.
    /// </summary>
    private DateTime? _trimFloor;

    internal ObjectUsageChainBuilder(DuckDbInitializer duckDb, Func<DateTime>? utcNow = null, Func<double>? resetPointMb = null)
    {
        _duckDb = duckDb ?? throw new ArgumentNullException(nameof(duckDb));
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _resetPointMb = resetPointMb ?? (() => HotStoreResetLimit.Current.Mb);
    }

    /// <summary>Test seam (#9596): awaited with the snapshot's time after each step, outside every lock, so a test can run a size reset or a
    /// collector write between two steps of one pass. Null in the shipped app.</summary>
    internal Func<DateTime, Task>? StepCompletedForTests { get; set; }

    /// <summary>The most chain rows (all three tables) the cap allows.</summary>
    internal long CapRows => (long)(_resetPointMb() * 1048576.0 * CapShareOfResetPoint / EstimatedBytesPerRow);

    /// <summary>What an index's presence run is keyed on: the columns whose change starts a new run.</summary>
    private readonly record struct RunIdentity(string SchemaName, string ObjectName, bool IsIndexedView, bool? IsMemoryOptimized);

    private readonly record struct ChainRow(DateTime Time, string? Server, string NodeKey, DateTime? KeptStart);

    /// <summary>
    /// One pass: builds the chain of every listed database, up to <paramref name="maxSteps"/> snapshots and <see cref="MaxPassDuration"/>, then
    /// purges what is past <see cref="RetentionDays"/>. Returns how many snapshots it built. A database that throws is logged and skipped.
    /// </summary>
    internal async Task<int> BuildPassAsync(int maxSteps, CancellationToken cancellationToken)
    {
        if (!await _gate.WaitAsync(0, cancellationToken))
        {
            return 0;
        }

        try
        {
            var started = DateTime.UtcNow;
            var built = 0;
            _capHit = null;
            var notes = new List<string>();
            AddNote(notes, await EnforceCapAsync(cancellationToken));
            foreach (var (serverId, databaseName) in await ReadPendingPairsAsync(cancellationToken))
            {
                if (built >= maxSteps || DateTime.UtcNow - started >= MaxPassDuration || _capHit is not null)
                {
                    break;
                }

                if (_stopped.TryGetValue((serverId, databaseName), out var need))
                {
                    var (deltas, other) = await CountChainRowsAsync(cancellationToken);
                    if (deltas + other + need > (long)(CapRows * TrimTargetShare))
                    {
                        continue;
                    }

                    _stopped.Remove((serverId, databaseName));
                }

                try
                {
                    built += await BuildPairCoreAsync(serverId, databaseName, maxSteps - built, started, cancellationToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    AppLogger.Warn("ObjectUsage", $"The object usage chain of server {serverId} database '{databaseName}' was not built: {ex.Message}. The next pass retries it.");
                }
            }

            AddNote(notes, await EnforceCapAsync(cancellationToken));
            await PurgeAsync(_utcNow().AddDays(-RetentionDays), cancellationToken);
            if (notes.Count > 0)
            {
                AppLogger.Warn("ObjectUsage", string.Join(" ", notes));
            }

            return built;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Builds up to <paramref name="maxSteps"/> snapshots of one database's chain and returns how many it built. Zero means the chain was up to date.</summary>
    internal async Task<int> BuildPairAsync(int serverId, string databaseName, int maxSteps, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken);
        try
        {
            return await BuildPairCoreAsync(serverId, databaseName, maxSteps, DateTime.UtcNow, cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<List<(int ServerId, string DatabaseName)>> ReadPendingPairsAsync(CancellationToken cancellationToken)
    {
        var pairs = new List<(int, string)>();
        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = PendingPairsSql;
        command.Parameters.Add(new DuckDBParameter { Value = (short)ObjectUsageChainVersion.Current });
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (!reader.IsDBNull(1))
            {
                pairs.Add((Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture), reader.GetString(1)));
            }
        }

        return pairs;
    }

    private async Task<int> BuildPairCoreAsync(int serverId, string databaseName, int maxSteps, DateTime passStarted, CancellationToken cancellationToken)
    {
        if (maxSteps < 1)
        {
            return 0;
        }

        var chain = await ReadChainRowsAsync(serverId, databaseName, cancellationToken);
        var rebuild = chain.Rows.Count == 0 || chain.Versions.Any(v => v < ObjectUsageChainVersion.Current);
        var stepper = new ObjectUsageStepper();
        var open = new Dictionary<(int ObjectId, int IndexId), RunIdentity>();
        DateTime? watermark = null;

        if (rebuild)
        {
            await DeletePairAsync(serverId, databaseName, cancellationToken);
        }
        else
        {
            watermark = chain.Rows[^1].Time;
            await RestoreAsync(serverId, databaseName, chain.Rows, stepper, cancellationToken);
            using var readLock = _duckDb.AcquireReadLock(cancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);
            using var command = connection.CreateCommand();
            command.CommandText = OpenRunsSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = databaseName });
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                open[(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture), Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture))] =
                    new RunIdentity(reader.GetString(2), reader.GetString(3), reader.GetBoolean(4), reader.IsDBNull(5) ? null : reader.GetBoolean(5));
            }
        }

        var times = await ReadTimesAsync(serverId, databaseName, watermark, _trimFloor, cancellationToken);

        /* #9596: only a snapshot whose collection run has finished. The collector appends a database's rows through one appender with no explicit
           transaction, which may flush part of a snapshot before the rest lands, and a snapshot folded short would never be folded again. A run's
           collection_log row is written when it ends and is stamped with its start, a little before the rows' own time, so a snapshot is whole when
           some row of the log ends at or after it: its own, or a later run's when its own was never written (a crash). A server with no log row
           for the collector has no whole snapshot yet. */
        var horizon = await ReadRunHorizonAsync(serverId, cancellationToken);
        times = horizon is { } whole ? times.FindAll(t => t <= whole) : [];
        var steps = 0;
        foreach (var time in times)
        {
            if (steps >= maxSteps || DateTime.UtcNow - passStarted >= MaxPassDuration)
            {
                break;
            }

            cancellationToken.ThrowIfCancellationRequested();
            var rows = await ReadRawAsync(serverId, databaseName, [time], cancellationToken);
            if (rows.Count == 0)
            {
                continue;
            }

            if (!await StepAsync(serverId, databaseName, stepper, open, time, rows, watermark, cancellationToken))
            {
                AppLogger.Info("ObjectUsage", $"The object usage chain of server {serverId} database '{databaseName}' changed under the pass (a size reset cleared it), so the pass stopped on it. The next pass starts again from what the store holds.");
                break;
            }

            watermark = time;
            steps++;
            if (StepCompletedForTests is { } stepCompleted)
            {
                await stepCompleted(time);
            }

            if (await CountChainRowsAsync(cancellationToken) is var chainRows && chainRows.Deltas + chainRows.Other >= CapRows)
            {
                _capHit = (serverId, databaseName);
                break;
            }
        }

        return steps;
    }

    /// <summary>
    /// One snapshot: step the shared stepper, then write the snapshots row, the delta rows and the run changes in one transaction. The stepper and
    /// the open runs live in memory for the whole pair, so the transaction first checks that the pair's stored chain still ends where this pass left
    /// it, <paramref name="storedLast"/>. A size reset between two steps recreates the store with the chain tables empty, and a step written into
    /// that would leave runs missing for every index that stayed open. The check and the writes sit under one read lock, which a reset cannot take.
    /// Returns false, having written nothing, when the chain no longer ends there.
    /// </summary>
    private async Task<bool> StepAsync(
        int serverId, string databaseName, ObjectUsageStepper stepper, Dictionary<(int ObjectId, int IndexId), RunIdentity> open, DateTime time,
        List<ObjectUsageSample> rows, DateTime? storedLast, CancellationToken cancellationToken)
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

        using (_duckDb.AcquireReadLock(cancellationToken))
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(cancellationToken);
            using var transaction = connection.BeginTransaction();
            try
            {
                using (var check = connection.CreateCommand())
                {
                    check.CommandText = "SELECT max(collection_time) FROM object_usage_snapshots WHERE server_id = $1 AND database_name = $2";
                    check.Parameters.Add(new DuckDBParameter { Value = serverId });
                    check.Parameters.Add(new DuckDBParameter { Value = databaseName });
                    var stored = await check.ExecuteScalarAsync(cancellationToken) as DateTime?;
                    if (stored != storedLast)
                    {
                        transaction.Rollback();
                        return false;
                    }
                }

                using (var insert = connection.CreateCommand())
                {
                    insert.CommandText = InsertSnapshotSql;
                    void P(object? value) => insert.Parameters.Add(new DuckDBParameter { Value = value ?? DBNull.Value });
                    P(serverId); P(databaseName); P(Naive(time)); P((short)ObjectUsageChainVersion.Current); P(node.Server); P(Db(node.Start));
                    P(node.NodeKey); P(Db(node.ComparedWith)); P(Db(node.KeptStart)); P((int)step.Reasons); P((short)step.Counted); P(step.IndexResets);
                    P(Db(step.BlindFrom)); P(Naive(DateTime.UtcNow));
                    await insert.ExecuteNonQueryAsync(cancellationToken);
                }

                if (deltas.Count > 0)
                {
                    using var appender = connection.CreateAppender("object_usage_deltas");
                    foreach (var delta in deltas)
                    {
                        var row = appender.CreateRow();
                        row.AppendValue(serverId).AppendValue(databaseName).AppendValue(Naive(time)).AppendValue(delta.ObjectId).AppendValue(delta.IndexId)
                            .AppendValue(delta.Reads).AppendValue(delta.Updates);
                        AppendTime(row, delta.Now.LastRead);
                        AppendTime(row, delta.Now.LastUpdate);
                        row.EndRow();
                    }
                }

                if (closing.Count > 0)
                {
                    /* An open run was present through the snapshot before this one. The keys go in through an appender and one statement ends
                       every run, however many indexes the snapshot changed. */
                    using (var stage = connection.CreateCommand())
                    {
                        stage.CommandText = StageClosingSql;
                        await stage.ExecuteNonQueryAsync(cancellationToken);
                    }

                    using (var appender = connection.CreateAppender(ClosingTable))
                    {
                        foreach (var key in closing)
                        {
                            appender.CreateRow().AppendValue(key.ObjectId).AppendValue(key.IndexId).EndRow();
                        }
                    }

                    using (var close = connection.CreateCommand())
                    {
                        close.CommandText = CloseRunsSql;
                        close.Parameters.Add(new DuckDBParameter { Value = serverId });
                        close.Parameters.Add(new DuckDBParameter { Value = databaseName });
                        close.Parameters.Add(new DuckDBParameter { Value = Naive(previousTime ?? time) });
                        await close.ExecuteNonQueryAsync(cancellationToken);
                    }

                    using (var drop = connection.CreateCommand())
                    {
                        drop.CommandText = "DROP TABLE " + ClosingTable;
                        await drop.ExecuteNonQueryAsync(cancellationToken);
                    }
                }

                if (opening.Count > 0)
                {
                    using var appender = connection.CreateAppender("object_usage_runs");
                    foreach (var (key, identity) in opening)
                    {
                        var row = appender.CreateRow();
                        row.AppendValue(serverId).AppendValue(databaseName).AppendValue(key.ObjectId).AppendValue(key.IndexId).AppendValue(Naive(time))
                            .AppendNullValue().AppendValue(identity.SchemaName).AppendValue(identity.ObjectName).AppendValue(identity.IsIndexedView);
                        if (identity.IsMemoryOptimized is { } memory)
                        {
                            row.AppendValue(memory);
                        }
                        else
                        {
                            row.AppendNullValue();
                        }

                        row.EndRow();
                    }
                }

                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }

        foreach (var key in closing)
        {
            open.Remove(key);
        }

        foreach (var (key, identity) in opening)
        {
            open[key] = identity;
        }

        return true;
    }

    private static void AppendTime(IDuckDBAppenderRow row, DateTime? value)
    {
        if (value is { } time)
        {
            row.AppendValue(Naive(time));
        }
        else
        {
            row.AppendNullValue();
        }
    }

    private async Task<(List<ChainRow> Rows, List<short> Versions)> ReadChainRowsAsync(int serverId, string databaseName, CancellationToken cancellationToken)
    {
        var rows = new List<ChainRow>();
        var versions = new List<short>();
        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = ChainRowsSql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            versions.Add(Convert.ToInt16(reader.GetValue(1), CultureInfo.InvariantCulture));
            rows.Add(new ChainRow(
                reader.GetDateTime(0), reader.IsDBNull(2) ? null : reader.GetString(2), reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetDateTime(4)));
        }

        return (rows, versions);
    }

    /// <summary>
    /// The snapshot times a pair has left to build, oldest first: those after <paramref name="watermark"/> when it has a chain, else those from
    /// <paramref name="floor"/> on when a trim has set one, else all of them.
    /// </summary>
    private async Task<List<DateTime>> ReadTimesAsync(int serverId, string databaseName, DateTime? watermark, DateTime? floor, CancellationToken cancellationToken)
    {
        var times = new List<DateTime>();
        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = watermark is not null ? TimesAfterSql : floor is not null ? TimesFromSql : TimesAllSql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        if ((watermark ?? floor) is { } bound)
        {
            command.Parameters.Add(new DuckDBParameter { Value = Naive(bound) });
        }

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            times.Add(reader.GetDateTime(0));
        }

        return times;
    }

    /// <summary>
    /// The end of the newest <c>index_object_stats</c> collection run of a server that the log knows, plus a second of slack, or null when it has none.
    /// The hot <c>collection_log</c> answers when it holds a run of the server: archival moves only rows older than its cutoff, so the newest run is
    /// among the hot rows whenever any is hot, and the archived months are not read. <c>v_collection_log</c> answers only when the hot table holds
    /// none, as just after the size reset.
    /// </summary>
    private async Task<DateTime?> ReadRunHorizonAsync(int serverId, CancellationToken cancellationToken)
    {
        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        foreach (var relation in new[] { "collection_log", "v_collection_log" })
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"""
                SELECT max(collection_time + to_milliseconds(COALESCE(duration_ms, 0) + 1000))
                FROM {relation}
                WHERE server_id = $1
                AND   collector_name = 'index_object_stats'
                """;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            if (await command.ExecuteScalarAsync(cancellationToken) is DateTime horizon)
            {
                return horizon;
            }
        }

        return null;
    }

    /// <summary>
    /// Puts the stepper where the chain's last snapshot left it. The fold's state is each node's last snapshot, its kept start and that
    /// snapshot's deduped counters, the node and the carried server name of the snapshot before, and the first snapshot (for the node baseline's
    /// start rule). All but the counters come from the chain rows, and the counters from the raw rows of each node's last snapshot. An unnamed
    /// node exists in the fold only until a name takes it over, so it is restored only while no snapshot has had a name.
    /// </summary>
    private async Task RestoreAsync(int serverId, string databaseName, List<ChainRow> chain, ObjectUsageStepper stepper, CancellationToken cancellationToken)
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

        var raw = await ReadRawAsync(serverId, databaseName, last.Values.Select(r => r.Time).Distinct().ToList(), cancellationToken);
        var byTime = raw.GroupBy(r => r.CollectionTime).ToDictionary(g => g.Key, g => ObjectUsageCounterStep.Dedupe(g.ToList()));

        var nodes = last.Select(kv => (
            kv.Key,
            kv.Value.Time,
            kv.Value.KeptStart,
            byTime.TryGetValue(kv.Value.Time, out var counters) ? counters : new Dictionary<(int ObjectId, int IndexId), ObjectUsageCounters>()));
        stepper.Nodes.Restore(nodes, chain[^1].NodeKey, server, chain[0].Time, chain[^1].Time);
    }

    /// <summary>The raw rows of the named snapshots of one database, in <see cref="ObjectUsageWindowRead.Columns"/> order, the rows of one snapshot in collection order.</summary>
    private async Task<List<ObjectUsageSample>> ReadRawAsync(int serverId, string databaseName, List<DateTime> times, CancellationToken cancellationToken)
    {
        var rows = new List<ObjectUsageSample>();
        if (times.Count == 0)
        {
            return rows;
        }

        var sql = new StringBuilder("SELECT ").Append(ObjectUsageWindowRead.Columns)
            .Append("\nFROM v_index_object_stats AS s\nWHERE s.server_id = $1\nAND   s.database_name = $2\nAND   s.collection_time IN (");
        for (var i = 0; i < times.Count; i++)
        {
            sql.Append(i == 0 ? "$" : ", $").Append((i + 3).ToString(CultureInfo.InvariantCulture));
        }

        sql.Append(")\nORDER BY\n    s.collection_time,\n    s.collection_id");

        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = sql.ToString();
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        foreach (var time in times)
        {
            command.Parameters.Add(new DuckDBParameter { Value = Naive(time) });
        }

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ObjectUsageWindowRead.Map(reader));
        }

        return rows;
    }

    private async Task DeletePairAsync(int serverId, string databaseName, CancellationToken cancellationToken)
    {
        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        using var transaction = connection.BeginTransaction();
        try
        {
            foreach (var statement in DeletePairSql)
            {
                using var command = connection.CreateCommand();
                command.CommandText = statement;
                command.Parameters.Add(new DuckDBParameter { Value = serverId });
                command.Parameters.Add(new DuckDBParameter { Value = databaseName });
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    private static void AddNote(List<string> notes, string? note)
    {
        if (note is not null)
        {
            notes.Add(note);
        }
    }

    /// <summary>The delta rows, and the snapshot and run rows, of the chain. One metadata count per table.</summary>
    private async Task<(long Deltas, long Other)> CountChainRowsAsync(CancellationToken cancellationToken)
    {
        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                (SELECT count(*) FROM object_usage_deltas),
                (SELECT count(*) FROM object_usage_snapshots) + (SELECT count(*) FROM object_usage_runs)
            """;
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return (Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture), Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// Keeps the three chain tables under <see cref="CapRows"/>. Over it, the oldest days of snapshots go through <see cref="PurgeAsync"/> with a later
    /// cutoff until the rest is under <see cref="TrimTargetShare"/> of the cap, so the newest windows keep the chain and a longer window folds (the
    /// reader needs the chain's facts at the window's baseline). If the newest <see cref="MinKeptDays"/> days do not fit, one database's chain is
    /// dropped and the check runs again: the database being extended when the cap was reached, else the one whose newest days hold the most rows.
    /// So only the databases that do not fit lose their chain, and the rest keep theirs, trimmed when they still need it. A dropped database waits in
    /// <see cref="_stopped"/> until it fits. A trim's cutoff becomes <see cref="_trimFloor"/>, where a database with no chain starts its own.
    /// Returns the sentence for the pass's one warning, or null when the chain was under the cap.
    /// </summary>
    private async Task<string?> EnforceCapAsync(CancellationToken cancellationToken)
    {
        var capRows = CapRows;
        var preferred = _capHit;
        var dropped = new List<(int ServerId, string DatabaseName)>();
        DateTime? trimmedFrom = null;
        while (true)
        {
            var (deltas, other) = await CountChainRowsAsync(cancellationToken);
            if (deltas + other <= capRows)
            {
                break;
            }

            var days = await ReadDeltaDaysAsync(cancellationToken);
            var budget = (long)(capRows * TrimTargetShare) - other;
            long kept = 0;
            DateTime? cutoff = null;
            foreach (var (day, rows) in days)
            {
                if (kept + rows > budget)
                {
                    break;
                }

                kept += rows;
                cutoff = day;
            }

            if (cutoff is { } from && days.Count > 0 && (days[0].Day - from).TotalDays >= MinKeptDays - 1)
            {
                await PurgeAsync(from, cancellationToken);
                trimmedFrom = from;
                _trimFloor = from;
                break;
            }

            var spans = await ReadMinimumSpanRowsAsync(cancellationToken);
            if (spans.Count == 0)
            {
                break;
            }

            var victim = spans.FindIndex(s => preferred is { } hit && s.Pair == hit) is var at and >= 0 ? spans[at] : spans[0];
            preferred = null;
            await DeletePairAsync(victim.Pair.ServerId, victim.Pair.DatabaseName, cancellationToken);
            _stopped[victim.Pair] = Math.Max(victim.Rows, _stopped.GetValueOrDefault(victim.Pair));
            dropped.Add(victim.Pair);
        }

        if (trimmedFrom is null && dropped.Count == 0)
        {
            return null;
        }

        var note = new StringBuilder(
            $"The object usage chain is over its cap of {capRows * EstimatedBytesPerRow / 1048576.0:F1} MB ({PercentText.Of(CapShareOfResetPoint)} of the hot store reset point, hot_store_reset_mb).");
        if (dropped.Count > 0)
        {
            note.Append(CultureInfo.InvariantCulture,
                $" The newest {MinKeptDays} days of {dropped.Count} database(s) do not fit beside the others, so their chain was dropped and is rebuilt once it fits. Their reads fold.");
        }

        if (trimmedFrom is { } trimmed)
        {
            note.Append(CultureInfo.InvariantCulture, $" Snapshots before {trimmed:yyyy-MM-dd} were trimmed, so longer windows fold.");
        }

        return note.ToString();
    }

    /// <summary>The delta rows of the chain by day, newest day first.</summary>
    private async Task<List<(DateTime Day, long Rows)>> ReadDeltaDaysAsync(CancellationToken cancellationToken)
    {
        var days = new List<(DateTime Day, long Rows)>();
        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                CAST(CAST(collection_time AS DATE) AS TIMESTAMP),
                count(*)
            FROM object_usage_deltas
            GROUP BY CAST(collection_time AS DATE)
            ORDER BY 1 DESC
            """;
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            days.Add((reader.GetDateTime(0), Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture)));
        }

        return days;
    }

    /// <summary>
    /// Each database with a chain and the rows a trim to its newest <see cref="MinKeptDays"/> days would leave it: the delta and snapshot rows of those
    /// days and all its runs. The most rows first. $1 is <see cref="MinKeptDays"/> less one, the days before the newest day that the span holds.
    /// </summary>
    private async Task<List<((int ServerId, string DatabaseName) Pair, long Rows)>> ReadMinimumSpanRowsAsync(CancellationToken cancellationToken)
    {
        var spans = new List<((int, string), long)>();
        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = """
            WITH newest AS
            (
                SELECT
                    server_id,
                    database_name,
                    CAST(CAST(max(collection_time) AS DATE) AS TIMESTAMP) - to_days(CAST($1 AS INTEGER)) AS since
                FROM object_usage_snapshots
                GROUP BY
                    server_id,
                    database_name
            )
            SELECT
                n.server_id,
                n.database_name,
                (
                    SELECT count(*)
                    FROM object_usage_deltas AS d
                    WHERE d.server_id = n.server_id
                    AND   d.database_name = n.database_name
                    AND   d.collection_time >= n.since
                )
              + (
                    SELECT count(*)
                    FROM object_usage_snapshots AS s
                    WHERE s.server_id = n.server_id
                    AND   s.database_name = n.database_name
                    AND   s.collection_time >= n.since
                )
              + (
                    SELECT count(*)
                    FROM object_usage_runs AS r
                    WHERE r.server_id = n.server_id
                    AND   r.database_name = n.database_name
                ) AS span_rows
            FROM newest AS n
            ORDER BY
                span_rows DESC,
                n.server_id,
                n.database_name
            """;
        command.Parameters.Add(new DuckDBParameter { Value = MinKeptDays - 1 });
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            spans.Add(((Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture), reader.GetString(1)),
                Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture)));
        }

        return spans;
    }

    /// <summary>
    /// Deletes the chain rows older than <paramref name="cutoff"/>: snapshots and deltas by their time, closed runs by the time they ended, and
    /// the open runs of a database that has no snapshot row left. Cheap statements over small tables. A failure is logged, never thrown: the
    /// next pass tries again.
    /// </summary>
    internal async Task PurgeAsync(DateTime cutoff, CancellationToken cancellationToken)
    {
        string[] statements =
        [
            "DELETE FROM object_usage_deltas WHERE collection_time < $1",
            "DELETE FROM object_usage_snapshots WHERE collection_time < $1",
            "DELETE FROM object_usage_runs WHERE last_time IS NOT NULL AND last_time < $1",
            """
            DELETE FROM object_usage_runs
            WHERE last_time IS NULL
            AND   NOT EXISTS
                  (
                      SELECT 1
                      FROM object_usage_snapshots AS s
                      WHERE s.server_id = object_usage_runs.server_id
                      AND   s.database_name = object_usage_runs.database_name
                  )
            """,
        ];

        try
        {
            using var readLock = _duckDb.AcquireReadLock(cancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);
            foreach (var statement in statements)
            {
                using var command = connection.CreateCommand();
                command.CommandText = statement;
                if (statement.Contains("$1", StringComparison.Ordinal))
                {
                    command.Parameters.Add(new DuckDBParameter { Value = Naive(cutoff) });
                }

                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            AppLogger.Debug("ObjectUsage", $"The object usage chain purge did not run: {ex.Message}");
        }
    }

    private static object Db(DateTime? value) => value is { } time ? Naive(time) : DBNull.Value;

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);
}
