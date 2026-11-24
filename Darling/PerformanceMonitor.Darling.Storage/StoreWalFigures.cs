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
/// The store's own WAL figures (#9419): one hourly row per sweep (<c>object_kind = 'wal'</c> on <c>collect.store_metrics</c>)
/// carrying <c>pg_stat_wal</c>'s CUMULATIVE counters and the size and file count of <c>pg_wal</c>. A checkpoint settings trial on a
/// store needed the WAL bytes and the <c>pg_wal</c> size per hour and read them by hand.
///
/// <para><b>Why columns and not more rows of the checkpointer's shape.</b> The checkpointer row fits its three counters in columns
/// the table already had. The WAL row needs eleven figures and a reset stamp, and no existing column means any of them. Borrowing
/// <c>total_bytes</c> and the row count column would also put the row into the inventory's byte sums. So V305 adds eleven nullable
/// columns, and the row leaves <c>total_bytes</c> empty like the checkpointer's.</para>
///
/// <para><b>Raw counters, the read takes the difference.</b> Same rule as the checkpointer row (<see cref="StoreSelfMetrics.CheckpointerInsertSql"/>
/// has the reasoning): a service restart costs nothing and the reader sees a reset from the two rows. <c>wal_stats_reset</c> is stored
/// beside the counters, so a reset that is followed by more WAL than the whole earlier lifetime still reads as a reset. The row does
/// not store <c>pg_postmaster_start_time()</c>: a clean restart keeps the counters and adds only WAL that was really written, so
/// nothing in the interval is false. A crash loses the statistics, which moves <c>stats_reset</c> and reads as a reset.</para>
///
/// <para><b>PostgreSQL 14 to 18.</b> <c>pg_stat_wal</c> arrives in 14 with <c>wal_buffers_full</c>. In 18 the write and sync counts
/// and times leave it (they moved to <c>pg_stat_io</c>), so the 18 statement stores NULL in those four columns. Below 14 there is no
/// view and no row is written.</para>
///
/// <para><b>The <c>pg_wal</c> size</b> comes from <c>pg_ls_waldir()</c>, which needs <c>pg_monitor</c> or an explicit EXECUTE grant. A
/// role without it gets the row with <c>wal_dir_bytes</c> and <c>wal_dir_files</c> NULL, and the read answers that part with a
/// <c>precondition</c> and the grant. It is never stored as zero.</para>
/// </summary>
public static class StoreWalFigures
{
    /// <summary>The store migration that adds the WAL columns. One constant so a renumber is mechanical.</summary>
    public const int Version = 305;

    /// <summary>The <c>object_kind</c> of the WAL row. A const because the reader filters on it.</summary>
    public const string WalObjectKind = "wal";

    /// <summary>The <c>object_name</c> of the one WAL row.</summary>
    public const string WalObjectName = "pg_stat_wal";

    /// <summary>The first PostgreSQL major with <c>pg_stat_wal</c> (14).</summary>
    public const int FirstMajorWithView = 14;

    /// <summary>The first PostgreSQL major whose <c>pg_stat_wal</c> no longer has the write and sync counts and times (18).</summary>
    public const int FirstMajorWithoutWriteSync = 18;

    /// <summary>PostgreSQL's SQLSTATE for a missing privilege, what <c>pg_ls_waldir()</c> raises for a role that lacks the grant.</summary>
    public const string InsufficientPrivilege = "42501";

    /// <summary>
    /// V305 (#9419): the WAL columns on <c>collect.store_metrics</c>, all nullable and filled on the <c>wal</c> row only. Plain table with no
    /// passthrough view, so the ALTER stands alone, as V137 and V156 did. Existing rows keep NULL.
    /// </summary>
    public const string AddColumnsSql = @"
ALTER TABLE collect.store_metrics
    ADD COLUMN IF NOT EXISTS wal_records bigint,
    ADD COLUMN IF NOT EXISTS wal_fpi bigint,
    ADD COLUMN IF NOT EXISTS wal_bytes bigint,
    ADD COLUMN IF NOT EXISTS wal_buffers_full bigint,
    ADD COLUMN IF NOT EXISTS wal_write_count bigint,
    ADD COLUMN IF NOT EXISTS wal_sync_count bigint,
    ADD COLUMN IF NOT EXISTS wal_write_ms bigint,
    ADD COLUMN IF NOT EXISTS wal_sync_ms bigint,
    ADD COLUMN IF NOT EXISTS wal_stats_reset timestamp,
    ADD COLUMN IF NOT EXISTS wal_dir_bytes bigint,
    ADD COLUMN IF NOT EXISTS wal_dir_files bigint;";

    /// <summary>The size and file count of <c>pg_wal</c>. Needs <c>pg_monitor</c> or EXECUTE on <c>pg_ls_waldir()</c>.</summary>
    public const string WalDirSql = "SELECT coalesce(sum(size), 0)::bigint, count(*)::bigint FROM pg_ls_waldir()";

    /// <summary>
    /// The WAL row on PostgreSQL 14 to 17. $1 metric_time, $2 the <c>pg_wal</c> bytes (or NULL), $3 its file count (or NULL).
    /// <c>stats_reset</c> is <c>timestamptz</c> and is stored as naive UTC. <c>wal_write_time</c> and <c>wal_sync_time</c> are
    /// <c>double precision</c> milliseconds that stay zero unless <c>track_wal_io_timing</c> is on.
    /// </summary>
    public const string InsertSql = $@"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, wal_records, wal_fpi, wal_bytes, wal_buffers_full, wal_write_count, wal_sync_count, wal_write_ms, wal_sync_ms, wal_stats_reset, wal_dir_bytes, wal_dir_files)
SELECT
    $1,
    '{WalObjectName}',
    '{WalObjectKind}',
    w.wal_records,
    w.wal_fpi,
    w.wal_bytes::bigint,
    w.wal_buffers_full,
    w.wal_write,
    w.wal_sync,
    round(w.wal_write_time)::bigint,
    round(w.wal_sync_time)::bigint,
    w.stats_reset AT TIME ZONE 'UTC',
    $2::bigint,
    $3::bigint
FROM pg_stat_wal AS w";

    /// <summary><see cref="InsertSql"/> for PostgreSQL 18 and later, where the write and sync figures are gone from the view.</summary>
    public const string InsertWithoutWriteSyncSql = $@"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, wal_records, wal_fpi, wal_bytes, wal_buffers_full, wal_stats_reset, wal_dir_bytes, wal_dir_files)
SELECT
    $1,
    '{WalObjectName}',
    '{WalObjectKind}',
    w.wal_records,
    w.wal_fpi,
    w.wal_bytes::bigint,
    w.wal_buffers_full,
    w.stats_reset AT TIME ZONE 'UTC',
    $2::bigint,
    $3::bigint
FROM pg_stat_wal AS w";

    private static int _privilegeLogged;

    /// <summary>
    /// Writes the WAL row for one sweep and returns the rows written (0 or 1). A failure is logged and never thrown, so a store whose
    /// role cannot read what this needs never fails the sweep. A missing <c>pg_ls_waldir()</c> grant is logged once per process,
    /// and the row is written without the directory figures.
    /// </summary>
    public static async Task<int> CaptureAsync(
        NpgsqlConnection connection,
        DateTime metricTime,
        ILogger? logger,
        CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var major = connection.PostgreSqlVersion.Major;
        if (major < FirstMajorWithView)
        {
            return 0;
        }

        try
        {
            long? dirBytes = null;
            long? dirFiles = null;
            try
            {
                using var dir = new NpgsqlCommand(WalDirSql, connection) { CommandTimeout = StoreSelfMetrics.SweepTimeoutSeconds };
                using var reader = await dir.ExecuteReaderAsync(cancellationToken);
                if (await reader.ReadAsync(cancellationToken))
                {
                    dirBytes = reader.GetInt64(0);
                    dirFiles = reader.GetInt64(1);
                }
            }
            catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilege)
            {
                if (System.Threading.Interlocked.Exchange(ref _privilegeLogged, 1) == 0)
                {
                    logger?.LogWarning("The store's role cannot read pg_wal's size (pg_ls_waldir): the WAL figures are recorded without it. GRANT pg_monitor TO the role the service connects to the store as, or GRANT EXECUTE ON FUNCTION pg_ls_waldir() TO it.");
                }
            }

            using var insert = new NpgsqlCommand(major >= FirstMajorWithoutWriteSync ? InsertWithoutWriteSyncSql : InsertSql, connection)
            {
                CommandTimeout = StoreSelfMetrics.SweepTimeoutSeconds,
            };
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(metricTime, DateTimeKind.Unspecified));
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = dirBytes is long b ? b : DBNull.Value });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = dirFiles is long f ? f : DBNull.Value });
            return await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(ex, "Recording the store's WAL figures failed; the sweep goes on without them");
            return 0;
        }
    }
}
