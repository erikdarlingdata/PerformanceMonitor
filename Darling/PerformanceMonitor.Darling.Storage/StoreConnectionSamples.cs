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
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>What the store's host offered at one sample. Any figure the host could not give is null.</summary>
/// <param name="CpuPercent">Host CPU use, percent.</param>
/// <param name="AvailableMemoryBytes">Memory the host could still hand out, bytes.</param>
/// <param name="DiskQueueLength">The data volume's disk queue length (or await), as the host reports it.</param>
public readonly record struct StoreHostLoad(double? CpuPercent, long? AvailableMemoryBytes, double? DiskQueueLength);

/// <summary>
/// The store's own connection pressure, once a minute (#9437). When a client of the store timed out opening a connection, nothing
/// afterwards said whether the pool was full, the slots were taken, or the host stalled. Two plain tables answer it:
/// <c>collect.store_connection_samples</c> holds one row per (role, application name, state, wait event type) with the count of client
/// backends, and <c>collect.store_connection_slots</c> holds one row per sample with the slots in use against the slots a normal role
/// may use, plus the host's load when the host reader has it.
///
/// <para><b>One statement.</b> A single statement groups <c>pg_stat_activity</c> once and writes both tables from that one snapshot, so the
/// minute's slot row and its group rows always agree. The sample counts <c>backend_type = 'client backend'</c> only: background workers,
/// autovacuum workers and WAL senders have their own limits and do not use a <c>max_connections</c> slot.</para>
///
/// <para><b>Headroom.</b> <c>slots_available</c> is <c>max_connections</c> less <c>superuser_reserved_connections</c> less
/// <c>reserved_connections</c> (PostgreSQL 16 and later, 0 before) less every client backend counted, the sampling connection included. It is
/// stored raw and can go below zero when superusers are using the reserved slots, which is itself the finding.</para>
///
/// <para><b>Visibility.</b> A role that is not a superuser and not in <c>pg_read_all_stats</c> sees NULL state and wait columns for other
/// roles' backends. The counts stay right, and the rows keep the NULLs, so a read can tell a store the role could not see into.</para>
///
/// <para><b>Host load.</b> The host figures are filled only from what the service already reads for the store's host, and only for a store on
/// the same host. Anything else is NULL, and the read says <c>host_load: not_available</c> once.</para>
/// </summary>
public static class StoreConnectionSamples
{
    /// <summary>The store migration that creates the two tables. One constant so a renumber is mechanical.</summary>
    public const int Version = 314;

    /// <summary>
    /// Days the samples are kept. Short on purpose: the rows are for finding out what a stall looked like, and a minute grain across the
    /// longer horizon of the other store self-metrics would be millions of rows nobody reads. The sample's own pass deletes past it.
    /// </summary>
    public const int RetentionDays = 14;

    /// <summary>The longest one capture may run, seconds. It is one statement over an in-memory view, so this is a ceiling and not an expectation.</summary>
    public const int CaptureTimeoutSeconds = 10;

    /// <summary>
    /// V314 (#9437): the two tables, created empty. No data moves. An older build ignores both.
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.store_connection_samples
(
    sample_time timestamp(6) without time zone NOT NULL,
    role_name text NOT NULL,
    application_name text NOT NULL,
    state text,
    wait_event_type text,
    backends integer NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_store_connection_samples_time
    ON collect.store_connection_samples (sample_time DESC);

CREATE TABLE IF NOT EXISTS collect.store_connection_slots
(
    sample_time timestamp(6) without time zone NOT NULL,
    max_connections integer NOT NULL,
    superuser_reserved_connections integer NOT NULL,
    reserved_connections integer NOT NULL,
    backends_total integer NOT NULL,
    slots_available integer NOT NULL,
    host_cpu_percent double precision,
    host_available_memory_bytes bigint,
    host_disk_queue double precision,
    CONSTRAINT pk_store_connection_slots PRIMARY KEY (sample_time)
);";

    /// <summary>
    /// The sample. $1 sample_time (naive UTC), $2 host CPU percent, $3 host available memory bytes, $4 host disk queue, each NULL when the
    /// host reader has no figure. <c>reserved_connections</c> exists from PostgreSQL 16, and <c>current_setting(name, true)</c> answers NULL
    /// for a setting the server does not have, which the statement reads as 0.
    /// </summary>
    public const string CaptureSql = @"
WITH g AS
(
    SELECT
        coalesce(a.usename, '')           AS role_name,
        coalesce(a.application_name, '')  AS application_name,
        a.state                           AS state,
        a.wait_event_type                 AS wait_event_type,
        count(*)::integer                 AS backends
    FROM pg_stat_activity AS a
    WHERE a.backend_type = 'client backend'
    GROUP BY a.usename, a.application_name, a.state, a.wait_event_type
),
written AS
(
    INSERT INTO collect.store_connection_samples (sample_time, role_name, application_name, state, wait_event_type, backends)
    SELECT $1, g.role_name, g.application_name, g.state, g.wait_event_type, g.backends
    FROM g
    RETURNING 1
)
INSERT INTO collect.store_connection_slots
    (sample_time, max_connections, superuser_reserved_connections, reserved_connections, backends_total, slots_available,
     host_cpu_percent, host_available_memory_bytes, host_disk_queue)
SELECT
    $1,
    s.max_connections,
    s.superuser_reserved,
    s.reserved,
    t.total,
    s.max_connections - s.superuser_reserved - s.reserved - t.total,
    $2::double precision,
    $3::bigint,
    $4::double precision
FROM
(
    SELECT
        current_setting('max_connections')::integer                                    AS max_connections,
        current_setting('superuser_reserved_connections')::integer                     AS superuser_reserved,
        coalesce(current_setting('reserved_connections', true), '0')::integer          AS reserved
) AS s
CROSS JOIN (SELECT coalesce(sum(g.backends), 0)::integer AS total FROM g) AS t";

    /// <summary>The retention delete for the group rows. $1 the cutoff, naive UTC.</summary>
    public const string PurgeSamplesSql = "DELETE FROM collect.store_connection_samples WHERE sample_time < $1";

    /// <summary>The retention delete for the slot rows. $1 the cutoff, naive UTC.</summary>
    public const string PurgeSlotsSql = "DELETE FROM collect.store_connection_slots WHERE sample_time < $1";

    /// <summary>
    /// Takes one sample and prunes past <see cref="RetentionDays"/>. Throws on a failed statement: the caller owns the failure policy, which for the
    /// service is a skipped minute and a Debug line. <paramref name="hostLoad"/> is null for a store whose host the service does not read.
    /// </summary>
    public static async Task CaptureAsync(
        NpgsqlConnection connection,
        DateTime sampleTime,
        StoreHostLoad? hostLoad,
        CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var stamp = DateTime.SpecifyKind(sampleTime, DateTimeKind.Unspecified);

        await using (var capture = new NpgsqlCommand(CaptureSql, connection) { CommandTimeout = CaptureTimeoutSeconds })
        {
            capture.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = stamp });
            capture.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Double, Value = hostLoad?.CpuPercent is double cpu ? cpu : DBNull.Value });
            capture.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = hostLoad?.AvailableMemoryBytes is long mem ? mem : DBNull.Value });
            capture.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Double, Value = hostLoad?.DiskQueueLength is double queue ? queue : DBNull.Value });
            await capture.ExecuteNonQueryAsync(cancellationToken);
        }

        var cutoff = stamp.AddDays(-RetentionDays);
        foreach (var purgeSql in new[] { PurgeSamplesSql, PurgeSlotsSql })
        {
            await using var purge = new NpgsqlCommand(purgeSql, connection) { CommandTimeout = CaptureTimeoutSeconds };
            purge.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = cutoff });
            await purge.ExecuteNonQueryAsync(cancellationToken);
        }

    }
}
