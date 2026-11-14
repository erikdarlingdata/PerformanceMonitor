/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The TimescaleDB background jobs' CRASH counters (#8787), read live. TimescaleDB counts a crash (the job's process died or
/// was killed) apart from a failure (the job returned an error), and a crashed job backs off by its retry period times the
/// number of consecutive crashes, so a job that crashed several times in a row can sit hours from its next start while
/// <c>total_failures</c> still reads zero. <c>get_store_metrics</c> shows these beside each job's failure counts, and
/// <see cref="DarlingStoreMaintenanceReadinessReader"/> treats a crashing job as a reason not to start maintenance.
///
/// <para><b>Where the counters live.</b> <c>_timescaledb_internal.bgw_job_stat</c> on the TimescaleDB versions this product ships
/// with (2.30.1 at the time of writing), joined to <c>timescaledb_information.jobs</c> for the job's name. A version or a role
/// that cannot read it gives <see cref="StoreJobCrashRead.Available"/> false and a one-sentence
/// <see cref="StoreJobCrashRead.UnavailableReason"/>, never an exception: the caller says "not available" and carries on. A store
/// without the TimescaleDB extension has no jobs, so it reads as available with no rows.</para>
/// </summary>
public static class DarlingStoreJobCrashReader
{
    /// <summary>The read. One row per job TimescaleDB has run at least once (a job that never ran has no stat row).</summary>
    public const string Sql = @"
SELECT s.job_id, j.proc_name, j.hypertable_name, s.consecutive_crashes, s.total_crashes, s.consecutive_failures,
       CASE WHEN pg_catalog.isfinite(s.next_start) THEN s.next_start END AS next_start,
       (s.next_start = '-infinity' AND s.consecutive_crashes > 0) AS crash_backoff
FROM _timescaledb_internal.bgw_job_stat AS s
JOIN timescaledb_information.jobs AS j USING (job_id)
ORDER BY s.consecutive_crashes DESC, s.total_crashes DESC, s.job_id";

    /// <summary>One job's crash counters (<see cref="Sql"/>).</summary>
    /// <param name="JobId">TimescaleDB's job id.</param>
    /// <param name="ProcName">The job's procedure, for example <c>policy_compression</c>.</param>
    /// <param name="HypertableName">The hypertable or continuous aggregate the job serves, null for a job with none.</param>
    /// <param name="ConsecutiveCrashes">Crashes since the job last finished a run. Above zero the job is backing off.</param>
    /// <param name="TotalCrashes">Crashes over the job's life.</param>
    /// <param name="ConsecutiveFailures">Failures (error returns) since the job last succeeded.</param>
    /// <param name="NextStart">When TimescaleDB will start the job next (UTC). Null when none is scheduled, and null for an infinite
    /// value, which Npgsql would otherwise turn into the year 0001.</param>
    /// <param name="CrashBackoff">True when <c>next_start</c> is <c>-infinity</c> and the job has crashed since it last finished a run.
    /// That is a crash backoff: the scheduler sets the next start itself, so the job is not lost.</param>
    public sealed record StoreJobCrash(
        int JobId, string ProcName, string? HypertableName, int ConsecutiveCrashes, long TotalCrashes, int ConsecutiveFailures, DateTime? NextStart, bool CrashBackoff);

    /// <summary>The result of <see cref="ReadAsync"/>.</summary>
    /// <param name="Available">True when the counters were read (a store without TimescaleDB reads as available with no jobs).</param>
    /// <param name="UnavailableReason">Why they could not be read, one sentence with the SQLSTATE, null when <paramref name="Available"/>.</param>
    /// <param name="Jobs">One row per job, most crashes first. Empty when not available.</param>
    public sealed record StoreJobCrashRead(bool Available, string? UnavailableReason, IReadOnlyList<StoreJobCrash> Jobs)
    {
        /// <summary>The jobs whose consecutive crash count is above zero.</summary>
        public IEnumerable<StoreJobCrash> Crashing
        {
            get
            {
                foreach (var job in Jobs)
                {
                    if (job.ConsecutiveCrashes > 0)
                    {
                        yield return job;
                    }
                }
            }
        }
    }

    /// <summary>Reads the counters. Never throws for a missing view, a missing column or a refused role: it says why in the result.</summary>
    public static async Task<StoreJobCrashRead> ReadAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        try
        {
            await using (var probe = postgres.CreateCommand(DarlingStoreTableHealthReader.TimescaleProbeSql))
            {
                probe.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
                if (!(bool)(await probe.ExecuteScalarAsync(cancellationToken))!)
                {
                    return new StoreJobCrashRead(true, null, Array.Empty<StoreJobCrash>());
                }
            }

            var jobs = new List<StoreJobCrash>();
            await using var command = postgres.CreateCommand(Sql);
            command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                jobs.Add(new StoreJobCrash(
                    JobId: reader.GetInt32(0),
                    ProcName: reader.GetString(1),
                    HypertableName: reader.IsDBNull(2) ? null : reader.GetString(2),
                    ConsecutiveCrashes: reader.GetInt32(3),
                    TotalCrashes: reader.GetInt64(4),
                    ConsecutiveFailures: reader.GetInt32(5),
                    NextStart: reader.IsDBNull(6) ? null : DateTime.SpecifyKind(reader.GetDateTime(6), DateTimeKind.Utc),
                    CrashBackoff: reader.GetBoolean(7)));
            }

            return new StoreJobCrashRead(true, null, jobs);
        }
        catch (PostgresException ex) when (ex.SqlState is "42P01" or "42703" or "42501" or "3F000" or "42883")
        {
            return new StoreJobCrashRead(
                false,
                string.Create(CultureInfo.InvariantCulture,
                    $"not available: this TimescaleDB version or this store role does not expose the job crash counters (SQLSTATE {ex.SqlState})"),
                Array.Empty<StoreJobCrash>());
        }
    }
}
