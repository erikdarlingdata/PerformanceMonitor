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
       (s.next_start = '-infinity' AND s.consecutive_crashes > 0) AS crash_backoff,
       COALESCE(js.job_status = 'Running', false) AS running
FROM _timescaledb_internal.bgw_job_stat AS s
JOIN timescaledb_information.jobs AS j USING (job_id)
LEFT JOIN timescaledb_information.job_stats AS js ON js.job_id = s.job_id";

    /// <summary>
    /// The same read without the running flag (#9391), for a store whose <c>job_stats</c> view cannot be read. Every job then reads as
    /// not running, so a job in the middle of a run shows the one provisional crash TimescaleDB adds when a run starts.
    /// </summary>
    public const string SqlWithoutRunning = @"
SELECT s.job_id, j.proc_name, j.hypertable_name, s.consecutive_crashes, s.total_crashes, s.consecutive_failures,
       CASE WHEN pg_catalog.isfinite(s.next_start) THEN s.next_start END AS next_start,
       (s.next_start = '-infinity' AND s.consecutive_crashes > 0) AS crash_backoff,
       false AS running
FROM _timescaledb_internal.bgw_job_stat AS s
JOIN timescaledb_information.jobs AS j USING (job_id)";

    /// <summary>What a read says when it could not tell which jobs are running (#9391).</summary>
    public const string RunningUnknownNote =
        "could not tell which jobs are running, so a job in the middle of a run may show one crash that is not real";

    /// <summary>One job's crash counters (<see cref="Sql"/>).</summary>
    /// <param name="JobId">TimescaleDB's job id.</param>
    /// <param name="ProcName">The job's procedure, for example <c>policy_compression</c>.</param>
    /// <param name="HypertableName">The hypertable or continuous aggregate the job serves, null for a job with none.</param>
    /// <param name="ConsecutiveCrashes">Crashes since the job last finished a run, not counting the provisional crash of a run in progress
    /// (#9391). Above zero the job is backing off.</param>
    /// <param name="TotalCrashes">Crashes over the job's life, not counting the provisional crash of a run in progress.</param>
    /// <param name="ConsecutiveFailures">Failures (error returns) since the job last succeeded.</param>
    /// <param name="NextStart">When TimescaleDB will start the job next (UTC). Null when none is scheduled, and null for an infinite
    /// value, which Npgsql would otherwise turn into the year 0001.</param>
    /// <param name="CrashBackoff">True when <c>next_start</c> is <c>-infinity</c> and the job has crashed since it last finished a run.
    /// That is a crash backoff: the scheduler sets the next start itself, so the job is not lost. Always false while the job runs.</param>
    /// <param name="Running">True when the job has a live backend (#9391). TimescaleDB counts a crash when a run starts and takes it back
    /// when the run finishes cleanly, so a running job's stored counters are one too high. <see cref="Adjust"/> takes that one out.</param>
    public sealed record StoreJobCrash(
        int JobId, string ProcName, string? HypertableName, int ConsecutiveCrashes, long TotalCrashes, int ConsecutiveFailures, DateTime? NextStart, bool CrashBackoff,
        bool Running = false);

    /// <summary>
    /// The crash counters of a job as TimescaleDB stored them, turned into the real ones (#9391). While a run is in progress the stored
    /// counters include the one provisional crash the run's start added (<c>consecutive_crashes</c> and <c>total_crashes</c> up by one,
    /// <c>next_start</c> set to <c>-infinity</c>), so a running job reads one crash lower, never below zero, and is not in a crash
    /// backoff. A job that died mid-run has no live backend, reads as not running, and keeps its crash.
    /// </summary>
    public static StoreJobCrash Adjust(StoreJobCrash stored, bool running)
        => running
            ? stored with
            {
                ConsecutiveCrashes = Math.Max(0, stored.ConsecutiveCrashes - 1),
                TotalCrashes = Math.Max(0L, stored.TotalCrashes - 1),
                CrashBackoff = false,
                Running = true,
            }
            : stored with { Running = false };

    /// <summary>The result of <see cref="ReadAsync"/>.</summary>
    /// <param name="Available">True when the counters were read (a store without TimescaleDB reads as available with no jobs).</param>
    /// <param name="UnavailableReason">Why they could not be read, one sentence with the SQLSTATE, null when <paramref name="Available"/>.</param>
    /// <param name="Jobs">One row per job, most crashes first. Empty when not available.</param>
    /// <param name="RunningNote">Set (to <see cref="RunningUnknownNote"/>) when the counters were read but the running state was not, so
    /// the counts are the stored ones. Null on a normal read.</param>
    public sealed record StoreJobCrashRead(bool Available, string? UnavailableReason, IReadOnlyList<StoreJobCrash> Jobs, string? RunningNote = null)
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

    private static async Task<IReadOnlyList<StoreJobCrash>> ReadJobsAsync(NpgsqlDataSource postgres, string sql, CancellationToken cancellationToken)
    {
        var jobs = new List<StoreJobCrash>();
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            jobs.Add(Adjust(
                new StoreJobCrash(
                    JobId: reader.GetInt32(0),
                    ProcName: reader.GetString(1),
                    HypertableName: reader.IsDBNull(2) ? null : reader.GetString(2),
                    ConsecutiveCrashes: reader.GetInt32(3),
                    TotalCrashes: reader.GetInt64(4),
                    ConsecutiveFailures: reader.GetInt32(5),
                    NextStart: reader.IsDBNull(6) ? null : DateTime.SpecifyKind(reader.GetDateTime(6), DateTimeKind.Utc),
                    CrashBackoff: reader.GetBoolean(7)),
                running: reader.GetBoolean(8)));
        }

        /* Most crashes first, by the real counts. */
        jobs.Sort(static (x, y) =>
        {
            var byConsecutive = y.ConsecutiveCrashes.CompareTo(x.ConsecutiveCrashes);
            if (byConsecutive != 0)
            {
                return byConsecutive;
            }

            var byTotal = y.TotalCrashes.CompareTo(x.TotalCrashes);
            return byTotal != 0 ? byTotal : x.JobId.CompareTo(y.JobId);
        });
        return jobs;
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

            try
            {
                return new StoreJobCrashRead(true, null, await ReadJobsAsync(postgres, Sql, cancellationToken));
            }
            catch (PostgresException ex) when (ex.SqlState is "42P01" or "42703" or "42501" or "3F000" or "42883")
            {
                /* job_stats is the only new part of the read (#9391). When it cannot be read, fall back to the stored counters and say
                   so. A failure again here is the counters themselves being unreadable: the catch below says "not available". */
                return new StoreJobCrashRead(true, null, await ReadJobsAsync(postgres, SqlWithoutRunning, cancellationToken), RunningUnknownNote);
            }
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
