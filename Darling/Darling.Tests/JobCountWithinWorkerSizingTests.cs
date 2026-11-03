/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres and never touches the
   shared store. */

/// <summary>
/// #5633 item 14: the managed runtime's worker sizing must cover every TimescaleDB policy job a migrated store
/// holds, because after any downtime every job is overdue at the next start and each one asks for its own
/// background worker at once. Measured on a rig: 142 jobs against the old 74 slots (hypertables + 2) logged 61-68
/// "out of background workers" launch failures at one restart. The sizing is RAM-bound (16 slots per GB) between that
/// floor and a ceiling of every job the schema can create. The pure pin holds the formula; the live fact
/// holds the ceiling to the real job count, so a new collector or aggregate that adds policy jobs faster than
/// the formula grows fails here instead of in a field restart.
/// </summary>
public sealed class JobCountWithinWorkerSizingTests
{
    private const long Gb = 1024L * 1024 * 1024;

    /// <summary>
    /// The formula, pinned at 4, 8 and 16 GB hosts for the measured schema (72 hypertables, 29 aggregates) and at
    /// the empty store. workers = clamp(16 x GB, hypertables + 2, hypertables + 3 x aggregates + 2 + 16).
    /// </summary>
    [Fact]
    public void DeriveWorkerSettings_IsRamBoundedBetweenTheFloorAndTheJobCeiling()
    {
        Assert.Equal(16, DarlingManagedPostgres.WorkerSlotsPerGb);
        Assert.Equal(3, DarlingManagedPostgres.PolicyJobsPerContinuousAggregate);
        Assert.Equal(2, DarlingManagedPostgres.NonObjectPolicyJobs);
        Assert.Equal(16, DarlingManagedPostgres.WorkerSlotHeadroom);

        /* 4 GB: 64 is under the floor, so the old sizing (and its retried launches) stays. */
        var at4 = DarlingManagedPostgres.DeriveWorkerSettings(72, 29, 4 * Gb);
        Assert.Equal(74, at4.MaxBackgroundWorkers);
        Assert.Equal(85, at4.MaxWorkerProcesses);

        var at8 = DarlingManagedPostgres.DeriveWorkerSettings(72, 29, 8 * Gb);
        Assert.Equal(128, at8.MaxBackgroundWorkers);
        Assert.Equal(139, at8.MaxWorkerProcesses);

        /* 16 GB: 256 would be past every job the schema can create (72 + 3 x 29 + 2 + 16 = 177). */
        var at16 = DarlingManagedPostgres.DeriveWorkerSettings(72, 29, 16 * Gb);
        Assert.Equal(177, at16.MaxBackgroundWorkers);
        Assert.Equal(188, at16.MaxWorkerProcesses);
        Assert.Equal(177, DarlingManagedPostgres.DeriveWorkerCeiling(72, 29));

        /* The ceiling counts aggregates: one more aggregate adds up to three jobs and so three slots. */
        Assert.Equal(180, DarlingManagedPostgres.DeriveWorkerSettings(72, 30, 16 * Gb).MaxBackgroundWorkers);

        /* The floor is the pre-#5633 shape, and the empty store never goes below 2 + headroom. */
        Assert.Equal(DarlingManagedPostgres.DeriveWorkerFloor(72), at4);
        Assert.Equal(2, DarlingManagedPostgres.DeriveWorkerFloor(0).MaxBackgroundWorkers);
        Assert.Equal(18, DarlingManagedPostgres.DeriveWorkerSettings(0, 0, 8 * Gb).MaxBackgroundWorkers);

        /* Never below the floor, never above the ceiling, whatever the RAM (a 1 GB host and a 1 TB host). */
        foreach (var ram in new[] { 1 * Gb, 2 * Gb, 4 * Gb, 6 * Gb, 8 * Gb, 11 * Gb, 12 * Gb, 64 * Gb, 1024 * Gb })
        {
            var w = DarlingManagedPostgres.DeriveWorkerSettings(72, 29, ram);
            Assert.InRange(w.MaxBackgroundWorkers, 74, 177);
            Assert.Equal(3 + w.MaxBackgroundWorkers + 8, w.MaxWorkerProcesses);
        }

        /* The burst bound the factor exists for: at 15 MB a backend (the review's figure, above the 4-6 MB
           measured) a full burst of RAM-bound slots stays under a quarter of the host. */
        foreach (var gb in new[] { 8L, 12L, 16L })
        {
            var jobBackends = DarlingManagedPostgres.DeriveWorkerSettings(72, 29, gb * Gb).MaxBackgroundWorkers - 1;
            Assert.True(jobBackends * 15L * 1024 * 1024 <= gb * Gb / 4,
                $"at {gb} GB the RAM-bound slots must keep the burst under a quarter of RAM");
        }
    }

    /// <summary>
    /// A store built by the real migrations and every startup convergence step that creates jobs carries no
    /// more policy jobs than the sizing leaves room for, with one slot kept for the database's scheduler.
    /// </summary>
    [Fact]
    public async Task MigratedStore_PolicyJobCount_FitsTheManagedWorkerSizing_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live policy-job count test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct),
            "TimescaleDB must be available on CI for the policy-job count test");

        /* The same steps DarlingWorker's store-object convergence runs that create jobs. */
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct);
        await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        await TimescaleSupport.EnsureAggregateCompressionAsync(connection, null, ct);
        await TimescaleSupport.EnsureRetentionPoliciesAsync(connection, null, ct);

        await using var command = new NpgsqlCommand("SELECT count(*) FROM timescaledb_information.jobs", connection);
        var jobs = Convert.ToInt32(await command.ExecuteScalarAsync(ct));

        await using var aggregateCommand = new NpgsqlCommand("SELECT count(*) FROM timescaledb_information.continuous_aggregates", connection);
        var aggregates = Convert.ToInt32(await aggregateCommand.ExecuteScalarAsync(ct));
        await using var hypertableCommand = new NpgsqlCommand("SELECT count(*) FROM timescaledb_information.hypertables", connection);
        var hypertables = Convert.ToInt32(await hypertableCommand.ExecuteScalarAsync(ct));

        /* The counts the formula is keyed on are the store's real ones (a new aggregate must move the constant). */
        Assert.Equal(aggregates, TimescaleSupport.ContinuousAggregateCount);
        Assert.Equal(hypertables, TimescaleSupport.HypertableCount);

        /* The ceiling (what a host with enough RAM gets) covers every real job plus the scheduler, and is not
           loose: within 40 slots of the real count, so a count that outgrows the model or a constant that
           inflates it fails here. */
        var ceiling = DarlingManagedPostgres.DeriveWorkerSettings(
            TimescaleSupport.HypertableCount, TimescaleSupport.ContinuousAggregateCount, 1024L * Gb).MaxBackgroundWorkers;
        Assert.True(jobs > 0, "a migrated store has policy jobs");
        Assert.True(jobs + 1 <= ceiling,
            $"A migrated store holds {jobs} TimescaleDB jobs (+1 scheduler) but the managed ceiling grants {ceiling} background "
            + $"workers for {hypertables} hypertables and {aggregates} aggregates. After any downtime every job is overdue at the next "
            + "start, so the shortfall shows as 'out of background workers' launch failures (#5633 item 14). Raise "
            + "DarlingManagedPostgres.PolicyJobsPerContinuousAggregate, NonObjectPolicyJobs or WorkerSlotHeadroom.");
        Assert.True(ceiling - (jobs + 1) <= 40,
            $"The worker ceiling {ceiling} is more than 40 slots above the {jobs} jobs (+1 scheduler) a migrated store holds; "
            + "lower the per-aggregate or headroom constants so the ceiling stays a measured bound.");

        /* A host big enough for the ceiling gets every job its own worker. */
        Assert.True(DarlingManagedPostgres.DeriveWorkerSettings(TimescaleSupport.HypertableCount, 16 * Gb).MaxBackgroundWorkers >= jobs + 1);
    }
}
