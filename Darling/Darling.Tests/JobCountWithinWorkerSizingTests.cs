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
/// background worker at once. Measured on a rig: 142 jobs against the old 74 slots (hypertables + 2) logged 61
/// "out of background workers" launch failures at one restart. The pure pin holds the formula; the live fact
/// holds the formula to the real job count, so a new collector or aggregate that adds policy jobs faster than
/// the formula grows fails here instead of in a field restart.
/// </summary>
public sealed class JobCountWithinWorkerSizingTests
{
    /// <summary>The formula, pinned at the numbers the measurement was taken at and at the empty store.</summary>
    [Fact]
    public void DeriveWorkerSettings_IsTwoJobsPerHypertablePlusHeadroom_AndFailsTheOldHypertablesPlusTwo()
    {
        Assert.Equal(2, DarlingManagedPostgres.PolicyJobsPerHypertable);
        Assert.Equal(16, DarlingManagedPostgres.WorkerSlotHeadroom);

        var at72 = DarlingManagedPostgres.DeriveWorkerSettings(72);
        Assert.Equal(160, at72.MaxBackgroundWorkers);
        Assert.Equal(171, at72.MaxWorkerProcesses);

        var empty = DarlingManagedPostgres.DeriveWorkerSettings(0);
        Assert.Equal(16, empty.MaxBackgroundWorkers);
        Assert.Equal(27, empty.MaxWorkerProcesses);

        /* The old shape (hypertables + 2 = 74 at 72) is below the 142 jobs measured on a migrated store. */
        const int MeasuredJobsAt72Hypertables = 142;
        Assert.True(at72.MaxBackgroundWorkers > MeasuredJobsAt72Hypertables,
            "the sizing must exceed the measured policy-job count, scheduler slot included");
        Assert.True(72 + 2 < MeasuredJobsAt72Hypertables, "the pre-#5633 formula could not launch every due job");

        /* max_worker_processes keeps drawing 3 + workers + 8 (parallel workers), so the other consumers keep
           their slots as the workers grow. */
        Assert.Equal(3 + at72.MaxBackgroundWorkers + 8, at72.MaxWorkerProcesses);
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

        var workers = DarlingManagedPostgres.DeriveWorkerSettings(TimescaleSupport.HypertableCount).MaxBackgroundWorkers;
        Assert.True(jobs > 0, "a migrated store has policy jobs");
        Assert.True(jobs + 1 <= workers,
            $"A migrated store holds {jobs} TimescaleDB jobs (+1 scheduler) but the managed sizing grants {workers} background "
            + $"workers for {TimescaleSupport.HypertableCount} hypertables. After any downtime every job is overdue at the next "
            + "start, so the shortfall shows as 'out of background workers' launch failures (#5633 item 14). Raise "
            + "DarlingManagedPostgres.PolicyJobsPerHypertable or WorkerSlotHeadroom.");
    }
}
