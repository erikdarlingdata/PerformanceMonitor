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
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every live test mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #8549: a Job History load that a newer load has superseded cancels its store reads, so a quick re-pick does not leave the older grid
/// statement running beside the newest (the field showed two statements of 16 and 18 seconds overlapping and ending together). A cancelled
/// read shows no error, no stamp and no empty grid, and the newest load still binds.
/// </summary>
public sealed class JobHistorySupersededLoadCancelLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #8549 superseded load tests.";

    [Fact]
    public async Task AClaim_CancelsTheOlderLoadsToken_AndNeverTheNewestOrAnotherSurfaces()
    {
        var loads = new ScopedLoadGenerations();
        var (first, firstToken) = loads.ClaimCancelling("grid");
        var (other, otherToken) = loads.ClaimCancelling("other");
        Assert.False(firstToken.IsCancellationRequested);

        var read = Task.Delay(Timeout.Infinite, firstToken);
        var (second, secondToken) = loads.ClaimCancelling("grid");

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read);
        Assert.True(firstToken.IsCancellationRequested);
        Assert.True(loads.Superseded("grid", first));
        Assert.False(loads.Superseded("grid", second));
        Assert.False(secondToken.IsCancellationRequested, "the newest load is never cancelled by its own claim");
        Assert.False(otherToken.IsCancellationRequested, "another surface's load is not cancelled");
        Assert.False(loads.Superseded("other", other));
    }

    [Fact]
    public async Task ASupersededLoad_LeavesNoReadRunningBesideTheNewest_AndTheNewestStillBinds_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using (var seed = new NpgsqlConnection(scratch.ConnectionString))
        {
            await seed.OpenAsync(ct);
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.job_history
    (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled, step_id, step_name,
     run_status, run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
SELECT g, (now() AT TIME ZONE 'utc') - interval '30 minutes', 9301, 'jh-cancel', g, 'job-1', 'Nightly', true, 0, '(Job outcome)', 1,
       'The job succeeded.', (now() AT TIME ZONE 'utc') - make_interval(mins => g::int), 5, 0, NULL
FROM generate_series(1, 5) AS g", seed);
            await insert.ExecuteNonQueryAsync(ct);
        }

        var statuses = new List<string>();
        var blockedAfterSupersede = -1;
        var rowsBound = 0;

        StaTestThread.Run(async () =>
        {
            var tab = new JobHistoryTab();
            tab.Initialize(new ViewerDataService(scratch.ConnectionString));
            tab.StatusChanged += statuses.Add;
            var host = new System.Windows.Controls.Border { Width = 1000, Height = 680, Child = tab };
            host.Measure(new System.Windows.Size(1000, 680));
            host.Arrange(new System.Windows.Rect(0, 0, 1000, 680));

            /* A held lock keeps every read of the table waiting, so a read that is not cancelled is still running when it is counted. */
            await using var holder = new NpgsqlConnection(scratch.ConnectionString);
            await holder.OpenAsync(ct);
            await using var tx = await holder.BeginTransactionAsync(ct);
            await using (var lockIt = new NpgsqlCommand("LOCK TABLE collect.job_history IN ACCESS EXCLUSIVE MODE", holder, tx))
            {
                await lockIt.ExecuteNonQueryAsync(ct);
            }

            var first = tab.RefreshJobsAsync();
            Assert.True(await WaitForBlockedGridReadsAsync(scratch.ConnectionString, 1, ct), "the first load's grid read is waiting on the lock");

            var second = tab.RefreshJobsAsync();
            /* Long enough for the second read to reach the lock and for a cancel to reach the first's backend. */
            await Task.Delay(2000, ct);
            blockedAfterSupersede = await BlockedGridReadsAsync(scratch.ConnectionString, ct);

            await tx.RollbackAsync(ct);
            await second;
            await first;
            rowsBound = tab.JobHistoryDataGrid.Items.Count;
        });

        Assert.Equal(1, blockedAfterSupersede);
        Assert.Equal(5, rowsBound);
        Assert.DoesNotContain(statuses, s => s.StartsWith("failed", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>How many grid statements (the one whose first CTE is <c>active_servers</c>) wait on a lock right now.</summary>
    private static async Task<int> BlockedGridReadsAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(@"
SELECT count(*) FROM pg_stat_activity
WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%active_servers AS%'", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<bool> WaitForBlockedGridReadsAsync(string connectionString, int count, CancellationToken ct, bool atLeast = false)
    {
        var until = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < until)
        {
            var blocked = await BlockedGridReadsAsync(connectionString, ct);
            if (atLeast ? blocked >= count : blocked == count)
            {
                return true;
            }

            await Task.Delay(100, ct);
        }

        return false;
    }
}
