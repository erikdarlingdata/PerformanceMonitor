/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5703: the desktop Job History tab's All Servers grid lost a failure the single-server grid showed. The read keeps the newest
/// <c>RowCap</c> runs of the WHOLE fleet; one busy server (a job every minute) fills that cap with its latest runs, and the tab then
/// applied its Status and Category filters to those rows on the client, so an older failure on a quieter server was never in the rows
/// the filter saw. The tab now passes both filters into the read, which applies them inside each server's newest rows, ahead of the cap.
///
/// <para>Two servers: a busy one with 60 recent successful runs, and a quiet one with a single older failure. With a cap of 20 the
/// unfiltered fleet read is all busy-server runs (the old shape: nothing for a client filter to find), while the fleet read with the
/// Failed status returns exactly what the quiet server's own read returns.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class JobHistoryFilterBeforeCapLiveTests
{
    private const int BusyServer = -570_301;
    private const int QuietServer = -570_302;
    private const long IdBase = 5_703_000_000L;
    private const string CategoryA = "darling-5703-category-a";
    private const string CategoryB = "darling-5703-category-b";

    [Fact]
    public async Task AllServers_WithAStatusOrCategoryFilter_KeepsTheQuietServersOlderRuns_ThatTheFleetCapPushedOut()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #5703 job-history filter-before-cap live test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var viewer = new ViewerDataService(connectionString!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, BusyServer, "darling-5703-busy", ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, QuietServer, "darling-5703-quiet", ct);

            /* Neither server has a server_properties row, so both clocks are UTC. */
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var start = now.AddDays(-7);
            var id = 0L;

            /* The busy server: a successful outcome every minute for the last hour, newer than everything on the quiet server. */
            for (var i = 0; i < 60; i++)
            {
                await InsertAsync(connection, ct, ++id, BusyServer, now.AddMinutes(-i), "busy_job_" + i, 1, CategoryA);
            }

            /* The quiet server: a failure three days ago (category B) and a success four days ago (category A). */
            await InsertAsync(connection, ct, ++id, QuietServer, now.AddDays(-3), "quiet_failure", 0, CategoryB);
            await InsertAsync(connection, ct, ++id, QuietServer, now.AddDays(-4), "quiet_success", 1, CategoryA);

            const int cap = 20;

            /* The single-server reads: the quiet server's failure is there with the Failed filter, and its category-B runs with that one. */
            var quietFailed = await viewer.GetJobHistoryAsync(start, QuietServer, cap, runStatus: 0, cancellationToken: ct);
            Assert.Equal(new[] { "quiet_failure" }, quietFailed.Select(r => r.JobName).ToArray());

            /* The old shape: the fleet read with no filter is the busy server's newest 20 runs, so a client-side Failed filter finds nothing. */
            var unfiltered = await viewer.GetJobHistoryAsync(start, null, cap, cancellationToken: ct);
            Assert.Equal(cap, unfiltered.Count);
            Assert.All(unfiltered, r => Assert.Equal(BusyServer, r.ServerId));
            Assert.DoesNotContain(unfiltered, r => r.RunStatus == 0);

            /* The fix: Status goes into the read, ahead of the cap, so All Servers holds what the quiet server's own read holds. */
            var fleetFailed = await viewer.GetJobHistoryAsync(start, null, cap, runStatus: 0, cancellationToken: ct);
            Assert.Equal(quietFailed.Select(r => r.JobName).ToArray(), fleetFailed.Select(r => r.JobName).ToArray());
            Assert.Equal(QuietServer, Assert.Single(fleetFailed).ServerId);

            /* The same for Category: category B exists only on the quiet server. */
            var fleetCategoryB = await viewer.GetJobHistoryAsync(start, null, cap, category: CategoryB.ToUpperInvariant(), cancellationToken: ct);
            Assert.Equal(new[] { "quiet_failure" }, fleetCategoryB.Select(r => r.JobName).ToArray());

            /* Both together, and a filter that matches nothing. */
            var both = await viewer.GetJobHistoryAsync(start, null, cap, runStatus: 0, category: CategoryB, cancellationToken: ct);
            Assert.Equal(new[] { "quiet_failure" }, both.Select(r => r.JobName).ToArray());
            var none = await viewer.GetJobHistoryAsync(start, null, cap, runStatus: 0, category: CategoryA, cancellationToken: ct);
            Assert.Empty(none);

            /* A finished range's end composes with the filters: the failure is inside a range that ended two days ago. */
            var ended = await viewer.GetJobHistoryAsync(start, null, cap, untilUtc: now.AddDays(-2), runStatus: 0, cancellationToken: ct);
            Assert.Equal(new[] { "quiet_failure" }, ended.Select(r => r.JobName).ToArray());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, CleanupAsync);
        }
    }

    private static Task InsertAsync(NpgsqlConnection connection, CancellationToken ct, long id, int serverId, DateTime at, string job, int runStatus, string category) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id,
                                       job_id, job_name, job_enabled, category_name, step_id, step_name, run_status,
                                       run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
              VALUES ($1,$2,$3,$4,$5,$6,$7,true,$8,0,'(Job outcome)',$9,$10,$11,5,0,NULL)",
            IdBase + id, at, serverId, "srv" + serverId.ToString(CultureInfo.InvariantCulture), IdBase + id, job, job, category,
            runStatus, runStatus == 0 ? "The job failed." : "The job succeeded.", at);

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var jh = new NpgsqlCommand("DELETE FROM job_history WHERE server_id IN (-570301, -570302)", connection);
        await jh.ExecuteNonQueryAsync(ct);
        await using var svr = new NpgsqlCommand("DELETE FROM servers WHERE server_id IN (-570301, -570302)", connection);
        await svr.ExecuteNonQueryAsync(ct);
    }
}
