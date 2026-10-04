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
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The stall probe's query against a real SQL Server (#5097). Skipped unless <c>DARLING_TEST_SQL</c> is set,
/// so CI does not run it.
/// </summary>
public class StallWaitProbeLiveTests
{
    [Fact]
    public async Task TheProbe_RanksAPlantedFreshUserWait_AboveTheInstancesIdleBackgroundWaits()
    {
        var sqlHost = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(sqlHost),
            "Set DARLING_TEST_SQL to run the live stall probe test.");

        var ct = TestContext.Current.CancellationToken;
        var sqlUser = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var config = new MonitoredServer
        {
            Name = "darling-stall-probe-e2e",
            Host = sqlHost!,
            Auth = string.IsNullOrEmpty(sqlUser) ? "integrated" : "sql",
            Username = sqlUser,
            Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"),
            TrustServerCertificate = true,
        };

        var runtime = await DarlingServerConnector.ConnectAsync(config, logger: null, ct);
        Assert.SkipWhen(runtime.Target.IsAzureSqlDb, "The planted WAITFOR session is not visible across an Azure SQL Database pool.");

        /* One session of ours sits in a fresh WAITFOR, a user-task wait. */
        await using var waiter = new SqlConnection(runtime.ConnectionString);
        await waiter.OpenAsync(ct);
        await using var waitCommand = new SqlCommand("WAITFOR DELAY '00:00:20';", waiter) { CommandTimeout = 60 };
        var waiting = waitCommand.ExecuteNonQueryAsync(ct);

        try
        {
            await Task.Delay(TimeSpan.FromSeconds(2), ct);

            /* A second connection with the same application name runs the probe query. */
            await using var probe = new SqlConnection(runtime.ConnectionString);
            await probe.OpenAsync(ct);
            await using var command = new SqlCommand(StallWaitProbePolicy.QueryText, probe) { CommandTimeout = 10 };
            await using var reader = await command.ExecuteReaderAsync(ct);
            var sample = await StallWaitProbePolicy.ReadAsync(reader, ct);

            Assert.NotNull(sample);
            Assert.True(sample!.BackgroundWaitingTasks > 0, "every instance has idle background waiters when the login holds VIEW SERVER STATE; a login without that permission sees none");
            Assert.NotNull(sample.WaitSummary);
            Assert.Contains("WAITFOR:", sample.WaitSummary, StringComparison.Ordinal);
            Assert.True(sample.CollectorSessions >= 1, "our planted session should be visible by program and host");
            Assert.Equal("WAITFOR", sample.CollectorWaitType);
        }
        finally
        {
            try
            {
                await waiting;
            }
            catch (SqlException)
            {
                /* The 20-second delay ending is all this waits for. */
            }
        }
    }
}
