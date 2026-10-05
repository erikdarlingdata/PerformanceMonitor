/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
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
    public async Task TheProbe_CountsAPlantedWaitforAsIdle_AndStillSeesOurOwnSessionWaiting()
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

            /* WAITFOR is on the shared idle list: the planted session is counted in the tail, not ranked. */
            Assert.DoesNotContain("WAITFOR:", sample.WaitSummary, StringComparison.Ordinal);
            Assert.NotEqual("WAITFOR", sample.TopWaitType);
            Assert.Matches(@"idle [1-9]\d* tasks/[1-9]\d* types", sample.WaitSummary);
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

    /// <summary>
    /// An idle wait does not rank as the top user-task wait (#5267). One connection runs
    /// <c>sp_server_diagnostics</c>, which repeats and sits in <c>SP_SERVER_DIAGNOSTICS_SLEEP</c> on an
    /// <c>is_user_process = 1</c> session, exactly as the availability-group health check does. A second
    /// connection blocks on a lock a third holds. The real probe query must rank the lock wait first and count
    /// the idle wait in the tail rather than in the ranking.
    /// </summary>
    [Fact]
    public async Task TheProbe_RanksALockWait_AboveSpServerDiagnosticsSleep_AndCountsTheIdleWait()
    {
        var sqlHost = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(sqlHost),
            "Set DARLING_TEST_SQL to run the live stall probe test.");

        var ct = TestContext.Current.CancellationToken;
        var sqlUser = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var config = new MonitoredServer
        {
            Name = "darling-stall-probe-idle-e2e",
            Host = sqlHost!,
            Auth = string.IsNullOrEmpty(sqlUser) ? "integrated" : "sql",
            Username = sqlUser,
            Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"),
            TrustServerCertificate = true,
        };

        var runtime = await DarlingServerConnector.ConnectAsync(config, logger: null, ct);
        Assert.SkipWhen(runtime.Target.IsAzureSqlDb, "The planted sessions are not visible across an Azure SQL Database pool.");

        await using var holder = new SqlConnection(runtime.ConnectionString);
        await holder.OpenAsync(ct);
        await using var blocked = new SqlConnection(runtime.ConnectionString);
        await blocked.OpenAsync(ct);
        await using var diagnostics = new SqlConnection(runtime.ConnectionString);
        await diagnostics.OpenAsync(ct);

        var table = "tempdb.dbo.stall_probe_5267_" + Guid.NewGuid().ToString("N");
        await using var block = new SqlCommand("SELECT id FROM " + table + " WITH (READCOMMITTEDLOCK);", blocked) { CommandTimeout = 120 };
        await using var diag = new SqlCommand("EXEC sp_server_diagnostics 5;", diagnostics) { CommandTimeout = 0 };
        Task? blockedTask = null;
        Task? diagnosticsTask = null;
        SqlTransaction? held = null;
        var diagnosticsSpid = 0;
        var blockedSpid = 0;
        await using (var spid = new SqlCommand("SELECT CONVERT(integer, @@SPID);", diagnostics))
        {
            diagnosticsSpid = (int)(await spid.ExecuteScalarAsync(ct))!;
        }

        await using (var spid = new SqlCommand("SELECT CONVERT(integer, @@SPID);", blocked))
        {
            blockedSpid = (int)(await spid.ExecuteScalarAsync(ct))!;
        }

        try
        {
            await using (var create = new SqlCommand("CREATE TABLE " + table + " (id integer NOT NULL); INSERT " + table + " VALUES (1);", holder))
            {
                await create.ExecuteNonQueryAsync(ct);
            }

            /* The third connection holds an exclusive lock for the rest of the test. */
            held = holder.BeginTransaction();
            await using (var hold = new SqlCommand("UPDATE " + table + " SET id = 2;", holder, held))
            {
                await hold.ExecuteNonQueryAsync(ct);
            }

            blockedTask = block.ExecuteScalarAsync(CancellationToken.None);
            diagnosticsTask = diag.ExecuteNonQueryAsync(CancellationToken.None);

            /* Long enough for the lock wait to build and for the diagnostics procedure to be in its sleep. */
            await Task.Delay(TimeSpan.FromSeconds(3), ct);

            await using var probe = new SqlConnection(runtime.ConnectionString);
            await probe.OpenAsync(ct);
            await using var command = new SqlCommand(StallWaitProbePolicy.QueryText, probe) { CommandTimeout = 10 };
            await using var reader = await command.ExecuteReaderAsync(ct);
            var sample = await StallWaitProbePolicy.ReadAsync(reader, ct);

            Assert.NotNull(sample);
            Assert.NotNull(sample!.TopWaitType);
            Assert.StartsWith("LCK_M_", sample.TopWaitType, StringComparison.Ordinal);
            Assert.NotNull(sample.WaitSummary);
            Assert.DoesNotContain("SP_SERVER_DIAGNOSTICS_SLEEP:", sample.WaitSummary, StringComparison.Ordinal);

            var idle = Regex.Match(sample.WaitSummary!, @"idle (\d+) tasks/(\d+) types");
            Assert.True(idle.Success, "the tail names the idle waits: " + sample.WaitSummary);
            Assert.True(long.Parse(idle.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture) >= 1, "the diagnostics session's idle wait is counted: " + sample.WaitSummary);
            Assert.True(int.Parse(idle.Groups[2].Value, System.Globalization.CultureInfo.InvariantCulture) >= 1);

            /* The total still counts every waiting task, the idle one included. */
            Assert.True(sample.WaitingTaskCount >= 2);
        }
        finally
        {
            held?.Rollback();

            /* KILL ends both waiting sessions at once; sp_server_diagnostics repeats until it is stopped. */
            foreach (var victim in new[] { diagnosticsSpid, blockedSpid })
            {
                try
                {
                    await using var kill = new SqlCommand("KILL " + victim.ToString(System.Globalization.CultureInfo.InvariantCulture) + ";", holder);
                    await kill.ExecuteNonQueryAsync(CancellationToken.None);
                }
                catch (SqlException)
                {
                    /* The session may already be gone. */
                }
            }

            foreach (var task in new[] { blockedTask, diagnosticsTask })
            {
                if (task is null)
                {
                    continue;
                }

                try
                {
                    await task;
                }
                catch (Exception ex) when (ex is SqlException or OperationCanceledException)
                {
                    /* Cancelling the two waiting sessions is all this waits for. */
                }
            }

            try
            {
                await using var drop = new SqlCommand("DROP TABLE IF EXISTS " + table + ";", holder);
                await drop.ExecuteNonQueryAsync(CancellationToken.None);
            }
            catch (SqlException)
            {
                /* The table lives in tempdb and goes with the next restart. */
            }
        }
    }
}
