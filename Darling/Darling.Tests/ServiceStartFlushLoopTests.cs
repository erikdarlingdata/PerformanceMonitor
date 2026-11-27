/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The service start history's flush loop (#9415) when the worker's startup scope is left by cancellation during the store-retry wait, the exit no
/// catch filter admits. The scope is declared the way the worker declares it: the recorder, then the data source, then the loop handle, so leaving the
/// scope by any route has to stop the loop and wait for its last flush before the data source and the recorder's gate are released.
/// </summary>
/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres. The unobserved-exception hook is process-wide, so each
   fact looks only at the task it started. */
public sealed class ServiceStartFlushLoopTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the live flush loop test.";

    private static readonly DateTime T0 = new(2026, 11, 1, 8, 0, 0, DateTimeKind.Utc);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The worker's startup scope in miniature: the store-retry wait ends in the cancellation, and every disposal runs on the way out.</summary>
    private static async Task RunLikeTheWorkerAsync(
        string connectionString, ServiceStartRecorder recorder, CancellationToken stopping, List<Task> loopTasks)
    {
        using var ownedRecorder = recorder;
        await using var source = NpgsqlDataSource.Create(connectionString);
        await using var startHistory = recorder.StartFlushLoop(source, NullLogger.Instance, stopping);
        loopTasks.Add(startHistory.Completion);
        await Task.Delay(Timeout.Infinite, stopping);
    }

    private static ServiceStartRecorder NewRecorder(Guid startId) =>
        new(new ServiceLogTally(), startId, "host-a", "3.14.0-test.1", "0123abc", T0, () => T0.AddMinutes(1));

    private static async Task<List<Exception>> LeaveByCancellationAsync(string connectionString, ServiceStartRecorder recorder)
    {
        var unobserved = new List<Exception>();
        var loopTasks = new List<Task>();
        void OnUnobserved(object? sender, UnobservedTaskExceptionEventArgs args)
        {
            if (loopTasks.Contains((Task)sender!))
            {
                lock (unobserved)
                {
                    unobserved.AddRange(args.Exception.Flatten().InnerExceptions);
                }
            }
        }

        TaskScheduler.UnobservedTaskException += OnUnobserved;
        try
        {
            using var stopping = new CancellationTokenSource();
            stopping.CancelAfter(TimeSpan.FromMilliseconds(250));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => RunLikeTheWorkerAsync(connectionString, recorder, stopping.Token, loopTasks));

            /* The scope has been left: the loop is over, so its last flush already ran and nothing is left to fault later. */
            Assert.True(Assert.Single(loopTasks).IsCompletedSuccessfully, "leaving the scope must wait for the flush loop to end");

            loopTasks.Clear();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            await Task.Delay(50, TestContext.Current.CancellationToken);
            return unobserved;
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= OnUnobserved;
        }
    }

    /// <summary>The store is down: the last flush finds no store, keeps what it holds, and the loop ends without a fault.</summary>
    [Fact]
    public async Task LeavingByCancellation_WhileTheStoreIsDown_EndsTheLoopWithoutAFault()
    {
        const string deadStore = "Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x;Timeout=2;Pooling=false";
        var recorder = NewRecorder(Guid.NewGuid());
        recorder.RungFinished(1, "make-history-tables", T0, 5, MigrationRungOutcome.Applied, null);

        var unobserved = await LeaveByCancellationAsync(deadStore, recorder);

        Assert.Empty(unobserved);
    }

    /// <summary>The store is up: the last flush the loop makes on the way out writes the start row and the rung the recorder was holding.</summary>
    [Fact]
    public async Task LeavingByCancellation_RunsTheLastFlush_BeforeTheDataSourceGoes()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using (var setup = NpgsqlDataSource.Create(scratch.ConnectionString))
            await using (var create = setup.CreateCommand("CREATE SCHEMA IF NOT EXISTS collect; " + ServiceStartHistory.CreateSql + ServiceStartRungObjects.CreateSql))
            {
                create.CommandTimeout = 30;
                await create.ExecuteNonQueryAsync(ct);
            }

            var startId = Guid.NewGuid();
            var recorder = NewRecorder(startId);
            recorder.RungFinished(1, "make-history-tables", T0, 5, MigrationRungOutcome.Applied, null);

            var unobserved = await LeaveByCancellationAsync(scratch.ConnectionString, recorder);

            Assert.Empty(unobserved);
            await using var read = NpgsqlDataSource.Create(scratch.ConnectionString);
            var record = Assert.Single(await ServiceStartHistory.ReadAsync(read, 10, serviceHost: null, ct));
            Assert.Equal(startId, record.Start.StartId);
            Assert.Contains(record.Rungs, r => r.Version == 1 && r.Outcome == MigrationRungOutcome.Applied);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
