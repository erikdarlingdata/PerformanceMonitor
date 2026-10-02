/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB pins for #4487's job_history fixes on Lite's side: (c) the numeric watermark
/// (<c>GetLastCollectedInstanceIdAsync</c>) is scoped to the newest batch's <c>collection_time</c> rather
/// than a plain unscoped MAX, so a returning identity epoch's lower-valued rows are not shadowed forever
/// by an old epoch's higher max; and (d) <c>WriteBatch</c>'s pre-insert natural-key dedupe drops rows a
/// batch already stored (server_id + instance_id + job_id + step_id + run_datetime) before the appender
/// runs, so a re-read of an already-stored window writes only the genuinely new half.
/// </summary>
public sealed class JobHistoryDedupeAndWatermarkLiteTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly DuckDbInitializer _duckDb;
    private readonly Watermark _watermark;

    public JobHistoryDedupeAndWatermarkLiteTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _watermark = new Watermark(fixture.DuckDb);
    }

    /// <summary>Exposes the runner's protected numeric-watermark read; only <c>_duckDb</c> is exercised.</summary>
    private sealed class Watermark(DuckDbInitializer duckDb)
        : RemoteCollectorService(duckDb, serverManager: null!, scheduleManager: null!)
    {
        public Task<long?> LastInstanceIdAsync(int serverId) =>
            GetLastCollectedInstanceIdAsync(serverId, "job_history", "instance_id", CancellationToken.None);
    }

    /// <summary>The private static generic <c>WriteBatch&lt;TRow&gt;</c> in
    /// RemoteCollectorService.DefinitionRunner.cs, invoked by reflection the way
    /// <c>ServerWatermarkCacheRunnerLiveTests</c> invokes Darling's twin.</summary>
    private static readonly MethodInfo WriteBatchMethod = typeof(RemoteCollectorService)
        .GetMethod("WriteBatch", BindingFlags.NonPublic | BindingFlags.Static)!;

    private static int WriteBatch<TRow>(
        DuckDB.NET.Data.DuckDBConnection duckConnection, ICollectorDefinition<TRow> definition,
        List<TRow> rows, int serverId, string serverName, DateTime collectionTime, CollectorContext context)
    {
        return (int)WriteBatchMethod.MakeGenericMethod(typeof(TRow)).Invoke(
            null, new object?[] { duckConnection, definition, rows, serverId, serverName, collectionTime, context })!;
    }

    private static CollectorContext MakeContext(DateTime collectionTime) => new()
    {
        ServerId = 1,
        ServerName = "S1",
        CollectionTime = collectionTime,
        Deltas = new RecordingCollectorDeltaCalculator(),
        Target = new CollectorTargetInfo(),
    };

    private static JobHistoryCollector.Row Row(long instanceId, DateTime runDateTime, string message) => new()
    {
        InstanceId = instanceId,
        JobId = "AAAAAAAA-1111-2222-3333-444444444444",
        JobName = "Job",
        JobEnabled = true,
        StepId = 0,
        RunStatus = 1,
        RunStatusDesc = "Succeeded",
        RunDateTime = runDateTime,
        RunDurationSeconds = 1,
        RetriesAttempted = 0,
        Message = message,
    };

    /// <summary>
    /// (c) The newest-batch scope: an old-epoch batch (ids 10,200,001..040, older collection_time), then a
    /// new-epoch batch (ids 9,545,001..030, the NEWEST collection_time) — the numeric watermark reads the
    /// new epoch's own max, not the old epoch's higher-valued max. Reproduces the identity-reseed shape
    /// #4487 fixed: a plain unscoped MAX(instance_id) would keep returning 10,200,040 forever.
    /// </summary>
    [Fact]
    public async Task GetLastCollectedInstanceIdAsync_ReturnsTheNewestBatchsMax_NotTheHighestEverStored()
    {
        var oldEpochTime = new DateTime(2026, 8, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var newEpochTime = new DateTime(2026, 8, 2, 0, 0, 0, DateTimeKind.Unspecified);

        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);

            var oldBatch = new List<JobHistoryCollector.Row>();
            for (var id = 10_200_001L; id <= 10_200_040L; id++)
            {
                oldBatch.Add(Row(id, oldEpochTime, "old-epoch"));
            }

            var newBatch = new List<JobHistoryCollector.Row>();
            for (var id = 9_545_001L; id <= 9_545_030L; id++)
            {
                newBatch.Add(Row(id, newEpochTime, "new-epoch"));
            }

            WriteBatch(connection, JobHistoryCollector.Instance, oldBatch, serverId: 1, serverName: "S1", oldEpochTime, MakeContext(oldEpochTime));
            WriteBatch(connection, JobHistoryCollector.Instance, newBatch, serverId: 1, serverName: "S1", newEpochTime, MakeContext(newEpochTime));
        }

        var lastInstanceId = await _watermark.LastInstanceIdAsync(serverId: 1);

        Assert.Equal(9_545_030L, lastInstanceId);
    }

    /// <summary>
    /// (d) The pre-insert dedupe: a batch half of whose rows (by natural key) this server already stored
    /// under the SAME collection_time WriteBatch's pre-insert read is scoped to (server_id only — it reads
    /// back every already-stored row for this server, not just the newest batch) is written again — only
    /// the genuinely new half is appended, and the resulting table has zero natural-key duplicates.
    /// </summary>
    [Fact]
    public async Task WriteBatch_JobHistory_DropsTheAlreadyStoredHalf_AndLeavesZeroNaturalKeyDuplicates()
    {
        var firstRunTime = new DateTime(2026, 8, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var secondRunTime = new DateTime(2026, 8, 1, 0, 5, 0, DateTimeKind.Unspecified);

        var firstBatch = new List<JobHistoryCollector.Row>
        {
            Row(1, firstRunTime, "first-A"),
            Row(2, firstRunTime, "first-B"),
        };

        /* The second read re-fetches the SAME two rows (a legitimate failback re-read) plus two genuinely
           new ones. Only the two new rows must survive into the table. */
        var secondBatch = new List<JobHistoryCollector.Row>
        {
            Row(1, firstRunTime, "first-A"),
            Row(2, firstRunTime, "first-B"),
            Row(3, firstRunTime, "second-C"),
            Row(4, firstRunTime, "second-D"),
        };

        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        var firstWritten = WriteBatch(connection, JobHistoryCollector.Instance, firstBatch, serverId: 1, serverName: "S1", firstRunTime, MakeContext(firstRunTime));
        var secondWritten = WriteBatch(connection, JobHistoryCollector.Instance, secondBatch, serverId: 1, serverName: "S1", secondRunTime, MakeContext(secondRunTime));

        Assert.Equal(2, firstWritten);
        Assert.Equal(2, secondWritten); /* only rows 3 and 4 actually appended */

        using var countCmd = connection.CreateCommand();
        countCmd.CommandText = "SELECT COUNT(*) FROM job_history WHERE server_id = 1";
        var total = (long)(await countCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
        Assert.Equal(4, total);

        using var dupCmd = connection.CreateCommand();
        dupCmd.CommandText =
            "SELECT COUNT(*) FROM (SELECT COUNT(*) AS c FROM job_history WHERE server_id = 1 " +
            "GROUP BY instance_id, job_id, step_id, run_datetime HAVING COUNT(*) > 1)";
        var duplicateKeyCount = (long)(await dupCmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
        Assert.Equal(0, duplicateKeyCount);
    }
}
