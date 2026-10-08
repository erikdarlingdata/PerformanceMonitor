/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5320: the statement text fetch records its failures in the collection log under its own name. Collection
/// health bands a collector from the counts of its rows in the trailing window and the age of its newest
/// success, so a name that only ever wrote errors would read FAILING until the error aged out of the window,
/// whatever happened afterwards. These tests pin the chosen behavior: an error alone reads failing, and a
/// refresh that works writes a SUCCESS row, so an error followed by later successes reads healthy again.
/// </summary>
public sealed class PgStatementTextHealthSemanticsTests
{
    private const string WorkerPath = "Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs";

    private static CollectorHealth Row(long errors, long successes, double hoursSinceLastRun)
    {
        var now = DateTime.UtcNow;
        return new CollectorHealth
        {
            CollectorName = PgStatementText.CollectorName,
            TotalRuns = errors + successes,
            ErrorCount = errors,
            SuccessCount = successes,
            RowsStored = successes * 100,
            RunsWithRows = successes,
            LastSuccessTime = successes > 0 ? now.AddHours(-hoursSinceLastRun) : null,
            LastRunTime = now.AddHours(-hoursSinceLastRun),
            LastErrorTime = errors > 0 ? now.AddHours(-hoursSinceLastRun) : null,
        };
    }

    [Fact]
    public void AnErrorWithNoSuccessReadsFailing()
    {
        Assert.Equal(CollectorHealthClassifier.Failing, Row(errors: 1, successes: 0, hoursSinceLastRun: 0.1).HealthStatus);
    }

    [Fact]
    public void AnErrorFollowedByLaterSuccessesReadsHealthy()
    {
        /* Hourly cadence: the error is one run in six once five refreshes have worked, under the 20 percent line. */
        Assert.Equal(CollectorHealthClassifier.Healthy, Row(errors: 1, successes: 5, hoursSinceLastRun: 0.1).HealthStatus);
    }

    [Fact]
    public void AnErrorFollowedByOneSuccessReadsWarningRatherThanFailing()
    {
        Assert.Equal(CollectorHealthClassifier.Warning, Row(errors: 1, successes: 1, hoursSinceLastRun: 0.1).HealthStatus);
    }

    [Fact]
    public void TheNameIsNotInTheScheduleCatalog_SoTheWorkerNeverDispatchesItAsACollector()
    {
        /* Health bands any name in collection_log, registered or not, so no registration is needed for the row to
           show; registering would make the sweep dispatch a collector that has no definition. */
        Assert.False(CollectorScheduleDefaults.All.ContainsKey(PgStatementText.CollectorName));
    }

    [Fact]
    public void ASuccessfulRefreshWritesASuccessRowUnderTheSameName()
    {
        var source = ReadRepoFile(WorkerPath);
        var start = source.IndexOf("private async Task TryRefreshPgStatementTextAsync(", StringComparison.Ordinal);
        Assert.True(start > 0);
        var end = source.IndexOf("catch (OperationCanceledException)", start, StringComparison.Ordinal);
        Assert.True(end > start);
        var body = source[start..end];

        var upsertAt = body.IndexOf("upsert.ExecuteNonQueryAsync", StringComparison.Ordinal);
        Assert.True(upsertAt > 0);
        var afterUpsert = body[upsertAt..];
        Assert.Contains("PgStatementText.CollectorName, \"SUCCESS\", queryIds.Count", afterUpsert, StringComparison.Ordinal);
    }
}
