/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9241: a window the archive holds nothing from reads the hot table alone. A row written through the query text dimension has
/// a NULL inline text there, so a hot-only read that names the bare table returns no text, drops the WAITFOR filter and merges
/// every text into one group. These tests write rows with the collectors' real <c>WriteBatch</c> into a store with no archive
/// file and read them back through the service's own readers.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class LiteTextDimensionHotReadTests : IDisposable
{
    private const string ServerName = "HotReadServer";
    private const int ServerId = 1;
    private const string TextA = "SELECT alpha FROM dbo.TextTableA";
    private const string TextB = "SELECT beta FROM dbo.TextTableB";
    private const string WaitForText = "WAITFOR DELAY '00:00:30'";
    private static readonly DateTime s_now = DateTime.UtcNow;

    private readonly string _tempDir;
    private readonly DuckDbInitializer _store;

    public LiteTextDimensionHotReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTextDimHot_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _store = new DuckDbInitializer(Path.Combine(_tempDir, "hot.duckdb"));
    }

    public void Dispose()
    {
        _store.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private static void Write<TRow>(DuckDbInitializer store, ICollectorDefinition<TRow> definition, List<TRow> rows, DateTime at, DeltaCalculator deltas)
    {
        using var readLock = store.AcquireReadLock();
        using var connection = store.CreateConnection();
        connection.Open();
        var context = new CollectorContext { ServerId = ServerId, ServerName = ServerName, CollectionTime = at, Deltas = deltas };
        RemoteCollectorService.WriteBatch(connection, definition, rows, ServerId, ServerName, at, context);
    }

    private static QueryStatsCollector.Row StatsRow(string text, string hash, long executions) => new()
    {
        DatabaseName = "AppDb",
        QueryHash = hash,
        QueryPlanHash = "0xBB",
        CreationTime = s_now.AddDays(-1),
        LastExecutionTime = s_now.AddMinutes(-1),
        ExecutionCount = executions,
        TotalWorkerTime = executions * 1000,
        TotalElapsedTime = executions * 2000,
        TotalLogicalReads = executions * 50,
        SqlHandle = "0x01" + hash[2..],
        PlanHandle = "0x02" + hash[2..],
        QueryText = text,
    };

    /// <summary>Three statements collected at four instants, so each has a delta in both of the comparison's windows.</summary>
    private async Task SeedQueryStatsAsync()
    {
        await _store.InitializeAsync();
        var deltas = new DeltaCalculator();
        var rounds = new[] { (Minutes: -50, Executions: 100L), (Minutes: -40, Executions: 200L), (Minutes: -20, Executions: 300L), (Minutes: -10, Executions: 400L) };
        foreach (var (minutes, executions) in rounds)
        {
            Write(_store, QueryStatsCollector.Instance,
                [StatsRow(TextA, "0xA1", executions), StatsRow(TextB, "0xA2", executions), StatsRow(WaitForText, "0xA3", executions)],
                s_now.AddMinutes(minutes), deltas);
        }

        /* The rows really went through the dimension, and the window really is hot-only: no archive file exists. */
        using var connection = _store.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT count(*) FROM query_stats WHERE query_text IS NOT NULL OR query_text_digest IS NULL";
        Assert.Equal(0L, Convert.ToInt64(command.ExecuteScalar()));
        var plan = await _store.PlanArchiveReadAsync("query_stats", "collection_time", s_now.AddHours(-6), TestContext.Current.CancellationToken);
        Assert.True(plan.IsHot("query_stats"), "the seeded store has no archive, so the window must read the hot table alone");
    }

    [Fact]
    public async Task TopQueriesByCpu_OverAHotOnlyWindow_ReturnsTheDimensionText_AndStillDropsAWaitFor()
    {
        await SeedQueryStatsAsync();

        var rows = await new LocalDataService(_store).GetTopQueriesByCpuAsync(ServerId, hoursBack: 6);

        Assert.Equal(new[] { TextA, TextB }, rows.Select(r => r.QueryText).OrderBy(t => t, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task QueryStatsComparison_OverAHotOnlyWindow_ReturnsTheDimensionText()
    {
        await SeedQueryStatsAsync();

        var items = await new LocalDataService(_store).GetQueryStatsComparisonAsync(
            ServerId, s_now.AddMinutes(-30), s_now, s_now.AddMinutes(-60), s_now.AddMinutes(-30));

        var texts = items.Select(i => i.QueryText).ToHashSet(StringComparer.Ordinal);
        Assert.Contains(TextA, texts);
        Assert.Contains(TextB, texts);
        Assert.DoesNotContain("", texts);
    }

    [Fact]
    public async Task FinOpsExpensiveQueries_OverAHotOnlyWindow_KeepTwoTextsInTwoGroups()
    {
        await SeedQueryStatsAsync();

        var rows = await new LocalDataService(_store).GetExpensiveQueriesAsync(ServerId, hoursBack: 6, topN: 20);

        var texts = rows.Select(r => r.FullQueryText).OrderBy(t => t, StringComparer.Ordinal).ToArray();
        Assert.Contains(TextA, texts);
        Assert.Contains(TextB, texts);
        Assert.DoesNotContain("", texts);
        Assert.Equal(texts.Length, texts.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task ForcePlanFailures_OverAHotOnlyWindow_ReadsTheRowsWrittenThroughTheDimension()
    {
        await _store.InitializeAsync();
        var deltas = new DeltaCalculator();
        foreach (var (minutes, failures) in new[] { (-50, 1L), (-20, 4L) })
        {
            Write(_store, QueryStoreCollector.Instance, [new QueryStoreCollector.Row
            {
                DatabaseName = "AppDb",
                QueryId = 7,
                PlanId = 70,
                ExecutionTypeDesc = "Regular",
                FirstExecutionTime = s_now.AddHours(-2),
                LastExecutionTime = s_now.AddMinutes(-2),
                QueryText = TextA,
                QueryHash = "0xAB7",
                ExecutionCount = 25,
                AvgDurationUs = 4000,
                QueryPlanHash = "0xCD70",
                IntervalStartTimeUtc = s_now.AddMinutes(-30),
                IsForcedPlan = true,
                ForceFailureCount = failures,
                LastForceFailureReason = "GENERAL_FAILURE",
            }], s_now.AddMinutes(minutes), deltas);
        }

        var plan = await _store.PlanArchiveReadAsync("query_store_stats", "collection_time", s_now.AddHours(-2), TestContext.Current.CancellationToken);
        Assert.True(plan.IsHot("query_store_stats"), "the seeded store has no archive, so the window must read the hot table alone");

        var failed = await new LocalDataService(_store).GetForcePlanFailuresAsync(ServerId, TestContext.Current.CancellationToken);

        Assert.Equal(3, Assert.Single(failed).FailureDelta);
    }
}
