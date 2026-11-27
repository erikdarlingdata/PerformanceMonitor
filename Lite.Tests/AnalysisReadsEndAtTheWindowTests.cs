/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// An analysis pass reads what the store held by the end of its window, so a pass as of a past instant gives the answer
/// that instant had. A capture stored after the window's end changes neither the database configuration fact, which
/// reads the newest capture at or before the end, nor the data span the history gate measures. The same holds for the
/// plan regression fact and its drill-down, which read the Query Store rows collected by the end, and for the
/// configuration drill-down, the plan analysis server metadata and the max degree of parallelism the high-DOP count is
/// held to, which read the newest capture at or before it. The anomaly detector's window leaves its end out, so its
/// Azure edition read takes the newest row before the end.
/// </summary>
public sealed class AnalysisReadsEndAtTheWindowTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = -941_001;

    private static readonly DateTime End = new(2026, 10, 8, 12, 0, 0, DateTimeKind.Utc);

    private readonly DuckDbInitializer _duckDb;
    private long _id = -94_100_000;

    public AnalysisReadsEndAtTheWindowTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private async Task SeedAsync(Func<DuckDBConnection, Task> seed)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await seed(connection);
    }

    private async Task<Dictionary<string, Fact>> FactsAsync(DateTime end)
    {
        var context = new AnalysisContext { ServerId = ServerId, ServerName = "window-end", TimeRangeStart = end.AddHours(-4), TimeRangeEnd = end };
        await SecondaryReplicaScope.EnsureAsync(_duckDb, context);
        return (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context)).ToDictionary(f => f.Key);
    }

    /// <summary>The capture a day before the end holds two databases with auto shrink on; the capture an hour after the end holds
    /// one with it off.</summary>
    private Task SeedTwoDatabaseConfigCapturesAsync() => SeedAsync(async c =>
    {
        await LatestValueSeed.InsertDatabaseConfigAsync(c, ServerId, End.AddDays(-1), "DbA", autoShrink: true, rcsiOn: false);
        await LatestValueSeed.InsertDatabaseConfigAsync(c, ServerId, End.AddDays(-1), "DbB", autoShrink: true, rcsiOn: false);
        await LatestValueSeed.InsertDatabaseConfigAsync(c, ServerId, End.AddHours(1), "DbA", autoShrink: false, rcsiOn: true);
    });

    [Fact]
    public async Task TheDatabaseConfigFact_ReadsTheNewestCaptureAtOrBeforeTheWindowEnd()
    {
        await SeedTwoDatabaseConfigCapturesAsync();

        var config = (await FactsAsync(End))["DB_CONFIG"];

        Assert.Equal(2, config.Metadata["database_count"]);
        Assert.Equal(2, config.Metadata["auto_shrink_on_count"]);
    }

    [Fact]
    public async Task TheDatabaseConfigFact_ReadsTheLaterCapture_OnceTheWindowEndsAtIt()
    {
        await SeedTwoDatabaseConfigCapturesAsync();

        var config = (await FactsAsync(End.AddHours(1)))["DB_CONFIG"];

        Assert.Equal(1, config.Metadata["database_count"]);
        Assert.Equal(0, config.Metadata["auto_shrink_on_count"]);
    }

    private Task SeedWaitStatsAtAsync(params DateTime[] stamps) => SeedAsync(async c =>
    {
        foreach (var stamp in stamps)
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type) VALUES ($1, $2, $3, 'window-end', 'SOS_SCHEDULER_YIELD')";
            cmd.Parameters.Add(new DuckDBParameter { Value = _id-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = stamp });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    });

    /// <summary>Rows sit 30 hours and 1 hour before the end and 10 hours after it: the server had 29 hours of history by the end,
    /// and 40 hours in all.</summary>
    [Fact]
    public async Task TheDataSpan_MeasuresTheHistoryTheServerHadByTheWindowEnd()
    {
        await SeedWaitStatsAtAsync(End.AddHours(-30), End.AddHours(-1), End.AddHours(10));
        var analysis = new AnalysisService(_duckDb);

        Assert.Equal(29.0, await analysis.GetTotalDataSpanHoursAsync(ServerId, End), 3);
        Assert.Equal(40.0, await analysis.GetTotalDataSpanHoursAsync(ServerId), 3);
    }

    /// <summary>A row stamped exactly on the window's end is history the server had by then.</summary>
    [Fact]
    public async Task TheDataSpan_CountsARowStampedOnTheWindowEnd()
    {
        await SeedWaitStatsAtAsync(End.AddHours(-30), End);
        var analysis = new AnalysisService(_duckDb);

        Assert.Equal(30.0, await analysis.GetTotalDataSpanHoursAsync(ServerId, End), 3);
    }

    private static AnalysisContext ContextFor(DateTime end)
        => new() { ServerId = ServerId, ServerName = "window-end", TimeRangeStart = end.AddHours(-4), TimeRangeEnd = end };

    /// <summary>One Query Store interval of a plan of <paramref name="queryId"/>: its outcome, how many times it ran, when it
    /// last ran and when the row was collected.</summary>
    private Task SeedQueryStoreRowAsync(DateTime collectedAt, long queryId, long planId, string planHash, long intervalId,
        DateTime lastExec, long executions, long cpuUs, string outcome = "Regular") => SeedAsync(async c =>
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count,
     runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, $3, 'window-end', 'SalesDb', $4, $5, $6, $7, $8, '0xQH', $9, $10, $10, $11, false, 0, $12, $7)";
        foreach (var value in new object[] { _id--, collectedAt, ServerId, queryId, planId, outcome, lastExec.AddMinutes(-30), lastExec, executions, cpuUs, planHash, intervalId })
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    });

    /// <summary>Query 7 ran a cheap plan five days before the end and runs a plan five times as costly inside the window.</summary>
    private async Task SeedARegressionAsync()
    {
        await SeedQueryStoreRowAsync(End.AddDays(-5), queryId: 7, planId: 71, "0xCHEAP", intervalId: 1, lastExec: End.AddDays(-5).AddMinutes(-5), executions: 100, cpuUs: 100_000);
        await SeedQueryStoreRowAsync(End.AddMinutes(-5), queryId: 7, planId: 72, "0xCOSTLY", intervalId: 11, lastExec: End.AddMinutes(-10), executions: 100, cpuUs: 500_000);
    }

    /// <summary>The cheap plan again, in an interval that last ran a minute before the end and was collected five minutes after
    /// it. A read that takes this row finds the cheap plan the newest one and reports no regression.</summary>
    private Task SeedTheCheapPlanCollectedAfterTheEndAsync()
        => SeedQueryStoreRowAsync(End.AddMinutes(5), queryId: 7, planId: 71, "0xCHEAP", intervalId: 12, lastExec: End.AddMinutes(-1), executions: 100, cpuUs: 100_000);

    private static string Figures(Fact fact)
        => string.Join(";", fact.Metadata.OrderBy(m => m.Key, StringComparer.Ordinal).Select(m => m.Key + "=" + m.Value.ToString("R", CultureInfo.InvariantCulture)));

    [Fact]
    public async Task ThePlanRegressionFact_IsTheSame_WithARowCollectedAfterTheWindowEnd()
    {
        await SeedARegressionAsync();
        var before = (await FactsAsync(End))["PLAN_REGRESSION"];

        await SeedTheCheapPlanCollectedAfterTheEndAsync();
        var after = (await FactsAsync(End))["PLAN_REGRESSION"];

        Assert.Equal(1, before.Metadata["offender_count"]);
        Assert.Equal(7, before.Metadata["worst_query_id"]);
        Assert.Equal(before.Value, after.Value);
        Assert.Equal(Figures(before), Figures(after));
    }

    /// <summary>The row collected five minutes after the end belongs to a window that ends there: the cheap plan is the
    /// newest one, so that window has no regression.</summary>
    [Fact]
    public async Task ThePlanRegressionFact_ReadsTheLaterRow_OnceTheWindowEndsAtItsCollection()
    {
        await SeedARegressionAsync();
        await SeedTheCheapPlanCollectedAfterTheEndAsync();

        Assert.DoesNotContain("PLAN_REGRESSION", (await FactsAsync(End.AddMinutes(5))).Keys);
    }

    private async Task<IReadOnlyList<PlanRegressionFailureRule.Failure>> PlanRegressionFailuresAsync(DateTime end)
    {
        var context = ContextFor(end);
        await SecondaryReplicaScope.EnsureAsync(_duckDb, context);
        await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context);
        return context.PlanRegressionFailures ?? [];
    }

    /// <summary>Query 9 timed out 100 times in an interval collected inside the window. An hour after the end the store gains
    /// an interval of 400 more, last run a minute before the end.</summary>
    [Fact]
    public async Task ThePlanRegressionFailures_AreTheSame_WithARowCollectedAfterTheWindowEnd()
    {
        await SeedQueryStoreRowAsync(End.AddMinutes(-5), queryId: 9, planId: 91, "0xTIMEOUT", intervalId: 21, lastExec: End.AddMinutes(-10), executions: 100, cpuUs: 1_000, outcome: "Aborted");
        var before = await PlanRegressionFailuresAsync(End);

        await SeedQueryStoreRowAsync(End.AddHours(1), queryId: 9, planId: 91, "0xTIMEOUT", intervalId: 22, lastExec: End.AddMinutes(-1), executions: 400, cpuUs: 1_000, outcome: "Aborted");
        var after = await PlanRegressionFailuresAsync(End);

        Assert.Equal(100L, Assert.Single(before).RecentTimeouts);
        Assert.Equal(before, after);
        Assert.Equal(500L, Assert.Single(await PlanRegressionFailuresAsync(End.AddHours(1))).RecentTimeouts);
    }

    /// <summary>The rows the drill-down of a finding on <paramref name="factKey"/> stores under <paramref name="drillDownKey"/>,
    /// as JSON text, for a pass whose window ends at <paramref name="end"/>.</summary>
    private async Task<List<string>> DrillDownRowsAsync(string factKey, string drillDownKey, DateTime end)
    {
        var context = ContextFor(end);
        await SecondaryReplicaScope.EnsureAsync(_duckDb, context);
        var finding = new AnalysisFinding { RootFactKey = factKey, StoryPath = factKey, PathKeys = [factKey], Severity = 1.0 };

        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], context);

        if (finding.DrillDown is null || !finding.DrillDown.TryGetValue(drillDownKey, out var raw))
            return [];
        return [.. JsonSerializer.SerializeToElement(raw).EnumerateArray().Select(row => row.GetRawText())];
    }

    [Fact]
    public async Task TheRegressedQueriesDrillDown_IsTheSame_WithARowCollectedAfterTheWindowEnd()
    {
        await SeedARegressionAsync();
        var before = await DrillDownRowsAsync("PLAN_REGRESSION", "regressed_queries", End);

        await SeedTheCheapPlanCollectedAfterTheEndAsync();
        var after = await DrillDownRowsAsync("PLAN_REGRESSION", "regressed_queries", End);

        Assert.Single(before);
        Assert.Equal(before, after);
        Assert.Empty(await DrillDownRowsAsync("PLAN_REGRESSION", "regressed_queries", End.AddMinutes(5)));
    }

    /// <summary>The capture a day before the end has auto shrink on in DbA; the capture an hour after the end has it off, and
    /// nothing else to report.</summary>
    [Fact]
    public async Task TheConfigIssuesDrillDown_IsTheSame_WithACaptureStoredAfterTheWindowEnd()
    {
        await SeedAsync(c => LatestValueSeed.InsertDatabaseConfigAsync(c, ServerId, End.AddDays(-1), "DbA", autoShrink: true, rcsiOn: true));
        var before = await DrillDownRowsAsync("DB_CONFIG", "config_issues", End);

        await SeedAsync(c => LatestValueSeed.InsertDatabaseConfigAsync(c, ServerId, End.AddHours(1), "DbA", autoShrink: false, rcsiOn: true));
        var after = await DrillDownRowsAsync("DB_CONFIG", "config_issues", End);

        Assert.Equal("DbA", JsonDocument.Parse(Assert.Single(before)).RootElement.GetProperty("database").GetString());
        Assert.Equal(before, after);
        Assert.Empty(await DrillDownRowsAsync("DB_CONFIG", "config_issues", End.AddHours(1)));
    }

    /// <summary>One capture of the server at <paramref name="at"/>: its properties row and its max degree of parallelism.</summary>
    private Task SeedServerCaptureAsync(DateTime at, int engineEdition, int cpuCount, long maxDop) => SeedAsync(async c =>
    {
        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = @"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition,
     cpu_count, hyperthread_ratio, physical_memory_mb, socket_count, cores_per_socket)
VALUES ($1, $2, $3, 'window-end', 'Enterprise Edition (64-bit)', '16.0.4000.1', 'RTM', $4, $5, 2, 65536, 1, 8)";
            foreach (var value in new object[] { _id--, at, ServerId, engineEdition, cpuCount })
                cmd.Parameters.Add(new DuckDBParameter { Value = value });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        using (var cmd = c.CreateCommand())
        {
            cmd.CommandText = @"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, 'window-end', 'max degree of parallelism', $4, $4, true, true)";
            foreach (var value in new object[] { _id--, at, ServerId, maxDop })
                cmd.Parameters.Add(new DuckDBParameter { Value = value });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    });

    /// <summary>An hour before the end the server is a boxed product with 8 CPUs at MAXDOP 4; the capture an hour after the end
    /// is an Azure SQL Database with 64 at MAXDOP 16.</summary>
    private async Task SeedTheServerBeforeTheEndAsync() => await SeedServerCaptureAsync(End.AddHours(-1), engineEdition: 3, cpuCount: 8, maxDop: 4);

    private async Task SeedTheServerAfterTheEndAsync() => await SeedServerCaptureAsync(End.AddHours(1), engineEdition: 5, cpuCount: 64, maxDop: 16);

    [Fact]
    public async Task ThePlanAnalysisServerMetadata_IsTheSame_WithACaptureStoredAfterTheWindowEnd()
    {
        var collector = new DrillDownCollector(_duckDb);
        await SeedTheServerBeforeTheEndAsync();
        var before = await collector.ReadServerMetadataForPlanAnalysisAsync(ServerId, End, TestContext.Current.CancellationToken);

        await SeedTheServerAfterTheEndAsync();
        var after = await collector.ReadServerMetadataForPlanAnalysisAsync(ServerId, End, TestContext.Current.CancellationToken);
        var later = await collector.ReadServerMetadataForPlanAnalysisAsync(ServerId, End.AddHours(1), TestContext.Current.CancellationToken);

        Assert.Equal((8, 4, 3), (before!.CpuCount, before.MaxDop, before.EngineEdition));
        Assert.Equal((before.CpuCount, before.MaxDop, before.EngineEdition), (after!.CpuCount, after.MaxDop, after.EngineEdition));
        Assert.Equal((64, 16, 5), (later!.CpuCount, later.MaxDop, later.EngineEdition));
    }

    /// <summary>One query ran at DOP 16 half an hour before the end, while the server's max degree of parallelism was 16. The
    /// capture an hour after the end has the setting at 4, which a plan at DOP 16 cannot have run under.</summary>
    [Fact]
    public async Task TheHighDopCount_IsTheSame_WithAMaxDopCaptureStoredAfterTheWindowEnd()
    {
        await SeedAsync(async c =>
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     max_dop, min_dop, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_spills, query_text)
VALUES ($1, $2, $3, 'window-end', 'SalesDb', '0xDOPHASH', '0xDOPPLAN', 16, 1, 10, 100000, 200000, 0, 'SELECT 1')";
            foreach (var value in new object[] { _id--, End.AddMinutes(-30), ServerId })
                cmd.Parameters.Add(new DuckDBParameter { Value = value });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        });
        await SeedServerCaptureAsync(End.AddHours(-1), engineEdition: 3, cpuCount: 8, maxDop: 16);
        var before = (await FactsAsync(End))["QUERY_HIGH_DOP"];

        await SeedServerCaptureAsync(End.AddHours(1), engineEdition: 3, cpuCount: 8, maxDop: 4);
        var after = await FactsAsync(End);

        Assert.Equal(1, before.Metadata["high_dop_query_count"]);
        Assert.Contains("QUERY_HIGH_DOP", after.Keys);
        Assert.Equal(Figures(before), Figures(after["QUERY_HIGH_DOP"]));
        Assert.DoesNotContain("QUERY_HIGH_DOP", (await FactsAsync(End.AddHours(1))).Keys);
    }

    private async Task<bool> IsAzureSqlDatabaseAsync(DateTime end)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return await AnomalyDetector.IsAzureSqlDatabaseAsync(connection, ContextFor(end));
    }

    [Fact]
    public async Task TheAzureEditionRead_IsTheSame_WithAPropertiesRowStoredAfterTheWindowEnd()
    {
        await SeedTheServerBeforeTheEndAsync();
        var before = await IsAzureSqlDatabaseAsync(End);

        await SeedTheServerAfterTheEndAsync();

        Assert.False(before);
        Assert.Equal(before, await IsAzureSqlDatabaseAsync(End));
        /* The later row is stamped an hour after the end: a window ending on that stamp leaves it to the next window. */
        Assert.False(await IsAzureSqlDatabaseAsync(End.AddHours(1)));
        Assert.True(await IsAzureSqlDatabaseAsync(End.AddHours(2)));
    }
}
