/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests.Helpers;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6074: <c>get_alert_history</c> takes <c>metric_name</c> and <c>family</c>. Both apply in the SQL, ahead of the row cap,
/// so a filtered page is the newest rows OF THAT METRIC: the pins seed one old row of the wanted metric under a busier
/// stream of another and ask for a page smaller than the stream. A filter applied after the cap would return nothing.
/// </summary>
public sealed class AlertHistoryFilterTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly TestAlertDataHelper _helper;

    public AlertHistoryFilterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _helper = new TestAlertDataHelper(_dbPath);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* Best-effort cleanup */ }
    }

    private async Task<LocalDataService> SeedAsync(params (int MinutesAgo, string Metric)[] rows)
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            foreach (var (minutesAgo, metric) in rows)
            {
                await _helper.InsertLiveAlertAsync(connection, DateTime.UtcNow.AddMinutes(-minutesAgo), 1, "srv", metric);
            }
        }

        return new LocalDataService(initializer);
    }

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static string[] Metrics(JsonElement answer) =>
        answer.GetProperty("alerts").EnumerateArray().Select(a => a.GetProperty("metric_name").GetString()!).ToArray();

    [Fact]
    public async Task MetricName_IsAppliedBeforeTheRowCap_AndMatchesIgnoringCase()
    {
        /* Five newer Blocking rows and one older High CPU row: a limit of 2 holds only Blocking rows unless the filter runs first. */
        var service = await SeedAsync((1, "Blocking Detected"), (2, "Blocking Detected"), (3, "Blocking Detected"),
            (4, "Blocking Detected"), (5, "Blocking Detected"), (30, "High CPU"));

        var answer = Parse(await McpAlertTools.GetAlertHistory(service, 24, 2, metric_name: "high cpu"));

        Assert.Equal(new[] { "High CPU" }, Metrics(answer));
        Assert.False(answer.GetProperty("truncated").GetBoolean());
        Assert.Equal("high cpu", answer.GetProperty("metric_name_filter").GetString());
    }

    [Fact]
    public async Task Family_IsAppliedBeforeTheRowCap_AndFollowsTheCensus()
    {
        var service = await SeedAsync((1, "Blocking Detected"), (2, "Blocking Detected"), (3, "Blocking Detected"),
            (4, "Custom:abc"), (20, "Failed Agent Job"), (25, "Long-Running Job"), (40, "Collection Stopped"));

        var jobs = Parse(await McpAlertTools.GetAlertHistory(service, 24, 2, family: "Agent-Jobs"));
        Assert.Equal(new[] { "Failed Agent Job", "Long-Running Job" }, Metrics(jobs));
        Assert.False(jobs.GetProperty("truncated").GetBoolean());
        Assert.Equal("agent-jobs", jobs.GetProperty("family_filter").GetString());

        var self = Parse(await McpAlertTools.GetAlertHistory(service, 24, 5, family: "self-monitor"));
        Assert.Equal(new[] { "Collection Stopped" }, Metrics(self));

        /* The fall-through family also owns the names the census does not list, such as a custom alert. */
        var performance = Parse(await McpAlertTools.GetAlertHistory(service, 24, 50, family: "performance"));
        Assert.Equal(new[] { "Blocking Detected", "Blocking Detected", "Blocking Detected", "Custom:abc" }, Metrics(performance));
    }

    [Fact]
    public async Task AnUnknownFamilyOrMetric_IsAnEmptyAnswerThatNamesTheFilter_NotAnError()
    {
        var service = await SeedAsync((1, "High CPU"));

        var badFamily = Parse(await McpAlertTools.GetAlertHistory(service, 24, 10, family: "nonsense"));
        Assert.Equal("empty", badFamily.GetProperty("status").GetString());
        Assert.Contains("family 'nonsense'", badFamily.GetProperty("message").GetString(), StringComparison.Ordinal);

        var badMetric = Parse(await McpAlertTools.GetAlertHistory(service, 24, 10, metric_name: "No Such Alert"));
        Assert.Equal("empty", badMetric.GetProperty("status").GetString());
        Assert.Contains("metric_name 'No Such Alert'", badMetric.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheFamilyPredicate_AgreesWithAlertFamilyOf_ForEveryCensusNameAndPrefixShape()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        var names = AlertFamily.MetricFamilies.Keys.Concat(AlertFamily.RecoveryPairs.Keys)
            .Concat(new[] { "Custom:1", "External:src:key", "Analysis: cpu_pressure [ab12cd34]", "Some Uncensused Alert" })
            .Distinct().ToList();

        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var family in AlertFamily.All)
        {
            var predicate = AlertFamily.SqlFamilyPredicate("n", family);
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT n FROM (VALUES {string.Join(", ", names.Select(n => "('" + n.Replace("'", "''") + "')"))}) t(n) WHERE {predicate}";
            var matched = new System.Collections.Generic.List<string>();
            using (var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
            {
                while (await reader.ReadAsync(TestContext.Current.CancellationToken)) matched.Add(reader.GetString(0));
            }

            Assert.Equal(names.Where(n => AlertFamily.Of(n) == family).OrderBy(n => n, StringComparer.Ordinal),
                matched.OrderBy(n => n, StringComparer.Ordinal));
        }
    }
}
