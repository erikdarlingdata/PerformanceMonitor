/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6831: the by-tag window says what a section's missing source is in words ("This store has no query stats data yet."), as the Darling
/// Viewer does, not the view's own name. The test drops the v_query_stats view from a freshly initialized store, so the plan cache section
/// is unavailable, as it is in a store whose view was never created.
/// </summary>
public sealed class LiteQueryTagMissingSourceTests : IDisposable
{
    private readonly string _tempDir;

    public LiteQueryTagMissingSourceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteQueryTagMissingSourceTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task AMissingSource_IsSaidInWords_NotAsTheViewsName()
    {
        using var initializer = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        await initializer.InitializeFromTemplateAsync();
        var now = DateTime.UtcNow;
        using (var connection = initializer.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "DROP VIEW IF EXISTS v_query_stats";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var data = await new LocalDataService(initializer).GetQueryTagRollupDataAsync([1], now.AddHours(-2), now);

        Assert.True(data.Unavailable.TryGetValue(QueryTagRollupSection.PlanCache.Name, out var planCache), "the plan cache section was served from a store with no v_query_stats");
        Assert.Equal("This store has no query stats data yet.", planCache);
        foreach (var (section, reason) in data.Unavailable)
        {
            Assert.DoesNotContain("v_", reason, StringComparison.Ordinal);
            Assert.DoesNotContain("does not exist", reason, StringComparison.OrdinalIgnoreCase);
            Assert.StartsWith("This store has no ", reason, StringComparison.Ordinal);
            Assert.EndsWith(" data yet.", reason, StringComparison.Ordinal);
        }

        var view = QueryTagUsageView.Build(data, [(1, "s")], allServers: false, tagFilter: null);
        Assert.Contains("plan_cache is unavailable: This store has no query stats data yet.", view.Message, StringComparison.Ordinal);
    }

    /// <summary>#6850: every section, with its source dropped, says the same sentence the Darling Viewer says for it (the words, never the view or table name).</summary>
    [Fact]
    public async Task EverySection_WithItsSourceMissing_SaysTheSameWordsAsTheViewer()
    {
        using var initializer = new DuckDbInitializer(Path.Combine(_tempDir, "every.duckdb"));
        await initializer.InitializeFromTemplateAsync();
        var now = DateTime.UtcNow;
        using (var connection = initializer.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            foreach (var drop in new[]
            {
                "DROP VIEW IF EXISTS v_query_stats", "DROP VIEW IF EXISTS v_query_store_stats", "DROP VIEW IF EXISTS v_procedure_stats",
                "DROP VIEW IF EXISTS v_dmv_blocking_snapshots", "DROP TABLE IF EXISTS blocked_process_reports", "DROP TABLE IF EXISTS deadlocks",
            })
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = drop;
                await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }
        }

        var data = await new LocalDataService(initializer).GetQueryTagRollupDataAsync([1], now.AddHours(-2), now);

        foreach (var (section, sentence) in LiteQueryTagSourceWordsTests.Expected)
        {
            Assert.True(data.Unavailable.TryGetValue(section, out var reason), $"section {section} was served with its source dropped");
            Assert.Equal(sentence, reason);
        }

        Assert.Equal(LiteQueryTagSourceWordsTests.Expected.Count, data.Unavailable.Count);
    }
}
