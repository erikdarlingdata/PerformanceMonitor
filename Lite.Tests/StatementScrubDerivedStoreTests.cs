/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Tasks;
using Darling.Tests;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4348, #5320 (lane R10, Layer 0b), Lite twin of Darling's <c>StatementScrubDerivedStoreTests</c>: a finding and an
/// alert built from a raw planted source row store the marker in DuckDB, and a clean row is stored byte for byte as
/// before. The shared helper's own cases (same instance on clean input, unreadable JSON) live in Darling's class and
/// cover both apps; here the two Lite writers are proven against a real DuckDB file.
/// </summary>
public sealed class StatementScrubDerivedStoreTests : IDisposable
{
    private const int ServerId = 532010;

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;

    public StatementScrubDerivedStoreTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private static AnalysisFinding Finding(long id, string hash, string story, Dictionary<string, object>? drillDown) => new()
    {
        FindingId = id,
        ServerId = ServerId,
        ServerName = "derived-scrub-finding",
        Category = "queries",
        StoryPath = "SCRUB_A -> SCRUB_B",
        StoryPathHash = hash,
        IncidentId = "inc-" + hash,
        Severity = 1.2,
        Confidence = StoryConfidence.Compute(1, 5, 2),
        FactCount = 2,
        RootFactKey = "SCRUB_A",
        RootFactValue = 1.0,
        AnalysisTime = DateTime.UtcNow,
        StoryText = story,
        DrillDown = drillDown,
    };

    private static Dictionary<string, object> DrillDown(string statement) => new()
    {
        ["top_queries"] = new[]
        {
            new Dictionary<string, object> { ["query_text"] = statement, ["executions"] = 7 },
        },
    };

    [Fact]
    public async Task AFindingBuiltFromARawPlantedRowStoresTheMarker()
    {
        await _duckDb.InitializeAsync();
        var store = new FindingStore(_duckDb);
        var plainDrill = DrillDown(StatementScrubCanary.PlainStatement);
        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = "derived-scrub-finding",
            TimeRangeStart = DateTime.UtcNow.AddHours(-4),
            TimeRangeEnd = DateTime.UtcNow,
        };

        await store.InsertFindingsAsync(new List<AnalysisFinding>
        {
            Finding(1, "rawcanary01", "Blocked on " + StatementScrubCanary.CanaryStatement, DrillDown(StatementScrubCanary.CanaryStatement)),
            Finding(2, "plaincanary01", "Waits rose on " + StatementScrubCanary.PlainStatement, plainDrill),
        }, context);

        var raw = await ReadFindingAsync("rawcanary01");
        Assert.Equal(SensitiveStatements.PlaceholderText, raw.Story);
        Assert.NotNull(raw.Drill);
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, raw.Drill, StringComparison.Ordinal);
        }
        Assert.Contains(SensitiveStatements.PlaceholderText, raw.Drill, StringComparison.Ordinal);
        using (JsonDocument.Parse(raw.Drill!)) { }

        /* The clean finding is stored byte for byte as the serializer wrote it. */
        var plain = await ReadFindingAsync("plaincanary01");
        Assert.Equal("Waits rose on " + StatementScrubCanary.PlainStatement, plain.Story);
        Assert.Equal(DrillDownSerializer.Serialize(plainDrill), plain.Drill);
    }

    private async Task<(string Story, string? Drill)> ReadFindingAsync(string hash)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT story_text, drill_down_json FROM analysis_findings WHERE server_id = $1 AND story_path_hash = $2";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = ServerId });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = hash });
        using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    [Fact]
    public async Task AnAlertBuiltFromARawPlantedRowStoresTheMarker()
    {
        await _duckDb.InitializeAsync();
        var store = new DuckDbAlertHistoryStore(_duckDb);
        var serverKey = ServerId.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var contextJson = JsonSerializer.Serialize(new { dedup = "abc123", query_text = StatementScrubCanary.CanaryStatement });
        var plainContext = JsonSerializer.Serialize(new { dedup = "abc124", query_text = StatementScrubCanary.PlainStatement });

        await store.RecordAlertAsync(new AlertHistoryRecord(
            serverKey, "derived-scrub-alert", "Scrub Raw", "1", "1", 1, 1, AlertDelivery.NoChannelApplies(), false,
            "Long query: " + StatementScrubCanary.CanaryStatement, contextJson));
        await store.RecordAlertAsync(new AlertHistoryRecord(
            serverKey, "derived-scrub-alert", "Scrub Plain", "1", "1", 1, 1, AlertDelivery.NoChannelApplies(), false,
            "Long query: " + StatementScrubCanary.PlainStatement, plainContext));

        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var read = connection.CreateCommand();
        read.CommandText = "SELECT metric_name, detail_text, context_json FROM config_alert_log WHERE server_id = $1";
        read.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = ServerId });
        var rows = new Dictionary<string, (string? Detail, string? Context)>();
        using (var reader = await read.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                rows[reader.GetString(0)] = (reader.IsDBNull(1) ? null : reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2));
            }
        }

        Assert.Equal(2, rows.Count);
        var raw = rows["Scrub Raw"];
        Assert.Equal(SensitiveStatements.PlaceholderText, raw.Detail);
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, raw.Context, StringComparison.Ordinal);
        }
        Assert.Contains(SensitiveStatements.PlaceholderText, raw.Context, StringComparison.Ordinal);
        Assert.Contains("abc123", raw.Context, StringComparison.Ordinal);

        var plain = rows["Scrub Plain"];
        Assert.Equal("Long query: " + StatementScrubCanary.PlainStatement, plain.Detail);
        Assert.Equal(plainContext, plain.Context);
    }

    /// <summary>The Lite writers call the helper; reverting either call fails this on the old shape.</summary>
    [Fact]
    public void TheLiteWritersCallTheHelper()
    {
        var finding = File.ReadAllText(RepoPath("Lite/Analysis/FindingStore.cs"));
        Assert.Contains("DerivedStoreScrub.Text(finding.StoryText)", finding, StringComparison.Ordinal);
        Assert.Contains("DerivedStoreScrub.Json(DrillDownSerializer.Serialize(finding.DrillDown))", finding, StringComparison.Ordinal);

        var alert = File.ReadAllText(RepoPath("Lite/Services/DuckDbAlertHistoryStore.cs"));
        Assert.Contains("DerivedStoreScrub.Text(record.DetailText)", alert, StringComparison.Ordinal);
        Assert.Contains("DerivedStoreScrub.Json(record.ContextJson)", alert, StringComparison.Ordinal);
    }

    private static string RepoPath(string relative, [CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", relative));
}
