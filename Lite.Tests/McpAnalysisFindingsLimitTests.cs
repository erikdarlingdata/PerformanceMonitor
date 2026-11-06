/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7901: <c>get_analysis_findings</c> validates <c>limit</c> the way every other paged tool does
/// (<see cref="McpHelpers.ValidateTop(int, string)"/>): zero, a negative and a value over the shared
/// ceiling each answer the <c>invalid</c> envelope naming <c>limit</c>, not a data payload with an empty
/// page and a note saying "only the top 0 are returned". A limit inside the range still answers as before.
/// Darling's twin is <c>DarlingMcpAnalysisFindingsLimitLiveTests</c>.
/// </summary>
public sealed class McpAnalysisFindingsLimitTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "FindingsLimitServer";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;

    public McpAnalysisFindingsLimitTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "McpFindingsLimitTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        _serverManager = new ServerManager(configDir);
        _serverManager.AddServer(new ServerConnection { ServerName = ServerName, DisplayName = ServerName });
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(McpHelpers.MaxTop + 1)]
    public async Task Limit_OutsideTheSharedRange_IsRefusedByName(int limit)
    {
        var json = await McpAnalysisTools.GetAnalysisFindings(new AnalysisService(_duckDb), _serverManager, ServerName, 24, limit: limit);

        Assert.True(McpHelpers.IsRefusalEnvelope(json), $"limit {limit} answered a data payload instead of the invalid envelope: {json}");
        using var doc = JsonDocument.Parse(json);
        Assert.Equal("limit", doc.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(McpHelpers.MaxTop)]
    public async Task Limit_InsideTheSharedRange_StillAnswers(int limit)
    {
        var json = await McpAnalysisTools.GetAnalysisFindings(new AnalysisService(_duckDb), _serverManager, ServerName, 24, limit: limit);

        Assert.False(McpHelpers.IsRefusalEnvelope(json), $"limit {limit} was refused: {json}");
        using var doc = JsonDocument.Parse(json);
        /* No finding is planted, so the window's answer is the shared empty envelope. */
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
    }
}
