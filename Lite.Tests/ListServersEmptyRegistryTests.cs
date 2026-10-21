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
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6277: Lite's <c>list_servers</c> answers an empty server list with the <c>empty</c> status envelope, the same true
/// negative Darling answers a store with nothing registered with, instead of bare prose that a client cannot tell from
/// a refusal.
/// </summary>
public sealed class ListServersEmptyRegistryTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly SharedDuckDbFixture _fixture;
    private readonly string _configDir;

    public ListServersEmptyRegistryTests(SharedDuckDbFixture fixture)
    {
        _fixture = fixture;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-6277-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task NoServers_AnswersTheEmptyEnvelope()
    {
        var answer = await McpDiscoveryTools.ListServers(new ServerManager(_configDir), new LocalDataService(_fixture.DuckDb));

        using var doc = JsonDocument.Parse(answer);
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("No servers are configured.", doc.RootElement.GetProperty("message").GetString());
    }
}
