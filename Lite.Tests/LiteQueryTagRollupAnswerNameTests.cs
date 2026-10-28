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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6860: <c>get_resource_use_by_tag</c> answers with a server's display name (#6179), the name the caller passes back as
/// <c>server_name</c>, in the envelope and in every per-server block of the fleet view, never the storage key.
/// </summary>
public sealed class LiteQueryTagRollupAnswerNameTests : IDisposable
{
    private const string KeyOne = "srv-one";
    private const string NameOne = "Orders Reporting";
    private const string KeyTwo = "srv-two";
    private const string NameTwo = "Billing Warehouse";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private readonly ServerManager _serverManager;

    public LiteQueryTagRollupAnswerNameTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "QueryTagAnswerName_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "answername.duckdb"));
        _duckDb.InitializeAsync().GetAwaiter().GetResult();
        _service = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(_tempDir);
        _serverManager.AddServer(new ServerConnection { ServerName = KeyOne, DisplayName = NameOne });
        _serverManager.AddServer(new ServerConnection { ServerName = KeyTwo, DisplayName = NameTwo });
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task OneServer_IsAnsweredByItsDisplayName_NotItsStorageKey()
    {
        var json = await McpQueryTagUsageTools.GetResourceUseByTag(_service, _serverManager, NameOne, 24);
        using var doc = JsonDocument.Parse(json);

        Assert.Equal(NameOne, doc.RootElement.GetProperty("server").GetString());
        Assert.DoesNotContain(KeyOne, json, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheFleetView_NamesEveryPerServerBlockByDisplayName()
    {
        var json = await McpQueryTagUsageTools.GetResourceUseByTag(_service, _serverManager, null, 24);
        using var doc = JsonDocument.Parse(json);

        var names = doc.RootElement.GetProperty("sections").EnumerateArray()
            .SelectMany(s => s.GetProperty("servers").EnumerateArray().Select(b => b.GetProperty("server").GetString()!))
            .ToList();

        Assert.Equal(12, names.Count);
        Assert.Equal(6, names.Count(n => n == NameOne));
        Assert.Equal(6, names.Count(n => n == NameTwo));
        Assert.DoesNotContain(KeyOne, json, StringComparison.Ordinal);
        Assert.DoesNotContain(KeyTwo, json, StringComparison.Ordinal);
    }
}
