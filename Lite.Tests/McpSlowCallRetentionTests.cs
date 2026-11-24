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
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9423: the mcp_slow_calls table is bounded only by the trim that <c>RecordMcpSlowCallAsync</c> runs after each insert: rows older than
/// the retention are dropped, and so is anything past the newest row cap. The rows are seeded in bulk, then one more goes through the real
/// write path, and the table is read back.
/// </summary>
public sealed class McpSlowCallRetentionTests : IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _initializer;
    private readonly LocalDataService _data;

    public McpSlowCallRetentionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _initializer = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _data = new LocalDataService(_initializer);
    }

    public void Dispose()
    {
        _initializer.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* Best-effort cleanup */ }
    }

    [Fact]
    public async Task TheWrite_DropsRowsPastTheRetention_AndKeepsTheNewestRowCap()
    {
        await _initializer.InitializeFromTemplateAsync();
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        const int recent = LocalDataService.McpSlowCallMaxRows + 100;

        using (var seed = await _data.OpenWriteConnectionAsync())
        {
            /* total_ms numbers the recent rows 0 (newest) to recent-1 (oldest), a minute or more back. The old rows sit past the retention. */
            using var recentRows = seed.CreateCommand();
            recentRows.CommandText = @"
INSERT INTO mcp_slow_calls (call_time, tool, outcome, total_ms, response_bytes)
SELECT $1::TIMESTAMP - to_seconds(60 + i), 'recent', 'ok', i, 1 FROM range($2) t(i)";
            recentRows.Parameters.Add(new DuckDBParameter { Value = now });
            recentRows.Parameters.Add(new DuckDBParameter { Value = recent });
            await recentRows.ExecuteNonQueryAsync();

            using var oldRows = seed.CreateCommand();
            oldRows.CommandText = @"
INSERT INTO mcp_slow_calls (call_time, tool, outcome, total_ms, response_bytes)
SELECT $1::TIMESTAMP - to_days($2) - to_seconds(i), 'old', 'ok', 5000, 1 FROM range(5) t(i)";
            oldRows.Parameters.Add(new DuckDBParameter { Value = now });
            oldRows.Parameters.Add(new DuckDBParameter { Value = LocalDataService.McpSlowCallRetentionDays + 1 });
            await oldRows.ExecuteNonQueryAsync();
        }

        Assert.Equal(recent + 5, await CountAsync("1 = 1"));

        await _data.RecordMcpSlowCallAsync(new McpSlowCall(DateTime.UtcNow, "newest", "ok", 1, null, null, null, null, 1, null, null, null, null, null, null));

        Assert.Equal(0, await CountAsync("tool = 'old'"));
        Assert.Equal(LocalDataService.McpSlowCallMaxRows, await CountAsync("1 = 1"));
        Assert.Equal(1, await CountAsync("tool = 'newest'"));
        /* The newest row and the newest cap-1 recent rows stay (total_ms 0 to cap-2), the older recent rows go. */
        Assert.Equal(LocalDataService.McpSlowCallMaxRows - 1, await CountAsync("tool = 'recent'"));
        Assert.Equal(LocalDataService.McpSlowCallMaxRows - 2, await MaxTotalMsAsync("recent"));
    }

    private async Task<long> CountAsync(string where)
    {
        using var connection = await _data.OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT count(*) FROM mcp_slow_calls WHERE " + where;
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }

    private async Task<long> MaxTotalMsAsync(string tool)
    {
        using var connection = await _data.OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT max(total_ms) FROM mcp_slow_calls WHERE tool = '" + tool + "'";
        return Convert.ToInt64(await command.ExecuteScalarAsync());
    }
}
