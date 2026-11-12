/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8572 row 1, Lite's half: no default MCP reply carries a number with more than six decimals (48.06999999999999, 0.32255504310332606).
/// The reads come from the tool catalog itself: every <c>[McpServerTool]</c> method in the Lite assembly, found by reflection, so a read
/// added tomorrow is scanned without this test being edited. Each is called with its own parameter defaults and only the server name, over
/// a store seeded with blocked-process reports and query samples whose averages and rates do not divide evenly. A read whose parameters
/// cannot be bound from defaults (a required query hash, a plan id) is not driven here: its refusal carries no figure. The floor test holds
/// how many reads answered with figures, so the scan cannot go quiet.
/// </summary>
public sealed class McpReplyFloatNoiseTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "NoiseServer";
    private const int MaxDecimals = 6;

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -7_000_000;
    private DuckDBConnection? _seedConn;

    public McpReplyFloatNoiseTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _tempDir = Path.Combine(Path.GetTempPath(), "McpFloatNoise_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        _dataService = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private static List<string> NoisyNumbers(string reply)
    {
        var found = new List<string>();
        JsonDocument document;
        try { document = JsonDocument.Parse(reply); }
        catch (JsonException) { return found; }

        using (document)
        {
            Walk(document.RootElement, "$", found);
        }

        return found;
    }

    private static void Walk(JsonElement element, string path, List<string> found)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Number:
                var raw = element.GetRawText();
                var dot = raw.IndexOf('.');
                if (dot >= 0)
                {
                    var end = raw.IndexOfAny(['e', 'E'], dot);
                    if ((end < 0 ? raw.Length : end) - dot - 1 > MaxDecimals)
                    {
                        found.Add(path + " = " + raw);
                    }
                }

                break;
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    Walk(property.Value, path + "." + property.Name, found);
                }

                break;
            case JsonValueKind.Array:
                var index = 0;
                foreach (var item in element.EnumerateArray())
                {
                    Walk(item, path + "[" + (index++).ToString(CultureInfo.InvariantCulture) + "]", found);
                }

                break;
        }
    }

    [Fact]
    public void TheScan_FlagsNoise_AndPassesRoundedFiguresAndTimestamps()
    {
        Assert.Equal(["$.avg_cpu_ms = 1185.150596030991"], NoisyNumbers("{\"avg_cpu_ms\":1185.150596030991,\"b\":0.123456,\"t\":\"2026-11-11T06:03:49.1234567Z\"}"));
        Assert.Empty(NoisyNumbers("not json 0.123456789"));
    }

    private async Task<List<(string Read, string Reply)>> DriveEveryReadAsync()
    {
        var now = DateTime.UtcNow;
        for (var i = 0; i < 3; i++)
        {
            await SeedBlockedProcessReportAsync(now.AddMinutes(-10 * (i + 1)), 50 + i);
        }

        await ExecAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, query_text,
     execution_count, total_elapsed_time, total_worker_time, total_logical_reads, total_logical_writes, total_physical_reads,
     delta_execution_count, delta_elapsed_time, delta_worker_time, delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills)
VALUES ($1, $2, $3, $4, 'appdb', '0xAVG', '0xPLAN', 'SELECT Averages FROM Numbers',
        3, 2000011, 1000007, 100, 0, 0, 3, 2000011, 1000007, 100, 0, 0, 3, 0)",
            _nextId--, Naive(now.AddMinutes(-9)), _serverId, ServerName);

        var replies = new List<(string, string)>();
        var tools = typeof(McpQueryTools).Assembly.GetTypes()
            .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Static))
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .OrderBy(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name, StringComparer.Ordinal);
        foreach (var method in tools)
        {
            var args = new List<object?>();
            var bound = true;
            foreach (var parameter in method.GetParameters())
            {
                if (parameter.ParameterType == typeof(LocalDataService)) args.Add(_dataService);
                else if (parameter.ParameterType == typeof(ServerManager)) args.Add(_serverManager);
                else if (parameter.Name == "server_name") args.Add(ServerName);
                else if (parameter.HasDefaultValue) args.Add(parameter.DefaultValue);
                else { bound = false; break; }
            }

            if (!bound)
            {
                continue;
            }

            try
            {
                var task = (Task)method.Invoke(null, args.ToArray())!;
                await task;
                var reply = (string?)task.GetType().GetProperty("Result")?.GetValue(task) ?? string.Empty;
                replies.Add((method.GetCustomAttribute<McpServerToolAttribute>()!.Name!, reply));
            }
            catch (Exception)
            {
                /* A read that needs a service this bare call does not carry is not driven here. */
            }
        }

        return replies;
    }

    [Fact]
    public async Task EveryReadsDefaultReply_CarriesNoNumberWithMoreThanSixDecimals()
    {
        var replies = await DriveEveryReadAsync();
        var noisy = replies.SelectMany(r => NoisyNumbers(r.Reply).Select(n => $"{r.Read}: {n}")).ToList();
        Assert.True(noisy.Count == 0, "Replies carry a number with more than " + MaxDecimals + " decimals. Round it where the reply is built:\n" + string.Join("\n", noisy.Take(40)));
        Assert.True(replies.Count(r => r.Reply.StartsWith('{')) >= 40, "Fewer than 40 reads answered a default call: the scan is no longer reaching the catalog.");
    }

    /// <summary>#8572 rows 2, 3 and 11: no default reply says an em dash, a tracker reference or a count of four or more digits without its
    /// thousands separators in its own prose. The reads are the same ones the float scan drives.</summary>
    [Fact]
    public async Task EveryReadsDefaultReply_CarriesNoEmDashTrackerNumberOrUngroupedCount()
    {
        var replies = await DriveEveryReadAsync();
        var hits = replies.SelectMany(r => ReplyTextScan.Hits(r.Reply).Select(h => $"{r.Read}: {h}")).ToList();
        Assert.True(hits.Count == 0, "Replies break the reply-text rules (no U+2014, no tracker number, grouped counts):\n" + string.Join("\n", hits.Take(200)));
    }

    [Fact]
    public void TheTextScan_FlagsEachRule_AndPassesDataThatIsNotACount()
    {
        Assert.Equal(3, ReplyTextScan.Hits("{\"a\":\"one \u2014 two\",\"b\":\"retired by #3593\",\"c\":\"saw 1438 captures\"}").Count);
        /* Grouped counts, ids in a token, dates, times, decimals, hashes and the text of a captured statement are not defects. */
        Assert.Empty(ReplyTextScan.Hits("{\"a\":\"saw 1,438 captures, 12.5 ms, 2026-11-11T06:03:49Z, 0x1234ABCD, port:1433, v3.12.0\",\"query_text\":\"SELECT 12345 -- #12 \u2014\"}"));
        Assert.Single(ReplyTextScan.Hits("plain text reply with 443230195 block accesses"));
    }

    private async Task ExecAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
            cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private Task SeedBlockedProcessReportAsync(DateTime at, int blockedSpid) => ExecAsync(@"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, blocked_process_report_xml, contentious_object)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
        _nextId--, Naive(at), _serverId, ServerName, Naive(at), "Db", blockedSpid, 90, 8000L, "X", "SELECT 1", "UPDATE T SET x = 1",
        "<blocked-process-report><blocked-process><process spid=\"55\"/></blocked-process></blocked-process-report>", "dbo.T");
}
