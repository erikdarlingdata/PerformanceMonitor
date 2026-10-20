/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6180: <c>get_perfmon_stats</c> (545 counters measured at 82 KB) and <c>get_blocked_process_xml</c> (3 reports measured at
/// 54 KB) answered over the shared 32 KB response target at DEFAULT arguments with no note. Each now keeps what fits, whole,
/// and says what it left out and how to read it. Local DuckDB store, no rig.
/// </summary>
public sealed class McpResponseBudgetPerfmonAndBlockedXmlToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "Budget6180Srv";

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public McpResponseBudgetPerfmonAndBlockedXmlToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-budget6180-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, IsEnabled = true };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task PerfmonStats_DefaultCall_WithHundredsOfCounters_FitsAndSaysWhatItLeftOff()
    {
        var service = new LocalDataService(_duckDb);
        var at = DateTime.UtcNow.AddMinutes(-2);
        for (var i = 0; i < 545; i++)
        {
            await SeedPerfmonAsync(at, "SQLServer:Databases", $"Log Flushes/sec {i}", "UserDb" + (i % 60), 1000 + i);
        }

        var json = await McpPerfmonTools.GetPerfmonStats(service, _serverManager, ServerName);
        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
            $"default get_perfmon_stats was {Encoding.UTF8.GetByteCount(json):N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        var returned = root.GetProperty("counters").GetArrayLength();
        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(545, root.GetProperty("counters_total").GetInt32());
        Assert.Equal(returned, root.GetProperty("counters_returned").GetInt32());
        Assert.InRange(returned, 1, 544);
        Assert.Contains("counter_name", root.GetProperty("size_note").GetString(), StringComparison.Ordinal);

        /* The note's own advice works: a filter that narrows to a handful is served whole and not marked cut. */
        using var narrow = JsonDocument.Parse(await McpPerfmonTools.GetPerfmonStats(service, _serverManager, ServerName, counter_name: "Log Flushes/sec 7", instance_name: "UserDb7"));
        Assert.False(narrow.RootElement.GetProperty("truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, narrow.RootElement.GetProperty("size_note").ValueKind);
        Assert.True(narrow.RootElement.GetProperty("counters").GetArrayLength() >= 1);
    }

    [Fact]
    public async Task BlockedProcessXml_DefaultCall_KeepsWholeReportsThatFit_AndListsTheRest()
    {
        var service = new LocalDataService(_duckDb);
        var bigXml = BuildReportXml(18_000);
        var newest = DateTime.UtcNow.AddMinutes(-5);
        for (var i = 0; i < 5; i++)
        {
            await SeedBlockedReportAsync(newest.AddMinutes(-i * 10), 100 + i, bigXml);
        }

        var json = await McpBlockingTools.GetBlockedProcessXml(service, _serverManager, ServerName);
        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
            $"default get_blocked_process_xml was {Encoding.UTF8.GetByteCount(json):N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        var returned = root.GetProperty("reports").GetArrayLength();
        Assert.InRange(returned, 1, 4);
        Assert.Equal(returned, root.GetProperty("reports_returned").GetInt32());
        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(5 - returned, root.GetProperty("left_out_count").GetInt32());
        Assert.Equal(5 - returned, root.GetProperty("left_out_reports").GetArrayLength());
        Assert.Contains("limit 1", root.GetProperty("size_note").GetString(), StringComparison.Ordinal);

        /* Every report returned is a whole, well-formed document: never cut mid-way. */
        foreach (var report in root.GetProperty("reports").EnumerateArray())
        {
            var xml = report.GetProperty("blocked_process_report_xml").GetString()!;
            Assert.Equal(bigXml, xml);
            System.Xml.Linq.XDocument.Parse(xml);
        }

        /* A left-out report is fetched by itself with its event_time as as_of and limit 1. */
        var leftOut = root.GetProperty("left_out_reports")[0];
        var eventTime = leftOut.GetProperty("event_time").GetString()!;
        using var one = JsonDocument.Parse(await McpBlockingTools.GetBlockedProcessXml(
            service, _serverManager, ServerName, hours_back: 24, limit: 1, as_of: eventTime));
        Assert.Equal(1, one.RootElement.GetProperty("reports").GetArrayLength());
        Assert.Equal(leftOut.GetProperty("blocked_spid").GetInt32(), one.RootElement.GetProperty("reports")[0].GetProperty("blocked_spid").GetInt32());
    }

    private static string BuildReportXml(int approxLength)
    {
        var sb = new StringBuilder("<blocked-process-report monitorLoop=\"1\"><blocked-process><process id=\"p1\"><inputbuf>");
        var i = 0;
        while (sb.Length < approxLength)
        {
            sb.Append($"SELECT Id, Title FROM dbo.Posts WHERE ParentId = {i} AND Score > 0;\n");
            i++;
        }

        return sb.Append("</inputbuf></process></blocked-process><blocking-process><process id=\"p2\"/></blocking-process></blocked-process-report>").ToString();
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedPerfmonAsync(DateTime timeUtc, string objectName, string counter, string instance, long value)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO perfmon_stats
    (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name,
     cntr_value, delta_cntr_value, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(timeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = objectName });
        cmd.Parameters.Add(new DuckDBParameter { Value = counter });
        cmd.Parameters.Add(new DuckDBParameter { Value = instance });
        cmd.Parameters.Add(new DuckDBParameter { Value = value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 60L });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedBlockedReportAsync(DateTime eventTimeUtc, int blockedSpid, string xml)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, blocked_process_report_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        var t = DateTime.SpecifyKind(eventTimeUtc, DateTimeKind.Unspecified);
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = t });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = t });
        cmd.Parameters.Add(new DuckDBParameter { Value = "StackOverflow" });
        cmd.Parameters.Add(new DuckDBParameter { Value = blockedSpid });
        cmd.Parameters.Add(new DuckDBParameter { Value = 51 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = xml });
        await cmd.ExecuteNonQueryAsync();
    }
}
