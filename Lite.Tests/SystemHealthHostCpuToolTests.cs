/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7416 item 1: SQL Server on Linux reports the host's idle CPU as 0 on some builds, so the system_health SYSTEM component's
/// <c>systemCpuUtilization</c> reads 100 on every row while the CPU tab reads 0 to 1 percent. The read decides from the stored
/// host platform (<c>server_properties.host_os_version</c>), not from the value, so a real 100 on a Windows host still shows.
/// Run through the MCP tool and through <see cref="LocalDataService.GetSystemHealthAsync"/>, which feeds the desktop CPU chart.
/// Darling twin: <c>DarlingMcpHealthParserToolsHostCpuLiveTests</c>.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class SystemHealthHostCpuToolTests : IDisposable
{
    private const string ServerName = "HostCpuServer";
    private const string AsOf = "2026-09-10T12:00:00Z";
    private static readonly DateTime Anchor = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);

    private readonly int _serverId;
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly PendingSeedSession _seed;
    private readonly ServerManager _serverManager;

    public SystemHealthHostCpuToolTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "SystemHealthHostCpu_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "config"));
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _seed = new PendingSeedSession(_duckDb);

        _serverManager = new ServerManager(Path.Combine(_tempDir, "config"));
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seed.Dispose();
        _duckDb.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private static DateTime Naive(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    private async Task SeedAsync(string? hostOsVersion)
    {
        await _duckDb.InitializeFromTemplateAsync();
        var xml = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", "sp_server_diagnostics_system.xml"));
        var eventTime = Anchor.AddHours(-2);
        await _seed.ExecuteAsync(@"
INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7)",
            new object?[] { 1L, Naive(eventTime), _serverId, ServerName, Naive(eventTime), SystemHealthParser.SpServerDiagnosticsEvent, xml });
        if (hostOsVersion is not null)
        {
            await _seed.ExecuteAsync(@"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition,
     cpu_count, hyperthread_ratio, physical_memory_mb, host_os_version)
VALUES ($1, $2, $3, $4, 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, $5)",
                new object?[] { 1L, Naive(eventTime), _serverId, ServerName, hostOsVersion });
        }

        _seed.Flush();
    }

    private LocalDataService Service() => new(_duckDb);

    /// <summary>#7688: one scheduler-monitor sample 2 hours before the anchor with the given SQL CPU and idle (memory healthy at 100).</summary>
    private async Task SeedSchedulerSampleAsync(string? hostOsVersion, int sqlCpu, int idle)
    {
        await _duckDb.InitializeFromTemplateAsync();
        var eventTime = Anchor.AddHours(-2);
        var xml = $@"<event name=""scheduler_monitor_system_health_ring_buffer_recorded"" package=""sqlos"" timestamp=""{eventTime:yyyy-MM-dd'T'HH:mm:ss.fff}Z"">
  <data name=""process_utilization""><type name=""uint32"" package=""package0""/><value>{sqlCpu}</value></data>
  <data name=""system_idle""><type name=""uint32"" package=""package0""/><value>{idle}</value></data>
  <data name=""page_faults""><type name=""uint32"" package=""package0""/><value>505</value></data>
  <data name=""working_set_delta""><type name=""int64"" package=""package0""/><value>4096</value></data>
  <data name=""memory_utilization""><type name=""uint32"" package=""package0""/><value>100</value></data>
</event>";
        await _seed.ExecuteAsync(@"
INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7)",
            new object?[] { 1L, Naive(eventTime), _serverId, ServerName, Naive(eventTime), SystemHealthParser.SchedulerMonitorEvent, xml });
        if (hostOsVersion is not null)
        {
            await _seed.ExecuteAsync(@"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition,
     cpu_count, hyperthread_ratio, physical_memory_mb, host_os_version)
VALUES ($1, $2, $3, $4, 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, $5)",
                new object?[] { 1L, Naive(eventTime), _serverId, ServerName, hostOsVersion });
        }

        _seed.Flush();
    }

    /* #7688: SQL Server on Linux reports idle as 0 on some builds, so "100 - SQL CPU - idle" read 100 minus the SQL figure on
       every quiet minute and every sample was listed as a scheduler issue. */
    [Fact]
    public async Task OnLinux_QuietSchedulerSamplesAreNotIssues_AndTheAnswerSaysWhy()
    {
        await SeedSchedulerSampleAsync("Ubuntu", sqlCpu: 0, idle: 0);

        var root = JsonDocument.Parse(await McpHealthParserTools.GetSchedulerIssues(Service(), _serverManager, ServerName, 24, 50, AsOf)).RootElement;

        Assert.False(root.TryGetProperty("issues", out _));
        Assert.Contains("Linux", root.GetProperty("system_cpu_note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnLinux_ABusySqlSampleIsStillAnIssue_WithNoOtherProcessOrIdleFigure()
    {
        await SeedSchedulerSampleAsync("Ubuntu", sqlCpu: 94, idle: 0);

        var root = JsonDocument.Parse(await McpHealthParserTools.GetSchedulerIssues(Service(), _serverManager, ServerName, 24, 50, AsOf)).RootElement;

        var issue = Assert.Single(root.GetProperty("issues").EnumerateArray());
        Assert.Equal(94, issue.GetProperty("sql_cpu_utilization").GetInt64());
        Assert.Equal(JsonValueKind.Null, issue.GetProperty("other_process_cpu").ValueKind);
        Assert.Equal(JsonValueKind.Null, issue.GetProperty("system_idle").ValueKind);
        Assert.Contains("Linux", root.GetProperty("system_cpu_note").GetString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Windows")]
    [InlineData(null)]
    public async Task OnWindowsOrAnUnknownPlatform_AHighOtherProcessSampleIsStillAnIssue(string? hostOs)
    {
        await SeedSchedulerSampleAsync(hostOs, sqlCpu: 5, idle: 0);

        var root = JsonDocument.Parse(await McpHealthParserTools.GetSchedulerIssues(Service(), _serverManager, ServerName, 24, 50, AsOf)).RootElement;

        var issue = Assert.Single(root.GetProperty("issues").EnumerateArray());
        Assert.Equal(95, issue.GetProperty("other_process_cpu").GetInt64());
        Assert.Equal(0, issue.GetProperty("system_idle").GetInt64());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("system_cpu_note").ValueKind);
    }

    [Fact]
    public async Task OnLinux_TheDesktopSchedulerGridRead_ListsNoQuietMinute()
    {
        await SeedSchedulerSampleAsync("Ubuntu", sqlCpu: 0, idle: 0);

        Assert.Empty(await Service().GetSchedulerIssuesAsync(_serverId, 24, asOfUtc: Anchor));
    }

    [Fact]
    public async Task OnLinux_TheDesktopSchedulerGridRead_CarriesNoOtherProcessOrIdle()
    {
        await SeedSchedulerSampleAsync("Ubuntu", sqlCpu: 94, idle: 0);

        var rows = await Service().GetSchedulerIssuesAsync(_serverId, 24, asOfUtc: Anchor);

        var row = Assert.Single(rows);
        Assert.Null(row.OtherProcessCpu);
        Assert.Null(row.SystemIdle);
        Assert.Equal(94, row.SqlCpuUtilization);
    }

    [Theory]
    [InlineData("Ubuntu")]
    [InlineData("Red Hat Enterprise Linux")]
    [InlineData("Linux (Ubuntu 22.04.3 LTS) <X64>")]
    public async Task OnLinux_TheToolReturnsNoSystemCpuAndSaysWhy(string hostOs)
    {
        await SeedAsync(hostOs);

        var root = JsonDocument.Parse(await McpHealthParserTools.GetSystemHealth(Service(), _serverManager, ServerName, 24, 50, AsOf)).RootElement;

        var entry = root.GetProperty("entries")[0];
        Assert.Equal(JsonValueKind.Null, entry.GetProperty("system_cpu_utilization").ValueKind);
        Assert.Equal(40, entry.GetProperty("sql_cpu_utilization").GetInt64());
        Assert.Contains("Linux", root.GetProperty("system_cpu_note").GetString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Windows")]
    [InlineData("Windows Server 2022 Datacenter 10.0 <X64>")]
    [InlineData(null)]
    public async Task OnWindowsOrAnUnknownPlatform_TheFigureIsKept(string? hostOs)
    {
        await SeedAsync(hostOs);

        var root = JsonDocument.Parse(await McpHealthParserTools.GetSystemHealth(Service(), _serverManager, ServerName, 24, 50, AsOf)).RootElement;

        Assert.Equal(65, root.GetProperty("entries")[0].GetProperty("system_cpu_utilization").GetInt64());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("system_cpu_note").ValueKind);
    }

    [Fact]
    public async Task OnLinux_TheDesktopChartRead_CarriesNoSystemCpu()
    {
        await SeedAsync("Ubuntu");

        var rows = await Service().GetSystemHealthAsync(_serverId, 24, asOfUtc: Anchor);

        var row = Assert.Single(rows);
        Assert.Null(row.SystemCpuUtilization);
        Assert.Equal(40, row.SqlCpuUtilization);
    }
}
