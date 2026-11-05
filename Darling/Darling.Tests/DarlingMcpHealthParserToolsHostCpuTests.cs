/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7416 item 1: SQL Server on Linux reports the host's idle CPU as 0 on some builds, so the SYSTEM component's
/// <c>systemCpuUtilization</c> reads 100 on every row while the CPU tab reads 0 to 1 percent. The system_health read decides from
/// the stored host platform (<c>server_properties.host_os_version</c>), not from the value, so a real 100 on a Windows host still
/// shows. Both the MCP entries read and the web chart's bucketed read leave System CPU null on Linux and say why. Lite twin:
/// <c>SystemHealthHostCpuToolTests</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpHealthParserToolsHostCpuLiveTests
{
    private static readonly string[] Tables = ["system_health_events", "server_properties"];

    private static string Name(string scenario) => "darling-health-hostcpu-" + scenario;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task RunAsync(string scenario, string? hostOs, Func<NpgsqlDataSource, DateTime, string, Task> body) =>
        WindowFloorLiveHarness.RunAsync(ConnectionString, ["system_health_events"], [Name(scenario)], Tables, async (c, ds, end) =>
        {
            var name = Name(scenario);
            var serverId = ServerIdHelper.GetDeterministicHashCode(name);
            await WindowFloorLiveHarness.SeedServerAsync(c, name, end.AddDays(-3), "system_health_events", end.AddDays(-2), 30, end.AddMinutes(-30),
                Tables, TestContext.Current.CancellationToken);
            var eventTime = end.AddHours(-2);
            var xml = System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", "sp_server_diagnostics_system.xml"));
            xml = Regex.Replace(xml, "timestamp=\"[^\"]*\"", $"timestamp=\"{eventTime:yyyy-MM-dd'T'HH:mm:ss.fff}Z\"");
            await DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
                @"INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(eventTime), serverId, name,
                DarlingMcpTestData.Naive(eventTime), SystemHealthParser.SpServerDiagnosticsEvent, xml);
            if (hostOs is not null)
            {
                await DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
                    "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, host_os_version) VALUES ($1,$2,$3,$4,$5)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(eventTime), serverId, name, hostOs);
            }

            await body(ds, end, name);
        });

    [Theory]
    [InlineData("Ubuntu")]
    [InlineData("Linux (Ubuntu 22.04.3 LTS) <X64>")]
    public Task OnLinux_TheEntriesRead_ReturnsNoSystemCpuAndSaysWhy(string hostOs) =>
        RunAsync("entries-linux-" + hostOs.Length, hostOs, async (ds, end, name) =>
        {
            var root = JsonDocument.Parse(await DarlingMcpHealthParserTools.GetSystemHealth(ds, name, 24, 50, WebDataStartNote.FormatWindowEnd(end))).RootElement;

            var entry = root.GetProperty("entries")[0];
            Assert.Equal(JsonValueKind.Null, entry.GetProperty("system_cpu_utilization").ValueKind);
            Assert.Equal(40, entry.GetProperty("sql_cpu_utilization").GetInt64());
            Assert.Contains("Linux", root.GetProperty("system_cpu_note").GetString(), StringComparison.Ordinal);
        });

    [Fact]
    public Task OnLinux_TheWebChartRead_ReturnsNoSystemCpuAndSaysWhy() =>
        RunAsync("chart-linux", "Ubuntu", async (ds, end, name) =>
        {
            var root = JsonDocument.Parse(await DarlingMcpHealthParserTools.GetSystemHealthChart(ds, name, 24, null, WebDataStartNote.FormatWindowEnd(end))).RootElement;

            var entry = root.GetProperty("entries")[0];
            Assert.Equal(JsonValueKind.Null, entry.GetProperty("system_cpu_utilization").ValueKind);
            Assert.Equal(40, entry.GetProperty("sql_cpu_utilization").GetDouble());
            Assert.Contains("Linux", root.GetProperty("system_cpu_note").GetString(), StringComparison.Ordinal);
        });

    /// <summary>#7688: seeds one scheduler-monitor sample (healthy memory) instead of the SYSTEM component, then runs the scheduler read.</summary>
    private static Task RunSchedulerAsync(string scenario, string? hostOs, int sqlCpu, int idle, Func<JsonElement, Task> body) =>
        WindowFloorLiveHarness.RunAsync(ConnectionString, ["system_health_events"], [Name(scenario)], Tables, async (c, ds, end) =>
        {
            var name = Name(scenario);
            var serverId = ServerIdHelper.GetDeterministicHashCode(name);
            await WindowFloorLiveHarness.SeedServerAsync(c, name, end.AddDays(-3), "system_health_events", end.AddDays(-2), 30, end.AddMinutes(-30),
                Tables, TestContext.Current.CancellationToken);
            var eventTime = end.AddHours(-2);
            var xml = $@"<event name=""scheduler_monitor_system_health_ring_buffer_recorded"" package=""sqlos"" timestamp=""{eventTime:yyyy-MM-dd'T'HH:mm:ss.fff}Z"">
  <data name=""process_utilization""><type name=""uint32"" package=""package0""/><value>{sqlCpu}</value></data>
  <data name=""system_idle""><type name=""uint32"" package=""package0""/><value>{idle}</value></data>
  <data name=""page_faults""><type name=""uint32"" package=""package0""/><value>505</value></data>
  <data name=""working_set_delta""><type name=""int64"" package=""package0""/><value>4096</value></data>
  <data name=""memory_utilization""><type name=""uint32"" package=""package0""/><value>100</value></data>
</event>";
            await DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
                @"INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(eventTime), serverId, name,
                DarlingMcpTestData.Naive(eventTime), SystemHealthParser.SchedulerMonitorEvent, xml);
            if (hostOs is not null)
            {
                await DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
                    "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, host_os_version) VALUES ($1,$2,$3,$4,$5)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(eventTime), serverId, name, hostOs);
            }

            await body(JsonDocument.Parse(await DarlingMcpHealthParserTools.GetSchedulerIssues(ds, name, 24, 50, WebDataStartNote.FormatWindowEnd(end))).RootElement);
        });

    /* #7688: idle reads 0 on Linux, so "100 - SQL CPU - idle" read 100 minus the SQL figure on every quiet minute. */
    [Fact]
    public Task OnLinux_QuietSchedulerSamplesAreNotIssues_AndTheAnswerSaysWhy() =>
        RunSchedulerAsync("sched-linux-quiet", "Ubuntu", sqlCpu: 0, idle: 0, root =>
        {
            Assert.False(root.TryGetProperty("issues", out _));
            Assert.Equal(SystemHealthHostCpu.SchedulerLinuxNote, root.GetProperty("system_cpu_note").GetString());
            return Task.CompletedTask;
        });

    [Fact]
    public Task OnLinux_ABusySqlSampleIsStillAnIssue_WithNoOtherProcessOrIdleFigure() =>
        RunSchedulerAsync("sched-linux-busy", "Ubuntu", sqlCpu: 94, idle: 0, root =>
        {
            var issue = Assert.Single(root.GetProperty("issues").EnumerateArray());
            Assert.Equal(94, issue.GetProperty("sql_cpu_utilization").GetInt64());
            Assert.Equal(JsonValueKind.Null, issue.GetProperty("other_process_cpu").ValueKind);
            Assert.Equal(JsonValueKind.Null, issue.GetProperty("system_idle").ValueKind);
            Assert.Equal(SystemHealthHostCpu.SchedulerLinuxNote, root.GetProperty("system_cpu_note").GetString());
            return Task.CompletedTask;
        });

    [Theory]
    [InlineData("Windows")]
    [InlineData(null)]
    public Task OnWindowsOrAnUnknownPlatform_AHighOtherProcessSampleIsStillAnIssue(string? hostOs) =>
        RunSchedulerAsync("sched-keep-" + (hostOs ?? "none"), hostOs, sqlCpu: 5, idle: 0, root =>
        {
            var issue = Assert.Single(root.GetProperty("issues").EnumerateArray());
            Assert.Equal(95, issue.GetProperty("other_process_cpu").GetInt64());
            Assert.Equal(0, issue.GetProperty("system_idle").GetInt64());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("system_cpu_note").ValueKind);
            return Task.CompletedTask;
        });

    [Theory]
    [InlineData("Windows")]
    [InlineData(null)]
    public Task OnWindowsOrAnUnknownPlatform_BothReadsKeepTheFigure(string? hostOs) =>
        RunAsync("keep-" + (hostOs ?? "none"), hostOs, async (ds, end, name) =>
        {
            var asOf = WebDataStartNote.FormatWindowEnd(end);
            var entries = JsonDocument.Parse(await DarlingMcpHealthParserTools.GetSystemHealth(ds, name, 24, 50, asOf)).RootElement;
            var chart = JsonDocument.Parse(await DarlingMcpHealthParserTools.GetSystemHealthChart(ds, name, 24, null, asOf)).RootElement;

            Assert.Equal(65, entries.GetProperty("entries")[0].GetProperty("system_cpu_utilization").GetInt64());
            Assert.Equal(JsonValueKind.Null, entries.GetProperty("system_cpu_note").ValueKind);
            Assert.Equal(65, chart.GetProperty("entries")[0].GetProperty("system_cpu_utilization").GetDouble());
        });
}

/// <summary>#7416 item 1: the platform rule itself, which the desktop viewers and both MCP hosts share.</summary>
public sealed class SystemHealthHostCpuRuleTests
{
    [Theory]
    [InlineData("Windows", false)]
    [InlineData("windows server 2019 datacenter 10.0 <x64>", false)]
    [InlineData("Ubuntu", true)]
    [InlineData("Red Hat Enterprise Linux", true)]
    [InlineData("Linux (Ubuntu 22.04.3 LTS) <X64>", true)]
    [InlineData("", false)]
    [InlineData("   ", false)]
    [InlineData(null, false)]
    public void OnlyANamedNonWindowsPlatformLeavesSystemCpuOut(string? hostOs, bool unreported) =>
        Assert.Equal(unreported, SystemHealthHostCpu.IsHostCpuUnreported(hostOs));

    [Fact]
    public void ASchedulerSample_KeepsOtherProcessAndIdleOnWindows_AndClearsThemOnLinux()
    {
        var sample = new SchedulerIssueRecord { SqlCpuUtilization = 3, OtherProcessCpu = 97, SystemIdle = 0, MemoryUtilization = 100 };

        Assert.Equal(97, SystemHealthHostCpu.Apply(sample, "Windows").OtherProcessCpu);
        Assert.Equal(97, SystemHealthHostCpu.Apply(sample, null).OtherProcessCpu);

        var linux = SystemHealthHostCpu.Apply(sample, "Ubuntu");
        Assert.Null(linux.OtherProcessCpu);
        Assert.Null(linux.SystemIdle);
        Assert.Equal(3, linux.SqlCpuUtilization);
        Assert.False(SystemHealthSignificance.IsSignificant(linux));
    }

    [Fact]
    public void TheNoteIsAddedToAnEmptyAnswerOnlyOnLinux()
    {
        const string empty = "{\"status\":\"empty\",\"message\":\"none\"}";

        Assert.Equal(empty, SystemHealthHostCpu.WithNote(empty, "Windows"));
        Assert.Equal(empty, SystemHealthHostCpu.WithNote(empty, null));
        Assert.Equal(SystemHealthHostCpu.SchedulerLinuxNote,
            JsonDocument.Parse(SystemHealthHostCpu.WithNote(empty, "Ubuntu")!).RootElement.GetProperty("system_cpu_note").GetString());
    }

    [Fact]
    public void ARealHundredPercentOnWindowsIsKept_AndOnLinuxItIsCleared()
    {
        var rows = new List<SystemHealthRecord> { new() { EventTime = DateTime.UtcNow, SystemCpuUtilization = 100, SqlCpuUtilization = 3 } };

        Assert.Equal(100, SystemHealthHostCpu.Apply(rows, "Windows")[0].SystemCpuUtilization);

        var linux = SystemHealthHostCpu.Apply(rows, "Ubuntu");
        Assert.Null(linux[0].SystemCpuUtilization);
        Assert.Equal(3, linux[0].SqlCpuUtilization);
    }
}
