using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9938: the line Lite writes when a target's gate turns a collector off names the facts about the target that gates
/// read, not only the engine edition and version, and it is written at INFO once per server and collector, and again
/// when those facts change. Every other repeat is DEBUG. The gate itself is each definition's AppliesTo; these tests
/// cover the line and its once-per-facts rule, then run the real pre-dispatch check against a server whose status the
/// test sets.
///
/// <para>Collection <c>app-logger-statics</c>: the end-to-end tests drain the process-wide log buffer and change its level.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class CollectorSkipNoticeTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private readonly LogLevel _levelBefore = AppLogger.MinimumLevel;
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);

    public CollectorSkipNoticeTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        AppLogger.SetMinimumLevel(_levelBefore);

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

    [Fact]
    public void Describe_AnAwsRdsTarget_NamesRds_ThenEditionAndVersion()
    {
        var line = CollectorSkipNotice.Describe("agent_status", new CollectorTargetInfo { IsAwsRds = true, SqlMajorVersion = 16 }, engineEdition: 3);

        Assert.Equal("agent_status not collected on this target (AWS RDS, engine edition 3, version 16)", line);
    }

    [Fact]
    public void Describe_AnRdsWebOrExpressTarget_NamesBothRdsFacts()
    {
        var target = new CollectorTargetInfo { IsAwsRds = true, IsAwsRdsWebOrExpress = true, SqlMajorVersion = 15 };

        Assert.Equal(
            "deadlocks not collected on this target (AWS RDS, RDS Web or Express edition, engine edition 2, version 15)",
            CollectorSkipNotice.Describe("deadlocks", target, engineEdition: 2));
    }

    [Fact]
    public void Describe_AnAzureSqlDatabaseTarget_NamesAzureSqlDatabase()
    {
        var line = CollectorSkipNotice.Describe("server_config", new CollectorTargetInfo { IsAzureSqlDb = true, SqlMajorVersion = 12 }, engineEdition: 5);

        Assert.Equal("server_config not collected on this target (Azure SQL Database, engine edition 5, version 12)", line);
    }

    [Fact]
    public void Describe_AnAzureManagedInstanceTarget_NamesManagedInstance()
    {
        var line = CollectorSkipNotice.Describe("trace_analysis", new CollectorTargetInfo { IsAzureManagedInstance = true, SqlMajorVersion = 12 }, engineEdition: 8);

        Assert.Equal("trace_analysis not collected on this target (Azure SQL Managed Instance, engine edition 8, version 12)", line);
    }

    /// <summary>No gate reads msdb access, so it is not a fact the line names: the text is the same with it and without it.</summary>
    [Fact]
    public void Describe_MsdbAccess_DoesNotChangeTheLine()
    {
        var withAccess = CollectorSkipNotice.Describe("running_jobs", new CollectorTargetInfo { IsAwsRds = true, HasMsdbAccess = true, SqlMajorVersion = 16 }, engineEdition: 3);
        var withoutAccess = CollectorSkipNotice.Describe("running_jobs", new CollectorTargetInfo { IsAwsRds = true, HasMsdbAccess = false, SqlMajorVersion = 16 }, engineEdition: 3);

        Assert.Equal("running_jobs not collected on this target (AWS RDS, engine edition 3, version 16)", withAccess);
        Assert.Equal(withAccess, withoutAccess);
        Assert.DoesNotContain("msdb", withoutAccess, StringComparison.Ordinal);
    }

    [Fact]
    public void Describe_ATargetWithNoHostingFact_NamesOnlyEditionAndVersion()
    {
        var line = CollectorSkipNotice.Describe("query_store", new CollectorTargetInfo { SqlMajorVersion = 12 }, engineEdition: 3);

        Assert.Equal("query_store not collected on this target (engine edition 3, version 12)", line);
    }

    [Fact]
    public void Describe_SeveralFacts_ListsThemInAFixedOrder()
    {
        var target = new CollectorTargetInfo { IsAzureSqlDb = true, IsAwsRds = true, IsAwsRdsWebOrExpress = true, SqlMajorVersion = 14 };

        Assert.Equal(
            "agent_status not collected on this target (Azure SQL Database, AWS RDS, RDS Web or Express edition, engine edition 2, version 14)",
            CollectorSkipNotice.Describe("agent_status", target, engineEdition: 2));
    }

    [Fact]
    public void ShouldLogAtInfo_TheSameLineTwice_IsInfoOnce()
    {
        var notice = new CollectorSkipNotice();
        var line = CollectorSkipNotice.Describe("agent_status", new CollectorTargetInfo { IsAwsRds = true, SqlMajorVersion = 16 }, 3);

        Assert.True(notice.ShouldLogAtInfo(1, "agent_status", line));
        Assert.False(notice.ShouldLogAtInfo(1, "agent_status", line));
        Assert.False(notice.ShouldLogAtInfo(1, "agent_status", line));
    }

    [Fact]
    public void ShouldLogAtInfo_WhenTheFactsChange_IsInfoAgain_AndOnceMoreAtTheNewFacts()
    {
        var notice = new CollectorSkipNotice();
        var rds = CollectorSkipNotice.Describe("agent_status", new CollectorTargetInfo { IsAwsRds = true, SqlMajorVersion = 16 }, 3);
        var rdsWebOrExpress = CollectorSkipNotice.Describe("agent_status", new CollectorTargetInfo { IsAwsRds = true, IsAwsRdsWebOrExpress = true, SqlMajorVersion = 16 }, 2);

        Assert.True(notice.ShouldLogAtInfo(1, "agent_status", rds));
        Assert.False(notice.ShouldLogAtInfo(1, "agent_status", rds));
        Assert.True(notice.ShouldLogAtInfo(1, "agent_status", rdsWebOrExpress));
        Assert.False(notice.ShouldLogAtInfo(1, "agent_status", rdsWebOrExpress));
        Assert.True(notice.ShouldLogAtInfo(1, "agent_status", rds));
    }

    [Fact]
    public void ShouldLogAtInfo_IsPerServerAndPerCollector()
    {
        var notice = new CollectorSkipNotice();
        const string line = "x not collected on this target (AWS RDS, engine edition 3, version 16)";

        Assert.True(notice.ShouldLogAtInfo(1, "agent_status", line));
        Assert.True(notice.ShouldLogAtInfo(2, "agent_status", line));
        Assert.True(notice.ShouldLogAtInfo(1, "running_jobs", line));
        Assert.False(notice.ShouldLogAtInfo(2, "agent_status", line));
        Assert.Equal(3, notice.Count);
    }

    [Fact]
    public void Forget_DropsOneServersEntries_SoItsNextSkipIsInfo()
    {
        var notice = new CollectorSkipNotice();
        const string line = "x not collected on this target (AWS RDS, engine edition 3, version 16)";
        notice.ShouldLogAtInfo(1, "agent_status", line);
        notice.ShouldLogAtInfo(1, "running_jobs", line);
        notice.ShouldLogAtInfo(2, "agent_status", line);

        notice.Forget(1);

        Assert.Equal(1, notice.Count);
        Assert.True(notice.ShouldLogAtInfo(1, "agent_status", line));
        Assert.False(notice.ShouldLogAtInfo(2, "agent_status", line));
    }

    [Fact]
    public void ForgetExcept_KeepsOnlyTheActiveServers()
    {
        var notice = new CollectorSkipNotice();
        const string line = "x not collected on this target (AWS RDS, engine edition 3, version 16)";
        notice.ShouldLogAtInfo(1, "agent_status", line);
        notice.ShouldLogAtInfo(2, "agent_status", line);
        notice.ShouldLogAtInfo(3, "agent_status", line);

        notice.ForgetExcept([2]);

        Assert.Equal(1, notice.Count);
        Assert.False(notice.ShouldLogAtInfo(2, "agent_status", line));
        Assert.True(notice.ShouldLogAtInfo(1, "agent_status", line));
    }

    [Fact]
    public async Task ShouldLogAtInfo_RacingThreads_YieldExactlyOneInfoForOneLine()
    {
        var notice = new CollectorSkipNotice();
        const string line = "x not collected on this target (AWS RDS, engine edition 3, version 16)";

        var results = await Task.WhenAll(Enumerable.Range(0, 64).Select(_ =>
            Task.Run(() => notice.ShouldLogAtInfo(7, "agent_status", line), TestContext.Current.CancellationToken)));

        Assert.Single(results, r => r);
        Assert.Equal(1, notice.Count);
    }

    /// <summary>A server on a real manager whose connection status the test sets, as a connect probe would.</summary>
    private (RemoteCollectorService Service, ServerConnection Server, ServerConnectionStatus Status) RdsServer(string name)
    {
        var configDir = Path.Combine(_tempDir, "config_" + name);
        Directory.CreateDirectory(configDir);
        var serverManager = new ServerManager(configDir);
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = name, DisplayName = name };
        serverManager.AddServer(server);

        var status = serverManager.GetConnectionStatus(server.Id);
        status.IsOnline = true;
        status.SqlEngineEdition = 3;
        status.SqlMajorVersion = 16;
        status.IsAwsRds = true;

        return (new RemoteCollectorService(_duckDb, serverManager, new ScheduleManager(configDir)), server, status);
    }

    private static List<string> SkipLines(string serverName, string collector) =>
        AppLogger.DrainBufferedLines()
            .Where(l => l.Contains($"[{serverName}]", StringComparison.Ordinal)
                && l.Contains($"{collector} not collected on this target", StringComparison.Ordinal))
            .ToList();

    [Fact]
    public async Task ARepeatedSkipOnAnRdsServer_WritesOneInfoLine_ThenDebug()
    {
        AppLogger.SetMinimumLevel(LogLevel.Debug);
        const string name = "skip-rds-9938";
        var (service, server, _) = RdsServer(name);

        AppLogger.DrainBufferedLines();
        await service.RunCollectorAsync(server, "agent_status", TestContext.Current.CancellationToken);
        await service.RunCollectorAsync(server, "agent_status", TestContext.Current.CancellationToken);
        await service.RunCollectorAsync(server, "agent_status", TestContext.Current.CancellationToken);
        var lines = SkipLines(name, "agent_status");

        Assert.Equal(3, lines.Count);
        var info = Assert.Single(lines, l => l.Contains("[INFO ]", StringComparison.Ordinal));
        Assert.Contains("agent_status not collected on this target (AWS RDS, engine edition 3, version 16)", info, StringComparison.Ordinal);
        Assert.Equal(2, lines.Count(l => l.Contains("[DEBUG]", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task ARepeatedSkip_AtTheDefaultLevel_WritesOneLineInAll()
    {
        AppLogger.SetMinimumLevel(LogLevel.Information);
        const string name = "skip-default-level-9938";
        var (service, server, _) = RdsServer(name);

        AppLogger.DrainBufferedLines();
        for (var i = 0; i < 5; i++)
        {
            await service.RunCollectorAsync(server, "agent_status", TestContext.Current.CancellationToken);
        }

        var lines = SkipLines(name, "agent_status");

        Assert.Single(lines);
        Assert.Contains("[INFO ]", lines[0], StringComparison.Ordinal);
    }

    [Fact]
    public async Task WhenTheTargetsFactsChange_TheSkipIsInfoAgain()
    {
        AppLogger.SetMinimumLevel(LogLevel.Debug);
        const string name = "skip-facts-9938";
        var (service, server, status) = RdsServer(name);

        AppLogger.DrainBufferedLines();
        await service.RunCollectorAsync(server, "agent_status", TestContext.Current.CancellationToken);
        await service.RunCollectorAsync(server, "agent_status", TestContext.Current.CancellationToken);
        status.SqlEngineEdition = 5;
        await service.RunCollectorAsync(server, "agent_status", TestContext.Current.CancellationToken);
        await service.RunCollectorAsync(server, "agent_status", TestContext.Current.CancellationToken);
        var lines = SkipLines(name, "agent_status");

        Assert.Equal(4, lines.Count);
        Assert.Contains("[INFO ]", lines[0], StringComparison.Ordinal);
        Assert.Contains("(AWS RDS, engine edition 3, version 16)", lines[0], StringComparison.Ordinal);
        Assert.Contains("[DEBUG]", lines[1], StringComparison.Ordinal);
        Assert.Contains("[INFO ]", lines[2], StringComparison.Ordinal);
        Assert.Contains("(Azure SQL Database, AWS RDS, engine edition 5, version 16)", lines[2], StringComparison.Ordinal);
        Assert.Contains("[DEBUG]", lines[3], StringComparison.Ordinal);
    }

    /// <summary>No gate reads msdb access, so a change in it alone is not a change in the target's facts: no second INFO.</summary>
    [Fact]
    public async Task AChangeInMsdbAccessAlone_WritesNoSecondInfoLine()
    {
        AppLogger.SetMinimumLevel(LogLevel.Debug);
        const string name = "skip-msdb-9938";
        var (service, server, status) = RdsServer(name);

        AppLogger.DrainBufferedLines();
        await service.RunCollectorAsync(server, "agent_status", TestContext.Current.CancellationToken);
        status.HasMsdbAccess = false;
        await service.RunCollectorAsync(server, "agent_status", TestContext.Current.CancellationToken);
        status.HasMsdbAccess = true;
        await service.RunCollectorAsync(server, "agent_status", TestContext.Current.CancellationToken);
        var lines = SkipLines(name, "agent_status");

        Assert.Equal(3, lines.Count);
        var info = Assert.Single(lines, l => l.Contains("[INFO ]", StringComparison.Ordinal));
        Assert.Contains("agent_status not collected on this target (AWS RDS, engine edition 3, version 16)", info, StringComparison.Ordinal);
        Assert.All(lines, l => Assert.DoesNotContain("msdb", l, StringComparison.Ordinal));
        Assert.Equal(2, lines.Count(l => l.Contains("[DEBUG]", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task EachCollectorOnTheServer_WritesItsOwnInfoLine()
    {
        AppLogger.SetMinimumLevel(LogLevel.Information);
        const string name = "skip-two-collectors-9938";
        var (service, server, _) = RdsServer(name);

        AppLogger.DrainBufferedLines();
        foreach (var collector in new[] { "agent_status", "running_jobs", "agent_status", "running_jobs" })
        {
            await service.RunCollectorAsync(server, collector, TestContext.Current.CancellationToken);
        }

        var lines = AppLogger.DrainBufferedLines()
            .Where(l => l.Contains($"[{name}]", StringComparison.Ordinal) && l.Contains("not collected on this target", StringComparison.Ordinal))
            .ToList();

        Assert.Equal(2, lines.Count);
        Assert.Single(lines, l => l.Contains("agent_status not collected", StringComparison.Ordinal));
        Assert.Single(lines, l => l.Contains("running_jobs not collected", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ARemovedServer_WritesItsSkipAtInfoAgain_WhenAddedBack()
    {
        AppLogger.SetMinimumLevel(LogLevel.Information);
        const string name = "skip-readd-9938";
        var (service, server, _) = RdsServer(name);
        var serverId = RemoteCollectorService.GetServerId(server);

        AppLogger.DrainBufferedLines();
        await service.RunCollectorAsync(server, "agent_status", TestContext.Current.CancellationToken);
        await service.RunCollectorAsync(server, "agent_status", TestContext.Current.CancellationToken);
        Assert.Single(SkipLines(name, "agent_status"));

        service.ClearHealthForServer(serverId);
        Assert.Equal(0, service.SkipNotice.Count);

        AppLogger.DrainBufferedLines();
        await service.RunCollectorAsync(server, "agent_status", TestContext.Current.CancellationToken);
        Assert.Single(SkipLines(name, "agent_status"));
    }
}
