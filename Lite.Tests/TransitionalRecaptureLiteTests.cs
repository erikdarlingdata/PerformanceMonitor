using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9270: Lite's half of the transitional-database recapture. <see cref="ScheduleManager.ScheduleTransitionalRecaptureForServer"/> makes
/// <c>database_config</c> due in minutes when the stored captures say so, and
/// <see cref="RemoteCollectorService.ReadRecaptureDueAsync"/> is the read of them that decides it (the shared <see cref="TransitionalDatabaseRuns"/> rule).
/// </summary>
public sealed class TransitionalRecaptureLiteTests : IDisposable
{
    private const string Server = "srv-transitional";
    private const string Collector = "database_config";
    private static readonly DateTime Now = DateTime.UtcNow;
    private static long s_nextId = -1;

    private readonly string _dir = Directory.CreateTempSubdirectory("pm-lite-transitional-tests-").FullName;
    private readonly List<DuckDbInitializer> _initializers = [];

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private static bool IsDue(ScheduleManager manager, DateTime atUtc) =>
        manager.GetDueCollectorsForServer(Server, atUtc).Any(s => s.Name == Collector);

    private ScheduleManager SettledManager()
    {
        var manager = new ScheduleManager(_dir);
        manager.MarkCollectorRunForServer(Server, Collector, Now);
        return manager;
    }

    [Fact]
    public void ATransitionalCapture_MakesTheCollectorDueInAboutTenMinutes_NotADay()
    {
        var manager = SettledManager();
        Assert.False(IsDue(manager, Now.AddMinutes(60)));

        Assert.True(manager.ScheduleTransitionalRecaptureForServer(Server, Collector, recaptureDue: true, Now));

        Assert.False(IsDue(manager, Now.AddMinutes(9)));
        Assert.True(IsDue(manager, Now.AddMinutes(11)));
    }

    [Fact]
    public void ASettledCapture_LeavesTheNormalIntervalAlone()
    {
        var manager = SettledManager();

        Assert.False(manager.ScheduleTransitionalRecaptureForServer(Server, Collector, recaptureDue: false, Now));

        Assert.False(IsDue(manager, Now.AddMinutes(60)));
    }

    [Fact]
    public void ACollectorSwitchedOffOrNotListed_IsLeftAlone()
    {
        var manager = SettledManager();
        manager.SetScheduleForServer(Server, new List<CollectorSchedule>
        {
            new() { Name = Collector, Enabled = false, FrequencyMinutes = 0 },
        });

        Assert.False(manager.ScheduleTransitionalRecaptureForServer(Server, Collector, true, Now));
        Assert.False(manager.ScheduleTransitionalRecaptureForServer(Server, "not_a_collector", true, Now));
    }

    private async Task<(DuckDbInitializer DuckDb, RemoteCollectorService Service, ScheduleManager Manager, ServerConnection Server)> OpenAsync()
    {
        var duckDb = new DuckDbInitializer(Path.Combine(_dir, "pm.duckdb"));
        _initializers.Add(duckDb);
        await duckDb.InitializeFromTemplateAsync();
        var servers = new ServerManager(_dir);
        var server = new ServerConnection { ServerName = "transitional-test", DisplayName = "transitional-test" };
        servers.AddServer(server);
        var manager = new ScheduleManager(_dir);
        return (duckDb, new RemoteCollectorService(duckDb, servers, manager), manager, server);
    }

    private static async Task CaptureAsync(DuckDbInitializer duckDb, int storageServerId, DateTime captureTime, string database, string? state)
    {
        using var readLock = duckDb.AcquireReadLock();
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, state_desc)
            VALUES ($1, $2, $3, 'transitional-test', $4, $5)";
        command.Parameters.Add(new DuckDBParameter { Value = Interlocked.Decrement(ref s_nextId) });
        command.Parameters.Add(new DuckDBParameter { Value = captureTime });
        command.Parameters.Add(new DuckDBParameter { Value = storageServerId });
        command.Parameters.Add(new DuckDBParameter { Value = database });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)state ?? DBNull.Value });
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task TheNewestCaptureHoldingARestoringDatabase_IsTransitional_InAnyLetterCase()
    {
        var (duckDb, service, _, server) = await OpenAsync();
        var id = RemoteCollectorService.GetServerId(server);
        await CaptureAsync(duckDb, id, Now.AddMinutes(-30), "restoring_db", "ONLINE");
        await CaptureAsync(duckDb, id, Now.AddMinutes(-5), "settled_db", "ONLINE");
        await CaptureAsync(duckDb, id, Now.AddMinutes(-5), "restoring_db", "restoring");

        Assert.True(await service.ReadRecaptureDueAsync(id, CancellationToken.None));
    }

    [Theory]
    [InlineData("RECOVERING")]
    [InlineData("RECOVERY_PENDING")]
    public async Task TheOtherTransitionalStates_AreTransitionalToo(string state)
    {
        var (duckDb, service, _, server) = await OpenAsync();
        var id = RemoteCollectorService.GetServerId(server);
        await CaptureAsync(duckDb, id, Now.AddMinutes(-30), "db", "ONLINE");
        await CaptureAsync(duckDb, id, Now.AddMinutes(-5), "db", state);

        Assert.True(await service.ReadRecaptureDueAsync(id, CancellationToken.None));
    }

    [Fact]
    public async Task AnOlderTransitionalCapture_DoesNotCount_OnceTheNewestIsSettled()
    {
        var (duckDb, service, _, server) = await OpenAsync();
        var id = RemoteCollectorService.GetServerId(server);
        await CaptureAsync(duckDb, id, Now.AddMinutes(-30), "db", "RESTORING");
        await CaptureAsync(duckDb, id, Now.AddMinutes(-5), "db", "ONLINE");

        Assert.False(await service.ReadRecaptureDueAsync(id, CancellationToken.None));
    }

    [Fact]
    public async Task AnotherServersRestoringDatabase_DoesNotCount_AndNoCaptureAndANullStateAreSettled()
    {
        var (duckDb, service, _, server) = await OpenAsync();
        var id = RemoteCollectorService.GetServerId(server);
        Assert.False(await service.ReadRecaptureDueAsync(id, CancellationToken.None));

        await CaptureAsync(duckDb, id + 1, Now.AddMinutes(-5), "db", "RESTORING");
        await CaptureAsync(duckDb, id, Now.AddMinutes(-5), "db", null);

        Assert.False(await service.ReadRecaptureDueAsync(id, CancellationToken.None));
    }

    [Fact]
    public async Task AfterARestoringCapture_TheServersCollectorIsDueInMinutes_AndAfterASettledOneItIsNot()
    {
        var (duckDb, service, manager, server) = await OpenAsync();
        var id = RemoteCollectorService.GetServerId(server);
        manager.MarkCollectorRunForServer(server.Id, Collector, DateTime.UtcNow);
        await CaptureAsync(duckDb, id, Now.AddMinutes(-20), "db", "ONLINE");
        await CaptureAsync(duckDb, id, Now.AddMinutes(-1), "db", "RESTORING");

        await service.ScheduleTransitionalRecaptureAsync(server, CancellationToken.None);

        Assert.Contains(Collector, manager.GetDueCollectorsForServer(server.Id, DateTime.UtcNow.AddMinutes(11)).Select(s => s.Name));

        await CaptureAsync(duckDb, id, Now, "db", "ONLINE");
        manager.MarkCollectorRunForServer(server.Id, Collector, DateTime.UtcNow);
        await service.ScheduleTransitionalRecaptureAsync(server, CancellationToken.None);

        Assert.DoesNotContain(Collector, manager.GetDueCollectorsForServer(server.Id, DateTime.UtcNow.AddMinutes(60)).Select(s => s.Name));
    }

    [Fact]
    public async Task TheSeventhCaptureOfARun_KeepsTheNormalInterval_AndTheSixthDoesNot()
    {
        var (duckDb, service, _, server) = await OpenAsync();
        var id = RemoteCollectorService.GetServerId(server);
        await CaptureAsync(duckDb, id, Now.AddMinutes(-100), "db", "ONLINE");
        for (var i = 1; i <= TransitionalDatabaseRuns.MaxRecaptures; i++)
        {
            await CaptureAsync(duckDb, id, Now.AddMinutes(-100 + (i * 10)), "db", "RESTORING");
            Assert.True(await service.ReadRecaptureDueAsync(id, CancellationToken.None), "capture " + i + " of the run");
        }

        await CaptureAsync(duckDb, id, Now.AddMinutes(-100 + ((TransitionalDatabaseRuns.MaxRecaptures + 1) * 10)), "db", "RESTORING");

        Assert.False(await service.ReadRecaptureDueAsync(id, CancellationToken.None));
    }

    [Fact]
    public async Task ADatabaseTransitionalSinceFirstSeen_KeepsTheNormalInterval()
    {
        var (duckDb, service, _, server) = await OpenAsync();
        var id = RemoteCollectorService.GetServerId(server);
        await CaptureAsync(duckDb, id, Now.AddMinutes(-30), "standby", "RESTORING");
        await CaptureAsync(duckDb, id, Now.AddMinutes(-1), "standby", "RESTORING");

        Assert.False(await service.ReadRecaptureDueAsync(id, CancellationToken.None));
    }

    [Fact]
    public async Task ARestartWithAStuckDatabasePresent_IsDueInTenMinutes_ForAnotherDatabaseNewlyRecovering()
    {
        var (duckDb, service, _, server) = await OpenAsync();
        var id = RemoteCollectorService.GetServerId(server);
        await CaptureAsync(duckDb, id, Now.AddDays(-3), "stuck", "ONLINE");
        await CaptureAsync(duckDb, id, Now.AddDays(-3), "other", "ONLINE");
        for (var i = 0; i < TransitionalDatabaseRuns.MaxRecaptures + 2; i++)
        {
            await CaptureAsync(duckDb, id, Now.AddDays(-2).AddMinutes(i * 10), "stuck", "RECOVERY_PENDING");
        }

        await CaptureAsync(duckDb, id, Now.AddMinutes(-1), "stuck", "RECOVERY_PENDING");
        await CaptureAsync(duckDb, id, Now.AddMinutes(-1), "other", "RECOVERING");

        Assert.True(await service.ReadRecaptureDueAsync(id, CancellationToken.None));
    }

    [Fact]
    public async Task AStuckDatabaseAlone_KeepsTheNormalInterval()
    {
        var (duckDb, service, _, server) = await OpenAsync();
        var id = RemoteCollectorService.GetServerId(server);
        await CaptureAsync(duckDb, id, Now.AddDays(-3), "stuck", "ONLINE");
        for (var i = 0; i < TransitionalDatabaseRuns.MaxRecaptures + 2; i++)
        {
            await CaptureAsync(duckDb, id, Now.AddDays(-2).AddMinutes(i * 10), "stuck", "RECOVERY_PENDING");
        }

        Assert.False(await service.ReadRecaptureDueAsync(id, CancellationToken.None));
    }
}
