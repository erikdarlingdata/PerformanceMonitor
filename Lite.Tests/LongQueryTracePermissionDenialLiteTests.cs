/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5378: a denied long-query session create (error 15247, no ALTER ANY EVENT SESSION) is a permission state. The reconcile
/// runs outside the collector's run, so before this it re-ran the create every cycle for as long as the login stayed
/// restricted. It now stops for the lifetime of the PERMISSIONS suppression every collector gets: the rest of the app
/// session, lifted by a restart (or, for the flag, by the server's health being cleared).
/// </summary>
[Collection("app-logger-statics")]
public sealed class LongQueryTracePermissionDenialLiteTests : IDisposable
{
    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _configDir;

    public LongQueryTracePermissionDenialLiteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(_configDir);
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

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

    private sealed class Rig
    {
        public required Func<RemoteCollectorService> NewService { get; init; }
        public required RemoteCollectorService Service { get; set; }
        public required ServerConnection Server { get; init; }

        /* Every create or start statement the reconcile tried. */
        public int Statements { get; set; }

        /* The error the statement raises: null to succeed. */
        public Func<Exception?> Refusal { get; set; } = () => null;
    }

    private async Task<Rig> BuildRigAsync()
    {
        var server = new ServerConnection
        {
            ServerName = "onprem.example.test",
            DisplayName = "denied-" + Guid.NewGuid().ToString("N")[..8],
        };

        var duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _initializers.Add(duckDb);
        await duckDb.InitializeAsync();

        var servers = new ServerManager(_configDir);
        servers.AddServer(server);
        servers.GetConnectionStatus(server.Id).SqlEngineEdition = 3;

        var schedules = new ScheduleManager(_configDir);
        schedules.UpdateSchedule("long_query_completions", enabled: true);

        Rig? rig = null;
        RemoteCollectorService NewService()
        {
            var service = new RemoteCollectorService(
                duckDb, servers, schedules,
                installIdStore: new InstallIdStore(_configDir, "test-machine", null));
            service.LongQueryTraceUtcNowForTests = () => new DateTime(2026, 10, 6, 12, 0, 0, DateTimeKind.Utc);
            service.LegacyLongQuerySessionExistsForTests = (_, _) => false;
            service.LongQueryTraceDatabaseOverrideForTests = (_, _, _, sessionName, _) =>
            {
                if (sessionName == PerformanceMonitor.Collectors.LongQueryCompletionsCollector.LegacyXeSessionName)
                {
                    return Task.CompletedTask;
                }

                rig!.Statements++;
                return rig.Refusal() is { } refusal ? Task.FromException(refusal) : Task.CompletedTask;
            };
            return service;
        }

        rig = new Rig { NewService = NewService, Service = NewService(), Server = server };
        return rig;
    }

    private static Exception Denied() =>
        SqlExceptionFactory.Create(15247, errorClass: 14, message: "User does not have permission to perform this action.");

    private static Task ReconcileAsync(Rig rig) =>
        rig.Service.ReconcileLongQueryCompletionsXeSessionAsync(rig.Server, CancellationToken.None);

    [Fact]
    public async Task ADeniedCreate_IsNotRetriedOnTheNextCycles_AndIsTriedAgainAfterARestart()
    {
        var rig = await BuildRigAsync();
        rig.Refusal = Denied;

        await ReconcileAsync(rig);
        Assert.Equal(1, rig.Statements);
        Assert.NotNull(rig.Service.LongQueryTraceFaultState(rig.Server.Id));

        await ReconcileAsync(rig);
        await ReconcileAsync(rig);
        Assert.Equal(1, rig.Statements);

        /* The kept fault is still there, so the run records PERMISSIONS from it. */
        Assert.NotNull(rig.Service.LongQueryTraceFaultState(rig.Server.Id));

        /* A restart is a new service with nothing in memory, as for every collector: the login may have been granted since. */
        rig.Service = rig.NewService();
        rig.Refusal = () => null;
        await ReconcileAsync(rig);
        Assert.Equal(2, rig.Statements);
        Assert.Null(rig.Service.LongQueryTraceFaultState(rig.Server.Id));
    }

    [Fact]
    public async Task ACollectorRunThatRecordedPermissions_StopsTheCreate_UntilTheServersHealthIsCleared()
    {
        var rig = await BuildRigAsync();
        var id = RemoteCollectorService.GetServerId(rig.Server);
        rig.Service.RecordCollectorResult(id, "long_query_completions", "PERMISSIONS", "denied", xeSessionUnavailable: true);

        await ReconcileAsync(rig);
        await ReconcileAsync(rig);
        Assert.Equal(0, rig.Statements);

        rig.Service.ClearHealthForServer(id);
        await ReconcileAsync(rig);
        Assert.Equal(1, rig.Statements);
    }

    [Fact]
    public async Task AFailureThatIsNotAPermissionDenial_IsStillRetriedOnEveryCycle()
    {
        var rig = await BuildRigAsync();
        rig.Refusal = () => SqlExceptionFactory.Create(1105, errorClass: 17, message: "Could not allocate space.");

        await ReconcileAsync(rig);
        await ReconcileAsync(rig);
        await ReconcileAsync(rig);

        Assert.Equal(3, rig.Statements);
    }
}
