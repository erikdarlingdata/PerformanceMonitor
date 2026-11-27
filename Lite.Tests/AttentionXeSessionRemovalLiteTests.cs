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
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Removing a server-scoped server drops this install's own client-timeout session (#9895), by its per-install name only, and never
/// the shared <c>PerformanceMonitor_Attention</c> an older install may still read.
/// </summary>
[Collection("app-logger-statics")]
public sealed class AttentionXeSessionRemovalLiteTests : IDisposable
{
    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _configDir;
    private readonly string _dbPath;

    public AttentionXeSessionRemovalLiteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(_configDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
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
        public required RemoteCollectorService Service { get; init; }
        public required ServerManager Servers { get; init; }
        public required ServerConnection Server { get; init; }
        public List<string> Drops { get; } = new();
    }

    private async Task<Rig> BuildRigAsync(int engineEdition)
    {
        var duckDb = new DuckDbInitializer(_dbPath);
        _initializers.Add(duckDb);
        await duckDb.InitializeFromTemplateAsync();

        var server = new ServerConnection { ServerName = "attention-removal-sql", DisplayName = "attention-" + Guid.NewGuid().ToString("N")[..8] };
        var servers = new ServerManager(_configDir);
        servers.AddServer(server);
        servers.GetConnectionStatus(server.Id).SqlEngineEdition = engineEdition;

        var schedules = new ScheduleManager(_configDir);
        schedules.UpdateSchedule("attention", enabled: true);

        var service = new RemoteCollectorService(
            duckDb, servers, schedules, installIdStore: new InstallIdStore(_configDir, "test-machine", null));
        var rig = new Rig { Service = service, Servers = servers, Server = server };
        service.AttentionSessionDropOverrideForTests = (_, sessionName, _) =>
        {
            rig.Drops.Add(sessionName);
            return Task.CompletedTask;
        };

        return rig;
    }

    [Fact]
    public async Task RemovingAServerScopedServer_DropsThisInstallsOwnAttentionSession_NeverTheSharedOne()
    {
        var rig = await BuildRigAsync(engineEdition: 3);
        await rig.Service.DropLongQueryTraceOfRemovedServerAsync(rig.Server, CancellationToken.None);

        var dropped = Assert.Single(rig.Drops);
        Assert.Equal(AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.LiteProduct, rig.Service.GetInstallId(), AlwaysOnXeSessionKind.Attention), dropped);
        Assert.NotEqual(AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Attention), dropped);
    }

    [Fact]
    public async Task RemovingAnAzureSqlDatabaseServer_SendsNoServerScopedAttentionDrop()
    {
        var rig = await BuildRigAsync(engineEdition: 5);

        await rig.Service.DropLongQueryTraceOfRemovedServerAsync(rig.Server, CancellationToken.None);

        Assert.Empty(rig.Drops);
    }

    [Fact]
    public async Task RemovingAServer_WithAnotherRegistrationThatCouldKeepTheSession_LeavesIt()
    {
        var rig = await BuildRigAsync(engineEdition: 3);
        rig.Servers.AddServer(new ServerConnection { ServerName = "attention-removal-other", DisplayName = "attention-other" });

        await rig.Service.DropLongQueryTraceOfRemovedServerAsync(rig.Server, CancellationToken.None);

        Assert.Empty(rig.Drops);
    }

    [Fact]
    public async Task AServerBeingRemoved_IsNotEnsuredByACycleThatRunsDuringTheDrop()
    {
        var rig = await BuildRigAsync(engineEdition: 3);
        var ensures = 0;
        rig.Service.XeSessionServerEnsureOverrideForTests = (_, _, _) =>
        {
            ensures++;
            return Task.CompletedTask;
        };

        /* The removal has begun: the server is still registered, and the collection timer can still run its attention cycle. */
        await rig.Service.DropLongQueryTraceOfRemovedServerAsync(rig.Server, CancellationToken.None);
        await rig.Service.EnsureAttentionXeSessionAsync(rig.Server, engineEdition: 3, CancellationToken.None);

        Assert.Equal(0, ensures);

        /* A server that is not being removed is still ensured, so the guard is what stopped the call. */
        var other = new ServerConnection { ServerName = "attention-removal-kept", DisplayName = "attention-kept" };
        rig.Servers.AddServer(other);
        await rig.Service.EnsureAttentionXeSessionAsync(other, engineEdition: 3, CancellationToken.None);

        Assert.Equal(1, ensures);
    }
}
