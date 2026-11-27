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
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9084: a server whose data collection is off gets no query from a background loop of Lite. The periodic connection check and
/// the alert sweep's live msdb read both used to run over every server in the list. No test opens a connection: the check takes
/// the per-server step as a delegate, and the msdb read is a spy.
/// </summary>
[InMemoryCredentials]
public sealed class DisabledServerNoBackgroundQueryTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "LiteDisabledServer_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }

    private static ServerConnection Server(string id, string host, bool enabled) => new()
    {
        Id = id,
        ServerName = host,
        DisplayName = "server " + id,
        AuthenticationType = AuthenticationTypes.Windows,
        IsEnabled = enabled
    };

    private ServerManager NewManager(params ServerConnection[] servers)
    {
        var shared = Path.Combine(_root, "shared");
        Directory.CreateDirectory(shared);
        File.WriteAllText(Path.Combine(shared, "servers.json"), JsonSerializer.Serialize(new { Servers = servers }));
        return new ServerManager(
            shared,
            hostBindingsPath: Path.Combine(_root, "user", ServerHostBindings.FileName),
            credentialService: new CredentialService(new InMemoryCredentialStore()));
    }

    private static async Task<List<string>> IdsTheCheckReaches(ServerManager manager)
    {
        var checkedIds = new List<string>();
        await manager.CheckAllConnectionsAsync(id =>
        {
            lock (checkedIds) { checkedIds.Add(id); }
            return Task.CompletedTask;
        });
        checkedIds.Sort(StringComparer.Ordinal);
        return checkedIds;
    }

    [Fact]
    public async Task TheBackgroundConnectionCheck_ReachesOnlyTheEnabledServer()
    {
        var manager = NewManager(Server("on", "sql-on", enabled: true), Server("off", "sql-off", enabled: false));

        Assert.Equal(new[] { "on" }, await IdsTheCheckReaches(manager));
    }

    [Fact]
    public async Task TheBackgroundConnectionCheck_ForgetsTheLiveStatusItLastHadForADisabledServer()
    {
        var manager = NewManager(Server("on", "sql-on", enabled: true), Server("off", "sql-off", enabled: false));
        manager.GetConnectionStatus("off").IsOnline = true;
        manager.GetConnectionStatus("on").IsOnline = true;

        await IdsTheCheckReaches(manager);

        Assert.Null(manager.GetConnectionStatus("off").IsOnline);
        Assert.True(manager.GetConnectionStatus("on").IsOnline);
    }

    [Fact]
    public async Task TurningCollectionBackOn_PutsTheServerInTheNextCheck_WithNoRestart()
    {
        var manager = NewManager(Server("on", "sql-on", enabled: true), Server("off", "sql-off", enabled: false));
        Assert.Equal(new[] { "on" }, await IdsTheCheckReaches(manager));

        /* The Edit dialog changes the live object and saves it. */
        var edited = manager.GetServerById("off")!;
        edited.IsEnabled = true;
        manager.UpdateServer(edited);

        Assert.Equal(new[] { "off", "on" }, await IdsTheCheckReaches(manager));

        edited.IsEnabled = false;
        manager.UpdateServer(edited);

        Assert.Equal(new[] { "on" }, await IdsTheCheckReaches(manager));
    }

    [Fact]
    public void TheAlertSweep_SkipsTheCardOfADisabledServer()
    {
        var on = Server("on", "sql-on", enabled: true);
        var off = Server("off", "sql-off", enabled: false);
        var summaries = new[]
        {
            new ServerSummaryItem { ServerId = RemoteCollectorService.GetServerId(on), ServerName = "sql-on" },
            new ServerSummaryItem { ServerId = RemoteCollectorService.GetServerId(off), ServerName = "sql-off" },
        };

        var swept = DisabledServerGate.SummariesToSweep(summaries, new[] { on, off });

        var only = Assert.Single(swept);
        Assert.Equal(RemoteCollectorService.GetServerId(on), only.ServerId);

        off.IsEnabled = true;
        Assert.Equal(2, DisabledServerGate.SummariesToSweep(summaries, new[] { on, off }).Count);
    }

    [Fact]
    public async Task TheFailedJobsRead_IsNeverCalledForADisabledServer_EvenWhenItsLastCheckSaidOnlineWithMsdbAccess()
    {
        var off = Server("off", "sql-off", enabled: false);
        var status = new ServerConnectionStatus { ServerId = "off", IsOnline = true, SqlEngineEdition = 3, HasMsdbAccess = true };
        var reads = 0;

        var jobs = await DisabledServerGate.FetchFailedJobsAsync(off, status, _ =>
        {
            reads++;
            return Task.FromResult(new List<FailedJobInfo> { new() });
        });

        Assert.Empty(jobs);
        Assert.Equal(0, reads);
    }

    [Fact]
    public async Task TheFailedJobsRead_StillRunsForAnEnabledOnlineServer_AndForADisabledOneOnceCollectionIsBackOn()
    {
        var server = Server("s", "sql-s", enabled: true);
        var status = new ServerConnectionStatus { ServerId = "s", IsOnline = true, SqlEngineEdition = 3, HasMsdbAccess = true };
        var reads = 0;
        Task<List<FailedJobInfo>> Read(ServerConnection _)
        {
            reads++;
            return Task.FromResult(new List<FailedJobInfo> { new() });
        }

        Assert.Single(await DisabledServerGate.FetchFailedJobsAsync(server, status, Read));
        Assert.Equal(1, reads);

        server.IsEnabled = false;
        Assert.Empty(await DisabledServerGate.FetchFailedJobsAsync(server, status, Read));
        server.IsEnabled = true;
        Assert.Single(await DisabledServerGate.FetchFailedJobsAsync(server, status, Read));
        Assert.Equal(2, reads);
    }

    [Fact]
    public void TheSidebarAndTheCard_ShowADisabledServerAsDisabled_NotAsItsLastLiveAnswer()
    {
        var server = Server("s", "sql-s", enabled: false);
        server.IsOnline = true;
        var card = new ServerSummaryItem { IsOnline = true, IsEnabled = false };

        Assert.Equal(ServerCardStatus.Disabled, server.CardStatus);
        Assert.Equal(ServerCardStatus.Disabled, card.CardStatus);

        server.IsEnabled = true;
        card.IsEnabled = true;

        Assert.Equal(ServerCardStatus.Online, server.CardStatus);
        Assert.Equal(ServerCardStatus.Online, card.CardStatus);
    }
}
