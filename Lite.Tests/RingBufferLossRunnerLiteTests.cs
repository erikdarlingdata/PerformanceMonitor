/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8146: the ring-buffer loss measurements are held with the item's staged state and reach the run only when the host lands
/// it after the write. Drives the real server-scoped path of <see cref="RemoteCollectorService.RunCollectorDefinitionAsync"/>
/// twice, through <c>ServerReaderOverrideForTests</c> and a real DuckDB file: the first run's position is saved, the second run
/// reads 25 new events with 10 held, and its collection note carries the 15 evicted.
/// </summary>
public sealed class RingBufferLossRunnerLiteTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private static readonly DateTime Created = new(2026, 11, 7, 6, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime FirstRead = new(2026, 11, 7, 9, 0, 0, DateTimeKind.Utc);

    private readonly string _configDir;
    private readonly ServerConnection _server;
    private readonly RemoteCollectorService _collector;

    public RingBufferLossRunnerLiteTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-ringloss-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        var serverManager = new ServerManager(_configDir);
        _server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "RingLossSrv", DisplayName = "RingLossSrv", IsEnabled = true };
        serverManager.AddServer(_server);
        _collector = new RemoteCollectorService(fixture.DuckDb, serverManager, new ScheduleManager(_configDir));
    }

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    /* No deadlock rows, then the gate row: execution_count, gated, the three truncation columns, then the position. */
    private static FakeCollectorDataReader DeadlocksRead(long total, long held, DateTime readAt) =>
        FakeCollectorDataReader.WithResultSets(
            Array.Empty<object[]>(),
            new[] { new object[] { 5L, false, false, 0L, DBNull.Value, total, held, Created, 0L, 1000L, readAt } });

    [Fact]
    public async Task ServerScopedRun_CarriesTheLossNote()
    {
        _collector.ServerReaderOverrideForTests = _ => DeadlocksRead(34, 10, FirstRead);
        await _collector.RunCollectorDefinitionAsync(DeadlocksCollector.Instance, _server, CancellationToken.None);

        var first = _collector.TelemetryFor(RemoteCollectorService.GetServerId(_server)).Note;
        Assert.DoesNotContain("ring_buffer_evicted", first ?? string.Empty, StringComparison.Ordinal);

        _collector.ServerReaderOverrideForTests = _ => DeadlocksRead(59, 10, FirstRead.AddMinutes(1));
        await _collector.RunCollectorDefinitionAsync(DeadlocksCollector.Instance, _server, CancellationToken.None);

        var second = _collector.TelemetryFor(RemoteCollectorService.GetServerId(_server)).Note;
        Assert.Contains("ring_buffer_evicted=15", second, StringComparison.Ordinal);
        Assert.Contains("ring_buffer_loss_window_s=60", second, StringComparison.Ordinal);
    }
}
