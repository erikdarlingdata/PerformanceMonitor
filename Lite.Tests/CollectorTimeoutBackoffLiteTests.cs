/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8891: a collector whose statement times out on every run holds up the collectors after it in its server's sweep for its
/// whole command timeout. Repeated timeouts feed the #6186 backoff under one fixed answer, so the collector is tried less often
/// until a run ends some other way, and every timeout still writes its ERROR row. Each test runs the real collector run with the
/// server-wide reader replaced by one that raises the error, so no server is needed.
///
/// <para>In <c>app-logger-statics</c> because the tests read <see cref="AppLogger"/>'s process-wide buffer, which another
/// reader would drain from under them.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class CollectorTimeoutBackoffLiteTests : IDisposable
{
    private const string Collector = "tempdb_stats";
    private readonly string _tempDir;
    private readonly string _configDir;
    private DuckDbInitializer? _duckDb;

    public CollectorTimeoutBackoffLiteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(_configDir);
    }

    public void Dispose()
    {
        _duckDb?.Dispose();
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

    private async Task<(RemoteCollectorService Service, ServerConnection Server)> CreateAsync()
    {
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        await _duckDb.InitializeFromTemplateAsync();

        var server = new ServerConnection { ServerName = "slow-host", DisplayName = "slow-" + Guid.NewGuid().ToString("N")[..8] };
        var servers = new ServerManager(_configDir);
        servers.AddServer(server);
        var schedules = new ScheduleManager(_configDir);
        schedules.UpdateSchedule(Collector, enabled: true);
        return (new RemoteCollectorService(_duckDb, servers, schedules), server);
    }

    private async Task<string[]> StatusesAsync(ServerConnection server)
    {
        using var connection = _duckDb!.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT status FROM collection_log WHERE server_id = {RemoteCollectorService.GetServerId(server)} AND collector_name = '{Collector}' ORDER BY collection_time";
        using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        var statuses = new System.Collections.Generic.List<string>();
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            statuses.Add(reader.GetString(0));
        }

        return statuses.ToArray();
    }

    [Fact]
    public async Task ACollectorTimingOutRunAfterRun_IsTriedLessOften_AndARunThatEndsSomeOtherWayRestoresTheInterval()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();
            var (service, server) = await CreateAsync();
            var serverId = RemoteCollectorService.GetServerId(server);
            var reads = 0;
            var timesOut = true;

            /* The timeout's message carries the elapsed time, so each run's text differs; the streak must still build. */
            service.ServerReaderOverrideForTests = _ =>
            {
                reads++;
                throw timesOut
                    ? SqlExceptionFactory.Create(-2, 11, $"Execution Timeout Expired after {30_000 + reads} ms.")
                    : SqlExceptionFactory.Create(50000, 16, "Something else went wrong.");
            };

            for (var run = 1; run <= RefusedCollectorBackoff.FreeRefusals + 1; run++)
            {
                await service.RunCollectorAsync(server, Collector, CancellationToken.None);
            }

            Assert.Equal(RefusedCollectorBackoff.FreeRefusals + 1, service.GetRefusedBackoff(serverId).Streak(Collector));
            Assert.True(service.IsCollectorBackedOff(serverId, Collector));
            Assert.Single(
                AppLogger.DrainBufferedLines(),
                line => line.Contains(server.DisplayName, StringComparison.Ordinal)
                    && line.Contains("[INFO ]", StringComparison.Ordinal)
                    && line.Contains("timed out", StringComparison.Ordinal)
                    && line.Contains("#8891", StringComparison.Ordinal));

            /* Backed off: the next run does not read and writes no row. */
            await service.RunCollectorAsync(server, Collector, CancellationToken.None);
            Assert.Equal(RefusedCollectorBackoff.FreeRefusals + 1, reads);

            /* Every timeout wrote its ERROR row. */
            var statuses = await StatusesAsync(server);
            Assert.Equal(RefusedCollectorBackoff.FreeRefusals + 1, statuses.Length);
            Assert.All(statuses, status => Assert.Equal("ERROR", status));

            /* After the wait it is tried again, and a run that ends some other way ends the streak. */
            service.GetRefusedBackoff(serverId).SetNotBeforeForTests(Collector, DateTime.UtcNow.AddSeconds(-1));
            timesOut = false;
            await service.RunCollectorAsync(server, Collector, CancellationToken.None);
            Assert.Equal(RefusedCollectorBackoff.FreeRefusals + 2, reads);
            Assert.Equal(0, service.GetRefusedBackoff(serverId).Streak(Collector));
            Assert.False(service.IsCollectorBackedOff(serverId, Collector));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public void ATimeoutAfterARefusalStreak_StartsItsOwnStreak_AtTheNormalInterval()
    {
        var service = new RemoteCollectorService(duckDb: null!, serverManager: null!, scheduleManager: null!);
        const int serverId = 62;
        for (var run = 0; run < 6; run++)
        {
            service.RecordCollectorResult(serverId, Collector, "PERMISSIONS", "The server refused the read.");
        }

        Assert.True(service.IsCollectorBackedOff(serverId, Collector));

        service.RecordCollectorResult(serverId, Collector, "ERROR", "Execution Timeout Expired.", timedOut: true);
        Assert.Equal(1, service.GetRefusedBackoff(serverId).Streak(Collector));
        Assert.False(service.IsCollectorBackedOff(serverId, Collector));
    }
}
