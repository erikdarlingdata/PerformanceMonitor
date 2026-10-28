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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6859: a collector run the server refuses with a permission denial is recorded as PERMISSIONS and logs one Warning line,
/// as Darling does. The SQL error arm of <c>RunCollectorAsync</c> used to write its Error line before it classified the
/// error, so every refused run logged an Error and a Warning. A run that stays ERROR keeps its Error line. Each test runs the
/// real collector run with the server-wide reader replaced by one that raises the error, so no server is needed.
///
/// <para>In <c>app-logger-statics</c> because the tests read <see cref="AppLogger"/>'s process-wide buffer, which another
/// reader would drain from under them.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class CollectorSqlErrorLogLevelLiteTests : IDisposable
{
    private const string Collector = "tempdb_stats";

    private readonly string _tempDir;
    private readonly string _configDir;
    private DuckDbInitializer? _duckDb;

    public CollectorSqlErrorLogLevelLiteTests()
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

    /// <summary>Runs the collector once against a server that raises the error, and returns the run's status and the lines it logged.</summary>
    private async Task<(string Status, List<string> Lines)> RunRefusedAsync(int number, byte errorClass)
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
            await _duckDb.InitializeFromTemplateAsync();

            var server = new ServerConnection { ServerName = "refusing-host", DisplayName = "refusing-" + Guid.NewGuid().ToString("N")[..8] };
            var servers = new ServerManager(_configDir);
            servers.AddServer(server);
            var schedules = new ScheduleManager(_configDir);
            schedules.UpdateSchedule(Collector, enabled: true);

            var service = new RemoteCollectorService(_duckDb, servers, schedules);
            service.ServerReaderOverrideForTests = _ =>
                throw SqlExceptionFactory.Create(number, errorClass, $"The server refused the read with error {number}.");

            await service.RunCollectorAsync(server, Collector, CancellationToken.None);

            var lines = AppLogger.DrainBufferedLines()
                .Where(line => line.Contains(server.DisplayName, StringComparison.Ordinal))
                .ToList();

            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT status FROM collection_log WHERE server_id = {RemoteCollectorService.GetServerId(server)} AND collector_name = '{Collector}'";
            var status = Convert.ToString(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken)) ?? "";
            return (status, lines);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    private static int Count(IEnumerable<string> lines, string level) =>
        lines.Count(line => line.Contains($"[{level,-5}]", StringComparison.Ordinal));

    [Theory]
    [InlineData(262, 14)] /* The permission denied on the tempdb view that a contained login gets on Azure SQL Database. */
    [InlineData(229, 14)]
    public async Task APermissionDenial_IsRecordedAsPermissions_AndLogsOneWarningAndNoError(int number, int errorClass)
    {
        var (status, lines) = await RunRefusedAsync(number, (byte)errorClass);

        Assert.Equal("PERMISSIONS", status);
        Assert.Equal(1, Count(lines, "WARN"));
        Assert.Equal(0, Count(lines, "ERROR"));
        Assert.Contains(lines, line => line.Contains("permission denied", StringComparison.Ordinal)
            && line.Contains($"error {number}", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AColumnNotFound_StaysError_AndKeepsItsErrorLineAndItsWarning()
    {
        var (status, lines) = await RunRefusedAsync(207, errorClass: 16);

        Assert.Equal("ERROR", status);
        Assert.Equal(1, Count(lines, "ERROR"));
        Assert.Equal(1, Count(lines, "WARN"));
    }

    [Fact]
    public async Task AnUnclassifiedSqlError_StaysError_AndKeepsBothErrorLines()
    {
        var (status, lines) = await RunRefusedAsync(50000, errorClass: 16);

        Assert.Equal("ERROR", status);
        Assert.Equal(2, Count(lines, "ERROR"));
        Assert.Equal(0, Count(lines, "WARN"));
    }
}
