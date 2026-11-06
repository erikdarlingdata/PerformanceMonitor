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
/// A stop while a read is in flight reaches the runner as the PROVIDER's exception, not an
/// OperationCanceledException: SqlClient reports a cancelled command as a SqlException. The run must still be
/// recorded as CANCELLED. Each test runs the real <see cref="RemoteCollectorService.RunCollectorAsync"/> with the
/// reader replaced by one that cancels the run's own token and then throws, and reads the status back from
/// <c>collection_log</c>, so no server is needed.
/// </summary>
public sealed class StopDuringReadIsCancellationLiteTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _configDir;
    private DuckDbInitializer? _duckDb;

    public StopDuringReadIsCancellationLiteTests()
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

    private sealed record Rig(RemoteCollectorService Service, ServerConnection Server, ServerManager Servers);

    private async Task<Rig> BuildRigAsync(string collector)
    {
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        await _duckDb.InitializeFromTemplateAsync();

        var server = new ServerConnection { ServerName = "stopping-host", DisplayName = "stopping-" + Guid.NewGuid().ToString("N")[..8] };
        var servers = new ServerManager(_configDir);
        servers.AddServer(server);
        var schedules = new ScheduleManager(_configDir);
        schedules.UpdateSchedule(collector, enabled: true);

        return new Rig(new RemoteCollectorService(_duckDb, servers, schedules), server, servers);
    }

    /// <summary>The one <c>collection_log</c> row the run wrote: its status and its error message.</summary>
    private async Task<(string Status, string ErrorMessage)> ReadRunRowAsync(ServerConnection server, string collector)
    {
        using var connection = _duckDb!.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT status, error_message FROM collection_log WHERE server_id = {RemoteCollectorService.GetServerId(server)} AND collector_name = '{collector}'";
        using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken), "the run wrote a collection_log row");
        var status = reader.GetString(0);
        var errorMessage = reader.IsDBNull(1) ? "" : reader.GetString(1);
        Assert.False(await reader.ReadAsync(TestContext.Current.CancellationToken), "the run wrote one collection_log row");
        return (status, errorMessage);
    }

    /// <summary>
    /// The Azure per-database loop: one database, whose read cancels the run and then throws a provider exception. Counted as
    /// a failed database, the stop would be the loop's only failure and surface as an ERROR run with the provider's message.
    /// </summary>
    [Fact]
    public async Task AStopDuringAnAzureDatabaseRead_RecordsCancelled_NotAFailedDatabase()
    {
        const string collector = "file_io_stats";
        const string database = "stopped-db";
        var rig = await BuildRigAsync(collector);
        /* Azure SQL Database engine edition, so the definition takes the per-database loop. */
        rig.Servers.GetConnectionStatus(rig.Server.Id).SqlEngineEdition = 5;

        using var stop = new CancellationTokenSource();
        rig.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { database });
        rig.Service.AzureDatabaseReaderOverrideForTests = (_, _) =>
        {
            stop.Cancel();
            throw new InvalidOperationException("Operation cancelled by user.");
        };

        await rig.Service.RunCollectorAsync(rig.Server, collector, stop.Token);

        var (status, errorMessage) = await ReadRunRowAsync(rig.Server, collector);
        Assert.Equal("CANCELLED", status);
        /* Not a failed database: no note or message names it. */
        Assert.DoesNotContain(database, errorMessage, StringComparison.Ordinal);
        Assert.DoesNotContain("Operation cancelled by user", errorMessage, StringComparison.Ordinal);
    }

    /// <summary>
    /// The server-scoped single-read path: the read cancels the run and then throws the SqlException SqlClient raises for a
    /// cancelled command. Left as a SqlException, it reaches the SQL error arm ahead of the cancellation arm and records ERROR.
    /// </summary>
    [Fact]
    public async Task AStopDuringAServerScopedRead_RecordsCancelled_NotError()
    {
        const string collector = "tempdb_stats";
        var rig = await BuildRigAsync(collector);

        using var stop = new CancellationTokenSource();
        rig.Service.ServerReaderOverrideForTests = _ =>
        {
            stop.Cancel();
            throw SqlExceptionFactory.Create(0, errorClass: 11, message: "Operation cancelled by user.");
        };

        await rig.Service.RunCollectorAsync(rig.Server, collector, stop.Token);

        var (status, errorMessage) = await ReadRunRowAsync(rig.Server, collector);
        Assert.Equal("CANCELLED", status);
        Assert.DoesNotContain("SQL Error", errorMessage, StringComparison.Ordinal);
    }
}
