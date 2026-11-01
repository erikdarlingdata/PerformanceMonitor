/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. It is in darling-config-env because it points
   DARLING_CONFIG at a temp folder, so the service's key and state files go there and not beside the test binary. */

/// <summary>
/// #6468: the service starts with no monitored servers. This runs the worker's real collection loop (the code
/// ExecuteAsync reaches after it loads and validates darling.json) against a throwaway store with an empty server list,
/// and checks that it migrates the store, seeds the configuration, publishes an empty registry, says in one plain
/// Information line that no servers are configured yet, and logs nothing at Warning or above.
/// </summary>
[Collection("darling-config-env")]
public sealed class DarlingStartWithNoServersLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheCollectionLoop_StartsWithAnEmptyServerList_MigratesTheStore_AndLogsOnePlainLine()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live start-with-no-servers test.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var configFolder = Directory.CreateTempSubdirectory("darling-no-servers-").FullName;
        var previousConfig = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        var bodySucceeded = false;
        try
        {
            var configPath = Path.Combine(configFolder, "darling.json");
            await File.WriteAllTextAsync(
                configPath,
                "{ \"postgres\": { \"managed\": false, \"connectionString\": \"" + scratch.ConnectionString.Replace("\\", "\\\\") + "\" } }",
                ct);
            Environment.SetEnvironmentVariable("DARLING_CONFIG", configPath);

            /* The file as an install writes it: store settings only, no servers key at all. */
            var config = DarlingConfig.Load();
            Assert.Empty(config.Servers);
            Assert.Empty(config.Validate());

            var logger = new RecordingLogger();
            var registry = new MonitoredServerRegistryState();
            var worker = new DarlingWorker(
                logger, NullLoggerFactory.Instance, new McpRuntimeState(), new WebRuntimeState(), registry,
                new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache(), new ReadLatencyAccumulator());
            var loop = typeof(DarlingWorker).GetMethod("RunCollectionLoopAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

            using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            stop.CancelAfter(TimeSpan.FromMinutes(3));
            var task = (Task)loop.Invoke(
                worker, [config, scratch.ConnectionString, null, null, null, null, false, null, null, stop.Token])!;

            /* The loop runs until it is stopped. Wait for the line that says it is up and waiting, then stop it. */
            while (!logger.Lines.Any(l => l.Contains("No monitored servers are configured yet", StringComparison.Ordinal))
                && !task.IsCompleted)
            {
                await Task.Delay(100, ct);
            }

            Assert.False(task.IsFaulted, "the loop failed instead of waiting for a first server: " + task.Exception?.GetBaseException().Message);
            Assert.False(task.IsCompleted && !stop.IsCancellationRequested, "the loop returned instead of waiting for a first server");
            stop.Cancel();
            try
            {
                await task;
            }
            catch (OperationCanceledException)
            {
            }

            var plain = Assert.Single(
                logger.Entries, e => e.Message.Contains("No monitored servers are configured yet", StringComparison.Ordinal));
            Assert.Equal(LogLevel.Information, plain.Level);
            Assert.Contains("--add-server", plain.Message, StringComparison.Ordinal);
            Assert.True(
                !Faults(logger).Any(),
                "nothing at Warning or above may be logged: " + string.Join(" || ", Faults(logger).Select(e => e.Level + " " + e.Message)));

            var published = registry.Read();
            Assert.NotNull(published);
            Assert.Empty(published!.Servers);

            /* The store was migrated and seeded, so the Viewer, web and MCP have something to read and a place to add a server. */
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await using var command = new NpgsqlCommand(
                "SELECT (SELECT count(*) FROM config.config_service), (SELECT count(*) FROM config.config_monitored_servers)", connection);
            await using var reader = await command.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal(1L, reader.GetInt64(0));
            Assert.Equal(0L, reader.GetInt64(1));
            bodySucceeded = true;
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", previousConfig);
            try
            {
                Directory.Delete(configFolder, recursive: true);
            }
            catch (IOException)
            {
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    /// <summary>
    /// #6468: <c>--validate-config</c> (and the installer's <c>--test-connection</c> pre-flight) on a config with no servers passes and
    /// says plainly that there is nothing to test, rather than printing "All servers reachable." over an empty list.
    /// </summary>
    [Fact]
    public async Task ValidateConfig_WithNoServers_PassesAndSaysThereIsNothingToTest()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live --validate-config no-servers test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        var root = Directory.CreateTempSubdirectory("darling-validate-no-servers-");
        try
        {
            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(
                configPath,
                "{ \"postgres\": { \"connectionString\": " + System.Text.Json.JsonSerializer.Serialize(scratch.ConnectionString) + " } }",
                ct);

            var output = new StringWriter();
            var error = new StringWriter();
            var exit = await DarlingCliCommands.ValidateConfigAsync(configPath, output, error, ct);

            Assert.Equal(0, exit);
            Assert.Contains("No monitored servers are configured yet", output.ToString(), StringComparison.Ordinal);
            Assert.DoesNotContain("All servers reachable", output.ToString(), StringComparison.Ordinal);
            Assert.Equal(string.Empty, error.ToString());
        }
        finally
        {
            Directory.Delete(root.FullName, recursive: true);
        }
    }

    /// <summary>
    /// A stop that lands while a pass is waiting on the store ends the collection loop cleanly. The config beacon read is held on a
    /// table lock (and the external alert pass on another), then the service is stopped the moment the beacon read is seen waiting:
    /// the loop must return without raising, which means it reached the drain of the background loops before the store sources
    /// were disposed, and nothing at Warning or above may be logged after the stop. Before the loop's pass body was inside the stop
    /// handler, the cancelled beacon read left the method by exception, skipping the drain and disposing the store sources under
    /// the alert loop. The beacon read (3 s) and the alert pass (10 s) have command deadlines that log a Warning when they expire
    /// while the lock is held, so only what is logged after the stop is judged: those expiries happen with or without a stop and
    /// say nothing about it. Whether the alert pass is also waiting at that moment depends on how the two 15 second loops line up;
    /// the assertion holds either way, and the cancelled beacon read alone fails the old code on the loop's return.
    /// </summary>
    [Fact]
    public async Task AStopWhilePassesWaitOnTheStore_EndsTheLoopCleanly_AfterTheBackgroundLoopsDrain()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live stop-during-a-pass test.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        var configFolder = Directory.CreateTempSubdirectory("darling-stop-pass-").FullName;
        var previousConfig = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        var bodySucceeded = false;
        try
        {
            var configPath = Path.Combine(configFolder, "darling.json");
            await File.WriteAllTextAsync(
                configPath,
                "{ \"postgres\": { \"managed\": false, \"connectionString\": \"" + scratch.ConnectionString.Replace("\\", "\\\\") + "\" } }",
                ct);
            Environment.SetEnvironmentVariable("DARLING_CONFIG", configPath);
            var config = DarlingConfig.Load();

            var logger = new RecordingLogger();
            var worker = new DarlingWorker(
                logger, NullLoggerFactory.Instance, new McpRuntimeState(), new WebRuntimeState(), new MonitoredServerRegistryState(),
                new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache(), new ReadLatencyAccumulator());
            var loop = typeof(DarlingWorker).GetMethod("RunCollectionLoopAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

            using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            stop.CancelAfter(TimeSpan.FromMinutes(3));
            var task = (Task)loop.Invoke(
                worker, [config, scratch.ConnectionString, null, null, null, null, false, null, null, stop.Token])!;

            while (!logger.Lines.Any(l => l.Contains("No monitored servers are configured yet", StringComparison.Ordinal))
                && !task.IsCompleted)
            {
                await Task.Delay(100, ct);
            }

            Assert.False(task.IsFaulted, "the loop failed before the test could hold it: " + task.Exception?.GetBaseException().Message);
            Assert.False(task.IsCompleted, "the loop ended before the test could hold it");

            /* From here every read of the config beacon and every external alert statement waits on a lock this connection holds. */
            await using var holder = new NpgsqlConnection(scratch.ConnectionString);
            await holder.OpenAsync(ct);
            await using (var begin = new NpgsqlCommand(
                "BEGIN; LOCK TABLE config.config_service IN ACCESS EXCLUSIVE MODE; LOCK TABLE config.external_alert_pending IN ACCESS EXCLUSIVE MODE;",
                holder))
            {
                await begin.ExecuteNonQueryAsync(ct);
            }

            /* The beacon is read once a pass and a pass starts every 15 seconds (a read held on the lock gives up after 3 seconds), so a
               read is waiting on the lock within about 18 seconds. The stop goes as soon as one is seen. */
            await using var probe = new NpgsqlConnection(scratch.ConnectionString);
            await probe.OpenAsync(ct);
            var deadline = DateTime.UtcNow.AddSeconds(45);
            var held = false;
            while (!held && DateTime.UtcNow < deadline)
            {
                await using var look = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query ILIKE '%config_version%'", probe);
                await using var reader = await look.ExecuteReaderAsync(ct);
                await reader.ReadAsync(ct);
                held = reader.GetInt64(0) > 0;
                if (!held)
                {
                    await Task.Delay(50, ct);
                }
            }

            Assert.True(held, "the beacon read was not seen waiting on the lock within 45 seconds");

            /* Anything logged up to here (a deadline that expired on the held lock) is not about the stop. */
            var loggedBeforeStop = logger.Entries.Count;
            stop.Cancel();
            var finished = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(60), ct));
            Assert.True(ReferenceEquals(finished, task), "the loop did not finish within 60 seconds of the stop");
            await task;

            var afterStop = logger.Entries.Skip(loggedBeforeStop)
                .Where(e => e.Level >= LogLevel.Warning && !e.Message.StartsWith("Statement statistics:", StringComparison.Ordinal))
                .ToList();
            Assert.True(
                afterStop.Count == 0,
                "nothing at Warning or above may be logged after the stop: " + string.Join(" || ", afterStop.Select(e => e.Level + " " + e.Message)));
            bodySucceeded = true;
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", previousConfig);
            try
            {
                Directory.Delete(configFolder, recursive: true);
            }
            catch (IOException)
            {
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    /* A store the test borrows can carry settings of its own that the service rightly warns about (pg_stat_statements.track_utility on
       a plain cluster). Those say nothing about the server list, so they are the one kind left out. */
    private static IEnumerable<(LogLevel Level, string Message)> Faults(RecordingLogger logger) =>
        logger.Entries.Where(e => e.Level >= LogLevel.Warning
            && !e.Message.StartsWith("Statement statistics:", StringComparison.Ordinal));

    private sealed class RecordingLogger : ILogger<DarlingWorker>
    {
        private readonly List<(LogLevel Level, string Message)> _entries = new();

        public IReadOnlyList<(LogLevel Level, string Message)> Entries
        {
            get
            {
                lock (_entries)
                {
                    return _entries.ToList();
                }
            }
        }

        public IReadOnlyList<string> Lines => Entries.Select(e => e.Message).ToList();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_entries)
            {
                _entries.Add((logLevel, formatter(state, exception) + (exception is null ? string.Empty : " | " + exception.Message)));
            }
        }
    }
}
