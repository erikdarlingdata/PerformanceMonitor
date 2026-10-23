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
