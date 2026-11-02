using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class starts its own bundled PostgreSQL on a private port and data directory and never
   touches another test's rows. It is in darling-config-env because it points DARLING_CONFIG at a temp folder. */

/// <summary>
/// #7162: a bundled store this service started must not outlive a managed bootstrap that was cancelled or failed
/// after the server was already up. The worker starts the store, then still creates the database, runs the
/// post-upgrade analyze staging and verifies TimescaleDB. A service stop or a terminal error in any of those
/// returned from the worker with postgres.exe left running, and nothing later stopped it: the next start adopts
/// a running store as not its own, and the upgrade script refuses while a process runs under the install tree.
/// These tests run the worker's real <c>ExecuteAsync</c> against a staged runtime and break the bootstrap right
/// after the server has started.
/// </summary>
[Collection("darling-config-env")]
public sealed class ManagedBootstrapStopsStoreLiveTests
{
    private const string RuntimeGateMessage =
        "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe) " +
        "to run the managed bootstrap live tests.";

    [Fact]
    public Task ACancelledBootstrap_AfterTheStoreStarted_StopsTheStore_Gated() => RunAsync(cancel: true);

    [Fact]
    public Task ATerminalBootstrapFailure_AfterTheStoreStarted_StopsTheStore_Gated() => RunAsync(cancel: false);

    private static async Task RunAsync(bool cancel)
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot), RuntimeGateMessage);
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-bootstrap-stop-").FullName;
        var dataDirectory = Path.Combine(root, "pg");
        var port = DarlingManagedPostgresTests.FindFreeTcpPort();
        var configPath = Path.Combine(root, "darling.json");
        var previousConfig = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        DarlingManagedPostgres? cluster = null;
        try
        {
            await File.WriteAllTextAsync(
                configPath,
                "{ \"postgres\": { \"managed\": true, \"port\": " + port + ", \"dataDirectory\": \""
                    + dataDirectory.Replace("\\", "\\\\") + "\" } }",
                TestContext.Current.CancellationToken);
            Environment.SetEnvironmentVariable("DARLING_CONFIG", configPath);

            using var stop = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            stop.CancelAfter(TimeSpan.FromMinutes(5));

            var startedWhenBroken = false;
            var logger = new HookLogger(message =>
            {
                /* EnsureDatabaseAsync logs this line AFTER the server is up and the started-by-this-process flag is
                   set, and before it issues CREATE DATABASE: the exact window the bug lives in. */
                if (message.StartsWith("Creating the '", StringComparison.Ordinal) && cluster is not null)
                {
                    startedWhenBroken = cluster.StartedByThisProcess;
                    if (cancel)
                    {
                        stop.Cancel();
                    }
                    else
                    {
                        throw new InvalidOperationException("simulated terminal bootstrap failure after the store started");
                    }
                }
            });

            var worker = new DarlingWorker(
                logger, NullLoggerFactory.Instance, new McpRuntimeState(), new WebRuntimeState(),
                new MonitoredServerRegistryState(), new CollectorRuntimeState(), new WebTlsCertificateState(),
                new BaselineCache(), new ReadLatencyAccumulator())
            {
                ManagedPostgresFactoryOverride = (postgres, log) =>
                    cluster = new DarlingManagedPostgres(postgres, log, runtimeRoot),
            };

            await worker.StartAsync(stop.Token);
            await worker.ExecuteTask!.WaitAsync(TimeSpan.FromMinutes(5), TestContext.Current.CancellationToken);

            Assert.NotNull(cluster);
            Assert.True(startedWhenBroken, "the bootstrap was not broken while this process owned a running store");
            Assert.False(cluster!.StartedByThisProcess, "the worker returned without stopping the store it started");
            Assert.False(
                File.Exists(Path.Combine(dataDirectory, "postmaster.pid")),
                "postmaster.pid is still there: the store the bootstrap started is still running");
            Assert.False(await Listening(port), "something still listens on the store's port after the worker returned");
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", previousConfig);
            if (cluster is not null)
            {
                await cluster.StopIfStartedByThisProcessAsync();
            }

            try
            {
                Directory.Delete(root, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    private static async Task<bool> Listening(int port)
    {
        using var client = new System.Net.Sockets.TcpClient();
        try
        {
            await client.ConnectAsync("127.0.0.1", port, TestContext.Current.CancellationToken);
            return true;
        }
        catch (System.Net.Sockets.SocketException)
        {
            return false;
        }
    }

    private sealed class HookLogger(Action<string> onMessage) : ILogger<DarlingWorker>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => onMessage(formatter(state, exception));
    }
}
