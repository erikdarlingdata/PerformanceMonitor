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
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6217: a diagnostics bundle on a managed store finds the store the service runs, and when it cannot it says whether
/// the store is not running, is still starting, or is somewhere the bundle did not look. Every address here is synthetic.
/// </summary>
[Collection("live-postgres")]
public sealed class DiagnosticsBundleManagedStoreTests
{
    private const int ConfiguredPort = 5641;

    private static NpgsqlException Refused() =>
        new("connect failed", new SocketException((int)SocketError.ConnectionRefused));

    private static NpgsqlDataSource AnySource(int port) =>
        NpgsqlDataSource.Create($"Host=127.0.0.1;Port={port};Username=u;Database=d");

    private static string WritePidFile(string dataDirectory, int pid, int port, string? status)
    {
        Directory.CreateDirectory(dataDirectory);
        var lines = new List<string> { pid.ToString(), dataDirectory, "1700000000", port.ToString(), "", "127.0.0.1", "1234 5678" };
        if (status is not null)
        {
            lines.Add(status);
        }

        var path = Path.Combine(dataDirectory, "postmaster.pid");
        File.WriteAllLines(path, lines);
        return path;
    }

    private static bool Alive(int pid) => pid == 4242;

    [Fact]
    public void Locate_MissingDataDirectory_IsNamedAsTheBundleLookingInTheWrongPlace()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-locate-");
        try
        {
            var located = DiagnosticsBundleStoreLocator.Locate(Path.Combine(root.FullName, "no-such"), ConfiguredPort, Alive);
            Assert.Equal(DiagnosticsBundleStoreLocator.StoreState.DataDirectoryMissing, located.State);
            Assert.Equal("data_directory_missing", DiagnosticsBundleStoreLocator.ReasonFor(located.State));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void Locate_NoPidFile_DeadProcess_OrStopping_AreNotRunning()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-locate-");
        try
        {
            var data = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(data);
            Assert.Equal(DiagnosticsBundleStoreLocator.StoreState.NotRunning, DiagnosticsBundleStoreLocator.Locate(data, ConfiguredPort, Alive).State);

            /* A pid file a crash left behind names a process that is gone, or one that is not postgres. */
            WritePidFile(data, 99999, ConfiguredPort, "ready");
            Assert.Equal(DiagnosticsBundleStoreLocator.StoreState.NotRunning, DiagnosticsBundleStoreLocator.Locate(data, ConfiguredPort, Alive).State);

            WritePidFile(data, 4242, ConfiguredPort, "stopping");
            Assert.Equal(DiagnosticsBundleStoreLocator.StoreState.NotRunning, DiagnosticsBundleStoreLocator.Locate(data, ConfiguredPort, Alive).State);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void Locate_LiveStore_ReportsStartingReadyAndWhichPort()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-locate-");
        try
        {
            var data = Path.Combine(root.FullName, "pg");
            WritePidFile(data, 4242, ConfiguredPort, "starting");
            Assert.Equal(DiagnosticsBundleStoreLocator.StoreState.Starting, DiagnosticsBundleStoreLocator.Locate(data, ConfiguredPort, Alive).State);

            WritePidFile(data, 4242, ConfiguredPort, "ready");
            Assert.Equal(DiagnosticsBundleStoreLocator.StoreState.ReadyOnConfiguredPort, DiagnosticsBundleStoreLocator.Locate(data, ConfiguredPort, Alive).State);

            var other = DiagnosticsBundleStoreLocator.Locate(data, ConfiguredPort + 1, Alive);
            Assert.Equal(DiagnosticsBundleStoreLocator.StoreState.ReadyOnOtherPort, other.State);
            Assert.Equal(ConfiguredPort, other.Port);

            /* A server older than PostgreSQL 10 has no status line: it is ready. */
            WritePidFile(data, 4242, ConfiguredPort, null);
            Assert.Equal(DiagnosticsBundleStoreLocator.StoreState.ReadyOnConfiguredPort, DiagnosticsBundleStoreLocator.Locate(data, ConfiguredPort, Alive).State);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void Locate_ServerOnAQuiescedStartsPrivatePort_IsStartingNotAnotherPort()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The managed store, and its quiesced-start marker, are Windows-only.");
        var root = Directory.CreateTempSubdirectory("darling-bundle-locate-");
        try
        {
            var data = Path.Combine(root.FullName, "pg");
            WritePidFile(data, 4242, 6001, "ready");
            File.WriteAllText(Path.Combine(data, DarlingStoreUpgrade.QuiescedUpdateMarkerFileName), "6001");
            Assert.Equal(DiagnosticsBundleStoreLocator.StoreState.Starting, DiagnosticsBundleStoreLocator.Locate(data, ConfiguredPort, Alive).State);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task Connect_StoreNotRunning_GivesTheNotRunningCode_AndKeepsTheError()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-connect-");
        try
        {
            var data = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(data);
            var result = await DiagnosticsBundleStoreLocator.ConnectAsync(
                data, ConfiguredPort, (_, _) => throw Refused(), TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(5),
                TestContext.Current.CancellationToken, Alive);

            Assert.Null(result.DataSource);
            Assert.Equal("store_not_running", result.Reason);
            Assert.Equal("ConnectionRefused", DiagnosticsBundle.SocketErrorOf(result.Error));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task Connect_LiveStoreOnAnotherPort_IsReachedThere_AndSaysSo()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-connect-");
        try
        {
            var data = Path.Combine(root.FullName, "pg");
            WritePidFile(data, 4242, 6002, "ready");
            var tried = new List<int>();
            var result = await DiagnosticsBundleStoreLocator.ConnectAsync(
                data, ConfiguredPort,
                (port, _) =>
                {
                    tried.Add(port);
                    return port == 6002 ? Task.FromResult(AnySource(port)) : throw Refused();
                },
                TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(5), TestContext.Current.CancellationToken, Alive);

            Assert.NotNull(result.DataSource);
            Assert.Null(result.Reason);
            Assert.Equal(new[] { ConfiguredPort, 6002 }, tried);
            Assert.Equal(DiagnosticsBundleStoreLocator.PortDiffersNote, Assert.Single(result.Notes));
            await result.DataSource!.DisposeAsync();
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task Connect_StoreStillStarting_IsWaitedFor_ThenReached()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-connect-");
        try
        {
            var data = Path.Combine(root.FullName, "pg");
            WritePidFile(data, 4242, ConfiguredPort, "starting");
            var attempts = 0;
            var result = await DiagnosticsBundleStoreLocator.ConnectAsync(
                data, ConfiguredPort,
                (port, _) => ++attempts < 3 ? throw Refused() : Task.FromResult(AnySource(port)),
                TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(5), TestContext.Current.CancellationToken, Alive);

            Assert.NotNull(result.DataSource);
            Assert.Equal(3, attempts);
            Assert.Empty(result.Notes);
            await result.DataSource!.DisposeAsync();
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task Connect_StoreThatNeverFinishesStarting_GivesTheStartingCode_WhenTheWaitIsUp()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-connect-");
        try
        {
            var data = Path.Combine(root.FullName, "pg");
            WritePidFile(data, 4242, ConfiguredPort, "starting");
            var result = await DiagnosticsBundleStoreLocator.ConnectAsync(
                data, ConfiguredPort, (_, _) => throw Refused(), TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(10),
                TestContext.Current.CancellationToken, Alive);

            Assert.Null(result.DataSource);
            Assert.Equal("store_starting", result.Reason);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task Connect_RejectedLogin_IsNotABundleLookingInTheWrongPlace_AndDoesNotRetry()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-connect-");
        try
        {
            var data = Path.Combine(root.FullName, "pg");
            WritePidFile(data, 4242, 6003, "ready");
            var attempts = 0;
            var result = await DiagnosticsBundleStoreLocator.ConnectAsync(
                data, ConfiguredPort,
                (_, _) =>
                {
                    attempts++;
                    throw new PostgresException("password authentication failed", "FATAL", "FATAL", "28P01");
                },
                TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(5), TestContext.Current.CancellationToken, Alive);

            Assert.Equal(1, attempts);
            Assert.Null(result.Reason);
            Assert.Equal("auth_failed", DiagnosticsBundle.ClassifyStoreError(result.Error));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    private static int GetClosedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>A managed install's layout: the data directory, with the DPAPI credential beside it, and darling.json.</summary>
    private static string WriteManagedInstall(DirectoryInfo root, int port)
    {
        var data = Path.Combine(root.FullName, "pg");
        Directory.CreateDirectory(data);
        File.WriteAllText(Path.Combine(root.FullName, DarlingManagedPostgres.CredentialFileName), DarlingSecrets.Protect("unused-synthetic-password"));
        var config = Path.Combine(root.FullName, "darling.json");
        File.WriteAllText(config, JsonSerializer.Serialize(new { postgres = new { managed = true, port, dataDirectory = data }, servers = new object[] { new { name = "alpha-sql-01", host = "alpha-sql-01.example.test" } } }));
        return config;
    }

    private static async Task<(int Exit, string Err, JsonNode? Bundle)> RunBundleAsync(DirectoryInfo root, string config)
    {
        var bundle = Path.Combine(root.FullName, "b.json");
        var error = new StringWriter();
        var exit = await DarlingCliCommands.DiagnosticsBundleAsync(
            new[] { bundle, "--config", config, "--log-dir", root.FullName }, new StringWriter(), error, TestContext.Current.CancellationToken);
        return (exit, error.ToString(), File.Exists(bundle) ? JsonNode.Parse(File.ReadAllText(bundle)) : null);
    }

    [Fact]
    public async Task Verb_ManagedStore_NotRunning_WritesTheReducedBundleWithTheNotRunningCode_AndNoAddress()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "A managed store's credential is DPAPI-protected: Windows only.");
        var root = Directory.CreateTempSubdirectory("darling-bundle-managed-");
        try
        {
            var port = GetClosedPort();
            var (exit, _, bundle) = await RunBundleAsync(root, WriteManagedInstall(root, port));

            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.StoreUnreachable, exit);
            var section = bundle!["sections"]!["store_unreachable"]!;
            Assert.Equal("store_not_running", section["reason"]!.GetValue<string>());
            Assert.Equal("ConnectionRefused", section["socket_error"]!.GetValue<string>());
            var text = bundle.ToJsonString();
            Assert.DoesNotContain(port.ToString(), text, StringComparison.Ordinal);
            Assert.DoesNotContain("127.0.0.1", text, StringComparison.Ordinal);
            Assert.DoesNotContain(root.FullName, text, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task Verb_ManagedStore_DataDirectoryGone_SaysTheBundleLookedInTheWrongPlace()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "A managed store's credential is DPAPI-protected: Windows only.");
        var root = Directory.CreateTempSubdirectory("darling-bundle-managed-");
        try
        {
            var config = WriteManagedInstall(root, GetClosedPort());
            Directory.Delete(Path.Combine(root.FullName, "pg"));
            var (exit, _, bundle) = await RunBundleAsync(root, config);

            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.StoreUnreachable, exit);
            Assert.Equal("data_directory_missing", bundle!["sections"]!["store_unreachable"]!["reason"]!.GetValue<string>());
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// The reported failure: the service is running and its store is up, but darling.json names another port than the one
    /// the store listens on. The store's own pid file (a real postgres backend's PID, the live store's port) leads the
    /// bundle to it, so the run is not "store_unreachable".
    /// </summary>
    [Fact]
    public async Task Verb_ManagedStore_LiveOnAnotherPortThanTheConfig_IsReached()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "A managed store's credential is DPAPI-protected: Windows only.");
        var live = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(live), "Set DARLING_TEST_PG to run the live managed-store test.");
        var builder = new NpgsqlConnectionStringBuilder(live);
        Assert.SkipUnless(string.Equals(builder.Username, DarlingManagedPostgres.UserName, StringComparison.Ordinal),
            "The managed connection logs in as the darling role.");
        var root = Directory.CreateTempSubdirectory("darling-bundle-managed-");
        try
        {
            /* The connection stays open, so the backend whose PID the pid file names is alive for the whole run. */
            await using var connection = new NpgsqlConnection(live);
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await using var command = new NpgsqlCommand("SELECT pg_backend_pid()", connection);
            var backendPid = Convert.ToInt32(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken));

            var config = WriteManagedInstall(root, GetClosedPort());
            WritePidFile(Path.Combine(root.FullName, "pg"), backendPid, builder.Port, "ready");
            var (exit, _, bundle) = await RunBundleAsync(root, config);

            Assert.NotEqual(DarlingCliCommands.DiagnosticsBundleExitCode.StoreUnreachable, exit);
            Assert.Null(bundle?["sections"]?["store_unreachable"]);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }
}
