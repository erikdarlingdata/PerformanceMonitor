/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Buffers.Binary;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The exit codes of <c>--enable-collector</c> and <c>--disable-collector</c> (#4744): <c>0</c> the row was written
/// and read back, <c>1</c> a usage or configuration problem, <c>2</c> a store that could not be reached or that
/// refused the change. Every failure used to exit 1, so a script could not tell a typo from a store that was down.
/// None of these needs a live store: a closed port stands in for an unreachable one, and a small listener that
/// answers the login with an error stands in for a store that says no.
/// </summary>
public sealed class DarlingCollectorToggleExitCodeTests
{
    private const int UsageOrConfig = DarlingCliCommands.CollectorToggleExitCode.UsageOrConfig;
    private const int StoreUnavailable = DarlingCliCommands.CollectorToggleExitCode.StoreUnavailable;

    /// <summary>Scripts compare against the numbers, so the numbers are pinned, and the README lists the same
    /// three next to <c>--check-settings</c>'s.</summary>
    [Fact]
    public void TheExitCodes_AreTheDocumentedNumbers()
    {
        Assert.Equal(0, DarlingCliCommands.CollectorToggleExitCode.Success);
        Assert.Equal(1, DarlingCliCommands.CollectorToggleExitCode.UsageOrConfig);
        Assert.Equal(2, DarlingCliCommands.CollectorToggleExitCode.StoreUnavailable);

        var readme = RepoFile.ReadRepoFile("Darling", "README.md");
        Assert.Contains("`--enable-collector` and `--disable-collector` (described under", readme, StringComparison.Ordinal);
        Assert.Contains("`0` the row was written and read back, `1` a usage or configuration problem", readme, StringComparison.Ordinal);
        Assert.Contains("`2` the store could not be reached or refused the change", readme, StringComparison.Ordinal);
    }

    /// <summary>A TCP port nothing is listening on, proven closed by bind-then-release, so a connection attempt
    /// fails fast (refused) rather than timing out against an address that merely never answers.</summary>
    private static int GetClosedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static string ConnectionString(int port) =>
        $"Host=127.0.0.1;Port={port};Username=darling;Password=x;Database=darlingtest;Timeout=5;SSL Mode=Disable";

    private static string WriteConfig(DirectoryInfo root, string postgresBlock)
    {
        var path = Path.Combine(root.FullName, "darling.json");
        File.WriteAllText(path, "{ \"postgres\": " + postgresBlock + " }");
        return path;
    }

    private static string PostgresBlock(string connectionString) =>
        "{ \"connectionString\": " + JsonSerializer.Serialize(connectionString) + " }";

    private static async Task<(int Exit, string Output, string Error)> RunAsync(bool enable, params string[] rest)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await DarlingCliCommands.ToggleCollectorAsync(enable, rest, output, error, CancellationToken.None);
        return (exit, output.ToString(), error.ToString());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task NoCollectorName_ExitsWithTheUsageCode(bool enable)
    {
        var (exit, _, error) = await RunAsync(enable);

        Assert.Equal(UsageOrConfig, exit);
        Assert.Contains("needs a collector name", error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task UnknownCollector_ExitsWithTheUsageCode(bool enable)
    {
        var (exit, _, error) = await RunAsync(enable, "not_a_collector");

        Assert.Equal(UsageOrConfig, exit);
        Assert.Contains("unknown collector 'not_a_collector'", error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task MissingConfigFile_ExitsWithTheUsageCode(bool enable)
    {
        var missing = Path.Combine(Path.GetTempPath(), "does-not-exist-4744.json");

        var (exit, _, error) = await RunAsync(enable, "wait_stats", "--config", missing);

        Assert.Equal(UsageOrConfig, exit);
        Assert.Contains("Could not load configuration", error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ConfigThatDoesNotParse_ExitsWithTheUsageCode(bool enable)
    {
        var root = Directory.CreateTempSubdirectory("darling-toggle-4744-parse-");
        try
        {
            var path = Path.Combine(root.FullName, "darling.json");
            File.WriteAllText(path, "{ not valid json");

            var (exit, _, error) = await RunAsync(enable, "wait_stats", "--config", path);

            Assert.Equal(UsageOrConfig, exit);
            Assert.Contains("Could not load configuration", error, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ConfigWithNoStoreConnectionString_ExitsWithTheUsageCode(bool enable)
    {
        var root = Directory.CreateTempSubdirectory("darling-toggle-4744-nostore-");
        try
        {
            var path = WriteConfig(root, PostgresBlock(string.Empty));

            var (exit, _, error) = await RunAsync(enable, "wait_stats", "--config", path);

            Assert.Equal(UsageOrConfig, exit);
            Assert.Contains("postgres.connectionString is empty", error, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>A managed store whose credential was never written is a configuration problem, not a store that
    /// could not be reached: there is no store login to try. (On a host that is not Windows the same config stops
    /// at the DPAPI guard, with the same code.)</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ManagedStoreWithNoStoredCredential_ExitsWithTheUsageCode(bool enable)
    {
        var root = Directory.CreateTempSubdirectory("darling-toggle-4744-managed-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var path = WriteConfig(root,
                "{ \"managed\": true, \"port\": " + GetClosedPort() + ", \"dataDirectory\": " + JsonSerializer.Serialize(dataDirectory) + " }");

            var (exit, _, error) = await RunAsync(enable, "wait_stats", "--config", path);

            Assert.Equal(UsageOrConfig, exit);
            Assert.NotEqual(string.Empty, error);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task UnreachableStore_ExitsWithTheStoreCode(bool enable)
    {
        var root = Directory.CreateTempSubdirectory("darling-toggle-4744-unreach-");
        try
        {
            var path = WriteConfig(root, PostgresBlock(ConnectionString(GetClosedPort())));

            var (exit, _, error) = await RunAsync(enable, "wait_stats", "--config", path);

            Assert.Equal(StoreUnavailable, exit);
            Assert.Contains("Could not update the control-plane store", error, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>With <c>--server</c> the first thing the verb asks the store is its server registry, so an
    /// unreachable store fails there, before any write is attempted.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task UnreachableStore_WhileResolvingTheServer_ExitsWithTheStoreCode(bool enable)
    {
        var root = Directory.CreateTempSubdirectory("darling-toggle-4744-unreach-server-");
        try
        {
            var path = WriteConfig(root, PostgresBlock(ConnectionString(GetClosedPort())));

            var (exit, _, error) = await RunAsync(enable, "wait_stats", "--server", "sql01", "--config", path);

            Assert.Equal(StoreUnavailable, exit);
            Assert.Contains("Could not read the servers registry from the store", error, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    /// <summary>A store that is up and answers, but says no: it refuses the login with an error, as a store does
    /// for a wrong password or a role that may not connect. That is the store's answer, not a usage problem, so it
    /// takes the store code.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task StoreThatRefusesTheLogin_ExitsWithTheStoreCode(bool enable)
    {
        var root = Directory.CreateTempSubdirectory("darling-toggle-4744-refused-");
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var served = RefuseEveryLoginAsync(listener);
        try
        {
            var path = WriteConfig(root, PostgresBlock(ConnectionString(port)));

            var (exit, _, error) = await RunAsync(enable, "wait_stats", "--config", path);

            Assert.Equal(StoreUnavailable, exit);
            Assert.Contains("Could not update the control-plane store", error, StringComparison.Ordinal);
            Assert.Contains("password authentication failed", error, StringComparison.Ordinal);
        }
        finally
        {
            listener.Stop();
            await served;
            root.Delete(recursive: true);
        }
    }

    /// <summary>Accepts connections until the listener stops and answers each startup packet with a FATAL
    /// 28P01 ErrorResponse, then closes. An SSL or GSS encryption request that comes first is declined with
    /// <c>N</c>, which is what a server without either says.</summary>
    private static async Task RefuseEveryLoginAsync(TcpListener listener)
    {
        while (true)
        {
            TcpClient client;
            try
            {
                client = await listener.AcceptTcpClientAsync();
            }
            catch (Exception ex) when (ex is ObjectDisposedException or SocketException or InvalidOperationException)
            {
                return;
            }

            using (client)
            {
                try
                {
                    var stream = client.GetStream();
                    var header = new byte[8];
                    while (true)
                    {
                        await stream.ReadExactlyAsync(header);
                        var length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(0, 4));
                        var code = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(4, 4));
                        var rest = new byte[Math.Max(0, length - 8)];
                        await stream.ReadExactlyAsync(rest);
                        if (code is 80877103 or 80877104)
                        {
                            await stream.WriteAsync(new[] { (byte)'N' });
                            continue;
                        }

                        break;
                    }

                    var fields = new MemoryStream();
                    foreach (var (tag, value) in new[]
                    {
                        ('S', "FATAL"), ('V', "FATAL"), ('C', "28P01"), ('M', "password authentication failed for user \"darling\""),
                    })
                    {
                        fields.WriteByte((byte)tag);
                        fields.Write(Encoding.UTF8.GetBytes(value));
                        fields.WriteByte(0);
                    }

                    fields.WriteByte(0);
                    var message = new byte[5 + (int)fields.Length];
                    message[0] = (byte)'E';
                    BinaryPrimitives.WriteInt32BigEndian(message.AsSpan(1, 4), 4 + (int)fields.Length);
                    fields.ToArray().CopyTo(message, 5);
                    await stream.WriteAsync(message);
                    await stream.FlushAsync();
                }
                catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException or EndOfStreamException)
                {
                    /* The client hung up first: nothing left to answer. */
                }
            }
        }
    }
}
