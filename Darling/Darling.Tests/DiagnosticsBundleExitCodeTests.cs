/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#5097: the exit codes that need no live store.</summary>
public sealed class DiagnosticsBundleExitCodeTests
{
    private static int GetClosedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static string WriteConfig(DirectoryInfo root, int port)
    {
        var path = Path.Combine(root.FullName, "darling.json");
        File.WriteAllText(path, $$"""
            {
              "postgres": { "connectionString": "Host=127.0.0.1;Port={{port}};Username=betaowner;Password=Zq8-secretvalue;Database=darlingtest;Timeout=2" },
              "servers": [ { "name": "alpha-sql-01", "host": "alpha-sql-01.example.test" } ]
            }
            """);
        return path;
    }

    private static async Task<(int Exit, string Out, string Err)> RunAsync(params string[] args)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await DarlingCliCommands.DiagnosticsBundleAsync(args, output, error, CancellationToken.None);
        return (exit, output.ToString(), error.ToString());
    }

    [Fact]
    public async Task ConfigParseError_ExitsOne_AndWritesNoFile()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-cfgerr-");
        try
        {
            var config = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(config, "{ not valid json");
            var bundle = Path.Combine(root.FullName, "b.json");
            var (exit, _, err) = await RunAsync(bundle, "--config", config);
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.ConfigError, exit);
            Assert.Contains("Could not load configuration", err, StringComparison.Ordinal);
            Assert.False(File.Exists(bundle));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task UnreachableStore_ExitsTwo_AndWritesAReducedBundleWithTheHostAliased()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-unreach-");
        try
        {
            var logs = Directory.CreateDirectory(Path.Combine(root.FullName, "logs"));
            File.WriteAllText(
                Path.Combine(logs.FullName, $"darling-service_{DateTime.Now:yyyyMMdd}.log"),
                $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [ERROR] [Store] could not reach alpha-sql-01.example.test\n");
            var config = WriteConfig(root, GetClosedPort());
            var bundle = Path.Combine(root.FullName, "b.json");

            var (exit, _, _) = await RunAsync(bundle, "--config", config, "--log-dir", logs.FullName);

            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.StoreUnreachable, exit);
            var text = await File.ReadAllTextAsync(bundle);
            var tree = JsonNode.Parse(text)!;
            Assert.Equal("store_unreachable", tree["sections"]!["store_unreachable"]!["status"]!.GetValue<string>());
            Assert.Equal("ok", tree["sections"]!["service_log"]!["status"]!.GetValue<string>());
            Assert.NotNull(tree["sections"]!["config_shape"]);
            foreach (var forbidden in new[] { "127.0.0.1", "alpha-sql-01", "example.test", "betaowner", "Zq8-secretvalue" })
            {
                Assert.DoesNotContain(forbidden, text, StringComparison.OrdinalIgnoreCase);
            }
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task ExistingFile_WithoutForce_ExitsFour_AndIsLeftAlone()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-exists-");
        try
        {
            var config = WriteConfig(root, GetClosedPort());
            var bundle = Path.Combine(root.FullName, "b.json");
            await File.WriteAllTextAsync(bundle, "keep me");
            var (exit, _, err) = await RunAsync(bundle, "--config", config);
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.OutputError, exit);
            Assert.Contains("--force", err, StringComparison.Ordinal);
            Assert.Equal("keep me", await File.ReadAllTextAsync(bundle));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task MissingOutputDirectory_ExitsFour()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-nodir-");
        try
        {
            var config = WriteConfig(root, GetClosedPort());
            var (exit, _, _) = await RunAsync(Path.Combine(root.FullName, "no-such-dir", "b.json"), "--config", config);
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.OutputError, exit);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task AliasMap_IsOptIn_StartsWithDoNotAttach_AndIsNotInTheBundle()
    {
        var root = Directory.CreateTempSubdirectory("darling-bundle-map-");
        try
        {
            var config = WriteConfig(root, GetClosedPort());
            var bundle = Path.Combine(root.FullName, "b.json");
            var map = Path.Combine(root.FullName, "map.txt");
            var (exit, _, _) = await RunAsync(bundle, "--config", config, "--log-dir", root.FullName, "--alias-map", map);
            Assert.Equal(DarlingCliCommands.DiagnosticsBundleExitCode.StoreUnreachable, exit);
            var first = (await File.ReadAllLinesAsync(map))[0];
            Assert.StartsWith("DO NOT ATTACH", first, StringComparison.Ordinal);
            Assert.DoesNotContain("DO NOT ATTACH", await File.ReadAllTextAsync(bundle), StringComparison.Ordinal);
            Assert.Contains("alpha-sql-01", await File.ReadAllTextAsync(map), StringComparison.Ordinal);

            var noMapBundle = Path.Combine(root.FullName, "c.json");
            await RunAsync(noMapBundle, "--config", config, "--log-dir", root.FullName);
            Assert.Single(Directory.GetFiles(root.FullName, "map*"));
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }
}
