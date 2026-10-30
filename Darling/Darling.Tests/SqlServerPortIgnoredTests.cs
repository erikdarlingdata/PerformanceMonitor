/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6583: a SQL Server entry's <c>port</c> is never used (the connection builders read the host alone), so it must never be
/// dropped silently: a new entry that carries one is refused, one that exists is warned about, and a failed connect names the
/// target that was actually tried.
/// </summary>
public sealed class SqlServerPortIgnoredTests
{
    private const string DeadStore = "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    private static readonly DarlingMcpServerAdminTools.ServerProbe ThrowingProbe =
        (_, _) => throw new InvalidOperationException("the probe must not run for a refused entry");

    private static ConnectionProbeResult Failed() => new(false, 0, 0, null, false, false, false, false, "driver text");

    [Fact]
    public async Task AddServers_ASqlServerEntryWithAPort_IsRefused_WithTheAdvice_AndNeverProbed()
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpServerAdminTools.AddServersAsync(
            dead, "[{\"host\":\"localhost\",\"port\":14340}]", ThrowingProbe, CancellationToken.None);

        using var doc = JsonDocument.Parse(result);
        Assert.Equal(0, doc.RootElement.GetProperty("added").GetInt32());
        Assert.Equal(1, doc.RootElement.GetProperty("failed").GetInt32());
        var entry = doc.RootElement.GetProperty("results")[0];
        Assert.Equal("invalid", entry.GetProperty("status").GetString());
        Assert.Contains("host,port", entry.GetProperty("detail").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddServers_ASqlServerEntryWithPortZero_IsNotRefusedForThePort()
    {
        /* Zero is the omitted value. The entry gets past the port check and on to the store, which here is dead. */
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpServerAdminTools.AddServersAsync(
            dead, "[{\"host\":\"localhost\",\"port\":0}]", (_, _) => Task.FromResult(Failed()), CancellationToken.None);

        Assert.DoesNotContain("host,port", result, StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectFailureReply_ForASqlServerEntry_NamesTheHostThatWasTried_NotTheIgnoredPort()
    {
        var server = new MonitoredServer { Host = "sql-01", Port = 14340 };

        var reply = DarlingMcpServerAdminTools.ConnectFailureReply(server, Failed(), null, false);

        Assert.Contains("sql-01", reply, StringComparison.Ordinal);
        Assert.DoesNotContain("14340", reply, StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectFailureReply_ForAPostgresEntry_StillNamesItsPort()
    {
        var server = new MonitoredServer { Host = "pg-01", Port = 5433, Engine = "postgres" };

        var reply = DarlingMcpServerAdminTools.ConnectFailureReply(server, Failed(), null, false);

        Assert.Contains("pg-01,5433", reply, StringComparison.Ordinal);
    }

    [Fact]
    public void Warnings_NameEachSqlServerEntryHoldingAPort_AndNoOtherEntry()
    {
        var warnings = SqlServerPortAdvice.Warnings(new[]
        {
            new MonitoredServer { Name = "Demo", Host = "localhost", Port = 14340 },
            new MonitoredServer { Name = "Plain", Host = "sql-02" },
            new MonitoredServer { Name = "Pg", Host = "pg-01", Port = 5433, Engine = "postgres" },
        });

        var warning = Assert.Single(warnings);
        Assert.Contains("'Demo'", warning, StringComparison.Ordinal);
        Assert.Contains("14340", warning, StringComparison.Ordinal);
        Assert.Contains("localhost,14340", warning, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ValidateConfig_ASqlServerEntryWithAPort_WarnsAndTriesTheHostAlone()
    {
        var ct = TestContext.Current.CancellationToken;
        var root = Directory.CreateTempSubdirectory("darling-validate-port-");
        try
        {
            /* The host names port 1, which refuses at once; the entry's own port is the one that would be a different target. */
            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath, """
                {
                  "postgres": { "connectionString": "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1" },
                  "servers": [ { "name": "demo", "host": "127.0.0.1,1", "port": 14340, "auth": "integrated" } ]
                }
                """, ct);

            var output = new StringWriter();
            var error = new StringWriter();
            await DarlingCliCommands.ValidateConfigAsync(configPath, output, error, ct);

            var lines = output.ToString().Split('\n').Where(l => l.StartsWith("WARNING:", StringComparison.Ordinal)).ToArray();
            Assert.True(
                lines.Any(l => l.Contains("port 14340 is ignored", StringComparison.Ordinal) && l.Contains("host,port", StringComparison.Ordinal)),
                "output: " + output + " error: " + error);
            Assert.DoesNotContain("14340", error.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(root.FullName, recursive: true);
        }
    }
}
