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
using System.Text.Json;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6352: a server's host is never a named pipe, a UNC path or a SqlClient protocol prefix other than tcp:. Lite's
/// add and edit dialogs call <see cref="ServerManager.HostRefusal"/>; the manager's own add, edit and import refuse the
/// same hosts, so no path saves one. A server saved before the rule keeps its host and stays editable.
/// </summary>
public sealed class ServerHostRefusalTests : IDisposable
{
    private readonly string _configDir = Path.Combine(Path.GetTempPath(), "pmlite-test-" + Guid.NewGuid().ToString("N"));

    public ServerHostRefusalTests() => Directory.CreateDirectory(_configDir);

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch { /* best effort */ }
    }

    [Theory]
    [InlineData("np:sql01")]
    [InlineData("admin:sql01")]
    [InlineData("lpc:sql01")]
    [InlineData("\\\\sql01\\pipe\\sql\\query")]
    [InlineData("//sql01/pipe/sql/query")]
    public void AddServer_RefusesAPipeOrAdminOrPathHost_AndSavesNothing(string host)
    {
        var manager = new ServerManager(_configDir);

        var refused = Assert.Throws<InvalidOperationException>(
            () => manager.AddServer(new ServerConnection { ServerName = host, DisplayName = "x" }));

        Assert.Contains("is not accepted", refused.Message, StringComparison.Ordinal);
        Assert.Empty(manager.GetAllServers());
    }

    [Fact]
    public void AddServer_StillAcceptsPlainAndTcpPrefixedAndIpv6Hosts()
    {
        var manager = new ServerManager(_configDir);

        foreach (var host in new[] { "sql01,1433", "sql02\\INST1", "tcp:sql03", "[fe80::1]", "2001:db8::1", "10.1.2.3,1433" })
        {
            manager.AddServer(new ServerConnection { ServerName = host, DisplayName = host });
        }

        Assert.Equal(6, manager.GetAllServers().Count);
    }

    [Fact]
    public void UpdateServer_RefusesMovingTheHostToAPipe_AndKeepsTheSavedHost()
    {
        var manager = new ServerManager(_configDir);
        var saved = new ServerConnection { ServerName = "sql01", DisplayName = "x" };
        manager.AddServer(saved);

        var moved = new ServerConnection { Id = saved.Id, ServerName = "np:sql01", DisplayName = "x" };
        Assert.Throws<InvalidOperationException>(() => manager.UpdateServer(moved));

        Assert.Equal("sql01", manager.GetAllServers().Single().ServerName);
    }

    [Fact]
    public void AServerSavedWithAPipeHost_StaysEditableAndLoadable_WhileItsHostIsLeftAlone()
    {
        var legacy = new ServerConnection { ServerName = "np:legacy01", DisplayName = "legacy" };
        File.WriteAllText(Path.Combine(_configDir, "servers.json"),
            JsonSerializer.Serialize(new { Servers = new[] { legacy } }));
        var manager = new ServerManager(_configDir);
        Assert.Equal("np:legacy01", manager.GetAllServers().Single().ServerName);

        var renamed = new ServerConnection { Id = legacy.Id, ServerName = "np:legacy01", DisplayName = "renamed" };
        manager.UpdateServer(renamed);

        Assert.Equal("renamed", manager.GetAllServers().Single().DisplayName);
        Assert.Null(ServerManager.HostRefusal("np:legacy01", "np:legacy01"));
        Assert.NotNull(ServerManager.HostRefusal("np:other", "np:legacy01"));
        Assert.NotNull(ServerManager.HostRefusal("np:legacy01", null));
    }

    [Fact]
    public void ImportServersFromFile_SkipsAPipeHost_AndImportsThePlainOne()
    {
        var source = Path.Combine(_configDir, "import-servers.json");
        File.WriteAllText(source, JsonSerializer.Serialize(new
        {
            Servers = new[]
            {
                new ServerConnection { ServerName = "np:sql01", DisplayName = "pipe" },
                new ServerConnection { ServerName = "sql02", DisplayName = "plain" },
            },
        }));
        var targetDir = Path.Combine(_configDir, "target");
        Directory.CreateDirectory(targetDir);
        var manager = new ServerManager(targetDir);

        var (imported, skipped, collided) = manager.ImportServersFromFile(source);

        Assert.Equal((1, 1, 0), (imported, skipped, collided));
        Assert.Equal("sql02", manager.GetAllServers().Single().ServerName);
    }

    [Fact]
    public void UpdateServer_RefusesAPipeHost_WhenTheLiveListObjectWasChangedBeforeTheCall_WithNothingPassed()
    {
        // The edit dialog changes the object the manager holds, then calls UpdateServer with that same object, so the
        // live copy already holds the new host. The manager compares against its own copy of the last saved host.
        var manager = new ServerManager(_configDir);
        var saved = new ServerConnection { ServerName = "sql01", DisplayName = "x" };
        manager.AddServer(saved);
        var live = manager.GetAllServers().Single();

        live.ServerName = "np:attacker.example";

        Assert.Throws<InvalidOperationException>(() => manager.UpdateServer(live));
    }

    [Fact]
    public void UpdateServer_LetsAnUnchangedLegacyPipeHostThrough_OnTheLiveObject()
    {
        var legacy = new ServerConnection { ServerName = "np:legacy01", DisplayName = "legacy" };
        File.WriteAllText(Path.Combine(_configDir, "servers.json"),
            JsonSerializer.Serialize(new { Servers = new[] { legacy } }));
        var manager = new ServerManager(_configDir);
        var live = manager.GetAllServers().Single();

        live.DisplayName = "renamed";
        manager.UpdateServer(live);

        Assert.Equal("renamed", manager.GetAllServers().Single().DisplayName);
    }

    [Theory]
    [InlineData("np:attacker", "\u00A0np:attacker")]
    [InlineData("np:attacker", " np:attacker")]
    [InlineData("np:attacker", "np:attacker ")]
    public void AHostThatDiffersOnlyByWhitespaceFromTheSavedOne_IsNotTheSameHost(string newHost, string savedHost)
    {
        // A stored "<U+00A0>np:x" is not a pipe and "np:x" is: an edit to it is a changed host, so it is checked.
        Assert.NotNull(ServerManager.HostRefusal(newHost, savedHost));
    }

    [Fact]
    public void ARefusedHostWithLineBreaks_IsNotEchoedWithThem_SoAnImportCannotForgeALogLine()
    {
        var refusal = ServerManager.HostRefusal("np:a\r\n2026-01-01 INFO forged", null);

        Assert.NotNull(refusal);
        Assert.DoesNotContain('\r', refusal);
        Assert.DoesNotContain('\n', refusal);
    }
}
