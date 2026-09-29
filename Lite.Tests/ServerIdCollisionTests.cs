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
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4789: Lite derives a server's id from a 32-bit hash of its storage name (address, database, read-only intent),
/// while servers.json is keyed by GUID. Two DIFFERENT servers can derive one id and then collect into ONE DuckDB
/// server_id: each tab shows both servers' rows and nothing says so. Nothing is overwritten, but the histories mix.
/// The add and edit paths refuse to put a server on an id another server already holds, and the bulk add counts
/// such a row apart from a duplicate and from a failure. Import Settings (ImportServersFromFile) refuses such an
/// entry the same way and counts it apart from a skipped duplicate.
///
/// <para>The pair is synthetic and is the one the Darling viewer's collision tests use: two different hosts that
/// hash to one id.</para>
/// </summary>
public sealed class ServerIdCollisionTests : IDisposable
{
    private const string HolderHost = "sql9jocsv";
    private const string CollidingHost = "sqlsvvqew";

    private readonly string _configDir = Path.Combine(Path.GetTempPath(), "pmlite-test-" + Guid.NewGuid().ToString("N"));
    private readonly List<string> _credentialIds = new();
    private readonly CredentialService _credentials = new();

    public ServerIdCollisionTests()
    {
        Directory.CreateDirectory(_configDir);
    }

    public void Dispose()
    {
        foreach (var id in _credentialIds)
        {
            _credentials.DeleteCredential(id);
        }

        try { Directory.Delete(_configDir, recursive: true); } catch { /* best effort */ }
    }

    private string ServersJson => Path.Combine(_configDir, "servers.json");

    private ServerManager NewManager() => new(_configDir);

    private ServerManager NewManager(ILogger<ServerManager> logger) => new(_configDir, logger);

    private static ServerConnection Holder() => new() { ServerName = HolderHost, DisplayName = "Holder" };

    private static ServerConnection Colliding() => new() { ServerName = CollidingHost, DisplayName = "Colliding" };

    /// <summary>A SQL-authentication server whose credential this test may write, deleted at the end.</summary>
    private ServerConnection WithCredential(string host, string displayName)
    {
        var server = new ServerConnection
        {
            ServerName = host,
            DisplayName = displayName,
            AuthenticationType = AuthenticationTypes.SqlServer
        };
        _credentialIds.Add(server.Id);
        return server;
    }

    [Fact]
    public void ThePair_DerivesOneLiteId_FromTwoDifferentStorageNames()
    {
        var holder = Holder();
        var colliding = Colliding();

        Assert.NotEqual(
            RemoteCollectorService.GetServerNameForStorage(holder),
            RemoteCollectorService.GetServerNameForStorage(colliding));
        Assert.Equal(RemoteCollectorService.GetServerId(holder), RemoteCollectorService.GetServerId(colliding));
    }

    [Fact]
    public void AddServer_OfADifferentServerOnTheSameId_IsRefused_NamingTheHolder_AndChangesNothing()
    {
        var manager = NewManager();
        var holder = Holder();
        manager.AddServer(holder);
        var colliding = WithCredential(CollidingHost, "Colliding");
        var before = File.ReadAllBytes(ServersJson);

        var ex = Assert.Throws<InvalidOperationException>(() => manager.AddServer(colliding, "monitor", "secret"));

        Assert.Contains("'Holder'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("collides", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("already monitored as", ex.Message, StringComparison.Ordinal);
        Assert.Equal(new[] { holder.Id }, manager.GetAllServers().Select(s => s.Id));
        Assert.Null(manager.GetServerById(colliding.Id));
        Assert.Equal(before, File.ReadAllBytes(ServersJson));
        Assert.False(manager.CredentialService.CredentialExists(colliding.Id));
    }

    [Fact]
    public void AddServer_OfTheSameServerUnderANewGuid_IsRefusedAsAlreadyMonitored()
    {
        var manager = NewManager();
        manager.AddServer(Holder());
        var again = new ServerConnection { ServerName = HolderHost, DisplayName = "Again" };

        var ex = Assert.Throws<InvalidOperationException>(() => manager.AddServer(again));

        Assert.Contains("already monitored as 'Holder'", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("collides", ex.Message, StringComparison.Ordinal);
        Assert.Single(manager.GetAllServers());
    }

    [Fact]
    public void AddServer_OfTheSameHostWithAnotherDatabaseOrIntent_IsStillAdded()
    {
        var manager = NewManager();
        manager.AddServer(Holder());

        manager.AddServer(new ServerConnection { ServerName = HolderHost, DisplayName = "Db", DatabaseName = "sales" });
        manager.AddServer(new ServerConnection { ServerName = HolderHost, DisplayName = "Ro", ReadOnlyIntent = true });

        Assert.Equal(3, manager.GetAllServers().Count);
    }

    [Fact]
    public void UpdateServer_RenamingOntoAnotherServersId_IsRefused_AndChangesNothing()
    {
        var manager = NewManager();
        manager.AddServer(Holder());
        var other = WithCredential("other01", "Other");
        manager.AddServer(other, "monitor", "secret");
        var before = File.ReadAllBytes(ServersJson);

        var edited = new ServerConnection
        {
            Id = other.Id,
            ServerName = CollidingHost,
            DisplayName = "Other",
            AuthenticationType = AuthenticationTypes.SqlServer
        };
        var ex = Assert.Throws<InvalidOperationException>(() => manager.UpdateServer(edited, "monitor2", "secret2"));

        Assert.Contains("'Holder'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("collides", ex.Message, StringComparison.Ordinal);
        Assert.Equal("other01", manager.GetServerById(other.Id)!.ServerName);
        Assert.Equal(before, File.ReadAllBytes(ServersJson));
        var credential = manager.CredentialService.GetCredential(other.Id);
        Assert.Equal("monitor", credential?.Username);
        Assert.Equal("secret", credential?.Password);
    }

    [Fact]
    public void UpdateServer_ThatKeepsItsId_IsAllowed_WhetherOrNotThePlainNameChanged()
    {
        var manager = NewManager();
        manager.AddServer(Holder());
        var other = new ServerConnection { ServerName = "other01", DisplayName = "Other" };
        manager.AddServer(other);

        // Only the display name changes: same storage name, same id.
        manager.UpdateServer(new ServerConnection { Id = other.Id, ServerName = "other01", DisplayName = "Renamed" });
        Assert.Equal("Renamed", manager.GetServerById(other.Id)!.DisplayName);

        // A new address that lands on a FREE id is not refused either.
        manager.UpdateServer(new ServerConnection { Id = other.Id, ServerName = "other02", DisplayName = "Renamed" });
        Assert.Equal("other02", manager.GetServerById(other.Id)!.ServerName);
    }

    [Fact]
    public void APairSavedBeforeTheCheck_LoadsWhole_AndStaysEditableWhenTheEditKeepsTheId()
    {
        // servers.json exactly as a version without the check would have written it: two servers, one id.
        var holder = Holder();
        var colliding = Colliding();
        File.WriteAllText(ServersJson, JsonSerializer.Serialize(new { Servers = new[] { holder, colliding } }));

        var manager = NewManager();

        // Loading goes around AddServer and UpdateServer, so the refusal never applies to a pair already saved.
        Assert.Equal(2, manager.GetAllServers().Count);
        Assert.Equal(holder.Id, manager.FindServerIdHolder(colliding)!.Id);

        // An edit that keeps the id is allowed although the other server still shares it.
        manager.UpdateServer(new ServerConnection { Id = colliding.Id, ServerName = CollidingHost, DisplayName = "Colliding, edited" });
        Assert.Equal("Colliding, edited", manager.GetServerById(colliding.Id)!.DisplayName);
        Assert.Null(manager.FindServerIdHolderForEdit(
            colliding,
            new ServerConnection { Id = colliding.Id, ServerName = CollidingHost }));
    }

    [Fact]
    public void FindServerIdHolder_ForAServerCheckedAgainstItself_IsNull()
    {
        var manager = NewManager();
        var holder = Holder();
        manager.AddServer(holder);

        Assert.Null(manager.FindServerIdHolder(holder));
        Assert.Null(manager.FindServerIdHolder(new ServerConnection { Id = holder.Id, ServerName = HolderHost }));
    }

    [Fact]
    public void FindServerIdHolder_NamesTheHolder_AndIsSameServerTellsTheTwoCasesApart()
    {
        var manager = NewManager();
        var holder = Holder();
        manager.AddServer(holder);
        var different = Colliding();
        var same = new ServerConnection { ServerName = HolderHost };
        var free = new ServerConnection { ServerName = "other01" };

        Assert.Equal(holder.Id, manager.FindServerIdHolder(different)!.Id);
        Assert.False(ServerManager.IsSameServer(different, holder));
        Assert.Equal(holder.Id, manager.FindServerIdHolder(same)!.Id);
        Assert.True(ServerManager.IsSameServer(same, holder));
        Assert.Null(manager.FindServerIdHolder(free));
    }

    [Fact]
    public void FindServerIdHolderForEdit_JudgesTheEditAgainstTheServerAsItWasBeforeIt()
    {
        // The edit dialog changes the live server in place, so it asks with that unedited server and a copy that
        // carries the form's identity: after the change the stored server and the edited one are one object.
        var manager = NewManager();
        var holder = Holder();
        manager.AddServer(holder);
        var other = new ServerConnection { ServerName = "other01", DisplayName = "Other" };
        manager.AddServer(other);

        var ontoTheHolder = new ServerConnection { Id = other.Id, ServerName = CollidingHost };
        var keepingItsId = new ServerConnection { Id = other.Id, ServerName = "other01" };

        Assert.Equal(holder.Id, manager.FindServerIdHolderForEdit(other, ontoTheHolder)!.Id);
        Assert.Null(manager.FindServerIdHolderForEdit(other, keepingItsId));
    }

    [Fact]
    public void DescribeIdHolder_SaysWhichCaseItIs_AndNamesTheHolder()
    {
        var holder = Holder();
        var different = Colliding();
        var same = new ServerConnection { ServerName = HolderHost };

        var collision = ServerManager.DescribeIdHolder(different, holder);
        Assert.Contains("collides with 'Holder'", collision, StringComparison.Ordinal);
        Assert.Contains("nothing was changed", collision, StringComparison.Ordinal);

        Assert.Contains("already monitored as 'Holder'", ServerManager.DescribeIdHolder(same, holder), StringComparison.Ordinal);
        Assert.Contains("Edit it from Manage Servers", ServerManager.DescribeIdHolder(same, holder), StringComparison.Ordinal);

        // An edit is already in the editor, so it is not pointed back at it.
        var edit = ServerManager.DescribeIdHolder(same, holder, isEdit: true);
        Assert.Contains("already monitored as 'Holder'", edit, StringComparison.Ordinal);
        Assert.DoesNotContain("Edit it from Manage Servers", edit, StringComparison.Ordinal);
    }

    // ---- the bulk add ---------------------------------------------------------------------------

    private static BulkServerParseLine Line(int lineNumber, string host) => new(lineNumber, host, host, null, null);

    private static ServerConnection Row(string host, string? displayName = null) =>
        new() { ServerName = host, DisplayName = displayName ?? host };

    [Fact]
    public void PlanAdditions_RefusesARowWhoseIdIsHeldByADifferentExistingServer_AndCountsItApart()
    {
        var existing = new[] { Holder() };
        var batch = new[]
        {
            (Line(1, CollidingHost), Row(CollidingHost)),
            (Line(2, CollidingHost), Row(CollidingHost)),
            (Line(3, HolderHost), Row(HolderHost)),
            (Line(4, "other01"), Row("other01")),
        };

        var plan = AddMultipleServersDialog.PlanAdditions(batch, existing);

        // The free row is the only one added; the existing server again is a duplicate; the other two collide, and
        // the repeated line collides again instead of reading as a duplicate of a server that was never added.
        Assert.Equal(new[] { 4 }, plan.ToAdd.Select(r => r.Line.LineNumber));
        Assert.Equal(1, plan.Skipped);
        Assert.Equal(new[] { 1, 2 }, plan.Collisions.Select(c => c.Line.LineNumber));
        Assert.All(plan.Collisions, c => Assert.Equal(existing[0].Id, c.Holder.Id));
        Assert.Equal(
            "Not added: its id collides with Holder",
            AddMultipleServersDialog.CollisionRowStatus(plan.Collisions[0].Holder));
    }

    [Fact]
    public void PlanAdditions_RefusesARowWhoseIdIsHeldByAnEarlierRowOfThePaste()
    {
        var first = Row(HolderHost, "First");
        var second = Row(CollidingHost, "Second");

        var plan = AddMultipleServersDialog.PlanAdditions(
            new[] { (Line(1, HolderHost), first), (Line(2, CollidingHost), second) },
            Array.Empty<ServerConnection>());

        Assert.Same(first, Assert.Single(plan.ToAdd).Server);
        Assert.Equal(0, plan.Skipped);
        var collision = Assert.Single(plan.Collisions);
        Assert.Equal(2, collision.Line.LineNumber);
        Assert.Same(first, collision.Holder);
    }

    [Fact]
    public void PlanAdditions_AddsEveryRowWhenNoIdIsShared()
    {
        var plan = AddMultipleServersDialog.PlanAdditions(
            new[] { (Line(1, "alpha01"), Row("alpha01")), (Line(2, "beta02"), Row("beta02")) },
            new[] { Holder() });

        Assert.Equal(2, plan.ToAdd.Count);
        Assert.Equal(0, plan.Skipped);
        Assert.Empty(plan.Collisions);
    }

    /* ---------------- Import Settings (#4789) ---------------- */

    /// <summary>Writes a servers.json to import from, in the shape ServerManager itself saves.</summary>
    private string WriteImportFile(params ServerConnection[] servers)
    {
        var path = Path.Combine(_configDir, "import-" + Guid.NewGuid().ToString("N") + ".json");
        File.WriteAllText(path, JsonSerializer.Serialize(new { Servers = servers }));
        return path;
    }

    private static string WarningOf(CapturingLogger log) =>
        Assert.Single(log.Entries, e => e.Level == LogLevel.Warning).Message;

    [Fact]
    public void ImportServersFromFile_RefusesAServerWhoseIdADifferentServerHolds_CountsItApart_AndNamesTheHolder()
    {
        var log = new CapturingLogger();
        var manager = NewManager(log);
        var holder = Holder();
        manager.AddServer(holder);
        var colliding = Colliding();
        var before = File.ReadAllBytes(ServersJson);

        var result = manager.ImportServersFromFile(WriteImportFile(colliding));

        Assert.Equal((0, 0, 1), result);
        Assert.Equal(new[] { holder.Id }, manager.GetAllServers().Select(s => s.Id));
        Assert.Null(manager.GetServerById(colliding.Id));
        Assert.Equal(before, File.ReadAllBytes(ServersJson));
        var warning = WarningOf(log);
        Assert.Contains("'Colliding'", warning, StringComparison.Ordinal);
        Assert.Contains("'Holder'", warning, StringComparison.Ordinal);
        Assert.Contains("collides", warning, StringComparison.Ordinal);
    }

    [Fact]
    public void ImportServersFromFile_RefusesTheSecondOfAPairInOneImport_AndKeepsTheFirst()
    {
        var log = new CapturingLogger();
        var manager = NewManager(log);
        var holder = Holder();
        var colliding = Colliding();

        var result = manager.ImportServersFromFile(WriteImportFile(holder, colliding));

        Assert.Equal((1, 0, 1), result);
        Assert.Equal(new[] { holder.Id }, manager.GetAllServers().Select(s => s.Id));

        // What was saved is what was accepted: a manager that loads the file sees the first of the pair only.
        Assert.Equal(new[] { holder.Id }, NewManager().GetAllServers().Select(s => s.Id));
        var warning = WarningOf(log);
        Assert.Contains("'Colliding'", warning, StringComparison.Ordinal);
        Assert.Contains("'Holder'", warning, StringComparison.Ordinal);
    }

    [Fact]
    public void ImportServersFromFile_CountsACollidedServerApartFromADuplicate_AndStillImportsTheFreeOnes()
    {
        var manager = NewManager();
        var holder = Holder();
        manager.AddServer(holder);
        var again = new ServerConnection { ServerName = HolderHost, DisplayName = "Again" };
        var free = new ServerConnection { ServerName = "other01", DisplayName = "Other" };

        var result = manager.ImportServersFromFile(WriteImportFile(Colliding(), again, free));

        Assert.Equal((1, 1, 1), result);
        Assert.Equal(
            new[] { holder.Id, free.Id }.OrderBy(id => id, StringComparer.Ordinal),
            manager.GetAllServers().Select(s => s.Id).OrderBy(id => id, StringComparer.Ordinal));
    }

    [Fact]
    public void ImportServersFromFile_OfTheSameServerAgain_StaysASkip_WithNoWarning()
    {
        var log = new CapturingLogger();
        var manager = NewManager(log);
        var holder = Holder();
        manager.AddServer(holder);
        var underANewGuid = new ServerConnection { ServerName = HolderHost, DisplayName = "Again" };

        // The same entry (the same GUID) and the same server under a new GUID: both are the server already monitored.
        var result = manager.ImportServersFromFile(WriteImportFile(holder, underANewGuid));

        Assert.Equal((0, 2, 0), result);
        Assert.Single(manager.GetAllServers());
        Assert.DoesNotContain(log.Entries, e => e.Level == LogLevel.Warning);
    }

    [Fact]
    public void ImportServersFromFile_OfAPairSavedBeforeTheCheck_SkipsBothAsDuplicates_NotAsCollided()
    {
        // servers.json exactly as a version without the check would have written it: two servers, one id.
        var holder = Holder();
        var colliding = Colliding();
        File.WriteAllText(ServersJson, JsonSerializer.Serialize(new { Servers = new[] { holder, colliding } }));
        var log = new CapturingLogger();
        var manager = NewManager(log);

        // Importing that same file again finds both servers already there, so neither is a collision to report.
        var result = manager.ImportServersFromFile(WriteImportFile(holder, colliding));

        Assert.Equal((0, 2, 0), result);
        Assert.Equal(2, manager.GetAllServers().Count);
        Assert.DoesNotContain(log.Entries, e => e.Level == LogLevel.Warning);
    }

    [Fact]
    public void ImportSettings_ReportsTheCollidedCount_NextToTheSkippedOne()
    {
        var window = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "MainWindow.xaml.cs"));

        Assert.Contains(
            "var (imported, skipped, collided) = _serverManager.ImportServersFromFile(",
            window,
            StringComparison.Ordinal);
        Assert.Contains("if (collided > 0)", window, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }

    /// <summary>Captures formatted log lines with their level, so a refusal's warning can be asserted.</summary>
    private sealed class CapturingLogger : ILogger<ServerManager>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Entries.Add((logLevel, formatter(state, exception)));
    }
}
