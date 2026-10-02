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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Several monitored databases on one Azure SQL Database server are separate servers that share one host name and
/// differ only in database. The viewer's Import Settings used to judge "already here" by the host name alone, so it
/// kept the first database on a host and dropped every other one as a duplicate. The <c>--open-server</c> deep link
/// opened the first server that answered to a host name, though until the service first connects to each database
/// the server list names every one of them by that host.
/// </summary>
public sealed class ViewerSameHostServersTests : IDisposable
{
    private const string Host = "shared-host";

    private readonly string _path = Path.Combine(Path.GetTempPath(), $"viewer-servers-{Guid.NewGuid():N}.json");

    public void Dispose()
    {
        try
        {
            if (File.Exists(_path))
            {
                File.Delete(_path);
            }
        }
        catch
        {
            /* best-effort temp cleanup */
        }
    }

    private ViewerServerStore NewStore() => new(_path, new FakeSecretStore());

    private static ViewerServerEntry Entry(string? database, bool readOnlyIntent = false) => new()
    {
        ServerName = Host,
        DatabaseName = database,
        ReadOnlyIntent = readOnlyIntent,
        DisplayName = database is null ? Host : $"{Host}/{database}"
    };

    private static (int Imported, int Skipped) ImportFrom(ViewerServerStore target, params ViewerServerEntry[] incoming)
    {
        var source = Path.Combine(Path.GetTempPath(), $"viewer-servers-src-{Guid.NewGuid():N}.json");
        try
        {
            var sourceStore = new ViewerServerStore(source, new FakeSecretStore());
            foreach (var entry in incoming)
            {
                sourceStore.AddServer(entry, null, null);
            }

            return target.ImportServersFromFile(source);
        }
        finally
        {
            try { File.Delete(source); } catch { /* best-effort */ }
        }
    }

    [Fact]
    public void Import_KeepsEveryDatabaseOnOneHost_AndSkipsOnlyTheServerTheRegistryAlreadyHolds()
    {
        var store = NewStore();
        store.AddServer(Entry("DbA"), null, null);

        var (imported, skipped) = ImportFrom(store, Entry("DbA"), Entry("DbB"), Entry("DbC"));

        Assert.Equal(2, imported);   // DbB and DbC: other databases on a host the registry already knows
        Assert.Equal(1, skipped);    // DbA: the server it already holds
        Assert.Equal(
            new[] { "DbA", "DbB", "DbC" },
            store.GetAllServers().Select(s => s.DatabaseName!).OrderBy(d => d, StringComparer.Ordinal));
    }

    [Fact]
    public void Import_TreatsTheSameDatabaseInAnotherLetterCase_AsTheServerAlreadyHeld()
    {
        var store = NewStore();
        store.AddServer(Entry("DbA"), null, null);

        var (imported, skipped) = ImportFrom(store, new ViewerServerEntry { ServerName = Host.ToUpperInvariant(), DatabaseName = "DBA" });

        Assert.Equal(0, imported);
        Assert.Equal(1, skipped);
        Assert.Single(store.GetAllServers());
    }

    [Fact]
    public void Import_TreatsAHostWithNoDatabase_AndTheSameHostWithOne_AsTwoServers()
    {
        var store = NewStore();
        store.AddServer(Entry(null), null, null);

        var (imported, skipped) = ImportFrom(store, Entry(null), Entry("DbA"), Entry(null, readOnlyIntent: true));

        Assert.Equal(2, imported);   // the host with a database, and the host's read-only registration
        Assert.Equal(1, skipped);    // the host with no database, which the registry already holds
        Assert.Equal(3, store.GetAllServers().Count);
    }

    private static DarlingServer Listed(int serverId, string serverName, string displayName) =>
        new(serverId, serverName, displayName, isEnabled: true, sqlMajorVersion: null);

    [Fact]
    public void ServersNamed_GivenAHostSeveralNotYetCollectedDatabasesShare_ReturnsEveryOne()
    {
        /* Before the service first connects to a database its row carries the host as its name, which every
           database on the host shares; the display names are what tell them apart. */
        var fleet = new[]
        {
            Listed(1, Host, "orders"),
            Listed(2, Host, "billing"),
            Listed(3, "another-host", "another")
        };

        var named = ViewerArgs.ServersNamed(fleet, Host.ToUpperInvariant());

        Assert.Equal(new[] { 1, 2 }, named.Select(s => s.ServerId).ToArray());
    }

    [Fact]
    public void OpenServer_GivenOneCollectedDatabasesName_NamesThatDatabaseAlone()
    {
        var fleet = new[]
        {
            Listed(1, Host + ":DbA", "orders"),
            Listed(2, Host + ":DbB", "billing"),
            Listed(3, Host + ":DbC", "audit")
        };

        Assert.Equal(2, Assert.Single(ViewerArgs.ServersNamed(fleet, Host + ":dbb")).ServerId);
        Assert.Equal(3, Assert.Single(ViewerArgs.ServersNamed(fleet, Host + ":DbC")).ServerId);
        Assert.Empty(ViewerArgs.ServersNamed(fleet, Host));
        Assert.Empty(ViewerArgs.ServersNamed(fleet, "no-such-server"));
    }

    [Fact]
    public void ChooseServerToOpen_OneMatch_OpensIt()
    {
        var fleet = new[] { Listed(1, Host + ":DbA", "orders"), Listed(2, Host + ":DbB", "billing") };

        var choice = ViewerArgs.ChooseServerToOpen(ViewerArgs.ServersNamed(fleet, Host + ":dbb"));

        Assert.NotNull(choice.Open);
        Assert.Equal(2, choice.Open!.ServerId);
        Assert.Empty(choice.Ambiguous);
    }

    [Fact]
    public void ChooseServerToOpen_SeveralMatches_OpensNone_AndListsEveryOne()
    {
        var fleet = new[]
        {
            Listed(1, Host, "orders"),
            Listed(2, Host, "billing"),
            Listed(3, Host, "audit"),
            Listed(4, "another-host", "another")
        };

        var choice = ViewerArgs.ChooseServerToOpen(ViewerArgs.ServersNamed(fleet, Host));

        Assert.Null(choice.Open);
        Assert.Equal(new[] { 1, 2, 3 }, choice.Ambiguous.Select(s => s.ServerId).ToArray());
    }

    [Fact]
    public void ChooseServerToOpen_NoMatch_OpensNone_AndListsNothing()
    {
        var fleet = new[] { Listed(1, Host + ":DbA", "orders") };

        var choice = ViewerArgs.ChooseServerToOpen(ViewerArgs.ServersNamed(fleet, "no-such-server"));

        Assert.Null(choice.Open);
        Assert.Empty(choice.Ambiguous);
    }

    [Fact]
    public void DescribeServer_TellsTwoServersThatShareADisplayNameApart_ByTheirStoredName()
    {
        var first = Listed(1, Host + ":DbA", "orders");
        var second = Listed(2, Host + ":DbB", "orders");
        var plain = Listed(3, "plain-host", "plain-host");

        Assert.Equal("orders (" + Host + ":DbA)", ViewerArgs.DescribeServer(first));
        Assert.Equal("orders (" + Host + ":DbB)", ViewerArgs.DescribeServer(second));
        Assert.Equal("plain-host", ViewerArgs.DescribeServer(plain));
    }

    /// <summary>In-memory <see cref="IViewerServerSecretStore"/> so tests never touch Windows Credential Manager.</summary>
    private sealed class FakeSecretStore : IViewerServerSecretStore
    {
        private readonly Dictionary<string, (string Username, string Password)> _map = new();

        public void Save(string id, string username, string password) => _map[id] = (username, password);

        public (string Username, string Password)? Find(string id) =>
            _map.TryGetValue(id, out var v) ? v : null;

        public void Delete(string id) => _map.Remove(id);
    }
}
