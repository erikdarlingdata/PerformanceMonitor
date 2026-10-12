/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// F14: the dedup key hashes the server's display name, so two registrations with the SAME display name (two
/// databases on one Azure SQL Database server with blank names, or two typed "Prod") sent the same key for the
/// same incident and a downstream pager merged two real incidents. Only a name that another registration also
/// carries gets the store id; a server whose display name is unique keeps its key byte for byte, host-named or not.
/// </summary>
public class AlertFingerprintServerIdentityTests
{
    /// <summary>Captured from the code BEFORE this fix: ForObjects("Prod SQL 1", Deadlock, [dbo.t1, dbo.t2]).
    /// If this literal ever has to change, every named server's dedup key in every integration changed.</summary>
    private const string GoldenNamedServerKey = "f95e071656627f742e706ec57659c37b913b3421579427b907ed9ebb2bfc37cf";

    /// <summary>ForObjects("host1", Deadlock, [dbo.t1, dbo.t2]) — a server alone under the display name "host1".</summary>
    private static readonly string GoldenHost1Key = AlertFingerprint.ForObjects("host1", AlertFingerprint.Deadlock, new[] { "dbo.t1", "dbo.t2" })!.DedupKey;

    private static readonly string[] Objects = { "dbo.t1", "dbo.t2" };

    private static string Key(string serverName) =>
        AlertFingerprint.ForObjects(serverName, AlertFingerprint.Deadlock, Objects)!.DedupKey;

    private static AlertServerSnapshot Snapshot(string name, int? id, bool shared) =>
        new("k", name, true, null, null, false, false, null) { ServerId = id, ServerNameIsShared = shared };

    private static MonitoredServer Server(string name, string host, string? database = null) =>
        new() { Name = name, Host = host, Database = database ?? "" };

    /// <summary>The worker's flag for one registration, over a registry holding <paramref name="all"/>.</summary>
    private static bool WorkerFlag(MonitoredServer one, params MonitoredServer[] all)
    {
        var state = new PerformanceMonitor.Darling.Service.Mcp.MonitoredServerRegistryState();
        state.Publish(all);
        return DarlingWorker.ServerNameIsShared(state.Read(), one);
    }

    [Fact]
    public void NamedServer_KeyIsByteIdenticalToTodays_ThroughServerIdentityAndTheSnapshot()
    {
        Assert.Equal(GoldenNamedServerKey, Key("Prod SQL 1"));
        Assert.Equal(GoldenNamedServerKey, Key(AlertFingerprint.ServerIdentity("Prod SQL 1", 7, false)));
        Assert.Equal(GoldenNamedServerKey, Key(Snapshot("Prod SQL 1", 7, false).FingerprintServerName));
    }

    /// <summary>A server added by host with no display name, and alone under that name, keeps the key it had
    /// before this change: the golden value for a host-named lone server.</summary>
    [Fact]
    public void ALoneHostNamedServer_KeepsItsKeyByteForByte()
    {
        var lone = Server("", "host1");
        Assert.False(WorkerFlag(lone, lone, Server("Prod", "host2")));
        Assert.Equal(GoldenHost1Key, Key(Snapshot("host1", 7, WorkerFlag(lone, lone, Server("Prod", "host2"))).FingerprintServerName));
    }

    [Fact]
    public void TwoBlankNamedServersOnOneHost_GetDifferentKeys()
    {
        var a = Server("", "host1", "dbA");
        var b = Server("", "host1", "dbB");
        var keyA = Key(Snapshot("host1", 7, WorkerFlag(a, a, b)).FingerprintServerName);
        var keyB = Key(Snapshot("host1", 8, WorkerFlag(b, a, b)).FingerprintServerName);
        Assert.NotEqual(keyA, keyB);
        Assert.NotEqual(GoldenHost1Key, keyA);
    }

    [Fact]
    public void TwoServersTypedWithTheSameName_GetDifferentKeys()
    {
        var a = Server("Prod", "host1");
        var b = Server("Prod", "host2");
        Assert.True(WorkerFlag(a, a, b));
        var keyA = Key(Snapshot("Prod", 7, WorkerFlag(a, a, b)).FingerprintServerName);
        var keyB = Key(Snapshot("Prod", 8, WorkerFlag(b, a, b)).FingerprintServerName);
        Assert.NotEqual(keyA, keyB);
    }

    [Fact]
    public void SameBlankNamedServer_GetsTheSameKeyFromFreshSnapshots()
    {
        Assert.Equal(
            Key(Snapshot("host1", 7, true).FingerprintServerName),
            Key(Snapshot("host1", 7, true).FingerprintServerName));
    }

    [Fact]
    public void ServerIdentity_AppendsTheIdOnlyForASharedNameWithAnId()
    {
        Assert.Equal("host1#7", AlertFingerprint.ServerIdentity("host1", 7, true));
        Assert.Equal("Prod#7", AlertFingerprint.ServerIdentity("Prod", 7, true));
        Assert.Equal("host1", AlertFingerprint.ServerIdentity("host1", null, true));
        Assert.Equal("host1", AlertFingerprint.ServerIdentity("host1", 7, false));
    }

    [Fact]
    public void SharedDisplayNames_AreTheNamesMoreThanOneRegistrationCarries_Ordinal()
    {
        var shared = AlertFingerprint.SharedDisplayNames(new[] { "Prod", "Prod", "host1", "Host1", "solo" });
        Assert.Equal(new[] { "Prod" }, shared.OrderBy(n => n, StringComparer.Ordinal));
        Assert.Empty(AlertFingerprint.SharedDisplayNames(Array.Empty<string>()));
    }

    /// <summary>Adding a second same-named registration suffixes both; removing it un-suffixes the survivor.</summary>
    [Fact]
    public void JoiningAndLeaving_SuffixesBothThenUnsuffixesTheSurvivor()
    {
        var a = Server("", "host1", "dbA");
        var b = Server("", "host1", "dbB");
        var before = Key(Snapshot("host1", 7, WorkerFlag(a, a)).FingerprintServerName);
        var joined = Key(Snapshot("host1", 7, WorkerFlag(a, a, b)).FingerprintServerName);
        var joinedB = Key(Snapshot("host1", 8, WorkerFlag(b, a, b)).FingerprintServerName);
        var after = Key(Snapshot("host1", 7, WorkerFlag(a, a)).FingerprintServerName);

        Assert.Equal(GoldenHost1Key, before);
        Assert.NotEqual(before, joined);
        Assert.NotEqual(joined, joinedB);
        Assert.Equal(before, after);
    }

    /// <summary>The MCP filter computes the flag over the store's enabled rows with the same helper, so for one
    /// population it gives the worker's key.</summary>
    [Fact]
    public void WorkerAndMcpFilter_GiveTheSameKeyForOnePopulation()
    {
        var a = Server("", "host1", "dbA");
        var b = Server("Prod", "host2");
        var c = Server("Prod", "host3");
        var rows = new[]
        {
            new DarlingServerResolver.RegisteredServer(7, "host1:dbA", a.DisplayName),
            new DarlingServerResolver.RegisteredServer(8, "host1:dbB", "host1"),
            new DarlingServerResolver.RegisteredServer(9, "host2", b.DisplayName),
            new DarlingServerResolver.RegisteredServer(10, "host3", c.DisplayName),
            new DarlingServerResolver.RegisteredServer(11, "host4", "host4"),
        };
        var shared = DarlingServerResolver.SharedNamesOf(rows);
        var population = new[] { a, Server("", "host1", "dbB"), b, c, Server("host4", "host4") };

        for (var i = 0; i < rows.Length; i++)
        {
            var alertSide = Snapshot(population[i].DisplayName, rows[i].ServerId,
                WorkerFlag(population[i], population)).FingerprintServerName;
            Assert.Equal(alertSide, DarlingServerResolver.FingerprintNameOf(rows[i], shared));
        }
    }

    [Fact]
    public void DisplayName_IsTheNameOrTheHostWhenBlank()
    {
        Assert.Equal("host1", Server("", "host1").DisplayName);
        Assert.Equal("host1", Server("  ", "host1").DisplayName);
        Assert.Equal("Prod SQL 1", Server("Prod SQL 1", "host1").DisplayName);
    }
}
