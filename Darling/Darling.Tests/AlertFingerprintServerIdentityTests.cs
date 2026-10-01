/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// F14: the dedup key hashes the server's display name, and a blank name falls back to the host, so two
/// databases registered with blank names on one Azure SQL Database server sent the SAME key for the same
/// incident and a downstream pager merged two real incidents. Only the host-fallback case gets the store
/// id; every named server's key must stay byte-identical so integrations see no change.
/// </summary>
public class AlertFingerprintServerIdentityTests
{
    /// <summary>Captured from the code BEFORE this fix: ForObjects("Prod SQL 1", Deadlock, [dbo.t1, dbo.t2]).
    /// If this literal ever has to change, every named server's dedup key in every integration changed.</summary>
    private const string GoldenNamedServerKey = "f95e071656627f742e706ec57659c37b913b3421579427b907ed9ebb2bfc37cf";

    private static readonly string[] Objects = { "dbo.t1", "dbo.t2" };

    private static string Key(string serverName) =>
        AlertFingerprint.ForObjects(serverName, AlertFingerprint.Deadlock, Objects)!.DedupKey;

    private static AlertServerSnapshot Snapshot(string name, int? id, bool fallback) =>
        new("k", name, true, null, null, false, false, null) { ServerId = id, ServerNameIsHostFallback = fallback };

    [Fact]
    public void NamedServer_KeyIsByteIdenticalToTodays_ThroughServerIdentityAndTheSnapshot()
    {
        Assert.Equal(GoldenNamedServerKey, Key("Prod SQL 1"));
        Assert.Equal(GoldenNamedServerKey, Key(AlertFingerprint.ServerIdentity("Prod SQL 1", 7, false)));
        Assert.Equal(GoldenNamedServerKey, Key(Snapshot("Prod SQL 1", 7, false).FingerprintServerName));
    }

    [Fact]
    public void TwoBlankNamedServersOnOneHost_GetDifferentKeys()
    {
        var a = Key(Snapshot("host1", 7, true).FingerprintServerName);
        var b = Key(Snapshot("host1", 8, true).FingerprintServerName);
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void SameBlankNamedServer_GetsTheSameKeyFromFreshSnapshots()
    {
        Assert.Equal(
            Key(Snapshot("host1", 7, true).FingerprintServerName),
            Key(Snapshot("host1", 7, true).FingerprintServerName));
    }

    [Fact]
    public void ServerIdentity_AppendsTheIdOnlyForTheFallbackWithAnId()
    {
        Assert.Equal("host1#7", AlertFingerprint.ServerIdentity("host1", 7, true));
        Assert.Equal("host1", AlertFingerprint.ServerIdentity("host1", null, true));
        Assert.Equal("host1", AlertFingerprint.ServerIdentity("host1", 7, false));
    }

    [Fact]
    public void DisplayNameIsHostFallback_IsTrueWhenTheNameIsBlankOrIsExactlyTheHost()
    {
        Assert.True(new MonitoredServer { Name = "", Host = "host1" }.DisplayNameIsHostFallback);
        Assert.True(new MonitoredServer { Name = "  ", Host = "host1" }.DisplayNameIsHostFallback);
        /* A name typed identical to the host displays exactly like the fallback, and the registry cannot tell the
           two apart, so it keys the same way. */
        Assert.True(new MonitoredServer { Name = "host1", Host = "host1" }.DisplayNameIsHostFallback);
        Assert.False(new MonitoredServer { Name = "Host1", Host = "host1" }.DisplayNameIsHostFallback);
        var named = new MonitoredServer { Name = "Prod SQL 1", Host = "host1" };
        Assert.False(named.DisplayNameIsHostFallback);
        Assert.Equal("Prod SQL 1", named.DisplayName);
    }
}
