/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2254: the diagnostic for servers that exist in <c>darling.json</c> but not in the store.
///
/// <para><b>The field report.</b> An operator added a second server to the file, ran
/// <c>--test-connection</c> (which validated BOTH as PASS, because it reads the file), restarted the service,
/// and the server never appeared. The seed only runs while <c>config_monitored_servers</c> is empty, so after
/// the first start a file edit is a permanent no-op — and nothing said so. Two correct outputs answering
/// different questions, with no way to see the disagreement: config edit, restart, support round trip.</para>
///
/// <para><b>Why the comparison is on server_id.</b> That is the identity the collectors and the registry key
/// on. Comparing by NAME would report a file entry as present when its host or read-only intent differs from
/// the stored row — which is the same identity confusion behind the split-history class in #2158. The
/// same-name-different-host case below is the one that would pass a naive name match.</para>
/// </summary>
public sealed class FileOnlyServerWarningTests
{
    private static MonitoredServer Server(string name, string host) =>
        new() { Name = name, Host = host };

    private static int IdOf(MonitoredServer server) =>
        ServerIdHelper.GetDeterministicHashCode(server.StorageName);

    /// <summary>The steady state: everything in the file is registered, so there is nothing to warn about and
    /// the log stays quiet on every start.</summary>
    [Fact]
    public void WhenTheStoreHasEveryFileServer_NothingIsReported()
    {
        var a = Server("plvs-itebd", "plvs-itebd");
        var b = Server("dcvs-bd01", "dcvs-bd01");

        var missing = StoreConfigProvider.ServersOnlyInFile(
            new[] { a, b },
            new HashSet<int> { IdOf(a), IdOf(b) });

        Assert.Empty(missing);
    }

    /// <summary>The reported case exactly: two in the file, one seeded, and the second silently ignored.</summary>
    [Fact]
    public void TheServerAddedAfterTheSeed_IsReportedByName()
    {
        var seeded = Server("plvs-itebd", "plvs-itebd");
        var added = Server("dcvs-bd01", "dcvs-bd01");

        var missing = StoreConfigProvider.ServersOnlyInFile(
            new[] { seeded, added },
            new HashSet<int> { IdOf(seeded) });

        Assert.Equal(new[] { "dcvs-bd01" }, missing);
    }

    /// <summary>
    /// The case a name comparison gets wrong. Same display name, different host — so a different
    /// <c>server_id</c>, a different registry row, and a server that is genuinely not being monitored under
    /// the identity the file describes.
    /// </summary>
    [Fact]
    public void SameNameButADifferentHost_IsStillReportedAsAbsent()
    {
        var inFile = Server("reporting", "reporting-new-host");
        var inStore = Server("reporting", "reporting-old-host");

        Assert.NotEqual(IdOf(inFile), IdOf(inStore));

        var missing = StoreConfigProvider.ServersOnlyInFile(
            new[] { inFile },
            new HashSet<int> { IdOf(inStore) });

        Assert.Equal(new[] { "reporting" }, missing);
    }

    /// <summary>An empty store reports everything. This is the shape a seed that failed leaves behind, and it
    /// should be loud rather than silent.</summary>
    [Fact]
    public void AnEmptyStoreReportsEveryFileServer()
    {
        var missing = StoreConfigProvider.ServersOnlyInFile(
            new[] { Server("a", "host-a"), Server("b", "host-b") },
            new HashSet<int>());

        Assert.Equal(new[] { "a", "b" }, missing);
    }

    /// <summary>A config with no servers must not throw on the startup path — this runs before anything is
    /// monitored, so a null-reference here would take the service down over a diagnostic.</summary>
    [Fact]
    public void ANullOrEmptyFileListIsNotAnError()
    {
        Assert.Empty(StoreConfigProvider.ServersOnlyInFile(null!, new HashSet<int>()));
        Assert.Empty(StoreConfigProvider.ServersOnlyInFile(new List<MonitoredServer>(), new HashSet<int> { 1 }));
    }
}
