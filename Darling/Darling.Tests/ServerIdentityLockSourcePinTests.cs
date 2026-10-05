/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The desktop viewer takes the same server identity lock as the web and MCP writers (#5240, PR 2), and these facts
/// hold the parts of that a live race cannot: the viewer's COPY of the lock text is the service's text, and each
/// identity write takes the lock first, in the transaction that writes. The live facts are in
/// <see cref="ServerIdentityLockLiveTests"/>; these run without a store.
///
/// <para>The Viewer project cannot reference the service, so <see cref="ViewerDataService.MonitoredServerIdentityLockSql"/>
/// is a copy of <c>DarlingMcpServerAdminTools.IdentityLockSql</c>. A copy that drifted would take a different advisory
/// key and serialise with nothing, and every write in the viewer would look locked while racing the web and MCP
/// writers freely.</para>
/// </summary>
public sealed class ServerIdentityLockSourcePinTests
{
    private static readonly Regex LockLiteral = new(
        @"const\s+string\s+\w*IdentityLock\w*Sql\s*=\s*""(?<text>[^""]*)""",
        RegexOptions.CultureInvariant);

    private static string ServiceSource() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpServerAdminTools.cs");

    private static string ViewerSource() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.MonitoredServers.cs");

    private static string StoreConfigSource() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "StoreConfigProvider.cs");

    /// <summary>The text between two markers, which must both be there and in that order.</summary>
    private static string Between(string source, string startMarker, string endMarker)
    {
        var start = source.IndexOf(startMarker, StringComparison.Ordinal);
        Assert.True(start >= 0, "missing: " + startMarker);
        var end = source.IndexOf(endMarker, start + startMarker.Length, StringComparison.Ordinal);
        Assert.True(end > start, "missing, or not after the start: " + endMarker);
        return source[start..end];
    }

    /// <summary>Asserts each needle is in <paramref name="body"/>, in the order given.</summary>
    private static void AssertInOrder(string body, params string[] needles)
    {
        var at = -1;
        foreach (var needle in needles)
        {
            var found = body.IndexOf(needle, at + 1, StringComparison.Ordinal);
            Assert.True(found > at, "expected, after the step before it: " + needle);
            at = found;
        }
    }

    [Fact]
    public void TheViewersLockText_IsTheServicesLockText_InSourceAndAsCompiled()
    {
        var service = LockLiteral.Match(ServiceSource());
        var viewer = LockLiteral.Match(ViewerSource());

        /* Positive control: both declarations are found, so an empty equals-empty cannot pass. */
        Assert.True(service.Success, "the service's lock constant was not found in DarlingMcpServerAdminTools.cs");
        Assert.True(viewer.Success, "the viewer's lock constant was not found in ViewerDataService.MonitoredServers.cs");
        Assert.Contains("pg_advisory_xact_lock", service.Groups["text"].Value, StringComparison.Ordinal);

        Assert.Equal(service.Groups["text"].Value, viewer.Groups["text"].Value);
        Assert.Equal(DarlingMcpServerAdminTools.IdentityLockSql, ViewerDataService.MonitoredServerIdentityLockSql);
    }

    [Fact]
    public void TheViewersAdd_TakesTheLockAndReadsTheAddressesFirst_ThenInsertsAndCommitsInOneTransaction()
    {
        var body = Between(
            ViewerSource(),
            "public async Task<MonitoredServerAddResult> AddMonitoredServerAsync(",
            "public static MonitoredServerAddOutcome ClassifyAddAgainstOccupant(");

        AssertInOrder(
            body,
            "BeginTransactionAsync(",
            "TakeIdentityLockAndFindClaimantAsync(",
            "MonitoredServerAddOutcome.Duplicate",
            "new NpgsqlCommand(MonitoredServerInsertIfAbsentSql, connection, transaction)",
            "CommitAsync(");
        Assert.DoesNotContain("_dataSource.CreateCommand(", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheViewersEdit_TakesTheLockAndReadsTheOtherAddressesFirst_ThenUpsertsAndCommitsInOneTransaction()
    {
        var body = Between(
            ViewerSource(),
            "public async Task UpsertMonitoredServerAsync(",
            "private static string StorageKeyOf(MonitoredServerRow row)");

        AssertInOrder(
            body,
            "BeginTransactionAsync(",
            "TakeIdentityLockAndFindClaimantAsync(connection, transaction, row, row.ServerId, cancellationToken)",
            "throw new MonitoredServerAddressClaimedException(",
            "new NpgsqlCommand(MonitoredServerUpsertSql, connection, transaction)",
            "CommitAsync(");
        Assert.DoesNotContain("_dataSource.CreateCommand(", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheViewersInsertIfAbsent_IsAnIdentityWriteToo_LockFirstThenInsertInOneTransaction()
    {
        var body = Between(
            ViewerSource(),
            "public async Task<bool> InsertMonitoredServerIfAbsentAsync(",
            "public async Task<MonitoredServerAddResult> AddMonitoredServerAsync(");

        AssertInOrder(
            body,
            "BeginTransactionAsync(",
            "TakeIdentityLockAndFindClaimantAsync(connection, transaction, row, null, cancellationToken)",
            "new NpgsqlCommand(MonitoredServerInsertIfAbsentSql, connection, transaction)",
            "CommitAsync(");
        Assert.DoesNotContain("_dataSource.CreateCommand(", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheViewersHelper_TakesTheLockBeforeItReadsAnyAddress_BothOnTheCallersTransaction()
    {
        var body = Between(
            ViewerSource(),
            "private static async Task<MonitoredServerRow?> TakeIdentityLockAndFindClaimantAsync(",
            "public async Task<bool> InsertMonitoredServerIfAbsentAsync(");

        AssertInOrder(
            body,
            "new NpgsqlCommand(MonitoredServerIdentityLockSql, connection, transaction)",
            "new NpgsqlCommand(MonitoredServersSelectSql, connection, transaction)",
            "StringComparison.OrdinalIgnoreCase");
    }

    [Fact]
    public void TheStartupSeed_TakesTheSameLockFirst_ReadsTheHeldAddresses_ThenInsertsInOneTransaction()
    {
        var body = Between(
            StoreConfigSource(),
            "private static async Task SeedMonitoredServersAsync(",
            "/* ---------------- read (store -> in-memory view) ---------------- */");

        AssertInOrder(
            body,
            "BeginTransactionAsync(",
            "DarlingMcpServerAdminTools.IdentityLockSql, connection, transaction",
            "DarlingMcpServerAdminTools.ExistingServersSql, connection, transaction",
            "heldAddresses.Add(server.StorageName)",
            "INSERT INTO config_monitored_servers",
            "ON CONFLICT (server_id) DO NOTHING\", connection, transaction)",
            "CommitAsync(");
    }
}
