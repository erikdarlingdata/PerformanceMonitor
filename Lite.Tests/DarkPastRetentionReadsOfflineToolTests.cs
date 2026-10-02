/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3967, the Lite half: a server whose whole history has aged out of the archive reads Offline on the Overview
/// card, not "Never" beside an amber "No collection has ever landed for this server".
///
/// <para><b>Why Lite has the case.</b> The card's newest-collection read has no window (hot table, then the
/// archive view), but <see cref="RetentionService.CleanupOldArchives"/> deletes archive files after
/// <see cref="RetentionService.ArchiveRetentionMonths"/> months. A server dark for longer has no row anywhere,
/// and the ladder calls a null never collected. The registration is the server's <c>CreatedDate</c> in
/// servers.json: Lite collects from a server only once it has been added, so the Darling rule applies unchanged,
/// with what the archive is certain to still hold as the horizon.</para>
/// </summary>
public sealed class DarkPastRetentionReadsOfflineToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;

    public DarkPastRetentionReadsOfflineToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-3967-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    /// <summary>
    /// The horizon: the first month start at or after the cleanup's cutoff, because a month's archive file goes
    /// whole. Exact month starts stay where they are; a month-end clamps the way <c>AddMonths</c> does.
    /// </summary>
    [Fact]
    public void TheHorizon_IsTheFirstMonthStartTheArchiveStillHoldsWhole()
    {
        Assert.Equal(new DateTime(2026, 7, 1), RetentionService.OldestRetainedInstant(new DateTime(2026, 9, 23, 12, 0, 0, DateTimeKind.Utc)));
        Assert.Equal(new DateTime(2026, 6, 1), RetentionService.OldestRetainedInstant(new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc)));
        Assert.Equal(new DateTime(2026, 3, 1), RetentionService.OldestRetainedInstant(new DateTime(2026, 5, 31, 10, 0, 0, DateTimeKind.Utc)));
        Assert.Equal(DateTimeKind.Unspecified, RetentionService.OldestRetainedInstant(DateTime.UtcNow).Kind);
    }

    /// <summary>
    /// The horizon against the cleanup itself, so the two cannot drift: the monthly file for the horizon's
    /// month survives <see cref="RetentionService.CleanupOldArchives"/>, and the month before it does not.
    /// </summary>
    [Fact]
    public void TheCleanup_KeepsTheHorizonsMonth_AndDeletesTheOneBefore()
    {
        var archive = Path.Combine(_configDir, "archive");
        Directory.CreateDirectory(archive);

        var horizon = RetentionService.OldestRetainedInstant(DateTime.UtcNow);
        var kept = Path.Combine(archive, $"{horizon:yyyyMM}_collection_log.parquet");
        var deleted = Path.Combine(archive, $"{horizon.AddMonths(-1):yyyyMM}_collection_log.parquet");
        File.WriteAllBytes(kept, [1]);
        File.WriteAllBytes(deleted, [1]);

        new RetentionService(archive).CleanupOldArchives();

        Assert.True(File.Exists(kept), "the archive deleted the month the horizon says is still held");
        Assert.False(File.Exists(deleted), "the month before the horizon survived the cleanup, so the horizon is later than it needs to be");
    }

    /// <summary>
    /// The card, through the shipped summary read: a server added six months ago with no row left reads
    /// Offline, and its Last Collect row says none is retained; one added ten days ago with no row still reads
    /// never collected; one with no registration keeps that reading too.
    /// </summary>
    [Fact]
    public async Task TheOverviewCard_ReadsAServerWhoseHistoryAgedOut_Offline()
    {
        var service = new LocalDataService(_duckDb);

        var agedOut = await service.GetServerSummaryAsync(967001, "aged-out", DateTime.UtcNow.AddMonths(-6));
        Assert.Equal(ServerFreshness.Offline, agedOut!.CollectionFreshness);
        Assert.Null(agedOut.LastCollectionTime);
        Assert.Equal("None retained (stopped)", agedOut.LastCollectionDisplay);

        var justAdded = await service.GetServerSummaryAsync(967002, "just-added", DateTime.UtcNow.AddDays(-10));
        Assert.Equal(ServerFreshness.NeverCollected, justAdded!.CollectionFreshness);
        Assert.Equal("Never", justAdded.LastCollectionDisplay);

        var unknown = await service.GetServerSummaryAsync(967003, "unknown", registeredAtUtc: null);
        Assert.Equal(ServerFreshness.NeverCollected, unknown!.CollectionFreshness);
    }

    /// <summary>A reading with a newest collection is untouched: an old registration does not move a fresh,
    /// stale or offline card.</summary>
    [Fact]
    public void AReadingWithANewestCollection_BandsOnTheLadder_WhateverTheRegistration()
    {
        var now = DateTime.UtcNow;
        foreach (var lastCollection in new[] { now.AddSeconds(-30), now.AddMinutes(-10), now.AddHours(-3), now.AddDays(-20) })
        {
            var card = new ServerSummaryItem { LastCollectionTime = lastCollection };
            card.ApplyCollectionFreshness(now, now.AddMonths(-6));
            Assert.Equal(ServerHealthClassifier.ClassifyFreshness(lastCollection, now), card.CollectionFreshness);
        }
    }

    /// <summary>
    /// The seam from servers.json: the registration the Overview and both MCP reads pass is the server's
    /// <c>CreatedDate</c> in UTC, and it is not written back to servers.json.
    /// </summary>
    [Fact]
    public async Task TheRegistration_ComesFromTheServerEntry_AndIsNotPersisted()
    {
        var added = DateTime.Now.AddMonths(-6);
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "AgedOutSrv", IsEnabled = true, CreatedDate = added };
        _serverManager.AddServer(server);

        Assert.Equal(added.ToUniversalTime(), server.RegisteredAtUtc);
        Assert.DoesNotContain("RegisteredAtUtc", JsonSerializer.Serialize(server), StringComparison.Ordinal);

        var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        Assert.Equal(server.RegisteredAtUtc, ServerResolver.RegisteredAtUtc(_serverManager, serverId));

        /* And list_servers says so, where it used to say "No data collected". */
        var list = await McpDiscoveryTools.ListServers(_serverManager, new LocalDataService(_duckDb));
        Assert.Contains("(last collection: none retained)", list, StringComparison.Ordinal);
        Assert.DoesNotContain("No data collected", list, StringComparison.Ordinal);
    }

    /// <summary>A server added ten days ago with nothing collected yet is still "No data collected" in
    /// list_servers — the reading the fix must not disturb.</summary>
    [Fact]
    public async Task ListServers_StillSaysNoDataCollected_ForAServerAddedInsideTheRetention()
    {
        _serverManager.AddServer(new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "NewSrv", IsEnabled = true, CreatedDate = DateTime.Now.AddDays(-10) });

        var list = await McpDiscoveryTools.ListServers(_serverManager, new LocalDataService(_duckDb));
        Assert.Contains("(last collection: No data collected)", list, StringComparison.Ordinal);
    }

    /// <summary>
    /// A newest collection that IS retained is still read and shown: the rule does not reach past a real row.
    /// Seeded into the hot table, where the card's read looks first.
    /// </summary>
    [Fact]
    public async Task ARetainedCollection_IsStillReadAndShown()
    {
        var collected = DateTime.UtcNow.AddDays(-20);
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES (96700401, 967004, 'dark-inside', 'wait_stats', $1, 'SUCCESS')";
            cmd.Parameters.Add(new DuckDBParameter { Value = collected });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var card = await new LocalDataService(_duckDb).GetServerSummaryAsync(967004, "dark-inside", DateTime.UtcNow.AddMonths(-6));
        Assert.Equal(ServerFreshness.Offline, card!.CollectionFreshness);
        Assert.NotNull(card.LastCollectionTime);
        Assert.EndsWith("(stopped)", card.LastCollectionDisplay, StringComparison.Ordinal);
        Assert.NotEqual("None retained (stopped)", card.LastCollectionDisplay);
    }
}
