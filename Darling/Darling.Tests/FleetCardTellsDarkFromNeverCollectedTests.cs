/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3935: the fleet card tells a server that went dark before its last-collection window apart from one that
/// has never collected. The window is 48 hours and stays (a probe with none plans every retained chunk of
/// <c>collection_log</c>); a server silent for longer used to reach the card as a null, and a null is what
/// <see cref="ServerHealthClassifier.ClassifyFreshness"/> calls never collected — "Awaiting first collection",
/// banded Warning — while the WPF viewer, reading the same timestamp with no window, called it Offline.
///
/// <para><b>What decides it.</b> The last-collection read reports every enabled registry server, with the
/// registry's <c>created_date</c> (the first successful connect) beside a newest collection that is null
/// when the window holds none. Registered before the window: Offline. Registered inside it: never collected,
/// exactly, because the worker collects only after connecting. These pins hold the rule, its boundary, the
/// card and the reason it produces, and the read's shape; <see cref="FleetReadsAreBoundedLivePostgresTests"/>
/// holds the seam against a store.</para>
/// </summary>
public sealed class FleetCardTellsDarkFromNeverCollectedTests
{
    private static readonly DateTime Now = new(2026, 9, 23, 12, 0, 0, DateTimeKind.Utc);

    private static DateTime WindowStart => DarlingFleetReader.LastCollectionWindowStart(Now);

    /// <summary>
    /// The windowed ladder is the shared one plus ONE arm. Every newest collection the window holds bands
    /// exactly as <see cref="ServerHealthClassifier.ClassifyFreshness"/> bands it, whatever the registration
    /// says; an empty window is Offline only on a registration older than the window, and keeps the shared
    /// reading (never collected) on one inside it or on none at all.
    /// </summary>
    [Fact]
    public void TheWindowedLadder_DepartsFromTheSharedOne_OnlyForAnEmptyWindowOnAnOlderRegistration()
    {
        var registrations = new DateTime?[]
        {
            null,
            Now.AddDays(-30),
            WindowStart.AddTicks(-1),
            WindowStart,
            Now.AddMinutes(-10),
        };
        var collections = new DateTime?[]
        {
            Now.AddSeconds(-30),                                                    // fresh
            Now - ServerHealthThresholds.StaleThreshold - TimeSpan.FromMinutes(1), // stale
            Now - ServerHealthThresholds.OfflineThreshold - TimeSpan.FromHours(3), // dark, inside the window
            WindowStart,                                                            // the window's first instant
        };

        foreach (var registeredAt in registrations)
        {
            foreach (var lastCollection in collections)
            {
                Assert.Equal(
                    ServerHealthClassifier.ClassifyFreshness(lastCollection, Now),
                    DarlingFleetReader.ClassifyWindowedFreshness(lastCollection, registeredAt, Now));
            }
        }

        Assert.Equal(ServerFreshness.Offline, DarlingFleetReader.ClassifyWindowedFreshness(null, Now.AddDays(-30), Now));
        Assert.Equal(ServerFreshness.NeverCollected, DarlingFleetReader.ClassifyWindowedFreshness(null, Now.AddMinutes(-10), Now));
        Assert.Equal(ServerFreshness.NeverCollected, DarlingFleetReader.ClassifyWindowedFreshness(null, null, Now));
    }

    /// <summary>
    /// The boundary is the instant the read's <c>$1</c> is bound to. A server registered AT the window's start
    /// could only have collected at or after it, which is inside the window the read searched (its predicate
    /// is <c>collection_time &gt;= $1</c>), so an empty window proves it never has. One tick earlier and a
    /// collection outside the window is possible, so it is dark.
    /// </summary>
    [Fact]
    public void TheBoundary_IsTheInstantTheReadIsBoundTo()
    {
        Assert.Equal(ServerFreshness.NeverCollected, DarlingFleetReader.ClassifyWindowedFreshness(null, WindowStart, Now));
        Assert.Equal(ServerFreshness.Offline, DarlingFleetReader.ClassifyWindowedFreshness(null, WindowStart.AddTicks(-1), Now));

        /* One derivation for the bind and the rule: two days, as naive UTC (the store's convention — a
           Kind=Utc value binds as timestamptz and PostgreSQL zone-shifts it against the naive column). */
        Assert.Equal(TimeSpan.FromHours(48), DarlingFleetReader.LastCollectionWindow);
        Assert.Equal(new DateTime(2026, 9, 21, 12, 0, 0), WindowStart);
        Assert.Equal(DateTimeKind.Unspecified, WindowStart.Kind);
    }

    /// <summary>
    /// A server the read did not report at all is handed the row's <c>default</c>, two nulls, and keeps the
    /// no-claim reading. That is the lesson of the rotating false-Offlines on actively-collecting servers
    /// that the reader's null fallback was written for: an ABSENCE never becomes a red Offline. Only the
    /// read's positive report of an empty window can.
    /// </summary>
    [Fact]
    public void AServerTheReadDidNotReport_KeepsTheNoClaimReading()
    {
        var missing = default(DarlingFleetReader.LastCollectionRow);

        Assert.Null(missing.LastCollection);
        Assert.Null(missing.RegisteredAt);
        Assert.Equal(
            ServerFreshness.NeverCollected,
            DarlingFleetReader.ClassifyWindowedFreshness(missing.LastCollection, missing.RegisteredAt, Now));

        var card = Card(missing.LastCollection, missing.RegisteredAt);
        Assert.NotEqual(FleetHealthBand.Offline, card.Band);
        Assert.NotEqual(false, card.IsOnline);
    }

    /// <summary>
    /// The issue's card: registered weeks ago, nothing in the window. It reads Offline on every field a
    /// consumer keys on — the band, the status word, <c>is_online</c>, the awaiting flag — and the
    /// "needs attention" reason says so, where it used to say "Awaiting first collection". The window's
    /// newest collection is null because there is none; <c>status</c> is what tells this null from the
    /// never-collected one.
    /// </summary>
    [Fact]
    public void ADarkServersCard_ReadsOffline_NotAwaitingItsFirstCollection()
    {
        var card = Card(null, Now.AddDays(-30));

        Assert.Equal(FleetHealthBand.Offline, card.Band);
        Assert.Equal(ServerCollectionStatus.Offline.Word(), card.Status);
        Assert.False(card.IsOnline);
        Assert.False(card.AwaitingFirstCollection);
        Assert.False(card.CollectionStale);
        Assert.Null(card.LastCollectionTime);
        Assert.Equal("Offline - no recent collection", DarlingFleetReader.BuildReason(card));

        /* And the roll-up counts it where the viewer's would: Offline, not Warning. */
        var rollup = DarlingFleetReader.BuildRollup(new[] { card }, Now, Now.AddHours(-1), Now);
        Assert.Equal(1, rollup.OfflineCount);
        Assert.Equal(0, rollup.WarningCount);
        Assert.Equal(FleetHealthBand.Offline, Assert.Single(rollup.WorstServers).Band);
    }

    /// <summary>
    /// The other half, and the reason the registry is consulted at all rather than calling every empty
    /// window Offline: a server the service has just connected to and not yet collected from is the bootstrap
    /// state, and a red Offline on it is what sent a 24-server field report chasing a phantom scheduler bug
    /// (<see cref="ServerFreshness.NeverCollected"/>).
    /// </summary>
    [Fact]
    public void ANewlyRegisteredServersCard_StillReadsAwaitingFirstCollection()
    {
        var card = Card(null, Now.AddMinutes(-10));

        Assert.Equal(FleetHealthBand.Warning, card.Band);
        Assert.Equal(ServerCollectionStatus.AwaitingFirstCollection.Word(), card.Status);
        Assert.Null(card.IsOnline);
        Assert.True(card.AwaitingFirstCollection);
        Assert.Equal(ServerCollectionStatus.AwaitingFirstCollection.Word(), DarlingFleetReader.BuildReason(card));
    }

    /// <summary>
    /// The new arm produces Offline and nothing else, so <c>collection_stale</c> keeps exactly the population
    /// its name claims (#3098): the Stale band, which only a collection inside the window can produce.
    /// </summary>
    [Fact]
    public void TheCollectionStaleFlag_KeepsItsPopulation()
    {
        foreach (var registeredAt in new DateTime?[] { null, Now.AddDays(-30), Now.AddMinutes(-10) })
        {
            foreach (var age in new TimeSpan?[] { null, TimeSpan.FromSeconds(5), ServerHealthThresholds.StaleThreshold + TimeSpan.FromMinutes(1), TimeSpan.FromHours(20) })
            {
                DateTime? lastCollection = age.HasValue ? Now - age.Value : null;
                Assert.Equal(
                    ServerCollectionStatusRules.FlagsFor(ServerHealthClassifier.ClassifyFreshness(lastCollection, Now)).CollectionStale,
                    Card(lastCollection, registeredAt).CollectionStale);
            }
        }
    }

    /// <summary>
    /// The read's shape: still the per-server probe #3895 made it, still windowed on the partition column,
    /// but a LEFT join, so a server whose window is empty is REPORTED with a null rather than dropped, and the
    /// registration rides the same row. It reads <c>collection_log</c> exactly once; the discriminator is a
    /// registry column, not a second, unwindowed look at the history (the price the window exists to avoid).
    /// </summary>
    [Fact]
    public void TheLastCollectionRead_ReportsEveryEnabledServer_WithItsRegistration()
    {
        var sql = DarlingFleetReader.FleetLastCollectionSql;

        Assert.Contains("FROM servers AS s", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN LATERAL", sql, StringComparison.Ordinal);
        Assert.Contains(") AS latest ON TRUE", sql, StringComparison.Ordinal);
        Assert.Contains("s.created_date", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $1", sql, StringComparison.Ordinal);
        Assert.Matches(@"ORDER BY collection_time DESC\s+LIMIT 1", sql);
        Assert.Single(Regex.Matches(sql, @"\bv_collection_log\b"));
        Assert.DoesNotContain("CROSS JOIN", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GROUP BY", sql, StringComparison.Ordinal);
        Assert.True(FleetReadsAreBoundedByTheFleetTests.IsBoundedByTheFleet(sql));

        /* The reader binds the window through the same derivation the rule uses, so the window the read
           searched and the window the card reasons about cannot drift apart: a bind narrower than the rule
           would call a server that collected between the two "never collected". */
        var reader = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingFleetReader.cs");
        Assert.Contains("AddTimestamp(command, LastCollectionWindowStart(now));", reader, StringComparison.Ordinal);
        Assert.DoesNotContain("AddHours(-48)", reader, StringComparison.Ordinal);
    }

    /// <summary>A calm SQL Server card whose only variables are the two freshness inputs, assembled by the
    /// reader's own <see cref="DarlingFleetReader.BuildCard"/>.</summary>
    private static FleetServerCard Card(DateTime? lastCollection, DateTime? registeredAt) =>
        DarlingFleetReader.BuildCard(
            new DarlingFleetReader.FleetServerRow(1, "sql-01", "sql-01", 3, MonitoredEngineKind.SqlServer, false),
            new DarlingFleetReader.CpuRow(20, 5),
            default,
            default,
            default,
            new DarlingFleetReader.ThreadsRow(512, 100, 0, 0),
            default,
            default,
            default,
            lastCollection,
            /* Forty collectors banded, none failing: the collectors row is a measured reading (#3539 A6). */
            new DarlingFleetReader.CollectorCounts(40, 0, 40, 0),
            null,
            Now,
            TimeSpan.FromHours(1),
            DeadlockRateThresholds.Default,
            registeredAt);
}
