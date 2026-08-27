/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the replication-slot read: latest state per slot joined to its earliest reading in the window,
/// and a severity classification that turns on whether retained WAL is still growing rather than on how
/// large it currently is.
/// </summary>
public class DarlingPgSlotReaderTests
{
    private static string Sql => DarlingPgSlotReader.PgSlotsSql;

    /// <summary>Slot state is a level, so the current reading is the latest one, not an aggregate.</summary>
    [Fact]
    public void TakesTheLatestStatePerSlot()
    {
        Assert.Contains("DISTINCT ON (slot_name)", Sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY slot_name, collection_time DESC", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The earliest reading is what makes retained WAL actionable — without it a steady 45 GB and a 45 GB
    /// that grew from 2 GB in an hour are indistinguishable, and only the second is an emergency.
    /// </summary>
    [Fact]
    public void CarriesTheEarliestRetainedFigureForGrowthComparison()
    {
        Assert.Contains("ORDER BY slot_name, collection_time ASC", Sql, StringComparison.Ordinal);
        Assert.Contains("first_retained_wal_bytes", Sql, StringComparison.Ordinal);
        Assert.Contains("first_seen_at", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// An inner join is safe only because both branches read the same window, so every slot in one is in
    /// the other. Pinned because narrowing one branch's predicate later would silently drop slots.
    /// </summary>
    [Fact]
    public void JoinsTheTwoBranchesOnSlotNameOverTheSameWindow()
    {
        Assert.Contains("JOIN earliest AS e ON e.slot_name = l.slot_name", Sql, StringComparison.Ordinal);
        Assert.Equal(2, Sql.Split("server_id = $1").Length - 1);
        Assert.Equal(2, Sql.Split("collection_time >= $2").Length - 1);
        Assert.Equal(2, Sql.Split("collection_time <= $3").Length - 1);
    }

    /// <summary>Biggest hole first, and both failure modes present in the projection.</summary>
    [Fact]
    public void OrdersByRetainedWalAndSelectsBothFailureModes()
    {
        Assert.Contains("ORDER BY l.retained_wal_bytes DESC", Sql, StringComparison.Ordinal);
        Assert.Contains("l.wal_status", Sql, StringComparison.Ordinal);
        Assert.Contains("l.catalog_xmin_age", Sql, StringComparison.Ordinal);
        Assert.Contains("l.inactive_since", Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadsTheReplicationSlotsTable()
    {
        /* The STORE table, which is deliberately NOT named pg_replication_slots: that name belongs to
           pg_catalog's view, which pg_catalog resolves first, so this read would have silently returned the
           monitoring store's own (empty) slot list. Schema-qualified as well, to say which PostgreSQL the
           query is aimed at. */
        Assert.Contains("FROM collect.pg_replication_slot_stats", Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM pg_replication_slots", Sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The two states where WAL is already gone or the slot is unusable are critical on their face — no
    /// growth evidence needed, because the damage is done.
    /// </summary>
    [Theory]
    [InlineData("lost", "critical_slot_lost")]
    [InlineData("unreserved", "critical_wal_already_removed")]
    public void TerminalWalStatesAreCriticalRegardlessOfActivityOrGrowth(string walStatus, string expected)
    {
        Assert.Equal(expected, DarlingMcpPgSlotTools.Classify(walStatus, isActive: true, retainedWalGrowing: false));
        Assert.Equal(expected, DarlingMcpPgSlotTools.Classify(walStatus, isActive: false, retainedWalGrowing: true));
    }

    /// <summary>
    /// The disk bomb is the conjunction, not any one part: WAL retained BECAUSE of this slot, nobody
    /// consuming it, and the pile still growing. Each part alone is a lesser finding.
    /// </summary>
    [Fact]
    public void TheOrphanFillingDiskRequiresAllThreeConditions()
    {
        Assert.Equal(
            "critical_orphan_filling_disk",
            DarlingMcpPgSlotTools.Classify("extended", isActive: false, retainedWalGrowing: true));

        /* Active consumer: it is behind, but someone is draining it. */
        Assert.Equal(
            "warning_retaining_wal",
            DarlingMcpPgSlotTools.Classify("extended", isActive: true, retainedWalGrowing: true));

        /* Inactive but flat: a consumer between polls, not a volume filling. */
        Assert.Equal(
            "warning_retaining_wal",
            DarlingMcpPgSlotTools.Classify("extended", isActive: false, retainedWalGrowing: false));
    }

    /// <summary>
    /// A healthy slot that has gone quiet and is accumulating still deserves a warning even while
    /// wal_status says reserved — reserved only means the WAL is inside the configured floor, and that
    /// floor is where "extended" starts, not where the risk does.
    /// </summary>
    [Fact]
    public void InactiveAndGrowingWarnsEvenWhileReserved()
    {
        Assert.Equal(
            "warning_inactive_and_growing",
            DarlingMcpPgSlotTools.Classify("reserved", isActive: false, retainedWalGrowing: true));

        Assert.Equal(
            "info_inactive",
            DarlingMcpPgSlotTools.Classify("reserved", isActive: false, retainedWalGrowing: false));
    }

    /// <summary>An active slot inside its reservation is the healthy default and must not cry wolf.</summary>
    [Fact]
    public void ActiveReservedSlotIsOk()
    {
        Assert.Equal("ok", DarlingMcpPgSlotTools.Classify("reserved", isActive: true, retainedWalGrowing: true));
        Assert.Equal("ok", DarlingMcpPgSlotTools.Classify("reserved", isActive: true, retainedWalGrowing: false));
    }

    /// <summary>
    /// wal_status is NULL on a slot with no restart_lsn yet, and on Aurora it can be absent entirely.
    /// That must fall through to the activity/growth branches rather than throwing or reading as healthy
    /// when the slot is inactive and growing.
    /// </summary>
    [Fact]
    public void NullWalStatusFallsThroughToActivityAndGrowth()
    {
        Assert.Equal("ok", DarlingMcpPgSlotTools.Classify(null, isActive: true, retainedWalGrowing: false));
        Assert.Equal("info_inactive", DarlingMcpPgSlotTools.Classify(null, isActive: false, retainedWalGrowing: false));
        Assert.Equal(
            "warning_inactive_and_growing",
            DarlingMcpPgSlotTools.Classify(null, isActive: false, retainedWalGrowing: true));
    }

    /// <summary>Every severity the classifier can emit is a distinct string, so callers can switch on it.</summary>
    [Fact]
    public void SeveritiesAreDistinct()
    {
        var severities = new[]
        {
            DarlingMcpPgSlotTools.Classify("lost", false, false),
            DarlingMcpPgSlotTools.Classify("unreserved", false, false),
            DarlingMcpPgSlotTools.Classify("extended", false, true),
            DarlingMcpPgSlotTools.Classify("extended", true, false),
            DarlingMcpPgSlotTools.Classify("reserved", false, true),
            DarlingMcpPgSlotTools.Classify("reserved", false, false),
            DarlingMcpPgSlotTools.Classify("reserved", true, false),
        };

        Assert.Equal(severities.Length, severities.Distinct().Count());
    }
}
