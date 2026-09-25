/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The shared Availability Group reader (#4228), ungated: the Postgres dialect and shape of every statement it
/// ships, the source pin that none of them regresses to the unbounded shape this issue replaced, and the pure
/// staleness split that decides the shared floor. This is now the ONE place the Viewer's AG tab, <c>/api/ag</c>
/// and <c>get_ag_health</c> get their SQL from — <see cref="DarlingAgReaderTests"/> and
/// <see cref="AgTopologyCardsTests"/> no longer carry their own copies of these pins.
/// </summary>
public sealed class DarlingAgStatesReaderTests
{
    private static readonly string[] StepTwoStatements =
    {
        DarlingAgStatesReader.ReplicaStatesSql,
        DarlingAgStatesReader.DatabaseReplicaStatesSql,
        DarlingAgStatesReader.ReplicaGroupCountSql,
    };

    private static readonly string[] StepOneStatements =
    {
        DarlingAgStatesReader.ReplicaNewestInstantSql,
        DarlingAgStatesReader.DatabaseNewestInstantSql,
    };

    /* ─────────────────────────── source pin ─────────────────────────── */

    /// <summary>The issue's own pin (#4228): no statement this reader ships may run
    /// <c>MAX(collection_time) ... GROUP BY server_id</c> with nothing bounding <c>collection_time</c> between
    /// the FROM and the GROUP BY — that shape is what read every retained row for every server (2,803 ms,
    /// 441 k buffers for 398 rows). Reflects over every public string constant on the type, so a future
    /// statement pasted in the old shape fails here instead of shipping the regression again.</summary>
    [Fact]
    public void NoStatement_RunsTheUnboundedNewestSnapshotAggregate()
    {
        var unbounded = new Regex(
            @"FROM\s+ag_(replica_states|database_replica_states)\s*\r?\n\s*GROUP BY server_id",
            RegexOptions.IgnoreCase);

        var constants = typeof(DarlingAgStatesReader)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string));

        var checkedAny = false;
        foreach (var field in constants)
        {
            var sql = (string)field.GetValue(null)!;
            checkedAny = true;
            Assert.False(
                unbounded.IsMatch(sql),
                $"{field.Name} runs MAX(collection_time)/GROUP BY server_id with no collection_time floor (#4228).");
        }

        Assert.True(checkedAny, "no public SQL constants found to check — the reflection scope is wrong.");
    }

    /* ─────────────────────────── step one: dialect + shape ─────────────────────────── */

    [Fact]
    public void StepOneSql_IsPostgresDialect_WithPositionalServerFilter()
    {
        foreach (var sql in StepOneStatements)
        {
            Assert.Contains("$1", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
            Assert.Equal(1, Occurrences(sql, "$1::integer IS NULL"));
        }
    }

    [Fact]
    public void StepOneSql_IsAnOrderedDescentPerServer_NotAnAggregate()
    {
        foreach (var sql in StepOneStatements)
        {
            Assert.Contains("ORDER BY", sql, StringComparison.Ordinal);
            Assert.Contains("DESC", sql, StringComparison.Ordinal);
            Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("MAX(collection_time)", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("GROUP BY", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>The ruling this issue adds on top of its own sketch: step one is driven from EVERY row of
    /// <c>servers</c>, not scoped to <c>is_enabled</c>. #4236 was a by-server read scoped to an enabled table
    /// that dropped a disabled server's own rows when looked up directly; step one plays that same
    /// per-server-lookup role for the floor/staleness split, so it stays unscoped and lets the OUTPUT-side
    /// join (unchanged, still present below) decide visibility, exactly as today.</summary>
    [Fact]
    public void StepOneSql_IsNotScopedToEnabledServers()
    {
        foreach (var sql in StepOneStatements)
        {
            Assert.DoesNotContain("is_enabled", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void StepOneSql_ReadsTheBareTables()
    {
        Assert.Contains("FROM ag_replica_states", DarlingAgStatesReader.ReplicaNewestInstantSql, StringComparison.Ordinal);
        Assert.Contains("FROM ag_database_replica_states", DarlingAgStatesReader.DatabaseNewestInstantSql, StringComparison.Ordinal);

        foreach (var sql in StepOneStatements.Concat(StepTwoStatements))
        {
            Assert.DoesNotContain("collect.", sql, StringComparison.Ordinal);
        }
    }

    /* ─────────────────────────── step two: dialect + shape ─────────────────────────── */

    [Fact]
    public void StepTwoSql_IsPostgresDialect_WithPositionalFloorAndServerFilter()
    {
        foreach (var sql in StepTwoStatements)
        {
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);

            /* The floor guards BOTH the inner latest-instant aggregate and the outer scan — count, don't just
               Contains, or deleting either WHERE leaves this green (#4228's whole point). */
            Assert.Equal(2, Occurrences(sql, "collection_time >= $1"));

            /* The optional server filter guards both arms too, exactly as the shipped Service statement did
               before the move (renumbered to $2 now that $1 is the floor). */
            Assert.Equal(2, Occurrences(sql, "$2::integer IS NULL"));
        }
    }

    [Fact]
    public void StepTwoSql_KeepsEveryRowAtEachServersNewestCollection()
    {
        /* DISTINCT ON would keep ONE row per server — wrong here, since a snapshot is many rows (one per
           replica / per database). The join against MAX(collection_time) keeps them all; unchanged by the
           floor bound added alongside it. */
        foreach (var sql in new[] { DarlingAgStatesReader.ReplicaStatesSql, DarlingAgStatesReader.DatabaseReplicaStatesSql })
        {
            Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
            Assert.Contains("GROUP BY server_id", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("DISTINCT ON", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void StepTwoSql_RestrictsToTheEnabledServerRegistry()
    {
        /* Unchanged from the shipped statements: a server disabled in the control plane leaves the fleet
           surfaces at once instead of showing stale AG cards until its rows age out. */
        foreach (var sql in StepTwoStatements)
        {
            Assert.Contains("JOIN servers AS s", sql, StringComparison.Ordinal);
            Assert.Contains("s.is_enabled", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void StepTwoSql_SelectsIsLocalOnBothGrains()
    {
        Assert.Contains("r.is_local", DarlingAgStatesReader.ReplicaStatesSql, StringComparison.Ordinal);
        Assert.Contains("d.is_local", DarlingAgStatesReader.DatabaseReplicaStatesSql, StringComparison.Ordinal);
    }

    [Fact]
    public void StepTwoSql_SelectsEveryColumnTheDtosSurface()
    {
        foreach (var column in new[]
        {
            "role_desc", "operational_state_desc", "connected_state_desc", "recovery_health_desc",
            "synchronization_health_desc", "availability_mode_desc", "failover_mode_desc", "endpoint_url",
        })
        {
            Assert.Contains(column, DarlingAgStatesReader.ReplicaStatesSql, StringComparison.Ordinal);
        }

        foreach (var column in new[]
        {
            "synchronization_state_desc", "last_hardened_lsn", "last_commit_lsn", "log_send_queue_size",
            "redo_queue_size", "log_send_rate", "redo_rate", "is_suspended", "suspend_reason_desc",
            "secondary_lag_seconds", "is_local",
        })
        {
            Assert.Contains(column, DarlingAgStatesReader.DatabaseReplicaStatesSql, StringComparison.Ordinal);
        }
    }

    private static int Occurrences(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    /* ─────────────────────────── the staleness split (pure) ─────────────────────────── */

    [Fact]
    public void SplitCurrentAndStale_EmptyInput_YieldsNoFloorAndNoStale()
    {
        var (floor, stale) = DarlingAgStatesReader.SplitCurrentAndStale(
            Array.Empty<(int ServerId, DateTime Instant)>(), DateTime.UtcNow);

        Assert.Null(floor);
        Assert.Empty(stale);
    }

    [Fact]
    public void SplitCurrentAndStale_EveryServerCurrent_FloorIsTheMinimum_NoneStale()
    {
        var now = new DateTime(2026, 9, 25, 12, 0, 0, DateTimeKind.Utc);
        var instants = new List<(int ServerId, DateTime Instant)>
        {
            (1, now.AddMinutes(-5)),
            (2, now.AddMinutes(-30)),
            (3, now),
        };

        var (floor, stale) = DarlingAgStatesReader.SplitCurrentAndStale(instants, now);

        Assert.Equal(now.AddMinutes(-30), floor);
        Assert.Empty(stale);
    }

    /// <summary>The scenario #4228 exists to fix: one server's AG rows stopped a day ago. Its instant must not
    /// become the shared floor — it is read on its own instead, and the shared floor tracks only the servers
    /// still collecting.</summary>
    [Fact]
    public void SplitCurrentAndStale_OneServerStoppedADayAgo_IsCarvedOutAndDoesNotDragTheFloor()
    {
        var now = new DateTime(2026, 9, 25, 12, 0, 0, DateTimeKind.Utc);
        var instants = new List<(int ServerId, DateTime Instant)>
        {
            (1, now.AddMinutes(-5)),
            (2, now.AddDays(-1).AddMinutes(-1)),
        };

        var (floor, stale) = DarlingAgStatesReader.SplitCurrentAndStale(instants, now);

        Assert.Equal(now.AddMinutes(-5), floor);
        var staleServer = Assert.Single(stale);
        Assert.Equal(2, staleServer.ServerId);
        Assert.Equal(now.AddDays(-1).AddMinutes(-1), staleServer.Instant);
    }

    [Fact]
    public void SplitCurrentAndStale_EveryServerStale_NoFloor_EveryServerReadOnItsOwn()
    {
        var now = new DateTime(2026, 9, 25, 12, 0, 0, DateTimeKind.Utc);
        var instants = new List<(int ServerId, DateTime Instant)>
        {
            (1, now.AddDays(-3)),
            (2, now.AddDays(-10)),
        };

        var (floor, stale) = DarlingAgStatesReader.SplitCurrentAndStale(instants, now);

        Assert.Null(floor);
        Assert.Equal(2, stale.Count);
    }

    /// <summary>The horizon boundary: exactly at <see cref="DarlingAgStatesReader.StalenessHorizon"/> is stale
    /// (the split uses a strict less-than on the horizon START), one tick inside it is current. Pins the
    /// edge rather than leaving it to whichever side floating-point-free <c>DateTime</c> math happens to fall.</summary>
    [Fact]
    public void SplitCurrentAndStale_HorizonBoundary_IsExclusive()
    {
        var now = new DateTime(2026, 9, 25, 12, 0, 0, DateTimeKind.Utc);
        var horizonStart = now - DarlingAgStatesReader.StalenessHorizon;

        var atBoundary = DarlingAgStatesReader.SplitCurrentAndStale(
            new List<(int ServerId, DateTime Instant)> { (1, horizonStart) }, now);
        Assert.Equal(horizonStart, atBoundary.Floor);
        Assert.Empty(atBoundary.Stale);

        var justStale = DarlingAgStatesReader.SplitCurrentAndStale(
            new List<(int ServerId, DateTime Instant)> { (1, horizonStart.AddTicks(-1)) }, now);
        Assert.Null(justStale.Floor);
        Assert.Single(justStale.Stale);
    }
}
