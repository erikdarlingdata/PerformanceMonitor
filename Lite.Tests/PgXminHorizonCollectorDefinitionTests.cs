/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the xmin-horizon collector, whose entire value is attribution: all four causes look identical
/// from the symptom side, so the collector must name which one is holding the horizon.
/// </summary>
public class PgXminHorizonCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext()
        => new()
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 11, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = true },
            ExcludedDatabases = Array.Empty<string>(),
        };

    [Fact]
    public void Identity_Pinned()
    {
        Assert.Equal("pg_xmin_horizon", PgXminHorizonCollector.Instance.Name);
        Assert.Equal("pg_xmin_horizon", PgXminHorizonCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgXminHorizonCollector.Instance.TargetEngine);
    }

    /// <summary>Core catalog only, so it must run on non-Aurora PostgreSQL too.</summary>
    [Fact]
    public void AppliesToAnyPostgresTarget()
    {
        Assert.True(PgXminHorizonCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = false }));
        Assert.False(CollectorCatalog.AppliesTo(
            PgXminHorizonCollector.Instance, new CollectorTargetInfo()));
    }

    /// <summary>
    /// All five holder sources must be queried. Missing one does not degrade the answer, it INVERTS it:
    /// the collector would report a different cause as the winner and send someone to fix the wrong thing.
    /// </summary>
    [Fact]
    public void QueriesEveryHolderSource()
    {
        var sql = PgXminHorizonCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("pg_stat_activity", sql, StringComparison.Ordinal);
        Assert.Contains("pg_replication_slots", sql, StringComparison.Ordinal);
        Assert.Contains("pg_stat_replication", sql, StringComparison.Ordinal);
        Assert.Contains("pg_prepared_xacts", sql, StringComparison.Ordinal);

        Assert.Contains("backend_xmin", sql, StringComparison.Ordinal);
        /* A slot's xmin blocks row cleanup; its catalog_xmin blocks CATALOG cleanup, which is what a
           logical decoding slot pins. They can differ by a lot, so both are separate sources. */
        Assert.Contains("s.xmin", sql, StringComparison.Ordinal);
        Assert.Contains("s.catalog_xmin", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Bounded to the oldest holder per source. pg_stat_activity can carry hundreds of backends with an
    /// xmin, and storing all of them every minute would be a great many rows saying one thing.
    /// </summary>
    [Fact]
    public void ReducesToTheOldestHolderPerSource()
    {
        var sql = PgXminHorizonCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("DISTINCT ON (source)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY source, xmin_age DESC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PayloadColumns_OrderAndTypes_Pinned()
    {
        var expected = new (string Name, CollectorColumnType Type)[]
        {
            ("source", CollectorColumnType.Varchar),
            ("xmin_age", CollectorColumnType.BigInt),
            ("holder", CollectorColumnType.Varchar),
            ("detail", CollectorColumnType.Varchar),
            ("is_winner", CollectorColumnType.Boolean),
        };

        var actual = PgXminHorizonCollector.Instance.PayloadColumns;
        Assert.Equal(expected.Length, actual.Count);
        for (var i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Name, actual[i].Name);
            Assert.Equal(expected[i].Type, actual[i].Type);
        }
    }

    /// <summary>The oldest holder across all sources is the one actually setting the horizon.</summary>
    [Fact]
    public async Task StampsTheOldestHolderAcrossSourcesAsTheWinner()
    {
        var reader = new FakeCollectorDataReader(
            new object[] { "session", 5_000L, "12345", "state=idle in transaction" },
            new object[] { "replication_slot", 900_000L, "debezium_slot", "active=false" },
            new object[] { "prepared_transaction", 40_000L, "gid-1", "owner=app" });

        var rows = await PgXminHorizonCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        Assert.Equal(3, rows.Count);
        var winner = Assert.Single(rows, r => r.IsWinner);
        Assert.Equal("replication_slot", winner.Source);
        Assert.Equal(900_000L, winner.XminAge);
    }

    /// <summary>Exactly one winner even when two sources tie — a stored row must not be ambiguous.</summary>
    [Fact]
    public async Task StampsExactlyOneWinnerOnATie()
    {
        var reader = new FakeCollectorDataReader(
            new object[] { "session", 700L, "1", "a" },
            new object[] { "standby_feedback", 700L, "replica-1", "state=streaming" });

        var rows = await PgXminHorizonCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        Assert.Single(rows, r => r.IsWinner);
    }

    /// <summary>
    /// No holders is the HEALTHY state. Returning zero rows must not be mistaken for a failed
    /// collection, which is why nothing here throws or synthesizes a placeholder row.
    /// </summary>
    [Fact]
    public async Task NoHoldersYieldsNoRowsRatherThanAPlaceholder()
    {
        var rows = await PgXminHorizonCollector.Instance.ReadAsync(
            new FakeCollectorDataReader(), MakeContext(), CancellationToken.None);

        Assert.Empty(rows);
    }

    /// <summary>An xmin age is a level, like freeze age — no deltas.</summary>
    [Fact]
    public void TakesNoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = new CollectorContext
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 11, 12, 0, 0, DateTimeKind.Utc),
            Deltas = deltas,
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
            ExcludedDatabases = Array.Empty<string>(),
        };

        PgXminHorizonCollector.Instance.WritePayload(
            new PgXminHorizonCollector.Row("session", 1, "1", "d", true),
            new RecordingCollectorRowWriter(),
            context);

        Assert.Empty(deltas.Calls);
    }

    [Fact]
    public void RegisteredInBothTheCatalogAndTheSchedule()
    {
        Assert.Contains(CollectorCatalog.All, d => d.Name == "pg_xmin_horizon");

        var schedule = CollectorScheduleDefaults.All["pg_xmin_horizon"];
        Assert.Equal(1, schedule.FrequencyMinutes);
        Assert.Equal(30, schedule.RetentionDays);
        Assert.True(schedule.DefaultEnabled);
    }

    /// <summary>
    /// The round-2 fixes, pinned the way the three sibling collectors already pin theirs — this was the
    /// one fixed collector left unpinned. Timestamptz renders go through AT TIME ZONE 'UTC', never a
    /// bare ::text (which renders in the SESSION's TimeZone and agrees with UTC only while every server's
    /// timezone GUC says UTC — exactly what keeps the bug invisible on this fleet); the horizon takes the
    /// GREATEST of both counters so an idle-in-transaction backend (backend_xid, no snapshot) is visible;
    /// and the monitor's own session can never be the reported holder.
    /// </summary>
    [Fact]
    public void HorizonQuery_RendersUtc_SeesXidOnlyBackends_AndExcludesItself()
    {
        var sql = PgXminHorizonCollector.Instance.BuildQuery(MakeContext()).Text;

        /* The three timestamptz renders. */
        Assert.Contains("(a.xact_start AT TIME ZONE 'UTC')::text", sql, StringComparison.Ordinal);
        Assert.Contains("(a.query_start AT TIME ZONE 'UTC')::text", sql, StringComparison.Ordinal);
        Assert.Contains("(p.prepared AT TIME ZONE 'UTC')::text", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("a.xact_start::text", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("a.query_start::text", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("p.prepared::text", sql, StringComparison.Ordinal);

        /* Idle-in-transaction visibility: either counter qualifies a backend, and the age is the worse
           of the two — an xid-only session pins the horizon exactly as hard as a snapshot-holder. */
        Assert.Contains("(a.backend_xmin IS NOT NULL OR a.backend_xid IS NOT NULL)", sql, StringComparison.Ordinal);
        Assert.Contains("coalesce(age(a.backend_xid), 0)", sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(", sql, StringComparison.Ordinal);

        /* Self-attribution: the collector's own connection holds a snapshot while it reads. */
        Assert.Contains("a.pid <> pg_backend_pid()", sql, StringComparison.Ordinal);
    }
}
