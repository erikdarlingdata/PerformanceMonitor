/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the replication-slot collector: the recovery-safe LSN reference, the computed retained-WAL
/// measure that does not depend on a column which is NULL by default, and the version gating that keeps
/// the table shape constant across a mixed-version fleet.
/// </summary>
public class PgReplicationSlotsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(int major = 17)
        => new()
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 11, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = major,
            },
            ExcludedDatabases = Array.Empty<string>(),
        };

    [Fact]
    public void Identity_Pinned()
    {
        Assert.Equal("pg_replication_slots", PgReplicationSlotsCollector.Instance.Name);
        /* NOT pg_replication_slots: that is pg_catalog's view, which resolves ahead of anything in
           search_path, so a store table of that name breaks CREATE INDEX loudly and every unqualified read
           silently. Name and TargetTable differing is established practice (query_store -> query_store_stats). */
        Assert.Equal("pg_replication_slot_stats", PgReplicationSlotsCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgReplicationSlotsCollector.Instance.TargetEngine);
    }

    [Fact]
    public void AppliesToAnyPostgresTargetButNeverSqlServer()
    {
        Assert.True(PgReplicationSlotsCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = false }));
        Assert.False(CollectorCatalog.AppliesTo(
            PgReplicationSlotsCollector.Instance, new CollectorTargetInfo()));
    }

    /// <summary>
    /// pg_current_wal_lsn() ERRORS on a standby, and Aurora readers are legitimate targets, so the LSN
    /// reference has to switch on recovery state or the whole collection fails on a reader.
    /// </summary>
    [Fact]
    public void UsesARecoverySafeLsnReference()
    {
        var sql = PgReplicationSlotsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("pg_is_in_recovery()", sql, StringComparison.Ordinal);
        Assert.Contains("pg_last_wal_receive_lsn()", sql, StringComparison.Ordinal);
        Assert.Contains("pg_current_wal_lsn()", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Retained WAL is computed from restart_lsn rather than read from safe_wal_size, which is NULL
    /// whenever max_slot_wal_keep_size is -1 — the default. Depending on that column would mean
    /// reporting nothing precisely where retention is unbounded.
    /// </summary>
    [Fact]
    public void ComputesRetainedWalRatherThanTrustingSafeWalSize()
    {
        var sql = PgReplicationSlotsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("s.restart_lsn", sql, StringComparison.Ordinal);
        Assert.Contains("retained_wal_bytes", sql, StringComparison.Ordinal);
    }

    /// <summary>wal_status is the single most diagnostic column and must always be selected.</summary>
    [Fact]
    public void SelectsWalStatusAndBothXminCounters()
    {
        var sql = PgReplicationSlotsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("s.wal_status", sql, StringComparison.Ordinal);
        Assert.Contains("age(s.xmin)", sql, StringComparison.Ordinal);
        Assert.Contains("age(s.catalog_xmin)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// PG17 columns are read on 17 and substituted on 16, so the payload shape is identical either way.
    /// A version-conditional COLUMN COUNT would change the table shape mid-fleet.
    /// </summary>
    [Fact]
    public void GatesTheNewerColumnsByVersionWithoutChangingShape()
    {
        var pg17 = PgReplicationSlotsCollector.Instance.BuildQuery(MakeContext(17)).Text;
        var pg16 = PgReplicationSlotsCollector.Instance.BuildQuery(MakeContext(16)).Text;

        Assert.Contains("s.inactive_since", pg17, StringComparison.Ordinal);
        Assert.Contains("s.invalidation_reason", pg17, StringComparison.Ordinal);
        Assert.Contains("s.conflicting", pg17, StringComparison.Ordinal);

        Assert.DoesNotContain("s.inactive_since", pg16, StringComparison.Ordinal);
        Assert.DoesNotContain("s.invalidation_reason", pg16, StringComparison.Ordinal);
        Assert.Contains("NULL::timestamp", pg16, StringComparison.Ordinal);
        Assert.Contains("NULL::text", pg16, StringComparison.Ordinal);

        /* conflicting is 16+, so 16 reads it and only an older major would substitute. */
        Assert.Contains("s.conflicting", pg16, StringComparison.Ordinal);

        /* Same number of selected expressions on both, so the row shape cannot drift. */
        Assert.Equal(
            pg17.Split(" AS ").Length,
            pg16.Split(" AS ").Length);
    }

    /// <summary>
    /// inactive_since is `timestamp with time zone`, and selecting it bare is a runtime failure, not a
    /// style issue: Npgsql maps a timestamptz read to DateTime with Kind=Utc and refuses to write that
    /// into the store's `timestamp without time zone` column, so collection would break on any PG17
    /// target with a slot that has ever gone inactive — the exact servers this collector exists for.
    /// `::timestamp` would be correctly typed but timezone-dependent, so neither shortcut is acceptable.
    /// </summary>
    [Fact]
    public void ConvertsInactiveSinceWithAnExplicitUtcZone()
    {
        var sql = PgReplicationSlotsCollector.Instance.BuildQuery(MakeContext(17)).Text;

        Assert.Contains("(s.inactive_since AT TIME ZONE 'UTC')", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("s.inactive_since::timestamp", sql, StringComparison.Ordinal);

        /* The substituted branch was always correctly typed; both branches must stay `timestamp`. */
        var pg16 = PgReplicationSlotsCollector.Instance.BuildQuery(MakeContext(16)).Text;
        Assert.Contains("NULL::timestamp", pg16, StringComparison.Ordinal);
    }

    [Fact]
    public void PayloadColumns_CountAndKeyTypes_Pinned()
    {
        var columns = PgReplicationSlotsCollector.Instance.PayloadColumns;

        Assert.Equal(16, columns.Count);
        Assert.Equal("slot_name", columns[0].Name);
        Assert.Equal("wal_status", columns[8].Name);
        Assert.Equal("retained_wal_bytes", columns[10].Name);
        Assert.Equal(CollectorColumnType.Timestamp, columns[13].Type);   // inactive_since
        Assert.Equal(CollectorColumnType.Boolean, columns[15].Type);     // conflicting
    }

    /// <summary>
    /// No slots is normal on most servers and must yield no rows rather than an error or a placeholder.
    /// </summary>
    [Fact]
    public async Task NoSlotsYieldsNoRows()
    {
        var rows = await PgReplicationSlotsCollector.Instance.ReadAsync(
            new FakeCollectorDataReader(), MakeContext(), CancellationToken.None);

        Assert.Empty(rows);
    }

    /// <summary>
    /// The orphan shape end to end: inactive, WAL being retained because of it, and an xmin pinned — one
    /// slot causing both the disk and the vacuum failure mode at once.
    /// </summary>
    [Fact]
    public async Task ReadsAnAbandonedSlotIncludingBothFailureModes()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                "debezium_orphan", "logical", "pgoutput", "appdb", false, 0L, false, false,
                "extended", -1L, 48_318_382_080L, 900_000L, 900_000L,
                new DateTime(2026, 7, 20, 3, 0, 0, DateTimeKind.Unspecified), DBNull.Value, false,
            });

        var rows = await PgReplicationSlotsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var slot = Assert.Single(rows);
        Assert.False(slot.IsActive);
        Assert.Equal("extended", slot.WalStatus);          // WAL retained because of this slot
        Assert.Equal(48_318_382_080L, slot.RetainedWalBytes);
        Assert.Equal(-1, slot.SafeWalSizeBytes);           // not-applicable sentinel, the stock default
        Assert.Equal(900_000L, slot.CatalogXminAge);       // and vacuum is pinned too
        Assert.NotNull(slot.InactiveSince);
    }

    /// <summary>Retained WAL is the size of a hole, not work done — a level, so no deltas.</summary>
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
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17 },
            ExcludedDatabases = Array.Empty<string>(),
        };

        PgReplicationSlotsCollector.Instance.WritePayload(
            new PgReplicationSlotsCollector.Row(
                "s", "physical", null, null, true, 1, false, false, "reserved", -1, 0, -1, -1, null, null, false),
            new RecordingCollectorRowWriter(),
            context);

        Assert.Empty(deltas.Calls);
    }

    [Fact]
    public void RegisteredInBothTheCatalogAndTheSchedule()
    {
        Assert.Contains(CollectorCatalog.All, d => d.Name == "pg_replication_slots");

        var schedule = CollectorScheduleDefaults.All["pg_replication_slots"];
        Assert.Equal(1, schedule.FrequencyMinutes);
        Assert.Equal(90, schedule.RetentionDays);
        Assert.True(schedule.DefaultEnabled);
    }
}
