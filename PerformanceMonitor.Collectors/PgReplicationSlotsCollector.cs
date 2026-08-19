/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Replication slot state — an abandoned slot is one of the few PostgreSQL conditions that can take a
/// server down by itself, and it can do so two independent ways.
/// <para><b>Disk exhaustion.</b> A slot retains every WAL segment its consumer has not confirmed. With
/// <c>max_slot_wal_keep_size</c> at its default of <c>-1</c> that retention is <i>unbounded</i>: an
/// inactive slot will hold WAL until the volume fills, and a full WAL volume stops the server. This is
/// the failure mode <see cref="PgXminHorizonCollector"/> cannot see, which is why slots get their own
/// collector even though that one already reads them for the horizon.</para>
/// <para><b>Vacuum starvation.</b> The same slot pins <c>xmin</c>/<c>catalog_xmin</c>, so nothing can be
/// reclaimed cluster-wide. Both counters are recorded here as well, so slot state is legible on its own
/// without a join.</para>
/// <para>The orphans are rarely deliberate: a removed CDC task, a finished blue/green deployment (which
/// creates one slot per database), a Debezium consumer that was decommissioned, or a failed major-version
/// upgrade. Nothing complains — the slot simply keeps its promise to retain WAL for a consumer that is
/// never coming back.</para>
/// <para>Core catalog only, so it runs on any PostgreSQL target.</para>
/// </summary>
public sealed class PgReplicationSlotsCollector : PostgresCollectorDefinitionBase<PgReplicationSlotsCollector.Row>
{
    public static PgReplicationSlotsCollector Instance { get; } = new();

    private PgReplicationSlotsCollector()
    {
    }

    public readonly record struct Row(
        string SlotName,
        string? SlotType,
        string? Plugin,
        string? DatabaseName,
        bool IsActive,
        long ActivePid,
        bool IsTemporary,
        bool TwoPhase,
        string? WalStatus,
        long SafeWalSizeBytes,
        long RetainedWalBytes,
        long XminAge,
        long CatalogXminAge,
        DateTime? InactiveSince,
        string? InvalidationReason,
        bool Conflicting);

    /* Version-gated because the most useful diagnostics are recent additions:
         PG16+ : conflicting
         PG17+ : inactive_since, invalidation_reason

       inactive_since in particular is what turns "this slot is inactive" into "this slot has been
       inactive for three weeks" — the difference between a consumer between polls and an orphan. On 16
       the collector substitutes NULL rather than omitting the column, so the table shape stays constant
       across the fleet and a chart does not change shape at an upgrade.

       Retained WAL is computed rather than read, because the column that would answer it directly
       (safe_wal_size) is NULL whenever max_slot_wal_keep_size is -1, which is the DEFAULT. Relying on it
       would mean reporting nothing on a stock server, precisely where the risk is unbounded.

       The LSN reference must switch on recovery state: pg_current_wal_lsn() ERRORS on a standby, so a
       reader target would fail the whole collection. pg_last_wal_receive_lsn() is the standby's
       equivalent. Aurora readers are legitimate targets here, so this is not hypothetical. */
    private static string BuildQueryText(int postgresMajorVersion)
    {
        var conflicting = postgresMajorVersion >= 16 ? "s.conflicting" : "false";
        /* AT TIME ZONE 'UTC', not a bare select and not ::timestamp. inactive_since is
           `timestamp with time zone`, and two things go wrong if that is not converted HERE:

             * Npgsql maps a timestamptz read to DateTime with Kind=Utc, and refuses to write a Kind=Utc
               DateTime into the store's `timestamp without time zone` column — so a bare select fails at
               COPY time on any PG17 target with a slot that has ever been inactive.
             * `::timestamp` would convert, but it renders the instant in the SESSION's TimeZone before
               dropping the offset. The fleet's parameter groups all say UTC today, so it would agree
               today and silently shift the moment one of them did not.

           AT TIME ZONE 'UTC' is the only form that is both correctly typed and timezone-independent. */
        var inactiveSince = postgresMajorVersion >= 17
            ? "(s.inactive_since AT TIME ZONE 'UTC')"
            : "NULL::timestamp";
        var invalidationReason = postgresMajorVersion >= 17 ? "s.invalidation_reason" : "NULL::text";

        return $@"
SELECT
    s.slot_name                                                  AS slot_name,
    s.slot_type                                                  AS slot_type,
    s.plugin                                                     AS plugin,
    s.database                                                   AS database_name,
    s.active                                                     AS is_active,
    coalesce(s.active_pid, 0)::bigint                            AS active_pid,
    s.temporary                                                  AS is_temporary,
    s.two_phase                                                  AS two_phase,
    s.wal_status                                                 AS wal_status,
    coalesce(s.safe_wal_size, -1)::bigint                        AS safe_wal_size_bytes,
    coalesce(
        (CASE
             WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn()
             ELSE pg_current_wal_lsn()
         END - s.restart_lsn)::bigint, -1)                       AS retained_wal_bytes,
    coalesce(age(s.xmin)::bigint, -1)                            AS xmin_age,
    coalesce(age(s.catalog_xmin)::bigint, -1)                    AS catalog_xmin_age,
    {inactiveSince}                                              AS inactive_since,
    {invalidationReason}                                         AS invalidation_reason,
    coalesce({conflicting}, false)                               AS conflicting
FROM pg_replication_slots AS s
ORDER BY s.slot_name";
    }

    public override string Name => "pg_replication_slots";

    /// <summary>
    /// NOT <c>pg_replication_slots</c> — that name is taken by <c>pg_catalog.pg_replication_slots</c>, the
    /// system view this collector READS, and a store table cannot share it.
    /// <para><c>pg_catalog</c> is searched implicitly and FIRST, ahead of every entry in <c>search_path</c>,
    /// so an unqualified reference to that name resolves to the system view no matter what the store
    /// contains. It fails loudly in one place — <c>CREATE INDEX</c> on a view is 42809, which aborted the
    /// whole migration and left the store unusable — and SILENTLY everywhere else: a reader's
    /// <c>FROM pg_replication_slots</c> would have returned the MONITORING STORE's own (empty) slot list
    /// instead of collected history, so the tool would always report no slots and the retention alert would
    /// never fire. A muted outage predictor is worse than none, which is why the name changes rather than
    /// every reference being schema-qualified and hoped over.</para>
    /// <para>Name and TargetTable differing is established practice here (query_store → query_store_stats,
    /// cpu_utilization → cpu_utilization_stats, and two more), so the collector keeps naming its source.</para>
    /// </summary>
    public override string TargetTable => "pg_replication_slot_stats";

    /// <summary>Core catalog only — any PostgreSQL target.</summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context)
        => new(BuildQueryText(context.Target.PostgresMajorVersion));

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("slot_name", CollectorColumnType.Varchar),
        new CollectorColumn("slot_type", CollectorColumnType.Varchar),
        new CollectorColumn("plugin", CollectorColumnType.Varchar),
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("is_active", CollectorColumnType.Boolean),
        new CollectorColumn("active_pid", CollectorColumnType.BigInt),
        new CollectorColumn("is_temporary", CollectorColumnType.Boolean),
        new CollectorColumn("two_phase", CollectorColumnType.Boolean),
        /* The single most diagnostic column: reserved = healthy, extended = WAL is being retained
           BECAUSE of this slot (the disk-fill warning), unreserved = required WAL is already gone,
           lost = the slot is unusable. */
        new CollectorColumn("wal_status", CollectorColumnType.Varchar),
        /* -1 means "not applicable", which on a stock server is the norm: this column is NULL whenever
           max_slot_wal_keep_size is -1. Stored as a sentinel rather than NULL so a consumer cannot
           mistake "no limit configured" for "no data collected". */
        new CollectorColumn("safe_wal_size_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("retained_wal_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("xmin_age", CollectorColumnType.BigInt),
        new CollectorColumn("catalog_xmin_age", CollectorColumnType.BigInt),
        new CollectorColumn("inactive_since", CollectorColumnType.Timestamp),
        new CollectorColumn("invalidation_reason", CollectorColumnType.Varchar),
        new CollectorColumn("conflicting", CollectorColumnType.Boolean),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                SlotName: reader.GetString(0),
                SlotType: reader.IsDBNull(1) ? null : reader.GetString(1),
                Plugin: reader.IsDBNull(2) ? null : reader.GetString(2),
                DatabaseName: reader.IsDBNull(3) ? null : reader.GetString(3),
                IsActive: !reader.IsDBNull(4) && reader.GetBoolean(4),
                ActivePid: reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                IsTemporary: !reader.IsDBNull(6) && reader.GetBoolean(6),
                TwoPhase: !reader.IsDBNull(7) && reader.GetBoolean(7),
                WalStatus: reader.IsDBNull(8) ? null : reader.GetString(8),
                SafeWalSizeBytes: reader.IsDBNull(9) ? -1 : reader.GetInt64(9),
                RetainedWalBytes: reader.IsDBNull(10) ? -1 : reader.GetInt64(10),
                XminAge: reader.IsDBNull(11) ? -1 : reader.GetInt64(11),
                CatalogXminAge: reader.IsDBNull(12) ? -1 : reader.GetInt64(12),
                InactiveSince: reader.IsDBNull(13) ? null : reader.GetDateTime(13),
                InvalidationReason: reader.IsDBNull(14) ? null : reader.GetString(14),
                Conflicting: !reader.IsDBNull(15) && reader.GetBoolean(15)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. Retained WAL is a level — it is the size of a hole, not work done — and the useful
           reading is how big it is now plus whether it is still growing, which a trend of this column
           shows directly. */
        writer
            .Value(row.SlotName)
            .Value(row.SlotType)
            .Value(row.Plugin)
            .Value(row.DatabaseName)
            .Value(row.IsActive)
            .Value(row.ActivePid)
            .Value(row.IsTemporary)
            .Value(row.TwoPhase)
            .Value(row.WalStatus)
            .Value(row.SafeWalSizeBytes)
            .Value(row.RetainedWalBytes)
            .Value(row.XminAge)
            .Value(row.CatalogXminAge)
            .Value(row.InactiveSince)
            .Value(row.InvalidationReason)
            .Value(row.Conflicting);
    }
}
