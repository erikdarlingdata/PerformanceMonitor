/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using System;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Transaction ID and MultiXact ID freeze headroom per database — the single most consequential thing
/// to monitor on a PostgreSQL server, and something SQL Server has no counterpart for at all.
/// <para>PostgreSQL's transaction ids are 32-bit and wrap. Autovacuum normally keeps the oldest
/// unfrozen id far from the wrap point, but when it cannot the consequences escalate on a documented
/// ladder: an anti-wraparound autovacuum is forced at <c>autovacuum_freeze_max_age</c> (200M by
/// default), a failsafe mode that abandons cost limits and index cleanup engages at
/// <c>vacuum_failsafe_age</c> (1.6B), warnings begin around 40M remaining ids, and at 3M remaining the
/// server <b>refuses to assign new transaction ids</b>: writes and DDL stop, reads continue. That last
/// state is a full write outage that no failover fixes, because every replica shares the condition.</para>
/// <para><b>MultiXact ids are a second, independent counter that is separately fatal</b> and almost
/// universally unmonitored. They are consumed when a row is locked by multiple transactions at once, so
/// a workload heavy on <c>SELECT FOR UPDATE</c>/<c>FOR SHARE</c> or on foreign-key checks burns them far
/// faster than plain transaction ids — a server can be comfortable on XID age and in trouble on
/// MultiXact age. Both live on <c>pg_database</c>, so both are collected here rather than pretending
/// one implies the other.</para>
/// <para>Not Aurora-gated: this reads only core catalog surfaces, so it works on any PostgreSQL target.
/// It is the first collector here that does. Aurora does add one escalation worth knowing about, though
/// it is observed rather than collected — Aurora restarts its read replicas as a cluster approaches
/// wraparound, so on Aurora read availability degrades <i>before</i> the writer goes read-only.</para>
/// </summary>
public sealed class PgWraparoundStatsCollector : PostgresCollectorDefinitionBase<PgWraparoundStatsCollector.Row>
{
    public static PgWraparoundStatsCollector Instance { get; } = new();

    private PgWraparoundStatsCollector()
    {
    }

    public readonly record struct Row(
        string DatabaseName,
        long FrozenXidAge,
        long MinMultiXidAge,
        long AutovacuumFreezeMaxAge,
        long AutovacuumMultixactFreezeMaxAge,
        bool AllowsConnections);

    /// <summary>
    /// The hard ceiling on transaction id age. PostgreSQL stops assigning new ids about 3 million short
    /// of 2^31, so this is the denominator for "percent of the way to a write outage" — as opposed to
    /// percent of the way to an emergency vacuum, which is a different and much nearer threshold.
    /// </summary>
    public const long WraparoundCeiling = 2_147_483_648L;

    /// <summary>
    /// The ids PostgreSQL holds back: it stops accepting new write transactions with roughly this many still
    /// unconsumed, rather than running the counter to the wall. So the useful "how much runway is left"
    /// figure is <see cref="WraparoundCeiling"/> minus this minus the age — which is also what makes the
    /// MCP tool's 99.86%-of-space writes-stop point agree with the stored column.
    /// </summary>
    public const long StopMargin = 3_000_000L;

    /* Reads pg_database, which is a SHARED catalog: one connection sees every database, so this needs
       no per-database fan-out. Verified readable under pg_monitor on our fleet.

       age()/mxid_age() are the correct comparisons rather than arithmetic on the raw xid, because both
       handle the modular wrap that makes a naive subtraction wrong near the boundary — which is exactly
       the region where being wrong matters.

       datfrozenxid's TOAST caveat: a table's own relfrozenxid can be fine while its TOAST relation's is
       not, and pg_database.datfrozenxid already accounts for both because it is the minimum across all
       relations in the database. Per-relation attribution — which table is holding the floor — is a
       per-database read and belongs in its own collector; this one answers "how much time is left",
       which is the alerting question. */
    private const string QueryText = @"
SELECT
    d.datname                                                        AS database_name,
    age(d.datfrozenxid)::bigint                                      AS frozen_xid_age,
    mxid_age(d.datminmxid)::bigint                                   AS min_multixid_age,
    current_setting('autovacuum_freeze_max_age')::bigint             AS autovacuum_freeze_max_age,
    current_setting('autovacuum_multixact_freeze_max_age')::bigint   AS autovacuum_multixact_freeze_max_age,
    d.datallowconn                                                   AS allows_connections
FROM pg_database AS d
/* EVERY database, templates included. The cluster-wide stop limit derives from the oldest datfrozenxid in
   pg_database, so excluding template0/template1 could understate cluster risk by exactly the amount that
   matters — and template0 aging without ever being vacuumed is a real, documented way to get there, typically
   after a major upgrade.

   The per-database autovacuum collector DOES exclude templates, and that is not inconsistent: it needs a
   CONNECTION per database and template0 refuses them (datallowconn = false). This read needs no connection at
   all — pg_database is a SHARED catalog, verified on live Aurora 17.7 where template0's datfrozenxid reads
   fine despite datallowconn = false. allows_connections is already stored, so a consumer can still tell a
   template row apart. */
ORDER BY age(d.datfrozenxid) DESC";

    public override string Name => "pg_wraparound_stats";

    public override string TargetTable => "pg_wraparound_stats";

    /// <summary>Core catalog only — every PostgreSQL target, Aurora or not.</summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("frozen_xid_age", CollectorColumnType.BigInt),
        new CollectorColumn("min_multixid_age", CollectorColumnType.BigInt),
        new CollectorColumn("autovacuum_freeze_max_age", CollectorColumnType.BigInt),
        new CollectorColumn("autovacuum_multixact_freeze_max_age", CollectorColumnType.BigInt),
        /* Both percentages are stored rather than computed on read, because the DENOMINATORS are
           per-server settings that can change: a stored percentage stays true to the configuration in
           force when it was measured, where recomputing later against today's setting would silently
           rewrite history after someone tunes autovacuum_freeze_max_age. */
        new CollectorColumn("pct_toward_emergency_vacuum", CollectorColumnType.Double),
        new CollectorColumn("pct_toward_wraparound", CollectorColumnType.Double),
        new CollectorColumn("pct_toward_multixact_emergency", CollectorColumnType.Double),
        new CollectorColumn("pct_toward_multixact_wraparound", CollectorColumnType.Double),
        new CollectorColumn("xids_remaining", CollectorColumnType.BigInt),
        new CollectorColumn("multixids_remaining", CollectorColumnType.BigInt),
        new CollectorColumn("allows_connections", CollectorColumnType.Boolean),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                DatabaseName: reader.GetString(0),
                FrozenXidAge: reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                MinMultiXidAge: reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                AutovacuumFreezeMaxAge: reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                AutovacuumMultixactFreezeMaxAge: reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                AllowsConnections: !reader.IsDBNull(5) && reader.GetBoolean(5)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. Age is not a counter that accumulates work — it is a distance from a wall, and it
           falls when autovacuum freezes. A delta would describe the rate of approach, which is
           interesting, but the alertable fact is the LEVEL, and a level is what a trend chart of this
           column already shows. */
        writer
            .Value(row.DatabaseName)
            .Value(row.FrozenXidAge)
            .Value(row.MinMultiXidAge)
            .Value(row.AutovacuumFreezeMaxAge)
            .Value(row.AutovacuumMultixactFreezeMaxAge)
            .Value(Pct(row.FrozenXidAge, row.AutovacuumFreezeMaxAge))
            .Value(Pct(row.FrozenXidAge, WraparoundCeiling))
            .Value(Pct(row.MinMultiXidAge, row.AutovacuumMultixactFreezeMaxAge))
            .Value(Pct(row.MinMultiXidAge, WraparoundCeiling))
            /* To where writes STOP, not to the raw 2^31. PostgreSQL refuses new transactions with about
               3,000,000 ids still on the clock, so counting to the ceiling overstated the runway by that
               margin — and disagreed with the MCP tool's own 99.86% figure, which already accounts for it.
               Clamped at 0 so a database past the stop point reports "none left" rather than a negative. */
            .Value(Math.Max(0, WraparoundCeiling - StopMargin - row.FrozenXidAge))
            .Value(Math.Max(0, WraparoundCeiling - StopMargin - row.MinMultiXidAge))
            .Value(row.AllowsConnections);
    }

    /// <summary>
    /// Percentage of <paramref name="ceiling"/> reached, or 0 when the ceiling is unknown. Returning 0
    /// for an unreadable setting is deliberate: an unknown denominator must not manufacture a
    /// percentage, and it must not manufacture an alert either.
    /// </summary>
    private static double Pct(long age, long ceiling)
        => ceiling > 0 ? (double)age / ceiling * 100.0 : 0.0;
}
