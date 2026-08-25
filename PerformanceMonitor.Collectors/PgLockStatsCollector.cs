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
/// Lock state by mode, type and relation — <c>pg_locks</c> (#2544, the locks slice).
///
/// <para><b>This does not overlap <see cref="PgBlockingCollector"/>, although it looks like it should.</b>
/// That collector reads <c>pg_blocking_pids()</c> and produces blocked/blocker PAIRS: which backend is stuck
/// behind which, with both sides' query text and durations. It never reads <c>pg_locks</c>. So today the
/// product can say "pid 4821 is blocked by pid 3390" and cannot say what lock, in what mode, or on which
/// relation — and that is the half that decides what to do.</para>
///
/// <para><b>The mode IS the remedy.</b> Measured on a real pile-up: one <c>AccessExclusiveLock</c> granted on
/// a relation with two <c>AccessShareLock</c> requests queued behind it is a DDL blocking every reader, and
/// the fix is to kill the DDL or wait it out. The identical pair shape with <c>RowExclusiveLock</c> is
/// ordinary write concurrency and needs no action at all. The pairs view cannot tell those apart.</para>
///
/// <para><b>And it sees waiters the pairs view structurally cannot.</b> A lock waiting on a PREPARED
/// TRANSACTION has no live backend for <c>pg_blocking_pids()</c> to name, so it is invisible there while
/// being exactly the thing nobody can find. Here it is an ungranted row like any other.</para>
///
/// <para><b>Aggregated, not one row per lock.</b> A busy server holds thousands of lock rows and the grain
/// anyone reasons at is "how many backends want this mode on this relation" — so the snapshot groups by
/// <c>(database, locktype, mode, granted, relation)</c>. That keeps the row count bounded by contention
/// rather than by concurrency.</para>
///
/// <para><b>The scope trap.</b> <c>pg_locks</c> is CLUSTER-WIDE while <c>pg_class</c> is PER-DATABASE, so a
/// lock held on a relation in another database resolves to an OID and no name — measured, not assumed. The
/// row is kept with <c>relation_name</c> NULL and the database named, because dropping it would hide real
/// contention and labelling it from the connected database's <c>pg_class</c> would name the wrong table.
/// This is the third catalog in this effort whose scope does not match its name (see #2543, #2545).</para>
///
/// <para>Readable by <c>pg_monitor</c> with no extra grant and no helper object. Cluster-wide, so no
/// per-database fan-out, and valid on a standby — where a replay-blocking lock is precisely the thing that
/// stalls recovery.</para>
/// </summary>
public sealed class PgLockStatsCollector : PostgresCollectorDefinitionBase<PgLockStatsCollector.Row>
{
    public static PgLockStatsCollector Instance { get; } = new();

    private PgLockStatsCollector()
    {
    }

    /// <param name="LockType">The lockable object class — <c>relation</c>, <c>transactionid</c>,
    /// <c>virtualxid</c>, <c>advisory</c>, <c>tuple</c> and friends.</param>
    /// <param name="Mode">The lock mode. This is the column that decides the remedy.</param>
    /// <param name="Granted">False means these backends are QUEUED.</param>
    /// <param name="RelationOid">Present for relation locks; the OID is stored even when the name cannot be
    /// resolved, so a lock in another database is still identifiable.</param>
    /// <param name="RelationName">NULL when the relation lives in a database other than the connected one —
    /// NOT an absence of contention.</param>
    /// <param name="OldestWaitMs">How long the longest-waiting backend in this group has been waiting. Null
    /// on granted rows, and what turns "two backends queued" into "two backends queued for four minutes".</param>
    public readonly record struct Row(
        string? DatabaseName,
        string? LockType,
        string? Mode,
        bool Granted,
        long? RelationOid,
        string? RelationName,
        long BackendCount,
        double? OldestWaitMs);

    /* pg_locks joined to pg_stat_activity for the wait clock, and to pg_database for the database NAME -
       which is resolvable cluster-wide, unlike the relation name.

       The relation join is deliberately LEFT and deliberately allowed to miss: see the type header. A row
       whose relation_name is NULL is either a non-relation lock (transactionid, virtualxid, advisory - which
       have no relation at all) or a relation in another database. relation_oid tells those apart, so both
       are honestly representable without inventing a name.

       pg_backend_pid() is excluded so the collector never reports its own AccessShareLocks on the catalogs
       it is reading - a self-marker, the same rule the statement collector applies.

       Wait time comes from pg_stat_activity.state_change rather than query_start: a backend waiting on a
       lock has been in its current STATE since it started waiting, whereas query_start also covers the work
       it did before hitting the lock. Reporting the latter would overstate every wait by however long the
       statement had already been running. */
    private const string QueryText = @"
SELECT
    d.datname::text                                       AS database_name,
    l.locktype::text                                      AS lock_type,
    l.mode::text                                          AS mode,
    l.granted                                             AS granted,
    l.relation::bigint                                    AS relation_oid,
    c.relname::text                                       AS relation_name,
    count(*)::bigint                                      AS backend_count,
    /* Granted rows have no wait to report, and MAX over an empty filtered set is NULL rather than 0 -
       which is the honest answer, since 0 would read as 'granted instantly'. */
    max(CASE WHEN NOT l.granted
             THEN EXTRACT(EPOCH FROM (clock_timestamp() - a.state_change)) * 1000
        END)                                              AS oldest_wait_ms
FROM pg_catalog.pg_locks AS l
LEFT JOIN pg_catalog.pg_database AS d
  ON d.oid = l.database
LEFT JOIN pg_catalog.pg_class AS c
  ON c.oid = l.relation
LEFT JOIN pg_catalog.pg_stat_activity AS a
  ON a.pid = l.pid
WHERE l.pid <> pg_catalog.pg_backend_pid()
GROUP BY d.datname, l.locktype, l.mode, l.granted, l.relation, c.relname
/* Ungranted first, then the biggest queues: the row that matters is always a queue, and a grid that opened
   with a hundred granted AccessShareLocks would bury it. */
ORDER BY l.granted, count(*) DESC, l.mode";

    public override string Name => "pg_lock_stats";

    public override string TargetTable => "pg_lock_stats";

    /// <summary>
    /// Every PostgreSQL target. <c>pg_locks</c> is core and has no version floor worth gating on, and a
    /// standby's answer matters on its own account — a lock that conflicts with WAL replay stalls recovery,
    /// which is a failure mode with no primary-side equivalent.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("lock_type", CollectorColumnType.Varchar),
        /* The single most important column here. AccessExclusiveLock ungranted is a DDL queue;
           RowExclusiveLock contention is ordinary write traffic. Same shape, opposite advice. */
        new CollectorColumn("mode", CollectorColumnType.Varchar),
        new CollectorColumn("granted", CollectorColumnType.Boolean),
        /* BIGINT rather than integer: OIDs are unsigned 32-bit, so one past 2^31 overflows a signed int and
           lands negative. Rare, and silently wrong when it happens. */
        new CollectorColumn("relation_oid", CollectorColumnType.BigInt),
        new CollectorColumn("relation_name", CollectorColumnType.Varchar),
        new CollectorColumn("backend_count", CollectorColumnType.BigInt),
        new CollectorColumn("oldest_wait_ms", CollectorColumnType.Double),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                LockType: reader.IsDBNull(1) ? null : reader.GetString(1),
                Mode: reader.IsDBNull(2) ? null : reader.GetString(2),
                /* An unreadable granted flag reads as NOT granted, so the row surfaces as a queue rather
                   than being quietly filed under "fine". Over-reporting contention is recoverable; hiding
                   it is the failure that matters. */
                Granted: !reader.IsDBNull(3) && reader.GetBoolean(3),
                RelationOid: reader.IsDBNull(4) ? null : reader.GetInt64(4),
                RelationName: reader.IsDBNull(5) ? null : reader.GetString(5),
                BackendCount: reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                OldestWaitMs: reader.IsDBNull(7) ? null : reader.GetDouble(7)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. This is a SAMPLE of instantaneous state, not a counter - the same shape as
           pg_blocking, and read the same way: the capture count is the denominator, so three ungranted rows
           means something different in 60 samples than in 4. */
        writer
            .Value(row.DatabaseName)
            .Value(row.LockType)
            .Value(row.Mode)
            .Value(row.Granted)
            .Value(row.RelationOid)
            .Value(row.RelationName)
            .Value(row.BackendCount)
            .Value(row.OldestWaitMs);
    }
}
