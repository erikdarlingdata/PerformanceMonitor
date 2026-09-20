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

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Per-database size for a PostgreSQL target, hourly — the SERIES the object-growth question needs and the
/// engine has no source for (#3691, design §6 "collector follow-ups").
///
/// <para><b>Why a series and not a snapshot.</b> Every size question an operator asks is a rate: "how fast is
/// this database growing", "when does it cross the volume", "did the growth start with the deploy". A number
/// read at question time answers none of them; a row an hour, kept for a year, answers all three from one
/// table. The SQL Server side has had this since V1 (<c>database_size_stats</c>); the PostgreSQL side had
/// nothing — <c>pg_table_bloat_stats</c> sizes RELATIONS, per database, for the bloat question, and the
/// instance total was never stored anywhere.</para>
///
/// <para><b>One read of the shared catalog, every database, templates included.</b> <c>pg_database</c> is a
/// shared catalog, so one connection sees every database and this collector needs no per-database fan-out
/// — the <see cref="PgWraparoundStatsCollector"/> shape exactly. Templates are collected rather than skipped:
/// they are tiny, <c>is_template</c> is stored so a consumer can set them aside, and a collector that
/// decides what is interesting is a collector whose absence a consumer cannot tell from a zero. Aurora
/// reports a per-database size through <c>pg_database_size()</c> like any PostgreSQL even though its storage
/// volume is shared; the figure is the database's, not the volume's.</para>
///
/// <para><b>A database this role may not size is NULL, never 0.</b> <c>pg_database_size()</c> raises unless
/// the caller has <c>CONNECT</c> on the database or the privileges of <c>pg_read_all_stats</c>, and one
/// raise fails the whole read. The query asks both questions FIRST (<c>has_database_privilege</c> and
/// <c>pg_has_role … 'USAGE'</c>, the two tests <c>dbsize.c</c> makes) and sizes only the databases that
/// pass, so a managed target's vendor-owned database — the one a monitoring role is routinely not granted
/// — lands as a row with <c>size_bytes NULL</c> instead of taking the whole collection down. A 0 there
/// would read as an empty database; NULL reads as "not measured", which is the truth.</para>
///
/// <para><b><c>total_bytes</c> is the instance total, denormalized onto every row — and NULL when any
/// database's size is.</b> Denormalized so the instance series is one read with no GROUP BY, the shape
/// <c>pg_session_states.total_sessions</c> established. NULL rather than a partial sum because a sum over
/// the databases this role CAN size is not the instance total, and a consumer that trends it would see the
/// vendor database's growth as the operator's databases shrinking. The per-database <c>size_bytes</c> stay
/// populated on that row; only the total declines to guess.</para>
///
/// <para><b>Cadence and cost.</b> Hourly: a database's size moves in checkpoint- and autovacuum-sized steps,
/// not continuously, and a trend a year long does not need minutes. A year of retention (the ladder's
/// existing 365-day tier — <c>server_properties</c> and the PostgreSQL configuration snapshots live there)
/// because "how has this grown since last year" is the question the series is for. The row cost is
/// trivial: ~50 clusters × ~5 databases × 24 rows a day is ~6,000 rows a day fleet-wide, a rounding error
/// beside the per-minute families.</para>
///
/// <para><b>Named <c>pg_database_size_stats</c>, not <c>pg_database_size</c>.</b> The bare name is PostgreSQL's
/// own function, and this product already calls that function by name in its store self-metrics and its
/// viewer's server-status read; a store TABLE spelled identically would make every text search for the
/// function find the table and every unqualified <c>FROM</c> read as the other, and the viewer coverage
/// ratchet — a substring scan over the reader layer — would count the function call as a reader of the
/// table. <c>_stats</c> is the SQL Server twin's suffix (<c>database_size_stats</c>) and the family's
/// (<c>pg_wraparound_stats</c>, <c>pg_database_stats</c>).</para>
///
/// <para>Read by nothing yet. The consumer lanes — object growth, the disk-free composition — are #3691's
/// later slices; this collector's job is to have a year of rows waiting when they land.</para>
/// </summary>
public sealed class PgDatabaseSizeStatsCollector : PostgresCollectorDefinitionBase<PgDatabaseSizeStatsCollector.Row>
{
    public static PgDatabaseSizeStatsCollector Instance { get; } = new();

    private PgDatabaseSizeStatsCollector()
    {
    }

    /// <param name="DatabaseName">The database's name, <c>pg_database.datname</c>.</param>
    /// <param name="SizeBytes"><c>pg_database_size(oid)</c> in BYTES, or NULL where this role lacks both
    /// <c>CONNECT</c> on the database and <c>pg_read_all_stats</c> — "not measured", never 0.</param>
    /// <param name="TotalBytes">The instance total in BYTES, the same figure on every row of one collection;
    /// NULL when any database's <see cref="SizeBytes"/> is NULL, because a partial sum is not a total.
    /// Filled by <see cref="ReadAsync"/> after every row is read, not by the query.</param>
    /// <param name="IsTemplate"><c>pg_database.datistemplate</c>, so a consumer can set templates aside.</param>
    /// <param name="AllowsConnections"><c>pg_database.datallowconn</c>, the same flag
    /// <see cref="PgWraparoundStatsCollector"/> stores for the same reason.</param>
    public readonly record struct Row(
        string DatabaseName,
        long? SizeBytes,
        long? TotalBytes,
        bool IsTemplate,
        bool AllowsConnections);

    /* Reads pg_database, a SHARED catalog: one connection sees every database, so no per-database fan-out.

       CASE evaluates lazily in PostgreSQL, so pg_database_size() is only ever called on a database the two
       privilege tests admit — has_database_privilege(oid, 'CONNECT') and pg_has_role(current_user,
       'pg_read_all_stats', 'USAGE') are the two checks dbsize.c makes (aclcheck CONNECT, then
       has_privs_of_role(pg_read_all_stats)), and 'USAGE' rather than 'MEMBER' because has_privs_of_role is
       the privileges-without-SET-ROLE test. A database that fails both lands as NULL rather than raising.

       No total here: a window SUM over the admitted databases would be a partial sum where any size is
       NULL, and the collector's ReadAsync is where "NULL if any is NULL" is decided, once. */
    private const string QueryText = @"
SELECT
    d.datname                                                       AS database_name,
    CASE
        WHEN has_database_privilege(d.oid, 'CONNECT')
          OR pg_has_role(current_user, 'pg_read_all_stats', 'USAGE')
        THEN pg_database_size(d.oid)
        ELSE NULL
    END                                                             AS size_bytes,
    d.datistemplate                                                 AS is_template,
    d.datallowconn                                                  AS allows_connections
FROM pg_database AS d
ORDER BY d.datname";

    public override string Name => "pg_database_size_stats";

    public override string TargetTable => "pg_database_size_stats";

    /// <summary>Core catalog only — every PostgreSQL target, Aurora or not, like
    /// <see cref="PgWraparoundStatsCollector"/>.</summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        /* BYTES, bigint, both of them — the one naming contract a later join against the store's own
           collect.store_metrics (total_bytes) or the SQL Server side's size columns depends on. Never MB or
           GiB: a unit in the column name is a unit a reader has to know, and two readers will know two. */
        new CollectorColumn("size_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("total_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("is_template", CollectorColumnType.Boolean),
        new CollectorColumn("allows_connections", CollectorColumnType.Boolean),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                DatabaseName: reader.GetString(0),
                SizeBytes: reader.IsDBNull(1) ? null : reader.GetInt64(1),
                TotalBytes: null,
                IsTemplate: !reader.IsDBNull(2) && reader.GetBoolean(2),
                AllowsConnections: !reader.IsDBNull(3) && reader.GetBoolean(3)));
        }

        return WithInstanceTotal(rows);
    }

    /// <summary>
    /// Stamps the instance total onto every row: the sum of every <see cref="Row.SizeBytes"/> when all of
    /// them are measured, NULL on every row when any one is not. Static and public so the "partial sum is
    /// not a total" rule is assertable without a reader or a connection — it is the one decision this
    /// collector makes that the query does not. Public rather than internal so the Darling rung test can drive the real writer with rows the real rule shaped.
    /// </summary>
    public static List<Row> WithInstanceTotal(List<Row> rows)
    {
        long? total = 0;

        foreach (var row in rows)
        {
            if (row.SizeBytes is null)
            {
                total = null;
                break;
            }

            total += row.SizeBytes.Value;
        }

        for (var i = 0; i < rows.Count; i++)
        {
            rows[i] = rows[i] with { TotalBytes = total };
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. Size is a LEVEL; the growth rate is a difference over the stored series, which is what
           the series is for. Both byte columns are written as they are — null stays null, per the type
           header: 0 would be an empty database, and nobody measured one. */
        writer
            .Value(row.DatabaseName)
            .Value(row.SizeBytes)
            .Value(row.TotalBytes)
            .Value(row.IsTemplate)
            .Value(row.AllowsConnections);
    }
}
