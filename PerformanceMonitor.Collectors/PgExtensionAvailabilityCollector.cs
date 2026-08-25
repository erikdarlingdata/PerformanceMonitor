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
/// Which PostgreSQL extensions this target has, could have, or cannot have — the third capability axis
/// (#2545), after engine kind (#2536) and engine edition (#2511).
///
/// <para><b>This axis is the only one whose answer is ACTIONABLE.</b> "Your engine does not support this"
/// is a wall. "This read needs <c>pg_stat_kcache</c>, which is available on this server but not installed —
/// one <c>CREATE EXTENSION</c> away" is a setup step, and that is the difference between a product that
/// looks thin on PostgreSQL and one that says how to make it richer.</para>
///
/// <para><b>Four states, not a boolean.</b> Collapsing to installed/not-installed loses the distinction the
/// whole feature turns on:</para>
///
/// <list type="bullet">
/// <item><c>installed</c> — present in this database and at the newest version the server offers.</item>
/// <item><c>outdated</c> — installed, but <c>default_version</c> is newer. Its own state because a stale
/// extension can be missing columns a collector reads, which surfaces as a confusing 42703 rather than as a
/// capability gap. Fixed with <c>ALTER EXTENSION … UPDATE</c>.</item>
/// <item><c>available</c> — the server has the files; one <c>CREATE EXTENSION</c> installs it. The
/// actionable state.</item>
/// <item><c>absent</c> — not offered by this server at all, so it is an OS-level install or, on managed
/// PostgreSQL, simply not on the menu. Not fixable from SQL.</item>
/// </list>
///
/// <para><b>The scope trap this is written around.</b> The two catalogs have DIFFERENT scopes and it is not
/// obvious from their names: <c>pg_available_extensions</c> is cluster-wide (what the binaries offer), while
/// <c>pg_extension</c> is PER-DATABASE (what has been created here). Measured: <c>pgstattuple</c> reported
/// installed in one database and not in another on the same cluster, while the available list said yes in
/// both. So <c>installed</c> and <c>outdated</c> are claims about THIS DATABASE ONLY, and the column names
/// and the read both have to say so — reporting "not installed" from the maintenance database about an
/// extension living in the application database is the obvious way to get this wrong.</para>
///
/// <para><b>Preload-only modules are deliberately NOT handled here.</b> <c>auto_explain</c> and
/// <c>pg_wait_sampling</c> have no <c>CREATE EXTENSION</c> and never appear in
/// <c>pg_available_extensions</c> — on any server, including ones actively running them. Reporting them
/// through this collector would mark them <c>absent</c> on a server where they are loaded and working, which
/// is a defect this codebase has already shipped once (#2564, fixed in #2584). They are detected by
/// <c>shared_preload_libraries</c> plus the presence of their own GUCs, which is what
/// <see cref="PgPlanCaptureReadinessCollector"/> does for <c>auto_explain</c>.</para>
///
/// <para>Both catalogs read fine for a <c>pg_monitor</c>-only role (measured: 45 and 2 rows), so unlike the
/// column-statistics axis this needs no helper object in the monitored database.</para>
/// </summary>
public sealed class PgExtensionAvailabilityCollector : PostgresCollectorDefinitionBase<PgExtensionAvailabilityCollector.Row>
{
    public static PgExtensionAvailabilityCollector Instance { get; } = new();

    private PgExtensionAvailabilityCollector()
    {
    }

    /// <param name="ExtensionName">The extension's name as PostgreSQL knows it.</param>
    /// <param name="State">One of <c>installed</c>, <c>outdated</c>, <c>available</c>, <c>absent</c>.</param>
    /// <param name="InstalledVersion">Version created IN THIS DATABASE, or null when not installed here.</param>
    /// <param name="DefaultVersion">Newest version the server offers, or null when the server does not
    /// offer it at all.</param>
    /// <param name="IsMonitoringRelevant">Whether this is one of the extensions this product can actually
    /// use, as opposed to one that merely happens to be on the server.</param>
    public readonly record struct Row(
        string? DatabaseName,
        string ExtensionName,
        string State,
        string? InstalledVersion,
        string? DefaultVersion,
        bool IsMonitoringRelevant,
        string? Comment);

    /* The row set is a UNION of two things with different provenance, and the difference is the point.

       The DERIVED half is every extension the server offers or this database has installed - a full outer
       join of the two catalogs, so nothing is missed because it was not on somebody's list. That half cannot
       report `absent`, because absence is not a row in any catalog.

       The ENUMERATED half is the monitoring-relevant roster below, which exists ONLY so absence is
       reportable. It is the smallest enumerated list that makes the actionable state expressible, and it is
       marked as such (`is_monitoring_relevant`) rather than filtering the derived half - a server's other
       extensions are still worth recording, they simply carry no advice from us.

       auto_explain and pg_wait_sampling are absent from the roster ON PURPOSE. Both are preload-only modules
       that never appear in pg_available_extensions even where they are loaded, so including them here would
       manufacture a permanent false `absent`. See the type header. */
    private const string QueryText = @"
WITH relevant (name, comment) AS (
    VALUES
        ('pg_stat_statements', 'Per-statement execution counters. The spine every other query-level read joins to.'),
        ('pgstattuple',        'Exact heap and index bloat, and it runs for a pg_monitor role - a full relation scan, so it is a measurement rather than an estimate.'),
        ('pg_buffercache',     'What is resident in shared buffers, by relation.'),
        ('pg_stat_kcache',     'Real OS CPU and disk per query, on top of pg_stat_statements.'),
        ('pg_qualstats',       'Predicate statistics - which columns are filtered on and how selectively.'),
        ('hypopg',             'Hypothetical indexes, for testing an index without building it.'),
        ('pg_trgm',            'Trigram matching; its absence explains why some LIKE predicates cannot be indexed.'),
        ('pg_cron',            'In-database scheduling. Worth knowing about because its jobs are workload nobody attributes.')
),
present AS (
    SELECT
        a.name                       AS name,
        a.default_version            AS default_version,
        /* PER-DATABASE. pg_extension describes the CONNECTED database only, while pg_available_extensions
           above it is cluster-wide - the two are joined here precisely so the difference is visible rather
           than assumed away. */
        e.extversion                 AS installed_version
    FROM pg_catalog.pg_available_extensions AS a
    FULL OUTER JOIN pg_catalog.pg_extension AS e
      ON e.extname = a.name
)
SELECT
    current_database()::text       AS database_name,
    coalesce(p.name, r.name)::text AS extension_name,
    CASE
        WHEN p.installed_version IS NULL AND p.default_version IS NULL THEN 'absent'
        WHEN p.installed_version IS NULL                               THEN 'available'
        WHEN p.default_version IS NOT NULL
         AND p.installed_version <> p.default_version                  THEN 'outdated'
        ELSE 'installed'
    END::text AS state,
    p.installed_version::text      AS installed_version,
    p.default_version::text        AS default_version,
    (r.name IS NOT NULL)           AS is_monitoring_relevant,
    r.comment::text                AS comment
FROM present AS p
FULL OUTER JOIN relevant AS r
  ON r.name = p.name
ORDER BY (r.name IS NOT NULL) DESC, coalesce(p.name, r.name)";

    public override string Name => "pg_extension_availability";

    public override string TargetTable => "pg_extension_availability";

    /// <summary>
    /// Every PostgreSQL target. Both catalogs are core and readable by any role, and a standby's answer is
    /// worth having on its own account — an extension created on the primary is replicated, but the
    /// server's AVAILABLE set is a property of the binaries on that machine and can genuinely differ.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    /// <summary>
    /// Per database, because half of what this collector reports is a per-database fact.
    ///
    /// <para>This started as a once-per-target collector and that was wrong (#2599). <c>installed</c> and
    /// <c>outdated</c> are derived from <c>pg_extension</c>, which describes the CONNECTED database only;
    /// <c>available</c> and <c>absent</c> come from <c>pg_available_extensions</c> and are genuinely
    /// cluster-wide. Reading both through a single connection produced a table whose answer depended on
    /// which database the target happened to be configured for, with nothing in the row to say which.</para>
    ///
    /// <para>Measured on a live Aurora target: this collector reported <c>pgstattuple</c> as
    /// <c>available</c> while <c>pg_table_bloat_stats</c> — which already ran per database — reported a
    /// <c>pg_extension</c> row for it in the database holding the application's tables. Both reads were
    /// correct; they were looking at different databases. The cluster-wide half is now duplicated once per
    /// database, which is the honest cost of making the per-database half attributable, and it is cheap at
    /// this collector's daily cadence.</para>
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        /* Two of the four states are per-database claims and two are cluster-wide, so the row is only
           interpretable alongside the database it was read in (#2599). See RunsPerDatabase below. */
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("extension_name", CollectorColumnType.Varchar),
        new CollectorColumn("state", CollectorColumnType.Varchar),
        /* Both versions are TEXT and neither is parsed. PostgreSQL extension versions are free-form strings
           ('1.11', '1.5', '2.0-beta') and are compared for EQUALITY only, never ordered - deciding that
           '1.10' is newer than '1.9' needs a version parser this has no business carrying, and the server
           already told us which one is default. */
        new CollectorColumn("installed_version", CollectorColumnType.Varchar),
        new CollectorColumn("default_version", CollectorColumnType.Varchar),
        new CollectorColumn("is_monitoring_relevant", CollectorColumnType.Boolean),
        new CollectorColumn("comment", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                ExtensionName: reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                State: reader.IsDBNull(2) ? "absent" : reader.GetString(2),
                InstalledVersion: reader.IsDBNull(3) ? null : reader.GetString(3),
                DefaultVersion: reader.IsDBNull(4) ? null : reader.GetString(4),
                /* An unreadable flag defaults to NOT relevant. The flag only decides whether we attach
                   advice, and claiming an extension is one we can use when we do not know is the direction
                   that produces a recommendation nobody can act on. */
                IsMonitoringRelevant: !reader.IsDBNull(5) && reader.GetBoolean(5),
                Comment: reader.IsDBNull(6) ? null : reader.GetString(6)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. Every column is a current state; the history exists so somebody can see when an
           extension appeared, which is exactly the question asked after a plan changed shape. */
        writer
            .Value(row.ExtensionName)
            .Value(row.State)
            .Value(row.InstalledVersion)
            .Value(row.DefaultVersion)
            .Value(row.IsMonitoringRelevant)
            .Value(row.Comment);
    }
}
