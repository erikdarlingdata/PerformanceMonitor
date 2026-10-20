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
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The plans Query Store has forced (#5632), per database, every pass. The <c>query_store</c> collector reads only
/// plans that RAN since its last pass (<c>WatermarkColumn => "last_execution_time"</c>), so a forced plan the engine
/// ignores, or that never runs, never reaches the store. This reads <c>sys.query_store_plan</c> itself, where
/// <c>is_forced_plan = 1</c>, a handful of rows per database, so the store can say whether a force works.
///
/// <para><b>One marker row per database per pass, with no plan</b> (every payload column but <c>database_name</c>
/// NULL), written even when the database has no forced plan. It is how a reader tells a force that was REMOVED (gone
/// from the newest capture of a database that was captured) from a database that was not captured. The precedent is
/// <see cref="PlanCorrectionCollector"/>'s enablement-only row.</para>
///
/// <para><b>Which databases.</b> The ones <see cref="QueryStoreCollector"/> collects: Query Store in a usable state
/// (<c>actual_state IN (1, 2, 4)</c> and not a readable secondary) and the same vendor-name screen. The state check
/// runs inside each database's own statement, so the enumeration is one cheap <c>sys.databases</c> read instead of a
/// probe cursor.</para>
///
/// <para><b>Load.</b> Per database one statement over <c>sys.query_store_plan</c> with <c>is_forced_plan = 1</c>,
/// bounded by <see cref="MaxRowsPerDatabase"/>, never the plan XML. The XML of the plans of a forced query that compiled AFTER
/// the force (higher plan_id) is only searched with <c>CHARINDEX</c> for two attributes (#6160), and that search is the whole
/// cost of a pass (#6221, measured on SQL Server 2025: the forced-plan read is about 20 ms for 300 forced plans; reading
/// the XML of their 900 later plans is 0.5 to 0.6 s at 12 KB a plan and 4.5 s for 240 plans at 360 KB a plan, so 95 percent
/// or more of the time). A plan's stored XML never changes for a plan id, so each plan's XML is read ONCE per process:
/// the verdict (variant or not, guide name) is cached per (server, database, plan id) and sent back with each pass as
/// <c>#pm_known_plans</c>, and only plans not in it are read, at most <see cref="MaxNewXmlPlansPerDatabasePerPass"/> per
/// database per pass, newest first. The rest are carried to later passes (not counted as variants until read). A restart
/// empties the cache, so the first passes read up to the cap each until the database is caught up. A cached entry also
/// carries the plan's <c>query_plan_hash</c>, so a plan id that Query Store reuses after a CLEAR is read again, and a
/// cached plan that is no longer among a forced query's later plans is dropped from the cache. A plain table on every store
/// (<see cref="StaysPlainTable"/>): the volume is a few rows per database per pass, the same grid decision as
/// the attention table (#3035, #3044, #3107).</para>
///
/// <para><b>Variants and plan guides (#6160).</b> SQL Server can recompile a forced plan under a NEW plan id of the same query; that
/// plan's stored XML carries <c>UsePlan="1"</c> and no <c>PlanGuideName</c> (a variant, <see cref="Row.VariantPlanIds"/>). A
/// <c>USE PLAN</c> plan guide on the forced query is read from <c>sys.plan_guides</c> in the same database: an OBJECT-scoped guide
/// by <c>scope_object_id</c> = the query's <c>object_id</c> (the guide may cover another statement of the same object: the scope
/// is the only match, no query text is compared); a guide of any scope whose name a later plan of the query carries as
/// <c>PlanGuideName</c>. Only guides in the query's own database are seen.</para>
///
/// <para><b>Version gates.</b> <c>plan_forcing_type_desc</c> is SQL Server 2017 and later (and always on Azure), read as
/// a typed NULL below that, the way <see cref="QueryStoreCollector"/> gates the same column.</para>
/// </summary>
public sealed class QueryStoreForcedPlansCollector : CollectorDefinitionBase<QueryStoreForcedPlansCollector.Row>
{
    public static QueryStoreForcedPlansCollector Instance { get; } = new();

    private QueryStoreForcedPlansCollector()
        : this(MaxNewXmlPlansPerDatabasePerPass)
    {
    }

    /// <summary>A collector with its own cache and its own per-pass read cap; the tests use it, the host uses <see cref="Instance"/>.</summary>
    internal QueryStoreForcedPlansCollector(int maxNewXmlPlansPerDatabasePerPass)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxNewXmlPlansPerDatabasePerPass);
        _maxNewXmlPlans = maxNewXmlPlansPerDatabasePerPass;
    }

    /// <summary>What one plan's stored XML said, read once (a plan's XML never changes for its plan id).</summary>
    private readonly record struct CheckedPlan(string PlanHash, bool IsVariant, string? GuideName);

    private readonly int _maxNewXmlPlans;
    private readonly object _cacheLock = new();
    private readonly Dictionary<(int ServerId, string Database), Dictionary<long, CheckedPlan>> _checked = new();
    private long _xmlPlansChecked;

    /// <summary>How many plans' XML this collector has read since it was created (#6221). A pass over plans it already read adds none.</summary>
    public long XmlPlansChecked => Interlocked.Read(ref _xmlPlansChecked);

    /// <summary>How many plans' verdicts the cache holds for one server's database.</summary>
    internal int CachedPlanCount(int serverId, string database)
    {
        lock (_cacheLock)
        {
            return _checked.TryGetValue((serverId, database), out var plans) ? plans.Count : 0;
        }
    }

    public sealed class Row
    {
        public string DbName { get; set; } = "";

        /// <summary>NULL on the marker row.</summary>
        public long? PlanId { get; set; }
        public long? QueryId { get; set; }
        public string? QueryPlanHash { get; set; }
        public string? PlanForcingType { get; set; }
        public long? ForceFailureCount { get; set; }

        /// <summary>The engine's reason text; NULL when it says NONE.</summary>
        public string? LastForceFailureReason { get; set; }
        public DateTime? LastExecutionTimeUtc { get; set; }
        public DateTime? LastCompileStartTimeUtc { get; set; }

        /// <summary>
        /// #6160: the plan ids of the same query, newer than the forced plan, whose stored XML carries <c>UsePlan="1"</c> and no
        /// <c>PlanGuideName</c>, as a comma-separated list in ascending order with no spaces (<c>12,15</c>), at most
        /// <see cref="MaxVariantPlanIds"/> of them. NULL on the marker row and when there are none.
        /// </summary>
        public string? VariantPlanIds { get; set; }

        /// <summary>#6160: the name of a plan guide that covers the forced query (see the class remarks); NULL when none was found.</summary>
        public string? PlanGuideName { get; set; }

        /// <summary>#6160: NULL when no guide was found, else whether the guide is enabled.</summary>
        public bool? PlanGuideEnabled { get; set; }

        /// <summary>#6160: the guide's <c>scope_type_desc</c> (OBJECT, SQL or TEMPLATE); NULL when no guide was found.</summary>
        public string? PlanGuideScope { get; set; }
    }

    /// <summary>The most variant plan ids one forced plan lists (the oldest first). A query with more is not a normal one.</summary>
    public const int MaxVariantPlanIds = 20;

    /// <summary>
    /// The most later plans (newest first) of forced queries one database's pass considers, cached or not. It bounds the cache and the
    /// text of the known-plans list sent with the pass. One production store has about 300 forced plans per server with about three
    /// later plans each (about 900), so a normal database sits well under it.
    /// </summary>
    public const int MaxXmlPlansPerDatabase = 2000;

    /// <summary>
    /// The most plans whose XML one database's pass reads for the first time (#6221), newest first. A plan whose XML was read earlier in
    /// this process costs no XML read. Measured on SQL Server 2025: about 0.7 ms a plan at 12 KB of XML and about 19 ms a plan at 360 KB, so a full
    /// pass of 100 costs 70 ms to 2 s on the monitored server (the old read of all 900 later plans cost 0.5 s at the small size and
    /// 4.5 s for 240 plans at the large one, every pass). A database with 900 unread later plans is caught up in nine passes.
    /// </summary>
    public const int MaxNewXmlPlansPerDatabasePerPass = 100;

    /// <summary>The most forced plans one database ships in one pass. A database with more forces than this is not a normal one.</summary>
    public const int MaxRowsPerDatabase = 500;

    /// <summary>The per-command deadline: a few rows per database should take milliseconds, so this is a backstop, not a budget.</summary>
    public const int CommandTimeoutSeconds = 120;

    public override string Name => "query_store_forced_plans";

    public override string TargetTable => "query_store_forced_plans";

    public override int? CommandTimeoutSecondsOverride => CommandTimeoutSeconds;

    /// <summary>The wall clock for one server's whole pass, the plan_correction bound: a pass past it ships nothing and retries.</summary>
    public override TimeSpan? PerItemWallClockBudget => TimeSpan.FromSeconds(120);

    /// <summary>The plan table stays a plain table with a 30-day time-sliced DELETE (see the class remarks).</summary>
    public override bool StaysPlainTable => true;

    /// <summary>Darling stores and reads it; Lite has no twin yet (the read is a Darling MCP tool).</summary>
    public override bool IsDarlingOnly => true;

    /// <summary>Query Store is 2016 (v13) and later; Azure SQL DB and Managed Instance are never version-gated; unknown (0) is assumed newest.</summary>
    public override bool AppliesTo(CollectorTargetInfo target) =>
        target is null
        || target.SqlMajorVersion == 0
        || target.SqlMajorVersion >= 13
        || target.IsAzureSqlDb
        || target.IsAzureManagedInstance;

    /// <summary>Azure SQL DB rejects three-part names, so the host connects per database there instead.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target is not null && target.IsAzureSqlDb;

    /// <summary>The same databases <see cref="QueryStoreCollector"/> enumerates, minus its per-database state probe (the per-database statement checks it).</summary>
    private const string OnPremDatabaseListQueryText = @"
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    d.name
FROM sys.databases AS d
LEFT JOIN sys.dm_hadr_database_replica_states AS drs
    ON d.database_id = drs.database_id
    AND drs.is_local = 1
WHERE d.database_id > 4
AND   d.database_id < 32761
AND   d.state_desc = N'ONLINE'
AND   d.name <> N'PerformanceMonitor'
AND   HAS_DBACCESS(d.name) = 1
AND   d.name NOT IN
      (
          N'master', N'model', N'msdb', N'tempdb',
          N'rdsadmin', N'gcloud_cloudsqladmin',
          N'ReportServer', N'ReportServerTempDB',
          N'DWConfiguration', N'DWDiagnostics', N'DWQueue',
          N'DBAUtil', N'DBAUtils', N'Utility'
      )
AND
(
    drs.database_id IS NULL
    OR drs.is_primary_replica = 1
)
/*EXCLUSION_FILTER*/
ORDER BY d.name
OPTION(RECOMPILE);";

    public override CollectorQuery? BuildEnumerationQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (context.Target.IsAzureSqlDb)
        {
            return null;
        }

        var (exclusionClause, exclusionParameters) = DatabaseScopeFilter.BuildEnumerationPredicate(context, "d.name");
        var text = OnPremDatabaseListQueryText
            .Replace("/*EXCLUSION_FILTER*/", exclusionClause, StringComparison.Ordinal);

        return new CollectorQuery(text, exclusionParameters);
    }

    public override CollectorQuery? BuildEnumerationProbe(CollectorContext context)
        => new(QueryStoreCollector.ProductVersionProbeText);

    /// <summary>
    /// The per-database read, one body for both paths (the on-prem path quote-doubles it into
    /// <c>[db].sys.sp_executesql</c>, Azure runs it as-is). The eligibility gate returns NO result set for a database
    /// whose Query Store is off, in error or a readable secondary, so such a database writes nothing (not captured is
    /// not the same as no forced plans). Otherwise it returns ONE all-NULL marker row, always, then every forced plan
    /// (the marker is how a reader tells the capture happened). <c>is_forced_plan = 1</c> is the only filter.
    /// </summary>
    internal static string BuildPayloadBody(CollectorContext context) =>
        BuildPayloadBody(context, "", MaxNewXmlPlansPerDatabasePerPass);

    /// <summary>
    /// <paramref name="knownPlansInsert"/> is the INSERT statements that load the plans whose XML an earlier pass already read
    /// (<see cref="KnownPlansInsert"/>), empty on a first pass; <paramref name="maxNewXmlPlans"/> is the per-pass read cap.
    /// </summary>
    private static string BuildPayloadBody(CollectorContext context, string knownPlansInsert, int maxNewXmlPlans)
    {
        var productVersion = context.EnumerationProbeResult is null
            ? QueryStoreCollector.DefaultProductVersion
            : Convert.ToInt32(context.EnumerationProbeResult, CultureInfo.InvariantCulture);
        var hasForcingType = productVersion > 13 || context.Target.IsAzureSqlDb || context.Target.IsAzureManagedInstance;
        var forcingTypeColumn = hasForcingType
            ? "plan_forcing_type = qsp.plan_forcing_type_desc,"
            : "plan_forcing_type = CONVERT(nvarchar(60), NULL),";

        return $@"
SET NOCOUNT ON;

IF NOT EXISTS
(
    SELECT
        1
    FROM sys.database_query_store_options
    WHERE actual_state IN (1, 2, 4)
    AND   readonly_reason & 8 = 0
)
BEGIN
    RETURN;
END;

DROP TABLE IF EXISTS #pm_forced_plans;
DROP TABLE IF EXISTS #pm_forced_later_plans;
DROP TABLE IF EXISTS #pm_known_plans;

CREATE TABLE #pm_known_plans
(
    plan_id bigint NOT NULL PRIMARY KEY,
    query_plan_hash varchar(64) NULL,
    is_variant bit NOT NULL,
    guide_name nvarchar(128) NULL
);
{knownPlansInsert}

SELECT TOP ({MaxRowsPerDatabase})
    plan_id = qsp.plan_id,
    query_id = qsp.query_id,
    object_id = CONVERT(bigint, qsq.object_id),
    query_plan_hash = CONVERT(varchar(64), qsp.query_plan_hash, 1),
    {forcingTypeColumn}
    force_failure_count = CONVERT(bigint, qsp.force_failure_count),
    last_force_failure_reason = NULLIF(qsp.last_force_failure_reason_desc, N'NONE'),
    last_execution_time = CONVERT(datetime2, qsp.last_execution_time AT TIME ZONE 'UTC'),
    last_compile_start_time = CONVERT(datetime2, qsp.last_compile_start_time AT TIME ZONE 'UTC')
INTO #pm_forced_plans
FROM sys.query_store_plan AS qsp
LEFT JOIN sys.query_store_query AS qsq
    ON qsq.query_id = qsp.query_id
WHERE qsp.is_forced_plan = 1
ORDER BY qsp.plan_id
OPTION(RECOMPILE);

/* The later plans of forced queries, newest first. No plan XML is read here: a plan an earlier pass read comes back
   with its stored verdict (#pm_known_plans, matched on plan id AND hash, so a plan id Query Store reuses after a CLEAR
   is read again). */
SELECT TOP ({MaxXmlPlansPerDatabase})
    query_id = l.query_id,
    plan_id = l.plan_id,
    query_plan_hash = CONVERT(varchar(64), l.query_plan_hash, 1),
    is_checked = CONVERT(bit, CASE WHEN k.plan_id IS NULL THEN 0 ELSE 1 END),
    is_variant = CONVERT(bit, k.is_variant),
    guide_name = k.guide_name
INTO #pm_forced_later_plans
FROM #pm_forced_plans AS f
JOIN sys.query_store_plan AS l
    ON  l.query_id = f.query_id
    AND l.plan_id > f.plan_id
LEFT JOIN #pm_known_plans AS k
    ON  k.plan_id = l.plan_id
    AND ISNULL(k.query_plan_hash, '') = ISNULL(CONVERT(varchar(64), l.query_plan_hash, 1), '')
ORDER BY l.plan_id DESC
OPTION(RECOMPILE);

/* The only statement that reads plan XML: the newest {maxNewXmlPlans} plans nobody has read yet (#6221). The rest wait for
   the next pass. The XML of a plan id never changes, so a plan read here is never read again by this process. */
UPDATE l
SET
    is_checked = 1,
    is_variant =
        CONVERT
        (
            bit,
            CASE
                WHEN CHARINDEX(N'UsePlan=""1""', p.query_plan) > 0
                AND  x.guide_at = 0
                THEN 1
                ELSE 0
            END
        ),
    guide_name =
        CONVERT
        (
            nvarchar(128),
            CASE
                WHEN x.guide_at > 0
                AND  CHARINDEX(N'""', p.query_plan, x.guide_at + 15) > 0
                THEN SUBSTRING(p.query_plan, x.guide_at + 15, CHARINDEX(N'""', p.query_plan, x.guide_at + 15) - x.guide_at - 15)
            END
        )
FROM #pm_forced_later_plans AS l
JOIN
(
    SELECT TOP ({maxNewXmlPlans})
        plan_id = n.plan_id
    FROM #pm_forced_later_plans AS n
    WHERE n.is_checked = 0
    ORDER BY n.plan_id DESC
) AS t
    ON t.plan_id = l.plan_id
JOIN sys.query_store_plan AS p
    ON p.plan_id = l.plan_id
CROSS APPLY
(
    SELECT
        guide_at = CHARINDEX(N'PlanGuideName=""', p.query_plan)
) AS x
OPTION(RECOMPILE);

SELECT /* PerformanceMonitorLite */
    plan_id = CONVERT(bigint, NULL),
    query_id = CONVERT(bigint, NULL),
    query_plan_hash = CONVERT(varchar(64), NULL),
    plan_forcing_type = CONVERT(nvarchar(60), NULL),
    force_failure_count = CONVERT(bigint, NULL),
    last_force_failure_reason = CONVERT(nvarchar(60), NULL),
    last_execution_time = CONVERT(datetime2, NULL),
    last_compile_start_time = CONVERT(datetime2, NULL),
    variant_plan_ids = CONVERT(varchar(400), NULL),
    plan_guide_name = CONVERT(nvarchar(128), NULL),
    plan_guide_enabled = CONVERT(bit, NULL),
    plan_guide_scope = CONVERT(nvarchar(60), NULL)
UNION ALL
SELECT
    f.plan_id,
    f.query_id,
    f.query_plan_hash,
    f.plan_forcing_type,
    f.force_failure_count,
    f.last_force_failure_reason,
    f.last_execution_time,
    f.last_compile_start_time,
    v.variant_plan_ids,
    g.plan_guide_name,
    g.plan_guide_enabled,
    g.plan_guide_scope
FROM #pm_forced_plans AS f
OUTER APPLY
(
    SELECT
        variant_plan_ids =
            CONVERT
            (
                varchar(400),
                STUFF
                (
                    (
                        SELECT TOP ({MaxVariantPlanIds})
                            ',' + CONVERT(varchar(20), l.plan_id)
                        FROM #pm_forced_later_plans AS l
                        WHERE l.query_id = f.query_id
                        AND   l.is_variant = 1
                        ORDER BY
                            l.plan_id
                        FOR XML PATH('')
                    ),
                    1,
                    1,
                    ''
                )
            )
) AS v
OUTER APPLY
(
    SELECT TOP (1)
        plan_guide_name = pg.name,
        plan_guide_enabled = CONVERT(bit, CASE WHEN pg.is_disabled = 1 THEN 0 ELSE 1 END),
        plan_guide_scope = pg.scope_type_desc
    FROM sys.plan_guides AS pg
    WHERE
    (
        pg.scope_type_desc = N'OBJECT'
        AND f.object_id <> 0
        AND pg.scope_object_id = f.object_id
        AND pg.hints LIKE N'%USE PLAN%'
    )
    OR pg.name IN
    (
        SELECT
            l.guide_name
        FROM #pm_forced_later_plans AS l
        WHERE l.query_id = f.query_id
        AND   l.guide_name IS NOT NULL
    )
    ORDER BY
        CASE WHEN pg.is_disabled = 0 THEN 0 ELSE 1 END,
        pg.name
) AS g
ORDER BY
    plan_id
OPTION(RECOMPILE);

/* Second result set: every later plan whose XML has been read, from this pass or an earlier one. The collector keeps exactly
   these as its cache, so a plan that left the query's later plans drops out of it. */
SELECT
    plan_id = l.plan_id,
    query_plan_hash = l.query_plan_hash,
    is_variant = l.is_variant,
    guide_name = l.guide_name
FROM #pm_forced_later_plans AS l
WHERE l.is_checked = 1
ORDER BY
    l.plan_id
OPTION(RECOMPILE);

DROP TABLE IF EXISTS #pm_forced_plans;
DROP TABLE IF EXISTS #pm_forced_later_plans;
DROP TABLE IF EXISTS #pm_known_plans;";
    }

    /// <summary>The Azure per-database branch: the connection is already in the database.</summary>
    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!context.Target.IsAzureSqlDb)
        {
            throw new NotSupportedException(
                "query_store_forced_plans enumerates databases off Azure SQL DB; BuildEnumerationQuery drives the cycle.");
        }

        return new CollectorQuery(BuildPayloadBody(context, KnownPlansInsert(context.ServerId, context.CurrentDatabaseName ?? ""), _maxNewXmlPlans));
    }

    public override CollectorQuery BuildPerItemQuery(string item, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(context);

        var text = $@"
EXECUTE [{item.Replace("]", "]]", StringComparison.Ordinal)}].sys.sp_executesql
    N'{BuildPayloadBody(context, KnownPlansInsert(context.ServerId, item), _maxNewXmlPlans).Replace("'", "''", StringComparison.Ordinal)}'";

        return new CollectorQuery(text);
    }

    public override async ValueTask ReadItemAsync(string item, DbDataReader reader, List<Row> rows, CollectorContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(rows);
        ArgumentNullException.ThrowIfNull(context);

        await ReadPassAsync(item, reader, rows, context.ServerId, cancellationToken);
    }

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(context);

        var rows = new List<Row>();
        await ReadPassAsync(context.CurrentDatabaseName ?? "", reader, rows, context.ServerId, cancellationToken);
        return rows;
    }

    /// <summary>
    /// One database's pass: the forced plans (and the marker row), then the second result set, the plans whose XML is read. A
    /// database that returned no result set (Query Store off) leaves the cache as it was; a database that returned the marker row
    /// replaces its cache with the second result set, which is how a plan that left the query's later plans is dropped.
    /// </summary>
    private async ValueTask ReadPassAsync(string database, DbDataReader reader, List<Row> rows, int serverId, CancellationToken cancellationToken)
    {
        var sawResult = false;
        while (await reader.ReadAsync(cancellationToken))
        {
            sawResult = true;
            rows.Add(ReadRow(reader, database));
        }

        if (!sawResult || !await reader.NextResultAsync(cancellationToken))
        {
            return;
        }

        var current = new Dictionary<long, CheckedPlan>();
        while (await reader.ReadAsync(cancellationToken))
        {
            current[Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture)] = new CheckedPlan(
                NullableString(reader, 1) ?? "",
                !reader.IsDBNull(2) && Convert.ToBoolean(reader.GetValue(2), CultureInfo.InvariantCulture),
                NullableString(reader, 3));
        }

        lock (_cacheLock)
        {
            var key = (serverId, database);
            var previous = _checked.GetValueOrDefault(key);
            var newlyRead = previous is null ? current.Count : current.Count(c => !previous.TryGetValue(c.Key, out var was) || was.PlanHash != c.Value.PlanHash);
            Interlocked.Add(ref _xmlPlansChecked, newlyRead);

            if (current.Count == 0)
            {
                _checked.Remove(key);
            }
            else
            {
                _checked[key] = current;
            }
            _touched[key] = DateTime.UtcNow;
            DropForgottenDatabases();
        }
    }

    /// <summary>
    /// A database dropped from a server, or a server removed, would keep its cache for the life of the process. An entry not
    /// touched for a day is dropped (a database in the collector's scope is touched every five minutes). Caller holds the lock.
    /// </summary>
    private void DropForgottenDatabases()
    {
        var cutoff = DateTime.UtcNow - CacheIdleLimit;
        foreach (var stale in _touched.Where(t => t.Value < cutoff).Select(t => t.Key).ToList())
        {
            _touched.Remove(stale);
            _checked.Remove(stale);
        }
    }

    /// <summary>How long a database's cache survives with no pass over it.</summary>
    private static readonly TimeSpan CacheIdleLimit = TimeSpan.FromDays(1);

    private readonly Dictionary<(int ServerId, string Database), DateTime> _touched = new();

    /// <summary>
    /// The statements that load the plans an earlier pass already read into <c>#pm_known_plans</c>, newest first, at most
    /// <see cref="MaxXmlPlansPerDatabase"/>, in chunks under the engine's 1,000-row VALUES limit. Empty on a first pass. Values are
    /// literals (plan id, the hash text, 0 or 1, the guide name quote-doubled), because the pass runs inside <c>sp_executesql</c>.
    /// </summary>
    private string KnownPlansInsert(int serverId, string database)
    {
        List<KeyValuePair<long, CheckedPlan>> known;
        lock (_cacheLock)
        {
            if (!_checked.TryGetValue((serverId, database), out var plans))
            {
                return "";
            }

            known = plans.OrderByDescending(p => p.Key).Take(MaxXmlPlansPerDatabase).ToList();
        }

        var text = new StringBuilder();
        for (var i = 0; i < known.Count; i++)
        {
            if (i % 500 == 0)
            {
                text.Append(i == 0 ? "" : ";\n").Append("INSERT #pm_known_plans (plan_id, query_plan_hash, is_variant, guide_name) VALUES\n");
            }
            else
            {
                text.Append(",\n");
            }

            var (planId, plan) = (known[i].Key, known[i].Value);
            text.Append(CultureInfo.InvariantCulture, $"({planId},")
                .Append(plan.PlanHash.Length == 0 ? "NULL" : "'" + plan.PlanHash.Replace("'", "''", StringComparison.Ordinal) + "'")
                .Append(plan.IsVariant ? ",1," : ",0,")
                .Append(plan.GuideName is null ? "NULL" : "N'" + plan.GuideName.Replace("'", "''", StringComparison.Ordinal) + "'")
                .Append(')');
        }

        return text.Length == 0 ? "" : text.Append(";\n").ToString();
    }

    /// <summary>Ordinals follow the SELECT list of <see cref="BuildPayloadBody"/>.</summary>
    private static Row ReadRow(DbDataReader reader, string databaseName) =>
        new()
        {
            DbName = databaseName,
            PlanId = NullableLong(reader, 0),
            QueryId = NullableLong(reader, 1),
            QueryPlanHash = NullableString(reader, 2),
            PlanForcingType = NullableString(reader, 3),
            ForceFailureCount = NullableLong(reader, 4),
            LastForceFailureReason = NullableString(reader, 5),
            LastExecutionTimeUtc = NullableDateTime(reader, 6),
            LastCompileStartTimeUtc = NullableDateTime(reader, 7),
            VariantPlanIds = NullableString(reader, 8),
            PlanGuideName = NullableString(reader, 9),
            PlanGuideEnabled = reader.IsDBNull(10) ? null : Convert.ToBoolean(reader.GetValue(10), CultureInfo.InvariantCulture),
            PlanGuideScope = NullableString(reader, 11),
        };

    private static string? NullableString(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetValue(ordinal).ToString();

    private static DateTime? NullableDateTime(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetDateTime(ordinal);

    private static long? NullableLong(DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : Convert.ToInt64(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("plan_id", CollectorColumnType.BigInt),
        new CollectorColumn("query_id", CollectorColumnType.BigInt),
        new CollectorColumn("query_plan_hash", CollectorColumnType.Varchar),
        new CollectorColumn("plan_forcing_type", CollectorColumnType.Varchar),
        new CollectorColumn("force_failure_count", CollectorColumnType.BigInt),
        new CollectorColumn("last_force_failure_reason", CollectorColumnType.Varchar),
        new CollectorColumn("last_execution_time", CollectorColumnType.Timestamp),
        new CollectorColumn("last_compile_start_time", CollectorColumnType.Timestamp),
        /* V201 (#6160), appended last: the writer is a positional binary COPY. */
        new CollectorColumn("variant_plan_ids", CollectorColumnType.Varchar),
        new CollectorColumn("plan_guide_name", CollectorColumnType.Varchar),
        new CollectorColumn("plan_guide_enabled", CollectorColumnType.Boolean),
        new CollectorColumn("plan_guide_scope", CollectorColumnType.Varchar),
    };

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(writer);

        writer
            .Value(row.DbName)                      /* database_name VARCHAR */
            .Value(row.PlanId)                      /* plan_id BIGINT, NULL on the marker row */
            .Value(row.QueryId)                     /* query_id BIGINT */
            .Value(row.QueryPlanHash)               /* query_plan_hash VARCHAR */
            .Value(row.PlanForcingType)             /* plan_forcing_type VARCHAR, NULL below SQL Server 2017 */
            .Value(row.ForceFailureCount)           /* force_failure_count BIGINT */
            .Value(row.LastForceFailureReason)      /* last_force_failure_reason VARCHAR, NULL when NONE */
            .Value(row.LastExecutionTimeUtc)        /* last_execution_time TIMESTAMP, UTC */
            .Value(row.LastCompileStartTimeUtc)     /* last_compile_start_time TIMESTAMP, UTC */
            .Value(row.VariantPlanIds)              /* variant_plan_ids VARCHAR, "12,15" form, NULL when none */
            .Value(row.PlanGuideName)               /* plan_guide_name VARCHAR, NULL when no guide covers the forced query */
            .Value(row.PlanGuideEnabled)            /* plan_guide_enabled BOOLEAN */
            .Value(row.PlanGuideScope);             /* plan_guide_scope VARCHAR: OBJECT, SQL or TEMPLATE */
    }
}
