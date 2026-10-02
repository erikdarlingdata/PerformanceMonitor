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
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Per-database Query Store health from <c>sys.database_query_store_options</c> (#2319) — the fields that
/// answer "is Query Store actually working, and how close to its cap is it", which
/// <c>database_config</c>'s single <c>is_query_store_on</c> bit cannot: <c>actual_state</c> vs
/// <c>desired_state</c> (the classic silent failure is desired READ_WRITE with actual READ_ONLY after the
/// storage cap hit — <c>readonly_reason</c> says why), current vs max storage, the cleanup mode and
/// thresholds, and the runtime-stats interval length (the grain an investigation like #2312 needs to
/// interpret per-interval cost).
///
/// <para>TWO execution shapes, ONE payload — the shape <see cref="DatabaseScopedConfigCollector"/> took in
/// #3755, carried here in #3764 because this collector had the identical defect. On-prem, RDS and Managed
/// Instance — every target that honors a cross-database three-part reference — keep the enumeration shape:
/// list databases first (non-AG or primary-replica databases the login can actually enter), then EXECUTE
/// <c>[db].sys.sp_executesql</c> per database on the same connection, the idiom plan_correction shares.
/// Azure SQL DB cannot do that. A three-part reference there resolves only when the named database IS the
/// connection's current database, so on a logical-server registration — the connection sits in master and
/// every enumerated database is some other database — every fan-out item was rejected: "Reference to
/// database and/or server name in 'xedb1.sys.sp_executesql' is not supported in this version of SQL
/// Server". Because the host tolerates a failing database as a skip, the run logged SUCCESS with zero rows
/// and Collection Health read HEALTHY (that swallowing is #3754's). The Azure case had been considered
/// here, but only for the database LIST: this collector carried a master-side list query for Azure and
/// then executed each item through the on-prem idiom, under a comment asserting the idiom was
/// Azure-compatible. A registration that names a database was the accidental exception — its list held
/// only the connected database, and a three-part name that names the current database is the one form
/// Azure accepts — which is how the comment survived. It matters more here than for a config snapshot:
/// this collector exists to catch the cap-hit transition to READ_ONLY (#2319), and on an Azure
/// logical-server registration it had never observed one.</para>
///
/// <para>So on Azure SQL DB the host now connects per database (<see cref="RunsPerDatabase"/>) and
/// <see cref="BuildQuery"/> runs the payload bare against the connected database — the same override,
/// for the same reason, as database_scoped_config (#3755), query_store (#1836), plan_correction,
/// index_object_stats and procedure_stats (#1833). Both hosts test <c>RunsPerDatabase</c> BEFORE they ask
/// for an enumeration, so a definition that is per-database on a target never enumerates there;
/// <see cref="BuildEnumerationQuery"/> returns null on Azure to say the same thing from this side, and the
/// Azure list query is deleted rather than left unreachable, so it cannot be revived by a future edit that
/// flips the gate. Both shapes are built from the SINGLE <see cref="PayloadBody"/> — the on-prem
/// wrapper only quote-doubles it for nesting — so there is no second copy of the SELECT for the two paths
/// to drift on.</para>
///
/// <para>The database SET is the same on both paths, and that is held by a predicate rather than by a
/// list. On the per-database branch the HOST owns the database list: a registration that names a database
/// sweeps that database alone (#2220), and a logical-server registration lists every online database from
/// master — master included, since that list is <c>database_id > 0</c> and serves every per-database
/// collector on the target, some of whose on-prem enumerations admit master too. This collector's on-prem
/// list screens the system databases out (<c>database_id > 4 OR database_id = 2</c>: user databases plus
/// tempdb), and the payload carries the same screen on <c>DB_ID()</c>, so master's Query Store row never
/// lands beside the user databases' on Azure. Redundant on-prem, where the list already applied it;
/// load-bearing on an Azure logical-server registration, where it is the only thing that does. A database
/// that fails is skipped with a warning by the host on either path.</para>
///
/// <para>The database set is deliberately NOT filtered to <c>is_query_store_on = 1</c>:
/// <c>sys.database_query_store_options</c> returns exactly one row even when Query Store is off
/// (<c>actual_state_desc = 'OFF'</c>), so every database yields one honest row and OFF is recorded as OFF
/// — an absent row means "not collected", never "off". The collector itself gates on 2016+ via
/// <see cref="AppliesTo"/> (the view does not exist before v13). WITHIN the view, every column the
/// original nine ordinals select exists from 2016 on, and until V137 that was the whole payload and there
/// were no per-column version gates. V137 (#3796) added the two capture modes, and ONE of them is gated:
/// <c>query_capture_mode_desc</c> (<c>ALL</c> / <c>AUTO</c> / <c>CUSTOM</c> / <c>NONE</c>) shipped with the view
/// in 2016 and is always selected; <c>wait_stats_capture_mode_desc</c> (<c>ON</c> / <c>OFF</c>) arrived in
/// SQL Server 2017 (v14), so on a 2016 target the column does not exist and a body that names it fails to
/// COMPILE for the whole database — not one NULL cell, no row at all for that database. The gate is
/// <see cref="HasWaitStatsCaptureMode"/>, the <see cref="DatabaseConfigCollector"/> idiom: the SELECT
/// carries the column only where the engine has it, the reader reads the ordinal only where the SELECT
/// carried it, and the row stores NULL where the engine cannot say. Both shapes are built from the ONE
/// body, so the gate applies identically on the enumeration path and the Azure per-database path.</para>
///
/// <para><b>Why the capture modes are on this row at all (#3796).</b> The health row stored every Query
/// Store option except the one that names a plan-churn factory. Measured on one production store class,
/// the plan dimension takes ~755 k NEW distinct plans a day from 42 servers, and one database dominates
/// the <c>query_store</c> collector's per-database fan-out at 92–96 % of the run — both are exactly what
/// <c>QUERY_CAPTURE_MODE = ALL</c> produces on an ad-hoc workload, and the row could not say whether that
/// was the cause because it never asked; the fleet's other three knobs (200 plans / 21 days / 8 GB) are
/// uniform, so capture mode is the remaining explanatory variable. Stored as the <c>*_desc</c> spelling
/// verbatim, so <c>CUSTOM</c> reads as <c>CUSTOM</c> and a reader that wants the 2019+ <c>capture_policy_*</c>
/// knobs behind it knows to ask for them — this rung does not collect those. Read by nothing yet; the
/// clutter view (#3797) is the consumer, with churn × <c>ALL</c> as its "switch to AUTO" arm.</para>
///
/// <para>Hourly rather than the config family's on-load cadence, because unlike the scoped-config knobs
/// (which only change when an operator changes them) <c>actual_state</c>, <c>readonly_reason</c> and
/// <c>current_storage_size_mb</c> change BY THEMSELVES — the cap-hit transition to READ_ONLY is the whole
/// point of collecting this, and an on-load snapshot would miss it until the next reconnect.</para>
/// </summary>
public sealed class QueryStoreHealthCollector : CollectorDefinitionBase<QueryStoreHealthCollector.Row>
{
    public static QueryStoreHealthCollector Instance { get; } = new();

    private QueryStoreHealthCollector()
    {
    }

    public sealed class Row
    {
        public string DbName { get; set; } = "";
        public string? ActualState { get; set; }
        public string? DesiredState { get; set; }
        public int ReadonlyReason { get; set; }
        public long CurrentStorageMb { get; set; }
        public long MaxStorageMb { get; set; }
        public string? SizeBasedCleanupMode { get; set; }
        public long StaleQueryThresholdDays { get; set; }
        public long MaxPlansPerQuery { get; set; }
        public long IntervalLengthMinutes { get; set; }

        /// <summary>V137 (#3796): <c>query_capture_mode_desc</c> verbatim — <c>ALL</c>, <c>AUTO</c>, <c>CUSTOM</c>
        /// or <c>NONE</c>. 2016+, so always selected; NULL only where the engine returned NULL.</summary>
        public string? QueryCaptureMode { get; set; }

        /// <summary>V137 (#3796): <c>wait_stats_capture_mode_desc</c> verbatim — <c>ON</c> or <c>OFF</c>. 2017+ (v14):
        /// NULL on a 2016 target, where the column does not exist and the SELECT never names it, and that
        /// NULL means "the engine cannot say", not OFF.</summary>
        public string? WaitStatsCaptureMode { get; set; }
    }

    private const string OnPremDatabaseListQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    d.name
FROM sys.databases AS d
LEFT JOIN sys.dm_hadr_database_replica_states AS drs
    ON d.database_id = drs.database_id
    AND drs.is_local = 1
WHERE (d.database_id > 4 OR d.database_id = 2)
AND   d.database_id < 32761
AND   d.name <> N'PerformanceMonitor'
AND   d.state_desc = N'ONLINE'
AND   HAS_DBACCESS(d.name) = 1 /*a least-privilege login without per-db access raised 916 per db per cycle (#1823); the sibling per-database collectors already self-skip this way. On-prem only: from master on Azure SQL DB this returns 0 for every user database, and Azure SQL DB does not use this enumeration at all (#3764) — the host connects per database there.*/
AND
(
    drs.database_id IS NULL          /*not in any AG*/
    OR drs.is_primary_replica = 1    /*primary replica*/
)
/*EXCLUSION_FILTER*/
ORDER BY d.name
OPTION(RECOMPILE);";

    /// <summary>
    /// The per-database read — the ONE body both execution paths run (#3764). Written as ordinary T-SQL
    /// because that is what Azure SQL DB's per-database connection executes verbatim;
    /// <see cref="BuildPerItemQuery"/> quote-doubles the same string to nest it inside
    /// <c>[db].sys.sp_executesql N'...'</c> for on-prem. Both forms therefore select the identical reader
    /// ordinals in the identical order, and the shared row loop reads them the same way. One row always —
    /// the view answers for the database whether Query Store is on or off.
    ///
    /// <para>The <c>DB_ID()</c> screen is the on-prem list's system-database predicate carried onto the row
    /// read — see the class remarks. On-prem the list has already excluded every database this would
    /// exclude, so it filters nothing; on an Azure logical-server registration the host's list includes
    /// master and this is what keeps master out of the payload.</para>
    ///
    /// <para>Since V137 (#3796) the body is BUILT rather than a constant, because its last column is
    /// version-gated: <see cref="PayloadBody"/> appends <c>wait_stats_capture_mode</c> as the eleventh
    /// ordinal only where <see cref="HasWaitStatsCaptureMode"/> says the engine has the column. The
    /// ten ungated ordinals — the original nine plus <c>query_capture_mode</c>, which every 2016+ engine
    /// has — are this text, in <see cref="ReadRowsAsync"/>'s order.</para>
    /// </summary>
    private const string UngatedPayloadBodyText = @"
SELECT
    actual_state = qso.actual_state_desc,
    desired_state = qso.desired_state_desc,
    readonly_reason = qso.readonly_reason,
    current_storage_size_mb = qso.current_storage_size_mb,
    max_storage_size_mb = qso.max_storage_size_mb,
    size_based_cleanup_mode = qso.size_based_cleanup_mode_desc,
    stale_query_threshold_days = qso.stale_query_threshold_days,
    max_plans_per_query = qso.max_plans_per_query,
    interval_length_minutes = qso.interval_length_minutes,
    query_capture_mode = qso.query_capture_mode_desc";

    /// <summary>The 2017+ ordinal (#3796), appended to <see cref="UngatedPayloadBodyText"/> only where the
    /// engine has the column; see <see cref="HasWaitStatsCaptureMode"/>.</summary>
    private const string WaitStatsCaptureModeColumnText = @",
    wait_stats_capture_mode = qso.wait_stats_capture_mode_desc";

    private const string PayloadBodyTailText = @"
FROM sys.database_query_store_options AS qso
WHERE (DB_ID() > 4 OR DB_ID() = 2)
OPTION(RECOMPILE);";

    /// <summary>
    /// The per-database body for THIS target: the ten ungated ordinals, then <c>wait_stats_capture_mode</c>
    /// where the engine has it. One method, called by both <see cref="BuildQuery"/> and
    /// <see cref="BuildPerItemQuery"/>, so the two execution shapes cannot disagree about which columns
    /// a given target is asked for.
    /// </summary>
    private static string PayloadBody(CollectorTargetInfo target) =>
        UngatedPayloadBodyText
        + (HasWaitStatsCaptureMode(target) ? WaitStatsCaptureModeColumnText : string.Empty)
        + PayloadBodyTailText;

    /// <summary>
    /// V137 (#3796): whether <c>sys.database_query_store_options.wait_stats_capture_mode_desc</c> exists on
    /// the target. It arrived in SQL Server 2017 (v14); both Azure flavours always have it; 0 = version
    /// unknown = assume newest — the same reading <see cref="AppliesTo"/> gives the version, so a target
    /// the gate admits at all is asked for the column unless its version is KNOWN to be 2016. On a 2016
    /// target the SELECT does not name the column and <see cref="ReadRowsAsync"/> does not read the
    /// ordinal; the row stores NULL there, which readers must publish as "engine predates the option",
    /// never as OFF. A column that does not exist fails the batch's compile for the whole database, so
    /// this cannot be a <c>CASE</c> inside the SELECT — the reference itself is the error.
    /// </summary>
    public static bool HasWaitStatsCaptureMode(CollectorTargetInfo target)
    {
        if (target is null)
        {
            throw new ArgumentNullException(nameof(target));
        }

        return target.SqlMajorVersion == 0 || target.SqlMajorVersion >= 14 || target.IsAzureSqlDb || target.IsAzureManagedInstance;
    }

    /// <summary>
    /// Query Store shipped in SQL Server 2016 (v13); sys.database_query_store_options does not exist
    /// before it, so without this gate a pre-2016 target errors once per database per hour (review
    /// catch). The same condition QueryStoreCollector gates on, so Lite and Darling skip identically;
    /// 0 = version unknown = assume newest, and both Azure flavors always have the catalog.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) =>
        target.SqlMajorVersion == 0 || target.SqlMajorVersion >= 13 || target.IsAzureSqlDb || target.IsAzureManagedInstance;

    public override string Name => "query_store_health";

    public override string TargetTable => "query_store_health";

    /// <summary>The config snapshots' prefix is config_id/capture_time in Lite's schema; Darling mirrors it.</summary>
    public override string PrefixIdColumnName => "config_id";

    public override string PrefixTimeColumnName => "capture_time";

    /// <summary>
    /// Azure SQL DB only (#3764): the host opens one connection per database and drives
    /// <see cref="BuildQuery"/>. Every other target — box SQL Server, RDS, and Managed Instance, all of
    /// which honor the cross-database <c>[db].sys.sp_executesql</c> reference — keeps the single connection
    /// and the enumeration/per-item pair. Same override, same reasoning, as database_scoped_config (#3755),
    /// query_store (#1836) and plan_correction.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => target.IsAzureSqlDb;

    /// <summary>
    /// The Azure SQL DB per-database query (#3764): the shared payload, bare, against the CURRENT database —
    /// the one the host's per-database connection is attached to. Never reached on any other target: there
    /// the enumeration/per-item pair drives the cycle, and this throws to say so rather than silently
    /// collecting the connection's own catalog (master, when the server entry leaves its Database field
    /// blank) as if it were the whole instance.
    /// </summary>
    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        if (!context.Target.IsAzureSqlDb)
        {
            throw new NotSupportedException("query_store_health enumerates databases on this target; BuildEnumerationQuery drives the cycle.");
        }

        return new CollectorQuery(PayloadBody(context.Target));
    }

    /// <summary>
    /// On-prem / RDS / Managed Instance only: list the online user databases the login can enter (AG-aware),
    /// then collect each through <see cref="BuildPerItemQuery"/>. Null on Azure SQL DB — enumeration is not
    /// how that target is collected (<see cref="RunsPerDatabase"/>), and the per-item query this would feed
    /// is the three-part reference Azure SQL DB rejects for every database (#3764).
    /// </summary>
    public override CollectorQuery? BuildEnumerationQuery(CollectorContext context)
    {
        if (context.Target.IsAzureSqlDb)
        {
            return null;
        }

        /* #3477: the composed scope+exclusion predicate rides the same splice point the exclusion
           always used — the placeholder keeps its name because it marks WHERE the filter lands, and
           the composition (allow-list AND NOT excluded) is DatabaseScopeFilter's to state once. */
        var (exclusionClause, exclusionParameters) = DatabaseScopeFilter.BuildEnumerationPredicate(context, "d.name");
        var text = OnPremDatabaseListQueryText
            .Replace("/*EXCLUSION_FILTER*/", exclusionClause, StringComparison.Ordinal);

        return new CollectorQuery(text, exclusionParameters);
    }

    /// <summary>
    /// On-prem / RDS / Managed Instance: the SAME body <see cref="BuildQuery"/> runs on Azure, only
    /// quote-doubled and nested inside <c>[db].sys.sp_executesql</c> so one connection can reach every
    /// database. The single <c>Replace</c> is the whole difference between the two paths' SQL — there is
    /// no second copy of the payload to keep in step (the database_scoped_config / query_store /
    /// plan_correction precedent). Not Azure SQL DB compatible, and never was: a three-part reference is
    /// rejected there for every database, which is why <see cref="RunsPerDatabase"/> routes Azure around
    /// this method (#3764).
    /// </summary>
    public override CollectorQuery BuildPerItemQuery(string item, CollectorContext context)
    {
        /* Double single quotes so the body survives nesting inside [db].sys.sp_executesql N'...' */
        var escapedBody = PayloadBody(context.Target).Replace("'", "''", StringComparison.Ordinal);
        var escapedDbName = item.Replace("]", "]]", StringComparison.Ordinal);

        var text = $@"
EXECUTE [{escapedDbName}].sys.sp_executesql
    N'{escapedBody}'";

        return new CollectorQuery(text);
    }

    /// <summary>On-prem / RDS / MI: the enumerated item IS the database the per-item query ran in.</summary>
    public override ValueTask ReadItemAsync(string item, DbDataReader reader, List<Row> rows, CollectorContext context, CancellationToken cancellationToken)
        => new(ReadRowsAsync(item, reader, rows, HasWaitStatsCaptureMode(context.Target), cancellationToken));

    /// <summary>
    /// Azure SQL DB per-database path (#3764). The payload carries no database_name column — the on-prem
    /// path takes it from the enumerated item — so here it comes from
    /// <see cref="CollectorContext.CurrentDatabaseName"/>, the database the host's per-database loop
    /// connected to, which for a per-database connection IS the row's database. Same reader contract as
    /// the enumerated path: one loop serves both.
    ///
    /// <para>Throws rather than defaulting when the host left CurrentDatabaseName unset (the query_store
    /// rule): an empty database_name is not a survivable fallback for a latest-snapshot collector whose
    /// readers group and filter by database — those rows would land in the Query Store health surfaces
    /// and <c>get_query_store_health</c> under a blank database, indistinguishable from each other, and a
    /// READ_ONLY transition on one database could not be told from a healthy row on another. A wiring
    /// mistake surfaces as one loud, classified failure instead.</para>
    /// </summary>
    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var databaseName = context.CurrentDatabaseName;
        if (string.IsNullOrEmpty(databaseName))
        {
            throw new InvalidOperationException(
                "query_store_health rows read on the per-database path need CollectorContext.CurrentDatabaseName; the host must set it before reading.");
        }

        var rows = new List<Row>();
        await ReadRowsAsync(databaseName, reader, rows, HasWaitStatsCaptureMode(context.Target), cancellationToken);
        return rows;
    }

    /// <summary>
    /// The one row loop both paths share. Ordinals 0–9 are always present (the ungated body); ordinal 10 is
    /// read only when <paramref name="hasWaitStatsCaptureMode"/> says the SELECT carried it — the same
    /// decision, from the same target, that built the body. A 2016 target's reader therefore has ten
    /// columns and the row's <see cref="Row.WaitStatsCaptureMode"/> is NULL by construction rather than by
    /// an out-of-range read.
    /// </summary>
    private static async Task ReadRowsAsync(string databaseName, DbDataReader reader, List<Row> rows, bool hasWaitStatsCaptureMode, CancellationToken cancellationToken)
    {
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row
            {
                DbName = databaseName,
                ActualState = reader.IsDBNull(0) ? null : reader.GetString(0),
                DesiredState = reader.IsDBNull(1) ? null : reader.GetString(1),
                ReadonlyReason = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                CurrentStorageMb = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture),
                MaxStorageMb = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture),
                SizeBasedCleanupMode = reader.IsDBNull(5) ? null : reader.GetString(5),
                StaleQueryThresholdDays = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture),
                MaxPlansPerQuery = reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7), CultureInfo.InvariantCulture),
                IntervalLengthMinutes = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8), CultureInfo.InvariantCulture),
                QueryCaptureMode = reader.IsDBNull(9) ? null : reader.GetString(9),
                WaitStatsCaptureMode = hasWaitStatsCaptureMode && !reader.IsDBNull(10) ? reader.GetString(10) : null,
            });
        }
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("actual_state", CollectorColumnType.Varchar),
        new CollectorColumn("desired_state", CollectorColumnType.Varchar),
        new CollectorColumn("readonly_reason", CollectorColumnType.Integer),
        new CollectorColumn("current_storage_size_mb", CollectorColumnType.BigInt),
        new CollectorColumn("max_storage_size_mb", CollectorColumnType.BigInt),
        new CollectorColumn("size_based_cleanup_mode", CollectorColumnType.Varchar),
        new CollectorColumn("stale_query_threshold_days", CollectorColumnType.BigInt),
        new CollectorColumn("max_plans_per_query", CollectorColumnType.BigInt),
        new CollectorColumn("interval_length_minutes", CollectorColumnType.BigInt),
        /* V137 / Lite v64 (#3796): the two capture modes, appended LAST in this order so the positional
           writers (Darling's binary COPY, Lite's appender) and an upgraded store's ALTER agree on where
           they sit. Nullable on both stores; wait_stats_capture_mode is NULL by construction on a 2016
           engine (HasWaitStatsCaptureMode). */
        new CollectorColumn("query_capture_mode", CollectorColumnType.Varchar),
        new CollectorColumn("wait_stats_capture_mode", CollectorColumnType.Varchar),
    };

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.DbName)                    /* database_name VARCHAR */
            .Value(row.ActualState)               /* actual_state VARCHAR */
            .Value(row.DesiredState)              /* desired_state VARCHAR */
            .Value(row.ReadonlyReason)            /* readonly_reason INTEGER */
            .Value(row.CurrentStorageMb)          /* current_storage_size_mb BIGINT */
            .Value(row.MaxStorageMb)              /* max_storage_size_mb BIGINT */
            .Value(row.SizeBasedCleanupMode)      /* size_based_cleanup_mode VARCHAR */
            .Value(row.StaleQueryThresholdDays)   /* stale_query_threshold_days BIGINT */
            .Value(row.MaxPlansPerQuery)          /* max_plans_per_query BIGINT */
            .Value(row.IntervalLengthMinutes)     /* interval_length_minutes BIGINT */
            .Value(row.QueryCaptureMode)          /* query_capture_mode VARCHAR (V137, 2016+) */
            .Value(row.WaitStatsCaptureMode);     /* wait_stats_capture_mode VARCHAR (V137, 2017+; NULL where the engine predates it) */
    }
}
