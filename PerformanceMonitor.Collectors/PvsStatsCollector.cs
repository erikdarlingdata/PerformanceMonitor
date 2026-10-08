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
/// Accelerated Database Recovery persistent version store (PVS) pressure, one row per database, from
/// <c>sys.dm_tran_persistent_version_store_stats</c> (#1951). ADR keeps its version store INSIDE the user
/// database, so a transaction that pins version cleanup grows the database's data files with nothing in the
/// size telemetry explaining why — this collector is the explanation, and it sits beside database_size_stats
/// for exactly that reason.
///
/// <para>SOURCES. The DMV, its column list and the one version-gated column:
/// learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-objects/sys-dm-tran-persistent-version-store-stats.
/// The size denominator follows Microsoft's own diagnostic query in
/// learn.microsoft.com/en-us/sql/relational-databases/accelerated-database-recovery-troubleshoot, and
/// <c>is_accelerated_database_recovery_on</c> comes from
/// learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-databases-transact-sql.</para>
///
/// <para>WHAT MICROSOFT'S PUBLISHED QUERY GETS WRONG, measured rather than assumed (SQL Server 2025,
/// 17.0.4045.5, #1951). That diagnostic query resolves a wall-clock time and a session id with two LEFT
/// JOINs, and NEITHER of them matches: both were reproduced against a live ADR database with a genuinely
/// open transaction and a genuinely open snapshot scan, and both returned NULL.
/// <list type="bullet">
/// <item><description><c>ON pvss.oldest_active_transaction_id = dt.transaction_id</c> joins two DIFFERENT
/// ID SPACES. With one open transaction holding 664 MB of PVS, the DMV reported
/// <c>oldest_active_transaction_id = 66881</c> while that transaction's
/// <c>sys.dm_tran_database_transactions.transaction_id</c> was 5694862. The PVS column is an internal
/// sequence number in the same magnitude band as <c>min_transaction_timestamp</c> (47886 in the same
/// reading), not an engine transaction id.</description></item>
/// <item><description><c>ON pvss.min_transaction_timestamp = asdt.transaction_sequence_num</c> shares the
/// right ID space but compares the wrong values: with one open SNAPSHOT transaction the DMV reported
/// <c>min_transaction_timestamp = 62903</c> against that transaction's
/// <c>transaction_sequence_num = 62919</c>. <c>min_transaction_timestamp</c> is a cleanup LOW-WATER MARK,
/// so equality holds only by coincidence.</description></item>
/// </list>
/// So the collector does NOT carry an <c>oldest_active_transaction_begin_time</c> or a snapshot
/// session/elapsed pair. Shipping them would mean three columns that are structurally always NULL while
/// a grid header promises "the transaction holding cleanup back" — worse than absent, because an
/// operator would read the blank as "nothing is holding it." Repairing the predicate would mean
/// inventing an inequality join MS does not document, against an instance-wide DMV, which is folklore
/// with a performance cost. What IS collected supports MS's documented diagnostic path without any
/// join: <c>current_aborted_transaction_count</c>, <c>oldest_active_transaction_id</c> against
/// <c>oldest_aborted_transaction_id</c> (same DMV, same ID space, and MS's own instruction is to compare
/// them to each other), and the six <c>pvs_off_row_page_skipped_*</c> counters. Three of those are how the
/// troubleshooting guide attributes a stuck cleanup: <c>..._low_water_mark</c> to a long-running query on a
/// secondary replica, <c>..._min_useful_xts</c> to a long snapshot scan, and <c>..._oldest_aborted_xdesid</c>
/// to space still held by aborted transactions. <b>The snapshot counter really is
/// <c>..._min_useful_xts</c></b> — MS: "shows the number of pages skipped during cleanup due to a long
/// snapshot scan" — and NOT the similarly-named <c>..._oldest_snapshot</c>, which the troubleshooting flow
/// never reads; the grids label the former "Skipped: Snapshot" for that reason, and it is not a mis-binding.
/// The other three are collected but not surfaced, one of them because MS says
/// <c>..._transaction_not_cleaned</c> "can be ignored when troubleshooting large PVS issues".</para>
///
/// <para>UNITS, and the caveat that outranks them. The DMV reports KILOBYTES; the size columns here are
/// converted to MB to match every other size in the FinOps surface (database_size_stats' total_size_mb /
/// used_size_mb) — a grid that mixes KB and MB is a usability bug. The caveat that matters more: MS documents
/// <c>persistent_version_store_size_kb</c> as "the size of the OFF-ROW versions in PVS ... does not include the
/// size of row versions stored in-row." ADR stores versions both ways, so this number STRUCTURALLY understates
/// total version space and a workload whose versions fit in-row can read near zero while still paying ADR cost.
/// Every surface presenting it says "off-row".</para>
///
/// <para>VERSION GATE. The DMV is SQL Server 2019 (15.x)+, so <see cref="AppliesTo"/> gates on major version 15
/// with the codebase's standard shape — unknown (0) assumes newest, and Azure SQL DB / Managed Instance are
/// never version-gated because they report a low ProductMajorVersion yet ship the DMV (the same condition
/// <see cref="QueryStoreCollector"/> uses for its 2016 floor). Below 2019 the object does not exist and the
/// collector never runs, so the repo's 2016 minimum is honored without a per-cycle probe.</para>
///
/// <para>ONE version-gated COLUMN. <c>pvs_off_row_page_skipped_oldest_aborted_xdesid</c> is documented
/// "Applies to: SQL Server 2022 (16.x) and later versions"; it is the only column in the DMV carrying a
/// per-column availability note. On 2019 it is selected as a typed NULL rather than being omitted, so the
/// payload shape is IDENTICAL on every version — the storage schema never forks, and a fleet mixing 2019 and
/// 2022 writes one table. Columns are enumerated explicitly and never <c>SELECT *</c>, which MS calls out
/// directly: "Don't use the syntax SELECT * FROM dynamic_management_view_name in production code because the
/// number of columns returned might change and break your application."</para>
///
/// <para>AZURE SQL DB is a SEPARATE query text, not a skip: MS documents ADR as ALWAYS ENABLED there, which
/// makes PVS pressure more relevant on Azure than on box, not less. Three things force the fork. The DMV is
/// effectively database-scoped on SQL DB (MS's own Azure variant filters <c>database_id IN (DB_ID(), 2)</c>);
/// the on-prem size denominator <c>sys.master_files</c> is not documented for SQL DB — MS ships two variants of
/// the same diagnostic query for precisely this reason, so the Azure text uses <c>sys.database_files</c>; and
/// the ADR flag CANNOT be joined by id there. That last one is the sharp edge. MS documents that on Azure SQL
/// Database <c>sys.databases.database_id</c> is unique within the LOGICAL SERVER while <c>DB_ID()</c> and the
/// <c>database_id</c> of every other system view — including this DMV — are unique only within the database or
/// elastic pool ("DB_ID may not return the same value as the database_id column in sys.databases"). An
/// <c>ON d.database_id = pvss.database_id</c> inner join therefore drops the row on Azure and the collector
/// silently writes NOTHING, per database, with no error — which is why MS's own Azure variant never joins
/// <c>sys.databases</c> at all. The flag is read by NAME instead (<c>d.name = DB_NAME()</c>, deterministic
/// because in a user database that view returns only the current database and master), and the join is gone.
/// The Azure text scopes to <c>DB_ID()</c> only and deliberately drops MS's tempdb row: on SQL DB tempdb is not
/// the customer's to size, and the host already runs this collector once per monitored database there. Managed
/// Instance takes the on-prem path — it is a full instance with instance-wide <c>sys.databases</c> and
/// <c>sys.master_files</c>, and the id-space split above is SQL DB only.</para>
///
/// <para>ONE ROW PER DATABASE, guaranteed. Dropping the two unsound joins removes the row-multiplication
/// hazard they carried with them (MS's snapshot join uses <c>OR</c> across two timestamps and returns two
/// rows for a database matching both — harmless in a human's result set, corrupting in a collector, where a
/// doubled row doubles every downstream aggregate). What remains is one <c>OUTER APPLY</c> for the size
/// denominator, which is a SCALAR aggregate over <c>sys.master_files</c> with no GROUP BY and therefore
/// returns exactly one row — a NULL one when nothing matches. That means CROSS APPLY would behave
/// identically here (measured, not assumed from the usual OUTER-vs-CROSS rule), so OUTER is the honest
/// default rather than a load-bearing choice.</para>
///
/// <para>NULL cleaner END times are SEMANTIC, not missing: MS documents "if start time has value but the end
/// time doesn't, it means PVS cleanup is ongoing on this database." They are stored as read — nothing
/// coalesces them to a sentinel, because that shape is the single best "cleanup is stuck" signal.</para>
///
/// <para>The transaction-ID columns (<c>oldest_active_transaction_id</c>, <c>oldest_aborted_transaction_id</c>,
/// <c>min_transaction_timestamp</c>, <c>secondary_low_water_mark</c>) are internal identifiers, NOT wall-clock
/// values and NOT durations — charting them raw produces garbage, and no surface here does. They are collected
/// because MS's troubleshooting flow reads them AGAINST EACH OTHER: "if oldest_aborted_transaction_id is much
/// lower than oldest_active_transaction_id ... there's likely an old aborted transaction preventing PVS
/// cleanup." That comparison needs no join and is the reason these survive while the joined columns did not.</para>
///
/// <para>NOT gated on <c>is_accelerated_database_recovery_on = 1</c>. MS's own PVS-filegroup-move procedure
/// says to disable ADR and then watch this DMV until the size reaches zero, so rows outlive the setting — and
/// a draining PVS is exactly when an operator is watching. The flag is collected ALONGSIDE the size as
/// inventory instead (the issue asks for it explicitly: knowing ADR is ON is context worth having at zero
/// pressure).</para>
///
/// <para>The <c>sys.databases</c> join is INNER, and that is a row-set decision rather than an accident. The
/// DMV returns rows for hidden internal databases a user connection cannot resolve — measured on SQL Server
/// 2025: ids 32761 (<c>model_msdb</c>), 32762 (<c>model_replicatedmaster</c>) and 32767, the last with no
/// name at all. They are permanently zero and cannot carry user PVS. Worse, under a LEFT JOIN their presence
/// depended on CONFIGURATION: the exclusion filter compares <c>d.name</c>, and <c>NULL NOT IN (...)</c> is
/// UNKNOWN, so those rows vanished the moment an operator excluded any database and reappeared when the list
/// was cleared. An inner join makes the row set deterministic — every database the catalog can name, always,
/// exclusions or not.</para>
///
/// <para>PERMISSIONS: <c>VIEW SERVER STATE</c> on 2019; 2022+ documents the granular
/// <c>VIEW SERVER PERFORMANCE STATE</c>, which <c>VIEW SERVER STATE</c> IMPLIES (that direction, per GRANT
/// Server Permissions) — the product already requires the latter, so no new grant either way.
/// On Azure SQL DB's Basic/S0/S1 objectives and in elastic pools, membership in
/// <c>##MS_ServerPerformanceStateReader##</c> is required rather than a plain grant.</para>
/// </summary>
public sealed class PvsStatsCollector : CollectorDefinitionBase<PvsStatsCollector.Row>
{
    public static PvsStatsCollector Instance { get; } = new();

    private PvsStatsCollector()
    {
    }

    public readonly record struct Row(
        string? DatabaseName,
        int DatabaseId,
        bool? IsAcceleratedDatabaseRecoveryOn,
        short? PvsFilegroupId,
        decimal? PersistentVersionStoreSizeMb,
        decimal? OnlineIndexVersionStoreSizeMb,
        decimal? DatabaseDataSizeMb,
        long? CurrentAbortedTransactionCount,
        long? OldestActiveTransactionId,
        long? OldestAbortedTransactionId,
        long? MinTransactionTimestamp,
        long? OnlineIndexMinTransactionTimestamp,
        long? SecondaryLowWaterMark,
        DateTime? OffrowVersionCleanerStartTime,
        DateTime? OffrowVersionCleanerEndTime,
        DateTime? AbortedVersionCleanerStartTime,
        DateTime? AbortedVersionCleanerEndTime,
        long? PvsOffRowPageSkippedLowWaterMark,
        long? PvsOffRowPageSkippedTransactionNotCleaned,
        long? PvsOffRowPageSkippedOldestActiveXdesid,
        long? PvsOffRowPageSkippedMinUsefulXts,
        long? PvsOffRowPageSkippedOldestSnapshot,
        long? PvsOffRowPageSkippedOldestAbortedXdesid);

    /// <summary>
    /// Selected as the real column on 2022+ and as a typed NULL below it, spliced into both query texts at
    /// <c>/*SKIPPED_OLDEST_ABORTED*/</c> so the payload shape never varies by version.
    /// </summary>
    private const string SkippedOldestAbortedColumn =
        "pvss.pvs_off_row_page_skipped_oldest_aborted_xdesid";

    private const string SkippedOldestAbortedNull =
        "CONVERT(bigint, NULL)";

    private const string OnPremQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    database_name =
        d.name,
    database_id =
        pvss.database_id,
    is_accelerated_database_recovery_on =
        d.is_accelerated_database_recovery_on,
    pvs_filegroup_id =
        pvss.pvs_filegroup_id,
    persistent_version_store_size_mb =
        CONVERT(decimal(19,2), pvss.persistent_version_store_size_kb / 1024.0),
    online_index_version_store_size_mb =
        CONVERT(decimal(19,2), pvss.online_index_version_store_size_kb / 1024.0),
    database_data_size_mb =
        CONVERT(decimal(19,2), df.total_db_size_kb / 1024.0),
    current_aborted_transaction_count =
        pvss.current_aborted_transaction_count,
    oldest_active_transaction_id =
        pvss.oldest_active_transaction_id,
    oldest_aborted_transaction_id =
        pvss.oldest_aborted_transaction_id,
    min_transaction_timestamp =
        pvss.min_transaction_timestamp,
    online_index_min_transaction_timestamp =
        pvss.online_index_min_transaction_timestamp,
    secondary_low_water_mark =
        pvss.secondary_low_water_mark,
    offrow_version_cleaner_start_time =
        pvss.offrow_version_cleaner_start_time,
    offrow_version_cleaner_end_time =
        pvss.offrow_version_cleaner_end_time,
    aborted_version_cleaner_start_time =
        pvss.aborted_version_cleaner_start_time,
    aborted_version_cleaner_end_time =
        pvss.aborted_version_cleaner_end_time,
    pvs_off_row_page_skipped_low_water_mark =
        pvss.pvs_off_row_page_skipped_low_water_mark,
    pvs_off_row_page_skipped_transaction_not_cleaned =
        pvss.pvs_off_row_page_skipped_transaction_not_cleaned,
    pvs_off_row_page_skipped_oldest_active_xdesid =
        pvss.pvs_off_row_page_skipped_oldest_active_xdesid,
    pvs_off_row_page_skipped_min_useful_xts =
        pvss.pvs_off_row_page_skipped_min_useful_xts,
    pvs_off_row_page_skipped_oldest_snapshot =
        pvss.pvs_off_row_page_skipped_oldest_snapshot,
    pvs_off_row_page_skipped_oldest_aborted_xdesid =
        /*SKIPPED_OLDEST_ABORTED*/
FROM sys.dm_tran_persistent_version_store_stats AS pvss
JOIN sys.databases AS d
  ON d.database_id = pvss.database_id
OUTER APPLY
(
    SELECT
        total_db_size_kb =
            SUM(mf.size * 8.0)
    FROM sys.master_files AS mf
    WHERE mf.database_id = pvss.database_id
    AND   mf.state = 0 /*ONLINE*/
    AND   mf.type = 0 /*ROWS*/
) AS df
WHERE 1 = 1
/*EXCLUSION_FILTER*/
ORDER BY
    pvss.persistent_version_store_size_kb DESC
OPTION(RECOMPILE);";

    private const string AzureSqlDbQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    database_name =
        DB_NAME(),
    database_id =
        pvss.database_id,
    is_accelerated_database_recovery_on =
        (
            SELECT
                d.is_accelerated_database_recovery_on
            FROM sys.databases AS d
            WHERE d.name = DB_NAME()
        ),
    pvs_filegroup_id =
        pvss.pvs_filegroup_id,
    persistent_version_store_size_mb =
        CONVERT(decimal(19,2), pvss.persistent_version_store_size_kb / 1024.0),
    online_index_version_store_size_mb =
        CONVERT(decimal(19,2), pvss.online_index_version_store_size_kb / 1024.0),
    database_data_size_mb =
        CONVERT(decimal(19,2), df.total_db_size_kb / 1024.0),
    current_aborted_transaction_count =
        pvss.current_aborted_transaction_count,
    oldest_active_transaction_id =
        pvss.oldest_active_transaction_id,
    oldest_aborted_transaction_id =
        pvss.oldest_aborted_transaction_id,
    min_transaction_timestamp =
        pvss.min_transaction_timestamp,
    online_index_min_transaction_timestamp =
        pvss.online_index_min_transaction_timestamp,
    secondary_low_water_mark =
        pvss.secondary_low_water_mark,
    offrow_version_cleaner_start_time =
        pvss.offrow_version_cleaner_start_time,
    offrow_version_cleaner_end_time =
        pvss.offrow_version_cleaner_end_time,
    aborted_version_cleaner_start_time =
        pvss.aborted_version_cleaner_start_time,
    aborted_version_cleaner_end_time =
        pvss.aborted_version_cleaner_end_time,
    pvs_off_row_page_skipped_low_water_mark =
        pvss.pvs_off_row_page_skipped_low_water_mark,
    pvs_off_row_page_skipped_transaction_not_cleaned =
        pvss.pvs_off_row_page_skipped_transaction_not_cleaned,
    pvs_off_row_page_skipped_oldest_active_xdesid =
        pvss.pvs_off_row_page_skipped_oldest_active_xdesid,
    pvs_off_row_page_skipped_min_useful_xts =
        pvss.pvs_off_row_page_skipped_min_useful_xts,
    pvs_off_row_page_skipped_oldest_snapshot =
        pvss.pvs_off_row_page_skipped_oldest_snapshot,
    pvs_off_row_page_skipped_oldest_aborted_xdesid =
        /*SKIPPED_OLDEST_ABORTED*/
FROM sys.dm_tran_persistent_version_store_stats AS pvss
OUTER APPLY
(
    SELECT
        total_db_size_kb =
            SUM(dbf.size * 8.0)
    FROM sys.database_files AS dbf
    WHERE dbf.state = 0 /*ONLINE*/
    AND   dbf.type = 0 /*ROWS*/
) AS df
WHERE pvss.database_id = DB_ID()
OPTION(RECOMPILE);";

    public override string Name => "pvs_stats";

    public override string TargetTable => "pvs_stats";

    /// <summary>
    /// SQL Server 2019 (15.x) introduced <c>sys.dm_tran_persistent_version_store_stats</c>; on a 2016/2017
    /// box the object does not exist, so the collector is gated off entirely rather than failing a cycle.
    /// Unknown version (0) assumes newest and Azure SQL DB / Managed Instance are never version-gated —
    /// the shape query_store and database_config already use.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) =>
        target.SqlMajorVersion == 0
        || target.SqlMajorVersion >= 15
        || target.IsAzureSqlDb
        || target.IsAzureManagedInstance;

    /// <summary>
    /// Never per-database. On the on-prem/MI path the DMV is instance-wide, so one query covers the whole
    /// server; on Azure SQL DB the query is database-scoped and reads through the connection the host
    /// already holds. Enumerating would connect to <c>master</c> for nothing — the #1631 trap
    /// database_size_stats documents.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        /* MS documents pvs_off_row_page_skipped_oldest_aborted_xdesid as 2022+; every other column spans
           the whole 2019..2025/Azure range. Azure SQL DB and MI always have it. */
        var target = context.Target;
        bool has2022Column =
            target.SqlMajorVersion == 0
            || target.SqlMajorVersion >= 16
            || target.IsAzureSqlDb
            || target.IsAzureManagedInstance;

        string skippedOldestAborted = has2022Column
            ? SkippedOldestAbortedColumn
            : SkippedOldestAbortedNull;

        if (target.IsAzureSqlDb)
        {
            return new CollectorQuery(
                AzureSqlDbQueryText.Replace("/*SKIPPED_OLDEST_ABORTED*/", skippedOldestAborted, StringComparison.Ordinal));
        }

        var (exclusionClause, exclusionParameters) =
            DatabaseExclusionFilter.Build(context.ExcludedDatabases, "d.name");

        string text = OnPremQueryText
            .Replace("/*SKIPPED_OLDEST_ABORTED*/", skippedOldestAborted, StringComparison.Ordinal)
            .Replace("/*EXCLUSION_FILTER*/", exclusionClause, StringComparison.Ordinal);

        return new CollectorQuery(text, exclusionParameters);
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("database_id", CollectorColumnType.Integer),
        new CollectorColumn("is_accelerated_database_recovery_on", CollectorColumnType.Boolean),
        new CollectorColumn("pvs_filegroup_id", CollectorColumnType.SmallInt),
        new CollectorColumn("persistent_version_store_size_mb", CollectorColumnType.Decimal, 19, 2),
        new CollectorColumn("online_index_version_store_size_mb", CollectorColumnType.Decimal, 19, 2),
        new CollectorColumn("database_data_size_mb", CollectorColumnType.Decimal, 19, 2),
        new CollectorColumn("current_aborted_transaction_count", CollectorColumnType.BigInt),
        new CollectorColumn("oldest_active_transaction_id", CollectorColumnType.BigInt),
        new CollectorColumn("oldest_aborted_transaction_id", CollectorColumnType.BigInt),
        new CollectorColumn("min_transaction_timestamp", CollectorColumnType.BigInt),
        new CollectorColumn("online_index_min_transaction_timestamp", CollectorColumnType.BigInt),
        new CollectorColumn("secondary_low_water_mark", CollectorColumnType.BigInt),
        new CollectorColumn("offrow_version_cleaner_start_time", CollectorColumnType.Timestamp),
        new CollectorColumn("offrow_version_cleaner_end_time", CollectorColumnType.Timestamp),
        new CollectorColumn("aborted_version_cleaner_start_time", CollectorColumnType.Timestamp),
        new CollectorColumn("aborted_version_cleaner_end_time", CollectorColumnType.Timestamp),
        new CollectorColumn("pvs_off_row_page_skipped_low_water_mark", CollectorColumnType.BigInt),
        new CollectorColumn("pvs_off_row_page_skipped_transaction_not_cleaned", CollectorColumnType.BigInt),
        new CollectorColumn("pvs_off_row_page_skipped_oldest_active_xdesid", CollectorColumnType.BigInt),
        new CollectorColumn("pvs_off_row_page_skipped_min_useful_xts", CollectorColumnType.BigInt),
        new CollectorColumn("pvs_off_row_page_skipped_oldest_snapshot", CollectorColumnType.BigInt),
        new CollectorColumn("pvs_off_row_page_skipped_oldest_aborted_xdesid", CollectorColumnType.BigInt),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                DatabaseId: Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
                IsAcceleratedDatabaseRecoveryOn: reader.IsDBNull(2) ? null : reader.GetBoolean(2),
                PvsFilegroupId: reader.IsDBNull(3) ? null : Convert.ToInt16(reader.GetValue(3), CultureInfo.InvariantCulture),
                PersistentVersionStoreSizeMb: reader.IsDBNull(4) ? null : reader.GetDecimal(4),
                OnlineIndexVersionStoreSizeMb: reader.IsDBNull(5) ? null : reader.GetDecimal(5),
                DatabaseDataSizeMb: reader.IsDBNull(6) ? null : reader.GetDecimal(6),
                CurrentAbortedTransactionCount: reader.IsDBNull(7) ? null : Convert.ToInt64(reader.GetValue(7), CultureInfo.InvariantCulture),
                OldestActiveTransactionId: reader.IsDBNull(8) ? null : Convert.ToInt64(reader.GetValue(8), CultureInfo.InvariantCulture),
                OldestAbortedTransactionId: reader.IsDBNull(9) ? null : Convert.ToInt64(reader.GetValue(9), CultureInfo.InvariantCulture),
                MinTransactionTimestamp: reader.IsDBNull(10) ? null : Convert.ToInt64(reader.GetValue(10), CultureInfo.InvariantCulture),
                OnlineIndexMinTransactionTimestamp: reader.IsDBNull(11) ? null : Convert.ToInt64(reader.GetValue(11), CultureInfo.InvariantCulture),
                SecondaryLowWaterMark: reader.IsDBNull(12) ? null : Convert.ToInt64(reader.GetValue(12), CultureInfo.InvariantCulture),
                OffrowVersionCleanerStartTime: reader.IsDBNull(13) ? null : reader.GetDateTime(13),
                OffrowVersionCleanerEndTime: reader.IsDBNull(14) ? null : reader.GetDateTime(14),
                AbortedVersionCleanerStartTime: reader.IsDBNull(15) ? null : reader.GetDateTime(15),
                AbortedVersionCleanerEndTime: reader.IsDBNull(16) ? null : reader.GetDateTime(16),
                PvsOffRowPageSkippedLowWaterMark: reader.IsDBNull(17) ? null : Convert.ToInt64(reader.GetValue(17), CultureInfo.InvariantCulture),
                PvsOffRowPageSkippedTransactionNotCleaned: reader.IsDBNull(18) ? null : Convert.ToInt64(reader.GetValue(18), CultureInfo.InvariantCulture),
                PvsOffRowPageSkippedOldestActiveXdesid: reader.IsDBNull(19) ? null : Convert.ToInt64(reader.GetValue(19), CultureInfo.InvariantCulture),
                PvsOffRowPageSkippedMinUsefulXts: reader.IsDBNull(20) ? null : Convert.ToInt64(reader.GetValue(20), CultureInfo.InvariantCulture),
                PvsOffRowPageSkippedOldestSnapshot: reader.IsDBNull(21) ? null : Convert.ToInt64(reader.GetValue(21), CultureInfo.InvariantCulture),
                PvsOffRowPageSkippedOldestAbortedXdesid: reader.IsDBNull(22) ? null : Convert.ToInt64(reader.GetValue(22), CultureInfo.InvariantCulture)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.DatabaseName)                                 /* database_name VARCHAR */
            .Value(row.DatabaseId)                                   /* database_id INTEGER */
            .Value(row.IsAcceleratedDatabaseRecoveryOn)              /* is_accelerated_database_recovery_on BOOLEAN */
            .Value(row.PvsFilegroupId)                               /* pvs_filegroup_id SMALLINT */
            .Value(row.PersistentVersionStoreSizeMb)                 /* persistent_version_store_size_mb DECIMAL(19,2) */
            .Value(row.OnlineIndexVersionStoreSizeMb)                /* online_index_version_store_size_mb DECIMAL(19,2) */
            .Value(row.DatabaseDataSizeMb)                           /* database_data_size_mb DECIMAL(19,2) */
            .Value(row.CurrentAbortedTransactionCount)               /* current_aborted_transaction_count BIGINT */
            .Value(row.OldestActiveTransactionId)                    /* oldest_active_transaction_id BIGINT */
            .Value(row.OldestAbortedTransactionId)                   /* oldest_aborted_transaction_id BIGINT */
            .Value(row.MinTransactionTimestamp)                      /* min_transaction_timestamp BIGINT */
            .Value(row.OnlineIndexMinTransactionTimestamp)           /* online_index_min_transaction_timestamp BIGINT */
            .Value(row.SecondaryLowWaterMark)                        /* secondary_low_water_mark BIGINT */
            .Value(row.OffrowVersionCleanerStartTime)                /* offrow_version_cleaner_start_time TIMESTAMP */
            .Value(row.OffrowVersionCleanerEndTime)                  /* offrow_version_cleaner_end_time TIMESTAMP */
            .Value(row.AbortedVersionCleanerStartTime)               /* aborted_version_cleaner_start_time TIMESTAMP */
            .Value(row.AbortedVersionCleanerEndTime)                 /* aborted_version_cleaner_end_time TIMESTAMP */
            .Value(row.PvsOffRowPageSkippedLowWaterMark)             /* pvs_off_row_page_skipped_low_water_mark BIGINT */
            .Value(row.PvsOffRowPageSkippedTransactionNotCleaned)    /* pvs_off_row_page_skipped_transaction_not_cleaned BIGINT */
            .Value(row.PvsOffRowPageSkippedOldestActiveXdesid)       /* pvs_off_row_page_skipped_oldest_active_xdesid BIGINT */
            .Value(row.PvsOffRowPageSkippedMinUsefulXts)             /* pvs_off_row_page_skipped_min_useful_xts BIGINT */
            .Value(row.PvsOffRowPageSkippedOldestSnapshot)           /* pvs_off_row_page_skipped_oldest_snapshot BIGINT */
            .Value(row.PvsOffRowPageSkippedOldestAbortedXdesid);     /* pvs_off_row_page_skipped_oldest_aborted_xdesid BIGINT */
    }
}
