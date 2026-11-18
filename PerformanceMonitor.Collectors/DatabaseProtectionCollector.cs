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
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #6873 (part of #6872): per database, the last good CHECKDB, the last full backup and the last log backup, as raw facts plus a
/// platform label. One pass over <c>sys.databases</c> with <c>DATABASEPROPERTYEX(name, 'LastGoodCheckDbTime')</c>, and ONE grouped
/// read of <c>msdb.dbo.backupset</c> over the last <see cref="BackupWindowDays"/> days (MAX(backup_finish_date) per database and type,
/// copy_only backups included: a copy-only full restores like any full and a copy-only log restores on top of the chain, #8771). Whether a figure is OVERDUE is not decided here: the store keeps what the server said and the
/// reader judges it against the thresholds.
///
/// <para><b>States, so a reader never has to guess.</b> <c>checkdb_state</c>: <c>checked</c> (a real time), <c>never</c>
/// (DATABASEPROPERTYEX returned 1900-01-01), <c>unknown</c> (NULL: the build cannot answer, or the database is not online).
/// <c>full_backup_state</c> and <c>log_backup_state</c>: <c>recorded</c> (a time), <c>none_in_window</c> (nothing in the last
/// <see cref="BackupWindowDays"/> days, which is NOT "never": an unpruned msdb is bounded by the window), <c>not_expected</c>
/// (a log backup of a SIMPLE-recovery database), <c>unavailable</c> (Azure SQL Database where the read of
/// <c>sys.dm_database_backups</c> was refused, for example without VIEW DATABASE PERFORMANCE STATE). A read that succeeds and finds no
/// rows, as Hyperscale does always and a General Purpose database does before its first backup, is <c>none_in_window</c>, which on the
/// platform-managed label shows no age and is never overdue.</para>
///
/// <para><b>Platform label.</b> <c>native</c> (SQL Server), <c>platform_managed</c> (Azure SQL Database and Managed Instance: the
/// platform takes the backups and owns integrity), <c>native_only</c> (Amazon RDS: backupset holds native backups only, the
/// automated snapshots are not per database, so the two backup facts are not judged. CHECKDB is: RDS does not run it for the owner and the
/// last good time is read as on SQL Server, #6902).</para>
///
/// <para><b>Azure SQL Database.</b> A target is one database, so the pass reads the current database only, with no msdb: CHECKDB
/// from <c>DATABASEPROPERTYEX(DB_NAME(), ...)</c> and backups from <c>sys.dm_database_backups</c> (needs VIEW DATABASE PERFORMANCE STATE,
/// which VIEW DATABASE STATE grants). A refused grant is caught inside the batch and reads as <c>unavailable</c>, never as an error that
/// repeats every cycle. <b>Times are stored in UTC</b>: msdb stamps the server's local clock. A backup converts with the offset it
/// recorded itself (<c>backupset.time_zone</c>), so one finished before a clock change keeps the right age, and the CHECKDB and create
/// times, which record none, convert with the offset in force now (the <c>ProcedureStatsCollector</c> pattern), so those can read an
/// hour off in the hour after a change.</para>
///
/// <para>tempdb is not listed (it cannot be backed up), nor are database snapshots. A Darling-only collector: Lite has no twin.</para>
/// </summary>
public sealed class DatabaseProtectionCollector : CollectorDefinitionBase<DatabaseProtectionCollector.Row>
{
    public static DatabaseProtectionCollector Instance { get; } = new();

    private DatabaseProtectionCollector()
    {
    }

    /// <summary>The backupset read is bounded to this many days; an older backup reads as <c>none_in_window</c>.</summary>
    public const int BackupWindowDays = 60;

    /// <summary>A few hundred rows of catalog and one grouped msdb read: a backstop, not a budget.</summary>
    public const int CommandTimeoutSeconds = 120;

    public const string PlatformNative = "native";
    public const string PlatformManaged = "platform_managed";
    public const string PlatformNativeOnly = "native_only";

    public const string CheckDbChecked = "checked";
    public const string CheckDbNever = "never";
    public const string CheckDbUnknown = "unknown";

    public const string BackupRecorded = "recorded";
    public const string BackupNoneInWindow = "none_in_window";
    public const string BackupNotExpected = "not_expected";
    public const string BackupUnavailable = "unavailable";

    /* #8865: how the SQL Server error log read for a row went. Raw facts only; what they mean for the band is decided at read time. */
    public const string ErrorLogRead = "read";
    public const string ErrorLogRefused = "refused";
    public const string ErrorLogFailed = "failed";
    public const string ErrorLogNotApplicable = "not_applicable";

    public readonly record struct Row(
        string DatabaseName,
        string? StateDesc,
        string? RecoveryModelDesc,
        DateTime? CreateDate,
        string? ReplicaRole,
        string Platform,
        DateTime? LastGoodCheckDbTime,
        string CheckDbState,
        DateTime? LastFullBackupTime,
        string FullBackupState,
        DateTime? LastLogBackupTime,
        string LogBackupState,
        string ErrorLogState,
        string? ErrorLogMessage,
        string? ErrorLogGrant,
        bool? ErrorLogBackfillComplete,
        string? ErrorLogSkippedArchives,
        int? ErrorLogUnparsedLines,
        DateTime? ErrorLogCheckDbTime,
        string? ErrorLogCheckDbOptions,
        int? ErrorLogCheckDbErrors,
        bool? ReplicaReadable);

    /* Result shape, both queries: name, state_desc, recovery_model_desc, create_date (UTC), replica_role, checkdb (raw datetime, local
       to the server), last_full (UTC), last_log (UTC), backups_readable (bit), instance_name (@@SERVERNAME, NULL on Azure SQL Database),
       replica_allow_connections (this replica's secondary-role setting, NULL outside an Availability Group). #8865: the on-premises
       batch adds a second result set, the error logs the instance lists (enumerate_ok, archive, log_date UTC, enumerate_error); a platform-managed
       target has no readable error log and builds neither the listing nor the second set. */
    /* The server's Excluded Databases list is applied to the sys.databases read (#8775). The backups CTE joins to d.name, so a name left out of the
       catalog read is left out of the batch whole. The names are bound as parameters, so the text is built per call. */
    /* #8865: the Availability Group sources are staged into #temp tables before the join, as the AG collectors do (#8889): joined live
       inside the loop over sys.databases, the AG views were measured at minutes and then error 701 on a server with many groups, and this
       batch carries the backup and boot-page facts. A platform-managed target reads no error log, so it skips the readability lookup. */
    private static string OnPremQuery(string exclusionClause, bool platformManaged) => $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 5000;
SET NOCOUNT ON;

DECLARE
    @offset_minutes integer = DATEDIFF(MINUTE, GETDATE(), GETUTCDATE());

/* An unparameterised batch runs at session scope, so a #temp table left by an earlier cycle on the same connection would still be there. */
DROP TABLE IF EXISTS #pm_protection_local_replicas;
DROP TABLE IF EXISTS #pm_protection_replicas;

SELECT /* PerformanceMonitorLite */
    database_id = CONVERT(integer, hdrs.database_id),
    replica_id = CONVERT(uniqueidentifier, hdrs.replica_id),
    is_primary_replica = CONVERT(bit, hdrs.is_primary_replica)
INTO #pm_protection_local_replicas
FROM sys.dm_hadr_database_replica_states AS hdrs
WHERE hdrs.is_local = 1
OPTION(RECOMPILE);
" + (platformManaged ? string.Empty : ReplicaReadabilityStaging) + $@"
WITH
    backups AS
(
    SELECT /* PerformanceMonitorLite */
        bs.database_name,
        last_full =
            MAX(CASE WHEN bs.type = 'D' THEN u.finish_utc END),
        last_log =
            MAX(CASE WHEN bs.type = 'L' THEN u.finish_utc END)
    FROM msdb.dbo.backupset AS bs
    /* Each backup is converted with the offset it recorded itself (time_zone, 15-minute steps, 127 or NULL = unknown), not the offset
       in force now, so a log backup taken just before a clock change does not read an hour off (#6873). The MAX is taken over the
       converted value. */
    CROSS APPLY
    (
        SELECT
            finish_utc =
                DATEADD
                (
                    MINUTE,
                    CASE
                        WHEN bs.time_zone BETWEEN -48 AND 48
                        THEN -15 * CONVERT(integer, bs.time_zone)
                        ELSE @offset_minutes
                    END,
                    bs.backup_finish_date
                )
    ) AS u
    /* Copy-only backups count: a backup tool that writes every backup as copy-only would otherwise leave every database unprotected (#8771). */
    WHERE bs.backup_finish_date >= DATEADD(DAY, -{BackupWindowDays}, GETDATE())
    AND   bs.type IN ('D', 'L')
    GROUP BY
        bs.database_name
)
SELECT
    d.name,
    d.state_desc,
    d.recovery_model_desc,
    create_date = DATEADD(MINUTE, @offset_minutes, d.create_date),
    replica_role =
        CASE
            WHEN rs.database_id IS NULL THEN NULL
            WHEN rs.is_primary_replica = 1 THEN N'PRIMARY'
            ELSE N'SECONDARY'
        END,
    last_good_checkdb_time =
        CASE
            WHEN CONVERT(datetime, DATABASEPROPERTYEX(d.name, N'LastGoodCheckDbTime')) > '19000102'
            THEN DATEADD(MINUTE, @offset_minutes, CONVERT(datetime, DATABASEPROPERTYEX(d.name, N'LastGoodCheckDbTime')))
            ELSE CONVERT(datetime, DATABASEPROPERTYEX(d.name, N'LastGoodCheckDbTime'))
        END,
    last_full = b.last_full,
    last_log = b.last_log,
    backups_readable = CONVERT(bit, 1),
    /* #8865: @@SERVERNAME is NULL on a renamed host with no sys.servers row; the fallback keeps the error-log read on rather than reading
       not_applicable. */
    instance_name = COALESCE(CONVERT(sysname, @@SERVERNAME), CONVERT(sysname, SERVERPROPERTY('ServerName'))),
    replica_allow_connections = " + (platformManaged ? "CONVERT(nvarchar(60), NULL)" : "ar.secondary_role_allow_connections_desc") + @"
FROM sys.databases AS d
LEFT JOIN backups AS b
  ON b.database_name = d.name
LEFT JOIN #pm_protection_local_replicas AS rs
  ON rs.database_id = d.database_id
" + (platformManaged ? string.Empty : @"LEFT JOIN #pm_protection_replicas AS ar
  ON ar.replica_id = rs.replica_id
") + @"WHERE d.database_id <> 2
AND   d.source_database_id IS NULL
" + DatabaseScopeFilter.VendorControlledDatabaseScreen + @"
" + exclusionClause + @"
ORDER BY
    d.name
OPTION(RECOMPILE);

DROP TABLE IF EXISTS #pm_protection_local_replicas;
DROP TABLE IF EXISTS #pm_protection_replicas;
" + (platformManaged ? string.Empty : ErrorLogListing);

    private const string ReplicaReadabilityStaging = @"
SELECT /* PerformanceMonitorLite */
    replica_id = CONVERT(uniqueidentifier, ar.replica_id),
    secondary_role_allow_connections_desc = CONVERT(nvarchar(60), ar.secondary_role_allow_connections_desc)
INTO #pm_protection_replicas
FROM sys.availability_replicas AS ar
OPTION(RECOMPILE);
";

    /* #8865: which error logs the instance holds, so ReadAsync can decide which of them the supplemental reads. The listing runs inside
       TRY/CATCH: a refused EXECUTE on sp_enumerrorlogs is a runtime error, and it must read as refused (a single (0, NULL, NULL) row),
       never as an error that fails the primary read of every database every cycle. The dates are the server's local clock, converted
       to UTC with the offset the batch already took.
       #8865: the procedure returns each date as text in month/day/year order (11/17/2026  10:16) whatever the session's language, so it
       is staged as text and converted with style 101. Inserting it into a datetime column would parse it under the login's DATEFORMAT:
       a dmy language (British, German, ...) fails the insert on a day after the 12th and swaps day and month below it. The CATCH returns
       the error number, so a list that failed for another reason is not mistaken for a refusal. A date style 101 cannot read comes back
       NULL, and the collector treats the whole list as unreadable. */
    private const string ErrorLogListing = @"
DECLARE
    @enumerate_ok bit = 1,
    @enumerate_error integer = NULL;

DECLARE
    @error_logs table
(
    archive integer NOT NULL,
    log_date nvarchar(30) NULL,
    log_size bigint NULL
);

BEGIN TRY
    /* #8865: the marker sits inside the statement, so a self-exclusion reading the statement text sees it. */
    INSERT /* PerformanceMonitorLite */
        @error_logs
    (
        archive,
        log_date,
        log_size
    )
    EXECUTE sys.sp_enumerrorlogs;
END TRY
BEGIN CATCH
    SET @enumerate_ok = 0;
    SET @enumerate_error = ERROR_NUMBER();
END CATCH;

SELECT
    enumerate_ok = @enumerate_ok,
    archive = e.archive,
    log_date = DATEADD(MINUTE, @offset_minutes, TRY_CONVERT(datetime, e.log_date, 101)),
    enumerate_error = @enumerate_error
FROM @error_logs AS e
WHERE @enumerate_ok = 1
UNION ALL
SELECT
    enumerate_ok = @enumerate_ok,
    archive = CONVERT(integer, NULL),
    log_date = CONVERT(datetime, NULL),
    enumerate_error = @enumerate_error
WHERE @enumerate_ok = 0
ORDER BY
    archive;";

    /* The backups read runs in sp_executesql inside TRY/CATCH: a refused VIEW DATABASE STATE is a compile-time permission error of the
       inner batch, which a CATCH in the outer batch catches. backup_finish_date there is already UTC. */
    private static readonly string AzureSqlDbQueryText = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 5000;
SET NOCOUNT ON;

DECLARE
    @last_full datetime2 = NULL,
    @last_log datetime2 = NULL,
    @readable bit = 0;

BEGIN TRY
    EXECUTE sys.sp_executesql
        N'SELECT /* PerformanceMonitorLite */
              @last_full = MAX(CASE WHEN backup_type = ''D'' THEN backup_finish_date END),
              @last_log = MAX(CASE WHEN backup_type = ''L'' THEN backup_finish_date END),
              @readable = 1
          FROM sys.dm_database_backups
          WHERE backup_finish_date >= DATEADD(DAY, -{BackupWindowDays}, SYSUTCDATETIME())
          OPTION(RECOMPILE);',
        N'@last_full datetime2 OUTPUT, @last_log datetime2 OUTPUT, @readable bit OUTPUT',
        @last_full = @last_full OUTPUT,
        @last_log = @last_log OUTPUT,
        @readable = @readable OUTPUT;
END TRY
BEGIN CATCH
    SET @last_full = NULL;
    SET @last_log = NULL;
    SET @readable = 0;
END CATCH;

SELECT
    d.name,
    d.state_desc,
    d.recovery_model_desc,
    create_date = d.create_date,
    replica_role = CONVERT(nvarchar(60), NULL),
    last_good_checkdb_time = CONVERT(datetime, DATABASEPROPERTYEX(DB_NAME(), N'LastGoodCheckDbTime')),
    last_full = @last_full,
    last_log = @last_log,
    backups_readable = @readable,
    instance_name = CONVERT(sysname, NULL),
    replica_allow_connections = CONVERT(nvarchar(60), NULL)
FROM sys.databases AS d
WHERE d.name = DB_NAME()
OPTION(RECOMPILE);";

    public override string Name => "database_protection";

    public override string TargetTable => "database_protection";

    public override int? CommandTimeoutSecondsOverride => CommandTimeoutSeconds;

    /// <summary>The query sets a five-second lock timeout, so a 1222 on an msdb backup history lock is a yield, not a failure.</summary>
    public override bool YieldsOnLockTimeout => true;

    /// <summary>A plain table with a time-sliced DELETE: one row per database per hour.</summary>
    public override bool StaysPlainTable => true;

    /// <summary>Darling stores and reads it; Lite has no twin (new collection is Darling-only).</summary>
    public override bool IsDarlingOnly => true;

    /* AppliesTo stays true on every SQL Server shape: Azure SQL Database, Managed Instance and RDS all have rows to show, labelled by
       platform, so none of them reads as "not collected". */

    public override IReadOnlyList<string> StateKeys { get; } = new[] { DatabaseProtectionErrorLogState.InstanceKey };

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        /* Azure SQL Database reads one database, the target itself, so there is nothing to leave out: an excluded name there is a database the
           user chose to connect to (#8775). */
        if (context.Target.IsAzureSqlDb)
        {
            return new CollectorQuery(AzureSqlDbQueryText);
        }

        var (exclusionClause, exclusionParameters) = DatabaseExclusionFilter.Build(context.ExcludedDatabases, "d.name");
        return new CollectorQuery(OnPremQuery(exclusionClause, context.Target.IsAzureManagedInstance), exclusionParameters);
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("state_desc", CollectorColumnType.Varchar),
        new CollectorColumn("recovery_model_desc", CollectorColumnType.Varchar),
        new CollectorColumn("create_date", CollectorColumnType.Timestamp),
        new CollectorColumn("replica_role", CollectorColumnType.Varchar),
        new CollectorColumn("platform", CollectorColumnType.Varchar),
        new CollectorColumn("last_good_checkdb_time", CollectorColumnType.Timestamp),
        new CollectorColumn("checkdb_state", CollectorColumnType.Varchar),
        new CollectorColumn("last_full_backup_time", CollectorColumnType.Timestamp),
        new CollectorColumn("full_backup_state", CollectorColumnType.Varchar),
        new CollectorColumn("last_log_backup_time", CollectorColumnType.Timestamp),
        new CollectorColumn("log_backup_state", CollectorColumnType.Varchar),
        new CollectorColumn("errorlog_state", CollectorColumnType.Varchar),
        new CollectorColumn("errorlog_message", CollectorColumnType.Varchar),
        new CollectorColumn("errorlog_grant", CollectorColumnType.Varchar),
        new CollectorColumn("errorlog_backfill_complete", CollectorColumnType.Boolean),
        new CollectorColumn("errorlog_skipped_archives", CollectorColumnType.Varchar),
        new CollectorColumn("errorlog_unparsed_lines", CollectorColumnType.Integer),
        new CollectorColumn("errorlog_checkdb_time", CollectorColumnType.Timestamp),
        new CollectorColumn("errorlog_checkdb_options", CollectorColumnType.Varchar),
        new CollectorColumn("errorlog_checkdb_errors", CollectorColumnType.Integer),
        new CollectorColumn("replica_readable", CollectorColumnType.Boolean),
    };

    /// <summary>The platform label for a target: Azure SQL Database and Managed Instance are platform_managed, RDS native_only.</summary>
    public static string PlatformFor(CollectorTargetInfo target)
    {
        ArgumentNullException.ThrowIfNull(target);
        if (target.IsAzureSqlDb || target.IsAzureManagedInstance)
        {
            return PlatformManaged;
        }

        return target.IsAwsRds ? PlatformNativeOnly : PlatformNative;
    }

    /// <summary>The message a row carries until the supplemental read of the error logs succeeds and replaces it.</summary>
    public const string ErrorLogDidNotComplete = "The error log read did not complete in this cycle (timeout or error).";

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(context);

        var platform = PlatformFor(context.Target);
        var rows = new List<Row>();
        string? instance = null;
        var allowConnections = new List<string?>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var recovery = reader.IsDBNull(2) ? null : reader.GetString(2);
            var checkDbRaw = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5);
            var lastFull = reader.IsDBNull(6) ? (DateTime?)null : reader.GetDateTime(6);
            var lastLog = reader.IsDBNull(7) ? (DateTime?)null : reader.GetDateTime(7);
            var readable = reader.IsDBNull(8) || Convert.ToBoolean(reader.GetValue(8), CultureInfo.InvariantCulture);

            /* 1900-01-01 is what the function returns for a database CHECKDB has never passed; NULL is a build or state that cannot answer. */
            string checkDbState;
            DateTime? checkDb;
            if (checkDbRaw is null)
            {
                checkDbState = CheckDbUnknown;
                checkDb = null;
            }
            else if (checkDbRaw.Value.Year <= 1900)
            {
                checkDbState = CheckDbNever;
                checkDb = null;
            }
            else
            {
                checkDbState = CheckDbChecked;
                checkDb = checkDbRaw;
            }

            var simple = string.Equals(recovery, "SIMPLE", StringComparison.OrdinalIgnoreCase);
            instance ??= reader.IsDBNull(9) ? null : reader.GetString(9);
            allowConnections.Add(reader.IsDBNull(10) ? null : reader.GetString(10));

            rows.Add(new Row(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                recovery,
                reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                platform,
                checkDb,
                checkDbState,
                lastFull,
                BackupStateFor(lastFull, readable, notExpected: false),
                lastLog,
                BackupStateFor(lastLog, readable, notExpected: simple),
                ErrorLogNotApplicable,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null));
        }

        if (platform != PlatformManaged && instance is not null)
        {
            await PlanErrorLogReadAsync(reader, context, rows, allowConnections, instance, cancellationToken);
        }

        return rows;
    }

    /// <summary>
    /// #8865: reads the error log list (the second result set), decides which logs the supplemental will read, and gives every row what
    /// is already known without reading a log: the check remembered from earlier cycles. Each row says <c>failed</c> until the
    /// supplemental succeeds and overwrites it, so a read that never completes is visible rather than silent.
    ///
    /// <para>The state staged here assumes the supplemental never applies: every planned archive gets an attempt, and the markers
    /// move past the archives given up on. A successful supplemental overwrites these values with what the read found. That is what
    /// bounds the backfill on a server whose current log times out every cycle: the archives are skipped after
    /// <see cref="DatabaseProtectionErrorLogState.MaxArchiveAttempts"/> tries even though none was ever read.</para>
    /// </summary>
    private static async Task PlanErrorLogReadAsync(
        DbDataReader reader, CollectorContext context, List<Row> rows, List<string?> allowConnections, string instance, CancellationToken cancellationToken)
    {
        var enumerated = new List<DatabaseProtectionErrorLogState.ArchiveInfo>();
        var enumerateOk = false;
        int? enumerateError = null;
        if (await reader.NextResultAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!Convert.ToBoolean(reader.GetValue(0), CultureInfo.InvariantCulture))
                {
                    enumerateOk = false;
                    enumerateError = reader.IsDBNull(3) ? null : reader.GetInt32(3);
                    enumerated.Clear();
                    break;
                }

                /* #8865: an archive whose date the batch could not convert has no identity the markers can use; planning around a hole
                   would mark it read, so the list as a whole counts as unreadable and moves nothing this cycle. */
                if (reader.IsDBNull(1) || reader.IsDBNull(2))
                {
                    enumerateOk = false;
                    enumerated.Clear();
                    break;
                }

                enumerateOk = true;

                /* #8865: SQL Server on Linux can keep more than 99 archives, and sp_readerrorlog on archive 100 or higher fails the whole
                   batch with a severe error through SqlClient (sqlcmd hides it). Those archives are left out as if they were not retained,
                   so the backfill ends at archive 99. */
                var archive = reader.GetInt32(1);
                if (archive <= DatabaseProtectionErrorLogState.MaxReadableArchive)
                {
                    enumerated.Add(new DatabaseProtectionErrorLogState.ArchiveInfo(archive, reader.GetDateTime(2)));
                }
            }
        }

        var state = context.State;
        var loaded = DatabaseProtectionErrorLogState.Load(state, instance);
        var plan = DatabaseProtectionErrorLogState.PlanArchives(
            loaded, enumerated, enumerateOk, DatabaseProtectionErrorLogState.ArchivesPerCycle, enumerateError);
        context.DatabaseProtectionErrorLogPlan = plan;

        var attempts = new Dictionary<DateTime, int>(plan.Before.Attempts);
        foreach (var date in DatabaseProtectionErrorLogState.PlannedDates(plan))
        {
            attempts[date] = attempts.GetValueOrDefault(date) + 1;
        }

        var skipped = plan.Before.Skipped.Union(plan.NewlySkipped).ToList();
        var markers = DatabaseProtectionErrorLogState.MarkersAfter(plan, Array.Empty<DateTime>(), enumerated);

        var pending = context.PendingState;
        DatabaseProtectionErrorLogState.StageIfChanged(pending, state, DatabaseProtectionErrorLogState.InstanceKey, instance);
        if (loaded.Reset)
        {
            /* The old instance's checks must not come back once the instance key matches again, so each is overwritten with nothing. */
            foreach (var pair in state.Where(p => p.Key.StartsWith(DatabaseProtectionErrorLogState.CheckKeyPrefix, StringComparison.Ordinal)
                                                  && p.Value.Length > 0))
            {
                pending[pair.Key] = string.Empty;
            }

            StageText(context, DatabaseProtectionErrorLogState.UnparsedKey, string.Empty);
        }

        var unparsed = DatabaseProtectionErrorLogState.RememberedUnparsed(state, loaded.Reset);

        if (StagesMarkers(plan))
        {
            StageMarkers(context, StageText, markers, attempts, skipped);
        }

        var skippedText = skipped.Count > 0 ? DatabaseProtectionErrorLogState.FormatDates(skipped) : null;
        var backfillComplete = BackfillCompleteFor(plan, markers);
        for (var i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            var stored = DatabaseProtectionErrorLogState.StoredFor(state, row.DatabaseName, loaded.Reset);
            var check = DatabaseProtectionErrorLogState.Effective(stored, null, row.CreateDate);
            rows[i] = row with
            {
                ErrorLogState = ErrorLogFailed,
                ErrorLogMessage = ErrorLogDidNotComplete,
                ErrorLogBackfillComplete = backfillComplete,
                ErrorLogSkippedArchives = skippedText,
                ErrorLogUnparsedLines = unparsed,
                ErrorLogCheckDbTime = check?.TimeUtc,
                ErrorLogCheckDbOptions = check?.Options,
                ErrorLogCheckDbErrors = check?.Errors,
                ReplicaReadable = string.Equals(row.ReplicaRole, "SECONDARY", StringComparison.Ordinal) ? ReplicaReadableFor(allowConnections[i]) : null,
            };
        }
    }

    /// <summary>
    /// #8865: what the monitoring login is told to grant when the error log is refused. Checked live: a login with only CONNECT SQL and
    /// VIEW SERVER STATE reads <c>sp_readerrorlog</c> on SQL Server 2019 and 2022, so VIEW SERVER STATE is enough on every version this
    /// collector reads. A SQL Server 2022 login holding only VIEW SERVER PERFORMANCE STATE (enough for the primary batch) is refused,
    /// with error 27219.
    /// </summary>
    public const string ErrorLogGrantStatement = "VIEW SERVER STATE";

    /* The numbers sp_readerrorlog raises when the caller may not read the log: 27219 observed live on 2019 and 2022 for a login without
       the grant; 15003 (the old "only members of ..." text), 229 (EXECUTE denied) and 300 (VIEW SERVER STATE denied) are the other
       permission errors a read of the log or a server DMV raises. Anything else is a failure, not a refusal. */
    private static bool IsPermissionError(int? number) => number is 15003 or 229 or 300 or 27219;

    /* One log is read per loop pass, each inside its own TRY/CATCH, so an archive that errors (or a refused current log) leaves the others
       readable and is reported in the first result set. The archive list is parsed with CHARINDEX rather than STRING_SPLIT, which needs
       database compatibility level 130 and master may be lower. sp_readerrorlog filters on its search string in the server, so only the
       CHECKDB lines come back. #8865: the one search string is the command, which the instance never translates. A second string on the
       English " executed by " would drop every completion line of an instance whose error log is in another language, and those lines
       must come back to be counted as unparsed. Message 8957 does not start with the command in every language (some put a word or the
       login first), so the lines kept are those with a "(" after the command, where 8957 names the database in every language; the
       English corruption advisories that only tell the reader to run DBCC CHECKDB mostly have none, and the collector ignores the rest.
       #8865: the lines are not matched to databases here. Matching every database against every line cost databases x lines (45 s for
       2,000 databases and 56,000 lines, most of the 60 s budget). The server instead keeps the newest line per head, which is at most a
       few lines per database however many days the logs hold, and the collector matches those to its rows by name. The head (binary
       collation) is the text before " executed by " on an English line: the database and the options. A line without it (another
       language) has no marker to cut at, so its head is the text up to the first ")" after the first "(" that follows the command: the
       words and login before the command, the command and the database (or a shorter part of a name that holds ")", which only merges
       such lines further). The time and counts after it, which differ on every line, are left out. A line of message 5235 ("... terminated abnormally due to error state N") has the same
       head as the good line it would shadow, so it is dropped before the reduction. */
    private const string SupplementalQueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 5000;
SET NOCOUNT ON;

DECLARE
    @offset_minutes integer = DATEDIFF(MINUTE, GETDATE(), GETUTCDATE()),
    @pending nvarchar(128) = @archives + N',',
    @archive integer,
    @comma integer;

DECLARE
    @status table
(
    archive integer NOT NULL,
    ok bit NOT NULL,
    error_number integer NULL,
    message nvarchar(4000) NULL
);

DECLARE
    @one table
(
    log_date datetime NULL,
    process_info nvarchar(50) NULL,
    text nvarchar(max) NULL
);

DECLARE
    @lines table
(
    log_date datetime NULL,
    text nvarchar(max) NULL
);

WHILE LEN(@pending) > 0
BEGIN
    SET @comma = CHARINDEX(N',', @pending);
    SET @archive = TRY_CONVERT(integer, LEFT(@pending, @comma - 1));
    SET @pending = SUBSTRING(@pending, @comma + 1, 128);

    IF @archive IS NULL
    BEGIN
        CONTINUE;
    END;

    BEGIN TRY
        DELETE @one;

        INSERT /* PerformanceMonitorLite */
            @one
        (
            log_date,
            process_info,
            text
        )
        EXECUTE sys.sp_readerrorlog @archive, 1, N'DBCC CHECKDB';

        INSERT
            @lines
        (
            log_date,
            text
        )
        SELECT
            o.log_date,
            o.text
        FROM @one AS o;

        INSERT
            @status
        (
            archive,
            ok
        )
        VALUES
        (
            @archive,
            1
        );
    END TRY
    BEGIN CATCH
        INSERT
            @status
        (
            archive,
            ok,
            error_number,
            message
        )
        VALUES
        (
            @archive,
            0,
            ERROR_NUMBER(),
            LEFT(ERROR_MESSAGE(), 4000)
        );
    END CATCH;
END;

SELECT
    s.archive,
    s.ok,
    s.error_number,
    s.message
FROM @status AS s
ORDER BY
    s.archive;

SELECT /* PerformanceMonitorLite */
    line_time_utc = DATEADD(MINUTE, @offset_minutes, r.log_date),
    line_text = r.text
FROM
(
    SELECT
        l.log_date,
        l.text,
        n =
            ROW_NUMBER() OVER
            (
                PARTITION BY
                    CONVERT
                    (
                        nvarchar(1000),
                        LEFT
                        (
                            l.text,
                            ISNULL
                            (
                                NULLIF(CHARINDEX(N' executed by ', l.text), 0),
                                ISNULL
                                (
                                    NULLIF(CHARINDEX(N')', l.text, CHARINDEX(N'(', l.text, CHARINDEX(N'DBCC CHECKDB', l.text))), 0),
                                    1000
                                )
                            )
                        )
                    ) COLLATE Latin1_General_100_BIN2
                ORDER BY
                    l.log_date DESC
            )
    FROM @lines AS l
    WHERE l.text LIKE N'%DBCC CHECKDB%(%'
    AND   l.text NOT LIKE N'% terminated abnormally due to error state %'
) AS r
WHERE r.n = 1
OPTION(RECOMPILE);";

    /// <summary>
    /// #8865: reads the logs <see cref="ReadAsync"/> planned. Null when there is no plan (a platform-managed target, or a pass that listed no
    /// databases), so those cycles cost no second round trip.
    /// </summary>
    public override CollectorQuery? BuildSupplementalQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var plan = context.DatabaseProtectionErrorLogPlan;
        if (plan is null)
        {
            return null;
        }

        /* No exclusion list here: the lines are matched to the primary rows, which already left the excluded databases out. */
        var parameters = new List<CollectorParameter>
        {
            new("@archives", DatabaseProtectionErrorLogState.ArchiveList(plan.Read), CollectorParameterType.NVarChar128),
        };
        return new CollectorQuery(SupplementalQueryText, parameters);
    }

    /// <summary>
    /// #8865: applies what the supplemental read. Every row's error log fields are rebuilt from the remembered check and the line seen this
    /// cycle, and the state ReadAsync staged on the assumption that nothing would be read is overwritten with what actually was: the
    /// markers move over the archives that read fine, and those archives lose their attempt. A value that equals what state already holds
    /// is unstaged, so a quiet cycle stages nothing.
    /// </summary>
    public override ValueTask ApplySupplementalAsync(List<Row> rows, DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(rows);
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(context);
        return ApplyErrorLogAsync(rows, reader, context, cancellationToken);
    }

    private static async ValueTask ApplyErrorLogAsync(List<Row> rows, DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var plan = context.DatabaseProtectionErrorLogPlan;
        if (plan is null)
        {
            return;
        }

        var status = new Dictionary<int, (bool Ok, int? ErrorNumber, string? Message)>();
        while (await reader.ReadAsync(cancellationToken))
        {
            status[reader.GetInt32(0)] = (
                Convert.ToBoolean(reader.GetValue(1), CultureInfo.InvariantCulture),
                reader.IsDBNull(2) ? null : reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetString(3));
        }

        /* #8865: the newest line per database, matched here by name against the rows (the server returned the newest line per head). A line is
           about one database, the longest name it can be read as. */
        var names = new HashSet<string>(rows.Select(r => r.DatabaseName), StringComparer.Ordinal);
        var newest = new Dictionary<string, (DateTime Time, string Text)>(StringComparer.Ordinal);
        var unreadable = 0;
        if (await reader.NextResultAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                if (reader.IsDBNull(0) || reader.IsDBNull(1))
                {
                    continue;
                }

                var time = reader.GetDateTime(0);
                var text = reader.GetString(1);
                var name = CheckDbErrorLogLine.DatabaseName(text, names);
                if (name is null)
                {
                    /* #8865: a completion line in another language names no database this build can find, and is counted, not dropped. */
                    if (CheckDbErrorLogLine.IsUnreadableCompletion(text, names))
                    {
                        unreadable++;
                    }
                }
                else if (!newest.TryGetValue(name, out var held) || time > held.Time)
                {
                    newest[name] = (time, text);
                }
            }
        }

        var seen = new Dictionary<string, DatabaseProtectionErrorLogState.StoredCheck>(StringComparer.Ordinal);
        var unparsed = unreadable;
        foreach (var (name, line) in newest)
        {
            if (CheckDbErrorLogLine.TryParse(line.Text, name, out var options, out var errors))
            {
                seen[name] = new DatabaseProtectionErrorLogState.StoredCheck(line.Time, options, errors);
            }
            else
            {
                unparsed++;
            }
        }

        var state = context.State;
        var reset = plan.Before.Reset;

        /* #8865: this cycle's count when it found unreadable lines; otherwise the remembered one, until a parsed line with none unreadable
           shows the log is readable after all. */
        if (unparsed == 0 && seen.Count == 0)
        {
            unparsed = DatabaseProtectionErrorLogState.RememberedUnparsed(state, reset);
        }

        Overwrite(context, DatabaseProtectionErrorLogState.UnparsedKey, unparsed > 0 ? unparsed.ToString(CultureInfo.InvariantCulture) : string.Empty);

        /* #8865: archives are identified by a minute-precision date, so a date counts as read only when every archive carrying it read fine. */
        var readOk = DatabaseProtectionErrorLogState.DatesReadWhole(plan, archive => status.TryGetValue(archive, out var s) && s.Ok);
        var markers = DatabaseProtectionErrorLogState.MarkersAfter(plan, readOk, plan.Enumerated ?? plan.Read);
        var attempts = new Dictionary<DateTime, int>(plan.Before.Attempts);
        /* #8865: a permission refusal is not an archive fault. When the current log is refused, or every archive of a date that did not
           read failed with a permission error, the date keeps the attempt count it was loaded with (ReadAsync staged +1 in advance), so a
           missing grant never uses up the archives' tries and the backlog is still there to backfill once the grant is added. A timeout
           never reaches here and counts. One attempt per date, however many archives share it. */
        var currentRefused = status.TryGetValue(0, out var currentStatus) && !currentStatus.Ok && IsPermissionError(currentStatus.ErrorNumber);
        foreach (var group in plan.Read.Where(a => a.Archive > 0).GroupBy(a => a.LogDate))
        {
            var refused = currentRefused
                || group.All(a => status.TryGetValue(a.Archive, out var s) && (s.Ok || IsPermissionError(s.ErrorNumber)));
            if (!refused)
            {
                attempts[group.Key] = attempts.GetValueOrDefault(group.Key) + 1;
            }
        }

        foreach (var date in readOk)
        {
            attempts.Remove(date);
        }

        var skipped = plan.Before.Skipped.Union(plan.NewlySkipped).ToList();
        if (StagesMarkers(plan))
        {
            StageMarkers(context, Overwrite, markers, attempts, skipped);
        }

        var (logState, message) = ErrorLogOutcome(status, plan);
        var grant = logState == ErrorLogRefused ? ErrorLogGrantStatement : null;
        var skippedText = skipped.Count > 0 ? DatabaseProtectionErrorLogState.FormatDates(skipped) : null;
        var backfillComplete = BackfillCompleteFor(plan, markers);
        for (var i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            var stored = DatabaseProtectionErrorLogState.StoredFor(state, row.DatabaseName, reset);
            DatabaseProtectionErrorLogState.StoredCheck? lineCheck = seen.TryGetValue(row.DatabaseName, out var found) ? found : null;
            var check = DatabaseProtectionErrorLogState.Effective(stored, lineCheck, row.CreateDate);
            if (check is { } effective && (stored is null || effective != stored))
            {
                Overwrite(context, DatabaseProtectionErrorLogState.CheckKeyPrefix + row.DatabaseName, effective.Format());
            }

            rows[i] = row with
            {
                ErrorLogState = logState,
                ErrorLogMessage = message,
                ErrorLogGrant = grant,
                ErrorLogBackfillComplete = backfillComplete,
                ErrorLogSkippedArchives = skippedText,
                ErrorLogUnparsedLines = unparsed,
                ErrorLogCheckDbTime = check?.TimeUtc,
                ErrorLogCheckDbOptions = check?.Options,
                ErrorLogCheckDbErrors = check?.Errors,
            };
        }
    }

    /* The state of the current log decides the row's state: it is the log every cycle reads, and an archive that fails is only a note. */
    private static (string State, string? Message) ErrorLogOutcome(
        Dictionary<int, (bool Ok, int? ErrorNumber, string? Message)> status, DatabaseProtectionErrorLogState.ErrorLogPlan plan)
    {
        if (!status.TryGetValue(0, out var current))
        {
            return (ErrorLogFailed, ErrorLogDidNotComplete);
        }

        if (!current.Ok)
        {
            return IsPermissionError(current.ErrorNumber)
                ? (ErrorLogRefused, "The login may not read the SQL Server error log. Grant " + ErrorLogGrantStatement + " to the monitoring login.")
                : (ErrorLogFailed, "The current error log could not be read: " + (current.Message ?? "unknown error") + ".");
        }

        /* #8865: a list refused for want of a grant is the documented current-log-only fallback and says nothing; one that failed for
           another reason is a fault, named so it is not silent while the older logs wait. */
        if (!plan.EnumerateOk && !IsPermissionError(plan.EnumerateError))
        {
            return (ErrorLogRead, plan.EnumerateError is { } number
                ? string.Create(CultureInfo.InvariantCulture, $"The list of older error logs could not be read (error {number}); only the current log was read, and the older ones are read once the list works.")
                : "The list of older error logs could not be read; only the current log was read, and the older ones are read once the list works.");
        }

        var failedArchives = plan.Read.Count(a => a.Archive > 0 && !(status.TryGetValue(a.Archive, out var s) && s.Ok));
        return failedArchives > 0
            ? (ErrorLogRead, string.Create(CultureInfo.InvariantCulture, $"{failedArchives} older error log(s) could not be read; they are retried in the next cycle."))
            : (ErrorLogRead, null);
    }

    /* #8865: a list that was refused or failed says nothing about which archives exist, so the markers, attempts and skips stay as they
       were: staging them would end the backfill before any archive was read. An instance reset is the exception: the old node's values
       must still be overwritten with the empty ones the reset loaded (R7). */
    private static bool StagesMarkers(DatabaseProtectionErrorLogState.ErrorLogPlan plan) => plan.EnumerateOk || plan.Before.Reset;

    /* With no readable list nothing older than the current log can be read this cycle, so the rows do not wait on the backfill (no
       `pending`) even though the markers say it is unfinished; the next cycle whose list works carries on from them. */
    private static bool BackfillCompleteFor(DatabaseProtectionErrorLogState.ErrorLogPlan plan, DatabaseProtectionErrorLogState.Markers markers)
        => !plan.EnumerateOk || markers.BackfillDone;

    private static void StageMarkers(
        CollectorContext context, Action<CollectorContext, string, string> stage, DatabaseProtectionErrorLogState.Markers markers,
        IReadOnlyDictionary<DateTime, int> attempts, IReadOnlyCollection<DateTime> skipped)
    {
        stage(context, DatabaseProtectionErrorLogState.NewestArchiveKey, DatabaseProtectionErrorLogState.FormatNewestArchive(markers));
        stage(context, DatabaseProtectionErrorLogState.BackfillBeforeKey, markers.BackfillBefore is { } before ? DatabaseProtectionErrorLogState.FormatDates(new[] { before }) : string.Empty);
        stage(context, DatabaseProtectionErrorLogState.BackfillDoneKey, markers.BackfillDone ? bool.TrueString : bool.FalseString);
        stage(context, DatabaseProtectionErrorLogState.AttemptsKey, DatabaseProtectionErrorLogState.FormatAttempts(attempts));
        stage(context, DatabaseProtectionErrorLogState.SkippedKey, DatabaseProtectionErrorLogState.FormatDates(skipped));
    }

    /* Replaces whatever ReadAsync staged for the key with the post-read value, staging it only when it differs from state. The staged entry
       has to go first: a post-read value equal to state would otherwise leave the earlier, different one in place. */
    private static void Overwrite(CollectorContext context, string key, string value)
    {
        context.PendingState.Remove(key);
        StageText(context, key, value);
    }

    /* Stages a text value only when it differs from what state holds; an empty value for a key state never held is no change. */
    private static void StageText(CollectorContext context, string key, string value)
    {
        if (value.Length == 0 && !context.State.ContainsKey(key))
        {
            return;
        }

        DatabaseProtectionErrorLogState.StageIfChanged(context.PendingState, context.State, key, value);
    }

    /* NO means the replica takes no read connections, so its error log cannot be read through one. The caller asks only for a SECONDARY row (#8865
       R8): the secondary-role setting says nothing about a replica that is primary right now. NULL: not in an Availability Group. */
    private static bool? ReplicaReadableFor(string? allowConnections) =>
        allowConnections is null ? null : !string.Equals(allowConnections, "NO", StringComparison.OrdinalIgnoreCase);

    private static string BackupStateFor(DateTime? time, bool readable, bool notExpected)
    {
        if (time is not null)
        {
            return BackupRecorded;
        }

        if (!readable)
        {
            return BackupUnavailable;
        }

        return notExpected ? BackupNotExpected : BackupNoneInWindow;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.DatabaseName)           /* database_name VARCHAR */
            .Value(row.StateDesc)              /* state_desc VARCHAR */
            .Value(row.RecoveryModelDesc)      /* recovery_model_desc VARCHAR */
            .Value(row.CreateDate)             /* create_date TIMESTAMP */
            .Value(row.ReplicaRole)            /* replica_role VARCHAR */
            .Value(row.Platform)               /* platform VARCHAR */
            .Value(row.LastGoodCheckDbTime)    /* last_good_checkdb_time TIMESTAMP */
            .Value(row.CheckDbState)           /* checkdb_state VARCHAR */
            .Value(row.LastFullBackupTime)     /* last_full_backup_time TIMESTAMP */
            .Value(row.FullBackupState)        /* full_backup_state VARCHAR */
            .Value(row.LastLogBackupTime)      /* last_log_backup_time TIMESTAMP */
            .Value(row.LogBackupState)         /* log_backup_state VARCHAR */
            .Value(row.ErrorLogState)          /* errorlog_state VARCHAR */
            .Value(row.ErrorLogMessage)        /* errorlog_message VARCHAR */
            .Value(row.ErrorLogGrant)          /* errorlog_grant VARCHAR */
            .Value(row.ErrorLogBackfillComplete) /* errorlog_backfill_complete BOOLEAN */
            .Value(row.ErrorLogSkippedArchives) /* errorlog_skipped_archives VARCHAR */
            .Value(row.ErrorLogUnparsedLines)  /* errorlog_unparsed_lines INTEGER */
            .Value(row.ErrorLogCheckDbTime)    /* errorlog_checkdb_time TIMESTAMP */
            .Value(row.ErrorLogCheckDbOptions) /* errorlog_checkdb_options VARCHAR */
            .Value(row.ErrorLogCheckDbErrors)  /* errorlog_checkdb_errors INTEGER */
            .Value(row.ReplicaReadable);       /* replica_readable BOOLEAN */
    }
}
