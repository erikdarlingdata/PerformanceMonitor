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
/// #6873 (part of #6872): per database, the last good CHECKDB, the last full backup and the last log backup, as raw facts plus a
/// platform label. One pass over <c>sys.databases</c> with <c>DATABASEPROPERTYEX(name, 'LastGoodCheckDbTime')</c>, and ONE grouped
/// read of <c>msdb.dbo.backupset</c> over the last <see cref="BackupWindowDays"/> days (MAX(backup_finish_date) per database and type,
/// copy_only backups left out). Whether a figure is OVERDUE is not decided here: the store keeps what the server said and the
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
/// automated snapshots are not per database).</para>
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
        string LogBackupState);

    /* Result shape, both queries: name, state_desc, recovery_model_desc, create_date (UTC), replica_role, checkdb (raw datetime, local
       to the server), last_full (UTC), last_log (UTC), backups_readable (bit). */
    private static readonly string OnPremQueryText = $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET LOCK_TIMEOUT 5000;
SET NOCOUNT ON;

DECLARE
    @offset_minutes integer = DATEDIFF(MINUTE, GETDATE(), GETUTCDATE());

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
    WHERE bs.backup_finish_date >= DATEADD(DAY, -{BackupWindowDays}, GETDATE())
    AND   bs.is_copy_only = 0
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
    backups_readable = CONVERT(bit, 1)
FROM sys.databases AS d
LEFT JOIN backups AS b
  ON b.database_name = d.name
LEFT JOIN sys.dm_hadr_database_replica_states AS rs
  ON  rs.database_id = d.database_id
  AND rs.is_local = 1
WHERE d.database_id <> 2
AND   d.source_database_id IS NULL
ORDER BY
    d.name
OPTION(RECOMPILE);";

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
    backups_readable = @readable
FROM sys.databases AS d
WHERE d.name = DB_NAME()
OPTION(RECOMPILE);";

    public override string Name => "database_protection";

    public override string TargetTable => "database_protection";

    public override int? CommandTimeoutSecondsOverride => CommandTimeoutSeconds;

    /// <summary>A plain table with a time-sliced DELETE: one row per database per hour.</summary>
    public override bool StaysPlainTable => true;

    /// <summary>Darling stores and reads it; Lite has no twin (new collection is Darling-only).</summary>
    public override bool IsDarlingOnly => true;

    /* AppliesTo stays true on every SQL Server shape: Azure SQL Database, Managed Instance and RDS all have rows to show, labelled by
       platform, so none of them reads as "not collected". */

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return new CollectorQuery(context.Target.IsAzureSqlDb ? AzureSqlDbQueryText : OnPremQueryText);
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

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(context);

        var platform = PlatformFor(context.Target);
        var rows = new List<Row>();

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
                BackupStateFor(lastLog, readable, notExpected: simple)));
        }

        return rows;
    }

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
            .Value(row.LogBackupState);        /* log_backup_state VARCHAR */
    }
}
