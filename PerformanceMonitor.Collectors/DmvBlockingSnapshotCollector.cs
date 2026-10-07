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
/// Always-on DMV blocking snapshot (blocker → blocked pair rows from sys.dm_os_waiting_tasks +
/// sys.dm_exec_*), the BPR-independent fallback for when the blocked_process_report XE captured
/// nothing. Extracted verbatim from Lite's RemoteCollectorService.DmvBlockingSnapshot.cs,
/// including the layered minimum-wait floors (LCK 2s / PAGELATCH 0.5s / PAGEIOLATCH 1s /
/// RESOURCE_SEMAPHORE 5s — kept in lockstep with install/56) and the SYNTHETIC NEGATIVE
/// monitor_loop (seconds since 2020-01-01, negated) so a DMV episode can never collide with a
/// real, non-negative BPR monitorLoop.
/// </summary>
public sealed class DmvBlockingSnapshotCollector : CollectorDefinitionBase<DmvBlockingSnapshotCollector.Row>
{
    public static DmvBlockingSnapshotCollector Instance { get; } = new();

    private DmvBlockingSnapshotCollector()
    {
    }

    public readonly record struct Row(
        string? DatabaseName,
        int BlockedSpid,
        int BlockedEcid,
        DateTime? BlockedLastTranStarted,
        int BlockingSpid,
        int BlockingEcid,
        DateTime? BlockingLastTranStarted,
        long WaitTimeMs,
        string? LockMode,
        string? BlockingStatus,
        string? ContentiousObject,
        string? BlockedSqlText,
        string? BlockingSqlText,
        string? BlockedLoginName,
        string? BlockedHostName,
        string? BlockedClientApp,
        string? BlockingLoginName,
        string? BlockingHostName,
        string? BlockingClientApp);

    private static readonly DateTime s_monitorLoopEpoch = new(2020, 1, 1);

    /// <summary>
    /// Synthetic monitor_loop, NEGATIVE so it can never collide with a real (non-negative) BPR
    /// monitorLoop. One value per cycle, distinct per second (collection cadence is >= 1 minute).
    /// </summary>
    public static int SyntheticMonitorLoop(DateTime collectionTime)
        => -(int)(collectionTime - s_monitorLoopEpoch).TotalSeconds;

    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    database_name = DB_NAME(der_b.database_id),
    blocked_spid = CONVERT(integer, wt.session_id),
    blocked_ecid = CONVERT(integer, wt.exec_context_id),
    blocked_last_tran_started = tat_b.transaction_begin_time,
    blocking_spid = CONVERT(integer, wt.blocking_session_id),
    blocking_ecid = CONVERT(integer, ISNULL(wt.blocking_exec_context_id, 0)),
    blocking_last_tran_started = tat_k.transaction_begin_time,
    wait_time_ms = wt.wait_duration_ms,
    lock_mode = REPLACE(REPLACE(REPLACE(wt.wait_type, N'LCK_M_', N''), N'PAGEIOLATCH_', N''), N'PAGELATCH_', N''),
    blocking_status = ISNULL(der_k.status, ses_k.status),
    contentious_object =
        CASE
            WHEN objparse.object_id IS NOT NULL
            THEN ISNULL(QUOTENAME(OBJECT_SCHEMA_NAME(objparse.object_id, objparse.database_id)) + N'.' + QUOTENAME(OBJECT_NAME(objparse.object_id, objparse.database_id)), der_b.wait_resource)
            /* #1893: a lock this collector does not resolve now says WHICH DATABASE the contended object
               lives in, in the blocked_process_report collector's exact sentinel shape, so the two
               collectors' rows for one object fingerprint identically (#1876). The database has to come
               from the RESOURCE, not the session: a cross-database lock is held in one database by a
               session running in another, and database_name above is the session's. */
            WHEN resparse.resource_database_id IS NOT NULL
            THEN N'Unresolved: ' +
                 CASE
                     WHEN resparse.lock_type IS NULL
                     THEN N''
                     ELSE LOWER(resparse.lock_type) + N' lock, '
                 END +
                 N'database: ' + COALESCE(DB_NAME(resparse.resource_database_id), DB_NAME(der_b.database_id), N'unknown')
            ELSE der_b.wait_resource
        END,
    blocked_sql_text = dest_b.text,
    blocking_sql_text = dest_k.text,
    blocked_login_name = ses_b.login_name,
    blocked_host_name = ses_b.host_name,
    blocked_client_app = ses_b.program_name,
    blocking_login_name = ses_k.login_name,
    blocking_host_name = ses_k.host_name,
    blocking_client_app = ses_k.program_name
FROM sys.dm_os_waiting_tasks AS wt
JOIN sys.dm_exec_sessions AS ses_b
  ON ses_b.session_id = wt.session_id
LEFT JOIN sys.dm_exec_requests AS der_b
  ON der_b.session_id = wt.session_id
LEFT JOIN sys.dm_exec_sessions AS ses_k
  ON ses_k.session_id = wt.blocking_session_id
LEFT JOIN sys.dm_exec_requests AS der_k
  ON der_k.session_id = wt.blocking_session_id
LEFT JOIN sys.dm_exec_connections AS con_k
  ON con_k.session_id = wt.blocking_session_id
OUTER APPLY sys.dm_exec_sql_text(der_b.sql_handle) AS dest_b
OUTER APPLY sys.dm_exec_sql_text(COALESCE(der_k.sql_handle, con_k.most_recent_sql_handle)) AS dest_k
OUTER APPLY
(
    SELECT TOP (1) tat.transaction_begin_time
    FROM sys.dm_tran_session_transactions AS stx
    JOIN sys.dm_tran_active_transactions AS tat
      ON tat.transaction_id = stx.transaction_id
    WHERE stx.session_id = wt.session_id
    ORDER BY tat.transaction_begin_time ASC
) AS tat_b
OUTER APPLY
(
    SELECT TOP (1) tat.transaction_begin_time
    FROM sys.dm_tran_session_transactions AS stx
    JOIN sys.dm_tran_active_transactions AS tat
      ON tat.transaction_id = stx.transaction_id
    WHERE stx.session_id = wt.blocking_session_id
    ORDER BY tat.transaction_begin_time ASC
) AS tat_k
OUTER APPLY
(
    SELECT
        database_id = TRY_CONVERT(integer, PARSENAME(REPLACE(SUBSTRING(der_b.wait_resource, 9, 200), N':', N'.'), 3)),
        object_id = TRY_CONVERT(integer, PARSENAME(REPLACE(SUBSTRING(der_b.wait_resource, 9, 200), N':', N'.'), 2))
    WHERE der_b.wait_resource LIKE N'OBJECT: %'
) AS objparse
/* #1893: the resource's database id, and the lock type to name it with.
   The id comes from sys.dm_os_waiting_tasks.resource_description rather than from wait_resource,
   because MS Learn documents EVERY lock resource type's description as carrying a dbid= token --
   keylock, pagelock, ridlock, objectlock, databaselock, filelock, extentlock, applicationlock,
   metadatalock, hobtlock and allocunitlock all end with (or contain) dbid=<db-id> -- so one parse
   covers every lock shape instead of a per-type positional split of wait_resource. Verified live on
   SQL 2022: a cross-database KEY lock reports
   'keylock hobtid=72057594045726720 dbid=11 id=lock... mode=X associatedObjectId=...'.
   The lock TYPE still comes from wait_resource, using the report collector's classifier verbatim, so
   the two sides produce the same token for the same lock.
   Restricted to LCK_% waits with a wait_resource: latch and RESOURCE_SEMAPHORE rows have no
   'TYPE: ' resource shape and no dbid= to read, and they keep exactly the value they had. */
OUTER APPLY
(
    SELECT
        resource_database_id =
            TRY_CONVERT
            (
                integer,
                NULLIF(LEFT(d.tail, PATINDEX(N'%[^0-9]%', d.tail + N'.') - 1), N'')
            ),
        lock_type =
            CASE
                WHEN der_b.wait_resource LIKE N'%KEY: %'    THEN N'KEY'
                WHEN der_b.wait_resource LIKE N'%OBJECT: %' THEN N'OBJECT'
                WHEN der_b.wait_resource LIKE N'%RID: %'    THEN N'RID'
                WHEN der_b.wait_resource LIKE N'%PAGE: %'   THEN N'PAGE'
                ELSE LEFT(UPPER(LEFT(der_b.wait_resource, CHARINDEX(N':', der_b.wait_resource + N':') - 1)), 32)
            END
    FROM
    (
        SELECT
            tail = SUBSTRING(wt.resource_description, CHARINDEX(N'dbid=', wt.resource_description) + 5, 10)
    ) AS d
    WHERE wt.wait_type LIKE N'LCK[_]%'
    AND   der_b.wait_resource IS NOT NULL
    AND   der_b.wait_resource <> N''
    AND   CHARINDEX(N'dbid=', wt.resource_description) > 0
) AS resparse
WHERE wt.blocking_session_id IS NOT NULL
AND   wt.blocking_session_id <> 0
AND   wt.blocking_session_id <> wt.session_id
AND   wt.session_id > 50
AND   wt.session_id <> @@SPID
AND   (
          wt.wait_type LIKE N'LCK[_]%'
          OR wt.wait_type LIKE N'PAGELATCH[_]%'
          OR wt.wait_type LIKE N'PAGEIOLATCH[_]%'
          OR wt.wait_type LIKE N'RESOURCE_SEMAPHORE%'
      )
/* Layered minimum-wait floor (kept in lockstep with install/56): locks must persist to matter (2s); page
   latches churn faster (PAGELATCH 0.5s, PAGEIOLATCH 1s); memory-grant / compile-gate waits run long, so 5s. */
AND   wt.wait_duration_ms >=
      CASE
          WHEN wt.wait_type LIKE N'LCK[_]%'             THEN 2000
          WHEN wt.wait_type LIKE N'PAGELATCH[_]%'       THEN 500
          WHEN wt.wait_type LIKE N'PAGEIOLATCH[_]%'     THEN 1000
          WHEN wt.wait_type LIKE N'RESOURCE_SEMAPHORE%' THEN 5000
          ELSE 1000
      END
OPTION(RECOMPILE);";

    public override string Name => "dmv_blocking_snapshot";

    public override string TargetTable => "dmv_blocking_snapshots";

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("monitor_loop", CollectorColumnType.Integer),
        new CollectorColumn("event_time", CollectorColumnType.Timestamp),
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_spid", CollectorColumnType.Integer),
        new CollectorColumn("blocked_ecid", CollectorColumnType.Integer),
        new CollectorColumn("blocked_last_tran_started", CollectorColumnType.Timestamp),
        new CollectorColumn("blocking_spid", CollectorColumnType.Integer),
        new CollectorColumn("blocking_ecid", CollectorColumnType.Integer),
        new CollectorColumn("blocking_last_tran_started", CollectorColumnType.Timestamp),
        new CollectorColumn("wait_time_ms", CollectorColumnType.BigInt),
        new CollectorColumn("lock_mode", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_status", CollectorColumnType.Varchar),
        new CollectorColumn("contentious_object", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_sql_text", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_sql_text", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_login_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_host_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocked_client_app", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_login_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_host_name", CollectorColumnType.Varchar),
        new CollectorColumn("blocking_client_app", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();
        /* #4348: the two statement columns are judged where they first enter the row. */
        var scrub = context.BeginStatementScrub();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                reader.IsDBNull(0) ? null : reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                reader.IsDBNull(7) ? 0L : reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.IsDBNull(9) ? null : reader.GetString(9),
                reader.IsDBNull(10) ? null : reader.GetString(10),
                reader.IsDBNull(11) ? null : scrub.Text(reader.GetString(11)),
                reader.IsDBNull(12) ? null : scrub.Text(reader.GetString(12)),
                reader.IsDBNull(13) ? null : reader.GetString(13),
                reader.IsDBNull(14) ? null : reader.GetString(14),
                reader.IsDBNull(15) ? null : reader.GetString(15),
                reader.IsDBNull(16) ? null : reader.GetString(16),
                reader.IsDBNull(17) ? null : reader.GetString(17),
                reader.IsDBNull(18) ? null : reader.GetString(18)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(SyntheticMonitorLoop(context.CollectionTime))  /* monitor_loop INTEGER (negative) */
            .Value(context.CollectionTime)                        /* event_time TIMESTAMP */
            .Value(row.DatabaseName)
            .Value(row.BlockedSpid)
            .Value(row.BlockedEcid)
            .Value(row.BlockedLastTranStarted)
            .Value(row.BlockingSpid)
            .Value(row.BlockingEcid)
            .Value(row.BlockingLastTranStarted)
            .Value(row.WaitTimeMs)
            .Value(row.LockMode)
            .Value(row.BlockingStatus)
            .Value(row.ContentiousObject)
            .Value(row.BlockedSqlText)
            .Value(row.BlockingSqlText)
            .Value(row.BlockedLoginName)
            .Value(row.BlockedHostName)
            .Value(row.BlockedClientApp)
            .Value(row.BlockingLoginName)
            .Value(row.BlockingHostName)
            .Value(row.BlockingClientApp);
    }
}
