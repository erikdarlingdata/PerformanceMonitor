/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>
/// The per-category "significant / warning" predicates the System Events surface applies AFTER
/// <see cref="SystemHealthParser"/> shreds the raw system_health events — sp_HealthParser's per-category WHERE
/// filtering, re-implemented ONCE in Common so every consumer shares it. Stage 2a (the parser) deliberately
/// preserved every field and applied NO WHERE filter; this class re-applies the significant-set gate so a tab
/// or tool shows the same SIGNIFICANT rows the sibling Dashboard surfaces.
///
/// <para><b>One source, three consumers.</b> These predicates were previously hand-duplicated: the Darling
/// viewer's <c>SystemEventSignificance</c> and the headless MCP host's <c>DarlingSystemHealthSignificance</c>
/// each carried a separate copy, and they had ALREADY drifted — the service copy was missing the
/// significant-wait predicate (plus its wait-duration constant and idle-wait ignore list) entirely. They now
/// live here once; the Lite and Darling viewers and the Darling MCP host all call this class, so the
/// sp_HealthParser semantics can never diverge between them. (The Default Trace significant-set gate is the
/// sibling <see cref="DefaultTraceEventSignificance"/>, already shared the same way.)</para>
///
/// <para><b>Parameter choice (load-bearing):</b> the predicates match <c>sp_HealthParser</c> run at the
/// product's canonical parameters — <c>@warnings_only = 1</c> and <c>@wait_duration_ms = 500</c>. Those are
/// exactly what the Dashboard's server-side collector passes
/// (<c>install/28_collect_system_health_wrapper.sql</c> defaults <c>@warnings_only = 1</c>; 500 ms is
/// sp_HealthParser's own default), so parse-on-read output matches the Dashboard's <c>report.HealthParser_*</c>
/// tables. Under <c>@warnings_only = 1</c> the gated categories tighten to the "warning" rows only (WARNING
/// scheduler status, RESOURCE_MEMPHYSICAL_LOW notifications, severity &gt;= 19); memory-node-OOM is never
/// gated (every recorded OOM is significant), and the wait predicate is always-on regardless of warnings_only.
/// Each predicate cites the sp_HealthParser source line.</para>
///
/// <para>Pure and dependency-free (over the Common records) so it is unit-testable without a live store.</para>
/// </summary>
public static class SystemHealthSignificance
{
    /// <summary>
    /// sp_HealthParser severe-error cutoff under <c>@warnings_only = 1</c>: severity &gt;= 19
    /// (sp_HealthParser.sql line 4759-4760 — the always-on floor is 16, and warnings_only raises it to 19).
    /// </summary>
    public const int SevereErrorMinSeverity = 19;

    /// <summary>
    /// sp_HealthParser <c>@wait_duration_ms</c> default (500 ms) — the minimum <c>duration</c> for a
    /// wait_info row to count as a significant wait (sp_HealthParser.sql line 1960).
    /// </summary>
    public const long SignificantWaitMinDurationMs = 500;

    /// <summary>
    /// sp_HealthParser <c>@pending_task_threshold</c> default (10) — the minimum <c>pendingTasks</c> for a
    /// QUERY_PROCESSING row to count as a significant CPU-task warning (sp_HealthParser.sql lines 54, 2981).
    /// </summary>
    public const long CpuTaskMinPendingTasks = 10;

    /// <summary>The scheduler <c>status</c> text sp_HealthParser keeps under warnings_only (line 4540).</summary>
    public const string WarningStatus = "WARNING";

    /// <summary>
    /// The low-physical-memory notification sp_HealthParser keeps under warnings_only for the
    /// MemoryConditions (line 3420) and MemoryBroker (line 3651) categories.
    /// </summary>
    public const string LowMemoryNotification = "RESOURCE_MEMPHYSICAL_LOW";

    /// <summary>
    /// error_reported numbers sp_HealthParser always excludes from severe errors — 17830 (a network
    /// TDS/connection-reset error) and 18056 (the benign "connection was reset by the client" pool churn),
    /// which are severity 20 but not actionable (sp_HealthParser.sql line 4733-4734).
    /// </summary>
    public static readonly IReadOnlySet<int> IgnoredSevereErrorNumbers = new HashSet<int> { 17830, 18056 };

    /// <summary>
    /// The "boring waits" ignore list sp_HealthParser excludes from significant waits — copied verbatim
    /// from <c>#ignore_waits</c> (sp_HealthParser.sql line 1501-1520): idle/queue/background wait types
    /// that a discrete long wait_info event on will never be actionable. OrdinalIgnoreCase because XE emits
    /// these as the canonical upper-case wait-type enum text.
    /// </summary>
    public static readonly IReadOnlySet<string> IgnoredWaitTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "ASYNC_IO_COMPLETION", "AZURE_IMDS_VERSIONS", "BROKER_EVENTHANDLER", "BROKER_RECEIVE_WAITFOR",
        "BROKER_TASK_STOP", "BROKER_TO_FLUSH", "BROKER_TRANSMITTER", "CHECKPOINT_QUEUE",
        "CHKPT", "CLR_AUTO_EVENT", "CLR_MANUAL_EVENT", "CLR_SEMAPHORE",
        "DBMIRROR_DBM_EVENT", "DBMIRROR_DBM_MUTEX", "DBMIRROR_EVENTS_QUEUE", "DBMIRROR_SEND",
        "DBMIRROR_WORKER_QUEUE", "DBMIRRORING_CMD", "DIRTY_PAGE_POLL", "DISPATCHER_QUEUE_SEMAPHORE",
        "FSAGENT", "FT_IFTS_SCHEDULER_IDLE_WAIT", "FT_IFTSHC_MUTEX", "HADR_CLUSAPI_CALL",
        "HADR_FILESTREAM_IOMGR_IOCOMPLETION", "HADR_LOGCAPTURE_WAIT", "HADR_NOTIFICATION_DEQUEUE", "HADR_TIMER_TASK",
        "HADR_WORK_QUEUE", "KSOURCE_WAKEUP", "LAZYWRITER_SLEEP", "LOGMGR_QUEUE",
        "MEMORY_ALLOCATION_EXT", "ONDEMAND_TASK_QUEUE", "PARALLEL_REDO_DRAIN_WORKER", "PARALLEL_REDO_LOG_CACHE",
        "PARALLEL_REDO_TRAN_LIST", "PARALLEL_REDO_WORKER_SYNC", "PARALLEL_REDO_WORKER_WAIT_WORK", "PREEMPTIVE_OS_FLUSHFILEBUFFERS",
        "PREEMPTIVE_XE_GETTARGETSTATE", "PVS_PREALLOCATE", "PWAIT_ALL_COMPONENTS_INITIALIZED", "PWAIT_DIRECTLOGCONSUMER_GETNEXT",
        "PWAIT_EXTENSIBILITY_CLEANUP_TASK", "QDS_ASYNC_QUEUE", "QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP", "QDS_PERSIST_TASK_MAIN_LOOP_SLEEP",
        "QDS_SHUTDOWN_QUEUE", "REDO_THREAD_PENDING_WORK", "REQUEST_FOR_DEADLOCK_SEARCH", "RESOURCE_QUEUE",
        "SERVER_IDLE_CHECK", "SLEEP_DBSTARTUP", "SLEEP_DCOMSTARTUP", "SLEEP_MASTERDBREADY",
        "SLEEP_MASTERMDREADY", "SLEEP_MASTERUPGRADED", "SLEEP_MSDBSTARTUP", "SLEEP_SYSTEMTASK",
        "SLEEP_TEMPDBSTARTUP", "SNI_HTTP_ACCEPT", "SOS_WORK_DISPATCHER", "SP_SERVER_DIAGNOSTICS_SLEEP",
        "SQLTRACE_BUFFER_FLUSH", "SQLTRACE_INCREMENTAL_FLUSH_SLEEP", "SQLTRACE_WAIT_ENTRIES", "UCS_SESSION_REGISTRATION",
        "VDI_CLIENT_OTHER", "WAIT_FOR_RESULTS", "WAIT_XTP_CKPT_CLOSE", "WAIT_XTP_HOST_WAIT",
        "WAIT_XTP_OFFLINE_CKPT_NEW_LOG", "WAIT_XTP_RECOVERY", "WAITFOR", "WAITFOR_TASKSHUTDOWN",
        "XE_DISPATCHER_JOIN", "XE_DISPATCHER_WAIT", "XE_FILE_TARGET_TVF", "XE_LIVE_TARGET_TVF", "XE_TIMER_EVENT",
    };

    /// <summary>
    /// A scheduler-monitor row is significant when its status is WARNING (a non-yielding or offline
    /// scheduler), per sp_HealthParser.sql line 4540 under <c>@warnings_only = 1</c>. Routine heartbeat
    /// rows (any other status) are dropped.
    /// </summary>
    public static bool IsSignificant(SchedulerIssueRecord record) =>
        string.Equals(record.Status, WarningStatus, StringComparison.Ordinal);

    /// <summary>
    /// An error_reported row is significant when severity &gt;= 19 and the error number is not one of the
    /// benign connection-reset numbers, per sp_HealthParser.sql lines 4759-4767 under
    /// <c>@warnings_only = 1</c>. A missing severity drops the row (sp requires the severity node);
    /// a missing error number keeps it (it can't match the ignore list). The always-on base floor
    /// (severity &gt;= 16, and the same ignore list) is applied once, upstream, by
    /// <see cref="SystemHealthParser.ParseSevereError"/> itself — this predicate only adds the
    /// warnings_only-specific raise from 16 to 19 on top of rows the parser already let through.
    /// </summary>
    public static bool IsSignificant(SevereErrorRecord record) =>
        record.Severity >= SevereErrorMinSeverity &&
        (record.ErrorNumber is not { } errorNumber || !IgnoredSevereErrorNumbers.Contains(errorNumber));

    /// <summary>
    /// A memory-conditions snapshot is significant when its <c>lastNotification</c> is
    /// RESOURCE_MEMPHYSICAL_LOW (the server is under physical-memory pressure), per sp_HealthParser.sql
    /// line 3420 under <c>@warnings_only = 1</c>.
    /// </summary>
    public static bool IsSignificant(MemoryConditionsRecord record) =>
        string.Equals(record.LastNotification, LowMemoryNotification, StringComparison.Ordinal);

    /// <summary>
    /// A memory-broker ratio-change row is significant when its <c>notification</c> is
    /// RESOURCE_MEMPHYSICAL_LOW, per sp_HealthParser.sql line 3651 under <c>@warnings_only = 1</c>.
    /// </summary>
    public static bool IsSignificant(MemoryBrokerRecord record) =>
        string.Equals(record.Notification, LowMemoryNotification, StringComparison.Ordinal);

    /// <summary>
    /// Every recorded memory-node OOM is significant — sp_HealthParser applies NO WHERE filter to this
    /// category (sp_HealthParser.sql line 3944-3946: the CROSS APPLY has no predicate), so the tab shows
    /// every shredded row. Present for symmetry so the data-service read filters uniformly.
    /// </summary>
    public static bool IsSignificant(MemoryNodeOomRecord record) => true;

    /// <summary>
    /// A wait_info row is a significant wait when it belongs to a real session, carries a non-BACKUP
    /// statement, lasted at least <see cref="SignificantWaitMinDurationMs"/>, and its wait type is not on
    /// the idle/background ignore list, per sp_HealthParser.sql lines 1957-1967. This predicate is
    /// always-on (it is not gated by warnings_only).
    /// </summary>
    public static bool IsSignificant(SignificantWaitRecord record) =>
        record.SessionId != 0 &&                                                    // session_id <> 0 (line 1957)
        !string.IsNullOrEmpty(record.QueryText) &&                                  // sql_text present (line 1958)
        record.QueryText.IndexOf("BACKUP", StringComparison.OrdinalIgnoreCase) < 0 && // not a BACKUP (line 1959)
        record.DurationMs >= SignificantWaitMinDurationMs &&                        // duration >= @wait_duration_ms (line 1960)
        !IgnoredWaitTypes.Contains(record.WaitType ?? string.Empty);               // wait_type NOT IN #ignore_waits (line 1961-1967)

    /// <summary>
    /// A CPU-task-details row is significant when the QUERY_PROCESSING component state is WARNING and its
    /// <c>pendingTasks</c> is at least <see cref="CpuTaskMinPendingTasks"/>, per sp_HealthParser.sql lines
    /// 2976 and 2981 under <c>@warnings_only = 1</c>. A missing state or a pendingTasks below the threshold
    /// (including null) drops the row, exactly as the sp's <c>state = "WARNING"</c> +
    /// <c>@pendingTasks[.>= threshold]</c> exist-predicates do.
    /// </summary>
    public static bool IsSignificant(CpuTasksRecord record) =>
        string.Equals(record.State, WarningStatus, StringComparison.Ordinal) &&
        record.PendingTasks >= CpuTaskMinPendingTasks;

    /// <summary>
    /// A potential-IO-issue row is significant when the IO_SUBSYSTEM component state is WARNING, per
    /// sp_HealthParser.sql line 2757 under <c>@warnings_only = 1</c>. (The "has a pending request with a
    /// duration" predicate — sp_HealthParser's <c>WHERE duration IS NOT NULL</c> — is applied upstream in
    /// <see cref="SystemHealthParser.ParseIoIssues"/>, which emits no record for an event without one.)
    /// </summary>
    public static bool IsSignificant(IoIssuesRecord record) =>
        string.Equals(record.State, WarningStatus, StringComparison.Ordinal);
}
