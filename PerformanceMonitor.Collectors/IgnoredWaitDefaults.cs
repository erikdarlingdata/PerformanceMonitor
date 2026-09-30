/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The default benign wait types excluded from wait_stats collection — the shared source both
/// SKUs filter by, so portable Lite and the Darling service collect identical wait sets. Lite
/// ships the same list as config/ignored_wait_types.json (user-overridable per machine) and an
/// identity-pin test asserts the two cannot drift; Darling consumes this constant directly. Entries are the
/// clean names: the collectors trim the trailing space a few wait names carry before they match (see
/// <see cref="PerformanceMonitor.Common.WaitTypeName"/>), so an entry never needs one.
/// </summary>
public static class IgnoredWaitDefaults
{
    public static IReadOnlySet<string> All { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "AZURE_IMDS_VERSIONS",
        "BMPALLOCATION",
        "BMPBUILD",
        "BMPREPARTITION",
        "BROKER_EVENTHANDLER",
        "BROKER_RECEIVE_WAITFOR",
        "BROKER_TASK_STOP",
        "BROKER_TO_FLUSH",
        "BROKER_TRANSMITTER",
        "BUFFERPOOL_SCAN",
        "CHECKPOINT_QUEUE",
        "CHKPT",
        "CLR_AUTO_EVENT",
        "CLR_MANUAL_EVENT",
        "CLR_SEMAPHORE",
        "COLUMNSTORE_BUILD_THROTTLE",
        "DAC_INIT",
        "DBMIRRORING_CMD",
        "DBMIRROR_DBM_EVENT",
        "DBMIRROR_DBM_MUTEX",
        "DBMIRROR_EVENTS_QUEUE",
        "DBMIRROR_SEND",
        "DBMIRROR_WORKER_QUEUE",
        "DIRTY_PAGE_POLL",
        "DIRTY_PAGE_TABLE_LOCK",
        "DISPATCHER_QUEUE_SEMAPHORE",
        "FSAGENT",
        "FT_IFTSHC_MUTEX",
        "FT_IFTS_SCHEDULER_IDLE_WAIT",
        "HADR_CLUSAPI_CALL",
        "HADR_FABRIC_CALLBACK",
        "HADR_FILESTREAM_IOMGR_IOCOMPLETION",
        "HADR_LOGCAPTURE_WAIT",
        "HADR_NOTIFICATION_DEQUEUE",
        "HADR_TIMER_TASK",
        "HADR_WORK_QUEUE",
        "KSOURCE_WAKEUP",
        "LAZYWRITER_SLEEP",
        "LOGMGR_QUEUE",
        "MEMORY_ALLOCATION_EXT",
        "ONDEMAND_TASK_QUEUE",
        "PARALLEL_REDO_DRAIN_WORKER",
        "PARALLEL_REDO_FLOW_CONTROL",
        "PARALLEL_REDO_LOG_CACHE",
        "PARALLEL_REDO_TRAN_LIST",
        "PARALLEL_REDO_TRAN_TURN",
        "PARALLEL_REDO_WORKER_SYNC",
        "PARALLEL_REDO_WORKER_WAIT_WORK",
        "PERFORMANCE_COUNTERS_RWLOCK",
        "PREEMPTIVE_OS_FLUSHFILEBUFFERS",
        "PREEMPTIVE_XE_CALLBACKEXECUTE",
        "PREEMPTIVE_XE_DISPATCHER",
        "PREEMPTIVE_XE_GETTARGETSTATE",
        "PREEMPTIVE_XE_SESSIONCOMMIT",
        "PREEMPTIVE_XE_TARGETFINALIZE",
        "PREEMPTIVE_XE_TARGETINIT",
        "PRINT_ROLLBACK_PROGRESS",
        "PURVIEW_POLICY_SDK_PREEMPTIVE_SCHEDULING",
        "PVS_PREALLOCATE",
        "PWAIT_ALL_COMPONENTS_INITIALIZED",
        "PWAIT_DIRECTLOGCONSUMER_GETNEXT",
        "PWAIT_EXTENSIBILITY_CLEANUP_TASK",
        "PWAIT_HADRSIM",
        "PWAIT_HADR_ACTION_COMPLETED",
        "PWAIT_HADR_CHANGE_NOTIFIER_TERMINATION_SYNC",
        "PWAIT_HADR_CLUSTER_INTEGRATION",
        "PWAIT_HADR_FAILOVER_COMPLETED",
        "PWAIT_HADR_JOIN",
        "PWAIT_HADR_OFFLINE_COMPLETED",
        "PWAIT_HADR_ONLINE_COMPLETED",
        "PWAIT_HADR_POST_ONLINE_COMPLETED",
        "PWAIT_HADR_SERVER_READY_CONNECTIONS",
        "PWAIT_HADR_WORKITEM_COMPLETED",
        "PWAIT_MASTERDBREADY",
        "QDS_ASYNC_QUEUE",
        "QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP",
        "QDS_PERSIST_TASK_MAIN_LOOP_SLEEP",
        "QDS_SHUTDOWN_QUEUE",
        "QUERY_EXECUTION_INDEX_SORT_EVENT_OPEN",
        "QUERY_TASK_ENQUEUE_MUTEX",
        "REDO_THREAD_PENDING_WORK",
        "REQUEST_FOR_DEADLOCK_SEARCH",
        "RESOURCE_QUEUE",
        "RESOURCE_SEMAPHORE_MUTEX",
        "SECURITY_CNG_PROVIDER_MUTEX",
        "SERVER_IDLE_CHECK",
        "SLEEP_BUFFERPOOL_HELPLW",
        "SLEEP_DBSTARTUP",
        "SLEEP_DCOMSTARTUP",
        "SLEEP_MASTERDBREADY",
        "SLEEP_MASTERMDREADY",
        "SLEEP_MASTERUPGRADED",
        "SLEEP_MSDBSTARTUP",
        "SLEEP_PHYSMASTERDBREADY",
        "SLEEP_SYSTEMTASK",
        "SLEEP_TASK",
        "SLEEP_TEMPDBSTARTUP",
        "SNI_CRITICAL_SECTION",
        "SNI_HTTP_ACCEPT",
        "SOS_PROCESS_AFFINITY_MUTEX",
        "SOS_WORK_DISPATCHER",
        "SP_SERVER_DIAGNOSTICS_SLEEP",
        "SQLTRACE_BUFFER_FLUSH",
        "SQLTRACE_FILE_BUFFER",
        "SQLTRACE_FILE_READ_IO_COMPLETION",
        "SQLTRACE_FILE_WRITE_IO_COMPLETION",
        "SQLTRACE_INCREMENTAL_FLUSH_SLEEP",
        "SQLTRACE_WAIT_ENTRIES",
        "UCS_SESSION_REGISTRATION",
        "VDI_CLIENT_OTHER",
        "WAITFOR",
        "WAITFOR_TASKSHUTDOWN",
        "WAIT_FOR_RESULTS",
        "WAIT_XTP_CKPT_CLOSE",
        "WAIT_XTP_HOST_WAIT",
        "WAIT_XTP_OFFLINE_CKPT_NEW_LOG",
        "WAIT_XTP_RECOVERY",
        "WINDOW_AGGREGATES_MULTIPASS",
        "XE_BUFFERMGR_ALLPROCESSED_EVENT",
        "XE_DISPATCHER_JOIN",
        "XE_DISPATCHER_WAIT",
        "XE_FILE_TARGET_TVF",
        "XE_LIVE_TARGET_TVF",
        "XE_TIMER_EVENT",

        /* Two Azure SQL Database Hyperscale platform timers, grouped here because one measurement decides both.
           Measured on one Hyperscale database over 3 hours (93 two-minute buckets):
             RBIO_COMM_RETRY      ONE waiter, a steady ~1,000 ms of wait per second (min 960, max 1,126);
                                  735 waits that average exactly 15.0 s.
             SQP_STATS_REPORTING  ONE waiter; 37 waits that average exactly 300.0 s. The server reports this name
                                  with a trailing space, which the collectors trim at the read.
           One waiter on a fixed interval, at a rate that does not follow the workload, is a timer and not
           contention. Kept, it only adds a constant line to the wait charts.

           REMOTE_BLOCK_IO has the same timer shape in that measurement and is deliberately NOT listed. Its name
           says real remote I/O (page-server reads on Hyperscale), the test workload most likely never forced
           one, and Microsoft Learn's sys.dm_os_wait_stats page marks it "Internal use only" instead of calling
           it idle. A page-server read wait is what a Hyperscale operator needs to see, so it stays visible.
           The same page does not list RBIO_COMM_RETRY or SQP_STATS_REPORTING at all. */
        "RBIO_COMM_RETRY",
        "SQP_STATS_REPORTING",
    };
}
