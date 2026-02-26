/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

For usage, licensing, and support:
https://github.com/erikdarlingdata/DarlingData

Ensure Config Tables - Performance Monitor
Erik Darling - erik@erikdarling.com

Self-healing procedure to ensure all config schema tables exist and are properly initialized
*/

SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;
SET IMPLICIT_TRANSACTIONS OFF;
SET STATISTICS TIME, IO OFF;
GO

USE PerformanceMonitor;
GO

IF OBJECT_ID(N'config.ensure_config_tables', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE config.ensure_config_tables AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    config.ensure_config_tables
(
    @debug bit = 0 /*Print debugging information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @start_time datetime2(7) = SYSDATETIME(),
        @tables_created integer = 0,
        @tables_initialized integer = 0,
        @error_message nvarchar(2048) = N'',
        @log_to_table bit = 0;

    BEGIN TRY
        /*
        Create config.collection_log FIRST since all other operations log to it
        */
        IF OBJECT_ID(N'config.collection_log', N'U') IS NULL
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Creating config.collection_log table', 0, 1) WITH NOWAIT;
            END;

            CREATE TABLE
                config.collection_log
            (
                log_id bigint IDENTITY NOT NULL,
                collection_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
                collector_name nvarchar(100) NOT NULL,
                collection_status nvarchar(20) NOT NULL,
                rows_collected integer NOT NULL DEFAULT 0,
                duration_ms integer NOT NULL DEFAULT 0,
                error_message nvarchar(4000) NULL,
                CONSTRAINT PK_collection_log PRIMARY KEY CLUSTERED (log_id) WITH (DATA_COMPRESSION = PAGE)
            );

            CREATE INDEX
                collection_log_time_collector
            ON config.collection_log (collection_time, collector_name)
            WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX
                collection_log_status
            ON config.collection_log (collection_status)
            WITH (DATA_COMPRESSION = PAGE);

            SET @tables_created = @tables_created + 1;
        END;

        /*
        Now we can log to config.collection_log
        */
        SET @log_to_table = 1;

        /*
        Create config.collection_schedule
        */
        IF OBJECT_ID(N'config.collection_schedule', N'U') IS NULL
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Creating config.collection_schedule table', 0, 1) WITH NOWAIT;
            END;

            CREATE TABLE
                config.collection_schedule
            (
                schedule_id integer IDENTITY NOT NULL,
                collector_name sysname NOT NULL,
                [enabled] bit NOT NULL DEFAULT CONVERT(bit, 'true'),
                frequency_minutes integer NOT NULL DEFAULT 15,
                last_run_time datetime2(7) NULL,
                next_run_time datetime2(7) NULL,
                max_duration_minutes integer NOT NULL DEFAULT 5,
                retention_days integer NOT NULL DEFAULT 30,
                [description] nvarchar(500) NULL,
                created_date datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
                modified_date datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
                CONSTRAINT PK_collection_schedule PRIMARY KEY CLUSTERED (schedule_id) WITH (DATA_COMPRESSION = PAGE)
            );

            CREATE UNIQUE INDEX
                UQ_collection_schedule_collector
            ON config.collection_schedule (collector_name)
            WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX
                collection_schedule_next_run
            ON config.collection_schedule (next_run_time, enabled)
            WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX
                collection_schedule_enabled
            ON config.collection_schedule (enabled, collector_name)
            WITH (DATA_COMPRESSION = PAGE);

            SET @tables_created = @tables_created + 1;

            INSERT INTO
                config.collection_log
            (
                collector_name,
                collection_status,
                error_message
            )
            VALUES
            (
                N'ensure_config_tables',
                N'TABLE_CREATED',
                N'Created config.collection_schedule table'
            );
        END;

        /*
        Initialize collection_schedule if empty
        */
        IF NOT EXISTS (SELECT 1/0 FROM config.collection_schedule)
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Initializing config.collection_schedule with default values', 0, 1) WITH NOWAIT;
            END;

            INSERT INTO
                config.collection_schedule
            (
                collector_name,
                enabled,
                frequency_minutes,
                max_duration_minutes,
                retention_days,
                description
            )
            VALUES
                (N'wait_stats_collector', 1, 5, 2, 30, N'Wait statistics - high frequency for trending'),
                (N'query_stats_collector', 1, 15, 5, 30, N'Plan cache queries - recent activity focused'),
                (N'memory_stats_collector', 1, 10, 2, 30, N'Memory pressure monitoring'),
                (N'memory_pressure_events_collector', 1, 30, 5, 30, N'Ring buffer system events'),
                (N'system_health_collector', 1, 60, 10, 30, N'System health extended events via sp_HealthParser'),
                (N'blocked_process_xml_collector', 1, 5, 2, 30, N'Fast blocked process XML collection'),
                (N'deadlock_xml_collector', 1, 15, 3, 30, N'Fast deadlock XML collection'),
                (N'process_blocked_process_xml', 1, 10, 5, 30, N'Parse blocked process XML via sp_HumanEventsBlockViewer'),
                (N'blocking_deadlock_analyzer', 1, 60, 5, 30, N'Analyze blocking/deadlock trends and alert on significant increases'),
                (N'process_deadlock_xml', 1, 10, 5, 30, N'Parse deadlock XML via sp_BlitzLock'),
                (N'query_store_collector', 1, 30, 10, 30, N'Query Store data collection'),
                (N'procedure_stats_collector', 1, 45, 10, 30, N'Procedure/trigger/function statistics'),
                (N'query_snapshots_collector', 1, 1, 2, 10, N'Currently executing queries with session wait stats (every minute - high frequency)'),
                (N'file_io_stats_collector', 1, 10, 2, 30, N'File I/O statistics from dm_io_virtual_file_stats'),
                (N'memory_grant_stats_collector', 1, 10, 2, 30, N'Memory grant semaphore pressure monitoring'),
                (N'cpu_scheduler_stats_collector', 1, 10, 2, 30, N'CPU scheduler and workload group statistics'),
                (N'memory_clerks_stats_collector', 1, 15, 3, 30, N'Memory clerk allocation tracking'),
                (N'perfmon_stats_collector', 1, 10, 2, 30, N'Performance counter statistics from dm_os_performance_counters'),
                (N'cpu_utilization_stats_collector', 1, 5, 2, 30, N'CPU utilization from ring buffer (SQL vs other processes)'),
                (N'trace_management_collector', 1, 1440, 5, 30, N'SQL Trace management for long-running queries'),
                (N'trace_analysis_collector', 1, 30, 5, 30, N'Process trace files into analysis tables'),
                (N'default_trace_collector', 1, 15, 3, 30, N'System events from default trace (memory, autogrow, config changes)'),
                (N'server_configuration_collector', 1, 1440, 5, 30, N'Server-level configuration settings and trace flags (daily collection)'),
                (N'database_configuration_collector', 1, 1440, 10, 30, N'Database-level configuration settings including scoped configs (daily collection)'),
                (N'configuration_issues_analyzer', 1, 1, 2, 30, N'Analyze configuration for issues: database config (Query Store, auto shrink/close), memory/CPU pressure warnings, server config (MAXDOP, priority boost)'),
                (N'latch_stats_collector', 1, 15, 3, 30, N'Latch contention statistics - internal synchronization object waits'),
                (N'spinlock_stats_collector', 1, 15, 3, 30, N'Spinlock contention statistics - lightweight synchronization primitive collisions'),
                (N'tempdb_stats_collector', 1, 5, 2, 30, N'TempDB space usage - version store, user/internal objects, allocation contention'),
                (N'plan_cache_stats_collector', 1, 60, 5, 30, N'Plan cache composition statistics - single-use plans and plan cache bloat detection'),
                (N'session_stats_collector', 1, 5, 2, 30, N'Session and connection statistics - connection leaks and application patterns'),
                (N'waiting_tasks_collector', 1, 5, 2, 30, N'Currently waiting tasks - blocking chains and wait analysis'),
                (N'running_jobs_collector', 1, 5, 2, 7, N'Currently running SQL Agent jobs with historical duration comparison');

            /*
            Stagger initial run times
            */
            UPDATE
                config.collection_schedule
            SET
                next_run_time = DATEADD(SECOND, (schedule_id * 2), SYSDATETIME());

            SET @tables_initialized = @tables_initialized + 1;

            INSERT INTO
                config.collection_log
            (
                collector_name,
                collection_status,
                rows_collected,
                error_message
            )
            VALUES
            (
                N'ensure_config_tables',
                N'TABLE_INITIALIZED',
                ROWCOUNT_BIG(),
                N'Initialized config.collection_schedule with default collector configurations'
            );
        END;

        /*
        Create config.server_info_history
        */
        IF OBJECT_ID(N'config.server_info_history', N'U') IS NULL
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Creating config.server_info_history table', 0, 1) WITH NOWAIT;
            END;

            CREATE TABLE
                config.server_info_history
            (
                collection_id bigint IDENTITY NOT NULL,
                collection_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
                sqlserver_start_time datetime2(7) NOT NULL,
                server_name sysname NOT NULL,
                instance_name sysname NULL,
                sql_version sysname NOT NULL,
                edition sysname NOT NULL,
                physical_memory_mb bigint NOT NULL,
                cpu_count integer NOT NULL,
                environment_type nvarchar(50) NOT NULL,
                CONSTRAINT PK_server_info_history PRIMARY KEY CLUSTERED (collection_id) WITH (DATA_COMPRESSION = PAGE)
            );

            CREATE INDEX
                server_info_history_time_server
            ON config.server_info_history (collection_time DESC, server_name)
            WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX
                server_info_history_start_time
            ON config.server_info_history (sqlserver_start_time DESC)
            WITH (DATA_COMPRESSION = PAGE);

            SET @tables_created = @tables_created + 1;

            INSERT INTO
                config.collection_log
            (
                collector_name,
                collection_status,
                error_message
            )
            VALUES
            (
                N'ensure_config_tables',
                N'TABLE_CREATED',
                N'Created config.server_info_history table'
            );
        END;

        /*
        Create config.critical_issues
        Logs significant performance problems detected by collectors and analysis procedures
        */
        IF OBJECT_ID(N'config.critical_issues', N'U') IS NULL
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Creating config.critical_issues table', 0, 1) WITH NOWAIT;
            END;

            CREATE TABLE
                config.critical_issues
            (
                issue_id bigint IDENTITY NOT NULL,
                log_date datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
                severity nvarchar(20) NOT NULL,
                problem_area nvarchar(100) NOT NULL,
                source_collector sysname NOT NULL,
                affected_database sysname NULL,
                message nvarchar(max) NOT NULL,
                investigate_query nvarchar(max) NULL,
                threshold_value decimal(38,2) NULL,
                threshold_limit decimal(38,2) NULL,
                CONSTRAINT PK_critical_issues PRIMARY KEY CLUSTERED (issue_id) WITH (DATA_COMPRESSION = PAGE),
                CONSTRAINT CK_critical_issues_severity CHECK (severity IN (N'CRITICAL', N'WARNING', N'INFO'))
            );

            CREATE INDEX
                critical_issues_log_date
            ON config.critical_issues (log_date DESC)
            WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX
                critical_issues_severity
            ON config.critical_issues (severity, log_date DESC)
            WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX
                critical_issues_problem_area
            ON config.critical_issues (problem_area, log_date DESC)
            WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX
                critical_issues_source_collector
            ON config.critical_issues (source_collector, log_date DESC)
            WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX
                critical_issues_affected_database
            ON config.critical_issues (affected_database, log_date DESC)
            WHERE affected_database IS NOT NULL
            WITH (DATA_COMPRESSION = PAGE);

            SET @tables_created = @tables_created + 1;

            INSERT INTO
                config.collection_log
            (
                collector_name,
                collection_status,
                error_message
            )
            VALUES
            (
                N'ensure_config_tables',
                N'TABLE_CREATED',
                N'Created config.critical_issues table'
            );
        END;

        /*
        Create config.server_configuration_history
        */
        IF OBJECT_ID(N'config.server_configuration_history', N'U') IS NULL
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Creating config.server_configuration_history table', 0, 1) WITH NOWAIT;
            END;

            CREATE TABLE
                config.server_configuration_history
            (
                collection_id bigint IDENTITY NOT NULL,
                collection_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
                configuration_id integer NOT NULL,
                configuration_name nvarchar(128) NOT NULL,
                value_configured sql_variant NULL,
                value_in_use sql_variant NULL,
                value_minimum sql_variant NULL,
                value_maximum sql_variant NULL,
                is_dynamic bit NOT NULL,
                is_advanced bit NOT NULL,
                description nvarchar(512) NULL,
                CONSTRAINT PK_server_configuration_history PRIMARY KEY CLUSTERED (collection_id) WITH (DATA_COMPRESSION = PAGE)
            );

            CREATE INDEX
                server_configuration_history_time_name
            ON config.server_configuration_history (collection_time DESC, configuration_name)
            WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX
                server_configuration_history_config_id
            ON config.server_configuration_history (configuration_id, collection_time DESC)
            WITH (DATA_COMPRESSION = PAGE);

            SET @tables_created = @tables_created + 1;

            INSERT INTO
                config.collection_log
            (
                collector_name,
                collection_status,
                error_message
            )
            VALUES
            (
                N'ensure_config_tables',
                N'TABLE_CREATED',
                N'Created config.server_configuration_history table'
            );
        END;

        /*
        Create config.trace_flags_history
        */
        IF OBJECT_ID(N'config.trace_flags_history', N'U') IS NULL
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Creating config.trace_flags_history table', 0, 1) WITH NOWAIT;
            END;

            CREATE TABLE
                config.trace_flags_history
            (
                collection_id bigint IDENTITY NOT NULL,
                collection_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
                trace_flag integer NOT NULL,
                status bit NOT NULL,
                is_global bit NOT NULL,
                is_session bit NOT NULL,
                CONSTRAINT PK_trace_flags_history PRIMARY KEY CLUSTERED (collection_id) WITH (DATA_COMPRESSION = PAGE)
            );

            CREATE INDEX
                trace_flags_history_time
            ON config.trace_flags_history (collection_time DESC)
            WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX
                trace_flags_history_flag_time
            ON config.trace_flags_history (trace_flag, collection_time DESC)
            WITH (DATA_COMPRESSION = PAGE);

            SET @tables_created = @tables_created + 1;

            INSERT INTO
                config.collection_log
            (
                collector_name,
                collection_status,
                error_message
            )
            VALUES
            (
                N'ensure_config_tables',
                N'TABLE_CREATED',
                N'Created config.trace_flags_history table'
            );
        END;

        /*
        Create config.database_configuration_history
        */
        IF OBJECT_ID(N'config.database_configuration_history', N'U') IS NULL
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Creating config.database_configuration_history table', 0, 1) WITH NOWAIT;
            END;

            CREATE TABLE
                config.database_configuration_history
            (
                collection_id bigint IDENTITY NOT NULL,
                collection_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
                database_id integer NOT NULL,
                database_name sysname NOT NULL,
                setting_type varchar(50) NOT NULL,
                setting_name nvarchar(128) NOT NULL,
                setting_value sql_variant NULL,
                CONSTRAINT PK_database_configuration_history PRIMARY KEY CLUSTERED (collection_id) WITH (DATA_COMPRESSION = PAGE)
            );

            CREATE INDEX
                database_configuration_history_time_db
            ON config.database_configuration_history (collection_time DESC, database_name)
            WITH (DATA_COMPRESSION = PAGE);

            CREATE INDEX
                database_configuration_history_db_setting
            ON config.database_configuration_history (database_id, setting_type, setting_name, collection_time DESC)
            WITH (DATA_COMPRESSION = PAGE);

            SET @tables_created = @tables_created + 1;

            INSERT INTO
                config.collection_log
            (
                collector_name,
                collection_status,
                error_message
            )
            VALUES
            (
                N'ensure_config_tables',
                N'TABLE_CREATED',
                N'Created config.database_configuration_history table'
            );
        END;

          /*
          Create config.ignored_wait_types
          User-configurable table for wait types to exclude from collection
          Used by wait_stats_collector and waiting_tasks_collector
          */
          IF OBJECT_ID(N'config.ignored_wait_types', N'U') IS NULL
          BEGIN
              IF @debug = 1
              BEGIN
                  RAISERROR(N'Creating config.ignored_wait_types table', 0, 1) WITH NOWAIT;
              END;

              CREATE TABLE
                  config.ignored_wait_types
              (
                  wait_type_id integer IDENTITY NOT NULL,
                  wait_type sysname NOT NULL,
                  description nvarchar(500) NULL,
                  is_enabled bit NOT NULL DEFAULT 1,
                  created_date datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
                  modified_date datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
                  CONSTRAINT PK_ignored_wait_types PRIMARY KEY CLUSTERED (wait_type_id) WITH (DATA_COMPRESSION = PAGE)
              );

              CREATE UNIQUE INDEX
                  UQ_ignored_wait_types_wait_type
              ON config.ignored_wait_types (wait_type)
              WITH (DATA_COMPRESSION = PAGE);

              SET @tables_created = @tables_created + 1;

              INSERT INTO
                  config.collection_log
              (
                  collector_name,
                  collection_status,
                  error_message
              )
              VALUES
              (
                  N'ensure_config_tables',
                  N'TABLE_CREATED',
                  N'Created config.ignored_wait_types table'
              );
          END;

          /*
          Initialize ignored_wait_types if empty
          Matches sp_HealthParser exclusion list for consistency
          */
          IF NOT EXISTS (SELECT 1/0 FROM config.ignored_wait_types)
          BEGIN
              IF @debug = 1
              BEGIN
                  RAISERROR(N'Initializing config.ignored_wait_types with default values', 0, 1) WITH NOWAIT;
              END;

              INSERT INTO
                  config.ignored_wait_types
              (
                  wait_type
              )
              VALUES
                  (N'AZURE_IMDS_VERSIONS'), (N'BMPALLOCATION'), (N'BMPBUILD'), (N'BMPREPARTITION'),
                  (N'BROKER_EVENTHANDLER'), (N'BROKER_RECEIVE_WAITFOR'), (N'BROKER_TASK_STOP'), (N'BROKER_TO_FLUSH'),
                  (N'BROKER_TRANSMITTER'), (N'BUFFERPOOL_SCAN'), (N'CHECKPOINT_QUEUE'), (N'CHKPT'),
                  (N'CLR_AUTO_EVENT'), (N'CLR_MANUAL_EVENT'), (N'CLR_SEMAPHORE'), (N'COLUMNSTORE_BUILD_THROTTLE'),
                  (N'DAC_INIT'), (N'DBMIRROR_DBM_EVENT'), (N'DBMIRROR_DBM_MUTEX'), (N'DBMIRROR_EVENTS_QUEUE'),
                  (N'DBMIRROR_SEND'), (N'DBMIRROR_WORKER_QUEUE'), (N'DBMIRRORING_CMD'), (N'DIRTY_PAGE_POLL'),
                  (N'DIRTY_PAGE_TABLE_LOCK'), (N'DISPATCHER_QUEUE_SEMAPHORE'), (N'FSAGENT'), (N'FT_IFTS_SCHEDULER_IDLE_WAIT'),
                  (N'FT_IFTSHC_MUTEX'), (N'HADR_CLUSAPI_CALL'), (N'HADR_FABRIC_CALLBACK'), (N'HADR_FILESTREAM_IOMGR_IOCOMPLETION'),
                  (N'HADR_LOGCAPTURE_WAIT'), (N'HADR_NOTIFICATION_DEQUEUE'), (N'HADR_TIMER_TASK'), (N'HADR_WORK_QUEUE'),
                  (N'KSOURCE_WAKEUP'), (N'LAZYWRITER_SLEEP'), (N'LOGMGR_QUEUE'), (N'MEMORY_ALLOCATION_EXT'),
                  (N'ONDEMAND_TASK_QUEUE'), (N'PARALLEL_REDO_DRAIN_WORKER'), (N'PARALLEL_REDO_FLOW_CONTROL'),
                  (N'PARALLEL_REDO_LOG_CACHE'), (N'PARALLEL_REDO_TRAN_LIST'), (N'PARALLEL_REDO_TRAN_TURN'),
                  (N'PARALLEL_REDO_WORKER_SYNC'), (N'PARALLEL_REDO_WORKER_WAIT_WORK'), (N'PERFORMANCE_COUNTERS_RWLOCK'),
                  (N'PREEMPTIVE_OS_FLUSHFILEBUFFERS'), (N'PREEMPTIVE_XE_CALLBACKEXECUTE'), (N'PREEMPTIVE_XE_DISPATCHER'),
                  (N'PREEMPTIVE_XE_GETTARGETSTATE'), (N'PREEMPTIVE_XE_SESSIONCOMMIT'), (N'PREEMPTIVE_XE_TARGETFINALIZE'),
                  (N'PREEMPTIVE_XE_TARGETINIT'), (N'PRINT_ROLLBACK_PROGRESS'), (N'PURVIEW_POLICY_SDK_PREEMPTIVE_SCHEDULING'),
                  (N'PVS_PREALLOCATE'), (N'PWAIT_ALL_COMPONENTS_INITIALIZED'), (N'PWAIT_DIRECTLOGCONSUMER_GETNEXT'),
                  (N'PWAIT_EXTENSIBILITY_CLEANUP_TASK'), (N'PWAIT_HADR_ACTION_COMPLETED'), (N'PWAIT_HADR_CHANGE_NOTIFIER_TERMINATION_SYNC'),
                  (N'PWAIT_HADR_CLUSTER_INTEGRATION'), (N'PWAIT_HADR_FAILOVER_COMPLETED'), (N'PWAIT_HADR_JOIN'),
                  (N'PWAIT_HADR_OFFLINE_COMPLETED'), (N'PWAIT_HADR_ONLINE_COMPLETED'), (N'PWAIT_HADR_POST_ONLINE_COMPLETED'),
                  (N'PWAIT_HADR_SERVER_READY_CONNECTIONS'), (N'PWAIT_HADR_WORKITEM_COMPLETED'), (N'PWAIT_HADRSIM'),
                  (N'PWAIT_MASTERDBREADY'), (N'QDS_ASYNC_QUEUE'), (N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP'),
                  (N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP'), (N'QDS_SHUTDOWN_QUEUE'), (N'QUERY_EXECUTION_INDEX_SORT_EVENT_OPEN'),
                  (N'QUERY_TASK_ENQUEUE_MUTEX'), (N'REDO_THREAD_PENDING_WORK'), (N'REQUEST_FOR_DEADLOCK_SEARCH'),
                  (N'RESOURCE_QUEUE'), (N'RESOURCE_SEMAPHORE_MUTEX'), (N'SECURITY_CNG_PROVIDER_MUTEX'), (N'SERVER_IDLE_CHECK'),
                  (N'SLEEP_BUFFERPOOL_HELPLW'), (N'SLEEP_DBSTARTUP'), (N'SLEEP_DCOMSTARTUP'), (N'SLEEP_MASTERDBREADY'),
                  (N'SLEEP_MASTERMDREADY'), (N'SLEEP_MASTERUPGRADED'), (N'SLEEP_MSDBSTARTUP'), (N'SLEEP_PHYSMASTERDBREADY'),
                  (N'SLEEP_SYSTEMTASK'), (N'SLEEP_TASK'), (N'SLEEP_TEMPDBSTARTUP'), (N'SNI_CRITICAL_SECTION'),
                  (N'SNI_HTTP_ACCEPT'), (N'SOS_PROCESS_AFFINITY_MUTEX'), (N'SOS_WORK_DISPATCHER'), (N'SP_SERVER_DIAGNOSTICS_SLEEP'),
                  (N'SQLTRACE_BUFFER_FLUSH'), (N'SQLTRACE_FILE_BUFFER'), (N'SQLTRACE_FILE_READ_IO_COMPLETION'),
                  (N'SQLTRACE_FILE_WRITE_IO_COMPLETION'), (N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP'), (N'SQLTRACE_WAIT_ENTRIES'),
                  (N'UCS_SESSION_REGISTRATION'), (N'VDI_CLIENT_OTHER'), (N'WAIT_FOR_RESULTS'), (N'WAIT_XTP_CKPT_CLOSE'),
                  (N'WAIT_XTP_HOST_WAIT'), (N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG'), (N'WAIT_XTP_RECOVERY'), (N'WAITFOR'),
                  (N'WAITFOR_TASKSHUTDOWN'), (N'WINDOW_AGGREGATES_MULTIPASS'), (N'XE_BUFFERMGR_ALLPROCESSED_EVENT'),
                  (N'XE_DISPATCHER_JOIN'), (N'XE_DISPATCHER_WAIT'), (N'XE_FILE_TARGET_TVF'), (N'XE_LIVE_TARGET_TVF'),
                  (N'XE_TIMER_EVENT');

              SET @tables_initialized = @tables_initialized + 1;

              INSERT INTO
                  config.collection_log
              (
                  collector_name,
                  collection_status,
                  rows_collected,
                  error_message
              )
              VALUES
              (
                  N'ensure_config_tables',
                  N'TABLE_INITIALIZED',
                  ROWCOUNT_BIG(),
                  N'Initialized config.ignored_wait_types with default wait types'
              );
          END;

        /*
        Create config.installation_history
        */
        IF OBJECT_ID(N'config.installation_history', N'U') IS NULL
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Creating config.installation_history table', 0, 1) WITH NOWAIT;
            END;

            CREATE TABLE
                config.installation_history
            (
                installation_id bigint IDENTITY NOT NULL,
                installation_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
                installation_type nvarchar(50) NOT NULL,
                version_number nvarchar(20) NOT NULL,
                installed_by sysname NOT NULL,
                notes nvarchar(2000) NULL,
                CONSTRAINT PK_installation_history PRIMARY KEY CLUSTERED (installation_id) WITH (DATA_COMPRESSION = PAGE)
            );

            CREATE INDEX
                installation_history_time
            ON config.installation_history (installation_time DESC)
            WITH (DATA_COMPRESSION = PAGE);

            SET @tables_created = @tables_created + 1;

            INSERT INTO
                config.collection_log
            (
                collector_name,
                collection_status,
                error_message
            )
            VALUES
            (
                N'ensure_config_tables',
                N'TABLE_CREATED',
                N'Created config.installation_history table'
            );
        END;

        /*
        Log final summary
        */
        INSERT INTO
            config.collection_log
        (
            collector_name,
            collection_status,
            rows_collected,
            duration_ms,
            error_message
        )
        VALUES
        (
            N'ensure_config_tables',
            N'SUCCESS',
            @tables_created,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            N'Created ' + CONVERT(nvarchar(10), @tables_created) + N' tables, initialized ' + CONVERT(nvarchar(10), @tables_initialized) + N' tables'
        );

        IF @debug = 1
        BEGIN
            RAISERROR(N'Config tables verified: %d created, %d initialized', 0, 1, @tables_created, @tables_initialized) WITH NOWAIT;
        END;

    END TRY
    BEGIN CATCH
        SET @error_message = ERROR_MESSAGE();

        /*
        Only log to table if collection_log exists
        */
        IF @log_to_table = 1
        BEGIN
            INSERT INTO
                config.collection_log
            (
                collector_name,
                collection_status,
                duration_ms,
                error_message
            )
            VALUES
            (
                N'ensure_config_tables',
                N'ERROR',
                DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
                @error_message
            );
        END;

        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK;
        END;

        THROW;
    END CATCH;
END;
GO

PRINT 'Config tables self-healing procedure created successfully';
GO
