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

/*
██████╗ ███████╗██████╗ ███████╗ ██████╗ ██████╗ ███╗   ███╗ █████╗ ███╗   ██╗ ██████╗███████╗
██╔══██╗██╔════╝██╔══██╗██╔════╝██╔═══██╗██╔══██╗████╗ ████║██╔══██╗████╗  ██║██╔════╝██╔════╝
██████╔╝█████╗  ██████╔╝█████╗  ██║   ██║██████╔╝██╔████╔██║███████║██╔██╗ ██║██║     █████╗  
██╔═══╝ ██╔══╝  ██╔══██╗██╔══╝  ██║   ██║██╔══██╗██║╚██╔╝██║██╔══██║██║╚██╗██║██║     ██╔══╝  
██║     ███████╗██║  ██║██║     ╚██████╔╝██║  ██║██║ ╚═╝ ██║██║  ██║██║ ╚████║╚██████╗███████╗
╚═╝     ╚══════╝╚═╝  ╚═╝╚═╝      ╚═════╝ ╚═╝  ╚═╝╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝ ╚═════╝╚══════╝

███╗   ███╗ ██████╗ ███╗   ██╗██╗████████╗ ██████╗ ██████╗ 
████╗ ████║██╔═══██╗████╗  ██║██║╚══██╔══╝██╔═══██╗██╔══██╗
██╔████╔██║██║   ██║██╔██╗ ██║██║   ██║   ██║   ██║██████╔╝
██║╚██╔╝██║██║   ██║██║╚██╗██║██║   ██║   ██║   ██║██╔══██╗
██║ ╚═╝ ██║╚██████╔╝██║ ╚████║██║   ██║   ╚██████╔╝██║  ██║
╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝

Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

*/

/*
Create the database if it doesn't exist
*/
IF DB_ID(N'PerformanceMonitor') IS NULL
BEGIN
    DECLARE
        @data_path nvarchar(512) = N'',
        @log_path nvarchar(512) = N'',
        @sql nvarchar(max) = N'',
        @engine_edition integer = CONVERT(integer, SERVERPROPERTY(N'EngineEdition'));

    /*
    Azure SQL Managed Instance (engine edition 8) does not support
    specifying files and filegroups in CREATE DATABASE.
    Use a simple CREATE DATABASE for MI, full spec for on-prem/RDS.
    */
    IF @engine_edition = 8
    BEGIN
        CREATE DATABASE PerformanceMonitor;

        ALTER DATABASE
            PerformanceMonitor
        SET
            AUTO_CREATE_STATISTICS ON,
            AUTO_UPDATE_STATISTICS ON,
            AUTO_UPDATE_STATISTICS_ASYNC ON;

        PRINT 'Created PerformanceMonitor database (Managed Instance)';
    END;
    ELSE
    BEGIN
        /*
        Get the default data and log directories from instance properties
        */
        SELECT
            @data_path =
                CONVERT
                (
                    nvarchar(512),
                    SERVERPROPERTY(N'InstanceDefaultDataPath')
                ) +
                N'PerformanceMonitor.mdf',
            @log_path =
                CONVERT
                (
                    nvarchar(512),
                    SERVERPROPERTY(N'InstanceDefaultLogPath')
                ) +
                N'PerformanceMonitor_log.ldf';

        /*
        Build and execute CREATE DATABASE statement with proper file paths
        */
        SET @sql = N'
        CREATE DATABASE
            PerformanceMonitor
        ON PRIMARY
        (
            NAME = N''PerformanceMonitor'',
            FILENAME = N''' + @data_path + N''',
            SIZE = 1024MB,
            MAXSIZE = UNLIMITED,
            FILEGROWTH = 1024MB
        )
        LOG ON
        (
            NAME = N''PerformanceMonitor_log'',
            FILENAME = N''' + @log_path + N''',
            SIZE = 256MB,
            MAXSIZE = UNLIMITED,
            FILEGROWTH = 64MB
        );';

        EXECUTE sys.sp_executesql
            @sql;

        ALTER DATABASE
            PerformanceMonitor
        SET
            RECOVERY SIMPLE,
            AUTO_CREATE_STATISTICS ON,
            AUTO_UPDATE_STATISTICS ON,
            AUTO_UPDATE_STATISTICS_ASYNC ON;

        PRINT 'Created PerformanceMonitor database at ' + @data_path;
    END;
END;
ELSE
BEGIN
    PRINT 'PerformanceMonitor database already exists';
END;
GO

USE PerformanceMonitor;
GO

/*
Create schemas for organization
*/
IF NOT EXISTS (SELECT 1/0 FROM sys.schemas AS s WHERE s.name = N'collect')
BEGIN
    EXECUTE(N'CREATE SCHEMA collect AUTHORIZATION dbo;');
    PRINT 'Created collect schema';
END;

IF NOT EXISTS (SELECT 1/0 FROM sys.schemas AS s WHERE s.name = N'analyze')
BEGIN
    EXECUTE(N'CREATE SCHEMA analyze AUTHORIZATION dbo;');
    PRINT 'Created analyze schema';
END;

IF NOT EXISTS (SELECT 1/0 FROM sys.schemas AS s WHERE s.name = N'config')
BEGIN
    EXECUTE(N'CREATE SCHEMA config AUTHORIZATION dbo;');
    PRINT 'Created config schema';
END;

IF NOT EXISTS (SELECT 1/0 FROM sys.schemas AS s WHERE s.name = N'report')
BEGIN
    EXECUTE(N'CREATE SCHEMA report AUTHORIZATION dbo;');
    PRINT 'Created report schema';
END;
GO

/*
Create core configuration tables
*/

/*
Server information history - tracks changes to server configuration over time
Logs when SQL version changes (patches/upgrades), edition changes, hardware changes, etc.
Only collected after server restarts (when sqlserver_start_time changes)
*/
IF OBJECT_ID(N'config.server_info_history', N'U') IS NULL
BEGIN
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
        environment_type nvarchar(50) NOT NULL, /*OnPrem, AzureMI, AzureDB, AWSRDS*/
        CONSTRAINT
            PK_server_info_history
        PRIMARY KEY CLUSTERED
            (collection_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    CREATE INDEX
        server_info_history_time_server
    ON config.server_info_history
    (
        collection_time DESC,
        server_name
    )
    WITH
        (DATA_COMPRESSION = PAGE);

    CREATE INDEX
        server_info_history_start_time
    ON config.server_info_history
        (sqlserver_start_time DESC)
    WITH
        (DATA_COMPRESSION = PAGE);

    PRINT 'Created config.server_info_history table';
END;

/*
Collection log - tracks all collection runs
*/
IF OBJECT_ID(N'config.collection_log', N'U') IS NULL
BEGIN
    CREATE TABLE
        config.collection_log
    (
        log_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL 
            DEFAULT SYSDATETIME(),
        collector_name nvarchar(100) NOT NULL,
        collection_status nvarchar(20) NOT NULL, /*SUCCESS, ERROR*/
        rows_collected integer NOT NULL DEFAULT 0,
        duration_ms integer NOT NULL DEFAULT 0,
        error_message nvarchar(4000) NULL,
        CONSTRAINT 
            PK_collection_log 
        PRIMARY KEY CLUSTERED 
            (log_id) 
        WITH 
            (DATA_COMPRESSION = PAGE)
    );

        CREATE INDEX
            collection_log_time_collector
        ON config.collection_log
        (
            collection_time,
            collector_name
        )
        WITH 
            (DATA_COMPRESSION = PAGE);

        CREATE INDEX 
            collection_log_status 
        ON config.collection_log 
            (collection_status) 
        WITH 
            (DATA_COMPRESSION = PAGE);
    
    PRINT 'Created config.collection_log table';
END;


PRINT 'Database installation completed successfully';

/*
Collection schedule configuration table
Defines when each collector should run, frequency, and constraints
Enables flexible scheduling without hardcoded job frequencies
*/
IF OBJECT_ID(N'config.collection_schedule', N'U') IS NULL
BEGIN
    CREATE TABLE
        config.collection_schedule
    (
        schedule_id integer IDENTITY NOT NULL,
        collector_name sysname NOT NULL,
        [enabled] bit NOT NULL
            DEFAULT CONVERT(bit, 'true'),
        frequency_minutes integer NOT NULL
            DEFAULT 15,
        last_run_time datetime2(7) NULL,
        next_run_time datetime2(7) NULL,
        max_duration_minutes integer NOT NULL
            DEFAULT 5,
        retention_days integer NOT NULL
            DEFAULT 30,
        collect_query bit NOT NULL
            DEFAULT CONVERT(bit, 'true'),
        collect_plan bit NOT NULL
            DEFAULT CONVERT(bit, 'true'),
        [description] nvarchar(500) NULL,
        created_date datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        modified_date datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        CONSTRAINT
            PK_collection_schedule
        PRIMARY KEY CLUSTERED
            (schedule_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );

        CREATE UNIQUE INDEX
            UQ_collection_schedule_collector
        ON config.collection_schedule
            (collector_name)
        WITH
            (DATA_COMPRESSION = PAGE);

        CREATE INDEX
            collection_schedule_next_run
        ON config.collection_schedule
        (
            next_run_time,
            enabled
        )
        WITH
            (DATA_COMPRESSION = PAGE);

        CREATE INDEX
            collection_schedule_enabled
        ON config.collection_schedule
        (
            enabled,
            collector_name
        )
        WITH
            (DATA_COMPRESSION = PAGE);

    PRINT 'Created config.collection_schedule table';
END;

/*
Add collect_query and collect_plan columns for existing installations
Controls whether collectors store query text and execution plans
Both default to enabled (1) for backwards compatibility
*/
IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'config.collection_schedule')
    AND   name = N'collect_query'
)
BEGIN
    ALTER TABLE
        config.collection_schedule
    ADD collect_query bit NOT NULL
        CONSTRAINT DF_collection_schedule_collect_query
        DEFAULT CONVERT(bit, 'true');

    PRINT 'Added collect_query column to config.collection_schedule';
END;

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.columns
    WHERE object_id = OBJECT_ID(N'config.collection_schedule')
    AND   name = N'collect_plan'
)
BEGIN
    ALTER TABLE
        config.collection_schedule
    ADD collect_plan bit NOT NULL
        CONSTRAINT DF_collection_schedule_collect_plan
        DEFAULT CONVERT(bit, 'true');

    PRINT 'Added collect_plan column to config.collection_schedule';
END;

/*
Critical issues table
Logs significant performance problems detected by collectors and analysis procedures
Provides high-level alerting and triage view for DBAs
*/
IF OBJECT_ID(N'config.critical_issues', N'U') IS NULL
BEGIN
    CREATE TABLE
        config.critical_issues
    (
        issue_id bigint IDENTITY NOT NULL,
        log_date datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        severity nvarchar(20) NOT NULL,
        problem_area nvarchar(100) NOT NULL,
        source_collector sysname NOT NULL,
        affected_database sysname NULL,
        message nvarchar(max) NOT NULL,
        investigate_query nvarchar(max) NULL,
        threshold_value decimal(38,2) NULL,
        threshold_limit decimal(38,2) NULL,
        CONSTRAINT
            PK_critical_issues
        PRIMARY KEY CLUSTERED
            (issue_id)
        WITH
            (DATA_COMPRESSION = PAGE),
        CONSTRAINT
            CK_critical_issues_severity
        CHECK
            (severity IN (N'CRITICAL', N'WARNING', N'INFO'))
    );

        CREATE INDEX
            critical_issues_log_date
        ON config.critical_issues
            (log_date DESC)
        WITH
            (DATA_COMPRESSION = PAGE);
        
        CREATE INDEX
            critical_issues_severity
        ON config.critical_issues
        (
            severity,
            log_date DESC
        )
        WITH
            (DATA_COMPRESSION = PAGE);
        
        CREATE INDEX
            critical_issues_problem_area
        ON config.critical_issues
        (
            problem_area,
            log_date DESC
        )
        WITH
            (DATA_COMPRESSION = PAGE);
        
        CREATE INDEX
            critical_issues_source_collector
        ON config.critical_issues
        (
            source_collector,
            log_date DESC
        )
        WITH
            (DATA_COMPRESSION = PAGE);
        
        CREATE INDEX
            critical_issues_affected_database
        ON config.critical_issues
            (affected_database, log_date DESC)
        WHERE affected_database IS NOT NULL
        WITH
            (DATA_COMPRESSION = PAGE);

    PRINT 'Created config.critical_issues table';
END;

/*
Server Configuration History Table
Tracks changes to SQL Server instance-level configuration settings from sys.configurations
*/
IF OBJECT_ID(N'config.server_configuration_history', N'U') IS NULL
BEGIN
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
    ON config.server_configuration_history
    (
        collection_time DESC,
        configuration_name
    )
    WITH
        (DATA_COMPRESSION = PAGE);
    
    CREATE INDEX
        server_configuration_history_config_id
    ON config.server_configuration_history
    (
        configuration_id,
        collection_time DESC
    )
    WITH
        (DATA_COMPRESSION = PAGE);

    PRINT 'Created config.server_configuration_history table';
END;

/*
Trace Flags History Table
Tracks enabled trace flags from DBCC TRACESTATUS
*/
IF OBJECT_ID(N'config.trace_flags_history', N'U') IS NULL
BEGIN
    CREATE TABLE
        config.trace_flags_history
    (
        collection_id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
        trace_flag integer NOT NULL,
        status bit NOT NULL,
        is_global bit NOT NULL,
        is_session bit NOT NULL,
        CONSTRAINT PK_trace_flags_history PRIMARY KEY CLUSTERED (collection_id) WITH (DATA_COMPRESSION = PAGE));

    CREATE INDEX
        trace_flags_history_time_flag
    ON config.trace_flags_history
    (
        collection_time DESC,
        trace_flag
    )
    WITH
        (DATA_COMPRESSION = PAGE);

    
    PRINT 'Created config.trace_flags_history table';
END;

/*
Database Configuration History Table
Tracks changes to database-level settings from sys.databases and sys.database_scoped_configurations
Uses setting_type to distinguish between database properties and scoped configurations
*/
IF OBJECT_ID(N'config.database_configuration_history', N'U') IS NULL
BEGIN
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
    ON config.database_configuration_history
    (
        collection_time DESC,
        database_name
    )
    WITH
        (DATA_COMPRESSION = PAGE);
    
    CREATE INDEX
        database_configuration_history_db_setting
    ON config.database_configuration_history
    (
        database_id,
        setting_type,
        setting_name,
        collection_time DESC
    )
    WITH
        (DATA_COMPRESSION = PAGE);


    PRINT 'Created config.database_configuration_history table';
END;
GO

/*
Create ignored wait types table
User-configurable table for wait types to exclude from collection
Used by wait_stats_collector and waiting_tasks_collector
*/
IF OBJECT_ID(N'config.ignored_wait_types', N'U') IS NULL
BEGIN
    CREATE TABLE
        config.ignored_wait_types
    (
        wait_type_id integer IDENTITY NOT NULL,
        wait_type sysname NOT NULL,
        description nvarchar(500) NULL,
        is_enabled bit NOT NULL DEFAULT 1,
        created_date datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
        modified_date datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
        CONSTRAINT
            PK_ignored_wait_types
        PRIMARY KEY CLUSTERED
            (wait_type_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    CREATE UNIQUE INDEX
        UQ_ignored_wait_types_wait_type
    ON config.ignored_wait_types
        (wait_type)
    WITH
        (DATA_COMPRESSION = PAGE);

    /*
    Initialize with default wait types to ignore
    Matches sp_HealthParser exclusion list for consistency
    */
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

    PRINT 'Created config.ignored_wait_types table with default values';
END;
GO

/*
Create installation history table
*/
IF OBJECT_ID(N'config.installation_history', N'U') IS NULL
BEGIN
    CREATE TABLE
        config.installation_history
    (
        installation_id integer IDENTITY NOT NULL,
        installation_date datetime2(7) NOT NULL DEFAULT SYSDATETIME(),
        installer_version nvarchar(50) NOT NULL,
        installer_info_version nvarchar(100) NULL,
        sql_server_version nvarchar(255) NOT NULL,
        sql_server_edition nvarchar(255) NOT NULL,
        installation_type nvarchar(20) NOT NULL, /*INSTALL, UPGRADE, REINSTALL*/
        previous_version nvarchar(50) NULL,
        installation_status nvarchar(20) NOT NULL, /*SUCCESS, FAILED, PARTIAL*/
        files_executed integer NULL,
        files_failed integer NULL,
        installation_duration_ms integer NULL,
        installation_notes nvarchar(max) NULL,
        CONSTRAINT 
            PK_installation_history
        PRIMARY KEY CLUSTERED 
            (installation_id)
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created config.installation_history table';
END;
GO

/*
Create view to show current installed version
*/
CREATE OR ALTER VIEW 
    config.current_version
AS
SELECT TOP (1)
    installer_version,
    installer_info_version,
    installation_date,
    installation_type,
    sql_server_version,
    sql_server_edition
FROM config.installation_history
WHERE installation_status = N'SUCCESS'
ORDER BY
    installation_date DESC;
GO

/*
=============================================================================
SERVER CONFIGURATION
Configure SQL Server settings required for monitoring
=============================================================================
*/

/*
Configure blocked process threshold for blocked process detection
This enables the blocked_process_report Extended Events event
Default value is 0 (disabled), we set it to 5 seconds
*/
BEGIN TRY
    DECLARE
        @blocked_threshold_current integer,
        @show_advanced_current integer,
        @show_advanced_reset integer;

    /*Check current values*/
    SELECT
        @show_advanced_current = CONVERT(integer, c.value),
        @show_advanced_reset = CONVERT(integer, c.value)
    FROM sys.configurations AS c
    WHERE c.name = N'show advanced options';

    SELECT
        @blocked_threshold_current = CONVERT(integer, c.value)
    FROM sys.configurations AS c
    WHERE c.name = N'blocked process threshold (s)';

    /*Enable show advanced options if not already enabled*/
    IF @show_advanced_current = 0
    BEGIN
        PRINT 'Enabling show advanced options...';
        EXECUTE sys.sp_configure N'show advanced options', 1;
        RECONFIGURE;
    END;

    /*Configure blocked process threshold if not already set*/
    IF @blocked_threshold_current = 0
    BEGIN
        PRINT 'Configuring blocked process threshold to 5 seconds...';
        EXECUTE sys.sp_configure N'blocked process threshold (s)', 5;
        RECONFIGURE;
        PRINT 'Blocked process threshold configured successfully';
    END;

    /*Reset if required*/
    IF @show_advanced_reset = 0
    BEGIN
        PRINT 'Disabling show advanced options...';
        EXECUTE sys.sp_configure N'show advanced options', 0;
        RECONFIGURE;
    END;
    ELSE
    BEGIN
        PRINT 'Blocked process threshold already configured: ' + CONVERT(varchar(10), @blocked_threshold_current) + ' seconds';
    END;

    PRINT 'Server configuration complete';
END TRY
BEGIN CATCH
    PRINT 'Note: Server configuration skipped (insufficient permissions for sp_configure/RECONFIGURE).';
    PRINT 'Blocked process threshold must be configured manually if needed.';
END CATCH;
GO
