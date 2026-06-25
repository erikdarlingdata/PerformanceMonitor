/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

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

/*
Health Parser Tables
These tables store parsed output from sp_HealthParser (system_health extended events)
Tables are populated by the system_health_collector procedure
*/

IF OBJECT_ID(N'collect.HealthParser_CPUTasks', N'U') IS NULL
BEGIN
    CREATE TABLE
        collect.HealthParser_CPUTasks
    (
        id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        event_time datetime2(7) NULL,
        state nvarchar(256) NULL,
        maxWorkers bigint NULL,
        workersCreated bigint NULL,
        workersIdle bigint NULL,
        tasksCompletedWithinInterval bigint NULL,
        pendingTasks bigint NULL,
        oldestPendingTaskWaitingTime bigint NULL,
        hasUnresolvableDeadlockOccurred bit NULL,
        hasDeadlockedSchedulersOccurred bit NULL,
        didBlockingOccur bit NULL,
        PRIMARY KEY CLUSTERED
        (
            collection_time ASC,
            id ASC
        )
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.HealthParser_CPUTasks table';
END;
GO

IF OBJECT_ID(N'collect.HealthParser_IOIssues', N'U') IS NULL
BEGIN
    CREATE TABLE
        collect.HealthParser_IOIssues
    (
        id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        event_time datetime2(7) NULL,
        state nvarchar(256) NULL,
        ioLatchTimeouts bigint NULL,
        intervalLongIos bigint NULL,
        totalLongIos bigint NULL,
        longestPendingRequests_duration_ms nvarchar(30) NULL,
        longestPendingRequests_filePath nvarchar(500) NULL,
        PRIMARY KEY CLUSTERED
        (
            collection_time ASC,
            id ASC
        )
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.HealthParser_IOIssues table';
END;
GO

IF OBJECT_ID(N'collect.HealthParser_MemoryBroker', N'U') IS NULL
BEGIN
    CREATE TABLE
        collect.HealthParser_MemoryBroker
    (
        id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        event_time datetime2(7) NULL,
        broker_id bigint NULL,
        pool_metadata_id bigint NULL,
        delta_time bigint NULL,
        memory_ratio bigint NULL,
        new_target bigint NULL,
        overall bigint NULL,
        rate bigint NULL,
        currently_predicated bigint NULL,
        currently_allocated bigint NULL,
        previously_allocated bigint NULL,
        broker nvarchar(256) NULL,
        notification nvarchar(256) NULL,
        PRIMARY KEY CLUSTERED
        (
            collection_time ASC,
            id ASC
        )
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.HealthParser_MemoryBroker table';
END;
ELSE
BEGIN
    /*
    Alter existing columns from integer to bigint to match
    sp_HealthParser changes (integer overflow fix for SQL 2016)
    */
    IF EXISTS
    (
        SELECT
            1/0
        FROM sys.columns AS c
        JOIN sys.types AS t
          ON t.user_type_id = c.user_type_id
        WHERE c.object_id = OBJECT_ID(N'collect.HealthParser_MemoryBroker')
        AND   c.name = N'broker_id'
        AND   t.name = N'int'
    )
    BEGIN
        ALTER TABLE
            collect.HealthParser_MemoryBroker
        ALTER COLUMN
            broker_id bigint NULL;

        ALTER TABLE
            collect.HealthParser_MemoryBroker
        ALTER COLUMN
            pool_metadata_id bigint NULL;

        ALTER TABLE
            collect.HealthParser_MemoryBroker
        ALTER COLUMN
            memory_ratio bigint NULL;

        PRINT 'Altered collect.HealthParser_MemoryBroker: broker_id, pool_metadata_id, memory_ratio changed from integer to bigint';
    END;
END;
GO

IF OBJECT_ID(N'collect.HealthParser_MemoryConditions', N'U') IS NULL
BEGIN
    CREATE TABLE
        collect.HealthParser_MemoryConditions
    (
        id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        event_time datetime2(7) NULL,
        lastNotification nvarchar(128) NULL,
        outOfMemoryExceptions bigint NULL,
        isAnyPoolOutOfMemory bit NULL,
        processOutOfMemoryPeriod bigint NULL,
        name nvarchar(128) NULL,
        available_physical_memory_gb bigint NULL,
        available_virtual_memory_gb bigint NULL,
        available_paging_file_gb bigint NULL,
        working_set_gb bigint NULL,
        percent_of_committed_memory_in_ws bigint NULL,
        page_faults bigint NULL,
        system_physical_memory_high bigint NULL,
        system_physical_memory_low bigint NULL,
        process_physical_memory_low bigint NULL,
        process_virtual_memory_low bigint NULL,
        vm_reserved_gb bigint NULL,
        vm_committed_gb bigint NULL,
        locked_pages_allocated bigint NULL,
        large_pages_allocated bigint NULL,
        emergency_memory_gb bigint NULL,
        emergency_memory_in_use_gb bigint NULL,
        target_committed_gb bigint NULL,
        current_committed_gb bigint NULL,
        pages_allocated bigint NULL,
        pages_reserved bigint NULL,
        pages_free bigint NULL,
        pages_in_use bigint NULL,
        page_alloc_potential bigint NULL,
        numa_growth_phase bigint NULL,
        last_oom_factor bigint NULL,
        last_os_error bigint NULL,
        PRIMARY KEY CLUSTERED
        (
            collection_time ASC,
            id ASC
        )
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.HealthParser_MemoryConditions table';
END;
GO

IF OBJECT_ID(N'collect.HealthParser_MemoryNodeOOM', N'U') IS NULL
BEGIN
    CREATE TABLE
        collect.HealthParser_MemoryNodeOOM
    (
        id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        event_time datetime2(7) NULL,
        node_id bigint NULL,
        memory_node_id bigint NULL,
        memory_utilization_pct bigint NULL,
        total_physical_memory_kb bigint NULL,
        available_physical_memory_kb bigint NULL,
        total_page_file_kb bigint NULL,
        available_page_file_kb bigint NULL,
        total_virtual_address_space_kb bigint NULL,
        available_virtual_address_space_kb bigint NULL,
        target_kb bigint NULL,
        reserved_kb bigint NULL,
        committed_kb bigint NULL,
        shared_committed_kb numeric(38, 0) NULL,
        awe_kb bigint NULL,
        pages_kb bigint NULL,
        failure_type nvarchar(256) NULL,
        failure_value integer NULL,
        resources integer NULL,
        factor_text nvarchar(256) NULL,
        factor_value integer NULL,
        last_error integer NULL,
        pool_metadata_id integer NULL,
        is_process_in_job nvarchar(10) NULL,
        is_system_physical_memory_high nvarchar(10) NULL,
        is_system_physical_memory_low nvarchar(10) NULL,
        is_process_physical_memory_low nvarchar(10) NULL,
        is_process_virtual_memory_low nvarchar(10) NULL,
        PRIMARY KEY CLUSTERED
        (
            collection_time ASC,
            id ASC
        )
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.HealthParser_MemoryNodeOOM table';
END;
ELSE
BEGIN
    /*
    Alter existing columns from integer to bigint to match
    sp_HealthParser changes (integer overflow fix for SQL 2016)
    */
    IF EXISTS
    (
        SELECT
            1/0
        FROM sys.columns AS c
        JOIN sys.types AS t
          ON t.user_type_id = c.user_type_id
        WHERE c.object_id = OBJECT_ID(N'collect.HealthParser_MemoryNodeOOM')
        AND   c.name = N'node_id'
        AND   t.name = N'int'
    )
    BEGIN
        ALTER TABLE
            collect.HealthParser_MemoryNodeOOM
        ALTER COLUMN
            node_id bigint NULL;

        ALTER TABLE
            collect.HealthParser_MemoryNodeOOM
        ALTER COLUMN
            memory_node_id bigint NULL;

        ALTER TABLE
            collect.HealthParser_MemoryNodeOOM
        ALTER COLUMN
            memory_utilization_pct bigint NULL;

        PRINT 'Altered collect.HealthParser_MemoryNodeOOM: node_id, memory_node_id, memory_utilization_pct changed from integer to bigint';
    END;
END;
GO

IF OBJECT_ID(N'collect.HealthParser_SchedulerIssues', N'U') IS NULL
BEGIN
    CREATE TABLE
        collect.HealthParser_SchedulerIssues
    (
        id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        event_time datetime2(7) NULL,
        scheduler_id integer NULL,
        cpu_id integer NULL,
        status nvarchar(256) NULL,
        is_online bit NULL,
        is_runnable bit NULL,
        is_running bit NULL,
        non_yielding_time_ms nvarchar(30) NULL,
        thread_quantum_ms nvarchar(30) NULL,
        PRIMARY KEY CLUSTERED
        (
            collection_time ASC,
            id ASC
        )
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.HealthParser_SchedulerIssues table';
END;
GO

IF OBJECT_ID(N'collect.HealthParser_SevereErrors', N'U') IS NULL
BEGIN
    CREATE TABLE
        collect.HealthParser_SevereErrors
    (
        id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        event_time datetime2(7) NULL,
        error_number integer NULL,
        severity integer NULL,
        state integer NULL,
        message nvarchar(max) NULL,
        database_name sysname NULL,
        database_id integer NULL,
        PRIMARY KEY CLUSTERED
        (
            collection_time ASC,
            id ASC
        )
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.HealthParser_SevereErrors table';
END;
GO

IF OBJECT_ID(N'collect.HealthParser_SignificantWaits', N'U') IS NULL
BEGIN
    CREATE TABLE
        collect.HealthParser_SignificantWaits
    (
        id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        event_time datetime2(7) NULL,
        wait_type nvarchar(60) NULL,
        duration_ms nvarchar(30) NULL,
        signal_duration_ms nvarchar(30) NULL,
        wait_resource nvarchar(256) NULL,
        query_text xml NULL,
        session_id integer NULL,
        PRIMARY KEY CLUSTERED
        (
            collection_time ASC,
            id ASC
        )
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.HealthParser_SignificantWaits table';
END;
GO

IF OBJECT_ID(N'collect.HealthParser_SystemHealth', N'U') IS NULL
BEGIN
    CREATE TABLE
        collect.HealthParser_SystemHealth
    (
        id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        event_time datetime2(7) NULL,
        state nvarchar(256) NULL,
        spinlockBackoffs bigint NULL,
        sickSpinlockType nvarchar(256) NULL,
        sickSpinlockTypeAfterAv nvarchar(256) NULL,
        latchWarnings bigint NULL,
        isAccessViolationOccurred bigint NULL,
        writeAccessViolationCount bigint NULL,
        totalDumpRequests bigint NULL,
        intervalDumpRequests bigint NULL,
        nonYieldingTasksReported bigint NULL,
        pageFaults bigint NULL,
        systemCpuUtilization bigint NULL,
        sqlCpuUtilization bigint NULL,
        BadPagesDetected bigint NULL,
        BadPagesFixed bigint NULL,
        PRIMARY KEY CLUSTERED
        (
            collection_time ASC,
            id ASC
        )
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.HealthParser_SystemHealth table';
END;
GO

IF OBJECT_ID(N'collect.HealthParser_WaitsByCount', N'U') IS NULL
BEGIN
    CREATE TABLE
        collect.HealthParser_WaitsByCount
    (
        id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        event_time_rounded datetime2(7) NULL,
        wait_type nvarchar(60) NULL,
        waits nvarchar(30) NULL,
        average_wait_time_ms nvarchar(30) NULL,
        max_wait_time_ms nvarchar(30) NULL,
        PRIMARY KEY CLUSTERED
        (
            collection_time ASC,
            id ASC
        )
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.HealthParser_WaitsByCount table';
END;
GO

IF OBJECT_ID(N'collect.HealthParser_WaitsByDuration', N'U') IS NULL
BEGIN
    CREATE TABLE
        collect.HealthParser_WaitsByDuration
    (
        id bigint IDENTITY NOT NULL,
        collection_time datetime2(7) NOT NULL
            DEFAULT SYSDATETIME(),
        event_time_rounded datetime2(7) NULL,
        wait_type nvarchar(60) NULL,
        average_wait_time_ms nvarchar(30) NULL,
        max_wait_time_ms nvarchar(30) NULL,
        PRIMARY KEY CLUSTERED
        (
            collection_time ASC,
            id ASC
        )
        WITH
            (DATA_COMPRESSION = PAGE)
    );

    PRINT 'Created collect.HealthParser_WaitsByDuration table';
END;
GO

PRINT '';
PRINT 'Health Parser tables created successfully';
PRINT 'These tables store parsed output from sp_HealthParser (system_health extended events)';
GO
