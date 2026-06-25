/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.11.0 to 3.0.0
WS5: surface Lock Pages in Memory (LPIM), Instant File Initialization (IFI),
and SQL Server memory dumps as advise-only server-health recommendations.

Adds three nullable columns to collect.server_properties so the
server_properties_collector can persist these values for the analysis path
(CONFIG_LPIM_DISABLED / CONFIG_IFI_DISABLED / SERVER_MEMORY_DUMPS facts).

Idempotent and partial-failure safe: each column gets its own guarded ALTER,
so a re-run (or a re-run after an interrupted run) converges to the target
shape. Matches the IF NOT EXISTS (sys.columns) ... ALTER ADD style used in
install/02_create_tables.sql.
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

IF OBJECT_ID(N'collect.server_properties', N'U') IS NULL
BEGIN
    PRINT 'collect.server_properties does not exist - no action taken';
END;
ELSE
BEGIN
    IF NOT EXISTS
    (
        SELECT
            1/0
        FROM sys.columns AS c
        WHERE c.object_id = OBJECT_ID(N'collect.server_properties')
        AND   c.name = N'lock_pages_in_memory'
    )
    BEGIN
        ALTER TABLE collect.server_properties
            ADD lock_pages_in_memory bit NULL;

        PRINT 'Added lock_pages_in_memory to collect.server_properties';
    END;
    ELSE
    BEGIN
        PRINT 'collect.server_properties.lock_pages_in_memory already exists - no action taken';
    END;

    IF NOT EXISTS
    (
        SELECT
            1/0
        FROM sys.columns AS c
        WHERE c.object_id = OBJECT_ID(N'collect.server_properties')
        AND   c.name = N'instant_file_initialization_enabled'
    )
    BEGIN
        ALTER TABLE collect.server_properties
            ADD instant_file_initialization_enabled bit NULL;

        PRINT 'Added instant_file_initialization_enabled to collect.server_properties';
    END;
    ELSE
    BEGIN
        PRINT 'collect.server_properties.instant_file_initialization_enabled already exists - no action taken';
    END;

    IF NOT EXISTS
    (
        SELECT
            1/0
        FROM sys.columns AS c
        WHERE c.object_id = OBJECT_ID(N'collect.server_properties')
        AND   c.name = N'memory_dump_count'
    )
    BEGIN
        ALTER TABLE collect.server_properties
            ADD memory_dump_count integer NULL;

        PRINT 'Added memory_dump_count to collect.server_properties';
    END;
    ELSE
    BEGIN
        PRINT 'collect.server_properties.memory_dump_count already exists - no action taken';
    END;
END;
GO
