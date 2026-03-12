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

/*******************************************************************************
Collector: server_properties_collector
Purpose: Captures server edition, version, CPU/memory hardware metadata, and
         Enterprise feature usage for license audit and FinOps cost attribution.
Collection Type: Deduplication snapshot (skip if unchanged)
Target Table: collect.server_properties
Frequency: Daily (1440 minutes)
Dependencies: SERVERPROPERTY, sys.dm_os_sys_info, sys.dm_db_persisted_sku_features
Notes: Enterprise features enumeration gated by DMV existence.
       Uses FOR XML PATH for SQL 2016, STRING_AGG for 2017+.
       Azure SQL DB uses DATABASEPROPERTYEX for service objective.
*******************************************************************************/

IF OBJECT_ID(N'collect.server_properties_collector', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.server_properties_collector AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.server_properties_collector
(
    @debug bit = 0 /*Print debugging information*/
)
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @rows_collected bigint = 0,
        @start_time datetime2(7) = SYSDATETIME(),
        @error_message nvarchar(4000),
        @engine_edition integer =
            CONVERT(integer, SERVERPROPERTY(N'EngineEdition')),
        @major_version integer;

    /*
    Parse major version for feature gating
    */
    SET @major_version =
        CONVERT
        (
            integer,
            PARSENAME
            (
                CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')),
                4
            )
        );

    BEGIN TRY
        /*
        Ensure target table exists
        */
        IF OBJECT_ID(N'collect.server_properties', N'U') IS NULL
        BEGIN
            INSERT INTO
                config.collection_log
            (
                collection_time,
                collector_name,
                collection_status,
                rows_collected,
                duration_ms,
                error_message
            )
            VALUES
            (
                @start_time,
                N'server_properties_collector',
                N'TABLE_MISSING',
                0,
                0,
                N'Table collect.server_properties does not exist, calling ensure procedure'
            );

            EXECUTE config.ensure_collection_table
                @table_name = N'server_properties',
                @debug = @debug;

            IF OBJECT_ID(N'collect.server_properties', N'U') IS NULL
            BEGIN
                RAISERROR(N'Table collect.server_properties still missing after ensure procedure', 16, 1);
                RETURN;
            END;
        END;

        /*
        Collect enterprise features in use across databases
        sys.dm_db_persisted_sku_features lists Enterprise features per database
        Not available on Azure SQL DB (engine edition 5)
        */
        DECLARE
            @enterprise_features nvarchar(max) = NULL;

        IF @engine_edition <> 5
        AND OBJECT_ID(N'sys.dm_db_persisted_sku_features', N'V') IS NOT NULL
        BEGIN
            CREATE TABLE
                #sku_features
            (
                database_name sysname NOT NULL,
                feature_name sysname NOT NULL
            );

            DECLARE
                @db_name sysname,
                @sql nvarchar(max);

            DECLARE sku_cursor CURSOR LOCAL FAST_FORWARD FOR
                SELECT
                    d.name
                FROM sys.databases AS d
                WHERE d.state = 0 /*ONLINE only — skip RESTORING databases (mirroring/AG secondary)*/
                AND   d.database_id > 4 /*Skip system databases*/
                AND   HAS_DBACCESS(d.name) = 1
                ORDER BY
                    d.database_id;

            OPEN sku_cursor;
            FETCH NEXT FROM sku_cursor INTO @db_name;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                BEGIN TRY
                    SET @sql = N'
                    SELECT
                        database_name = ' + QUOTENAME(@db_name, N'''') + N',
                        feature_name = f.feature_name
                    FROM ' + QUOTENAME(@db_name) + N'.sys.dm_db_persisted_sku_features AS f;';

                    INSERT INTO #sku_features
                    (
                        database_name,
                        feature_name
                    )
                    EXECUTE sys.sp_executesql @sql;
                END TRY
                BEGIN CATCH
                    /*Skip databases we cannot query*/
                    IF @debug = 1
                    BEGIN
                        DECLARE @sku_err nvarchar(4000) = ERROR_MESSAGE();
                        RAISERROR(N'SKU features error for [%s]: %s', 0, 1, @db_name, @sku_err) WITH NOWAIT;
                    END;
                END CATCH;

                FETCH NEXT FROM sku_cursor INTO @db_name;
            END;

            CLOSE sku_cursor;
            DEALLOCATE sku_cursor;

            /*
            Aggregate features into comma-delimited string
            Format: "DatabaseName: Feature1, Feature2; DatabaseName2: Feature3"
            Use FOR XML PATH (works on SQL 2016+)
            */
            SELECT
                @enterprise_features =
                    STUFF
                    (
                        (
                            SELECT
                                N'; ' + sf.database_name + N': ' + sf.feature_name
                            FROM #sku_features AS sf
                            ORDER BY
                                sf.database_name,
                                sf.feature_name
                            FOR XML PATH(N''), TYPE
                        ).value(N'.', N'nvarchar(max)'),
                        1,
                        2,
                        N''
                    );

            DROP TABLE #sku_features;
        END;

        /*
        Deduplication: check if anything changed since last collection
        */
        DECLARE
            @current_hash binary(32),
            @last_hash binary(32);

        SELECT
            @current_hash =
                HASHBYTES
                (
                    N'SHA2_256',
                    CONCAT
                    (
                        CONVERT(nvarchar(128), SERVERPROPERTY(N'Edition')), N'|',
                        CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')), N'|',
                        CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductLevel')), N'|',
                        @engine_edition, N'|',
                        (SELECT osi.cpu_count FROM sys.dm_os_sys_info AS osi), N'|',
                        (SELECT osi.physical_memory_kb FROM sys.dm_os_sys_info AS osi), N'|',
                        ISNULL(@enterprise_features, N'')
                    )
                );

        SELECT TOP (1)
            @last_hash = sp.row_hash
        FROM collect.server_properties AS sp
        ORDER BY
            sp.collection_time DESC;

        IF @current_hash = @last_hash
        BEGIN
            IF @debug = 1
            BEGIN
                RAISERROR(N'Server properties unchanged since last collection, skipping', 0, 1) WITH NOWAIT;
            END;

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
                N'server_properties_collector',
                N'SKIPPED',
                0,
                DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
                N'Properties unchanged since last collection'
            );

            RETURN;
        END;

        /*
        Insert new row
        */
        INSERT INTO
            collect.server_properties
        (
            collection_time,
            server_name,
            edition,
            product_version,
            product_level,
            product_update_level,
            engine_edition,
            cpu_count,
            hyperthread_ratio,
            physical_memory_mb,
            socket_count,
            cores_per_socket,
            is_hadr_enabled,
            is_clustered,
            enterprise_features,
            service_objective,
            row_hash
        )
        SELECT
            collection_time = @start_time,
            server_name =
                CONVERT(sysname, SERVERPROPERTY(N'ServerName')),
            edition =
                CONVERT(sysname, SERVERPROPERTY(N'Edition')),
            product_version =
                CONVERT(sysname, SERVERPROPERTY(N'ProductVersion')),
            product_level =
                CONVERT(sysname, SERVERPROPERTY(N'ProductLevel')),
            product_update_level =
                CONVERT(sysname, SERVERPROPERTY(N'ProductUpdateLevel')),
            engine_edition = @engine_edition,
            cpu_count = osi.cpu_count,
            hyperthread_ratio = osi.hyperthread_ratio,
            physical_memory_mb =
                osi.physical_memory_kb / 1024,
            socket_count = osi.socket_count,
            cores_per_socket = osi.cores_per_socket,
            is_hadr_enabled =
                CONVERT(bit, SERVERPROPERTY(N'IsHadrEnabled')),
            is_clustered =
                CONVERT(bit, SERVERPROPERTY(N'IsClustered')),
            enterprise_features = @enterprise_features,
            service_objective =
                CASE
                    WHEN @engine_edition = 5
                    THEN CONVERT(sysname, DATABASEPROPERTYEX(DB_NAME(), N'ServiceObjective'))
                    ELSE NULL
                END,
            row_hash = @current_hash
        FROM sys.dm_os_sys_info AS osi
        OPTION(RECOMPILE);

        SET @rows_collected = ROWCOUNT_BIG();

        /*
        Debug output
        */
        IF @debug = 1
        BEGIN
            RAISERROR(N'Collected %d server properties row(s)', 0, 1, @rows_collected) WITH NOWAIT;

            SELECT TOP (1)
                sp.server_name,
                sp.edition,
                sp.product_version,
                sp.cpu_count,
                sp.hyperthread_ratio,
                sp.physical_memory_mb,
                sp.socket_count,
                sp.cores_per_socket,
                sp.enterprise_features,
                sp.service_objective
            FROM collect.server_properties AS sp
            WHERE sp.collection_time = @start_time;
        END;

        /*
        Log successful collection
        */
        INSERT INTO
            config.collection_log
        (
            collector_name,
            collection_status,
            rows_collected,
            duration_ms
        )
        VALUES
        (
            N'server_properties_collector',
            N'SUCCESS',
            @rows_collected,
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME())
        );

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        /*
        Clean up cursor if open
        */
        IF CURSOR_STATUS(N'local', N'sku_cursor') >= 0
        BEGIN
            CLOSE sku_cursor;
            DEALLOCATE sku_cursor;
        END;

        SET @error_message = ERROR_MESSAGE();

        /*
        Log the error
        */
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
            N'server_properties_collector',
            N'ERROR',
            DATEDIFF(MILLISECOND, @start_time, SYSDATETIME()),
            @error_message
        );

        RAISERROR(N'Error in server properties collector: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Server properties collector created successfully';
PRINT 'Captures edition, version, CPU/memory hardware, and Enterprise feature usage';
PRINT 'Use: EXECUTE collect.server_properties_collector @debug = 1;';
GO
