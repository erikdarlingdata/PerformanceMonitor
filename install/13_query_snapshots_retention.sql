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
Query snapshots retention procedure
Drops daily query_snapshots tables older than the retention period
*/

IF OBJECT_ID(N'collect.query_snapshots_retention', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.query_snapshots_retention AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.query_snapshots_retention
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT OFF;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @retention_days integer,
        @dsql nvarchar(max) = N'',
        @tables_dropped integer = 0,
        @error_message nvarchar(4000);

    /*
    Get retention days from collection schedule
    */
    SELECT
        @retention_days = cs.retention_days
    FROM config.collection_schedule AS cs
    WHERE cs.collector_name = N'query_snapshots_collector';

    /*
    Default to 30 days if not configured
    */
    IF @retention_days IS NULL
    BEGIN
        SET @retention_days = 30;
    END;

    BEGIN TRY
        /*
        Find and drop tables older than retention period
        */
        IF EXISTS
        (
            SELECT
                1/0
            FROM sys.tables AS t
            WHERE t.name LIKE N'query[_]snapshots[_][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]%'
            AND   t.schema_id = SCHEMA_ID(N'collect')
            AND   TRY_CONVERT(date, SUBSTRING(t.name, 17, 8), 112) < DATEADD(DAY, -@retention_days, CONVERT(date, SYSDATETIME()))
        )
        BEGIN
            /*
            Build DROP TABLE statements
            */
            SELECT
                @dsql +=
            (
                SELECT TOP (9223372036854775807)
                    [text()] =
                        N'DROP TABLE ' +
                        QUOTENAME(SCHEMA_NAME(t.schema_id)) +
                        N'.' +
                        QUOTENAME(t.name) +
                        N';'
                FROM sys.tables AS t
                WHERE t.name LIKE N'query[_]snapshots[_][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]%'
                AND   t.schema_id = SCHEMA_ID(N'collect')
                AND   TRY_CONVERT(date, SUBSTRING(t.name, 17, 8), 112) < DATEADD(DAY, -@retention_days, CONVERT(date, SYSDATETIME()))
                ORDER BY
                    t.name DESC
                FOR XML
                    PATH(N''),
                    TYPE
            ).value
                 (
                     './text()[1]',
                     'nvarchar(max)'
                 );

            /*
            Execute DROP statements
            */
            IF @dsql IS NOT NULL AND LEN(@dsql) > 0
            BEGIN
                EXECUTE sys.sp_executesql
                    @dsql;

                /*
                Count how many tables were dropped
                */
                SET @tables_dropped =
                (
                    LEN(@dsql) -
                    LEN(REPLACE(@dsql, N'DROP TABLE', N''))
                ) / LEN(N'DROP TABLE');

                /*
                Log retention execution with tables dropped
                */
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
                    N'query_snapshots_retention',
                    N'SUCCESS',
                    @tables_dropped,
                    N'Dropped ' + CONVERT(nvarchar(20), @tables_dropped) + N' old query_snapshots tables'
                );
            END;
        END;
        ELSE
        BEGIN
            /*
            Log successful execution even when no tables dropped
            */
            INSERT INTO
                config.collection_log
            (
                collector_name,
                collection_status,
                rows_collected
            )
            VALUES
            (
                N'query_snapshots_retention',
                N'SUCCESS',
                0
            );
        END;

        /*
        Recreate views after dropping tables
        */
        EXECUTE collect.query_snapshots_create_views;

    END TRY
    BEGIN CATCH
        SET @error_message = ERROR_MESSAGE();

        INSERT INTO
            config.collection_log
        (
            collector_name,
            collection_status,
            error_message
        )
        VALUES
        (
            N'query_snapshots_retention',
            N'ERROR',
            @error_message
        );

        RAISERROR(N'Error in query snapshots retention: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Query snapshots retention procedure created successfully';
GO
