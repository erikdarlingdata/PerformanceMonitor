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
Query snapshots view creator
Creates UNION ALL view over daily query_snapshots tables and blocking chain view
Automatically maintains views as new daily tables are created
*/

IF OBJECT_ID(N'collect.query_snapshots_create_views', N'P') IS NULL
BEGIN
    EXECUTE(N'CREATE PROCEDURE collect.query_snapshots_create_views AS RETURN 138;');
END;
GO

ALTER PROCEDURE
    collect.query_snapshots_create_views
WITH RECOMPILE
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT OFF;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE
        @vsql nvarchar(max) = N'',
        @error_message nvarchar(4000);

    BEGIN TRY
        /*
        Build UNION ALL view over all daily query_snapshots tables
        */
        SET @vsql = N'
    CREATE OR ALTER VIEW
        report.query_snapshots
    AS
    ';

        SELECT
            @vsql +=
        (
            SELECT TOP (9223372036854775807)
                [text()] =
                    N'SELECT * FROM ' +
                    QUOTENAME(SCHEMA_NAME(t.schema_id)) +
                    N'.' +
                    QUOTENAME(t.name) +
                    NCHAR(10) +
                    N'UNION ALL ' +
                    NCHAR(10)
            FROM sys.tables AS t
            WHERE t.name LIKE N'query[_]snapshots[_][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]%'
            AND   t.schema_id = SCHEMA_ID(N'collect')
            ORDER BY
                t.create_date DESC
            FOR XML
                PATH(N''),
                TYPE
        ).value
             (
                 './text()[1]',
                 'nvarchar(max)'
             );

        /*
        Only create view if tables exist
        */
        IF @vsql IS NOT NULL AND LEN(@vsql) > 100
        BEGIN
            /*Remove trailing UNION ALL*/
            SET @vsql =
                SUBSTRING
                (
                    @vsql,
                    0,
                    LEN(@vsql) - 11
                ) + N';';

            EXECUTE sys.sp_executesql
                @vsql;
        END;

        /*
        Create or update blocking chain view
        */
        DECLARE
            @sql nvarchar(max) = N'
            CREATE OR ALTER VIEW
                report.query_snapshots_blocking
            AS
            WITH blocking_chain AS
            (
                /*
                Anchor: Find blocking leaders (sessions that are blocking others but not blocked themselves)
                */
                SELECT
                    level = 0,
                    qs.collection_time,
                    qs.[dd hh:mm:ss.mss],
                    qs.sql_text,
                    qs.sql_command,
                    qs.login_name,
                    qs.wait_info,
                    qs.session_id,
                    qs.blocking_session_id,
                    qs.blocked_session_count,
                    qs.status,
                    qs.open_tran_count,
                    qs.query_plan,
                    blocking_path =
                        CONVERT
                        (
                            varchar(1000),
                            ''|---> Lead SPID: '' +
                            CONVERT(varchar(10), qs.session_id)
                        )
                FROM report.query_snapshots AS qs
                WHERE (qs.blocking_session_id IS NULL
                        OR  qs.blocking_session_id = qs.session_id)
                AND   EXISTS
                      (
                          SELECT
                              1/0
                          FROM report.query_snapshots AS qs2
                          WHERE qs2.blocking_session_id = qs.session_id
                          AND   qs2.collection_time = qs.collection_time
                          AND   qs2.blocking_session_id <> qs2.session_id
                      )

                UNION ALL

                /*
                Recursive: Walk the blocking chain
                */
                SELECT
                    level = bc.level + 1,
                    qs.collection_time,
                    qs.[dd hh:mm:ss.mss],
                    qs.sql_text,
                    qs.sql_command,
                    qs.login_name,
                    qs.wait_info,
                    qs.session_id,
                    qs.blocking_session_id,
                    qs.blocked_session_count,
                    qs.status,
                    qs.open_tran_count,
                    qs.query_plan,
                    blocking_path =
                        CONVERT
                        (
                            varchar(1000),
                            bc.blocking_path +
                            '' |---> Blocked SPID: '' +
                            CONVERT(varchar(10), qs.session_id)
                        )
                FROM report.query_snapshots AS qs
                JOIN blocking_chain AS bc
                  ON  qs.blocking_session_id = bc.session_id
                  AND bc.collection_time = qs.collection_time
                WHERE qs.blocking_session_id IS NOT NULL
                AND   qs.blocking_session_id <> qs.session_id
            )
            SELECT TOP (9223372036854775807)
                bc.[dd hh:mm:ss.mss],
                blocking = bc.blocking_path,
                bc.wait_info,
                bc.sql_text,
                bc.sql_command,
                bc.query_plan,
                bc.session_id,
                bc.blocking_session_id,
                bc.blocked_session_count,
                bc.status,
                bc.open_tran_count,
                bc.login_name,
                bc.collection_time,
                bc.level
            FROM blocking_chain AS bc
            ORDER BY
                bc.collection_time,
                bc.blocked_session_count DESC,
                bc.level;';

            EXECUTE sys.sp_executesql
            @sql;

        /*
        Log successful execution
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
            N'query_snapshots_create_views',
            N'SUCCESS',
            0
        );

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
            N'query_snapshots_create_views',
            N'ERROR',
            @error_message
        );

        RAISERROR(N'Error creating query snapshots views: %s', 16, 1, @error_message);
    END CATCH;
END;
GO

PRINT 'Query snapshots view creator procedure created successfully';
GO
