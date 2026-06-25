/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.11.0 to 3.0.0
Add TRANSACTION_MUTEX to config.ignored_wait_types. It is an internal synchronization
wait (multiple requests sharing one transaction / MARS / app pattern), not a DBA-tunable
condition -- it belongs with the other *_MUTEX waits already ignored
(RESOURCE_SEMAPHORE_MUTEX, QUERY_TASK_ENQUEUE_MUTEX, SOS_PROCESS_AFFINITY_MUTEX, ...). It
was simply omitted from the default seed, so existing servers keep collecting it and can
surface noise (e.g. an ANOMALY_WAIT_TRANSACTION_MUTEX finding). The base seed in
install/03 also adds it for fresh installs. Any already-collected rows age out of the
analysis window on their own once collection stops.
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

/* Idempotent: only insert when missing. */
IF NOT EXISTS
(
    SELECT
        1/0
    FROM config.ignored_wait_types
    WHERE wait_type = N'TRANSACTION_MUTEX'
)
BEGIN
    INSERT INTO
        config.ignored_wait_types
    (
        wait_type
    )
    VALUES
        (N'TRANSACTION_MUTEX');
END;
GO
