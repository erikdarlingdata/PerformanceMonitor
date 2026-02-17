using System.Collections.Generic;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// Contains all DuckDB table schema definitions as SQL constants.
/// </summary>
public static class Schema
{
    public const string CreateServersTable = @"
CREATE TABLE IF NOT EXISTS servers (
    server_id INTEGER PRIMARY KEY,
    server_name VARCHAR NOT NULL,
    display_name VARCHAR,
    use_windows_auth BOOLEAN NOT NULL DEFAULT TRUE,
    username VARCHAR,
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)";

    public const string CreateCollectionScheduleTable = @"
CREATE TABLE IF NOT EXISTS collection_schedule (
    schedule_id INTEGER PRIMARY KEY,
    collector_name VARCHAR NOT NULL UNIQUE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    frequency_minutes INTEGER NOT NULL DEFAULT 15,
    last_run_time TIMESTAMP,
    next_run_time TIMESTAMP,
    max_duration_minutes INTEGER DEFAULT 5,
    retention_days INTEGER DEFAULT 30,
    description VARCHAR,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)";

    public const string CreateCollectionLogTable = @"
CREATE TABLE IF NOT EXISTS collection_log (
    log_id BIGINT PRIMARY KEY,
    server_id INTEGER NOT NULL,
    server_name VARCHAR,
    collector_name VARCHAR NOT NULL,
    collection_time TIMESTAMP NOT NULL,
    duration_ms INTEGER,
    status VARCHAR NOT NULL,
    error_message VARCHAR,
    rows_collected INTEGER,
    sql_duration_ms INTEGER,
    duckdb_duration_ms INTEGER
)";

    public const string CreateWaitStatsTable = @"
CREATE TABLE IF NOT EXISTS wait_stats (
    collection_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    wait_type VARCHAR NOT NULL,
    waiting_tasks_count BIGINT,
    wait_time_ms BIGINT,
    signal_wait_time_ms BIGINT,
    delta_waiting_tasks BIGINT,
    delta_wait_time_ms BIGINT,
    delta_signal_wait_time_ms BIGINT
)";

    public const string CreateQueryStatsTable = @"
CREATE TABLE IF NOT EXISTS query_stats (
    collection_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    database_name VARCHAR,
    query_hash VARCHAR,
    query_plan_hash VARCHAR,
    execution_count BIGINT,
    total_worker_time BIGINT,
    total_elapsed_time BIGINT,
    total_logical_reads BIGINT,
    total_logical_writes BIGINT,
    total_physical_reads BIGINT,
    total_rows BIGINT,
    total_spills BIGINT,
    min_worker_time BIGINT,
    max_worker_time BIGINT,
    min_elapsed_time BIGINT,
    max_elapsed_time BIGINT,
    min_dop INTEGER,
    max_dop INTEGER,
    query_text VARCHAR,
    query_plan_xml VARCHAR,
    sql_handle VARCHAR,
    plan_handle VARCHAR,
    delta_execution_count BIGINT,
    delta_worker_time BIGINT,
    delta_elapsed_time BIGINT,
    delta_logical_reads BIGINT,
    delta_logical_writes BIGINT,
    delta_physical_reads BIGINT,
    delta_rows BIGINT,
    delta_spills BIGINT
)";

    public const string CreateCpuUtilizationStatsTable = @"
CREATE TABLE IF NOT EXISTS cpu_utilization_stats (
    collection_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    sample_time TIMESTAMP NOT NULL,
    sqlserver_cpu_utilization INTEGER,
    other_process_cpu_utilization INTEGER
)";

    public const string CreateFileIoStatsTable = @"
CREATE TABLE IF NOT EXISTS file_io_stats (
    collection_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    database_name VARCHAR,
    file_name VARCHAR,
    file_type VARCHAR,
    physical_name VARCHAR,
    size_mb DECIMAL(18,2),
    num_of_reads BIGINT,
    num_of_writes BIGINT,
    read_bytes BIGINT,
    write_bytes BIGINT,
    io_stall_read_ms BIGINT,
    io_stall_write_ms BIGINT,
    delta_reads BIGINT,
    delta_writes BIGINT,
    delta_read_bytes BIGINT,
    delta_write_bytes BIGINT,
    delta_stall_read_ms BIGINT,
    delta_stall_write_ms BIGINT
)";

    public const string CreateMemoryStatsTable = @"
CREATE TABLE IF NOT EXISTS memory_stats (
    collection_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    total_physical_memory_mb DECIMAL(18,2),
    available_physical_memory_mb DECIMAL(18,2),
    total_page_file_mb DECIMAL(18,2),
    available_page_file_mb DECIMAL(18,2),
    system_memory_state VARCHAR,
    sql_memory_model VARCHAR,
    target_server_memory_mb DECIMAL(18,2),
    total_server_memory_mb DECIMAL(18,2),
    buffer_pool_mb DECIMAL(18,2),
    plan_cache_mb DECIMAL(18,2)
)";

    public const string CreateMemoryClerksTable = @"
CREATE TABLE IF NOT EXISTS memory_clerks (
    collection_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    clerk_type VARCHAR NOT NULL,
    memory_mb DECIMAL(18,2)
)";

    public const string CreateDeadlocksTable = @"
CREATE TABLE IF NOT EXISTS deadlocks (
    deadlock_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    deadlock_time TIMESTAMP,
    victim_process_id VARCHAR,
    victim_sql_text VARCHAR,
    deadlock_graph_xml VARCHAR
)";

    public const string CreateProcedureStatsTable = @"
CREATE TABLE IF NOT EXISTS procedure_stats (
    collection_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    database_name VARCHAR,
    schema_name VARCHAR,
    object_name VARCHAR,
    object_type VARCHAR,
    execution_count BIGINT,
    total_worker_time BIGINT,
    total_elapsed_time BIGINT,
    total_logical_reads BIGINT,
    total_physical_reads BIGINT,
    total_logical_writes BIGINT,
    min_worker_time BIGINT,
    max_worker_time BIGINT,
    min_elapsed_time BIGINT,
    max_elapsed_time BIGINT,
    total_spills BIGINT,
    sql_handle VARCHAR,
    plan_handle VARCHAR,
    delta_execution_count BIGINT,
    delta_worker_time BIGINT,
    delta_elapsed_time BIGINT,
    delta_logical_reads BIGINT,
    delta_logical_writes BIGINT,
    delta_physical_reads BIGINT
)";

    public const string CreateQueryStoreStatsTable = @"
CREATE TABLE IF NOT EXISTS query_store_stats (
    collection_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    database_name VARCHAR NOT NULL,
    query_id BIGINT,
    plan_id BIGINT,
    query_text VARCHAR,
    query_hash VARCHAR,
    execution_count BIGINT,
    avg_duration_ms DECIMAL(18,2),
    avg_cpu_time_ms DECIMAL(18,2),
    avg_logical_reads DECIMAL(18,2),
    avg_logical_writes DECIMAL(18,2),
    avg_physical_reads DECIMAL(18,2),
    avg_rowcount DECIMAL(18,2),
    last_execution_time TIMESTAMP,
    query_plan_hash VARCHAR
)";

    public const string CreateQuerySnapshotsTable = @"
CREATE TABLE IF NOT EXISTS query_snapshots (
    collection_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    session_id INTEGER,
    database_name VARCHAR,
    elapsed_time_formatted VARCHAR,
    query_text VARCHAR,
    query_plan VARCHAR,
    live_query_plan VARCHAR,
    status VARCHAR,
    blocking_session_id INTEGER,
    wait_type VARCHAR,
    wait_time_ms BIGINT,
    wait_resource VARCHAR,
    cpu_time_ms BIGINT,
    total_elapsed_time_ms BIGINT,
    reads BIGINT,
    writes BIGINT,
    logical_reads BIGINT,
    granted_query_memory_gb DECIMAL(18,2),
    transaction_isolation_level VARCHAR,
    dop INTEGER,
    parallel_worker_count INTEGER
)";

    public const string CreateTempdbStatsTable = @"
CREATE TABLE IF NOT EXISTS tempdb_stats (
    collection_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    user_object_reserved_mb DECIMAL(18,2),
    internal_object_reserved_mb DECIMAL(18,2),
    version_store_reserved_mb DECIMAL(18,2),
    total_reserved_mb DECIMAL(18,2),
    unallocated_mb DECIMAL(18,2),
    total_sessions_using_tempdb INTEGER,
    top_session_id INTEGER,
    top_session_tempdb_mb DECIMAL(18,2)
)";

    public const string CreatePerfmonStatsTable = @"
CREATE TABLE IF NOT EXISTS perfmon_stats (
    collection_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    object_name VARCHAR NOT NULL,
    counter_name VARCHAR NOT NULL,
    instance_name VARCHAR,
    cntr_value BIGINT,
    delta_cntr_value BIGINT,
    sample_interval_seconds INTEGER
)";

    public const string CreateServerConfigTable = @"
CREATE TABLE IF NOT EXISTS server_config (
    config_id BIGINT PRIMARY KEY,
    capture_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    configuration_name VARCHAR NOT NULL,
    value_configured BIGINT,
    value_in_use BIGINT,
    is_dynamic BOOLEAN,
    is_advanced BOOLEAN
)";

    public const string CreateMemoryGrantStatsTable = @"
CREATE TABLE IF NOT EXISTS memory_grant_stats (
    collection_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    session_id INTEGER,
    database_name VARCHAR,
    query_text VARCHAR,
    requested_memory_mb DECIMAL(18,2),
    granted_memory_mb DECIMAL(18,2),
    used_memory_mb DECIMAL(18,2),
    max_used_memory_mb DECIMAL(18,2),
    ideal_memory_mb DECIMAL(18,2),
    required_memory_mb DECIMAL(18,2),
    wait_time_ms BIGINT,
    is_small_grant BOOLEAN,
    dop INTEGER,
    query_cost DECIMAL(18,2)
)";

    public const string CreateWaitingTasksTable = @"
CREATE TABLE IF NOT EXISTS waiting_tasks (
    collection_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    session_id INTEGER,
    wait_type VARCHAR,
    wait_duration_ms BIGINT,
    blocking_session_id INTEGER,
    resource_description VARCHAR,
    database_name VARCHAR
)";

    public const string CreateBlockedProcessReportsTable = @"
CREATE TABLE IF NOT EXISTS blocked_process_reports (
    blocked_report_id BIGINT PRIMARY KEY,
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    event_time TIMESTAMP,
    database_name VARCHAR,
    blocked_spid INTEGER,
    blocked_ecid INTEGER,
    blocking_spid INTEGER,
    blocking_ecid INTEGER,
    wait_time_ms BIGINT,
    wait_resource VARCHAR,
    lock_mode VARCHAR,
    blocked_status VARCHAR,
    blocked_isolation_level VARCHAR,
    blocked_log_used BIGINT,
    blocked_transaction_count INTEGER,
    blocked_client_app VARCHAR,
    blocked_host_name VARCHAR,
    blocked_login_name VARCHAR,
    blocked_sql_text VARCHAR,
    blocking_status VARCHAR,
    blocking_isolation_level VARCHAR,
    blocking_client_app VARCHAR,
    blocking_host_name VARCHAR,
    blocking_login_name VARCHAR,
    blocking_sql_text VARCHAR,
    blocked_transaction_name VARCHAR,
    blocking_transaction_name VARCHAR,
    blocked_last_tran_started TIMESTAMP,
    blocking_last_tran_started TIMESTAMP,
    blocked_last_batch_started TIMESTAMP,
    blocking_last_batch_started TIMESTAMP,
    blocked_last_batch_completed TIMESTAMP,
    blocking_last_batch_completed TIMESTAMP,
    blocked_priority INTEGER,
    blocking_priority INTEGER,
    blocked_process_report_xml VARCHAR
)";

    public const string CreateDatabaseConfigTable = @"
CREATE TABLE IF NOT EXISTS database_config (
    config_id BIGINT PRIMARY KEY,
    capture_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    database_name VARCHAR NOT NULL,
    compatibility_level INTEGER,
    recovery_model VARCHAR,
    is_auto_close_on BOOLEAN,
    is_auto_shrink_on BOOLEAN,
    is_query_store_on BOOLEAN,
    page_verify_option VARCHAR,
    target_recovery_time_seconds INTEGER,
    delayed_durability VARCHAR
)";

    // Index definitions
    public const string CreateWaitStatsIndex = @"
CREATE INDEX IF NOT EXISTS idx_wait_stats_time ON wait_stats(server_id, collection_time)";

    public const string CreateQueryStatsIndex = @"
CREATE INDEX IF NOT EXISTS idx_query_stats_time ON query_stats(server_id, collection_time)";

    public const string CreateProcedureStatsIndex = @"
CREATE INDEX IF NOT EXISTS idx_procedure_stats_time ON procedure_stats(server_id, collection_time)";

    public const string CreateQueryStoreIndex = @"
CREATE INDEX IF NOT EXISTS idx_query_store_time ON query_store_stats(server_id, collection_time)";

    public const string CreateQuerySnapshotsIndex = @"
CREATE INDEX IF NOT EXISTS idx_query_snapshots_time ON query_snapshots(server_id, collection_time)";

    public const string CreateCpuIndex = @"
CREATE INDEX IF NOT EXISTS idx_cpu_time ON cpu_utilization_stats(server_id, collection_time)";

    public const string CreateFileIoIndex = @"
CREATE INDEX IF NOT EXISTS idx_file_io_time ON file_io_stats(server_id, collection_time)";

    public const string CreateMemoryIndex = @"
CREATE INDEX IF NOT EXISTS idx_memory_time ON memory_stats(server_id, collection_time)";

    public const string CreateTempdbIndex = @"
CREATE INDEX IF NOT EXISTS idx_tempdb_time ON tempdb_stats(server_id, collection_time)";

    public const string CreatePerfmonIndex = @"
CREATE INDEX IF NOT EXISTS idx_perfmon_time ON perfmon_stats(server_id, collection_time)";

    public const string CreateDeadlocksIndex = @"
CREATE INDEX IF NOT EXISTS idx_deadlocks_time ON deadlocks(server_id, collection_time)";

    public const string CreateCollectionLogIndex = @"
CREATE INDEX IF NOT EXISTS idx_collection_log_time ON collection_log(server_id, collection_time)";

    public const string CreateMemoryGrantStatsIndex = @"
CREATE INDEX IF NOT EXISTS idx_memory_grant_stats_time ON memory_grant_stats(server_id, collection_time)";

    public const string CreateWaitingTasksIndex = @"
CREATE INDEX IF NOT EXISTS idx_waiting_tasks_time ON waiting_tasks(server_id, collection_time)";

    public const string CreateBlockedProcessReportsIndex = @"
CREATE INDEX IF NOT EXISTS idx_blocked_process_reports_time ON blocked_process_reports(server_id, collection_time)";

    public const string CreateDatabaseScopedConfigTable = @"
CREATE TABLE IF NOT EXISTS database_scoped_config (
    config_id BIGINT PRIMARY KEY,
    capture_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    database_name VARCHAR NOT NULL,
    configuration_name VARCHAR NOT NULL,
    value VARCHAR,
    value_for_secondary VARCHAR
)";

    public const string CreateDatabaseScopedConfigIndex = @"
CREATE INDEX IF NOT EXISTS idx_database_scoped_config_time ON database_scoped_config(server_id, capture_time)";

    public const string CreateTraceFlagsTable = @"
CREATE TABLE IF NOT EXISTS trace_flags (
    config_id BIGINT PRIMARY KEY,
    capture_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    trace_flag INTEGER NOT NULL,
    status BOOLEAN NOT NULL,
    is_global BOOLEAN NOT NULL,
    is_session BOOLEAN NOT NULL
)";

    public const string CreateTraceFlagsIndex = @"
CREATE INDEX IF NOT EXISTS idx_trace_flags_time ON trace_flags(server_id, capture_time)";

    public const string CreateRunningJobsTable = @"
CREATE TABLE IF NOT EXISTS running_jobs (
    collection_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    job_name VARCHAR NOT NULL,
    job_id VARCHAR NOT NULL,
    job_enabled BOOLEAN NOT NULL,
    start_time TIMESTAMP NOT NULL,
    current_duration_seconds BIGINT NOT NULL,
    avg_duration_seconds BIGINT NOT NULL,
    p95_duration_seconds BIGINT NOT NULL,
    successful_run_count BIGINT NOT NULL,
    is_running_long BOOLEAN NOT NULL,
    percent_of_average DECIMAL(10,1)
)";

    public const string CreateRunningJobsIndex = @"
CREATE INDEX IF NOT EXISTS idx_running_jobs_time ON running_jobs(server_id, collection_time)";

    public const string CreateAlertLogTable = @"
CREATE TABLE IF NOT EXISTS config_alert_log (
    alert_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    metric_name VARCHAR NOT NULL,
    current_value DOUBLE NOT NULL,
    threshold_value DOUBLE NOT NULL,
    alert_sent BOOLEAN NOT NULL DEFAULT false,
    notification_type VARCHAR NOT NULL DEFAULT 'tray',
    send_error VARCHAR,
    dismissed BOOLEAN NOT NULL DEFAULT false
)";

    /// <summary>
    /// Returns all table creation statements in order.
    /// </summary>
    public static IEnumerable<string> GetAllTableStatements()
    {
        yield return CreateServersTable;
        yield return CreateCollectionScheduleTable;
        yield return CreateCollectionLogTable;
        yield return CreateWaitStatsTable;
        yield return CreateQueryStatsTable;
        yield return CreateCpuUtilizationStatsTable;
        yield return CreateFileIoStatsTable;
        yield return CreateMemoryStatsTable;
        yield return CreateMemoryClerksTable;
        yield return CreateDeadlocksTable;
        yield return CreateProcedureStatsTable;
        yield return CreateQueryStoreStatsTable;
        yield return CreateQuerySnapshotsTable;
        yield return CreateTempdbStatsTable;
        yield return CreatePerfmonStatsTable;
        yield return CreateServerConfigTable;
        yield return CreateDatabaseConfigTable;
        yield return CreateMemoryGrantStatsTable;
        yield return CreateWaitingTasksTable;
        yield return CreateBlockedProcessReportsTable;
        yield return CreateDatabaseScopedConfigTable;
        yield return CreateTraceFlagsTable;
        yield return CreateRunningJobsTable;
        yield return CreateAlertLogTable;
    }

    /// <summary>
    /// Returns all index creation statements.
    /// </summary>
    public static IEnumerable<string> GetAllIndexStatements()
    {
        yield return CreateWaitStatsIndex;
        yield return CreateQueryStatsIndex;
        yield return CreateProcedureStatsIndex;
        yield return CreateQueryStoreIndex;
        yield return CreateQuerySnapshotsIndex;
        yield return CreateCpuIndex;
        yield return CreateFileIoIndex;
        yield return CreateMemoryIndex;
        yield return CreateTempdbIndex;
        yield return CreatePerfmonIndex;
        yield return CreateDeadlocksIndex;
        yield return CreateCollectionLogIndex;
        yield return CreateMemoryGrantStatsIndex;
        yield return CreateWaitingTasksIndex;
        yield return CreateBlockedProcessReportsIndex;
        yield return CreateDatabaseScopedConfigIndex;
        yield return CreateTraceFlagsIndex;
        yield return CreateRunningJobsIndex;
    }
}
