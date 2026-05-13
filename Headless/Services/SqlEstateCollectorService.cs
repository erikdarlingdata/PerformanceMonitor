using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using PerformanceMonitor.Headless.Models;
using PerformanceMonitor.Headless.Storage;

namespace PerformanceMonitor.Headless.Services;

public sealed class SqlEstateCollectorService : BackgroundService
{
    private static readonly HashSet<string> IgnoredWaitTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "BROKER_EVENTHANDLER", "BROKER_RECEIVE_WAITFOR", "BROKER_TASK_STOP", "BROKER_TO_FLUSH",
        "BROKER_TRANSMITTER", "CHECKPOINT_QUEUE", "CHKPT", "CLR_AUTO_EVENT", "CLR_MANUAL_EVENT",
        "DIRTY_PAGE_POLL", "DISPATCHER_QUEUE_SEMAPHORE", "EXECSYNC", "FSAGENT", "FT_IFTS_SCHEDULER_IDLE_WAIT",
        "HADR_FILESTREAM_IOMGR_IOCOMPLETION", "KSOURCE_WAKEUP", "LAZYWRITER_SLEEP", "LOGMGR_QUEUE",
        "ONDEMAND_TASK_QUEUE", "PARALLEL_REDO_DRAIN_WORKER", "PARALLEL_REDO_LOG_CACHE",
        "PARALLEL_REDO_TRAN_LIST", "PARALLEL_REDO_WORKER_SYNC", "PARALLEL_REDO_WORKER_WAIT_WORK",
        "PREEMPTIVE_XE_GETTARGETSTATE", "PWAIT_ALL_COMPONENTS_INITIALIZED", "PWAIT_DIRECTLOGCONSUMER_GETNEXT",
        "QDS_PERSIST_TASK_MAIN_LOOP_SLEEP", "QDS_ASYNC_QUEUE", "QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP",
        "REQUEST_FOR_DEADLOCK_SEARCH", "RESOURCE_QUEUE", "SERVER_IDLE_CHECK", "SLEEP_BPOOL_FLUSH",
        "SLEEP_DBSTARTUP", "SLEEP_DCOMSTARTUP", "SLEEP_MASTERDBREADY", "SLEEP_MASTERMDREADY",
        "SLEEP_MASTERUPGRADED", "SLEEP_MSDBSTARTUP", "SLEEP_SYSTEMTASK", "SLEEP_TASK",
        "SLEEP_TEMPDBSTARTUP", "SNI_HTTP_ACCEPT", "SOS_WORK_DISPATCHER", "SP_SERVER_DIAGNOSTICS_SLEEP",
        "SQLTRACE_BUFFER_FLUSH", "SQLTRACE_INCREMENTAL_FLUSH_SLEEP", "SQLTRACE_WAIT_ENTRIES",
        "WAIT_FOR_RESULTS", "WAITFOR", "WAITFOR_TASKSHUTDOWN", "XE_DISPATCHER_JOIN",
        "XE_DISPATCHER_WAIT", "XE_TIMER_EVENT"
    };

    private readonly MonitorOptions _options;
    private readonly HeadlessStore _store;
    private readonly ILogger<SqlEstateCollectorService> _logger;
    private readonly Dictionary<(string ServerId, string CollectorName), DateTime> _lastRuns = new();
    private DateTime _lastArchiveTime = DateTime.UtcNow;

    public SqlEstateCollectorService(
        IOptions<MonitorOptions> options,
        HeadlessStore store,
        ILogger<SqlEstateCollectorService> logger)
    {
        _options = options.Value;
        _store = store;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _store.InitializeAsync(stoppingToken);
        await _store.UpsertConfiguredServersAsync(_options.Servers, stoppingToken);

        _logger.LogInformation(
            "Headless monitor started. DuckDB={DatabasePath}; Parquet={ArchiveDirectory}",
            _store.DatabasePath,
            _store.ArchiveDirectory);

        await RunCollectionCycleAsync(stoppingToken);

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(Math.Max(10, _options.CollectionIntervalSeconds)));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            await RunCollectionCycleAsync(stoppingToken);
        }
    }

    private async Task RunCollectionCycleAsync(CancellationToken cancellationToken)
    {
        var enabledServers = _options.Servers
            .Where(s => s.Enabled)
            .Where(s => !string.IsNullOrWhiteSpace(s.Id))
            .ToList();

        if (enabledServers.Count == 0)
        {
            _logger.LogDebug("No enabled servers configured");
            return;
        }

        using var throttle = new SemaphoreSlim(Math.Max(1, _options.MaxConcurrentServers));
        var tasks = enabledServers.Select(async server =>
        {
            await throttle.WaitAsync(cancellationToken);
            try
            {
                await CollectServerAsync(server, cancellationToken);
            }
            finally
            {
                throttle.Release();
            }
        });

        await Task.WhenAll(tasks);
        await ArchiveIfDueAsync(cancellationToken);
    }

    private async Task CollectServerAsync(MonitoredServerOptions server, CancellationToken cancellationToken)
    {
        var connectionString = server.ResolveConnectionString();
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            await _store.SetServerStatusAsync(server, "ERROR", "No connection string configured", null, cancellationToken);
            return;
        }

        try
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            await _store.SetServerStatusAsync(server, "ONLINE", null, null, cancellationToken);

            foreach (var collector in _options.GetEffectiveCollectors().Where(c => c.Enabled))
            {
                if (!IsDue(server.Id, collector))
                {
                    continue;
                }

                await RunCollectorAsync(server, connection, collector.Name, cancellationToken);
                MarkRun(server.Id, collector.Name);
            }
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException)
        {
            _logger.LogWarning(ex, "Connection failed for server {Server}", server.ServerNameForStorage);
            await _store.SetServerStatusAsync(server, "ERROR", ex.Message, null, cancellationToken);
        }
    }

    private async Task RunCollectorAsync(
        MonitoredServerOptions server,
        SqlConnection connection,
        string collectorName,
        CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        var totalWatch = Stopwatch.StartNew();
        var sqlWatch = new Stopwatch();
        var storageWatch = new Stopwatch();
        var rowsCollected = 0;
        var status = "SUCCESS";
        string? errorMessage = null;

        try
        {
            switch (collectorName)
            {
                case "server_properties":
                    sqlWatch.Start();
                    var properties = await CollectServerPropertiesAsync(connection, cancellationToken);
                    sqlWatch.Stop();

                    storageWatch.Start();
                    await _store.InsertServerPropertiesAsync(server, startTime, properties, cancellationToken);
                    await _store.SetServerStatusAsync(server, "ONLINE", null, properties, cancellationToken);
                    storageWatch.Stop();
                    rowsCollected = 1;
                    break;

                case "wait_stats":
                    sqlWatch.Start();
                    var waitStats = await CollectWaitStatsAsync(connection, cancellationToken);
                    sqlWatch.Stop();

                    storageWatch.Start();
                    await _store.InsertWaitStatsAsync(server, startTime, waitStats, cancellationToken);
                    storageWatch.Stop();
                    rowsCollected = waitStats.Count;
                    break;

                case "cpu_utilization":
                    var lastSampleTime = await _store.GetLastCpuSampleTimeAsync(server.Id, cancellationToken);
                    sqlWatch.Start();
                    var cpuSamples = await CollectCpuUtilizationAsync(connection, lastSampleTime, cancellationToken);
                    sqlWatch.Stop();

                    storageWatch.Start();
                    await _store.InsertCpuSamplesAsync(server, startTime, cpuSamples, cancellationToken);
                    storageWatch.Stop();
                    rowsCollected = cpuSamples.Count;
                    break;

                default:
                    status = "SKIPPED";
                    errorMessage = $"Unknown collector '{collectorName}'";
                    _logger.LogWarning("Unknown collector {Collector}", collectorName);
                    break;
            }
        }
        catch (SqlException ex) when (IsPermissionError(ex))
        {
            status = "PERMISSIONS";
            errorMessage = $"SQL Error #{ex.Number}: {ex.Message}";
            _logger.LogWarning("Collector {Collector} permission denied for {Server}: {Message}",
                collectorName, server.ServerNameForStorage, ex.Message);
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException or DataException)
        {
            status = "ERROR";
            errorMessage = ex.Message;
            _logger.LogWarning(ex, "Collector {Collector} failed for {Server}",
                collectorName, server.ServerNameForStorage);
        }
        finally
        {
            totalWatch.Stop();
            await _store.InsertCollectionLogAsync(
                server,
                collectorName,
                startTime,
                (int)totalWatch.ElapsedMilliseconds,
                status,
                errorMessage,
                rowsCollected,
                sqlWatch.ElapsedMilliseconds,
                storageWatch.ElapsedMilliseconds,
                cancellationToken);
        }
    }

    private bool IsDue(string serverId, CollectorScheduleOptions collector)
    {
        var frequencySeconds = Math.Max(1, collector.FrequencySeconds);
        return !_lastRuns.TryGetValue((serverId, collector.Name), out var lastRun)
               || DateTime.UtcNow - lastRun >= TimeSpan.FromSeconds(frequencySeconds);
    }

    private void MarkRun(string serverId, string collectorName)
        => _lastRuns[(serverId, collectorName)] = DateTime.UtcNow;

    private async Task ArchiveIfDueAsync(CancellationToken cancellationToken)
    {
        if (_options.ArchiveIntervalMinutes <= 0)
        {
            return;
        }

        if (DateTime.UtcNow - _lastArchiveTime < TimeSpan.FromMinutes(_options.ArchiveIntervalMinutes))
        {
            return;
        }

        try
        {
            await _store.ArchiveOldDataAsync(cancellationToken);
            _lastArchiveTime = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Parquet archival failed");
        }
    }

    private async Task<ServerPropertiesSnapshot> CollectServerPropertiesAsync(
        SqlConnection connection,
        CancellationToken cancellationToken)
    {
        const string query = """
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    machine_name = CONVERT(nvarchar(128), SERVERPROPERTY(N'MachineName')),
    instance_name = CONVERT(nvarchar(128), SERVERPROPERTY(N'InstanceName')),
    product_version = CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')),
    product_level = CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductLevel')),
    edition = CONVERT(nvarchar(256), SERVERPROPERTY(N'Edition')),
    engine_edition = CONVERT(integer, SERVERPROPERTY(N'EngineEdition')),
    sql_major_version = CONVERT(integer, SERVERPROPERTY(N'ProductMajorVersion')),
    cpu_count = CONVERT(integer, dosi.cpu_count),
    physical_memory_mb = CONVERT(bigint, dosi.physical_memory_kb / 1024),
    sqlserver_start_time = dosi.sqlserver_start_time
FROM sys.dm_os_sys_info AS dosi
OPTION(RECOMPILE);
""";

        await using var command = new SqlCommand(query, connection);
        command.CommandTimeout = Math.Max(1, _options.CommandTimeoutSeconds);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new DataException("Server properties query returned no rows");
        }

        return new ServerPropertiesSnapshot(
            reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetString(4),
            reader.GetInt32(5),
            reader.GetInt32(6),
            reader.GetInt32(7),
            reader.GetInt64(8),
            reader.GetDateTime(9));
    }

    private async Task<IReadOnlyList<WaitStatSnapshot>> CollectWaitStatsAsync(
        SqlConnection connection,
        CancellationToken cancellationToken)
    {
        const string query = """
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    wait_type = ws.wait_type,
    waiting_tasks_count = ws.waiting_tasks_count,
    wait_time_ms = ws.wait_time_ms,
    signal_wait_time_ms = ws.signal_wait_time_ms
FROM sys.dm_os_wait_stats AS ws
WHERE ws.wait_time_ms > 0
ORDER BY ws.wait_time_ms DESC
OPTION(RECOMPILE);
""";

        var rows = new List<WaitStatSnapshot>();
        await using var command = new SqlCommand(query, connection);
        command.CommandTimeout = Math.Max(1, _options.CommandTimeoutSeconds);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var waitType = reader.GetString(0);
            if (IgnoredWaitTypes.Contains(waitType))
            {
                continue;
            }

            rows.Add(new WaitStatSnapshot(
                waitType,
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetInt64(3)));
        }

        return rows;
    }

    private async Task<IReadOnlyList<CpuSample>> CollectCpuUtilizationAsync(
        SqlConnection connection,
        DateTime? lastSampleTime,
        CancellationToken cancellationToken)
    {
        const string query = """
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

DECLARE
    @ms_ticks bigint;

SELECT
    @ms_ticks = dosi.ms_ticks
FROM sys.dm_os_sys_info AS dosi;

SELECT TOP (60)
    sample_time = DATEADD(SECOND, -((@ms_ticks - t.timestamp) / 1000), SYSDATETIME()),
    sqlserver_cpu_utilization = t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'integer'),
    other_process_cpu_utilization =
        CASE
            WHEN (100 - t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'integer')
                      - t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'integer')) < 0
            THEN 0
            ELSE 100 - t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'integer')
                     - t.record.value('(Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'integer')
        END
FROM
(
    SELECT
        dorb.timestamp,
        record = CONVERT(xml, dorb.record)
    FROM sys.dm_os_ring_buffers AS dorb
    WHERE dorb.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
) AS t
ORDER BY t.timestamp DESC
OPTION(RECOMPILE);
""";

        var rows = new List<CpuSample>();
        await using var command = new SqlCommand(query, connection);
        command.CommandTimeout = Math.Max(1, _options.CommandTimeoutSeconds);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var sampleTime = reader.GetDateTime(0);
            if (lastSampleTime.HasValue && sampleTime <= lastSampleTime.Value)
            {
                continue;
            }

            rows.Add(new CpuSample(
                sampleTime,
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2)));
        }

        return rows;
    }

    private static bool IsPermissionError(SqlException ex)
    {
        foreach (SqlError error in ex.Errors)
        {
            if (error.Number is 229 or 297 or 300)
            {
                return true;
            }
        }

        return false;
    }
}
