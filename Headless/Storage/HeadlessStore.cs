using DuckDB.NET.Data;
using Microsoft.Extensions.Options;
using PerformanceMonitor.Headless.Models;

namespace PerformanceMonitor.Headless.Storage;

public sealed class HeadlessStore
{
    private readonly MonitorOptions _options;
    private readonly IHostEnvironment _environment;
    private readonly ILogger<HeadlessStore> _logger;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private static long s_idCounter = DateTime.UtcNow.Ticks;

    public HeadlessStore(
        IOptions<MonitorOptions> options,
        IHostEnvironment environment,
        ILogger<HeadlessStore> logger)
    {
        _options = options.Value;
        _environment = environment;
        _logger = logger;
    }

    public string DatabasePath => ResolvePath(_options.StoragePath);
    public string ArchiveDirectory => ResolvePath(_options.ArchiveDirectory);

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(DatabasePath)!);
        Directory.CreateDirectory(ArchiveDirectory);

        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);

        foreach (var sql in SchemaStatements)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    public DuckDBConnection CreateConnection()
        => new($"Data Source={DatabasePath}");

    public async Task UpsertConfiguredServersAsync(IEnumerable<MonitoredServerOptions> servers, CancellationToken cancellationToken)
    {
        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);

            foreach (var server in servers)
            {
                await using var insert = connection.CreateCommand();
                insert.CommandText = @"
INSERT INTO servers (server_id, server_name, display_name, purpose, is_enabled, last_status)
VALUES ($1, $2, $3, $4, $5, 'UNKNOWN')
ON CONFLICT(server_id) DO UPDATE
SET server_name = excluded.server_name,
    display_name = excluded.display_name,
    purpose = excluded.purpose,
    is_enabled = excluded.is_enabled";
                insert.Parameters.Add(new DuckDBParameter { Value = server.Id });
                insert.Parameters.Add(new DuckDBParameter { Value = server.ServerNameForStorage });
                insert.Parameters.Add(new DuckDBParameter { Value = server.DisplayName });
                insert.Parameters.Add(new DuckDBParameter { Value = server.PurposeForDisplay });
                insert.Parameters.Add(new DuckDBParameter { Value = server.Enabled });
                await insert.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task SetServerStatusAsync(
        MonitoredServerOptions server,
        string status,
        string? errorMessage,
        ServerPropertiesSnapshot? properties,
        CancellationToken cancellationToken)
    {
        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = @"
UPDATE servers
SET last_seen_time = $2,
    last_status = $3,
    last_error = $4,
    product_version = COALESCE($5, product_version),
    edition = COALESCE($6, edition),
    sql_engine_edition = COALESCE($7, sql_engine_edition),
    sql_major_version = COALESCE($8, sql_major_version)
WHERE server_id = $1";
            command.Parameters.Add(new DuckDBParameter { Value = server.Id });
            command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
            command.Parameters.Add(new DuckDBParameter { Value = status });
            command.Parameters.Add(new DuckDBParameter { Value = errorMessage ?? (object)DBNull.Value });
            command.Parameters.Add(new DuckDBParameter { Value = properties?.ProductVersion ?? (object)DBNull.Value });
            command.Parameters.Add(new DuckDBParameter { Value = properties?.Edition ?? (object)DBNull.Value });
            command.Parameters.Add(new DuckDBParameter { Value = properties?.EngineEdition ?? (object)DBNull.Value });
            command.Parameters.Add(new DuckDBParameter { Value = properties?.SqlMajorVersion ?? (object)DBNull.Value });
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task InsertServerPropertiesAsync(
        MonitoredServerOptions server,
        DateTime collectionTime,
        ServerPropertiesSnapshot properties,
        CancellationToken cancellationToken)
    {
        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);
            using var appender = connection.CreateAppender("server_properties");
            var row = appender.CreateRow()
                .AppendValue(NextId())
                .AppendValue(collectionTime)
                .AppendValue(server.Id)
                .AppendValue(server.ServerNameForStorage)
                .AppendValue(properties.MachineName);

            if (properties.InstanceName is null)
            {
                row.AppendNullValue();
            }
            else
            {
                row.AppendValue(properties.InstanceName);
            }

            row
                .AppendValue(properties.ProductVersion)
                .AppendValue(properties.ProductLevel)
                .AppendValue(properties.Edition)
                .AppendValue(properties.EngineEdition)
                .AppendValue(properties.SqlMajorVersion)
                .AppendValue(properties.CpuCount)
                .AppendValue(properties.PhysicalMemoryMb)
                .AppendValue(properties.SqlServerStartTime)
                .EndRow();
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task InsertWaitStatsAsync(
        MonitoredServerOptions server,
        DateTime collectionTime,
        IReadOnlyList<WaitStatSnapshot> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return;
        }

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);
            using var appender = connection.CreateAppender("wait_stats");
            foreach (var row in rows)
            {
                appender.CreateRow()
                    .AppendValue(NextId())
                    .AppendValue(collectionTime)
                    .AppendValue(server.Id)
                    .AppendValue(server.ServerNameForStorage)
                    .AppendValue(row.WaitType)
                    .AppendValue(row.WaitingTasksCount)
                    .AppendValue(row.WaitTimeMs)
                    .AppendValue(row.SignalWaitTimeMs)
                    .EndRow();
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task<DateTime?> GetLastCpuSampleTimeAsync(string serverId, CancellationToken cancellationToken)
    {
        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT MAX(sample_time) FROM cpu_utilization_stats WHERE server_id = $1";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is DateTime dateTime ? dateTime : null;
    }

    public async Task InsertCpuSamplesAsync(
        MonitoredServerOptions server,
        DateTime collectionTime,
        IReadOnlyList<CpuSample> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return;
        }

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);
            using var appender = connection.CreateAppender("cpu_utilization_stats");
            foreach (var row in rows)
            {
                appender.CreateRow()
                    .AppendValue(NextId())
                    .AppendValue(collectionTime)
                    .AppendValue(server.Id)
                    .AppendValue(server.ServerNameForStorage)
                    .AppendValue(row.SampleTime)
                    .AppendValue(row.SqlServerCpuUtilization)
                    .AppendValue(row.OtherProcessCpuUtilization)
                    .EndRow();
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task InsertCollectionLogAsync(
        MonitoredServerOptions server,
        string collectorName,
        DateTime collectionTime,
        int durationMs,
        string status,
        string? errorMessage,
        int rowsCollected,
        long sqlDurationMs,
        long storageDurationMs,
        CancellationToken cancellationToken)
    {
        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, storage_duration_ms)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
            command.Parameters.Add(new DuckDBParameter { Value = NextId() });
            command.Parameters.Add(new DuckDBParameter { Value = server.Id });
            command.Parameters.Add(new DuckDBParameter { Value = server.ServerNameForStorage });
            command.Parameters.Add(new DuckDBParameter { Value = collectorName });
            command.Parameters.Add(new DuckDBParameter { Value = collectionTime });
            command.Parameters.Add(new DuckDBParameter { Value = durationMs });
            command.Parameters.Add(new DuckDBParameter { Value = status });
            command.Parameters.Add(new DuckDBParameter { Value = errorMessage ?? (object)DBNull.Value });
            command.Parameters.Add(new DuckDBParameter { Value = rowsCollected });
            command.Parameters.Add(new DuckDBParameter { Value = (int)sqlDurationMs });
            command.Parameters.Add(new DuckDBParameter { Value = (int)storageDurationMs });
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task<IReadOnlyList<ServerHealthDto>> GetServersAsync(CancellationToken cancellationToken)
    {
        var servers = new List<ServerHealthDto>();
        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    s.server_id,
    s.display_name,
    COALESCE(NULLIF(TRIM(s.purpose), ''), 'Unassigned') AS purpose,
    s.is_enabled,
    s.last_seen_time,
    s.last_status,
    s.last_error,
    s.product_version,
    s.edition,
    s.sql_major_version,
    (
        SELECT COUNT(*)
        FROM collection_log AS cl
        WHERE cl.server_id = s.server_id
        AND   cl.collection_time >= $1
        AND   cl.status IN ('ERROR', 'PERMISSIONS')
    ) AS active_alert_count,
    (
        SELECT cl.error_message
        FROM collection_log AS cl
        WHERE cl.server_id = s.server_id
        AND   cl.collection_time >= $1
        AND   cl.status IN ('ERROR', 'PERMISSIONS')
        ORDER BY cl.collection_time DESC
        LIMIT 1
    ) AS recent_alert,
    (
        SELECT cu.sqlserver_cpu_utilization
        FROM cpu_utilization_stats AS cu
        WHERE cu.server_id = s.server_id
        ORDER BY cu.sample_time DESC
        LIMIT 1
    ) AS latest_sql_cpu,
    (
        SELECT ws.wait_type
        FROM wait_stats AS ws
        WHERE ws.server_id = s.server_id
        ORDER BY ws.collection_time DESC, ws.wait_time_ms DESC
        LIMIT 1
    ) AS top_wait_type
FROM servers AS s
ORDER BY
    s.is_enabled DESC,
    CASE LOWER(COALESCE(NULLIF(TRIM(s.purpose), ''), 'unassigned'))
        WHEN 'production' THEN 1
        WHEN 'prod' THEN 1
        WHEN 'staging' THEN 2
        WHEN 'stage' THEN 2
        WHEN 'development' THEN 3
        WHEN 'dev' THEN 3
        WHEN 'test' THEN 4
        ELSE 5
    END,
    s.display_name";
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddMinutes(-15) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.GetString(0);
            var displayName = reader.IsDBNull(1) ? serverId : reader.GetString(1);
            var purpose = reader.IsDBNull(2) ? "Unassigned" : reader.GetString(2);
            var isEnabled = reader.GetBoolean(3);
            var lastSeenTime = reader.IsDBNull(4) ? (DateTime?)null : reader.GetDateTime(4);
            var lastStatus = reader.IsDBNull(5) ? "UNKNOWN" : reader.GetString(5);
            var lastError = reader.IsDBNull(6) ? null : reader.GetString(6);
            var productVersion = reader.IsDBNull(7) ? null : reader.GetString(7);
            var edition = reader.IsDBNull(8) ? null : reader.GetString(8);
            var sqlMajorVersion = reader.IsDBNull(9) ? (int?)null : reader.GetInt32(9);
            var activeAlertCount = reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetInt64(10));
            var recentAlert = reader.IsDBNull(11) ? null : reader.GetString(11);
            var latestSqlCpu = reader.IsDBNull(12) ? (int?)null : reader.GetInt32(12);
            var topWaitType = reader.IsDBNull(13) ? null : reader.GetString(13);
            var (healthState, healthReason) = ComputeHealth(isEnabled, lastSeenTime, lastStatus, lastError, activeAlertCount, recentAlert);

            servers.Add(new ServerHealthDto(
                serverId,
                displayName,
                purpose,
                isEnabled,
                lastSeenTime,
                lastStatus,
                recentAlert ?? lastError,
                productVersion,
                edition,
                sqlMajorVersion,
                healthState,
                healthReason,
                activeAlertCount,
                latestSqlCpu,
                topWaitType));
        }

        return servers;
    }

    public async Task<EstateSummaryDto> GetEstateSummaryAsync(CancellationToken cancellationToken)
    {
        var servers = await GetServersAsync(cancellationToken);
        return new EstateSummaryDto(
            servers.Count,
            servers.Count(s => string.Equals(s.HealthState, "green", StringComparison.OrdinalIgnoreCase)),
            servers.Count(s => string.Equals(s.HealthState, "yellow", StringComparison.OrdinalIgnoreCase)),
            servers.Count(s => string.Equals(s.HealthState, "red", StringComparison.OrdinalIgnoreCase)),
            servers.Count(s => s.IsEnabled && string.Equals(s.LastStatus, "ERROR", StringComparison.OrdinalIgnoreCase)),
            servers.Count(s => !s.IsEnabled),
            DateTime.UtcNow,
            servers);
    }

    private (string HealthState, string HealthReason) ComputeHealth(
        bool isEnabled,
        DateTime? lastSeenTime,
        string lastStatus,
        string? lastError,
        int activeAlertCount,
        string? recentAlert)
    {
        if (!isEnabled)
        {
            return ("disabled", "Monitoring disabled");
        }

        if (string.Equals(lastStatus, "ERROR", StringComparison.OrdinalIgnoreCase))
        {
            return ("red", lastError ?? "Connection failed");
        }

        if (activeAlertCount > 0)
        {
            return ("red", recentAlert ?? $"{activeAlertCount} collector alert(s) in the last 15 minutes");
        }

        if (!lastSeenTime.HasValue)
        {
            return ("yellow", "No successful collection yet");
        }

        var staleAfter = TimeSpan.FromSeconds(Math.Max(180, _options.CollectionIntervalSeconds * 3));
        if (DateTime.UtcNow - lastSeenTime.Value > staleAfter)
        {
            return ("yellow", $"No server contact for {DateTime.UtcNow - lastSeenTime.Value:g}");
        }

        return ("green", "All good");
    }

    public async Task<IReadOnlyList<CollectionLogDto>> GetCollectionLogAsync(int limit, CancellationToken cancellationToken)
    {
        var logs = new List<CollectionLogDto>();
        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT collection_time, server_id, server_name, collector_name, status, rows_collected, duration_ms, error_message
FROM collection_log
ORDER BY collection_time DESC
LIMIT $1";
        command.Parameters.Add(new DuckDBParameter { Value = Math.Clamp(limit, 1, 1000) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            logs.Add(new CollectionLogDto(
                reader.GetDateTime(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetString(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                reader.IsDBNull(7) ? null : reader.GetString(7)));
        }

        return logs;
    }

    public async Task<IReadOnlyList<TopWaitDto>> GetTopWaitsAsync(string serverId, int hoursBack, int limit, CancellationToken cancellationToken)
    {
        var waits = new List<TopWaitDto>();
        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = @"
WITH wait_window AS
(
    SELECT
        wait_type,
        MAX(wait_time_ms) - MIN(wait_time_ms) AS wait_time_delta_ms,
        MAX(signal_wait_time_ms) - MIN(signal_wait_time_ms) AS signal_wait_time_delta_ms,
        MAX(waiting_tasks_count) - MIN(waiting_tasks_count) AS waiting_tasks_delta
    FROM wait_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    GROUP BY wait_type
)
SELECT wait_type, wait_time_delta_ms, signal_wait_time_delta_ms, waiting_tasks_delta
FROM wait_window
WHERE wait_time_delta_ms > 0
ORDER BY wait_time_delta_ms DESC
LIMIT $3";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-Math.Clamp(hoursBack, 1, 720)) });
        command.Parameters.Add(new DuckDBParameter { Value = Math.Clamp(limit, 1, 100) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            waits.Add(new TopWaitDto(
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetInt64(3)));
        }

        return waits;
    }

    public async Task<IReadOnlyList<CpuSampleDto>> GetCpuSamplesAsync(string serverId, int hoursBack, CancellationToken cancellationToken)
    {
        var samples = new List<CpuSampleDto>();
        await using var connection = CreateConnection();
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization
FROM cpu_utilization_stats
WHERE server_id = $1
AND   sample_time >= $2
ORDER BY sample_time";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-Math.Clamp(hoursBack, 1, 720)) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            samples.Add(new CpuSampleDto(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2)));
        }

        return samples;
    }

    public async Task ArchiveOldDataAsync(CancellationToken cancellationToken)
    {
        if (_options.HotDataDays <= 0)
        {
            return;
        }

        var cutoff = DateTime.UtcNow.AddDays(-_options.HotDataDays);
        var tables = new[] { "wait_stats", "cpu_utilization_stats", "server_properties", "collection_log" };

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(cancellationToken);

            foreach (var table in tables)
            {
                var timeColumn = table == "cpu_utilization_stats" ? "sample_time" : "collection_time";
                await using var countCommand = connection.CreateCommand();
                countCommand.CommandText = $"SELECT COUNT(*) FROM {table} WHERE {timeColumn} < $1";
                countCommand.Parameters.Add(new DuckDBParameter { Value = cutoff });
                var count = Convert.ToInt64(await countCommand.ExecuteScalarAsync(cancellationToken));
                if (count == 0)
                {
                    continue;
                }

                var tableArchiveDirectory = Path.Combine(ArchiveDirectory, table);
                Directory.CreateDirectory(tableArchiveDirectory);
                var archiveFile = Path.Combine(tableArchiveDirectory, $"{table}_{DateTime.UtcNow:yyyyMMddTHHmmss}.parquet");
                var archiveFileSql = archiveFile.Replace("\\", "/").Replace("'", "''");
                var cutoffSql = cutoff.ToString("yyyy-MM-dd HH:mm:ss.fffffff");

                await using var copyCommand = connection.CreateCommand();
                copyCommand.CommandText = $@"
COPY
(
    SELECT *
    FROM {table}
    WHERE {timeColumn} < TIMESTAMP '{cutoffSql}'
)
TO '{archiveFileSql}'
(FORMAT PARQUET)";
                await copyCommand.ExecuteNonQueryAsync(cancellationToken);

                await using var deleteCommand = connection.CreateCommand();
                deleteCommand.CommandText = $"DELETE FROM {table} WHERE {timeColumn} < $1";
                deleteCommand.Parameters.Add(new DuckDBParameter { Value = cutoff });
                await deleteCommand.ExecuteNonQueryAsync(cancellationToken);

                _logger.LogInformation("Archived {RowCount} rows from {Table} to {File}", count, table, archiveFile);
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private string ResolvePath(string configuredPath)
    {
        var expanded = Environment.ExpandEnvironmentVariables(configuredPath);
        if (!Path.IsPathRooted(expanded))
        {
            expanded = Path.Combine(_environment.ContentRootPath, expanded);
        }

        return Path.GetFullPath(expanded);
    }

    private static long NextId() => Interlocked.Increment(ref s_idCounter);

    private static readonly string[] SchemaStatements =
    [
        """
        CREATE TABLE IF NOT EXISTS servers (
            server_id VARCHAR PRIMARY KEY,
            server_name VARCHAR NOT NULL,
            display_name VARCHAR,
            purpose VARCHAR NOT NULL DEFAULT 'Unassigned',
            is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
            last_seen_time TIMESTAMP,
            last_status VARCHAR NOT NULL DEFAULT 'UNKNOWN',
            last_error VARCHAR,
            product_version VARCHAR,
            edition VARCHAR,
            sql_engine_edition INTEGER,
            sql_major_version INTEGER,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        "ALTER TABLE servers ADD COLUMN IF NOT EXISTS purpose VARCHAR DEFAULT 'Unassigned'",
        "UPDATE servers SET purpose = 'Unassigned' WHERE purpose IS NULL OR TRIM(purpose) = ''",
        """
        CREATE TABLE IF NOT EXISTS collection_log (
            log_id BIGINT PRIMARY KEY,
            server_id VARCHAR NOT NULL,
            server_name VARCHAR NOT NULL,
            collector_name VARCHAR NOT NULL,
            collection_time TIMESTAMP NOT NULL,
            duration_ms INTEGER,
            status VARCHAR NOT NULL,
            error_message VARCHAR,
            rows_collected INTEGER,
            sql_duration_ms INTEGER,
            storage_duration_ms INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS server_properties (
            collection_id BIGINT PRIMARY KEY,
            collection_time TIMESTAMP NOT NULL,
            server_id VARCHAR NOT NULL,
            server_name VARCHAR NOT NULL,
            machine_name VARCHAR,
            instance_name VARCHAR,
            product_version VARCHAR,
            product_level VARCHAR,
            edition VARCHAR,
            engine_edition INTEGER,
            sql_major_version INTEGER,
            cpu_count INTEGER,
            physical_memory_mb BIGINT,
            sqlserver_start_time TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS wait_stats (
            collection_id BIGINT PRIMARY KEY,
            collection_time TIMESTAMP NOT NULL,
            server_id VARCHAR NOT NULL,
            server_name VARCHAR NOT NULL,
            wait_type VARCHAR NOT NULL,
            waiting_tasks_count BIGINT,
            wait_time_ms BIGINT,
            signal_wait_time_ms BIGINT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS cpu_utilization_stats (
            collection_id BIGINT PRIMARY KEY,
            collection_time TIMESTAMP NOT NULL,
            server_id VARCHAR NOT NULL,
            server_name VARCHAR NOT NULL,
            sample_time TIMESTAMP NOT NULL,
            sqlserver_cpu_utilization INTEGER,
            other_process_cpu_utilization INTEGER
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_servers_status ON servers(is_enabled, last_status)",
        "CREATE INDEX IF NOT EXISTS idx_collection_log_time ON collection_log(collection_time)",
        "CREATE INDEX IF NOT EXISTS idx_wait_stats_time ON wait_stats(server_id, collection_time)",
        "CREATE INDEX IF NOT EXISTS idx_cpu_time ON cpu_utilization_stats(server_id, sample_time)",
        "CREATE INDEX IF NOT EXISTS idx_server_properties_time ON server_properties(server_id, collection_time)"
    ];
}
