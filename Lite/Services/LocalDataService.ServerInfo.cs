/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// Gets the latest server properties snapshot (edition, version, CPU, memory).
    /// </summary>
    public async Task<ServerPropertiesRow?> GetLatestServerPropertiesAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT edition, product_version, product_level, product_update_level,
       engine_edition, cpu_count, hyperthread_ratio, physical_memory_mb,
       socket_count, cores_per_socket, is_hadr_enabled, is_clustered,
       enterprise_features, service_objective, collection_time
FROM v_server_properties
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return null;

        return new ServerPropertiesRow
        {
            Edition = reader.IsDBNull(0) ? "" : reader.GetString(0),
            ProductVersion = reader.IsDBNull(1) ? "" : reader.GetString(1),
            ProductLevel = reader.IsDBNull(2) ? "" : reader.GetString(2),
            ProductUpdateLevel = reader.IsDBNull(3) ? "" : reader.GetString(3),
            EngineEdition = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
            CpuCount = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
            HyperthreadRatio = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
            PhysicalMemoryMb = reader.IsDBNull(7) ? 0 : ToInt64(reader.GetValue(7)),
            SocketCount = reader.IsDBNull(8) ? 0 : reader.GetInt32(8),
            CoresPerSocket = reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
            IsHadrEnabled = !reader.IsDBNull(10) && reader.GetBoolean(10),
            IsClustered = !reader.IsDBNull(11) && reader.GetBoolean(11),
            EnterpriseFeatures = reader.IsDBNull(12) ? "" : reader.GetString(12),
            ServiceObjective = reader.IsDBNull(13) ? "" : reader.GetString(13),
            CollectionTime = reader.GetDateTime(14)
        };
    }

    /// <summary>
    /// Gets the latest database size stats (file sizes, volume space).
    /// </summary>
    public async Task<List<DatabaseSizeStatsRow>> GetLatestDatabaseSizeStatsAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT database_name, file_name, file_type_desc, physical_name,
       total_size_mb, used_size_mb, auto_growth_mb, max_size_mb,
       volume_mount_point, volume_total_mb, volume_free_mb,
       collection_time
FROM v_database_size_stats
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM v_database_size_stats WHERE server_id = $1)
ORDER BY database_name, file_type_desc, file_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<DatabaseSizeStatsRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DatabaseSizeStatsRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                FileName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                FileTypeDesc = reader.IsDBNull(2) ? "" : reader.GetString(2),
                PhysicalName = reader.IsDBNull(3) ? "" : reader.GetString(3),
                TotalSizeMb = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                UsedSizeMb = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                AutoGrowthMb = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                MaxSizeMb = reader.IsDBNull(7) ? 0 : ToDouble(reader.GetValue(7)),
                VolumeMountPoint = reader.IsDBNull(8) ? "" : reader.GetString(8),
                VolumeTotalMb = reader.IsDBNull(9) ? 0 : ToDouble(reader.GetValue(9)),
                VolumeFreeMb = reader.IsDBNull(10) ? 0 : ToDouble(reader.GetValue(10)),
                CollectionTime = reader.GetDateTime(11)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets the latest session stats (connection counts by application).
    /// </summary>
    public async Task<List<SessionStatsRow>> GetLatestSessionStatsAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT program_name, connection_count, running_count, sleeping_count, dormant_count,
       total_cpu_time_ms, total_reads, total_writes, total_logical_reads,
       collection_time
FROM v_session_stats
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM v_session_stats WHERE server_id = $1)
ORDER BY connection_count DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var items = new List<SessionStatsRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new SessionStatsRow
            {
                ProgramName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                ConnectionCount = reader.IsDBNull(1) ? 0 : ToInt64(reader.GetValue(1)),
                RunningCount = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                SleepingCount = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                DormantCount = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                TotalCpuTimeMs = reader.IsDBNull(5) ? 0 : ToInt64(reader.GetValue(5)),
                TotalReads = reader.IsDBNull(6) ? 0 : ToInt64(reader.GetValue(6)),
                TotalWrites = reader.IsDBNull(7) ? 0 : ToInt64(reader.GetValue(7)),
                TotalLogicalReads = reader.IsDBNull(8) ? 0 : ToInt64(reader.GetValue(8)),
                CollectionTime = reader.GetDateTime(9)
            });
        }

        return items;
    }
}

public class ServerPropertiesRow
{
    public string Edition { get; set; } = "";
    public string ProductVersion { get; set; } = "";
    public string ProductLevel { get; set; } = "";
    public string ProductUpdateLevel { get; set; } = "";
    public int EngineEdition { get; set; }
    public int CpuCount { get; set; }
    public int HyperthreadRatio { get; set; }
    public long PhysicalMemoryMb { get; set; }
    public int SocketCount { get; set; }
    public int CoresPerSocket { get; set; }
    public bool IsHadrEnabled { get; set; }
    public bool IsClustered { get; set; }
    public string EnterpriseFeatures { get; set; } = "";
    public string ServiceObjective { get; set; } = "";
    public DateTime CollectionTime { get; set; }
}

public class DatabaseSizeStatsRow
{
    public string DatabaseName { get; set; } = "";
    public string FileName { get; set; } = "";
    public string FileTypeDesc { get; set; } = "";
    public string PhysicalName { get; set; } = "";
    public double TotalSizeMb { get; set; }
    public double UsedSizeMb { get; set; }
    public double AutoGrowthMb { get; set; }
    public double MaxSizeMb { get; set; }
    public string VolumeMountPoint { get; set; } = "";
    public double VolumeTotalMb { get; set; }
    public double VolumeFreeMb { get; set; }
    public DateTime CollectionTime { get; set; }
}

public class SessionStatsRow
{
    public string ProgramName { get; set; } = "";
    public long ConnectionCount { get; set; }
    public int RunningCount { get; set; }
    public int SleepingCount { get; set; }
    public int DormantCount { get; set; }
    public long TotalCpuTimeMs { get; set; }
    public long TotalReads { get; set; }
    public long TotalWrites { get; set; }
    public long TotalLogicalReads { get; set; }
    public DateTime CollectionTime { get; set; }
}
