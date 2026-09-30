/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// The server's probed <c>SERVERPROPERTY('EngineEdition')</c> from its newest collected
    /// <c>v_server_properties</c> row, or <see cref="CollectorEngineCapability.UnknownEngineEdition"/> when the
    /// server has no row yet (one that has never completed a collection).
    ///
    /// <para><b>The durable fact is the collected row, not a registry column.</b> This used to read
    /// <c>servers.sql_engine_edition</c>, but nothing in this SKU ever inserts a <c>servers</c> row — the only
    /// writer was an UPDATE that matched nothing — so the read answered unknown for every server and the
    /// <c>not_collected</c> gate in <see cref="Mcp.McpEngineCapability"/> never fired on Azure SQL Database.
    /// <c>server_properties</c> is written by the collector on every cycle and is what the analysis engine
    /// already reads its own edition fact from (<c>DuckDbFactCollector</c>), so both answer from the same row.</para>
    ///
    /// <para><b>Read from the STORE, not from the in-memory connection status</b> (#2511). The MCP surface
    /// resolves a server to the deterministic storage-name hash, not to the ServerManager's config id, so the
    /// live <c>ServerConnectionStatus</c> is not reachable from a tool by the id it holds. Deliberately takes NO
    /// <c>asOfUtc</c>: an engine edition is a property of the server, not of a window, and a read that
    /// accepted an anchor here would invite a caller to believe the capability answer had moved with it.</para>
    /// </summary>
    public async Task<int> GetSqlEngineEditionAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT engine_edition
FROM v_server_properties
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var scalar = await command.ExecuteScalarAsync();
        return scalar is null or DBNull
            ? CollectorEngineCapability.UnknownEngineEdition
            : Convert.ToInt32(scalar);
    }
}
