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
    /// The server's probed <c>SERVERPROPERTY('EngineEdition')</c>, or
    /// <see cref="CollectorEngineCapability.UnknownEngineEdition"/> when the registry has no row, a NULL, or a
    /// zero (a server that has never completed a connect). Written by
    /// <c>RemoteCollectorService.PersistServerMetadataAsync</c> once per collection cycle.
    ///
    /// <para><b>Read from the STORE, not from the in-memory connection status</b> (#2511). The MCP surface
    /// resolves a server to the deterministic storage-name hash, not to the ServerManager's config id, so the
    /// live <c>ServerConnectionStatus</c> is not reachable from a tool by the id it holds — and the stored
    /// column is the durable fact anyway, which is the one Darling's twin reads. Deliberately takes NO
    /// <c>asOfUtc</c>: an engine edition is a property of the server, not of a window, and a read that
    /// accepted an anchor here would invite a caller to believe the capability answer had moved with it.</para>
    /// </summary>
    public async Task<int> GetSqlEngineEditionAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT sql_engine_edition
FROM servers
WHERE server_id = $1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var scalar = await command.ExecuteScalarAsync();
        return scalar is null or DBNull
            ? CollectorEngineCapability.UnknownEngineEdition
            : Convert.ToInt32(scalar);
    }
}
