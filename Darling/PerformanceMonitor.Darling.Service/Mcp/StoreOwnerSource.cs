/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Npgsql;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The service's own (store owner) data source, handed to the one MCP tool that has to read a monitored server's remediation slot (#5632,
/// step 7). The <c>mcp</c> store role cannot read that column by design, so the worker publishes the data source it already holds, the way
/// it publishes the monitored-server registry (<see cref="MonitoredServerRegistryState"/>). No tool exposes it: it is a service
/// parameter, so a client cannot supply one, and <see cref="DarlingMcpPlanForceTools"/> reads one server's login
/// name and secret reference to hand to the remediation code. The secret itself is resolved in that code, never read here.
/// The web host also uses this source outside its read pool to load and activate sealed managed-group tokens;
/// ordinary requests read only group metadata through the web role. No endpoint exposes the owner data source.
/// </summary>
public sealed class StoreOwnerSource
{
    private volatile NpgsqlDataSource? _current;

    /// <summary>Publishes the worker's data source (once at start). Null clears it.</summary>
    public void Publish(NpgsqlDataSource? source) => _current = source;

    /// <summary>The published data source, or null before the worker has published one.</summary>
    public NpgsqlDataSource? Read() => _current;
}
