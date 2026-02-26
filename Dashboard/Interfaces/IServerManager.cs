/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Interfaces
{
    /// <summary>
    /// Interface for server connection management.
    /// </summary>
    public interface IServerManager
    {
        /// <summary>
        /// Gets all configured servers, ordered by favorites and last connected.
        /// </summary>
        List<ServerConnection> GetAllServers();

        /// <summary>
        /// Gets a server by its unique identifier.
        /// </summary>
        ServerConnection? GetServerById(string id);

        /// <summary>
        /// Adds a new server configuration.
        /// </summary>
        void AddServer(ServerConnection server, string? username = null, string? password = null);

        /// <summary>
        /// Updates an existing server configuration.
        /// </summary>
        void UpdateServer(ServerConnection server, string? username = null, string? password = null);

        /// <summary>
        /// Deletes a server configuration.
        /// </summary>
        void DeleteServer(string id);

        /// <summary>
        /// Updates the last connected timestamp for a server.
        /// </summary>
        void UpdateLastConnected(string id);

        /// <summary>
        /// Gets the current connection status for a server.
        /// </summary>
        ServerConnectionStatus GetConnectionStatus(string serverId);

        /// <summary>
        /// Tests connectivity to a single server and updates its status.
        /// </summary>
        Task<ServerConnectionStatus> CheckConnectionAsync(string serverId, bool allowInteractiveAuth = false);

        /// <summary>
        /// Tests connectivity to all servers and updates their statuses.
        /// </summary>
        Task CheckAllConnectionsAsync();
    }
}
