/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitorDashboard.Interfaces
{
    /// <summary>
    /// Interface for secure credential storage operations.
    /// </summary>
    public interface ICredentialService
    {
        /// <summary>
        /// Saves SQL Server credentials to secure storage.
        /// </summary>
        /// <param name="serverId">Unique server identifier</param>
        /// <param name="username">SQL Server username</param>
        /// <param name="password">SQL Server password</param>
        /// <returns>True if successful, false otherwise</returns>
        bool SaveCredential(string serverId, string username, string password);

        /// <summary>
        /// Retrieves SQL Server credentials from secure storage.
        /// </summary>
        /// <param name="serverId">Unique server identifier</param>
        /// <returns>Tuple of (username, password) or null if not found</returns>
        (string Username, string Password)? GetCredential(string serverId);

        /// <summary>
        /// Deletes SQL Server credentials from secure storage.
        /// </summary>
        /// <param name="serverId">Unique server identifier</param>
        /// <returns>True if deleted or didn't exist, false on error</returns>
        bool DeleteCredential(string serverId);

        /// <summary>
        /// Checks if credentials exist for a server.
        /// </summary>
        /// <param name="serverId">Unique server identifier</param>
        /// <returns>True if credentials exist</returns>
        bool CredentialExists(string serverId);

        /// <summary>
        /// Updates existing credentials. If credentials don't exist, creates them.
        /// </summary>
        /// <param name="serverId">Unique server identifier</param>
        /// <param name="username">SQL Server username</param>
        /// <param name="password">SQL Server password</param>
        /// <returns>True if successful, false otherwise</returns>
        bool UpdateCredential(string serverId, string username, string password);
    }
}
