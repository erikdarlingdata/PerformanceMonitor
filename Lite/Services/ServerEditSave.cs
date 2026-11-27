/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #9640: the write the Edit Server window's Save makes once its checks pass, with no window in it, so the window and the
/// <c>edit_server</c> MCP tool store an edit the same way. The caller has already put the new settings on
/// <paramref name="server"/> (the live list entry, as the window does).
/// </summary>
internal static class ServerEditSave
{
    /// <summary>
    /// Saves an edited server. A profile-backed server stores no per-server secret: the per-server Azure and managed identity
    /// ids are cleared and any stored credential is deleted, so a later switch back to inline cannot bring a stale secret
    /// back (M-3). Otherwise the username and password go to <see cref="ServerManager.UpdateServer"/>. The host the server now
    /// names is then confirmed for this user, as Save does after the password was entered again where the server has one.
    /// </summary>
    internal static void Persist(ServerManager manager, ServerConnection server, bool useProfile, string? username, string? password)
    {
        if (useProfile)
        {
            server.AzureClientId = null;
            server.AzureTenantId = null;
            server.ManagedIdentityClientId = null;
            manager.UpdateServer(server, null, null);
            if (ServerCredentialIds.Allows(server.Id, server.DisplayName))
            {
                manager.CredentialService.DeleteCredential(server.Id);
            }
        }
        else
        {
            manager.UpdateServer(server, username, password);
        }

        manager.ConfirmHost(server);
    }
}
