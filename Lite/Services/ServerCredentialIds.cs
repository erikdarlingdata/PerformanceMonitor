/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// A server's saved password is kept in Windows Credential Manager under <c>PerformanceMonitorLite_</c> plus the server's Id.
/// Lite's other saved secrets (the pseudonym key, the webhook URLs, the SMTP password) use the same prefix with fixed names,
/// so a server whose Id spelled one of those names would read, overwrite or delete that secret. Lite makes every server Id with
/// <c>Guid.NewGuid()</c>, and no fixed name is a GUID, so a saved password is read, saved or deleted only for a server whose
/// Id is a GUID. A server with any other Id (from a hand-edited or imported <c>servers.json</c>) stays in the list and is
/// monitored, with no saved password; the first refusal for it is logged once, naming its display name.
/// </summary>
internal static class ServerCredentialIds
{
    private static readonly ConcurrentDictionary<string, byte> Logged = new(StringComparer.Ordinal);

    private static readonly ConcurrentDictionary<string, byte> MissingLogged = new(StringComparer.Ordinal);

    /// <summary>
    /// #9730: a SQL Server or service principal server whose saved password is not found under this Windows logon connects with no
    /// user and fails as <c>Login failed for user ''</c>, which names neither the cause nor the fix. Logs one warning per server
    /// and process that says so. The text carries no password and no user name.
    /// </summary>
    internal static void WarnPasswordMissing(string serverId, string? displayName)
    {
        if (MissingLogged.TryAdd("server:" + serverId, 0))
        {
            AppLogger.Warn("Credentials",
                $"No saved password was found for server '{displayName}' under this Windows logon. "
                + "Lite keeps each password in the Windows Credential Manager of the user that saved it. "
                + "A scheduled task set to Do not store password can read saved passwords only while that user is signed in. "
                + "A password saved from a remote PowerShell session can be unreadable to a scheduled task. "
                + "Add the server again with --add-server as the Windows user that runs Lite, in a session on this machine, "
                + "or open the server in Edit Server and save the password.");
        }
    }

    /// <summary>
    /// #9730: the saved password exists but Windows refused to read it (an Unreadable read, not a NotFound). Says so, once per server
    /// and process, apart from the not-found line. <paramref name="failure"/> is "Windows returned error N" or an exception type name.
    /// </summary>
    internal static void WarnPasswordUnreadable(string serverId, string? displayName, string? failure)
    {
        if (MissingLogged.TryAdd("server-unreadable:" + serverId, 0))
        {
            AppLogger.Warn("Credentials",
                $"A saved password for server '{displayName}' exists but Windows refused to read it under this Windows logon ({failure}). "
                + "Lite keeps each password in the Windows Credential Manager of the user that saved it. "
                + "A password saved from a remote PowerShell session can be unreadable to a scheduled task. "
                + "Add the server again with --add-server as the Windows user that runs Lite, in a session on this machine, "
                + "or open the server in Edit Server and save the password.");
        }
    }

    /// <summary>#9730: the unreadable-entry warning for a credential profile, once per profile and process.</summary>
    internal static void WarnProfileSecretUnreadable(string profileId, string? profileName, string? serverDisplayName, string? failure)
    {
        if (MissingLogged.TryAdd("profile-unreadable:" + profileId, 0))
        {
            AppLogger.Warn("Credentials",
                $"A saved password for credential profile '{profileName}' (first used by server '{serverDisplayName}') exists but Windows refused to read it under this Windows logon ({failure}). "
                + "Lite keeps each password in the Windows Credential Manager of the user that saved it. "
                + "Open the profile and save its password as the Windows user that runs Lite, in a session on this machine.");
        }
    }

    /// <summary>#9730: the same warning for a credential profile with no saved secret, once per profile and process.</summary>
    internal static void WarnProfileSecretMissing(string profileId, string? profileName, string? serverDisplayName)
    {
        if (MissingLogged.TryAdd("profile:" + profileId, 0))
        {
            AppLogger.Warn("Credentials",
                $"No saved password was found for credential profile '{profileName}' (first used by server '{serverDisplayName}') under this Windows logon. "
                + "Lite keeps each password in the Windows Credential Manager of the user that saved it. "
                + "A scheduled task set to Do not store password can read saved passwords only while that user is signed in. "
                + "Open the profile and save its password as the Windows user that runs Lite, in a session on this machine.");
        }
    }

    /// <summary>True when <paramref name="serverId"/> is a GUID, the only kind of Id a saved password is used for.</summary>
    internal static bool IsUsable(string? serverId) => Guid.TryParse(serverId, out _);

    /// <summary>
    /// True when the server's saved password may be read, saved or deleted. For any other Id it returns false and logs one line
    /// (once per Id and process) naming <paramref name="displayName"/>.
    /// </summary>
    internal static bool Allows(string? serverId, string? displayName)
    {
        if (IsUsable(serverId))
        {
            return true;
        }

        if (Logged.TryAdd(serverId ?? "", 0))
        {
            AppLogger.Warn("Credentials",
                $"Server '{displayName}' has an Id that is not a GUID, so no saved password is read, saved or deleted for it. "
                + "Remove the server and add it again to give it a normal Id.");
        }

        return false;
    }
}
