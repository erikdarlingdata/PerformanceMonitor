/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The per-server Query Store replica setting (#6254, <c>config_monitored_servers.query_store_replica</c>): a host, optionally
/// with a port (<c>host,port</c>), or an availability group listener name. Empty, the default, keeps today's behaviour:
/// Query Store is read from the monitored server itself.
///
/// <para>The setting is a field of <see cref="ServerConnectionIdentity"/>, so every edit and connection test treats a change of it
/// as a change of how the server is reached. <see cref="PasswordBinding.ForServer"/> binds it into a sealed password only when it
/// is not empty, so a seal made before the setting existed still opens, and a writer who sets or changes the replica host
/// cannot make the service send the stored password there: the password has to be entered again.</para>
/// </summary>
public static class QueryStoreReplicaSetting
{
    /// <summary>The setting as it is used: trimmed, and null when it is null, empty or only white space. Every comparison,
    /// binding and connection string reads the setting through this one rule.</summary>
    public static string? Normalize(string? value)
    {
        var trimmed = value?.Trim();
        return string.IsNullOrEmpty(trimmed) ? null : trimmed;
    }

    /// <summary>True when the two settings name different replicas (ordinally, after <see cref="Normalize"/>; both empty
    /// is the same). A stored password, or a secret reference the file declares, may be used for the replica only when this
    /// is false against the setting it was saved or declared for.</summary>
    public static bool Differ(string? a, string? b) =>
        !string.Equals(Normalize(a), Normalize(b), StringComparison.Ordinal);
}
