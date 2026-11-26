/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #9780: the keys of an <c>add_servers</c> / <c>--add-server</c> entry that Darling's parse does not read, named in that entry's result
/// (the logic is <see cref="AddServerEntryKeys"/>, shared with Lite's add, over the keys this parse reads). Darling matches key names exactly.
/// </summary>
internal static class DarlingMcpAddServerKeys
{
    /// <summary>Every key <see cref="DarlingMcpServerAdminTools.ParseRequest(string)"/> reads from an entry. A key the parse starts to read
    /// is added here, or the answer names it as unread.</summary>
    internal static readonly string[] Read =
    [
        "host", "display_name", "database", "engine", "auth", "username", "password", "port",
        "encrypt_mode", "trust_server_certificate", "read_only_intent", "multi_subnet_failover",
        "aws_role_arn", "aws_external_id", "query_store_replica", "capture_plans", "alert_delivery_mode_override",
    ];

    private static readonly AddServerEntryKeys s_keys = new(Read, StringComparer.Ordinal);

    /// <summary>The unread keys of every entry in <paramref name="serversJson"/>, by the entry's index in the array. An entry that is not
    /// an object has none here: the parse already answers for it.</summary>
    internal static Dictionary<int, IReadOnlyList<UnreadServerKey>> ByEntry(string serversJson)
    {
        var byEntry = new Dictionary<int, IReadOnlyList<UnreadServerKey>>();
        if (JsonNode.Parse(serversJson) is not JsonArray array)
        {
            return byEntry;
        }

        for (var i = 0; i < array.Count; i++)
        {
            if (array[i] is JsonObject entry && s_keys.Unread(entry.Select(property => property.Key)) is { Count: > 0 } unread)
            {
                byEntry[i] = unread;
            }
        }

        return byEntry;
    }
}
