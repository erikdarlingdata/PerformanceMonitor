/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Over MCP, what a server registration connects to is kept apart from what routes cover (#6281): a notification route
/// with an agent setting covers servers by id, tag or fleet-wide, so a second registration of a machine a first one
/// already names (another database or read-only intent, which hash to a new id) would sit outside the first one's tags
/// and routes. <c>add_servers</c> over MCP therefore refuses an entry whose target (engine, host and port) another
/// registration, removed ones included, already holds under a different storage name. The same storage name is not
/// refused here: it is the existing duplicate answer, or the re-use of a removed registration's own id.
/// </summary>
public sealed partial class DarlingMcpServerAdminTools
{
    /// <summary>A registration an add is compared with: a live definition (host, engine and port known) or a removed one
    /// (<c>collect.servers</c> keeps only its storage name).</summary>
    internal sealed record RegisteredTarget(string StorageKey, string? Host, string? Engine, int Port, string Label, bool Removed);

    /// <summary>The engine and port part of a storage name: <c>:pg:5432</c>, <c>:pg</c>, <c>:5432</c> or empty.</summary>
    private static string TargetSuffixOf(MonitoredServer server)
    {
        var target = ServerIdHelper.BuildStorageName(server.Host, null, false, server.Engine, server.Port);
        return target.Substring(server.Host.Length);
    }

    private static readonly Regex EngineSuffixPattern = new(@":pg(:\d+)?$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <summary>True when <paramref name="held"/> is a registration of the machine <paramref name="entry"/> names, under any
    /// database or read-only intent.</summary>
    internal static bool SameTarget(RegisteredTarget held, ParsedServerEntry entry)
    {
        var config = entry.ProbeConfig;
        var wanted = ServerIdHelper.BuildStorageName(config.Host, null, false, config.Engine, config.Port);
        if (!held.Removed)
        {
            return held.Host is not null
                && string.Equals(ServerIdHelper.BuildStorageName(held.Host, null, false, held.Engine, held.Port), wanted, StringComparison.OrdinalIgnoreCase);
        }

        /* A removed registration is known by its storage name alone: host[:database][:engine][:port][:RO]. */
        var name = held.StorageKey;
        if (name.EndsWith(":RO", StringComparison.OrdinalIgnoreCase))
        {
            name = name.Substring(0, name.Length - 3);
        }

        if (string.Equals(name, wanted, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var suffix = TargetSuffixOf(config);
        if (!name.StartsWith(config.Host + ":", StringComparison.OrdinalIgnoreCase)
            || name.Length <= config.Host.Length + 1 + suffix.Length)
        {
            return false;
        }

        return suffix.Length > 0
            ? name.EndsWith(suffix, StringComparison.OrdinalIgnoreCase)
            : !EngineSuffixPattern.IsMatch(name);
    }

    /// <summary>The refusal for an entry whose machine another registration already holds, or null.</summary>
    internal static string? TargetHeldRefusal(IReadOnlyList<RegisteredTarget> held, ParsedServerEntry entry)
    {
        foreach (var other in held)
        {
            if (string.Equals(other.StorageKey, entry.StorageKey, StringComparison.OrdinalIgnoreCase) || !SameTarget(other, entry))
            {
                continue;
            }

            return $"Not added: {entry.ProbeConfig.Host} is already registered as '{other.Label}'"
                + (other.Removed ? " (a removed registration)" : "")
                + ". Over MCP a machine has one registration, because the notification routes and tags that cover a server are tied to it. "
                + "Register another database or intent in the Viewer or on the web dashboard.";
        }

        return null;
    }

    private const string RegisteredTargetsSql =
        "SELECT host, database, read_only_intent, engine, port, name FROM config_monitored_servers";

    private const string RemovedRegistrationsSql =
        "SELECT server_name, display_name FROM collect.servers WHERE server_id NOT IN (SELECT server_id FROM config_monitored_servers)";

    private static async Task<List<RegisteredTarget>> LoadRegisteredTargetsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var targets = new List<RegisteredTarget>();
        await using (var command = postgres.CreateCommand(RegisteredTargetsSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var host = reader.GetString(0);
                var engine = reader.IsDBNull(3) ? null : reader.GetString(3);
                var port = reader.IsDBNull(4) ? 0 : reader.GetInt32(4);
                var key = StorageKeyOf(reader);
                var label = reader.IsDBNull(5) || string.IsNullOrWhiteSpace(reader.GetString(5)) ? key : reader.GetString(5);
                targets.Add(new RegisteredTarget(key, host, engine, port, label, false));
            }
        }

        await using (var command = postgres.CreateCommand(RemovedRegistrationsSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var name = reader.GetString(0);
                var label = reader.IsDBNull(1) || string.IsNullOrWhiteSpace(reader.GetString(1)) ? name : reader.GetString(1);
                targets.Add(new RegisteredTarget(name, null, null, 0, label, true));
            }
        }

        return targets;
    }
}
