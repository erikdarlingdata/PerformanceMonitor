/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Queues one command row for the service through <c>config.request_mcp_command</c> (#6230): the only way the <c>mcp</c> login writes a
/// command row. The function names the command types it accepts, stamps the row <c>mcp</c> whatever the caller sends, and
/// refuses a repeat inside 15 seconds, so the tools that queue (pause, resume, analyze, purge, connection test) share this one call.
/// </summary>
internal static class DarlingMcpCommandQueue
{
    internal const string RequestedBy = "mcp";

    /// <summary>What the store function answered: the new command id, or the refusal in plain words.</summary>
    internal readonly record struct Queued(long CommandId, string? Reason, string? Message);

    /// <summary>Queues one command. Its refusal codes map to plain reasons; any other failure propagates to the caller.</summary>
    internal static async Task<Queued> QueueAsync(
        NpgsqlDataSource postgres, string commandType, int? serverId, int? retentionDays, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = postgres.CreateCommand("SELECT config.request_mcp_command($1, $2, $3, $4)");
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = commandType });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId.HasValue ? serverId.Value : DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = retentionDays.HasValue ? retentionDays.Value : DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = RequestedBy });
            var id = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
            return new Queued(id, null, null);
        }
        catch (PostgresException ex) when (ex.SqlState is "DL400" or "DL404" or "DL409" or "DL423" or "DL429")
        {
            var reason = ex.SqlState switch
            {
                "DL404" => "unknown_server",
                "DL409" => "already_queued",
                "DL423" => "busy",
                "DL429" => "too_soon",
                _ => "invalid",
            };
            return new Queued(0, reason, ex.MessageText + " Nothing was queued.");
        }
        catch (PostgresException ex) when (ex.SqlState is "42883" or "42501")
        {
            return new Queued(0, "store_not_ready",
                "The store has no queue function for this tool yet, or the mcp login may not call it. Restart the Darling service so it re-provisions the store roles, or re-run " + DarlingManagedRoles.ProvisionRolesScriptPath + " on a self-managed store. Nothing was queued.");
        }
    }
}
