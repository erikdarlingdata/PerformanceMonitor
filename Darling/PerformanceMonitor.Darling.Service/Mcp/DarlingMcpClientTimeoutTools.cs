/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The Client Timeouts read (#5640) — get_client_timeouts — over Darling's PostgreSQL store. The twin of Lite's
/// <c>McpClientTimeoutTools</c>: both run the one shared <see cref="AttentionWindowSql"/> text and judge it with the one shared
/// <see cref="ClientTimeoutsRead"/>, which applies the alert's own freshness and span rule, so the tool and the alert cannot
/// disagree about how many timeouts a window held. A STORED read (no live monitored-server hit). A PostgreSQL target answers
/// <c>not_collected</c> through the engine-capability question; a missing or denied capture session answers <c>precondition</c>.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpClientTimeoutTools
{
    private const string Collector = "attention";

    [McpServerTool(Name = "get_client_timeouts"), Description("Gets client timeouts (queries a client cancelled or timed out) for one server over a time range: the exact count and rate per minute, plus sampled top statements, databases, client apps and recent events. Samples are a subset, never the total. empty = collector ran, none counted; unavailable = no recent collector run; not_collected = PostgreSQL target; precondition = capture session missing.")]
    public static async Task<string> GetClientTimeouts(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 1.")] int hours_back = 1,
        [Description("Sampled events to return, newest first. Default 20. Never cuts the count or top lists.")] int limit = 20,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var unmet = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, Collector, cancellationToken);
            if (unmet != null)
            {
                return unmet;
            }

            var windowStart = windowEnd.AddHours(-hours_back);
            var (window, samples) = await ReadRangeAsync(postgres, resolved.ServerId, windowStart, windowEnd, limit, cancellationToken);
            var (kind, reading) = ClientTimeoutsRead.Classify(window, windowEnd);

            if (kind == ClientTimeoutsKind.NotObserved)
            {
                /* A collector that recorded a denial or a missing session on its latest run has a specific sentence, which is better
                   evidence than the generic one. */
                return await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.ServerName, Collector, cancellationToken)
                    ?? McpHelpers.Status("unavailable", ClientTimeoutsRead.UnavailableMessage(resolved.ServerName, window, windowEnd, hours_back));
            }

            if (kind == ClientTimeoutsKind.Quiet)
            {
                return McpHelpers.Status("empty", ClientTimeoutsRead.EmptyMessage(resolved.ServerName, reading!, hours_back));
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                client_timeouts = ClientTimeoutsRead.Payload(reading!, window, samples, limit, 2000),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_client_timeouts", ex);
        }
    }

    /// <summary>
    /// The seven reads on ONE connection: the six shared statements with the ceiling on ($3 = the window end), then the newest
    /// <paramref name="sampleLimit"/> + 1 sampled events. $1 server_id, $2/$3 naive UTC (unspecified kind, so Npgsql binds
    /// <c>timestamp</c> and never converts through the session's time zone).
    /// </summary>
    internal static async Task<(AttentionWindow Window, List<ClientTimeoutSample> Samples)> ReadRangeAsync(
        NpgsqlDataSource postgres, int serverId, DateTime windowStart, DateTime windowEnd, int sampleLimit, CancellationToken cancellationToken)
    {
        var floor = DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified);
        var ceiling = DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified);

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

        async Task<List<T>> Query<T>(string sql, Func<NpgsqlDataReader, T> map)
        {
            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, floor);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, ceiling);

            var rows = new List<T>();
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                rows.Add(map(reader));
            }

            return rows;
        }

        var runs = await Query(AttentionWindowSql.Runs("collection_log", bounded: true), r => r.GetDateTime(0));
        var counts = await Query(AttentionWindowSql.Counts("attention_events", bounded: true),
            r => new AttentionCountRow(r.GetDateTime(0), r.GetInt64(1)));
        var groups = await Query(AttentionWindowSql.SampleGroups("attention_events", bounded: true),
            r => new AttentionSampleGroup(
                r.GetDateTime(0),
                r.IsDBNull(1) ? null : r.GetString(1),
                r.IsDBNull(2) ? null : r.GetString(2),
                r.GetInt64(3)));
        var statements = await Query(AttentionWindowSql.TopStatements("attention_events", bounded: true),
            r => new AttentionTopItem(r.GetString(0), r.GetInt64(1), r.IsDBNull(2) ? null : r.GetString(2)));
        var databases = await Query(AttentionWindowSql.TopBy("attention_events", "database_name", bounded: true),
            r => new AttentionTopItem(r.GetString(0), r.GetInt64(1)));
        var applications = await Query(AttentionWindowSql.TopBy("attention_events", "client_app_name", bounded: true),
            r => new AttentionTopItem(r.GetString(0), r.GetInt64(1)));
        var samples = await Query(AttentionWindowSql.Samples("attention_events", sampleLimit + 1),
            r => new ClientTimeoutSample(
                r.IsDBNull(0) ? null : r.GetDateTime(0),
                r.GetDateTime(1),
                r.IsDBNull(2) ? null : r.GetString(2),
                r.IsDBNull(3) ? null : r.GetString(3),
                r.IsDBNull(4) ? null : r.GetString(4),
                r.IsDBNull(5) ? null : r.GetInt32(5),
                r.IsDBNull(6) ? null : r.GetInt64(6),
                r.IsDBNull(7) ? null : r.GetString(7),
                r.IsDBNull(8) ? null : r.GetString(8)));

        return (new AttentionWindow(runs, counts, groups, statements, databases, applications), samples);
    }
}
