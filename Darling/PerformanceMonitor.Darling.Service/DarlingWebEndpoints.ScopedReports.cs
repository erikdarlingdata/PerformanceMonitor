/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service;

public static partial class DarlingWebEndpoints
{
    internal static async Task<FleetSweepRun> ScopeSweepAsync(
        NpgsqlDataSource postgres, FleetSweepRun run, IReadOnlyList<int> ids, CancellationToken cancellationToken) =>
        (await ScopeSweepsAsync(postgres, new[] { run }, ids, cancellationToken))[0];

    internal static async Task<List<FleetSweepRun>> ScopeSweepsAsync(
        NpgsqlDataSource postgres, IReadOnlyList<FleetSweepRun> runs, IReadOnlyList<int> ids, CancellationToken cancellationToken)
    {
        var verdicts = new Dictionary<long, List<FleetSweepServerVerdict>>();
        var ledger = new Dictionary<long, List<FleetSweepWouldHavePagedEntry>>();
        // Fetch predecessors outside the requested window too, once each. A wider timeline must not
        // turn into two or three round trips per sweep, or read other servers' evidence into memory.
        var sweepIds = runs.Select(run => run.SweepId)
            .Concat(runs.Where(run => run.PreviousSweepId.HasValue).Select(run => run.PreviousSweepId!.Value))
            .Distinct().ToArray();
        if (sweepIds.Length > 0 && ids.Count > 0)
        {
            await using (var command = postgres.CreateCommand("""
                SELECT sweep_id, server_id, server_name, band, previous_band, band_reason, verdict_json
                FROM collect.fleet_sweep_server_verdicts
                WHERE sweep_id = ANY($1)
                AND   server_id = ANY($2)
                ORDER BY sweep_id, server_name, server_id
                """))
            {
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Bigint, sweepIds);
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, ids.ToArray());
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var sweepId = reader.GetInt64(0);
                    if (!verdicts.TryGetValue(sweepId, out var rows)) verdicts[sweepId] = rows = new();
                    rows.Add(new FleetSweepServerVerdict(reader.GetInt32(1), reader.GetString(2), reader.GetString(3),
                        reader.IsDBNull(4) ? null : reader.GetString(4),
                        reader.IsDBNull(5) ? null : reader.GetString(5),
                        reader.IsDBNull(6) ? null : reader.GetString(6)));
                }
            }

            var mutedIds = runs.Where(run => !run.AlertsEnabled).Select(run => run.SweepId).Distinct().ToArray();
            if (mutedIds.Length > 0)
            {
                await using var command = postgres.CreateCommand("""
                    SELECT sweep_id, server_id, alert_family, evidence_json
                    FROM collect.fleet_sweep_would_have_paged
                    WHERE sweep_id = ANY($1)
                    AND   server_id = ANY($2)
                    ORDER BY sweep_id, server_id, alert_family
                    """);
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Bigint, mutedIds);
                command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Integer, ids.ToArray());
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var sweepId = reader.GetInt64(0);
                    if (!ledger.TryGetValue(sweepId, out var rows)) ledger[sweepId] = rows = new();
                    rows.Add(new FleetSweepWouldHavePagedEntry(reader.GetInt32(1), reader.GetString(2), reader.GetString(3)));
                }
            }
        }

        return runs.Select(run => ScopeSweep(run, ids,
            verdicts.GetValueOrDefault(run.SweepId) ?? new(),
            run.PreviousSweepId is long previousId ? verdicts.GetValueOrDefault(previousId) ?? new() : new(),
            ledger.GetValueOrDefault(run.SweepId) ?? new())).ToList();
    }

    private static FleetSweepRun ScopeSweep(
        FleetSweepRun run, IReadOnlyList<int> ids, List<FleetSweepServerVerdict> verdicts,
        List<FleetSweepServerVerdict> previous, List<FleetSweepWouldHavePagedEntry> ledger)
    {
        var original = ReadScopedReportObject(run.ReportJson);
        var watch = original["watch"] as JsonObject;
        var reported = verdicts.Count(verdict =>
            ReadScopedReportObject(verdict.VerdictJson)["has_data"] is JsonValue node &&
            node.TryGetValue<bool>(out var hasData) && hasData);
        var currentIds = verdicts.Select(verdict => verdict.ServerId).ToHashSet();
        var report = new JsonObject
        {
            ["sweep_id"] = run.SweepId.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["previous_sweep_id"] = run.PreviousSweepId?.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["alerts_enabled"] = run.AlertsEnabled,
            ["instruments_alive"] = run.InstrumentsAlive,
            ["no_previous_sweep"] = run.PreviousSweepId is null,
            ["fleet"] = new JsonObject
            {
                ["servers_expected"] = verdicts.Count,
                ["servers_reported"] = reported,
                ["bands"] = JsonSerializer.SerializeToNode(verdicts.GroupBy(verdict => verdict.Band)
                    .ToDictionary(group => group.Key, group => group.Count())),
            },
            ["changes"] = new JsonObject
            {
                ["band_transitions"] = JsonSerializer.SerializeToNode(verdicts
                    .Where(verdict => verdict.PreviousBand is not null && verdict.Band != verdict.PreviousBand)
                    .Select(verdict => new { server = verdict.ServerName, from = verdict.PreviousBand, to = verdict.Band, reason = verdict.BandReason })),
                ["new_servers"] = JsonSerializer.SerializeToNode(verdicts
                    .Where(verdict => run.PreviousSweepId is not null && verdict.PreviousBand is null)
                    .Select(verdict => verdict.ServerName)),
                ["departed_servers"] = JsonSerializer.SerializeToNode(previous.Where(verdict => !currentIds.Contains(verdict.ServerId))
                    .Select(verdict => verdict.ServerName)),
            },
            ["watch"] = new JsonObject
            {
                ["opened"] = ScopedWatchEntries(watch?["opened"], ids),
                ["closed"] = ScopedWatchEntries(watch?["closed"], ids),
                ["carried"] = ScopedWatchEntries(watch?["carried"], ids),
            },
        };
        if (!run.AlertsEnabled)
        {
            report["would_have_paged"] = JsonSerializer.SerializeToNode(ledger.Select(entry =>
                new { server_id = entry.ServerId, family = entry.AlertFamily, evidence = entry.EvidenceJson }));
        }

        var liveness = new JsonObject
        {
            ["scope"] = "service",
            ["notes"] = new JsonArray("Collector-service health is shared. Server lists and counts are restricted to this token's tags."),
        };
        return run with
        {
            ServersExpected = verdicts.Count,
            ServersReported = reported,
            ReportJson = report.ToJsonString(),
            InstrumentLivenessJson = liveness.ToJsonString(),
        };
    }

    private static JsonArray ScopedWatchEntries(JsonNode? source, IReadOnlyList<int> ids)
    {
        var entries = new JsonArray();
        if (source is not JsonArray array) return entries;
        foreach (var item in array.OfType<JsonObject>())
        {
            if (item["server_id"] is not JsonValue idNode || !idNode.TryGetValue<int>(out var id) || !ids.Contains(id))
            {
                continue;
            }

            var entry = new JsonObject();
            foreach (var key in new[] { "server_id", "item", "condition", "consecutive_hits", "consecutive_misses" })
            {
                if (item[key] is { } value) entry[key] = value.DeepClone();
            }

            entries.Add(entry);
        }

        return entries;
    }

    private static JsonObject ReadScopedReportObject(string? text)
    {
        if (text is null) return new JsonObject();
        try
        {
            return JsonNode.Parse(text) as JsonObject ?? new JsonObject();
        }
        catch (JsonException)
        {
            return new JsonObject();
        }
    }
}
