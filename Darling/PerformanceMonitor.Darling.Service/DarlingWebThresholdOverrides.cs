/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The threshold-overrides grid's routes (#5916 part 4), mapped in <see cref="DarlingWebEndpoints"/> (the statement census allows no other mapper file): <c>GET /api/threshold-overrides</c> lists every override with its server,
/// the global value it replaces and who set it, and <c>PUT /api/threshold-overrides</c> sets or clears one. The write is the
/// <c>set_alert_threshold_override</c> tool's own body (<see cref="DarlingMcpThresholdOverrideTools.SetAsync"/>) run on the writes
/// arm, so the key registry, the bounds, the database scope and the store statement are the tool's, not a second copy. It answers as
/// the pending-note decisions do (#5903): <c>application/json</c> required, born gated by the host's write gate (a read-only seat
/// is refused every unsafe method), and 403 with <see cref="RefusedMessage"/> when the store login holds no write grant on the table
/// (the <c>viewer</c> role of a managed or compose store, or a bring-your-own login that is not an admin one), 400 for a refused key,
/// value or server, a fixed-sentence 500 for any other store failure. The change is stamped with the seat's
/// <see cref="DarlingWebSeat.EditorPrincipal"/> as every web write is.
/// </summary>
internal static class DarlingWebThresholdOverrides
{
    /// <summary>The 403 sentence a refused write answers: the web host runs on whatever login <c>postgres.webConnectionString</c>
    /// names, and writing an override is an <c>admin</c> role write.</summary>
    internal static string RefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("change threshold overrides", managedStore, "Change them in the Viewer app",
            " If this login already is the admin role, the store has not had "
            + DarlingManagedRoles.ProvisionRolesScriptPath + " re-run since the upgrade that added threshold overrides: run it once as a superuser (it is idempotent).",
            adminWritesOff);

    /// <summary>The list body: <c>{overrides: [{server_id, server_name, tag_id, tag_name, orphaned, database, key, value, global_value, set_by,
    /// request_id, updated_at}], keys: [{key, min, max, database_scope}], servers: [{server_id, server_name}], tags: [{tag_id, tag_name}],
    /// can_edit, note}</c>. A tag override (#6926) has a tag_id, a null server_id and server_name, and orphaned true when its tag was deleted.
    /// <c>can_edit</c> is whether the store login holds the write grant, so a page can say list-only before anyone tries.</summary>
    internal static async Task<JsonObject> ListBodyAsync(NpgsqlDataSource postgres, CancellationToken ct, NpgsqlDataSource? accessSource = null)
    {
        var overrides = new JsonArray();
        string? note = null;
        try
        {
            foreach (var r in await ThresholdOverrideStore.ListAsync(
                         postgres, k => DarlingThresholdKeys.TryGet(k, out var e) ? e.Column : null, ct))
            {
                overrides.Add(new JsonObject
                {
                    ["server_id"] = r.IsTag ? null : r.ServerId,
                    ["server_name"] = r.IsTag ? null : r.ServerName,
                    ["tag_id"] = r.TagId,
                    ["tag_name"] = r.IsTag && !r.Orphaned ? r.ServerName : null,
                    ["orphaned"] = r.Orphaned,
                    ["database"] = r.Database,
                    ["key"] = r.Key,
                    ["value"] = r.Value,
                    ["global_value"] = r.GlobalValue,
                    ["set_by"] = r.SetBy,
                    ["request_id"] = r.RequestId,
                    ["updated_at"] = r.UpdatedAt.ToString("o", CultureInfo.InvariantCulture),
                });
            }
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            note = DarlingMcpThresholdOverrideTools.PreconditionMessage;
        }

        var keys = new JsonArray();
        foreach (var entry in DarlingThresholdKeys.Entries)
        {
            keys.Add(new JsonObject
            {
                ["key"] = entry.Wire,
                ["min"] = entry.Min,
                ["max"] = entry.Max,
                ["database_scope"] = entry.Scope == AlertThresholdScope.ServerAndDatabase,
            });
        }

        var servers = new JsonArray();
        await using (var command = postgres.CreateCommand(
                         "SELECT server_id, server_name FROM collect.servers WHERE is_enabled ORDER BY server_name"))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                servers.Add(new JsonObject { ["server_id"] = reader.GetInt32(0), ["server_name"] = reader.GetString(1) });
        }

        var tags = new JsonArray();
        await using (var command = postgres.CreateCommand("SELECT id, name FROM config.server_tags ORDER BY name, id"))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                tags.Add(new JsonObject { ["tag_id"] = reader.GetInt32(0), ["tag_name"] = reader.GetString(1) });
        }

        return new JsonObject
        {
            ["overrides"] = overrides,
            ["keys"] = keys,
            ["servers"] = servers,
            ["tags"] = tags,
            ["can_edit"] = await DarlingMcpThresholdOverrideTools.HoldsGrantAsync(accessSource ?? postgres, ct),
            ["note"] = note,
        };
    }

    /// <summary>
    /// One write: the tool's writes-arm body, its answer mapped to an HTTP status. The tool answers <c>precondition</c> when the
    /// store login has no write grant (checked before any statement) or the store refuses for privilege (SQLSTATE 42501): both
    /// are this route's 403. <c>refused</c> is a key, value, database or <c>set_by</c> the tool will not write: 400.
    /// </summary>
    internal static async Task<(int Status, JsonNode Body)> ChangeAsync(
        NpgsqlDataSource postgres, ILogger logger, JsonObject body, string editor, CancellationToken ct, bool managedStore = false, bool adminWritesOff = false)
    {
        var server = body["server_name"]?.GetValueKind() == JsonValueKind.String ? body["server_name"]!.GetValue<string>() : null;
        var key = body["key"]?.GetValueKind() == JsonValueKind.String ? body["key"]!.GetValue<string>() : null;
        var database = body["database_name"]?.GetValueKind() == JsonValueKind.String ? body["database_name"]!.GetValue<string>() : null;
        var tag = body["tag"]?.GetValueKind() == JsonValueKind.String ? body["tag"]!.GetValue<string>() : null;
        var clear = body["clear"]?.GetValueKind() == JsonValueKind.True;

        int? value = null;
        if (body["value"] is JsonValue v && v.GetValueKind() != JsonValueKind.Null)
        {
            if (v.GetValueKind() != JsonValueKind.Number || !v.TryGetValue<int>(out var parsed))
                return (400, new JsonObject { ["error"] = "value must be an integer." });
            value = parsed;
        }

        if (string.IsNullOrWhiteSpace(server) && string.IsNullOrWhiteSpace(tag))
            return (400, new JsonObject { ["error"] = "server_name or tag is required." });

        var json = await DarlingMcpThresholdOverrideTools.SetAsync(
            postgres, new DarlingMcpWritePolicy.Effective(true, false),
            new InvestigationRequestStore(postgres, McpCommandDeadlines.ReadSeconds), logger,
            server, key ?? string.Empty, value, database, clear, ThresholdOverrideStore.SetByFor(editor), null, ct, tag: tag);

        var answer = JsonNode.Parse(json)!.AsObject();
        var status = answer["status"]?.GetValue<string>();
        switch (status)
        {
            case "ok":
            case "cleared":
                return (200, answer);
            case "precondition":
                return (403, new JsonObject { ["error"] = RefusedMessage(managedStore, adminWritesOff) });
            case "refused":
            case "invalid":
                return (400, new JsonObject
                {
                    ["error"] = answer["message"]?.GetValue<string>() ?? "That override was refused.",
                    ["reason"] = answer["reason"]?.GetValue<string>(),
                });
            default:
                return (500, new JsonObject { ["error"] = "The override could not be saved." });
        }
    }
}
