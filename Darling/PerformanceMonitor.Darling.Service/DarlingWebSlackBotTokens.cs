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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #7656: the web Admin page's write of the Slack bot token and app-level token on the notification settings row
/// (<c>PUT /api/slack-bot-tokens</c>). Each token is sealed by the service for its own slot on the settings row and no proxy
/// (<see cref="NotificationRouteWriter.SealNewSettingsValue"/>, the code the Viewer and the channels tool seal with) and written
/// through the store's <c>set_notification_channels</c> function, which stores only sealed text or empty and returns only whether
/// each was saved before. A field left out, null or blank keeps the saved token; <c>-</c> removes it. The answer says set or not
/// set and never carries a value, sealed or typed; a refused value is named by its field and never echoed.
/// </summary>
internal static class DarlingWebSlackBotTokens
{
    internal const string Route = "/api/slack-bot-tokens";

    /// <summary>The body fields, in the store function's column names, with each one's slot and label.</summary>
    private static readonly (string Field, string Slot, string Label)[] s_fields =
    [
        ("slack_bot_token", PasswordBinding.SlackBotTokenSlot, "Slack bot token"),
        ("slack_app_token", PasswordBinding.SlackAppTokenSlot, "Slack app-level token"),
    ];

    /// <summary>
    /// PURE: the columns to change and their stored values (sealed text, or empty for a clear) from the body, or the sentence that refuses
    /// it: an unknown field, a value that is not text, saved text instead of a value, or a ring that cannot seal (its own words).
    /// </summary>
    internal static (string[] Columns, string[] Values, string? Error) BuildWrite(JsonObject body, IPasswordKeyRing ring)
    {
        ArgumentNullException.ThrowIfNull(body);
        ArgumentNullException.ThrowIfNull(ring);
        if (body.Select(p => p.Key).FirstOrDefault(k => !s_fields.Any(f => string.Equals(f.Field, k, StringComparison.Ordinal))) is { } unknown)
        {
            return ([], [], "Unknown field: " + (unknown.Length > 40 ? unknown[..40] : unknown) + ".");
        }

        var (seal, refusal) = DarlingMcpAlertTools.RouteSealer(ring);
        var columns = new List<string>();
        var values = new List<string>();
        foreach (var (field, slot, label) in s_fields)
        {
            if (!body.ContainsKey(field) || body[field] is null)
            {
                continue;
            }

            if (body[field] is not JsonValue node || node.GetValueKind() != JsonValueKind.String)
            {
                return ([], [], field + " must be text.");
            }

            var text = node.GetValue<string>().Trim();
            if (text.Length == 0)
            {
                continue;
            }

            try
            {
                columns.Add(field);
                values.Add(text == NotificationRouteWriter.ClearMarker ? "" : NotificationRouteWriter.SealNewSettingsValue(slot, text, null, null, label, seal, refusal));
            }
            catch (NotificationRouteRefusedException ex)
            {
                return ([], [], ex.Message);
            }
        }

        return (columns.ToArray(), values.ToArray(), null);
    }

    /// <summary>PURE: the answer. Each named token as set or not set before (the function's flag) and after (whether a value was stored).</summary>
    internal static JsonObject Answer(string[] columns, bool?[] previousSaved, string[] values)
    {
        ArgumentNullException.ThrowIfNull(columns);
        ArgumentNullException.ThrowIfNull(previousSaved);
        ArgumentNullException.ThrowIfNull(values);
        var secrets = new JsonObject();
        for (var i = 0; i < columns.Length; i++)
        {
            var index = Array.FindIndex(s_fields, f => string.Equals(f.Field, columns[i], StringComparison.Ordinal));
            var before = index >= 0 && index < previousSaved.Length && previousSaved[index] == true;
            secrets[columns[i]] = new JsonObject { ["before"] = before ? "set" : "not set", ["after"] = values[i].Length > 0 ? "set" : "not set" };
        }

        return new JsonObject { ["status"] = "updated", ["changed"] = new JsonArray(columns.Select(c => (JsonNode)c).ToArray()), ["secrets"] = secrets };
    }

    /// <summary>The write: seal, call the store function, answer presence. 400 for a refused body or value, 403 when the store refuses for
    /// privilege (the read roles of a managed or compose store hold no EXECUTE on the function).</summary>
    internal static async Task<(int Status, JsonNode Body)> SaveAsync(
        NpgsqlDataSource postgres, ILogger logger, JsonObject body, string editor, IPasswordKeyRing ring, CancellationToken ct,
        bool managedStore = false, bool adminWritesOff = false)
    {
        var (columns, values, error) = BuildWrite(body, ring);
        if (error is not null)
        {
            return (400, new JsonObject { ["error"] = error });
        }

        if (columns.Length == 0)
        {
            return (200, new JsonObject { ["status"] = "unchanged", ["message"] = "Nothing differs from the saved Slack bot tokens." });
        }

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(ct);
            await using var command = new NpgsqlCommand(
                "SELECT prev_slack_bot_token_set, prev_slack_app_token_set FROM config.set_notification_channels("
                + "$1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, $2, $3)", connection);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = columns });
            foreach (var field in s_fields)
            {
                var at = Array.IndexOf(columns, field.Field);
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = at >= 0 ? values[at] : DBNull.Value });
            }

            var previous = new bool?[s_fields.Length];
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                if (await reader.ReadAsync(ct))
                {
                    for (var i = 0; i < previous.Length; i++)
                    {
                        previous[i] = reader.IsDBNull(i) ? null : reader.GetBoolean(i);
                    }
                }
            }

            /* The fields' names only: never a value. */
            logger.LogInformation("Web Slack bot token write by {Editor}: fields changed: {Fields}", editor, string.Join(", ", columns));
            return (200, Answer(columns, previous, values));
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return (403, new JsonObject { ["error"] = DarlingWebStoreWrites.Refused("change the Slack bot tokens", managedStore, adminWritesOff: adminWritesOff) });
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedFunction)
        {
            return (400, new JsonObject { ["error"] = "The store's channel settings function predates the Slack bot tokens. Run the current provisioning script again, then save." });
        }
        catch (PostgresException ex) when (ex.SqlState.StartsWith("DL", StringComparison.Ordinal))
        {
            return (400, new JsonObject { ["error"] = ex.MessageText });
        }
    }
}
