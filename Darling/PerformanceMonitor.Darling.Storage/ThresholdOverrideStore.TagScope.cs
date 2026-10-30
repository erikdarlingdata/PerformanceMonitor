/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>The tag-scoped statements of <see cref="ThresholdOverrideStore"/> (#6926): a threshold held for a server tag, the deepest tag
/// a server is under winning as a notification route's does (<see cref="TagThresholdResolver"/>).</summary>
public static partial class ThresholdOverrideStore
{
    private static NpgsqlCommand TagCommand(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, int tagId, string wire)
    {
        var command = new NpgsqlCommand(sql, connection) { Transaction = transaction, CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = tagId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = wire });
        return command;
    }

    /// <summary>The tag's stored value for a key, or null.</summary>
    public static async Task<double?> ReadTagValueAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int tagId, string wire, CancellationToken ct)
    {
        await using var command = TagCommand(connection, transaction,
            "SELECT value FROM " + Table + " WHERE scope_tag_id = $1 AND threshold_key = $2", tagId, wire);
        return await command.ExecuteScalarAsync(ct) is double d ? d : null;
    }

    public static async Task DeleteTagAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int tagId, string wire, CancellationToken ct)
    {
        await using var command = TagCommand(connection, transaction,
            "DELETE FROM " + Table + " WHERE scope_tag_id = $1 AND threshold_key = $2", tagId, wire);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task UpsertTagAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int tagId, string wire, int value,
        string setBy, Guid? requestId, CancellationToken ct)
    {
        await using var command = TagCommand(connection, transaction,
            "INSERT INTO " + Table + " (scope_tag_id, threshold_key, value, set_by, request_id) VALUES ($1, $2, $3, $4, $5) " +
            "ON CONFLICT (scope_tag_id, threshold_key) WHERE scope_tag_id IS NOT NULL DO UPDATE " +
            "SET value = EXCLUDED.value, set_by = EXCLUDED.set_by, request_id = EXCLUDED.request_id, updated_at = now()",
            tagId, wire);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Double, Value = (double)value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = setBy });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Uuid, Value = (object?)requestId ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Finds a tag from the text a person or an agent gave: a number is a tag id, anything else is a name matched without regard
    /// to case. A name two tags share, or one that matches nothing, comes back as a refusal sentence.</summary>
    public static async Task<(int? TagId, string? Name, string? Problem)> ResolveTagAsync(NpgsqlDataSource postgres, string text, CancellationToken ct)
    {
        var wanted = text.Trim();
        if (wanted.Length == 0)
            return (null, null, "A tag name or id is required.");
        var matches = new List<(int Id, string Name)>();
        await using var command = postgres.CreateCommand(
            "SELECT id, name FROM config.server_tags WHERE id::text = $1 OR lower(name) = lower($1) ORDER BY id");
        command.CommandTimeout = CommandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = wanted });
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
                matches.Add((reader.GetInt32(0), reader.GetString(1)));
        }

        var byId = matches.Where(m => m.Id.ToString(CultureInfo.InvariantCulture) == wanted).ToList();
        if (byId.Count == 1)
            return (byId[0].Id, byId[0].Name, null);
        if (matches.Count == 1)
            return (matches[0].Id, matches[0].Name, null);
        return matches.Count == 0
            ? (null, null, $"No server tag is named or numbered '{wanted}'.")
            : (null, null, $"More than one server tag is named '{wanted}' (ids {string.Join(", ", matches.Select(m => m.Id))}): use the tag id.");
    }

    /// <summary>The tag list statement: every tag override with its tag's name. A tag that was deleted has no name and is orphaned.</summary>
    public const string TagListSql =
        "SELECT o.scope_tag_id, t.name, o.threshold_key, o.value, o.set_by, o.request_id, o.updated_at " +
        "FROM " + Table + " AS o LEFT JOIN config.server_tags AS t ON t.id = o.scope_tag_id " +
        "WHERE o.scope_tag_id IS NOT NULL ORDER BY t.name NULLS LAST, o.scope_tag_id, o.threshold_key";
}
