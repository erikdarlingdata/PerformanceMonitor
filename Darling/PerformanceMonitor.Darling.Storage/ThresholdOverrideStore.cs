/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One row of the threshold-overrides grid (#5916 part 4): the override, its server's name, and the global value the
/// override replaces (null when the store has no global row or no such column yet).</summary>
public sealed record ThresholdOverrideRow(
    int ServerId, string ServerName, string? Database, string Key, double Value, double? GlobalValue,
    string SetBy, Guid? RequestId, DateTime UpdatedAt, int? TagId = null, bool Orphaned = false)
{
    /// <summary>True for a row held for a tag (#6926): <see cref="TagId"/> is set, <see cref="ServerId"/> is 0, <see cref="ServerName"/>
    /// holds the tag's name (empty when the tag was deleted) and <see cref="Orphaned"/> says the tag no longer exists, so the row matches nothing.</summary>
    public bool IsTag => TagId.HasValue;
}

/// <summary>
/// The statements behind <c>config.config_threshold_overrides</c> (#5916), shared by the <c>set_alert_threshold_override</c> tool,
/// the web admin route and the Viewer's grid so the three cannot write the table three ways. Each takes the caller's connection
/// and transaction (the tool journals its write in the same transaction) or a data source (the list). Key and bound checks are
/// not here: they are <c>DarlingThresholdKeys</c>' (<c>PerformanceMonitor.Alerting</c>), applied by every caller before any write.
/// </summary>
public static partial class ThresholdOverrideStore
{
    /// <summary>The table, schema-qualified; the mcp role holds SELECT, INSERT, UPDATE and DELETE on it (#5920).</summary>
    public const string Table = "config.config_threshold_overrides";

    private const int CommandTimeoutSeconds = 30;

    /// <summary>A person's login shaped to what the store's <c>set_by</c> accepts, 1-64 of <c>A-Za-z0-9._:-</c>: anything else becomes
    /// <c>_</c>, an empty name becomes <paramref name="fallback"/>. The web seat's editor name and the Viewer's Windows login both pass here.</summary>
    public static string SetByFor(string? name, string fallback = "web")
    {
        var chars = new char[Math.Min(name?.Length ?? 0, 64)];
        for (var i = 0; i < chars.Length; i++)
        {
            var c = name![i];
            chars[i] = char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or ':' or '-' ? c : '_';
        }

        return chars.Length == 0 ? fallback : new string(chars);
    }

    private static NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, int serverId, string? database, string wire)
    {
        var command = new NpgsqlCommand(sql, connection) { Transaction = transaction, CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)database ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = wire });
        return command;
    }

    public static async Task<double?> ReadValueAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, string? database, string wire,
        CancellationToken ct)
    {
        await using var command = Command(connection, transaction,
            "SELECT value FROM " + Table + " WHERE server_id = $1 AND coalesce(database_name, '') = coalesce($2::text, '') AND threshold_key = $3",
            serverId, database, wire);
        return await command.ExecuteScalarAsync(ct) is double d ? d : null;
    }

    public static async Task DeleteAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, string? database, string wire, CancellationToken ct)
    {
        await using var command = Command(connection, transaction,
            "DELETE FROM " + Table + " WHERE server_id = $1 AND coalesce(database_name, '') = coalesce($2::text, '') AND threshold_key = $3",
            serverId, database, wire);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task UpsertAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, string? database, string wire, int value,
        string setBy, Guid? requestId, CancellationToken ct)
    {
        await using var command = Command(connection, transaction,
            "INSERT INTO " + Table + " (server_id, database_name, threshold_key, value, set_by, request_id) " +
            "VALUES ($1, $2, $3, $4, $5, $6) " +
            "ON CONFLICT (server_id, (coalesce(database_name, '')), threshold_key) DO UPDATE " +
            "SET value = EXCLUDED.value, set_by = EXCLUDED.set_by, request_id = EXCLUDED.request_id, updated_at = now()",
            serverId, database, wire);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Double, Value = (double)value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = setBy });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Uuid, Value = (object?)requestId ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The list statement: every override with its server's name, ordered by server, database then key. The name resolves through the
    /// registry the override's foreign key points at, so a row always has one.</summary>
    public const string ListSql =
        "SELECT o.server_id, s.name, o.database_name, o.threshold_key, o.value, o.set_by, o.request_id, o.updated_at " +
        "FROM " + Table + " AS o JOIN config.config_monitored_servers AS s ON s.server_id = o.server_id " +
        "ORDER BY s.name, o.database_name NULLS FIRST, o.threshold_key";

    /// <summary>The global row as JSON, so each key's own column is read by name and a column an older store lacks reads as absent
    /// rather than failing the whole list.</summary>
    public const string GlobalRowSql = "SELECT to_jsonb(a) FROM config.config_alert_settings AS a WHERE id = 1";

    /// <summary>
    /// Every override with the global value it replaces. <paramref name="columnFor"/> maps a wire name to the global setting's
    /// column on <c>config.config_alert_settings</c> (null for a key this build does not know, which keeps its row and shows no
    /// global value). A role without SELECT on the table raises 42501, which the caller reports.
    /// </summary>
    public static async Task<IReadOnlyList<ThresholdOverrideRow>> ListAsync(
        NpgsqlDataSource postgres, Func<string, string?> columnFor, CancellationToken ct)
    {
        JsonElement? global = null;
        try
        {
            await using var globalCommand = postgres.CreateCommand(GlobalRowSql);
            globalCommand.CommandTimeout = CommandTimeoutSeconds;
            if (await globalCommand.ExecuteScalarAsync(ct) is string json)
                global = JsonDocument.Parse(json).RootElement.Clone();
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            global = null;
        }

        var rows = new List<ThresholdOverrideRow>();
        await using var command = postgres.CreateCommand(ListSql);
        command.CommandTimeout = CommandTimeoutSeconds;
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var key = reader.GetString(3);
            double? globalValue = null;
            if (global is { } g && columnFor(key) is { } column
                && g.TryGetProperty(column, out var element) && element.ValueKind == JsonValueKind.Number)
            {
                globalValue = element.GetDouble();
            }

            rows.Add(new ThresholdOverrideRow(
                reader.GetInt32(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2), key, reader.GetDouble(4), globalValue,
                reader.GetString(5), reader.IsDBNull(6) ? null : reader.GetGuid(6), reader.GetDateTime(7)));
        }

        await reader.DisposeAsync();

        await using var tagCommand = postgres.CreateCommand(TagListSql);
        tagCommand.CommandTimeout = CommandTimeoutSeconds;
        await using var tagReader = await tagCommand.ExecuteReaderAsync(ct);
        while (await tagReader.ReadAsync(ct))
        {
            var key = tagReader.GetString(2);
            double? globalValue = null;
            if (global is { } g && columnFor(key) is { } column
                && g.TryGetProperty(column, out var element) && element.ValueKind == JsonValueKind.Number)
            {
                globalValue = element.GetDouble();
            }

            var name = tagReader.IsDBNull(1) ? null : tagReader.GetString(1);
            rows.Add(new ThresholdOverrideRow(
                0, name ?? string.Empty, null, key, tagReader.GetDouble(3), globalValue,
                tagReader.GetString(4), tagReader.IsDBNull(5) ? null : tagReader.GetGuid(5), tagReader.GetDateTime(6),
                tagReader.GetInt32(0), name is null));
        }

        return rows;
    }
}
