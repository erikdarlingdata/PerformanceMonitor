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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #1074 Gemini-schema guard shared by the slice-3 tool tests: walks an advertised tools/list input
/// schema and flags any union <c>type</c> array or <c>default</c> keyword the Gemini/Antigravity transform
/// must have stripped, plus reads the required-param set. Identical to the walker slices 1+2 inline per
/// test file, lifted here so the four new test files share one copy.
/// </summary>
internal static class DarlingMcpSchemaAssert
{
    public static List<string> Violations(string toolName, JsonElement schema)
    {
        var violations = new List<string>();
        Walk(schema, toolName);
        return violations;

        void Walk(JsonElement element, string path)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.Object:
                    foreach (var property in element.EnumerateObject())
                    {
                        if (property.NameEquals("type") && property.Value.ValueKind == JsonValueKind.Array)
                            violations.Add($"{path}: union type {property.Value.GetRawText()}");
                        if (property.NameEquals("default"))
                            violations.Add($"{path}: default = {property.Value.GetRawText()}");
                        Walk(property.Value, $"{path}.{property.Name}");
                    }
                    break;
                case JsonValueKind.Array:
                    var i = 0;
                    foreach (var item in element.EnumerateArray())
                        Walk(item, $"{path}[{i++}]");
                    break;
            }
        }
    }

    public static string[] RequiredOf(JsonElement schema) =>
        schema.TryGetProperty("required", out var req) && req.ValueKind == JsonValueKind.Array
            ? req.EnumerateArray().Select(e => e.GetString()!).ToArray()
            : Array.Empty<string>();
}

/// <summary>
/// Shared plant / assert utilities for the gated (DARLING_TEST_PG) slice-3 live round-trips — the sentinel
/// server upsert, a positional-parameter INSERT runner, naive-UTC helpers, and the envelope/status asserts,
/// modeled on the slice-2 <c>DarlingMcpDataToolsLivePostgresTests</c> helpers.
/// </summary>
internal static class DarlingMcpTestData
{
    public static async Task RegisterServerAsync(NpgsqlConnection connection, int serverId, string serverName, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, 15, $4, $4)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(Naive(DateTime.UtcNow));
        await command.ExecuteNonQueryAsync(ct);
    }

    public static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object?[] values)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var v in values)
            command.Parameters.AddWithValue(v ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    public static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    public static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    public static void AssertEnvelope(string json, string serverName, string expectedKey)
    {
        Assert.False(McpHelpers.IsErrorEnvelope(json), $"tool returned an error: {json}");
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(serverName, doc.RootElement.GetProperty("server").GetString());
        Assert.True(json.Contains(expectedKey, StringComparison.Ordinal), $"expected '{expectedKey}' in: {json}");
    }

    public static string StatusOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("status").GetString()!;
    }
}
