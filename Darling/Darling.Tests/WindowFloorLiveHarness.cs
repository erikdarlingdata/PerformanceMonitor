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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The shared scaffolding of the #4966 window-floor live cases for the collection log, plan corrections and memory pressure
/// events: the current-minute anchor, a server registered at a chosen instant with its collector runs logged, and the run
/// wrapper that sets the fleet schedule rows aside (the coverage probe reads the purge edge from them) and puts them back.
/// </summary>
internal static class WindowFloorLiveHarness
{
    /// <summary>The current minute, taken once per test: the coverage probe measures its purge edge from the store's own clock.</summary>
    public static DateTime AnchorNow()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
    }

    public static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    /// <summary>A server registered at <paramref name="created"/>, and its runs of <paramref name="collector"/> logged every
    /// <paramref name="stepMinutes"/> minutes from <paramref name="runsFrom"/> to <paramref name="runsTo"/> (none when null).</summary>
    public static async Task SeedServerAsync(
        NpgsqlConnection connection, string name, DateTime created, string collector, DateTime? runsFrom, int stepMinutes, DateTime runsTo,
        string[] tables, CancellationToken ct)
    {
        var serverId = ServerIdHelper.GetDeterministicHashCode(name);
        await DeleteServerAsync(connection, name, tables, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, name, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET created_date = $2 WHERE server_id = $1", serverId, DarlingMcpTestData.Naive(created));

        if (runsFrom is DateTime from)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT row_number() OVER (), $1, $2, $3, t, 12, 'SUCCESS', 0
FROM generate_series($4::timestamp, $5::timestamp, make_interval(mins => $6)) AS t",
                serverId, name, collector, DarlingMcpTestData.Naive(from), DarlingMcpTestData.Naive(runsTo), stepMinutes);
        }
    }

    public static async Task DeleteServerAsync(NpgsqlConnection connection, string name, string[] tables, CancellationToken ct)
    {
        var serverId = ServerIdHelper.GetDeterministicHashCode(name);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_collector_schedules WHERE server_id = $1", serverId);
        foreach (var table in tables)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", serverId);
        }

        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", serverId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", serverId);
    }

    /// <summary>Runs <paramref name="body"/> with the fleet-wide schedule rows of <paramref name="collectors"/> set aside, then restores
    /// them, clears the probe seam and deletes the servers in <paramref name="serverNames"/> (and their <paramref name="tables"/> rows).</summary>
    public static async Task RunAsync(
        string? cs, string[] collectors, string[] serverNames, string[] tables, Func<NpgsqlConnection, NpgsqlDataSource, DateTime, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live window-floor test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var saved = new List<(string Name, int? Frequency, int? Retention, bool Enabled, string[]? Databases)>();
        if (collectors.Length > 0)
        {
            await using var save = new NpgsqlCommand(
                "SELECT collector_name, frequency_minutes, retention_days, enabled, databases FROM config_collector_schedules WHERE server_id IS NULL AND lower(collector_name) = ANY($1)", connection);
            save.Parameters.AddWithValue(collectors);
            await using (var reader = await save.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    saved.Add((reader.GetString(0),
                        reader.IsDBNull(1) ? null : reader.GetInt32(1),
                        reader.IsDBNull(2) ? null : reader.GetInt32(2),
                        reader.GetBoolean(3),
                        reader.IsDBNull(4) ? null : (string[])reader.GetValue(4)));
                }
            }

            await using var clear = new NpgsqlCommand("DELETE FROM config_collector_schedules WHERE server_id IS NULL AND lower(collector_name) = ANY($1)", connection);
            clear.Parameters.AddWithValue(collectors);
            await clear.ExecuteNonQueryAsync(ct);
        }

        var bodySucceeded = false;
        try
        {
            await body(connection, postgres, AnchorNow());
            bodySucceeded = true;
        }
        finally
        {
            foreach (var row in saved)
            {
                await DarlingMcpTestData.ExecAsync(connection, CancellationToken.None,
                    "INSERT INTO config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled, databases) VALUES (NULL, $1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
                    row.Name, (object?)row.Frequency ?? DBNull.Value, (object?)row.Retention ?? DBNull.Value, row.Enabled, (object?)row.Databases ?? DBNull.Value);
            }

            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () =>
            {
                PerformanceMonitor.Darling.Service.Mcp.DarlingMcpWindowNotice.TestOnlyProbe = null;
                return Task.CompletedTask;
            });
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                foreach (var name in serverNames)
                {
                    await DeleteServerAsync(cleanup, name, tables, cleanupCt);
                }
            });
        }
    }
}
