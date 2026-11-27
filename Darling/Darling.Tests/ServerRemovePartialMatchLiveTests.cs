/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the
   shared one. It seeds a fixed server id and reads no wall clock. */

/// <summary><c>remove_server</c> given a name that matches one definition only in part (#9665): it answers
/// <c>partial_match</c> with that server named and removes nothing, and the exact name still removes it.</summary>
public sealed class ServerRemovePartialMatchLiveTests
{
    [Fact]
    public async Task RemoveServer_APartialNameOfTheOnlyServer_AnswersPartialMatch_AndKeepsTheDefinition()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live remove_server test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        await using var source = NpgsqlDataSource.Create(connectionString);
        await ExecAsync(source, "INSERT INTO config_monitored_servers (server_id, name, host) VALUES (7301, 'partial-only', 'partial-only.example.test')", ct);

        /* One server is defined, so the text matches exactly one definition: the case a guess would have removed. */
        using (var doc = JsonDocument.Parse(await DarlingMcpServerAdminTools.RemoveServer(source, "partial-o")))
        {
            Assert.Equal("partial_match", doc.RootElement.GetProperty("status").GetString());
            Assert.Equal("partial", doc.RootElement.GetProperty("matched_by").GetString());
            var guess = Assert.Single(doc.RootElement.GetProperty("candidates").EnumerateArray());
            Assert.Equal("partial-only.example.test", guess.GetProperty("server").GetString());
            Assert.Equal("partial-only", guess.GetProperty("display_name").GetString());
            Assert.False(guess.GetProperty("ever_connected").GetBoolean());
        }

        Assert.Equal(1L, await CountAsync(source, "SELECT count(*) FROM config_monitored_servers WHERE server_id = 7301", ct));

        /* The exact display name the answer gave removes it. */
        using (var doc = JsonDocument.Parse(await DarlingMcpServerAdminTools.RemoveServer(source, "partial-only")))
        {
            Assert.Equal("removed", doc.RootElement.GetProperty("status").GetString());
            Assert.Equal("exact", doc.RootElement.GetProperty("matched_by").GetString());
        }

        Assert.Equal(0L, await CountAsync(source, "SELECT count(*) FROM config_monitored_servers WHERE server_id = 7301", ct));
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountAsync(NpgsqlDataSource source, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }
}
