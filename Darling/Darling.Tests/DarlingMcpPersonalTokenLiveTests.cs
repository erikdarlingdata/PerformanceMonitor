/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// Personal MCP tokens (#8737) through the real MCP pipeline on a live store, with the snapshot's own loader on the owner connection: a store
/// one rung behind refuses personal tokens and keeps the file tokens working, and a personal operator's write is journaled under the person
/// and the token.
/// </summary>
public sealed class DarlingMcpPersonalTokenLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PersonalTokenSettings Personal() => new(30, 5, McpTokenEntry.OperatorRole, null);

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>E1: the store as V271 leaves it (the V272 table and its trigger function dropped). The host starts, a personal token gets 401,
    /// the file tokens work, and several ticks log the missing table once.</summary>
    [Fact]
    public async Task Personal_StoreOneRungBehind_401_FileTokensWork()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the personal token live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection, "DROP TABLE config.mcp_personal_token; DROP FUNCTION config.mcp_personal_token_owner_only()", ct);
        }

        await using var rig = await PersonalMcpRig.StartAsync(Personal(), storeConnection: scratch.ConnectionString);
        for (var tick = 0; tick < 4; tick++)
        {
            await rig.RefreshAsync();
        }

        var personal = PersonalTokenFormat.Mint();
        var unknown = await rig.PostAsync("not-a-served-token-value-0123456789", PersonalMcpRig.ToolsListJson);
        PersonalMcpRig.AssertSameBytes(unknown, await rig.PostAsync(personal.Value, PersonalMcpRig.ToolsListJson));
        Assert.Equal(StatusCodes.Status200OK, (await rig.PostAsync(PersonalMcpRig.Legacy, PersonalMcpRig.ToolsListJson)).Status);
        Assert.Equal(StatusCodes.Status200OK, (await rig.PostAsync(PersonalMcpRig.ReaderToken, PersonalMcpRig.ToolsListJson)).Status);

        Assert.Single(rig.Log.Lines, l => l.Contains($"has not reached V{PersonalTokenSchema.Version}", StringComparison.Ordinal));
    }

    /// <summary>A personal operator's write: the tool answers with the person and the token as its author, and the journal row's actor
    /// carries the same text.</summary>
    [Fact]
    public async Task Personal_Identity_InTokenIdentityFieldAndJournal()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the personal token live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using (var owner = NpgsqlDataSource.Create(scratch.ConnectionString))
        {
            var minted = PersonalTokenFormat.Mint();
            Assert.Equal(PersonalTokenCreateOutcome.Created, await PersonalTokenStore.CreateAsync(
                owner, minted.TokenId, minted.SecretSha256, "owner-alice", "alice@corp", "ci", McpTokenEntry.OperatorRole,
                DateTimeOffset.UtcNow, 30, 5, "person:alice@corp", ct));

            await using var rig = await PersonalMcpRig.StartAsync(Personal(), storeConnection: scratch.ConnectionString);
            rig.Now = DateTime.UtcNow;
            await rig.RefreshAsync();

            var response = await rig.PostAsync(minted.Value, PersonalMcpRig.ToolsCallJson(
                "write_investigation_note", "{\"note_text\":\"A note from a personal token.\"}"));
            Assert.Equal(StatusCodes.Status200OK, response.Status);
            var dataLine = response.Body.Split('\n').FirstOrDefault(l => l.StartsWith("data:", StringComparison.Ordinal));
            using var document = JsonDocument.Parse(dataLine is null ? response.Body : dataLine["data:".Length..].Trim());
            var text = document.RootElement.GetProperty("result").GetProperty("content")[0].GetProperty("text").GetString()!;
            using var answer = JsonDocument.Parse(text);
            Assert.Equal("written", answer.RootElement.GetProperty("status").GetString());
            Assert.Equal("person:mcp personal token ci of alice@corp", answer.RootElement.GetProperty("author").GetString());
        }

        Assert.Equal("1", await ScalarAsync(connection,
            "SELECT count(*) FROM config.investigation_journal WHERE event = 'note_written' AND actor = 'person:mcp personal token ci of alice@corp'", ct));
    }
}
