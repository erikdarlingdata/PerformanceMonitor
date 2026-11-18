/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own database through ScratchPostgres and so cannot race the shared one. */
/// <summary>V275 (#8737): the personal MCP token table, its checks and the Viewer's schema probe.</summary>
public sealed class PersonalTokenRungTests
{
    private const string InsertSql = "INSERT INTO config.mcp_personal_token (token_id, secret_sha256, owner_key, subject, token_name, role, created_at, expires_at) VALUES (@id, @hash, 'owner', 'subject', 'name', @role, @created, @expires)";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the personal token rung live tests.");
        return connectionString!;
    }

    private static NpgsqlCommand InsertCommand(NpgsqlConnection connection, string id, byte[] hash, string role, DateTimeOffset created, DateTimeOffset expires)
    {
        var command = new NpgsqlCommand(InsertSql, connection);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("hash", hash);
        command.Parameters.AddWithValue("role", role);
        command.Parameters.AddWithValue("created", created);
        command.Parameters.AddWithValue("expires", expires);
        return command;
    }

    private static async Task AssertRefusedByCheckAsync(NpgsqlConnection connection, string id, byte[] hash, string role, DateTimeOffset created, DateTimeOffset expires, CancellationToken ct)
    {
        await using var command = InsertCommand(connection, id, hash, role, created, expires);
        var ex = await Assert.ThrowsAsync<PostgresException>(() => command.ExecuteNonQueryAsync(ct));
        Assert.Equal("23514", ex.SqlState);
    }

    [Fact]
    public async Task Rung_CreatesTableWithChecks()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct, emptyStore: true);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);

        var now = DateTimeOffset.UtcNow;
        const string GoodId = "0123456789abcdef";
        var goodHash = new byte[32];
        await AssertRefusedByCheckAsync(owner, GoodId, goodHash, "admin", now, now.AddDays(1), ct);
        await AssertRefusedByCheckAsync(owner, GoodId, new byte[31], "reader", now, now.AddDays(1), ct);
        await AssertRefusedByCheckAsync(owner, GoodId, goodHash, "reader", now, now, ct);
        await AssertRefusedByCheckAsync(owner, "NOT-HEX", goodHash, "reader", now, now.AddDays(1), ct);

        await using var good = InsertCommand(owner, GoodId, goodHash, "operator", now, now.AddDays(1));
        Assert.Equal(1, await good.ExecuteNonQueryAsync(ct));

        await using var index = new NpgsqlCommand("SELECT count(*) FROM pg_indexes WHERE schemaname = 'config' AND indexname = 'mcp_personal_token_owner_key'", owner);
        Assert.Equal(1L, await index.ExecuteScalarAsync(ct));
    }

    [Fact]
    public void RevokeBlock_EscapesTheRoleNamesInItsLiterals()
    {
        var sql = PersonalTokenSchema.ReassertPermissionsSql("config", "ad'min", "viewer", "web", "mcp");

        Assert.Contains("rolname IN ('ad''min', 'viewer', 'web', 'mcp')", sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Rung_IsIdempotent()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct, emptyStore: true);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);

        for (var run = 0; run < 2; run++)
        {
            await using var again = new NpgsqlCommand(PersonalTokenSchema.CreateSql, owner);
            await again.ExecuteNonQueryAsync(ct);
        }

        await using var triggers = new NpgsqlCommand("SELECT count(*) FROM pg_trigger WHERE tgrelid = 'config.mcp_personal_token'::regclass AND NOT tgisinternal", owner);
        Assert.Equal(2L, await triggers.ExecuteScalarAsync(ct));
    }

    [Fact]
    public async Task Probe_ReportsTheRung()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct, emptyStore: true);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);

        /* The store's own sentinel, not the ladder's top: a later rung's sentinel may be on too, so this maps the probed row with every
           sentinel after hasPersonalTokens forced off, and asserts the rung and, with its own sentinel off, the rung below it. */
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        var ordinal = Array.FindIndex(parameters, p => p.Name == "hasPersonalTokens");
        Assert.True(ordinal >= 0);

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, owner);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        var probed = new object[parameters.Length];
        var sentinels = new object[parameters.Length];
        for (var i = 0; i < sentinels.Length; i++)
        {
            probed[i] = reader.GetBoolean(i);
            sentinels[i] = i > ordinal ? false : probed[i];
        }

        /* The unmodified probed row of the fully migrated store maps to the build's own top, whatever rung that is. */
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, probed)!);

        Assert.True((bool)sentinels[ordinal]);
        Assert.Equal(PersonalTokenSchema.Version, (int)method.Invoke(null, sentinels)!);
        sentinels[ordinal] = false;
        Assert.Equal(PersonalTokenSchema.Version - 1, (int)method.Invoke(null, sentinels)!);
    }
}
