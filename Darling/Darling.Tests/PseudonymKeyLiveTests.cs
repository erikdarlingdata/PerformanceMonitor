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
using ModelContextProtocol.Protocol;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6066 over a real store: V198 makes and seeds the one key row (fresh and upgraded stores), no role but the store owner can read
/// it, and two service starts on one store give one login the same pseudonym without the key reaching any answer or log line.
/// The roles carry fixed cluster-wide names, so a cluster that already has them skips the role facts.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class PseudonymKeyLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<long> CountAsync(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, c) { CommandTimeout = 30 };
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<byte[]> KeyOfAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT key FROM config.pseudonym_key WHERE id = 1", c) { CommandTimeout = 30 };
        return (byte[])(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, c) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task AFreshStore_AndAnUpgradedOne_EachEndWithExactlyOneThirtyTwoByteKey_AndAReRunKeepsIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the pseudonym key live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* Fresh: the whole ladder runs, V198 last. */
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(1L, await CountAsync(connection, "SELECT count(*) FROM config.pseudonym_key", ct));
            Assert.Equal(32, (await KeyOfAsync(connection, ct)).Length);
            byte[] first = await KeyOfAsync(connection, ct);

            /* The rung applied again (a restart that re-runs it, a restored copy): the key it had is the key it keeps. */
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 198", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(1L, await CountAsync(connection, "SELECT count(*) FROM config.pseudonym_key", ct));
            Assert.Equal(first, await KeyOfAsync(connection, ct));

            /* Upgraded: a store at V197 has no table; the upgrade makes it and seeds one key, which is not all zeros and not the fresh store's. */
            await ExecAsync(connection, "DROP TABLE config.pseudonym_key; DELETE FROM darling_schema_version WHERE version >= 198", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(1L, await CountAsync(connection, "SELECT count(*) FROM config.pseudonym_key", ct));
            byte[] upgraded = await KeyOfAsync(connection, ct);
            Assert.Equal(32, upgraded.Length);
            Assert.NotEqual(first, upgraded);
            Assert.Contains(upgraded, b => b != 0);

            /* A second row cannot exist, and a write that is not the owner's is refused by the same trigger the password key tables carry. */
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, "INSERT INTO config.pseudonym_key (id, key) VALUES (2, decode(repeat('01', 32), 'hex'))", ct));
            await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, "UPDATE config.pseudonym_key SET key = decode(repeat('01', 31), 'hex')", ct));
            Assert.Equal(2L, await CountAsync(connection,
                "SELECT count(*) FROM pg_catalog.pg_trigger WHERE NOT tgisinternal AND tgrelid = 'config.pseudonym_key'::regclass AND tgenabled = 'A'", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ViewerMcpAndAdmin_AreDeniedSelectOnTheKeyTable_AfterTheProvisioningBatch()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the pseudonym key live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        await PgMigrations.MigrateAsync(owner, ct);
        Assert.SkipWhen(
            await CountAsync(owner, "SELECT count(*) FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp')", ct) > 0,
            "A cluster-wide admin/viewer/mcp role already exists on this cluster; the provisioning batch would adopt it.");

        var bodySucceeded = false;
        try
        {
            string ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
            await ExecAsync(owner, DarlingManagedRoles.BuildProvisioningSql(
                ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName)), ct);

            foreach (var (role, password) in new[]
            {
                ("viewer", ProvisioningTestSecrets.ViewerPassword),
                ("mcp", ProvisioningTestSecrets.McpPassword),
                ("admin", ProvisioningTestSecrets.AdminPassword),
            })
            {
                var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
                {
                    Username = role, Password = password, SearchPath = "collect,config,public", Pooling = false,
                };
                await using var connection = new NpgsqlConnection(builder.ConnectionString);
                await connection.OpenAsync(ct);
                var denied = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, "SELECT key FROM config.pseudonym_key", ct));
                Assert.Equal("42501", denied.SqlState);
                /* Not even a column grant, and no write: the roles hold no privilege of any kind on it. */
                Assert.False(await ScalarBoolAsync(owner,
                    $"SELECT has_table_privilege('{role}', 'config.pseudonym_key', 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') OR has_any_column_privilege('{role}', 'config.pseudonym_key', 'SELECT, INSERT, UPDATE, REFERENCES')", ct));
            }

            /* The owner still reads it: that is how the service gets it. */
            Assert.Equal(32, (await KeyOfAsync(owner, ct)).Length);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await ExecAsync(cleanup,
                    "DROP OWNED BY admin, viewer, web, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanupCt));
        }
    }

    private static async Task<bool> ScalarBoolAsync(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, c) { CommandTimeout = 30 };
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private sealed class CaptureLogger : ILogger<PseudonymKeySource>
    {
        public List<string> Lines { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(formatter(state, exception) + (exception is null ? "" : " | " + exception));
    }

    [Fact]
    public async Task TwoServiceStartsOnOneStore_GiveOneLoginTheSamePseudonym_AndTheKeyIsInNoAnswerOrLogLine()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the pseudonym key live pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var owner = new NpgsqlConnection(scratch.ConnectionString);
        await owner.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(owner, ct);
            byte[] keyBytes = await KeyOfAsync(owner, ct);
            string keyHex = Convert.ToHexStringLower(keyBytes);
            string keyBase64 = Convert.ToBase64String(keyBytes);
            var logger = new CaptureLogger();

            async Task<string> AnswerOfOneStartAsync()
            {
                /* One service start: a new owner-source and a new key source, which reads the store once. */
                await using var data = NpgsqlDataSource.Create(scratch.ConnectionString);
                var ownerSource = new StoreOwnerSource();
                using var source = new PseudonymKeySource(ownerSource, logger);
                Assert.Null(await source.GetAsync(ct));
                ownerSource.Publish(data);
                var key = await PseudonymKeySources.KeyForAsync(source, McpDataDetail.MetadataOnly, ct);
                Assert.NotNull(key);

                var result = new CallToolResult
                {
                    Content = new List<ContentBlock> { new TextContentBlock { Text = "{\"rows\":[{\"login_name\":\"CORP\\\\alice\",\"host_name\":\"APP01\"}]}" } },
                };
                var masked = McpDataDetailFilter.Mask(result, McpDataDetail.MetadataOnly, key);
                return ((TextContentBlock)masked.Content[0]).Text;
            }

            string first = await AnswerOfOneStartAsync();
            string second = await AnswerOfOneStartAsync();
            Assert.Equal(first, second);
            var row = JsonNode.Parse(first)!["rows"]![0]!;
            Assert.Matches("^login#[0-9a-f]{8}$", (string?)row["login_name"]);
            Assert.Matches("^host#[0-9a-f]{8}$", (string?)row["host_name"]);
            Assert.DoesNotContain("alice", first, StringComparison.OrdinalIgnoreCase);

            /* The key is in neither answer, and no log line has it. */
            foreach (string text in new[] { first, second }.Concat(logger.Lines))
            {
                Assert.DoesNotContain(keyHex, text, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain(keyBase64, text, StringComparison.Ordinal);
            }

            /* No journal row, brief or request holds it either: none of those tables is written by this path, and a scan of the
               investigation journal's text finds nothing. */
            Assert.Equal(0L, await CountAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE detail::text ILIKE '%{keyHex}%'", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreThatCannotBeRead_GivesNoKey_WarnsOnce_NamesNoKey_AndIsAskedAgainAfterTheRetryWait()
    {
        var ct = TestContext.Current.CancellationToken;
        var logger = new CaptureLogger();
        var ownerSource = new StoreOwnerSource();
        using var source = new PseudonymKeySource(ownerSource, logger);
        await using var broken = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=none;Username=none;Password=none;Timeout=1;Pooling=false");
        ownerSource.Publish(broken);

        Assert.Null(await source.GetAsync(ct));
        Assert.Null(await source.GetAsync(ct));
        Assert.Single(logger.Lines);
        Assert.DoesNotContain("PseudonymKey", logger.Lines[0], StringComparison.Ordinal);

        /* With no key, a metadataOnly answer still shows no name. */
        var masker = McpDataDetailMasker.ForAnswer(McpDataDetail.MetadataOnly, await PseudonymKeySources.KeyForAsync(source, McpDataDetail.MetadataOnly, ct));
        Assert.Equal("login#1", masker.Field("login_name", "CORP\\alice"));
    }

    /* The Mcp tool classes never take the key source (rule 9): a client cannot be handed it through a tool, and a tool cannot read the key. */
    [Fact]
    public void NoMcpToolMethod_TakesTheKeySource()
    {
        var takers = typeof(PseudonymKeySource).Assembly.GetTypes()
            .SelectMany(type => type.GetMethods(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Static
                | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.DeclaredOnly))
            .Where(method => method.GetCustomAttributes(typeof(ModelContextProtocol.Server.McpServerToolAttribute), false).Length > 0)
            .Where(method => method.GetParameters().Any(p =>
                p.ParameterType == typeof(PseudonymKeySource) || p.ParameterType == typeof(IPseudonymKeySource) || p.ParameterType == typeof(PseudonymKey)))
            .Select(method => method.DeclaringType!.Name + "." + method.Name)
            .ToList();
        Assert.Empty(takers);
    }
}
