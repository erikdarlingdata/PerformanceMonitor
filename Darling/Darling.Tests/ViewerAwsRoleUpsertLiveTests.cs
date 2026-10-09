/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres and never touches another one. */

/// <summary>
/// The desktop viewer's upsert and the external ID stored with a server's AWS role (#5452), run as the store's owner
/// against a migrated store: a save that changes the role with nothing typed leaves no external ID, a save that keeps
/// the role keeps it, and a typed or cleared box replaces it.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerAwsRoleUpsertLiveTests
{
    private const int ServerId = 7601;
    private const string Role = "arn:aws:iam::123456789012:role/darling-monitor";
    private const string OtherRole = "arn:aws:iam::123456789012:role/darling-other";
    private const string StoredExternal = "tenant-1234";
    private const string TypedExternal = "tenant-5678";

    private sealed record Rig(ScratchPostgres Scratch, NpgsqlDataSource Owner) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Owner.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private static async Task<Rig> OpenAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live viewer AWS role upsert tests (each mints its own scratch database).");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        var owner = NpgsqlDataSource.Create(ownerString);
        await using (var command = owner.CreateCommand(@"INSERT INTO config_monitored_servers
            (server_id, name, host, auth, username, encrypt_mode, excluded_databases, is_enabled, monthly_cost_usd, aws_role_arn, aws_external_id)
            VALUES ($1, 'aws-upsert', 'aws-upsert.example.test', 'sql', 'app-user', 'Mandatory', ARRAY[]::text[], TRUE, 0, $2, $3)"))
        {
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(Role);
            command.Parameters.AddWithValue(StoredExternal);
            await command.ExecuteNonQueryAsync(ct);
        }

        return new Rig(scratch, owner);
    }

    private static MonitoredServerRow Row(string? role, string? externalId, bool externalIdSent) => new()
    {
        ServerId = ServerId,
        Name = "aws-upsert",
        Host = "aws-upsert.example.test",
        Auth = "sql",
        Username = "app-user",
        EncryptMode = "Mandatory",
        AwsRoleArn = role,
        AwsExternalId = externalId,
        AwsExternalIdSent = externalIdSent,
    };

    private static async Task<string> StoredAsync(Rig rig, CancellationToken ct)
    {
        await using var command = rig.Owner.CreateCommand(
            "SELECT COALESCE(aws_role_arn, '<null>') || ' | ' || COALESCE(aws_external_id, '<null>') FROM config_monitored_servers WHERE server_id = $1");
        command.Parameters.AddWithValue(ServerId);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    [Fact]
    public async Task ARoleChange_WithNothingTyped_LeavesNoExternalId_AndASameRoleSaveKeepsIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);

        await viewer.UpsertMonitoredServerAsync(Row(Role, null, externalIdSent: false), ct);
        Assert.Equal(Role + " | " + StoredExternal, await StoredAsync(rig, ct));

        await viewer.UpsertMonitoredServerAsync(Row(OtherRole, null, externalIdSent: false), ct);
        Assert.Equal(OtherRole + " | <null>", await StoredAsync(rig, ct));
    }

    [Fact]
    public async Task ARoleChange_WithAnExternalIdTyped_StoresIt_AndAClearedBoxRemovesIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);

        await viewer.UpsertMonitoredServerAsync(Row(OtherRole, TypedExternal, externalIdSent: true), ct);
        Assert.Equal(OtherRole + " | " + TypedExternal, await StoredAsync(rig, ct));

        await viewer.UpsertMonitoredServerAsync(Row(OtherRole, null, externalIdSent: true), ct);
        Assert.Equal(OtherRole + " | <null>", await StoredAsync(rig, ct));
    }
}
