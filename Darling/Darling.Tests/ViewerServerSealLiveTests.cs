/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The viewer seals a server password for its connection settings and saves it (#5366), against a real store: the
/// value in the raw row, read back through <see cref="ServerConnectionIdentity.FromStoredColumns"/>, opens with the key
/// and the stored settings, and a change of how the server is reached with the stored value kept is refused.
/// </summary>
public sealed class ViewerServerSealLiveTests
{
    private const string Password = "p@ss-not-real";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static MonitoredServerRow Row(bool trust) => new()
    {
        ServerId = ViewerDataService.ComputeServerId("alpha-live", null, false),
        Name = "Alpha live",
        Host = "alpha-live",
        Auth = "sql",
        Username = "monitor",
        EncryptMode = "Mandatory",
        TrustServerCertificate = trust,
    };

    private static async Task MigrateStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
    }

    private static async Task PublishAsync(ScratchPostgres scratch, PasswordPrivateKey key, string serviceState, string? note, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using (var keyCommand = new NpgsqlCommand(
            "INSERT INTO config.password_key (key_id, public_key, algorithm, state) VALUES ($1, $2, $3, 'current')", connection))
        {
            keyCommand.Parameters.Add(new NpgsqlParameter { Value = key.PublicKey.KeyId });
            keyCommand.Parameters.Add(new NpgsqlParameter { Value = key.PublicKey.Spki });
            keyCommand.Parameters.Add(new NpgsqlParameter { Value = PasswordSeal.Algorithm });
            await keyCommand.ExecuteNonQueryAsync(ct);
        }

        await using var stateCommand = new NpgsqlCommand(
            "INSERT INTO config.password_key_service (service_host, key_id, state, note, updated_at) "
            + "VALUES ('alpha-host', $1, $2, $3, now() AT TIME ZONE 'UTC')", connection);
        stateCommand.Parameters.Add(new NpgsqlParameter { Value = key.PublicKey.KeyId });
        stateCommand.Parameters.Add(new NpgsqlParameter { Value = serviceState });
        stateCommand.Parameters.Add(new NpgsqlParameter { Value = (object?)note ?? DBNull.Value });
        await stateCommand.ExecuteNonQueryAsync(ct);
    }

    private static async Task<(ServerConnectionIdentity Identity, string Stored)> ReadRawRowAsync(
        ScratchPostgres scratch, int serverId, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(
            "SELECT " + ServerConnectionIdentity.StoredColumns + ", encrypted_password FROM config.config_monitored_servers WHERE server_id = $1",
            connection);
        command.Parameters.Add(new NpgsqlParameter { Value = serverId });
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        var identity = ServerConnectionIdentity.FromStoredColumns(
            reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt32(1),
            reader.IsDBNull(2) ? null : reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3),
            !reader.IsDBNull(4) && reader.GetBoolean(4), reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6), reader.IsDBNull(7) ? null : reader.GetString(7),
            !reader.IsDBNull(8) && reader.GetBoolean(8), !reader.IsDBNull(9) && reader.GetBoolean(9));
        return (identity, reader.GetString(10));
    }

    [Fact]
    public async Task AddAndEdit_SealForTheStoredSettings_AndAMovedReachKeepsNoStoredValue()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live seal round trip.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var directory = Directory.CreateTempSubdirectory("darling-seal-live-").FullName;
        using var key = PasswordPrivateKey.Generate();
        var bodySucceeded = false;
        try
        {
            await MigrateStoreAsync(scratch, ct);
            await PublishAsync(scratch, key, "ok", null, ct);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            var (published, state) = await viewer.GetPublishedPasswordKeyAsync(ct);
            var decision = ViewerPasswordKey.Evaluate(published, state, viewer.StoreConnectionIdentity, new ViewerPasswordKeyPins(Path.Combine(directory, "pins.json")));
            Assert.NotNull(decision.Key);
            var sealer = new ViewerPasswordSealer(decision.Key!);

            /* Add: the value in the raw row opens with the key and the settings the row stores. */
            var row = Row(trust: false);
            row.EncryptedPassword = sealer.Seal(Password, row);
            Assert.Equal(MonitoredServerAddOutcome.Added, (await viewer.AddMonitoredServerAsync(row, ct)).Outcome);
            var (identity, stored) = await ReadRawRowAsync(scratch, row.ServerId, ct);
            Assert.Equal(Password, PasswordSeal.Open(stored, key, PasswordBinding.ForServer(identity)));

            /* Edit that changes only the trust setting and keeps the stored value: refused, nothing written. */
            var kept = Row(trust: true);
            kept.EncryptedPassword = stored;
            await Assert.ThrowsAsync<MonitoredServerPasswordNeededException>(() => viewer.UpsertMonitoredServerAsync(kept, ct));
            Assert.Equal(stored, (await ReadRawRowAsync(scratch, row.ServerId, ct)).Stored);

            /* The same edit with the password typed again seals for the new settings. */
            var moved = Row(trust: true);
            moved.EncryptedPassword = sealer.Seal(Password, moved);
            await viewer.UpsertMonitoredServerAsync(moved, ct);
            var (movedIdentity, movedStored) = await ReadRawRowAsync(scratch, row.ServerId, ct);
            Assert.True(movedIdentity.TrustServerCertificate);
            Assert.Equal(Password, PasswordSeal.Open(movedStored, key, PasswordBinding.ForServer(movedIdentity)));
            Assert.Throws<PasswordSealException>(() => PasswordSeal.Open(movedStored, key, PasswordBinding.ForServer(identity)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
            try { Directory.Delete(directory, recursive: true); } catch { /* best-effort */ }
        }
    }

    [Fact]
    public async Task AServiceStateThatIsNotOk_RefusesWithItsNote_OnARealStore()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live seal round trip.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var directory = Directory.CreateTempSubdirectory("darling-seal-live-").FullName;
        using var key = PasswordPrivateKey.Generate();
        var bodySucceeded = false;
        try
        {
            await MigrateStoreAsync(scratch, ct);
            await PublishAsync(scratch, key, "missing", "The key file was not found.", ct);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            var (published, state) = await viewer.GetPublishedPasswordKeyAsync(ct);
            var decision = ViewerPasswordKey.Evaluate(published, state, viewer.StoreConnectionIdentity, new ViewerPasswordKeyPins(Path.Combine(directory, "pins.json")));

            Assert.Null(decision.Key);
            Assert.Contains("The key file was not found.", decision.Refusal, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
            try { Directory.Delete(directory, recursive: true); } catch { /* best-effort */ }
        }
    }
}
