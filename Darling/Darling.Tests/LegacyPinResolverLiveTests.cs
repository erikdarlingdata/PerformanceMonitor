/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another one. */

/// <summary>
/// The service reads the pins taken at upgrade from <c>config.legacy_secret_pin</c> with the config, and opens a saved
/// password against the row it reads (#5366): an old-format value opens while the row still matches its pin, and a row that
/// the store owner changed directly (a new host, or another value) is refused. A sealed value opens only for the connection
/// in the row it is read from.
/// </summary>
public sealed class LegacyPinResolverLiveTests
{
    private const string Plain = "p@ss-not-real";
    private const int ServerId = 5366001;

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct, params (string Name, NpgsqlDbType Type, object Value)[] parameters)
    {
        await using var command = source.CreateCommand(sql);
        foreach (var (name, type, value) in parameters)
        {
            command.Parameters.Add(new NpgsqlParameter(name, type) { Value = value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<MonitoredServer> ReadAlphaAsync(NpgsqlDataSource owner, CancellationToken ct)
    {
        await using var connection = await owner.OpenConnectionAsync(ct);
        var servers = await StoreConfigProvider.ReadMonitoredServersAsync(connection, new DarlingConfig(), ct);
        return servers.Single(s => s.Name == "alpha");
    }

    private static Task InsertServerAsync(NpgsqlDataSource owner, string stored, CancellationToken ct) => ExecAsync(
        owner,
        "INSERT INTO config_monitored_servers (server_id, name, host, auth, username, encrypted_password) " +
        $"VALUES ({ServerId}, 'alpha', 'alpha-host.example.test', 'sql', 'monitor', @stored)",
        ct,
        ("stored", NpgsqlDbType.Text, stored));

    [Fact]
    public async Task AnOldFormatPasswordOpensWhileItsRowMatchesItsPinAndIsRefusedOnceTheOwnerChangesTheHost()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Old-format values are Windows DPAPI values.");
        var ct = TestContext.Current.CancellationToken;
        var stored = DarlingSecrets.Protect(Plain);
        var (scratchStore, owner, _) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var scratchHolder = scratchStore;
        await using var ownerHolder = owner;
        await InsertServerAsync(owner, stored, ct);
        var ring = DarlingPasswordKey.Refusing(DarlingPasswordKey.NotReadyReason);

        /* No pin: refused, and nothing reads the value. */
        var unpinned = await ReadAlphaAsync(owner, ct);
        var refused = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(unpinned, out _, ring));
        Assert.Contains("is in the old format", refused.Message, StringComparison.Ordinal);

        /* The pin the upgrade writes: the value's hash and the hash of the connection as the row holds it. */
        await ExecAsync(
            owner,
            "INSERT INTO config.legacy_secret_pin (server_id, slot, value_sha256, binding_sha256) VALUES (@id, 'server', @value, @binding)",
            ct,
            ("id", NpgsqlDbType.Integer, ServerId),
            ("value", NpgsqlDbType.Bytea, SHA256.HashData(Encoding.UTF8.GetBytes(stored))),
            ("binding", NpgsqlDbType.Bytea, unpinned.SecretBinding.LegacyPinHash()));

        var pinned = await ReadAlphaAsync(owner, ct);
        Assert.NotNull(pinned.SecretPin);
        Assert.Equal(Plain, DarlingSecrets.ResolvePassword(pinned, out _, ring));

        /* A direct change of the host by the owner: the pin no longer matches the row. */
        await ExecAsync(owner, $"UPDATE config_monitored_servers SET host = 'moved-host.example.test' WHERE server_id = {ServerId}", ct);
        var moved = await ReadAlphaAsync(owner, ct);
        Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(moved, out _, ring));

        /* Back at the pinned host the pin matches again; a different stored value does not match the pin. */
        await ExecAsync(owner, $"UPDATE config_monitored_servers SET host = 'alpha-host.example.test' WHERE server_id = {ServerId}", ct);
        Assert.Equal(Plain, DarlingSecrets.ResolvePassword(await ReadAlphaAsync(owner, ct), out _, ring));
        await ExecAsync(
            owner,
            $"UPDATE config_monitored_servers SET encrypted_password = @other WHERE server_id = {ServerId}",
            ct,
            ("other", NpgsqlDbType.Text, DarlingSecrets.Protect("another-p@ss-not-real")));
        var changed = await ReadAlphaAsync(owner, ct);
        Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(changed, out _, ring));
    }

    [Fact]
    public async Task ASealedPasswordOpensForTheRowItWasSavedForAndNotForARowTheOwnerMoved()
    {
        var ct = TestContext.Current.CancellationToken;
        using var key = PasswordPrivateKey.Generate();
        var ring = DarlingPasswordKey.FromPrivateKey(key);
        var (scratchStore, owner, _) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var scratchHolder = scratchStore;
        await using var ownerHolder = owner;
        await InsertServerAsync(owner, "placeholder", ct);

        var row = await ReadAlphaAsync(owner, ct);
        var sealedText = PasswordSeal.Seal(Plain, key.PublicKey, row.SecretBinding);
        await ExecAsync(
            owner,
            $"UPDATE config_monitored_servers SET encrypted_password = @sealed WHERE server_id = {ServerId}",
            ct,
            ("sealed", NpgsqlDbType.Text, sealedText));

        Assert.Equal(Plain, DarlingSecrets.ResolvePassword(await ReadAlphaAsync(owner, ct), out _, ring));

        await ExecAsync(owner, $"UPDATE config_monitored_servers SET host = 'moved-host.example.test' WHERE server_id = {ServerId}", ct);
        var movedRow = await ReadAlphaAsync(owner, ct);
        var refused = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(movedRow, out _, ring));
        Assert.Contains("saved for a different connection, or was changed", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ThePinTableReadReturnsEverySlotIncludingSmtp()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratchStore, owner, _) = await ServerAddViewerRoleLiveTests.OpenAsync(ct);
        await using var scratchHolder = scratchStore;
        await using var ownerHolder = owner;
        foreach (var (id, slot) in new[] { (ServerId, "server"), (ServerId, "remediation"), (0, "smtp") })
        {
            await ExecAsync(
                owner,
                "INSERT INTO config.legacy_secret_pin (server_id, slot, value_sha256, binding_sha256) VALUES (@id, @slot, @value, @binding)",
                ct,
                ("id", NpgsqlDbType.Integer, id),
                ("slot", NpgsqlDbType.Text, slot),
                ("value", NpgsqlDbType.Bytea, new byte[] { 1 }),
                ("binding", NpgsqlDbType.Bytea, new byte[] { 2 }));
        }

        await using var connection = await owner.OpenConnectionAsync(ct);
        var pins = await StoreConfigProvider.ReadLegacyPinsAsync(connection, ct);

        Assert.Equal(3, pins.Count);
        Assert.True(pins.ContainsKey((0, "smtp")));
        Assert.True(pins.ContainsKey((ServerId, "remediation")));
        Assert.Equal(new byte[] { 1 }, pins[(ServerId, "server")].ValueSha256);
    }
}
