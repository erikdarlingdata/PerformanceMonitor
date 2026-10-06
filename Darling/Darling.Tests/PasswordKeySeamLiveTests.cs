/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact here mints its own scratch database through ScratchPostgres, runs the service's key start
   against it as the store owner, and never touches another test's rows, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// End to end through the seams between the pieces of the password key (#5366), each tested alone elsewhere: the Viewer's
/// sealer seals to the key the service published, the row is written, the service's store read maps it, and the resolver
/// opens it; and a legacy value pinned by the key start opens, then is refused once the owner changes the row's host.
/// </summary>
public sealed class PasswordKeySeamLiveTests
{
    private const string FakePassword = "p@ss-not-real";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string InsertServer = @"
INSERT INTO config.config_monitored_servers
    (server_id, name, host, database, engine, port, read_only_intent, auth, username, encrypt_mode, trust_server_certificate,
     multi_subnet_failover, encrypted_password)
VALUES (@id, @name, 'alpha-example', 'example_db', 'sqlserver', 1433, FALSE, 'sql', 'monitor_login', 'Mandatory', FALSE, FALSE, @stored);";

    [Fact]
    public async Task ASealedValueFromTheViewerPath_IsReadByTheStoreRead_AndOpenedByTheResolver_ForItsOwnConnectionOnly()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the password key seam pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var directory = Directory.CreateTempSubdirectory("pmkey-seam-").FullName;
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);

            // The service starts: it makes a key and publishes the public half.
            IPasswordKeyRing? ring = null;
            var runtime = await DarlingPasswordKeyRuntime.StartAsync(
                directory, source, new CapturingTestLogger(), ct,
                new PasswordKeyStartOptions { IsWindows = false, SetRing = r => ring = r, ServiceHost = "example-host" });
            Assert.Equal("ok", runtime.State);
            Assert.NotNull(ring);

            // The Viewer reads the published key and seals the password for the row it is about to save.
            PublishedPasswordKey? published;
            await using (var read = await source.OpenConnectionAsync(ct))
            {
                published = await PasswordKeyTables.ReadCurrentAsync(read, ct);
            }

            Assert.NotNull(published);
            var sealer = new ViewerPasswordSealer(PasswordPublicKey.FromSpki(published.Spki));
            var row = new MonitoredServerRow
            {
                Host = "alpha-example", Database = "example_db", Engine = "sqlserver", Port = 1433, Auth = "sql",
                Username = "monitor_login", EncryptMode = "Mandatory",
            };
            var stored = sealer.Seal(FakePassword, row);
            Assert.StartsWith("sealed:v1:" + published.KeyId + ":", stored, StringComparison.Ordinal);
            Assert.DoesNotContain(FakePassword, stored, StringComparison.Ordinal);

            await ExecAsync(source, InsertServer, ct, ("id", 1), ("name", "alpha-example"), ("stored", stored));

            // The service's store read maps the row, and the resolver opens the value with the service's ring.
            var server = await LoadOnlyServerAsync(source, ct);
            Assert.Equal(stored, server.EncryptedPassword);
            Assert.Equal(FakePassword, DarlingSecrets.ResolvePassword(server, out var usedPlaintext, ring!, new LegacyDpapi(false, NoDpapi)));
            Assert.False(usedPlaintext);

            // The same text under another connection setting is refused, and so is a different host after a later edit.
            await ExecAsync(source, "UPDATE config.config_monitored_servers SET host = 'beta-example' WHERE server_id = 1;", ct);
            var moved = await LoadOnlyServerAsync(source, ct);
            var refused = Assert.Throws<InvalidOperationException>(
                () => DarlingSecrets.ResolvePassword(moved, out _, ring!, new LegacyDpapi(false, NoDpapi)));
            Assert.DoesNotContain(FakePassword, refused.Message, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
            TryDeleteDirectory(directory);
        }
    }

    [Fact]
    public async Task ALegacyValuePinnedByTheKeyStart_Opens_AndIsRefusedAfterTheOwnerChangesItsHost()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the password key seam pins (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var directory = Directory.CreateTempSubdirectory("pmkey-seam-").FullName;
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);

            // A value saved before the upgrade, in the old format.
            await ExecAsync(source, InsertServer, ct, ("id", 1), ("name", "alpha-example"), ("stored", "legacy-server-blob"));

            var opened = 0;
            string Unprotect(string text)
            {
                opened++;
                return text == "legacy-server-blob" ? FakePassword : throw new System.Security.Cryptography.CryptographicException("not a saved value");
            }

            // The first start on Windows pins the value to its row and connection.
            IPasswordKeyRing? ring = null;
            await DarlingPasswordKeyRuntime.StartAsync(
                directory, source, new CapturingTestLogger(), ct,
                new PasswordKeyStartOptions { IsWindows = true, Unprotect = Unprotect, SetRing = r => ring = r, ServiceHost = "example-host" });
            var openedByTheStart = opened;
            Assert.True(openedByTheStart > 0, "the start did not open the old value to pin it");

            var dpapi = new LegacyDpapi(true, Unprotect);
            var server = await LoadOnlyServerAsync(source, ct);
            Assert.NotNull(server.SecretPin);
            Assert.Equal(FakePassword, DarlingSecrets.ResolvePassword(server, out _, ring!, dpapi));

            // The owner changes the host: the pin no longer matches, so the old value is refused before it is opened.
            await ExecAsync(source, "UPDATE config.config_monitored_servers SET host = 'beta-example' WHERE server_id = 1;", ct);
            var moved = await LoadOnlyServerAsync(source, ct);
            var openedBeforeRefusal = opened;
            var refused = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(moved, out _, ring!, dpapi));
            Assert.DoesNotContain(FakePassword, refused.Message, StringComparison.Ordinal);
            Assert.Equal(openedBeforeRefusal, opened);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
            TryDeleteDirectory(directory);
        }
    }

    private static string NoDpapi(string text) => throw new InvalidOperationException("The old format must not be opened here.");

    private static async Task<MonitoredServer> LoadOnlyServerAsync(NpgsqlDataSource source, System.Threading.CancellationToken ct)
    {
        var view = await new StoreConfigProvider(source).LoadViewAsync(new DarlingConfig(), ct);
        Assert.NotNull(view);
        return view.EnabledServers.Single();
    }

    private static async Task ExecAsync(
        NpgsqlDataSource source, string sql, System.Threading.CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var c = await source.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(sql, c);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static void TryDeleteDirectory(string directory)
    {
        try
        {
            Directory.Delete(directory, recursive: true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
