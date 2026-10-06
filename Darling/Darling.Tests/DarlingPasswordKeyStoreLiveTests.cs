/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact here mints its own scratch database through ScratchPostgres, runs the service's key start
   against it as the store owner, and never touches another test's rows, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// The service's password key at run time against a real store (#5366): the start (generate, publish, use, retire after a
/// reset, a key kept from an open directory), the per-host state row, the legacy pin snapshot, the per-sweep check and
/// the trigger check. Each fact uses its own scratch database and its own key directory.
/// </summary>
public sealed class DarlingPasswordKeyStoreLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AFreshStore_GeneratesAKey_PublishesIt_AndRecordsTheHostAsOk()
    {
        await using var store = await StoreFixture.CreateAsync();
        var bodySucceeded = false;
        try
        {
            var logger = new ListLogger();
            var runtime = await store.StartAsync(logger: logger);

            Assert.Equal("ok", runtime.State);
            Assert.True(runtime.Ring.Status.CanSeal);
            Assert.Same(runtime.Ring, store.LastRing);
            Assert.True(File.Exists(Path.Combine(store.Directory, DarlingPasswordKeyFile.FileName)));

            var published = await store.ReadCurrentAsync();
            Assert.NotNull(published);
            Assert.Equal(runtime.Ring.Status.KeyId, published.KeyId);
            Assert.Equal("RSA3072-OAEP-SHA256/A256GCM", published.Algorithm);

            var state = await store.ReadStateAsync("example-host");
            Assert.Equal(("ok", published.KeyId), (state.State, state.KeyId));
            Assert.Contains(logger.Lines, l => l.StartsWith($"Password key {PasswordSeal.DisplayKeyId(published.KeyId)} generated in ", StringComparison.Ordinal));

            // A second start finds the file and the published key agree and says "loaded".
            var again = await store.StartAsync(logger: logger);
            Assert.Equal("ok", again.State);
            Assert.Equal(runtime.Ring.Status.KeyId, again.Ring.Status.KeyId);
            Assert.Contains(logger.Lines, l => l.StartsWith($"Password key {PasswordSeal.DisplayKeyId(published.KeyId)} loaded from ", StringComparison.Ordinal));

            // What the ring seals, the ring opens, and what is stored carries the published key's id.
            var binding = PasswordBinding.ForSmtp("smtp.example.com", 25, false, "user");
            var sealedText = again.Ring.Seal("p@ss-not-real", binding);
            Assert.Equal("p@ss-not-real", again.Ring.Open(sealedText, binding));
            Assert.Equal(published.KeyId, PasswordSeal.KeyIdOf(sealedText));
            bodySucceeded = true;
        }
        finally
        {
            await store.CleanupAsync(bodySucceeded);
        }
    }

    [Fact]
    public async Task AResetThenAStart_RetiresTheOldFile_MakesANewKey_AndMarksTheOldRowReplaced()
    {
        await using var store = await StoreFixture.CreateAsync();
        var bodySucceeded = false;
        try
        {
            var first = await store.StartAsync();
            var oldId = first.Ring.Status.KeyId!;

            await using (var c = await store.Source.OpenConnectionAsync(TestContext.Current.CancellationToken))
            {
                Assert.Equal(oldId, await DarlingPasswordKeyStore.MarkCurrentResetAsync(c, TestContext.Current.CancellationToken));
            }

            var second = await store.StartAsync();

            Assert.Equal("ok", second.State);
            Assert.NotEqual(oldId, second.Ring.Status.KeyId);
            Assert.True(File.Exists(Path.Combine(store.Directory, DarlingPasswordKeyFile.FileName + ".retired")));
            Assert.True(File.Exists(Path.Combine(store.Directory, DarlingPasswordKeyFile.FileName)));

            var rows = await store.ReadRowsAsync();
            Assert.Equal(["current", "replaced"], [rows[second.Ring.Status.KeyId!].State, rows[oldId].State]);
            Assert.Equal("reset", rows[oldId].Reason);
            Assert.True(rows[oldId].ReplacedAtSet);
            bodySucceeded = true;
        }
        finally
        {
            await store.CleanupAsync(bodySucceeded);
        }
    }

    [Fact]
    public async Task AKeyFileWithNoPublishedKey_IsPublished_AndAMissingFileRefusesWithTheStateMissing()
    {
        await using var store = await StoreFixture.CreateAsync();
        var bodySucceeded = false;
        try
        {
            using var made = PasswordPrivateKey.Generate();
            var written = DarlingPasswordKeyFile.Generate(store.Directory, made.ExportPkcs8, new ListLogger());
            Assert.True(written.Present);

            var published = await store.StartAsync();
            Assert.Equal("ok", published.State);
            Assert.Equal(made.PublicKey.KeyId, (await store.ReadCurrentAsync())!.KeyId);

            // The store publishes a key and the directory no longer has it.
            File.Move(Path.Combine(store.Directory, DarlingPasswordKeyFile.FileName), Path.Combine(store.Directory, "moved-away"));
            var missing = await store.StartAsync();

            Assert.Equal("missing", missing.State);
            Assert.False(missing.Ring.Status.CanSeal);
            Assert.Contains("is missing from the credentials directory", missing.Ring.Status.Reason, StringComparison.Ordinal);
            Assert.Equal("missing", (await store.ReadStateAsync("example-host")).State);
            Assert.False(File.Exists(Path.Combine(store.Directory, DarlingPasswordKeyFile.FileName)));
            bodySucceeded = true;
        }
        finally
        {
            await store.CleanupAsync(bodySucceeded);
        }
    }

    [Fact]
    public async Task AKeptKeyThatPassesTheSelfTest_IsAccepted_AndIsBackAtTheLiveNameAfterTheStart()
    {
        await using var store = await StoreFixture.CreateAsync();
        var bodySucceeded = false;
        try
        {
            var first = await store.StartAsync();
            var keptPath = Path.Combine(store.Directory, DarlingPasswordKeyFile.QuarantineFileName);
            File.Move(Path.Combine(store.Directory, DarlingPasswordKeyFile.FileName), keptPath);

            var logger = new ListLogger();
            var second = await store.StartAsync(logger: logger);

            Assert.Equal("ok", second.State);
            Assert.Equal(first.Ring.Status.KeyId, second.Ring.Status.KeyId);
            Assert.True(File.Exists(Path.Combine(store.Directory, DarlingPasswordKeyFile.FileName)));
            Assert.False(File.Exists(keptPath));
            Assert.Contains(logger.Lines, l => l.Contains("another user may have read it", StringComparison.Ordinal));
            Assert.Contains("another user may have read it", (await store.ReadStateAsync("example-host")).Note, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await store.CleanupAsync(bodySucceeded);
        }
    }

    [Fact]
    public async Task AKeptKeyWhosePrivateHalfDoesNotOpenTheSelfTest_IsNotUsed_AndStaysWhereItWas()
    {
        await using var store = await StoreFixture.CreateAsync();
        var bodySucceeded = false;
        try
        {
            await store.StartAsync();
            var keptPath = Path.Combine(store.Directory, DarlingPasswordKeyFile.QuarantineFileName);
            File.Move(Path.Combine(store.Directory, DarlingPasswordKeyFile.FileName), keptPath);

            var refused = await store.StartAsync(selfTest: static (_, _) => false);

            Assert.Equal("mismatch", refused.State);
            Assert.False(refused.Ring.Status.CanSeal);
            Assert.True(File.Exists(keptPath));
            Assert.False(File.Exists(Path.Combine(store.Directory, DarlingPasswordKeyFile.FileName)));
            Assert.Equal("mismatch", (await store.ReadStateAsync("example-host")).State);
            bodySucceeded = true;
        }
        finally
        {
            await store.CleanupAsync(bodySucceeded);
        }
    }

    [Fact]
    public async Task ADisabledOrExtraTrigger_MakesTheStartRefuse_AndNoKeyIsPublished()
    {
        await using var store = await StoreFixture.CreateAsync();
        var bodySucceeded = false;
        try
        {
            await store.ExecAsync("ALTER TABLE config.password_key DISABLE TRIGGER trg_password_key_owner_only_row;");
            var disabled = await store.StartAsync();
            Assert.Equal("refused", disabled.State);
            Assert.False(disabled.Ring.Status.CanSeal);
            Assert.Contains("trg_password_key_owner_only_row", disabled.Ring.Status.Reason, StringComparison.Ordinal);
            Assert.Null(await store.ReadCurrentAsync());
            Assert.Equal("refused", (await store.ReadStateAsync("example-host")).State);

            await store.ExecAsync("ALTER TABLE config.password_key ENABLE ALWAYS TRIGGER trg_password_key_owner_only_row;");
            await store.ExecAsync(
                "CREATE TRIGGER trg_extra BEFORE INSERT ON config.legacy_secret_pin FOR EACH ROW EXECUTE FUNCTION config.password_key_owner_only();");
            var extra = await store.StartAsync();
            Assert.Equal("refused", extra.State);
            Assert.Contains("trg_extra", extra.Ring.Status.Reason, StringComparison.Ordinal);

            await store.ExecAsync("DROP TRIGGER trg_extra ON config.legacy_secret_pin;");
            Assert.Equal("ok", (await store.StartAsync()).State);
            bodySucceeded = true;
        }
        finally
        {
            await store.CleanupAsync(bodySucceeded);
        }
    }

    [Fact]
    public async Task TheSweepCheck_RefusesOnAResetAMismatchOrADisabledTrigger_AndRecoversWhenTheStoreIsRight()
    {
        await using var store = await StoreFixture.CreateAsync();
        var bodySucceeded = false;
        try
        {
            var runtime = await store.StartAsync();
            var ct = TestContext.Current.CancellationToken;
            var heldId = runtime.Ring.Status.KeyId!;

            await runtime.SweepCheckAsync(ct);
            Assert.Same(runtime.Ring, store.LastRing);

            // A trigger that stops protecting the tables.
            await store.ExecAsync("ALTER TABLE config.password_key_service DISABLE TRIGGER trg_password_key_service_owner_only_row;");
            await runtime.SweepCheckAsync(ct);
            Assert.False(store.LastRing!.Status.CanSeal);
            Assert.Equal("refused", (await store.ReadStateAsync("example-host")).State);
            await store.ExecAsync("ALTER TABLE config.password_key_service ENABLE ALWAYS TRIGGER trg_password_key_service_owner_only_row;");
            await runtime.SweepCheckAsync(ct);
            Assert.Same(runtime.Ring, store.LastRing);
            Assert.Equal("ok", (await store.ReadStateAsync("example-host")).State);

            // A reset made while the service runs: no current row.
            await using (var c = await store.Source.OpenConnectionAsync(ct))
            {
                await DarlingPasswordKeyStore.MarkCurrentResetAsync(c, ct);
            }

            await runtime.SweepCheckAsync(ct);
            Assert.False(store.LastRing!.Status.CanSeal);
            Assert.Equal("reset_pending", (await store.ReadStateAsync("example-host")).State);

            // A different key published in its place.
            using var other = PasswordPrivateKey.Generate();
            await store.ExecAsync(
                "INSERT INTO config.password_key (key_id, public_key, algorithm, state) VALUES (@id, @key, 'RSA3072-OAEP-SHA256/A256GCM', 'current');",
                ("id", other.PublicKey.KeyId), ("key", other.PublicKey.Spki));
            await runtime.SweepCheckAsync(ct);
            Assert.False(store.LastRing!.Status.CanSeal);
            Assert.Equal("mismatch", (await store.ReadStateAsync("example-host")).State);
            Assert.NotEqual(heldId, other.PublicKey.KeyId);
            bodySucceeded = true;
        }
        finally
        {
            await store.CleanupAsync(bodySucceeded);
        }
    }

    [Fact]
    public async Task StateRowsNotUpdatedForThirtyDays_AreDeletedAtStart_AndRecentOnesStay()
    {
        await using var store = await StoreFixture.CreateAsync();
        var bodySucceeded = false;
        try
        {
            await store.ExecAsync(
                "INSERT INTO config.password_key_service (service_host, key_id, state, updated_at) VALUES ('old-host-example', NULL, 'ok', (now() AT TIME ZONE 'UTC') - interval '31 days'), " +
                "('recent-host-example', NULL, 'ok', (now() AT TIME ZONE 'UTC') - interval '29 days');");

            await store.StartAsync();

            var hosts = await store.ReadHostsAsync();
            Assert.Equal(["example-host", "recent-host-example"], hosts);
            bodySucceeded = true;
        }
        finally
        {
            await store.CleanupAsync(bodySucceeded);
        }
    }

    [Fact]
    public async Task OnWindows_ThePinSnapshotPinsEachOpenableLegacyValue_ToItsRowAndConnection_ThenIsDone()
    {
        await using var store = await StoreFixture.CreateAsync();
        var bodySucceeded = false;
        try
        {
            await store.SeedServersAsync();
            string? Open(string stored) => stored.StartsWith("legacy-", StringComparison.Ordinal) ? "p@ss-not-real" : throw new CryptographicException("not a blob");

            var logger = new ListLogger();
            await store.StartAsync(isWindows: true, unprotect: Open, logger: logger);

            Assert.Equal("done", await store.MarkerAsync());
            var pins = await store.ReadPinsAsync();
            Assert.Equal(["0/smtp", "1/remediation", "1/server"], pins.Keys.Order(StringComparer.Ordinal).ToArray());

            // value_sha256 is the hash of the STORED text, never of the password.
            Assert.Equal(SHA256.HashData(Encoding.UTF8.GetBytes("legacy-server-blob")), pins["1/server"].Value);
            Assert.NotEqual(SHA256.HashData(Encoding.UTF8.GetBytes("p@ss-not-real")), pins["1/server"].Value);
            Assert.Equal(SHA256.HashData(Encoding.UTF8.GetBytes("legacy-remediation-blob")), pins["1/remediation"].Value);
            Assert.Equal(SHA256.HashData(Encoding.UTF8.GetBytes("legacy-smtp-blob")), pins["0/smtp"].Value);

            var server = ServerConnectionIdentity.FromStoredColumns("alpha-example", 1433, "sqlserver", "", false, "sql", "monitor_login", "Mandatory", false, false);
            Assert.Equal(PasswordBinding.ForServer(server).LegacyPinHash(), pins["1/server"].Binding);
            Assert.Equal(PasswordBinding.ForRemediation(server, "fix_login").LegacyPinHash(), pins["1/remediation"].Binding);
            Assert.Equal(PasswordBinding.ForSmtp("smtp.example.com", 587, true, "mail_login").LegacyPinHash(), pins["0/smtp"].Binding);

            // The sealed, reference and empty values were not pinned (server 2, 3 and 4 hold them), and a second start adds nothing.
            await store.StartAsync(isWindows: true, unprotect: Open);
            Assert.Equal(3, (await store.ReadPinsAsync()).Count);
            bodySucceeded = true;
        }
        finally
        {
            await store.CleanupAsync(bodySucceeded);
        }
    }

    [Fact]
    public async Task OnLinux_ThePendingMarkerBecomesSkipped_AndNothingIsPinned()
    {
        await using var store = await StoreFixture.CreateAsync();
        var bodySucceeded = false;
        try
        {
            await store.SeedServersAsync();
            await store.StartAsync(isWindows: false, unprotect: static _ => throw new InvalidOperationException("must not be called"));

            Assert.Equal("skipped", await store.MarkerAsync());
            Assert.Empty(await store.ReadPinsAsync());
            bodySucceeded = true;
        }
        finally
        {
            await store.CleanupAsync(bodySucceeded);
        }
    }

    [Fact]
    public async Task AMissingMarkerRow_PinsNothing_AndCreatesNoMarker()
    {
        await using var store = await StoreFixture.CreateAsync();
        var bodySucceeded = false;
        try
        {
            await store.SeedServersAsync();
            await store.ExecAsync("DELETE FROM config.legacy_secret_pin_marker;");

            var runtime = await store.StartAsync(isWindows: true, unprotect: static _ => "p@ss-not-real");

            Assert.Equal("ok", runtime.State);
            Assert.Null(await store.MarkerAsync());
            Assert.Empty(await store.ReadPinsAsync());
            bodySucceeded = true;
        }
        finally
        {
            await store.CleanupAsync(bodySucceeded);
        }
    }

    [Fact]
    public async Task ANonOwnerWhoUpdatesThePublishedKey_GetsPW010()
    {
        await using var store = await StoreFixture.CreateAsync();
        var bodySucceeded = false;
        var role = "pk_probe_" + Guid.NewGuid().ToString("N")[..10];
        try
        {
            await store.StartAsync();
            await store.ExecAsync($"CREATE ROLE {role} LOGIN PASSWORD 'p@ss-not-real'; GRANT USAGE ON SCHEMA config TO {role}; GRANT SELECT, UPDATE ON config.password_key TO {role};");
            var builder = new NpgsqlConnectionStringBuilder(store.Scratch.ConnectionString) { Username = role, Password = "p@ss-not-real", Pooling = false };
            await using var connection = new NpgsqlConnection(builder.ConnectionString);
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await using var update = new NpgsqlCommand("UPDATE config.password_key SET public_key = decode(repeat('01', 400), 'hex');", connection);

            var thrown = await Assert.ThrowsAsync<PostgresException>(() => update.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
            Assert.Equal(PasswordKeyTables.OwnerOnlySqlState, thrown.SqlState);
            bodySucceeded = true;
        }
        finally
        {
            await store.CleanupAsync(bodySucceeded, $"DROP OWNED BY {role}; DROP ROLE IF EXISTS {role};");
        }
    }

    private sealed class ListLogger : ILogger
    {
        public List<string> Lines { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(formatter(state, exception));
    }

    /// <summary>One scratch store, migrated, with its own key directory and the owner's data source.</summary>
    private sealed class StoreFixture : IAsyncDisposable
    {
        private StoreFixture(ScratchPostgres scratch, NpgsqlDataSource source, string directory)
        {
            Scratch = scratch;
            Source = source;
            Directory = directory;
        }

        public ScratchPostgres Scratch { get; }

        public NpgsqlDataSource Source { get; }

        public string Directory { get; }

        public IPasswordKeyRing? LastRing { get; private set; }

        public static async Task<StoreFixture> CreateAsync()
        {
            Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the password key runtime live pins (each mints its own scratch database).");
            var ct = TestContext.Current.CancellationToken;
            var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);
                await PgMigrations.MigrateAsync(owner, ct);
            }

            var directory = Path.Combine(Path.GetTempPath(), "pmkey-" + Guid.NewGuid().ToString("N")[..12]);
            System.IO.Directory.CreateDirectory(directory);
            return new StoreFixture(scratch, NpgsqlDataSource.Create(scratch.ConnectionString), directory);
        }

        public Task<DarlingPasswordKeyRuntime> StartAsync(
            bool isWindows = false, Func<string, string?>? unprotect = null, Func<PasswordPrivateKey, PublishedKey, bool>? selfTest = null,
            ILogger? logger = null) =>
            DarlingPasswordKeyRuntime.StartAsync(
                Directory, Source, logger ?? new ListLogger(), TestContext.Current.CancellationToken,
                new PasswordKeyStartOptions
                {
                    IsWindows = isWindows,
                    Unprotect = unprotect ?? (static _ => null),
                    SelfTest = selfTest,
                    SetRing = ring => LastRing = ring,
                    ServiceHost = "example-host",
                });

        public async Task ExecAsync(string sql, params (string Name, object Value)[] parameters)
        {
            await using var c = await Source.OpenConnectionAsync(TestContext.Current.CancellationToken);
            await using var command = new NpgsqlCommand(sql, c);
            foreach (var (name, value) in parameters)
            {
                command.Parameters.AddWithValue(name, value);
            }

            await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        public async Task<PublishedPasswordKey?> ReadCurrentAsync()
        {
            await using var c = await Source.OpenConnectionAsync(TestContext.Current.CancellationToken);
            return await PasswordKeyTables.ReadCurrentAsync(c, TestContext.Current.CancellationToken);
        }

        public async Task<(string State, string? KeyId, string? Note)> ReadStateAsync(string host)
        {
            await using var c = await Source.OpenConnectionAsync(TestContext.Current.CancellationToken);
            await using var command = new NpgsqlCommand("SELECT state, key_id, note FROM config.password_key_service WHERE service_host = @h;", c);
            command.Parameters.AddWithValue("h", host);
            await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken), $"No state row for {host}.");
            return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2));
        }

        public async Task<List<string>> ReadHostsAsync()
        {
            var hosts = new List<string>();
            await using var c = await Source.OpenConnectionAsync(TestContext.Current.CancellationToken);
            await using var command = new NpgsqlCommand("SELECT service_host FROM config.password_key_service ORDER BY service_host;", c);
            await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            while (await reader.ReadAsync(TestContext.Current.CancellationToken))
            {
                hosts.Add(reader.GetString(0));
            }

            return hosts;
        }

        public async Task<Dictionary<string, (string State, string? Reason, bool ReplacedAtSet)>> ReadRowsAsync()
        {
            var rows = new Dictionary<string, (string, string?, bool)>();
            await using var c = await Source.OpenConnectionAsync(TestContext.Current.CancellationToken);
            await using var command = new NpgsqlCommand("SELECT key_id, state, replaced_reason, replaced_at IS NOT NULL FROM config.password_key;", c);
            await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            while (await reader.ReadAsync(TestContext.Current.CancellationToken))
            {
                rows[reader.GetString(0)] = (reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2), reader.GetBoolean(3));
            }

            return rows;
        }

        public async Task<string?> MarkerAsync()
        {
            await using var c = await Source.OpenConnectionAsync(TestContext.Current.CancellationToken);
            await using var command = new NpgsqlCommand("SELECT state FROM config.legacy_secret_pin_marker WHERE id = 1;", c);
            return (string?)await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        }

        public async Task<Dictionary<string, (byte[] Value, byte[] Binding)>> ReadPinsAsync()
        {
            var pins = new Dictionary<string, (byte[], byte[])>();
            await using var c = await Source.OpenConnectionAsync(TestContext.Current.CancellationToken);
            await using var command = new NpgsqlCommand("SELECT server_id, slot, value_sha256, binding_sha256 FROM config.legacy_secret_pin;", c);
            await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            while (await reader.ReadAsync(TestContext.Current.CancellationToken))
            {
                pins[$"{reader.GetInt32(0)}/{reader.GetString(1)}"] = ((byte[])reader.GetValue(2), (byte[])reader.GetValue(3));
            }

            return pins;
        }

        /// <summary>Server 1 holds a legacy blob for the server and the remediation login; server 2 a sealed value, 3 a reference and
        /// 4 an empty one; the notification row holds a legacy blob. Only the legacy blobs may be pinned.</summary>
        public async Task SeedServersAsync()
        {
            await ExecAsync(@"
INSERT INTO config.config_monitored_servers (server_id, name, host, database, auth, username, encrypted_password, remediation_username, remediation_encrypted_password, port)
VALUES (1, 'alpha-example', 'alpha-example', '', 'sql', 'monitor_login', 'legacy-server-blob', 'fix_login', 'legacy-remediation-blob', 1433),
       (2, 'beta-example', 'beta-example', NULL, 'sql', 'monitor_login', 'sealed:v1:0123456789abcdef:AAAA', NULL, NULL, 1433),
       (3, 'gamma-example', 'gamma-example', NULL, 'sql', 'monitor_login', 'env:EXAMPLE_PASSWORD', NULL, NULL, 1433),
       (4, 'delta-example', 'delta-example', NULL, 'sql', 'monitor_login', '', NULL, NULL, 1433);
INSERT INTO config.config_notification (id, smtp_host, smtp_port, smtp_use_ssl, smtp_username, smtp_encrypted_password)
VALUES (1, 'smtp.example.com', 587, TRUE, 'mail_login', 'legacy-smtp-blob')
ON CONFLICT (id) DO UPDATE SET smtp_host = EXCLUDED.smtp_host, smtp_port = EXCLUDED.smtp_port, smtp_use_ssl = EXCLUDED.smtp_use_ssl,
    smtp_username = EXCLUDED.smtp_username, smtp_encrypted_password = EXCLUDED.smtp_encrypted_password;");
        }

        public async Task CleanupAsync(bool bodySucceeded, string? extraSql = null)
        {
            try
            {
                await LiveStoreCleanup.RunAsync(Scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                {
                    if (extraSql is not null)
                    {
                        await using var command = new NpgsqlCommand(extraSql, cleanup);
                        await command.ExecuteNonQueryAsync(cleanupCt);
                    }
                });
            }
            finally
            {
                try
                {
                    System.IO.Directory.Delete(Directory, recursive: true);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Source.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }
}
