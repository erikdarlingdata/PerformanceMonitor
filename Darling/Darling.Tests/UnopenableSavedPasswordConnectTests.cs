/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9749: a server whose saved password cannot be opened is a failed connect with a named cause. The connector refuses before any
/// network work, the refusal is the failed attempt the worker handles, and the text the worker keeps for the registry is the
/// resolver's own sentence. The sealed value and the password are never part of it.
/// </summary>
public sealed class UnopenableSavedPasswordConnectTests
{
    internal const string Plain = "p@ss-not-real";
    private static readonly Lazy<PasswordPrivateKey> s_savedWith = new(PasswordPrivateKey.Generate);
    private static readonly Lazy<PasswordPrivateKey> s_heldNow = new(PasswordPrivateKey.Generate);

    internal static MonitoredServer SqlAuthServer() => new()
    {
        Name = "alpha",
        Host = "alpha-host.example.test",
        Auth = "sql",
        Username = "monitor",
    };

    /// <summary>A password sealed for this server's connection with the key the service held when it was saved.</summary>
    internal static string SealedFor(MonitoredServer server) =>
        PasswordSeal.Seal(Plain, s_savedWith.Value.PublicKey, server.SecretBinding);

    /// <summary>The ring of a service whose key file is gone while the store still publishes the key: it refuses with the start-up decision's reason.</summary>
    internal static IPasswordKeyRing KeyFileMissing()
    {
        var published = s_savedWith.Value.PublicKey;
        var decision = DarlingPasswordKeyState.Decide(new PasswordKeyFacts(
            FilePresent: false, FileUntrusted: false, FileRefusal: null, FileSpki: null,
            FoundAfterOpenDirectory: false, StoreCurrent: new PublishedKey(published.KeyId, published.Spki), FileKeyRetiredInStore: false));
        Assert.Equal(PasswordKeyAction.Missing, decision.Action);
        return DarlingPasswordKey.Refusing(decision.Reason!);
    }

    /// <summary>The ring of a service that holds a different key from the one the password was sealed to (a reset key, another credentials volume).</summary>
    internal static IPasswordKeyRing AnotherKey() => DarlingPasswordKey.FromPrivateKey(s_heldNow.Value);

    internal static DarlingWorker NewWorker(NpgsqlDataSource? store = null, ILogger<DarlingWorker>? logger = null) =>
        new(
            logger ?? NullLogger<DarlingWorker>.Instance,
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator())
        {
            StoreForTests = store,
            SelfAlertsForTests = new DarlingSelfAlertEvaluator(
                new DarlingSelfAlertTests.FakeSettings(), new DarlingSelfAlertTests.RecordingDeliverer(),
                new DarlingSelfAlertTests.FakeHistoryStore(), _ => false),
        };

    internal static async Task TryConnectAsync(DarlingWorker worker, DarlingWorker.ServerLoopState state, ConnectAttempt attempt, CancellationToken ct)
    {
        var tryConnect = typeof(DarlingWorker).GetMethod("TryConnectAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        await (Task)tryConnect.Invoke(worker, [state, null, new DarlingConfig(), attempt, ct])!;
    }

    [Fact]
    public async Task The_connector_refuses_before_the_network_and_the_refusal_is_the_failed_attempt()
    {
        var server = SqlAuthServer();
        server.EncryptedPassword = SealedFor(server);
        using var gate = new SemaphoreSlim(1, 1);

        /* The connect the sweep makes: the real connector, in the connect gate. The password is opened with the process's own ring. */
        var attempt = await ServerConnectProbe.AttemptAsync(
            server, (target, token) => DarlingServerConnector.ConnectAsync(target, NullLogger.Instance, token), gate, TestContext.Current.CancellationToken);

        Assert.Null(attempt.Runtime);
        /* The process's own ring decides which type: a ring that is not loaded yet is an ordinary failure, one that refuses is the typed refusal. */
        var failure = Assert.IsAssignableFrom<InvalidOperationException>(attempt.Failure);
        var refusal = Assert.ThrowsAny<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(server, out _));
        Assert.Equal(refusal.Message, failure.Message);
        Assert.DoesNotContain(server.EncryptedPassword, failure.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(Plain, failure.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task The_failed_attempt_reaches_the_workers_failure_path_with_the_resolvers_sentence(bool keyFileMissing)
    {
        var server = SqlAuthServer();
        server.EncryptedPassword = SealedFor(server);
        var refusal = Assert.Throws<SavedSecretUnavailableException>(
            () => DarlingSecrets.ResolvePassword(server, out _, keyFileMissing ? KeyFileMissing() : AnotherKey()));

        /* No store: the registry write is failure-isolated, so the failure path still runs to its end. */
        var state = new DarlingWorker.ServerLoopState { Config = server };
        var before = DateTime.UtcNow;
        var serviceLog = new CapturingTestLogger();
        await TryConnectAsync(NewWorker(logger: new WorkerLog(serviceLog)), state, new ConnectAttempt(server, null, refusal), TestContext.Current.CancellationToken);

        /* The service log says it will not clear on its own, once, with the resolver's sentence and without a retry promise. */
        Assert.Equal(1, serviceLog.CountAtLevel(LogLevel.Error));
        Assert.Equal(0, serviceLog.CountAtLevel(LogLevel.Warning));
        var errorLine = Assert.Single(serviceLog.Lines, line => line.StartsWith("Error:", StringComparison.Ordinal));
        Assert.Contains("will keep failing until fixed", errorLine, StringComparison.Ordinal);
        Assert.Contains(refusal.Message, errorLine, StringComparison.Ordinal);
        Assert.DoesNotContain(server.EncryptedPassword, errorLine, StringComparison.Ordinal);
        Assert.DoesNotContain(Plain, errorLine, StringComparison.Ordinal);

        Assert.Null(state.Runtime);
        Assert.Equal(1, state.ConsecutiveConnectFailures);
        Assert.True(state.NextConnectAttempt > before, "the server is in the connect backoff, not dropped");
        Assert.Equal(refusal.Message, state.LastConnectFailureLogged);
        Assert.DoesNotContain(server.EncryptedPassword, state.LastConnectFailureLogged, StringComparison.Ordinal);
        Assert.DoesNotContain(Plain, state.LastConnectFailureLogged, StringComparison.Ordinal);
    }
}

/// <summary>The worker's typed logger over a <see cref="CapturingTestLogger"/>.</summary>
internal sealed class WorkerLog(CapturingTestLogger inner) : ILogger<DarlingWorker>
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);

    public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
        inner.Log(logLevel, eventId, state, exception, formatter);
}

/// <summary>
/// #9749: the service log line for a failed connect. A saved password this host cannot open does not clear on its own, so it is an Error
/// that says so, written once per cause with the hourly reminder after it. A network failure is still a Warning that says it retries.
/// </summary>
public sealed class UnopenableSavedPasswordLogTests
{
    private static readonly DateTime T0 = new(2026, 11, 26, 0, 0, 0, DateTimeKind.Utc);

    private static (DarlingWorker Worker, CapturingTestLogger Log, Func<DateTime> Now, Action<TimeSpan> Advance) NewWorker()
    {
        var log = new CapturingTestLogger();
        var worker = UnopenableSavedPasswordConnectTests.NewWorker(logger: new WorkerLog(log));
        var now = T0;
        worker.RepeatLog.Clock = () => now;
        return (worker, log, () => now, by => now = T0 + by);
    }

    private static DarlingWorker.ServerLoopState NewState() => new()
    {
        Config = UnopenableSavedPasswordConnectTests.SqlAuthServer(),
    };

    private static SavedSecretUnavailableException Refusal(bool keyFileMissing)
    {
        var server = UnopenableSavedPasswordConnectTests.SqlAuthServer();
        server.EncryptedPassword = UnopenableSavedPasswordConnectTests.SealedFor(server);
        return Assert.Throws<SavedSecretUnavailableException>(() => DarlingSecrets.ResolvePassword(
            server, out _, keyFileMissing ? UnopenableSavedPasswordConnectTests.KeyFileMissing() : UnopenableSavedPasswordConnectTests.AnotherKey()));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void A_password_this_host_cannot_open_is_one_error_that_says_it_will_keep_failing(bool keyFileMissing)
    {
        var (worker, log, _, _) = NewWorker();
        var cause = Refusal(keyFileMissing);

        var kept = worker.LogConnectFailure(NewState(), cause, 240);

        Assert.Equal(cause.Message, kept);
        Assert.Equal(1, log.CountAtLevel(LogLevel.Error));
        Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));
        var line = Assert.Single(log.Lines);
        Assert.Contains("will keep failing until fixed", line, StringComparison.Ordinal);
        Assert.Contains(cause.Message, line, StringComparison.Ordinal);
        Assert.DoesNotContain("retrying", line, StringComparison.Ordinal);
    }

    private const string OldFormatText = "AQIDBAUGBwgJCg==";

    private static LegacyDpapi WindowsDpapi() => new(true, _ => UnopenableSavedPasswordConnectTests.Plain);

    private static MonitoredServer OldFormatServer(bool pinStepWaiting)
    {
        var server = UnopenableSavedPasswordConnectTests.SqlAuthServer();
        server.EncryptedPassword = OldFormatText;
        server.SecretPinWaiting = pinStepWaiting;
        return server;
    }

    /* #9749: while the one-time pin step waits to run (#5456) an unpinned old-format password opens once it has. The failed attempt the
       worker handles is the real resolver's refusal, and it logs the retrying Warning with the sentence that says to wait. */
    [Fact]
    public async Task An_unpinned_old_format_password_while_the_pin_step_waits_is_a_retrying_warning_that_says_to_wait()
    {
        var server = OldFormatServer(pinStepWaiting: true);
        var refusal = Assert.Throws<InvalidOperationException>(
            () => DarlingSecrets.ResolvePassword(server, out _, UnopenableSavedPasswordConnectTests.AnotherKey(), WindowsDpapi()));
        Assert.IsNotType<SavedSecretUnavailableException>(refusal);

        var state = new DarlingWorker.ServerLoopState { Config = server };
        var serviceLog = new CapturingTestLogger();
        await UnopenableSavedPasswordConnectTests.TryConnectAsync(
            UnopenableSavedPasswordConnectTests.NewWorker(logger: new WorkerLog(serviceLog)), state,
            new ConnectAttempt(server, null, refusal), TestContext.Current.CancellationToken);

        Assert.Equal(0, serviceLog.CountAtLevel(LogLevel.Error));
        Assert.Equal(1, serviceLog.CountAtLevel(LogLevel.Warning));
        var line = Assert.Single(serviceLog.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal));
        Assert.Contains("retrying in", line, StringComparison.Ordinal);
        Assert.Contains("one-time pin step", line, StringComparison.Ordinal);
        Assert.Contains("Do not enter the password again yet", line, StringComparison.Ordinal);
        Assert.DoesNotContain("will keep failing", line, StringComparison.Ordinal);
        Assert.Equal(refusal.Message, state.LastConnectFailureLogged);
        Assert.Equal(1, state.ConsecutiveConnectFailures);
    }

    [Fact]
    public void The_same_password_opens_once_the_pins_have_landed()
    {
        var server = OldFormatServer(pinStepWaiting: true);
        Assert.Throws<InvalidOperationException>(
            () => DarlingSecrets.ResolvePassword(server, out _, UnopenableSavedPasswordConnectTests.AnotherKey(), WindowsDpapi()));

        /* The step pins the value and raises the config version: the reload carries the pin, and the connect no longer fails. */
        var reloaded = OldFormatServer(pinStepWaiting: false);
        reloaded.SecretPin = new LegacyPin(SHA256.HashData(Encoding.UTF8.GetBytes(OldFormatText)), reloaded.SecretBinding.LegacyPinHash());

        Assert.Equal(
            UnopenableSavedPasswordConnectTests.Plain,
            DarlingSecrets.ResolvePassword(reloaded, out _, UnopenableSavedPasswordConnectTests.AnotherKey(), WindowsDpapi()));
    }

    [Fact]
    public void An_unpinned_old_format_password_with_no_pin_step_waiting_is_still_the_error_that_says_it_will_keep_failing()
    {
        var (worker, log, _, _) = NewWorker();
        var server = OldFormatServer(pinStepWaiting: false);
        var cause = Assert.Throws<SavedSecretUnavailableException>(
            () => DarlingSecrets.ResolvePassword(server, out _, UnopenableSavedPasswordConnectTests.AnotherKey(), WindowsDpapi()));

        worker.LogConnectFailure(NewState(), cause, 240);

        Assert.Equal(1, log.CountAtLevel(LogLevel.Error));
        Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));
        var line = Assert.Single(log.Lines);
        Assert.Contains("will keep failing until fixed", line, StringComparison.Ordinal);
        Assert.Contains("is in the old format", line, StringComparison.Ordinal);
    }

    [Fact]
    public void A_key_ring_that_is_not_loaded_yet_is_a_retrying_warning_and_a_ring_that_refuses_is_an_error()
    {
        var (worker, log, _, _) = NewWorker();
        var server = UnopenableSavedPasswordConnectTests.SqlAuthServer();
        server.EncryptedPassword = UnopenableSavedPasswordConnectTests.SealedFor(server);

        var notReady = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolvePassword(
            server, out _, DarlingPasswordKey.Refusing(DarlingPasswordKey.NotReadyReason), WindowsDpapi()));
        Assert.IsNotType<SavedSecretUnavailableException>(notReady);
        worker.LogConnectFailure(NewState(), notReady, 240);

        Assert.Equal(0, log.CountAtLevel(LogLevel.Error));
        Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
        var line = Assert.Single(log.Lines);
        Assert.Contains("retrying in 240s", line, StringComparison.Ordinal);
        Assert.Contains(DarlingPasswordKey.NotReadyReason, line, StringComparison.Ordinal);

        /* A ring that refuses, with any other reason, needs an operator. */
        var refused = Assert.Throws<SavedSecretUnavailableException>(() => DarlingSecrets.ResolvePassword(
            server, out _, UnopenableSavedPasswordConnectTests.KeyFileMissing(), WindowsDpapi()));
        worker.LogConnectFailure(NewState(), refused, 240);

        Assert.Equal(1, log.CountAtLevel(LogLevel.Error));
        Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public void A_network_failure_is_still_a_warning_that_says_it_is_retrying()
    {
        var (worker, log, _, _) = NewWorker();

        worker.LogConnectFailure(NewState(), new InvalidOperationException("connection refused"), 240);

        Assert.Equal(0, log.CountAtLevel(LogLevel.Error));
        Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
        var line = Assert.Single(log.Lines);
        Assert.Contains("retrying in 240s", line, StringComparison.Ordinal);
        Assert.DoesNotContain("will keep failing", line, StringComparison.Ordinal);
    }

    [Fact]
    public void The_unopenable_cause_logs_once_then_an_hourly_reminder_and_a_changed_cause_logs_at_once()
    {
        var (worker, log, _, advance) = NewWorker();
        var state = NewState();
        var keyFileMissing = Refusal(keyFileMissing: true);

        for (var minute = 0; minute <= 3 * 60; minute += 5)
        {
            advance(TimeSpan.FromMinutes(minute));
            worker.LogConnectFailure(state, keyFileMissing, 240);
        }

        /* One Error for the cause, then the reminder each hour: at 1, 2 and 3 hours. */
        Assert.Equal(1, log.CountAtLevel(LogLevel.Error));
        Assert.Equal(3, log.CountAtLevel(LogLevel.Warning));
        Assert.Equal(3, log.Lines.Where(line => line.Contains("Connect still failing", StringComparison.Ordinal)).Count());

        /* The key file comes back as a different key: a new cause, so a new Error at once, inside the hour. */
        advance(TimeSpan.FromMinutes(3 * 60 + 5));
        worker.LogConnectFailure(state, Refusal(keyFileMissing: false), 240);

        Assert.Equal(2, log.CountAtLevel(LogLevel.Error));
    }
}

/// <summary>
/// #9749 against a store: the configuration load keeps a server whose saved password cannot be opened, the failed connect writes the
/// named cause on the registry and no collection log row, and the reads a user looks at (collection health, the server list, the fleet
/// card) carry that cause. None of them carries the sealed value.
/// </summary>
[Collection("live-postgres")]
public sealed class UnopenableSavedPasswordConnectLiveTests
{
    private const int ServerId = 97;

    /// <summary>#9749: the configuration read carries the pin step's waiting state onto every server it reads, so the resolver's refusal for an unpinned old-format value says to wait.</summary>
    [Fact]
    public async Task The_load_carries_the_pin_step_waiting_state_to_the_resolver()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live unopenable-password test.");
        var ct = TestContext.Current.CancellationToken;
        /* #1776 own-store: this fact mints its own scratch database through ScratchPostgres. */
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        var windows = new LegacyDpapi(true, _ => UnopenableSavedPasswordConnectTests.Plain);

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await using (var seed = new NpgsqlCommand(
                "INSERT INTO config.config_monitored_servers (server_id, name, host, auth, username, encrypted_password, is_enabled) VALUES ($1, 'alpha', 'alpha-host.example.test', 'sql', 'monitor', 'AQIDBAUGBwgJCg==', TRUE)", connection))
            {
                seed.Parameters.AddWithValue(ServerId);
                await seed.ExecuteNonQueryAsync(ct);
            }

            var waiting = Assert.Single(await StoreConfigProvider.ReadMonitoredServersAsync(connection, new DarlingConfig(), ct, pinsWaiting: true));
            Assert.True(waiting.SecretPinWaiting);
            Assert.Null(waiting.SecretPin);
            var wait = Assert.Throws<InvalidOperationException>(
                () => DarlingSecrets.ResolvePassword(waiting, out _, UnopenableSavedPasswordConnectTests.AnotherKey(), windows));
            Assert.Contains("Do not enter the password again yet", wait.Message, StringComparison.Ordinal);

            /* Nothing waiting, and a reader with no worker behind it (--validate-config) leaves the flag false: the hard refusal. */
            var settled = Assert.Single(await StoreConfigProvider.ReadMonitoredServersAsync(connection, new DarlingConfig(), ct));
            Assert.False(settled.SecretPinWaiting);
            Assert.Throws<SavedSecretUnavailableException>(
                () => DarlingSecrets.ResolvePassword(settled, out _, UnopenableSavedPasswordConnectTests.AnotherKey(), windows));
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task The_server_is_kept_at_load_and_every_read_names_the_cause(bool keyFileMissing)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live unopenable-password test.");
        var ct = TestContext.Current.CancellationToken;
        /* #1776 own-store: this fact mints its own scratch database through ScratchPostgres. */
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        MonitoredServer loaded;
        string sealedText;
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await using (var seed = new NpgsqlCommand(
                "INSERT INTO config.config_monitored_servers (server_id, name, host, auth, username, is_enabled) VALUES ($1, 'alpha', 'alpha-host.example.test', 'sql', 'monitor', TRUE)", connection))
            {
                seed.Parameters.AddWithValue(ServerId);
                await seed.ExecuteNonQueryAsync(ct);
            }

            /* Sealed for the connection the loader builds from the row, then saved on the row. */
            var unsaved = Assert.Single(await StoreConfigProvider.ReadMonitoredServersAsync(connection, new DarlingConfig(), ct));
            sealedText = UnopenableSavedPasswordConnectTests.SealedFor(unsaved);
            await using (var save = new NpgsqlCommand("UPDATE config.config_monitored_servers SET encrypted_password = $2 WHERE server_id = $1", connection))
            {
                save.Parameters.AddWithValue(ServerId);
                save.Parameters.AddWithValue(sealedText);
                await save.ExecuteNonQueryAsync(ct);
            }

            /* The load keeps the server, with the value it cannot open. */
            loaded = Assert.Single(await StoreConfigProvider.ReadMonitoredServersAsync(connection, new DarlingConfig(), ct));
        }

        Assert.Equal(ServerId, loaded.ServerId);
        Assert.Equal(sealedText, loaded.EncryptedPassword);
        Assert.True(loaded.RequiresResolvedSecret);

        var refusal = Assert.Throws<SavedSecretUnavailableException>(() => DarlingSecrets.ResolvePassword(
            loaded, out _, keyFileMissing ? UnopenableSavedPasswordConnectTests.KeyFileMissing() : UnopenableSavedPasswordConnectTests.AnotherKey()));

        var state = new DarlingWorker.ServerLoopState { Config = loaded };
        await UnopenableSavedPasswordConnectTests.TryConnectAsync(
            UnopenableSavedPasswordConnectTests.NewWorker(postgres), state, new ConnectAttempt(loaded, null, refusal), ct);

        /* The registry carries the cause. */
        await using (var row = postgres.CreateCommand("SELECT connect_down_since IS NOT NULL, last_connect_error, next_connect_attempt IS NOT NULL FROM servers WHERE server_id = $1"))
        {
            row.Parameters.AddWithValue(ServerId);
            await using var reader = await row.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct), "the failed connect wrote the registry row");
            Assert.True(reader.GetBoolean(0));
            Assert.Equal(refusal.Message, reader.GetString(1));
            Assert.True(reader.GetBoolean(2));
        }

        /* The collection log holds nothing for the server: the registry, not a log row, is where a failed connect is kept (#7590). */
        await using (var log = postgres.CreateCommand("SELECT count(*) FROM v_collection_log WHERE server_id = $1"))
        {
            log.Parameters.AddWithValue(ServerId);
            Assert.Equal(0L, await log.ExecuteScalarAsync(ct));
        }

        /* get_collection_health: the connection block and the answer's own sentence carry the cause. */
        var healthJson = await DarlingMcpDataTools.GetCollectionHealthUncut(postgres, "alpha", ct);
        using (var health = JsonDocument.Parse(healthJson))
        {
            var root = health.RootElement;
            Assert.Equal("waiting_on_connection", root.GetProperty("server_connection").GetProperty("status").GetString());
            Assert.Equal(refusal.Message, root.GetProperty("server_connection").GetProperty("last_connect_error").GetString());
            Assert.Contains(refusal.Message, root.GetProperty("message").GetString(), StringComparison.Ordinal);
        }

        /* list_servers: Offline, with the cause as the reason. */
        var listJson = await DarlingMcpDataTools.ListServers(postgres, ct);
        using (var list = JsonDocument.Parse(listJson))
        {
            var server = list.RootElement.GetProperty("servers")[0];
            Assert.Equal("Offline", server.GetProperty("status").GetString());
            Assert.Contains(refusal.Message, server.GetProperty("reason").GetString(), StringComparison.Ordinal);
        }

        /* The fleet card behind get_fleet_overview and the web dashboard: Offline, the cause in its reason and in last_connect_error. */
        var now = DateTime.UtcNow;
        var fleet = await DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-1), now, now, cancellationToken: ct);
        var card = Assert.Single(fleet.Cards);
        Assert.Equal("Offline", card.Status);
        Assert.Equal(refusal.Message, card.LastConnectError);
        var reason = DarlingFleetReader.BuildReason(card);
        Assert.Contains(refusal.Message, reason, StringComparison.Ordinal);

        foreach (var text in new[] { refusal.Message, healthJson, listJson, reason })
        {
            Assert.DoesNotContain(sealedText, text, StringComparison.Ordinal);
            Assert.DoesNotContain(UnopenableSavedPasswordConnectTests.Plain, text, StringComparison.Ordinal);
        }
    }
}
