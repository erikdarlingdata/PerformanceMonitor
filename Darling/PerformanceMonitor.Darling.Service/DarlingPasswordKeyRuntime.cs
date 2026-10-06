/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// What a caller may replace when it starts the password key runtime. Production passes none of them; tests use them to
/// point the runtime at a temporary directory, to stand in for the platform, and to see the ring the start produced
/// without touching the process-wide <see cref="DarlingPasswordKey.Current"/>.
/// </summary>
internal sealed record PasswordKeyStartOptions
{
    /// <summary>The platform answer for the legacy pin snapshot. Null means this machine.</summary>
    public bool? IsWindows { get; init; }

    /// <summary>Opens a legacy value. Null means <see cref="DarlingSecrets.Unprotect"/> on Windows.</summary>
    public Func<string, string?>? Unprotect { get; init; }

    /// <summary>Judges a key kept from an open directory against the published key. Null means <see cref="DarlingPasswordKeyRuntime.SelfTest"/>.</summary>
    public Func<PasswordPrivateKey, PublishedKey, bool>? SelfTest { get; init; }

    /// <summary>Receives each ring the runtime decides on. Null means set <see cref="DarlingPasswordKey.Current"/>.</summary>
    public Action<IPasswordKeyRing>? SetRing { get; init; }

    /// <summary>This service host's name in the state table. Null means the machine name.</summary>
    public string? ServiceHost { get; init; }
}

/// <summary>
/// The service's password key at run time (#5366). At start it loads the key file, compares it with the key the store
/// publishes, acts on <see cref="DarlingPasswordKeyState.Decide"/> (use, generate, publish, retire), records the result
/// in the store, and sets the ring every writer and the resolver seal and open through. On every sweep it checks that
/// the store still publishes the key the service holds and that the key tables still have all their protection.
/// </summary>
internal sealed class DarlingPasswordKeyRuntime
{
    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger _logger;
    private readonly Action<IPasswordKeyRing> _setRing;
    private readonly string _serviceHost;
    private readonly IPasswordKeyRing? _held;
    private readonly byte[]? _heldSpki;
    private readonly string? _heldKeyId;
    private string? _sweepState;
    private bool _sweepReadFailed;

    private DarlingPasswordKeyRuntime(
        NpgsqlDataSource postgres, ILogger logger, Action<IPasswordKeyRing> setRing, string serviceHost,
        IPasswordKeyRing ring, IPasswordKeyRing? held, byte[]? heldSpki, string? heldKeyId, string state, string? note)
    {
        _postgres = postgres;
        _logger = logger;
        _setRing = setRing;
        _serviceHost = serviceHost;
        Ring = ring;
        _held = held;
        _heldSpki = heldSpki;
        _heldKeyId = heldKeyId;
        State = state;
        Note = note;
    }

    /// <summary>The ring this start decided on: keyed when the key could be used, otherwise refusing with the reason.</summary>
    public IPasswordKeyRing Ring { get; }

    /// <summary>The state written for this host: <c>ok</c>, <c>missing</c>, <c>mismatch</c> or <c>refused</c>.</summary>
    public string State { get; }

    /// <summary>The note written with <see cref="State"/>: the refusal reason, or the open-directory warning, or null.</summary>
    public string? Note { get; }

    /// <summary>Starts the runtime for the service: resolves the directory beside the other credentials, then
    /// <see cref="StartAsync(string, NpgsqlDataSource, ILogger, CancellationToken, PasswordKeyStartOptions?)"/>.</summary>
    public static async Task<DarlingPasswordKeyRuntime> StartForServiceAsync(
        DarlingConfig config, string configPath, NpgsqlDataSource postgres, ILogger logger, CancellationToken ct)
    {
        string directory;
        try
        {
            directory = DarlingLogHashKeyFile.DirectoryFor(config, configPath);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var reason = $"The directory for the service's password key could not be resolved ({ex.Message}).";
            logger.LogError("{Reason}", reason);
            var ring = DarlingPasswordKey.Refusing(reason);
            DarlingPasswordKey.Current = ring;
            return new DarlingPasswordKeyRuntime(
                postgres, logger, r => DarlingPasswordKey.Current = r, Environment.MachineName, ring, null, null, null, "refused", reason);
        }

        return await StartAsync(directory, postgres, logger, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Loads, decides and acts. Never throws for anything the store, the file system or the key can do: the result is a
    /// refusing ring with the reason, and the same reason is logged. Runs as the store owner.
    /// </summary>
    public static async Task<DarlingPasswordKeyRuntime> StartAsync(
        string directory, NpgsqlDataSource postgres, ILogger logger, CancellationToken ct, PasswordKeyStartOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(directory);
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(logger);

        options ??= new PasswordKeyStartOptions();
        var setRing = options.SetRing ?? (r => DarlingPasswordKey.Current = r);
        var host = options.ServiceHost ?? Environment.MachineName;
        var selfTest = options.SelfTest ?? SelfTest;

        PasswordPrivateKey? held = null;
        try
        {
            var outcome = await DecideAndActAsync(directory, postgres, logger, selfTest, host, ct).ConfigureAwait(false);
            held = outcome.Key;
            IPasswordKeyRing ring;
            if (held is not null && outcome.State == "ok")
            {
                ring = DarlingPasswordKey.FromPrivateKey(held);
            }
            else
            {
                ring = DarlingPasswordKey.Refusing(outcome.Note ?? "The service's password key cannot be used.");
                held?.Dispose();
                held = null;
            }

            setRing(ring);
            await WriteStateAsync(postgres, logger, host, outcome.KeyId, outcome.State, outcome.Note, ct).ConfigureAwait(false);
            LogOutcome(logger, outcome, directory);

            var runtime = new DarlingPasswordKeyRuntime(
                postgres, logger, setRing, host, ring, held is null ? null : ring, held?.PublicKey.Spki, held?.PublicKey.KeyId,
                outcome.State, outcome.Note);
            await SnapshotPinsAsync(postgres, logger, options, outcome.TriggersOk, ct).ConfigureAwait(false);
            return runtime;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            held?.Dispose();
            var reason = $"The service could not load its password key ({ex.Message}). Saved passwords cannot be used or added until it can.";
            logger.LogError("{Reason}", reason);
            var ring = DarlingPasswordKey.Refusing(reason);
            setRing(ring);
            await WriteStateAsync(postgres, logger, host, null, "refused", reason, ct).ConfigureAwait(false);
            return new DarlingPasswordKeyRuntime(postgres, logger, setRing, host, ring, null, null, null, "refused", reason);
        }
    }

    /// <summary>
    /// Seals a random value to the published key and opens it with the key from the file. True only when the value comes
    /// back: the file's private half really belongs to the key the store publishes.
    /// </summary>
    internal static bool SelfTest(PasswordPrivateKey fileKey, PublishedKey published)
    {
        ArgumentNullException.ThrowIfNull(fileKey);
        ArgumentNullException.ThrowIfNull(published);
        try
        {
            var publicKey = PasswordPublicKey.FromSpki(published.Spki);
            var value = Convert.ToBase64String(RandomNumberGenerator.GetBytes(32));
            var binding = PasswordBinding.ForSmtp("self-test.example", 25, false, "self-test");
            var sealedText = PasswordSeal.Seal(value, publicKey, binding);
            return string.Equals(PasswordSeal.Open(sealedText, fileKey, binding), value, StringComparison.Ordinal);
        }
        catch (Exception ex) when (ex is PasswordSealException or CryptographicException)
        {
            return false;
        }
    }

    /// <summary>
    /// The per-sweep check: one read of the current published key and the key tables' triggers. A key that no longer
    /// matches the one this service holds, no published key at all (a reset), or a trigger that is missing, extra or not
    /// enabled in every session state turns the ring to refusing and records <c>mismatch</c>, <c>reset_pending</c> or
    /// <c>refused</c> once. When the store is as expected again, the ring the start loaded is put back. Does nothing when
    /// the start did not load a key. Never throws.
    /// </summary>
    public async Task SweepCheckAsync(CancellationToken ct)
    {
        if (_held is null || _heldSpki is null)
        {
            return;
        }

        try
        {
            await using var c = await _postgres.OpenConnectionAsync(ct).ConfigureAwait(false);
            var triggerProblem = await DarlingPasswordKeyStore.CheckTriggersAsync(c, ct).ConfigureAwait(false);
            PublishedKey? current = null;
            if (triggerProblem is null)
            {
                current = await DarlingPasswordKeyStore.ReadCurrentAsync(c, ServiceCommandDeadlines.SerialLoopSeconds, ct).ConfigureAwait(false);
            }

            string? problem = null;
            var state = "ok";
            if (triggerProblem is not null)
            {
                problem = triggerProblem;
                state = "refused";
            }
            else if (current is null)
            {
                problem = "The password key was reset. Restart the service: it will make a new password key, and the saved passwords must be entered again.";
                state = "reset_pending";
            }
            else if (!current.Spki.AsSpan().SequenceEqual(_heldSpki))
            {
                problem = $"The store now publishes the password key {PasswordSeal.DisplayKeyId(PasswordSeal.KeyIdFor(current.Spki))}, not the key this service holds " +
                    $"({PasswordSeal.DisplayKeyId(_heldKeyId ?? "")}). Restart the service, or restore the credentials volume that holds the key.";
                state = "mismatch";
            }

            _sweepReadFailed = false;
            if (problem is not null && !string.Equals(_sweepState, state, StringComparison.Ordinal))
            {
                _sweepState = state;
                _setRing(DarlingPasswordKey.Refusing(problem));
                _logger.LogError("Password key check: {Problem}", problem);
                await DarlingPasswordKeyStore.WriteServiceStateAsync(
                    c, _serviceHost, _heldKeyId, state, problem, ServiceCommandDeadlines.SerialLoopSeconds, ct).ConfigureAwait(false);
            }
            else if (problem is null && _sweepState is not null)
            {
                _sweepState = null;
                _setRing(_held);
                _logger.LogInformation("Password key check: the store publishes this service's key again, so saved passwords are usable.");
                await DarlingPasswordKeyStore.WriteServiceStateAsync(
                    c, _serviceHost, _heldKeyId, "ok", null, ServiceCommandDeadlines.SerialLoopSeconds, ct).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A store that cannot be read says nothing about the key: leave the ring as it is, and say so once.
            if (!_sweepReadFailed)
            {
                _sweepReadFailed = true;
                _logger.LogWarning("Password key check could not read the store: {Message}", ex.Message);
            }
        }
    }

    private sealed record Outcome(PasswordPrivateKey? Key, string State, string? Note, string? KeyId, bool Generated, bool TriggersOk, string? Warning);

    private static async Task<Outcome> DecideAndActAsync(
        string directory, NpgsqlDataSource postgres, ILogger logger, Func<PasswordPrivateKey, PublishedKey, bool> selfTest,
        string host, CancellationToken ct)
    {
        await using var c = await postgres.OpenConnectionAsync(ct).ConfigureAwait(false);

        var triggerProblem = await DarlingPasswordKeyStore.CheckTriggersAsync(c, ct).ConfigureAwait(false);
        if (triggerProblem is not null)
        {
            return new Outcome(null, "refused", triggerProblem, null, false, false, null);
        }

        await DarlingPasswordKeyStore.WriteServiceStateAsync(
            c, host, null, "loading", null, ServiceCommandDeadlines.BootstrapSeconds, ct).ConfigureAwait(false);

        var load = DarlingPasswordKeyFile.Load(directory, logger);
        PasswordPrivateKey? fileKey = null;
        string? unusable = null;
        if (load.Untrusted)
        {
            unusable = load.Refusal ?? "The password key file cannot be used.";
        }
        else if (load.Present)
        {
            if (load.Pkcs8 is null)
            {
                unusable = load.Refusal ?? "The password key file could not be read.";
            }
            else
            {
                try
                {
                    fileKey = PasswordPrivateKey.FromPkcs8(load.Pkcs8);
                }
                catch (PasswordSealException)
                {
                    unusable = $"The password key file in {directory} is not a usable RSA-3072 key.";
                }
                finally
                {
                    CryptographicOperations.ZeroMemory(load.Pkcs8);
                }
            }
        }

        var ownsFileKey = true;
        try
        {
            var storeCurrent = await DarlingPasswordKeyStore.ReadCurrentAsync(c, ServiceCommandDeadlines.BootstrapSeconds, ct).ConfigureAwait(false);
            var fileKeyId = fileKey?.PublicKey.KeyId;
            var retiredInStore = fileKeyId is not null
                && await DarlingPasswordKeyStore.IsReplacedAsync(c, fileKeyId, ct).ConfigureAwait(false);
            var facts = new PasswordKeyFacts(
                FilePresent: load.Present,
                FileUntrusted: unusable is not null,
                FileRefusal: unusable,
                FileSpki: fileKey?.PublicKey.Spki,
                FoundAfterOpenDirectory: load.FoundAfterOpenDirectory,
                StoreCurrent: storeCurrent,
                FileKeyRetiredInStore: retiredInStore);
            var decision = DarlingPasswordKeyState.Decide(facts);

            if (decision.RetireFileFirst)
            {
                try
                {
                    DarlingPasswordKeyFile.Retire(directory, decision.RetireAs!, logger);
                }
                catch (Exception ex) when (ex is IOException or ArgumentException)
                {
                    return new Outcome(null, "refused", ex.Message, null, false, true, null);
                }

                fileKey?.Dispose();
                fileKey = null;
            }

            switch (decision.Action)
            {
                case PasswordKeyAction.Refused:
                    return new Outcome(null, "refused", decision.Reason, null, false, true, null);

                case PasswordKeyAction.Mismatch:
                    return new Outcome(null, "mismatch", decision.Reason, fileKey?.PublicKey.KeyId, false, true, null);

                case PasswordKeyAction.Missing:
                    return new Outcome(null, "missing", decision.Reason, null, false, true, null);

                case PasswordKeyAction.Use:
                {
                    if (load.FoundAfterOpenDirectory)
                    {
                        // The file was kept from an open directory: it counts only when its private half opens a value
                        // sealed to the published key, and only then does it go back to the live name.
                        if (!selfTest(fileKey!, storeCurrent!))
                        {
                            var reason = "The password key file kept from the credentials directory has the published key's id but cannot open a value sealed to it, so it was not used. " +
                                "Restore the credentials volume that holds the key, or run --reset-password-key and enter the saved passwords again.";
                            return new Outcome(null, "mismatch", reason, fileKey!.PublicKey.KeyId, false, true, null);
                        }

                        try
                        {
                            DarlingPasswordKeyFile.Accept(directory, logger);
                        }
                        catch (Exception ex) when (ex is IOException or InvalidOperationException)
                        {
                            return new Outcome(null, "refused", ex.Message, null, false, true, null);
                        }
                    }

                    ownsFileKey = false;
                    return new Outcome(fileKey, "ok", decision.Warning, fileKey!.PublicKey.KeyId, false, true, decision.Warning);
                }

                case PasswordKeyAction.PublishFile:
                {
                    var published = await TryPublishAsync(c, fileKey!, ct).ConfigureAwait(false);
                    if (published is not null)
                    {
                        return new Outcome(null, "mismatch", published, fileKey!.PublicKey.KeyId, false, true, null);
                    }

                    ownsFileKey = false;
                    return new Outcome(fileKey, "ok", null, fileKey!.PublicKey.KeyId, false, true, null);
                }

                case PasswordKeyAction.Generate:
                {
                    var made = PasswordPrivateKey.Generate();
                    var written = DarlingPasswordKeyFile.Generate(directory, made.ExportPkcs8, logger);
                    if (written.Pkcs8 is not null)
                    {
                        CryptographicOperations.ZeroMemory(written.Pkcs8);
                    }

                    if (!written.Present || written.Pkcs8 is null)
                    {
                        made.Dispose();
                        return new Outcome(null, "refused", written.Refusal ?? "The password key file could not be written.", null, false, true, null);
                    }

                    var conflict = await TryPublishAsync(c, made, ct).ConfigureAwait(false);
                    if (conflict is not null)
                    {
                        made.Dispose();
                        return new Outcome(null, "mismatch", conflict, null, false, true, null);
                    }

                    return new Outcome(made, "ok", null, made.PublicKey.KeyId, true, true, null);
                }

                default:
                    return new Outcome(null, "refused", "The service's password key cannot be used.", null, false, true, null);
            }
        }
        finally
        {
            if (ownsFileKey)
            {
                fileKey?.Dispose();
            }
        }
    }

    // Null when the key was published; otherwise the sentence for the case where another key became current first.
    private static async Task<string?> TryPublishAsync(NpgsqlConnection c, PasswordPrivateKey key, CancellationToken ct)
    {
        try
        {
            await DarlingPasswordKeyStore.PublishAsync(c, key.PublicKey.KeyId, key.PublicKey.Spki, ct).ConfigureAwait(false);
            return null;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            return "Another service published a password key at the same moment. Restart the service so it uses that key.";
        }
    }

    private static async Task WriteStateAsync(
        NpgsqlDataSource postgres, ILogger logger, string host, string? keyId, string state, string? note, CancellationToken ct)
    {
        try
        {
            await using var c = await postgres.OpenConnectionAsync(ct).ConfigureAwait(false);
            await DarlingPasswordKeyStore.WriteServiceStateAsync(
                c, host, keyId, state, note, ServiceCommandDeadlines.BootstrapSeconds, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning("The password key's state could not be recorded in the store: {Message}", ex.Message);
        }
    }

    private static void LogOutcome(ILogger logger, Outcome outcome, string directory)
    {
        if (outcome.State == "ok" && outcome.KeyId is not null)
        {
            var id = PasswordSeal.DisplayKeyId(outcome.KeyId);
            if (outcome.Generated)
            {
                logger.LogInformation("Password key {KeyId} generated in {Directory}", id, directory);
            }
            else
            {
                logger.LogInformation("Password key {KeyId} loaded from {Directory}", id, directory);
            }

            if (outcome.Warning is not null)
            {
                logger.LogWarning("{Warning}", outcome.Warning);
            }

            return;
        }

        logger.LogError("Password key not loaded ({State}): {Reason}", outcome.State, outcome.Note);
    }

    private static async Task SnapshotPinsAsync(
        NpgsqlDataSource postgres, ILogger logger, PasswordKeyStartOptions options, bool triggersOk, CancellationToken ct)
    {
        if (!triggersOk)
        {
            return;
        }

        try
        {
            var windows = options.IsWindows ?? OperatingSystem.IsWindows();
            var unprotect = options.Unprotect ?? WindowsUnprotect;
            var snapshot = await DarlingPasswordKeyStore.SnapshotLegacyPinsAsync(postgres, windows, unprotect, logger, ct).ConfigureAwait(false);
            if (windows && string.Equals(snapshot.MarkerState, "done", StringComparison.Ordinal))
            {
                logger.LogInformation("Legacy password pins: {Pinned} saved password(s) pinned to their connection.", snapshot.Pinned);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning("The legacy password pin snapshot could not run, and will be tried again at the next start: {Message}", ex.Message);
        }
    }

    private static string? WindowsUnprotect(string stored) =>
        OperatingSystem.IsWindows() ? DarlingSecrets.Unprotect(stored) : null;
}
