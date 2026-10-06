/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The key <c>--add-server</c> seals a password with (#5366). The verb runs beside the service on the same host, so it
/// reads the key file the service keeps (read only, never generating or changing it) and uses it when it opens and its
/// public half is the key the store publishes. When the file cannot be read it seals to the key the store publishes,
/// provided the published key id is the one its bytes give and the service's newest state row says the key is healthy;
/// it then prints the key id so the operator can check it against the service log.
/// </summary>
internal static class DarlingCliSealKey
{
    /// <summary>What the key source decided: a ring to seal with, or the sentence to refuse a literal password with.</summary>
    internal sealed record Decision(IPasswordKeyRing Ring, string? Notice);

    /// <summary>The sentence a published-only seal prints after it has sealed a password.</summary>
    internal static string NoticeFor(string keyId) =>
        $"Sealed to password key {PasswordSeal.DisplayKeyId(keyId)}. It must match the 'Password key' line in the service log.";

    internal const string NoTablesReason = "This store has no password key tables yet. Update or restart the Darling service, then try again.";
    internal const string NoKeyReason = "The store has no published password key yet. Start the Darling service once, then try again.";
    internal const string InvalidKeyReason = "The password key the store publishes is not valid, so no password can be sealed to it.";
    internal const string NoStateReason = "The Darling service has not reported the state of its password key yet. Start the service, then try again.";
    internal const string FileMismatchReason =
        "The password key on this host is not the key the store publishes. Check the 'Password key' line in the service log.";

    internal static string StateReason(PasswordKeyServiceState state)
    {
        var note = string.IsNullOrWhiteSpace(state.Note) ? "" : " " + state.Note.Trim();
        return $"The Darling service reports its password key as {state.State}.{note}";
    }

    /// <summary>
    /// Reads the key file (when the directory can be resolved) and the store's published key and state on the owner
    /// connection, then <see cref="Decide"/>. Never throws for anything the file system or the store can do: the answer is a
    /// refusing ring with the reason.
    /// </summary>
    internal static async Task<Decision> ResolveAsync(
        DarlingConfig config, string? configPath, NpgsqlDataSource postgres, ILogger? logger, CancellationToken ct)
    {
        PublishedPasswordKey? published;
        PasswordKeyServiceState? state;
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(ct).ConfigureAwait(false);
            published = await PasswordKeyTables.ReadCurrentAsync(connection, ct).ConfigureAwait(false);
            state = await PasswordKeyTables.ReadNewestServiceStateAsync(connection, ct).ConfigureAwait(false);
        }
        catch (PostgresException ex) when (ex.SqlState == "42P01")
        {
            return Refused(NoTablesReason);
        }

        byte[]? pkcs8 = null;
        try
        {
            try
            {
                if (configPath is not null)
                {
                    var directory = DarlingLogHashKeyFile.DirectoryFor(config, configPath);
                    var load = DarlingPasswordKeyFile.Load(directory, logger ?? NullLogger.Instance);
                    pkcs8 = load.Pkcs8;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                pkcs8 = null;
            }

            return Decide(published, state, pkcs8);
        }
        finally
        {
            if (pkcs8 is not null)
            {
                CryptographicOperations.ZeroMemory(pkcs8);
            }
        }
    }

    /// <summary>
    /// PURE: the rule over what was read. A key file that opens is used when its public half equals the published key (a
    /// different one is refused); with no usable file the published key is used when its id is the one its bytes give and
    /// the service's newest state row is <c>ok</c>.
    /// </summary>
    internal static Decision Decide(PublishedPasswordKey? published, PasswordKeyServiceState? state, byte[]? filePkcs8)
    {
        if (published is null)
        {
            return Refused(NoKeyReason);
        }

        PasswordPublicKey publicKey;
        try
        {
            if (!string.Equals(PasswordSeal.KeyIdFor(published.Spki), published.KeyId, StringComparison.Ordinal))
            {
                return Refused(InvalidKeyReason);
            }

            publicKey = PasswordPublicKey.FromSpki(published.Spki);
        }
        catch (PasswordSealException)
        {
            return Refused(InvalidKeyReason);
        }

        if (filePkcs8 is not null)
        {
            try
            {
                var fileKey = PasswordPrivateKey.FromPkcs8(filePkcs8);
                if (!fileKey.PublicKey.Spki.AsSpan().SequenceEqual(published.Spki))
                {
                    fileKey.Dispose();
                    return Refused(FileMismatchReason);
                }

                return new Decision(DarlingPasswordKey.FromPrivateKey(fileKey), null);
            }
            catch (Exception ex) when (ex is PasswordSealException or CryptographicException or ArgumentException)
            {
                /* A file that cannot be parsed is the same as no file here: the published key is judged on its own. */
            }
        }

        if (state is null)
        {
            return Refused(NoStateReason);
        }

        if (!string.Equals(state.State, "ok", StringComparison.Ordinal))
        {
            return Refused(StateReason(state));
        }

        /* #5366: the state row must be about the key that is published now; a row that names another key says
           nothing about this one. */
        if (!string.Equals(state.KeyId, published.KeyId, StringComparison.Ordinal))
        {
            return Refused(NoStateReason);
        }

        return new Decision(new PublishedKeyRing(publicKey), NoticeFor(publicKey.KeyId));
    }

    /// <summary>
    /// The ring a diagnostics bundle opens sealed webhook values with when the service is not the one making it: the key
    /// file read only (never generated or changed), or null when there is no directory, no file, or the file cannot be
    /// read. The in-process ring is used instead when it holds a key.
    /// </summary>
    internal static IPasswordKeyRing? TryOpenFileRing(DarlingConfig config, string? configPath, ILogger? logger)
    {
        if (DarlingPasswordKey.Current.Status.CanSeal)
        {
            return DarlingPasswordKey.Current;
        }

        byte[]? pkcs8 = null;
        try
        {
            if (configPath is null)
            {
                return null;
            }

            var directory = DarlingLogHashKeyFile.DirectoryFor(config, configPath);
            pkcs8 = DarlingPasswordKeyFile.Load(directory, logger ?? NullLogger.Instance).Pkcs8;
            return pkcs8 is null ? null : DarlingPasswordKey.FromPrivateKey(PasswordPrivateKey.FromPkcs8(pkcs8));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
        finally
        {
            if (pkcs8 is not null)
            {
                CryptographicOperations.ZeroMemory(pkcs8);
            }
        }
    }

    private static Decision Refused(string reason) => new(DarlingPasswordKey.Refusing(reason), null);

    /// <summary>A ring that seals to a public key and cannot open anything. Counts what it sealed, so the verb prints
    /// the key id only when a password was sealed.</summary>
    internal sealed class PublishedKeyRing : IPasswordKeyRing
    {
        private readonly PasswordPublicKey _key;
        private int _sealed;

        public PublishedKeyRing(PasswordPublicKey key)
        {
            _key = key;
            Status = new PasswordKeyStatus(true, null, key.KeyId, key);
        }

        public PasswordKeyStatus Status { get; }

        /// <summary>How many passwords this ring has sealed.</summary>
        public int SealedCount => Volatile.Read(ref _sealed);

        public string Seal(string plaintext, PasswordBinding binding)
        {
            var text = PasswordSeal.Seal(plaintext, _key, binding);
            Interlocked.Increment(ref _sealed);
            return text;
        }

        public string Open(string sealedText, PasswordBinding binding) =>
            throw new PasswordSealException(PasswordSealFailure.UnknownKey, PasswordSeal.KeyIdOf(sealedText ?? ""));
    }
}
