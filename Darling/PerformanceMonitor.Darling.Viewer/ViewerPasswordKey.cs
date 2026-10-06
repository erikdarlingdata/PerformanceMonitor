/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>A password the viewer cannot save, with a sentence that says why and never carries the value.</summary>
public sealed class ViewerPasswordRefusedException(string message) : Exception(message);

/// <summary>
/// Seals a server password to the service's published key, for the connection settings of one store row (#5366). The
/// sealed text is readable only by the service, and only for those settings.
/// </summary>
public sealed class ViewerPasswordSealer(PasswordPublicKey key)
{
    private static readonly UTF8Encoding s_strictUtf8 = new(false, true);

    /// <summary>The refusal for a password that is not valid text (a lone surrogate).</summary>
    public const string PasswordCharactersText = "The password contains characters that cannot be stored.";

    /// <summary>The refusal for a password longer than a sealed value can hold.</summary>
    public const string PasswordTooLongText = "The password is longer than can be stored.";

    /// <summary>The key id the passwords are sealed to.</summary>
    public string KeyId => key.KeyId;

    /// <summary>The refusal for a connection field that is not valid text, naming the field.</summary>
    public static string FieldCharactersText(string field) => $"The {field} contains characters that cannot be stored.";

    /// <summary>The connection settings of a row, mapped the one way every reader maps a stored row.</summary>
    public static ServerConnectionIdentity IdentityOf(MonitoredServerRow row) =>
        ServerConnectionIdentity.FromStoredColumns(
            row.Host, row.Port, row.Engine, row.Database, row.ReadOnlyIntent, row.Auth, row.Username, row.EncryptMode,
            row.TrustServerCertificate, row.MultiSubnetFailover);

    /// <summary>The binding a row's password is sealed for.</summary>
    public static PasswordBinding BindingOf(MonitoredServerRow row)
    {
        var identity = IdentityOf(row);
        foreach (var (name, text) in new (string, string?)[]
                 {
                     ("server name", identity.Host), ("database name", identity.Database), ("user name", identity.Username),
                     ("authentication", identity.Auth), ("encryption mode", identity.EncryptMode), ("engine", identity.Engine),
                 })
        {
            if (!IsValidText(text))
            {
                throw new ViewerPasswordRefusedException(FieldCharactersText(name));
            }
        }

        try
        {
            return PasswordBinding.ForServer(identity);
        }
        catch (ArgumentException)
        {
            throw new ViewerPasswordRefusedException(FieldCharactersText("connection settings"));
        }
    }

    /// <summary>Seals <paramref name="plaintext"/> for <paramref name="row"/>'s connection settings. Throws
    /// <see cref="ViewerPasswordRefusedException"/> when the password or a connection field is not valid text.</summary>
    public string Seal(string plaintext, MonitoredServerRow row)
    {
        ArgumentNullException.ThrowIfNull(plaintext);
        ArgumentNullException.ThrowIfNull(row);

        var binding = BindingOf(row);
        if (!IsValidText(plaintext))
        {
            throw new ViewerPasswordRefusedException(PasswordCharactersText);
        }

        try
        {
            return PasswordSeal.Seal(plaintext, key, binding);
        }
        catch (PasswordSealException)
        {
            throw new ViewerPasswordRefusedException(
                s_strictUtf8.GetByteCount(plaintext) > PasswordSeal.MaxPlaintextBytes ? PasswordTooLongText : PasswordCharactersText);
        }
    }

    internal static bool IsValidText(string? text)
    {
        if (text is null)
        {
            return true;
        }

        try
        {
            _ = s_strictUtf8.GetByteCount(text);
            return true;
        }
        catch (EncoderFallbackException)
        {
            return false;
        }
    }
}

/// <summary>
/// Keeps the last sealed value of a dialog so a connection test and the save that follows it store the same text: the
/// password is sealed once for its settings, and sealed again only when the password, the settings or the key change.
/// Holds a hash of the password, never the password.
/// </summary>
public sealed class ViewerSealCache
{
    private string? _signature;
    private string? _sealed;

    public string GetOrSeal(ViewerPasswordSealer sealer, string plaintext, MonitoredServerRow row)
    {
        var binding = ViewerPasswordSealer.BindingOf(row);
        /* Checked before the cache key: the key hashes the text as UTF-8, which turns a lone surrogate into the same bytes as
           the replacement character, so a cached value must never answer for text that cannot be stored. */
        if (!ViewerPasswordSealer.IsValidText(plaintext))
        {
            throw new ViewerPasswordRefusedException(ViewerPasswordSealer.PasswordCharactersText);
        }

        var signature = sealer.KeyId + "|" + string.Join('\u001f', binding.Fields) + "|"
            + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(plaintext)));
        if (_signature == signature && _sealed is not null)
        {
            return _sealed;
        }

        var sealedText = sealer.Seal(plaintext, row);
        _signature = signature;
        _sealed = sealedText;
        return sealedText;
    }
}

/// <summary>What the viewer found when it asked for the key to seal with: a sealer, or the sentence that says why not.</summary>
public sealed class ViewerSealKeyResult
{
    public ViewerPasswordSealer? Sealer { get; init; }

    /// <summary>Why no password can be saved, or null when <see cref="Sealer"/> is set.</summary>
    public string? Refusal { get; init; }

    /// <summary>A sentence to show once, on the first connect to a store or when the saved keys could not be read.</summary>
    public string? Notice { get; init; }
}

/// <summary>The published key weighed against the service state and the key this viewer saw before.</summary>
public sealed record ViewerPasswordKeyDecision(
    PasswordPublicKey? Key, string? Refusal, string? Notice, ViewerPasswordKeyChange? Change);

/// <summary>A published key that differs from the saved one, with what the trust dialog shows.</summary>
public sealed record ViewerPasswordKeyChange(
    string SavedDisplay, string NewDisplay, PasswordPublicKey NewKey, string SavedFingerprint = "", string NewFingerprint = "");

/// <summary>
/// The viewer's read of the key it seals passwords with (#5366). A password is sealed only when the store publishes a
/// current key whose id is the id of its own public part, the service reports its key as <c>ok</c>, and the key is the
/// one this viewer saved for the store (or the operator trusted the new one).
/// </summary>
public static class ViewerPasswordKey
{
    public const string NoKeyText =
        "Passwords cannot be saved: the Darling service has not published a password key for this store yet. Start the service, then try again.";

    public const string InvalidKeyText =
        "Passwords cannot be saved: the password key this store publishes is not valid.";

    public const string NoStateText =
        "Passwords cannot be saved: the Darling service has not reported the state of its password key yet.";

    public const string ReadOnlyText = "Passwords cannot be saved from a read-only connection.";

    public const string NoTablesText =
        "Passwords cannot be saved: this store has no password key tables yet. Update or restart the Darling service.";

    public const string PinsNotSavedText =
        "The service's password key could not be saved on this computer, so passwords cannot be stored from here. "
        + "Check that the Viewer can write to its settings folder.";

    public const string UnreadablePinsText =
        "The saved list of password keys could not be read, so this store's key was saved again.";

    /// <summary>The store connection a saved key belongs to: host (lower case, trimmed), port (5432 when not given) and database.</summary>
    public static string StoreIdentityOf(string connectionString)
    {
        try
        {
            var builder = new NpgsqlConnectionStringBuilder(connectionString);
            var port = builder.Port > 0 ? builder.Port : 5432;
            return $"{(builder.Host ?? "").Trim().ToLowerInvariant()}:{port}/{builder.Database ?? ""}";
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException)
        {
            return "unknown";
        }
    }

    /// <summary>The sentence shown once when a store's key is saved for the first time.</summary>
    public static string FirstConnectNotice(string displayKeyId) =>
        $"This store's password key is {displayKeyId}. It should match the 'Password key' line in the service log.";

    /// <summary>The sentence a save of a password gives when the store's key is not the one saved for it.</summary>
    public static string ChangedText(string savedDisplay, string newDisplay) =>
        $"Passwords cannot be saved: this store's password key changed from {savedDisplay} to {newDisplay}. "
        + "If the service's password key was reset on purpose, choose Trust the new key. "
        + "If it was not, check it against the 'Password key' line in the service log before entering any passwords.";

    /// <summary>
    /// Weighs the published key and the service state, then the saved key for <paramref name="storeId"/>. With no saved
    /// key the new one is saved and the first-connect sentence is returned as the notice. With a different saved key
    /// nothing is saved: <see cref="ViewerPasswordKeyDecision.Change"/> says what to ask the operator.
    /// </summary>
    public static ViewerPasswordKeyDecision Evaluate(
        PublishedPasswordKey? key, PasswordKeyServiceState? state, string storeId, ViewerPasswordKeyPins pins)
    {
        ArgumentNullException.ThrowIfNull(pins);

        if (key is null)
        {
            return Refused(NoKeyText);
        }

        PasswordPublicKey publicKey;
        try
        {
            if (!string.Equals(PasswordSeal.KeyIdFor(key.Spki), key.KeyId, StringComparison.Ordinal))
            {
                return Refused(InvalidKeyText);
            }

            publicKey = PasswordPublicKey.FromSpki(key.Spki);
        }
        catch (PasswordSealException)
        {
            return Refused(InvalidKeyText);
        }

        if (state is null)
        {
            return Refused(NoStateText);
        }

        if (!string.Equals(state.State, "ok", StringComparison.Ordinal))
        {
            var note = string.IsNullOrWhiteSpace(state.Note) ? "" : " " + state.Note.Trim();
            return Refused($"Passwords cannot be saved: the Darling service reports its password key as {state.State}.{note}");
        }

        /* An ok state counts only when it is for the key that is published now. */
        if (!string.Equals(state.KeyId, key.KeyId, StringComparison.Ordinal))
        {
            return Refused(NoStateText);
        }

        var saved = pins.Find(storeId, out var unreadable);
        var fingerprint = Convert.ToHexString(publicKey.Fingerprint).ToLowerInvariant();
        if (saved is null)
        {
            if (!pins.Save(storeId, publicKey.Fingerprint, publicKey.KeyId))
            {
                return Refused(PinsNotSavedText);
            }

            var notice = FirstConnectNotice(PasswordSeal.DisplayKeyId(publicKey.KeyId));
            return new ViewerPasswordKeyDecision(publicKey, null, unreadable ? UnreadablePinsText + " " + notice : notice, null);
        }

        if (string.Equals(saved, fingerprint, StringComparison.Ordinal))
        {
            return new ViewerPasswordKeyDecision(publicKey, null, null, null);
        }

        var savedDisplay = PasswordSeal.DisplayKeyId(saved[..16]);
        var newDisplay = PasswordSeal.DisplayKeyId(publicKey.KeyId);
        return new ViewerPasswordKeyDecision(
            null, ChangedText(savedDisplay, newDisplay), null,
            new ViewerPasswordKeyChange(savedDisplay, newDisplay, publicKey, FormatFingerprint(saved), FormatFingerprint(fingerprint)));
    }

    /// <summary>A full fingerprint (64 hex characters) as upper-case groups of eight joined by dashes, for the operator to compare.</summary>
    public static string FormatFingerprint(string hex)
    {
        var upper = (hex ?? "").ToUpperInvariant();
        var groups = new System.Collections.Generic.List<string>();
        for (var i = 0; i < upper.Length; i += 8)
        {
            groups.Add(upper.Substring(i, Math.Min(8, upper.Length - i)));
        }

        return string.Join('-', groups);
    }

    /// <summary>
    /// Reads the key the store publishes and decides whether this viewer may seal with it. A changed key shows
    /// <see cref="PasswordKeyChangedDialog"/> over <paramref name="owner"/>: trusting it replaces the saved key and the
    /// save goes on, declining refuses with the changed-key sentence. A null <paramref name="owner"/> means there is no
    /// window to ask in: a changed key is refused with the changed-key sentence and no dialog is shown. A read-only
    /// connection never reads the key tables.
    /// </summary>
    public static async Task<ViewerSealKeyResult> GetSealKeyAsync(
        ViewerDataService data, Window? owner, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(data);

        if (data.IsReadOnly)
        {
            return new ViewerSealKeyResult { Refusal = ReadOnlyText };
        }

        PublishedPasswordKey? key;
        PasswordKeyServiceState? state;
        try
        {
            (key, state) = await data.GetPublishedPasswordKeyAsync(ct);
        }
        catch (PostgresException ex) when (ex.SqlState == "42501")
        {
            return new ViewerSealKeyResult { Refusal = ReadOnlyText };
        }
        catch (PostgresException ex) when (ex.SqlState == "42P01")
        {
            return new ViewerSealKeyResult { Refusal = NoTablesText };
        }

        var pins = new ViewerPasswordKeyPins();
        var decision = Evaluate(key, state, data.StoreConnectionIdentity, pins);
        return Resolve(decision, data.StoreConnectionIdentity, pins, owner is not null, change =>
        {
            var dialog = new PasswordKeyChangedDialog(change);
            if (owner is not null && owner.IsLoaded)
            {
                dialog.Owner = owner;
            }

            return dialog.ShowDialog() == true;
        });
    }

    /// <summary>
    /// Turns a decision into the answer for the caller. A changed key is put to <paramref name="ask"/> only when
    /// <paramref name="canAsk"/> is true; trusting it saves the new key (and refuses when that cannot be saved), declining
    /// or having no one to ask refuses with the changed-key sentence.
    /// </summary>
    internal static ViewerSealKeyResult Resolve(
        ViewerPasswordKeyDecision decision, string storeId, ViewerPasswordKeyPins pins, bool canAsk,
        Func<ViewerPasswordKeyChange, bool> ask)
    {
        if (decision.Change is { } change)
        {
            if (canAsk && ask(change))
            {
                if (!pins.Save(storeId, change.NewKey.Fingerprint, change.NewKey.KeyId))
                {
                    return new ViewerSealKeyResult { Refusal = PinsNotSavedText };
                }

                return new ViewerSealKeyResult { Sealer = new ViewerPasswordSealer(change.NewKey) };
            }

            return new ViewerSealKeyResult { Refusal = decision.Refusal };
        }

        return decision.Key is { } publicKey
            ? new ViewerSealKeyResult { Sealer = new ViewerPasswordSealer(publicKey), Notice = decision.Notice }
            : new ViewerSealKeyResult { Refusal = decision.Refusal };
    }

    /// <summary>
    /// The check on connect: saves the store's key the first time it is seen and returns the notice to show. A changed key
    /// shows nothing here; the save of a password asks. Returns null for a read-only connection, a store without a healthy
    /// key, or a key already saved. Never throws.
    /// </summary>
    public static async Task<string?> CheckOnConnectAsync(ViewerDataService data, CancellationToken ct = default)
    {
        try
        {
            if (data.IsReadOnly)
            {
                return null;
            }

            var (key, state) = await data.GetPublishedPasswordKeyAsync(ct);
            return Evaluate(key, state, data.StoreConnectionIdentity, new ViewerPasswordKeyPins()).Notice;
        }
        catch (Exception ex) when (ex is PostgresException or NpgsqlException or InvalidOperationException or OperationCanceledException)
        {
            ViewerLogger.Warn("ViewerPasswordKey", "The password key check was skipped: " + ex.Message);
            return null;
        }
    }

    private static ViewerPasswordKeyDecision Refused(string text) => new(null, text, null, null);
}
