/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>The password key the store currently publishes.</summary>
public sealed record PublishedKey(string KeyId, byte[] Spki);

/// <summary>What the service found about its password key at start: the key file, and the key the store publishes.</summary>
public sealed record PasswordKeyFacts(
    bool FilePresent, bool FileUntrusted, string? FileRefusal, byte[]? FileSpki,
    bool FoundAfterOpenDirectory, PublishedKey? StoreCurrent, bool FileKeyRetiredInStore);

/// <summary>What the service does with its password key at start.</summary>
public enum PasswordKeyAction
{
    /// <summary>Use the key file.</summary>
    Use,

    /// <summary>Make a new key and publish it.</summary>
    Generate,

    /// <summary>Publish the key the file holds.</summary>
    PublishFile,

    /// <summary>The file holds a different key from the one the store publishes. Saved passwords cannot be opened.</summary>
    Mismatch,

    /// <summary>The store publishes a key and the file is missing. Saved passwords cannot be opened.</summary>
    Missing,

    /// <summary>The file cannot be used and must not be replaced.</summary>
    Refused,
}

/// <summary>The decision for one start. <see cref="RetireAs"/> is <c>retired</c> or <c>discarded</c>; the reason and the
/// warning are plain sentences for the log and the admin pages.</summary>
public sealed record PasswordKeyDecision(
    PasswordKeyAction Action, bool RetireFileFirst, string? RetireAs, string? Reason, string? Warning);

/// <summary>The one place the service's start-up choice about its password key is made, from facts only, so every
/// row of the decision table can be tested without a file system or a store.</summary>
public static class DarlingPasswordKeyState
{
    /// <summary>The warning that goes with using a key file found after its directory was opened to other users.</summary>
    public const string OpenDirectoryWarning =
        "The credentials directory was open to other users until this start. The password key still matches the store, " +
        "so saved passwords work, but another user may have read it. If that is possible, run --reset-password-key " +
        "and enter the saved passwords again.";

    private const string DiscardedFileReason =
        "The password key file was found while its directory was open to other users and does not match the store, so it was set aside.";

    /// <summary>Decides what to do, checking the rows of the table in order.</summary>
    public static PasswordKeyDecision Decide(PasswordKeyFacts f)
    {
        ArgumentNullException.ThrowIfNull(f);

        // A file that exists but cannot be trusted is never replaced.
        if (f.FileUntrusted)
        {
            return new PasswordKeyDecision(
                PasswordKeyAction.Refused, false, null,
                string.IsNullOrWhiteSpace(f.FileRefusal) ? "The password key file cannot be used." : f.FileRefusal,
                null);
        }

        var current = f.StoreCurrent;

        // A published key whose id does not match its own key is not one to trust either way.
        if (current is not null && !string.Equals(current.KeyId, PasswordSeal.KeyIdFor(current.Spki), StringComparison.Ordinal))
        {
            return f.FilePresent
                ? new PasswordKeyDecision(PasswordKeyAction.Mismatch, false, null, MismatchReason(f.FileSpki, current), null)
                : new PasswordKeyDecision(
                    PasswordKeyAction.Refused, false, null,
                    "The store's published password key is inconsistent: its id does not match the key. " +
                    "Run --reset-password-key and enter the saved passwords again.",
                    null);
        }

        if (f.FilePresent && f.FoundAfterOpenDirectory)
        {
            if (current is not null && f.FileSpki is not null && SameKey(current.Spki, f.FileSpki))
            {
                return new PasswordKeyDecision(PasswordKeyAction.Use, false, null, null, OpenDirectoryWarning);
            }

            // The file does not match the store: set it aside, then decide as if it were absent.
            var withoutFile = DecideWithoutFile(current);
            return withoutFile with { RetireFileFirst = true, RetireAs = "discarded", Reason = withoutFile.Reason ?? DiscardedFileReason };
        }

        if (!f.FilePresent)
        {
            return DecideWithoutFile(current);
        }

        if (f.FileSpki is null)
        {
            return new PasswordKeyDecision(
                PasswordKeyAction.Refused, false, null, "The password key file could not be read.", null);
        }

        if (current is null)
        {
            return f.FileKeyRetiredInStore
                ? new PasswordKeyDecision(PasswordKeyAction.Generate, true, "retired", null, null)
                : new PasswordKeyDecision(PasswordKeyAction.PublishFile, false, null, null, null);
        }

        if (SameKey(current.Spki, f.FileSpki))
        {
            return new PasswordKeyDecision(PasswordKeyAction.Use, false, null, null, null);
        }

        return new PasswordKeyDecision(PasswordKeyAction.Mismatch, false, null, MismatchReason(f.FileSpki, current), null);
    }

    // Both ids are worked out from the key bytes, never printed as the store holds them.
    private static string MismatchReason(byte[]? fileSpki, PublishedKey current)
    {
        var fileId = fileSpki is null ? "unreadable" : PasswordSeal.DisplayKeyId(PasswordSeal.KeyIdFor(fileSpki));
        var storeId = PasswordSeal.DisplayKeyId(PasswordSeal.KeyIdFor(current.Spki));
        return $"The password key in the credentials directory ({fileId}) is not the key the store publishes ({storeId}). " +
            "Restore the credentials volume that holds the key, or run --reset-password-key and enter the saved passwords again.";
    }

    private static PasswordKeyDecision DecideWithoutFile(PublishedKey? current) =>
        current is null
            ? new PasswordKeyDecision(PasswordKeyAction.Generate, false, null, null, null)
            : new PasswordKeyDecision(
                PasswordKeyAction.Missing, false, null,
                $"The password key {PasswordSeal.DisplayKeyId(PasswordSeal.KeyIdFor(current.Spki))} is missing from the credentials directory. " +
                "Restore the credentials volume, or run --reset-password-key and enter the saved passwords again.",
                null);

    private static bool SameKey(byte[] a, byte[] b) => a.AsSpan().SequenceEqual(b);
}

/// <summary>What the service can do with its password key right now.</summary>
public sealed record PasswordKeyStatus(bool CanSeal, string? Reason, string? KeyId, PasswordPublicKey? PublicKey);

/// <summary>The service's password key as its callers see it: seal and open, or a reason it cannot.</summary>
public interface IPasswordKeyRing
{
    /// <summary>Whether the ring can seal, and why not when it cannot.</summary>
    PasswordKeyStatus Status { get; }

    /// <summary>Seals a password for <paramref name="binding"/>. Throws <see cref="InvalidOperationException"/> with
    /// <see cref="PasswordKeyStatus.Reason"/> when the ring cannot seal.</summary>
    string Seal(string plaintext, PasswordBinding binding);

    /// <summary>Opens a sealed password for <paramref name="binding"/>. Throws <see cref="PasswordSealException"/>; a ring
    /// that holds no key throws <see cref="PasswordSealFailure.UnknownKey"/>.</summary>
    string Open(string sealedText, PasswordBinding binding);
}

/// <summary>The service's password key ring. Production code passes <see cref="Current"/> to every consumer; tests pass
/// a ring and never set it.</summary>
public static class DarlingPasswordKey
{
    /// <summary>The reason given until the service has loaded its key.</summary>
    public const string NotReadyReason = "The service is still loading its password key. Try again in a moment.";

    private static volatile IPasswordKeyRing _current = Refusing(NotReadyReason);

    /// <summary>The ring in use. It starts as one that refuses with <see cref="NotReadyReason"/>.</summary>
    public static IPasswordKeyRing Current
    {
        get => _current;
        internal set => _current = value ?? throw new ArgumentNullException(nameof(value));
    }

    /// <summary>A ring that seals and opens with <paramref name="key"/>.</summary>
    public static IPasswordKeyRing FromPrivateKey(PasswordPrivateKey key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return new KeyedRing(key);
    }

    /// <summary>A ring that cannot seal and holds no key, with <paramref name="reason"/> as the answer.</summary>
    public static IPasswordKeyRing Refusing(string reason) => new RefusingRing(reason);

    private sealed class KeyedRing : IPasswordKeyRing
    {
        private readonly PasswordPrivateKey _key;

        public KeyedRing(PasswordPrivateKey key)
        {
            _key = key;
            Status = new PasswordKeyStatus(true, null, key.PublicKey.KeyId, key.PublicKey);
        }

        public PasswordKeyStatus Status { get; }

        public string Seal(string plaintext, PasswordBinding binding) =>
            PasswordSeal.Seal(plaintext, _key.PublicKey, binding);

        public string Open(string sealedText, PasswordBinding binding) =>
            PasswordSeal.Open(sealedText, _key, binding);
    }

    private sealed class RefusingRing : IPasswordKeyRing
    {
        private readonly string _reason;

        public RefusingRing(string reason)
        {
            _reason = reason;
            Status = new PasswordKeyStatus(false, reason, null, null);
        }

        public PasswordKeyStatus Status { get; }

        public string Seal(string plaintext, PasswordBinding binding) =>
            throw new InvalidOperationException(_reason);

        public string Open(string sealedText, PasswordBinding binding) =>
            throw new PasswordSealException(PasswordSealFailure.UnknownKey, PasswordSeal.KeyIdOf(sealedText ?? ""));
    }
}
