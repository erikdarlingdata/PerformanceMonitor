/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

/// <summary>Where <see cref="LitePseudonymKeySource"/> keeps its one entry: Windows Credential Manager in the app, a fake in a test.</summary>
internal interface IPseudonymKeyVault
{
    /// <summary>Reads the entry's text (the key as base64).</summary>
    CredentialReadOutcome Read(out string? text, out string? failure);

    /// <summary>Writes the entry; returns why it failed, or null on success.</summary>
    string? Write(string text);
}

/// <summary>The real vault: the <c>PerformanceMonitorLite_PseudonymKey</c> entry of Windows Credential Manager, through <see cref="CredentialService"/>.</summary>
internal sealed class CredentialManagerPseudonymKeyVault : IPseudonymKeyVault
{
    /// <summary>The key part of the target name; the service adds the <c>PerformanceMonitorLite_</c> prefix, so the entry is <c>PerformanceMonitorLite_PseudonymKey</c>.</summary>
    internal const string EntryName = "PseudonymKey";

    private const string UserName = "PseudonymKey";

    private readonly CredentialService _credentials;

    internal CredentialManagerPseudonymKeyVault(CredentialService credentials) => _credentials = credentials;

    public CredentialReadOutcome Read(out string? text, out string? failure)
    {
        CredentialReadOutcome outcome = _credentials.TryReadCredential(EntryName, out var credential, out failure);
        text = credential?.Password;
        return outcome;
    }

    public string? Write(string text) => _credentials.TrySaveCredential(EntryName, UserName, text) is { Succeeded: false } result ? result.Failure : null;
}

/// <summary>
/// Lite's pseudonym key (#6066): 32 random bytes in Windows Credential Manager under <c>PerformanceMonitorLite_PseudonymKey</c>, made
/// the first time no entry exists and read back unchanged ever after. <b>An entry it cannot read, or cannot parse, is never replaced</b>:
/// replacing it would change every pseudonym an agent has seen. Then it answers null, the masker numbers per answer (<c>login#1</c>,
/// still no name), and the reason is logged once, without the key. Settings export and import never carry the entry
/// (<c>SettingsImport</c> copies named files only; a Credential Manager entry is not a file). No Lite MCP tool reads Credential Manager.
/// It is asked again at most every <see cref="RetryAfterMilliseconds"/> after a failure. Not <c>store_identity</c> (a collection reset
/// replaces it) and not <c>InstallIdStore</c>'s rebuild-on-mismatch (that would rotate the key).
/// </summary>
internal sealed class LitePseudonymKeySource : IPseudonymKeySource
{
    /// <summary>The least time between two failed attempts, in milliseconds.</summary>
    internal const int RetryAfterMilliseconds = 30_000;

    private readonly IPseudonymKeyVault _vault;
    private readonly Action<string> _warn;
    private readonly object _gate = new();
    private PseudonymKey? _key;
    private long _nextAttemptAtTick;
    private bool _warned;

    internal LitePseudonymKeySource(IPseudonymKeyVault vault, Action<string>? warn = null)
    {
        _vault = vault ?? throw new ArgumentNullException(nameof(vault));
        _warn = warn ?? (static _ => { });
    }

    /// <inheritdoc />
    public ValueTask<PseudonymKey?> GetAsync(CancellationToken cancellationToken = default) => new(Get());

    private PseudonymKey? Get()
    {
        lock (_gate)
        {
            if (_key is not null) return _key;
            if (Environment.TickCount64 < _nextAttemptAtTick) return null;

            CredentialReadOutcome outcome = _vault.Read(out string? text, out string? failure);
            switch (outcome)
            {
                case CredentialReadOutcome.Found:
                {
                    if (TryParse(text, out PseudonymKey? parsed))
                    {
                        _key = parsed;
                        return _key;
                    }

                    return Fail("the stored entry is not a 32-byte key; it is left as it is");
                }

                case CredentialReadOutcome.NotFound:
                {
                    byte[] bytes = RandomNumberGenerator.GetBytes(PseudonymKey.Length);
                    string? writeFailure = _vault.Write(Convert.ToBase64String(bytes));
                    if (writeFailure is not null)
                    {
                        Array.Clear(bytes);
                        return Fail("the key could not be saved: " + writeFailure);
                    }

                    var made = new PseudonymKey(bytes);
                    Array.Clear(bytes);
                    _key = made;
                    return _key;
                }

                default:
                    return Fail("the entry could not be read: " + (failure ?? "unknown"));
            }
        }
    }

    private static bool TryParse(string? text, out PseudonymKey? key)
    {
        key = null;
        if (string.IsNullOrEmpty(text)) return false;
        Span<byte> bytes = stackalloc byte[PseudonymKey.Length + 4];
        if (!Convert.TryFromBase64String(text, bytes, out int written) || written != PseudonymKey.Length) return false;
        key = new PseudonymKey(bytes[..written]);
        bytes.Clear();
        return true;
    }

    private PseudonymKey? Fail(string reason)
    {
        _nextAttemptAtTick = Environment.TickCount64 + RetryAfterMilliseconds;
        if (!_warned)
        {
            _warned = true;
            PseudonymKeyNotice.Spent();
            _warn("The pseudonym key is not available (" + reason + "). Under mcp_data_detail metadataOnly, logins and hosts are numbered per answer "
                + "(login#1) until it is; every name stays hidden. Lite never replaces an entry it cannot read.");
        }

        return null;
    }
}
