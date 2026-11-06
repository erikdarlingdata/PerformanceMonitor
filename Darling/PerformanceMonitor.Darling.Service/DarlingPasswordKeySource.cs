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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The externally supplied password key (#8006): the <c>passwordKey</c> setting in darling.json names a
/// <c>file:</c> or <c>env:</c> reference the service loads its password key from, instead of generating and
/// keeping a key file in the credentials directory.
///
/// <para><see cref="LoadConfigured"/> is the one loader every caller uses (the start, the CLI, the diagnostics), so
/// the setting is read, refused and parsed the same way everywhere. With the setting it reads only the reference and
/// never generates, writes or falls back to the key file; without it, it is exactly
/// <see cref="DarlingPasswordKeyFile.Load"/>.</para>
/// </summary>
public static class DarlingPasswordKeySource
{
    /// <summary>The darling.json setting name, used in every sentence about the setting.</summary>
    public const string SettingName = "passwordKey";

    /// <summary>
    /// The validation problem for a literal <c>passwordKey</c>. A constant on purpose: the sentence never repeats the
    /// value, because that value would be the key.
    /// </summary>
    public const string LiteralRefusal = "passwordKey must be a file: or env: reference to the password key, never the key itself. Put the key in a secret file or an environment variable and point passwordKey at it.";

    private const string FilePrefix = "file:";
    private const string EnvPrefix = "env:";
    /* A UTF-8 BOM as it reads after decoding, written as a code so no editor can strip it unseen. */
    private const char ByteOrderMark = (char)0xFEFF;
    private const string PemBegin = "-----BEGIN";
    private const string PrivateKeyLabel = "PRIVATE KEY";
    private const string Pkcs1Label = "RSA PRIVATE KEY";
    private const string EncryptedLabel = "ENCRYPTED PRIVATE KEY";

    /* The problems ParseKeyText reports. Each is a clause about the text ("it ..."), never a piece of it: the caller
       puts the setting and the target in front. */
    private const string EmptyProblem = "it is empty";
    private const string NotAKeyProblem =
        "it is neither base64 of a PKCS#8 DER key nor a -----BEGIN PRIVATE KEY----- PEM block";
    private const string TwoKeysProblem = "it holds more than one key";
    private const string UnreadablePemProblem = "it has a -----BEGIN line but no PEM block that can be read";
    private const string OtherPemProblem = "it holds a PEM block that is not -----BEGIN PRIVATE KEY-----";
    private const string EscapedNewlinesProblem =
        "it is a PEM block whose line breaks are the two characters \\n instead of new lines, as docker run -e or a " +
        "YAML string can pass one. For an environment variable use base64: the PKCS#8 DER key as one line of base64";
    private const string Pkcs1Problem =
        "it is a PKCS#1 key (-----BEGIN RSA PRIVATE KEY-----), not PKCS#8. Convert it with " +
        "openssl pkcs8 -topk8 -nocrypt -in <key.pem> -out <pkcs8.pem>, or make a new key with --new-password-key <path>";
    private const string EncryptedProblem =
        "it is an encrypted key (-----BEGIN ENCRYPTED PRIVATE KEY-----), and the service has no passphrase to open it. " +
        "Give it the key unencrypted (openssl pkcs8 -topk8 -nocrypt) and let the secret mount or variable protect it";
    private const string NotAPasswordKeyProblem =
        "it does not load as a password key (an RSA-3072 key with the public exponent 65537)";

    /// <summary>
    /// The password key this service uses, by its configuration. With <c>passwordKey</c> set (non-blank, the rule
    /// <see cref="DarlingConfig.Validate"/> uses) it is <see cref="LoadFromSetting"/>, checked against the credentials
    /// directory when <paramref name="configPath"/> is known. Without it, and with no config path, there is nothing to
    /// look at. Otherwise it is <see cref="DarlingPasswordKeyFile.Load"/> on the credentials directory, unchanged.
    /// </summary>
    internal static PasswordKeyFileLoad LoadConfigured(DarlingConfig config, string? configPath, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(config);

        if (!string.IsNullOrWhiteSpace(config.PasswordKey))
        {
            return LoadFromSetting(
                config.PasswordKey,
                configPath is null ? null : DarlingLogHashKeyFile.DirectoryFor(config, configPath),
                logger);
        }

        if (configPath is null)
        {
            return new PasswordKeyFileLoad(false, false, null, null, false, "");
        }

        return DarlingPasswordKeyFile.Load(DarlingLogHashKeyFile.DirectoryFor(config, configPath), logger);
    }

    /// <summary>
    /// The line <c>--validate-config</c> prints about <c>passwordKey</c>: null when the setting is unset,
    /// <c>passwordKey: password key {fingerprint}</c> when the key loads, otherwise <c>WARNING: </c> and the refusal. A
    /// warning, not a failure, because the start refuses such a key without stopping collection. The key is loaded only
    /// to work out its fingerprint, and is disposed and zeroed before this returns.
    /// </summary>
    internal static string? DescribeForValidation(DarlingConfig config, string? configPath)
    {
        ArgumentNullException.ThrowIfNull(config);

        if (string.IsNullOrWhiteSpace(config.PasswordKey))
        {
            return null;
        }

        var load = LoadFromSetting(
            config.PasswordKey,
            configPath is null ? null : DarlingLogHashKeyFile.DirectoryFor(config, configPath),
            NullLogger.Instance);
        var pkcs8 = load.Pkcs8;
        if (pkcs8 is null)
        {
            return "WARNING: " + load.Refusal;
        }

        try
        {
            using var key = PasswordPrivateKey.FromPkcs8(pkcs8);
            return $"{SettingName}: password key {PasswordSeal.DisplayKeyId(key.PublicKey.KeyId)}";
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
        }
    }

    /// <summary>
    /// Reads the password key a <c>passwordKey</c> reference names. It never throws, generates or writes.
    ///
    /// <para>A key: <c>Present</c> true, <c>Untrusted</c> false, <c>Refusal</c> null, <c>Pkcs8</c> the key (the
    /// caller's copy, to zero when done), <c>Path</c> the reference. Every problem (a literal, a <c>file:</c> path
    /// inside <paramref name="credentialsDirectory"/>, a target that cannot be read, text that is not a usable key):
    /// <c>Present</c> true, <c>Untrusted</c> true, <c>Pkcs8</c> null and a <c>Refusal</c> that names
    /// <c>passwordKey</c> and the target, never the text. <c>Path</c> is the reference, except for a literal, where it
    /// is <see cref="SettingName"/>: the literal would be the key.</para>
    /// </summary>
    /// <param name="reference">The setting's value: <c>file:&lt;path&gt;</c> or <c>env:&lt;NAME&gt;</c>.</param>
    /// <param name="credentialsDirectory">The service's credentials directory
    /// (<see cref="DarlingLogHashKeyFile.DirectoryFor"/>), which a <c>file:</c> path must stay out of; null when it is
    /// not known.</param>
    /// <param name="logger">Where a successful read is noted.</param>
    public static PasswordKeyFileLoad LoadFromSetting(string reference, string? credentialsDirectory, ILogger logger)
    {
        var path = reference ?? "";

        /* Resolve passes a literal through unchanged, and not every caller ran Validate first. The prefix counts only at
           the start of the value as written, the rule of Resolve and of the owned set: a value with leading whitespace
           is a literal here too, so no key can load from a reference the owned set does not hold. */
        if (!DarlingSecretSource.IsReference(path))
        {
            return Refused(SettingName, LiteralRefusal);
        }

        var isFile = path.StartsWith(FilePrefix, StringComparison.Ordinal);
        var named = isFile
            ? path.Substring(FilePrefix.Length).Trim()
            : path.Substring(EnvPrefix.Length).Trim();
        var target = isFile ? $"file '{named}'" : $"environment variable '{named}'";

        byte[]? pkcs8 = null;
        try
        {
            if (isFile && named.Length > 0 && !string.IsNullOrWhiteSpace(credentialsDirectory)
                && IsInside(named, credentialsDirectory))
            {
                return Refused(path,
                    $"{SettingName}: {target} is inside the service's credentials directory '{credentialsDirectory}', " +
                    "where the service renames and removes files itself. Put the key in a secret mount outside the " +
                    $"credentials directory and point {SettingName} at it.");
            }

            string text;
            try
            {
                text = DarlingSecretSource.Resolve(path, SettingName);
            }
            catch (InvalidOperationException ex)
            {
                /* Resolve's sentences already start "passwordKey: " and name the path or the variable, not the contents. */
                return Refused(path, ex.Message);
            }

            string? problem;
            (pkcs8, problem) = ParseKeyText(text);
            if (pkcs8 is null)
            {
                return Refused(path, $"{SettingName}: the key in {target} cannot be used: {problem ?? NotAKeyProblem}.");
            }

            logger?.LogDebug("{Setting}: read the password key from {Target}.", SettingName, target);
            return new PasswordKeyFileLoad(true, false, null, pkcs8, false, path);
        }
        catch (Exception ex)
        {
            /* A key already parsed when something after it failed is not handed out, so it is zeroed here. */
            if (pkcs8 is not null)
            {
                CryptographicOperations.ZeroMemory(pkcs8);
            }

            /* Only the exception's type: its message could, in principle, quote what it failed on. */
            return Refused(path, $"{SettingName}: the key in {target} could not be read ({ex.GetType().Name}).");
        }
    }

    /// <summary>
    /// Parses key text: base64 of a PKCS#8 DER RSA private key, or one <c>-----BEGIN PRIVATE KEY-----</c> PEM block,
    /// after a leading UTF-8 BOM and surrounding whitespace are dropped. The key must pass
    /// <see cref="DarlingPasswordKeyFile.CheckPkcs8"/> and load through <see cref="PasswordPrivateKey.FromPkcs8"/>.
    /// Returns the key (the caller zeroes it) and a null problem, or no key and a clause starting "it ..." that says
    /// what is wrong without repeating any of the text. Every buffer it decodes into and does not return is zeroed.
    /// </summary>
    internal static (byte[]? Pkcs8, string? Problem) ParseKeyText(string text)
    {
        var span = (text ?? "").AsSpan().Trim();
        if (span.Length > 0 && span[0] == ByteOrderMark)
        {
            span = span.Slice(1).Trim();
        }

        if (span.IsEmpty)
        {
            return (null, EmptyProblem);
        }

        byte[]? der = null;
        try
        {
            string? problem;
            (der, problem) = span.Contains(PemBegin.AsSpan(), StringComparison.Ordinal) ? DecodePem(span) : DecodeBase64(span);
            if (der is null)
            {
                return (null, problem ?? NotAKeyProblem);
            }

            var check = DarlingPasswordKeyFile.CheckPkcs8(der);
            if (check is not null)
            {
                return (null, check);
            }

            try
            {
                /* CheckPkcs8 has passed, so what is left to refuse here is the public exponent. */
                using var _ = PasswordPrivateKey.FromPkcs8(der);
            }
            catch (PasswordSealException)
            {
                return (null, NotAPasswordKeyProblem);
            }

            var key = der;
            der = null;
            return (key, null);
        }
        finally
        {
            if (der is not null)
            {
                CryptographicOperations.ZeroMemory(der);
            }
        }
    }

    private static (byte[]? Der, string? Problem) DecodePem(ReadOnlySpan<char> span)
    {
        /* A backslash is never in a PEM; "\n" as two characters is a PEM whose line breaks were escaped on the way in. */
        if (span.Contains("\\n".AsSpan(), StringComparison.Ordinal))
        {
            return (null, EscapedNewlinesProblem);
        }

        if (!PemEncoding.TryFind(span, out var fields))
        {
            return (null, UnreadablePemProblem);
        }

        /* One key only: a second block, before or after this one, is refused, never silently ignored. */
        if (span[..fields.Location.Start].Contains(PemBegin.AsSpan(), StringComparison.Ordinal)
            || span[fields.Location.End..].Contains(PemBegin.AsSpan(), StringComparison.Ordinal))
        {
            return (null, TwoKeysProblem);
        }

        var label = span[fields.Label];
        if (label.SequenceEqual(Pkcs1Label.AsSpan()))
        {
            return (null, Pkcs1Problem);
        }

        if (label.SequenceEqual(EncryptedLabel.AsSpan()))
        {
            return (null, EncryptedProblem);
        }

        if (!label.SequenceEqual(PrivateKeyLabel.AsSpan()))
        {
            return (null, OtherPemProblem);
        }

        var buffer = new byte[fields.DecodedDataLength];
        if (!Convert.TryFromBase64Chars(span[fields.Base64Data], buffer, out var written))
        {
            CryptographicOperations.ZeroMemory(buffer);
            return (null, UnreadablePemProblem);
        }

        return (Exactly(buffer, written), null);
    }

    private static (byte[]? Der, string? Problem) DecodeBase64(ReadOnlySpan<char> span)
    {
        var buffer = new byte[(span.Length * 3 / 4) + 3];
        if (!Convert.TryFromBase64Chars(span, buffer, out var written) || written == 0)
        {
            CryptographicOperations.ZeroMemory(buffer);
            return (null, NotAKeyProblem);
        }

        return (Exactly(buffer, written), null);
    }

    /// <summary>The first <paramref name="length"/> bytes of <paramref name="buffer"/>; a longer buffer is zeroed once
    /// they are copied out.</summary>
    private static byte[] Exactly(byte[] buffer, int length)
    {
        if (length == buffer.Length)
        {
            return buffer;
        }

        var exact = buffer.AsSpan(0, length).ToArray();
        CryptographicOperations.ZeroMemory(buffer);
        return exact;
    }

    /// <summary>True when <paramref name="path"/> is <paramref name="directory"/> or anything under it, however either is
    /// written: both go through <see cref="Path.GetFullPath(string)"/> (which resolves <c>..</c>) and lose a trailing
    /// separator, and Windows compares without case, as its file system does.</summary>
    private static bool IsInside(string path, string directory)
    {
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        var full = Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));
        var root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(directory));
        if (string.Equals(full, root, comparison))
        {
            return true;
        }

        var prefix = Path.EndsInDirectorySeparator(root) ? root : root + Path.DirectorySeparatorChar;
        return full.StartsWith(prefix, comparison);
    }

    private static PasswordKeyFileLoad Refused(string path, string sentence) =>
        new(true, true, sentence, null, false, path);
}
