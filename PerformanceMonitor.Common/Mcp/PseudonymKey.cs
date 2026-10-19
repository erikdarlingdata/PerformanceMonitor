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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Common;

/// <summary>
/// The store's pseudonym key (#6066): 32 random bytes that turn a login or host name into the same
/// <c>login#3fa9c2d1</c> or <c>host#a06e9ea5</c> text in every <c>metadataOnly</c> answer from one store. It holds the bytes
/// and says nothing about them: <see cref="ToString"/> prints the type name, and no member returns the bytes. It is never
/// put in an answer, a log line, a journal row, a brief, a webhook, an export or an error.
/// </summary>
public sealed class PseudonymKey
{
    /// <summary>The key's length in bytes.</summary>
    public const int Length = 32;

    private readonly byte[] _bytes;

    /// <summary>A key over a copy of <paramref name="bytes"/>, which must be exactly <see cref="Length"/> bytes.</summary>
    public PseudonymKey(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != Length)
            throw new ArgumentException("A pseudonym key is " + Length.ToString(CultureInfo.InvariantCulture) + " bytes.", nameof(bytes));
        _bytes = bytes.ToArray();
    }

    /// <summary>A new random key.</summary>
    public static PseudonymKey Generate() => new(RandomNumberGenerator.GetBytes(Length));

    /// <summary>The raw bytes, for the one place that stores them (Lite's credential entry, a test vector). Never log them.</summary>
    internal byte[] Export() => (byte[])_bytes.Clone();

    /// <summary>Prints the type name only, so a log line that formats a key leaks nothing.</summary>
    public override string ToString() => nameof(PseudonymKey);

    /// <summary>
    /// The first 4 bytes, as 8 lowercase hex characters, of <c>HMAC-SHA256(key, "&lt;kind&gt;\n" + name)</c>, with
    /// <paramref name="name"/> trimmed and lower-cased (<see cref="string.ToLowerInvariant"/>): logins and host names
    /// compare case-insensitively on SQL Server by default, so <c>CORP\alice</c> and <c>corp\alice</c> are one name. The kind
    /// (<c>login</c> or <c>host</c>) is part of the input, so a login and a host with the same text differ.
    /// </summary>
    internal string Hex8(string kind, string name)
    {
        byte[] input = Encoding.UTF8.GetBytes(kind + "\n" + name.Trim().ToLowerInvariant());
        byte[] mac = HMACSHA256.HashData(_bytes, input);
        return Convert.ToHexStringLower(mac.AsSpan(0, 4));
    }

    /// <summary>The pseudonym of <paramref name="name"/> as a <paramref name="kind"/> (<c>login</c> or <c>host</c>):
    /// <c>login#3fa9c2d1</c>. The masker adds a <c>-2</c> style suffix itself when two names in one answer collide.</summary>
    public string Pseudonym(string kind, string name) => kind + "#" + Hex8(kind, name);
}

/// <summary>
/// Where an app's <see cref="PseudonymKey"/> comes from (#6066). Darling reads it once from the store over the owner
/// connection; Lite keeps it in Windows Credential Manager. <see cref="GetAsync"/> returns null while the key is not
/// available (the read has not worked yet, or failed): the masker then numbers per answer, which still hides every
/// name, and the source is asked again on the next answer. It never throws for an unavailable key, and it logs why once.
/// </summary>
public interface IPseudonymKeySource
{
    /// <summary>The key, or null when it is not available now.</summary>
    ValueTask<PseudonymKey?> GetAsync(CancellationToken cancellationToken = default);
}

/// <summary>A source over one key already in hand (a test, or a caller that read it).</summary>
public sealed class FixedPseudonymKeySource : IPseudonymKeySource
{
    private readonly PseudonymKey? _key;

    /// <summary>A source that always answers <paramref name="key"/>, which may be null.</summary>
    public FixedPseudonymKeySource(PseudonymKey? key) => _key = key;

    /// <inheritdoc />
    public ValueTask<PseudonymKey?> GetAsync(CancellationToken cancellationToken = default) => new(_key);
}

/// <summary>Null-safe reads of an optional source, shared by every call site.</summary>
public static class PseudonymKeySources
{
    /// <summary>The key from <paramref name="source"/> when <paramref name="mode"/> is <see cref="McpDataDetail.MetadataOnly"/>
    /// (the only level that uses it) and the source has one; otherwise null. A source that throws is treated as having none.</summary>
    public static async ValueTask<PseudonymKey?> KeyForAsync(IPseudonymKeySource? source, McpDataDetail mode, CancellationToken cancellationToken = default)
    {
        if (source is null || mode != McpDataDetail.MetadataOnly) return null;
        try
        {
            return await source.GetAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
#pragma warning disable CA1031 // a failing key source means "no key": the masker then numbers per answer and never shows the name
        catch (Exception)
#pragma warning restore CA1031
        {
            return null;
        }
    }
}

/// <summary>
/// The one warning for a <c>metadataOnly</c> answer made without a key (#6066): logged once per process through the sink the
/// app set at start, with no key and no name in it. The answer is still masked: it uses the per-answer numbering.
/// </summary>
public static class PseudonymKeyNotice
{
    private static int s_warned;

    /// <summary>Where the warning goes (an app's logger); null drops it, and the once-flag is then not spent.</summary>
    public static Action<string>? Sink { get; set; }

    /// <summary>A source that has already warned with its own reason spends the once-flag, so the process warns once, not twice.</summary>
    internal static void Spent() => Interlocked.Exchange(ref s_warned, 1);

    /// <summary>Clears the once-flag (tests).</summary>
    internal static void Reset() => Interlocked.Exchange(ref s_warned, 0);

    internal static void NoKey()
    {
        Action<string>? sink = Sink;
        if (sink is null || Interlocked.Exchange(ref s_warned, 1) == 1) return;
        try
        {
            sink("mcp.dataDetail is metadataOnly but the pseudonym key is not available, so logins and hosts are numbered per answer "
                + "(login#1) instead of keeping one pseudonym each. Every name is still hidden. This is retried on the next answer.");
        }
#pragma warning disable CA1031 // a failing log sink must not fail an answer
        catch (Exception)
#pragma warning restore CA1031
        {
        }
    }
}
