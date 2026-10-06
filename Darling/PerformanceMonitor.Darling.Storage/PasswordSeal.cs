/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>Why a sealed password could not be opened or a key could not be read (#5366).</summary>
public enum PasswordSealFailure
{
    /// <summary>The text or key is not in the expected structure.</summary>
    Malformed,

    /// <summary>The text names a sealed format version this build does not know.</summary>
    NewerVersion,

    /// <summary>The text was sealed with a key other than the one asked to open it.</summary>
    UnknownKey,

    /// <summary>The value did not open for these connection settings: they differ from the ones it was sealed for, or
    /// the stored text was changed.</summary>
    BindingOrTamper,
}

/// <summary>
/// A sealed password could not be opened. The message is one fixed sentence per <see cref="Kind"/>; it never carries
/// text from the crypto provider or the value itself, and there is never an inner exception.
/// </summary>
public sealed class PasswordSealException : Exception
{
    /// <summary>Creates the exception for <paramref name="kind"/>, optionally naming the key id the text carried.</summary>
    public PasswordSealException(PasswordSealFailure kind, string? keyId = null)
        : base(MessageFor(kind))
    {
        Kind = kind;
        KeyId = keyId;
    }

    internal PasswordSealException(PasswordSealFailure kind, string message, string? keyId)
        : base(message)
    {
        Kind = kind;
        KeyId = keyId;
    }

    /// <summary>What went wrong.</summary>
    public PasswordSealFailure Kind { get; }

    /// <summary>The key id the sealed text carried, when it got far enough to read one.</summary>
    public string? KeyId { get; }

    private static string MessageFor(PasswordSealFailure kind) => kind switch
    {
        PasswordSealFailure.Malformed => "The saved password is not in a recognised sealed format.",
        PasswordSealFailure.NewerVersion => "The saved password was sealed by a newer version of the service.",
        PasswordSealFailure.UnknownKey => "The saved password was sealed with a key this service does not have.",
        _ => "The saved password does not open for this server's connection settings.",
    };
}

/// <summary>The public half of the service's password key: what every writer seals to.</summary>
public sealed class PasswordPublicKey
{
    private readonly byte[] _spki;

    private PasswordPublicKey(byte[] spki)
    {
        _spki = spki;
        KeyId = PasswordSeal.KeyIdFor(spki);
        Fingerprint = SHA256.HashData(spki);
    }

    /// <summary>The key id: 16 lowercase hex characters of the SHA-256 of <see cref="Spki"/>.</summary>
    public string KeyId { get; }

    /// <summary>The full SHA-256 of <see cref="Spki"/>, 32 bytes. A viewer pins this.</summary>
    public byte[] Fingerprint { get; }

    /// <summary>The DER SubjectPublicKeyInfo of the key (a copy).</summary>
    public byte[] Spki => (byte[])_spki.Clone();

    /// <summary>Reads a public key. It must be an RSA key of exactly 3072 bits; anything else is
    /// <see cref="PasswordSealFailure.Malformed"/>.</summary>
    public static PasswordPublicKey FromSpki(byte[] spki)
    {
        if (spki is null || spki.Length == 0)
        {
            throw new PasswordSealException(PasswordSealFailure.Malformed);
        }

        var ok = false;
        try
        {
            using var rsa = RSA.Create();
            rsa.ImportSubjectPublicKeyInfo(spki, out var read);
            ok = read == spki.Length && rsa.KeySize == PasswordSeal.KeyBits;
        }
        catch (CryptographicException)
        {
        }
        catch (ArgumentException)
        {
        }

        if (!ok)
        {
            throw new PasswordSealException(PasswordSealFailure.Malformed);
        }

        return new PasswordPublicKey((byte[])spki.Clone());
    }
}

/// <summary>The private half of the service's password key. It opens sealed values; it is never stored in the store.</summary>
public sealed class PasswordPrivateKey : IDisposable
{
    private readonly RSA _rsa;
    private readonly object _gate = new();

    private PasswordPrivateKey(RSA rsa)
    {
        _rsa = rsa;
        PublicKey = PasswordPublicKey.FromSpki(rsa.ExportSubjectPublicKeyInfo());
    }

    /// <summary>The public half.</summary>
    public PasswordPublicKey PublicKey { get; }

    /// <summary>Makes a new RSA-3072 key.</summary>
    public static PasswordPrivateKey Generate() => new(RSA.Create(PasswordSeal.KeyBits));

    /// <summary>Reads a private key from PKCS#8 DER. It must be an RSA key of exactly 3072 bits; anything else is
    /// <see cref="PasswordSealFailure.Malformed"/>.</summary>
    public static PasswordPrivateKey FromPkcs8(ReadOnlySpan<byte> der)
    {
        RSA? rsa = null;
        var ok = false;
        try
        {
            rsa = RSA.Create();
            rsa.ImportPkcs8PrivateKey(der, out var read);
            ok = read == der.Length && rsa.KeySize == PasswordSeal.KeyBits;
        }
        catch (CryptographicException)
        {
        }
        catch (ArgumentException)
        {
        }

        if (!ok)
        {
            rsa?.Dispose();
            throw new PasswordSealException(PasswordSealFailure.Malformed);
        }

        return new PasswordPrivateKey(rsa!);
    }

    /// <summary>The key as PKCS#8 DER. The caller owns the bytes and should zero them after use.</summary>
    public byte[] ExportPkcs8()
    {
        lock (_gate)
        {
            return _rsa.ExportPkcs8PrivateKey();
        }
    }

    /// <summary>Releases the key.</summary>
    public void Dispose()
    {
        lock (_gate)
        {
            _rsa.Dispose();
        }
    }

    internal byte[] DecryptKey(byte[] wrapped)
    {
        lock (_gate)
        {
            return _rsa.Decrypt(wrapped, RSAEncryptionPadding.OaepSHA256);
        }
    }
}

/// <summary>
/// Seals a password to the service's public key and opens it with the private key (#5366). A value is
/// <c>sealed:v1:&lt;keyId&gt;:&lt;base64&gt;</c>. The payload is the AES-256 key wrapped with RSA-OAEP-SHA256, then a
/// 12-byte nonce, a 16-byte tag and the AES-GCM ciphertext of the length-prefixed, zero-padded password. The
/// <see cref="PasswordBinding"/> is the GCM associated data, so a value opens only for the connection it was sealed for.
/// </summary>
public static class PasswordSeal
{
    /// <summary>Every sealed version starts with this text.</summary>
    public const string RoutingPrefix = "sealed:";

    /// <summary>The prefix of a version 1 value.</summary>
    public const string V1Prefix = "sealed:v1:";

    /// <summary>The algorithm text shown for the key.</summary>
    public const string Algorithm = "RSA3072-OAEP-SHA256/A256GCM";

    /// <summary>The longest password, in UTF-8 bytes, that can be sealed.</summary>
    public const int MaxPlaintextBytes = 8192;

    internal const int KeyBits = 3072;

    private const int WrappedKeyBytes = KeyBits / 8;
    private const int NonceBytes = 12;
    private const int TagBytes = 16;
    private const int HeaderBytes = WrappedKeyBytes + NonceBytes + TagBytes;
    private const int PadBlock = 32;
    private const int KeyIdChars = 16;
    private const int MaxPaddedBytes = ((MaxPlaintextBytes + 4 + PadBlock - 1) / PadBlock) * PadBlock;

    /// <summary>True when the text starts a sealed value of any version.</summary>
    public static bool IsSealed(string? value) =>
        value is not null && value.StartsWith(RoutingPrefix, StringComparison.Ordinal);

    /// <summary>The key id a version 1 value carries, or null when the text is not a version 1 value with a key id.</summary>
    public static string? KeyIdOf(string value)
    {
        if (value is null || !value.StartsWith(V1Prefix, StringComparison.Ordinal))
        {
            return null;
        }

        var rest = value.AsSpan(V1Prefix.Length);
        return rest.Length > KeyIdChars && rest[KeyIdChars] == ':' && IsKeyId(rest[..KeyIdChars])
            ? rest[..KeyIdChars].ToString()
            : null;
    }

    /// <summary>The key id for a key: the first 8 bytes of the SHA-256 of its SubjectPublicKeyInfo, as lowercase hex.</summary>
    public static string KeyIdFor(ReadOnlySpan<byte> spki)
    {
        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(spki, hash);
        return Convert.ToHexStringLower(hash[..8]);
    }

    /// <summary>A key id as shown to people: uppercase, in groups of four (<c>AB12-CD34-EF56-7890</c>). Text that is not
    /// a key id comes back unchanged.</summary>
    public static string DisplayKeyId(string keyId)
    {
        if (keyId is null || !IsKeyId(keyId))
        {
            return keyId ?? "";
        }

        var upper = keyId.ToUpperInvariant();
        return string.Join('-', upper[..4], upper[4..8], upper[8..12], upper[12..16]);
    }

    /// <summary>Seals <paramref name="plaintext"/> to <paramref name="key"/> for <paramref name="binding"/>. The result is
    /// different every time. A password over <see cref="MaxPlaintextBytes"/> is refused.</summary>
    public static string Seal(string plaintext, PasswordPublicKey key, PasswordBinding binding)
    {
        ArgumentNullException.ThrowIfNull(plaintext);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(binding);

        var length = Encoding.UTF8.GetByteCount(plaintext);
        if (length > MaxPlaintextBytes)
        {
            throw new PasswordSealException(
                PasswordSealFailure.Malformed, "The password is longer than a sealed value can hold.", null);
        }

        var padded = new byte[Math.Max(PadBlock, (length + 4 + PadBlock - 1) / PadBlock * PadBlock)];
        var aesKey = new byte[32];
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(padded, length);
            Encoding.UTF8.GetBytes(plaintext, padded.AsSpan(4));
            RandomNumberGenerator.Fill(aesKey);

            var payload = new byte[HeaderBytes + padded.Length];
            var nonce = payload.AsSpan(WrappedKeyBytes, NonceBytes);
            var tag = payload.AsSpan(WrappedKeyBytes + NonceBytes, TagBytes);
            RandomNumberGenerator.Fill(nonce);

            using (var rsa = RSA.Create())
            {
                rsa.ImportSubjectPublicKeyInfo(key.Spki, out _);
                rsa.Encrypt(aesKey, RSAEncryptionPadding.OaepSHA256).CopyTo(payload, 0);
            }

            using (var aes = new AesGcm(aesKey, TagBytes))
            {
                aes.Encrypt(nonce, padded, payload.AsSpan(HeaderBytes), tag, binding.EncodeAad(key.KeyId));
            }

            return V1Prefix + key.KeyId + ":" + Convert.ToBase64String(payload);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(aesKey);
            CryptographicOperations.ZeroMemory(padded);
        }
    }

    /// <summary>
    /// Opens <paramref name="sealedText"/> with <paramref name="key"/> for <paramref name="binding"/>. Every failure is a
    /// <see cref="PasswordSealException"/> with no inner exception: <see cref="PasswordSealFailure.Malformed"/> for text
    /// that is not a sealed value, <see cref="PasswordSealFailure.NewerVersion"/> for a version this build does not
    /// know, <see cref="PasswordSealFailure.UnknownKey"/> for another key, and
    /// <see cref="PasswordSealFailure.BindingOrTamper"/> for everything that fails after that.
    /// </summary>
    public static string Open(string sealedText, PasswordPrivateKey key, PasswordBinding binding)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(binding);

        if (sealedText is null || !sealedText.StartsWith(RoutingPrefix, StringComparison.Ordinal))
        {
            throw new PasswordSealException(PasswordSealFailure.Malformed);
        }

        if (!sealedText.StartsWith(V1Prefix, StringComparison.Ordinal))
        {
            throw new PasswordSealException(PasswordSealFailure.NewerVersion);
        }

        var rest = sealedText.AsSpan(V1Prefix.Length);
        if (rest.Length <= KeyIdChars || rest[KeyIdChars] != ':' || !IsKeyId(rest[..KeyIdChars]))
        {
            throw new PasswordSealException(PasswordSealFailure.Malformed);
        }

        var keyId = rest[..KeyIdChars].ToString();
        var payload = DecodePayload(rest[(KeyIdChars + 1)..], keyId);
        if (!string.Equals(keyId, key.PublicKey.KeyId, StringComparison.Ordinal))
        {
            throw new PasswordSealException(PasswordSealFailure.UnknownKey, keyId);
        }

        byte[]? aesKey = null;
        byte[]? padded = null;
        var failed = false;
        string? result = null;
        try
        {
            aesKey = key.DecryptKey(payload[..WrappedKeyBytes]);
            if (aesKey.Length != 32)
            {
                failed = true;
            }
            else
            {
                padded = new byte[payload.Length - HeaderBytes];
                using var aes = new AesGcm(aesKey, TagBytes);
                aes.Decrypt(
                    payload.AsSpan(WrappedKeyBytes, NonceBytes),
                    payload.AsSpan(HeaderBytes),
                    payload.AsSpan(WrappedKeyBytes + NonceBytes, TagBytes),
                    padded,
                    binding.EncodeAad(keyId));
                result = ReadPadded(padded);
                failed = result is null;
            }
        }
        catch (CryptographicException)
        {
            failed = true;
        }
        finally
        {
            if (aesKey is not null)
            {
                CryptographicOperations.ZeroMemory(aesKey);
            }

            if (padded is not null)
            {
                CryptographicOperations.ZeroMemory(padded);
            }
        }

        if (failed || result is null)
        {
            throw new PasswordSealException(PasswordSealFailure.BindingOrTamper, keyId);
        }

        return result;
    }

    private static byte[] DecodePayload(ReadOnlySpan<char> base64, string keyId)
    {
        var maxChars = (HeaderBytes + MaxPaddedBytes + 2) / 3 * 4;
        if (base64.Length == 0 || base64.Length > maxChars || base64.Length % 4 != 0)
        {
            throw new PasswordSealException(PasswordSealFailure.Malformed, keyId);
        }

        foreach (var c in base64)
        {
            if (!(c is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '+' or '/' or '='))
            {
                throw new PasswordSealException(PasswordSealFailure.Malformed, keyId);
            }
        }

        var buffer = new byte[base64.Length / 4 * 3];
        if (!Convert.TryFromBase64Chars(base64, buffer, out var written))
        {
            throw new PasswordSealException(PasswordSealFailure.Malformed, keyId);
        }

        var minimum = HeaderBytes + PadBlock;
        if (written < minimum || written > HeaderBytes + MaxPaddedBytes || (written - HeaderBytes) % PadBlock != 0)
        {
            throw new PasswordSealException(PasswordSealFailure.Malformed, keyId);
        }

        return buffer[..written];
    }

    private static string? ReadPadded(byte[] padded)
    {
        var length = BinaryPrimitives.ReadInt32BigEndian(padded);
        if (length < 0 || length > MaxPlaintextBytes || length > padded.Length - 4)
        {
            return null;
        }

        for (var i = 4 + length; i < padded.Length; i++)
        {
            if (padded[i] != 0)
            {
                return null;
            }
        }

        try
        {
            return new UTF8Encoding(false, true).GetString(padded, 4, length);
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

    private static bool IsKeyId(ReadOnlySpan<char> text)
    {
        if (text.Length != KeyIdChars)
        {
            return false;
        }

        foreach (var c in text)
        {
            if (!(c is >= '0' and <= '9' or >= 'a' and <= 'f'))
            {
                return false;
            }
        }

        return true;
    }
}
